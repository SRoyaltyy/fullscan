# Factor mine action — `combo_hj_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_hot_n4_h1/union_join_vol_green_h1 w=0.5,0.5 net=priority

Cash book **+10.64%** ($11,064) · signal-only (no cash/fees) was —. Starts YES **11/19**. Fills 180 · skips 46 · realized $+1064.02.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_hot_n4_h1 50%, union_join_vol_green_h1 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_hot_n4_h1 50%, union_join_vol_green_h1 50%.
- Member: union_hot_n4_h1 (50% · long · hold 1).
- Member: union_join_vol_green_h1 (50% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,064.00.

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
| 2026-08-14 | `BTBT` | 494 | — | $1.50 | +0.00 | $1.57 | +34.58 | +34.58 | +0.00 | +34.58 |
| 2026-08-14 | `BETR` | 50 | — | $14.80 | +0.00 | $13.73 | -53.50 | -53.50 | +0.00 | -53.50 |
| 2026-08-14 | `ANGX` | 172 | — | $4.31 | +0.00 | $4.37 | +10.32 | +10.32 | +0.00 | +10.32 |
| 2026-08-14 | `HYLN` | 177 | — | $4.18 | +0.00 | $4.06 | -21.24 | -21.24 | +0.00 | -21.24 |
| 2026-08-14 | `ADUR` | 44 | — | $16.50 | +0.00 | $16.17 | -14.52 | -14.52 | +0.00 | -14.52 |
| 2026-08-14 | `NCMI` | 275 | — | $2.69 | +0.00 | $2.86 | +46.75 | +46.75 | +0.00 | +46.75 |
| 2026-08-14 | `QMLS` | 101 | — | $7.29 | +0.00 | $7.32 | +3.03 | +3.03 | +0.00 | +3.03 |
| 2026-08-17 | `QMCO` | 52 | $26.11 | $24.83 | -66.56 | — | +0.00 | -66.56 | +7.80 | — |
| 2026-08-17 | `ARX` | 66 | $19.58 | $19.57 | -0.66 | — | +0.00 | -0.66 | +0.00 | — |
| 2026-08-17 | `ZENA` | 589 | $2.14 | $2.08 | -32.40 | — | +0.00 | -32.40 | -67.74 | — |
| 2026-08-17 | `AIRO` | 116 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -179.80 | — |
| 2026-08-17 | `BTBT` | 494 | $1.57 | $1.52 | -24.70 | — | +0.00 | -24.70 | +9.88 | — |
| 2026-08-17 | `BETR` | 50 | $13.73 | $13.67 | -3.00 | — | +0.00 | -3.00 | -56.50 | — |
| 2026-08-17 | `ANGX` | 172 | $4.37 | $4.60 | +39.56 | — | +0.00 | +39.56 | +49.88 | — |
| 2026-08-17 | `HYLN` | 177 | $4.06 | $4.10 | +7.08 | — | +0.00 | +7.08 | -14.16 | — |
| 2026-08-17 | `ADUR` | 44 | $16.17 | $15.73 | -19.36 | — | +0.00 | -19.36 | -33.88 | — |
| 2026-08-17 | `NCMI` | 275 | $2.86 | $2.80 | -16.50 | — | +0.00 | -16.50 | +30.25 | — |
| 2026-08-17 | `QMLS` | 101 | $7.32 | $7.24 | -8.08 | — | +0.00 | -8.08 | -5.05 | — |
| 2026-08-17 | `XHG` | 299 | — | $4.19 | +0.00 | $3.91 | -83.72 | -83.72 | +0.00 | -83.72 |
| 2026-08-17 | `CAPR` | 182 | — | $6.87 | +0.00 | $7.45 | +105.56 | +105.56 | +0.00 | +105.56 |
| 2026-08-17 | `STDN` | 91 | — | $13.64 | +0.00 | $13.31 | -30.03 | -30.03 | +0.00 | -30.03 |
| 2026-08-17 | `HTFL` | 30 | — | $41.23 | +0.00 | $41.94 | +21.30 | +21.30 | +0.00 | +21.30 |
| 2026-08-17 | `ABX` | 138 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `ALOY` | 86 | — | $14.66 | +0.00 | $13.86 | -69.23 | -69.23 | +0.00 | -69.23 |
| 2026-08-17 | `BORR` | 274 | — | $4.59 | +0.00 | $4.50 | -24.66 | -24.66 | +0.00 | -24.66 |
| 2026-08-17 | `MP` | 21 | — | $58.01 | +0.00 | $58.51 | +10.50 | +10.50 | +0.00 | +10.50 |
| 2026-08-18 | `XHG` | 299 | $3.91 | $3.94 | +8.97 | — | +0.00 | +8.97 | -74.75 | — |
| 2026-08-18 | `CAPR` | 182 | $7.45 | $7.50 | +9.10 | $7.08 | -76.44 | -67.34 | +114.66 | +38.22 |
| 2026-08-18 | `STDN` | 91 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -30.03 | — |
| 2026-08-18 | `HTFL` | 30 | $41.94 | $41.50 | -13.20 | — | +0.00 | -13.20 | +8.10 | — |
| 2026-08-18 | `ABX` | 138 | $9.12 | $9.03 | -12.42 | — | +0.00 | -12.42 | -12.42 | — |
| 2026-08-18 | `ALOY` | 86 | $13.86 | $13.19 | -57.19 | — | +0.00 | -57.19 | -126.42 | — |
| 2026-08-18 | `BORR` | 274 | $4.50 | $4.56 | +16.44 | — | +0.00 | +16.44 | -8.22 | — |
| 2026-08-18 | `MP` | 21 | $58.51 | $56.35 | -45.36 | — | +0.00 | -45.36 | -34.86 | — |
| 2026-08-19 | `CAPR` | 182 | $7.08 | $7.19 | +20.02 | — | +0.00 | +20.02 | +58.24 | — |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `CYPH` | 1062 | — | $1.15 | +0.00 | $1.19 | +42.48 | +42.48 | +0.00 | +42.48 |
| 2026-08-20 | `ABCL` | 103 | — | $11.81 | +0.00 | $11.57 | -25.23 | -25.23 | +0.00 | -25.23 |
| 2026-08-20 | `AZI` | 891 | — | $1.37 | +0.00 | $1.44 | +62.37 | +62.37 | +0.00 | +62.37 |
| 2026-08-20 | `AG` | 29 | — | $20.55 | +0.00 | $21.19 | +18.56 | +18.56 | +0.00 | +18.56 |
| 2026-08-20 | `CDE` | 29 | — | $20.65 | +0.00 | $21.11 | +13.34 | +13.34 | +0.00 | +13.34 |
| 2026-08-20 | `HDSN` | 105 | — | $5.77 | +0.00 | $5.57 | -21.00 | -21.00 | +0.00 | -21.00 |
| 2026-08-20 | `IAG` | 31 | — | $19.63 | +0.00 | $20.50 | +26.97 | +26.97 | +0.00 | +26.97 |
| 2026-08-20 | `KGC` | 20 | — | $29.63 | +0.00 | $31.43 | +36.00 | +36.00 | +0.00 | +36.00 |
| 2026-08-20 | `NFGC` | 348 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 4 | — | $144.54 | +0.00 | $150.25 | +22.84 | +22.84 | +0.00 | +22.84 |
| 2026-08-20 | `ABUS` | 124 | — | $4.92 | +0.00 | $4.77 | -18.60 | -18.60 | +0.00 | -18.60 |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | $145.13 | +96.16 | +94.48 | -136.24 | -40.08 |
| 2026-08-21 | `CYPH` | 1062 | $1.19 | $1.32 | +138.06 | $1.42 | +106.20 | +244.26 | +180.54 | +286.74 |
| 2026-08-21 | `ABCL` | 103 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -25.23 | — |
| 2026-08-21 | `AZI` | 891 | $1.44 | $1.46 | +17.82 | — | +0.00 | +17.82 | +80.19 | — |
| 2026-08-21 | `AG` | 29 | $21.19 | $21.90 | +20.59 | — | +0.00 | +20.59 | +39.15 | — |
| 2026-08-21 | `CDE` | 29 | $21.11 | $21.75 | +18.56 | — | +0.00 | +18.56 | +31.90 | — |
| 2026-08-21 | `HDSN` | 105 | $5.57 | $5.67 | +10.50 | — | +0.00 | +10.50 | -10.50 | — |
| 2026-08-21 | `IAG` | 31 | $20.50 | $21.17 | +20.77 | — | +0.00 | +20.77 | +47.74 | — |
| 2026-08-21 | `KGC` | 20 | $31.43 | $32.17 | +14.80 | — | +0.00 | +14.80 | +50.80 | — |
| 2026-08-21 | `NFGC` | 348 | $1.75 | $1.79 | +13.92 | — | +0.00 | +13.92 | +13.92 | — |
| 2026-08-21 | `WPM` | 4 | $150.25 | $154.70 | +17.80 | — | +0.00 | +17.80 | +40.64 | — |
| 2026-08-21 | `ABUS` | 124 | $4.77 | $5.20 | +53.32 | — | +0.00 | +53.32 | +34.72 | — |
| 2026-08-21 | `XHG` | 421 | — | $4.49 | +0.00 | $4.41 | -33.68 | -33.68 | +0.00 | -33.68 |
| 2026-08-21 | `CAPR` | 277 | — | $6.81 | +0.00 | $6.29 | -144.04 | -144.04 | +0.00 | -144.04 |
| 2026-08-21 | `AU` | 4 | — | $119.43 | +0.00 | $121.22 | +7.16 | +7.16 | +0.00 | +7.16 |
| 2026-08-21 | `AUPH` | 31 | — | $17.20 | +0.00 | $16.65 | -17.05 | -17.05 | +0.00 | -17.05 |
| 2026-08-21 | `AEM` | 2 | — | $216.30 | +0.00 | $216.06 | -0.48 | -0.48 | +0.00 | -0.48 |
| 2026-08-21 | `ARCT` | 48 | — | $11.13 | +0.00 | $13.45 | +111.36 | +111.36 | +0.00 | +111.36 |
| 2026-08-21 | `BTBT` | 325 | — | $1.66 | +0.00 | $1.53 | -42.25 | -42.25 | +0.00 | -42.25 |
| 2026-08-21 | `INDP` | 389 | — | $1.39 | +0.00 | $1.29 | -38.90 | -38.90 | +0.00 | -38.90 |
| 2026-08-21 | `MRVI` | 65 | — | $8.28 | +0.00 | $8.64 | +23.40 | +23.40 | +0.00 | +23.40 |
| 2026-08-24 | `MRNA` | 8 | $145.13 | $142.70 | -19.44 | — | +0.00 | -19.44 | -59.52 | — |
| 2026-08-24 | `CYPH` | 1062 | $1.42 | $1.83 | +435.42 | — | +0.00 | +435.42 | +722.16 | — |
| 2026-08-24 | `XHG` | 421 | $4.41 | $4.32 | -37.89 | — | +0.00 | -37.89 | -71.57 | — |
| 2026-08-24 | `CAPR` | 277 | $6.29 | $8.03 | +481.98 | — | +0.00 | +481.98 | +337.94 | — |
| 2026-08-24 | `AU` | 4 | $121.22 | $120.51 | -2.84 | — | +0.00 | -2.84 | +4.32 | — |
| 2026-08-24 | `AUPH` | 31 | $16.65 | $16.57 | -2.48 | — | +0.00 | -2.48 | -19.53 | — |
| 2026-08-24 | `AEM` | 2 | $216.06 | $217.03 | +1.94 | — | +0.00 | +1.94 | +1.46 | — |
| 2026-08-24 | `ARCT` | 48 | $13.45 | $13.33 | -5.76 | — | +0.00 | -5.76 | +105.60 | — |
| 2026-08-24 | `BTBT` | 325 | $1.53 | $1.55 | +6.50 | — | +0.00 | +6.50 | -35.75 | — |
| 2026-08-24 | `INDP` | 389 | $1.29 | $1.24 | -19.45 | — | +0.00 | -19.45 | -58.35 | — |
| 2026-08-24 | `MRVI` | 65 | $8.64 | $8.59 | -3.25 | — | +0.00 | -3.25 | +20.15 | — |
| 2026-08-25 | `REAX` | 56 | — | $24.11 | +0.00 | $28.43 | +241.92 | +241.92 | +0.00 | +241.92 |
| 2026-08-25 | `CYPH` | 870 | — | $1.56 | +0.00 | $1.64 | +69.60 | +69.60 | +0.00 | +69.60 |
| 2026-08-25 | `XHG` | 333 | — | $4.07 | +0.00 | $4.02 | -16.65 | -16.65 | +0.00 | -16.65 |
| 2026-08-25 | `ASST` | 71 | — | $19.04 | +0.00 | $21.39 | +166.85 | +166.85 | +0.00 | +166.85 |
| 2026-08-25 | `BMEA` | 416 | — | $1.63 | +0.00 | $1.73 | +41.60 | +41.60 | +0.00 | +41.60 |
| 2026-08-25 | `GORO` | 191 | — | $3.55 | +0.00 | $3.87 | +61.12 | +61.12 | +0.00 | +61.12 |
| 2026-08-25 | `ZURA` | 106 | — | $6.37 | +0.00 | $6.32 | -5.30 | -5.30 | +0.00 | -5.30 |
| 2026-08-25 | `EZPW` | 19 | — | $35.05 | +0.00 | $35.23 | +3.42 | +3.42 | +0.00 | +3.42 |
| 2026-08-25 | `ETON` | 10 | — | $64.55 | +0.00 | $63.05 | -15.00 | -15.00 | +0.00 | -15.00 |
| 2026-08-25 | `WPM` | 4 | — | $156.51 | +0.00 | $163.72 | +28.84 | +28.84 | +0.00 | +28.84 |
| 2026-08-25 | `SUZ` | 75 | — | $8.98 | +0.00 | $9.03 | +3.75 | +3.75 | +0.00 | +3.75 |
| 2026-08-25 | `IAUX` | 357 | — | $1.90 | +0.00 | $1.92 | +7.14 | +7.14 | +0.00 | +7.14 |
| 2026-08-26 | `REAX` | 56 | $28.43 | $26.61 | -101.92 | — | +0.00 | -101.92 | +140.00 | — |
| 2026-08-26 | `CYPH` | 870 | $1.64 | $1.60 | -34.80 | — | +0.00 | -34.80 | +34.80 | — |
| 2026-08-26 | `XHG` | 333 | $4.02 | $3.81 | -69.93 | $4.06 | +83.25 | +13.32 | -86.58 | -3.33 |
| 2026-08-26 | `ASST` | 71 | $21.39 | $20.72 | -47.57 | — | +0.00 | -47.57 | +119.28 | — |
| 2026-08-26 | `BMEA` | 416 | $1.73 | $1.75 | +10.40 | — | +0.00 | +10.40 | +52.00 | — |
| 2026-08-26 | `GORO` | 191 | $3.87 | $3.77 | -19.10 | — | +0.00 | -19.10 | +42.02 | — |
| 2026-08-26 | `ZURA` | 106 | $6.32 | $6.13 | -20.14 | — | +0.00 | -20.14 | -25.44 | — |
| 2026-08-26 | `EZPW` | 19 | $35.23 | $35.70 | +8.93 | — | +0.00 | +8.93 | +12.35 | — |
| 2026-08-26 | `ETON` | 10 | $63.05 | $63.60 | +5.50 | — | +0.00 | +5.50 | -9.50 | — |
| 2026-08-26 | `WPM` | 4 | $163.72 | $160.93 | -11.16 | — | +0.00 | -11.16 | +17.68 | — |
| 2026-08-26 | `SUZ` | 75 | $9.03 | $9.03 | +0.00 | — | +0.00 | +0.00 | +3.75 | — |
| 2026-08-26 | `IAUX` | 357 | $1.92 | $1.87 | -17.85 | — | +0.00 | -17.85 | -10.71 | — |
| 2026-08-26 | `BYND` | 231 | — | $14.11 | +0.00 | $14.25 | +32.34 | +32.34 | +0.00 | +32.34 |
| 2026-08-26 | `USDE` | 562 | — | $5.81 | +0.00 | $5.98 | +95.54 | +95.54 | +0.00 | +95.54 |
| 2026-08-26 | `SUJA` | 347 | — | $9.39 | +0.00 | $9.44 | +17.35 | +17.35 | +0.00 | +17.35 |
| 2026-08-27 | `XHG` | 333 | $4.06 | $4.06 | +0.00 | — | +0.00 | +0.00 | -3.33 | — |
| 2026-08-27 | `BYND` | 231 | $14.25 | $14.20 | -11.55 | — | +0.00 | -11.55 | +20.79 | — |
| 2026-08-27 | `USDE` | 562 | $5.98 | $6.50 | +292.24 | — | +0.00 | +292.24 | +387.78 | — |
| 2026-08-27 | `SUJA` | 347 | $9.44 | $9.41 | -10.41 | — | +0.00 | -10.41 | +6.94 | — |
| 2026-08-27 | `SLI` | 1109 | — | $2.60 | +0.00 | $2.64 | +44.36 | +44.36 | +0.00 | +44.36 |
| 2026-08-27 | `RRC` | 69 | — | $41.44 | +0.00 | $41.64 | +13.80 | +13.80 | +0.00 | +13.80 |
| 2026-08-27 | `PGY` | 125 | — | $22.93 | +0.00 | $23.26 | +41.25 | +41.25 | +0.00 | +41.25 |
| 2026-08-27 | `CRK` | 200 | — | $14.42 | +0.00 | $14.62 | +40.00 | +40.00 | +0.00 | +40.00 |
| 2026-08-28 | `SLI` | 1109 | $2.64 | $2.68 | +44.36 | — | +0.00 | +44.36 | +88.72 | — |
| 2026-08-28 | `RRC` | 69 | $41.64 | $41.74 | +6.90 | — | +0.00 | +6.90 | +20.70 | — |
| 2026-08-28 | `PGY` | 125 | $23.26 | $23.21 | -6.25 | — | +0.00 | -6.25 | +35.00 | — |
| 2026-08-28 | `CRK` | 200 | $14.62 | $14.63 | +2.00 | — | +0.00 | +2.00 | +42.00 | — |
| 2026-08-28 | `BYND` | 104 | — | $14.00 | +0.00 | $13.86 | -14.56 | -14.56 | +0.00 | -14.56 |
| 2026-08-28 | `CAPR` | 150 | — | $9.73 | +0.00 | $9.59 | -21.00 | -21.00 | +0.00 | -21.00 |
| 2026-08-28 | `MRNA` | 10 | — | $137.19 | +0.00 | $137.99 | +8.00 | +8.00 | +0.00 | +8.00 |
| 2026-08-28 | `ANF` | 9 | — | $146.07 | +0.00 | $148.42 | +21.15 | +21.15 | +0.00 | +21.15 |
| 2026-08-28 | `NCNO` | 86 | — | $23.30 | +0.00 | $22.99 | -26.66 | -26.66 | +0.00 | -26.66 |
| 2026-08-28 | `TH` | 106 | — | $19.00 | +0.00 | $18.55 | -47.70 | -47.70 | +0.00 | -47.70 |
| 2026-08-28 | `GAP` | 81 | — | $24.69 | +0.00 | $23.48 | -98.01 | -98.01 | +0.00 | -98.01 |
| 2026-08-31 | `BYND` | 104 | $13.86 | $13.81 | -5.20 | $13.30 | -53.04 | -58.24 | -19.76 | -72.80 |
| 2026-08-31 | `CAPR` | 150 | $9.59 | $9.50 | -13.50 | — | +0.00 | -13.50 | -34.50 | — |
| 2026-08-31 | `MRNA` | 10 | $137.99 | $134.10 | -38.90 | — | +0.00 | -38.90 | -30.90 | — |
| 2026-08-31 | `ANF` | 9 | $148.42 | $148.03 | -3.51 | — | +0.00 | -3.51 | +17.64 | — |
| 2026-08-31 | `NCNO` | 86 | $22.99 | $22.66 | -28.38 | — | +0.00 | -28.38 | -55.04 | — |
| 2026-08-31 | `TH` | 106 | $18.55 | $18.12 | -45.05 | — | +0.00 | -45.05 | -92.75 | — |
| 2026-08-31 | `GAP` | 81 | $23.48 | $22.98 | -40.50 | — | +0.00 | -40.50 | -138.51 | — |
| 2026-09-01 | `BYND` | 104 | $13.30 | $13.04 | -27.04 | — | +0.00 | -27.04 | -99.84 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GPRO` | 787 | — | $1.78 | +0.00 | $1.39 | -306.93 | -306.93 | +0.00 | -306.93 |
| 2026-09-03 | `REAX` | 76 | — | $18.40 | +0.00 | $18.40 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `CNH` | 102 | — | $13.71 | +0.00 | $13.84 | +13.26 | +13.26 | +0.00 | +13.26 |
| 2026-09-03 | `MMED` | 58 | — | $23.88 | +0.00 | $23.84 | -2.32 | -2.32 | +0.00 | -2.32 |
| 2026-09-03 | `RVTY` | 6 | — | $132.45 | +0.00 | $130.63 | -10.92 | -10.92 | +0.00 | -10.92 |
| 2026-09-03 | `ARCT` | 47 | — | $16.77 | +0.00 | $15.56 | -56.87 | -56.87 | +0.00 | -56.87 |
| 2026-09-03 | `CRDL` | 368 | — | $2.18 | +0.00 | $2.16 | -7.36 | -7.36 | +0.00 | -7.36 |
| 2026-09-03 | `NVAX` | 77 | — | $10.42 | +0.00 | $10.34 | -6.16 | -6.16 | +0.00 | -6.16 |
| 2026-09-03 | `BMEA` | 415 | — | $1.93 | +0.00 | $1.91 | -8.30 | -8.30 | +0.00 | -8.30 |
| 2026-09-03 | `DUOL` | 4 | — | $161.54 | +0.00 | $158.82 | -10.88 | -10.88 | +0.00 | -10.88 |
| 2026-09-03 | `ALMS` | 77 | — | $10.38 | +0.00 | $11.36 | +75.84 | +75.84 | +0.00 | +75.84 |
| 2026-09-04 | `GPRO` | 787 | $1.39 | $1.48 | +70.83 | $1.70 | +173.14 | +243.97 | -236.10 | -62.96 |
| 2026-09-04 | `REAX` | 76 | $18.40 | $18.15 | -19.00 | — | +0.00 | -19.00 | -19.00 | — |
| 2026-09-04 | `CNH` | 102 | $13.84 | $13.89 | +5.10 | — | +0.00 | +5.10 | +18.36 | — |
| 2026-09-04 | `MMED` | 58 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.32 | — |
| 2026-09-04 | `RVTY` | 6 | $130.63 | $130.03 | -3.60 | — | +0.00 | -3.60 | -14.52 | — |
| 2026-09-04 | `ARCT` | 47 | $15.56 | $15.61 | +2.35 | — | +0.00 | +2.35 | -54.52 | — |
| 2026-09-04 | `CRDL` | 368 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -7.36 | — |
| 2026-09-04 | `NVAX` | 77 | $10.34 | $10.50 | +12.32 | — | +0.00 | +12.32 | +6.16 | — |
| 2026-09-04 | `BMEA` | 415 | $1.91 | $1.90 | -4.15 | — | +0.00 | -4.15 | -12.45 | — |
| 2026-09-04 | `DUOL` | 4 | $158.82 | $157.46 | -5.44 | — | +0.00 | -5.44 | -16.32 | — |
| 2026-09-04 | `ALMS` | 77 | $11.36 | $11.23 | -10.01 | — | +0.00 | -10.01 | +65.84 | — |
| 2026-09-04 | `ASST` | 64 | — | $25.18 | +0.00 | $27.14 | +125.44 | +125.44 | +0.00 | +125.44 |
| 2026-09-04 | `USDE` | 205 | — | $7.87 | +0.00 | $7.93 | +12.30 | +12.30 | +0.00 | +12.30 |
| 2026-09-04 | `DFDV` | 279 | — | $5.79 | +0.00 | $5.87 | +22.32 | +22.32 | +0.00 | +22.32 |
| 2026-09-04 | `DELL` | 1 | — | $513.78 | +0.00 | $524.14 | +10.36 | +10.36 | +0.00 | +10.36 |
| 2026-09-04 | `TARS` | 9 | — | $82.70 | +0.00 | $90.78 | +72.72 | +72.72 | +0.00 | +72.72 |
| 2026-09-04 | `BRR` | 323 | — | $2.51 | +0.00 | $2.66 | +48.45 | +48.45 | +0.00 | +48.45 |
| 2026-09-04 | `MDB` | 2 | — | $378.34 | +0.00 | $368.74 | -19.20 | -19.20 | +0.00 | -19.20 |
| 2026-09-04 | `TDS` | 21 | — | $37.44 | +0.00 | $37.83 | +8.19 | +8.19 | +0.00 | +8.19 |
| 2026-09-04 | `AHCO` | 128 | — | $6.32 | +0.00 | $6.49 | +21.76 | +21.76 | +0.00 | +21.76 |
| 2026-09-08 | `GPRO` | 787 | $1.70 | $1.56 | -106.25 | — | +0.00 | -106.25 | -169.21 | — |
| 2026-09-08 | `ASST` | 64 | $27.14 | $26.44 | -44.80 | — | +0.00 | -44.80 | +80.64 | — |
| 2026-09-08 | `USDE` | 205 | $7.93 | $7.76 | -34.85 | — | +0.00 | -34.85 | -22.55 | — |
| 2026-09-08 | `DFDV` | 279 | $5.87 | $5.81 | -16.74 | — | +0.00 | -16.74 | +5.58 | — |
| 2026-09-08 | `DELL` | 1 | $524.14 | $521.15 | -2.99 | — | +0.00 | -2.99 | +7.37 | — |
| 2026-09-08 | `TARS` | 9 | $90.78 | $89.67 | -9.99 | — | +0.00 | -9.99 | +62.73 | — |
| 2026-09-08 | `BRR` | 323 | $2.66 | $2.66 | +0.00 | — | +0.00 | +0.00 | +48.45 | — |
| 2026-09-08 | `MDB` | 2 | $368.74 | $360.75 | -15.98 | — | +0.00 | -15.98 | -35.18 | — |
| 2026-09-08 | `TDS` | 21 | $37.83 | $37.75 | -1.68 | — | +0.00 | -1.68 | +6.51 | — |
| 2026-09-08 | `AHCO` | 128 | $6.49 | $6.48 | -1.28 | — | +0.00 | -1.28 | +20.48 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +386.21 | IREN, TNDM, TPG, INO | — | $0.54 | $10,345.37 | IREN×54, TNDM×107, TPG×49, INO×3085 |
| 2026-08-14 | +5.50 | $0.54 | IREN×54, TNDM×107, TPG×49, INO×3085 | $10,412.10 | +66.73 | -134.70 | QMCO, ARX, ZENA, AIRO, BTBT, BETR, ANGX, HYLN, ADUR, NCMI, QMLS | IREN, TNDM, TPG, INO | $6.23 | $10,196.45 | QMCO×52, ARX×66, ZENA×589, AIRO×116, BTBT×494, BETR×50, ANGX×172, HYLN×177, ADUR×44, NCMI×275, QMLS×101 |
| 2026-08-17 | +2.25 | $6.23 | QMCO×52, ARX×66, ZENA×589, AIRO×116, BTBT×494, BETR×50, ANGX×172, HYLN×177, ADUR×44, NCMI×275, QMLS×101 | $10,071.83 | -124.62 | -70.28 | XHG, CAPR, STDN, HTFL, ABX, ALOY, BORR, MP | QMCO, ARX, ZENA, AIRO, BTBT, BETR, ANGX, HYLN, ADUR, NCMI, QMLS | $38.13 | $9,944.33 | XHG×299, CAPR×182, STDN×91, HTFL×30, ABX×138, ALOY×86, BORR×274, MP×21 |
| 2026-08-18 | -6.20 | $38.13 | XHG×299, CAPR×182, STDN×91, HTFL×30, ABX×138, ALOY×86, BORR×274, MP×21 | $9,850.67 | -93.66 | -76.44 | — | XHG, STDN, HTFL, ABX, ALOY, BORR, MP | $8,467.00 | $9,755.56 | CAPR×182 |
| 2026-08-19 | -7.20 | $8,467.00 | CAPR×182 | $9,775.58 | +20.02 | +0.00 | — | CAPR | $9,773.00 | $9,773.00 | — |
| 2026-08-20 | +1.12 | $9,773.00 | — | $9,773.00 | +0.00 | +23.17 | MRNA, CYPH, ABCL, AZI, AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | — | $64.99 | $9,747.21 | MRNA×8, CYPH×1062, ABCL×103, AZI×891, AG×29, CDE×29, HDSN×105, IAG×31, KGC×20, NFGC×348, WPM×4, ABUS×124 |
| 2026-08-21 | +3.25 | $64.99 | MRNA×8, CYPH×1062, ABCL×103, AZI×891, AG×29, CDE×29, HDSN×105, IAG×31, KGC×20, NFGC×348, WPM×4, ABUS×124 | $10,071.67 | +324.46 | +67.88 | XHG, CAPR, AU, AUPH, AEM, ARCT, BTBT, INDP, MRVI | ABCL, AZI, AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | $169.86 | $10,077.29 | MRNA×8, CYPH×1062, XHG×421, CAPR×277, AU×4, AUPH×31, AEM×2, ARCT×48, BTBT×325, INDP×389, MRVI×65 |
| 2026-08-24 | -5.17 | $169.86 | MRNA×8, CYPH×1062, XHG×421, CAPR×277, AU×4, AUPH×31, AEM×2, ARCT×48, BTBT×325, INDP×389, MRVI×65 | $10,912.02 | +834.73 | +0.00 | — | MRNA, CYPH, XHG, CAPR, AU, AUPH, AEM, ARCT, BTBT, INDP, MRVI | $10,867.09 | $10,867.09 | — |
| 2026-08-25 | +1.80 | $10,867.09 | — | $10,867.09 | +0.00 | +587.29 | REAX, CYPH, XHG, ASST, BMEA, GORO, ZURA, EZPW, ETON, WPM, SUZ, IAUX | — | $88.94 | $11,411.38 | REAX×56, CYPH×870, XHG×333, ASST×71, BMEA×416, GORO×191, ZURA×106, EZPW×19, ETON×10, WPM×4, SUZ×75, IAUX×357 |
| 2026-08-26 | +2.02 | $88.94 | REAX×56, CYPH×870, XHG×333, ASST×71, BMEA×416, GORO×191, ZURA×106, EZPW×19, ETON×10, WPM×4, SUZ×75, IAUX×357 | $11,113.74 | -297.64 | +228.48 | BYND, USDE, SUJA | REAX, CYPH, ASST, BMEA, GORO, ZURA, EZPW, ETON, WPM, SUZ, IAUX | $8.13 | $11,288.30 | XHG×333, BYND×231, USDE×562, SUJA×347 |
| 2026-08-27 | — | $8.13 | XHG×333, BYND×231, USDE×562, SUJA×347 | $11,558.58 | +270.28 | +139.41 | SLI, RRC, PGY, CRK | XHG, BYND, USDE, SUJA | $24.77 | $11,657.19 | SLI×1109, RRC×69, PGY×125, CRK×200 |
| 2026-08-28 | +0.75 | $24.77 | SLI×1109, RRC×69, PGY×125, CRK×200 | $11,704.20 | +47.01 | -178.78 | BYND, CAPR, MRNA, ANF, NCNO, TH, GAP | SLI, RRC, PGY, CRK | $47.11 | $11,488.05 | BYND×104, CAPR×150, MRNA×10, ANF×9, NCNO×86, TH×106, GAP×81 |
| 2026-08-31 | -5.85 | $47.11 | BYND×104, CAPR×150, MRNA×10, ANF×9, NCNO×86, TH×106, GAP×81 | $11,313.01 | -175.04 | -53.04 | — | CAPR, MRNA, ANF, NCNO, TH, GAP | $9,863.34 | $11,246.54 | BYND×104 |
| 2026-09-01 | -6.30 | $9,863.34 | BYND×104 | $11,219.50 | -27.04 | +0.00 | — | BYND | $11,217.17 | $11,217.17 | — |
| 2026-09-02 | -3.83 | $11,217.17 | — | $11,217.17 | -0.00 | +0.00 | — | — | $11,217.17 | $11,217.17 | — |
| 2026-09-03 | -0.90 | $11,217.17 | — | $11,217.17 | -0.00 | -320.64 | GPRO, REAX, CNH, MMED, RVTY, ARCT, CRDL, NVAX, BMEA, DUOL, ALMS | — | $163.48 | $10,859.02 | GPRO×787, REAX×76, CNH×102, MMED×58, RVTY×6, ARCT×47, CRDL×368, NVAX×77, BMEA×415, DUOL×4, ALMS×77 |
| 2026-09-04 | +2.25 | $163.48 | GPRO×787, REAX×76, CNH×102, MMED×58, RVTY×6, ARCT×47, CRDL×368, NVAX×77, BMEA×415, DUOL×4, ALMS×77 | $10,907.42 | +48.40 | +475.48 | ASST, USDE, DFDV, DELL, TARS, BRR, MDB, TDS, AHCO | REAX, CNH, MMED, RVTY, ARCT, CRDL, NVAX, BMEA, DUOL, ALMS | $430.97 | $11,332.18 | GPRO×787, ASST×64, USDE×205, DFDV×279, DELL×1, TARS×9, BRR×323, MDB×2, TDS×21, AHCO×128 |
| 2026-09-08 | -11.47 | $430.97 | GPRO×787, ASST×64, USDE×205, DFDV×279, DELL×1, TARS×9, BRR×323, MDB×2, TDS×21, AHCO×128 | $11,097.63 | -234.55 | +0.00 | — | GPRO, ASST, USDE, DFDV, DELL, TARS, BRR, MDB, TDS, AHCO | $11,064.00 | $11,064.00 | — |
| 2026-09-09 | -13.95 | $11,064.00 | — | $11,064.00 | +0.00 | +0.00 | — | — | $11,064.00 | $11,064.00 | — |

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
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 116 | $11.12 | $2.34 | — | $5,191.95 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $1295.87; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 494 | $1.50 | $6.37 | — | $4,444.58 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $741.71; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 50 | $14.80 | $2.14 | — | $3,702.44 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; combo leftover $741.71; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 172 | $4.31 | $2.51 | — | $2,958.61 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $741.71; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 177 | $4.18 | $2.52 | — | $2,216.23 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; combo leftover $741.71; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 44 | $16.50 | $2.12 | — | $1,488.11 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; combo leftover $741.71; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 275 | $2.69 | $3.55 | — | $744.81 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; combo leftover $741.71; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 101 | $7.29 | $2.29 | — | $6.23 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; combo leftover $741.71; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.23 | ▼ close $10,196.45 vs 09:30 $10,412.10 (session -134.70) | 16:00 close · cash $6.23 · equity $10,196.45 vs 09:30 $10,412.10 (-215.65; session marks -134.70) · 11 name(s) marked open→close (per-name table). QMCO×52 09:30 $24.68 → close $26.11 +74.36; ARX×66 09:30 $19.57 → close $19.58 +0.66; ZENA×589 09:30 $2.20 → close $2.14 -35.34; AIRO×116 09:30 $11.12 → close $9.57 -179.80; BTBT×494 09:30 $1.50 → close $1.57 +34.58; BETR×50 09:30 $14.80 → close $13.73 -53.50; ANGX×172 09:30 $4.31 → close $4.37 +10.32; HYLN×177 09:30 $4.18 → close $4.06 -21.24; ADUR×44 09:30 $16.50 → close $16.17 -14.52; NCMI×275 09:30 $2.69 → close $2.86 +46.75; QMLS×101 09:30 $7.29 → close $7.32 +3.03 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.23 | ▼ 09:30 equity $10,071.83 vs yday $10,196.45 (-124.62) | 09:30 open · cash $6.23 (unchanged overnight, no fees) · equity $10,071.83 vs prior close $10,196.45 (-124.62) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $1,295.22 | ▲ +3.49 after sell → book $10,069.67; vs 09:30 mark -2.16 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 66 | $19.57 | $2.21 | $-4.40 | $2,584.63 | ▼ -4.40 after sell → book $10,067.46; vs 09:30 mark -2.21 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 589 | $2.08 | $7.71 | $-83.04 | $3,804.99 | ▼ -83.04 after sell → book $10,059.75; vs 09:30 mark -7.71 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 116 | $9.57 | $2.37 | $-184.51 | $4,912.74 | ▼ -184.51 after sell → book $10,057.38; vs 09:30 mark -2.37 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 494 | $1.52 | $6.46 | $-2.96 | $5,657.16 | ▼ -2.96 after sell → book $10,050.92; vs 09:30 mark -6.46 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 50 | $13.67 | $2.16 | $-60.80 | $6,338.50 | ▼ -60.80 after sell → book $10,048.76; vs 09:30 mark -2.16 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 172 | $4.60 | $2.54 | $+44.83 | $7,127.16 | ▲ +44.83 after sell → book $10,046.22; vs 09:30 mark -2.54 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 177 | $4.10 | $2.56 | $-19.24 | $7,850.30 | ▼ -19.24 after sell → book $10,043.66; vs 09:30 mark -2.56 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 44 | $15.73 | $2.14 | $-38.14 | $8,540.27 | ▼ -38.14 after sell → book $10,041.51; vs 09:30 mark -2.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 275 | $2.80 | $3.60 | $+23.10 | $9,306.67 | ▲ +23.10 after sell → book $10,037.91; vs 09:30 mark -3.60 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 101 | $7.24 | $2.32 | $-9.66 | $10,035.59 | ▼ -9.66 after sell → book $10,035.59; vs 09:30 mark -2.32 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 299 | $4.19 | $3.86 | — | $8,778.92 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $1254.45; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 182 | $6.87 | $2.54 | — | $7,526.05 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $1254.45; owner union_hot_n4_h1 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 91 | $13.64 | $2.26 | — | $6,282.54 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $1254.45; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 30 | $41.23 | $2.08 | — | $5,043.56 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $1254.45; owner union_hot_n4_h1 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 138 | $9.12 | $2.40 | — | $3,782.60 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; combo leftover $1260.89; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 86 | $14.66 | $2.25 | — | $2,519.59 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; combo leftover $1260.89; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 274 | $4.59 | $3.53 | — | $1,258.40 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; combo leftover $1260.89; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `MP` | 21 | $58.01 | $2.05 | — | $38.13 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.9; combo leftover $1260.89; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.13 | ▼ close $9,944.33 vs 09:30 $10,071.83 (session -70.28) | 16:00 close · cash $38.13 · equity $9,944.33 vs 09:30 $10,071.83 (-127.50; session marks -70.28) · 8 name(s) marked open→close (per-name table). XHG×299 09:30 $4.19 → close $3.91 -83.72; CAPR×182 09:30 $6.87 → close $7.45 +105.56; STDN×91 09:30 $13.64 → close $13.31 -30.03; HTFL×30 09:30 $41.23 → close $41.94 +21.30; ABX×138 09:30 $9.12 → close $9.12 +0.00; ALOY×86 09:30 $14.66 → close $13.86 -69.23; BORR×274 09:30 $4.59 → close $4.50 -24.66; MP×21 09:30 $58.01 → close $58.51 +10.50 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.13 | ▼ 09:30 equity $9,850.67 vs yday $9,944.33 (-93.66) | 09:30 open · cash $38.13 (unchanged overnight, no fees) · equity $9,850.67 vs prior close $9,944.33 (-93.66) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 299 | $3.94 | $3.92 | $-82.52 | $1,212.28 | ▼ -82.52 after sell → book $9,846.76; vs 09:30 mark -3.91 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 91 | $13.31 | $2.29 | $-34.58 | $2,421.20 | ▼ -34.58 after sell → book $9,844.47; vs 09:30 mark -2.29 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 30 | $41.50 | $2.10 | $+3.92 | $3,664.10 | ▲ +3.92 after sell → book $9,842.37; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 138 | $9.03 | $2.44 | $-17.26 | $4,907.80 | ▼ -17.26 after sell → book $9,839.93; vs 09:30 mark -2.44 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 86 | $13.19 | $2.27 | $-130.94 | $6,039.87 | ▼ -130.94 after sell → book $9,837.66; vs 09:30 mark -2.27 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 274 | $4.56 | $3.59 | $-15.34 | $7,285.72 | ▼ -15.34 after sell → book $9,834.07; vs 09:30 mark -3.59 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `MP` | 21 | $56.35 | $2.07 | $-38.99 | $8,467.00 | ▼ -38.99 after sell → book $9,832.00; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,467.00 | ▼ close $9,755.56 vs 09:30 $9,850.67 (session -76.44) | 16:00 close · cash $8,467.00 · equity $9,755.56 vs 09:30 $9,850.67 (-95.11; session marks -76.44) · 1 name(s) marked open→close (per-name table). CAPR×182 09:30 $7.50 → close $7.08 -76.44 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,467.00 | ▲ 09:30 equity $9,775.58 vs yday $9,755.56 (+20.02) | 09:30 open · cash $8,467.00 (unchanged overnight, no fees) · equity $9,775.58 vs prior close $9,755.56 (+20.02) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 182 | $7.19 | $2.58 | $+53.13 | $9,773.00 | ▲ +53.13 after sell → book $9,773.00; vs 09:30 mark -2.58 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,773.00 | ▲ close $9,773.00 vs 09:30 $9,775.58 (session +0.00) | 16:00 close · cash $9,773.00 · no lots left · equity $9,773.00. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,773.00 | ▲ 09:30 equity $9,773.00 vs yday $9,773.00 (+0.00) | 09:30 open · cash $9,773.00 (unchanged overnight, no fees) · equity $9,773.00 vs prior close $9,773.00 (+0.00) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $8,569.87 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $1221.63; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1062 | $1.15 | $13.70 | — | $7,334.87 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $1221.63; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 103 | $11.81 | $2.30 | — | $6,115.62 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1221.63; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 891 | $1.37 | $11.49 | — | $4,883.46 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $1221.63; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 29 | $20.55 | $2.08 | — | $4,285.43 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; combo leftover $610.43; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 29 | $20.65 | $2.08 | — | $3,684.51 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; combo leftover $610.43; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 105 | $5.77 | $2.31 | — | $3,076.35 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; combo leftover $610.43; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 31 | $19.63 | $2.08 | — | $2,465.74 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; combo leftover $610.43; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 20 | $29.63 | $2.05 | — | $1,871.09 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; combo leftover $610.43; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 348 | $1.75 | $4.49 | — | $1,257.60 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; combo leftover $610.43; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $677.44 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; combo leftover $610.43; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 124 | $4.92 | $2.36 | — | $64.99 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $610.43; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $64.99 | ▲ close $9,747.21 vs 09:30 $9,773.00 (session +23.17) | 16:00 close · cash $64.99 · equity $9,747.21 vs 09:30 $9,773.00 (-25.79; session marks +23.17) · 12 name(s) marked open→close (per-name table). MRNA×8 09:30 $150.14 → close $133.32 -134.56; CYPH×1062 09:30 $1.15 → close $1.19 +42.48; ABCL×103 09:30 $11.81 → close $11.57 -25.23; AZI×891 09:30 $1.37 → close $1.44 +62.37; AG×29 09:30 $20.55 → close $21.19 +18.56; CDE×29 09:30 $20.65 → close $21.11 +13.34; HDSN×105 09:30 $5.77 → close $5.57 -21.00; IAG×31 09:30 $19.63 → close $20.50 +26.97; KGC×20 09:30 $29.63 → close $31.43 +36.00; NFGC×348 09:30 $1.75 → close $1.75 +0.00; WPM×4 09:30 $144.54 → close $150.25 +22.84; ABUS×124 09:30 $4.92 → close $4.77 -18.60 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.99 | ▲ 09:30 equity $10,071.67 vs yday $9,747.21 (+324.46) | 09:30 open · cash $64.99 (unchanged overnight, no fees) · equity $10,071.67 vs prior close $9,747.21 (+324.46) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 103 | $11.57 | $2.33 | $-29.86 | $1,254.38 | ▼ -29.86 after sell → book $10,069.35; vs 09:30 mark -2.32 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 891 | $1.46 | $11.65 | $+57.04 | $2,543.59 | ▲ +57.04 after sell → book $10,057.70; vs 09:30 mark -11.65 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 29 | $21.90 | $2.10 | $+34.98 | $3,176.59 | ▲ +34.98 after sell → book $10,055.60; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 29 | $21.75 | $2.10 | $+27.73 | $3,805.24 | ▲ +27.73 after sell → book $10,053.50; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 105 | $5.67 | $2.33 | $-15.14 | $4,398.26 | ▼ -15.14 after sell → book $10,051.17; vs 09:30 mark -2.33 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 31 | $21.17 | $2.10 | $+43.55 | $5,052.43 | ▲ +43.55 after sell → book $10,049.07; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 20 | $32.17 | $2.07 | $+46.68 | $5,693.76 | ▲ +46.68 after sell → book $10,047.00; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 348 | $1.79 | $4.56 | $+4.87 | $6,312.12 | ▲ +4.87 after sell → book $10,042.44; vs 09:30 mark -4.56 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 4 | $154.70 | $2.02 | $+36.62 | $6,928.90 | ▲ +36.62 after sell → book $10,040.42; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 124 | $5.20 | $2.39 | $+29.97 | $7,571.30 | ▲ +29.97 after sell → book $10,038.02; vs 09:30 mark -2.40 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 421 | $4.49 | $5.43 | — | $5,675.58 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $1892.83; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 277 | $6.81 | $3.57 | — | $3,785.64 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $1892.83; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 4 | $119.43 | $2.00 | — | $3,305.92 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $540.81; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 31 | $17.20 | $2.08 | — | $2,770.64 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; combo leftover $540.81; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 2 | $216.30 | $2.00 | — | $2,336.04 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; combo leftover $540.81; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 48 | $11.13 | $2.13 | — | $1,799.67 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; combo leftover $540.81; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 325 | $1.66 | $4.19 | — | $1,255.97 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; combo leftover $540.81; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 389 | $1.39 | $5.02 | — | $710.24 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; combo leftover $540.81; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 65 | $8.28 | $2.19 | — | $169.86 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; combo leftover $540.81; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $169.86 | ▲ close $10,077.29 vs 09:30 $10,071.67 (session +67.88) | 16:00 close · cash $169.86 · equity $10,077.29 vs 09:30 $10,071.67 (+5.62; session marks +67.88) · 11 name(s) marked open→close (per-name table). MRNA×8 09:30 $133.11 → close $145.13 +96.16; CYPH×1062 09:30 $1.32 → close $1.42 +106.20; XHG×421 09:30 $4.49 → close $4.41 -33.68; CAPR×277 09:30 $6.81 → close $6.29 -144.04; AU×4 09:30 $119.43 → close $121.22 +7.16; AUPH×31 09:30 $17.20 → close $16.65 -17.05; AEM×2 09:30 $216.30 → close $216.06 -0.48; ARCT×48 09:30 $11.13 → close $13.45 +111.36; BTBT×325 09:30 $1.66 → close $1.53 -42.25; INDP×389 09:30 $1.39 → close $1.29 -38.90; MRVI×65 09:30 $8.28 → close $8.64 +23.40 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.86 | ▲ 09:30 equity $10,912.02 vs yday $10,077.29 (+834.73) | 09:30 open · cash $169.86 (unchanged overnight, no fees) · equity $10,912.02 vs prior close $10,077.29 (+834.73) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 8 | $142.70 | $2.03 | $-63.57 | $1,309.43 | ▼ -63.57 after sell → book $10,909.99; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1062 | $1.83 | $13.89 | $+694.57 | $3,238.99 | ▲ +694.57 after sell → book $10,896.09; vs 09:30 mark -13.90 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 421 | $4.32 | $5.52 | $-82.52 | $5,052.20 | ▼ -82.52 after sell → book $10,890.58; vs 09:30 mark -5.51 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 277 | $8.03 | $3.64 | $+330.73 | $7,272.87 | ▲ +330.73 after sell → book $10,886.94; vs 09:30 mark -3.64 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 4 | $120.51 | $2.02 | $+0.30 | $7,752.89 | ▲ +0.30 after sell → book $10,884.92; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 31 | $16.57 | $2.10 | $-23.72 | $8,264.46 | ▼ -23.72 after sell → book $10,882.82; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 2 | $217.03 | $2.02 | $-2.55 | $8,696.50 | ▼ -2.55 after sell → book $10,880.80; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 48 | $13.33 | $2.15 | $+101.31 | $9,334.19 | ▲ +101.31 after sell → book $10,878.65; vs 09:30 mark -2.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 325 | $1.55 | $4.26 | $-44.20 | $9,833.68 | ▼ -44.20 after sell → book $10,874.39; vs 09:30 mark -4.26 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 389 | $1.24 | $5.09 | $-68.46 | $10,310.95 | ▼ -68.46 after sell → book $10,869.30; vs 09:30 mark -5.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 65 | $8.59 | $2.21 | $+15.76 | $10,867.09 | ▲ +15.76 after sell → book $10,867.09; vs 09:30 mark -2.21 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,867.09 | ▲ close $10,867.09 vs 09:30 $10,912.02 (session +0.00) | 16:00 close · cash $10,867.09 · no lots left · equity $10,867.09. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,867.09 | ▲ 09:30 equity $10,867.09 vs yday $10,867.09 (+0.00) | 09:30 open · cash $10,867.09 (unchanged overnight, no fees) · equity $10,867.09 vs prior close $10,867.09 (+0.00) | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 56 | $24.11 | $2.16 | — | $9,514.77 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $1358.39; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 870 | $1.56 | $11.22 | — | $8,146.35 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $1358.39; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 333 | $4.07 | $4.30 | — | $6,786.75 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $1358.39; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 71 | $19.04 | $2.20 | — | $5,432.70 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $1358.39; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 416 | $1.63 | $5.37 | — | $4,749.26 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; combo leftover $679.09; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 191 | $3.55 | $2.56 | — | $4,068.64 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+27.9; combo leftover $679.09; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 106 | $6.37 | $2.31 | — | $3,391.11 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; combo leftover $679.09; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 19 | $35.05 | $2.05 | — | $2,723.12 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $679.09; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 10 | $64.55 | $2.02 | — | $2,075.60 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+4.4; combo leftover $679.09; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 4 | $156.51 | $2.00 | — | $1,447.56 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+17.4; combo leftover $679.09; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 75 | $8.98 | $2.21 | — | $771.84 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; combo leftover $679.09; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `IAUX` | 357 | $1.90 | $4.61 | — | $88.94 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.4; combo leftover $679.09; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.94 | ▲ close $11,411.38 vs 09:30 $10,867.09 (session +587.29) | 16:00 close · cash $88.94 · equity $11,411.38 vs 09:30 $10,867.09 (+544.29; session marks +587.29) · 12 name(s) marked open→close (per-name table). REAX×56 09:30 $24.11 → close $28.43 +241.92; CYPH×870 09:30 $1.56 → close $1.64 +69.60; XHG×333 09:30 $4.07 → close $4.02 -16.65; ASST×71 09:30 $19.04 → close $21.39 +166.85; BMEA×416 09:30 $1.63 → close $1.73 +41.60; GORO×191 09:30 $3.55 → close $3.87 +61.12; ZURA×106 09:30 $6.37 → close $6.32 -5.30; EZPW×19 09:30 $35.05 → close $35.23 +3.42; ETON×10 09:30 $64.55 → close $63.05 -15.00; WPM×4 09:30 $156.51 → close $163.72 +28.84; SUZ×75 09:30 $8.98 → close $9.03 +3.75; IAUX×357 09:30 $1.90 → close $1.92 +7.14 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.94 | ▼ 09:30 equity $11,113.74 vs yday $11,411.38 (-297.64) | 09:30 open · cash $88.94 (unchanged overnight, no fees) · equity $11,113.74 vs prior close $11,411.38 (-297.64) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 56 | $26.61 | $2.18 | $+135.66 | $1,576.92 | ▲ +135.66 after sell → book $11,111.56; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 870 | $1.60 | $11.38 | $+12.20 | $2,957.54 | ▲ +12.20 after sell → book $11,100.18; vs 09:30 mark -11.38 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 71 | $20.72 | $2.23 | $+114.85 | $4,426.43 | ▲ +114.85 after sell → book $11,097.95; vs 09:30 mark -2.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 416 | $1.75 | $5.45 | $+41.19 | $5,151.06 | ▲ +41.19 after sell → book $11,092.50; vs 09:30 mark -5.45 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 191 | $3.77 | $2.60 | $+36.85 | $5,868.53 | ▲ +36.85 after sell → book $11,089.90; vs 09:30 mark -2.60 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 106 | $6.13 | $2.34 | $-30.08 | $6,515.97 | ▼ -30.08 after sell → book $11,087.56; vs 09:30 mark -2.34 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 19 | $35.70 | $2.07 | $+8.24 | $7,192.21 | ▲ +8.24 after sell → book $11,085.50; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 10 | $63.60 | $2.04 | $-13.56 | $7,826.17 | ▼ -13.56 after sell → book $11,083.46; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 4 | $160.93 | $2.02 | $+13.66 | $8,467.87 | ▲ +13.66 after sell → book $11,081.44; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 75 | $9.03 | $2.24 | $-0.70 | $9,142.88 | ▼ -0.70 after sell → book $11,079.20; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `IAUX` | 357 | $1.87 | $4.67 | $-19.99 | $9,805.79 | ▼ -19.99 after sell → book $11,074.52; vs 09:30 mark -4.68 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 231 | $14.11 | $2.98 | — | $6,543.40 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $3268.60; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 562 | $5.81 | $7.25 | — | $3,270.93 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $3268.60; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SUJA` | 347 | $9.39 | $4.48 | — | $8.13 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+39.0; combo leftover $3268.60; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.13 | ▲ close $11,288.30 vs 09:30 $11,113.74 (session +228.48) | 16:00 close · cash $8.13 · equity $11,288.30 vs 09:30 $11,113.74 (+174.56; session marks +228.48) · 4 name(s) marked open→close (per-name table). XHG×333 09:30 $3.81 → close $4.06 +83.25; BYND×231 09:30 $14.11 → close $14.25 +32.34; USDE×562 09:30 $5.81 → close $5.98 +95.54; SUJA×347 09:30 $9.39 → close $9.44 +17.35 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.13 | ▲ 09:30 equity $11,558.58 vs yday $11,288.30 (+270.28) | 09:30 open · cash $8.13 (unchanged overnight, no fees) · equity $11,558.58 vs prior close $11,288.30 (+270.28) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 333 | $4.06 | $4.36 | $-11.99 | $1,355.75 | ▼ -11.99 after sell → book $11,554.22; vs 09:30 mark -4.36 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 231 | $14.20 | $3.04 | $+14.77 | $4,632.90 | ▲ +14.77 after sell → book $11,551.17; vs 09:30 mark -3.05 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 562 | $6.50 | $7.37 | $+373.16 | $8,278.53 | ▲ +373.16 after sell → book $11,543.80; vs 09:30 mark -7.37 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 347 | $9.41 | $4.56 | $-2.10 | $11,539.24 | ▼ -2.10 after sell → book $11,539.24; vs 09:30 mark -4.56 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1109 | $2.60 | $14.31 | — | $8,641.53 | — | top 4 by hot; rank hot_score; list flatten; ret5=+13.0; combo leftover $2884.81; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 69 | $41.44 | $2.20 | — | $5,779.98 | — | top 4 by hot; rank hot_score; list flatten; ret5=+3.1; combo leftover $2884.81; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 125 | $22.93 | $2.37 | — | $2,911.36 | — | top 4 by hot; rank hot_score; list mover_buy; 🔵; ret5=+9.5; combo leftover $2884.81; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 200 | $14.42 | $2.59 | — | $24.77 | — | top 4 by hot; rank hot_score; list flatten; ret5=+7.1; combo leftover $2884.81; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.77 | ▲ close $11,657.19 vs 09:30 $11,558.58 (session +139.41) | 16:00 close · cash $24.77 · equity $11,657.19 vs 09:30 $11,558.58 (+98.61; session marks +139.41) · 4 name(s) marked open→close (per-name table). SLI×1109 09:30 $2.60 → close $2.64 +44.36; RRC×69 09:30 $41.44 → close $41.64 +13.80; PGY×125 09:30 $22.93 → close $23.26 +41.25; CRK×200 09:30 $14.42 → close $14.62 +40.00 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.77 | ▲ 09:30 equity $11,704.20 vs yday $11,657.19 (+47.01) | 09:30 open · cash $24.77 (unchanged overnight, no fees) · equity $11,704.20 vs prior close $11,657.19 (+47.01) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 1109 | $2.68 | $14.51 | $+59.90 | $2,982.38 | ▲ +59.90 after sell → book $11,689.69; vs 09:30 mark -14.51 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 69 | $41.74 | $2.23 | $+16.27 | $5,860.21 | ▲ +16.27 after sell → book $11,687.46; vs 09:30 mark -2.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 125 | $23.21 | $2.41 | $+30.23 | $8,759.05 | ▲ +30.23 after sell → book $11,685.05; vs 09:30 mark -2.41 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 200 | $14.63 | $2.65 | $+36.76 | $11,682.40 | ▲ +36.76 after sell → book $11,682.40; vs 09:30 mark -2.65 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 104 | $14.00 | $2.30 | — | $10,224.10 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $1460.30; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 150 | $9.73 | $2.44 | — | $8,762.16 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+47.1; combo leftover $1460.30; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 10 | $137.19 | $2.02 | — | $7,388.24 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+7.1; combo leftover $1460.30; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $6,071.59 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $1460.30; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 86 | $23.30 | $2.25 | — | $4,065.54 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+14.5; combo leftover $2023.86; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 106 | $19.00 | $2.31 | — | $2,049.23 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+7.5; combo leftover $2023.86; owner union_join_vol_green_h1 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 81 | $24.69 | $2.23 | — | $47.11 | — | combo gate; gate join=good,vol=good,last_green=True; list earn_react; ret5=+5.8; combo leftover $2023.86; owner union_join_vol_green_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.11 | ▼ close $11,488.05 vs 09:30 $11,704.20 (session -178.78) | 16:00 close · cash $47.11 · equity $11,488.05 vs 09:30 $11,704.20 (-216.15; session marks -178.78) · 7 name(s) marked open→close (per-name table). BYND×104 09:30 $14.00 → close $13.86 -14.56; CAPR×150 09:30 $9.73 → close $9.59 -21.00; MRNA×10 09:30 $137.19 → close $137.99 +8.00; ANF×9 09:30 $146.07 → close $148.42 +21.15; NCNO×86 09:30 $23.30 → close $22.99 -26.66; TH×106 09:30 $19.00 → close $18.55 -47.70; GAP×81 09:30 $24.69 → close $23.48 -98.01 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.11 | ▼ 09:30 equity $11,313.01 vs yday $11,488.05 (-175.04) | 09:30 open · cash $47.11 (unchanged overnight, no fees) · equity $11,313.01 vs prior close $11,488.05 (-175.04) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 150 | $9.50 | $2.48 | $-39.42 | $1,469.64 | ▼ -39.42 after sell → book $11,310.54; vs 09:30 mark -2.47 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 10 | $134.10 | $2.04 | $-34.96 | $2,808.59 | ▼ -34.96 after sell → book $11,308.49; vs 09:30 mark -2.05 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $4,138.83 | ▲ +13.59 after sell → book $11,306.46; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 86 | $22.66 | $2.28 | $-59.57 | $6,085.31 | ▼ -59.57 after sell → book $11,304.18; vs 09:30 mark -2.28 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 106 | $18.12 | $2.34 | $-97.40 | $8,004.22 | ▼ -97.40 after sell → book $11,301.84; vs 09:30 mark -2.34 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 81 | $22.98 | $2.26 | $-143.00 | $9,863.34 | ▼ -143.00 after sell → book $11,299.58; vs 09:30 mark -2.26 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,863.34 | ▼ close $11,246.54 vs 09:30 $11,313.01 (session -53.04) | 16:00 close · cash $9,863.34 · equity $11,246.54 vs 09:30 $11,313.01 (-66.47; session marks -53.04) · 1 name(s) marked open→close (per-name table). BYND×104 09:30 $13.81 → close $13.30 -53.04 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,863.34 | ▼ 09:30 equity $11,219.50 vs yday $11,246.54 (-27.04) | 09:30 open · cash $9,863.34 (unchanged overnight, no fees) · equity $11,219.50 vs prior close $11,246.54 (-27.04) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 104 | $13.04 | $2.33 | $-104.47 | $11,217.17 | ▼ -104.47 after sell → book $11,217.17; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,217.17 | ▲ close $11,217.17 vs 09:30 $11,219.50 (session +0.00) | 16:00 close · cash $11,217.17 · no lots left · equity $11,217.17. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,217.17 | ▲ 09:30 equity $11,217.17 vs yday $11,217.17 (-0.00) | 09:30 open · cash $11,217.17 (unchanged overnight, no fees) · equity $11,217.17 vs prior close $11,217.17 (-0.00) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,217.17 | ▲ close $11,217.17 vs 09:30 $11,217.17 (session +0.00) | 16:00 close · cash $11,217.17 · no lots left · equity $11,217.17. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,217.17 | ▲ 09:30 equity $11,217.17 vs yday $11,217.17 (-0.00) | 09:30 open · cash $11,217.17 (unchanged overnight, no fees) · equity $11,217.17 vs prior close $11,217.17 (-0.00) | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 787 | $1.78 | $10.15 | — | $9,806.15 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $1402.15; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 76 | $18.40 | $2.22 | — | $8,405.54 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $1402.15; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 102 | $13.71 | $2.30 | — | $7,004.82 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $1402.15; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 58 | $23.88 | $2.16 | — | $5,617.62 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $1402.15; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 6 | $132.45 | $2.01 | — | $4,820.91 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; combo leftover $802.52; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 47 | $16.77 | $2.13 | — | $4,030.59 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; combo leftover $802.52; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 368 | $2.18 | $4.75 | — | $3,223.60 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; combo leftover $802.52; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 77 | $10.42 | $2.22 | — | $2,419.04 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; combo leftover $802.52; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 415 | $1.93 | $5.35 | — | $1,612.74 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $802.52; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 4 | $161.54 | $2.00 | — | $964.57 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+12.0; combo leftover $802.52; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 77 | $10.38 | $2.22 | — | $163.48 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-56.2; combo leftover $802.52; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $163.48 | ▼ close $10,859.02 vs 09:30 $11,217.17 (session -320.64) | 16:00 close · cash $163.48 · equity $10,859.02 vs 09:30 $11,217.17 (-358.15; session marks -320.64) · 11 name(s) marked open→close (per-name table). GPRO×787 09:30 $1.78 → close $1.39 -306.93; REAX×76 09:30 $18.40 → close $18.40 +0.00; CNH×102 09:30 $13.71 → close $13.84 +13.26; MMED×58 09:30 $23.88 → close $23.84 -2.32; RVTY×6 09:30 $132.45 → close $130.63 -10.92; ARCT×47 09:30 $16.77 → close $15.56 -56.87; CRDL×368 09:30 $2.18 → close $2.16 -7.36; NVAX×77 09:30 $10.42 → close $10.34 -6.16; BMEA×415 09:30 $1.93 → close $1.91 -8.30; DUOL×4 09:30 $161.54 → close $158.82 -10.88; ALMS×77 09:30 $10.38 → close $11.36 +75.84 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $163.48 | ▲ 09:30 equity $10,907.42 vs yday $10,859.02 (+48.40) | 09:30 open · cash $163.48 (unchanged overnight, no fees) · equity $10,907.42 vs prior close $10,859.02 (+48.40) | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 76 | $18.15 | $2.24 | $-23.46 | $1,540.64 | ▼ -23.46 after sell → book $10,905.18; vs 09:30 mark -2.24 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 102 | $13.89 | $2.32 | $+13.74 | $2,955.09 | ▲ +13.74 after sell → book $10,902.85; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 58 | $23.84 | $2.19 | $-6.67 | $4,335.63 | ▼ -6.67 after sell → book $10,900.67; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 6 | $130.03 | $2.03 | $-18.56 | $5,113.78 | ▼ -18.56 after sell → book $10,898.64; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 47 | $15.61 | $2.15 | $-58.80 | $5,845.30 | ▼ -58.80 after sell → book $10,896.49; vs 09:30 mark -2.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 368 | $2.16 | $4.82 | $-16.93 | $6,635.36 | ▼ -16.93 after sell → book $10,891.67; vs 09:30 mark -4.82 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 77 | $10.50 | $2.24 | $+1.70 | $7,441.62 | ▲ +1.70 after sell → book $10,889.43; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 415 | $1.90 | $5.43 | $-23.24 | $8,224.68 | ▼ -23.24 after sell → book $10,883.99; vs 09:30 mark -5.44 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 4 | $157.46 | $2.02 | $-20.34 | $8,852.50 | ▼ -20.34 after sell → book $10,881.97; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 77 | $11.23 | $2.24 | $+61.37 | $9,714.97 | ▲ +61.37 after sell → book $10,879.73; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 64 | $25.18 | $2.18 | — | $8,101.27 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $1619.16; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 205 | $7.87 | $2.64 | — | $6,485.27 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $1619.16; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 279 | $5.79 | $3.60 | — | $4,866.26 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $1619.16; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 1 | $513.78 | $1.99 | — | $4,350.49 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; combo leftover $811.04; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 9 | $82.70 | $2.02 | — | $3,604.17 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; combo leftover $811.04; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 323 | $2.51 | $4.17 | — | $2,789.28 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; combo leftover $811.04; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 2 | $378.34 | $2.00 | — | $2,030.60 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-12.7; combo leftover $811.04; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 21 | $37.44 | $2.05 | — | $1,242.31 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.1; combo leftover $811.04; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `AHCO` | 128 | $6.32 | $2.37 | — | $430.97 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+10.7; combo leftover $811.04; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $430.97 | ▲ close $11,332.18 vs 09:30 $10,907.42 (session +475.48) | 16:00 close · cash $430.97 · equity $11,332.18 vs 09:30 $10,907.42 (+424.76; session marks +475.48) · 10 name(s) marked open→close (per-name table). GPRO×787 09:30 $1.48 → close $1.70 +173.14; ASST×64 09:30 $25.18 → close $27.14 +125.44; USDE×205 09:30 $7.87 → close $7.93 +12.30; DFDV×279 09:30 $5.79 → close $5.87 +22.32; DELL×1 09:30 $513.78 → close $524.14 +10.36; TARS×9 09:30 $82.70 → close $90.78 +72.72; BRR×323 09:30 $2.51 → close $2.66 +48.45; MDB×2 09:30 $378.34 → close $368.74 -19.20; TDS×21 09:30 $37.44 → close $37.83 +8.19; AHCO×128 09:30 $6.32 → close $6.49 +21.76 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $430.97 | ▼ 09:30 equity $11,097.63 vs yday $11,332.18 (-234.55) | 09:30 open · cash $430.97 (unchanged overnight, no fees) · equity $11,097.63 vs prior close $11,332.18 (-234.55) | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 787 | $1.56 | $10.29 | $-189.65 | $1,652.33 | ▼ -189.65 after sell → book $11,087.33; vs 09:30 mark -10.30 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 64 | $26.44 | $2.21 | $+76.25 | $3,342.29 | ▲ +76.25 after sell → book $11,085.13; vs 09:30 mark -2.20 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 205 | $7.76 | $2.69 | $-27.89 | $4,930.40 | ▼ -27.89 after sell → book $11,082.44; vs 09:30 mark -2.69 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 279 | $5.81 | $3.66 | $-1.68 | $6,547.73 | ▼ -1.68 after sell → book $11,078.78; vs 09:30 mark -3.66 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 1 | $521.15 | $2.01 | $+3.36 | $7,066.87 | ▲ +3.36 after sell → book $11,076.77; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 9 | $89.67 | $2.04 | $+58.68 | $7,871.86 | ▲ +58.68 after sell → book $11,074.73; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 323 | $2.66 | $4.23 | $+40.05 | $8,726.81 | ▲ +40.05 after sell → book $11,070.50; vs 09:30 mark -4.23 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MDB` | 2 | $360.75 | $2.02 | $-39.19 | $9,446.29 | ▼ -39.19 after sell → book $11,068.48; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 21 | $37.75 | $2.07 | $+2.38 | $10,236.97 | ▲ +2.38 after sell → book $11,066.41; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AHCO` | 128 | $6.48 | $2.41 | $+15.70 | $11,064.00 | ▲ +15.70 after sell → book $11,064.00; vs 09:30 mark -2.41 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,064.00 | ▲ close $11,064.00 vs 09:30 $11,097.63 (session +0.00) | 16:00 close · cash $11,064.00 · no lots left · equity $11,064.00. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,064.00 | ▲ 09:30 equity $11,064.00 vs yday $11,064.00 (+0.00) | 09:30 open · cash $11,064.00 (unchanged overnight, no fees) · equity $11,064.00 vs prior close $11,064.00 (+0.00) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,064.00 | ▲ close $11,064.00 vs 09:30 $11,064.00 (session +0.00) | 16:00 close · cash $11,064.00 · no lots left · equity $11,064.00. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new union_hot_n4_h1 |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new union_hot_n4_h1 |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new union_hot_n4_h1 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new union_hot_n4_h1 |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new union_hot_n4_h1 |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new union_hot_n4_h1 |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new union_join_vol_green_h1 |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new union_join_vol_green_h1 |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new union_join_vol_green_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new union_join_vol_green_h1 |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new union_join_vol_green_h1 |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new union_join_vol_green_h1 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new union_join_vol_green_h1 |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new union_join_vol_green_h1 |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new union_hot_n4_h1 |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new union_hot_n4_h1 |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new union_hot_n4_h1 |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new union_join_vol_green_h1 |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new union_join_vol_green_h1 |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new union_hot_n4_h1 |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new union_hot_n4_h1 |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new union_hot_n4_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new union_hot_n4_h1 |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new union_hot_n4_h1 |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new union_hot_n4_h1 |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new union_hot_n4_h1 |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new union_join_vol_green_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new union_join_vol_green_h1 |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new union_join_vol_green_h1 |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new union_join_vol_green_h1 |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new union_join_vol_green_h1 |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new union_join_vol_green_h1 |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |
