# Factor mine action — `combo_oh_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared overnight_mega_h1/union_hot_n4_holdup w=0.5,0.5 net=priority

Cash book **+42.70%** ($14,270) · signal-only (no cash/fees) was —. Starts YES **29/30**. Fills 135 · skips 62 · realized $+1377.50.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: overnight_mega_h1 50%, union_hot_n4_holdup 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: overnight_mega_h1 50%, union_hot_n4_holdup 50%.
- Member: overnight_mega_h1 (50% · long · hold 1).
- Member: union_hot_n4_holdup (50% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,401.45.

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
| 2026-08-14 | `QMCO` | 105 | — | $24.68 | +0.00 | $26.11 | +150.15 | +150.15 | +0.00 | +150.15 |
| 2026-08-14 | `ARX` | 132 | — | $19.57 | +0.00 | $19.58 | +1.32 | +1.32 | +0.00 | +1.32 |
| 2026-08-14 | `ZENA` | 1178 | — | $2.20 | +0.00 | $2.14 | -70.68 | -70.68 | +0.00 | -70.68 |
| 2026-08-14 | `AIRO` | 231 | — | $11.12 | +0.00 | $9.57 | -358.05 | -358.05 | +0.00 | -358.05 |
| 2026-08-17 | `QMCO` | 105 | $26.11 | $24.83 | -134.40 | — | +0.00 | -134.40 | +15.75 | — |
| 2026-08-17 | `ARX` | 132 | $19.58 | $19.57 | -1.32 | — | +0.00 | -1.32 | +0.00 | — |
| 2026-08-17 | `ZENA` | 1178 | $2.14 | $2.08 | -64.79 | — | +0.00 | -64.79 | -135.47 | — |
| 2026-08-17 | `AIRO` | 231 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -358.05 | — |
| 2026-08-17 | `XHG` | 587 | — | $4.19 | +0.00 | $3.91 | -164.36 | -164.36 | +0.00 | -164.36 |
| 2026-08-17 | `CAPR` | 358 | — | $6.87 | +0.00 | $7.45 | +207.64 | +207.64 | +0.00 | +207.64 |
| 2026-08-17 | `STDN` | 180 | — | $13.64 | +0.00 | $13.31 | -59.40 | -59.40 | +0.00 | -59.40 |
| 2026-08-17 | `HTFL` | 59 | — | $41.23 | +0.00 | $41.94 | +41.89 | +41.89 | +0.00 | +41.89 |
| 2026-08-18 | `XHG` | 587 | $3.91 | $3.94 | +17.61 | — | +0.00 | +17.61 | -146.75 | — |
| 2026-08-18 | `CAPR` | 358 | $7.45 | $7.50 | +17.90 | $7.08 | -150.36 | -132.46 | +225.54 | +75.18 |
| 2026-08-18 | `STDN` | 180 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -59.40 | — |
| 2026-08-18 | `HTFL` | 59 | $41.94 | $41.50 | -25.96 | — | +0.00 | -25.96 | +15.93 | — |
| 2026-08-19 | `CAPR` | 358 | $7.08 | $7.19 | +39.38 | — | +0.00 | +39.38 | +114.56 | — |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `CYPH` | 1057 | — | $1.15 | +0.00 | $1.19 | +42.28 | +42.28 | +0.00 | +42.28 |
| 2026-08-20 | `ABCL` | 102 | — | $11.81 | +0.00 | $11.57 | -24.99 | -24.99 | +0.00 | -24.99 |
| 2026-08-20 | `AZI` | 888 | — | $1.37 | +0.00 | $1.44 | +62.16 | +62.16 | +0.00 | +62.16 |
| 2026-08-20 | `ROST` | 21 | — | $229.55 | +0.00 | $228.99 | -11.76 | -11.76 | +0.00 | -11.76 |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | $145.13 | +96.16 | +94.48 | -136.24 | -40.08 |
| 2026-08-21 | `CYPH` | 1057 | $1.19 | $1.32 | +137.41 | $1.42 | +105.70 | +243.11 | +179.69 | +285.39 |
| 2026-08-21 | `ABCL` | 102 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -24.99 | — |
| 2026-08-21 | `AZI` | 888 | $1.44 | $1.46 | +17.76 | — | +0.00 | +17.76 | +79.92 | — |
| 2026-08-21 | `ROST` | 21 | $228.99 | $243.85 | +312.06 | — | +0.00 | +312.06 | +300.30 | — |
| 2026-08-21 | `XHG` | 424 | — | $4.49 | +0.00 | $4.41 | -33.92 | -33.92 | +0.00 | -33.92 |
| 2026-08-21 | `CAPR` | 279 | — | $6.81 | +0.00 | $6.29 | -145.08 | -145.08 | +0.00 | -145.08 |
| 2026-08-21 | `PDD` | 42 | — | $90.03 | +0.00 | $88.38 | -69.30 | -69.30 | +0.00 | -69.30 |
| 2026-08-24 | `MRNA` | 8 | $145.13 | $142.70 | -19.44 | — | +0.00 | -19.44 | -59.52 | — |
| 2026-08-24 | `CYPH` | 1057 | $1.42 | $1.83 | +433.37 | — | +0.00 | +433.37 | +718.76 | — |
| 2026-08-24 | `XHG` | 424 | $4.41 | $4.32 | -38.16 | — | +0.00 | -38.16 | -72.08 | — |
| 2026-08-24 | `CAPR` | 279 | $6.29 | $8.03 | +485.46 | — | +0.00 | +485.46 | +340.38 | — |
| 2026-08-24 | `PDD` | 42 | $88.38 | $90.95 | +107.94 | — | +0.00 | +107.94 | +38.64 | — |
| 2026-08-25 | `REAX` | 56 | — | $24.11 | +0.00 | $28.43 | +241.92 | +241.92 | +0.00 | +241.92 |
| 2026-08-25 | `CYPH` | 878 | — | $1.56 | +0.00 | $1.64 | +70.24 | +70.24 | +0.00 | +70.24 |
| 2026-08-25 | `XHG` | 336 | — | $4.07 | +0.00 | $4.02 | -16.80 | -16.80 | +0.00 | -16.80 |
| 2026-08-25 | `ASST` | 72 | — | $19.04 | +0.00 | $21.39 | +169.20 | +169.20 | +0.00 | +169.20 |
| 2026-08-25 | `INTU` | 15 | — | $364.35 | +0.00 | $357.46 | -103.35 | -103.35 | +0.00 | -103.35 |
| 2026-08-26 | `REAX` | 56 | $28.43 | $26.61 | -101.92 | — | +0.00 | -101.92 | +140.00 | — |
| 2026-08-26 | `CYPH` | 878 | $1.64 | $1.60 | -35.12 | — | +0.00 | -35.12 | +35.12 | — |
| 2026-08-26 | `XHG` | 336 | $4.02 | $3.81 | -70.56 | $4.06 | +84.00 | +13.44 | -87.36 | -3.36 |
| 2026-08-26 | `ASST` | 72 | $21.39 | $20.72 | -48.24 | — | +0.00 | -48.24 | +120.96 | — |
| 2026-08-26 | `INTU` | 15 | $357.46 | $323.47 | -509.85 | — | +0.00 | -509.85 | -613.20 | — |
| 2026-08-26 | `BYND` | 109 | — | $14.11 | +0.00 | $14.25 | +15.26 | +15.26 | +0.00 | +15.26 |
| 2026-08-26 | `USDE` | 265 | — | $5.81 | +0.00 | $5.98 | +45.05 | +45.05 | +0.00 | +45.05 |
| 2026-08-26 | `PURR` | 132 | — | $11.59 | +0.00 | $11.56 | -3.30 | -3.30 | +0.00 | -3.30 |
| 2026-08-26 | `CM` | 5 | — | $118.50 | +0.00 | $118.20 | -1.50 | -1.50 | +0.00 | -1.50 |
| 2026-08-26 | `CRM` | 3 | — | $199.94 | +0.00 | $205.62 | +17.04 | +17.04 | +0.00 | +17.04 |
| 2026-08-26 | `CRWD` | 3 | — | $182.75 | +0.00 | $189.18 | +19.29 | +19.29 | +0.00 | +19.29 |
| 2026-08-26 | `NVDA` | 3 | — | $212.64 | +0.00 | $209.66 | -8.94 | -8.94 | +0.00 | -8.94 |
| 2026-08-26 | `RY` | 3 | — | $206.95 | +0.00 | $207.21 | +0.78 | +0.78 | +0.00 | +0.78 |
| 2026-08-26 | `SNPS` | 1 | — | $405.10 | +0.00 | $410.00 | +4.90 | +4.90 | +0.00 | +4.90 |
| 2026-08-26 | `TD` | 5 | — | $119.11 | +0.00 | $119.43 | +1.60 | +1.60 | +0.00 | +1.60 |
| 2026-08-27 | `XHG` | 336 | $4.06 | $4.06 | +0.00 | $3.80 | -87.36 | -87.36 | -3.36 | -90.72 |
| 2026-08-27 | `BYND` | 109 | $14.25 | $14.20 | -5.45 | — | +0.00 | -5.45 | +9.81 | — |
| 2026-08-27 | `USDE` | 265 | $5.98 | $6.50 | +137.80 | — | +0.00 | +137.80 | +182.85 | — |
| 2026-08-27 | `PURR` | 132 | $11.56 | $12.18 | +81.84 | — | +0.00 | +81.84 | +78.54 | — |
| 2026-08-27 | `CM` | 5 | $118.20 | $118.77 | +2.85 | — | +0.00 | +2.85 | +1.35 | — |
| 2026-08-27 | `CRM` | 3 | $205.62 | $230.05 | +73.29 | — | +0.00 | +73.29 | +90.33 | — |
| 2026-08-27 | `CRWD` | 3 | $189.18 | $208.25 | +57.21 | — | +0.00 | +57.21 | +76.50 | — |
| 2026-08-27 | `NVDA` | 3 | $209.66 | $222.86 | +39.60 | — | +0.00 | +39.60 | +30.66 | — |
| 2026-08-27 | `RY` | 3 | $207.21 | $206.82 | -1.17 | — | +0.00 | -1.17 | -0.39 | — |
| 2026-08-27 | `SNPS` | 1 | $410.00 | $419.66 | +9.66 | — | +0.00 | +9.66 | +14.56 | — |
| 2026-08-27 | `TD` | 5 | $119.43 | $120.17 | +3.70 | — | +0.00 | +3.70 | +5.30 | — |
| 2026-08-27 | `CAPR` | 175 | — | $9.19 | +0.00 | $10.06 | +152.25 | +152.25 | +0.00 | +152.25 |
| 2026-08-27 | `MRNA` | 11 | — | $144.18 | +0.00 | $142.77 | -15.51 | -15.51 | +0.00 | -15.51 |
| 2026-08-27 | `BZ` | 87 | — | $18.50 | +0.00 | $18.00 | -43.50 | -43.50 | +0.00 | -43.50 |
| 2026-08-27 | `ADSK` | 9 | — | $261.47 | +0.00 | $270.58 | +81.99 | +81.99 | +0.00 | +81.99 |
| 2026-08-27 | `MRVL` | 9 | — | $253.44 | +0.00 | $241.45 | -107.91 | -107.91 | +0.00 | -107.91 |
| 2026-08-28 | `XHG` | 336 | $3.80 | $3.69 | -36.96 | — | +0.00 | -36.96 | -127.68 | — |
| 2026-08-28 | `CAPR` | 175 | $10.06 | $9.73 | -57.75 | $9.59 | -24.50 | -82.25 | +94.50 | +70.00 |
| 2026-08-28 | `MRNA` | 11 | $142.77 | $137.19 | -61.38 | $137.99 | +8.80 | -52.58 | -76.89 | -68.09 |
| 2026-08-28 | `BZ` | 87 | $18.00 | $18.15 | +13.05 | — | +0.00 | +13.05 | -30.45 | — |
| 2026-08-28 | `ADSK` | 9 | $270.58 | $261.16 | -84.78 | — | +0.00 | -84.78 | -2.79 | — |
| 2026-08-28 | `MRVL` | 9 | $241.45 | $225.26 | -145.71 | — | +0.00 | -145.71 | -253.62 | — |
| 2026-08-28 | `BYND` | 265 | — | $14.00 | +0.00 | $13.86 | -37.10 | -37.10 | +0.00 | -37.10 |
| 2026-08-28 | `ANF` | 25 | — | $146.07 | +0.00 | $148.42 | +58.75 | +58.75 | +0.00 | +58.75 |
| 2026-08-31 | `CAPR` | 175 | $9.59 | $9.50 | -15.75 | — | +0.00 | -15.75 | +54.25 | — |
| 2026-08-31 | `MRNA` | 11 | $137.99 | $134.10 | -42.79 | — | +0.00 | -42.79 | -110.88 | — |
| 2026-08-31 | `BYND` | 265 | $13.86 | $13.81 | -13.25 | $13.30 | -135.15 | -148.40 | -50.35 | -185.50 |
| 2026-08-31 | `ANF` | 25 | $148.42 | $148.03 | -9.75 | — | +0.00 | -9.75 | +49.00 | — |
| 2026-09-01 | `BYND` | 265 | $13.30 | $13.04 | -68.90 | — | +0.00 | -68.90 | -254.40 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GPRO` | 1452 | — | $1.78 | +0.00 | $1.39 | -566.28 | -566.28 | +0.00 | -566.28 |
| 2026-09-03 | `REAX` | 140 | — | $18.40 | +0.00 | $18.40 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `CNH` | 188 | — | $13.71 | +0.00 | $13.84 | +24.44 | +24.44 | +0.00 | +24.44 |
| 2026-09-03 | `MMED` | 107 | — | $23.88 | +0.00 | $23.84 | -4.28 | -4.28 | +0.00 | -4.28 |
| 2026-09-04 | `GPRO` | 1452 | $1.39 | $1.48 | +130.68 | $1.70 | +319.44 | +450.12 | -435.60 | -116.16 |
| 2026-09-04 | `REAX` | 140 | $18.40 | $18.15 | -35.00 | — | +0.00 | -35.00 | -35.00 | — |
| 2026-09-04 | `CNH` | 188 | $13.84 | $13.89 | +9.40 | — | +0.00 | +9.40 | +33.84 | — |
| 2026-09-04 | `MMED` | 107 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -4.28 | — |
| 2026-09-04 | `ASST` | 102 | — | $25.18 | +0.00 | $27.14 | +199.92 | +199.92 | +0.00 | +199.92 |
| 2026-09-04 | `USDE` | 326 | — | $7.87 | +0.00 | $7.93 | +19.56 | +19.56 | +0.00 | +19.56 |
| 2026-09-04 | `DFDV` | 444 | — | $5.79 | +0.00 | $5.87 | +35.52 | +35.52 | +0.00 | +35.52 |
| 2026-09-08 | `GPRO` | 1452 | $1.70 | $1.56 | -196.02 | — | +0.00 | -196.02 | -312.18 | — |
| 2026-09-08 | `ASST` | 102 | $27.14 | $26.44 | -71.40 | — | +0.00 | -71.40 | +128.52 | — |
| 2026-09-08 | `USDE` | 326 | $7.93 | $7.76 | -55.42 | — | +0.00 | -55.42 | -35.86 | — |
| 2026-09-08 | `DFDV` | 444 | $5.87 | $5.81 | -26.64 | — | +0.00 | -26.64 | +8.88 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `INDP` | 930 | — | $2.70 | +0.00 | $2.77 | +65.10 | +65.10 | +0.00 | +65.10 |
| 2026-09-11 | `BNC` | 511 | — | $4.91 | +0.00 | $4.80 | -56.21 | -56.21 | +0.00 | -56.21 |
| 2026-09-11 | `IRD` | 407 | — | $6.16 | +0.00 | $6.04 | -48.84 | -48.84 | +0.00 | -48.84 |
| 2026-09-11 | `CMRC` | 795 | — | $3.13 | +0.00 | $3.50 | +298.12 | +298.12 | +0.00 | +298.12 |
| 2026-09-14 | `INDP` | 930 | $2.77 | $2.80 | +27.90 | $3.14 | +316.20 | +344.10 | +93.00 | +409.20 |
| 2026-09-14 | `BNC` | 511 | $4.80 | $5.03 | +117.53 | $5.27 | +122.64 | +240.17 | +61.32 | +183.96 |
| 2026-09-14 | `IRD` | 407 | $6.04 | $6.02 | -8.14 | — | +0.00 | -8.14 | -56.98 | — |
| 2026-09-14 | `CMRC` | 795 | $3.50 | $3.51 | +3.97 | $3.64 | +103.35 | +107.32 | +302.10 | +405.45 |
| 2026-09-15 | `INDP` | 930 | $3.14 | $3.40 | +241.80 | $3.64 | +223.20 | +465.00 | +651.00 | +874.20 |
| 2026-09-15 | `BNC` | 511 | $5.27 | $5.11 | -81.76 | — | +0.00 | -81.76 | +102.20 | — |
| 2026-09-15 | `CMRC` | 795 | $3.64 | $3.64 | +0.00 | — | +0.00 | +0.00 | +405.45 | — |
| 2026-09-16 | `INDP` | 930 | $3.64 | $3.66 | +18.60 | $3.21 | -418.50 | -399.90 | +892.80 | +474.30 |
| 2026-09-16 | `HLP` | 1469 | — | $1.80 | +0.00 | $2.07 | +396.63 | +396.63 | +0.00 | +396.63 |
| 2026-09-16 | `SDGR` | 113 | — | $23.29 | +0.00 | $23.93 | +72.32 | +72.32 | +0.00 | +72.32 |
| 2026-09-16 | `SSL` | 180 | — | $14.62 | +0.00 | $14.29 | -59.40 | -59.40 | +0.00 | -59.40 |
| 2026-09-17 | `INDP` | 930 | $3.21 | $3.30 | +83.70 | $3.93 | +585.90 | +669.60 | +558.00 | +1143.90 |
| 2026-09-17 | `HLP` | 1469 | $2.07 | $2.10 | +44.07 | $2.02 | -117.52 | -73.45 | +440.70 | +323.18 |
| 2026-09-17 | `SDGR` | 113 | $23.93 | $24.09 | +18.08 | — | +0.00 | +18.08 | +90.40 | — |
| 2026-09-17 | `SSL` | 180 | $14.29 | $13.77 | -93.60 | — | +0.00 | -93.60 | -153.00 | — |
| 2026-09-17 | `BBNX` | 115 | — | $22.46 | +0.00 | $21.43 | -118.45 | -118.45 | +0.00 | -118.45 |
| 2026-09-17 | `FPS` | 70 | — | $36.76 | +0.00 | $38.06 | +91.00 | +91.00 | +0.00 | +91.00 |
| 2026-09-18 | `INDP` | 930 | $3.93 | $3.85 | -74.40 | $3.55 | -279.00 | -353.40 | +1069.50 | +790.50 |
| 2026-09-18 | `HLP` | 1469 | $2.02 | $1.96 | -88.14 | — | +0.00 | -88.14 | +235.04 | — |
| 2026-09-18 | `BBNX` | 115 | $21.43 | $21.30 | -14.95 | — | +0.00 | -14.95 | -133.40 | — |
| 2026-09-18 | `FPS` | 70 | $38.06 | $39.50 | +100.80 | — | +0.00 | +100.80 | +191.80 | — |
| 2026-09-18 | `SDGR` | 92 | — | $29.32 | +0.00 | $29.02 | -27.60 | -27.60 | +0.00 | -27.60 |
| 2026-09-18 | `CYPH` | 890 | — | $3.04 | +0.00 | $3.60 | +502.85 | +502.85 | +0.00 | +502.85 |
| 2026-09-18 | `TEM` | 33 | — | $81.40 | +0.00 | $77.84 | -117.48 | -117.48 | +0.00 | -117.48 |
| 2026-09-21 | `INDP` | 930 | $3.55 | $3.55 | +0.00 | — | +0.00 | +0.00 | +790.50 | — |
| 2026-09-21 | `SDGR` | 92 | $29.02 | $29.43 | +37.72 | — | +0.00 | +37.72 | +10.12 | — |
| 2026-09-21 | `CYPH` | 890 | $3.60 | $4.00 | +356.00 | — | +0.00 | +356.00 | +858.85 | — |
| 2026-09-21 | `TEM` | 33 | $77.84 | $79.08 | +40.92 | — | +0.00 | +40.92 | -76.56 | — |
| 2026-09-21 | `FEAM` | 1230 | — | $2.47 | +0.00 | $2.48 | +12.30 | +12.30 | +0.00 | +12.30 |
| 2026-09-21 | `TJGC` | 179 | — | $16.91 | +0.00 | $17.58 | +119.93 | +119.93 | +0.00 | +119.93 |
| 2026-09-21 | `LVWR` | 1841 | — | $1.65 | +0.00 | $1.53 | -220.92 | -220.92 | +0.00 | -220.92 |
| 2026-09-21 | `SECZ` | 257 | — | $11.67 | +0.00 | $13.50 | +470.31 | +470.31 | +0.00 | +470.31 |
| 2026-09-22 | `FEAM` | 1230 | $2.48 | $2.48 | +0.00 | $2.48 | +0.00 | +0.00 | +12.30 | +12.30 |
| 2026-09-22 | `TJGC` | 179 | $17.58 | $17.58 | +0.00 | $17.58 | +0.00 | +0.00 | +119.93 | +119.93 |
| 2026-09-22 | `LVWR` | 1841 | $1.53 | $1.53 | +0.00 | $1.53 | +0.00 | +0.00 | -220.92 | -220.92 |
| 2026-09-22 | `SECZ` | 257 | $13.50 | $12.96 | -138.78 | $13.00 | +10.28 | -128.50 | +331.53 | +341.81 |
| 2026-09-23 | `FEAM` | 1230 | $2.48 | $2.92 | +541.20 | $2.64 | -344.40 | +196.80 | +553.50 | +209.10 |
| 2026-09-23 | `TJGC` | 179 | $17.58 | $16.92 | -118.14 | — | +0.00 | -118.14 | +1.79 | — |
| 2026-09-23 | `LVWR` | 1841 | $1.53 | $1.41 | -220.92 | — | +0.00 | -220.92 | -441.84 | — |
| 2026-09-23 | `SECZ` | 257 | $13.00 | $12.80 | -51.40 | — | +0.00 | -51.40 | +290.41 | — |
| 2026-09-23 | `GLND` | 1097 | — | $2.70 | +0.00 | $2.91 | +230.37 | +230.37 | +0.00 | +230.37 |
| 2026-09-23 | `VKTX` | 70 | — | $41.76 | +0.00 | $41.65 | -7.70 | -7.70 | +0.00 | -7.70 |
| 2026-09-23 | `SVIA` | 660 | — | $4.49 | +0.00 | $4.03 | -303.60 | -303.60 | +0.00 | -303.60 |
| 2026-09-24 | `FEAM` | 1230 | $2.64 | $2.68 | +49.20 | — | +0.00 | +49.20 | +258.30 | — |
| 2026-09-24 | `GLND` | 1097 | $2.91 | $3.22 | +337.88 | $5.35 | +2338.80 | +2676.68 | +568.25 | +2907.05 |
| 2026-09-24 | `VKTX` | 70 | $41.65 | $36.02 | -393.75 | — | +0.00 | -393.75 | -401.45 | — |
| 2026-09-24 | `SVIA` | 660 | $4.03 | $3.92 | -69.30 | — | +0.00 | -69.30 | -372.90 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +386.21 | IREN, TNDM, TPG, INO | — | $0.54 | $10,345.37 | IREN×54, TNDM×107, TPG×49, INO×3085 |
| 2026-08-14 | +5.50 | $0.54 | IREN×54, TNDM×107, TPG×49, INO×3085 | $10,412.10 | +66.73 | -277.26 | QMCO, ARX, ZENA, AIRO | IREN, TNDM, TPG, INO | $9.09 | $10,066.79 | QMCO×105, ARX×132, ZENA×1178, AIRO×231 |
| 2026-08-17 | +2.25 | $9.09 | QMCO×105, ARX×132, ZENA×1178, AIRO×231 | $9,866.28 | -200.51 | +25.77 | XHG, CAPR, STDN, HTFL | QMCO, ARX, ZENA, AIRO | $19.42 | $9,851.95 | XHG×587, CAPR×358, STDN×180, HTFL×59 |
| 2026-08-18 | -6.20 | $19.42 | XHG×587, CAPR×358, STDN×180, HTFL×59 | $9,861.50 | +9.55 | -150.36 | — | XHG, STDN, HTFL | $7,164.03 | $9,698.67 | CAPR×358 |
| 2026-08-19 | -7.20 | $7,164.03 | CAPR×358 | $9,738.05 | +39.38 | +0.00 | — | CAPR | $9,733.36 | $9,733.36 | — |
| 2026-08-20 | +1.12 | $9,733.36 | — | $9,733.36 | -0.00 | -66.87 | MRNA, CYPH, ABCL, AZI, ROST | — | $42.99 | $9,635.03 | MRNA×8, CYPH×1057, ABCL×102, AZI×888, ROST×21 |
| 2026-08-21 | +3.25 | $42.99 | MRNA×8, CYPH×1057, ABCL×102, AZI×888, ROST×21 | $10,100.58 | +465.55 | -46.44 | XHG, CAPR, PDD | ABCL, AZI, ROST | $28.23 | $10,026.92 | MRNA×8, CYPH×1057, XHG×424, CAPR×279, PDD×42 |
| 2026-08-24 | -5.17 | $28.23 | MRNA×8, CYPH×1057, XHG×424, CAPR×279, PDD×42 | $10,996.09 | +969.17 | +0.00 | — | MRNA, CYPH, XHG, CAPR, PDD | $10,968.85 | $10,968.85 | — |
| 2026-08-25 | +1.80 | $10,968.85 | — | $10,968.85 | +0.00 | +361.21 | REAX, CYPH, XHG, ASST, INTU | — | $23.30 | $11,308.00 | REAX×56, CYPH×878, XHG×336, ASST×72, INTU×15 |
| 2026-08-26 | +2.02 | $23.30 | REAX×56, CYPH×878, XHG×336, ASST×72, INTU×15 | $10,542.31 | -765.69 | +174.18 | BYND, USDE, PURR, CM, CRM, CRWD, NVDA, RY, SNPS, TD | REAX, CYPH, ASST, INTU | $615.21 | $10,676.40 | XHG×336, BYND×109, USDE×265, PURR×132, CM×5, CRM×3, CRWD×3, NVDA×3, RY×3, SNPS×1, TD×5 |
| 2026-08-27 | — | $615.21 | XHG×336, BYND×109, USDE×265, PURR×132, CM×5, CRM×3, CRWD×3, NVDA×3, RY×3, SNPS×1, TD×5 | $11,075.73 | +399.33 | -20.04 | CAPR, MRNA, BZ, ADSK, MRVL | BYND, USDE, PURR, CM, CRM, CRWD, NVDA, RY, SNPS, TD | $240.44 | $11,022.48 | XHG×336, CAPR×175, MRNA×11, BZ×87, ADSK×9, MRVL×9 |
| 2026-08-28 | +0.75 | $240.44 | XHG×336, CAPR×175, MRNA×11, BZ×87, ADSK×9, MRVL×9 | $10,648.95 | -373.53 | +5.95 | BYND, ANF | XHG, BZ, ADSK, MRVL | $59.11 | $10,638.65 | CAPR×175, MRNA×11, BYND×265, ANF×25 |
| 2026-08-31 | -5.85 | $59.11 | CAPR×175, MRNA×11, BYND×265, ANF×25 | $10,557.11 | -81.54 | -135.15 | — | CAPR, MRNA, ANF | $6,890.75 | $10,415.25 | BYND×265 |
| 2026-09-01 | -6.30 | $6,890.75 | BYND×265 | $10,346.35 | -68.90 | +0.00 | — | BYND | $10,342.86 | $10,342.86 | — |
| 2026-09-02 | -3.83 | $10,342.86 | — | $10,342.86 | +0.00 | +0.00 | — | — | $10,342.86 | $10,342.86 | — |
| 2026-09-03 | -0.90 | $10,342.86 | — | $10,342.86 | +0.00 | -546.12 | GPRO, REAX, CNH, MMED | — | $23.66 | $9,770.74 | GPRO×1452, REAX×140, CNH×188, MMED×107 |
| 2026-09-04 | +2.25 | $23.66 | GPRO×1452, REAX×140, CNH×188, MMED×107 | $9,875.82 | +105.08 | +574.44 | ASST, USDE, DFDV | REAX, CNH, MMED | $2.48 | $10,430.62 | GPRO×1452, ASST×102, USDE×326, DFDV×444 |
| 2026-09-08 | -11.47 | $2.48 | GPRO×1452, ASST×102, USDE×326, DFDV×444 | $10,081.14 | -349.48 | +0.00 | — | GPRO, ASST, USDE, DFDV | $10,049.71 | $10,049.71 | — |
| 2026-09-09 | -13.95 | $10,049.71 | — | $10,049.71 | +0.00 | +0.00 | — | — | $10,049.71 | $10,049.71 | — |
| 2026-09-10 | -13.28 | $10,049.71 | — | $10,049.71 | +0.00 | +0.00 | — | — | $10,049.71 | $10,049.71 | — |
| 2026-09-11 | +0.50 | $10,049.71 | — | $10,049.71 | +0.00 | +258.17 | INDP, BNC, IRD, CMRC | — | $0.14 | $10,273.79 | INDP×930, BNC×511, IRD×407, CMRC×795 |
| 2026-09-14 | -11.00 | $0.14 | INDP×930, BNC×511, IRD×407, CMRC×795 | $10,415.06 | +141.27 | +542.19 | — | IRD | $2,444.94 | $10,951.91 | INDP×930, BNC×511, CMRC×795 |
| 2026-09-15 | -3.84 | $2,444.94 | INDP×930, BNC×511, CMRC×795 | $11,111.95 | +160.04 | +223.20 | — | BNC, CMRC | $7,932.84 | $11,318.04 | INDP×930 |
| 2026-09-16 | +5.30 | $7,932.84 | INDP×930 | $11,336.64 | +18.60 | -8.95 | HLP, SDGR, SSL | — | $1.46 | $11,303.88 | INDP×930, HLP×1469, SDGR×113, SSL×180 |
| 2026-09-17 | +7.38 | $1.46 | INDP×930, HLP×1469, SDGR×113, SSL×180 | $11,356.13 | +52.25 | +440.93 | BBNX, FPS | SDGR, SSL | $36.65 | $11,787.58 | INDP×930, HLP×1469, BBNX×115, FPS×70 |
| 2026-09-18 | +4.86 | $36.65 | INDP×930, HLP×1469, BBNX×115, FPS×70 | $11,710.89 | -76.69 | +78.77 | SDGR, CYPH, TEM | HLP, BBNX, FPS | $5.94 | $11,750.00 | INDP×930, SDGR×92, CYPH×890, TEM×33 |
| 2026-09-21 | +12.87 | $5.94 | INDP×930, SDGR×92, CYPH×890, TEM×33 | $12,184.64 | +434.64 | +381.62 | FEAM, TJGC, LVWR, SECZ | INDP, SDGR, CYPH, TEM | $9.09 | $12,492.54 | FEAM×1230, TJGC×179, LVWR×1841, SECZ×257 |
| 2026-09-22 | -0.50 | $9.09 | FEAM×1230, TJGC×179, LVWR×1841, SECZ×257 | $12,353.76 | -138.78 | +10.28 | — | — | $9.09 | $12,364.04 | FEAM×1230, TJGC×179, LVWR×1841, SECZ×257 |
| 2026-09-23 | +2.29 | $9.09 | FEAM×1230, TJGC×179, LVWR×1841, SECZ×257 | $12,514.78 | +150.74 | -425.33 | GLND, VKTX, SVIA | TJGC, LVWR, SECZ | $19.78 | $12,034.55 | FEAM×1230, GLND×1097, VKTX×70, SVIA×660 |
| 2026-09-24 | -7.66 | $19.78 | FEAM×1230, GLND×1097, VKTX×70, SVIA×660 | $11,958.57 | -75.98 | +2,338.80 | — | FEAM, VKTX, SVIA | $8,401.45 | $14,270.40 | GLND×1097 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 (unchanged overnight, no fees) · equity $10,000.00 vs prior close $10,000.00 (+0.00) | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $7,514.93 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list flatten; ⚪; ret5=+12.3; combo leftover $2500.00; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 107 | $23.33 | $2.31 | — | $5,016.31 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list flatten; ⚪; ret5=+19.7; combo leftover $2500.00; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 49 | $50.62 | $2.14 | — | $2,533.63 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list flatten; ⚪; ret5=+6.2; combo leftover $2500.00; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3085 | $0.81 | $34.24 | — | $0.54 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list flatten; ⚪; ret5=+13.2; combo leftover $2500.00; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.54 | ▲ close $10,345.37 vs 09:30 $10,000.00 (session +386.21) | 16:00 close · cash $0.54 · equity $10,345.37 vs 09:30 $10,000.00 (+345.37; session marks +386.21) · 4 name(s) marked open→close (per-name table). IREN×54 09:30 $45.98 → close $44.76 -65.88; TNDM×107 09:30 $23.33 → close $23.13 -21.40; TPG×49 09:30 $50.62 → close $54.62 +195.84; INO×3085 09:30 $0.81 → close $0.90 +277.65 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.54 | ▲ 09:30 equity $10,412.10 vs yday $10,345.37 (+66.73) | 09:30 open · cash $0.54 (unchanged overnight, no fees) · equity $10,412.10 vs prior close $10,345.37 (+66.73) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 54 | $44.09 | $2.18 | $-106.39 | $2,379.22 | ▼ -106.39 after sell → book $10,409.92; vs 09:30 mark -2.18 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 107 | $22.92 | $2.35 | $-48.53 | $4,829.31 | ▼ -48.53 after sell → book $10,407.57; vs 09:30 mark -2.35 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 49 | $55.29 | $2.17 | $+224.37 | $7,536.35 | ▲ +224.37 after sell → book $10,405.40; vs 09:30 mark -2.17 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 3085 | $0.93 | $38.48 | $+297.48 | $10,366.92 | ▲ +297.48 after sell → book $10,366.92; vs 09:30 mark -38.48 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 105 | $24.68 | $2.31 | — | $7,773.22 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; combo leftover $2591.73; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 132 | $19.57 | $2.39 | — | $5,187.59 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $2591.73; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 1178 | $2.20 | $15.20 | — | $2,580.79 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; combo leftover $2591.73; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 231 | $11.12 | $2.98 | — | $9.09 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $2591.73; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.09 | ▼ close $10,066.79 vs 09:30 $10,412.10 (session -277.26) | 16:00 close · cash $9.09 · equity $10,066.79 vs 09:30 $10,412.10 (-345.31; session marks -277.26) · 4 name(s) marked open→close (per-name table). QMCO×105 09:30 $24.68 → close $26.11 +150.15; ARX×132 09:30 $19.57 → close $19.58 +1.32; ZENA×1178 09:30 $2.20 → close $2.14 -70.68; AIRO×231 09:30 $11.12 → close $9.57 -358.05 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.09 | ▼ 09:30 equity $9,866.28 vs yday $10,066.79 (-200.51) | 09:30 open · cash $9.09 (unchanged overnight, no fees) · equity $9,866.28 vs prior close $10,066.79 (-200.51) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 105 | $24.83 | $2.34 | $+11.10 | $2,613.90 | ▲ +11.10 after sell → book $9,863.94; vs 09:30 mark -2.34 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 132 | $19.57 | $2.43 | $-4.81 | $5,194.71 | ▼ -4.81 after sell → book $9,861.51; vs 09:30 mark -2.43 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 1178 | $2.08 | $15.41 | $-166.08 | $7,635.43 | ▼ -166.08 after sell → book $9,846.10; vs 09:30 mark -15.41 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 231 | $9.57 | $3.04 | $-364.07 | $9,843.06 | ▼ -364.07 after sell → book $9,843.06; vs 09:30 mark -3.04 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 587 | $4.19 | $7.57 | — | $7,375.96 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $2460.77; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 358 | $6.87 | $4.62 | — | $4,911.88 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $2460.77; owner union_hot_n4_holdup | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 180 | $13.64 | $2.53 | — | $2,454.15 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $2460.77; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 59 | $41.23 | $2.17 | — | $19.42 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $2460.77; owner union_hot_n4_holdup | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.42 | ▲ close $9,851.95 vs 09:30 $9,866.28 (session +25.77) | 16:00 close · cash $19.42 · equity $9,851.95 vs 09:30 $9,866.28 (-14.33; session marks +25.77) · 4 name(s) marked open→close (per-name table). XHG×587 09:30 $4.19 → close $3.91 -164.36; CAPR×358 09:30 $6.87 → close $7.45 +207.64; STDN×180 09:30 $13.64 → close $13.31 -59.40; HTFL×59 09:30 $41.23 → close $41.94 +41.89 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.42 | ▲ 09:30 equity $9,861.50 vs yday $9,851.95 (+9.55) | 09:30 open · cash $19.42 (unchanged overnight, no fees) · equity $9,861.50 vs prior close $9,851.95 (+9.55) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 587 | $3.94 | $7.69 | $-162.01 | $2,324.51 | ▼ -162.01 after sell → book $9,853.81; vs 09:30 mark -7.69 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 180 | $13.31 | $2.58 | $-64.51 | $4,717.73 | ▼ -64.51 after sell → book $9,851.23; vs 09:30 mark -2.58 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 59 | $41.50 | $2.20 | $+11.57 | $7,164.03 | ▲ +11.57 after sell → book $9,849.03; vs 09:30 mark -2.20 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,164.03 | ▼ close $9,698.67 vs 09:30 $9,861.50 (session -150.36) | 16:00 close · cash $7,164.03 · equity $9,698.67 vs 09:30 $9,861.50 (-162.83; session marks -150.36) · 1 name(s) marked open→close (per-name table). CAPR×358 09:30 $7.50 → close $7.08 -150.36 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,164.03 | ▲ 09:30 equity $9,738.05 vs yday $9,698.67 (+39.38) | 09:30 open · cash $7,164.03 (unchanged overnight, no fees) · equity $9,738.05 vs prior close $9,698.67 (+39.38) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 358 | $7.19 | $4.70 | $+105.24 | $9,733.36 | ▲ +105.24 after sell → book $9,733.36; vs 09:30 mark -4.69 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,733.36 | ▲ close $9,733.36 vs 09:30 $9,738.05 (session +0.00) | 16:00 close · cash $9,733.36 · no lots left · equity $9,733.36. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,733.36 | ▲ 09:30 equity $9,733.36 vs yday $9,733.36 (-0.00) | 09:30 open · cash $9,733.36 (unchanged overnight, no fees) · equity $9,733.36 vs prior close $9,733.36 (-0.00) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $8,530.22 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $1216.67; owner union_hot_n4_holdup | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1057 | $1.15 | $13.64 | — | $7,301.04 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $1216.67; owner union_hot_n4_holdup | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 102 | $11.81 | $2.30 | — | $6,093.61 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1216.67; owner union_hot_n4_holdup | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 888 | $1.37 | $11.46 | — | $4,865.59 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $1216.67; owner union_hot_n4_holdup | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ROST` | 21 | $229.55 | $2.05 | — | $42.99 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=-5.5; combo leftover $4865.59; owner overnight_mega_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.99 | ▼ close $9,635.03 vs 09:30 $9,733.36 (session -66.87) | 16:00 close · cash $42.99 · equity $9,635.03 vs 09:30 $9,733.36 (-98.33; session marks -66.87) · 5 name(s) marked open→close (per-name table). MRNA×8 09:30 $150.14 → close $133.32 -134.56; CYPH×1057 09:30 $1.15 → close $1.19 +42.28; ABCL×102 09:30 $11.81 → close $11.57 -24.99; AZI×888 09:30 $1.37 → close $1.44 +62.16; ROST×21 09:30 $229.55 → close $228.99 -11.76 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.99 | ▲ 09:30 equity $10,100.58 vs yday $9,635.03 (+465.55) | 09:30 open · cash $42.99 (unchanged overnight, no fees) · equity $10,100.58 vs prior close $9,635.03 (+465.55) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 102 | $11.57 | $2.32 | $-29.61 | $1,220.81 | ▼ -29.61 after sell → book $10,098.26; vs 09:30 mark -2.32 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 888 | $1.46 | $11.61 | $+56.85 | $2,505.68 | ▲ +56.85 after sell → book $10,086.65; vs 09:30 mark -11.61 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ROST` | 21 | $243.85 | $2.10 | $+296.14 | $7,624.42 | ▲ +296.14 after sell → book $10,084.54; vs 09:30 mark -2.11 | overnight_mega_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 424 | $4.49 | $5.47 | — | $5,715.19 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $1906.11; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 279 | $6.81 | $3.60 | — | $3,811.60 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $1906.11; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PDD` | 42 | $90.03 | $2.12 | — | $28.23 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=+6.4; combo leftover $3811.60; owner overnight_mega_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.23 | ▼ close $10,026.92 vs 09:30 $10,100.58 (session -46.44) | 16:00 close · cash $28.23 · equity $10,026.92 vs 09:30 $10,100.58 (-73.66; session marks -46.44) · 5 name(s) marked open→close (per-name table). MRNA×8 09:30 $133.11 → close $145.13 +96.16; CYPH×1057 09:30 $1.32 → close $1.42 +105.70; XHG×424 09:30 $4.49 → close $4.41 -33.92; CAPR×279 09:30 $6.81 → close $6.29 -145.08; PDD×42 09:30 $90.03 → close $88.38 -69.30 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.23 | ▲ 09:30 equity $10,996.09 vs yday $10,026.92 (+969.17) | 09:30 open · cash $28.23 (unchanged overnight, no fees) · equity $10,996.09 vs prior close $10,026.92 (+969.17) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 8 | $142.70 | $2.03 | $-63.57 | $1,167.79 | ▼ -63.57 after sell → book $10,994.05; vs 09:30 mark -2.04 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1057 | $1.83 | $13.83 | $+691.30 | $3,088.28 | ▲ +691.30 after sell → book $10,980.23; vs 09:30 mark -13.82 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 424 | $4.32 | $5.55 | $-83.10 | $4,914.40 | ▼ -83.10 after sell → book $10,974.67; vs 09:30 mark -5.56 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 279 | $8.03 | $3.66 | $+333.12 | $7,151.11 | ▲ +333.12 after sell → book $10,971.01; vs 09:30 mark -3.66 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PDD` | 42 | $90.95 | $2.16 | $+34.37 | $10,968.85 | ▲ +34.37 after sell → book $10,968.85; vs 09:30 mark -2.16 | overnight_mega_h1: dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,968.85 | ▲ close $10,968.85 vs 09:30 $10,996.09 (session +0.00) | 16:00 close · cash $10,968.85 · no lots left · equity $10,968.85. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,968.85 | ▲ 09:30 equity $10,968.85 vs yday $10,968.85 (+0.00) | 09:30 open · cash $10,968.85 (unchanged overnight, no fees) · equity $10,968.85 vs prior close $10,968.85 (+0.00) | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 56 | $24.11 | $2.16 | — | $9,616.53 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $1371.11; owner union_hot_n4_holdup | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 878 | $1.56 | $11.33 | — | $8,235.53 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $1371.11; owner union_hot_n4_holdup | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 336 | $4.07 | $4.33 | — | $6,863.67 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $1371.11; owner union_hot_n4_holdup | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 72 | $19.04 | $2.21 | — | $5,490.59 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $1371.11; owner union_hot_n4_holdup | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `INTU` | 15 | $364.35 | $2.04 | — | $23.30 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $5490.59; owner overnight_mega_h1 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.30 | ▲ close $11,308.00 vs 09:30 $10,968.85 (session +361.21) | 16:00 close · cash $23.30 · equity $11,308.00 vs 09:30 $10,968.85 (+339.15; session marks +361.21) · 5 name(s) marked open→close (per-name table). REAX×56 09:30 $24.11 → close $28.43 +241.92; CYPH×878 09:30 $1.56 → close $1.64 +70.24; XHG×336 09:30 $4.07 → close $4.02 -16.80; ASST×72 09:30 $19.04 → close $21.39 +169.20; INTU×15 09:30 $364.35 → close $357.46 -103.35 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.30 | ▼ 09:30 equity $10,542.31 vs yday $11,308.00 (-765.69) | 09:30 open · cash $23.30 (unchanged overnight, no fees) · equity $10,542.31 vs prior close $11,308.00 (-765.69) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 56 | $26.61 | $2.18 | $+135.66 | $1,511.28 | ▲ +135.66 after sell → book $10,540.13; vs 09:30 mark -2.18 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 878 | $1.60 | $11.48 | $+12.31 | $2,904.60 | ▲ +12.31 after sell → book $10,528.65; vs 09:30 mark -11.48 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 72 | $20.72 | $2.23 | $+116.52 | $4,394.21 | ▲ +116.52 after sell → book $10,526.42; vs 09:30 mark -2.23 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `INTU` | 15 | $323.47 | $2.08 | $-617.32 | $9,244.18 | ▼ -617.32 after sell → book $10,524.34; vs 09:30 mark -2.08 | overnight_mega_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟡 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 109 | $14.11 | $2.32 | — | $7,703.87 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $1540.70; owner union_hot_n4_holdup | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 265 | $5.81 | $3.42 | — | $6,160.80 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $1540.70; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 132 | $11.59 | $2.39 | — | $4,629.19 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list overnight; 🔵; ret5=+64.9; combo leftover $1540.70; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 5 | $118.50 | $2.00 | — | $4,034.69 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=-2.7; combo leftover $661.31; owner overnight_mega_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CRM` | 3 | $199.94 | $2.00 | — | $3,432.87 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; ret5=+2.1; combo leftover $661.31; owner overnight_mega_h1 | join🟡 sector🔴 gen🟢 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 catal🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CRWD` | 3 | $182.75 | $2.00 | — | $2,882.62 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=-12.9; combo leftover $661.31; owner overnight_mega_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NVDA` | 3 | $212.64 | $2.00 | — | $2,242.70 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=-3.0; combo leftover $661.31; owner overnight_mega_h1 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 catal🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `RY` | 3 | $206.95 | $2.00 | — | $1,619.85 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=-2.8; combo leftover $661.31; owner overnight_mega_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SNPS` | 1 | $405.10 | $1.99 | — | $1,212.76 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=+1.1; combo leftover $661.31; owner overnight_mega_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TD` | 5 | $119.11 | $2.00 | — | $615.21 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=-2.4; combo leftover $661.31; owner overnight_mega_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $615.21 | ▲ close $10,676.40 vs 09:30 $10,542.31 (session +174.18) | 16:00 close · cash $615.21 · equity $10,676.40 vs 09:30 $10,542.31 (+134.09; session marks +174.18) · 11 name(s) marked open→close (per-name table). XHG×336 09:30 $3.81 → close $4.06 +84.00; BYND×109 09:30 $14.11 → close $14.25 +15.26; USDE×265 09:30 $5.81 → close $5.98 +45.05; PURR×132 09:30 $11.59 → close $11.56 -3.30; CM×5 09:30 $118.50 → close $118.20 -1.50; CRM×3 09:30 $199.94 → close $205.62 +17.04; CRWD×3 09:30 $182.75 → close $189.18 +19.29; NVDA×3 09:30 $212.64 → close $209.66 -8.94; RY×3 09:30 $206.95 → close $207.21 +0.78; SNPS×1 09:30 $405.10 → close $410.00 +4.90; TD×5 09:30 $119.11 → close $119.43 +1.60 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $615.21 | ▲ 09:30 equity $11,075.73 vs yday $10,676.40 (+399.33) | 09:30 open · cash $615.21 (unchanged overnight, no fees) · equity $11,075.73 vs prior close $10,676.40 (+399.33) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 109 | $14.20 | $2.35 | $+5.15 | $2,160.66 | ▲ +5.15 after sell → book $11,073.38; vs 09:30 mark -2.35 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 265 | $6.50 | $3.48 | $+175.96 | $3,879.68 | ▲ +175.96 after sell → book $11,069.90; vs 09:30 mark -3.48 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟡 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 132 | $12.18 | $2.42 | $+73.73 | $5,485.02 | ▲ +73.73 after sell → book $11,067.48; vs 09:30 mark -2.42 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CM` | 5 | $118.77 | $2.02 | $-2.68 | $6,076.85 | ▼ -2.68 after sell → book $11,065.46; vs 09:30 mark -2.02 | overnight_mega_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CRM` | 3 | $230.05 | $2.02 | $+86.31 | $6,764.98 | ▲ +86.31 after sell → book $11,063.44; vs 09:30 mark -2.02 | overnight_mega_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRWD` | 3 | $208.25 | $2.02 | $+72.48 | $7,387.71 | ▲ +72.48 after sell → book $11,061.42; vs 09:30 mark -2.02 | overnight_mega_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NVDA` | 3 | $222.86 | $2.02 | $+26.64 | $8,054.27 | ▲ +26.64 after sell → book $11,059.40; vs 09:30 mark -2.02 | overnight_mega_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `RY` | 3 | $206.82 | $2.02 | $-4.41 | $8,672.71 | ▼ -4.41 after sell → book $11,057.38; vs 09:30 mark -2.02 | overnight_mega_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `SNPS` | 1 | $419.66 | $2.01 | $+10.55 | $9,090.36 | ▲ +10.55 after sell → book $11,055.37; vs 09:30 mark -2.01 | overnight_mega_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TD` | 5 | $120.17 | $2.02 | $+1.27 | $9,689.18 | ▲ +1.27 after sell → book $11,053.34; vs 09:30 mark -2.03 | overnight_mega_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 175 | $9.19 | $2.52 | — | $8,078.42 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; combo leftover $1614.86; owner union_hot_n4_holdup | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 11 | $144.18 | $2.02 | — | $6,490.41 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; ret5=-14.2; combo leftover $1614.86; owner union_hot_n4_holdup | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 87 | $18.50 | $2.25 | — | $4,878.66 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; combo leftover $1614.86; owner union_hot_n4_holdup | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 9 | $261.47 | $2.02 | — | $2,523.42 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=+1.4; combo leftover $2439.33; owner overnight_mega_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 9 | $253.44 | $2.02 | — | $240.44 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega,mover_buy; 🔵; ret5=+3.3; combo leftover $2439.33; owner overnight_mega_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $240.44 | ▼ close $11,022.48 vs 09:30 $11,075.73 (session -20.04) | 16:00 close · cash $240.44 · equity $11,022.48 vs 09:30 $11,075.73 (-53.25; session marks -20.04) · 6 name(s) marked open→close (per-name table). XHG×336 09:30 $4.06 → close $3.80 -87.36; CAPR×175 09:30 $9.19 → close $10.06 +152.25; MRNA×11 09:30 $144.18 → close $142.77 -15.51; BZ×87 09:30 $18.50 → close $18.00 -43.50; ADSK×9 09:30 $261.47 → close $270.58 +81.99; MRVL×9 09:30 $253.44 → close $241.45 -107.91 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $240.44 | ▼ 09:30 equity $10,648.95 vs yday $11,022.48 (-373.53) | 09:30 open · cash $240.44 (unchanged overnight, no fees) · equity $10,648.95 vs prior close $11,022.48 (-373.53) | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 336 | $3.69 | $4.40 | $-136.41 | $1,475.88 | ▼ -136.41 after sell → book $10,644.55; vs 09:30 mark -4.40 | union_hot_n4_holdup: dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 87 | $18.15 | $2.28 | $-34.98 | $3,052.65 | ▼ -34.98 after sell → book $10,642.27; vs 09:30 mark -2.28 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `ADSK` | 9 | $261.16 | $2.05 | $-6.85 | $5,401.04 | ▼ -6.85 after sell → book $10,640.22; vs 09:30 mark -2.05 | overnight_mega_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 9 | $225.26 | $2.04 | $-257.68 | $7,426.34 | ▼ -257.68 after sell → book $10,638.18; vs 09:30 mark -2.04 | overnight_mega_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 265 | $14.00 | $3.42 | — | $3,712.92 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $3713.17; owner union_hot_n4_holdup | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 25 | $146.07 | $2.06 | — | $59.11 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $3713.17; owner union_hot_n4_holdup | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.11 | ▲ close $10,638.65 vs 09:30 $10,648.95 (session +5.95) | 16:00 close · cash $59.11 · equity $10,638.65 vs 09:30 $10,648.95 (-10.30; session marks +5.95) · 4 name(s) marked open→close (per-name table). CAPR×175 09:30 $9.73 → close $9.59 -24.50; MRNA×11 09:30 $137.19 → close $137.99 +8.80; BYND×265 09:30 $14.00 → close $13.86 -37.10; ANF×25 09:30 $146.07 → close $148.42 +58.75 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.11 | ▼ 09:30 equity $10,557.11 vs yday $10,638.65 (-81.54) | 09:30 open · cash $59.11 (unchanged overnight, no fees) · equity $10,557.11 vs prior close $10,638.65 (-81.54) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 175 | $9.50 | $2.56 | $+49.18 | $1,719.05 | ▲ +49.18 after sell → book $10,554.55; vs 09:30 mark -2.56 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 11 | $134.10 | $2.04 | $-114.95 | $3,192.11 | ▼ -114.95 after sell → book $10,552.51; vs 09:30 mark -2.04 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 25 | $148.03 | $2.10 | $+44.83 | $6,890.75 | ▲ +44.83 after sell → book $10,550.40; vs 09:30 mark -2.11 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,890.75 | ▼ close $10,415.25 vs 09:30 $10,557.11 (session -135.15) | 16:00 close · cash $6,890.75 · equity $10,415.25 vs 09:30 $10,557.11 (-141.86; session marks -135.15) · 1 name(s) marked open→close (per-name table). BYND×265 09:30 $13.81 → close $13.30 -135.15 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,890.75 | ▼ 09:30 equity $10,346.35 vs yday $10,415.25 (-68.90) | 09:30 open · cash $6,890.75 (unchanged overnight, no fees) · equity $10,346.35 vs prior close $10,415.25 (-68.90) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 265 | $13.04 | $3.49 | $-261.31 | $10,342.86 | ▼ -261.31 after sell → book $10,342.86; vs 09:30 mark -3.49 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,342.86 | ▲ close $10,342.86 vs 09:30 $10,346.35 (session +0.00) | 16:00 close · cash $10,342.86 · no lots left · equity $10,342.86. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,342.86 | ▲ 09:30 equity $10,342.86 vs yday $10,342.86 (+0.00) | 09:30 open · cash $10,342.86 (unchanged overnight, no fees) · equity $10,342.86 vs prior close $10,342.86 (+0.00) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,342.86 | ▲ close $10,342.86 vs 09:30 $10,342.86 (session +0.00) | 16:00 close · cash $10,342.86 · no lots left · equity $10,342.86. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,342.86 | ▲ 09:30 equity $10,342.86 vs yday $10,342.86 (+0.00) | 09:30 open · cash $10,342.86 (unchanged overnight, no fees) · equity $10,342.86 vs prior close $10,342.86 (+0.00) | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1452 | $1.78 | $18.73 | — | $7,739.57 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $2585.72; owner union_hot_n4_holdup | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 140 | $18.40 | $2.41 | — | $5,161.16 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $2585.72; owner union_hot_n4_holdup | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 188 | $13.71 | $2.55 | — | $2,581.13 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $2585.72; owner union_hot_n4_holdup | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 107 | $23.88 | $2.31 | — | $23.66 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $2585.72; owner union_hot_n4_holdup | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.66 | ▼ close $9,770.74 vs 09:30 $10,342.86 (session -546.12) | 16:00 close · cash $23.66 · equity $9,770.74 vs 09:30 $10,342.86 (-572.12; session marks -546.12) · 4 name(s) marked open→close (per-name table). GPRO×1452 09:30 $1.78 → close $1.39 -566.28; REAX×140 09:30 $18.40 → close $18.40 +0.00; CNH×188 09:30 $13.71 → close $13.84 +24.44; MMED×107 09:30 $23.88 → close $23.84 -4.28 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.66 | ▲ 09:30 equity $9,875.82 vs yday $9,770.74 (+105.08) | 09:30 open · cash $23.66 (unchanged overnight, no fees) · equity $9,875.82 vs prior close $9,770.74 (+105.08) | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 140 | $18.15 | $2.45 | $-39.86 | $2,562.20 | ▼ -39.86 after sell → book $9,873.36; vs 09:30 mark -2.46 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 188 | $13.89 | $2.61 | $+28.68 | $5,170.92 | ▲ +28.68 after sell → book $9,870.76; vs 09:30 mark -2.60 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 107 | $23.84 | $2.35 | $-8.94 | $7,719.45 | ▼ -8.94 after sell → book $9,868.41; vs 09:30 mark -2.35 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 102 | $25.18 | $2.30 | — | $5,148.79 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $2573.15; owner union_hot_n4_holdup | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 326 | $7.87 | $4.21 | — | $2,578.97 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $2573.15; owner union_hot_n4_holdup | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 444 | $5.79 | $5.73 | — | $2.48 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $2573.15; owner union_hot_n4_holdup | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.48 | ▲ close $10,430.62 vs 09:30 $9,875.82 (session +574.44) | 16:00 close · cash $2.48 · equity $10,430.62 vs 09:30 $9,875.82 (+554.80; session marks +574.44) · 4 name(s) marked open→close (per-name table). GPRO×1452 09:30 $1.48 → close $1.70 +319.44; ASST×102 09:30 $25.18 → close $27.14 +199.92; USDE×326 09:30 $7.87 → close $7.93 +19.56; DFDV×444 09:30 $5.79 → close $5.87 +35.52 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.48 | ▼ 09:30 equity $10,081.14 vs yday $10,430.62 (-349.48) | 09:30 open · cash $2.48 (unchanged overnight, no fees) · equity $10,081.14 vs prior close $10,430.62 (-349.48) | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 1452 | $1.56 | $18.99 | $-349.90 | $2,255.87 | ▼ -349.90 after sell → book $10,062.15; vs 09:30 mark -18.99 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 102 | $26.44 | $2.33 | $+123.89 | $4,950.41 | ▲ +123.89 after sell → book $10,059.81; vs 09:30 mark -2.34 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 326 | $7.76 | $4.28 | $-44.35 | $7,475.89 | ▼ -44.35 after sell → book $10,055.53; vs 09:30 mark -4.28 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 444 | $5.81 | $5.82 | $-2.67 | $10,049.71 | ▼ -2.67 after sell → book $10,049.71; vs 09:30 mark -5.82 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,049.71 | ▲ close $10,049.71 vs 09:30 $10,081.14 (session +0.00) | 16:00 close · cash $10,049.71 · no lots left · equity $10,049.71. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,049.71 | ▲ 09:30 equity $10,049.71 vs yday $10,049.71 (+0.00) | 09:30 open · cash $10,049.71 (unchanged overnight, no fees) · equity $10,049.71 vs prior close $10,049.71 (+0.00) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,049.71 | ▲ close $10,049.71 vs 09:30 $10,049.71 (session +0.00) | 16:00 close · cash $10,049.71 · no lots left · equity $10,049.71. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,049.71 | ▲ 09:30 equity $10,049.71 vs yday $10,049.71 (+0.00) | 09:30 open · cash $10,049.71 (unchanged overnight, no fees) · equity $10,049.71 vs prior close $10,049.71 (+0.00) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,049.71 | ▲ close $10,049.71 vs 09:30 $10,049.71 (session +0.00) | 16:00 close · cash $10,049.71 · no lots left · equity $10,049.71. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,049.71 | ▲ 09:30 equity $10,049.71 vs yday $10,049.71 (+0.00) | 09:30 open · cash $10,049.71 (unchanged overnight, no fees) · equity $10,049.71 vs prior close $10,049.71 (+0.00) | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 930 | $2.70 | $12.00 | — | $7,526.71 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $2512.43; owner union_hot_n4_holdup | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 511 | $4.91 | $6.59 | — | $5,011.11 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $2512.43; owner union_hot_n4_holdup | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 407 | $6.16 | $5.25 | — | $2,498.74 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; combo leftover $2512.43; owner union_hot_n4_holdup | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 795 | $3.13 | $10.26 | — | $0.14 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; combo leftover $2512.43; owner union_hot_n4_holdup | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.14 | ▲ close $10,273.79 vs 09:30 $10,049.71 (session +258.17) | 16:00 close · cash $0.14 · equity $10,273.79 vs 09:30 $10,049.71 (+224.08; session marks +258.17) · 4 name(s) marked open→close (per-name table). INDP×930 09:30 $2.70 → close $2.77 +65.10; BNC×511 09:30 $4.91 → close $4.80 -56.21; IRD×407 09:30 $6.16 → close $6.04 -48.84; CMRC×795 09:30 $3.13 → close $3.50 +298.12 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.14 | ▲ 09:30 equity $10,415.06 vs yday $10,273.79 (+141.27) | 09:30 open · cash $0.14 (unchanged overnight, no fees) · equity $10,415.06 vs prior close $10,273.79 (+141.27) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 407 | $6.02 | $5.34 | $-67.57 | $2,444.94 | ▼ -67.57 after sell → book $10,409.72; vs 09:30 mark -5.34 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,444.94 | ▲ close $10,951.91 vs 09:30 $10,415.06 (session +542.19) | 16:00 close · cash $2,444.94 · equity $10,951.91 vs 09:30 $10,415.06 (+536.85; session marks +542.19) · 3 name(s) marked open→close (per-name table). INDP×930 09:30 $2.80 → close $3.14 +316.20; BNC×511 09:30 $5.03 → close $5.27 +122.64; CMRC×795 09:30 $3.51 → close $3.64 +103.35 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,444.94 | ▲ 09:30 equity $11,111.95 vs yday $10,951.91 (+160.04) | 09:30 open · cash $2,444.94 (unchanged overnight, no fees) · equity $11,111.95 vs prior close $10,951.91 (+160.04) | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 511 | $5.11 | $6.70 | $+88.91 | $5,049.45 | ▲ +88.91 after sell → book $11,105.25; vs 09:30 mark -6.70 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 795 | $3.64 | $10.41 | $+384.78 | $7,932.84 | ▲ +384.78 after sell → book $11,094.84; vs 09:30 mark -10.41 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,932.84 | ▲ close $11,318.04 vs 09:30 $11,111.95 (session +223.20) | 16:00 close · cash $7,932.84 · equity $11,318.04 vs 09:30 $11,111.95 (+206.09; session marks +223.20) · 1 name(s) marked open→close (per-name table). INDP×930 09:30 $3.40 → close $3.64 +223.20 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,932.84 | ▲ 09:30 equity $11,336.64 vs yday $11,318.04 (+18.60) | 09:30 open · cash $7,932.84 (unchanged overnight, no fees) · equity $11,336.64 vs prior close $11,318.04 (+18.60) | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 1469 | $1.80 | $18.95 | — | $5,269.69 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; combo leftover $2644.28; owner union_hot_n4_holdup | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 113 | $23.29 | $2.33 | — | $2,635.59 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; combo leftover $2644.28; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 180 | $14.62 | $2.53 | — | $1.46 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; combo leftover $2644.28; owner union_hot_n4_holdup | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.46 | ▼ close $11,303.88 vs 09:30 $11,336.64 (session -8.95) | 16:00 close · cash $1.46 · equity $11,303.88 vs 09:30 $11,336.64 (-32.76; session marks -8.95) · 4 name(s) marked open→close (per-name table). INDP×930 09:30 $3.66 → close $3.21 -418.50; HLP×1469 09:30 $1.80 → close $2.07 +396.63; SDGR×113 09:30 $23.29 → close $23.93 +72.32; SSL×180 09:30 $14.62 → close $14.29 -59.40 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.46 | ▲ 09:30 equity $11,356.13 vs yday $11,303.88 (+52.25) | 09:30 open · cash $1.46 (unchanged overnight, no fees) · equity $11,356.13 vs prior close $11,303.88 (+52.25) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 113 | $24.09 | $2.37 | $+85.70 | $2,721.26 | ▲ +85.70 after sell → book $11,353.76; vs 09:30 mark -2.37 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 180 | $13.77 | $2.58 | $-158.11 | $5,197.28 | ▼ -158.11 after sell → book $11,351.18; vs 09:30 mark -2.58 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 115 | $22.46 | $2.33 | — | $2,612.05 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; combo leftover $2598.64; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 news🔴 digest🔴 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 70 | $36.76 | $2.20 | — | $36.65 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; combo leftover $2598.64; owner union_hot_n4_holdup | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.65 | ▲ close $11,787.58 vs 09:30 $11,356.13 (session +440.93) | 16:00 close · cash $36.65 · equity $11,787.58 vs 09:30 $11,356.13 (+431.45; session marks +440.93) · 4 name(s) marked open→close (per-name table). INDP×930 09:30 $3.30 → close $3.93 +585.90; HLP×1469 09:30 $2.10 → close $2.02 -117.52; BBNX×115 09:30 $22.46 → close $21.43 -118.45; FPS×70 09:30 $36.76 → close $38.06 +91.00 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.65 | ▼ 09:30 equity $11,710.89 vs yday $11,787.58 (-76.69) | 09:30 open · cash $36.65 (unchanged overnight, no fees) · equity $11,710.89 vs prior close $11,787.58 (-76.69) | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 1469 | $1.96 | $19.22 | $+196.87 | $2,896.67 | ▲ +196.87 after sell → book $11,691.67; vs 09:30 mark -19.22 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 115 | $21.30 | $2.37 | $-138.11 | $5,343.80 | ▼ -138.11 after sell → book $11,689.30; vs 09:30 mark -2.37 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 70 | $39.50 | $2.23 | $+187.37 | $8,106.56 | ▲ +187.37 after sell → book $11,687.06; vs 09:30 mark -2.24 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 92 | $29.32 | $2.27 | — | $5,406.86 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $2702.19; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 890 | $3.04 | $11.48 | — | $2,694.23 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $2702.19; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 33 | $81.40 | $2.09 | — | $5.94 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; combo leftover $2702.19; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.94 | ▲ close $11,750.00 vs 09:30 $11,710.89 (session +78.77) | 16:00 close · cash $5.94 · equity $11,750.00 vs 09:30 $11,710.89 (+39.11; session marks +78.77) · 4 name(s) marked open→close (per-name table). INDP×930 09:30 $3.85 → close $3.55 -279.00; SDGR×92 09:30 $29.32 → close $29.02 -27.60; CYPH×890 09:30 $3.04 → close $3.60 +502.85; TEM×33 09:30 $81.40 → close $77.84 -117.48 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.94 | ▲ 09:30 equity $12,184.64 vs yday $11,750.00 (+434.64) | 09:30 open · cash $5.94 (unchanged overnight, no fees) · equity $12,184.64 vs prior close $11,750.00 (+434.64) | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 930 | $3.55 | $12.18 | $+766.33 | $3,295.26 | ▲ +766.33 after sell → book $12,172.46; vs 09:30 mark -12.18 | union_hot_n4_holdup: dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 92 | $29.43 | $2.30 | $+5.55 | $6,000.52 | ▲ +5.55 after sell → book $12,170.16; vs 09:30 mark -2.30 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 890 | $4.00 | $11.66 | $+835.71 | $9,548.86 | ▲ +835.71 after sell → book $12,158.50; vs 09:30 mark -11.66 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 33 | $79.08 | $2.12 | $-80.77 | $12,156.38 | ▼ -80.77 after sell → book $12,156.38; vs 09:30 mark -2.12 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 1230 | $2.47 | $15.87 | — | $9,102.41 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; combo leftover $3039.09; owner union_hot_n4_holdup | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 179 | $16.91 | $2.53 | — | $6,073.00 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $3039.09; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 1841 | $1.65 | $23.75 | — | $3,011.60 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; combo leftover $3039.09; owner union_hot_n4_holdup | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 257 | $11.67 | $3.32 | — | $9.09 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; combo leftover $3039.09; owner union_hot_n4_holdup | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.09 | ▲ close $12,492.54 vs 09:30 $12,184.64 (session +381.62) | 16:00 close · cash $9.09 · equity $12,492.54 vs 09:30 $12,184.64 (+307.90; session marks +381.62) · 4 name(s) marked open→close (per-name table). FEAM×1230 09:30 $2.47 → close $2.48 +12.30; TJGC×179 09:30 $16.91 → close $17.58 +119.93; LVWR×1841 09:30 $1.65 → close $1.53 -220.92; SECZ×257 09:30 $11.67 → close $13.50 +470.31 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.09 | ▼ 09:30 equity $12,353.76 vs yday $12,492.54 (-138.78) | 09:30 open · cash $9.09 (unchanged overnight, no fees) · equity $12,353.76 vs prior close $12,492.54 (-138.78) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.09 | ▲ close $12,364.04 vs 09:30 $12,353.76 (session +10.28) | 16:00 close · cash $9.09 · equity $12,364.04 vs 09:30 $12,353.76 (+10.28; session marks +10.28) · 4 name(s) marked open→close (per-name table). FEAM×1230 09:30 $2.48 → close $2.48 +0.00; TJGC×179 09:30 $17.58 → close $17.58 +0.00; LVWR×1841 09:30 $1.53 → close $1.53 +0.00; SECZ×257 09:30 $12.96 → close $13.00 +10.28 | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.09 | ▲ 09:30 equity $12,514.78 vs yday $12,364.04 (+150.74) | 09:30 open · cash $9.09 (unchanged overnight, no fees) · equity $12,514.78 vs prior close $12,364.04 (+150.74) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 179 | $16.92 | $2.58 | $-3.32 | $3,035.19 | ▼ -3.32 after sell → book $12,512.20; vs 09:30 mark -2.58 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 1841 | $1.41 | $24.08 | $-489.66 | $5,606.93 | ▼ -489.66 after sell → book $12,488.13; vs 09:30 mark -24.07 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 257 | $12.80 | $3.38 | $+283.71 | $8,893.14 | ▲ +283.71 after sell → book $12,484.74; vs 09:30 mark -3.39 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 1097 | $2.70 | $14.15 | — | $5,917.09 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $2964.38; owner union_hot_n4_holdup | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 70 | $41.76 | $2.20 | — | $2,991.69 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $2964.38; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 660 | $4.49 | $8.51 | — | $19.78 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; combo leftover $2964.38; owner union_hot_n4_holdup | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.78 | ▼ close $12,034.55 vs 09:30 $12,514.78 (session -425.33) | 16:00 close · cash $19.78 · equity $12,034.55 vs 09:30 $12,514.78 (-480.23; session marks -425.33) · 4 name(s) marked open→close (per-name table). FEAM×1230 09:30 $2.92 → close $2.64 -344.40; GLND×1097 09:30 $2.70 → close $2.91 +230.37; VKTX×70 09:30 $41.76 → close $41.65 -7.70; SVIA×660 09:30 $4.49 → close $4.03 -303.60 | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.78 | ▼ 09:30 equity $11,958.57 vs yday $12,034.55 (-75.98) | 09:30 open · cash $19.78 (unchanged overnight, no fees) · equity $11,958.57 vs prior close $12,034.55 (-75.98) | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 1230 | $2.68 | $16.10 | $+226.34 | $3,300.08 | ▲ +226.34 after sell → book $11,942.47; vs 09:30 mark -16.10 | union_hot_n4_holdup: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 70 | $36.02 | $2.23 | $-405.88 | $5,819.60 | ▼ -405.88 after sell → book $11,940.24; vs 09:30 mark -2.23 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 660 | $3.92 | $8.64 | $-390.06 | $8,401.45 | ▼ -390.06 after sell → book $11,931.60; vs 09:30 mark -8.64 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,401.45 | ▲ close $14,270.40 vs 09:30 $11,958.57 (session +2,338.80) | 16:00 close · cash $8,401.45 · equity $14,270.40 vs 09:30 $11,958.57 (+2311.83; session marks +2338.80) · 1 name(s) marked open→close (per-name table). GLND×1097 09:30 $3.22 → close $5.35 +2338.80 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new long overnight_mega_h1 |
| 2026-08-18 | `LOW` | hard_red | hard-red S=-6.20 sit; no new long overnight_mega_h1 |
| 2026-08-18 | `TGT` | hard_red | hard-red S=-6.20 sit; no new long overnight_mega_h1 |
| 2026-08-18 | `TJX` | hard_red | hard-red S=-6.20 sit; no new long overnight_mega_h1 |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_holdup |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_holdup |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_holdup |
| 2026-08-19 | `BABA` | hard_red | hard-red S=-7.20 sit; no new long overnight_mega_h1 |
| 2026-08-19 | `NTES` | hard_red | hard-red S=-7.20 sit; no new long overnight_mega_h1 |
| 2026-08-19 | `WMT` | hard_red | hard-red S=-7.20 sit; no new long overnight_mega_h1 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_holdup |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_holdup |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_holdup |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_holdup |
| 2026-08-24 | `BNS` | hard_red | hard-red S=-5.17 sit; no new long overnight_mega_h1 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_holdup |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_holdup |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_holdup |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_holdup |
| 2026-08-31 | `MDT` | hard_red | hard-red S=-5.85 sit; no new long overnight_mega_h1 |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_holdup |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_holdup |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_holdup |
| 2026-09-01 | `DELL` | hard_red | hard-red S=-6.30 sit; no new long overnight_mega_h1 |
| 2026-09-01 | `PANW` | hard_red | hard-red S=-6.30 sit; no new long overnight_mega_h1 |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_holdup |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_holdup |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_holdup |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_holdup |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new long overnight_mega_h1 |
| 2026-09-02 | `SNOW` | hard_red | hard-red S=-3.83 sit; no new long overnight_mega_h1 |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_holdup |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_holdup |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_holdup |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_holdup |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_holdup |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_holdup |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_holdup |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_holdup |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_holdup |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_holdup |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_holdup |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_holdup |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new long overnight_mega_h1 |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_holdup |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_holdup |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_holdup |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_holdup |
| 2026-09-14 | `GPRO` | hard_red | hard-red S=-11.00 sit; no new long union_hot_n4_holdup |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_holdup |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_holdup |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_holdup |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LVWR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `CRML` | cash | leftover split 3.03 < 1 share @ 9.11 |
| 2026-09-22 | `NUAI` | cash | leftover split 3.03 < 1 share @ 7.23 |
| 2026-09-24 | `COST` | hard_red | hard-red S=-7.66 sit; no new long overnight_mega_h1 |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_holdup |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_holdup |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_holdup |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GLND` | 1097 | 2026-09-23 @ $2.70 | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $2964.38; owner union_hot_n4_holdup |
