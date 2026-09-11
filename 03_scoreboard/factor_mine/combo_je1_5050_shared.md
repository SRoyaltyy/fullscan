# Factor mine action — `combo_je1_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_join_vol_green_h1/union_e_fresh_h1 w=0.5,0.5 net=priority

Cash book **+13.45%** ($11,345) · signal-only (no cash/fees) was —. Starts YES **14/20**. Fills 238 · skips 66 · realized $+1344.85.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_join_vol_green_h1 50%, union_e_fresh_h1 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_join_vol_green_h1 50%, union_e_fresh_h1 50%.
- Member: union_join_vol_green_h1 (50% · long · hold 1).
- Member: union_e_fresh_h1 (50% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,344.88.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `INO` | 6172 | — | $0.81 | +0.00 | $0.90 | +555.48 | +555.48 | +0.00 | +555.48 |
| 2026-08-13 | `VOR` | 223 | — | $22.01 | +0.00 | $23.29 | +285.44 | +285.44 | +0.00 | +285.44 |
| 2026-08-14 | `INO` | 6172 | $0.90 | $0.93 | +185.16 | — | +0.00 | +185.16 | +740.64 | — |
| 2026-08-14 | `VOR` | 223 | $23.29 | $23.33 | +8.92 | — | +0.00 | +8.92 | +294.36 | — |
| 2026-08-14 | `BTBT` | 453 | — | $1.50 | +0.00 | $1.57 | +31.71 | +31.71 | +0.00 | +31.71 |
| 2026-08-14 | `AIRO` | 61 | — | $11.12 | +0.00 | $9.57 | -94.55 | -94.55 | +0.00 | -94.55 |
| 2026-08-14 | `ARX` | 34 | — | $19.57 | +0.00 | $19.58 | +0.34 | +0.34 | +0.00 | +0.34 |
| 2026-08-14 | `MH` | 50 | — | $13.55 | +0.00 | $13.10 | -22.50 | -22.50 | +0.00 | -22.50 |
| 2026-08-14 | `CLBT` | 62 | — | $10.83 | +0.00 | $11.14 | +19.22 | +19.22 | +0.00 | +19.22 |
| 2026-08-14 | `EU` | 576 | — | $1.18 | +0.00 | $1.21 | +17.28 | +17.28 | +0.00 | +17.28 |
| 2026-08-14 | `LUNR` | 35 | — | $19.17 | +0.00 | $19.01 | -5.60 | -5.60 | +0.00 | -5.60 |
| 2026-08-14 | `NMAX` | 68 | — | $9.89 | +0.00 | $10.87 | +66.30 | +66.30 | +0.00 | +66.30 |
| 2026-08-14 | `BETR` | 61 | — | $14.80 | +0.00 | $13.73 | -65.27 | -65.27 | +0.00 | -65.27 |
| 2026-08-14 | `ANGX` | 211 | — | $4.31 | +0.00 | $4.37 | +12.66 | +12.66 | +0.00 | +12.66 |
| 2026-08-14 | `HYLN` | 217 | — | $4.18 | +0.00 | $4.06 | -26.04 | -26.04 | +0.00 | -26.04 |
| 2026-08-14 | `ADUR` | 55 | — | $16.50 | +0.00 | $16.17 | -18.15 | -18.15 | +0.00 | -18.15 |
| 2026-08-14 | `NCMI` | 338 | — | $2.69 | +0.00 | $2.86 | +57.46 | +57.46 | +0.00 | +57.46 |
| 2026-08-14 | `QMLS` | 124 | — | $7.29 | +0.00 | $7.32 | +3.72 | +3.72 | +0.00 | +3.72 |
| 2026-08-17 | `BTBT` | 453 | $1.57 | $1.52 | -22.65 | — | +0.00 | -22.65 | +9.06 | — |
| 2026-08-17 | `AIRO` | 61 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -94.55 | — |
| 2026-08-17 | `ARX` | 34 | $19.58 | $19.57 | -0.34 | — | +0.00 | -0.34 | +0.00 | — |
| 2026-08-17 | `MH` | 50 | $13.10 | $13.16 | +3.00 | — | +0.00 | +3.00 | -19.50 | — |
| 2026-08-17 | `CLBT` | 62 | $11.14 | $11.19 | +3.10 | — | +0.00 | +3.10 | +22.32 | — |
| 2026-08-17 | `EU` | 576 | $1.21 | $1.21 | +0.00 | — | +0.00 | +0.00 | +17.28 | — |
| 2026-08-17 | `LUNR` | 35 | $19.01 | $20.25 | +43.40 | — | +0.00 | +43.40 | +37.80 | — |
| 2026-08-17 | `NMAX` | 68 | $10.87 | $10.97 | +6.80 | — | +0.00 | +6.80 | +73.10 | — |
| 2026-08-17 | `BETR` | 61 | $13.73 | $13.67 | -3.66 | — | +0.00 | -3.66 | -68.93 | — |
| 2026-08-17 | `ANGX` | 211 | $4.37 | $4.60 | +48.53 | — | +0.00 | +48.53 | +61.19 | — |
| 2026-08-17 | `HYLN` | 217 | $4.06 | $4.10 | +8.68 | — | +0.00 | +8.68 | -17.36 | — |
| 2026-08-17 | `ADUR` | 55 | $16.17 | $15.73 | -24.20 | — | +0.00 | -24.20 | -42.35 | — |
| 2026-08-17 | `NCMI` | 338 | $2.86 | $2.80 | -20.28 | — | +0.00 | -20.28 | +37.18 | — |
| 2026-08-17 | `QMLS` | 124 | $7.32 | $7.24 | -9.92 | — | +0.00 | -9.92 | -6.20 | — |
| 2026-08-17 | `ABX` | 236 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `ALOY` | 147 | — | $14.66 | +0.00 | $13.86 | -118.33 | -118.33 | +0.00 | -118.33 |
| 2026-08-17 | `BORR` | 470 | — | $4.59 | +0.00 | $4.50 | -42.30 | -42.30 | +0.00 | -42.30 |
| 2026-08-17 | `XHG` | 515 | — | $4.19 | +0.00 | $3.91 | -144.20 | -144.20 | +0.00 | -144.20 |
| 2026-08-17 | `MP` | 37 | — | $58.01 | +0.00 | $58.51 | +18.50 | +18.50 | +0.00 | +18.50 |
| 2026-08-18 | `ABX` | 236 | $9.12 | $9.03 | -21.24 | — | +0.00 | -21.24 | -21.24 | — |
| 2026-08-18 | `ALOY` | 147 | $13.86 | $13.19 | -97.76 | — | +0.00 | -97.76 | -216.09 | — |
| 2026-08-18 | `BORR` | 470 | $4.50 | $4.56 | +28.20 | — | +0.00 | +28.20 | -14.10 | — |
| 2026-08-18 | `XHG` | 515 | $3.91 | $3.94 | +15.45 | — | +0.00 | +15.45 | -128.75 | — |
| 2026-08-18 | `MP` | 37 | $58.51 | $56.35 | -79.92 | — | +0.00 | -79.92 | -61.42 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `EL` | 6 | — | $97.43 | +0.00 | $96.15 | -7.68 | -7.68 | +0.00 | -7.68 |
| 2026-08-20 | `TOYO` | 145 | — | $4.43 | +0.00 | $4.51 | +12.32 | +12.32 | +0.00 | +12.32 |
| 2026-08-20 | `DVLT` | 2150 | — | $0.30 | +0.00 | $0.32 | +43.00 | +43.00 | +0.00 | +43.00 |
| 2026-08-20 | `AAP` | 13 | — | $46.85 | +0.00 | $42.39 | -57.98 | -57.98 | +0.00 | -57.98 |
| 2026-08-20 | `AEG` | 71 | — | $9.01 | +0.00 | $9.01 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `ALVO` | 165 | — | $3.89 | +0.00 | $4.27 | +62.70 | +62.70 | +0.00 | +62.70 |
| 2026-08-20 | `ATAT` | 18 | — | $34.05 | +0.00 | $34.25 | +3.60 | +3.60 | +0.00 | +3.60 |
| 2026-08-20 | `ATHM` | 28 | — | $22.44 | +0.00 | $22.12 | -8.96 | -8.96 | +0.00 | -8.96 |
| 2026-08-20 | `AG` | 32 | — | $20.55 | +0.00 | $21.19 | +20.48 | +20.48 | +0.00 | +20.48 |
| 2026-08-20 | `CDE` | 32 | — | $20.65 | +0.00 | $21.11 | +14.72 | +14.72 | +0.00 | +14.72 |
| 2026-08-20 | `HDSN` | 114 | — | $5.77 | +0.00 | $5.57 | -22.80 | -22.80 | +0.00 | -22.80 |
| 2026-08-20 | `IAG` | 33 | — | $19.63 | +0.00 | $20.50 | +28.71 | +28.71 | +0.00 | +28.71 |
| 2026-08-20 | `KGC` | 22 | — | $29.63 | +0.00 | $31.43 | +39.60 | +39.60 | +0.00 | +39.60 |
| 2026-08-20 | `NFGC` | 378 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 4 | — | $144.54 | +0.00 | $150.25 | +22.84 | +22.84 | +0.00 | +22.84 |
| 2026-08-20 | `ABUS` | 134 | — | $4.92 | +0.00 | $4.77 | -20.10 | -20.10 | +0.00 | -20.10 |
| 2026-08-21 | `EL` | 6 | $96.15 | $96.75 | +3.60 | — | +0.00 | +3.60 | -4.08 | — |
| 2026-08-21 | `TOYO` | 145 | $4.51 | $4.68 | +23.93 | — | +0.00 | +23.93 | +36.25 | — |
| 2026-08-21 | `DVLT` | 2150 | $0.32 | $0.31 | -21.50 | — | +0.00 | -21.50 | +21.50 | — |
| 2026-08-21 | `AAP` | 13 | $42.39 | $42.41 | +0.26 | $42.58 | +2.21 | +2.47 | -57.72 | -55.51 |
| 2026-08-21 | `AEG` | 71 | $9.01 | $9.04 | +2.13 | — | +0.00 | +2.13 | +2.13 | — |
| 2026-08-21 | `ALVO` | 165 | $4.27 | $4.32 | +8.25 | — | +0.00 | +8.25 | +70.95 | — |
| 2026-08-21 | `ATAT` | 18 | $34.25 | $34.31 | +1.08 | — | +0.00 | +1.08 | +4.68 | — |
| 2026-08-21 | `ATHM` | 28 | $22.12 | $22.20 | +2.24 | — | +0.00 | +2.24 | -6.72 | — |
| 2026-08-21 | `AG` | 32 | $21.19 | $21.90 | +22.72 | — | +0.00 | +22.72 | +43.20 | — |
| 2026-08-21 | `CDE` | 32 | $21.11 | $21.75 | +20.48 | — | +0.00 | +20.48 | +35.20 | — |
| 2026-08-21 | `HDSN` | 114 | $5.57 | $5.67 | +11.40 | — | +0.00 | +11.40 | -11.40 | — |
| 2026-08-21 | `IAG` | 33 | $20.50 | $21.17 | +22.11 | — | +0.00 | +22.11 | +50.82 | — |
| 2026-08-21 | `KGC` | 22 | $31.43 | $32.17 | +16.28 | — | +0.00 | +16.28 | +55.88 | — |
| 2026-08-21 | `NFGC` | 378 | $1.75 | $1.79 | +15.12 | — | +0.00 | +15.12 | +15.12 | — |
| 2026-08-21 | `WPM` | 4 | $150.25 | $154.70 | +17.80 | — | +0.00 | +17.80 | +40.64 | — |
| 2026-08-21 | `ABUS` | 134 | $4.77 | $5.20 | +57.62 | — | +0.00 | +57.62 | +37.52 | — |
| 2026-08-21 | `FUTU` | 6 | — | $115.18 | +0.00 | $123.64 | +50.76 | +50.76 | +0.00 | +50.76 |
| 2026-08-21 | `DE` | 1 | — | $623.26 | +0.00 | $647.47 | +24.21 | +24.21 | +0.00 | +24.21 |
| 2026-08-21 | `WMT` | 6 | — | $103.69 | +0.00 | $103.70 | +0.06 | +0.06 | +0.00 | +0.06 |
| 2026-08-21 | `BEKE` | 39 | — | $17.93 | +0.00 | $17.75 | -7.21 | -7.21 | +0.00 | -7.21 |
| 2026-08-21 | `BJ` | 7 | — | $93.98 | +0.00 | $96.42 | +17.08 | +17.08 | +0.00 | +17.08 |
| 2026-08-21 | `BKE` | 16 | — | $43.08 | +0.00 | $43.81 | +11.68 | +11.68 | +0.00 | +11.68 |
| 2026-08-21 | `PSEC` | 310 | — | $2.30 | +0.00 | $2.33 | +9.30 | +9.30 | +0.00 | +9.30 |
| 2026-08-21 | `AU` | 5 | — | $119.43 | +0.00 | $121.22 | +8.95 | +8.95 | +0.00 | +8.95 |
| 2026-08-21 | `AUPH` | 38 | — | $17.20 | +0.00 | $16.65 | -20.90 | -20.90 | +0.00 | -20.90 |
| 2026-08-21 | `AEM` | 3 | — | $216.30 | +0.00 | $216.06 | -0.72 | -0.72 | +0.00 | -0.72 |
| 2026-08-21 | `ARCT` | 59 | — | $11.13 | +0.00 | $13.45 | +136.88 | +136.88 | +0.00 | +136.88 |
| 2026-08-21 | `CYPH` | 501 | — | $1.32 | +0.00 | $1.42 | +50.10 | +50.10 | +0.00 | +50.10 |
| 2026-08-21 | `BTBT` | 399 | — | $1.66 | +0.00 | $1.53 | -51.87 | -51.87 | +0.00 | -51.87 |
| 2026-08-21 | `INDP` | 476 | — | $1.39 | +0.00 | $1.29 | -47.60 | -47.60 | +0.00 | -47.60 |
| 2026-08-21 | `MRVI` | 80 | — | $8.28 | +0.00 | $8.64 | +28.80 | +28.80 | +0.00 | +28.80 |
| 2026-08-24 | `AAP` | 13 | $42.58 | $43.05 | +6.11 | — | +0.00 | +6.11 | -49.40 | — |
| 2026-08-24 | `FUTU` | 6 | $123.64 | $121.00 | -15.84 | — | +0.00 | -15.84 | +34.92 | — |
| 2026-08-24 | `DE` | 1 | $647.47 | $653.04 | +5.57 | — | +0.00 | +5.57 | +29.78 | — |
| 2026-08-24 | `WMT` | 6 | $103.70 | $104.14 | +2.64 | — | +0.00 | +2.64 | +2.70 | — |
| 2026-08-24 | `BEKE` | 39 | $17.75 | $18.05 | +11.89 | — | +0.00 | +11.89 | +4.68 | — |
| 2026-08-24 | `BJ` | 7 | $96.42 | $97.02 | +4.20 | — | +0.00 | +4.20 | +21.28 | — |
| 2026-08-24 | `BKE` | 16 | $43.81 | $44.22 | +6.56 | — | +0.00 | +6.56 | +18.24 | — |
| 2026-08-24 | `PSEC` | 310 | $2.33 | $2.34 | +3.10 | — | +0.00 | +3.10 | +12.40 | — |
| 2026-08-24 | `AU` | 5 | $121.22 | $120.51 | -3.55 | — | +0.00 | -3.55 | +5.40 | — |
| 2026-08-24 | `AUPH` | 38 | $16.65 | $16.57 | -3.04 | — | +0.00 | -3.04 | -23.94 | — |
| 2026-08-24 | `AEM` | 3 | $216.06 | $217.03 | +2.91 | — | +0.00 | +2.91 | +2.19 | — |
| 2026-08-24 | `ARCT` | 59 | $13.45 | $13.33 | -7.08 | — | +0.00 | -7.08 | +129.80 | — |
| 2026-08-24 | `CYPH` | 501 | $1.42 | $1.83 | +205.41 | — | +0.00 | +205.41 | +255.51 | — |
| 2026-08-24 | `BTBT` | 399 | $1.53 | $1.55 | +7.98 | — | +0.00 | +7.98 | -43.89 | — |
| 2026-08-24 | `INDP` | 476 | $1.29 | $1.24 | -23.80 | — | +0.00 | -23.80 | -71.40 | — |
| 2026-08-24 | `MRVI` | 80 | $8.64 | $8.59 | -4.00 | — | +0.00 | -4.00 | +24.80 | — |
| 2026-08-25 | `BMO` | 3 | — | $175.01 | +0.00 | $173.46 | -4.65 | -4.65 | +0.00 | -4.65 |
| 2026-08-25 | `BNS` | 7 | — | $88.94 | +0.00 | $93.10 | +29.12 | +29.12 | +0.00 | +29.12 |
| 2026-08-25 | `BZ` | 44 | — | $15.28 | +0.00 | $16.29 | +44.44 | +44.44 | +0.00 | +44.44 |
| 2026-08-25 | `DKS` | 4 | — | $142.36 | +0.00 | $124.31 | -72.20 | -72.20 | +0.00 | -72.20 |
| 2026-08-25 | `EH` | 133 | — | $5.10 | +0.00 | $4.83 | -35.91 | -35.91 | +0.00 | -35.91 |
| 2026-08-25 | `GFI` | 14 | — | $47.89 | +0.00 | $48.87 | +13.72 | +13.72 | +0.00 | +13.72 |
| 2026-08-25 | `GRRR` | 48 | — | $13.92 | +0.00 | $14.04 | +5.76 | +5.76 | +0.00 | +5.76 |
| 2026-08-25 | `SHMD` | 149 | — | $4.54 | +0.00 | $3.42 | -167.62 | -167.62 | +0.00 | -167.62 |
| 2026-08-25 | `BMEA` | 443 | — | $1.63 | +0.00 | $1.73 | +44.30 | +44.30 | +0.00 | +44.30 |
| 2026-08-25 | `GORO` | 203 | — | $3.55 | +0.00 | $3.87 | +64.96 | +64.96 | +0.00 | +64.96 |
| 2026-08-25 | `ZURA` | 113 | — | $6.37 | +0.00 | $6.32 | -5.65 | -5.65 | +0.00 | -5.65 |
| 2026-08-25 | `EZPW` | 20 | — | $35.05 | +0.00 | $35.23 | +3.60 | +3.60 | +0.00 | +3.60 |
| 2026-08-25 | `ETON` | 11 | — | $64.55 | +0.00 | $63.05 | -16.50 | -16.50 | +0.00 | -16.50 |
| 2026-08-25 | `WPM` | 4 | — | $156.51 | +0.00 | $163.72 | +28.84 | +28.84 | +0.00 | +28.84 |
| 2026-08-25 | `SUZ` | 80 | — | $8.98 | +0.00 | $9.03 | +4.00 | +4.00 | +0.00 | +4.00 |
| 2026-08-25 | `IAUX` | 380 | — | $1.90 | +0.00 | $1.92 | +7.60 | +7.60 | +0.00 | +7.60 |
| 2026-08-26 | `BMO` | 3 | $173.46 | $173.22 | -0.72 | — | +0.00 | -0.72 | -5.37 | — |
| 2026-08-26 | `BNS` | 7 | $93.10 | $92.65 | -3.15 | — | +0.00 | -3.15 | +25.97 | — |
| 2026-08-26 | `BZ` | 44 | $16.29 | $16.77 | +21.12 | $18.84 | +91.08 | +112.20 | +65.56 | +156.64 |
| 2026-08-26 | `DKS` | 4 | $124.31 | $121.87 | -9.76 | $129.66 | +31.16 | +21.40 | -81.96 | -50.80 |
| 2026-08-26 | `EH` | 133 | $4.83 | $4.77 | -7.98 | — | +0.00 | -7.98 | -43.89 | — |
| 2026-08-26 | `GFI` | 14 | $48.87 | $48.24 | -8.82 | — | +0.00 | -8.82 | +4.90 | — |
| 2026-08-26 | `GRRR` | 48 | $14.04 | $14.03 | -0.48 | — | +0.00 | -0.48 | +5.28 | — |
| 2026-08-26 | `SHMD` | 149 | $3.42 | $3.38 | -5.96 | — | +0.00 | -5.96 | -173.59 | — |
| 2026-08-26 | `BMEA` | 443 | $1.73 | $1.75 | +11.07 | — | +0.00 | +11.07 | +55.38 | — |
| 2026-08-26 | `GORO` | 203 | $3.87 | $3.77 | -20.30 | — | +0.00 | -20.30 | +44.66 | — |
| 2026-08-26 | `ZURA` | 113 | $6.32 | $6.13 | -21.47 | — | +0.00 | -21.47 | -27.12 | — |
| 2026-08-26 | `EZPW` | 20 | $35.23 | $35.70 | +9.40 | — | +0.00 | +9.40 | +13.00 | — |
| 2026-08-26 | `ETON` | 11 | $63.05 | $63.60 | +6.05 | — | +0.00 | +6.05 | -10.45 | — |
| 2026-08-26 | `WPM` | 4 | $163.72 | $160.93 | -11.16 | — | +0.00 | -11.16 | +17.68 | — |
| 2026-08-26 | `SUZ` | 80 | $9.03 | $9.03 | +0.00 | — | +0.00 | +0.00 | +4.00 | — |
| 2026-08-26 | `IAUX` | 380 | $1.92 | $1.87 | -19.00 | — | +0.00 | -19.00 | -11.40 | — |
| 2026-08-26 | `SLQT` | 1352 | — | $0.58 | +0.00 | $0.55 | -44.62 | -44.62 | +0.00 | -44.62 |
| 2026-08-26 | `TIGR` | 151 | — | $5.21 | +0.00 | $5.46 | +37.75 | +37.75 | +0.00 | +37.75 |
| 2026-08-26 | `ANF` | 6 | — | $131.37 | +0.00 | $147.75 | +98.28 | +98.28 | +0.00 | +98.28 |
| 2026-08-26 | `BBWI` | 43 | — | $18.26 | +0.00 | $18.90 | +27.52 | +27.52 | +0.00 | +27.52 |
| 2026-08-26 | `BOX` | 22 | — | $34.30 | +0.00 | $33.39 | -20.02 | -20.02 | +0.00 | -20.02 |
| 2026-08-26 | `DY` | 2 | — | $326.91 | +0.00 | $310.91 | -32.00 | -32.00 | +0.00 | -32.00 |
| 2026-08-26 | `USDE` | 838 | — | $5.81 | +0.00 | $5.98 | +142.46 | +142.46 | +0.00 | +142.46 |
| 2026-08-27 | `BZ` | 44 | $18.84 | $18.50 | -14.96 | — | +0.00 | -14.96 | +141.68 | — |
| 2026-08-27 | `DKS` | 4 | $129.66 | $128.73 | -3.72 | — | +0.00 | -3.72 | -54.52 | — |
| 2026-08-27 | `SLQT` | 1352 | $0.55 | $0.53 | -27.04 | — | +0.00 | -27.04 | -71.66 | — |
| 2026-08-27 | `TIGR` | 151 | $5.46 | $5.49 | +4.53 | — | +0.00 | +4.53 | +42.28 | — |
| 2026-08-27 | `ANF` | 6 | $147.75 | $144.70 | -18.30 | — | +0.00 | -18.30 | +79.98 | — |
| 2026-08-27 | `BBWI` | 43 | $18.90 | $18.69 | -9.03 | — | +0.00 | -9.03 | +18.49 | — |
| 2026-08-27 | `BOX` | 22 | $33.39 | $33.79 | +8.80 | — | +0.00 | +8.80 | -11.22 | — |
| 2026-08-27 | `DY` | 2 | $310.91 | $314.90 | +7.98 | — | +0.00 | +7.98 | -24.02 | — |
| 2026-08-27 | `USDE` | 838 | $5.98 | $6.50 | +435.76 | — | +0.00 | +435.76 | +578.22 | — |
| 2026-08-27 | `NVDA` | 50 | — | $222.86 | +0.00 | $227.98 | +256.00 | +256.00 | +0.00 | +256.00 |
| 2026-08-28 | `NVDA` | 50 | $227.98 | $227.36 | -31.00 | — | +0.00 | -31.00 | +225.00 | — |
| 2026-08-28 | `GAP` | 29 | — | $24.69 | +0.00 | $23.48 | -35.09 | -35.09 | +0.00 | -35.09 |
| 2026-08-28 | `ADSK` | 2 | — | $261.16 | +0.00 | $260.66 | -1.00 | -1.00 | +0.00 | -1.00 |
| 2026-08-28 | `BBAR` | 48 | — | $15.01 | +0.00 | $14.47 | -25.92 | -25.92 | +0.00 | -25.92 |
| 2026-08-28 | `ESTC` | 6 | — | $103.89 | +0.00 | $99.91 | -23.88 | -23.88 | +0.00 | -23.88 |
| 2026-08-28 | `FINV` | 186 | — | $3.88 | +0.00 | $3.40 | -89.28 | -89.28 | +0.00 | -89.28 |
| 2026-08-28 | `FRO` | 16 | — | $44.40 | +0.00 | $44.19 | -3.36 | -3.36 | +0.00 | -3.36 |
| 2026-08-28 | `HAFN` | 86 | — | $8.35 | +0.00 | $8.47 | +10.32 | +10.32 | +0.00 | +10.32 |
| 2026-08-28 | `IREN` | 19 | — | $37.65 | +0.00 | $35.45 | -41.71 | -41.71 | +0.00 | -41.71 |
| 2026-08-28 | `ANF` | 13 | — | $146.07 | +0.00 | $148.42 | +30.55 | +30.55 | +0.00 | +30.55 |
| 2026-08-28 | `NCNO` | 87 | — | $23.30 | +0.00 | $22.99 | -26.97 | -26.97 | +0.00 | -26.97 |
| 2026-08-28 | `TH` | 106 | — | $19.00 | +0.00 | $18.55 | -47.70 | -47.70 | +0.00 | -47.70 |
| 2026-08-31 | `GAP` | 29 | $23.48 | $22.98 | -14.50 | — | +0.00 | -14.50 | -49.59 | — |
| 2026-08-31 | `ADSK` | 2 | $260.66 | $257.71 | -5.90 | — | +0.00 | -5.90 | -6.90 | — |
| 2026-08-31 | `BBAR` | 48 | $14.47 | $14.88 | +19.68 | — | +0.00 | +19.68 | -6.24 | — |
| 2026-08-31 | `ESTC` | 6 | $99.91 | $98.00 | -11.46 | — | +0.00 | -11.46 | -35.34 | — |
| 2026-08-31 | `FINV` | 186 | $3.40 | $3.39 | -1.86 | — | +0.00 | -1.86 | -91.14 | — |
| 2026-08-31 | `FRO` | 16 | $44.19 | $44.85 | +10.56 | — | +0.00 | +10.56 | +7.20 | — |
| 2026-08-31 | `HAFN` | 86 | $8.47 | $8.53 | +5.16 | — | +0.00 | +5.16 | +15.48 | — |
| 2026-08-31 | `IREN` | 19 | $35.45 | $35.81 | +6.84 | — | +0.00 | +6.84 | -34.87 | — |
| 2026-08-31 | `ANF` | 13 | $148.42 | $148.03 | -5.07 | — | +0.00 | -5.07 | +25.48 | — |
| 2026-08-31 | `NCNO` | 87 | $22.99 | $22.66 | -28.71 | — | +0.00 | -28.71 | -55.68 | — |
| 2026-08-31 | `TH` | 106 | $18.55 | $18.12 | -45.05 | — | +0.00 | -45.05 | -92.75 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AI` | 65 | — | $10.74 | +0.00 | $10.90 | +10.08 | +10.08 | +0.00 | +10.08 |
| 2026-09-03 | `AVGO` | 1 | — | $351.74 | +0.00 | $357.16 | +5.42 | +5.42 | +0.00 | +5.42 |
| 2026-09-03 | `CHPT` | 101 | — | $6.90 | +0.00 | $9.08 | +220.18 | +220.18 | +0.00 | +220.18 |
| 2026-09-03 | `CIEN` | 1 | — | $354.49 | +0.00 | $317.46 | -37.03 | -37.03 | +0.00 | -37.03 |
| 2026-09-03 | `CPB` | 31 | — | $22.32 | +0.00 | $22.13 | -5.89 | -5.89 | +0.00 | -5.89 |
| 2026-09-03 | `FIVE` | 2 | — | $257.00 | +0.00 | $239.96 | -34.08 | -34.08 | +0.00 | -34.08 |
| 2026-09-03 | `HPE` | 14 | — | $47.60 | +0.00 | $54.44 | +95.76 | +95.76 | +0.00 | +95.76 |
| 2026-09-03 | `MEI` | 46 | — | $15.09 | +0.00 | $15.32 | +10.58 | +10.58 | +0.00 | +10.58 |
| 2026-09-03 | `RVTY` | 6 | — | $132.45 | +0.00 | $130.63 | -10.92 | -10.92 | +0.00 | -10.92 |
| 2026-09-03 | `ARCT` | 48 | — | $16.77 | +0.00 | $15.56 | -58.08 | -58.08 | +0.00 | -58.08 |
| 2026-09-03 | `CRDL` | 372 | — | $2.18 | +0.00 | $2.16 | -7.44 | -7.44 | +0.00 | -7.44 |
| 2026-09-03 | `MMED` | 34 | — | $23.88 | +0.00 | $23.84 | -1.36 | -1.36 | +0.00 | -1.36 |
| 2026-09-03 | `NVAX` | 77 | — | $10.42 | +0.00 | $10.34 | -6.16 | -6.16 | +0.00 | -6.16 |
| 2026-09-03 | `BMEA` | 420 | — | $1.93 | +0.00 | $1.91 | -8.40 | -8.40 | +0.00 | -8.40 |
| 2026-09-03 | `DUOL` | 5 | — | $161.54 | +0.00 | $158.82 | -13.60 | -13.60 | +0.00 | -13.60 |
| 2026-09-03 | `ALMS` | 78 | — | $10.38 | +0.00 | $11.36 | +76.83 | +76.83 | +0.00 | +76.83 |
| 2026-09-04 | `AI` | 65 | $10.90 | $10.91 | +0.65 | — | +0.00 | +0.65 | +10.73 | — |
| 2026-09-04 | `AVGO` | 1 | $357.16 | $359.70 | +2.54 | — | +0.00 | +2.54 | +7.96 | — |
| 2026-09-04 | `CHPT` | 101 | $9.08 | $9.28 | +20.20 | — | +0.00 | +20.20 | +240.38 | — |
| 2026-09-04 | `CIEN` | 1 | $317.46 | $321.67 | +4.21 | — | +0.00 | +4.21 | -32.82 | — |
| 2026-09-04 | `CPB` | 31 | $22.13 | $22.10 | -0.93 | — | +0.00 | -0.93 | -6.82 | — |
| 2026-09-04 | `FIVE` | 2 | $239.96 | $238.88 | -2.16 | — | +0.00 | -2.16 | -36.24 | — |
| 2026-09-04 | `HPE` | 14 | $54.44 | $53.85 | -8.26 | — | +0.00 | -8.26 | +87.50 | — |
| 2026-09-04 | `MEI` | 46 | $15.32 | $15.34 | +0.92 | — | +0.00 | +0.92 | +11.50 | — |
| 2026-09-04 | `RVTY` | 6 | $130.63 | $130.03 | -3.60 | — | +0.00 | -3.60 | -14.52 | — |
| 2026-09-04 | `ARCT` | 48 | $15.56 | $15.61 | +2.40 | — | +0.00 | +2.40 | -55.68 | — |
| 2026-09-04 | `CRDL` | 372 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -7.44 | — |
| 2026-09-04 | `MMED` | 34 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -1.36 | — |
| 2026-09-04 | `NVAX` | 77 | $10.34 | $10.50 | +12.32 | — | +0.00 | +12.32 | +6.16 | — |
| 2026-09-04 | `BMEA` | 420 | $1.91 | $1.90 | -4.20 | — | +0.00 | -4.20 | -12.60 | — |
| 2026-09-04 | `DUOL` | 5 | $158.82 | $157.46 | -6.80 | — | +0.00 | -6.80 | -20.40 | — |
| 2026-09-04 | `ALMS` | 78 | $11.36 | $11.23 | -10.14 | — | +0.00 | -10.14 | +66.69 | — |
| 2026-09-04 | `AMBA` | 11 | — | $63.18 | +0.00 | $62.89 | -3.19 | -3.19 | +0.00 | -3.19 |
| 2026-09-04 | `ASAN` | 81 | — | $8.74 | +0.00 | $8.81 | +5.67 | +5.67 | +0.00 | +5.67 |
| 2026-09-04 | `DOCU` | 10 | — | $68.52 | +0.00 | $68.41 | -1.10 | -1.10 | +0.00 | -1.10 |
| 2026-09-04 | `DOMO` | 196 | — | $3.62 | +0.00 | $3.88 | +51.94 | +51.94 | +0.00 | +51.94 |
| 2026-09-04 | `GWRE` | 4 | — | $167.55 | +0.00 | $162.42 | -20.52 | -20.52 | +0.00 | -20.52 |
| 2026-09-04 | `IOT` | 15 | — | $44.90 | +0.00 | $40.20 | -70.50 | -70.50 | +0.00 | -70.50 |
| 2026-09-04 | `LULU` | 7 | — | $98.15 | +0.00 | $100.61 | +17.22 | +17.22 | +0.00 | +17.22 |
| 2026-09-04 | `MAMA` | 45 | — | $15.70 | +0.00 | $15.16 | -24.30 | -24.30 | +0.00 | -24.30 |
| 2026-09-04 | `DELL` | 1 | — | $513.78 | +0.00 | $524.14 | +10.36 | +10.36 | +0.00 | +10.36 |
| 2026-09-04 | `TARS` | 8 | — | $82.70 | +0.00 | $90.78 | +64.64 | +64.64 | +0.00 | +64.64 |
| 2026-09-04 | `BRR` | 288 | — | $2.51 | +0.00 | $2.66 | +43.20 | +43.20 | +0.00 | +43.20 |
| 2026-09-04 | `MDB` | 1 | — | $378.34 | +0.00 | $368.74 | -9.60 | -9.60 | +0.00 | -9.60 |
| 2026-09-04 | `ASST` | 28 | — | $25.18 | +0.00 | $27.14 | +54.88 | +54.88 | +0.00 | +54.88 |
| 2026-09-04 | `DFDV` | 125 | — | $5.79 | +0.00 | $5.87 | +10.00 | +10.00 | +0.00 | +10.00 |
| 2026-09-04 | `TDS` | 19 | — | $37.44 | +0.00 | $37.83 | +7.41 | +7.41 | +0.00 | +7.41 |
| 2026-09-04 | `AHCO` | 114 | — | $6.32 | +0.00 | $6.49 | +19.38 | +19.38 | +0.00 | +19.38 |
| 2026-09-08 | `AMBA` | 11 | $62.89 | $63.83 | +10.34 | — | +0.00 | +10.34 | +7.15 | — |
| 2026-09-08 | `ASAN` | 81 | $8.81 | $8.73 | -6.48 | — | +0.00 | -6.48 | -0.81 | — |
| 2026-09-08 | `DOCU` | 10 | $68.41 | $67.05 | -13.60 | — | +0.00 | -13.60 | -14.70 | — |
| 2026-09-08 | `DOMO` | 196 | $3.88 | $3.84 | -7.84 | — | +0.00 | -7.84 | +44.10 | — |
| 2026-09-08 | `GWRE` | 4 | $162.42 | $160.52 | -7.60 | — | +0.00 | -7.60 | -28.12 | — |
| 2026-09-08 | `IOT` | 15 | $40.20 | $39.56 | -9.60 | — | +0.00 | -9.60 | -80.10 | — |
| 2026-09-08 | `LULU` | 7 | $100.61 | $100.58 | -0.21 | — | +0.00 | -0.21 | +17.01 | — |
| 2026-09-08 | `MAMA` | 45 | $15.16 | $15.20 | +1.80 | — | +0.00 | +1.80 | -22.50 | — |
| 2026-09-08 | `DELL` | 1 | $524.14 | $521.15 | -2.99 | — | +0.00 | -2.99 | +7.37 | — |
| 2026-09-08 | `TARS` | 8 | $90.78 | $89.67 | -8.88 | — | +0.00 | -8.88 | +55.76 | — |
| 2026-09-08 | `BRR` | 288 | $2.66 | $2.66 | +0.00 | — | +0.00 | +0.00 | +43.20 | — |
| 2026-09-08 | `MDB` | 1 | $368.74 | $360.75 | -7.99 | — | +0.00 | -7.99 | -17.59 | — |
| 2026-09-08 | `ASST` | 28 | $27.14 | $26.44 | -19.60 | — | +0.00 | -19.60 | +35.28 | — |
| 2026-09-08 | `DFDV` | 125 | $5.87 | $5.81 | -7.50 | — | +0.00 | -7.50 | +2.50 | — |
| 2026-09-08 | `TDS` | 19 | $37.83 | $37.75 | -1.52 | — | +0.00 | -1.52 | +5.89 | — |
| 2026-09-08 | `AHCO` | 114 | $6.49 | $6.48 | -1.14 | — | +0.00 | -1.14 | +18.24 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +840.92 | INO, VOR | — | $21.06 | $10,769.53 | INO×6172, VOR×223 |
| 2026-08-14 | +5.50 | $21.06 | INO×6172, VOR×223 | $10,963.61 | +194.08 | -23.42 | BTBT, AIRO, ARX, MH, CLBT, EU, LUNR, NMAX, BETR, ANGX, HYLN, ADUR, NCMI, QMLS | INO, VOR | $5.36 | $10,817.54 | BTBT×453, AIRO×61, ARX×34, MH×50, CLBT×62, EU×576, LUNR×35, NMAX×68, BETR×61, ANGX×211, HYLN×217, ADUR×55, NCMI×338, QMLS×124 |
| 2026-08-17 | +2.25 | $5.36 | BTBT×453, AIRO×61, ARX×34, MH×50, CLBT×62, EU×576, LUNR×35, NMAX×68, BETR×61, ANGX×211, HYLN×217, ADUR×55, NCMI×338, QMLS×124 | $10,850.00 | +32.46 | -286.33 | ABX, ALOY, BORR, XHG, MP | BTBT, AIRO, ARX, MH, CLBT, EU, LUNR, NMAX, BETR, ANGX, HYLN, ADUR, NCMI, QMLS | $17.60 | $10,500.12 | ABX×236, ALOY×147, BORR×470, XHG×515, MP×37 |
| 2026-08-18 | -6.20 | $17.60 | ABX×236, ALOY×147, BORR×470, XHG×515, MP×37 | $10,344.86 | -155.26 | +0.00 | — | ABX, ALOY, BORR, XHG, MP | $10,324.26 | $10,324.26 | — |
| 2026-08-19 | -7.20 | $10,324.26 | — | $10,324.26 | -0.00 | +0.00 | — | — | $10,324.26 | $10,324.26 | — |
| 2026-08-20 | +1.12 | $10,324.26 | — | $10,324.26 | -0.00 | +130.45 | EL, TOYO, DVLT, AAP, AEG, ALVO, ATAT, ATHM, AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | — | $97.64 | $10,406.62 | EL×6, TOYO×145, DVLT×2150, AAP×13, AEG×71, ALVO×165, ATAT×18, ATHM×28, AG×32, CDE×32, HDSN×114, IAG×33, KGC×22, NFGC×378, WPM×4, ABUS×134 |
| 2026-08-21 | +3.25 | $97.64 | EL×6, TOYO×145, DVLT×2150, AAP×13, AEG×71, ALVO×165, ATAT×18, ATHM×28, AG×32, CDE×32, HDSN×114, IAG×33, KGC×22, NFGC×378, WPM×4, ABUS×134 | $10,610.14 | +203.52 | +211.73 | FUTU, DE, WMT, BEKE, BJ, BKE, PSEC, AU, AUPH, AEM, ARCT, CYPH, BTBT, INDP, MRVI | EL, TOYO, DVLT, AEG, ALVO, ATAT, ATHM, AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | $67.26 | $10,730.42 | AAP×13, FUTU×6, DE×1, WMT×6, BEKE×39, BJ×7, BKE×16, PSEC×310, AU×5, AUPH×38, AEM×3, ARCT×59, CYPH×501, BTBT×399, INDP×476, MRVI×80 |
| 2026-08-24 | -5.17 | $67.26 | AAP×13, FUTU×6, DE×1, WMT×6, BEKE×39, BJ×7, BKE×16, PSEC×310, AU×5, AUPH×38, AEM×3, ARCT×59, CYPH×501, BTBT×399, INDP×476, MRVI×80 | $10,929.48 | +199.06 | +0.00 | — | AAP, FUTU, DE, WMT, BEKE, BJ, BKE, PSEC, AU, AUPH, AEM, ARCT, CYPH, BTBT, INDP, MRVI | $10,882.47 | $10,882.47 | — |
| 2026-08-25 | +1.80 | $10,882.47 | — | $10,882.47 | +0.00 | -56.19 | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD, BMEA, GORO, ZURA, EZPW, ETON, WPM, SUZ, IAUX | — | $117.94 | $10,785.28 | BMO×3, BNS×7, BZ×44, DKS×4, EH×133, GFI×14, GRRR×48, SHMD×149, BMEA×443, GORO×203, ZURA×113, EZPW×20, ETON×11, WPM×4, SUZ×80, IAUX×380 |
| 2026-08-26 | +2.02 | $117.94 | BMO×3, BNS×7, BZ×44, DKS×4, EH×133, GFI×14, GRRR×48, SHMD×149, BMEA×443, GORO×203, ZURA×113, EZPW×20, ETON×11, WPM×4, SUZ×80, IAUX×380 | $10,724.13 | -61.15 | +331.61 | SLQT, TIGR, ANF, BBWI, BOX, DY, USDE | BMO, BNS, EH, GFI, GRRR, SHMD, BMEA, GORO, ZURA, EZPW, ETON, WPM, SUZ, IAUX | $2.54 | $10,985.04 | BZ×44, DKS×4, SLQT×1352, TIGR×151, ANF×6, BBWI×43, BOX×22, DY×2, USDE×838 |
| 2026-08-27 | — | $2.54 | BZ×44, DKS×4, SLQT×1352, TIGR×151, ANF×6, BBWI×43, BOX×22, DY×2, USDE×838 | $11,369.06 | +384.02 | +256.00 | NVDA | BZ, DKS, SLQT, TIGR, ANF, BBWI, BOX, DY, USDE | $186.57 | $11,585.57 | NVDA×50 |
| 2026-08-28 | +0.75 | $186.57 | NVDA×50 | $11,554.57 | -31.00 | -254.04 | GAP, ADSK, BBAR, ESTC, FINV, FRO, HAFN, IREN, ANF, NCNO, TH | NVDA | $141.05 | $11,274.61 | GAP×29, ADSK×2, BBAR×48, ESTC×6, FINV×186, FRO×16, HAFN×86, IREN×19, ANF×13, NCNO×87, TH×106 |
| 2026-08-31 | -5.85 | $141.05 | GAP×29, ADSK×2, BBAR×48, ESTC×6, FINV×186, FRO×16, HAFN×86, IREN×19, ANF×13, NCNO×87, TH×106 | $11,204.30 | -70.31 | +0.00 | — | GAP, ADSK, BBAR, ESTC, FINV, FRO, HAFN, IREN, ANF, NCNO, TH | $11,180.34 | $11,180.34 | — |
| 2026-09-01 | -6.30 | $11,180.34 | — | $11,180.34 | +0.00 | +0.00 | — | — | $11,180.34 | $11,180.34 | — |
| 2026-09-02 | -3.83 | $11,180.34 | — | $11,180.34 | +0.00 | +0.00 | — | — | $11,180.34 | $11,180.34 | — |
| 2026-09-03 | -0.90 | $11,180.34 | — | $11,180.34 | +0.00 | +235.89 | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI, RVTY, ARCT, CRDL, MMED, NVAX, BMEA, DUOL, ALMS | — | $20.29 | $11,376.62 | AI×65, AVGO×1, CHPT×101, CIEN×1, CPB×31, FIVE×2, HPE×14, MEI×46, RVTY×6, ARCT×48, CRDL×372, MMED×34, NVAX×77, BMEA×420, DUOL×5, ALMS×78 |
| 2026-09-04 | +2.25 | $20.29 | AI×65, AVGO×1, CHPT×101, CIEN×1, CPB×31, FIVE×2, HPE×14, MEI×46, RVTY×6, ARCT×48, CRDL×372, MMED×34, NVAX×77, BMEA×420, DUOL×5, ALMS×78 | $11,383.77 | +7.15 | +155.49 | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA, DELL, TARS, BRR, MDB, ASST, DFDV, TDS, AHCO | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI, RVTY, ARCT, CRDL, MMED, NVAX, BMEA, DUOL, ALMS | $637.02 | $11,463.65 | AMBA×11, ASAN×81, DOCU×10, DOMO×196, GWRE×4, IOT×15, LULU×7, MAMA×45, DELL×1, TARS×8, BRR×288, MDB×1, ASST×28, DFDV×125, TDS×19, AHCO×114 |
| 2026-09-08 | -11.47 | $637.02 | AMBA×11, ASAN×81, DOCU×10, DOMO×196, GWRE×4, IOT×15, LULU×7, MAMA×45, DELL×1, TARS×8, BRR×288, MDB×1, ASST×28, DFDV×125, TDS×19, AHCO×114 | $11,380.84 | -82.81 | +0.00 | — | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA, DELL, TARS, BRR, MDB, ASST, DFDV, TDS, AHCO | $11,344.88 | $11,344.88 | — |
| 2026-09-09 | -13.95 | $11,344.88 | — | $11,344.88 | -0.00 | +0.00 | — | — | $11,344.88 | $11,344.88 | — |
| 2026-09-10 | -13.28 | $11,344.88 | — | $11,344.88 | -0.00 | +0.00 | — | — | $11,344.88 | $11,344.88 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 (unchanged overnight, no fees) · equity $10,000.00 vs prior close $10,000.00 (+0.00) | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 6172 | $0.81 | $68.51 | — | $4,932.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; combo leftover $5000.00; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 223 | $22.01 | $2.88 | — | $21.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; combo leftover $5000.00; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.06 | ▲ close $10,769.53 vs 09:30 $10,000.00 (session +840.92) | 16:00 close · cash $21.06 · equity $10,769.53 vs 09:30 $10,000.00 (+769.53; session marks +840.92) · 2 name(s) marked open→close (per-name table). INO×6172 09:30 $0.81 → close $0.90 +555.48; VOR×223 09:30 $22.01 → close $23.29 +285.44 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.06 | ▲ 09:30 equity $10,963.61 vs yday $10,769.53 (+194.08) | 09:30 open · cash $21.06 (unchanged overnight, no fees) · equity $10,963.61 vs prior close $10,769.53 (+194.08) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 6172 | $0.93 | $76.99 | $+595.14 | $5,684.04 | ▲ +595.14 after sell → book $10,886.63; vs 09:30 mark -76.98 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 223 | $23.33 | $2.96 | $+288.53 | $10,883.67 | ▲ +288.53 after sell → book $10,883.67; vs 09:30 mark -2.96 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 453 | $1.50 | $5.84 | — | $10,198.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $680.23; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 61 | $11.12 | $2.17 | — | $9,517.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $680.23; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 34 | $19.57 | $2.09 | — | $8,850.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $680.23; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 50 | $13.55 | $2.14 | — | $8,170.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $680.23; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 62 | $10.83 | $2.18 | — | $7,497.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-30.1; combo leftover $680.23; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 576 | $1.18 | $7.43 | — | $6,809.98 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $680.23; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 35 | $19.17 | $2.10 | — | $6,136.93 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $680.23; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🔴 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 68 | $9.89 | $2.19 | — | $5,461.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; combo leftover $680.23; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 61 | $14.80 | $2.17 | — | $4,556.91 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; combo leftover $910.31; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 211 | $4.31 | $2.72 | — | $3,644.77 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $910.31; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 217 | $4.18 | $2.80 | — | $2,734.91 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; combo leftover $910.31; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 55 | $16.50 | $2.15 | — | $1,825.26 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; combo leftover $910.31; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 338 | $2.69 | $4.36 | — | $911.68 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; combo leftover $910.31; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 124 | $7.29 | $2.36 | — | $5.36 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; combo leftover $910.31; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.36 | ▼ close $10,817.54 vs 09:30 $10,963.61 (session -23.42) | 16:00 close · cash $5.36 · equity $10,817.54 vs 09:30 $10,963.61 (-146.07; session marks -23.42) · 14 name(s) marked open→close (per-name table). BTBT×453 09:30 $1.50 → close $1.57 +31.71; AIRO×61 09:30 $11.12 → close $9.57 -94.55; ARX×34 09:30 $19.57 → close $19.58 +0.34; MH×50 09:30 $13.55 → close $13.10 -22.50; CLBT×62 09:30 $10.83 → close $11.14 +19.22; EU×576 09:30 $1.18 → close $1.21 +17.28; LUNR×35 09:30 $19.17 → close $19.01 -5.60; NMAX×68 09:30 $9.89 → close $10.87 +66.30; BETR×61 09:30 $14.80 → close $13.73 -65.27; ANGX×211 09:30 $4.31 → close $4.37 +12.66; HYLN×217 09:30 $4.18 → close $4.06 -26.04; ADUR×55 09:30 $16.50 → close $16.17 -18.15; NCMI×338 09:30 $2.69 → close $2.86 +57.46; QMLS×124 09:30 $7.29 → close $7.32 +3.72 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.36 | ▲ 09:30 equity $10,850.00 vs yday $10,817.54 (+32.46) | 09:30 open · cash $5.36 (unchanged overnight, no fees) · equity $10,850.00 vs prior close $10,817.54 (+32.46) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 453 | $1.52 | $5.93 | $-2.71 | $687.99 | ▼ -2.71 after sell → book $10,844.07; vs 09:30 mark -5.93 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 61 | $9.57 | $2.19 | $-98.92 | $1,269.57 | ▼ -98.92 after sell → book $10,841.88; vs 09:30 mark -2.19 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 34 | $19.57 | $2.11 | $-4.20 | $1,932.83 | ▼ -4.20 after sell → book $10,839.76; vs 09:30 mark -2.12 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 50 | $13.16 | $2.16 | $-23.80 | $2,588.67 | ▼ -23.80 after sell → book $10,837.60; vs 09:30 mark -2.16 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 62 | $11.19 | $2.20 | $+17.95 | $3,280.26 | ▲ +17.95 after sell → book $10,835.41; vs 09:30 mark -2.19 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `EU` | 576 | $1.21 | $7.54 | $+2.31 | $3,969.68 | ▲ +2.31 after sell → book $10,827.87; vs 09:30 mark -7.54 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 35 | $20.25 | $2.12 | $+33.59 | $4,676.32 | ▲ +33.59 after sell → book $10,825.76; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 68 | $10.97 | $2.22 | $+68.69 | $5,420.06 | ▲ +68.69 after sell → book $10,823.54; vs 09:30 mark -2.22 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 61 | $13.67 | $2.19 | $-73.30 | $6,251.74 | ▼ -73.30 after sell → book $10,821.35; vs 09:30 mark -2.19 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 211 | $4.60 | $2.77 | $+55.70 | $7,219.57 | ▲ +55.70 after sell → book $10,818.58; vs 09:30 mark -2.77 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 217 | $4.10 | $2.85 | $-23.00 | $8,106.43 | ▼ -23.00 after sell → book $10,815.74; vs 09:30 mark -2.84 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 55 | $15.73 | $2.17 | $-46.68 | $8,969.40 | ▼ -46.68 after sell → book $10,813.56; vs 09:30 mark -2.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 338 | $2.80 | $4.43 | $+28.39 | $9,911.37 | ▲ +28.39 after sell → book $10,809.13; vs 09:30 mark -4.43 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 124 | $7.24 | $2.39 | $-10.95 | $10,806.74 | ▼ -10.95 after sell → book $10,806.74; vs 09:30 mark -2.39 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 236 | $9.12 | $3.04 | — | $8,651.38 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; combo leftover $2161.35; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 147 | $14.66 | $2.43 | — | $6,493.93 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; combo leftover $2161.35; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 470 | $4.59 | $6.06 | — | $4,330.56 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; combo leftover $2161.35; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 515 | $4.19 | $6.64 | — | $2,166.07 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ⚪; ret5=+291.8; combo leftover $2161.35; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `MP` | 37 | $58.01 | $2.10 | — | $17.60 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.9; combo leftover $2161.35; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.60 | ▼ close $10,500.12 vs 09:30 $10,850.00 (session -286.33) | 16:00 close · cash $17.60 · equity $10,500.12 vs 09:30 $10,850.00 (-349.88; session marks -286.33) · 5 name(s) marked open→close (per-name table). ABX×236 09:30 $9.12 → close $9.12 +0.00; ALOY×147 09:30 $14.66 → close $13.86 -118.33; BORR×470 09:30 $4.59 → close $4.50 -42.30; XHG×515 09:30 $4.19 → close $3.91 -144.20; MP×37 09:30 $58.01 → close $58.51 +18.50 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.60 | ▼ 09:30 equity $10,344.86 vs yday $10,500.12 (-155.26) | 09:30 open · cash $17.60 (unchanged overnight, no fees) · equity $10,344.86 vs prior close $10,500.12 (-155.26) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 236 | $9.03 | $3.10 | $-27.39 | $2,145.58 | ▼ -27.39 after sell → book $10,341.76; vs 09:30 mark -3.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 147 | $13.19 | $2.47 | $-220.99 | $4,082.04 | ▼ -220.99 after sell → book $10,339.29; vs 09:30 mark -2.47 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 470 | $4.56 | $6.16 | $-26.32 | $6,219.08 | ▼ -26.32 after sell → book $10,333.13; vs 09:30 mark -6.16 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 515 | $3.94 | $6.75 | $-142.14 | $8,241.43 | ▼ -142.14 after sell → book $10,326.38; vs 09:30 mark -6.75 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `MP` | 37 | $56.35 | $2.13 | $-65.65 | $10,324.26 | ▼ -65.65 after sell → book $10,324.26; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,324.26 | ▲ close $10,324.26 vs 09:30 $10,344.86 (session +0.00) | 16:00 close · cash $10,324.26 · no lots left · equity $10,324.26. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,324.26 | ▲ 09:30 equity $10,324.26 vs yday $10,324.26 (-0.00) | 09:30 open · cash $10,324.26 (unchanged overnight, no fees) · equity $10,324.26 vs prior close $10,324.26 (-0.00) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,324.26 | ▲ close $10,324.26 vs 09:30 $10,324.26 (session +0.00) | 16:00 close · cash $10,324.26 · no lots left · equity $10,324.26. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,324.26 | ▲ 09:30 equity $10,324.26 vs yday $10,324.26 (-0.00) | 09:30 open · cash $10,324.26 (unchanged overnight, no fees) · equity $10,324.26 vs prior close $10,324.26 (-0.00) | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 6 | $97.43 | $2.01 | — | $9,737.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $645.27; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 145 | $4.43 | $2.42 | — | $9,092.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $645.27; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 2150 | $0.30 | $12.90 | — | $8,434.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $645.27; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 13 | $46.85 | $2.03 | — | $7,823.91 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $645.27; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 71 | $9.01 | $2.20 | — | $7,182.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $645.27; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 165 | $3.89 | $2.48 | — | $6,537.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $645.27; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 18 | $34.05 | $2.04 | — | $5,922.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $645.27; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 28 | $22.44 | $2.07 | — | $5,292.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $645.27; owner union_e_fresh_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 32 | $20.55 | $2.09 | — | $4,632.64 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; combo leftover $661.54; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 32 | $20.65 | $2.09 | — | $3,969.76 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; combo leftover $661.54; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 114 | $5.77 | $2.33 | — | $3,309.64 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; combo leftover $661.54; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 33 | $19.63 | $2.09 | — | $2,659.76 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; combo leftover $661.54; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 22 | $29.63 | $2.06 | — | $2,005.85 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; combo leftover $661.54; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 378 | $1.75 | $4.88 | — | $1,339.47 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; combo leftover $661.54; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $759.31 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; combo leftover $661.54; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 134 | $4.92 | $2.39 | — | $97.64 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $661.54; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.64 | ▲ close $10,406.62 vs 09:30 $10,324.26 (session +130.45) | 16:00 close · cash $97.64 · equity $10,406.62 vs 09:30 $10,324.26 (+82.36; session marks +130.45) · 16 name(s) marked open→close (per-name table). EL×6 09:30 $97.43 → close $96.15 -7.68; TOYO×145 09:30 $4.43 → close $4.51 +12.32; DVLT×2150 09:30 $0.30 → close $0.32 +43.00; AAP×13 09:30 $46.85 → close $42.39 -57.98; AEG×71 09:30 $9.01 → close $9.01 +0.00; ALVO×165 09:30 $3.89 → close $4.27 +62.70; ATAT×18 09:30 $34.05 → close $34.25 +3.60; ATHM×28 09:30 $22.44 → close $22.12 -8.96; AG×32 09:30 $20.55 → close $21.19 +20.48; CDE×32 09:30 $20.65 → close $21.11 +14.72; HDSN×114 09:30 $5.77 → close $5.57 -22.80; IAG×33 09:30 $19.63 → close $20.50 +28.71; KGC×22 09:30 $29.63 → close $31.43 +39.60; NFGC×378 09:30 $1.75 → close $1.75 +0.00; WPM×4 09:30 $144.54 → close $150.25 +22.84; ABUS×134 09:30 $4.92 → close $4.77 -20.10 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.64 | ▲ 09:30 equity $10,610.14 vs yday $10,406.62 (+203.52) | 09:30 open · cash $97.64 (unchanged overnight, no fees) · equity $10,610.14 vs prior close $10,406.62 (+203.52) | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 6 | $96.75 | $2.03 | $-8.12 | $676.11 | ▼ -8.12 after sell → book $10,608.11; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 145 | $4.68 | $2.46 | $+31.37 | $1,352.25 | ▲ +31.37 after sell → book $10,605.65; vs 09:30 mark -2.46 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 2150 | $0.31 | $13.48 | $-4.88 | $2,005.27 | ▼ -4.88 after sell → book $10,592.17; vs 09:30 mark -13.48 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 71 | $9.04 | $2.22 | $-2.30 | $2,644.88 | ▼ -2.30 after sell → book $10,589.94; vs 09:30 mark -2.23 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALVO` | 165 | $4.32 | $2.52 | $+65.94 | $3,355.16 | ▲ +65.94 after sell → book $10,587.42; vs 09:30 mark -2.52 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 18 | $34.31 | $2.06 | $+0.57 | $3,970.68 | ▲ +0.57 after sell → book $10,585.36; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 28 | $22.20 | $2.09 | $-10.89 | $4,590.18 | ▼ -10.89 after sell → book $10,583.26; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 32 | $21.90 | $2.11 | $+39.01 | $5,288.88 | ▲ +39.01 after sell → book $10,581.16; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 32 | $21.75 | $2.11 | $+31.01 | $5,982.77 | ▲ +31.01 after sell → book $10,579.05; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 114 | $5.67 | $2.36 | $-16.09 | $6,626.79 | ▼ -16.09 after sell → book $10,576.69; vs 09:30 mark -2.36 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 33 | $21.17 | $2.11 | $+46.62 | $7,323.29 | ▲ +46.62 after sell → book $10,574.58; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 22 | $32.17 | $2.08 | $+51.75 | $8,028.96 | ▲ +51.75 after sell → book $10,572.51; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 378 | $1.79 | $4.95 | $+5.29 | $8,700.63 | ▲ +5.29 after sell → book $10,567.56; vs 09:30 mark -4.95 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 4 | $154.70 | $2.02 | $+36.62 | $9,317.41 | ▲ +36.62 after sell → book $10,565.54; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 134 | $5.20 | $2.42 | $+32.70 | $10,011.78 | ▲ +32.70 after sell → book $10,563.11; vs 09:30 mark -2.43 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 6 | $115.18 | $2.01 | — | $9,318.69 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $715.13; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $8,693.44 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list probable,yday_gainer; 🔵; ret5=+1.4; combo leftover $715.13; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 6 | $103.69 | $2.01 | — | $8,069.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; ret5=-10.3; combo leftover $715.13; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 39 | $17.93 | $2.11 | — | $7,367.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $715.13; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 7 | $93.98 | $2.01 | — | $6,707.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; combo leftover $715.13; owner union_e_fresh_h1 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 16 | $43.08 | $2.04 | — | $6,016.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $715.13; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 310 | $2.30 | $4.00 | — | $5,299.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $715.13; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 5 | $119.43 | $2.00 | — | $4,700.38 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $662.44; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 38 | $17.20 | $2.10 | — | $4,044.67 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; combo leftover $662.44; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 3 | $216.30 | $2.00 | — | $3,393.77 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; combo leftover $662.44; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 59 | $11.13 | $2.17 | — | $2,734.94 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; combo leftover $662.44; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 501 | $1.32 | $6.46 | — | $2,067.15 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; combo leftover $662.44; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 399 | $1.66 | $5.15 | — | $1,399.67 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; combo leftover $662.44; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 476 | $1.39 | $6.14 | — | $731.89 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; combo leftover $662.44; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 80 | $8.28 | $2.23 | — | $67.26 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; combo leftover $662.44; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.26 | ▲ close $10,730.42 vs 09:30 $10,610.14 (session +211.73) | 16:00 close · cash $67.26 · equity $10,730.42 vs 09:30 $10,610.14 (+120.28; session marks +211.73) · 16 name(s) marked open→close (per-name table). AAP×13 09:30 $42.41 → close $42.58 +2.21; FUTU×6 09:30 $115.18 → close $123.64 +50.76; DE×1 09:30 $623.26 → close $647.47 +24.21; WMT×6 09:30 $103.69 → close $103.70 +0.06; BEKE×39 09:30 $17.93 → close $17.75 -7.21; BJ×7 09:30 $93.98 → close $96.42 +17.08; BKE×16 09:30 $43.08 → close $43.81 +11.68; PSEC×310 09:30 $2.30 → close $2.33 +9.30; AU×5 09:30 $119.43 → close $121.22 +8.95; AUPH×38 09:30 $17.20 → close $16.65 -20.90; AEM×3 09:30 $216.30 → close $216.06 -0.72; ARCT×59 09:30 $11.13 → close $13.45 +136.88; CYPH×501 09:30 $1.32 → close $1.42 +50.10; BTBT×399 09:30 $1.66 → close $1.53 -51.87; INDP×476 09:30 $1.39 → close $1.29 -47.60; MRVI×80 09:30 $8.28 → close $8.64 +28.80 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.26 | ▲ 09:30 equity $10,929.48 vs yday $10,730.42 (+199.06) | 09:30 open · cash $67.26 (unchanged overnight, no fees) · equity $10,929.48 vs prior close $10,730.42 (+199.06) | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 13 | $43.05 | $2.05 | $-53.48 | $624.86 | ▼ -53.48 after sell → book $10,927.43; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 6 | $121.00 | $2.03 | $+30.88 | $1,348.83 | ▲ +30.88 after sell → book $10,925.41; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 1 | $653.04 | $2.01 | $+25.77 | $1,999.86 | ▲ +25.77 after sell → book $10,923.39; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 6 | $104.14 | $2.03 | $-1.34 | $2,622.67 | ▼ -1.34 after sell → book $10,921.36; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 39 | $18.05 | $2.13 | $+0.45 | $3,324.69 | ▲ +0.45 after sell → book $10,919.24; vs 09:30 mark -2.12 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 7 | $97.02 | $2.03 | $+17.24 | $4,001.80 | ▲ +17.24 after sell → book $10,917.21; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 16 | $44.22 | $2.06 | $+14.14 | $4,707.26 | ▲ +14.14 after sell → book $10,915.15; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 310 | $2.34 | $4.06 | $+4.34 | $5,428.60 | ▲ +4.34 after sell → book $10,911.09; vs 09:30 mark -4.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 5 | $120.51 | $2.02 | $+1.37 | $6,029.12 | ▲ +1.37 after sell → book $10,909.06; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 38 | $16.57 | $2.12 | $-28.17 | $6,656.66 | ▼ -28.17 after sell → book $10,906.94; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 3 | $217.03 | $2.02 | $-1.83 | $7,305.73 | ▼ -1.83 after sell → book $10,904.92; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 59 | $13.33 | $2.19 | $+125.45 | $8,090.01 | ▲ +125.45 after sell → book $10,902.73; vs 09:30 mark -2.19 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 501 | $1.83 | $6.56 | $+242.49 | $9,000.29 | ▲ +242.49 after sell → book $10,896.18; vs 09:30 mark -6.55 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 399 | $1.55 | $5.22 | $-54.26 | $9,613.51 | ▼ -54.26 after sell → book $10,890.95; vs 09:30 mark -5.23 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 476 | $1.24 | $6.23 | $-83.77 | $10,197.52 | ▼ -83.77 after sell → book $10,884.72; vs 09:30 mark -6.23 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 80 | $8.59 | $2.25 | $+20.32 | $10,882.47 | ▲ +20.32 after sell → book $10,882.47; vs 09:30 mark -2.25 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,882.47 | ▲ close $10,882.47 vs 09:30 $10,929.48 (session +0.00) | 16:00 close · cash $10,882.47 · no lots left · equity $10,882.47. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,882.47 | ▲ 09:30 equity $10,882.47 vs yday $10,882.47 (+0.00) | 09:30 open · cash $10,882.47 (unchanged overnight, no fees) · equity $10,882.47 vs prior close $10,882.47 (+0.00) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 3 | $175.01 | $2.00 | — | $10,355.44 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $680.15; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 7 | $88.94 | $2.01 | — | $9,730.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $680.15; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 44 | $15.28 | $2.12 | — | $9,056.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $680.15; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 4 | $142.36 | $2.00 | — | $8,484.97 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $680.15; owner union_e_fresh_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 133 | $5.10 | $2.39 | — | $7,804.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $680.15; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 14 | $47.89 | $2.03 | — | $7,131.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $680.15; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 48 | $13.92 | $2.13 | — | $6,461.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $680.15; owner union_e_fresh_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 149 | $4.54 | $2.44 | — | $5,781.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $680.15; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 443 | $1.63 | $5.71 | — | $5,054.04 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; combo leftover $722.73; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 203 | $3.55 | $2.62 | — | $4,330.78 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+27.9; combo leftover $722.73; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 113 | $6.37 | $2.33 | — | $3,608.64 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; combo leftover $722.73; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 20 | $35.05 | $2.05 | — | $2,905.59 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $722.73; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 11 | $64.55 | $2.02 | — | $2,193.51 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+4.4; combo leftover $722.73; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 4 | $156.51 | $2.00 | — | $1,565.47 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+17.4; combo leftover $722.73; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 80 | $8.98 | $2.23 | — | $844.84 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; combo leftover $722.73; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `IAUX` | 380 | $1.90 | $4.90 | — | $117.94 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.4; combo leftover $722.73; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $117.94 | ▼ close $10,785.28 vs 09:30 $10,882.47 (session -56.19) | 16:00 close · cash $117.94 · equity $10,785.28 vs 09:30 $10,882.47 (-97.19; session marks -56.19) · 16 name(s) marked open→close (per-name table). BMO×3 09:30 $175.01 → close $173.46 -4.65; BNS×7 09:30 $88.94 → close $93.10 +29.12; BZ×44 09:30 $15.28 → close $16.29 +44.44; DKS×4 09:30 $142.36 → close $124.31 -72.20; EH×133 09:30 $5.10 → close $4.83 -35.91; GFI×14 09:30 $47.89 → close $48.87 +13.72; GRRR×48 09:30 $13.92 → close $14.04 +5.76; SHMD×149 09:30 $4.54 → close $3.42 -167.62; BMEA×443 09:30 $1.63 → close $1.73 +44.30; GORO×203 09:30 $3.55 → close $3.87 +64.96; ZURA×113 09:30 $6.37 → close $6.32 -5.65; EZPW×20 09:30 $35.05 → close $35.23 +3.60; ETON×11 09:30 $64.55 → close $63.05 -16.50; WPM×4 09:30 $156.51 → close $163.72 +28.84; SUZ×80 09:30 $8.98 → close $9.03 +4.00; IAUX×380 09:30 $1.90 → close $1.92 +7.60 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $117.94 | ▼ 09:30 equity $10,724.13 vs yday $10,785.28 (-61.15) | 09:30 open · cash $117.94 (unchanged overnight, no fees) · equity $10,724.13 vs prior close $10,785.28 (-61.15) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMO` | 3 | $173.22 | $2.02 | $-9.39 | $635.58 | ▼ -9.39 after sell → book $10,722.11; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BNS` | 7 | $92.65 | $2.03 | $+21.93 | $1,282.10 | ▲ +21.93 after sell → book $10,720.08; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EH` | 133 | $4.77 | $2.42 | $-48.70 | $1,914.09 | ▼ -48.70 after sell → book $10,717.65; vs 09:30 mark -2.43 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GFI` | 14 | $48.24 | $2.05 | $+0.82 | $2,587.40 | ▲ +0.82 after sell → book $10,715.60; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 48 | $14.03 | $2.15 | $+0.99 | $3,258.68 | ▲ +0.99 after sell → book $10,713.45; vs 09:30 mark -2.15 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `SHMD` | 149 | $3.38 | $2.47 | $-178.49 | $3,759.83 | ▼ -178.49 after sell → book $10,710.98; vs 09:30 mark -2.47 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 443 | $1.75 | $5.80 | $+43.86 | $4,531.50 | ▲ +43.86 after sell → book $10,705.18; vs 09:30 mark -5.80 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 203 | $3.77 | $2.66 | $+39.38 | $5,294.15 | ▲ +39.38 after sell → book $10,702.52; vs 09:30 mark -2.66 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 113 | $6.13 | $2.36 | $-31.81 | $5,984.48 | ▼ -31.81 after sell → book $10,700.16; vs 09:30 mark -2.36 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 20 | $35.70 | $2.07 | $+8.88 | $6,696.41 | ▲ +8.88 after sell → book $10,698.09; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 11 | $63.60 | $2.04 | $-14.52 | $7,393.96 | ▼ -14.52 after sell → book $10,696.04; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 4 | $160.93 | $2.02 | $+13.66 | $8,035.66 | ▲ +13.66 after sell → book $10,694.02; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 80 | $9.03 | $2.25 | $-0.48 | $8,755.81 | ▼ -0.48 after sell → book $10,691.77; vs 09:30 mark -2.25 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `IAUX` | 380 | $1.87 | $4.98 | $-21.28 | $9,461.43 | ▼ -21.28 after sell → book $10,686.79; vs 09:30 mark -4.98 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 1352 | $0.58 | $11.94 | — | $8,661.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $788.45; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 151 | $5.21 | $2.44 | — | $7,872.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $788.45; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 6 | $131.37 | $2.01 | — | $7,081.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; combo leftover $788.45; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 43 | $18.26 | $2.12 | — | $6,294.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $788.45; owner union_e_fresh_h1 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 22 | $34.30 | $2.06 | — | $5,537.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $788.45; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 2 | $326.91 | $2.00 | — | $4,882.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-15.2; combo leftover $788.45; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 838 | $5.81 | $10.81 | — | $2.54 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $4882.13; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.54 | ▲ close $10,985.04 vs 09:30 $10,724.13 (session +331.61) | 16:00 close · cash $2.54 · equity $10,985.04 vs 09:30 $10,724.13 (+260.91; session marks +331.61) · 9 name(s) marked open→close (per-name table). BZ×44 09:30 $16.77 → close $18.84 +91.08; DKS×4 09:30 $121.87 → close $129.66 +31.16; SLQT×1352 09:30 $0.58 → close $0.55 -44.62; TIGR×151 09:30 $5.21 → close $5.46 +37.75; ANF×6 09:30 $131.37 → close $147.75 +98.28; BBWI×43 09:30 $18.26 → close $18.90 +27.52; BOX×22 09:30 $34.30 → close $33.39 -20.02; DY×2 09:30 $326.91 → close $310.91 -32.00; USDE×838 09:30 $5.81 → close $5.98 +142.46 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.54 | ▲ 09:30 equity $11,369.06 vs yday $10,985.04 (+384.02) | 09:30 open · cash $2.54 (unchanged overnight, no fees) · equity $11,369.06 vs prior close $10,985.04 (+384.02) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 44 | $18.50 | $2.14 | $+137.42 | $814.40 | ▲ +137.42 after sell → book $11,366.92; vs 09:30 mark -2.14 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 4 | $128.73 | $2.02 | $-58.54 | $1,327.29 | ▼ -58.54 after sell → book $11,364.89; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 1352 | $0.53 | $11.46 | $-95.05 | $2,032.40 | ▼ -95.05 after sell → book $11,353.44; vs 09:30 mark -11.45 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 151 | $5.49 | $2.48 | $+37.36 | $2,858.91 | ▲ +37.36 after sell → book $11,350.96; vs 09:30 mark -2.48 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ANF` | 6 | $144.70 | $2.03 | $+75.94 | $3,725.08 | ▲ +75.94 after sell → book $11,348.93; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBWI` | 43 | $18.69 | $2.14 | $+14.23 | $4,526.61 | ▲ +14.23 after sell → book $11,346.79; vs 09:30 mark -2.14 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BOX` | 22 | $33.79 | $2.08 | $-15.35 | $5,267.92 | ▼ -15.35 after sell → book $11,344.72; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DY` | 2 | $314.90 | $2.02 | $-28.03 | $5,895.70 | ▼ -28.03 after sell → book $11,342.70; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 838 | $6.50 | $10.99 | $+556.42 | $11,331.71 | ▲ +556.42 after sell → book $11,331.71; vs 09:30 mark -10.99 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 50 | $222.86 | $2.14 | — | $186.57 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list mover_buy; 🔵; ret5=-3.6; combo leftover $11331.71; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $186.57 | ▲ close $11,585.57 vs 09:30 $11,369.06 (session +256.00) | 16:00 close · cash $186.57 · equity $11,585.57 vs 09:30 $11,369.06 (+216.51; session marks +256.00) · 1 name(s) marked open→close (per-name table). NVDA×50 09:30 $222.86 → close $227.98 +256.00 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.57 | ▼ 09:30 equity $11,554.57 vs yday $11,585.57 (-31.00) | 09:30 open · cash $186.57 (unchanged overnight, no fees) · equity $11,554.57 vs prior close $11,585.57 (-31.00) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 50 | $227.36 | $2.24 | $+220.62 | $11,552.33 | ▲ +220.62 after sell → book $11,552.33; vs 09:30 mark -2.24 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 29 | $24.69 | $2.08 | — | $10,834.24 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $722.02; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 2 | $261.16 | $2.00 | — | $10,309.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $722.02; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 48 | $15.01 | $2.13 | — | $9,587.31 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $722.02; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 6 | $103.89 | $2.01 | — | $8,961.96 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $722.02; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 186 | $3.88 | $2.55 | — | $8,237.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $722.02; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 16 | $44.40 | $2.04 | — | $7,525.30 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $722.02; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 86 | $8.35 | $2.25 | — | $6,804.95 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $722.02; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 19 | $37.65 | $2.05 | — | $6,087.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $722.02; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 13 | $146.07 | $2.03 | — | $4,186.71 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $2029.22; owner union_join_vol_green_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 87 | $23.30 | $2.25 | — | $2,157.36 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+14.5; combo leftover $2029.22; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 106 | $19.00 | $2.31 | — | $141.05 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+7.5; combo leftover $2029.22; owner union_join_vol_green_h1 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $141.05 | ▼ close $11,274.61 vs 09:30 $11,554.57 (session -254.04) | 16:00 close · cash $141.05 · equity $11,274.61 vs 09:30 $11,554.57 (-279.96; session marks -254.04) · 11 name(s) marked open→close (per-name table). GAP×29 09:30 $24.69 → close $23.48 -35.09; ADSK×2 09:30 $261.16 → close $260.66 -1.00; BBAR×48 09:30 $15.01 → close $14.47 -25.92; ESTC×6 09:30 $103.89 → close $99.91 -23.88; FINV×186 09:30 $3.88 → close $3.40 -89.28; FRO×16 09:30 $44.40 → close $44.19 -3.36; HAFN×86 09:30 $8.35 → close $8.47 +10.32; IREN×19 09:30 $37.65 → close $35.45 -41.71; ANF×13 09:30 $146.07 → close $148.42 +30.55; NCNO×87 09:30 $23.30 → close $22.99 -26.97; TH×106 09:30 $19.00 → close $18.55 -47.70 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $141.05 | ▼ 09:30 equity $11,204.30 vs yday $11,274.61 (-70.31) | 09:30 open · cash $141.05 (unchanged overnight, no fees) · equity $11,204.30 vs prior close $11,274.61 (-70.31) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 29 | $22.98 | $2.10 | $-53.76 | $805.37 | ▼ -53.76 after sell → book $11,202.20; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 2 | $257.71 | $2.02 | $-10.91 | $1,318.78 | ▼ -10.91 after sell → book $11,200.19; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 48 | $14.88 | $2.15 | $-10.53 | $2,030.86 | ▼ -10.53 after sell → book $11,198.03; vs 09:30 mark -2.16 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 6 | $98.00 | $2.03 | $-39.38 | $2,616.83 | ▼ -39.38 after sell → book $11,196.00; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 186 | $3.39 | $2.59 | $-96.28 | $3,244.78 | ▼ -96.28 after sell → book $11,193.41; vs 09:30 mark -2.59 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 16 | $44.85 | $2.06 | $+3.10 | $3,960.33 | ▲ +3.10 after sell → book $11,191.36; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 86 | $8.53 | $2.27 | $+10.96 | $4,691.63 | ▲ +10.96 after sell → book $11,189.08; vs 09:30 mark -2.28 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 19 | $35.81 | $2.07 | $-38.98 | $5,369.96 | ▼ -38.98 after sell → book $11,187.02; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 13 | $148.03 | $2.05 | $+21.40 | $7,292.29 | ▲ +21.40 after sell → book $11,184.96; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 87 | $22.66 | $2.28 | $-60.21 | $9,261.43 | ▼ -60.21 after sell → book $11,182.68; vs 09:30 mark -2.28 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 106 | $18.12 | $2.34 | $-97.40 | $11,180.34 | ▼ -97.40 after sell → book $11,180.34; vs 09:30 mark -2.34 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,180.34 | ▲ close $11,180.34 vs 09:30 $11,204.30 (session +0.00) | 16:00 close · cash $11,180.34 · no lots left · equity $11,180.34. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,180.34 | ▲ 09:30 equity $11,180.34 vs yday $11,180.34 (+0.00) | 09:30 open · cash $11,180.34 (unchanged overnight, no fees) · equity $11,180.34 vs prior close $11,180.34 (+0.00) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,180.34 | ▲ close $11,180.34 vs 09:30 $11,180.34 (session +0.00) | 16:00 close · cash $11,180.34 · no lots left · equity $11,180.34. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,180.34 | ▲ 09:30 equity $11,180.34 vs yday $11,180.34 (+0.00) | 09:30 open · cash $11,180.34 (unchanged overnight, no fees) · equity $11,180.34 vs prior close $11,180.34 (+0.00) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,180.34 | ▲ close $11,180.34 vs 09:30 $11,180.34 (session +0.00) | 16:00 close · cash $11,180.34 · no lots left · equity $11,180.34. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,180.34 | ▲ 09:30 equity $11,180.34 vs yday $11,180.34 (+0.00) | 09:30 open · cash $11,180.34 (unchanged overnight, no fees) · equity $11,180.34 vs prior close $11,180.34 (+0.00) | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 65 | $10.74 | $2.19 | — | $10,479.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $698.77; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $10,126.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $698.77; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 101 | $6.90 | $2.29 | — | $9,426.80 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $698.77; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $9,070.32 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $698.77; owner union_e_fresh_h1 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 31 | $22.32 | $2.08 | — | $8,376.32 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $698.77; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 2 | $257.00 | $2.00 | — | $7,860.32 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $698.77; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 14 | $47.60 | $2.03 | — | $7,191.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $698.77; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 46 | $15.09 | $2.13 | — | $6,495.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $698.77; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 6 | $132.45 | $2.01 | — | $5,698.91 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; combo leftover $811.95; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 48 | $16.77 | $2.13 | — | $4,891.82 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; combo leftover $811.95; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 372 | $2.18 | $4.80 | — | $4,076.06 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; combo leftover $811.95; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 34 | $23.88 | $2.09 | — | $3,262.05 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $811.95; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 77 | $10.42 | $2.22 | — | $2,457.49 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; combo leftover $811.95; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 420 | $1.93 | $5.42 | — | $1,641.47 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $811.95; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 5 | $161.54 | $2.00 | — | $831.77 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+12.0; combo leftover $811.95; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 78 | $10.38 | $2.22 | — | $20.29 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-56.2; combo leftover $811.95; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.29 | ▲ close $11,376.62 vs 09:30 $11,180.34 (session +235.89) | 16:00 close · cash $20.29 · equity $11,376.62 vs 09:30 $11,180.34 (+196.28; session marks +235.89) · 16 name(s) marked open→close (per-name table). AI×65 09:30 $10.74 → close $10.90 +10.08; AVGO×1 09:30 $351.74 → close $357.16 +5.42; CHPT×101 09:30 $6.90 → close $9.08 +220.18; CIEN×1 09:30 $354.49 → close $317.46 -37.03; CPB×31 09:30 $22.32 → close $22.13 -5.89; FIVE×2 09:30 $257.00 → close $239.96 -34.08; HPE×14 09:30 $47.60 → close $54.44 +95.76; MEI×46 09:30 $15.09 → close $15.32 +10.58; RVTY×6 09:30 $132.45 → close $130.63 -10.92; ARCT×48 09:30 $16.77 → close $15.56 -58.08; CRDL×372 09:30 $2.18 → close $2.16 -7.44; MMED×34 09:30 $23.88 → close $23.84 -1.36; NVAX×77 09:30 $10.42 → close $10.34 -6.16; BMEA×420 09:30 $1.93 → close $1.91 -8.40; DUOL×5 09:30 $161.54 → close $158.82 -13.60; ALMS×78 09:30 $10.38 → close $11.36 +76.83 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.29 | ▲ 09:30 equity $11,383.77 vs yday $11,376.62 (+7.15) | 09:30 open · cash $20.29 (unchanged overnight, no fees) · equity $11,383.77 vs prior close $11,376.62 (+7.15) | — |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 65 | $10.91 | $2.21 | $+6.33 | $727.24 | ▲ +6.33 after sell → book $11,381.57; vs 09:30 mark -2.20 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 1 | $359.70 | $2.01 | $+3.95 | $1,084.92 | ▲ +3.95 after sell → book $11,379.55; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 101 | $9.28 | $2.32 | $+235.77 | $2,019.88 | ▲ +235.77 after sell → book $11,377.23; vs 09:30 mark -2.32 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 1 | $321.67 | $2.01 | $-36.83 | $2,339.54 | ▼ -36.83 after sell → book $11,375.22; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CPB` | 31 | $22.10 | $2.10 | $-11.01 | $3,022.54 | ▼ -11.01 after sell → book $11,373.12; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 2 | $238.88 | $2.02 | $-40.25 | $3,498.28 | ▼ -40.25 after sell → book $11,371.10; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 14 | $53.85 | $2.05 | $+83.42 | $4,250.13 | ▲ +83.42 after sell → book $11,369.05; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 46 | $15.34 | $2.15 | $+7.22 | $4,953.62 | ▲ +7.22 after sell → book $11,366.90; vs 09:30 mark -2.15 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 6 | $130.03 | $2.03 | $-18.56 | $5,731.77 | ▼ -18.56 after sell → book $11,364.87; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 48 | $15.61 | $2.15 | $-59.97 | $6,478.90 | ▼ -59.97 after sell → book $11,362.72; vs 09:30 mark -2.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 372 | $2.16 | $4.87 | $-17.11 | $7,277.55 | ▼ -17.11 after sell → book $11,357.85; vs 09:30 mark -4.87 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 34 | $23.84 | $2.11 | $-5.56 | $8,086.00 | ▼ -5.56 after sell → book $11,355.74; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 77 | $10.50 | $2.24 | $+1.70 | $8,892.25 | ▲ +1.70 after sell → book $11,353.49; vs 09:30 mark -2.25 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 420 | $1.90 | $5.50 | $-23.52 | $9,684.75 | ▼ -23.52 after sell → book $11,347.99; vs 09:30 mark -5.50 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 5 | $157.46 | $2.02 | $-24.43 | $10,470.03 | ▼ -24.43 after sell → book $11,345.97; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 78 | $11.23 | $2.25 | $+62.22 | $11,343.72 | ▲ +62.22 after sell → book $11,343.72; vs 09:30 mark -2.25 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 11 | $63.18 | $2.02 | — | $10,646.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $708.98; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 81 | $8.74 | $2.23 | — | $9,936.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $708.98; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 10 | $68.52 | $2.02 | — | $9,249.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $708.98; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 196 | $3.62 | $2.58 | — | $8,538.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $708.98; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 4 | $167.55 | $2.00 | — | $7,866.01 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $708.98; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 15 | $44.90 | $2.04 | — | $7,190.47 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $708.98; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 7 | $98.15 | $2.01 | — | $6,501.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $708.98; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 45 | $15.70 | $2.12 | — | $5,792.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $708.98; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 1 | $513.78 | $1.99 | — | $5,277.01 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; combo leftover $724.10; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 8 | $82.70 | $2.01 | — | $4,613.40 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; combo leftover $724.10; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 288 | $2.51 | $3.72 | — | $3,886.80 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; combo leftover $724.10; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 1 | $378.34 | $1.99 | — | $3,506.47 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-12.7; combo leftover $724.10; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 28 | $25.18 | $2.07 | — | $2,799.36 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $724.10; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 125 | $5.79 | $2.37 | — | $2,073.24 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $724.10; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 19 | $37.44 | $2.05 | — | $1,359.83 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.1; combo leftover $724.10; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `AHCO` | 114 | $6.32 | $2.33 | — | $637.02 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+10.7; combo leftover $724.10; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $637.02 | ▲ close $11,463.65 vs 09:30 $11,383.77 (session +155.49) | 16:00 close · cash $637.02 · equity $11,463.65 vs 09:30 $11,383.77 (+79.88; session marks +155.49) · 16 name(s) marked open→close (per-name table). AMBA×11 09:30 $63.18 → close $62.89 -3.19; ASAN×81 09:30 $8.74 → close $8.81 +5.67; DOCU×10 09:30 $68.52 → close $68.41 -1.10; DOMO×196 09:30 $3.62 → close $3.88 +51.94; GWRE×4 09:30 $167.55 → close $162.42 -20.52; IOT×15 09:30 $44.90 → close $40.20 -70.50; LULU×7 09:30 $98.15 → close $100.61 +17.22; MAMA×45 09:30 $15.70 → close $15.16 -24.30; DELL×1 09:30 $513.78 → close $524.14 +10.36; TARS×8 09:30 $82.70 → close $90.78 +64.64; BRR×288 09:30 $2.51 → close $2.66 +43.20; MDB×1 09:30 $378.34 → close $368.74 -9.60; ASST×28 09:30 $25.18 → close $27.14 +54.88; DFDV×125 09:30 $5.79 → close $5.87 +10.00; TDS×19 09:30 $37.44 → close $37.83 +7.41; AHCO×114 09:30 $6.32 → close $6.49 +19.38 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $637.02 | ▼ 09:30 equity $11,380.84 vs yday $11,463.65 (-82.81) | 09:30 open · cash $637.02 (unchanged overnight, no fees) · equity $11,380.84 vs prior close $11,463.65 (-82.81) | — |
| 2026-09-08 09:30 ET | **SELL** | `AMBA` | 11 | $63.83 | $2.04 | $+3.08 | $1,337.11 | ▲ +3.08 after sell → book $11,378.80; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASAN` | 81 | $8.73 | $2.26 | $-5.30 | $2,041.98 | ▼ -5.30 after sell → book $11,376.54; vs 09:30 mark -2.26 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 10 | $67.05 | $2.04 | $-18.76 | $2,710.44 | ▼ -18.76 after sell → book $11,374.50; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOMO` | 196 | $3.84 | $2.62 | $+38.90 | $3,460.46 | ▲ +38.90 after sell → book $11,371.88; vs 09:30 mark -2.62 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 4 | $160.52 | $2.02 | $-32.14 | $4,100.52 | ▼ -32.14 after sell → book $11,369.86; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 15 | $39.56 | $2.06 | $-84.19 | $4,691.87 | ▼ -84.19 after sell → book $11,367.81; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 7 | $100.58 | $2.03 | $+12.97 | $5,393.89 | ▲ +12.97 after sell → book $11,365.77; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MAMA` | 45 | $15.20 | $2.15 | $-26.77 | $6,075.75 | ▼ -26.77 after sell → book $11,363.63; vs 09:30 mark -2.14 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 1 | $521.15 | $2.01 | $+3.36 | $6,594.89 | ▲ +3.36 after sell → book $11,361.62; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 8 | $89.67 | $2.03 | $+51.71 | $7,310.21 | ▲ +51.71 after sell → book $11,359.58; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 288 | $2.66 | $3.77 | $+35.71 | $8,072.52 | ▲ +35.71 after sell → book $11,355.81; vs 09:30 mark -3.77 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MDB` | 1 | $360.75 | $2.01 | $-21.60 | $8,431.26 | ▼ -21.60 after sell → book $11,353.80; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 28 | $26.44 | $2.09 | $+31.11 | $9,169.48 | ▲ +31.11 after sell → book $11,351.70; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 125 | $5.81 | $2.40 | $-2.26 | $9,893.34 | ▼ -2.26 after sell → book $11,349.31; vs 09:30 mark -2.39 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 19 | $37.75 | $2.07 | $+1.78 | $10,608.52 | ▲ +1.78 after sell → book $11,347.24; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AHCO` | 114 | $6.48 | $2.36 | $+13.55 | $11,344.88 | ▲ +13.55 after sell → book $11,344.88; vs 09:30 mark -2.36 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,344.88 | ▲ close $11,344.88 vs 09:30 $11,380.84 (session +0.00) | 16:00 close · cash $11,344.88 · no lots left · equity $11,344.88. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,344.88 | ▲ 09:30 equity $11,344.88 vs yday $11,344.88 (-0.00) | 09:30 open · cash $11,344.88 (unchanged overnight, no fees) · equity $11,344.88 vs prior close $11,344.88 (-0.00) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,344.88 | ▲ close $11,344.88 vs 09:30 $11,344.88 (session +0.00) | 16:00 close · cash $11,344.88 · no lots left · equity $11,344.88. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,344.88 | ▲ 09:30 equity $11,344.88 vs yday $11,344.88 (-0.00) | 09:30 open · cash $11,344.88 (unchanged overnight, no fees) · equity $11,344.88 vs prior close $11,344.88 (-0.00) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,344.88 | ▲ close $11,344.88 vs 09:30 $11,344.88 (session +0.00) | 16:00 close · cash $11,344.88 · no lots left · equity $11,344.88. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new union_join_vol_green_h1 |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new union_join_vol_green_h1 |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new union_join_vol_green_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new union_join_vol_green_h1 |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new union_join_vol_green_h1 |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new union_join_vol_green_h1 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new union_join_vol_green_h1 |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new union_join_vol_green_h1 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new union_e_fresh_h1 |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new union_join_vol_green_h1 |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new union_join_vol_green_h1 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new union_e_fresh_h1 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new union_join_vol_green_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new union_join_vol_green_h1 |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new union_join_vol_green_h1 |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new union_join_vol_green_h1 |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new union_join_vol_green_h1 |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new union_join_vol_green_h1 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new union_e_fresh_h1 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new union_e_fresh_h1 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new union_join_vol_green_h1 |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new union_join_vol_green_h1 |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new union_join_vol_green_h1 |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new union_join_vol_green_h1 |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new union_join_vol_green_h1 |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new union_join_vol_green_h1 |
| 2026-09-10 | `UROY` | hard_red | hard-red S=-13.28 sit; no new union_join_vol_green_h1 |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
