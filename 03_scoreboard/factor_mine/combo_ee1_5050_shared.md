# Factor mine action — `combo_ee1_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_e_fresh_h3/union_e_fresh_h1 w=0.5,0.5 net=priority

Cash book **+22.95%** ($12,295) · signal-only (no cash/fees) was —. Starts YES **16/21**. Fills 96 · skips 191 · realized $+2247.51.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_e_fresh_h3 50%, union_e_fresh_h1 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_e_fresh_h3 50%, union_e_fresh_h1 50%.
- Member: union_e_fresh_h3 (50% · long · hold 3).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $195.62.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `INO` | 6172 | — | $0.81 | +0.00 | $0.90 | +555.48 | +555.48 | +0.00 | +555.48 |
| 2026-08-13 | `VOR` | 223 | — | $22.01 | +0.00 | $23.29 | +285.44 | +285.44 | +0.00 | +285.44 |
| 2026-08-14 | `INO` | 6172 | $0.90 | $0.93 | +185.16 | $1.09 | +987.52 | +1172.68 | +740.64 | +1728.16 |
| 2026-08-14 | `VOR` | 223 | $23.29 | $23.33 | +8.92 | $23.03 | -66.90 | -57.98 | +294.36 | +227.46 |
| 2026-08-14 | `BTBT` | 1 | — | $1.50 | +0.00 | $1.57 | +0.07 | +0.07 | +0.00 | +0.07 |
| 2026-08-14 | `EU` | 2 | — | $1.18 | +0.00 | $1.21 | +0.06 | +0.06 | +0.00 | +0.06 |
| 2026-08-17 | `INO` | 6172 | $1.09 | $1.07 | -123.44 | $1.15 | +493.76 | +370.32 | +1604.72 | +2098.48 |
| 2026-08-17 | `VOR` | 223 | $23.03 | $22.91 | -26.76 | $23.01 | +22.30 | -4.46 | +200.70 | +223.00 |
| 2026-08-17 | `BTBT` | 1 | $1.57 | $1.52 | -0.05 | $1.60 | +0.08 | +0.03 | +0.02 | +0.10 |
| 2026-08-17 | `EU` | 2 | $1.21 | $1.21 | +0.00 | $1.13 | -0.16 | -0.16 | +0.06 | -0.10 |
| 2026-08-18 | `INO` | 6172 | $1.15 | $1.14 | -61.72 | — | +0.00 | -61.72 | +2036.76 | — |
| 2026-08-18 | `VOR` | 223 | $23.01 | $22.82 | -42.37 | — | +0.00 | -42.37 | +180.63 | — |
| 2026-08-18 | `BTBT` | 1 | $1.60 | $1.54 | -0.06 | $1.45 | -0.09 | -0.15 | +0.04 | -0.05 |
| 2026-08-18 | `EU` | 2 | $1.13 | $1.13 | +0.00 | $1.07 | -0.12 | -0.12 | -0.10 | -0.22 |
| 2026-08-19 | `BTBT` | 1 | $1.45 | $1.42 | -0.03 | — | +0.00 | -0.03 | -0.08 | — |
| 2026-08-19 | `EU` | 2 | $1.07 | $1.07 | +0.00 | — | +0.00 | +0.00 | -0.22 | — |
| 2026-08-20 | `EL` | 15 | — | $97.43 | +0.00 | $96.15 | -19.20 | -19.20 | +0.00 | -19.20 |
| 2026-08-20 | `TOYO` | 340 | — | $4.43 | +0.00 | $4.51 | +28.90 | +28.90 | +0.00 | +28.90 |
| 2026-08-20 | `DVLT` | 5025 | — | $0.30 | +0.00 | $0.32 | +100.50 | +100.50 | +0.00 | +100.50 |
| 2026-08-20 | `AAP` | 32 | — | $46.85 | +0.00 | $42.39 | -142.72 | -142.72 | +0.00 | -142.72 |
| 2026-08-20 | `AEG` | 167 | — | $9.01 | +0.00 | $9.01 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `ALVO` | 387 | — | $3.89 | +0.00 | $4.27 | +147.06 | +147.06 | +0.00 | +147.06 |
| 2026-08-20 | `ATAT` | 44 | — | $34.05 | +0.00 | $34.25 | +8.80 | +8.80 | +0.00 | +8.80 |
| 2026-08-20 | `ATHM` | 67 | — | $22.44 | +0.00 | $22.12 | -21.44 | -21.44 | +0.00 | -21.44 |
| 2026-08-21 | `EL` | 15 | $96.15 | $96.75 | +9.00 | $101.94 | +77.85 | +86.85 | -10.20 | +67.65 |
| 2026-08-21 | `TOYO` | 340 | $4.51 | $4.68 | +56.10 | $4.82 | +47.60 | +103.70 | +85.00 | +132.60 |
| 2026-08-21 | `DVLT` | 5025 | $0.32 | $0.31 | -50.25 | $0.32 | +50.25 | +0.00 | +50.25 | +100.50 |
| 2026-08-21 | `AAP` | 32 | $42.39 | $42.41 | +0.64 | $42.58 | +5.44 | +6.08 | -142.08 | -136.64 |
| 2026-08-21 | `AEG` | 167 | $9.01 | $9.04 | +5.01 | $8.99 | -8.35 | -3.34 | +5.01 | -3.34 |
| 2026-08-21 | `ALVO` | 387 | $4.27 | $4.32 | +19.35 | $4.43 | +42.57 | +61.92 | +166.41 | +208.98 |
| 2026-08-21 | `ATAT` | 44 | $34.25 | $34.31 | +2.64 | $34.75 | +19.36 | +22.00 | +11.44 | +30.80 |
| 2026-08-21 | `ATHM` | 67 | $22.12 | $22.20 | +5.36 | $22.22 | +1.34 | +6.70 | -16.08 | -14.74 |
| 2026-08-21 | `PSEC` | 1 | — | $2.30 | +0.00 | $2.33 | +0.03 | +0.03 | +0.00 | +0.03 |
| 2026-08-24 | `EL` | 15 | $101.94 | $101.92 | -0.30 | $104.13 | +33.15 | +32.85 | +67.35 | +100.50 |
| 2026-08-24 | `TOYO` | 340 | $4.82 | $4.58 | -81.60 | $4.38 | -68.00 | -149.60 | +51.00 | -17.00 |
| 2026-08-24 | `DVLT` | 5025 | $0.32 | $0.31 | -50.25 | $0.31 | +0.00 | -50.25 | +50.25 | +50.25 |
| 2026-08-24 | `AAP` | 32 | $42.58 | $43.05 | +15.04 | $43.63 | +18.56 | +33.60 | -121.60 | -103.04 |
| 2026-08-24 | `AEG` | 167 | $8.99 | $9.15 | +26.72 | $9.19 | +6.68 | +33.40 | +23.38 | +30.06 |
| 2026-08-24 | `ALVO` | 387 | $4.43 | $4.79 | +139.32 | $5.25 | +178.02 | +317.34 | +348.30 | +526.32 |
| 2026-08-24 | `ATAT` | 44 | $34.75 | $34.70 | -2.20 | $34.75 | +2.20 | +0.00 | +28.60 | +30.80 |
| 2026-08-24 | `ATHM` | 67 | $22.22 | $22.00 | -14.74 | $21.85 | -10.05 | -24.79 | -29.48 | -39.53 |
| 2026-08-24 | `PSEC` | 1 | $2.33 | $2.34 | +0.01 | $2.32 | -0.02 | -0.01 | +0.04 | +0.02 |
| 2026-08-25 | `EL` | 15 | $104.13 | $104.00 | -1.95 | — | +0.00 | -1.95 | +98.55 | — |
| 2026-08-25 | `TOYO` | 340 | $4.38 | $4.42 | +13.60 | — | +0.00 | +13.60 | -3.40 | — |
| 2026-08-25 | `DVLT` | 5025 | $0.31 | $0.31 | +0.00 | — | +0.00 | +0.00 | +50.25 | — |
| 2026-08-25 | `AAP` | 32 | $43.63 | $43.63 | +0.00 | — | +0.00 | +0.00 | -103.04 | — |
| 2026-08-25 | `AEG` | 167 | $9.19 | $9.23 | +6.68 | — | +0.00 | +6.68 | +36.74 | — |
| 2026-08-25 | `ALVO` | 387 | $5.25 | $5.24 | -3.87 | — | +0.00 | -3.87 | +522.45 | — |
| 2026-08-25 | `ATAT` | 44 | $34.75 | $34.72 | -1.32 | — | +0.00 | -1.32 | +29.48 | — |
| 2026-08-25 | `ATHM` | 67 | $21.85 | $21.85 | +0.00 | — | +0.00 | +0.00 | -39.53 | — |
| 2026-08-25 | `PSEC` | 1 | $2.32 | $2.32 | +0.00 | $2.35 | +0.03 | +0.03 | +0.02 | +0.05 |
| 2026-08-25 | `BMO` | 8 | — | $175.01 | +0.00 | $173.46 | -12.40 | -12.40 | +0.00 | -12.40 |
| 2026-08-25 | `BNS` | 17 | — | $88.94 | +0.00 | $93.10 | +70.72 | +70.72 | +0.00 | +70.72 |
| 2026-08-25 | `BZ` | 102 | — | $15.28 | +0.00 | $16.29 | +103.02 | +103.02 | +0.00 | +103.02 |
| 2026-08-25 | `DKS` | 11 | — | $142.36 | +0.00 | $124.31 | -198.55 | -198.55 | +0.00 | -198.55 |
| 2026-08-25 | `EH` | 307 | — | $5.10 | +0.00 | $4.83 | -82.89 | -82.89 | +0.00 | -82.89 |
| 2026-08-25 | `GFI` | 32 | — | $47.89 | +0.00 | $48.87 | +31.36 | +31.36 | +0.00 | +31.36 |
| 2026-08-25 | `GRRR` | 112 | — | $13.92 | +0.00 | $14.04 | +13.44 | +13.44 | +0.00 | +13.44 |
| 2026-08-25 | `SHMD` | 345 | — | $4.54 | +0.00 | $3.42 | -388.12 | -388.12 | +0.00 | -388.12 |
| 2026-08-26 | `PSEC` | 1 | $2.35 | $2.35 | +0.00 | — | +0.00 | +0.00 | +0.05 | — |
| 2026-08-26 | `BMO` | 8 | $173.46 | $173.22 | -1.92 | $172.90 | -2.56 | -4.48 | -14.32 | -16.88 |
| 2026-08-26 | `BNS` | 17 | $93.10 | $92.65 | -7.65 | $93.59 | +15.98 | +8.33 | +63.07 | +79.05 |
| 2026-08-26 | `BZ` | 102 | $16.29 | $16.77 | +48.96 | $18.84 | +211.14 | +260.10 | +151.98 | +363.12 |
| 2026-08-26 | `DKS` | 11 | $124.31 | $121.87 | -26.84 | $129.66 | +85.69 | +58.85 | -225.39 | -139.70 |
| 2026-08-26 | `EH` | 307 | $4.83 | $4.77 | -18.42 | $4.86 | +26.10 | +7.68 | -101.31 | -75.21 |
| 2026-08-26 | `GFI` | 32 | $48.87 | $48.24 | -20.16 | $47.82 | -13.44 | -33.60 | +11.20 | -2.24 |
| 2026-08-26 | `GRRR` | 112 | $14.04 | $14.03 | -1.12 | $15.45 | +159.04 | +157.92 | +12.32 | +171.36 |
| 2026-08-26 | `SHMD` | 345 | $3.42 | $3.38 | -13.80 | $3.17 | -72.45 | -86.25 | -401.93 | -474.38 |
| 2026-08-26 | `SLQT` | 76 | — | $0.58 | +0.00 | $0.55 | -2.51 | -2.51 | +0.00 | -2.51 |
| 2026-08-26 | `TIGR` | 8 | — | $5.21 | +0.00 | $5.46 | +2.00 | +2.00 | +0.00 | +2.00 |
| 2026-08-26 | `BBWI` | 2 | — | $18.26 | +0.00 | $18.90 | +1.28 | +1.28 | +0.00 | +1.28 |
| 2026-08-26 | `BOX` | 1 | — | $34.30 | +0.00 | $33.39 | -0.91 | -0.91 | +0.00 | -0.91 |
| 2026-08-27 | `BMO` | 8 | $172.90 | $172.85 | -0.40 | $172.13 | -5.76 | -6.16 | -17.28 | -23.04 |
| 2026-08-27 | `BNS` | 17 | $93.59 | $93.52 | -1.19 | $92.93 | -10.03 | -11.22 | +77.86 | +67.83 |
| 2026-08-27 | `BZ` | 102 | $18.84 | $18.50 | -34.68 | $18.00 | -51.00 | -85.68 | +328.44 | +277.44 |
| 2026-08-27 | `DKS` | 11 | $129.66 | $128.73 | -10.23 | $131.77 | +33.44 | +23.21 | -149.93 | -116.49 |
| 2026-08-27 | `EH` | 307 | $4.86 | $4.90 | +13.81 | $4.66 | -73.68 | -59.87 | -61.40 | -135.08 |
| 2026-08-27 | `GFI` | 32 | $47.82 | $47.93 | +3.52 | $48.00 | +2.24 | +5.76 | +1.28 | +3.52 |
| 2026-08-27 | `GRRR` | 112 | $15.45 | $15.94 | +54.88 | $15.66 | -31.36 | +23.52 | +226.24 | +194.88 |
| 2026-08-27 | `SHMD` | 345 | $3.17 | $3.16 | -3.45 | $3.40 | +82.80 | +79.35 | -477.82 | -395.03 |
| 2026-08-27 | `SLQT` | 76 | $0.55 | $0.53 | -1.52 | $0.54 | +0.76 | -0.76 | -4.03 | -3.27 |
| 2026-08-27 | `TIGR` | 8 | $5.46 | $5.49 | +0.24 | $5.06 | -3.44 | -3.20 | +2.24 | -1.20 |
| 2026-08-27 | `BBWI` | 2 | $18.90 | $18.69 | -0.42 | $18.65 | -0.08 | -0.50 | +0.86 | +0.78 |
| 2026-08-27 | `BOX` | 1 | $33.39 | $33.79 | +0.40 | $34.74 | +0.95 | +1.35 | -0.51 | +0.44 |
| 2026-08-28 | `BMO` | 8 | $172.13 | $172.76 | +5.04 | — | +0.00 | +5.04 | -18.00 | — |
| 2026-08-28 | `BNS` | 17 | $92.93 | $93.30 | +6.29 | — | +0.00 | +6.29 | +74.12 | — |
| 2026-08-28 | `BZ` | 102 | $18.00 | $18.15 | +15.30 | — | +0.00 | +15.30 | +292.74 | — |
| 2026-08-28 | `DKS` | 11 | $131.77 | $132.80 | +11.33 | — | +0.00 | +11.33 | -105.16 | — |
| 2026-08-28 | `EH` | 307 | $4.66 | $4.58 | -24.56 | — | +0.00 | -24.56 | -159.64 | — |
| 2026-08-28 | `GFI` | 32 | $48.00 | $48.42 | +13.44 | — | +0.00 | +13.44 | +16.96 | — |
| 2026-08-28 | `GRRR` | 112 | $15.66 | $15.66 | +0.00 | — | +0.00 | +0.00 | +194.88 | — |
| 2026-08-28 | `SHMD` | 345 | $3.40 | $3.38 | -6.90 | — | +0.00 | -6.90 | -401.93 | — |
| 2026-08-28 | `SLQT` | 76 | $0.54 | $0.53 | -0.68 | $0.52 | -0.84 | -1.52 | -3.95 | -4.79 |
| 2026-08-28 | `TIGR` | 8 | $5.06 | $5.05 | -0.08 | $5.04 | -0.04 | -0.12 | -1.28 | -1.32 |
| 2026-08-28 | `BBWI` | 2 | $18.65 | $18.75 | +0.20 | $19.22 | +0.94 | +1.14 | +0.98 | +1.92 |
| 2026-08-28 | `BOX` | 1 | $34.74 | $34.75 | +0.01 | $34.98 | +0.23 | +0.24 | +0.45 | +0.68 |
| 2026-08-28 | `ADSK` | 5 | — | $261.16 | +0.00 | $260.66 | -2.50 | -2.50 | +0.00 | -2.50 |
| 2026-08-28 | `BBAR` | 101 | — | $15.01 | +0.00 | $14.47 | -54.54 | -54.54 | +0.00 | -54.54 |
| 2026-08-28 | `ESTC` | 14 | — | $103.89 | +0.00 | $99.91 | -55.72 | -55.72 | +0.00 | -55.72 |
| 2026-08-28 | `FINV` | 394 | — | $3.88 | +0.00 | $3.40 | -189.12 | -189.12 | +0.00 | -189.12 |
| 2026-08-28 | `FRO` | 34 | — | $44.40 | +0.00 | $44.19 | -7.14 | -7.14 | +0.00 | -7.14 |
| 2026-08-28 | `GAP` | 61 | — | $24.69 | +0.00 | $23.48 | -73.81 | -73.81 | +0.00 | -73.81 |
| 2026-08-28 | `HAFN` | 183 | — | $8.35 | +0.00 | $8.47 | +21.96 | +21.96 | +0.00 | +21.96 |
| 2026-08-28 | `IREN` | 40 | — | $37.65 | +0.00 | $35.45 | -87.80 | -87.80 | +0.00 | -87.80 |
| 2026-08-31 | `SLQT` | 76 | $0.52 | $0.51 | -0.76 | — | +0.00 | -0.76 | -5.55 | — |
| 2026-08-31 | `TIGR` | 8 | $5.04 | $5.00 | -0.36 | — | +0.00 | -0.36 | -1.68 | — |
| 2026-08-31 | `BBWI` | 2 | $19.22 | $19.25 | +0.06 | — | +0.00 | +0.06 | +1.98 | — |
| 2026-08-31 | `BOX` | 1 | $34.98 | $34.72 | -0.26 | — | +0.00 | -0.26 | +0.42 | — |
| 2026-08-31 | `ADSK` | 5 | $260.66 | $257.71 | -14.75 | $258.53 | +4.10 | -10.65 | -17.25 | -13.15 |
| 2026-08-31 | `BBAR` | 101 | $14.47 | $14.88 | +41.41 | $15.14 | +26.26 | +67.67 | -13.13 | +13.13 |
| 2026-08-31 | `ESTC` | 14 | $99.91 | $98.00 | -26.74 | $97.55 | -6.30 | -33.04 | -82.46 | -88.76 |
| 2026-08-31 | `FINV` | 394 | $3.40 | $3.39 | -3.94 | $3.67 | +110.32 | +106.38 | -193.06 | -82.74 |
| 2026-08-31 | `FRO` | 34 | $44.19 | $44.85 | +22.44 | $43.78 | -36.38 | -13.94 | +15.30 | -21.08 |
| 2026-08-31 | `GAP` | 61 | $23.48 | $22.98 | -30.50 | $22.31 | -40.87 | -71.37 | -104.31 | -145.18 |
| 2026-08-31 | `HAFN` | 183 | $8.47 | $8.53 | +10.98 | $8.44 | -16.47 | -5.49 | +32.94 | +16.47 |
| 2026-08-31 | `IREN` | 40 | $35.45 | $35.81 | +14.40 | $37.12 | +52.20 | +66.60 | -73.40 | -21.20 |
| 2026-09-01 | `ADSK` | 5 | $258.53 | $253.48 | -25.25 | $247.69 | -28.95 | -54.20 | -38.40 | -67.35 |
| 2026-09-01 | `BBAR` | 101 | $15.14 | $14.82 | -32.32 | $15.11 | +29.29 | -3.03 | -19.19 | +10.10 |
| 2026-09-01 | `ESTC` | 14 | $97.55 | $95.76 | -25.06 | $92.39 | -47.18 | -72.24 | -113.82 | -161.00 |
| 2026-09-01 | `FINV` | 394 | $3.67 | $3.58 | -35.46 | $3.32 | -102.44 | -137.90 | -118.20 | -220.64 |
| 2026-09-01 | `FRO` | 34 | $43.78 | $44.39 | +20.74 | $44.32 | -2.38 | +18.36 | -0.34 | -2.72 |
| 2026-09-01 | `GAP` | 61 | $22.31 | $22.05 | -15.86 | $22.00 | -3.05 | -18.91 | -161.04 | -164.09 |
| 2026-09-01 | `HAFN` | 183 | $8.44 | $8.56 | +21.96 | $8.59 | +5.49 | +27.45 | +38.43 | +43.92 |
| 2026-09-01 | `IREN` | 40 | $37.12 | $36.08 | -41.40 | $36.82 | +29.60 | -11.80 | -62.60 | -33.00 |
| 2026-09-02 | `ADSK` | 5 | $247.69 | $246.70 | -4.95 | — | +0.00 | -4.95 | -72.30 | — |
| 2026-09-02 | `BBAR` | 101 | $15.11 | $15.01 | -10.10 | — | +0.00 | -10.10 | +0.00 | — |
| 2026-09-02 | `ESTC` | 14 | $92.39 | $92.00 | -5.46 | — | +0.00 | -5.46 | -166.46 | — |
| 2026-09-02 | `FINV` | 394 | $3.32 | $3.32 | +0.00 | — | +0.00 | +0.00 | -220.64 | — |
| 2026-09-02 | `FRO` | 34 | $44.32 | $44.17 | -5.10 | — | +0.00 | -5.10 | -7.82 | — |
| 2026-09-02 | `GAP` | 61 | $22.00 | $21.97 | -1.83 | — | +0.00 | -1.83 | -165.92 | — |
| 2026-09-02 | `HAFN` | 183 | $8.59 | $8.58 | -1.83 | — | +0.00 | -1.83 | +42.09 | — |
| 2026-09-02 | `IREN` | 40 | $36.82 | $35.80 | -41.00 | — | +0.00 | -41.00 | -74.00 | — |
| 2026-09-03 | `AI` | 135 | — | $10.74 | +0.00 | $10.90 | +20.93 | +20.93 | +0.00 | +20.93 |
| 2026-09-03 | `AVGO` | 4 | — | $351.74 | +0.00 | $357.16 | +21.68 | +21.68 | +0.00 | +21.68 |
| 2026-09-03 | `CHPT` | 211 | — | $6.90 | +0.00 | $9.08 | +459.98 | +459.98 | +0.00 | +459.98 |
| 2026-09-03 | `CIEN` | 4 | — | $354.49 | +0.00 | $317.46 | -148.12 | -148.12 | +0.00 | -148.12 |
| 2026-09-03 | `CPB` | 65 | — | $22.32 | +0.00 | $22.13 | -12.35 | -12.35 | +0.00 | -12.35 |
| 2026-09-03 | `FIVE` | 5 | — | $257.00 | +0.00 | $239.96 | -85.20 | -85.20 | +0.00 | -85.20 |
| 2026-09-03 | `HPE` | 30 | — | $47.60 | +0.00 | $54.44 | +205.20 | +205.20 | +0.00 | +205.20 |
| 2026-09-03 | `MEI` | 96 | — | $15.09 | +0.00 | $15.32 | +22.08 | +22.08 | +0.00 | +22.08 |
| 2026-09-04 | `AI` | 135 | $10.90 | $10.91 | +1.35 | $10.46 | -60.75 | -59.40 | +22.28 | -38.47 |
| 2026-09-04 | `AVGO` | 4 | $357.16 | $359.70 | +10.16 | $357.90 | -7.20 | +2.96 | +31.84 | +24.64 |
| 2026-09-04 | `CHPT` | 211 | $9.08 | $9.28 | +42.20 | $9.89 | +128.71 | +170.91 | +502.18 | +630.89 |
| 2026-09-04 | `CIEN` | 4 | $317.46 | $321.67 | +16.84 | $321.00 | -2.68 | +14.16 | -131.28 | -133.96 |
| 2026-09-04 | `CPB` | 65 | $22.13 | $22.10 | -1.95 | $21.38 | -46.80 | -48.75 | -14.30 | -61.10 |
| 2026-09-04 | `FIVE` | 5 | $239.96 | $238.88 | -5.40 | $252.20 | +66.60 | +61.20 | -90.60 | -24.00 |
| 2026-09-04 | `HPE` | 30 | $54.44 | $53.85 | -17.70 | $52.00 | -55.50 | -73.20 | +187.50 | +132.00 |
| 2026-09-04 | `MEI` | 96 | $15.32 | $15.34 | +1.92 | $15.69 | +33.60 | +35.52 | +24.00 | +57.60 |
| 2026-09-04 | `ASAN` | 4 | — | $8.74 | +0.00 | $8.81 | +0.28 | +0.28 | +0.00 | +0.28 |
| 2026-09-04 | `DOMO` | 11 | — | $3.62 | +0.00 | $3.88 | +2.91 | +2.91 | +0.00 | +2.91 |
| 2026-09-04 | `MAMA` | 2 | — | $15.70 | +0.00 | $15.16 | -1.08 | -1.08 | +0.00 | -1.08 |
| 2026-09-08 | `AI` | 135 | $10.46 | $10.20 | -35.10 | $10.51 | +41.85 | +6.75 | -73.57 | -31.72 |
| 2026-09-08 | `AVGO` | 4 | $357.90 | $363.68 | +23.12 | $368.56 | +19.52 | +42.64 | +47.76 | +67.28 |
| 2026-09-08 | `CHPT` | 211 | $9.89 | $9.91 | +4.22 | $9.37 | -113.94 | -109.72 | +635.11 | +521.17 |
| 2026-09-08 | `CIEN` | 4 | $321.00 | $327.42 | +25.68 | $341.29 | +55.48 | +81.16 | -108.28 | -52.80 |
| 2026-09-08 | `CPB` | 65 | $21.38 | $21.30 | -5.20 | $21.76 | +29.90 | +24.70 | -66.30 | -36.40 |
| 2026-09-08 | `FIVE` | 5 | $252.20 | $251.22 | -4.90 | $254.07 | +14.25 | +9.35 | -28.90 | -14.65 |
| 2026-09-08 | `HPE` | 30 | $52.00 | $52.29 | +8.70 | $56.03 | +112.20 | +120.90 | +140.70 | +252.90 |
| 2026-09-08 | `MEI` | 96 | $15.69 | $15.80 | +10.56 | $14.06 | -167.04 | -156.48 | +68.16 | -98.88 |
| 2026-09-08 | `ASAN` | 4 | $8.81 | $8.73 | -0.32 | $8.79 | +0.24 | -0.08 | -0.04 | +0.20 |
| 2026-09-08 | `DOMO` | 11 | $3.88 | $3.84 | -0.44 | $3.83 | -0.11 | -0.55 | +2.47 | +2.36 |
| 2026-09-08 | `MAMA` | 2 | $15.16 | $15.20 | +0.08 | $15.50 | +0.60 | +0.68 | -1.00 | -0.40 |
| 2026-09-09 | `AI` | 135 | $10.51 | $10.51 | +0.00 | — | +0.00 | +0.00 | -31.72 | — |
| 2026-09-09 | `AVGO` | 4 | $368.56 | $366.23 | -9.32 | — | +0.00 | -9.32 | +57.96 | — |
| 2026-09-09 | `CHPT` | 211 | $9.37 | $9.39 | +4.22 | — | +0.00 | +4.22 | +525.39 | — |
| 2026-09-09 | `CIEN` | 4 | $341.29 | $341.90 | +2.44 | — | +0.00 | +2.44 | -50.36 | — |
| 2026-09-09 | `CPB` | 65 | $21.76 | $21.67 | -5.85 | — | +0.00 | -5.85 | -42.25 | — |
| 2026-09-09 | `FIVE` | 5 | $254.07 | $252.92 | -5.75 | — | +0.00 | -5.75 | -20.40 | — |
| 2026-09-09 | `HPE` | 30 | $56.03 | $56.94 | +27.30 | — | +0.00 | +27.30 | +280.20 | — |
| 2026-09-09 | `MEI` | 96 | $14.06 | $13.84 | -21.12 | — | +0.00 | -21.12 | -120.00 | — |
| 2026-09-09 | `ASAN` | 4 | $8.79 | $8.64 | -0.60 | $8.25 | -1.56 | -2.16 | -0.40 | -1.96 |
| 2026-09-09 | `DOMO` | 11 | $3.83 | $3.86 | +0.33 | $3.78 | -0.88 | -0.55 | +2.69 | +1.81 |
| 2026-09-09 | `MAMA` | 2 | $15.50 | $15.31 | -0.38 | $15.23 | -0.15 | -0.53 | -0.78 | -0.93 |
| 2026-09-10 | `ASAN` | 4 | $8.25 | $8.26 | +0.04 | — | +0.00 | +0.04 | -1.92 | — |
| 2026-09-10 | `DOMO` | 11 | $3.78 | $3.76 | -0.22 | — | +0.00 | -0.22 | +1.59 | — |
| 2026-09-10 | `MAMA` | 2 | $15.23 | $15.26 | +0.05 | — | +0.00 | +0.05 | -0.88 | — |
| 2026-09-11 | `ORCL` | 9 | — | $164.43 | +0.00 | $150.28 | -127.35 | -127.35 | +0.00 | -127.35 |
| 2026-09-11 | `DBI` | 259 | — | $5.91 | +0.00 | $5.88 | -7.77 | -7.77 | +0.00 | -7.77 |
| 2026-09-11 | `ADBE` | 6 | — | $242.17 | +0.00 | $252.23 | +60.36 | +60.36 | +0.00 | +60.36 |
| 2026-09-11 | `CPRT` | 47 | — | $32.01 | +0.00 | $29.95 | -96.82 | -96.82 | +0.00 | -96.82 |
| 2026-09-11 | `DSGX` | 21 | — | $71.71 | +0.00 | $76.04 | +90.93 | +90.93 | +0.00 | +90.93 |
| 2026-09-11 | `KR` | 27 | — | $56.02 | +0.00 | $58.49 | +66.69 | +66.69 | +0.00 | +66.69 |
| 2026-09-11 | `LPTH` | 163 | — | $9.37 | +0.00 | $9.20 | -27.71 | -27.71 | +0.00 | -27.71 |
| 2026-09-11 | `REF` | 116 | — | $13.10 | +0.00 | $14.03 | +107.88 | +107.88 | +0.00 | +107.88 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +840.92 | INO, VOR | — | $21.06 | $10,769.53 | INO×6172, VOR×223 |
| 2026-08-14 | +5.50 | $21.06 | INO×6172, VOR×223 | $10,963.61 | +194.08 | +920.75 | BTBT, EU | — | $17.16 | $11,884.32 | INO×6172, VOR×223, BTBT×1, EU×2 |
| 2026-08-17 | +2.25 | $17.16 | INO×6172, VOR×223, BTBT×1, EU×2 | $11,734.07 | -150.25 | +515.98 | — | — | $17.16 | $12,250.05 | INO×6172, VOR×223, BTBT×1, EU×2 |
| 2026-08-18 | -6.20 | $17.16 | INO×6172, VOR×223, BTBT×1, EU×2 | $12,145.90 | -104.15 | -0.21 | — | INO, VOR | $12,058.44 | $12,062.03 | BTBT×1, EU×2 |
| 2026-08-19 | -7.20 | $12,058.44 | BTBT×1, EU×2 | $12,062.00 | -0.03 | +0.00 | — | BTBT, EU | $12,061.92 | $12,061.92 | — |
| 2026-08-20 | +1.12 | $12,061.92 | — | $12,061.92 | -0.00 | +101.90 | EL, TOYO, DVLT, AAP, AEG, ALVO, ATAT, ATHM | — | $25.33 | $12,113.36 | EL×15, TOYO×340, DVLT×5025, AAP×32, AEG×167, ALVO×387, ATAT×44, ATHM×67 |
| 2026-08-21 | +3.25 | $25.33 | EL×15, TOYO×340, DVLT×5025, AAP×32, AEG×167, ALVO×387, ATAT×44, ATHM×67 | $12,161.21 | +47.85 | +236.09 | PSEC | — | $23.01 | $12,397.28 | EL×15, TOYO×340, DVLT×5025, AAP×32, AEG×167, ALVO×387, ATAT×44, ATHM×67, PSEC×1 |
| 2026-08-24 | -5.17 | $23.01 | EL×15, TOYO×340, DVLT×5025, AAP×32, AEG×167, ALVO×387, ATAT×44, ATHM×67, PSEC×1 | $12,429.28 | +32.00 | +160.54 | — | — | $23.01 | $12,589.82 | EL×15, TOYO×340, DVLT×5025, AAP×32, AEG×167, ALVO×387, ATAT×44, ATHM×67, PSEC×1 |
| 2026-08-25 | +1.80 | $23.01 | EL×15, TOYO×340, DVLT×5025, AAP×32, AEG×167, ALVO×387, ATAT×44, ATHM×67, PSEC×1 | $12,602.96 | +13.14 | -463.39 | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | EL, TOYO, DVLT, AAP, AEG, ALVO, ATAT, ATHM | $265.54 | $12,066.29 | PSEC×1, BMO×8, BNS×17, BZ×102, DKS×11, EH×307, GFI×32, GRRR×112, SHMD×345 |
| 2026-08-26 | +2.02 | $265.54 | PSEC×1, BMO×8, BNS×17, BZ×102, DKS×11, EH×307, GFI×32, GRRR×112, SHMD×345 | $12,025.34 | -40.95 | +409.36 | SLQT, TIGR, BBWI, BOX | PSEC | $109.20 | $12,432.82 | BMO×8, BNS×17, BZ×102, DKS×11, EH×307, GFI×32, GRRR×112, SHMD×345, SLQT×76, TIGR×8, BBWI×2, BOX×1 |
| 2026-08-27 | — | $109.20 | BMO×8, BNS×17, BZ×102, DKS×11, EH×307, GFI×32, GRRR×112, SHMD×345, SLQT×76, TIGR×8, BBWI×2, BOX×1 | $12,453.78 | +20.96 | -55.16 | — | — | $109.20 | $12,398.62 | BMO×8, BNS×17, BZ×102, DKS×11, EH×307, GFI×32, GRRR×112, SHMD×345, SLQT×76, TIGR×8, BBWI×2, BOX×1 |
| 2026-08-28 | +0.75 | $109.20 | BMO×8, BNS×17, BZ×102, DKS×11, EH×307, GFI×32, GRRR×112, SHMD×345, SLQT×76, TIGR×8, BBWI×2, BOX×1 | $12,418.01 | +19.39 | -448.38 | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | $368.67 | $11,927.83 | SLQT×76, TIGR×8, BBWI×2, BOX×1, ADSK×5, BBAR×101, ESTC×14, FINV×394, FRO×34, GAP×61, HAFN×183, IREN×40 |
| 2026-08-31 | -5.85 | $368.67 | SLQT×76, TIGR×8, BBWI×2, BOX×1, ADSK×5, BBAR×101, ESTC×14, FINV×394, FRO×34, GAP×61, HAFN×183, IREN×40 | $11,939.81 | +11.98 | +92.86 | — | SLQT, TIGR, BBWI, BOX | $518.78 | $12,030.80 | ADSK×5, BBAR×101, ESTC×14, FINV×394, FRO×34, GAP×61, HAFN×183, IREN×40 |
| 2026-09-01 | -6.30 | $518.78 | ADSK×5, BBAR×101, ESTC×14, FINV×394, FRO×34, GAP×61, HAFN×183, IREN×40 | $11,898.15 | -132.65 | -119.62 | — | — | $518.78 | $11,778.53 | ADSK×5, BBAR×101, ESTC×14, FINV×394, FRO×34, GAP×61, HAFN×183, IREN×40 |
| 2026-09-02 | -3.83 | $518.78 | ADSK×5, BBAR×101, ESTC×14, FINV×394, FRO×34, GAP×61, HAFN×183, IREN×40 | $11,708.26 | -70.27 | +0.00 | — | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | $11,687.69 | $11,687.69 | — |
| 2026-09-03 | -0.90 | $11,687.69 | — | $11,687.69 | -0.00 | +484.20 | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | — | $326.18 | $12,154.21 | AI×135, AVGO×4, CHPT×211, CIEN×4, CPB×65, FIVE×5, HPE×30, MEI×96 |
| 2026-09-04 | +2.25 | $326.18 | AI×135, AVGO×4, CHPT×211, CIEN×4, CPB×65, FIVE×5, HPE×30, MEI×96 | $12,201.63 | +47.42 | +58.09 | ASAN, DOMO, MAMA | — | $218.94 | $12,258.61 | AI×135, AVGO×4, CHPT×211, CIEN×4, CPB×65, FIVE×5, HPE×30, MEI×96, ASAN×4, DOMO×11, MAMA×2 |
| 2026-09-08 | -11.47 | $218.94 | AI×135, AVGO×4, CHPT×211, CIEN×4, CPB×65, FIVE×5, HPE×30, MEI×96, ASAN×4, DOMO×11, MAMA×2 | $12,285.01 | +26.40 | -7.05 | — | — | $218.94 | $12,277.96 | AI×135, AVGO×4, CHPT×211, CIEN×4, CPB×65, FIVE×5, HPE×30, MEI×96, ASAN×4, DOMO×11, MAMA×2 |
| 2026-09-09 | -13.95 | $218.94 | AI×135, AVGO×4, CHPT×211, CIEN×4, CPB×65, FIVE×5, HPE×30, MEI×96, ASAN×4, DOMO×11, MAMA×2 | $12,269.23 | -8.73 | -2.59 | — | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | $12,143.71 | $12,248.76 | ASAN×4, DOMO×11, MAMA×2 |
| 2026-09-10 | -13.28 | $12,143.71 | ASAN×4, DOMO×11, MAMA×2 | $12,248.63 | -0.13 | +0.00 | — | ASAN, DOMO, MAMA | $12,247.47 | $12,247.47 | — |
| 2026-09-11 | +0.50 | $12,247.47 | — | $12,247.47 | -0.00 | +66.21 | ORCL, DBI, ADBE, CPRT, DSGX, KR, LPTH, REF | — | $195.62 | $12,295.24 | ORCL×9, DBI×259, ADBE×6, CPRT×47, DSGX×21, KR×27, LPTH×163, REF×116 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 (unchanged overnight, no fees) · equity $10,000.00 vs prior close $10,000.00 (+0.00) | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 6172 | $0.81 | $68.51 | — | $4,932.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; combo leftover $5000.00; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 223 | $22.01 | $2.88 | — | $21.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; combo leftover $5000.00; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.06 | ▲ close $10,769.53 vs 09:30 $10,000.00 (session +840.92) | 16:00 close · cash $21.06 · equity $10,769.53 vs 09:30 $10,000.00 (+769.53; session marks +840.92) · 2 name(s) marked open→close (per-name table). INO×6172 09:30 $0.81 → close $0.90 +555.48; VOR×223 09:30 $22.01 → close $23.29 +285.44 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.06 | ▲ 09:30 equity $10,963.61 vs yday $10,769.53 (+194.08) | 09:30 open · cash $21.06 (unchanged overnight, no fees) · equity $10,963.61 vs prior close $10,769.53 (+194.08) | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 1 | $1.50 | $0.02 | — | $19.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $2.63; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 2 | $1.18 | $0.03 | — | $17.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $2.63; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.16 | ▲ close $11,884.32 vs 09:30 $10,963.61 (session +920.75) | 16:00 close · cash $17.16 · equity $11,884.32 vs 09:30 $10,963.61 (+920.71; session marks +920.75) · 4 name(s) marked open→close (per-name table). INO×6172 09:30 $0.93 → close $1.09 +987.52; VOR×223 09:30 $23.33 → close $23.03 -66.90; BTBT×1 09:30 $1.50 → close $1.57 +0.07; EU×2 09:30 $1.18 → close $1.21 +0.06 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.16 | ▼ 09:30 equity $11,734.07 vs yday $11,884.32 (-150.25) | 09:30 open · cash $17.16 (unchanged overnight, no fees) · equity $11,734.07 vs prior close $11,884.32 (-150.25) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.16 | ▲ close $12,250.05 vs 09:30 $11,734.07 (session +515.98) | 16:00 close · cash $17.16 · equity $12,250.05 vs 09:30 $11,734.07 (+515.98; session marks +515.98) · 4 name(s) marked open→close (per-name table). INO×6172 09:30 $1.07 → close $1.15 +493.76; VOR×223 09:30 $22.91 → close $23.01 +22.30; BTBT×1 09:30 $1.52 → close $1.60 +0.08; EU×2 09:30 $1.21 → close $1.13 -0.16 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.16 | ▼ 09:30 equity $12,145.90 vs yday $12,250.05 (-104.15) | 09:30 open · cash $17.16 (unchanged overnight, no fees) · equity $12,145.90 vs prior close $12,250.05 (-104.15) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 6172 | $1.14 | $80.70 | $+1887.55 | $6,972.54 | ▲ +1,887.55 after sell → book $12,065.20; vs 09:30 mark -80.70 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 223 | $22.82 | $2.95 | $+174.80 | $12,058.44 | ▲ +174.80 after sell → book $12,062.24; vs 09:30 mark -2.96 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,058.44 | ▼ close $12,062.03 vs 09:30 $12,145.90 (session -0.21) | 16:00 close · cash $12,058.44 · equity $12,062.03 vs 09:30 $12,145.90 (-83.87; session marks -0.21) · 2 name(s) marked open→close (per-name table). BTBT×1 09:30 $1.54 → close $1.45 -0.09; EU×2 09:30 $1.13 → close $1.07 -0.12 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,058.44 | ▼ 09:30 equity $12,062.00 vs yday $12,062.03 (-0.03) | 09:30 open · cash $12,058.44 (unchanged overnight, no fees) · equity $12,062.00 vs prior close $12,062.03 (-0.03) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 1 | $1.42 | $0.04 | $-0.14 | $12,059.83 | ▼ -0.14 after sell → book $12,061.97; vs 09:30 mark -0.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `EU` | 2 | $1.07 | $0.05 | $-0.30 | $12,061.92 | ▼ -0.30 after sell → book $12,061.92; vs 09:30 mark -0.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,061.92 | ▲ close $12,061.92 vs 09:30 $12,062.00 (session +0.00) | 16:00 close · cash $12,061.92 · no lots left · equity $12,061.92. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,061.92 | ▲ 09:30 equity $12,061.92 vs yday $12,061.92 (-0.00) | 09:30 open · cash $12,061.92 (unchanged overnight, no fees) · equity $12,061.92 vs prior close $12,061.92 (-0.00) | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 15 | $97.43 | $2.04 | — | $10,598.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $1507.74; owner union_e_fresh_h3 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 340 | $4.43 | $4.39 | — | $9,087.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $1507.74; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 5025 | $0.30 | $30.15 | — | $7,550.20 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $1507.74; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 32 | $46.85 | $2.09 | — | $6,048.91 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $1507.74; owner union_e_fresh_h3 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 167 | $9.01 | $2.49 | — | $4,541.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $1507.74; owner union_e_fresh_h3 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 387 | $3.89 | $4.99 | — | $3,031.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $1507.74; owner union_e_fresh_h3 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 44 | $34.05 | $2.12 | — | $1,531.01 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $1507.74; owner union_e_fresh_h3 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 67 | $22.44 | $2.19 | — | $25.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $1507.74; owner union_e_fresh_h3 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.33 | ▲ close $12,113.36 vs 09:30 $12,061.92 (session +101.90) | 16:00 close · cash $25.33 · equity $12,113.36 vs 09:30 $12,061.92 (+51.44; session marks +101.90) · 8 name(s) marked open→close (per-name table). EL×15 09:30 $97.43 → close $96.15 -19.20; TOYO×340 09:30 $4.43 → close $4.51 +28.90; DVLT×5025 09:30 $0.30 → close $0.32 +100.50; AAP×32 09:30 $46.85 → close $42.39 -142.72; AEG×167 09:30 $9.01 → close $9.01 +0.00; ALVO×387 09:30 $3.89 → close $4.27 +147.06; ATAT×44 09:30 $34.05 → close $34.25 +8.80; ATHM×67 09:30 $22.44 → close $22.12 -21.44 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.33 | ▲ 09:30 equity $12,161.21 vs yday $12,113.36 (+47.85) | 09:30 open · cash $25.33 (unchanged overnight, no fees) · equity $12,161.21 vs prior close $12,113.36 (+47.85) | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 1 | $2.30 | $0.03 | — | $23.01 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $3.62; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.01 | ▲ close $12,397.28 vs 09:30 $12,161.21 (session +236.09) | 16:00 close · cash $23.01 · equity $12,397.28 vs 09:30 $12,161.21 (+236.07; session marks +236.09) · 9 name(s) marked open→close (per-name table). EL×15 09:30 $96.75 → close $101.94 +77.85; TOYO×340 09:30 $4.68 → close $4.82 +47.60; DVLT×5025 09:30 $0.31 → close $0.32 +50.25; AAP×32 09:30 $42.41 → close $42.58 +5.44; AEG×167 09:30 $9.04 → close $8.99 -8.35; ALVO×387 09:30 $4.32 → close $4.43 +42.57; ATAT×44 09:30 $34.31 → close $34.75 +19.36; ATHM×67 09:30 $22.20 → close $22.22 +1.34; PSEC×1 09:30 $2.30 → close $2.33 +0.03 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.01 | ▲ 09:30 equity $12,429.28 vs yday $12,397.28 (+32.00) | 09:30 open · cash $23.01 (unchanged overnight, no fees) · equity $12,429.28 vs prior close $12,397.28 (+32.00) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.01 | ▲ close $12,589.82 vs 09:30 $12,429.28 (session +160.54) | 16:00 close · cash $23.01 · equity $12,589.82 vs 09:30 $12,429.28 (+160.54; session marks +160.54) · 9 name(s) marked open→close (per-name table). EL×15 09:30 $101.92 → close $104.13 +33.15; TOYO×340 09:30 $4.58 → close $4.38 -68.00; DVLT×5025 09:30 $0.31 → close $0.31 +0.00; AAP×32 09:30 $43.05 → close $43.63 +18.56; AEG×167 09:30 $9.15 → close $9.19 +6.68; ALVO×387 09:30 $4.79 → close $5.25 +178.02; ATAT×44 09:30 $34.70 → close $34.75 +2.20; ATHM×67 09:30 $22.00 → close $21.85 -10.05; PSEC×1 09:30 $2.34 → close $2.32 -0.02 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.01 | ▲ 09:30 equity $12,602.96 vs yday $12,589.82 (+13.14) | 09:30 open · cash $23.01 (unchanged overnight, no fees) · equity $12,602.96 vs prior close $12,589.82 (+13.14) | — |
| 2026-08-25 09:30 ET | **SELL** | `EL` | 15 | $104.00 | $2.06 | $+94.46 | $1,580.95 | ▲ +94.46 after sell → book $12,600.90; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TOYO` | 340 | $4.42 | $4.45 | $-12.24 | $3,079.30 | ▼ -12.24 after sell → book $12,596.45; vs 09:30 mark -4.45 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 5025 | $0.31 | $31.50 | $-11.40 | $4,605.55 | ▼ -11.40 after sell → book $12,564.95; vs 09:30 mark -31.50 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 32 | $43.63 | $2.11 | $-107.23 | $5,999.60 | ▼ -107.23 after sell → book $12,562.84; vs 09:30 mark -2.11 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 167 | $9.23 | $2.53 | $+31.72 | $7,538.48 | ▲ +31.72 after sell → book $12,560.31; vs 09:30 mark -2.53 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 387 | $5.24 | $5.07 | $+512.38 | $9,561.29 | ▲ +512.38 after sell → book $12,555.24; vs 09:30 mark -5.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 44 | $34.72 | $2.14 | $+25.21 | $11,086.82 | ▲ +25.21 after sell → book $12,553.09; vs 09:30 mark -2.15 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 67 | $21.85 | $2.21 | $-43.93 | $12,548.56 | ▼ -43.93 after sell → book $12,550.88; vs 09:30 mark -2.21 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 8 | $175.01 | $2.01 | — | $11,146.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $1568.57; owner union_e_fresh_h3 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 17 | $88.94 | $2.04 | — | $9,632.44 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $1568.57; owner union_e_fresh_h3 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 102 | $15.28 | $2.30 | — | $8,071.59 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $1568.57; owner union_e_fresh_h3 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 11 | $142.36 | $2.02 | — | $6,503.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $1568.57; owner union_e_fresh_h3 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 307 | $5.10 | $3.96 | — | $4,933.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $1568.57; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 32 | $47.89 | $2.09 | — | $3,399.38 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $1568.57; owner union_e_fresh_h3 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 112 | $13.92 | $2.33 | — | $1,838.01 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $1568.57; owner union_e_fresh_h3 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 345 | $4.54 | $4.45 | — | $265.54 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $1568.57; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $265.54 | ▼ close $12,066.29 vs 09:30 $12,602.96 (session -463.39) | 16:00 close · cash $265.54 · equity $12,066.29 vs 09:30 $12,602.96 (-536.67; session marks -463.39) · 9 name(s) marked open→close (per-name table). PSEC×1 09:30 $2.32 → close $2.35 +0.03; BMO×8 09:30 $175.01 → close $173.46 -12.40; BNS×17 09:30 $88.94 → close $93.10 +70.72; BZ×102 09:30 $15.28 → close $16.29 +103.02; DKS×11 09:30 $142.36 → close $124.31 -198.55; EH×307 09:30 $5.10 → close $4.83 -82.89; GFI×32 09:30 $47.89 → close $48.87 +31.36; GRRR×112 09:30 $13.92 → close $14.04 +13.44; SHMD×345 09:30 $4.54 → close $3.42 -388.12 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $265.54 | ▼ 09:30 equity $12,025.34 vs yday $12,066.29 (-40.95) | 09:30 open · cash $265.54 (unchanged overnight, no fees) · equity $12,025.34 vs prior close $12,066.29 (-40.95) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 1 | $2.35 | $0.05 | $-0.02 | $267.84 | ▼ -0.02 after sell → book $12,025.29; vs 09:30 mark -0.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 76 | $0.58 | $0.67 | — | $222.86 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $44.64; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 8 | $5.21 | $0.44 | — | $180.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $44.64; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 2 | $18.26 | $0.37 | — | $143.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $44.64; owner union_e_fresh_h3 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 1 | $34.30 | $0.35 | — | $109.20 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $44.64; owner union_e_fresh_h3 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.20 | ▲ close $12,432.82 vs 09:30 $12,025.34 (session +409.36) | 16:00 close · cash $109.20 · equity $12,432.82 vs 09:30 $12,025.34 (+407.48; session marks +409.36) · 12 name(s) marked open→close (per-name table). BMO×8 09:30 $173.22 → close $172.90 -2.56; BNS×17 09:30 $92.65 → close $93.59 +15.98; BZ×102 09:30 $16.77 → close $18.84 +211.14; DKS×11 09:30 $121.87 → close $129.66 +85.69; EH×307 09:30 $4.77 → close $4.86 +26.10; GFI×32 09:30 $48.24 → close $47.82 -13.44; GRRR×112 09:30 $14.03 → close $15.45 +159.04; SHMD×345 09:30 $3.38 → close $3.17 -72.45; SLQT×76 09:30 $0.58 → close $0.55 -2.51; TIGR×8 09:30 $5.21 → close $5.46 +2.00; BBWI×2 09:30 $18.26 → close $18.90 +1.28; BOX×1 09:30 $34.30 → close $33.39 -0.91 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.20 | ▲ 09:30 equity $12,453.78 vs yday $12,432.82 (+20.96) | 09:30 open · cash $109.20 (unchanged overnight, no fees) · equity $12,453.78 vs prior close $12,432.82 (+20.96) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.20 | ▼ close $12,398.62 vs 09:30 $12,453.78 (session -55.16) | 16:00 close · cash $109.20 · equity $12,398.62 vs 09:30 $12,453.78 (-55.16; session marks -55.16) · 12 name(s) marked open→close (per-name table). BMO×8 09:30 $172.85 → close $172.13 -5.76; BNS×17 09:30 $93.52 → close $92.93 -10.03; BZ×102 09:30 $18.50 → close $18.00 -51.00; DKS×11 09:30 $128.73 → close $131.77 +33.44; EH×307 09:30 $4.90 → close $4.66 -73.68; GFI×32 09:30 $47.93 → close $48.00 +2.24; GRRR×112 09:30 $15.94 → close $15.66 -31.36; SHMD×345 09:30 $3.16 → close $3.40 +82.80; SLQT×76 09:30 $0.53 → close $0.54 +0.76; TIGR×8 09:30 $5.49 → close $5.06 -3.44; BBWI×2 09:30 $18.69 → close $18.65 -0.08; BOX×1 09:30 $33.79 → close $34.74 +0.95 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.20 | ▲ 09:30 equity $12,418.01 vs yday $12,398.62 (+19.39) | 09:30 open · cash $109.20 (unchanged overnight, no fees) · equity $12,418.01 vs prior close $12,398.62 (+19.39) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 8 | $172.76 | $2.04 | $-22.05 | $1,489.25 | ▼ -22.05 after sell → book $12,415.97; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 17 | $93.30 | $2.06 | $+70.02 | $3,073.28 | ▲ +70.02 after sell → book $12,413.91; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 102 | $18.15 | $2.33 | $+288.12 | $4,922.26 | ▲ +288.12 after sell → book $12,411.58; vs 09:30 mark -2.33 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 11 | $132.80 | $2.04 | $-109.23 | $6,381.01 | ▼ -109.23 after sell → book $12,409.54; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 307 | $4.58 | $4.02 | $-167.62 | $7,783.05 | ▼ -167.62 after sell → book $12,405.52; vs 09:30 mark -4.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 32 | $48.42 | $2.11 | $+12.77 | $9,330.38 | ▲ +12.77 after sell → book $12,403.41; vs 09:30 mark -2.11 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 112 | $15.66 | $2.36 | $+190.20 | $11,081.94 | ▲ +190.20 after sell → book $12,401.05; vs 09:30 mark -2.36 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 345 | $3.38 | $4.52 | $-410.89 | $12,243.52 | ▼ -410.89 after sell → book $12,396.53; vs 09:30 mark -4.52 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $10,935.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $1530.44; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 101 | $15.01 | $2.29 | — | $9,417.42 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $1530.44; owner union_e_fresh_h3 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 14 | $103.89 | $2.03 | — | $7,960.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $1530.44; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 394 | $3.88 | $5.08 | — | $6,427.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $1530.44; owner union_e_fresh_h3 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 34 | $44.40 | $2.09 | — | $4,915.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $1530.44; owner union_e_fresh_h3 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 61 | $24.69 | $2.17 | — | $3,407.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $1530.44; owner union_e_fresh_h3 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 183 | $8.35 | $2.54 | — | $1,876.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $1530.44; owner union_e_fresh_h3 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 40 | $37.65 | $2.11 | — | $368.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $1530.44; owner union_e_fresh_h3 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $368.67 | ▼ close $11,927.83 vs 09:30 $12,418.01 (session -448.38) | 16:00 close · cash $368.67 · equity $11,927.83 vs 09:30 $12,418.01 (-490.18; session marks -448.38) · 12 name(s) marked open→close (per-name table). SLQT×76 09:30 $0.53 → close $0.52 -0.84; TIGR×8 09:30 $5.05 → close $5.04 -0.04; BBWI×2 09:30 $18.75 → close $19.22 +0.94; BOX×1 09:30 $34.75 → close $34.98 +0.23; ADSK×5 09:30 $261.16 → close $260.66 -2.50; BBAR×101 09:30 $15.01 → close $14.47 -54.54; ESTC×14 09:30 $103.89 → close $99.91 -55.72; FINV×394 09:30 $3.88 → close $3.40 -189.12; FRO×34 09:30 $44.40 → close $44.19 -7.14; GAP×61 09:30 $24.69 → close $23.48 -73.81; HAFN×183 09:30 $8.35 → close $8.47 +21.96; IREN×40 09:30 $37.65 → close $35.45 -87.80 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $368.67 | ▲ 09:30 equity $11,939.81 vs yday $11,927.83 (+11.98) | 09:30 open · cash $368.67 (unchanged overnight, no fees) · equity $11,939.81 vs prior close $11,927.83 (+11.98) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 76 | $0.51 | $0.64 | $-6.86 | $406.79 | ▼ -6.86 after sell → book $11,939.17; vs 09:30 mark -0.64 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 8 | $5.00 | $0.44 | $-2.56 | $446.35 | ▼ -2.56 after sell → book $11,938.73; vs 09:30 mark -0.44 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 2 | $19.25 | $0.41 | $+1.20 | $484.43 | ▲ +1.20 after sell → book $11,938.31; vs 09:30 mark -0.42 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BOX` | 1 | $34.72 | $0.37 | $-0.30 | $518.78 | ▼ -0.30 after sell → book $11,937.94; vs 09:30 mark -0.37 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $518.78 | ▲ close $12,030.80 vs 09:30 $11,939.81 (session +92.86) | 16:00 close · cash $518.78 · equity $12,030.80 vs 09:30 $11,939.81 (+90.99; session marks +92.86) · 8 name(s) marked open→close (per-name table). ADSK×5 09:30 $257.71 → close $258.53 +4.10; BBAR×101 09:30 $14.88 → close $15.14 +26.26; ESTC×14 09:30 $98.00 → close $97.55 -6.30; FINV×394 09:30 $3.39 → close $3.67 +110.32; FRO×34 09:30 $44.85 → close $43.78 -36.38; GAP×61 09:30 $22.98 → close $22.31 -40.87; HAFN×183 09:30 $8.53 → close $8.44 -16.47; IREN×40 09:30 $35.81 → close $37.12 +52.20 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $518.78 | ▼ 09:30 equity $11,898.15 vs yday $12,030.80 (-132.65) | 09:30 open · cash $518.78 (unchanged overnight, no fees) · equity $11,898.15 vs prior close $12,030.80 (-132.65) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $518.78 | ▼ close $11,778.53 vs 09:30 $11,898.15 (session -119.62) | 16:00 close · cash $518.78 · equity $11,778.53 vs 09:30 $11,898.15 (-119.62; session marks -119.62) · 8 name(s) marked open→close (per-name table). ADSK×5 09:30 $253.48 → close $247.69 -28.95; BBAR×101 09:30 $14.82 → close $15.11 +29.29; ESTC×14 09:30 $95.76 → close $92.39 -47.18; FINV×394 09:30 $3.58 → close $3.32 -102.44; FRO×34 09:30 $44.39 → close $44.32 -2.38; GAP×61 09:30 $22.05 → close $22.00 -3.05; HAFN×183 09:30 $8.56 → close $8.59 +5.49; IREN×40 09:30 $36.08 → close $36.82 +29.60 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $518.78 | ▼ 09:30 equity $11,708.26 vs yday $11,778.53 (-70.27) | 09:30 open · cash $518.78 (unchanged overnight, no fees) · equity $11,708.26 vs prior close $11,778.53 (-70.27) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 5 | $246.70 | $2.02 | $-76.33 | $1,750.26 | ▼ -76.33 after sell → book $11,706.24; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 101 | $15.01 | $2.32 | $-4.61 | $3,263.95 | ▼ -4.61 after sell → book $11,703.92; vs 09:30 mark -2.32 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 14 | $92.00 | $2.05 | $-170.54 | $4,549.90 | ▼ -170.54 after sell → book $11,701.87; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 394 | $3.32 | $5.16 | $-230.88 | $5,852.82 | ▼ -230.88 after sell → book $11,696.71; vs 09:30 mark -5.16 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 34 | $44.17 | $2.11 | $-12.03 | $7,352.48 | ▼ -12.03 after sell → book $11,694.59; vs 09:30 mark -2.12 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 61 | $21.97 | $2.19 | $-170.29 | $8,690.46 | ▼ -170.29 after sell → book $11,692.40; vs 09:30 mark -2.19 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 183 | $8.58 | $2.58 | $+36.97 | $10,258.02 | ▲ +36.97 after sell → book $11,689.82; vs 09:30 mark -2.58 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 40 | $35.80 | $2.13 | $-78.24 | $11,687.69 | ▼ -78.24 after sell → book $11,687.69; vs 09:30 mark -2.13 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,687.69 | ▲ close $11,687.69 vs 09:30 $11,708.26 (session +0.00) | 16:00 close · cash $11,687.69 · no lots left · equity $11,687.69. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,687.69 | ▲ 09:30 equity $11,687.69 vs yday $11,687.69 (-0.00) | 09:30 open · cash $11,687.69 (unchanged overnight, no fees) · equity $11,687.69 vs prior close $11,687.69 (-0.00) | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 135 | $10.74 | $2.40 | — | $10,234.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $1460.96; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 4 | $351.74 | $2.00 | — | $8,825.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $1460.96; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 211 | $6.90 | $2.72 | — | $7,367.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $1460.96; owner union_e_fresh_h3 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 4 | $354.49 | $2.00 | — | $5,947.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $1460.96; owner union_e_fresh_h3 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 65 | $22.32 | $2.19 | — | $4,494.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $1460.96; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 5 | $257.00 | $2.00 | — | $3,207.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $1460.96; owner union_e_fresh_h3 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 30 | $47.60 | $2.08 | — | $1,777.10 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $1460.96; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 96 | $15.09 | $2.28 | — | $326.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $1460.96; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $326.18 | ▲ close $12,154.21 vs 09:30 $11,687.69 (session +484.20) | 16:00 close · cash $326.18 · equity $12,154.21 vs 09:30 $11,687.69 (+466.52; session marks +484.20) · 8 name(s) marked open→close (per-name table). AI×135 09:30 $10.74 → close $10.90 +20.93; AVGO×4 09:30 $351.74 → close $357.16 +21.68; CHPT×211 09:30 $6.90 → close $9.08 +459.98; CIEN×4 09:30 $354.49 → close $317.46 -148.12; CPB×65 09:30 $22.32 → close $22.13 -12.35; FIVE×5 09:30 $257.00 → close $239.96 -85.20; HPE×30 09:30 $47.60 → close $54.44 +205.20; MEI×96 09:30 $15.09 → close $15.32 +22.08 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $326.18 | ▲ 09:30 equity $12,201.63 vs yday $12,154.21 (+47.42) | 09:30 open · cash $326.18 (unchanged overnight, no fees) · equity $12,201.63 vs prior close $12,154.21 (+47.42) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 4 | $8.74 | $0.36 | — | $290.86 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $40.77; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 11 | $3.62 | $0.43 | — | $250.66 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $40.77; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 2 | $15.70 | $0.32 | — | $218.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $40.77; owner union_e_fresh_h3 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $218.94 | ▲ close $12,258.61 vs 09:30 $12,201.63 (session +58.09) | 16:00 close · cash $218.94 · equity $12,258.61 vs 09:30 $12,201.63 (+56.98; session marks +58.09) · 11 name(s) marked open→close (per-name table). AI×135 09:30 $10.91 → close $10.46 -60.75; AVGO×4 09:30 $359.70 → close $357.90 -7.20; CHPT×211 09:30 $9.28 → close $9.89 +128.71; CIEN×4 09:30 $321.67 → close $321.00 -2.68; CPB×65 09:30 $22.10 → close $21.38 -46.80; FIVE×5 09:30 $238.88 → close $252.20 +66.60; HPE×30 09:30 $53.85 → close $52.00 -55.50; MEI×96 09:30 $15.34 → close $15.69 +33.60; ASAN×4 09:30 $8.74 → close $8.81 +0.28; DOMO×11 09:30 $3.62 → close $3.88 +2.91; MAMA×2 09:30 $15.70 → close $15.16 -1.08 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $218.94 | ▲ 09:30 equity $12,285.01 vs yday $12,258.61 (+26.40) | 09:30 open · cash $218.94 (unchanged overnight, no fees) · equity $12,285.01 vs prior close $12,258.61 (+26.40) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $218.94 | ▼ close $12,277.96 vs 09:30 $12,285.01 (session -7.05) | 16:00 close · cash $218.94 · equity $12,277.96 vs 09:30 $12,285.01 (-7.05; session marks -7.05) · 11 name(s) marked open→close (per-name table). AI×135 09:30 $10.20 → close $10.51 +41.85; AVGO×4 09:30 $363.68 → close $368.56 +19.52; CHPT×211 09:30 $9.91 → close $9.37 -113.94; CIEN×4 09:30 $327.42 → close $341.29 +55.48; CPB×65 09:30 $21.30 → close $21.76 +29.90; FIVE×5 09:30 $251.22 → close $254.07 +14.25; HPE×30 09:30 $52.29 → close $56.03 +112.20; MEI×96 09:30 $15.80 → close $14.06 -167.04; ASAN×4 09:30 $8.73 → close $8.79 +0.24; DOMO×11 09:30 $3.84 → close $3.83 -0.11; MAMA×2 09:30 $15.20 → close $15.50 +0.60 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $218.94 | ▼ 09:30 equity $12,269.23 vs yday $12,277.96 (-8.73) | 09:30 open · cash $218.94 (unchanged overnight, no fees) · equity $12,269.23 vs prior close $12,277.96 (-8.73) | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 135 | $10.51 | $2.43 | $-36.55 | $1,635.37 | ▼ -36.55 after sell → book $12,266.81; vs 09:30 mark -2.42 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 4 | $366.23 | $2.02 | $+53.93 | $3,098.26 | ▲ +53.93 after sell → book $12,264.78; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 211 | $9.39 | $2.77 | $+519.90 | $5,076.78 | ▲ +519.90 after sell → book $12,262.01; vs 09:30 mark -2.77 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 4 | $341.90 | $2.02 | $-54.38 | $6,442.36 | ▼ -54.38 after sell → book $12,259.99; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 65 | $21.67 | $2.21 | $-46.64 | $7,848.70 | ▼ -46.64 after sell → book $12,257.78; vs 09:30 mark -2.21 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 5 | $252.92 | $2.03 | $-24.43 | $9,111.27 | ▼ -24.43 after sell → book $12,255.75; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 30 | $56.94 | $2.10 | $+276.02 | $10,817.37 | ▲ +276.02 after sell → book $12,253.65; vs 09:30 mark -2.10 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 96 | $13.84 | $2.30 | $-124.58 | $12,143.71 | ▼ -124.58 after sell → book $12,251.35; vs 09:30 mark -2.30 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,143.71 | ▼ close $12,248.76 vs 09:30 $12,269.23 (session -2.59) | 16:00 close · cash $12,143.71 · equity $12,248.76 vs 09:30 $12,269.23 (-20.47; session marks -2.59) · 3 name(s) marked open→close (per-name table). ASAN×4 09:30 $8.64 → close $8.25 -1.56; DOMO×11 09:30 $3.86 → close $3.78 -0.88; MAMA×2 09:30 $15.31 → close $15.23 -0.15 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,143.71 | ▼ 09:30 equity $12,248.63 vs yday $12,248.76 (-0.13) | 09:30 open · cash $12,143.71 (unchanged overnight, no fees) · equity $12,248.63 vs prior close $12,248.76 (-0.13) | — |
| 2026-09-10 09:30 ET | **SELL** | `ASAN` | 4 | $8.26 | $0.36 | $-2.64 | $12,176.38 | ▼ -2.64 after sell → book $12,248.26; vs 09:30 mark -0.37 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 11 | $3.76 | $0.47 | $+0.70 | $12,217.28 | ▲ +0.70 after sell → book $12,247.80; vs 09:30 mark -0.46 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MAMA` | 2 | $15.26 | $0.33 | $-1.53 | $12,247.47 | ▼ -1.53 after sell → book $12,247.47; vs 09:30 mark -0.33 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,247.47 | ▲ close $12,247.47 vs 09:30 $12,248.63 (session +0.00) | 16:00 close · cash $12,247.47 · no lots left · equity $12,247.47. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,247.47 | ▲ 09:30 equity $12,247.47 vs yday $12,247.47 (-0.00) | 09:30 open · cash $12,247.47 (unchanged overnight, no fees) · equity $12,247.47 vs prior close $12,247.47 (-0.00) | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 9 | $164.43 | $2.02 | — | $10,765.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; 🔵; ⚪; ret5=+9.0; combo leftover $1530.93; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 259 | $5.91 | $3.34 | — | $9,231.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list probable,yday_gainer,yday_mover; ret5=-4.9; combo leftover $1530.93; owner union_e_fresh_h3 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 6 | $242.17 | $2.01 | — | $7,776.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-12.1; combo leftover $1530.93; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 47 | $32.01 | $2.13 | — | $6,269.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $1530.93; owner union_e_fresh_h3 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 21 | $71.71 | $2.05 | — | $4,761.96 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.3; combo leftover $1530.93; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 27 | $56.02 | $2.07 | — | $3,247.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.6; combo leftover $1530.93; owner union_e_fresh_h3 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 163 | $9.37 | $2.48 | — | $1,717.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $1530.93; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 116 | $13.10 | $2.34 | — | $195.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.2; combo leftover $1530.93; owner union_e_fresh_h3 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $195.62 | ▲ close $12,295.24 vs 09:30 $12,247.47 (session +66.21) | 16:00 close · cash $195.62 · equity $12,295.24 vs 09:30 $12,247.47 (+47.77; session marks +66.21) · 8 name(s) marked open→close (per-name table). ORCL×9 09:30 $164.43 → close $150.28 -127.35; DBI×259 09:30 $5.91 → close $5.88 -7.77; ADBE×6 09:30 $242.17 → close $252.23 +60.36; CPRT×47 09:30 $32.01 → close $29.95 -96.82; DSGX×21 09:30 $71.71 → close $76.04 +90.93; KR×27 09:30 $56.02 → close $58.49 +66.69; LPTH×163 09:30 $9.37 → close $9.20 -27.71; REF×116 09:30 $13.10 → close $14.03 +107.88 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `VOR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `ARX` | cash | leftover split 2.63 < 1 share @ 19.57 |
| 2026-08-14 | `AIRO` | cash | leftover split 2.63 < 1 share @ 11.12 |
| 2026-08-14 | `MH` | cash | leftover split 2.63 < 1 share @ 13.55 |
| 2026-08-14 | `CLBT` | cash | leftover split 2.63 < 1 share @ 10.83 |
| 2026-08-14 | `LUNR` | cash | leftover split 2.63 < 1 share @ 19.17 |
| 2026-08-14 | `NMAX` | cash | leftover split 2.63 < 1 share @ 9.89 |
| 2026-08-17 | `INO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `VOR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `BTBT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `EU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `BTBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `EU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h3 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h3 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h3 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h3 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h3 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h3 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h3 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h3 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h3 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h3 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h3 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h3 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h3 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h3 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h3 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-21 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TOYO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `DVLT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AEG` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ALVO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ATAT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ATHM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `FUTU` | cash | leftover split 3.62 < 1 share @ 115.18 |
| 2026-08-21 | `DE` | cash | leftover split 3.62 < 1 share @ 623.26 |
| 2026-08-21 | `WMT` | cash | leftover split 3.62 < 1 share @ 103.69 |
| 2026-08-21 | `BEKE` | cash | leftover split 3.62 < 1 share @ 17.93 |
| 2026-08-21 | `BJ` | cash | leftover split 3.62 < 1 share @ 93.98 |
| 2026-08-21 | `BKE` | cash | leftover split 3.62 < 1 share @ 43.08 |
| 2026-08-24 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TOYO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `DVLT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AAP` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AEG` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ALVO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATAT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATHM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `PSEC` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new union_e_fresh_h3 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new union_e_fresh_h1 |
| 2026-08-25 | `PSEC` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-26 | `BMO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BNS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `EH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GFI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GRRR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `SHMD` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `ANF` | cash | leftover split 44.64 < 1 share @ 131.37 |
| 2026-08-26 | `DY` | cash | leftover split 44.64 < 1 share @ 326.91 |
| 2026-08-27 | `BMO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BNS` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BZ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `DKS` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `EH` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GFI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GRRR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `SHMD` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `SLQT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `TIGR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BBWI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BOX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `NVDA` | cash | leftover split 109.20 < 1 share @ 222.86 |
| 2026-08-28 | `SLQT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `TIGR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBWI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BOX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `ADSK` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `BBAR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `ESTC` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FINV` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FRO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `GAP` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `HAFN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `IREN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new union_e_fresh_h3 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new union_e_fresh_h1 |
| 2026-09-01 | `ADSK` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `BBAR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `ESTC` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FINV` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FRO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `GAP` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `HAFN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `IREN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new union_e_fresh_h3 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h3 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h3 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h3 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h3 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h3 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h3 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-04 | `AI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `AVGO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CHPT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CIEN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CPB` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `FIVE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `HPE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `MEI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `AMBA` | cash | leftover split 40.77 < 1 share @ 63.18 |
| 2026-09-04 | `DOCU` | cash | leftover split 40.77 < 1 share @ 68.52 |
| 2026-09-04 | `GWRE` | cash | leftover split 40.77 < 1 share @ 167.55 |
| 2026-09-04 | `IOT` | cash | leftover split 40.77 < 1 share @ 44.90 |
| 2026-09-04 | `LULU` | cash | leftover split 40.77 < 1 share @ 98.15 |
| 2026-09-08 | `AI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `AVGO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CHPT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CIEN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CPB` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `FIVE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `HPE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `MEI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `ASAN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `DOMO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `MAMA` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new union_e_fresh_h3 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new union_e_fresh_h3 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new union_e_fresh_h3 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new union_e_fresh_h1 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new union_e_fresh_h1 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `ASAN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `DOMO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `MAMA` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h3 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h3 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h3 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h3 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h3 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h3 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h3 |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h3 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h3 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h3 |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h3 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h3 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h3 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h3 |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h3 |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ORCL` | 9 | 2026-09-11 @ $164.43 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; 🔵; ⚪; ret5=+9.0; combo leftover $1530.93; owner union_e_fresh_h3 |
| `DBI` | 259 | 2026-09-11 @ $5.91 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list probable,yday_gainer,yday_mover; ret5=-4.9; combo leftover $1530.93; owner union_e_fresh_h3 |
| `ADBE` | 6 | 2026-09-11 @ $242.17 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-12.1; combo leftover $1530.93; owner union_e_fresh_h3 |
| `CPRT` | 47 | 2026-09-11 @ $32.01 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $1530.93; owner union_e_fresh_h3 |
| `DSGX` | 21 | 2026-09-11 @ $71.71 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.3; combo leftover $1530.93; owner union_e_fresh_h3 |
| `KR` | 27 | 2026-09-11 @ $56.02 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.6; combo leftover $1530.93; owner union_e_fresh_h3 |
| `LPTH` | 163 | 2026-09-11 @ $9.37 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $1530.93; owner union_e_fresh_h3 |
| `REF` | 116 | 2026-09-11 @ $13.10 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.2; combo leftover $1530.93; owner union_e_fresh_h3 |
