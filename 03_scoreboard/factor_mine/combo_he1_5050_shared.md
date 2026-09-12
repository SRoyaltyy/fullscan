# Factor mine action — `combo_he1_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_hot_n4_h1/union_e_fresh_h1 w=0.5,0.5 net=priority

Cash book **+10.71%** ($11,072) · signal-only (no cash/fees) was —. Starts YES **7/21**. Fills 205 · skips 85 · realized $+1277.22.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_hot_n4_h1 50%, union_e_fresh_h1 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_hot_n4_h1 50%, union_e_fresh_h1 50%.
- Member: union_hot_n4_h1 (50% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $2,440.15.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `INO` | 3086 | — | $0.81 | +0.00 | $0.90 | +277.74 | +277.74 | +0.00 | +277.74 |
| 2026-08-13 | `VOR` | 113 | — | $22.01 | +0.00 | $23.29 | +144.64 | +144.64 | +0.00 | +144.64 |
| 2026-08-13 | `IREN` | 36 | — | $45.98 | +0.00 | $44.76 | -43.92 | -43.92 | +0.00 | -43.92 |
| 2026-08-13 | `TNDM` | 71 | — | $23.33 | +0.00 | $23.13 | -14.20 | -14.20 | +0.00 | -14.20 |
| 2026-08-13 | `TPG` | 32 | — | $50.62 | +0.00 | $54.62 | +127.90 | +127.90 | +0.00 | +127.90 |
| 2026-08-14 | `INO` | 3086 | $0.90 | $0.93 | +92.58 | — | +0.00 | +92.58 | +370.32 | — |
| 2026-08-14 | `VOR` | 113 | $23.29 | $23.33 | +4.52 | — | +0.00 | +4.52 | +149.16 | — |
| 2026-08-14 | `IREN` | 36 | $44.76 | $44.09 | -24.12 | — | +0.00 | -24.12 | -68.04 | — |
| 2026-08-14 | `TNDM` | 71 | $23.13 | $22.92 | -14.91 | — | +0.00 | -14.91 | -29.11 | — |
| 2026-08-14 | `TPG` | 32 | $54.62 | $55.29 | +21.44 | — | +0.00 | +21.44 | +149.34 | — |
| 2026-08-14 | `ARX` | 33 | — | $19.57 | +0.00 | $19.58 | +0.33 | +0.33 | +0.00 | +0.33 |
| 2026-08-14 | `AIRO` | 58 | — | $11.12 | +0.00 | $9.57 | -89.90 | -89.90 | +0.00 | -89.90 |
| 2026-08-14 | `BTBT` | 436 | — | $1.50 | +0.00 | $1.57 | +30.52 | +30.52 | +0.00 | +30.52 |
| 2026-08-14 | `MH` | 48 | — | $13.55 | +0.00 | $13.10 | -21.60 | -21.60 | +0.00 | -21.60 |
| 2026-08-14 | `CLBT` | 60 | — | $10.83 | +0.00 | $11.14 | +18.60 | +18.60 | +0.00 | +18.60 |
| 2026-08-14 | `EU` | 555 | — | $1.18 | +0.00 | $1.21 | +16.65 | +16.65 | +0.00 | +16.65 |
| 2026-08-14 | `LUNR` | 34 | — | $19.17 | +0.00 | $19.01 | -5.44 | -5.44 | +0.00 | -5.44 |
| 2026-08-14 | `NMAX` | 66 | — | $9.89 | +0.00 | $10.87 | +64.35 | +64.35 | +0.00 | +64.35 |
| 2026-08-14 | `QMCO` | 106 | — | $24.68 | +0.00 | $26.11 | +151.58 | +151.58 | +0.00 | +151.58 |
| 2026-08-14 | `ZENA` | 1189 | — | $2.20 | +0.00 | $2.14 | -71.34 | -71.34 | +0.00 | -71.34 |
| 2026-08-17 | `ARX` | 33 | $19.58 | $19.57 | -0.33 | — | +0.00 | -0.33 | +0.00 | — |
| 2026-08-17 | `AIRO` | 58 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -89.90 | — |
| 2026-08-17 | `BTBT` | 436 | $1.57 | $1.52 | -21.80 | — | +0.00 | -21.80 | +8.72 | — |
| 2026-08-17 | `MH` | 48 | $13.10 | $13.16 | +2.88 | — | +0.00 | +2.88 | -18.72 | — |
| 2026-08-17 | `CLBT` | 60 | $11.14 | $11.19 | +3.00 | — | +0.00 | +3.00 | +21.60 | — |
| 2026-08-17 | `EU` | 555 | $1.21 | $1.21 | +0.00 | — | +0.00 | +0.00 | +16.65 | — |
| 2026-08-17 | `LUNR` | 34 | $19.01 | $20.25 | +42.16 | — | +0.00 | +42.16 | +36.72 | — |
| 2026-08-17 | `NMAX` | 66 | $10.87 | $10.97 | +6.60 | — | +0.00 | +6.60 | +70.95 | — |
| 2026-08-17 | `QMCO` | 106 | $26.11 | $24.83 | -135.68 | — | +0.00 | -135.68 | +15.90 | — |
| 2026-08-17 | `ZENA` | 1189 | $2.14 | $2.08 | -65.40 | — | +0.00 | -65.40 | -136.74 | — |
| 2026-08-17 | `XHG` | 615 | — | $4.19 | +0.00 | $3.91 | -172.20 | -172.20 | +0.00 | -172.20 |
| 2026-08-17 | `CAPR` | 375 | — | $6.87 | +0.00 | $7.45 | +217.50 | +217.50 | +0.00 | +217.50 |
| 2026-08-17 | `STDN` | 189 | — | $13.64 | +0.00 | $13.31 | -62.37 | -62.37 | +0.00 | -62.37 |
| 2026-08-17 | `HTFL` | 62 | — | $41.23 | +0.00 | $41.94 | +44.02 | +44.02 | +0.00 | +44.02 |
| 2026-08-18 | `XHG` | 615 | $3.91 | $3.94 | +18.45 | — | +0.00 | +18.45 | -153.75 | — |
| 2026-08-18 | `CAPR` | 375 | $7.45 | $7.50 | +18.75 | $7.08 | -157.50 | -138.75 | +236.25 | +78.75 |
| 2026-08-18 | `STDN` | 189 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -62.37 | — |
| 2026-08-18 | `HTFL` | 62 | $41.94 | $41.50 | -27.28 | — | +0.00 | -27.28 | +16.74 | — |
| 2026-08-19 | `CAPR` | 375 | $7.08 | $7.19 | +41.25 | — | +0.00 | +41.25 | +120.00 | — |
| 2026-08-20 | `EL` | 6 | — | $97.43 | +0.00 | $96.15 | -7.68 | -7.68 | +0.00 | -7.68 |
| 2026-08-20 | `TOYO` | 143 | — | $4.43 | +0.00 | $4.51 | +12.15 | +12.15 | +0.00 | +12.15 |
| 2026-08-20 | `DVLT` | 2125 | — | $0.30 | +0.00 | $0.32 | +42.50 | +42.50 | +0.00 | +42.50 |
| 2026-08-20 | `AAP` | 13 | — | $46.85 | +0.00 | $42.39 | -57.98 | -57.98 | +0.00 | -57.98 |
| 2026-08-20 | `AEG` | 70 | — | $9.01 | +0.00 | $9.01 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `ALVO` | 163 | — | $3.89 | +0.00 | $4.27 | +61.94 | +61.94 | +0.00 | +61.94 |
| 2026-08-20 | `ATAT` | 18 | — | $34.05 | +0.00 | $34.25 | +3.60 | +3.60 | +0.00 | +3.60 |
| 2026-08-20 | `ATHM` | 28 | — | $22.44 | +0.00 | $22.12 | -8.96 | -8.96 | +0.00 | -8.96 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `CYPH` | 1131 | — | $1.15 | +0.00 | $1.19 | +45.24 | +45.24 | +0.00 | +45.24 |
| 2026-08-20 | `ABCL` | 110 | — | $11.81 | +0.00 | $11.57 | -26.95 | -26.95 | +0.00 | -26.95 |
| 2026-08-20 | `AZI` | 950 | — | $1.37 | +0.00 | $1.44 | +66.50 | +66.50 | +0.00 | +66.50 |
| 2026-08-21 | `EL` | 6 | $96.15 | $96.75 | +3.60 | — | +0.00 | +3.60 | -4.08 | — |
| 2026-08-21 | `TOYO` | 143 | $4.51 | $4.68 | +23.60 | — | +0.00 | +23.60 | +35.75 | — |
| 2026-08-21 | `DVLT` | 2125 | $0.32 | $0.31 | -21.25 | — | +0.00 | -21.25 | +21.25 | — |
| 2026-08-21 | `AAP` | 13 | $42.39 | $42.41 | +0.26 | $42.58 | +2.21 | +2.47 | -57.72 | -55.51 |
| 2026-08-21 | `AEG` | 70 | $9.01 | $9.04 | +2.10 | — | +0.00 | +2.10 | +2.10 | — |
| 2026-08-21 | `ALVO` | 163 | $4.27 | $4.32 | +8.15 | — | +0.00 | +8.15 | +70.09 | — |
| 2026-08-21 | `ATAT` | 18 | $34.25 | $34.31 | +1.08 | — | +0.00 | +1.08 | +4.68 | — |
| 2026-08-21 | `ATHM` | 28 | $22.12 | $22.20 | +2.24 | — | +0.00 | +2.24 | -6.72 | — |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | $145.13 | +96.16 | +94.48 | -136.24 | -40.08 |
| 2026-08-21 | `CYPH` | 1131 | $1.19 | $1.32 | +147.03 | $1.42 | +113.10 | +260.13 | +192.27 | +305.37 |
| 2026-08-21 | `ABCL` | 110 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -26.95 | — |
| 2026-08-21 | `AZI` | 950 | $1.44 | $1.46 | +19.00 | — | +0.00 | +19.00 | +85.50 | — |
| 2026-08-21 | `FUTU` | 4 | — | $115.18 | +0.00 | $123.64 | +33.84 | +33.84 | +0.00 | +33.84 |
| 2026-08-21 | `WMT` | 4 | — | $103.69 | +0.00 | $103.70 | +0.04 | +0.04 | +0.00 | +0.04 |
| 2026-08-21 | `BEKE` | 28 | — | $17.93 | +0.00 | $17.75 | -5.18 | -5.18 | +0.00 | -5.18 |
| 2026-08-21 | `BJ` | 5 | — | $93.98 | +0.00 | $96.42 | +12.20 | +12.20 | +0.00 | +12.20 |
| 2026-08-21 | `BKE` | 11 | — | $43.08 | +0.00 | $43.81 | +8.03 | +8.03 | +0.00 | +8.03 |
| 2026-08-21 | `PSEC` | 222 | — | $2.30 | +0.00 | $2.33 | +6.66 | +6.66 | +0.00 | +6.66 |
| 2026-08-21 | `XHG` | 482 | — | $4.49 | +0.00 | $4.41 | -38.56 | -38.56 | +0.00 | -38.56 |
| 2026-08-21 | `CAPR` | 316 | — | $6.81 | +0.00 | $6.29 | -164.32 | -164.32 | +0.00 | -164.32 |
| 2026-08-24 | `AAP` | 13 | $42.58 | $43.05 | +6.11 | — | +0.00 | +6.11 | -49.40 | — |
| 2026-08-24 | `MRNA` | 8 | $145.13 | $142.70 | -19.44 | — | +0.00 | -19.44 | -59.52 | — |
| 2026-08-24 | `CYPH` | 1131 | $1.42 | $1.83 | +463.71 | — | +0.00 | +463.71 | +769.08 | — |
| 2026-08-24 | `FUTU` | 4 | $123.64 | $121.00 | -10.56 | — | +0.00 | -10.56 | +23.28 | — |
| 2026-08-24 | `WMT` | 4 | $103.70 | $104.14 | +1.76 | — | +0.00 | +1.76 | +1.80 | — |
| 2026-08-24 | `BEKE` | 28 | $17.75 | $18.05 | +8.54 | — | +0.00 | +8.54 | +3.36 | — |
| 2026-08-24 | `BJ` | 5 | $96.42 | $97.02 | +3.00 | — | +0.00 | +3.00 | +15.20 | — |
| 2026-08-24 | `BKE` | 11 | $43.81 | $44.22 | +4.51 | — | +0.00 | +4.51 | +12.54 | — |
| 2026-08-24 | `PSEC` | 222 | $2.33 | $2.34 | +2.22 | — | +0.00 | +2.22 | +8.88 | — |
| 2026-08-24 | `XHG` | 482 | $4.41 | $4.32 | -43.38 | — | +0.00 | -43.38 | -81.94 | — |
| 2026-08-24 | `CAPR` | 316 | $6.29 | $8.03 | +549.84 | — | +0.00 | +549.84 | +385.52 | — |
| 2026-08-25 | `BMO` | 4 | — | $175.01 | +0.00 | $173.46 | -6.20 | -6.20 | +0.00 | -6.20 |
| 2026-08-25 | `BNS` | 7 | — | $88.94 | +0.00 | $93.10 | +29.12 | +29.12 | +0.00 | +29.12 |
| 2026-08-25 | `BZ` | 46 | — | $15.28 | +0.00 | $16.29 | +46.46 | +46.46 | +0.00 | +46.46 |
| 2026-08-25 | `DKS` | 4 | — | $142.36 | +0.00 | $124.31 | -72.20 | -72.20 | +0.00 | -72.20 |
| 2026-08-25 | `EH` | 137 | — | $5.10 | +0.00 | $4.83 | -36.99 | -36.99 | +0.00 | -36.99 |
| 2026-08-25 | `GFI` | 14 | — | $47.89 | +0.00 | $48.87 | +13.72 | +13.72 | +0.00 | +13.72 |
| 2026-08-25 | `GRRR` | 50 | — | $13.92 | +0.00 | $14.04 | +6.00 | +6.00 | +0.00 | +6.00 |
| 2026-08-25 | `SHMD` | 154 | — | $4.54 | +0.00 | $3.42 | -173.25 | -173.25 | +0.00 | -173.25 |
| 2026-08-25 | `REAX` | 60 | — | $24.11 | +0.00 | $28.43 | +259.20 | +259.20 | +0.00 | +259.20 |
| 2026-08-25 | `CYPH` | 940 | — | $1.56 | +0.00 | $1.64 | +75.20 | +75.20 | +0.00 | +75.20 |
| 2026-08-25 | `XHG` | 360 | — | $4.07 | +0.00 | $4.02 | -18.00 | -18.00 | +0.00 | -18.00 |
| 2026-08-25 | `ASST` | 77 | — | $19.04 | +0.00 | $21.39 | +180.95 | +180.95 | +0.00 | +180.95 |
| 2026-08-26 | `BMO` | 4 | $173.46 | $173.22 | -0.96 | — | +0.00 | -0.96 | -7.16 | — |
| 2026-08-26 | `BNS` | 7 | $93.10 | $92.65 | -3.15 | — | +0.00 | -3.15 | +25.97 | — |
| 2026-08-26 | `BZ` | 46 | $16.29 | $16.77 | +22.08 | $18.84 | +95.22 | +117.30 | +68.54 | +163.76 |
| 2026-08-26 | `DKS` | 4 | $124.31 | $121.87 | -9.76 | $129.66 | +31.16 | +21.40 | -81.96 | -50.80 |
| 2026-08-26 | `EH` | 137 | $4.83 | $4.77 | -8.22 | — | +0.00 | -8.22 | -45.21 | — |
| 2026-08-26 | `GFI` | 14 | $48.87 | $48.24 | -8.82 | — | +0.00 | -8.82 | +4.90 | — |
| 2026-08-26 | `GRRR` | 50 | $14.04 | $14.03 | -0.50 | — | +0.00 | -0.50 | +5.50 | — |
| 2026-08-26 | `SHMD` | 154 | $3.42 | $3.38 | -6.16 | — | +0.00 | -6.16 | -179.41 | — |
| 2026-08-26 | `REAX` | 60 | $28.43 | $26.61 | -109.20 | — | +0.00 | -109.20 | +150.00 | — |
| 2026-08-26 | `CYPH` | 940 | $1.64 | $1.60 | -37.60 | — | +0.00 | -37.60 | +37.60 | — |
| 2026-08-26 | `XHG` | 360 | $4.02 | $3.81 | -75.60 | $4.06 | +90.00 | +14.40 | -93.60 | -3.60 |
| 2026-08-26 | `ASST` | 77 | $21.39 | $20.72 | -51.59 | — | +0.00 | -51.59 | +129.36 | — |
| 2026-08-26 | `SLQT` | 1224 | — | $0.58 | +0.00 | $0.55 | -40.39 | -40.39 | +0.00 | -40.39 |
| 2026-08-26 | `TIGR` | 136 | — | $5.21 | +0.00 | $5.46 | +34.00 | +34.00 | +0.00 | +34.00 |
| 2026-08-26 | `ANF` | 5 | — | $131.37 | +0.00 | $147.75 | +81.90 | +81.90 | +0.00 | +81.90 |
| 2026-08-26 | `BBWI` | 39 | — | $18.26 | +0.00 | $18.90 | +24.96 | +24.96 | +0.00 | +24.96 |
| 2026-08-26 | `BOX` | 20 | — | $34.30 | +0.00 | $33.39 | -18.20 | -18.20 | +0.00 | -18.20 |
| 2026-08-26 | `DY` | 2 | — | $326.91 | +0.00 | $310.91 | -32.00 | -32.00 | +0.00 | -32.00 |
| 2026-08-26 | `BYND` | 104 | — | $14.11 | +0.00 | $14.25 | +14.56 | +14.56 | +0.00 | +14.56 |
| 2026-08-26 | `USDE` | 253 | — | $5.81 | +0.00 | $5.98 | +43.01 | +43.01 | +0.00 | +43.01 |
| 2026-08-26 | `SUJA` | 156 | — | $9.39 | +0.00 | $9.44 | +7.80 | +7.80 | +0.00 | +7.80 |
| 2026-08-27 | `BZ` | 46 | $18.84 | $18.50 | -15.64 | — | +0.00 | -15.64 | +148.12 | — |
| 2026-08-27 | `DKS` | 4 | $129.66 | $128.73 | -3.72 | — | +0.00 | -3.72 | -54.52 | — |
| 2026-08-27 | `XHG` | 360 | $4.06 | $4.06 | +0.00 | — | +0.00 | +0.00 | -3.60 | — |
| 2026-08-27 | `SLQT` | 1224 | $0.55 | $0.53 | -24.48 | — | +0.00 | -24.48 | -64.87 | — |
| 2026-08-27 | `TIGR` | 136 | $5.46 | $5.49 | +4.08 | — | +0.00 | +4.08 | +38.08 | — |
| 2026-08-27 | `ANF` | 5 | $147.75 | $144.70 | -15.25 | — | +0.00 | -15.25 | +66.65 | — |
| 2026-08-27 | `BBWI` | 39 | $18.90 | $18.69 | -8.19 | — | +0.00 | -8.19 | +16.77 | — |
| 2026-08-27 | `BOX` | 20 | $33.39 | $33.79 | +8.00 | — | +0.00 | +8.00 | -10.20 | — |
| 2026-08-27 | `DY` | 2 | $310.91 | $314.90 | +7.98 | — | +0.00 | +7.98 | -24.02 | — |
| 2026-08-27 | `BYND` | 104 | $14.25 | $14.20 | -5.20 | — | +0.00 | -5.20 | +9.36 | — |
| 2026-08-27 | `USDE` | 253 | $5.98 | $6.50 | +131.56 | — | +0.00 | +131.56 | +174.57 | — |
| 2026-08-27 | `SUJA` | 156 | $9.44 | $9.41 | -4.68 | — | +0.00 | -4.68 | +3.12 | — |
| 2026-08-27 | `NVDA` | 25 | — | $222.86 | +0.00 | $227.98 | +128.00 | +128.00 | +0.00 | +128.00 |
| 2026-08-27 | `SLI` | 573 | — | $2.60 | +0.00 | $2.64 | +22.92 | +22.92 | +0.00 | +22.92 |
| 2026-08-27 | `RRC` | 35 | — | $41.44 | +0.00 | $41.64 | +7.00 | +7.00 | +0.00 | +7.00 |
| 2026-08-27 | `PGY` | 64 | — | $22.93 | +0.00 | $23.26 | +21.12 | +21.12 | +0.00 | +21.12 |
| 2026-08-27 | `CRK` | 103 | — | $14.42 | +0.00 | $14.62 | +20.60 | +20.60 | +0.00 | +20.60 |
| 2026-08-28 | `NVDA` | 25 | $227.98 | $227.36 | -15.50 | — | +0.00 | -15.50 | +112.50 | — |
| 2026-08-28 | `SLI` | 573 | $2.64 | $2.68 | +22.92 | — | +0.00 | +22.92 | +45.84 | — |
| 2026-08-28 | `RRC` | 35 | $41.64 | $41.74 | +3.50 | — | +0.00 | +3.50 | +10.50 | — |
| 2026-08-28 | `PGY` | 64 | $23.26 | $23.21 | -3.20 | — | +0.00 | -3.20 | +17.92 | — |
| 2026-08-28 | `CRK` | 103 | $14.62 | $14.63 | +1.03 | — | +0.00 | +1.03 | +21.63 | — |
| 2026-08-28 | `ADSK` | 2 | — | $261.16 | +0.00 | $260.66 | -1.00 | -1.00 | +0.00 | -1.00 |
| 2026-08-28 | `BBAR` | 48 | — | $15.01 | +0.00 | $14.47 | -25.92 | -25.92 | +0.00 | -25.92 |
| 2026-08-28 | `ESTC` | 7 | — | $103.89 | +0.00 | $99.91 | -27.86 | -27.86 | +0.00 | -27.86 |
| 2026-08-28 | `FINV` | 188 | — | $3.88 | +0.00 | $3.40 | -90.24 | -90.24 | +0.00 | -90.24 |
| 2026-08-28 | `FRO` | 16 | — | $44.40 | +0.00 | $44.19 | -3.36 | -3.36 | +0.00 | -3.36 |
| 2026-08-28 | `GAP` | 29 | — | $24.69 | +0.00 | $23.48 | -35.09 | -35.09 | +0.00 | -35.09 |
| 2026-08-28 | `HAFN` | 87 | — | $8.35 | +0.00 | $8.47 | +10.44 | +10.44 | +0.00 | +10.44 |
| 2026-08-28 | `IREN` | 19 | — | $37.65 | +0.00 | $35.45 | -41.71 | -41.71 | +0.00 | -41.71 |
| 2026-08-28 | `BYND` | 109 | — | $14.00 | +0.00 | $13.86 | -15.26 | -15.26 | +0.00 | -15.26 |
| 2026-08-28 | `CAPR` | 157 | — | $9.73 | +0.00 | $9.59 | -21.98 | -21.98 | +0.00 | -21.98 |
| 2026-08-28 | `MRNA` | 11 | — | $137.19 | +0.00 | $137.99 | +8.80 | +8.80 | +0.00 | +8.80 |
| 2026-08-28 | `ANF` | 10 | — | $146.07 | +0.00 | $148.42 | +23.50 | +23.50 | +0.00 | +23.50 |
| 2026-08-31 | `ADSK` | 2 | $260.66 | $257.71 | -5.90 | — | +0.00 | -5.90 | -6.90 | — |
| 2026-08-31 | `BBAR` | 48 | $14.47 | $14.88 | +19.68 | — | +0.00 | +19.68 | -6.24 | — |
| 2026-08-31 | `ESTC` | 7 | $99.91 | $98.00 | -13.37 | — | +0.00 | -13.37 | -41.23 | — |
| 2026-08-31 | `FINV` | 188 | $3.40 | $3.39 | -1.88 | — | +0.00 | -1.88 | -92.12 | — |
| 2026-08-31 | `FRO` | 16 | $44.19 | $44.85 | +10.56 | — | +0.00 | +10.56 | +7.20 | — |
| 2026-08-31 | `GAP` | 29 | $23.48 | $22.98 | -14.50 | — | +0.00 | -14.50 | -49.59 | — |
| 2026-08-31 | `HAFN` | 87 | $8.47 | $8.53 | +5.22 | — | +0.00 | +5.22 | +15.66 | — |
| 2026-08-31 | `IREN` | 19 | $35.45 | $35.81 | +6.84 | — | +0.00 | +6.84 | -34.87 | — |
| 2026-08-31 | `BYND` | 109 | $13.86 | $13.81 | -5.45 | $13.30 | -55.59 | -61.04 | -20.71 | -76.30 |
| 2026-08-31 | `CAPR` | 157 | $9.59 | $9.50 | -14.13 | — | +0.00 | -14.13 | -36.11 | — |
| 2026-08-31 | `MRNA` | 11 | $137.99 | $134.10 | -42.79 | — | +0.00 | -42.79 | -33.99 | — |
| 2026-08-31 | `ANF` | 10 | $148.42 | $148.03 | -3.90 | — | +0.00 | -3.90 | +19.60 | — |
| 2026-09-01 | `BYND` | 109 | $13.30 | $13.04 | -28.34 | — | +0.00 | -28.34 | -104.64 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AI` | 65 | — | $10.74 | +0.00 | $10.90 | +10.08 | +10.08 | +0.00 | +10.08 |
| 2026-09-03 | `AVGO` | 2 | — | $351.74 | +0.00 | $357.16 | +10.84 | +10.84 | +0.00 | +10.84 |
| 2026-09-03 | `CHPT` | 102 | — | $6.90 | +0.00 | $9.08 | +222.36 | +222.36 | +0.00 | +222.36 |
| 2026-09-03 | `CIEN` | 1 | — | $354.49 | +0.00 | $317.46 | -37.03 | -37.03 | +0.00 | -37.03 |
| 2026-09-03 | `CPB` | 31 | — | $22.32 | +0.00 | $22.13 | -5.89 | -5.89 | +0.00 | -5.89 |
| 2026-09-03 | `FIVE` | 2 | — | $257.00 | +0.00 | $239.96 | -34.08 | -34.08 | +0.00 | -34.08 |
| 2026-09-03 | `HPE` | 14 | — | $47.60 | +0.00 | $54.44 | +95.76 | +95.76 | +0.00 | +95.76 |
| 2026-09-03 | `MEI` | 46 | — | $15.09 | +0.00 | $15.32 | +10.58 | +10.58 | +0.00 | +10.58 |
| 2026-09-03 | `GPRO` | 878 | — | $1.78 | +0.00 | $1.39 | -342.42 | -342.42 | +0.00 | -342.42 |
| 2026-09-03 | `REAX` | 84 | — | $18.40 | +0.00 | $18.40 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `CNH` | 113 | — | $13.71 | +0.00 | $13.84 | +14.69 | +14.69 | +0.00 | +14.69 |
| 2026-09-03 | `MMED` | 65 | — | $23.88 | +0.00 | $23.84 | -2.60 | -2.60 | +0.00 | -2.60 |
| 2026-09-04 | `AI` | 65 | $10.90 | $10.91 | +0.65 | — | +0.00 | +0.65 | +10.73 | — |
| 2026-09-04 | `AVGO` | 2 | $357.16 | $359.70 | +5.08 | — | +0.00 | +5.08 | +15.92 | — |
| 2026-09-04 | `CHPT` | 102 | $9.08 | $9.28 | +20.40 | — | +0.00 | +20.40 | +242.76 | — |
| 2026-09-04 | `CIEN` | 1 | $317.46 | $321.67 | +4.21 | — | +0.00 | +4.21 | -32.82 | — |
| 2026-09-04 | `CPB` | 31 | $22.13 | $22.10 | -0.93 | — | +0.00 | -0.93 | -6.82 | — |
| 2026-09-04 | `FIVE` | 2 | $239.96 | $238.88 | -2.16 | — | +0.00 | -2.16 | -36.24 | — |
| 2026-09-04 | `HPE` | 14 | $54.44 | $53.85 | -8.26 | — | +0.00 | -8.26 | +87.50 | — |
| 2026-09-04 | `MEI` | 46 | $15.32 | $15.34 | +0.92 | — | +0.00 | +0.92 | +11.50 | — |
| 2026-09-04 | `GPRO` | 878 | $1.39 | $1.48 | +79.02 | $1.70 | +193.16 | +272.18 | -263.40 | -70.24 |
| 2026-09-04 | `REAX` | 84 | $18.40 | $18.15 | -21.00 | — | +0.00 | -21.00 | -21.00 | — |
| 2026-09-04 | `CNH` | 113 | $13.84 | $13.89 | +5.65 | — | +0.00 | +5.65 | +20.34 | — |
| 2026-09-04 | `MMED` | 65 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.60 | — |
| 2026-09-04 | `AMBA` | 9 | — | $63.18 | +0.00 | $62.89 | -2.61 | -2.61 | +0.00 | -2.61 |
| 2026-09-04 | `ASAN` | 71 | — | $8.74 | +0.00 | $8.81 | +4.97 | +4.97 | +0.00 | +4.97 |
| 2026-09-04 | `DOCU` | 9 | — | $68.52 | +0.00 | $68.41 | -0.99 | -0.99 | +0.00 | -0.99 |
| 2026-09-04 | `DOMO` | 172 | — | $3.62 | +0.00 | $3.88 | +45.58 | +45.58 | +0.00 | +45.58 |
| 2026-09-04 | `GWRE` | 3 | — | $167.55 | +0.00 | $162.42 | -15.39 | -15.39 | +0.00 | -15.39 |
| 2026-09-04 | `IOT` | 13 | — | $44.90 | +0.00 | $40.20 | -61.10 | -61.10 | +0.00 | -61.10 |
| 2026-09-04 | `LULU` | 6 | — | $98.15 | +0.00 | $100.61 | +14.76 | +14.76 | +0.00 | +14.76 |
| 2026-09-04 | `MAMA` | 39 | — | $15.70 | +0.00 | $15.16 | -21.06 | -21.06 | +0.00 | -21.06 |
| 2026-09-04 | `ASST` | 69 | — | $25.18 | +0.00 | $27.14 | +135.24 | +135.24 | +0.00 | +135.24 |
| 2026-09-04 | `USDE` | 221 | — | $7.87 | +0.00 | $7.93 | +13.26 | +13.26 | +0.00 | +13.26 |
| 2026-09-04 | `DFDV` | 301 | — | $5.79 | +0.00 | $5.87 | +24.08 | +24.08 | +0.00 | +24.08 |
| 2026-09-08 | `GPRO` | 878 | $1.70 | $1.56 | -118.53 | — | +0.00 | -118.53 | -188.77 | — |
| 2026-09-08 | `AMBA` | 9 | $62.89 | $63.83 | +8.46 | — | +0.00 | +8.46 | +5.85 | — |
| 2026-09-08 | `ASAN` | 71 | $8.81 | $8.73 | -5.68 | — | +0.00 | -5.68 | -0.71 | — |
| 2026-09-08 | `DOCU` | 9 | $68.41 | $67.05 | -12.24 | — | +0.00 | -12.24 | -13.23 | — |
| 2026-09-08 | `DOMO` | 172 | $3.88 | $3.84 | -6.88 | — | +0.00 | -6.88 | +38.70 | — |
| 2026-09-08 | `GWRE` | 3 | $162.42 | $160.52 | -5.70 | — | +0.00 | -5.70 | -21.09 | — |
| 2026-09-08 | `IOT` | 13 | $40.20 | $39.56 | -8.32 | — | +0.00 | -8.32 | -69.42 | — |
| 2026-09-08 | `LULU` | 6 | $100.61 | $100.58 | -0.18 | — | +0.00 | -0.18 | +14.58 | — |
| 2026-09-08 | `MAMA` | 39 | $15.16 | $15.20 | +1.56 | — | +0.00 | +1.56 | -19.50 | — |
| 2026-09-08 | `ASST` | 69 | $27.14 | $26.44 | -48.30 | — | +0.00 | -48.30 | +86.94 | — |
| 2026-09-08 | `USDE` | 221 | $7.93 | $7.76 | -37.57 | — | +0.00 | -37.57 | -24.31 | — |
| 2026-09-08 | `DFDV` | 301 | $5.87 | $5.81 | -18.06 | — | +0.00 | -18.06 | +6.02 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `CPRT` | 22 | — | $32.01 | +0.00 | $29.95 | -45.32 | -45.32 | +0.00 | -45.32 |
| 2026-09-11 | `LPTH` | 75 | — | $9.37 | +0.00 | $9.20 | -12.75 | -12.75 | +0.00 | -12.75 |
| 2026-09-11 | `INDP` | 913 | — | $2.70 | +0.00 | $2.77 | +63.91 | +63.91 | +0.00 | +63.91 |
| 2026-09-11 | `IRD` | 400 | — | $6.16 | +0.00 | $6.04 | -48.00 | -48.00 | +0.00 | -48.00 |
| 2026-09-11 | `CYPH` | 1032 | — | $2.39 | +0.00 | $2.27 | -129.00 | -129.00 | +0.00 | -129.00 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +492.16 | INO, VOR, IREN, TNDM, TPG | — | $38.59 | $10,449.19 | INO×3086, VOR×113, IREN×36, TNDM×71, TPG×32 |
| 2026-08-14 | +5.50 | $38.59 | INO×3086, VOR×113, IREN×36, TNDM×71, TPG×32 | $10,528.70 | +79.51 | +93.75 | ARX, AIRO, BTBT, MH, CLBT, EU, LUNR, NMAX, QMCO, ZENA | INO, VOR, IREN, TNDM, TPG | $1.51 | $10,531.86 | ARX×33, AIRO×58, BTBT×436, MH×48, CLBT×60, EU×555, LUNR×34, NMAX×66, QMCO×106, ZENA×1189 |
| 2026-08-17 | +2.25 | $1.51 | ARX×33, AIRO×58, BTBT×436, MH×48, CLBT×60, EU×555, LUNR×34, NMAX×66, QMCO×106, ZENA×1189 | $10,363.29 | -168.57 | +26.95 | XHG, CAPR, STDN, HTFL | ARX, AIRO, BTBT, MH, CLBT, EU, LUNR, NMAX, QMCO, ZENA | $14.64 | $10,328.91 | XHG×615, CAPR×375, STDN×189, HTFL×62 |
| 2026-08-18 | -6.20 | $14.64 | XHG×615, CAPR×375, STDN×189, HTFL×62 | $10,338.83 | +9.92 | -157.50 | — | XHG, STDN, HTFL | $7,513.46 | $10,168.46 | CAPR×375 |
| 2026-08-19 | -7.20 | $7,513.46 | CAPR×375 | $10,209.71 | +41.25 | +0.00 | — | CAPR | $10,204.79 | $10,204.79 | — |
| 2026-08-20 | +1.12 | $10,204.79 | — | $10,204.79 | +0.00 | -4.20 | EL, TOYO, DVLT, AAP, AEG, ALVO, ATAT, ATHM, MRNA, CYPH, ABCL, AZI | — | $72.08 | $10,141.41 | EL×6, TOYO×143, DVLT×2125, AAP×13, AEG×70, ALVO×163, ATAT×18, ATHM×28, MRNA×8, CYPH×1131, ABCL×110, AZI×950 |
| 2026-08-21 | +3.25 | $72.08 | EL×6, TOYO×143, DVLT×2125, AAP×13, AEG×70, ALVO×163, ATAT×18, ATHM×28, MRNA×8, CYPH×1131, ABCL×110, AZI×950 | $10,325.54 | +184.13 | +64.18 | FUTU, WMT, BEKE, BJ, BKE, PSEC, XHG, CAPR | EL, TOYO, DVLT, AEG, ALVO, ATAT, ATHM, ABCL, AZI | $3.49 | $10,324.98 | AAP×13, MRNA×8, CYPH×1131, FUTU×4, WMT×4, BEKE×28, BJ×5, BKE×11, PSEC×222, XHG×482, CAPR×316 |
| 2026-08-24 | -5.17 | $3.49 | AAP×13, MRNA×8, CYPH×1131, FUTU×4, WMT×4, BEKE×28, BJ×5, BKE×11, PSEC×222, XHG×482, CAPR×316 | $11,291.29 | +966.31 | +0.00 | — | AAP, MRNA, CYPH, FUTU, WMT, BEKE, BJ, BKE, PSEC, XHG, CAPR | $11,248.83 | $11,248.83 | — |
| 2026-08-25 | +1.80 | $11,248.83 | — | $11,248.83 | +0.00 | +304.01 | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD, REAX, CYPH, XHG, ASST | — | $6.19 | $11,514.51 | BMO×4, BNS×7, BZ×46, DKS×4, EH×137, GFI×14, GRRR×50, SHMD×154, REAX×60, CYPH×940, XHG×360, ASST×77 |
| 2026-08-26 | +2.02 | $6.19 | BMO×4, BNS×7, BZ×46, DKS×4, EH×137, GFI×14, GRRR×50, SHMD×154, REAX×60, CYPH×940, XHG×360, ASST×77 | $11,225.03 | -289.48 | +332.02 | SLQT, TIGR, ANF, BBWI, BOX, DY, BYND, USDE, SUJA | BMO, BNS, EH, GFI, GRRR, SHMD, REAX, CYPH, ASST | $2.05 | $11,497.74 | BZ×46, DKS×4, XHG×360, SLQT×1224, TIGR×136, ANF×5, BBWI×39, BOX×20, DY×2, BYND×104, USDE×253, SUJA×156 |
| 2026-08-27 | — | $2.05 | BZ×46, DKS×4, XHG×360, SLQT×1224, TIGR×136, ANF×5, BBWI×39, BOX×20, DY×2, BYND×104, USDE×253, SUJA×156 | $11,572.20 | +74.46 | +199.64 | NVDA, SLI, RRC, PGY, CRK | BZ, DKS, XHG, SLQT, TIGR, ANF, BBWI, BOX, DY, BYND, USDE, SUJA | $53.62 | $11,717.74 | NVDA×25, SLI×573, RRC×35, PGY×64, CRK×103 |
| 2026-08-28 | +0.75 | $53.62 | NVDA×25, SLI×573, RRC×35, PGY×64, CRK×103 | $11,726.49 | +8.75 | -219.68 | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN, BYND, CAPR, MRNA, ANF | NVDA, SLI, RRC, PGY, CRK | $93.31 | $11,464.62 | ADSK×2, BBAR×48, ESTC×7, FINV×188, FRO×16, GAP×29, HAFN×87, IREN×19, BYND×109, CAPR×157, MRNA×11, ANF×10 |
| 2026-08-31 | -5.85 | $93.31 | ADSK×2, BBAR×48, ESTC×7, FINV×188, FRO×16, GAP×29, HAFN×87, IREN×19, BYND×109, CAPR×157, MRNA×11, ANF×10 | $11,405.00 | -59.62 | -55.59 | — | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN, CAPR, MRNA, ANF | $9,875.83 | $11,325.53 | BYND×109 |
| 2026-09-01 | -6.30 | $9,875.83 | BYND×109 | $11,297.19 | -28.34 | +0.00 | — | BYND | $11,294.84 | $11,294.84 | — |
| 2026-09-02 | -3.83 | $11,294.84 | — | $11,294.84 | +0.00 | +0.00 | — | — | $11,294.84 | $11,294.84 | — |
| 2026-09-03 | -0.90 | $11,294.84 | — | $11,294.84 | +0.00 | -57.71 | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI, GPRO, REAX, CNH, MMED | — | $23.53 | $11,202.34 | AI×65, AVGO×2, CHPT×102, CIEN×1, CPB×31, FIVE×2, HPE×14, MEI×46, GPRO×878, REAX×84, CNH×113, MMED×65 |
| 2026-09-04 | +2.25 | $23.53 | AI×65, AVGO×2, CHPT×102, CIEN×1, CPB×31, FIVE×2, HPE×14, MEI×46, GPRO×878, REAX×84, CNH×113, MMED×65 | $11,285.92 | +83.58 | +329.90 | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA, ASST, USDE, DFDV | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI, REAX, CNH, MMED | $2.29 | $11,566.28 | GPRO×878, AMBA×9, ASAN×71, DOCU×9, DOMO×172, GWRE×3, IOT×13, LULU×6, MAMA×39, ASST×69, USDE×221, DFDV×301 |
| 2026-09-08 | -11.47 | $2.29 | GPRO×878, AMBA×9, ASAN×71, DOCU×9, DOMO×172, GWRE×3, IOT×13, LULU×6, MAMA×39, ASST×69, USDE×221, DFDV×301 | $11,314.84 | -251.44 | +0.00 | — | GPRO, AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA, ASST, USDE, DFDV | $11,277.22 | $11,277.22 | — |
| 2026-09-09 | -13.95 | $11,277.22 | — | $11,277.22 | +0.00 | +0.00 | — | — | $11,277.22 | $11,277.22 | — |
| 2026-09-10 | -13.28 | $11,277.22 | — | $11,277.22 | +0.00 | +0.00 | — | — | $11,277.22 | $11,277.22 | — |
| 2026-09-11 | +0.50 | $11,277.22 | — | $11,277.22 | +0.00 | -171.16 | CPRT, LPTH, INDP, IRD, CYPH | — | $2,440.15 | $11,071.54 | CPRT×22, LPTH×75, INDP×913, IRD×400, CYPH×1032 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 (unchanged overnight, no fees) · equity $10,000.00 vs prior close $10,000.00 (+0.00) | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3086 | $0.81 | $34.25 | — | $7,466.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; combo leftover $2500.00; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 113 | $22.01 | $2.33 | — | $4,976.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; combo leftover $2500.00; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 36 | $45.98 | $2.10 | — | $3,319.25 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; combo leftover $1658.88; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 71 | $23.33 | $2.20 | — | $1,660.62 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; combo leftover $1658.88; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $38.59 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; combo leftover $1658.88; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.59 | ▲ close $10,449.19 vs 09:30 $10,000.00 (session +492.16) | 16:00 close · cash $38.59 · equity $10,449.19 vs 09:30 $10,000.00 (+449.19; session marks +492.16) · 5 name(s) marked open→close (per-name table). INO×3086 09:30 $0.81 → close $0.90 +277.74; VOR×113 09:30 $22.01 → close $23.29 +144.64; IREN×36 09:30 $45.98 → close $44.76 -43.92; TNDM×71 09:30 $23.33 → close $23.13 -14.20; TPG×32 09:30 $50.62 → close $54.62 +127.90 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.59 | ▲ 09:30 equity $10,528.70 vs yday $10,449.19 (+79.51) | 09:30 open · cash $38.59 (unchanged overnight, no fees) · equity $10,528.70 vs prior close $10,449.19 (+79.51) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 3086 | $0.93 | $38.49 | $+297.57 | $2,870.07 | ▲ +297.57 after sell → book $10,490.20; vs 09:30 mark -38.50 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 113 | $23.33 | $2.37 | $+144.46 | $5,504.00 | ▲ +144.46 after sell → book $10,487.84; vs 09:30 mark -2.36 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 36 | $44.09 | $2.12 | $-72.26 | $7,089.11 | ▼ -72.26 after sell → book $10,485.71; vs 09:30 mark -2.13 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 71 | $22.92 | $2.23 | $-33.54 | $8,714.21 | ▼ -33.54 after sell → book $10,483.49; vs 09:30 mark -2.22 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 32 | $55.29 | $2.11 | $+145.14 | $10,481.38 | ▲ +145.14 after sell → book $10,481.38; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 33 | $19.57 | $2.09 | — | $9,833.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $655.09; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 58 | $11.12 | $2.16 | — | $9,186.35 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $655.09; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 436 | $1.50 | $5.62 | — | $8,526.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $655.09; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 48 | $13.55 | $2.13 | — | $7,874.20 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $655.09; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 60 | $10.83 | $2.17 | — | $7,222.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-30.1; combo leftover $655.09; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 555 | $1.18 | $7.16 | — | $6,560.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $655.09; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 34 | $19.17 | $2.09 | — | $5,906.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $655.09; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🔴 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 66 | $9.89 | $2.19 | — | $5,251.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; combo leftover $655.09; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 106 | $24.68 | $2.31 | — | $2,632.65 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; combo leftover $2625.52; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 1189 | $2.20 | $15.34 | — | $1.51 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; combo leftover $2625.52; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.51 | ▲ close $10,531.86 vs 09:30 $10,528.70 (session +93.75) | 16:00 close · cash $1.51 · equity $10,531.86 vs 09:30 $10,528.70 (+3.16; session marks +93.75) · 10 name(s) marked open→close (per-name table). ARX×33 09:30 $19.57 → close $19.58 +0.33; AIRO×58 09:30 $11.12 → close $9.57 -89.90; BTBT×436 09:30 $1.50 → close $1.57 +30.52; MH×48 09:30 $13.55 → close $13.10 -21.60; CLBT×60 09:30 $10.83 → close $11.14 +18.60; EU×555 09:30 $1.18 → close $1.21 +16.65; LUNR×34 09:30 $19.17 → close $19.01 -5.44; NMAX×66 09:30 $9.89 → close $10.87 +64.35; QMCO×106 09:30 $24.68 → close $26.11 +151.58; ZENA×1189 09:30 $2.20 → close $2.14 -71.34 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.51 | ▼ 09:30 equity $10,363.29 vs yday $10,531.86 (-168.57) | 09:30 open · cash $1.51 (unchanged overnight, no fees) · equity $10,363.29 vs prior close $10,531.86 (-168.57) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 33 | $19.57 | $2.11 | $-4.20 | $645.21 | ▼ -4.20 after sell → book $10,361.19; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 58 | $9.57 | $2.18 | $-94.25 | $1,198.09 | ▼ -94.25 after sell → book $10,359.00; vs 09:30 mark -2.19 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 436 | $1.52 | $5.71 | $-2.61 | $1,855.10 | ▼ -2.61 after sell → book $10,353.29; vs 09:30 mark -5.71 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 48 | $13.16 | $2.15 | $-23.01 | $2,484.63 | ▼ -23.01 after sell → book $10,351.14; vs 09:30 mark -2.15 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 60 | $11.19 | $2.19 | $+17.24 | $3,153.84 | ▲ +17.24 after sell → book $10,348.95; vs 09:30 mark -2.19 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `EU` | 555 | $1.21 | $7.26 | $+2.23 | $3,818.12 | ▲ +2.23 after sell → book $10,341.69; vs 09:30 mark -7.26 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 34 | $20.25 | $2.11 | $+32.52 | $4,504.51 | ▲ +32.52 after sell → book $10,339.58; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 66 | $10.97 | $2.21 | $+66.55 | $5,226.32 | ▲ +66.55 after sell → book $10,337.37; vs 09:30 mark -2.21 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 106 | $24.83 | $2.35 | $+11.25 | $7,855.96 | ▲ +11.25 after sell → book $10,335.02; vs 09:30 mark -2.35 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 1189 | $2.08 | $15.56 | $-167.63 | $10,319.47 | ▼ -167.63 after sell → book $10,319.47; vs 09:30 mark -15.55 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 615 | $4.19 | $7.93 | — | $7,734.68 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $2579.87; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 375 | $6.87 | $4.84 | — | $5,153.60 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $2579.87; owner union_hot_n4_h1 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 189 | $13.64 | $2.56 | — | $2,573.08 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $2579.87; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 62 | $41.23 | $2.18 | — | $14.64 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $2579.87; owner union_hot_n4_h1 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.64 | ▲ close $10,328.91 vs 09:30 $10,363.29 (session +26.95) | 16:00 close · cash $14.64 · equity $10,328.91 vs 09:30 $10,363.29 (-34.38; session marks +26.95) · 4 name(s) marked open→close (per-name table). XHG×615 09:30 $4.19 → close $3.91 -172.20; CAPR×375 09:30 $6.87 → close $7.45 +217.50; STDN×189 09:30 $13.64 → close $13.31 -62.37; HTFL×62 09:30 $41.23 → close $41.94 +44.02 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.64 | ▲ 09:30 equity $10,338.83 vs yday $10,328.91 (+9.92) | 09:30 open · cash $14.64 (unchanged overnight, no fees) · equity $10,338.83 vs prior close $10,328.91 (+9.92) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 615 | $3.94 | $8.05 | $-169.74 | $2,429.69 | ▼ -169.74 after sell → book $10,330.78; vs 09:30 mark -8.05 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 189 | $13.31 | $2.61 | $-67.54 | $4,942.67 | ▼ -67.54 after sell → book $10,328.17; vs 09:30 mark -2.61 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 62 | $41.50 | $2.21 | $+12.36 | $7,513.46 | ▲ +12.36 after sell → book $10,325.96; vs 09:30 mark -2.21 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,513.46 | ▼ close $10,168.46 vs 09:30 $10,338.83 (session -157.50) | 16:00 close · cash $7,513.46 · equity $10,168.46 vs 09:30 $10,338.83 (-170.37; session marks -157.50) · 1 name(s) marked open→close (per-name table). CAPR×375 09:30 $7.50 → close $7.08 -157.50 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,513.46 | ▲ 09:30 equity $10,209.71 vs yday $10,168.46 (+41.25) | 09:30 open · cash $7,513.46 (unchanged overnight, no fees) · equity $10,209.71 vs prior close $10,168.46 (+41.25) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 375 | $7.19 | $4.92 | $+110.24 | $10,204.79 | ▲ +110.24 after sell → book $10,204.79; vs 09:30 mark -4.92 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,204.79 | ▲ close $10,204.79 vs 09:30 $10,209.71 (session +0.00) | 16:00 close · cash $10,204.79 · no lots left · equity $10,204.79. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,204.79 | ▲ 09:30 equity $10,204.79 vs yday $10,204.79 (+0.00) | 09:30 open · cash $10,204.79 (unchanged overnight, no fees) · equity $10,204.79 vs prior close $10,204.79 (+0.00) | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 6 | $97.43 | $2.01 | — | $9,618.20 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $637.80; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 143 | $4.43 | $2.42 | — | $8,982.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $637.80; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 2125 | $0.30 | $12.75 | — | $8,332.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $637.80; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 13 | $46.85 | $2.03 | — | $7,720.96 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $637.80; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 70 | $9.01 | $2.20 | — | $7,088.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $637.80; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 163 | $3.89 | $2.48 | — | $6,451.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $637.80; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 18 | $34.05 | $2.04 | — | $5,836.57 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $637.80; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 28 | $22.44 | $2.07 | — | $5,206.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $637.80; owner union_e_fresh_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $4,003.04 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $1301.54; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1131 | $1.15 | $14.59 | — | $2,687.80 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $1301.54; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 110 | $11.81 | $2.32 | — | $1,385.83 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1301.54; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 950 | $1.37 | $12.26 | — | $72.08 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $1301.54; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.08 | ▼ close $10,141.41 vs 09:30 $10,204.79 (session -4.20) | 16:00 close · cash $72.08 · equity $10,141.41 vs 09:30 $10,204.79 (-63.38; session marks -4.20) · 12 name(s) marked open→close (per-name table). EL×6 09:30 $97.43 → close $96.15 -7.68; TOYO×143 09:30 $4.43 → close $4.51 +12.15; DVLT×2125 09:30 $0.30 → close $0.32 +42.50; AAP×13 09:30 $46.85 → close $42.39 -57.98; AEG×70 09:30 $9.01 → close $9.01 +0.00; ALVO×163 09:30 $3.89 → close $4.27 +61.94; ATAT×18 09:30 $34.05 → close $34.25 +3.60; ATHM×28 09:30 $22.44 → close $22.12 -8.96; MRNA×8 09:30 $150.14 → close $133.32 -134.56; CYPH×1131 09:30 $1.15 → close $1.19 +45.24; ABCL×110 09:30 $11.81 → close $11.57 -26.95; AZI×950 09:30 $1.37 → close $1.44 +66.50 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.08 | ▲ 09:30 equity $10,325.54 vs yday $10,141.41 (+184.13) | 09:30 open · cash $72.08 (unchanged overnight, no fees) · equity $10,325.54 vs prior close $10,141.41 (+184.13) | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 6 | $96.75 | $2.03 | $-8.12 | $650.55 | ▼ -8.12 after sell → book $10,323.51; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 143 | $4.68 | $2.45 | $+30.88 | $1,317.34 | ▲ +30.88 after sell → book $10,321.06; vs 09:30 mark -2.45 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 2125 | $0.31 | $13.33 | $-4.83 | $1,962.76 | ▼ -4.83 after sell → book $10,307.73; vs 09:30 mark -13.33 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 70 | $9.04 | $2.22 | $-2.32 | $2,593.34 | ▼ -2.32 after sell → book $10,305.51; vs 09:30 mark -2.22 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALVO` | 163 | $4.32 | $2.52 | $+65.09 | $3,294.98 | ▲ +65.09 after sell → book $10,302.99; vs 09:30 mark -2.52 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 18 | $34.31 | $2.06 | $+0.57 | $3,910.50 | ▲ +0.57 after sell → book $10,300.93; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 28 | $22.20 | $2.09 | $-10.89 | $4,530.01 | ▼ -10.89 after sell → book $10,298.84; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 110 | $11.57 | $2.35 | $-31.62 | $5,800.36 | ▼ -31.62 after sell → book $10,296.49; vs 09:30 mark -2.35 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 950 | $1.46 | $12.42 | $+60.82 | $7,174.93 | ▲ +60.82 after sell → book $10,284.06; vs 09:30 mark -12.43 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 4 | $115.18 | $2.00 | — | $6,712.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $512.50; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 4 | $103.69 | $2.00 | — | $6,295.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; ret5=-10.3; combo leftover $512.50; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 28 | $17.93 | $2.07 | — | $5,791.20 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $512.50; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 5 | $93.98 | $2.00 | — | $5,319.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; combo leftover $512.50; owner union_e_fresh_h1 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 11 | $43.08 | $2.02 | — | $4,843.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $512.50; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 222 | $2.30 | $2.86 | — | $4,329.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $512.50; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 482 | $4.49 | $6.22 | — | $2,159.53 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $2164.96; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 316 | $6.81 | $4.08 | — | $3.49 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $2164.96; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.49 | ▲ close $10,324.98 vs 09:30 $10,325.54 (session +64.18) | 16:00 close · cash $3.49 · equity $10,324.98 vs 09:30 $10,325.54 (-0.56; session marks +64.18) · 11 name(s) marked open→close (per-name table). AAP×13 09:30 $42.41 → close $42.58 +2.21; MRNA×8 09:30 $133.11 → close $145.13 +96.16; CYPH×1131 09:30 $1.32 → close $1.42 +113.10; FUTU×4 09:30 $115.18 → close $123.64 +33.84; WMT×4 09:30 $103.69 → close $103.70 +0.04; BEKE×28 09:30 $17.93 → close $17.75 -5.18; BJ×5 09:30 $93.98 → close $96.42 +12.20; BKE×11 09:30 $43.08 → close $43.81 +8.03; PSEC×222 09:30 $2.30 → close $2.33 +6.66; XHG×482 09:30 $4.49 → close $4.41 -38.56; CAPR×316 09:30 $6.81 → close $6.29 -164.32 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.49 | ▲ 09:30 equity $11,291.29 vs yday $10,324.98 (+966.31) | 09:30 open · cash $3.49 (unchanged overnight, no fees) · equity $11,291.29 vs prior close $10,324.98 (+966.31) | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 13 | $43.05 | $2.05 | $-53.48 | $561.09 | ▼ -53.48 after sell → book $11,289.24; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 8 | $142.70 | $2.03 | $-63.57 | $1,700.66 | ▼ -63.57 after sell → book $11,287.21; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1131 | $1.83 | $14.79 | $+739.70 | $3,755.59 | ▲ +739.70 after sell → book $11,272.41; vs 09:30 mark -14.80 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 4 | $121.00 | $2.02 | $+19.26 | $4,237.57 | ▲ +19.26 after sell → book $11,270.39; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 4 | $104.14 | $2.02 | $-2.22 | $4,652.11 | ▼ -2.22 after sell → book $11,268.37; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 28 | $18.05 | $2.09 | $-0.81 | $5,155.56 | ▼ -0.81 after sell → book $11,266.28; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 5 | $97.02 | $2.02 | $+11.17 | $5,638.63 | ▲ +11.17 after sell → book $11,264.25; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 11 | $44.22 | $2.04 | $+8.47 | $6,123.01 | ▲ +8.47 after sell → book $11,262.21; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 222 | $2.34 | $2.91 | $+3.11 | $6,639.58 | ▲ +3.11 after sell → book $11,259.30; vs 09:30 mark -2.91 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 482 | $4.32 | $6.31 | $-94.47 | $8,715.50 | ▼ -94.47 after sell → book $11,252.98; vs 09:30 mark -6.32 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 316 | $8.03 | $4.15 | $+377.29 | $11,248.83 | ▲ +377.29 after sell → book $11,248.83; vs 09:30 mark -4.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,248.83 | ▲ close $11,248.83 vs 09:30 $11,291.29 (session +0.00) | 16:00 close · cash $11,248.83 · no lots left · equity $11,248.83. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,248.83 | ▲ 09:30 equity $11,248.83 vs yday $11,248.83 (+0.00) | 09:30 open · cash $11,248.83 (unchanged overnight, no fees) · equity $11,248.83 vs prior close $11,248.83 (+0.00) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 4 | $175.01 | $2.00 | — | $10,546.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $703.05; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 7 | $88.94 | $2.01 | — | $9,922.20 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $703.05; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 46 | $15.28 | $2.13 | — | $9,217.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $703.05; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 4 | $142.36 | $2.00 | — | $8,645.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $703.05; owner union_e_fresh_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 137 | $5.10 | $2.40 | — | $7,944.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $703.05; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 14 | $47.89 | $2.03 | — | $7,272.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $703.05; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 50 | $13.92 | $2.14 | — | $6,574.02 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $703.05; owner union_e_fresh_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 154 | $4.54 | $2.45 | — | $5,871.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $703.05; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 60 | $24.11 | $2.17 | — | $4,422.87 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $1467.91; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 940 | $1.56 | $12.13 | — | $2,944.34 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $1467.91; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 360 | $4.07 | $4.64 | — | $1,474.50 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $1467.91; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 77 | $19.04 | $2.22 | — | $6.19 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $1467.91; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.19 | ▲ close $11,514.51 vs 09:30 $11,248.83 (session +304.01) | 16:00 close · cash $6.19 · equity $11,514.51 vs 09:30 $11,248.83 (+265.68; session marks +304.01) · 12 name(s) marked open→close (per-name table). BMO×4 09:30 $175.01 → close $173.46 -6.20; BNS×7 09:30 $88.94 → close $93.10 +29.12; BZ×46 09:30 $15.28 → close $16.29 +46.46; DKS×4 09:30 $142.36 → close $124.31 -72.20; EH×137 09:30 $5.10 → close $4.83 -36.99; GFI×14 09:30 $47.89 → close $48.87 +13.72; GRRR×50 09:30 $13.92 → close $14.04 +6.00; SHMD×154 09:30 $4.54 → close $3.42 -173.25; REAX×60 09:30 $24.11 → close $28.43 +259.20; CYPH×940 09:30 $1.56 → close $1.64 +75.20; XHG×360 09:30 $4.07 → close $4.02 -18.00; ASST×77 09:30 $19.04 → close $21.39 +180.95 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.19 | ▼ 09:30 equity $11,225.03 vs yday $11,514.51 (-289.48) | 09:30 open · cash $6.19 (unchanged overnight, no fees) · equity $11,225.03 vs prior close $11,514.51 (-289.48) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMO` | 4 | $173.22 | $2.02 | $-11.18 | $697.05 | ▼ -11.18 after sell → book $11,223.01; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BNS` | 7 | $92.65 | $2.03 | $+21.93 | $1,343.57 | ▲ +21.93 after sell → book $11,220.98; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EH` | 137 | $4.77 | $2.43 | $-50.04 | $1,994.63 | ▼ -50.04 after sell → book $11,218.55; vs 09:30 mark -2.43 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GFI` | 14 | $48.24 | $2.05 | $+0.82 | $2,667.94 | ▲ +0.82 after sell → book $11,216.50; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 50 | $14.03 | $2.16 | $+1.20 | $3,367.28 | ▲ +1.20 after sell → book $11,214.34; vs 09:30 mark -2.16 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `SHMD` | 154 | $3.38 | $2.49 | $-184.35 | $3,885.31 | ▼ -184.35 after sell → book $11,211.85; vs 09:30 mark -2.49 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 60 | $26.61 | $2.19 | $+145.64 | $5,479.71 | ▲ +145.64 after sell → book $11,209.65; vs 09:30 mark -2.20 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 940 | $1.60 | $12.29 | $+13.18 | $6,971.42 | ▲ +13.18 after sell → book $11,197.36; vs 09:30 mark -12.29 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 77 | $20.72 | $2.25 | $+124.89 | $8,564.61 | ▲ +124.89 after sell → book $11,195.11; vs 09:30 mark -2.25 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 1224 | $0.58 | $10.81 | — | $7,840.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $713.72; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 136 | $5.21 | $2.40 | — | $7,129.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $713.72; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 5 | $131.37 | $2.00 | — | $6,470.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; combo leftover $713.72; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 39 | $18.26 | $2.11 | — | $5,756.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $713.72; owner union_e_fresh_h1 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 20 | $34.30 | $2.05 | — | $5,068.10 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $713.72; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 2 | $326.91 | $2.00 | — | $4,412.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-15.2; combo leftover $713.72; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 104 | $14.11 | $2.30 | — | $2,942.55 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $1470.76; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 253 | $5.81 | $3.26 | — | $1,469.35 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $1470.76; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SUJA` | 156 | $9.39 | $2.46 | — | $2.05 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+39.0; combo leftover $1470.76; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.05 | ▲ close $11,497.74 vs 09:30 $11,225.03 (session +332.02) | 16:00 close · cash $2.05 · equity $11,497.74 vs 09:30 $11,225.03 (+272.71; session marks +332.02) · 12 name(s) marked open→close (per-name table). BZ×46 09:30 $16.77 → close $18.84 +95.22; DKS×4 09:30 $121.87 → close $129.66 +31.16; XHG×360 09:30 $3.81 → close $4.06 +90.00; SLQT×1224 09:30 $0.58 → close $0.55 -40.39; TIGR×136 09:30 $5.21 → close $5.46 +34.00; ANF×5 09:30 $131.37 → close $147.75 +81.90; BBWI×39 09:30 $18.26 → close $18.90 +24.96; BOX×20 09:30 $34.30 → close $33.39 -18.20; DY×2 09:30 $326.91 → close $310.91 -32.00; BYND×104 09:30 $14.11 → close $14.25 +14.56; USDE×253 09:30 $5.81 → close $5.98 +43.01; SUJA×156 09:30 $9.39 → close $9.44 +7.80 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.05 | ▲ 09:30 equity $11,572.20 vs yday $11,497.74 (+74.46) | 09:30 open · cash $2.05 (unchanged overnight, no fees) · equity $11,572.20 vs prior close $11,497.74 (+74.46) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 46 | $18.50 | $2.15 | $+143.84 | $850.91 | ▲ +143.84 after sell → book $11,570.06; vs 09:30 mark -2.14 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 4 | $128.73 | $2.02 | $-58.54 | $1,363.80 | ▼ -58.54 after sell → book $11,568.03; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 360 | $4.06 | $4.72 | $-12.96 | $2,820.69 | ▼ -12.96 after sell → book $11,563.32; vs 09:30 mark -4.71 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 1224 | $0.53 | $10.37 | $-86.05 | $3,459.04 | ▼ -86.05 after sell → book $11,552.95; vs 09:30 mark -10.37 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 136 | $5.49 | $2.43 | $+33.25 | $4,203.25 | ▲ +33.25 after sell → book $11,550.52; vs 09:30 mark -2.43 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ANF` | 5 | $144.70 | $2.02 | $+62.62 | $4,924.72 | ▲ +62.62 after sell → book $11,548.49; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBWI` | 39 | $18.69 | $2.13 | $+12.54 | $5,651.50 | ▲ +12.54 after sell → book $11,546.36; vs 09:30 mark -2.13 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BOX` | 20 | $33.79 | $2.07 | $-14.32 | $6,325.23 | ▼ -14.32 after sell → book $11,544.29; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DY` | 2 | $314.90 | $2.02 | $-28.03 | $6,953.02 | ▼ -28.03 after sell → book $11,542.28; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 104 | $14.20 | $2.33 | $+4.73 | $8,427.49 | ▲ +4.73 after sell → book $11,539.95; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 253 | $6.50 | $3.32 | $+167.99 | $10,068.67 | ▲ +167.99 after sell → book $11,536.63; vs 09:30 mark -3.32 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 156 | $9.41 | $2.50 | $-1.83 | $11,534.13 | ▼ -1.83 after sell → book $11,534.13; vs 09:30 mark -2.50 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 25 | $222.86 | $2.06 | — | $5,960.57 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list mover_buy; 🔵; ret5=-3.6; combo leftover $5767.07; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 573 | $2.60 | $7.39 | — | $4,463.38 | — | top 4 by hot; rank hot_score; list flatten; ret5=+13.0; combo leftover $1490.14; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 35 | $41.44 | $2.10 | — | $3,010.88 | — | top 4 by hot; rank hot_score; list flatten; ret5=+3.1; combo leftover $1490.14; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 64 | $22.93 | $2.18 | — | $1,541.18 | — | top 4 by hot; rank hot_score; list mover_buy; 🔵; ret5=+9.5; combo leftover $1490.14; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 103 | $14.42 | $2.30 | — | $53.62 | — | top 4 by hot; rank hot_score; list flatten; ret5=+7.1; combo leftover $1490.14; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.62 | ▲ close $11,717.74 vs 09:30 $11,572.20 (session +199.64) | 16:00 close · cash $53.62 · equity $11,717.74 vs 09:30 $11,572.20 (+145.54; session marks +199.64) · 5 name(s) marked open→close (per-name table). NVDA×25 09:30 $222.86 → close $227.98 +128.00; SLI×573 09:30 $2.60 → close $2.64 +22.92; RRC×35 09:30 $41.44 → close $41.64 +7.00; PGY×64 09:30 $22.93 → close $23.26 +21.12; CRK×103 09:30 $14.42 → close $14.62 +20.60 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.62 | ▲ 09:30 equity $11,726.49 vs yday $11,717.74 (+8.75) | 09:30 open · cash $53.62 (unchanged overnight, no fees) · equity $11,726.49 vs prior close $11,717.74 (+8.75) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 25 | $227.36 | $2.12 | $+108.31 | $5,735.50 | ▲ +108.31 after sell → book $11,724.37; vs 09:30 mark -2.12 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 573 | $2.68 | $7.50 | $+30.95 | $7,263.64 | ▲ +30.95 after sell → book $11,716.87; vs 09:30 mark -7.50 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 35 | $41.74 | $2.12 | $+6.29 | $8,722.42 | ▲ +6.29 after sell → book $11,714.75; vs 09:30 mark -2.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 64 | $23.21 | $2.20 | $+13.53 | $10,205.66 | ▲ +13.53 after sell → book $11,712.55; vs 09:30 mark -2.20 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 103 | $14.63 | $2.33 | $+17.00 | $11,710.22 | ▲ +17.00 after sell → book $11,710.22; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 2 | $261.16 | $2.00 | — | $11,185.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $731.89; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 48 | $15.01 | $2.13 | — | $10,463.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $731.89; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 7 | $103.89 | $2.01 | — | $9,734.05 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $731.89; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 188 | $3.88 | $2.55 | — | $9,002.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $731.89; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 16 | $44.40 | $2.04 | — | $8,289.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $731.89; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 29 | $24.69 | $2.08 | — | $7,571.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $731.89; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 87 | $8.35 | $2.25 | — | $6,842.83 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $731.89; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 19 | $37.65 | $2.05 | — | $6,125.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $731.89; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 109 | $14.00 | $2.32 | — | $4,597.21 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $1531.38; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 157 | $9.73 | $2.46 | — | $3,067.14 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+47.1; combo leftover $1531.38; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 11 | $137.19 | $2.02 | — | $1,556.03 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+7.1; combo leftover $1531.38; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 10 | $146.07 | $2.02 | — | $93.31 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $1531.38; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $93.31 | ▼ close $11,464.62 vs 09:30 $11,726.49 (session -219.68) | 16:00 close · cash $93.31 · equity $11,464.62 vs 09:30 $11,726.49 (-261.87; session marks -219.68) · 12 name(s) marked open→close (per-name table). ADSK×2 09:30 $261.16 → close $260.66 -1.00; BBAR×48 09:30 $15.01 → close $14.47 -25.92; ESTC×7 09:30 $103.89 → close $99.91 -27.86; FINV×188 09:30 $3.88 → close $3.40 -90.24; FRO×16 09:30 $44.40 → close $44.19 -3.36; GAP×29 09:30 $24.69 → close $23.48 -35.09; HAFN×87 09:30 $8.35 → close $8.47 +10.44; IREN×19 09:30 $37.65 → close $35.45 -41.71; BYND×109 09:30 $14.00 → close $13.86 -15.26; CAPR×157 09:30 $9.73 → close $9.59 -21.98; MRNA×11 09:30 $137.19 → close $137.99 +8.80; ANF×10 09:30 $146.07 → close $148.42 +23.50 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.31 | ▼ 09:30 equity $11,405.00 vs yday $11,464.62 (-59.62) | 09:30 open · cash $93.31 (unchanged overnight, no fees) · equity $11,405.00 vs prior close $11,464.62 (-59.62) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 2 | $257.71 | $2.02 | $-10.91 | $606.71 | ▼ -10.91 after sell → book $11,402.98; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 48 | $14.88 | $2.15 | $-10.53 | $1,318.80 | ▼ -10.53 after sell → book $11,400.83; vs 09:30 mark -2.15 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 7 | $98.00 | $2.03 | $-45.27 | $2,002.77 | ▼ -45.27 after sell → book $11,398.80; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 188 | $3.39 | $2.60 | $-97.27 | $2,637.49 | ▼ -97.27 after sell → book $11,396.20; vs 09:30 mark -2.60 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 16 | $44.85 | $2.06 | $+3.10 | $3,353.03 | ▲ +3.10 after sell → book $11,394.14; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 29 | $22.98 | $2.10 | $-53.76 | $4,017.36 | ▼ -53.76 after sell → book $11,392.05; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 87 | $8.53 | $2.28 | $+11.13 | $4,757.19 | ▲ +11.13 after sell → book $11,389.77; vs 09:30 mark -2.28 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 19 | $35.81 | $2.07 | $-38.98 | $5,435.51 | ▼ -38.98 after sell → book $11,387.70; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 157 | $9.50 | $2.50 | $-41.07 | $6,924.51 | ▼ -41.07 after sell → book $11,385.20; vs 09:30 mark -2.50 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 11 | $134.10 | $2.04 | $-38.06 | $8,397.57 | ▼ -38.06 after sell → book $11,383.16; vs 09:30 mark -2.04 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 10 | $148.03 | $2.04 | $+15.54 | $9,875.83 | ▲ +15.54 after sell → book $11,381.12; vs 09:30 mark -2.04 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,875.83 | ▼ close $11,325.53 vs 09:30 $11,405.00 (session -55.59) | 16:00 close · cash $9,875.83 · equity $11,325.53 vs 09:30 $11,405.00 (-79.47; session marks -55.59) · 1 name(s) marked open→close (per-name table). BYND×109 09:30 $13.81 → close $13.30 -55.59 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,875.83 | ▼ 09:30 equity $11,297.19 vs yday $11,325.53 (-28.34) | 09:30 open · cash $9,875.83 (unchanged overnight, no fees) · equity $11,297.19 vs prior close $11,325.53 (-28.34) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 109 | $13.04 | $2.35 | $-109.30 | $11,294.84 | ▼ -109.30 after sell → book $11,294.84; vs 09:30 mark -2.35 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,294.84 | ▲ close $11,294.84 vs 09:30 $11,297.19 (session +0.00) | 16:00 close · cash $11,294.84 · no lots left · equity $11,294.84. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,294.84 | ▲ 09:30 equity $11,294.84 vs yday $11,294.84 (+0.00) | 09:30 open · cash $11,294.84 (unchanged overnight, no fees) · equity $11,294.84 vs prior close $11,294.84 (+0.00) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,294.84 | ▲ close $11,294.84 vs 09:30 $11,294.84 (session +0.00) | 16:00 close · cash $11,294.84 · no lots left · equity $11,294.84. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,294.84 | ▲ 09:30 equity $11,294.84 vs yday $11,294.84 (+0.00) | 09:30 open · cash $11,294.84 (unchanged overnight, no fees) · equity $11,294.84 vs prior close $11,294.84 (+0.00) | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 65 | $10.74 | $2.19 | — | $10,594.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $705.93; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 2 | $351.74 | $2.00 | — | $9,888.76 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $705.93; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 102 | $6.90 | $2.30 | — | $9,182.66 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $705.93; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $8,826.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $705.93; owner union_e_fresh_h1 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 31 | $22.32 | $2.08 | — | $8,132.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $705.93; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 2 | $257.00 | $2.00 | — | $7,616.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $705.93; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 14 | $47.60 | $2.03 | — | $6,947.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $705.93; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 46 | $15.09 | $2.13 | — | $6,251.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $705.93; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 878 | $1.78 | $11.33 | — | $4,677.31 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $1562.87; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 84 | $18.40 | $2.24 | — | $3,129.47 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $1562.87; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 113 | $13.71 | $2.33 | — | $1,577.91 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $1562.87; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 65 | $23.88 | $2.19 | — | $23.53 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $1562.87; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.53 | ▼ close $11,202.34 vs 09:30 $11,294.84 (session -57.71) | 16:00 close · cash $23.53 · equity $11,202.34 vs 09:30 $11,294.84 (-92.50; session marks -57.71) · 12 name(s) marked open→close (per-name table). AI×65 09:30 $10.74 → close $10.90 +10.08; AVGO×2 09:30 $351.74 → close $357.16 +10.84; CHPT×102 09:30 $6.90 → close $9.08 +222.36; CIEN×1 09:30 $354.49 → close $317.46 -37.03; CPB×31 09:30 $22.32 → close $22.13 -5.89; FIVE×2 09:30 $257.00 → close $239.96 -34.08; HPE×14 09:30 $47.60 → close $54.44 +95.76; MEI×46 09:30 $15.09 → close $15.32 +10.58; GPRO×878 09:30 $1.78 → close $1.39 -342.42; REAX×84 09:30 $18.40 → close $18.40 +0.00; CNH×113 09:30 $13.71 → close $13.84 +14.69; MMED×65 09:30 $23.88 → close $23.84 -2.60 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.53 | ▲ 09:30 equity $11,285.92 vs yday $11,202.34 (+83.58) | 09:30 open · cash $23.53 (unchanged overnight, no fees) · equity $11,285.92 vs prior close $11,202.34 (+83.58) | — |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 65 | $10.91 | $2.21 | $+6.33 | $730.47 | ▲ +6.33 after sell → book $11,283.71; vs 09:30 mark -2.21 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 2 | $359.70 | $2.02 | $+11.91 | $1,447.85 | ▲ +11.91 after sell → book $11,281.69; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 102 | $9.28 | $2.32 | $+238.14 | $2,392.09 | ▲ +238.14 after sell → book $11,279.37; vs 09:30 mark -2.32 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 1 | $321.67 | $2.01 | $-36.83 | $2,711.75 | ▼ -36.83 after sell → book $11,277.36; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CPB` | 31 | $22.10 | $2.10 | $-11.01 | $3,394.74 | ▼ -11.01 after sell → book $11,275.25; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 2 | $238.88 | $2.02 | $-40.25 | $3,870.49 | ▼ -40.25 after sell → book $11,273.24; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 14 | $53.85 | $2.05 | $+83.42 | $4,622.34 | ▲ +83.42 after sell → book $11,271.19; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 46 | $15.34 | $2.15 | $+7.22 | $5,325.83 | ▲ +7.22 after sell → book $11,269.04; vs 09:30 mark -2.15 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 84 | $18.15 | $2.27 | $-25.51 | $6,848.16 | ▼ -25.51 after sell → book $11,266.77; vs 09:30 mark -2.27 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 113 | $13.89 | $2.36 | $+15.65 | $8,415.37 | ▲ +15.65 after sell → book $11,264.41; vs 09:30 mark -2.36 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 65 | $23.84 | $2.21 | $-6.99 | $9,962.76 | ▼ -6.99 after sell → book $11,262.20; vs 09:30 mark -2.21 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 9 | $63.18 | $2.02 | — | $9,392.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $622.67; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 71 | $8.74 | $2.20 | — | $8,769.38 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $622.67; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 9 | $68.52 | $2.02 | — | $8,150.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $622.67; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 172 | $3.62 | $2.51 | — | $7,526.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $622.67; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 3 | $167.55 | $2.00 | — | $7,021.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $622.67; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 13 | $44.90 | $2.03 | — | $6,436.02 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $622.67; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 6 | $98.15 | $2.01 | — | $5,845.11 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $622.67; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 39 | $15.70 | $2.11 | — | $5,230.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $622.67; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 69 | $25.18 | $2.20 | — | $3,491.09 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $1743.57; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 221 | $7.87 | $2.85 | — | $1,748.97 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $1743.57; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 301 | $5.79 | $3.88 | — | $2.29 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $1743.57; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.29 | ▲ close $11,566.28 vs 09:30 $11,285.92 (session +329.90) | 16:00 close · cash $2.29 · equity $11,566.28 vs 09:30 $11,285.92 (+280.36; session marks +329.90) · 12 name(s) marked open→close (per-name table). GPRO×878 09:30 $1.48 → close $1.70 +193.16; AMBA×9 09:30 $63.18 → close $62.89 -2.61; ASAN×71 09:30 $8.74 → close $8.81 +4.97; DOCU×9 09:30 $68.52 → close $68.41 -0.99; DOMO×172 09:30 $3.62 → close $3.88 +45.58; GWRE×3 09:30 $167.55 → close $162.42 -15.39; IOT×13 09:30 $44.90 → close $40.20 -61.10; LULU×6 09:30 $98.15 → close $100.61 +14.76; MAMA×39 09:30 $15.70 → close $15.16 -21.06; ASST×69 09:30 $25.18 → close $27.14 +135.24; USDE×221 09:30 $7.87 → close $7.93 +13.26; DFDV×301 09:30 $5.79 → close $5.87 +24.08 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.29 | ▼ 09:30 equity $11,314.84 vs yday $11,566.28 (-251.44) | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $11,314.84 vs prior close $11,566.28 (-251.44) | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 878 | $1.56 | $11.48 | $-211.58 | $1,364.88 | ▼ -211.58 after sell → book $11,303.36; vs 09:30 mark -11.48 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `AMBA` | 9 | $63.83 | $2.04 | $+1.80 | $1,937.31 | ▲ +1.80 after sell → book $11,301.32; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASAN` | 71 | $8.73 | $2.22 | $-5.14 | $2,554.92 | ▼ -5.14 after sell → book $11,299.10; vs 09:30 mark -2.22 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 9 | $67.05 | $2.04 | $-17.28 | $3,156.33 | ▼ -17.28 after sell → book $11,297.06; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOMO` | 172 | $3.84 | $2.54 | $+33.65 | $3,814.27 | ▲ +33.65 after sell → book $11,294.52; vs 09:30 mark -2.54 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 3 | $160.52 | $2.02 | $-25.11 | $4,293.81 | ▼ -25.11 after sell → book $11,292.50; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 13 | $39.56 | $2.05 | $-73.50 | $4,806.04 | ▼ -73.50 after sell → book $11,290.45; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 6 | $100.58 | $2.03 | $+10.54 | $5,407.49 | ▲ +10.54 after sell → book $11,288.42; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MAMA` | 39 | $15.20 | $2.13 | $-23.73 | $5,998.17 | ▼ -23.73 after sell → book $11,286.30; vs 09:30 mark -2.12 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 69 | $26.44 | $2.22 | $+82.52 | $7,820.30 | ▲ +82.52 after sell → book $11,284.07; vs 09:30 mark -2.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 221 | $7.76 | $2.90 | $-30.06 | $9,532.36 | ▼ -30.06 after sell → book $11,281.17; vs 09:30 mark -2.90 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 301 | $5.81 | $3.95 | $-1.81 | $11,277.22 | ▼ -1.81 after sell → book $11,277.22; vs 09:30 mark -3.95 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,277.22 | ▲ close $11,277.22 vs 09:30 $11,314.84 (session +0.00) | 16:00 close · cash $11,277.22 · no lots left · equity $11,277.22. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,277.22 | ▲ 09:30 equity $11,277.22 vs yday $11,277.22 (+0.00) | 09:30 open · cash $11,277.22 (unchanged overnight, no fees) · equity $11,277.22 vs prior close $11,277.22 (+0.00) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,277.22 | ▲ close $11,277.22 vs 09:30 $11,277.22 (session +0.00) | 16:00 close · cash $11,277.22 · no lots left · equity $11,277.22. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,277.22 | ▲ 09:30 equity $11,277.22 vs yday $11,277.22 (+0.00) | 09:30 open · cash $11,277.22 (unchanged overnight, no fees) · equity $11,277.22 vs prior close $11,277.22 (+0.00) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,277.22 | ▲ close $11,277.22 vs 09:30 $11,277.22 (session +0.00) | 16:00 close · cash $11,277.22 · no lots left · equity $11,277.22. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,277.22 | ▲ 09:30 equity $11,277.22 vs yday $11,277.22 (+0.00) | 09:30 open · cash $11,277.22 (unchanged overnight, no fees) · equity $11,277.22 vs prior close $11,277.22 (+0.00) | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 22 | $32.01 | $2.06 | — | $10,570.95 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-1.2; combo leftover $704.83; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 75 | $9.37 | $2.21 | — | $9,865.98 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.9; combo leftover $704.83; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 913 | $2.70 | $11.78 | — | $7,389.11 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+61.6; combo leftover $2466.50; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 400 | $6.16 | $5.16 | — | $4,919.95 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.6; combo leftover $2466.50; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CYPH` | 1032 | $2.39 | $13.31 | — | $2,440.15 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+44.1; combo leftover $2466.50; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,440.15 | ▼ close $11,071.54 vs 09:30 $11,277.22 (session -171.16) | 16:00 close · cash $2,440.15 · equity $11,071.54 vs 09:30 $11,277.22 (-205.68; session marks -171.16) · 5 name(s) marked open→close (per-name table). CPRT×22 09:30 $32.01 → close $29.95 -45.32; LPTH×75 09:30 $9.37 → close $9.20 -12.75; INDP×913 09:30 $2.70 → close $2.77 +63.91; IRD×400 09:30 $6.16 → close $6.04 -48.00; CYPH×1032 09:30 $2.39 → close $2.27 -129.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new union_hot_n4_h1 |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new union_hot_n4_h1 |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new union_hot_n4_h1 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new union_hot_n4_h1 |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new union_hot_n4_h1 |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new union_hot_n4_h1 |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new union_hot_n4_h1 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-21 | `DE` | cash | leftover split 512.50 < 1 share @ 623.26 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new union_e_fresh_h1 |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new union_hot_n4_h1 |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new union_hot_n4_h1 |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new union_hot_n4_h1 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new union_e_fresh_h1 |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new union_hot_n4_h1 |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new union_hot_n4_h1 |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new union_hot_n4_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new union_hot_n4_h1 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new union_hot_n4_h1 |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new union_hot_n4_h1 |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new union_hot_n4_h1 |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new union_hot_n4_h1 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new union_e_fresh_h1 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new union_e_fresh_h1 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new union_hot_n4_h1 |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new union_hot_n4_h1 |
| 2026-09-10 | `PAYP` | hard_red | hard-red S=-13.28 sit; no new union_hot_n4_h1 |
| 2026-09-10 | `CRWV` | hard_red | hard-red S=-13.28 sit; no new union_hot_n4_h1 |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-11 | `ORCL` | no_price | no 09:30 open |
| 2026-09-11 | `DBI` | no_price | no 09:30 open |
| 2026-09-11 | `ADBE` | no_price | no 09:30 open |
| 2026-09-11 | `DSGX` | no_price | no 09:30 open |
| 2026-09-11 | `KR` | no_price | no 09:30 open |
| 2026-09-11 | `REF` | no_price | no 09:30 open |
| 2026-09-11 | `BNC` | no_price | no 09:30 open |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CPRT` | 22 | 2026-09-11 @ $32.01 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-1.2; combo leftover $704.83; owner union_e_fresh_h1 |
| `LPTH` | 75 | 2026-09-11 @ $9.37 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.9; combo leftover $704.83; owner union_e_fresh_h1 |
| `INDP` | 913 | 2026-09-11 @ $2.70 | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+61.6; combo leftover $2466.50; owner union_hot_n4_h1 |
| `IRD` | 400 | 2026-09-11 @ $6.16 | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.6; combo leftover $2466.50; owner union_hot_n4_h1 |
| `CYPH` | 1032 | 2026-09-11 @ $2.39 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+44.1; combo leftover $2466.50; owner union_hot_n4_h1 |
