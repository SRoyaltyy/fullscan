# Factor mine action — `combo_eer_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_e_fresh_h3/union_earn_react_h3 w=0.5,0.5 net=priority

Cash book **+15.71%** ($11,571) · signal-only (no cash/fees) was —. Starts YES **8/18**. Fills 89 · skips 175 · realized $+1031.49.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_e_fresh_h3 50%, union_earn_react_h3 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_e_fresh_h3 50%, union_earn_react_h3 50%.
- Member: union_e_fresh_h3 (50% · long · hold 3).
- Member: union_earn_react_h3 (50% · long · hold 3).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $180.55.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `INO` | 6172 | — | $0.81 | +0.00 | $0.90 | +555.48 | +555.48 | +0.00 | +555.48 |
| 2026-08-13 | `VOR` | 223 | — | $22.01 | +0.00 | $23.29 | +285.44 | +285.44 | +0.00 | +285.44 |
| 2026-08-14 | `INO` | 6172 | $0.90 | $0.93 | +185.16 | $1.09 | +987.52 | +1172.68 | +740.64 | +1728.16 |
| 2026-08-14 | `VOR` | 223 | $23.29 | $23.33 | +8.92 | $23.03 | -66.90 | -57.98 | +294.36 | +227.46 |
| 2026-08-14 | `EU` | 1 | — | $1.18 | +0.00 | $1.21 | +0.03 | +0.03 | +0.00 | +0.03 |
| 2026-08-14 | `BZAI` | 3 | — | $0.77 | +0.00 | $0.59 | -0.52 | -0.52 | +0.00 | -0.52 |
| 2026-08-14 | `DEFT` | 6 | — | $0.47 | +0.00 | $0.49 | +0.11 | +0.11 | +0.00 | +0.11 |
| 2026-08-17 | `INO` | 6172 | $1.09 | $1.07 | -123.44 | $1.15 | +493.76 | +370.32 | +1604.72 | +2098.48 |
| 2026-08-17 | `VOR` | 223 | $23.03 | $22.91 | -26.76 | $23.01 | +22.30 | -4.46 | +200.70 | +223.00 |
| 2026-08-17 | `EU` | 1 | $1.21 | $1.21 | +0.00 | $1.13 | -0.08 | -0.08 | +0.03 | -0.05 |
| 2026-08-17 | `BZAI` | 3 | $0.59 | $0.55 | -0.12 | $0.52 | -0.09 | -0.21 | -0.64 | -0.73 |
| 2026-08-17 | `DEFT` | 6 | $0.49 | $0.47 | -0.08 | $0.47 | -0.06 | -0.14 | +0.03 | -0.03 |
| 2026-08-18 | `INO` | 6172 | $1.15 | $1.14 | -61.72 | — | +0.00 | -61.72 | +2036.76 | — |
| 2026-08-18 | `VOR` | 223 | $23.01 | $22.82 | -42.37 | — | +0.00 | -42.37 | +180.63 | — |
| 2026-08-18 | `EU` | 1 | $1.13 | $1.13 | +0.00 | $1.07 | -0.06 | -0.06 | -0.05 | -0.11 |
| 2026-08-18 | `BZAI` | 3 | $0.52 | $0.49 | -0.09 | $0.56 | +0.20 | +0.11 | -0.83 | -0.62 |
| 2026-08-18 | `DEFT` | 6 | $0.47 | $0.45 | -0.09 | $0.44 | -0.08 | -0.17 | -0.12 | -0.20 |
| 2026-08-19 | `EU` | 1 | $1.07 | $1.07 | +0.00 | — | +0.00 | +0.00 | -0.11 | — |
| 2026-08-19 | `BZAI` | 3 | $0.56 | $0.57 | +0.04 | — | +0.00 | +0.04 | -0.59 | — |
| 2026-08-19 | `DEFT` | 6 | $0.44 | $0.43 | -0.01 | — | +0.00 | -0.01 | -0.21 | — |
| 2026-08-20 | `EL` | 7 | — | $97.43 | +0.00 | $96.15 | -8.96 | -8.96 | +0.00 | -8.96 |
| 2026-08-20 | `TOYO` | 170 | — | $4.43 | +0.00 | $4.51 | +14.45 | +14.45 | +0.00 | +14.45 |
| 2026-08-20 | `DVLT` | 2512 | — | $0.30 | +0.00 | $0.32 | +50.24 | +50.24 | +0.00 | +50.24 |
| 2026-08-20 | `AAP` | 16 | — | $46.85 | +0.00 | $42.39 | -71.36 | -71.36 | +0.00 | -71.36 |
| 2026-08-20 | `AEG` | 83 | — | $9.01 | +0.00 | $9.01 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `ALVO` | 193 | — | $3.89 | +0.00 | $4.27 | +73.34 | +73.34 | +0.00 | +73.34 |
| 2026-08-20 | `ATAT` | 22 | — | $34.05 | +0.00 | $34.25 | +4.40 | +4.40 | +0.00 | +4.40 |
| 2026-08-20 | `ATHM` | 33 | — | $22.44 | +0.00 | $22.12 | -10.56 | -10.56 | +0.00 | -10.56 |
| 2026-08-20 | `BABA` | 16 | — | $123.47 | +0.00 | $130.53 | +112.96 | +112.96 | +0.00 | +112.96 |
| 2026-08-20 | `BILL` | 41 | — | $49.00 | +0.00 | $47.40 | -65.60 | -65.60 | +0.00 | -65.60 |
| 2026-08-20 | `BULL` | 204 | — | $9.94 | +0.00 | $8.85 | -222.36 | -222.36 | +0.00 | -222.36 |
| 2026-08-21 | `EL` | 7 | $96.15 | $96.75 | +4.20 | $101.94 | +36.33 | +40.53 | -4.76 | +31.57 |
| 2026-08-21 | `TOYO` | 170 | $4.51 | $4.68 | +28.05 | $4.82 | +23.80 | +51.85 | +42.50 | +66.30 |
| 2026-08-21 | `DVLT` | 2512 | $0.32 | $0.31 | -25.12 | $0.32 | +25.12 | +0.00 | +25.12 | +50.24 |
| 2026-08-21 | `AAP` | 16 | $42.39 | $42.41 | +0.32 | $42.58 | +2.72 | +3.04 | -71.04 | -68.32 |
| 2026-08-21 | `AEG` | 83 | $9.01 | $9.04 | +2.49 | $8.99 | -4.15 | -1.66 | +2.49 | -1.66 |
| 2026-08-21 | `ALVO` | 193 | $4.27 | $4.32 | +9.65 | $4.43 | +21.23 | +30.88 | +82.99 | +104.22 |
| 2026-08-21 | `ATAT` | 22 | $34.25 | $34.31 | +1.32 | $34.75 | +9.68 | +11.00 | +5.72 | +15.40 |
| 2026-08-21 | `ATHM` | 33 | $22.12 | $22.20 | +2.64 | $22.22 | +0.66 | +3.30 | -7.92 | -7.26 |
| 2026-08-21 | `BABA` | 16 | $130.53 | $125.35 | -82.88 | $119.34 | -96.16 | -179.04 | +30.08 | -66.08 |
| 2026-08-21 | `BILL` | 41 | $47.40 | $47.50 | +4.10 | $47.82 | +13.12 | +17.22 | -61.50 | -48.38 |
| 2026-08-21 | `BULL` | 204 | $8.85 | $8.99 | +28.56 | $8.78 | -42.84 | -14.28 | -193.80 | -236.64 |
| 2026-08-21 | `PSEC` | 2 | — | $2.30 | +0.00 | $2.33 | +0.06 | +0.06 | +0.00 | +0.06 |
| 2026-08-24 | `EL` | 7 | $101.94 | $101.92 | -0.14 | $104.13 | +15.47 | +15.33 | +31.43 | +46.90 |
| 2026-08-24 | `TOYO` | 170 | $4.82 | $4.58 | -40.80 | $4.38 | -34.00 | -74.80 | +25.50 | -8.50 |
| 2026-08-24 | `DVLT` | 2512 | $0.32 | $0.31 | -25.12 | $0.31 | +0.00 | -25.12 | +25.12 | +25.12 |
| 2026-08-24 | `AAP` | 16 | $42.58 | $43.05 | +7.52 | $43.63 | +9.28 | +16.80 | -60.80 | -51.52 |
| 2026-08-24 | `AEG` | 83 | $8.99 | $9.15 | +13.28 | $9.19 | +3.32 | +16.60 | +11.62 | +14.94 |
| 2026-08-24 | `ALVO` | 193 | $4.43 | $4.79 | +69.48 | $5.25 | +88.78 | +158.26 | +173.70 | +262.48 |
| 2026-08-24 | `ATAT` | 22 | $34.75 | $34.70 | -1.10 | $34.75 | +1.10 | +0.00 | +14.30 | +15.40 |
| 2026-08-24 | `ATHM` | 33 | $22.22 | $22.00 | -7.26 | $21.85 | -4.95 | -12.21 | -14.52 | -19.47 |
| 2026-08-24 | `BABA` | 16 | $119.34 | $116.90 | -39.04 | $118.47 | +25.12 | -13.92 | -105.12 | -80.00 |
| 2026-08-24 | `BILL` | 41 | $47.82 | $47.68 | -5.74 | $48.16 | +19.68 | +13.94 | -54.12 | -34.44 |
| 2026-08-24 | `BULL` | 204 | $8.78 | $8.58 | -40.80 | $8.53 | -10.20 | -51.00 | -277.44 | -287.64 |
| 2026-08-24 | `PSEC` | 2 | $2.33 | $2.34 | +0.02 | $2.32 | -0.04 | -0.02 | +0.08 | +0.04 |
| 2026-08-25 | `EL` | 7 | $104.13 | $104.00 | -0.91 | — | +0.00 | -0.91 | +45.99 | — |
| 2026-08-25 | `TOYO` | 170 | $4.38 | $4.42 | +6.80 | — | +0.00 | +6.80 | -1.70 | — |
| 2026-08-25 | `DVLT` | 2512 | $0.31 | $0.31 | +0.00 | — | +0.00 | +0.00 | +25.12 | — |
| 2026-08-25 | `AAP` | 16 | $43.63 | $43.63 | +0.00 | — | +0.00 | +0.00 | -51.52 | — |
| 2026-08-25 | `AEG` | 83 | $9.19 | $9.23 | +3.32 | — | +0.00 | +3.32 | +18.26 | — |
| 2026-08-25 | `ALVO` | 193 | $5.25 | $5.24 | -1.93 | — | +0.00 | -1.93 | +260.55 | — |
| 2026-08-25 | `ATAT` | 22 | $34.75 | $34.72 | -0.66 | — | +0.00 | -0.66 | +14.74 | — |
| 2026-08-25 | `ATHM` | 33 | $21.85 | $21.85 | +0.00 | — | +0.00 | +0.00 | -19.47 | — |
| 2026-08-25 | `BABA` | 16 | $118.47 | $117.94 | -8.48 | — | +0.00 | -8.48 | -88.48 | — |
| 2026-08-25 | `BILL` | 41 | $48.16 | $47.98 | -7.17 | — | +0.00 | -7.17 | -41.62 | — |
| 2026-08-25 | `BULL` | 204 | $8.53 | $8.46 | -14.28 | — | +0.00 | -14.28 | -301.92 | — |
| 2026-08-25 | `PSEC` | 2 | $2.32 | $2.32 | +0.00 | $2.35 | +0.06 | +0.06 | +0.04 | +0.10 |
| 2026-08-25 | `BMO` | 8 | — | $175.01 | +0.00 | $173.46 | -12.40 | -12.40 | +0.00 | -12.40 |
| 2026-08-25 | `BNS` | 16 | — | $88.94 | +0.00 | $93.10 | +66.56 | +66.56 | +0.00 | +66.56 |
| 2026-08-25 | `BZ` | 96 | — | $15.28 | +0.00 | $16.29 | +96.96 | +96.96 | +0.00 | +96.96 |
| 2026-08-25 | `DKS` | 10 | — | $142.36 | +0.00 | $124.31 | -180.50 | -180.50 | +0.00 | -180.50 |
| 2026-08-25 | `EH` | 290 | — | $5.10 | +0.00 | $4.83 | -78.30 | -78.30 | +0.00 | -78.30 |
| 2026-08-25 | `GFI` | 30 | — | $47.89 | +0.00 | $48.87 | +29.40 | +29.40 | +0.00 | +29.40 |
| 2026-08-25 | `GRRR` | 106 | — | $13.92 | +0.00 | $14.04 | +12.72 | +12.72 | +0.00 | +12.72 |
| 2026-08-25 | `SHMD` | 325 | — | $4.54 | +0.00 | $3.42 | -365.62 | -365.62 | +0.00 | -365.62 |
| 2026-08-26 | `PSEC` | 2 | $2.35 | $2.35 | +0.00 | — | +0.00 | +0.00 | +0.10 | — |
| 2026-08-26 | `BMO` | 8 | $173.46 | $173.22 | -1.92 | $172.90 | -2.56 | -4.48 | -14.32 | -16.88 |
| 2026-08-26 | `BNS` | 16 | $93.10 | $92.65 | -7.20 | $93.59 | +15.04 | +7.84 | +59.36 | +74.40 |
| 2026-08-26 | `BZ` | 96 | $16.29 | $16.77 | +46.08 | $18.84 | +198.72 | +244.80 | +143.04 | +341.76 |
| 2026-08-26 | `DKS` | 10 | $124.31 | $121.87 | -24.40 | $129.66 | +77.90 | +53.50 | -204.90 | -127.00 |
| 2026-08-26 | `EH` | 290 | $4.83 | $4.77 | -17.40 | $4.86 | +24.65 | +7.25 | -95.70 | -71.05 |
| 2026-08-26 | `GFI` | 30 | $48.87 | $48.24 | -18.90 | $47.82 | -12.60 | -31.50 | +10.50 | -2.10 |
| 2026-08-26 | `GRRR` | 106 | $14.04 | $14.03 | -1.06 | $15.45 | +150.52 | +149.46 | +11.66 | +162.18 |
| 2026-08-26 | `SHMD` | 325 | $3.42 | $3.38 | -13.00 | $3.17 | -68.25 | -81.25 | -378.62 | -446.88 |
| 2026-08-26 | `SLQT` | 34 | — | $0.58 | +0.00 | $0.55 | -1.12 | -1.12 | +0.00 | -1.12 |
| 2026-08-26 | `TIGR` | 3 | — | $5.21 | +0.00 | $5.46 | +0.75 | +0.75 | +0.00 | +0.75 |
| 2026-08-26 | `BBWI` | 1 | — | $18.26 | +0.00 | $18.90 | +0.64 | +0.64 | +0.00 | +0.64 |
| 2026-08-26 | `FSCO` | 12 | — | $5.08 | +0.00 | $5.12 | +0.48 | +0.48 | +0.00 | +0.48 |
| 2026-08-27 | `BMO` | 8 | $172.90 | $172.85 | -0.40 | $172.13 | -5.76 | -6.16 | -17.28 | -23.04 |
| 2026-08-27 | `BNS` | 16 | $93.59 | $93.52 | -1.12 | $92.93 | -9.44 | -10.56 | +73.28 | +63.84 |
| 2026-08-27 | `BZ` | 96 | $18.84 | $18.50 | -32.64 | $18.00 | -48.00 | -80.64 | +309.12 | +261.12 |
| 2026-08-27 | `DKS` | 10 | $129.66 | $128.73 | -9.30 | $131.77 | +30.40 | +21.10 | -136.30 | -105.90 |
| 2026-08-27 | `EH` | 290 | $4.86 | $4.90 | +13.05 | $4.66 | -69.60 | -56.55 | -58.00 | -127.60 |
| 2026-08-27 | `GFI` | 30 | $47.82 | $47.93 | +3.30 | $48.00 | +2.10 | +5.40 | +1.20 | +3.30 |
| 2026-08-27 | `GRRR` | 106 | $15.45 | $15.94 | +51.94 | $15.66 | -29.68 | +22.26 | +214.12 | +184.44 |
| 2026-08-27 | `SHMD` | 325 | $3.17 | $3.16 | -3.25 | $3.40 | +78.00 | +74.75 | -450.12 | -372.12 |
| 2026-08-27 | `SLQT` | 34 | $0.55 | $0.53 | -0.68 | $0.54 | +0.34 | -0.34 | -1.80 | -1.46 |
| 2026-08-27 | `TIGR` | 3 | $5.46 | $5.49 | +0.09 | $5.06 | -1.29 | -1.20 | +0.84 | -0.45 |
| 2026-08-27 | `BBWI` | 1 | $18.90 | $18.69 | -0.21 | $18.65 | -0.04 | -0.25 | +0.43 | +0.39 |
| 2026-08-27 | `FSCO` | 12 | $5.12 | $5.10 | -0.24 | $5.12 | +0.24 | +0.00 | +0.24 | +0.48 |
| 2026-08-28 | `BMO` | 8 | $172.13 | $172.76 | +5.04 | — | +0.00 | +5.04 | -18.00 | — |
| 2026-08-28 | `BNS` | 16 | $92.93 | $93.30 | +5.92 | — | +0.00 | +5.92 | +69.76 | — |
| 2026-08-28 | `BZ` | 96 | $18.00 | $18.15 | +14.40 | — | +0.00 | +14.40 | +275.52 | — |
| 2026-08-28 | `DKS` | 10 | $131.77 | $132.80 | +10.30 | — | +0.00 | +10.30 | -95.60 | — |
| 2026-08-28 | `EH` | 290 | $4.66 | $4.58 | -23.20 | — | +0.00 | -23.20 | -150.80 | — |
| 2026-08-28 | `GFI` | 30 | $48.00 | $48.42 | +12.60 | — | +0.00 | +12.60 | +15.90 | — |
| 2026-08-28 | `GRRR` | 106 | $15.66 | $15.66 | +0.00 | — | +0.00 | +0.00 | +184.44 | — |
| 2026-08-28 | `SHMD` | 325 | $3.40 | $3.38 | -6.50 | — | +0.00 | -6.50 | -378.62 | — |
| 2026-08-28 | `SLQT` | 34 | $0.54 | $0.53 | -0.31 | $0.52 | -0.37 | -0.68 | -1.77 | -2.14 |
| 2026-08-28 | `TIGR` | 3 | $5.06 | $5.05 | -0.03 | $5.04 | -0.01 | -0.04 | -0.48 | -0.50 |
| 2026-08-28 | `BBWI` | 1 | $18.65 | $18.75 | +0.10 | $19.22 | +0.47 | +0.57 | +0.49 | +0.96 |
| 2026-08-28 | `FSCO` | 12 | $5.12 | $5.12 | +0.00 | $5.18 | +0.72 | +0.72 | +0.48 | +1.20 |
| 2026-08-28 | `ADSK` | 5 | — | $261.16 | +0.00 | $260.66 | -2.50 | -2.50 | +0.00 | -2.50 |
| 2026-08-28 | `BBAR` | 96 | — | $15.01 | +0.00 | $14.47 | -51.84 | -51.84 | +0.00 | -51.84 |
| 2026-08-28 | `ESTC` | 13 | — | $103.89 | +0.00 | $99.91 | -51.74 | -51.74 | +0.00 | -51.74 |
| 2026-08-28 | `FINV` | 373 | — | $3.88 | +0.00 | $3.40 | -179.04 | -179.04 | +0.00 | -179.04 |
| 2026-08-28 | `FRO` | 32 | — | $44.40 | +0.00 | $44.19 | -6.72 | -6.72 | +0.00 | -6.72 |
| 2026-08-28 | `GAP` | 58 | — | $24.69 | +0.00 | $23.48 | -70.18 | -70.18 | +0.00 | -70.18 |
| 2026-08-28 | `HAFN` | 173 | — | $8.35 | +0.00 | $8.47 | +20.76 | +20.76 | +0.00 | +20.76 |
| 2026-08-28 | `IREN` | 38 | — | $37.65 | +0.00 | $35.45 | -83.41 | -83.41 | +0.00 | -83.41 |
| 2026-08-31 | `SLQT` | 34 | $0.52 | $0.51 | -0.34 | — | +0.00 | -0.34 | -2.48 | — |
| 2026-08-31 | `TIGR` | 3 | $5.04 | $5.00 | -0.13 | — | +0.00 | -0.13 | -0.63 | — |
| 2026-08-31 | `BBWI` | 1 | $19.22 | $19.25 | +0.03 | — | +0.00 | +0.03 | +0.99 | — |
| 2026-08-31 | `FSCO` | 12 | $5.18 | $5.20 | +0.24 | — | +0.00 | +0.24 | +1.44 | — |
| 2026-08-31 | `ADSK` | 5 | $260.66 | $257.71 | -14.75 | $258.53 | +4.10 | -10.65 | -17.25 | -13.15 |
| 2026-08-31 | `BBAR` | 96 | $14.47 | $14.88 | +39.36 | $15.14 | +24.96 | +64.32 | -12.48 | +12.48 |
| 2026-08-31 | `ESTC` | 13 | $99.91 | $98.00 | -24.83 | $97.55 | -5.85 | -30.68 | -76.57 | -82.42 |
| 2026-08-31 | `FINV` | 373 | $3.40 | $3.39 | -3.73 | $3.67 | +104.44 | +100.71 | -182.77 | -78.33 |
| 2026-08-31 | `FRO` | 32 | $44.19 | $44.85 | +21.12 | $43.78 | -34.24 | -13.12 | +14.40 | -19.84 |
| 2026-08-31 | `GAP` | 58 | $23.48 | $22.98 | -29.00 | $22.31 | -38.86 | -67.86 | -99.18 | -138.04 |
| 2026-08-31 | `HAFN` | 173 | $8.47 | $8.53 | +10.38 | $8.44 | -15.57 | -5.19 | +31.14 | +15.57 |
| 2026-08-31 | `IREN` | 38 | $35.45 | $35.81 | +13.68 | $37.12 | +49.59 | +63.27 | -69.73 | -20.14 |
| 2026-09-01 | `ADSK` | 5 | $258.53 | $253.48 | -25.25 | $247.69 | -28.95 | -54.20 | -38.40 | -67.35 |
| 2026-09-01 | `BBAR` | 96 | $15.14 | $14.82 | -30.72 | $15.11 | +27.84 | -2.88 | -18.24 | +9.60 |
| 2026-09-01 | `ESTC` | 13 | $97.55 | $95.76 | -23.27 | $92.39 | -43.81 | -67.08 | -105.69 | -149.50 |
| 2026-09-01 | `FINV` | 373 | $3.67 | $3.58 | -33.57 | $3.32 | -96.98 | -130.55 | -111.90 | -208.88 |
| 2026-09-01 | `FRO` | 32 | $43.78 | $44.39 | +19.52 | $44.32 | -2.24 | +17.28 | -0.32 | -2.56 |
| 2026-09-01 | `GAP` | 58 | $22.31 | $22.05 | -15.08 | $22.00 | -2.90 | -17.98 | -153.12 | -156.02 |
| 2026-09-01 | `HAFN` | 173 | $8.44 | $8.56 | +20.76 | $8.59 | +5.19 | +25.95 | +36.33 | +41.52 |
| 2026-09-01 | `IREN` | 38 | $37.12 | $36.08 | -39.33 | $36.82 | +28.12 | -11.21 | -59.47 | -31.35 |
| 2026-09-02 | `ADSK` | 5 | $247.69 | $246.70 | -4.95 | — | +0.00 | -4.95 | -72.30 | — |
| 2026-09-02 | `BBAR` | 96 | $15.11 | $15.01 | -9.60 | — | +0.00 | -9.60 | +0.00 | — |
| 2026-09-02 | `ESTC` | 13 | $92.39 | $92.00 | -5.07 | — | +0.00 | -5.07 | -154.57 | — |
| 2026-09-02 | `FINV` | 373 | $3.32 | $3.32 | +0.00 | — | +0.00 | +0.00 | -208.88 | — |
| 2026-09-02 | `FRO` | 32 | $44.32 | $44.17 | -4.80 | — | +0.00 | -4.80 | -7.36 | — |
| 2026-09-02 | `GAP` | 58 | $22.00 | $21.97 | -1.74 | — | +0.00 | -1.74 | -157.76 | — |
| 2026-09-02 | `HAFN` | 173 | $8.59 | $8.58 | -1.73 | — | +0.00 | -1.73 | +39.79 | — |
| 2026-09-02 | `IREN` | 38 | $36.82 | $35.80 | -38.95 | — | +0.00 | -38.95 | -70.30 | — |
| 2026-09-03 | `AI` | 128 | — | $10.74 | +0.00 | $10.90 | +19.84 | +19.84 | +0.00 | +19.84 |
| 2026-09-03 | `AVGO` | 3 | — | $351.74 | +0.00 | $357.16 | +16.26 | +16.26 | +0.00 | +16.26 |
| 2026-09-03 | `CHPT` | 199 | — | $6.90 | +0.00 | $9.08 | +433.82 | +433.82 | +0.00 | +433.82 |
| 2026-09-03 | `CIEN` | 3 | — | $354.49 | +0.00 | $317.46 | -111.09 | -111.09 | +0.00 | -111.09 |
| 2026-09-03 | `CPB` | 61 | — | $22.32 | +0.00 | $22.13 | -11.59 | -11.59 | +0.00 | -11.59 |
| 2026-09-03 | `FIVE` | 5 | — | $257.00 | +0.00 | $239.96 | -85.20 | -85.20 | +0.00 | -85.20 |
| 2026-09-03 | `HPE` | 28 | — | $47.60 | +0.00 | $54.44 | +191.52 | +191.52 | +0.00 | +191.52 |
| 2026-09-03 | `MEI` | 91 | — | $15.09 | +0.00 | $15.32 | +20.93 | +20.93 | +0.00 | +20.93 |
| 2026-09-04 | `AI` | 128 | $10.90 | $10.91 | +1.28 | $10.46 | -57.60 | -56.32 | +21.12 | -36.48 |
| 2026-09-04 | `AVGO` | 3 | $357.16 | $359.70 | +7.62 | $357.90 | -5.40 | +2.22 | +23.88 | +18.48 |
| 2026-09-04 | `CHPT` | 199 | $9.08 | $9.28 | +39.80 | $9.89 | +121.39 | +161.19 | +473.62 | +595.01 |
| 2026-09-04 | `CIEN` | 3 | $317.46 | $321.67 | +12.63 | $321.00 | -2.01 | +10.62 | -98.46 | -100.47 |
| 2026-09-04 | `CPB` | 61 | $22.13 | $22.10 | -1.83 | $21.38 | -43.92 | -45.75 | -13.42 | -57.34 |
| 2026-09-04 | `FIVE` | 5 | $239.96 | $238.88 | -5.40 | $252.20 | +66.60 | +61.20 | -90.60 | -24.00 |
| 2026-09-04 | `HPE` | 28 | $54.44 | $53.85 | -16.52 | $52.00 | -51.80 | -68.32 | +175.00 | +123.20 |
| 2026-09-04 | `MEI` | 91 | $15.32 | $15.34 | +1.82 | $15.69 | +31.85 | +33.67 | +22.75 | +54.60 |
| 2026-09-04 | `AMBA` | 1 | — | $63.18 | +0.00 | $62.89 | -0.29 | -0.29 | +0.00 | -0.29 |
| 2026-09-04 | `ASAN` | 11 | — | $8.74 | +0.00 | $8.81 | +0.77 | +0.77 | +0.00 | +0.77 |
| 2026-09-04 | `DOCU` | 1 | — | $68.52 | +0.00 | $68.41 | -0.11 | -0.11 | +0.00 | -0.11 |
| 2026-09-04 | `DOMO` | 27 | — | $3.62 | +0.00 | $3.88 | +7.15 | +7.15 | +0.00 | +7.15 |
| 2026-09-04 | `IOT` | 2 | — | $44.90 | +0.00 | $40.20 | -9.40 | -9.40 | +0.00 | -9.40 |
| 2026-09-04 | `LULU` | 1 | — | $98.15 | +0.00 | $100.61 | +2.46 | +2.46 | +0.00 | +2.46 |
| 2026-09-04 | `MAMA` | 6 | — | $15.70 | +0.00 | $15.16 | -3.24 | -3.24 | +0.00 | -3.24 |
| 2026-09-08 | `AI` | 128 | $10.46 | $10.20 | -33.28 | $10.51 | +39.68 | +6.40 | -69.76 | -30.08 |
| 2026-09-08 | `AVGO` | 3 | $357.90 | $363.68 | +17.34 | $368.56 | +14.64 | +31.98 | +35.82 | +50.46 |
| 2026-09-08 | `CHPT` | 199 | $9.89 | $9.91 | +3.98 | $9.37 | -107.46 | -103.48 | +598.99 | +491.53 |
| 2026-09-08 | `CIEN` | 3 | $321.00 | $327.42 | +19.26 | $341.29 | +41.61 | +60.87 | -81.21 | -39.60 |
| 2026-09-08 | `CPB` | 61 | $21.38 | $21.30 | -4.88 | $21.76 | +28.06 | +23.18 | -62.22 | -34.16 |
| 2026-09-08 | `FIVE` | 5 | $252.20 | $251.22 | -4.90 | $254.07 | +14.25 | +9.35 | -28.90 | -14.65 |
| 2026-09-08 | `HPE` | 28 | $52.00 | $52.29 | +8.12 | $56.03 | +104.72 | +112.84 | +131.32 | +236.04 |
| 2026-09-08 | `MEI` | 91 | $15.69 | $15.80 | +10.01 | $14.06 | -158.34 | -148.33 | +64.61 | -93.73 |
| 2026-09-08 | `AMBA` | 1 | $62.89 | $63.83 | +0.94 | $63.48 | -0.35 | +0.59 | +0.65 | +0.30 |
| 2026-09-08 | `ASAN` | 11 | $8.81 | $8.73 | -0.88 | $8.79 | +0.66 | -0.22 | -0.11 | +0.55 |
| 2026-09-08 | `DOCU` | 1 | $68.41 | $67.05 | -1.36 | $65.08 | -1.97 | -3.33 | -1.47 | -3.44 |
| 2026-09-08 | `DOMO` | 27 | $3.88 | $3.84 | -1.08 | $3.83 | -0.27 | -1.35 | +6.07 | +5.80 |
| 2026-09-08 | `IOT` | 2 | $40.20 | $39.56 | -1.28 | $40.15 | +1.18 | -0.10 | -10.68 | -9.50 |
| 2026-09-08 | `LULU` | 1 | $100.61 | $100.58 | -0.03 | $103.19 | +2.61 | +2.58 | +2.43 | +5.04 |
| 2026-09-08 | `MAMA` | 6 | $15.16 | $15.20 | +0.24 | $15.50 | +1.80 | +2.04 | -3.00 | -1.20 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +840.92 | INO, VOR | — | $21.06 | $10,769.53 | INO×6172, VOR×223 |
| 2026-08-14 | +5.50 | $21.06 | INO×6172, VOR×223 | $10,963.61 | +194.08 | +920.24 | EU, BZAI, DEFT | — | $14.67 | $11,883.77 | INO×6172, VOR×223, EU×1, BZAI×3, DEFT×6 |
| 2026-08-17 | +2.25 | $14.67 | INO×6172, VOR×223, EU×1, BZAI×3, DEFT×6 | $11,733.36 | -150.41 | +515.83 | — | — | $14.67 | $12,249.19 | INO×6172, VOR×223, EU×1, BZAI×3, DEFT×6 |
| 2026-08-18 | -6.20 | $14.67 | INO×6172, VOR×223, EU×1, BZAI×3, DEFT×6 | $12,144.92 | -104.27 | +0.06 | — | INO, VOR | $12,055.96 | $12,061.33 | EU×1, BZAI×3, DEFT×6 |
| 2026-08-19 | -7.20 | $12,055.96 | EU×1, BZAI×3, DEFT×6 | $12,061.35 | +0.02 | +0.00 | — | EU, BZAI, DEFT | $12,061.21 | $12,061.21 | — |
| 2026-08-20 | +1.12 | $12,061.21 | — | $12,061.21 | -0.00 | -123.45 | EL, TOYO, DVLT, AAP, AEG, ALVO, ATAT, ATHM, BABA, BILL, BULL | — | $85.04 | $11,900.40 | EL×7, TOYO×170, DVLT×2512, AAP×16, AEG×83, ALVO×193, ATAT×22, ATHM×33, BABA×16, BILL×41, BULL×204 |
| 2026-08-21 | +3.25 | $85.04 | EL×7, TOYO×170, DVLT×2512, AAP×16, AEG×83, ALVO×193, ATAT×22, ATHM×33, BABA×16, BILL×41, BULL×204 | $11,873.73 | -26.67 | -10.43 | PSEC | — | $80.39 | $11,863.25 | EL×7, TOYO×170, DVLT×2512, AAP×16, AEG×83, ALVO×193, ATAT×22, ATHM×33, BABA×16, BILL×41, BULL×204, PSEC×2 |
| 2026-08-24 | -5.17 | $80.39 | EL×7, TOYO×170, DVLT×2512, AAP×16, AEG×83, ALVO×193, ATAT×22, ATHM×33, BABA×16, BILL×41, BULL×204, PSEC×2 | $11,793.55 | -69.70 | +113.56 | — | — | $80.39 | $11,907.11 | EL×7, TOYO×170, DVLT×2512, AAP×16, AEG×83, ALVO×193, ATAT×22, ATHM×33, BABA×16, BILL×41, BULL×204, PSEC×2 |
| 2026-08-25 | +1.80 | $80.39 | EL×7, TOYO×170, DVLT×2512, AAP×16, AEG×83, ALVO×193, ATAT×22, ATHM×33, BABA×16, BILL×41, BULL×204, PSEC×2 | $11,883.79 | -23.32 | -431.12 | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | EL, TOYO, DVLT, AAP, AEG, ALVO, ATAT, ATHM, BABA, BILL, BULL | $238.22 | $11,393.68 | PSEC×2, BMO×8, BNS×16, BZ×96, DKS×10, EH×290, GFI×30, GRRR×106, SHMD×325 |
| 2026-08-26 | +2.02 | $238.22 | PSEC×2, BMO×8, BNS×16, BZ×96, DKS×10, EH×290, GFI×30, GRRR×106, SHMD×325 | $11,355.88 | -37.80 | +384.17 | SLQT, TIGR, BBWI, FSCO | PSEC | $126.88 | $11,738.68 | BMO×8, BNS×16, BZ×96, DKS×10, EH×290, GFI×30, GRRR×106, SHMD×325, SLQT×34, TIGR×3, BBWI×1, FSCO×12 |
| 2026-08-27 | — | $126.88 | BMO×8, BNS×16, BZ×96, DKS×10, EH×290, GFI×30, GRRR×106, SHMD×325, SLQT×34, TIGR×3, BBWI×1, FSCO×12 | $11,759.22 | +20.54 | -52.73 | — | — | $126.88 | $11,706.49 | BMO×8, BNS×16, BZ×96, DKS×10, EH×290, GFI×30, GRRR×106, SHMD×325, SLQT×34, TIGR×3, BBWI×1, FSCO×12 |
| 2026-08-28 | +0.75 | $126.88 | BMO×8, BNS×16, BZ×96, DKS×10, EH×290, GFI×30, GRRR×106, SHMD×325, SLQT×34, TIGR×3, BBWI×1, FSCO×12 | $11,724.81 | +18.32 | -423.86 | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | $298.04 | $11,260.01 | SLQT×34, TIGR×3, BBWI×1, FSCO×12, ADSK×5, BBAR×96, ESTC×13, FINV×373, FRO×32, GAP×58, HAFN×173, IREN×38 |
| 2026-08-31 | -5.85 | $298.04 | SLQT×34, TIGR×3, BBWI×1, FSCO×12, ADSK×5, BBAR×96, ESTC×13, FINV×373, FRO×32, GAP×58, HAFN×173, IREN×38 | $11,272.04 | +12.03 | +88.57 | — | SLQT, TIGR, BBWI, FSCO | $410.66 | $11,359.24 | ADSK×5, BBAR×96, ESTC×13, FINV×373, FRO×32, GAP×58, HAFN×173, IREN×38 |
| 2026-09-01 | -6.30 | $410.66 | ADSK×5, BBAR×96, ESTC×13, FINV×373, FRO×32, GAP×58, HAFN×173, IREN×38 | $11,232.30 | -126.94 | -113.73 | — | — | $410.66 | $11,118.57 | ADSK×5, BBAR×96, ESTC×13, FINV×373, FRO×32, GAP×58, HAFN×173, IREN×38 |
| 2026-09-02 | -3.83 | $410.66 | ADSK×5, BBAR×96, ESTC×13, FINV×373, FRO×32, GAP×58, HAFN×173, IREN×38 | $11,051.73 | -66.84 | +0.00 | — | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | $11,031.50 | $11,031.50 | — |
| 2026-09-03 | -0.90 | $11,031.50 | — | $11,031.50 | +0.00 | +474.49 | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | — | $794.37 | $11,488.52 | AI×128, AVGO×3, CHPT×199, CIEN×3, CPB×61, FIVE×5, HPE×28, MEI×91 |
| 2026-09-04 | +2.25 | $794.37 | AI×128, AVGO×3, CHPT×199, CIEN×3, CPB×61, FIVE×5, HPE×28, MEI×91 | $11,527.92 | +39.40 | +56.45 | AMBA, ASAN, DOCU, DOMO, IOT, LULU, MAMA | — | $180.55 | $11,578.15 | AI×128, AVGO×3, CHPT×199, CIEN×3, CPB×61, FIVE×5, HPE×28, MEI×91, AMBA×1, ASAN×11, DOCU×1, DOMO×27, IOT×2, LULU×1, MAMA×6 |
| 2026-09-08 | -11.47 | $180.55 | AI×128, AVGO×3, CHPT×199, CIEN×3, CPB×61, FIVE×5, HPE×28, MEI×91, AMBA×1, ASAN×11, DOCU×1, DOMO×27, IOT×2, LULU×1, MAMA×6 | $11,590.35 | +12.20 | -19.18 | — | — | $180.55 | $11,571.17 | AI×128, AVGO×3, CHPT×199, CIEN×3, CPB×61, FIVE×5, HPE×28, MEI×91, AMBA×1, ASAN×11, DOCU×1, DOMO×27, IOT×2, LULU×1, MAMA×6 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 (unchanged overnight, no fees) · equity $10,000.00 vs prior close $10,000.00 (+0.00) | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 6172 | $0.81 | $68.51 | — | $4,932.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; combo leftover $5000.00; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 223 | $22.01 | $2.88 | — | $21.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; combo leftover $5000.00; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.06 | ▲ close $10,769.53 vs 09:30 $10,000.00 (session +840.92) | 16:00 close · cash $21.06 · equity $10,769.53 vs 09:30 $10,000.00 (+769.53; session marks +840.92) · 2 name(s) marked open→close (per-name table). INO×6172 09:30 $0.81 → close $0.90 +555.48; VOR×223 09:30 $22.01 → close $23.29 +285.44 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.06 | ▲ 09:30 equity $10,963.61 vs yday $10,769.53 (+194.08) | 09:30 open · cash $21.06 (unchanged overnight, no fees) · equity $10,963.61 vs prior close $10,769.53 (+194.08) | — |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 1 | $1.18 | $0.01 | — | $19.87 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $1.32; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 3 | $0.77 | $0.03 | — | $17.54 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+20.4; combo leftover $2.84; owner union_earn_react_h3 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DEFT` | 6 | $0.47 | $0.05 | — | $14.67 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+11.1; combo leftover $2.84; owner union_earn_react_h3 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.67 | ▲ close $11,883.77 vs 09:30 $10,963.61 (session +920.24) | 16:00 close · cash $14.67 · equity $11,883.77 vs 09:30 $10,963.61 (+920.16; session marks +920.24) · 5 name(s) marked open→close (per-name table). INO×6172 09:30 $0.93 → close $1.09 +987.52; VOR×223 09:30 $23.33 → close $23.03 -66.90; EU×1 09:30 $1.18 → close $1.21 +0.03; BZAI×3 09:30 $0.77 → close $0.59 -0.52; DEFT×6 09:30 $0.47 → close $0.49 +0.11 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.67 | ▼ 09:30 equity $11,733.36 vs yday $11,883.77 (-150.41) | 09:30 open · cash $14.67 (unchanged overnight, no fees) · equity $11,733.36 vs prior close $11,883.77 (-150.41) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.67 | ▲ close $12,249.19 vs 09:30 $11,733.36 (session +515.83) | 16:00 close · cash $14.67 · equity $12,249.19 vs 09:30 $11,733.36 (+515.83; session marks +515.83) · 5 name(s) marked open→close (per-name table). INO×6172 09:30 $1.07 → close $1.15 +493.76; VOR×223 09:30 $22.91 → close $23.01 +22.30; EU×1 09:30 $1.21 → close $1.13 -0.08; BZAI×3 09:30 $0.55 → close $0.52 -0.09; DEFT×6 09:30 $0.47 → close $0.47 -0.06 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.67 | ▼ 09:30 equity $12,144.92 vs yday $12,249.19 (-104.27) | 09:30 open · cash $14.67 (unchanged overnight, no fees) · equity $12,144.92 vs prior close $12,249.19 (-104.27) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 6172 | $1.14 | $80.70 | $+1887.55 | $6,970.05 | ▲ +1,887.55 after sell → book $12,064.22; vs 09:30 mark -80.70 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 223 | $22.82 | $2.95 | $+174.80 | $12,055.96 | ▲ +174.80 after sell → book $12,061.26; vs 09:30 mark -2.96 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,055.96 | ▲ close $12,061.33 vs 09:30 $12,144.92 (session +0.06) | 16:00 close · cash $12,055.96 · equity $12,061.33 vs 09:30 $12,144.92 (-83.59; session marks +0.06) · 3 name(s) marked open→close (per-name table). EU×1 09:30 $1.13 → close $1.07 -0.06; BZAI×3 09:30 $0.49 → close $0.56 +0.20; DEFT×6 09:30 $0.45 → close $0.44 -0.08 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,055.96 | ▲ 09:30 equity $12,061.35 vs yday $12,061.33 (+0.02) | 09:30 open · cash $12,055.96 (unchanged overnight, no fees) · equity $12,061.35 vs prior close $12,061.33 (+0.02) | — |
| 2026-08-19 09:30 ET | **SELL** | `EU` | 1 | $1.07 | $0.03 | $-0.16 | $12,057.00 | ▼ -0.16 after sell → book $12,061.32; vs 09:30 mark -0.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BZAI` | 3 | $0.57 | $0.05 | $-0.67 | $12,058.66 | ▼ -0.67 after sell → book $12,061.27; vs 09:30 mark -0.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DEFT` | 6 | $0.43 | $0.06 | $-0.32 | $12,061.21 | ▼ -0.32 after sell → book $12,061.21; vs 09:30 mark -0.06 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,061.21 | ▲ close $12,061.21 vs 09:30 $12,061.35 (session +0.00) | 16:00 close · cash $12,061.21 · no lots left · equity $12,061.21. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,061.21 | ▲ 09:30 equity $12,061.21 vs yday $12,061.21 (-0.00) | 09:30 open · cash $12,061.21 (unchanged overnight, no fees) · equity $12,061.21 vs prior close $12,061.21 (-0.00) | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 7 | $97.43 | $2.01 | — | $11,377.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $753.83; owner union_e_fresh_h3 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 170 | $4.43 | $2.50 | — | $10,621.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $753.83; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 2512 | $0.30 | $15.07 | — | $9,852.91 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $753.83; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 16 | $46.85 | $2.04 | — | $9,101.27 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $753.83; owner union_e_fresh_h3 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 83 | $9.01 | $2.24 | — | $8,351.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $753.83; owner union_e_fresh_h3 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 193 | $3.89 | $2.57 | — | $7,597.87 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $753.83; owner union_e_fresh_h3 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 22 | $34.05 | $2.06 | — | $6,846.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $753.83; owner union_e_fresh_h3 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 33 | $22.44 | $2.09 | — | $6,104.10 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $753.83; owner union_e_fresh_h3 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 16 | $123.47 | $2.04 | — | $4,126.54 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+2.9; combo leftover $2034.70; owner union_earn_react_h3 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BILL` | 41 | $49.00 | $2.11 | — | $2,115.43 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.0; combo leftover $2034.70; owner union_earn_react_h3 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 204 | $9.94 | $2.63 | — | $85.04 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+12.6; combo leftover $2034.70; owner union_earn_react_h3 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.04 | ▼ close $11,900.40 vs 09:30 $12,061.21 (session -123.45) | 16:00 close · cash $85.04 · equity $11,900.40 vs 09:30 $12,061.21 (-160.81; session marks -123.45) · 11 name(s) marked open→close (per-name table). EL×7 09:30 $97.43 → close $96.15 -8.96; TOYO×170 09:30 $4.43 → close $4.51 +14.45; DVLT×2512 09:30 $0.30 → close $0.32 +50.24; AAP×16 09:30 $46.85 → close $42.39 -71.36; AEG×83 09:30 $9.01 → close $9.01 +0.00; ALVO×193 09:30 $3.89 → close $4.27 +73.34; ATAT×22 09:30 $34.05 → close $34.25 +4.40; ATHM×33 09:30 $22.44 → close $22.12 -10.56; BABA×16 09:30 $123.47 → close $130.53 +112.96; BILL×41 09:30 $49.00 → close $47.40 -65.60; BULL×204 09:30 $9.94 → close $8.85 -222.36 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.04 | ▼ 09:30 equity $11,873.73 vs yday $11,900.40 (-26.67) | 09:30 open · cash $85.04 (unchanged overnight, no fees) · equity $11,873.73 vs prior close $11,900.40 (-26.67) | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 2 | $2.30 | $0.05 | — | $80.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $6.07; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $80.39 | ▼ close $11,863.25 vs 09:30 $11,873.73 (session -10.43) | 16:00 close · cash $80.39 · equity $11,863.25 vs 09:30 $11,873.73 (-10.48; session marks -10.43) · 12 name(s) marked open→close (per-name table). EL×7 09:30 $96.75 → close $101.94 +36.33; TOYO×170 09:30 $4.68 → close $4.82 +23.80; DVLT×2512 09:30 $0.31 → close $0.32 +25.12; AAP×16 09:30 $42.41 → close $42.58 +2.72; AEG×83 09:30 $9.04 → close $8.99 -4.15; ALVO×193 09:30 $4.32 → close $4.43 +21.23; ATAT×22 09:30 $34.31 → close $34.75 +9.68; ATHM×33 09:30 $22.20 → close $22.22 +0.66; BABA×16 09:30 $125.35 → close $119.34 -96.16; BILL×41 09:30 $47.50 → close $47.82 +13.12; BULL×204 09:30 $8.99 → close $8.78 -42.84; PSEC×2 09:30 $2.30 → close $2.33 +0.06 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.39 | ▼ 09:30 equity $11,793.55 vs yday $11,863.25 (-69.70) | 09:30 open · cash $80.39 (unchanged overnight, no fees) · equity $11,793.55 vs prior close $11,863.25 (-69.70) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $80.39 | ▲ close $11,907.11 vs 09:30 $11,793.55 (session +113.56) | 16:00 close · cash $80.39 · equity $11,907.11 vs 09:30 $11,793.55 (+113.56; session marks +113.56) · 12 name(s) marked open→close (per-name table). EL×7 09:30 $101.92 → close $104.13 +15.47; TOYO×170 09:30 $4.58 → close $4.38 -34.00; DVLT×2512 09:30 $0.31 → close $0.31 +0.00; AAP×16 09:30 $43.05 → close $43.63 +9.28; AEG×83 09:30 $9.15 → close $9.19 +3.32; ALVO×193 09:30 $4.79 → close $5.25 +88.78; ATAT×22 09:30 $34.70 → close $34.75 +1.10; ATHM×33 09:30 $22.00 → close $21.85 -4.95; BABA×16 09:30 $116.90 → close $118.47 +25.12; BILL×41 09:30 $47.68 → close $48.16 +19.68; BULL×204 09:30 $8.58 → close $8.53 -10.20; PSEC×2 09:30 $2.34 → close $2.32 -0.04 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.39 | ▼ 09:30 equity $11,883.79 vs yday $11,907.11 (-23.32) | 09:30 open · cash $80.39 (unchanged overnight, no fees) · equity $11,883.79 vs prior close $11,907.11 (-23.32) | — |
| 2026-08-25 09:30 ET | **SELL** | `EL` | 7 | $104.00 | $2.03 | $+41.95 | $806.36 | ▲ +41.95 after sell → book $11,881.76; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TOYO` | 170 | $4.42 | $2.54 | $-6.74 | $1,555.22 | ▼ -6.74 after sell → book $11,879.22; vs 09:30 mark -2.54 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 2512 | $0.31 | $15.75 | $-5.70 | $2,318.19 | ▼ -5.70 after sell → book $11,863.47; vs 09:30 mark -15.75 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 16 | $43.63 | $2.06 | $-55.62 | $3,014.21 | ▼ -55.62 after sell → book $11,861.41; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 83 | $9.23 | $2.26 | $+13.76 | $3,778.04 | ▲ +13.76 after sell → book $11,859.15; vs 09:30 mark -2.26 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 193 | $5.24 | $2.61 | $+255.37 | $4,786.75 | ▲ +255.37 after sell → book $11,856.54; vs 09:30 mark -2.61 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 22 | $34.72 | $2.08 | $+10.61 | $5,548.51 | ▲ +10.61 after sell → book $11,854.46; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 33 | $21.85 | $2.11 | $-23.67 | $6,267.45 | ▼ -23.67 after sell → book $11,852.36; vs 09:30 mark -2.10 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BABA` | 16 | $117.94 | $2.06 | $-92.58 | $8,152.43 | ▼ -92.58 after sell → book $11,850.29; vs 09:30 mark -2.07 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BILL` | 41 | $47.98 | $2.14 | $-45.87 | $10,117.67 | ▼ -45.87 after sell → book $11,848.15; vs 09:30 mark -2.14 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BULL` | 204 | $8.46 | $2.68 | $-307.23 | $11,840.83 | ▼ -307.23 after sell → book $11,845.47; vs 09:30 mark -2.68 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 8 | $175.01 | $2.01 | — | $10,438.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $1480.10; owner union_e_fresh_h3 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 16 | $88.94 | $2.04 | — | $9,013.66 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $1480.10; owner union_e_fresh_h3 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 96 | $15.28 | $2.28 | — | $7,544.50 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $1480.10; owner union_e_fresh_h3 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 10 | $142.36 | $2.02 | — | $6,118.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $1480.10; owner union_e_fresh_h3 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 290 | $5.10 | $3.74 | — | $4,636.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $1480.10; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 30 | $47.89 | $2.08 | — | $3,197.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $1480.10; owner union_e_fresh_h3 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 106 | $13.92 | $2.31 | — | $1,719.54 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $1480.10; owner union_e_fresh_h3 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 325 | $4.54 | $4.19 | — | $238.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $1480.10; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $238.22 | ▼ close $11,393.68 vs 09:30 $11,883.79 (session -431.12) | 16:00 close · cash $238.22 · equity $11,393.68 vs 09:30 $11,883.79 (-490.11; session marks -431.12) · 9 name(s) marked open→close (per-name table). PSEC×2 09:30 $2.32 → close $2.35 +0.06; BMO×8 09:30 $175.01 → close $173.46 -12.40; BNS×16 09:30 $88.94 → close $93.10 +66.56; BZ×96 09:30 $15.28 → close $16.29 +96.96; DKS×10 09:30 $142.36 → close $124.31 -180.50; EH×290 09:30 $5.10 → close $4.83 -78.30; GFI×30 09:30 $47.89 → close $48.87 +29.40; GRRR×106 09:30 $13.92 → close $14.04 +12.72; SHMD×325 09:30 $4.54 → close $3.42 -365.62 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $238.22 | ▼ 09:30 equity $11,355.88 vs yday $11,393.68 (-37.80) | 09:30 open · cash $238.22 (unchanged overnight, no fees) · equity $11,355.88 vs prior close $11,393.68 (-37.80) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 2 | $2.35 | $0.07 | $-0.02 | $242.84 | ▼ -0.02 after sell → book $11,355.80; vs 09:30 mark -0.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 34 | $0.58 | $0.30 | — | $222.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $20.24; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 3 | $5.21 | $0.17 | — | $206.93 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $20.24; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 1 | $18.26 | $0.19 | — | $188.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $20.24; owner union_e_fresh_h3 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FSCO` | 12 | $5.08 | $0.65 | — | $126.88 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-1.6; combo leftover $62.83; owner union_earn_react_h3 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.88 | ▲ close $11,738.68 vs 09:30 $11,355.88 (session +384.17) | 16:00 close · cash $126.88 · equity $11,738.68 vs 09:30 $11,355.88 (+382.80; session marks +384.17) · 12 name(s) marked open→close (per-name table). BMO×8 09:30 $173.22 → close $172.90 -2.56; BNS×16 09:30 $92.65 → close $93.59 +15.04; BZ×96 09:30 $16.77 → close $18.84 +198.72; DKS×10 09:30 $121.87 → close $129.66 +77.90; EH×290 09:30 $4.77 → close $4.86 +24.65; GFI×30 09:30 $48.24 → close $47.82 -12.60; GRRR×106 09:30 $14.03 → close $15.45 +150.52; SHMD×325 09:30 $3.38 → close $3.17 -68.25; SLQT×34 09:30 $0.58 → close $0.55 -1.12; TIGR×3 09:30 $5.21 → close $5.46 +0.75; BBWI×1 09:30 $18.26 → close $18.90 +0.64; FSCO×12 09:30 $5.08 → close $5.12 +0.48 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.88 | ▲ 09:30 equity $11,759.22 vs yday $11,738.68 (+20.54) | 09:30 open · cash $126.88 (unchanged overnight, no fees) · equity $11,759.22 vs prior close $11,738.68 (+20.54) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.88 | ▼ close $11,706.49 vs 09:30 $11,759.22 (session -52.73) | 16:00 close · cash $126.88 · equity $11,706.49 vs 09:30 $11,759.22 (-52.73; session marks -52.73) · 12 name(s) marked open→close (per-name table). BMO×8 09:30 $172.85 → close $172.13 -5.76; BNS×16 09:30 $93.52 → close $92.93 -9.44; BZ×96 09:30 $18.50 → close $18.00 -48.00; DKS×10 09:30 $128.73 → close $131.77 +30.40; EH×290 09:30 $4.90 → close $4.66 -69.60; GFI×30 09:30 $47.93 → close $48.00 +2.10; GRRR×106 09:30 $15.94 → close $15.66 -29.68; SHMD×325 09:30 $3.16 → close $3.40 +78.00; SLQT×34 09:30 $0.53 → close $0.54 +0.34; TIGR×3 09:30 $5.49 → close $5.06 -1.29; BBWI×1 09:30 $18.69 → close $18.65 -0.04; FSCO×12 09:30 $5.10 → close $5.12 +0.24 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.88 | ▲ 09:30 equity $11,724.81 vs yday $11,706.49 (+18.32) | 09:30 open · cash $126.88 (unchanged overnight, no fees) · equity $11,724.81 vs prior close $11,706.49 (+18.32) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 8 | $172.76 | $2.04 | $-22.05 | $1,506.92 | ▼ -22.05 after sell → book $11,722.77; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 16 | $93.30 | $2.06 | $+65.66 | $2,997.66 | ▲ +65.66 after sell → book $11,720.72; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 96 | $18.15 | $2.31 | $+270.93 | $4,737.75 | ▲ +270.93 after sell → book $11,718.41; vs 09:30 mark -2.31 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 10 | $132.80 | $2.04 | $-99.66 | $6,063.71 | ▼ -99.66 after sell → book $11,716.37; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 290 | $4.58 | $3.80 | $-158.34 | $7,388.11 | ▼ -158.34 after sell → book $11,712.57; vs 09:30 mark -3.80 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 30 | $48.42 | $2.10 | $+11.72 | $8,838.61 | ▲ +11.72 after sell → book $11,710.47; vs 09:30 mark -2.10 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 106 | $15.66 | $2.34 | $+179.79 | $10,496.23 | ▲ +179.79 after sell → book $11,708.13; vs 09:30 mark -2.34 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 325 | $3.38 | $4.26 | $-387.07 | $11,590.48 | ▼ -387.07 after sell → book $11,703.87; vs 09:30 mark -4.26 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $10,282.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $1448.81; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 96 | $15.01 | $2.28 | — | $8,839.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $1448.81; owner union_e_fresh_h3 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 13 | $103.89 | $2.03 | — | $7,486.83 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $1448.81; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 373 | $3.88 | $4.81 | — | $6,034.78 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $1448.81; owner union_e_fresh_h3 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 32 | $44.40 | $2.09 | — | $4,611.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $1448.81; owner union_e_fresh_h3 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 58 | $24.69 | $2.16 | — | $3,177.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $1448.81; owner union_e_fresh_h3 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 173 | $8.35 | $2.51 | — | $1,730.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $1448.81; owner union_e_fresh_h3 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 38 | $37.65 | $2.10 | — | $298.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $1448.81; owner union_e_fresh_h3 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $298.04 | ▼ close $11,260.01 vs 09:30 $11,724.81 (session -423.86) | 16:00 close · cash $298.04 · equity $11,260.01 vs 09:30 $11,724.81 (-464.80; session marks -423.86) · 12 name(s) marked open→close (per-name table). SLQT×34 09:30 $0.53 → close $0.52 -0.37; TIGR×3 09:30 $5.05 → close $5.04 -0.01; BBWI×1 09:30 $18.75 → close $19.22 +0.47; FSCO×12 09:30 $5.12 → close $5.18 +0.72; ADSK×5 09:30 $261.16 → close $260.66 -2.50; BBAR×96 09:30 $15.01 → close $14.47 -51.84; ESTC×13 09:30 $103.89 → close $99.91 -51.74; FINV×373 09:30 $3.88 → close $3.40 -179.04; FRO×32 09:30 $44.40 → close $44.19 -6.72; GAP×58 09:30 $24.69 → close $23.48 -70.18; HAFN×173 09:30 $8.35 → close $8.47 +20.76; IREN×38 09:30 $37.65 → close $35.45 -83.41 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $298.04 | ▲ 09:30 equity $11,272.04 vs yday $11,260.01 (+12.03) | 09:30 open · cash $298.04 (unchanged overnight, no fees) · equity $11,272.04 vs prior close $11,260.01 (+12.03) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 34 | $0.51 | $0.30 | $-3.08 | $315.08 | ▼ -3.08 after sell → book $11,271.74; vs 09:30 mark -0.30 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 3 | $5.00 | $0.18 | $-0.97 | $329.90 | ▼ -0.97 after sell → book $11,271.56; vs 09:30 mark -0.18 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 1 | $19.25 | $0.22 | $+0.59 | $348.94 | ▲ +0.59 after sell → book $11,271.35; vs 09:30 mark -0.21 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FSCO` | 12 | $5.20 | $0.68 | $+0.11 | $410.66 | ▲ +0.11 after sell → book $11,270.67; vs 09:30 mark -0.68 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $410.66 | ▲ close $11,359.24 vs 09:30 $11,272.04 (session +88.57) | 16:00 close · cash $410.66 · equity $11,359.24 vs 09:30 $11,272.04 (+87.20; session marks +88.57) · 8 name(s) marked open→close (per-name table). ADSK×5 09:30 $257.71 → close $258.53 +4.10; BBAR×96 09:30 $14.88 → close $15.14 +24.96; ESTC×13 09:30 $98.00 → close $97.55 -5.85; FINV×373 09:30 $3.39 → close $3.67 +104.44; FRO×32 09:30 $44.85 → close $43.78 -34.24; GAP×58 09:30 $22.98 → close $22.31 -38.86; HAFN×173 09:30 $8.53 → close $8.44 -15.57; IREN×38 09:30 $35.81 → close $37.12 +49.59 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $410.66 | ▼ 09:30 equity $11,232.30 vs yday $11,359.24 (-126.94) | 09:30 open · cash $410.66 (unchanged overnight, no fees) · equity $11,232.30 vs prior close $11,359.24 (-126.94) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $410.66 | ▼ close $11,118.57 vs 09:30 $11,232.30 (session -113.73) | 16:00 close · cash $410.66 · equity $11,118.57 vs 09:30 $11,232.30 (-113.73; session marks -113.73) · 8 name(s) marked open→close (per-name table). ADSK×5 09:30 $253.48 → close $247.69 -28.95; BBAR×96 09:30 $14.82 → close $15.11 +27.84; ESTC×13 09:30 $95.76 → close $92.39 -43.81; FINV×373 09:30 $3.58 → close $3.32 -96.98; FRO×32 09:30 $44.39 → close $44.32 -2.24; GAP×58 09:30 $22.05 → close $22.00 -2.90; HAFN×173 09:30 $8.56 → close $8.59 +5.19; IREN×38 09:30 $36.08 → close $36.82 +28.12 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $410.66 | ▼ 09:30 equity $11,051.73 vs yday $11,118.57 (-66.84) | 09:30 open · cash $410.66 (unchanged overnight, no fees) · equity $11,051.73 vs prior close $11,118.57 (-66.84) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 5 | $246.70 | $2.02 | $-76.33 | $1,642.13 | ▼ -76.33 after sell → book $11,049.70; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 96 | $15.01 | $2.31 | $-4.58 | $3,080.79 | ▼ -4.58 after sell → book $11,047.40; vs 09:30 mark -2.30 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 13 | $92.00 | $2.05 | $-158.65 | $4,274.74 | ▼ -158.65 after sell → book $11,045.35; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 373 | $3.32 | $4.88 | $-218.58 | $5,508.22 | ▼ -218.58 after sell → book $11,040.47; vs 09:30 mark -4.88 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 32 | $44.17 | $2.11 | $-11.55 | $6,919.55 | ▼ -11.55 after sell → book $11,038.36; vs 09:30 mark -2.11 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 58 | $21.97 | $2.18 | $-162.11 | $8,191.62 | ▼ -162.11 after sell → book $11,036.17; vs 09:30 mark -2.19 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 173 | $8.58 | $2.55 | $+34.73 | $9,673.41 | ▲ +34.73 after sell → book $11,033.62; vs 09:30 mark -2.55 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 38 | $35.80 | $2.12 | $-74.53 | $11,031.50 | ▼ -74.53 after sell → book $11,031.50; vs 09:30 mark -2.12 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,031.50 | ▲ close $11,031.50 vs 09:30 $11,051.73 (session +0.00) | 16:00 close · cash $11,031.50 · no lots left · equity $11,031.50. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,031.50 | ▲ 09:30 equity $11,031.50 vs yday $11,031.50 (+0.00) | 09:30 open · cash $11,031.50 (unchanged overnight, no fees) · equity $11,031.50 vs prior close $11,031.50 (+0.00) | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 128 | $10.74 | $2.37 | — | $9,653.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $1378.94; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,596.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $1378.94; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 199 | $6.90 | $2.59 | — | $7,220.86 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $1378.94; owner union_e_fresh_h3 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 3 | $354.49 | $2.00 | — | $6,155.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $1378.94; owner union_e_fresh_h3 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 61 | $22.32 | $2.17 | — | $4,791.70 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $1378.94; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 5 | $257.00 | $2.00 | — | $3,504.69 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $1378.94; owner union_e_fresh_h3 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 28 | $47.60 | $2.07 | — | $2,169.82 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $1378.94; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 91 | $15.09 | $2.26 | — | $794.37 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $1378.94; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $794.37 | ▲ close $11,488.52 vs 09:30 $11,031.50 (session +474.49) | 16:00 close · cash $794.37 · equity $11,488.52 vs 09:30 $11,031.50 (+457.02; session marks +474.49) · 8 name(s) marked open→close (per-name table). AI×128 09:30 $10.74 → close $10.90 +19.84; AVGO×3 09:30 $351.74 → close $357.16 +16.26; CHPT×199 09:30 $6.90 → close $9.08 +433.82; CIEN×3 09:30 $354.49 → close $317.46 -111.09; CPB×61 09:30 $22.32 → close $22.13 -11.59; FIVE×5 09:30 $257.00 → close $239.96 -85.20; HPE×28 09:30 $47.60 → close $54.44 +191.52; MEI×91 09:30 $15.09 → close $15.32 +20.93 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $794.37 | ▲ 09:30 equity $11,527.92 vs yday $11,488.52 (+39.40) | 09:30 open · cash $794.37 (unchanged overnight, no fees) · equity $11,527.92 vs prior close $11,488.52 (+39.40) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 1 | $63.18 | $0.63 | — | $730.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $99.30; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 11 | $8.74 | $0.99 | — | $633.42 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $99.30; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 1 | $68.52 | $0.69 | — | $564.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $99.30; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 27 | $3.62 | $1.06 | — | $465.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $99.30; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 2 | $44.90 | $0.90 | — | $374.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $99.30; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 1 | $98.15 | $0.98 | — | $275.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $99.30; owner union_e_fresh_h3 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 6 | $15.70 | $0.96 | — | $180.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $99.30; owner union_e_fresh_h3 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $180.55 | ▲ close $11,578.15 vs 09:30 $11,527.92 (session +56.45) | 16:00 close · cash $180.55 · equity $11,578.15 vs 09:30 $11,527.92 (+50.23; session marks +56.45) · 15 name(s) marked open→close (per-name table). AI×128 09:30 $10.91 → close $10.46 -57.60; AVGO×3 09:30 $359.70 → close $357.90 -5.40; CHPT×199 09:30 $9.28 → close $9.89 +121.39; CIEN×3 09:30 $321.67 → close $321.00 -2.01; CPB×61 09:30 $22.10 → close $21.38 -43.92; FIVE×5 09:30 $238.88 → close $252.20 +66.60; HPE×28 09:30 $53.85 → close $52.00 -51.80; MEI×91 09:30 $15.34 → close $15.69 +31.85; AMBA×1 09:30 $63.18 → close $62.89 -0.29; ASAN×11 09:30 $8.74 → close $8.81 +0.77; DOCU×1 09:30 $68.52 → close $68.41 -0.11; DOMO×27 09:30 $3.62 → close $3.88 +7.15; IOT×2 09:30 $44.90 → close $40.20 -9.40; LULU×1 09:30 $98.15 → close $100.61 +2.46; MAMA×6 09:30 $15.70 → close $15.16 -3.24 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $180.55 | ▲ 09:30 equity $11,590.35 vs yday $11,578.15 (+12.20) | 09:30 open · cash $180.55 (unchanged overnight, no fees) · equity $11,590.35 vs prior close $11,578.15 (+12.20) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $180.55 | ▼ close $11,571.17 vs 09:30 $11,590.35 (session -19.18) | 16:00 close · cash $180.55 · equity $11,571.17 vs 09:30 $11,590.35 (-19.18; session marks -19.18) · 15 name(s) marked open→close (per-name table). AI×128 09:30 $10.20 → close $10.51 +39.68; AVGO×3 09:30 $363.68 → close $368.56 +14.64; CHPT×199 09:30 $9.91 → close $9.37 -107.46; CIEN×3 09:30 $327.42 → close $341.29 +41.61; CPB×61 09:30 $21.30 → close $21.76 +28.06; FIVE×5 09:30 $251.22 → close $254.07 +14.25; HPE×28 09:30 $52.29 → close $56.03 +104.72; MEI×91 09:30 $15.80 → close $14.06 -158.34; AMBA×1 09:30 $63.83 → close $63.48 -0.35; ASAN×11 09:30 $8.73 → close $8.79 +0.66; DOCU×1 09:30 $67.05 → close $65.08 -1.97; DOMO×27 09:30 $3.84 → close $3.83 -0.27; IOT×2 09:30 $39.56 → close $40.15 +1.18; LULU×1 09:30 $100.58 → close $103.19 +2.61; MAMA×6 09:30 $15.20 → close $15.50 +1.80 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `VOR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `BTBT` | cash | leftover split 1.32 < 1 share @ 1.50 |
| 2026-08-14 | `ARX` | cash | leftover split 1.32 < 1 share @ 19.57 |
| 2026-08-14 | `AIRO` | cash | leftover split 1.32 < 1 share @ 11.12 |
| 2026-08-14 | `MH` | cash | leftover split 1.32 < 1 share @ 13.55 |
| 2026-08-14 | `CLBT` | cash | leftover split 1.32 < 1 share @ 10.83 |
| 2026-08-14 | `LUNR` | cash | leftover split 1.32 < 1 share @ 19.17 |
| 2026-08-14 | `NMAX` | cash | leftover split 1.32 < 1 share @ 9.89 |
| 2026-08-14 | `AIRJ` | cash | leftover split 2.84 < 1 share @ 5.51 |
| 2026-08-14 | `AMAT` | cash | leftover split 2.84 < 1 share @ 499.40 |
| 2026-08-14 | `AMPG` | cash | leftover split 2.84 < 1 share @ 4.37 |
| 2026-08-14 | `BRUN` | cash | leftover split 2.84 < 1 share @ 26.25 |
| 2026-08-14 | `DGXX` | cash | leftover split 2.84 < 1 share @ 3.92 |
| 2026-08-17 | `INO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `VOR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `EU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `BZAI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `DEFT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `EU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `BZAI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `DEFT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h3 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h3 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h3 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h3 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h3 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h3 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h3 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h3 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new union_earn_react_h3 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new union_earn_react_h3 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new union_earn_react_h3 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new union_earn_react_h3 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new union_earn_react_h3 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new union_earn_react_h3 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new union_earn_react_h3 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h3 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h3 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h3 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h3 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h3 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h3 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h3 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new union_earn_react_h3 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new union_earn_react_h3 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new union_earn_react_h3 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new union_earn_react_h3 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new union_earn_react_h3 |
| 2026-08-19 | `LOW` | hard_red | hard-red S=-7.20 sit; no new union_earn_react_h3 |
| 2026-08-19 | `LZB` | hard_red | hard-red S=-7.20 sit; no new union_earn_react_h3 |
| 2026-08-19 | `MRCY` | hard_red | hard-red S=-7.20 sit; no new union_earn_react_h3 |
| 2026-08-21 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TOYO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `DVLT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AEG` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ALVO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ATAT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ATHM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `BABA` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `BILL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `BULL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `FUTU` | cash | leftover split 6.07 < 1 share @ 115.18 |
| 2026-08-21 | `DE` | cash | leftover split 6.07 < 1 share @ 623.26 |
| 2026-08-21 | `WMT` | cash | leftover split 6.07 < 1 share @ 103.69 |
| 2026-08-21 | `BEKE` | cash | leftover split 6.07 < 1 share @ 17.93 |
| 2026-08-21 | `BJ` | cash | leftover split 6.07 < 1 share @ 93.98 |
| 2026-08-21 | `BKE` | cash | leftover split 6.07 < 1 share @ 43.08 |
| 2026-08-21 | `ROST` | cash | leftover split 80.39 < 1 share @ 243.85 |
| 2026-08-24 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TOYO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `DVLT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AAP` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AEG` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ALVO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATAT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATHM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BABA` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BILL` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BULL` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `PSEC` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new union_e_fresh_h3 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new union_earn_react_h3 |
| 2026-08-25 | `PSEC` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-26 | `BMO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BNS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `EH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GFI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GRRR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `SHMD` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `ANF` | cash | leftover split 20.24 < 1 share @ 131.37 |
| 2026-08-26 | `BOX` | cash | leftover split 20.24 < 1 share @ 34.30 |
| 2026-08-26 | `DY` | cash | leftover split 20.24 < 1 share @ 326.91 |
| 2026-08-26 | `HEI` | cash | leftover split 62.83 < 1 share @ 370.00 |
| 2026-08-26 | `INTU` | cash | leftover split 62.83 < 1 share @ 323.47 |
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
| 2026-08-27 | `FSCO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `NVDA` | cash | leftover split 126.88 < 1 share @ 222.86 |
| 2026-08-28 | `SLQT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `TIGR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBWI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `FSCO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `ADSK` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `BBAR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `ESTC` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FINV` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FRO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `GAP` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `HAFN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `IREN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new union_e_fresh_h3 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new union_earn_react_h3 |
| 2026-09-01 | `ADSK` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `BBAR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `ESTC` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FINV` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FRO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `GAP` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `HAFN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `IREN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new union_e_fresh_h3 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new union_earn_react_h3 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h3 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h3 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h3 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h3 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h3 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h3 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new union_earn_react_h3 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new union_earn_react_h3 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new union_earn_react_h3 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new union_earn_react_h3 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new union_earn_react_h3 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new union_earn_react_h3 |
| 2026-09-04 | `AI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `AVGO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CHPT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CIEN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CPB` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `FIVE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `HPE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `MEI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `GWRE` | cash | leftover split 99.30 < 1 share @ 167.55 |
| 2026-09-08 | `AI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `AVGO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CHPT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CIEN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CPB` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `FIVE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `HPE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `MEI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `AMBA` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `ASAN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `DOCU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `DOMO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `IOT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `LULU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `MAMA` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new union_e_fresh_h3 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new union_e_fresh_h3 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new union_e_fresh_h3 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new union_earn_react_h3 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new union_earn_react_h3 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new union_earn_react_h3 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `AI` | 128 | 2026-09-03 @ $10.74 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $1378.94; owner union_e_fresh_h3 |
| `AVGO` | 3 | 2026-09-03 @ $351.74 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $1378.94; owner union_e_fresh_h3 |
| `CHPT` | 199 | 2026-09-03 @ $6.90 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $1378.94; owner union_e_fresh_h3 |
| `CIEN` | 3 | 2026-09-03 @ $354.49 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $1378.94; owner union_e_fresh_h3 |
| `CPB` | 61 | 2026-09-03 @ $22.32 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $1378.94; owner union_e_fresh_h3 |
| `FIVE` | 5 | 2026-09-03 @ $257.00 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $1378.94; owner union_e_fresh_h3 |
| `HPE` | 28 | 2026-09-03 @ $47.60 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $1378.94; owner union_e_fresh_h3 |
| `MEI` | 91 | 2026-09-03 @ $15.09 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $1378.94; owner union_e_fresh_h3 |
| `AMBA` | 1 | 2026-09-04 @ $63.18 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $99.30; owner union_e_fresh_h3 |
| `ASAN` | 11 | 2026-09-04 @ $8.74 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $99.30; owner union_e_fresh_h3 |
| `DOCU` | 1 | 2026-09-04 @ $68.52 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $99.30; owner union_e_fresh_h3 |
| `DOMO` | 27 | 2026-09-04 @ $3.62 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $99.30; owner union_e_fresh_h3 |
| `IOT` | 2 | 2026-09-04 @ $44.90 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $99.30; owner union_e_fresh_h3 |
| `LULU` | 1 | 2026-09-04 @ $98.15 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $99.30; owner union_e_fresh_h3 |
| `MAMA` | 6 | 2026-09-04 @ $15.70 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $99.30; owner union_e_fresh_h3 |
