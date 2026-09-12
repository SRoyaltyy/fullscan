# Factor mine action — `combo_e1er_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_e_fresh_h1/union_earn_react_h3 w=0.5,0.5 net=priority

Cash book **-3.12%** ($9,688) · signal-only (no cash/fees) was —. Starts YES **5/21**. Fills 163 · skips 113 · realized $-257.75.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_e_fresh_h1 50%, union_earn_react_h3 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_e_fresh_h1 50%, union_earn_react_h3 50%.
- Member: union_e_fresh_h1 (50% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $35.81.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `INO` | 6172 | — | $0.81 | +0.00 | $0.90 | +555.48 | +555.48 | +0.00 | +555.48 |
| 2026-08-13 | `VOR` | 223 | — | $22.01 | +0.00 | $23.29 | +285.44 | +285.44 | +0.00 | +285.44 |
| 2026-08-14 | `INO` | 6172 | $0.90 | $0.93 | +185.16 | — | +0.00 | +185.16 | +740.64 | — |
| 2026-08-14 | `VOR` | 223 | $23.29 | $23.33 | +8.92 | — | +0.00 | +8.92 | +294.36 | — |
| 2026-08-14 | `BTBT` | 453 | — | $1.50 | +0.00 | $1.57 | +31.71 | +31.71 | +0.00 | +31.71 |
| 2026-08-14 | `ARX` | 34 | — | $19.57 | +0.00 | $19.58 | +0.34 | +0.34 | +0.00 | +0.34 |
| 2026-08-14 | `AIRO` | 61 | — | $11.12 | +0.00 | $9.57 | -94.55 | -94.55 | +0.00 | -94.55 |
| 2026-08-14 | `MH` | 50 | — | $13.55 | +0.00 | $13.10 | -22.50 | -22.50 | +0.00 | -22.50 |
| 2026-08-14 | `CLBT` | 62 | — | $10.83 | +0.00 | $11.14 | +19.22 | +19.22 | +0.00 | +19.22 |
| 2026-08-14 | `EU` | 576 | — | $1.18 | +0.00 | $1.21 | +17.28 | +17.28 | +0.00 | +17.28 |
| 2026-08-14 | `LUNR` | 35 | — | $19.17 | +0.00 | $19.01 | -5.60 | -5.60 | +0.00 | -5.60 |
| 2026-08-14 | `NMAX` | 68 | — | $9.89 | +0.00 | $10.87 | +66.30 | +66.30 | +0.00 | +66.30 |
| 2026-08-14 | `AIRJ` | 141 | — | $5.51 | +0.00 | $6.04 | +74.73 | +74.73 | +0.00 | +74.73 |
| 2026-08-14 | `AMAT` | 1 | — | $499.40 | +0.00 | $507.18 | +7.78 | +7.78 | +0.00 | +7.78 |
| 2026-08-14 | `AMPG` | 178 | — | $4.37 | +0.00 | $4.00 | -66.39 | -66.39 | +0.00 | -66.39 |
| 2026-08-14 | `BRUN` | 29 | — | $26.25 | +0.00 | $22.93 | -96.14 | -96.14 | +0.00 | -96.14 |
| 2026-08-14 | `BZAI` | 1018 | — | $0.77 | +0.00 | $0.59 | -176.11 | -176.11 | +0.00 | -176.11 |
| 2026-08-14 | `DEFT` | 1660 | — | $0.47 | +0.00 | $0.49 | +31.54 | +31.54 | +0.00 | +31.54 |
| 2026-08-14 | `DGXX` | 199 | — | $3.92 | +0.00 | $3.97 | +9.95 | +9.95 | +0.00 | +9.95 |
| 2026-08-17 | `BTBT` | 453 | $1.57 | $1.52 | -22.65 | — | +0.00 | -22.65 | +9.06 | — |
| 2026-08-17 | `ARX` | 34 | $19.58 | $19.57 | -0.34 | — | +0.00 | -0.34 | +0.00 | — |
| 2026-08-17 | `AIRO` | 61 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -94.55 | — |
| 2026-08-17 | `MH` | 50 | $13.10 | $13.16 | +3.00 | — | +0.00 | +3.00 | -19.50 | — |
| 2026-08-17 | `CLBT` | 62 | $11.14 | $11.19 | +3.10 | — | +0.00 | +3.10 | +22.32 | — |
| 2026-08-17 | `EU` | 576 | $1.21 | $1.21 | +0.00 | — | +0.00 | +0.00 | +17.28 | — |
| 2026-08-17 | `LUNR` | 35 | $19.01 | $20.25 | +43.40 | — | +0.00 | +43.40 | +37.80 | — |
| 2026-08-17 | `NMAX` | 68 | $10.87 | $10.97 | +6.80 | — | +0.00 | +6.80 | +73.10 | — |
| 2026-08-17 | `AIRJ` | 141 | $6.04 | $6.22 | +25.38 | $5.81 | -57.81 | -32.43 | +100.11 | +42.30 |
| 2026-08-17 | `AMAT` | 1 | $507.18 | $517.45 | +10.27 | $534.74 | +17.29 | +27.56 | +18.05 | +35.34 |
| 2026-08-17 | `AMPG` | 178 | $4.00 | $4.09 | +16.91 | $3.70 | -69.42 | -52.51 | -49.48 | -118.90 |
| 2026-08-17 | `BRUN` | 29 | $22.93 | $23.00 | +2.03 | $22.63 | -10.73 | -8.70 | -94.11 | -104.84 |
| 2026-08-17 | `BZAI` | 1018 | $0.59 | $0.55 | -41.74 | $0.52 | -31.56 | -73.30 | -217.85 | -249.41 |
| 2026-08-17 | `DEFT` | 1660 | $0.49 | $0.47 | -23.24 | $0.47 | -16.60 | -39.84 | +8.30 | -8.30 |
| 2026-08-17 | `DGXX` | 199 | $3.97 | $3.96 | -1.99 | $3.88 | -15.92 | -17.91 | +7.96 | -7.96 |
| 2026-08-18 | `AIRJ` | 141 | $5.81 | $5.63 | -25.38 | $5.32 | -43.71 | -69.09 | +16.92 | -26.79 |
| 2026-08-18 | `AMAT` | 1 | $534.74 | $506.83 | -27.91 | $513.78 | +6.95 | -20.96 | +7.43 | +14.38 |
| 2026-08-18 | `AMPG` | 178 | $3.70 | $3.58 | -21.72 | $3.53 | -8.54 | -30.26 | -140.62 | -149.16 |
| 2026-08-18 | `BRUN` | 29 | $22.63 | $21.54 | -31.61 | $20.13 | -40.89 | -72.50 | -136.45 | -177.34 |
| 2026-08-18 | `BZAI` | 1018 | $0.52 | $0.49 | -30.54 | $0.56 | +68.21 | +37.67 | -279.95 | -211.74 |
| 2026-08-18 | `DEFT` | 1660 | $0.47 | $0.45 | -24.90 | $0.44 | -21.58 | -46.48 | -33.20 | -54.78 |
| 2026-08-18 | `DGXX` | 199 | $3.88 | $3.77 | -21.89 | $3.58 | -37.81 | -59.70 | -29.85 | -67.66 |
| 2026-08-19 | `AIRJ` | 141 | $5.32 | $5.33 | +1.41 | — | +0.00 | +1.41 | -25.38 | — |
| 2026-08-19 | `AMAT` | 1 | $513.78 | $507.87 | -5.91 | — | +0.00 | -5.91 | +8.47 | — |
| 2026-08-19 | `AMPG` | 178 | $3.53 | $3.56 | +5.34 | — | +0.00 | +5.34 | -143.82 | — |
| 2026-08-19 | `BRUN` | 29 | $20.13 | $20.38 | +7.11 | — | +0.00 | +7.11 | -170.23 | — |
| 2026-08-19 | `BZAI` | 1018 | $0.56 | $0.57 | +12.22 | — | +0.00 | +12.22 | -199.53 | — |
| 2026-08-19 | `DEFT` | 1660 | $0.44 | $0.43 | -3.32 | — | +0.00 | -3.32 | -58.10 | — |
| 2026-08-19 | `DGXX` | 199 | $3.58 | $3.66 | +15.92 | — | +0.00 | +15.92 | -51.74 | — |
| 2026-08-20 | `EL` | 6 | — | $97.43 | +0.00 | $96.15 | -7.68 | -7.68 | +0.00 | -7.68 |
| 2026-08-20 | `TOYO` | 143 | — | $4.43 | +0.00 | $4.51 | +12.15 | +12.15 | +0.00 | +12.15 |
| 2026-08-20 | `DVLT` | 2118 | — | $0.30 | +0.00 | $0.32 | +42.36 | +42.36 | +0.00 | +42.36 |
| 2026-08-20 | `AAP` | 13 | — | $46.85 | +0.00 | $42.39 | -57.98 | -57.98 | +0.00 | -57.98 |
| 2026-08-20 | `AEG` | 70 | — | $9.01 | +0.00 | $9.01 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `ALVO` | 163 | — | $3.89 | +0.00 | $4.27 | +61.94 | +61.94 | +0.00 | +61.94 |
| 2026-08-20 | `ATAT` | 18 | — | $34.05 | +0.00 | $34.25 | +3.60 | +3.60 | +0.00 | +3.60 |
| 2026-08-20 | `ATHM` | 28 | — | $22.44 | +0.00 | $22.12 | -8.96 | -8.96 | +0.00 | -8.96 |
| 2026-08-20 | `BABA` | 13 | — | $123.47 | +0.00 | $130.53 | +91.78 | +91.78 | +0.00 | +91.78 |
| 2026-08-20 | `BILL` | 35 | — | $49.00 | +0.00 | $47.40 | -56.00 | -56.00 | +0.00 | -56.00 |
| 2026-08-20 | `BULL` | 173 | — | $9.94 | +0.00 | $8.85 | -188.57 | -188.57 | +0.00 | -188.57 |
| 2026-08-21 | `EL` | 6 | $96.15 | $96.75 | +3.60 | — | +0.00 | +3.60 | -4.08 | — |
| 2026-08-21 | `TOYO` | 143 | $4.51 | $4.68 | +23.60 | — | +0.00 | +23.60 | +35.75 | — |
| 2026-08-21 | `DVLT` | 2118 | $0.32 | $0.31 | -21.18 | — | +0.00 | -21.18 | +21.18 | — |
| 2026-08-21 | `AAP` | 13 | $42.39 | $42.41 | +0.26 | $42.58 | +2.21 | +2.47 | -57.72 | -55.51 |
| 2026-08-21 | `AEG` | 70 | $9.01 | $9.04 | +2.10 | — | +0.00 | +2.10 | +2.10 | — |
| 2026-08-21 | `ALVO` | 163 | $4.27 | $4.32 | +8.15 | — | +0.00 | +8.15 | +70.09 | — |
| 2026-08-21 | `ATAT` | 18 | $34.25 | $34.31 | +1.08 | — | +0.00 | +1.08 | +4.68 | — |
| 2026-08-21 | `ATHM` | 28 | $22.12 | $22.20 | +2.24 | — | +0.00 | +2.24 | -6.72 | — |
| 2026-08-21 | `BABA` | 13 | $130.53 | $125.35 | -67.34 | $119.34 | -78.13 | -145.47 | +24.44 | -53.69 |
| 2026-08-21 | `BILL` | 35 | $47.40 | $47.50 | +3.50 | $47.82 | +11.20 | +14.70 | -52.50 | -41.30 |
| 2026-08-21 | `BULL` | 173 | $8.85 | $8.99 | +24.22 | $8.78 | -36.33 | -12.11 | -164.35 | -200.68 |
| 2026-08-21 | `FUTU` | 2 | — | $115.18 | +0.00 | $123.64 | +16.92 | +16.92 | +0.00 | +16.92 |
| 2026-08-21 | `WMT` | 3 | — | $103.69 | +0.00 | $103.70 | +0.03 | +0.03 | +0.00 | +0.03 |
| 2026-08-21 | `BEKE` | 18 | — | $17.93 | +0.00 | $17.75 | -3.33 | -3.33 | +0.00 | -3.33 |
| 2026-08-21 | `BJ` | 3 | — | $93.98 | +0.00 | $96.42 | +7.32 | +7.32 | +0.00 | +7.32 |
| 2026-08-21 | `BKE` | 7 | — | $43.08 | +0.00 | $43.81 | +5.11 | +5.11 | +0.00 | +5.11 |
| 2026-08-21 | `PSEC` | 142 | — | $2.30 | +0.00 | $2.33 | +4.26 | +4.26 | +0.00 | +4.26 |
| 2026-08-21 | `ROST` | 11 | — | $243.85 | +0.00 | $239.04 | -52.91 | -52.91 | +0.00 | -52.91 |
| 2026-08-24 | `AAP` | 13 | $42.58 | $43.05 | +6.11 | — | +0.00 | +6.11 | -49.40 | — |
| 2026-08-24 | `BABA` | 13 | $119.34 | $116.90 | -31.72 | $118.47 | +20.41 | -11.31 | -85.41 | -65.00 |
| 2026-08-24 | `BILL` | 35 | $47.82 | $47.68 | -4.90 | $48.16 | +16.80 | +11.90 | -46.20 | -29.40 |
| 2026-08-24 | `BULL` | 173 | $8.78 | $8.58 | -34.60 | $8.53 | -8.65 | -43.25 | -235.28 | -243.93 |
| 2026-08-24 | `FUTU` | 2 | $123.64 | $121.00 | -5.28 | — | +0.00 | -5.28 | +11.64 | — |
| 2026-08-24 | `WMT` | 3 | $103.70 | $104.14 | +1.32 | — | +0.00 | +1.32 | +1.35 | — |
| 2026-08-24 | `BEKE` | 18 | $17.75 | $18.05 | +5.49 | — | +0.00 | +5.49 | +2.16 | — |
| 2026-08-24 | `BJ` | 3 | $96.42 | $97.02 | +1.80 | — | +0.00 | +1.80 | +9.12 | — |
| 2026-08-24 | `BKE` | 7 | $43.81 | $44.22 | +2.87 | — | +0.00 | +2.87 | +7.98 | — |
| 2026-08-24 | `PSEC` | 142 | $2.33 | $2.34 | +1.42 | — | +0.00 | +1.42 | +5.68 | — |
| 2026-08-24 | `ROST` | 11 | $239.04 | $238.08 | -10.56 | $241.52 | +37.84 | +27.28 | -63.47 | -25.63 |
| 2026-08-25 | `BABA` | 13 | $118.47 | $117.94 | -6.89 | — | +0.00 | -6.89 | -71.89 | — |
| 2026-08-25 | `BILL` | 35 | $48.16 | $47.98 | -6.12 | — | +0.00 | -6.12 | -35.53 | — |
| 2026-08-25 | `BULL` | 173 | $8.53 | $8.46 | -12.11 | — | +0.00 | -12.11 | -256.04 | — |
| 2026-08-25 | `ROST` | 11 | $241.52 | $241.50 | -0.22 | $241.19 | -3.41 | -3.63 | -25.85 | -29.26 |
| 2026-08-25 | `BMO` | 5 | — | $175.01 | +0.00 | $173.46 | -7.75 | -7.75 | +0.00 | -7.75 |
| 2026-08-25 | `BNS` | 10 | — | $88.94 | +0.00 | $93.10 | +41.60 | +41.60 | +0.00 | +41.60 |
| 2026-08-25 | `BZ` | 58 | — | $15.28 | +0.00 | $16.29 | +58.58 | +58.58 | +0.00 | +58.58 |
| 2026-08-25 | `DKS` | 6 | — | $142.36 | +0.00 | $124.31 | -108.30 | -108.30 | +0.00 | -108.30 |
| 2026-08-25 | `EH` | 174 | — | $5.10 | +0.00 | $4.83 | -46.98 | -46.98 | +0.00 | -46.98 |
| 2026-08-25 | `GFI` | 18 | — | $47.89 | +0.00 | $48.87 | +17.64 | +17.64 | +0.00 | +17.64 |
| 2026-08-25 | `GRRR` | 64 | — | $13.92 | +0.00 | $14.04 | +7.68 | +7.68 | +0.00 | +7.68 |
| 2026-08-25 | `SHMD` | 196 | — | $4.54 | +0.00 | $3.42 | -220.50 | -220.50 | +0.00 | -220.50 |
| 2026-08-26 | `ROST` | 11 | $241.19 | $242.50 | +14.41 | — | +0.00 | +14.41 | -14.85 | — |
| 2026-08-26 | `BMO` | 5 | $173.46 | $173.22 | -1.20 | — | +0.00 | -1.20 | -8.95 | — |
| 2026-08-26 | `BNS` | 10 | $93.10 | $92.65 | -4.50 | — | +0.00 | -4.50 | +37.10 | — |
| 2026-08-26 | `BZ` | 58 | $16.29 | $16.77 | +27.84 | $18.84 | +120.06 | +147.90 | +86.42 | +206.48 |
| 2026-08-26 | `DKS` | 6 | $124.31 | $121.87 | -14.64 | $129.66 | +46.74 | +32.10 | -122.94 | -76.20 |
| 2026-08-26 | `EH` | 174 | $4.83 | $4.77 | -10.44 | — | +0.00 | -10.44 | -57.42 | — |
| 2026-08-26 | `GFI` | 18 | $48.87 | $48.24 | -11.34 | — | +0.00 | -11.34 | +6.30 | — |
| 2026-08-26 | `GRRR` | 64 | $14.04 | $14.03 | -0.64 | — | +0.00 | -0.64 | +7.04 | — |
| 2026-08-26 | `SHMD` | 196 | $3.42 | $3.38 | -7.84 | — | +0.00 | -7.84 | -228.34 | — |
| 2026-08-26 | `SLQT` | 1112 | — | $0.58 | +0.00 | $0.55 | -36.70 | -36.70 | +0.00 | -36.70 |
| 2026-08-26 | `TIGR` | 124 | — | $5.21 | +0.00 | $5.46 | +31.00 | +31.00 | +0.00 | +31.00 |
| 2026-08-26 | `ANF` | 4 | — | $131.37 | +0.00 | $147.75 | +65.52 | +65.52 | +0.00 | +65.52 |
| 2026-08-26 | `BBWI` | 35 | — | $18.26 | +0.00 | $18.90 | +22.40 | +22.40 | +0.00 | +22.40 |
| 2026-08-26 | `BOX` | 18 | — | $34.30 | +0.00 | $33.39 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-08-26 | `DY` | 1 | — | $326.91 | +0.00 | $310.91 | -16.00 | -16.00 | +0.00 | -16.00 |
| 2026-08-26 | `FSCO` | 286 | — | $5.08 | +0.00 | $5.12 | +11.44 | +11.44 | +0.00 | +11.44 |
| 2026-08-26 | `HEI` | 3 | — | $370.00 | +0.00 | $346.15 | -71.55 | -71.55 | +0.00 | -71.55 |
| 2026-08-26 | `INTU` | 4 | — | $323.47 | +0.00 | $345.88 | +89.64 | +89.64 | +0.00 | +89.64 |
| 2026-08-27 | `BZ` | 58 | $18.84 | $18.50 | -19.72 | — | +0.00 | -19.72 | +186.76 | — |
| 2026-08-27 | `DKS` | 6 | $129.66 | $128.73 | -5.58 | — | +0.00 | -5.58 | -81.78 | — |
| 2026-08-27 | `SLQT` | 1112 | $0.55 | $0.53 | -22.24 | — | +0.00 | -22.24 | -58.94 | — |
| 2026-08-27 | `TIGR` | 124 | $5.46 | $5.49 | +3.72 | — | +0.00 | +3.72 | +34.72 | — |
| 2026-08-27 | `ANF` | 4 | $147.75 | $144.70 | -12.20 | — | +0.00 | -12.20 | +53.32 | — |
| 2026-08-27 | `BBWI` | 35 | $18.90 | $18.69 | -7.35 | — | +0.00 | -7.35 | +15.05 | — |
| 2026-08-27 | `BOX` | 18 | $33.39 | $33.79 | +7.20 | — | +0.00 | +7.20 | -9.18 | — |
| 2026-08-27 | `DY` | 1 | $310.91 | $314.90 | +3.99 | — | +0.00 | +3.99 | -12.01 | — |
| 2026-08-27 | `FSCO` | 286 | $5.12 | $5.10 | -5.72 | $5.12 | +5.72 | +0.00 | +5.72 | +11.44 |
| 2026-08-27 | `HEI` | 3 | $346.15 | $346.19 | +0.12 | $337.01 | -27.54 | -27.42 | -71.43 | -98.97 |
| 2026-08-27 | `INTU` | 4 | $345.88 | $353.54 | +30.64 | $348.00 | -22.16 | +8.48 | +120.28 | +98.12 |
| 2026-08-27 | `NVDA` | 25 | — | $222.86 | +0.00 | $227.98 | +128.00 | +128.00 | +0.00 | +128.00 |
| 2026-08-28 | `FSCO` | 286 | $5.12 | $5.12 | +0.00 | $5.18 | +17.16 | +17.16 | +11.44 | +28.60 |
| 2026-08-28 | `HEI` | 3 | $337.01 | $339.95 | +8.82 | $336.53 | -10.26 | -1.44 | -90.15 | -100.41 |
| 2026-08-28 | `INTU` | 4 | $348.00 | $347.82 | -0.72 | $358.06 | +40.96 | +40.24 | +97.40 | +138.36 |
| 2026-08-28 | `NVDA` | 25 | $227.98 | $227.36 | -15.50 | — | +0.00 | -15.50 | +112.50 | — |
| 2026-08-28 | `ADSK` | 2 | — | $261.16 | +0.00 | $260.66 | -1.00 | -1.00 | +0.00 | -1.00 |
| 2026-08-28 | `BBAR` | 48 | — | $15.01 | +0.00 | $14.47 | -25.92 | -25.92 | +0.00 | -25.92 |
| 2026-08-28 | `ESTC` | 7 | — | $103.89 | +0.00 | $99.91 | -27.86 | -27.86 | +0.00 | -27.86 |
| 2026-08-28 | `FINV` | 188 | — | $3.88 | +0.00 | $3.40 | -90.24 | -90.24 | +0.00 | -90.24 |
| 2026-08-28 | `FRO` | 16 | — | $44.40 | +0.00 | $44.19 | -3.36 | -3.36 | +0.00 | -3.36 |
| 2026-08-28 | `GAP` | 29 | — | $24.69 | +0.00 | $23.48 | -35.09 | -35.09 | +0.00 | -35.09 |
| 2026-08-28 | `HAFN` | 87 | — | $8.35 | +0.00 | $8.47 | +10.44 | +10.44 | +0.00 | +10.44 |
| 2026-08-28 | `IREN` | 19 | — | $37.65 | +0.00 | $35.45 | -41.71 | -41.71 | +0.00 | -41.71 |
| 2026-08-31 | `FSCO` | 286 | $5.18 | $5.20 | +5.72 | — | +0.00 | +5.72 | +34.32 | — |
| 2026-08-31 | `HEI` | 3 | $336.53 | $334.88 | -4.95 | — | +0.00 | -4.95 | -105.36 | — |
| 2026-08-31 | `INTU` | 4 | $358.06 | $356.05 | -8.04 | — | +0.00 | -8.04 | +130.32 | — |
| 2026-08-31 | `ADSK` | 2 | $260.66 | $257.71 | -5.90 | — | +0.00 | -5.90 | -6.90 | — |
| 2026-08-31 | `BBAR` | 48 | $14.47 | $14.88 | +19.68 | — | +0.00 | +19.68 | -6.24 | — |
| 2026-08-31 | `ESTC` | 7 | $99.91 | $98.00 | -13.37 | — | +0.00 | -13.37 | -41.23 | — |
| 2026-08-31 | `FINV` | 188 | $3.40 | $3.39 | -1.88 | — | +0.00 | -1.88 | -92.12 | — |
| 2026-08-31 | `FRO` | 16 | $44.19 | $44.85 | +10.56 | — | +0.00 | +10.56 | +7.20 | — |
| 2026-08-31 | `GAP` | 29 | $23.48 | $22.98 | -14.50 | — | +0.00 | -14.50 | -49.59 | — |
| 2026-08-31 | `HAFN` | 87 | $8.47 | $8.53 | +5.22 | — | +0.00 | +5.22 | +15.66 | — |
| 2026-08-31 | `IREN` | 19 | $35.45 | $35.81 | +6.84 | — | +0.00 | +6.84 | -34.87 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AI` | 110 | — | $10.74 | +0.00 | $10.90 | +17.05 | +17.05 | +0.00 | +17.05 |
| 2026-09-03 | `AVGO` | 3 | — | $351.74 | +0.00 | $357.16 | +16.26 | +16.26 | +0.00 | +16.26 |
| 2026-09-03 | `CHPT` | 172 | — | $6.90 | +0.00 | $9.08 | +374.96 | +374.96 | +0.00 | +374.96 |
| 2026-09-03 | `CIEN` | 3 | — | $354.49 | +0.00 | $317.46 | -111.09 | -111.09 | +0.00 | -111.09 |
| 2026-09-03 | `CPB` | 53 | — | $22.32 | +0.00 | $22.13 | -10.07 | -10.07 | +0.00 | -10.07 |
| 2026-09-03 | `FIVE` | 4 | — | $257.00 | +0.00 | $239.96 | -68.16 | -68.16 | +0.00 | -68.16 |
| 2026-09-03 | `HPE` | 24 | — | $47.60 | +0.00 | $54.44 | +164.16 | +164.16 | +0.00 | +164.16 |
| 2026-09-03 | `MEI` | 78 | — | $15.09 | +0.00 | $15.32 | +17.94 | +17.94 | +0.00 | +17.94 |
| 2026-09-04 | `AI` | 110 | $10.90 | $10.91 | +1.10 | — | +0.00 | +1.10 | +18.15 | — |
| 2026-09-04 | `AVGO` | 3 | $357.16 | $359.70 | +7.62 | — | +0.00 | +7.62 | +23.88 | — |
| 2026-09-04 | `CHPT` | 172 | $9.08 | $9.28 | +34.40 | — | +0.00 | +34.40 | +409.36 | — |
| 2026-09-04 | `CIEN` | 3 | $317.46 | $321.67 | +12.63 | — | +0.00 | +12.63 | -98.46 | — |
| 2026-09-04 | `CPB` | 53 | $22.13 | $22.10 | -1.59 | — | +0.00 | -1.59 | -11.66 | — |
| 2026-09-04 | `FIVE` | 4 | $239.96 | $238.88 | -4.32 | — | +0.00 | -4.32 | -72.48 | — |
| 2026-09-04 | `HPE` | 24 | $54.44 | $53.85 | -14.16 | — | +0.00 | -14.16 | +150.00 | — |
| 2026-09-04 | `MEI` | 78 | $15.32 | $15.34 | +1.56 | — | +0.00 | +1.56 | +19.50 | — |
| 2026-09-04 | `AMBA` | 19 | — | $63.18 | +0.00 | $62.89 | -5.51 | -5.51 | +0.00 | -5.51 |
| 2026-09-04 | `ASAN` | 141 | — | $8.74 | +0.00 | $8.81 | +9.87 | +9.87 | +0.00 | +9.87 |
| 2026-09-04 | `DOCU` | 18 | — | $68.52 | +0.00 | $68.41 | -1.98 | -1.98 | +0.00 | -1.98 |
| 2026-09-04 | `DOMO` | 343 | — | $3.62 | +0.00 | $3.88 | +90.89 | +90.89 | +0.00 | +90.89 |
| 2026-09-04 | `GWRE` | 7 | — | $167.55 | +0.00 | $162.42 | -35.91 | -35.91 | +0.00 | -35.91 |
| 2026-09-04 | `IOT` | 27 | — | $44.90 | +0.00 | $40.20 | -126.90 | -126.90 | +0.00 | -126.90 |
| 2026-09-04 | `LULU` | 12 | — | $98.15 | +0.00 | $100.61 | +29.52 | +29.52 | +0.00 | +29.52 |
| 2026-09-04 | `MAMA` | 79 | — | $15.70 | +0.00 | $15.16 | -42.66 | -42.66 | +0.00 | -42.66 |
| 2026-09-08 | `AMBA` | 19 | $62.89 | $63.83 | +17.86 | — | +0.00 | +17.86 | +12.35 | — |
| 2026-09-08 | `ASAN` | 141 | $8.81 | $8.73 | -11.28 | — | +0.00 | -11.28 | -1.41 | — |
| 2026-09-08 | `DOCU` | 18 | $68.41 | $67.05 | -24.48 | — | +0.00 | -24.48 | -26.46 | — |
| 2026-09-08 | `DOMO` | 343 | $3.88 | $3.84 | -13.72 | — | +0.00 | -13.72 | +77.17 | — |
| 2026-09-08 | `GWRE` | 7 | $162.42 | $160.52 | -13.30 | — | +0.00 | -13.30 | -49.21 | — |
| 2026-09-08 | `IOT` | 27 | $40.20 | $39.56 | -17.28 | — | +0.00 | -17.28 | -144.18 | — |
| 2026-09-08 | `LULU` | 12 | $100.61 | $100.58 | -0.36 | — | +0.00 | -0.36 | +29.16 | — |
| 2026-09-08 | `MAMA` | 79 | $15.16 | $15.20 | +3.16 | — | +0.00 | +3.16 | -39.50 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 3 | — | $164.43 | +0.00 | $150.28 | -42.45 | -42.45 | +0.00 | -42.45 |
| 2026-09-11 | `DBI` | 103 | — | $5.91 | +0.00 | $5.88 | -3.09 | -3.09 | +0.00 | -3.09 |
| 2026-09-11 | `ADBE` | 2 | — | $242.17 | +0.00 | $252.23 | +20.12 | +20.12 | +0.00 | +20.12 |
| 2026-09-11 | `CPRT` | 19 | — | $32.01 | +0.00 | $29.95 | -39.14 | -39.14 | +0.00 | -39.14 |
| 2026-09-11 | `DSGX` | 8 | — | $71.71 | +0.00 | $76.04 | +34.64 | +34.64 | +0.00 | +34.64 |
| 2026-09-11 | `KR` | 10 | — | $56.02 | +0.00 | $58.49 | +24.70 | +24.70 | +0.00 | +24.70 |
| 2026-09-11 | `LPTH` | 64 | — | $9.37 | +0.00 | $9.20 | -10.88 | -10.88 | +0.00 | -10.88 |
| 2026-09-11 | `REF` | 46 | — | $13.10 | +0.00 | $14.03 | +42.78 | +42.78 | +0.00 | +42.78 |
| 2026-09-11 | `RH` | 38 | — | $135.71 | +0.00 | $134.07 | -62.32 | -62.32 | +0.00 | -62.32 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +840.92 | INO, VOR | — | $21.06 | $10,769.53 | INO×6172, VOR×223 |
| 2026-08-14 | +5.50 | $21.06 | INO×6172, VOR×223 | $10,963.61 | +194.08 | -202.44 | BTBT, ARX, AIRO, MH, CLBT, EU, LUNR, NMAX, AIRJ, AMAT, AMPG, BRUN, BZAI, DEFT, DGXX | INO, VOR | $271.66 | $10,619.86 | BTBT×453, ARX×34, AIRO×61, MH×50, CLBT×62, EU×576, LUNR×35, NMAX×68, AIRJ×141, AMAT×1, AMPG×178, BRUN×29, BZAI×1018, DEFT×1660, DGXX×199 |
| 2026-08-17 | +2.25 | $271.66 | BTBT×453, ARX×34, AIRO×61, MH×50, CLBT×62, EU×576, LUNR×35, NMAX×68, AIRJ×141, AMAT×1, AMPG×178, BRUN×29, BZAI×1018, DEFT×1660, DGXX×199 | $10,640.79 | +20.93 | -184.75 | — | BTBT, ARX, AIRO, MH, CLBT, EU, LUNR, NMAX | $5,686.37 | $10,429.58 | AIRJ×141, AMAT×1, AMPG×178, BRUN×29, BZAI×1018, DEFT×1660, DGXX×199 |
| 2026-08-18 | -6.20 | $5,686.37 | AIRJ×141, AMAT×1, AMPG×178, BRUN×29, BZAI×1018, DEFT×1660, DGXX×199 | $10,245.64 | -183.94 | -77.37 | — | — | $5,686.37 | $10,168.26 | AIRJ×141, AMAT×1, AMPG×178, BRUN×29, BZAI×1018, DEFT×1660, DGXX×199 |
| 2026-08-19 | -7.20 | $5,686.37 | AIRJ×141, AMAT×1, AMPG×178, BRUN×29, BZAI×1018, DEFT×1660, DGXX×199 | $10,201.02 | +32.76 | +0.00 | — | AIRJ, AMAT, AMPG, BRUN, BZAI, DEFT, DGXX | $10,167.75 | $10,167.75 | — |
| 2026-08-20 | +1.12 | $10,167.75 | — | $10,167.75 | -0.00 | -107.36 | EL, TOYO, DVLT, AAP, AEG, ALVO, ATAT, ATHM, BABA, BILL, BULL | — | $124.91 | $10,025.80 | EL×6, TOYO×143, DVLT×2118, AAP×13, AEG×70, ALVO×163, ATAT×18, ATHM×28, BABA×13, BILL×35, BULL×173 |
| 2026-08-21 | +3.25 | $124.91 | EL×6, TOYO×143, DVLT×2118, AAP×13, AEG×70, ALVO×163, ATAT×18, ATHM×28, BABA×13, BILL×35, BULL×173 | $10,006.02 | -19.78 | -123.65 | FUTU, WMT, BEKE, BJ, BKE, PSEC, ROST | EL, TOYO, DVLT, AEG, ALVO, ATAT, ATHM | $109.52 | $9,841.23 | AAP×13, BABA×13, BILL×35, BULL×173, FUTU×2, WMT×3, BEKE×18, BJ×3, BKE×7, PSEC×142, ROST×11 |
| 2026-08-24 | -5.17 | $109.52 | AAP×13, BABA×13, BILL×35, BULL×173, FUTU×2, WMT×3, BEKE×18, BJ×3, BKE×7, PSEC×142, ROST×11 | $9,773.18 | -68.05 | +66.40 | — | AAP, FUTU, WMT, BEKE, BJ, BKE, PSEC | $2,466.81 | $9,824.93 | BABA×13, BILL×35, BULL×173, ROST×11 |
| 2026-08-25 | +1.80 | $2,466.81 | BABA×13, BILL×35, BULL×173, ROST×11 | $9,799.58 | -25.35 | -261.44 | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | BABA, BILL, BULL | $82.88 | $9,513.91 | ROST×11, BMO×5, BNS×10, BZ×58, DKS×6, EH×174, GFI×18, GRRR×64, SHMD×196 |
| 2026-08-26 | +2.02 | $82.88 | ROST×11, BMO×5, BNS×10, BZ×58, DKS×6, EH×174, GFI×18, GRRR×64, SHMD×196 | $9,505.56 | -8.35 | +246.17 | SLQT, TIGR, ANF, BBWI, BOX, DY, FSCO, HEI, INTU | ROST, BMO, BNS, EH, GFI, GRRR, SHMD | $498.13 | $9,708.17 | BZ×58, DKS×6, SLQT×1112, TIGR×124, ANF×4, BBWI×35, BOX×18, DY×1, FSCO×286, HEI×3, INTU×4 |
| 2026-08-27 | — | $498.13 | BZ×58, DKS×6, SLQT×1112, TIGR×124, ANF×4, BBWI×35, BOX×18, DY×1, FSCO×286, HEI×3, INTU×4 | $9,681.03 | -27.14 | +84.02 | NVDA | BZ, DKS, SLQT, TIGR, ANF, BBWI, BOX, DY | $171.90 | $9,738.75 | FSCO×286, HEI×3, INTU×4, NVDA×25 |
| 2026-08-28 | +0.75 | $171.90 | FSCO×286, HEI×3, INTU×4, NVDA×25 | $9,731.35 | -7.40 | -166.88 | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | NVDA | $269.08 | $9,545.24 | FSCO×286, HEI×3, INTU×4, ADSK×2, BBAR×48, ESTC×7, FINV×188, FRO×16, GAP×29, HAFN×87, IREN×19 |
| 2026-08-31 | -5.85 | $269.08 | FSCO×286, HEI×3, INTU×4, ADSK×2, BBAR×48, ESTC×7, FINV×188, FRO×16, GAP×29, HAFN×87, IREN×19 | $9,544.62 | -0.62 | +0.00 | — | FSCO, HEI, INTU, ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | $9,519.54 | $9,519.54 | — |
| 2026-09-01 | -6.30 | $9,519.54 | — | $9,519.54 | -0.00 | +0.00 | — | — | $9,519.54 | $9,519.54 | — |
| 2026-09-02 | -3.83 | $9,519.54 | — | $9,519.54 | -0.00 | +0.00 | — | — | $9,519.54 | $9,519.54 | — |
| 2026-09-03 | -0.90 | $9,519.54 | — | $9,519.54 | -0.00 | +401.05 | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | — | $484.46 | $9,903.33 | AI×110, AVGO×3, CHPT×172, CIEN×3, CPB×53, FIVE×4, HPE×24, MEI×78 |
| 2026-09-04 | +2.25 | $484.46 | AI×110, AVGO×3, CHPT×172, CIEN×3, CPB×53, FIVE×4, HPE×24, MEI×78 | $9,940.57 | +37.24 | -82.68 | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | $194.53 | $9,821.17 | AMBA×19, ASAN×141, DOCU×18, DOMO×343, GWRE×7, IOT×27, LULU×12, MAMA×79 |
| 2026-09-08 | -11.47 | $194.53 | AMBA×19, ASAN×141, DOCU×18, DOMO×343, GWRE×7, IOT×27, LULU×12, MAMA×79 | $9,761.77 | -59.40 | +0.00 | — | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | $9,742.29 | $9,742.29 | — |
| 2026-09-09 | -13.95 | $9,742.29 | — | $9,742.29 | -0.00 | +0.00 | — | — | $9,742.29 | $9,742.29 | — |
| 2026-09-10 | -13.28 | $9,742.29 | — | $9,742.29 | -0.00 | +0.00 | — | — | $9,742.29 | $9,742.29 | — |
| 2026-09-11 | +0.50 | $9,742.29 | — | $9,742.29 | -0.00 | -35.64 | ORCL, DBI, ADBE, CPRT, DSGX, KR, LPTH, REF, RH | — | $35.81 | $9,687.86 | ORCL×3, DBI×103, ADBE×2, CPRT×19, DSGX×8, KR×10, LPTH×64, REF×46, RH×38 |

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
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 34 | $19.57 | $2.09 | — | $9,530.86 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $680.23; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 61 | $11.12 | $2.17 | — | $8,850.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $680.23; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 50 | $13.55 | $2.14 | — | $8,170.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $680.23; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 62 | $10.83 | $2.18 | — | $7,497.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-30.1; combo leftover $680.23; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 576 | $1.18 | $7.43 | — | $6,809.98 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $680.23; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 35 | $19.17 | $2.10 | — | $6,136.93 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $680.23; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🔴 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 68 | $9.89 | $2.19 | — | $5,461.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; combo leftover $680.23; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRJ` | 141 | $5.51 | $2.41 | — | $4,682.56 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+13.1; combo leftover $780.27; owner union_earn_react_h3 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AMAT` | 1 | $499.40 | $1.99 | — | $4,181.16 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+1.3; combo leftover $780.27; owner union_earn_react_h3 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AMPG` | 178 | $4.37 | $2.52 | — | $3,401.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+10.3; combo leftover $780.27; owner union_earn_react_h3 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 29 | $26.25 | $2.08 | — | $2,637.95 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+31.2; combo leftover $780.27; owner union_earn_react_h3 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1018 | $0.77 | $10.85 | — | $1,847.31 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+20.4; combo leftover $780.27; owner union_earn_react_h3 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DEFT` | 1660 | $0.47 | $12.78 | — | $1,054.33 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+11.1; combo leftover $780.27; owner union_earn_react_h3 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DGXX` | 199 | $3.92 | $2.59 | — | $271.66 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+10.1; combo leftover $780.27; owner union_earn_react_h3 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $271.66 | ▼ close $10,619.86 vs 09:30 $10,963.61 (session -202.44) | 16:00 close · cash $271.66 · equity $10,619.86 vs 09:30 $10,963.61 (-343.75; session marks -202.44) · 15 name(s) marked open→close (per-name table). BTBT×453 09:30 $1.50 → close $1.57 +31.71; ARX×34 09:30 $19.57 → close $19.58 +0.34; AIRO×61 09:30 $11.12 → close $9.57 -94.55; MH×50 09:30 $13.55 → close $13.10 -22.50; CLBT×62 09:30 $10.83 → close $11.14 +19.22; EU×576 09:30 $1.18 → close $1.21 +17.28; LUNR×35 09:30 $19.17 → close $19.01 -5.60; NMAX×68 09:30 $9.89 → close $10.87 +66.30; AIRJ×141 09:30 $5.51 → close $6.04 +74.73; AMAT×1 09:30 $499.40 → close $507.18 +7.78; AMPG×178 09:30 $4.37 → close $4.00 -66.39; BRUN×29 09:30 $26.25 → close $22.93 -96.14; BZAI×1018 09:30 $0.77 → close $0.59 -176.11; DEFT×1660 09:30 $0.47 → close $0.49 +31.54; DGXX×199 09:30 $3.92 → close $3.97 +9.95 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $271.66 | ▲ 09:30 equity $10,640.79 vs yday $10,619.86 (+20.93) | 09:30 open · cash $271.66 (unchanged overnight, no fees) · equity $10,640.79 vs prior close $10,619.86 (+20.93) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 453 | $1.52 | $5.93 | $-2.71 | $954.29 | ▼ -2.71 after sell → book $10,634.86; vs 09:30 mark -5.93 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 34 | $19.57 | $2.11 | $-4.20 | $1,617.56 | ▼ -4.20 after sell → book $10,632.75; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 61 | $9.57 | $2.19 | $-98.92 | $2,199.14 | ▼ -98.92 after sell → book $10,630.55; vs 09:30 mark -2.20 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 50 | $13.16 | $2.16 | $-23.80 | $2,854.98 | ▼ -23.80 after sell → book $10,628.39; vs 09:30 mark -2.16 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 62 | $11.19 | $2.20 | $+17.95 | $3,546.56 | ▲ +17.95 after sell → book $10,626.20; vs 09:30 mark -2.19 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `EU` | 576 | $1.21 | $7.54 | $+2.31 | $4,235.99 | ▲ +2.31 after sell → book $10,618.66; vs 09:30 mark -7.54 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 35 | $20.25 | $2.12 | $+33.59 | $4,942.62 | ▲ +33.59 after sell → book $10,616.55; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 68 | $10.97 | $2.22 | $+68.69 | $5,686.37 | ▲ +68.69 after sell → book $10,614.33; vs 09:30 mark -2.22 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,686.37 | ▼ close $10,429.58 vs 09:30 $10,640.79 (session -184.75) | 16:00 close · cash $5,686.37 · equity $10,429.58 vs 09:30 $10,640.79 (-211.21; session marks -184.75) · 7 name(s) marked open→close (per-name table). AIRJ×141 09:30 $6.22 → close $5.81 -57.81; AMAT×1 09:30 $517.45 → close $534.74 +17.29; AMPG×178 09:30 $4.09 → close $3.70 -69.42; BRUN×29 09:30 $23.00 → close $22.63 -10.73; BZAI×1018 09:30 $0.55 → close $0.52 -31.56; DEFT×1660 09:30 $0.47 → close $0.47 -16.60; DGXX×199 09:30 $3.96 → close $3.88 -15.92 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,686.37 | ▼ 09:30 equity $10,245.64 vs yday $10,429.58 (-183.94) | 09:30 open · cash $5,686.37 (unchanged overnight, no fees) · equity $10,245.64 vs prior close $10,429.58 (-183.94) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,686.37 | ▼ close $10,168.26 vs 09:30 $10,245.64 (session -77.37) | 16:00 close · cash $5,686.37 · equity $10,168.26 vs 09:30 $10,245.64 (-77.38; session marks -77.37) · 7 name(s) marked open→close (per-name table). AIRJ×141 09:30 $5.63 → close $5.32 -43.71; AMAT×1 09:30 $506.83 → close $513.78 +6.95; AMPG×178 09:30 $3.58 → close $3.53 -8.54; BRUN×29 09:30 $21.54 → close $20.13 -40.89; BZAI×1018 09:30 $0.49 → close $0.56 +68.21; DEFT×1660 09:30 $0.45 → close $0.44 -21.58; DGXX×199 09:30 $3.77 → close $3.58 -37.81 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,686.37 | ▲ 09:30 equity $10,201.02 vs yday $10,168.26 (+32.76) | 09:30 open · cash $5,686.37 (unchanged overnight, no fees) · equity $10,201.02 vs prior close $10,168.26 (+32.76) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRJ` | 141 | $5.33 | $2.45 | $-30.24 | $6,435.45 | ▼ -30.24 after sell → book $10,198.57; vs 09:30 mark -2.45 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AMAT` | 1 | $507.87 | $2.01 | $+4.46 | $6,941.30 | ▲ +4.46 after sell → book $10,196.56; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AMPG` | 178 | $3.56 | $2.56 | $-148.91 | $7,572.42 | ▼ -148.91 after sell → book $10,194.00; vs 09:30 mark -2.56 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BRUN` | 29 | $20.38 | $2.10 | $-174.40 | $8,161.20 | ▼ -174.40 after sell → book $10,191.90; vs 09:30 mark -2.10 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BZAI` | 1018 | $0.57 | $9.04 | $-219.42 | $8,732.42 | ▼ -219.42 after sell → book $10,182.86; vs 09:30 mark -9.04 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DEFT` | 1660 | $0.43 | $12.49 | $-83.37 | $9,442.04 | ▼ -83.37 after sell → book $10,170.38; vs 09:30 mark -12.48 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DGXX` | 199 | $3.66 | $2.63 | $-56.96 | $10,167.75 | ▼ -56.96 after sell → book $10,167.75; vs 09:30 mark -2.63 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,167.75 | ▲ close $10,167.75 vs 09:30 $10,201.02 (session +0.00) | 16:00 close · cash $10,167.75 · no lots left · equity $10,167.75. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,167.75 | ▲ 09:30 equity $10,167.75 vs yday $10,167.75 (-0.00) | 09:30 open · cash $10,167.75 (unchanged overnight, no fees) · equity $10,167.75 vs prior close $10,167.75 (-0.00) | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 6 | $97.43 | $2.01 | — | $9,581.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $635.48; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 143 | $4.43 | $2.42 | — | $8,945.25 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $635.48; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 2118 | $0.30 | $12.71 | — | $8,297.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $635.48; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 13 | $46.85 | $2.03 | — | $7,686.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $635.48; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 70 | $9.01 | $2.20 | — | $7,053.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $635.48; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 163 | $3.89 | $2.48 | — | $6,416.61 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $635.48; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 18 | $34.05 | $2.04 | — | $5,801.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $635.48; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 28 | $22.44 | $2.07 | — | $5,171.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $635.48; owner union_e_fresh_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 13 | $123.47 | $2.03 | — | $3,564.14 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+2.9; combo leftover $1723.76; owner union_earn_react_h3 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BILL` | 35 | $49.00 | $2.10 | — | $1,847.04 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.0; combo leftover $1723.76; owner union_earn_react_h3 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 173 | $9.94 | $2.51 | — | $124.91 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+12.6; combo leftover $1723.76; owner union_earn_react_h3 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.91 | ▼ close $10,025.80 vs 09:30 $10,167.75 (session -107.36) | 16:00 close · cash $124.91 · equity $10,025.80 vs 09:30 $10,167.75 (-141.95; session marks -107.36) · 11 name(s) marked open→close (per-name table). EL×6 09:30 $97.43 → close $96.15 -7.68; TOYO×143 09:30 $4.43 → close $4.51 +12.15; DVLT×2118 09:30 $0.30 → close $0.32 +42.36; AAP×13 09:30 $46.85 → close $42.39 -57.98; AEG×70 09:30 $9.01 → close $9.01 +0.00; ALVO×163 09:30 $3.89 → close $4.27 +61.94; ATAT×18 09:30 $34.05 → close $34.25 +3.60; ATHM×28 09:30 $22.44 → close $22.12 -8.96; BABA×13 09:30 $123.47 → close $130.53 +91.78; BILL×35 09:30 $49.00 → close $47.40 -56.00; BULL×173 09:30 $9.94 → close $8.85 -188.57 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.91 | ▼ 09:30 equity $10,006.02 vs yday $10,025.80 (-19.78) | 09:30 open · cash $124.91 (unchanged overnight, no fees) · equity $10,006.02 vs prior close $10,025.80 (-19.78) | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 6 | $96.75 | $2.03 | $-8.12 | $703.38 | ▼ -8.12 after sell → book $10,003.99; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 143 | $4.68 | $2.45 | $+30.88 | $1,370.17 | ▲ +30.88 after sell → book $10,001.54; vs 09:30 mark -2.45 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 2118 | $0.31 | $13.28 | $-4.81 | $2,013.47 | ▼ -4.81 after sell → book $9,988.26; vs 09:30 mark -13.28 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 70 | $9.04 | $2.22 | $-2.32 | $2,644.05 | ▼ -2.32 after sell → book $9,986.04; vs 09:30 mark -2.22 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALVO` | 163 | $4.32 | $2.52 | $+65.09 | $3,345.69 | ▲ +65.09 after sell → book $9,983.52; vs 09:30 mark -2.52 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 18 | $34.31 | $2.06 | $+0.57 | $3,961.21 | ▲ +0.57 after sell → book $9,981.46; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 28 | $22.20 | $2.09 | $-10.89 | $4,580.72 | ▼ -10.89 after sell → book $9,979.37; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 2 | $115.18 | $2.00 | — | $4,348.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $327.19; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 3 | $103.69 | $2.00 | — | $4,035.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; ret5=-10.3; combo leftover $327.19; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 18 | $17.93 | $2.04 | — | $3,710.42 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $327.19; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 3 | $93.98 | $2.00 | — | $3,426.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; combo leftover $327.19; owner union_e_fresh_h1 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 7 | $43.08 | $2.01 | — | $3,122.91 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $327.19; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 142 | $2.30 | $2.42 | — | $2,793.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $327.19; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ROST` | 11 | $243.85 | $2.02 | — | $109.52 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.5; combo leftover $2793.89; owner union_earn_react_h3 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.52 | ▼ close $9,841.23 vs 09:30 $10,006.02 (session -123.65) | 16:00 close · cash $109.52 · equity $9,841.23 vs 09:30 $10,006.02 (-164.79; session marks -123.65) · 11 name(s) marked open→close (per-name table). AAP×13 09:30 $42.41 → close $42.58 +2.21; BABA×13 09:30 $125.35 → close $119.34 -78.13; BILL×35 09:30 $47.50 → close $47.82 +11.20; BULL×173 09:30 $8.99 → close $8.78 -36.33; FUTU×2 09:30 $115.18 → close $123.64 +16.92; WMT×3 09:30 $103.69 → close $103.70 +0.03; BEKE×18 09:30 $17.93 → close $17.75 -3.33; BJ×3 09:30 $93.98 → close $96.42 +7.32; BKE×7 09:30 $43.08 → close $43.81 +5.11; PSEC×142 09:30 $2.30 → close $2.33 +4.26; ROST×11 09:30 $243.85 → close $239.04 -52.91 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.52 | ▼ 09:30 equity $9,773.18 vs yday $9,841.23 (-68.05) | 09:30 open · cash $109.52 (unchanged overnight, no fees) · equity $9,773.18 vs prior close $9,841.23 (-68.05) | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 13 | $43.05 | $2.05 | $-53.48 | $667.12 | ▼ -53.48 after sell → book $9,771.13; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 2 | $121.00 | $2.02 | $+7.63 | $907.10 | ▲ +7.63 after sell → book $9,769.11; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 3 | $104.14 | $2.02 | $-2.67 | $1,217.50 | ▼ -2.67 after sell → book $9,767.09; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 18 | $18.05 | $2.06 | $-1.95 | $1,540.43 | ▼ -1.95 after sell → book $9,765.03; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 3 | $97.02 | $2.02 | $+5.10 | $1,829.47 | ▲ +5.10 after sell → book $9,763.01; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 7 | $44.22 | $2.03 | $+3.94 | $2,136.98 | ▲ +3.94 after sell → book $9,760.98; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 142 | $2.34 | $2.45 | $+0.81 | $2,466.81 | ▲ +0.81 after sell → book $9,758.53; vs 09:30 mark -2.45 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,466.81 | ▲ close $9,824.93 vs 09:30 $9,773.18 (session +66.40) | 16:00 close · cash $2,466.81 · equity $9,824.93 vs 09:30 $9,773.18 (+51.75; session marks +66.40) · 4 name(s) marked open→close (per-name table). BABA×13 09:30 $116.90 → close $118.47 +20.41; BILL×35 09:30 $47.68 → close $48.16 +16.80; BULL×173 09:30 $8.58 → close $8.53 -8.65; ROST×11 09:30 $238.08 → close $241.52 +37.84 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,466.81 | ▼ 09:30 equity $9,799.58 vs yday $9,824.93 (-25.35) | 09:30 open · cash $2,466.81 (unchanged overnight, no fees) · equity $9,799.58 vs prior close $9,824.93 (-25.35) | — |
| 2026-08-25 09:30 ET | **SELL** | `BABA` | 13 | $117.94 | $2.05 | $-75.97 | $3,997.98 | ▼ -75.97 after sell → book $9,797.53; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BILL` | 35 | $47.98 | $2.12 | $-39.74 | $5,675.33 | ▼ -39.74 after sell → book $9,795.41; vs 09:30 mark -2.12 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BULL` | 173 | $8.46 | $2.55 | $-261.10 | $7,136.37 | ▼ -261.10 after sell → book $9,792.87; vs 09:30 mark -2.54 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 5 | $175.01 | $2.00 | — | $6,259.31 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $892.05; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 10 | $88.94 | $2.02 | — | $5,367.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $892.05; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 58 | $15.28 | $2.16 | — | $4,479.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $892.05; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 6 | $142.36 | $2.01 | — | $3,623.32 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $892.05; owner union_e_fresh_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 174 | $5.10 | $2.51 | — | $2,733.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $892.05; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 18 | $47.89 | $2.04 | — | $1,869.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $892.05; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 64 | $13.92 | $2.18 | — | $976.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $892.05; owner union_e_fresh_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 196 | $4.54 | $2.58 | — | $82.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $892.05; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.88 | ▼ close $9,513.91 vs 09:30 $9,799.58 (session -261.44) | 16:00 close · cash $82.88 · equity $9,513.91 vs 09:30 $9,799.58 (-285.67; session marks -261.44) · 9 name(s) marked open→close (per-name table). ROST×11 09:30 $241.50 → close $241.19 -3.41; BMO×5 09:30 $175.01 → close $173.46 -7.75; BNS×10 09:30 $88.94 → close $93.10 +41.60; BZ×58 09:30 $15.28 → close $16.29 +58.58; DKS×6 09:30 $142.36 → close $124.31 -108.30; EH×174 09:30 $5.10 → close $4.83 -46.98; GFI×18 09:30 $47.89 → close $48.87 +17.64; GRRR×64 09:30 $13.92 → close $14.04 +7.68; SHMD×196 09:30 $4.54 → close $3.42 -220.50 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.88 | ▼ 09:30 equity $9,505.56 vs yday $9,513.91 (-8.35) | 09:30 open · cash $82.88 (unchanged overnight, no fees) · equity $9,505.56 vs prior close $9,513.91 (-8.35) | — |
| 2026-08-26 09:30 ET | **SELL** | `ROST` | 11 | $242.50 | $2.05 | $-18.93 | $2,748.33 | ▼ -18.93 after sell → book $9,503.51; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMO` | 5 | $173.22 | $2.02 | $-12.98 | $3,612.40 | ▼ -12.98 after sell → book $9,501.48; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BNS` | 10 | $92.65 | $2.04 | $+33.04 | $4,536.86 | ▲ +33.04 after sell → book $9,499.44; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EH` | 174 | $4.77 | $2.55 | $-62.48 | $5,364.29 | ▼ -62.48 after sell → book $9,496.89; vs 09:30 mark -2.55 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GFI` | 18 | $48.24 | $2.06 | $+2.19 | $6,230.55 | ▲ +2.19 after sell → book $9,494.83; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 64 | $14.03 | $2.20 | $+2.66 | $7,126.27 | ▲ +2.66 after sell → book $9,492.63; vs 09:30 mark -2.20 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `SHMD` | 196 | $3.38 | $2.62 | $-233.54 | $7,786.13 | ▼ -233.54 after sell → book $9,490.01; vs 09:30 mark -2.62 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 1112 | $0.58 | $9.82 | — | $7,128.01 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $648.84; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 124 | $5.21 | $2.36 | — | $6,479.61 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $648.84; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 4 | $131.37 | $2.00 | — | $5,952.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; combo leftover $648.84; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 35 | $18.26 | $2.10 | — | $5,310.93 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $648.84; owner union_e_fresh_h1 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 18 | $34.30 | $2.04 | — | $4,691.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $648.84; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 1 | $326.91 | $1.99 | — | $4,362.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-15.2; combo leftover $648.84; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FSCO` | 286 | $5.08 | $3.69 | — | $2,906.01 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-1.6; combo leftover $1454.19; owner union_earn_react_h3 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `HEI` | 3 | $370.00 | $2.00 | — | $1,794.02 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.6; combo leftover $1454.19; owner union_earn_react_h3 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `INTU` | 4 | $323.47 | $2.00 | — | $498.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+2.0; combo leftover $1454.19; owner union_earn_react_h3 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $498.13 | ▲ close $9,708.17 vs 09:30 $9,505.56 (session +246.17) | 16:00 close · cash $498.13 · equity $9,708.17 vs 09:30 $9,505.56 (+202.61; session marks +246.17) · 11 name(s) marked open→close (per-name table). BZ×58 09:30 $16.77 → close $18.84 +120.06; DKS×6 09:30 $121.87 → close $129.66 +46.74; SLQT×1112 09:30 $0.58 → close $0.55 -36.70; TIGR×124 09:30 $5.21 → close $5.46 +31.00; ANF×4 09:30 $131.37 → close $147.75 +65.52; BBWI×35 09:30 $18.26 → close $18.90 +22.40; BOX×18 09:30 $34.30 → close $33.39 -16.38; DY×1 09:30 $326.91 → close $310.91 -16.00; FSCO×286 09:30 $5.08 → close $5.12 +11.44; HEI×3 09:30 $370.00 → close $346.15 -71.55; INTU×4 09:30 $323.47 → close $345.88 +89.64 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $498.13 | ▼ 09:30 equity $9,681.03 vs yday $9,708.17 (-27.14) | 09:30 open · cash $498.13 (unchanged overnight, no fees) · equity $9,681.03 vs prior close $9,708.17 (-27.14) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 58 | $18.50 | $2.18 | $+182.41 | $1,568.95 | ▲ +182.41 after sell → book $9,678.85; vs 09:30 mark -2.18 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 6 | $128.73 | $2.03 | $-85.82 | $2,339.30 | ▼ -85.82 after sell → book $9,676.82; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 1112 | $0.53 | $9.42 | $-78.18 | $2,919.24 | ▼ -78.18 after sell → book $9,667.40; vs 09:30 mark -9.42 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 124 | $5.49 | $2.39 | $+29.97 | $3,597.60 | ▲ +29.97 after sell → book $9,665.00; vs 09:30 mark -2.40 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ANF` | 4 | $144.70 | $2.02 | $+49.30 | $4,174.38 | ▲ +49.30 after sell → book $9,662.98; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBWI` | 35 | $18.69 | $2.12 | $+10.84 | $4,826.42 | ▲ +10.84 after sell → book $9,660.87; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BOX` | 18 | $33.79 | $2.06 | $-13.29 | $5,432.57 | ▼ -13.29 after sell → book $9,658.80; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DY` | 1 | $314.90 | $2.01 | $-16.02 | $5,745.46 | ▼ -16.02 after sell → book $9,656.79; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 25 | $222.86 | $2.06 | — | $171.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list mover_buy; 🔵; ret5=-3.6; combo leftover $5745.46; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $171.90 | ▲ close $9,738.75 vs 09:30 $9,681.03 (session +84.02) | 16:00 close · cash $171.90 · equity $9,738.75 vs 09:30 $9,681.03 (+57.72; session marks +84.02) · 4 name(s) marked open→close (per-name table). FSCO×286 09:30 $5.10 → close $5.12 +5.72; HEI×3 09:30 $346.19 → close $337.01 -27.54; INTU×4 09:30 $353.54 → close $348.00 -22.16; NVDA×25 09:30 $222.86 → close $227.98 +128.00 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $171.90 | ▼ 09:30 equity $9,731.35 vs yday $9,738.75 (-7.40) | 09:30 open · cash $171.90 (unchanged overnight, no fees) · equity $9,731.35 vs prior close $9,738.75 (-7.40) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 25 | $227.36 | $2.12 | $+108.31 | $5,853.78 | ▲ +108.31 after sell → book $9,729.23; vs 09:30 mark -2.12 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 2 | $261.16 | $2.00 | — | $5,329.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $731.72; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 48 | $15.01 | $2.13 | — | $4,606.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $731.72; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 7 | $103.89 | $2.01 | — | $3,877.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $731.72; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 188 | $3.88 | $2.55 | — | $3,145.61 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $731.72; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 16 | $44.40 | $2.04 | — | $2,433.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $731.72; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 29 | $24.69 | $2.08 | — | $1,715.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $731.72; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 87 | $8.35 | $2.25 | — | $986.38 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $731.72; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 19 | $37.65 | $2.05 | — | $269.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $731.72; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $269.08 | ▼ close $9,545.24 vs 09:30 $9,731.35 (session -166.88) | 16:00 close · cash $269.08 · equity $9,545.24 vs 09:30 $9,731.35 (-186.11; session marks -166.88) · 11 name(s) marked open→close (per-name table). FSCO×286 09:30 $5.12 → close $5.18 +17.16; HEI×3 09:30 $339.95 → close $336.53 -10.26; INTU×4 09:30 $347.82 → close $358.06 +40.96; ADSK×2 09:30 $261.16 → close $260.66 -1.00; BBAR×48 09:30 $15.01 → close $14.47 -25.92; ESTC×7 09:30 $103.89 → close $99.91 -27.86; FINV×188 09:30 $3.88 → close $3.40 -90.24; FRO×16 09:30 $44.40 → close $44.19 -3.36; GAP×29 09:30 $24.69 → close $23.48 -35.09; HAFN×87 09:30 $8.35 → close $8.47 +10.44; IREN×19 09:30 $37.65 → close $35.45 -41.71 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $269.08 | ▼ 09:30 equity $9,544.62 vs yday $9,545.24 (-0.62) | 09:30 open · cash $269.08 (unchanged overnight, no fees) · equity $9,544.62 vs prior close $9,545.24 (-0.62) | — |
| 2026-08-31 09:30 ET | **SELL** | `FSCO` | 286 | $5.20 | $3.75 | $+26.88 | $1,752.53 | ▲ +26.88 after sell → book $9,540.87; vs 09:30 mark -3.75 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `HEI` | 3 | $334.88 | $2.02 | $-109.38 | $2,755.15 | ▼ -109.38 after sell → book $9,538.85; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `INTU` | 4 | $356.05 | $2.02 | $+126.29 | $4,177.33 | ▲ +126.29 after sell → book $9,536.83; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 2 | $257.71 | $2.02 | $-10.91 | $4,690.74 | ▼ -10.91 after sell → book $9,534.82; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 48 | $14.88 | $2.15 | $-10.53 | $5,402.82 | ▼ -10.53 after sell → book $9,532.66; vs 09:30 mark -2.16 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 7 | $98.00 | $2.03 | $-45.27 | $6,086.79 | ▼ -45.27 after sell → book $9,530.63; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 188 | $3.39 | $2.60 | $-97.27 | $6,721.52 | ▼ -97.27 after sell → book $9,528.04; vs 09:30 mark -2.59 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 16 | $44.85 | $2.06 | $+3.10 | $7,437.06 | ▲ +3.10 after sell → book $9,525.98; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 29 | $22.98 | $2.10 | $-53.76 | $8,101.38 | ▼ -53.76 after sell → book $9,523.88; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 87 | $8.53 | $2.28 | $+11.13 | $8,841.21 | ▲ +11.13 after sell → book $9,521.60; vs 09:30 mark -2.28 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 19 | $35.81 | $2.07 | $-38.98 | $9,519.54 | ▼ -38.98 after sell → book $9,519.54; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,519.54 | ▲ close $9,519.54 vs 09:30 $9,544.62 (session +0.00) | 16:00 close · cash $9,519.54 · no lots left · equity $9,519.54. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,519.54 | ▲ 09:30 equity $9,519.54 vs yday $9,519.54 (-0.00) | 09:30 open · cash $9,519.54 (unchanged overnight, no fees) · equity $9,519.54 vs prior close $9,519.54 (-0.00) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,519.54 | ▲ close $9,519.54 vs 09:30 $9,519.54 (session +0.00) | 16:00 close · cash $9,519.54 · no lots left · equity $9,519.54. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,519.54 | ▲ 09:30 equity $9,519.54 vs yday $9,519.54 (-0.00) | 09:30 open · cash $9,519.54 (unchanged overnight, no fees) · equity $9,519.54 vs prior close $9,519.54 (-0.00) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,519.54 | ▲ close $9,519.54 vs 09:30 $9,519.54 (session +0.00) | 16:00 close · cash $9,519.54 · no lots left · equity $9,519.54. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,519.54 | ▲ 09:30 equity $9,519.54 vs yday $9,519.54 (-0.00) | 09:30 open · cash $9,519.54 (unchanged overnight, no fees) · equity $9,519.54 vs prior close $9,519.54 (-0.00) | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 110 | $10.74 | $2.32 | — | $8,335.27 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $1189.94; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $7,278.05 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $1189.94; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 172 | $6.90 | $2.51 | — | $6,088.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $1189.94; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 3 | $354.49 | $2.00 | — | $5,023.27 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $1189.94; owner union_e_fresh_h1 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 53 | $22.32 | $2.15 | — | $3,838.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $1189.94; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 4 | $257.00 | $2.00 | — | $2,808.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $1189.94; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 24 | $47.60 | $2.06 | — | $1,663.70 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $1189.94; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 78 | $15.09 | $2.22 | — | $484.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $1189.94; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $484.46 | ▲ close $9,903.33 vs 09:30 $9,519.54 (session +401.05) | 16:00 close · cash $484.46 · equity $9,903.33 vs 09:30 $9,519.54 (+383.79; session marks +401.05) · 8 name(s) marked open→close (per-name table). AI×110 09:30 $10.74 → close $10.90 +17.05; AVGO×3 09:30 $351.74 → close $357.16 +16.26; CHPT×172 09:30 $6.90 → close $9.08 +374.96; CIEN×3 09:30 $354.49 → close $317.46 -111.09; CPB×53 09:30 $22.32 → close $22.13 -10.07; FIVE×4 09:30 $257.00 → close $239.96 -68.16; HPE×24 09:30 $47.60 → close $54.44 +164.16; MEI×78 09:30 $15.09 → close $15.32 +17.94 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $484.46 | ▲ 09:30 equity $9,940.57 vs yday $9,903.33 (+37.24) | 09:30 open · cash $484.46 (unchanged overnight, no fees) · equity $9,940.57 vs prior close $9,903.33 (+37.24) | — |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 110 | $10.91 | $2.35 | $+13.48 | $1,682.21 | ▲ +13.48 after sell → book $9,938.22; vs 09:30 mark -2.35 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $2,759.29 | ▲ +19.86 after sell → book $9,936.20; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 172 | $9.28 | $2.55 | $+404.31 | $4,352.90 | ▲ +404.31 after sell → book $9,933.65; vs 09:30 mark -2.55 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 3 | $321.67 | $2.02 | $-102.48 | $5,315.89 | ▼ -102.48 after sell → book $9,931.63; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CPB` | 53 | $22.10 | $2.17 | $-15.98 | $6,485.02 | ▼ -15.98 after sell → book $9,929.46; vs 09:30 mark -2.17 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 4 | $238.88 | $2.02 | $-76.50 | $7,438.52 | ▼ -76.50 after sell → book $9,927.44; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 24 | $53.85 | $2.08 | $+145.86 | $8,728.84 | ▲ +145.86 after sell → book $9,925.36; vs 09:30 mark -2.08 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 78 | $15.34 | $2.25 | $+15.03 | $9,923.11 | ▲ +15.03 after sell → book $9,923.11; vs 09:30 mark -2.25 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 19 | $63.18 | $2.05 | — | $8,720.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $1240.39; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 141 | $8.74 | $2.41 | — | $7,485.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $1240.39; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 18 | $68.52 | $2.04 | — | $6,250.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $1240.39; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 343 | $3.62 | $4.42 | — | $5,006.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $1240.39; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 7 | $167.55 | $2.01 | — | $3,831.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $1240.39; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 27 | $44.90 | $2.07 | — | $2,616.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $1240.39; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 12 | $98.15 | $2.03 | — | $1,437.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $1240.39; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 79 | $15.70 | $2.23 | — | $194.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $1240.39; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $194.53 | ▼ close $9,821.17 vs 09:30 $9,940.57 (session -82.68) | 16:00 close · cash $194.53 · equity $9,821.17 vs 09:30 $9,940.57 (-119.40; session marks -82.68) · 8 name(s) marked open→close (per-name table). AMBA×19 09:30 $63.18 → close $62.89 -5.51; ASAN×141 09:30 $8.74 → close $8.81 +9.87; DOCU×18 09:30 $68.52 → close $68.41 -1.98; DOMO×343 09:30 $3.62 → close $3.88 +90.89; GWRE×7 09:30 $167.55 → close $162.42 -35.91; IOT×27 09:30 $44.90 → close $40.20 -126.90; LULU×12 09:30 $98.15 → close $100.61 +29.52; MAMA×79 09:30 $15.70 → close $15.16 -42.66 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $194.53 | ▼ 09:30 equity $9,761.77 vs yday $9,821.17 (-59.40) | 09:30 open · cash $194.53 (unchanged overnight, no fees) · equity $9,761.77 vs prior close $9,821.17 (-59.40) | — |
| 2026-09-08 09:30 ET | **SELL** | `AMBA` | 19 | $63.83 | $2.07 | $+8.24 | $1,405.24 | ▲ +8.24 after sell → book $9,759.71; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASAN` | 141 | $8.73 | $2.45 | $-6.27 | $2,633.72 | ▼ -6.27 after sell → book $9,757.26; vs 09:30 mark -2.45 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 18 | $67.05 | $2.06 | $-30.57 | $3,838.56 | ▼ -30.57 after sell → book $9,755.20; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOMO` | 343 | $3.84 | $4.49 | $+68.26 | $5,151.18 | ▲ +68.26 after sell → book $9,750.70; vs 09:30 mark -4.50 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 7 | $160.52 | $2.03 | $-53.25 | $6,272.79 | ▼ -53.25 after sell → book $9,748.67; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 27 | $39.56 | $2.09 | $-148.34 | $7,338.82 | ▼ -148.34 after sell → book $9,746.58; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 12 | $100.58 | $2.05 | $+25.09 | $8,543.74 | ▲ +25.09 after sell → book $9,744.54; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MAMA` | 79 | $15.20 | $2.25 | $-43.98 | $9,742.29 | ▼ -43.98 after sell → book $9,742.29; vs 09:30 mark -2.25 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,742.29 | ▲ close $9,742.29 vs 09:30 $9,761.77 (session +0.00) | 16:00 close · cash $9,742.29 · no lots left · equity $9,742.29. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,742.29 | ▲ 09:30 equity $9,742.29 vs yday $9,742.29 (-0.00) | 09:30 open · cash $9,742.29 (unchanged overnight, no fees) · equity $9,742.29 vs prior close $9,742.29 (-0.00) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,742.29 | ▲ close $9,742.29 vs 09:30 $9,742.29 (session +0.00) | 16:00 close · cash $9,742.29 · no lots left · equity $9,742.29. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,742.29 | ▲ 09:30 equity $9,742.29 vs yday $9,742.29 (-0.00) | 09:30 open · cash $9,742.29 (unchanged overnight, no fees) · equity $9,742.29 vs prior close $9,742.29 (-0.00) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,742.29 | ▲ close $9,742.29 vs 09:30 $9,742.29 (session +0.00) | 16:00 close · cash $9,742.29 · no lots left · equity $9,742.29. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,742.29 | ▲ 09:30 equity $9,742.29 vs yday $9,742.29 (-0.00) | 09:30 open · cash $9,742.29 (unchanged overnight, no fees) · equity $9,742.29 vs prior close $9,742.29 (-0.00) | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 3 | $164.43 | $2.00 | — | $9,247.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; 🔵; ⚪; ret5=+9.0; combo leftover $608.89; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 103 | $5.91 | $2.30 | — | $8,635.97 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list probable,yday_gainer,yday_mover; ret5=-4.9; combo leftover $608.89; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 2 | $242.17 | $2.00 | — | $8,149.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-12.1; combo leftover $608.89; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 19 | $32.01 | $2.05 | — | $7,539.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $608.89; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 8 | $71.71 | $2.01 | — | $6,963.70 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.3; combo leftover $608.89; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 10 | $56.02 | $2.02 | — | $6,401.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.6; combo leftover $608.89; owner union_e_fresh_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 64 | $9.37 | $2.18 | — | $5,799.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $608.89; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 46 | $13.10 | $2.13 | — | $5,194.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.2; combo leftover $608.89; owner union_e_fresh_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 38 | $135.71 | $2.10 | — | $35.81 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.5; combo leftover $5194.89; owner union_earn_react_h3 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.81 | ▼ close $9,687.86 vs 09:30 $9,742.29 (session -35.64) | 16:00 close · cash $35.81 · equity $9,687.86 vs 09:30 $9,742.29 (-54.43; session marks -35.64) · 9 name(s) marked open→close (per-name table). ORCL×3 09:30 $164.43 → close $150.28 -42.45; DBI×103 09:30 $5.91 → close $5.88 -3.09; ADBE×2 09:30 $242.17 → close $252.23 +20.12; CPRT×19 09:30 $32.01 → close $29.95 -39.14; DSGX×8 09:30 $71.71 → close $76.04 +34.64; KR×10 09:30 $56.02 → close $58.49 +24.70; LPTH×64 09:30 $9.37 → close $9.20 -10.88; REF×46 09:30 $13.10 → close $14.03 +42.78; RH×38 09:30 $135.71 → close $134.07 -62.32 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `AIRJ` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `AMAT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `AMPG` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `BRUN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `BZAI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `DEFT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `DGXX` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `AIRJ` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `AMAT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `AMPG` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `BRUN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `BZAI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `DEFT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `DGXX` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new union_e_fresh_h1 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new union_earn_react_h3 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new union_earn_react_h3 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new union_earn_react_h3 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new union_earn_react_h3 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new union_earn_react_h3 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new union_earn_react_h3 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new union_earn_react_h3 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new union_e_fresh_h1 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new union_earn_react_h3 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new union_earn_react_h3 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new union_earn_react_h3 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new union_earn_react_h3 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new union_earn_react_h3 |
| 2026-08-19 | `LOW` | hard_red | hard-red S=-7.20 sit; no new union_earn_react_h3 |
| 2026-08-19 | `LZB` | hard_red | hard-red S=-7.20 sit; no new union_earn_react_h3 |
| 2026-08-19 | `MRCY` | hard_red | hard-red S=-7.20 sit; no new union_earn_react_h3 |
| 2026-08-21 | `BABA` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `BILL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `BULL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `DE` | cash | leftover split 327.19 < 1 share @ 623.26 |
| 2026-08-24 | `BABA` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BILL` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BULL` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ROST` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new union_e_fresh_h1 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new union_earn_react_h3 |
| 2026-08-25 | `ROST` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `FSCO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `HEI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `INTU` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `FSCO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `HEI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `INTU` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new union_e_fresh_h1 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new union_earn_react_h3 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new union_e_fresh_h1 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new union_earn_react_h3 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new union_e_fresh_h1 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new union_earn_react_h3 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new union_earn_react_h3 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new union_earn_react_h3 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new union_earn_react_h3 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new union_earn_react_h3 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new union_earn_react_h3 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new union_e_fresh_h1 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new union_e_fresh_h1 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new union_e_fresh_h1 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new union_earn_react_h3 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new union_earn_react_h3 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new union_earn_react_h3 |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new union_e_fresh_h1 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new union_earn_react_h3 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new union_earn_react_h3 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new union_earn_react_h3 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new union_earn_react_h3 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new union_earn_react_h3 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new union_earn_react_h3 |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new union_e_fresh_h1 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new union_earn_react_h3 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new union_earn_react_h3 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new union_earn_react_h3 |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new union_earn_react_h3 |
| 2026-09-10 | `M` | hard_red | hard-red S=-13.28 sit; no new union_earn_react_h3 |
| 2026-09-10 | `NAVN` | hard_red | hard-red S=-13.28 sit; no new union_earn_react_h3 |
| 2026-09-10 | `NB` | hard_red | hard-red S=-13.28 sit; no new union_earn_react_h3 |
| 2026-09-10 | `SHOE` | hard_red | hard-red S=-13.28 sit; no new union_earn_react_h3 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ORCL` | 3 | 2026-09-11 @ $164.43 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; 🔵; ⚪; ret5=+9.0; combo leftover $608.89; owner union_e_fresh_h1 |
| `DBI` | 103 | 2026-09-11 @ $5.91 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list probable,yday_gainer,yday_mover; ret5=-4.9; combo leftover $608.89; owner union_e_fresh_h1 |
| `ADBE` | 2 | 2026-09-11 @ $242.17 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-12.1; combo leftover $608.89; owner union_e_fresh_h1 |
| `CPRT` | 19 | 2026-09-11 @ $32.01 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $608.89; owner union_e_fresh_h1 |
| `DSGX` | 8 | 2026-09-11 @ $71.71 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.3; combo leftover $608.89; owner union_e_fresh_h1 |
| `KR` | 10 | 2026-09-11 @ $56.02 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.6; combo leftover $608.89; owner union_e_fresh_h1 |
| `LPTH` | 64 | 2026-09-11 @ $9.37 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $608.89; owner union_e_fresh_h1 |
| `REF` | 46 | 2026-09-11 @ $13.10 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.2; combo leftover $608.89; owner union_e_fresh_h1 |
| `RH` | 38 | 2026-09-11 @ $135.71 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.5; combo leftover $5194.89; owner union_earn_react_h3 |
