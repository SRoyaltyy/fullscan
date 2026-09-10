# Factor mine action — `combo_sh_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared short_news_r_h3/union_hot_n4_h1 w=0.5,0.5 net=priority

Cash book **+36.01%** ($13,601) · signal-only (no cash/fees) was —. Starts YES **17/19**. Fills 142 · skips 104 · realized $+3503.55.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: short_news_r_h3 50%, union_hot_n4_h1 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: short_news_r_h3 50%, union_hot_n4_h1 50%.
- Member: short_news_r_h3 (50% · short · hold 3).
- Member: union_hot_n4_h1 (50% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $19,985.43.

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
| 2026-08-14 | `EU` | 1462 | — | $1.18 | +0.00 | $1.21 | -43.86 | -43.86 | -0.00 | -43.86 |
| 2026-08-14 | `LUNR` | 90 | — | $19.17 | +0.00 | $19.01 | +14.40 | +14.40 | -0.00 | +14.40 |
| 2026-08-14 | `OWL` | 135 | — | $12.70 | +0.00 | $12.22 | +64.12 | +64.12 | -0.00 | +64.12 |
| 2026-08-17 | `QMCO` | 52 | $26.11 | $24.83 | -66.56 | — | +0.00 | -66.56 | +7.80 | — |
| 2026-08-17 | `ARX` | 66 | $19.58 | $19.57 | -0.66 | — | +0.00 | -0.66 | +0.00 | — |
| 2026-08-17 | `ZENA` | 589 | $2.14 | $2.08 | -32.40 | — | +0.00 | -32.40 | -67.74 | — |
| 2026-08-17 | `AIRO` | 116 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -179.80 | — |
| 2026-08-17 | `EU` | 1462 | $1.21 | $1.21 | +0.00 | $1.13 | +116.96 | +116.96 | -43.86 | +73.10 |
| 2026-08-17 | `LUNR` | 90 | $19.01 | $20.25 | -111.60 | $20.38 | -11.70 | -123.30 | -97.20 | -108.90 |
| 2026-08-17 | `OWL` | 135 | $12.22 | $12.12 | +13.50 | $11.66 | +62.10 | +75.60 | +77.63 | +139.73 |
| 2026-08-17 | `XHG` | 454 | — | $4.19 | +0.00 | $3.91 | -127.12 | -127.12 | +0.00 | -127.12 |
| 2026-08-17 | `CAPR` | 277 | — | $6.87 | +0.00 | $7.45 | +160.66 | +160.66 | +0.00 | +160.66 |
| 2026-08-17 | `STDN` | 139 | — | $13.64 | +0.00 | $13.31 | -45.87 | -45.87 | +0.00 | -45.87 |
| 2026-08-17 | `HTFL` | 46 | — | $41.23 | +0.00 | $41.94 | +32.66 | +32.66 | +0.00 | +32.66 |
| 2026-08-17 | `VERI` | 1086 | — | $1.15 | +0.00 | $1.08 | +70.59 | +70.59 | -0.00 | +70.59 |
| 2026-08-17 | `ZNTL` | 351 | — | $3.56 | +0.00 | $3.71 | -50.90 | -50.90 | -0.00 | -50.90 |
| 2026-08-17 | `APMD` | 39 | — | $31.70 | +0.00 | $32.55 | -33.15 | -33.15 | -0.00 | -33.15 |
| 2026-08-17 | `HIVE` | 415 | — | $3.01 | +0.00 | $3.07 | -24.90 | -24.90 | -0.00 | -24.90 |
| 2026-08-18 | `EU` | 1462 | $1.13 | $1.13 | +0.00 | $1.07 | +87.72 | +87.72 | +73.10 | +160.82 |
| 2026-08-18 | `LUNR` | 90 | $20.38 | $19.31 | +96.30 | $19.31 | +0.00 | +96.30 | -12.60 | -12.60 |
| 2026-08-18 | `OWL` | 135 | $11.66 | $11.54 | +16.20 | $11.59 | -6.75 | +9.45 | +155.93 | +149.18 |
| 2026-08-18 | `XHG` | 454 | $3.91 | $3.94 | +13.62 | — | +0.00 | +13.62 | -113.50 | — |
| 2026-08-18 | `CAPR` | 277 | $7.45 | $7.50 | +13.85 | $7.08 | -116.34 | -102.49 | +174.51 | +58.17 |
| 2026-08-18 | `STDN` | 139 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -45.87 | — |
| 2026-08-18 | `HTFL` | 46 | $41.94 | $41.50 | -20.24 | — | +0.00 | -20.24 | +12.42 | — |
| 2026-08-18 | `VERI` | 1086 | $1.08 | $1.05 | +38.01 | $0.99 | +59.73 | +97.74 | +108.60 | +168.33 |
| 2026-08-18 | `ZNTL` | 351 | $3.71 | $3.75 | -15.79 | $3.68 | +24.57 | +8.78 | -66.69 | -42.12 |
| 2026-08-18 | `APMD` | 39 | $32.55 | $32.85 | -11.70 | $31.81 | +40.56 | +28.86 | -44.85 | -4.29 |
| 2026-08-18 | `HIVE` | 415 | $3.07 | $2.96 | +45.65 | $2.78 | +74.70 | +120.35 | +20.75 | +95.45 |
| 2026-08-19 | `EU` | 1462 | $1.07 | $1.07 | +0.00 | — | +0.00 | +0.00 | +160.82 | — |
| 2026-08-19 | `LUNR` | 90 | $19.31 | $18.98 | +29.70 | $18.52 | +41.40 | +71.10 | +17.10 | +58.50 |
| 2026-08-19 | `OWL` | 135 | $11.59 | $11.75 | -21.60 | — | +0.00 | -21.60 | +127.58 | — |
| 2026-08-19 | `CAPR` | 277 | $7.08 | $7.19 | +30.47 | — | +0.00 | +30.47 | +88.64 | — |
| 2026-08-19 | `VERI` | 1086 | $0.99 | $1.00 | -5.43 | $0.97 | +36.92 | +31.49 | +162.90 | +199.82 |
| 2026-08-19 | `ZNTL` | 351 | $3.68 | $3.76 | -28.08 | $3.82 | -21.06 | -49.14 | -70.20 | -91.26 |
| 2026-08-19 | `APMD` | 39 | $31.81 | $32.13 | -12.48 | $32.03 | +3.90 | -8.58 | -16.77 | -12.87 |
| 2026-08-19 | `HIVE` | 415 | $2.78 | $2.78 | +0.00 | $2.82 | -16.60 | -16.60 | +95.45 | +78.85 |
| 2026-08-20 | `LUNR` | 90 | $18.52 | $18.13 | +35.10 | — | +0.00 | +35.10 | +93.60 | — |
| 2026-08-20 | `VERI` | 1086 | $0.97 | $0.96 | +3.26 | — | +0.00 | +3.26 | +203.08 | — |
| 2026-08-20 | `ZNTL` | 351 | $3.82 | $4.01 | -68.44 | — | +0.00 | -68.44 | -159.70 | — |
| 2026-08-20 | `APMD` | 39 | $32.03 | $31.87 | +6.24 | — | +0.00 | +6.24 | -6.63 | — |
| 2026-08-20 | `HIVE` | 415 | $2.82 | $2.95 | -53.95 | — | +0.00 | -53.95 | +24.90 | — |
| 2026-08-20 | `ABCL` | 109 | — | $11.81 | +0.00 | $11.57 | -26.70 | -26.70 | +0.00 | -26.70 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `CYPH` | 1125 | — | $1.15 | +0.00 | $1.19 | +45.00 | +45.00 | +0.00 | +45.00 |
| 2026-08-20 | `AZI` | 944 | — | $1.37 | +0.00 | $1.44 | +66.08 | +66.08 | +0.00 | +66.08 |
| 2026-08-20 | `AEM` | 3 | — | $204.45 | +0.00 | $212.04 | -22.77 | -22.77 | -0.00 | -22.77 |
| 2026-08-20 | `WYFI` | 34 | — | $21.40 | +0.00 | $21.16 | +8.16 | +8.16 | -0.00 | +8.16 |
| 2026-08-20 | `TOYO` | 166 | — | $4.43 | +0.00 | $4.51 | -14.11 | -14.11 | -0.00 | -14.11 |
| 2026-08-20 | `TEAM` | 4 | — | $173.90 | +0.00 | $174.91 | -4.04 | -4.04 | -0.00 | -4.04 |
| 2026-08-20 | `AAP` | 15 | — | $46.85 | +0.00 | $42.39 | +66.90 | +66.90 | -0.00 | +66.90 |
| 2026-08-20 | `WMT` | 6 | — | $106.38 | +0.00 | $103.84 | +15.24 | +15.24 | -0.00 | +15.24 |
| 2026-08-20 | `AQST` | 159 | — | $4.61 | +0.00 | $4.50 | +18.29 | +18.29 | -0.00 | +18.29 |
| 2026-08-21 | `ABCL` | 109 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -26.70 | — |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | $145.13 | +96.16 | +94.48 | -136.24 | -40.08 |
| 2026-08-21 | `CYPH` | 1125 | $1.19 | $1.32 | +146.25 | $1.42 | +112.50 | +258.75 | +191.25 | +303.75 |
| 2026-08-21 | `AZI` | 944 | $1.44 | $1.46 | +18.88 | — | +0.00 | +18.88 | +84.96 | — |
| 2026-08-21 | `AEM` | 3 | $212.04 | $216.30 | -12.78 | $216.06 | +0.72 | -12.06 | -35.55 | -34.83 |
| 2026-08-21 | `WYFI` | 34 | $21.16 | $21.54 | -12.92 | $20.72 | +27.88 | +14.96 | -4.76 | +23.12 |
| 2026-08-21 | `TOYO` | 166 | $4.51 | $4.68 | -27.39 | $4.82 | -23.24 | -50.63 | -41.50 | -64.74 |
| 2026-08-21 | `TEAM` | 4 | $174.91 | $174.22 | +2.76 | $171.81 | +9.64 | +12.40 | -1.28 | +8.36 |
| 2026-08-21 | `AAP` | 15 | $42.39 | $42.41 | -0.30 | $42.58 | -2.55 | -2.85 | +66.60 | +64.05 |
| 2026-08-21 | `WMT` | 6 | $103.84 | $103.69 | +0.90 | $103.70 | -0.06 | +0.84 | +16.14 | +16.08 |
| 2026-08-21 | `AQST` | 159 | $4.50 | $4.54 | -7.15 | $4.66 | -19.08 | -26.23 | +11.13 | -7.95 |
| 2026-08-21 | `XHG` | 707 | — | $4.49 | +0.00 | $4.41 | -56.56 | -56.56 | +0.00 | -56.56 |
| 2026-08-21 | `CAPR` | 466 | — | $6.81 | +0.00 | $6.29 | -242.32 | -242.32 | +0.00 | -242.32 |
| 2026-08-21 | `QTRX` | 334 | — | $3.11 | +0.00 | $2.99 | +40.08 | +40.08 | -0.00 | +40.08 |
| 2026-08-21 | `AUGO` | 11 | — | $89.10 | +0.00 | $87.26 | +20.24 | +20.24 | -0.00 | +20.24 |
| 2026-08-21 | `SSRM` | 27 | — | $38.40 | +0.00 | $37.77 | +17.01 | +17.01 | -0.00 | +17.01 |
| 2026-08-21 | `ARIS` | 49 | — | $20.90 | +0.00 | $20.86 | +1.96 | +1.96 | -0.00 | +1.96 |
| 2026-08-21 | `NOG` | 38 | — | $27.00 | +0.00 | $27.34 | -12.92 | -12.92 | -0.00 | -12.92 |
| 2026-08-24 | `MRNA` | 8 | $145.13 | $142.70 | -19.44 | — | +0.00 | -19.44 | -59.52 | — |
| 2026-08-24 | `CYPH` | 1125 | $1.42 | $1.83 | +461.25 | — | +0.00 | +461.25 | +765.00 | — |
| 2026-08-24 | `AEM` | 3 | $216.06 | $217.03 | -2.91 | $217.89 | -2.58 | -5.49 | -37.74 | -40.32 |
| 2026-08-24 | `WYFI` | 34 | $20.72 | $20.01 | +24.14 | $20.78 | -26.18 | -2.04 | +47.26 | +21.08 |
| 2026-08-24 | `TOYO` | 166 | $4.82 | $4.58 | +39.84 | $4.38 | +33.20 | +73.04 | -24.90 | +8.30 |
| 2026-08-24 | `TEAM` | 4 | $171.81 | $169.30 | +10.04 | $171.33 | -8.12 | +1.92 | +18.40 | +10.28 |
| 2026-08-24 | `AAP` | 15 | $42.58 | $43.05 | -7.05 | $43.63 | -8.70 | -15.75 | +57.00 | +48.30 |
| 2026-08-24 | `WMT` | 6 | $103.70 | $104.14 | -2.64 | $106.49 | -14.10 | -16.74 | +13.44 | -0.66 |
| 2026-08-24 | `AQST` | 159 | $4.66 | $4.67 | -1.59 | $4.80 | -20.67 | -22.26 | -9.54 | -30.21 |
| 2026-08-24 | `XHG` | 707 | $4.41 | $4.32 | -63.63 | — | +0.00 | -63.63 | -120.19 | — |
| 2026-08-24 | `CAPR` | 466 | $6.29 | $8.03 | +810.84 | — | +0.00 | +810.84 | +568.52 | — |
| 2026-08-24 | `QTRX` | 334 | $2.99 | $2.99 | +0.00 | $2.80 | +63.46 | +63.46 | +40.08 | +103.54 |
| 2026-08-24 | `AUGO` | 11 | $87.26 | $88.60 | -14.74 | $87.37 | +13.53 | -1.21 | +5.50 | +19.03 |
| 2026-08-24 | `SSRM` | 27 | $37.77 | $38.32 | -14.85 | $38.61 | -7.83 | -22.68 | +2.16 | -5.67 |
| 2026-08-24 | `ARIS` | 49 | $20.86 | $20.98 | -5.88 | $20.81 | +8.33 | +2.45 | -3.92 | +4.41 |
| 2026-08-24 | `NOG` | 38 | $27.34 | $27.12 | +8.36 | $26.84 | +10.64 | +19.00 | -4.56 | +6.08 |
| 2026-08-25 | `AEM` | 3 | $217.89 | $212.00 | +17.67 | — | +0.00 | +17.67 | -22.65 | — |
| 2026-08-25 | `WYFI` | 34 | $20.78 | $20.90 | -4.08 | — | +0.00 | -4.08 | +17.00 | — |
| 2026-08-25 | `TOYO` | 166 | $4.38 | $4.42 | -6.64 | — | +0.00 | -6.64 | +1.66 | — |
| 2026-08-25 | `TEAM` | 4 | $171.33 | $170.64 | +2.76 | — | +0.00 | +2.76 | +13.04 | — |
| 2026-08-25 | `AAP` | 15 | $43.63 | $43.63 | +0.00 | — | +0.00 | +0.00 | +48.30 | — |
| 2026-08-25 | `WMT` | 6 | $106.49 | $105.58 | +5.46 | — | +0.00 | +5.46 | +4.80 | — |
| 2026-08-25 | `AQST` | 159 | $4.80 | $4.77 | +4.77 | — | +0.00 | +4.77 | -25.44 | — |
| 2026-08-25 | `QTRX` | 334 | $2.80 | $2.80 | +0.00 | $2.79 | +3.34 | +3.34 | +103.54 | +106.88 |
| 2026-08-25 | `AUGO` | 11 | $87.37 | $85.78 | +17.49 | $90.47 | -51.59 | -34.10 | +36.52 | -15.07 |
| 2026-08-25 | `SSRM` | 27 | $38.61 | $37.75 | +23.22 | $39.21 | -39.42 | -16.20 | +17.55 | -21.87 |
| 2026-08-25 | `ARIS` | 49 | $20.81 | $20.45 | +17.64 | $21.18 | -35.77 | -18.13 | +22.05 | -13.72 |
| 2026-08-25 | `NOG` | 38 | $26.84 | $26.06 | +29.64 | $26.42 | -13.68 | +15.96 | +35.72 | +22.04 |
| 2026-08-25 | `REAX` | 85 | — | $24.11 | +0.00 | $28.43 | +367.20 | +367.20 | +0.00 | +367.20 |
| 2026-08-25 | `CYPH` | 1328 | — | $1.56 | +0.00 | $1.64 | +106.24 | +106.24 | +0.00 | +106.24 |
| 2026-08-25 | `XHG` | 509 | — | $4.07 | +0.00 | $4.02 | -25.45 | -25.45 | +0.00 | -25.45 |
| 2026-08-25 | `ASST` | 108 | — | $19.04 | +0.00 | $21.39 | +253.80 | +253.80 | +0.00 | +253.80 |
| 2026-08-25 | `AVAH` | 142 | — | $13.62 | +0.00 | $13.59 | +4.97 | +4.97 | -0.00 | +4.97 |
| 2026-08-25 | `ARE` | 35 | — | $54.51 | +0.00 | $52.90 | +56.35 | +56.35 | -0.00 | +56.35 |
| 2026-08-25 | `BMO` | 11 | — | $175.01 | +0.00 | $173.46 | +17.05 | +17.05 | -0.00 | +17.05 |
| 2026-08-26 | `QTRX` | 334 | $2.79 | $2.83 | -13.36 | — | +0.00 | -13.36 | +93.52 | — |
| 2026-08-26 | `AUGO` | 11 | $90.47 | $88.24 | +24.53 | — | +0.00 | +24.53 | +9.46 | — |
| 2026-08-26 | `SSRM` | 27 | $39.21 | $38.41 | +21.60 | — | +0.00 | +21.60 | -0.27 | — |
| 2026-08-26 | `ARIS` | 49 | $21.18 | $20.50 | +33.32 | — | +0.00 | +33.32 | +19.60 | — |
| 2026-08-26 | `NOG` | 38 | $26.42 | $26.00 | +15.96 | — | +0.00 | +15.96 | +38.00 | — |
| 2026-08-26 | `REAX` | 85 | $28.43 | $26.61 | -154.70 | — | +0.00 | -154.70 | +212.50 | — |
| 2026-08-26 | `CYPH` | 1328 | $1.64 | $1.60 | -53.12 | — | +0.00 | -53.12 | +53.12 | — |
| 2026-08-26 | `XHG` | 509 | $4.02 | $3.81 | -106.89 | $4.06 | +127.25 | +20.36 | -132.34 | -5.09 |
| 2026-08-26 | `ASST` | 108 | $21.39 | $20.72 | -72.36 | — | +0.00 | -72.36 | +181.44 | — |
| 2026-08-26 | `AVAH` | 142 | $13.59 | $13.65 | -8.52 | $13.62 | +4.26 | -4.26 | -3.55 | +0.71 |
| 2026-08-26 | `ARE` | 35 | $52.90 | $52.77 | +4.55 | $52.97 | -7.00 | -2.45 | +60.90 | +53.90 |
| 2026-08-26 | `BMO` | 11 | $173.46 | $173.22 | +2.64 | $172.90 | +3.52 | +6.16 | +19.69 | +23.21 |
| 2026-08-26 | `BYND` | 185 | — | $14.11 | +0.00 | $14.25 | +25.90 | +25.90 | +0.00 | +25.90 |
| 2026-08-26 | `USDE` | 450 | — | $5.81 | +0.00 | $5.98 | +76.50 | +76.50 | +0.00 | +76.50 |
| 2026-08-26 | `SUJA` | 278 | — | $9.39 | +0.00 | $9.44 | +13.90 | +13.90 | +0.00 | +13.90 |
| 2026-08-26 | `BE` | 6 | — | $213.94 | +0.00 | $218.21 | -25.62 | -25.62 | -0.00 | -25.62 |
| 2026-08-26 | `ABCL` | 122 | — | $12.22 | +0.00 | $12.24 | -2.44 | -2.44 | -0.00 | -2.44 |
| 2026-08-26 | `AQST` | 293 | — | $5.08 | +0.00 | $5.39 | -90.83 | -90.83 | -0.00 | -90.83 |
| 2026-08-26 | `NEM` | 11 | — | $132.64 | +0.00 | $131.60 | +11.44 | +11.44 | -0.00 | +11.44 |
| 2026-08-27 | `XHG` | 509 | $4.06 | $4.06 | +0.00 | — | +0.00 | +0.00 | -5.09 | — |
| 2026-08-27 | `AVAH` | 142 | $13.62 | $13.62 | +0.00 | $13.82 | -28.40 | -28.40 | +0.71 | -27.69 |
| 2026-08-27 | `ARE` | 35 | $52.97 | $52.45 | +18.20 | $52.28 | +5.95 | +24.15 | +72.10 | +78.05 |
| 2026-08-27 | `BMO` | 11 | $172.90 | $172.85 | +0.55 | $172.13 | +7.92 | +8.47 | +23.76 | +31.68 |
| 2026-08-27 | `BYND` | 185 | $14.25 | $14.20 | -9.25 | — | +0.00 | -9.25 | +16.65 | — |
| 2026-08-27 | `USDE` | 450 | $5.98 | $6.50 | +234.00 | — | +0.00 | +234.00 | +310.50 | — |
| 2026-08-27 | `SUJA` | 278 | $9.44 | $9.41 | -8.34 | — | +0.00 | -8.34 | +5.56 | — |
| 2026-08-27 | `BE` | 6 | $218.21 | $227.10 | -53.34 | $217.83 | +55.62 | +2.28 | -78.96 | -23.34 |
| 2026-08-27 | `ABCL` | 122 | $12.24 | $12.25 | -1.22 | $12.40 | -18.30 | -19.52 | -3.66 | -21.96 |
| 2026-08-27 | `AQST` | 293 | $5.39 | $5.39 | +0.00 | $5.16 | +67.39 | +67.39 | -90.83 | -23.44 |
| 2026-08-27 | `NEM` | 11 | $131.60 | $131.02 | +6.38 | $132.29 | -13.97 | -7.59 | +17.82 | +3.85 |
| 2026-08-27 | `SLI` | 2286 | — | $2.60 | +0.00 | $2.64 | +91.44 | +91.44 | +0.00 | +91.44 |
| 2026-08-27 | `RRC` | 143 | — | $41.44 | +0.00 | $41.64 | +28.60 | +28.60 | +0.00 | +28.60 |
| 2026-08-27 | `PGY` | 259 | — | $22.93 | +0.00 | $23.26 | +85.47 | +85.47 | +0.00 | +85.47 |
| 2026-08-27 | `CRK` | 411 | — | $14.42 | +0.00 | $14.62 | +82.20 | +82.20 | +0.00 | +82.20 |
| 2026-08-28 | `AVAH` | 142 | $13.82 | $13.90 | -11.36 | — | +0.00 | -11.36 | -39.05 | — |
| 2026-08-28 | `ARE` | 35 | $52.28 | $52.49 | -7.35 | — | +0.00 | -7.35 | +70.70 | — |
| 2026-08-28 | `BMO` | 11 | $172.13 | $172.76 | -6.93 | — | +0.00 | -6.93 | +24.75 | — |
| 2026-08-28 | `BE` | 6 | $217.83 | $215.71 | +12.75 | $210.77 | +29.61 | +42.36 | -10.59 | +19.02 |
| 2026-08-28 | `ABCL` | 122 | $12.40 | $12.30 | +11.59 | $11.35 | +116.51 | +128.10 | -10.37 | +106.14 |
| 2026-08-28 | `AQST` | 293 | $5.16 | $5.11 | +14.65 | $5.02 | +26.37 | +41.02 | -8.79 | +17.58 |
| 2026-08-28 | `NEM` | 11 | $132.29 | $132.35 | -0.66 | $127.98 | +48.07 | +47.41 | +3.19 | +51.26 |
| 2026-08-28 | `SLI` | 2286 | $2.64 | $2.68 | +91.44 | — | +0.00 | +91.44 | +182.88 | — |
| 2026-08-28 | `RRC` | 143 | $41.64 | $41.74 | +14.30 | — | +0.00 | +14.30 | +42.90 | — |
| 2026-08-28 | `PGY` | 259 | $23.26 | $23.21 | -12.95 | — | +0.00 | -12.95 | +72.52 | — |
| 2026-08-28 | `CRK` | 411 | $14.62 | $14.63 | +4.11 | — | +0.00 | +4.11 | +86.31 | — |
| 2026-08-28 | `BYND` | 163 | — | $14.00 | +0.00 | $13.86 | -22.82 | -22.82 | +0.00 | -22.82 |
| 2026-08-28 | `CAPR` | 235 | — | $9.73 | +0.00 | $9.59 | -32.90 | -32.90 | +0.00 | -32.90 |
| 2026-08-28 | `MRNA` | 16 | — | $137.19 | +0.00 | $137.99 | +12.80 | +12.80 | +0.00 | +12.80 |
| 2026-08-28 | `ANF` | 15 | — | $146.07 | +0.00 | $148.42 | +35.25 | +35.25 | +0.00 | +35.25 |
| 2026-08-28 | `SIMO` | 12 | — | $252.24 | +0.00 | $245.81 | +77.16 | +77.16 | -0.00 | +77.16 |
| 2026-08-28 | `FIG` | 104 | — | $30.18 | +0.00 | $28.82 | +141.44 | +141.44 | -0.00 | +141.44 |
| 2026-08-31 | `BE` | 6 | $210.77 | $208.88 | +11.34 | — | +0.00 | +11.34 | +30.36 | — |
| 2026-08-31 | `ABCL` | 122 | $11.35 | $11.10 | +30.50 | — | +0.00 | +30.50 | +136.64 | — |
| 2026-08-31 | `AQST` | 293 | $5.02 | $4.97 | +13.18 | — | +0.00 | +13.18 | +30.77 | — |
| 2026-08-31 | `NEM` | 11 | $127.98 | $127.45 | +5.83 | — | +0.00 | +5.83 | +57.09 | — |
| 2026-08-31 | `BYND` | 163 | $13.86 | $13.81 | -8.15 | $13.30 | -83.13 | -91.28 | -30.97 | -114.10 |
| 2026-08-31 | `CAPR` | 235 | $9.59 | $9.50 | -21.15 | — | +0.00 | -21.15 | -54.05 | — |
| 2026-08-31 | `MRNA` | 16 | $137.99 | $134.10 | -62.24 | — | +0.00 | -62.24 | -49.44 | — |
| 2026-08-31 | `ANF` | 15 | $148.42 | $148.03 | -5.85 | — | +0.00 | -5.85 | +29.40 | — |
| 2026-08-31 | `SIMO` | 12 | $245.81 | $247.05 | -14.88 | $246.84 | +2.52 | -12.36 | +62.28 | +64.80 |
| 2026-08-31 | `FIG` | 104 | $28.82 | $27.60 | +126.88 | $27.49 | +11.44 | +138.32 | +268.32 | +279.76 |
| 2026-09-01 | `BYND` | 163 | $13.30 | $13.04 | -42.38 | — | +0.00 | -42.38 | -156.48 | — |
| 2026-09-01 | `SIMO` | 12 | $246.84 | $240.09 | +81.00 | $237.35 | +32.88 | +113.88 | +145.80 | +178.68 |
| 2026-09-01 | `FIG` | 104 | $27.49 | $27.06 | +44.72 | $27.20 | -14.56 | +30.16 | +324.48 | +309.92 |
| 2026-09-02 | `SIMO` | 12 | $237.35 | $235.71 | +19.68 | — | +0.00 | +19.68 | +198.36 | — |
| 2026-09-02 | `FIG` | 104 | $27.20 | $26.78 | +43.68 | — | +0.00 | +43.68 | +353.60 | — |
| 2026-09-03 | `GPRO` | 925 | — | $1.78 | +0.00 | $1.39 | -360.75 | -360.75 | +0.00 | -360.75 |
| 2026-09-03 | `REAX` | 89 | — | $18.40 | +0.00 | $18.40 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `CNH` | 120 | — | $13.71 | +0.00 | $13.84 | +15.60 | +15.60 | +0.00 | +15.60 |
| 2026-09-03 | `MMED` | 69 | — | $23.88 | +0.00 | $23.84 | -2.76 | -2.76 | +0.00 | -2.76 |
| 2026-09-03 | `SLN` | 221 | — | $14.85 | +0.00 | $14.79 | +13.26 | +13.26 | -0.00 | +13.26 |
| 2026-09-03 | `OPK` | 1924 | — | $1.71 | +0.00 | $1.61 | +192.40 | +192.40 | -0.00 | +192.40 |
| 2026-09-04 | `GPRO` | 925 | $1.39 | $1.48 | +83.25 | $1.70 | +203.50 | +286.75 | -277.50 | -74.00 |
| 2026-09-04 | `REAX` | 89 | $18.40 | $18.15 | -22.25 | — | +0.00 | -22.25 | -22.25 | — |
| 2026-09-04 | `CNH` | 120 | $13.84 | $13.89 | +6.00 | — | +0.00 | +6.00 | +21.60 | — |
| 2026-09-04 | `MMED` | 69 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.76 | — |
| 2026-09-04 | `SLN` | 221 | $14.79 | $14.63 | +35.36 | $14.59 | +8.84 | +44.20 | +48.62 | +57.46 |
| 2026-09-04 | `OPK` | 1924 | $1.61 | $1.59 | +38.48 | $1.64 | -96.20 | -57.72 | +230.88 | +134.68 |
| 2026-09-04 | `ASST` | 119 | — | $25.18 | +0.00 | $27.14 | +233.24 | +233.24 | +0.00 | +233.24 |
| 2026-09-04 | `USDE` | 382 | — | $7.87 | +0.00 | $7.93 | +22.92 | +22.92 | +0.00 | +22.92 |
| 2026-09-04 | `DFDV` | 519 | — | $5.79 | +0.00 | $5.87 | +41.52 | +41.52 | +0.00 | +41.52 |
| 2026-09-04 | `GSM` | 702 | — | $4.67 | +0.00 | $4.67 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-09-04 | `PIPR` | 42 | — | $76.55 | +0.00 | $77.04 | -20.58 | -20.58 | -0.00 | -20.58 |
| 2026-09-08 | `GPRO` | 925 | $1.70 | $1.56 | -124.88 | — | +0.00 | -124.88 | -198.88 | — |
| 2026-09-08 | `SLN` | 221 | $14.59 | $14.24 | +77.35 | $13.69 | +121.55 | +198.90 | +134.81 | +256.36 |
| 2026-09-08 | `OPK` | 1924 | $1.64 | $1.63 | +19.24 | $1.59 | +76.96 | +96.20 | +153.92 | +230.88 |
| 2026-09-08 | `ASST` | 119 | $27.14 | $26.44 | -83.30 | — | +0.00 | -83.30 | +149.94 | — |
| 2026-09-08 | `USDE` | 382 | $7.93 | $7.76 | -64.94 | — | +0.00 | -64.94 | -42.02 | — |
| 2026-09-08 | `DFDV` | 519 | $5.87 | $5.81 | -31.14 | — | +0.00 | -31.14 | +10.38 | — |
| 2026-09-08 | `GSM` | 702 | $4.67 | $4.75 | -56.16 | $4.52 | +161.46 | +105.30 | -56.16 | +105.30 |
| 2026-09-08 | `PIPR` | 42 | $77.04 | $76.64 | +16.80 | $77.34 | -29.40 | -12.60 | -3.78 | -33.18 |
| 2026-09-09 | `SLN` | 221 | $13.69 | $13.60 | +19.89 | — | +0.00 | +19.89 | +276.25 | — |
| 2026-09-09 | `OPK` | 1924 | $1.59 | $1.58 | +19.24 | — | +0.00 | +19.24 | +250.12 | — |
| 2026-09-09 | `GSM` | 702 | $4.52 | $4.52 | +0.00 | $4.49 | +21.06 | +21.06 | +105.30 | +126.36 |
| 2026-09-09 | `PIPR` | 42 | $77.34 | $77.24 | +4.20 | $76.96 | +11.76 | +15.96 | -28.98 | -17.22 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +386.21 | IREN, TNDM, TPG, INO | — | $0.54 | $10,345.37 | IREN×54, TNDM×107, TPG×49, INO×3085 |
| 2026-08-14 | +5.50 | $0.54 | IREN×54, TNDM×107, TPG×49, INO×3085 | $10,412.10 | +66.73 | -105.46 | QMCO, ARX, ZENA, AIRO, EU, LUNR, OWL | IREN, TNDM, TPG, INO | $10,332.26 | $10,223.22 | QMCO×52, ARX×66, ZENA×589, AIRO×116, EU×1462, LUNR×90, OWL×135 |
| 2026-08-17 | +2.25 | $10,332.26 | QMCO×52, ARX×66, ZENA×589, AIRO×116, EU×1462, LUNR×90, OWL×135 | $10,025.50 | -197.72 | +149.33 | XHG, CAPR, STDN, HTFL, VERI, ZNTL, APMD, HIVE | QMCO, ARX, ZENA, AIRO | $12,584.44 | $10,119.93 | EU×1462, LUNR×90, OWL×135, XHG×454, CAPR×277, STDN×139, HTFL×46, VERI×1086, ZNTL×351, APMD×39, HIVE×415 |
| 2026-08-18 | -6.20 | $12,584.44 | EU×1462, LUNR×90, OWL×135, XHG×454, CAPR×277, STDN×139, HTFL×46, VERI×1086, ZNTL×351, APMD×39, HIVE×415 | $10,295.83 | +175.90 | +164.19 | — | XHG, STDN, HTFL | $18,121.74 | $10,449.47 | EU×1462, LUNR×90, OWL×135, CAPR×277, VERI×1086, ZNTL×351, APMD×39, HIVE×415 |
| 2026-08-19 | -7.20 | $18,121.74 | EU×1462, LUNR×90, OWL×135, CAPR×277, VERI×1086, ZNTL×351, APMD×39, HIVE×415 | $10,442.05 | -7.42 | +44.56 | — | EU, OWL, CAPR | $16,937.89 | $10,461.73 | LUNR×90, VERI×1086, ZNTL×351, APMD×39, HIVE×415 |
| 2026-08-20 | +1.12 | $16,937.89 | LUNR×90, VERI×1086, ZNTL×351, APMD×39, HIVE×415 | $10,383.93 | -77.80 | +17.49 | ABCL, MRNA, CYPH, AZI, AEM, WYFI, TOYO, TEAM, AAP, WMT, AQST | LUNR, VERI, ZNTL, APMD, HIVE | $10,079.51 | $10,327.03 | ABCL×109, MRNA×8, CYPH×1125, AZI×944, AEM×3, WYFI×34, TOYO×166, TEAM×4, AAP×15, WMT×6, AQST×159 |
| 2026-08-21 | +3.25 | $10,079.51 | ABCL×109, MRNA×8, CYPH×1125, AZI×944, AEM×3, WYFI×34, TOYO×166, TEAM×4, AAP×15, WMT×6, AQST×159 | $10,433.59 | +106.56 | -30.54 | XHG, CAPR, QTRX, AUGO, SSRM, ARIS, NOG | ABCL, AZI | $11,433.98 | $10,360.30 | MRNA×8, CYPH×1125, AEM×3, WYFI×34, TOYO×166, TEAM×4, AAP×15, WMT×6, AQST×159, XHG×707, CAPR×466, QTRX×334, AUGO×11, SSRM×27, ARIS×49, NOG×38 |
| 2026-08-24 | -5.17 | $11,433.98 | MRNA×8, CYPH×1125, AEM×3, WYFI×34, TOYO×166, TEAM×4, AAP×15, WMT×6, AQST×159, XHG×707, CAPR×466, QTRX×334, AUGO×11, SSRM×27, ARIS×49, NOG×38 | $11,582.04 | +1,221.74 | +40.98 | — | MRNA, CYPH, XHG, CAPR | $21,398.42 | $11,590.89 | AEM×3, WYFI×34, TOYO×166, TEAM×4, AAP×15, WMT×6, AQST×159, QTRX×334, AUGO×11, SSRM×27, ARIS×49, NOG×38 |
| 2026-08-25 | +1.80 | $21,398.42 | AEM×3, WYFI×34, TOYO×166, TEAM×4, AAP×15, WMT×6, AQST×159, QTRX×334, AUGO×11, SSRM×27, ARIS×49, NOG×38 | $11,698.82 | +107.93 | +643.04 | REAX, CYPH, XHG, ASST, AVAH, ARE, BMO | AEM, WYFI, TOYO, TEAM, AAP, WMT, AQST | $14,057.78 | $12,291.73 | QTRX×334, AUGO×11, SSRM×27, ARIS×49, NOG×38, REAX×85, CYPH×1328, XHG×509, ASST×108, AVAH×142, ARE×35, BMO×11 |
| 2026-08-26 | +2.02 | $14,057.78 | QTRX×334, AUGO×11, SSRM×27, ARIS×49, NOG×38, REAX×85, CYPH×1328, XHG×509, ASST×108, AVAH×142, ARE×35, BMO×11 | $11,985.38 | -306.35 | +136.88 | BYND, USDE, SUJA, BE, ABCL, AQST, NEM | QTRX, AUGO, SSRM, ARIS, NOG, REAX, CYPH, ASST | $13,566.42 | $12,065.23 | XHG×509, AVAH×142, ARE×35, BMO×11, BYND×185, USDE×450, SUJA×278, BE×6, ABCL×122, AQST×293, NEM×11 |
| 2026-08-27 | — | $13,566.42 | XHG×509, AVAH×142, ARE×35, BMO×11, BYND×185, USDE×450, SUJA×278, BE×6, ABCL×122, AQST×293, NEM×11 | $12,252.21 | +186.98 | +363.92 | SLI, RRC, PGY, CRK | XHG, BYND, USDE, SUJA | $6.56 | $12,556.76 | AVAH×142, ARE×35, BMO×11, BE×6, ABCL×122, AQST×293, NEM×11, SLI×2286, RRC×143, PGY×259, CRK×411 |
| 2026-08-28 | +0.75 | $6.56 | AVAH×142, ARE×35, BMO×11, BE×6, ABCL×122, AQST×293, NEM×11, SLI×2286, RRC×143, PGY×259, CRK×411 | $12,666.35 | +109.59 | +431.49 | BYND, CAPR, MRNA, ANF, SIMO, FIG | AVAH, ARE, BMO, SLI, RRC, PGY, CRK | $15,563.88 | $13,035.89 | BE×6, ABCL×122, AQST×293, NEM×11, BYND×163, CAPR×235, MRNA×16, ANF×15, SIMO×12, FIG×104 |
| 2026-08-31 | -5.85 | $15,563.88 | BE×6, ABCL×122, AQST×293, NEM×11, BYND×163, CAPR×235, MRNA×16, ANF×15, SIMO×12, FIG×104 | $13,111.35 | +75.46 | -69.17 | — | BE, ABCL, AQST, NEM, CAPR, MRNA, ANF | $16,677.94 | $13,024.80 | BYND×163, SIMO×12, FIG×104 |
| 2026-09-01 | -6.30 | $16,677.94 | BYND×163, SIMO×12, FIG×104 | $13,108.14 | +83.34 | +18.32 | — | BYND | $18,800.94 | $13,123.94 | SIMO×12, FIG×104 |
| 2026-09-02 | -3.83 | $18,800.94 | SIMO×12, FIG×104 | $13,187.30 | +63.36 | +0.00 | — | SIMO, FIG | $13,182.97 | $13,182.97 | — |
| 2026-09-03 | -0.90 | $13,182.97 | — | $13,182.97 | -0.00 | -142.25 | GPRO, REAX, CNH, MMED, SLN, OPK | — | $13,130.84 | $12,993.72 | GPRO×925, REAX×89, CNH×120, MMED×69, SLN×221, OPK×1924 |
| 2026-09-04 | +2.25 | $13,130.84 | GPRO×925, REAX×89, CNH×120, MMED×69, SLN×221, OPK×1924 | $13,134.56 | +140.84 | +393.24 | ASST, USDE, DFDV, GSM, PIPR | REAX, CNH, MMED | $15,511.23 | $13,495.41 | GPRO×925, SLN×221, OPK×1924, ASST×119, USDE×382, DFDV×519, GSM×702, PIPR×42 |
| 2026-09-08 | -11.47 | $15,511.23 | GPRO×925, SLN×221, OPK×1924, ASST×119, USDE×382, DFDV×519, GSM×702, PIPR×42 | $13,248.39 | -247.02 | +330.57 | — | GPRO, ASST, USDE, DFDV | $26,058.62 | $13,552.65 | SLN×221, OPK×1924, GSM×702, PIPR×42 |
| 2026-09-09 | -13.95 | $26,058.62 | SLN×221, OPK×1924, GSM×702, PIPR×42 | $13,595.98 | +43.33 | +32.82 | — | SLN, OPK | $19,985.43 | $13,601.13 | GSM×702, PIPR×42 |

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
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1462 | $1.18 | $19.16 | — | $6,897.95 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $1725.44; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 90 | $19.17 | $2.34 | — | $8,620.91 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $1725.44; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 135 | $12.70 | $2.48 | — | $10,332.26 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $1725.44; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,332.26 | ▼ close $10,223.22 vs 09:30 $10,412.10 (session -105.46) | 16:00 close · cash $10,332.26 · equity $10,223.22 vs 09:30 $10,412.10 (-188.88; session marks -105.46) · 7 name(s) marked open→close (per-name table). QMCO×52 09:30 $24.68 → close $26.11 +74.36; ARX×66 09:30 $19.57 → close $19.58 +0.66; ZENA×589 09:30 $2.20 → close $2.14 -35.34; AIRO×116 09:30 $11.12 → close $9.57 -179.80; EU×1462 09:30 $1.18 → close $1.21 -43.86; LUNR×90 09:30 $19.17 → close $19.01 +14.40; OWL×135 09:30 $12.70 → close $12.22 +64.12 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,332.26 | ▼ 09:30 equity $10,025.50 vs yday $10,223.22 (-197.72) | 09:30 open · cash $10,332.26 (unchanged overnight, no fees) · equity $10,025.50 vs prior close $10,223.22 (-197.72) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $11,621.25 | ▲ +3.49 after sell → book $10,023.34; vs 09:30 mark -2.16 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 66 | $19.57 | $2.21 | $-4.40 | $12,910.66 | ▼ -4.40 after sell → book $10,021.13; vs 09:30 mark -2.21 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 589 | $2.08 | $7.71 | $-83.04 | $14,131.02 | ▼ -83.04 after sell → book $10,013.42; vs 09:30 mark -7.71 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 116 | $9.57 | $2.37 | $-184.51 | $15,238.77 | ▼ -184.51 after sell → book $10,011.05; vs 09:30 mark -2.37 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 454 | $4.19 | $5.86 | — | $13,330.66 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $1904.85; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 277 | $6.87 | $3.57 | — | $11,424.09 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $1904.85; owner union_hot_n4_h1 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 139 | $13.64 | $2.41 | — | $9,525.73 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $1904.85; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 46 | $41.23 | $2.13 | — | $7,627.02 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $1904.85; owner union_hot_n4_h1 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 1086 | $1.15 | $14.23 | — | $8,861.69 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $1249.64; owner short_news_r_h3 | join🟡 sector🟢 gen🟢 news🔴 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 351 | $3.56 | $4.63 | — | $10,106.62 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $1249.64; owner short_news_r_h3 | join🟡 sector🔴 gen🟢 news🔴 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 39 | $31.70 | $2.16 | — | $11,340.75 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; combo leftover $1249.64; owner short_news_r_h3 | join🟡 sector🔴 gen🟢 news🔴 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 415 | $3.01 | $5.47 | — | $12,584.44 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $1249.64; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,584.44 | ▲ close $10,119.93 vs 09:30 $10,025.50 (session +149.33) | 16:00 close · cash $12,584.44 · equity $10,119.93 vs 09:30 $10,025.50 (+94.43; session marks +149.33) · 11 name(s) marked open→close (per-name table). EU×1462 09:30 $1.21 → close $1.13 +116.96; LUNR×90 09:30 $20.25 → close $20.38 -11.70; OWL×135 09:30 $12.12 → close $11.66 +62.10; XHG×454 09:30 $4.19 → close $3.91 -127.12; CAPR×277 09:30 $6.87 → close $7.45 +160.66; STDN×139 09:30 $13.64 → close $13.31 -45.87; HTFL×46 09:30 $41.23 → close $41.94 +32.66; VERI×1086 09:30 $1.15 → close $1.08 +70.59; ZNTL×351 09:30 $3.56 → close $3.71 -50.90; APMD×39 09:30 $31.70 → close $32.55 -33.15; HIVE×415 09:30 $3.01 → close $3.07 -24.90 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,584.44 | ▲ 09:30 equity $10,295.83 vs yday $10,119.93 (+175.90) | 09:30 open · cash $12,584.44 (unchanged overnight, no fees) · equity $10,295.83 vs prior close $10,119.93 (+175.90) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 454 | $3.94 | $5.95 | $-125.30 | $14,367.25 | ▼ -125.30 after sell → book $10,289.88; vs 09:30 mark -5.95 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 139 | $13.31 | $2.44 | $-50.72 | $16,214.90 | ▼ -50.72 after sell → book $10,287.44; vs 09:30 mark -2.44 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 46 | $41.50 | $2.15 | $+8.14 | $18,121.74 | ▲ +8.14 after sell → book $10,285.28; vs 09:30 mark -2.16 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,121.74 | ▲ close $10,449.47 vs 09:30 $10,295.83 (session +164.19) | 16:00 close · cash $18,121.74 · equity $10,449.47 vs 09:30 $10,295.83 (+153.64; session marks +164.19) · 8 name(s) marked open→close (per-name table). EU×1462 09:30 $1.13 → close $1.07 +87.72; LUNR×90 09:30 $19.31 → close $19.31 -0.00; OWL×135 09:30 $11.54 → close $11.59 -6.75; CAPR×277 09:30 $7.50 → close $7.08 -116.34; VERI×1086 09:30 $1.05 → close $0.99 +59.73; ZNTL×351 09:30 $3.75 → close $3.68 +24.57; APMD×39 09:30 $32.85 → close $31.81 +40.56; HIVE×415 09:30 $2.96 → close $2.78 +74.70 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,121.74 | ▼ 09:30 equity $10,442.05 vs yday $10,449.47 (-7.42) | 09:30 open · cash $18,121.74 (unchanged overnight, no fees) · equity $10,442.05 vs prior close $10,449.47 (-7.42) | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1462 | $1.07 | $18.86 | $+122.80 | $16,538.54 | ▲ +122.80 after sell → book $10,423.19; vs 09:30 mark -18.86 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 135 | $11.75 | $2.40 | $+122.70 | $14,949.90 | ▲ +122.70 after sell → book $10,420.80; vs 09:30 mark -2.39 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 277 | $7.19 | $3.64 | $+81.43 | $16,937.89 | ▲ +81.43 after sell → book $10,417.16; vs 09:30 mark -3.64 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,937.89 | ▲ close $10,461.73 vs 09:30 $10,442.05 (session +44.56) | 16:00 close · cash $16,937.89 · equity $10,461.73 vs 09:30 $10,442.05 (+19.68; session marks +44.56) · 5 name(s) marked open→close (per-name table). LUNR×90 09:30 $18.98 → close $18.52 +41.40; VERI×1086 09:30 $1.00 → close $0.97 +36.92; ZNTL×351 09:30 $3.76 → close $3.82 -21.06; APMD×39 09:30 $32.13 → close $32.03 +3.90; HIVE×415 09:30 $2.78 → close $2.82 -16.60 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,937.89 | ▼ 09:30 equity $10,383.93 vs yday $10,461.73 (-77.80) | 09:30 open · cash $16,937.89 (unchanged overnight, no fees) · equity $10,383.93 vs prior close $10,461.73 (-77.80) | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 90 | $18.13 | $2.26 | $+89.00 | $15,303.93 | ▲ +89.00 after sell → book $10,381.67; vs 09:30 mark -2.26 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 1086 | $0.96 | $13.72 | $+175.13 | $14,244.40 | ▲ +175.13 after sell → book $10,367.95; vs 09:30 mark -13.72 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 351 | $4.01 | $4.53 | $-168.86 | $12,830.61 | ▼ -168.86 after sell → book $10,363.43; vs 09:30 mark -4.52 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 39 | $31.87 | $2.11 | $-10.90 | $11,585.57 | ▼ -10.90 after sell → book $10,361.32; vs 09:30 mark -2.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 415 | $2.95 | $5.35 | $+14.08 | $10,355.97 | ▲ +14.08 after sell → book $10,355.97; vs 09:30 mark -5.35 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 109 | $11.81 | $2.32 | — | $9,065.81 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1294.50; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $7,862.68 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $1294.50; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1125 | $1.15 | $14.51 | — | $6,554.42 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $1294.50; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 944 | $1.37 | $12.18 | — | $5,248.96 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $1294.50; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $5,860.27 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $737.50; owner short_news_r_h3 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 34 | $21.40 | $2.13 | — | $6,585.74 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $737.50; owner short_news_r_h3 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 166 | $4.43 | $2.55 | — | $7,318.58 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; combo leftover $737.50; owner short_news_r_h3 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 4 | $173.90 | $2.04 | — | $8,012.14 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $737.50; owner short_news_r_h3 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 15 | $46.85 | $2.07 | — | $8,712.81 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; combo leftover $737.50; owner short_news_r_h3 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 6 | $106.38 | $2.05 | — | $9,349.05 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $737.50; owner short_news_r_h3 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 159 | $4.61 | $2.52 | — | $10,079.51 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $737.50; owner short_news_r_h3 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,079.51 | ▲ close $10,327.03 vs 09:30 $10,383.93 (session +17.49) | 16:00 close · cash $10,079.51 · equity $10,327.03 vs 09:30 $10,383.93 (-56.90; session marks +17.49) · 11 name(s) marked open→close (per-name table). ABCL×109 09:30 $11.81 → close $11.57 -26.70; MRNA×8 09:30 $150.14 → close $133.32 -134.56; CYPH×1125 09:30 $1.15 → close $1.19 +45.00; AZI×944 09:30 $1.37 → close $1.44 +66.08; AEM×3 09:30 $204.45 → close $212.04 -22.77; WYFI×34 09:30 $21.40 → close $21.16 +8.16; TOYO×166 09:30 $4.43 → close $4.51 -14.11; TEAM×4 09:30 $173.90 → close $174.91 -4.04; AAP×15 09:30 $46.85 → close $42.39 +66.90; WMT×6 09:30 $106.38 → close $103.84 +15.24; AQST×159 09:30 $4.61 → close $4.50 +18.29 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,079.51 | ▲ 09:30 equity $10,433.59 vs yday $10,327.03 (+106.56) | 09:30 open · cash $10,079.51 (unchanged overnight, no fees) · equity $10,433.59 vs prior close $10,327.03 (+106.56) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 109 | $11.57 | $2.35 | $-31.37 | $11,338.30 | ▼ -31.37 after sell → book $10,431.25; vs 09:30 mark -2.34 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 944 | $1.46 | $12.35 | $+60.44 | $12,704.19 | ▲ +60.44 after sell → book $10,418.90; vs 09:30 mark -12.35 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 707 | $4.49 | $9.12 | — | $9,520.64 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $3176.05; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 466 | $6.81 | $6.01 | — | $6,341.17 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $3176.05; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 334 | $3.11 | $4.40 | — | $7,375.51 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $1040.38; owner short_news_r_h3 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 11 | $89.10 | $2.07 | — | $8,353.54 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $1040.38; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 27 | $38.40 | $2.12 | — | $9,388.22 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $1040.38; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 49 | $20.90 | $2.19 | — | $10,410.13 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1040.38; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 38 | $27.00 | $2.15 | — | $11,433.98 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $1040.38; owner short_news_r_h3 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,433.98 | ▼ close $10,360.30 vs 09:30 $10,433.59 (session -30.54) | 16:00 close · cash $11,433.98 · equity $10,360.30 vs 09:30 $10,433.59 (-73.29; session marks -30.54) · 16 name(s) marked open→close (per-name table). MRNA×8 09:30 $133.11 → close $145.13 +96.16; CYPH×1125 09:30 $1.32 → close $1.42 +112.50; AEM×3 09:30 $216.30 → close $216.06 +0.72; WYFI×34 09:30 $21.54 → close $20.72 +27.88; TOYO×166 09:30 $4.68 → close $4.82 -23.24; TEAM×4 09:30 $174.22 → close $171.81 +9.64; AAP×15 09:30 $42.41 → close $42.58 -2.55; WMT×6 09:30 $103.69 → close $103.70 -0.06; AQST×159 09:30 $4.54 → close $4.66 -19.08; XHG×707 09:30 $4.49 → close $4.41 -56.56; CAPR×466 09:30 $6.81 → close $6.29 -242.32; QTRX×334 09:30 $3.11 → close $2.99 +40.08; AUGO×11 09:30 $89.10 → close $87.26 +20.24; SSRM×27 09:30 $38.40 → close $37.77 +17.01; ARIS×49 09:30 $20.90 → close $20.86 +1.96; NOG×38 09:30 $27.00 → close $27.34 -12.92 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,433.98 | ▲ 09:30 equity $11,582.04 vs yday $10,360.30 (+1,221.74) | 09:30 open · cash $11,433.98 (unchanged overnight, no fees) · equity $11,582.04 vs prior close $10,360.30 (+1221.74) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 8 | $142.70 | $2.03 | $-63.57 | $12,573.55 | ▼ -63.57 after sell → book $11,580.01; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1125 | $1.83 | $14.72 | $+735.77 | $14,617.58 | ▲ +735.77 after sell → book $11,565.29; vs 09:30 mark -14.72 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 707 | $4.32 | $9.26 | $-138.57 | $17,662.56 | ▼ -138.57 after sell → book $11,556.03; vs 09:30 mark -9.26 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 466 | $8.03 | $6.12 | $+556.39 | $21,398.42 | ▲ +556.39 after sell → book $11,549.91; vs 09:30 mark -6.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,398.42 | ▲ close $11,590.89 vs 09:30 $11,582.04 (session +40.98) | 16:00 close · cash $21,398.42 · equity $11,590.89 vs 09:30 $11,582.04 (+8.85; session marks +40.98) · 12 name(s) marked open→close (per-name table). AEM×3 09:30 $217.03 → close $217.89 -2.58; WYFI×34 09:30 $20.01 → close $20.78 -26.18; TOYO×166 09:30 $4.58 → close $4.38 +33.20; TEAM×4 09:30 $169.30 → close $171.33 -8.12; AAP×15 09:30 $43.05 → close $43.63 -8.70; WMT×6 09:30 $104.14 → close $106.49 -14.10; AQST×159 09:30 $4.67 → close $4.80 -20.67; QTRX×334 09:30 $2.99 → close $2.80 +63.46; AUGO×11 09:30 $88.60 → close $87.37 +13.53; SSRM×27 09:30 $38.32 → close $38.61 -7.83; ARIS×49 09:30 $20.98 → close $20.81 +8.33; NOG×38 09:30 $27.12 → close $26.84 +10.64 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,398.42 | ▲ 09:30 equity $11,698.82 vs yday $11,590.89 (+107.93) | 09:30 open · cash $21,398.42 (unchanged overnight, no fees) · equity $11,698.82 vs prior close $11,590.89 (+107.93) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $20,760.42 | ▼ -26.68 after sell → book $11,696.82; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 34 | $20.90 | $2.09 | $+12.78 | $20,047.73 | ▲ +12.78 after sell → book $11,694.73; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 166 | $4.42 | $2.49 | $-3.37 | $19,311.52 | ▼ -3.37 after sell → book $11,692.24; vs 09:30 mark -2.49 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 4 | $170.64 | $2.00 | $+9.00 | $18,626.96 | ▲ +9.00 after sell → book $11,690.24; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 15 | $43.63 | $2.04 | $+44.19 | $17,970.47 | ▲ +44.19 after sell → book $11,688.20; vs 09:30 mark -2.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 6 | $105.58 | $2.01 | $+0.75 | $17,334.99 | ▲ +0.75 after sell → book $11,686.20; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 159 | $4.77 | $2.47 | $-30.43 | $16,574.09 | ▼ -30.43 after sell → book $11,683.73; vs 09:30 mark -2.47 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 85 | $24.11 | $2.25 | — | $14,522.49 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $2071.76; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1328 | $1.56 | $17.13 | — | $12,433.68 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $2071.76; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 509 | $4.07 | $6.57 | — | $10,355.49 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $2071.76; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 108 | $19.04 | $2.31 | — | $8,296.85 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $2071.76; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 142 | $13.62 | $2.51 | — | $10,229.09 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1942.58; owner short_news_r_h3 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 35 | $54.51 | $2.17 | — | $12,134.77 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $1942.58; owner short_news_r_h3 | join🔴 sector🟡 gen🟡 news🔴 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 11 | $175.01 | $2.10 | — | $14,057.78 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; combo leftover $1942.58; owner short_news_r_h3 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,057.78 | ▲ close $12,291.73 vs 09:30 $11,698.82 (session +643.04) | 16:00 close · cash $14,057.78 · equity $12,291.73 vs 09:30 $11,698.82 (+592.91; session marks +643.04) · 12 name(s) marked open→close (per-name table). QTRX×334 09:30 $2.80 → close $2.79 +3.34; AUGO×11 09:30 $85.78 → close $90.47 -51.59; SSRM×27 09:30 $37.75 → close $39.21 -39.42; ARIS×49 09:30 $20.45 → close $21.18 -35.77; NOG×38 09:30 $26.06 → close $26.42 -13.68; REAX×85 09:30 $24.11 → close $28.43 +367.20; CYPH×1328 09:30 $1.56 → close $1.64 +106.24; XHG×509 09:30 $4.07 → close $4.02 -25.45; ASST×108 09:30 $19.04 → close $21.39 +253.80; AVAH×142 09:30 $13.62 → close $13.59 +4.97; ARE×35 09:30 $54.51 → close $52.90 +56.35; BMO×11 09:30 $175.01 → close $173.46 +17.05 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,057.78 | ▼ 09:30 equity $11,985.38 vs yday $12,291.73 (-306.35) | 09:30 open · cash $14,057.78 (unchanged overnight, no fees) · equity $11,985.38 vs prior close $12,291.73 (-306.35) | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 334 | $2.83 | $4.31 | $+84.81 | $13,108.25 | ▲ +84.81 after sell → book $11,981.07; vs 09:30 mark -4.31 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 11 | $88.24 | $2.02 | $+5.37 | $12,135.59 | ▲ +5.37 after sell → book $11,979.05; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 27 | $38.41 | $2.07 | $-4.46 | $11,096.45 | ▼ -4.46 after sell → book $11,976.98; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 49 | $20.50 | $2.14 | $+15.28 | $10,089.81 | ▲ +15.28 after sell → book $11,974.84; vs 09:30 mark -2.14 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 38 | $26.00 | $2.10 | $+33.74 | $9,099.71 | ▲ +33.74 after sell → book $11,972.74; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 85 | $26.61 | $2.28 | $+207.98 | $11,359.28 | ▲ +207.98 after sell → book $11,970.46; vs 09:30 mark -2.28 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 1328 | $1.60 | $17.37 | $+18.62 | $13,466.71 | ▲ +18.62 after sell → book $11,953.09; vs 09:30 mark -17.37 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 108 | $20.72 | $2.35 | $+176.78 | $15,702.12 | ▲ +176.78 after sell → book $11,950.74; vs 09:30 mark -2.35 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 185 | $14.11 | $2.54 | — | $13,089.23 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $2617.02; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 450 | $5.81 | $5.80 | — | $10,468.92 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $2617.02; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SUJA` | 278 | $9.39 | $3.59 | — | $7,854.92 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+39.0; combo leftover $2617.02; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 6 | $213.94 | $2.06 | — | $9,136.49 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $1492.35; owner short_news_r_h3 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 122 | $12.22 | $2.43 | — | $10,624.90 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $1492.35; owner short_news_r_h3 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 293 | $5.08 | $3.88 | — | $12,109.46 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $1492.35; owner short_news_r_h3 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 11 | $132.64 | $2.08 | — | $13,566.42 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $1492.35; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,566.42 | ▲ close $12,065.23 vs 09:30 $11,985.38 (session +136.88) | 16:00 close · cash $13,566.42 · equity $12,065.23 vs 09:30 $11,985.38 (+79.85; session marks +136.88) · 11 name(s) marked open→close (per-name table). XHG×509 09:30 $3.81 → close $4.06 +127.25; AVAH×142 09:30 $13.65 → close $13.62 +4.26; ARE×35 09:30 $52.77 → close $52.97 -7.00; BMO×11 09:30 $173.22 → close $172.90 +3.52; BYND×185 09:30 $14.11 → close $14.25 +25.90; USDE×450 09:30 $5.81 → close $5.98 +76.50; SUJA×278 09:30 $9.39 → close $9.44 +13.90; BE×6 09:30 $213.94 → close $218.21 -25.62; ABCL×122 09:30 $12.22 → close $12.24 -2.44; AQST×293 09:30 $5.08 → close $5.39 -90.83; NEM×11 09:30 $132.64 → close $131.60 +11.44 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,566.42 | ▲ 09:30 equity $12,252.21 vs yday $12,065.23 (+186.98) | 09:30 open · cash $13,566.42 (unchanged overnight, no fees) · equity $12,252.21 vs prior close $12,065.23 (+186.98) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 509 | $4.06 | $6.67 | $-18.32 | $15,626.29 | ▼ -18.32 after sell → book $12,245.54; vs 09:30 mark -6.67 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 185 | $14.20 | $2.60 | $+11.51 | $18,250.69 | ▲ +11.51 after sell → book $12,242.94; vs 09:30 mark -2.60 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 450 | $6.50 | $5.90 | $+298.79 | $21,169.79 | ▲ +298.79 after sell → book $12,237.04; vs 09:30 mark -5.90 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 278 | $9.41 | $3.65 | $-1.68 | $23,782.12 | ▼ -1.68 after sell → book $12,233.39; vs 09:30 mark -3.65 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 2286 | $2.60 | $29.49 | — | $17,809.03 | — | top 4 by hot; rank hot_score; list flatten; ret5=+13.0; combo leftover $5945.53; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 143 | $41.44 | $2.42 | — | $11,880.69 | — | top 4 by hot; rank hot_score; list flatten; ret5=+3.1; combo leftover $5945.53; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 259 | $22.93 | $3.34 | — | $5,938.48 | — | top 4 by hot; rank hot_score; list mover_buy; 🔵; ret5=+9.5; combo leftover $5945.53; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 411 | $14.42 | $5.30 | — | $6.56 | — | top 4 by hot; rank hot_score; list flatten; ret5=+7.1; combo leftover $5945.53; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.56 | ▲ close $12,556.76 vs 09:30 $12,252.21 (session +363.92) | 16:00 close · cash $6.56 · equity $12,556.76 vs 09:30 $12,252.21 (+304.55; session marks +363.92) · 11 name(s) marked open→close (per-name table). AVAH×142 09:30 $13.62 → close $13.82 -28.40; ARE×35 09:30 $52.45 → close $52.28 +5.95; BMO×11 09:30 $172.85 → close $172.13 +7.92; BE×6 09:30 $227.10 → close $217.83 +55.62; ABCL×122 09:30 $12.25 → close $12.40 -18.30; AQST×293 09:30 $5.39 → close $5.16 +67.39; NEM×11 09:30 $131.02 → close $132.29 -13.97; SLI×2286 09:30 $2.60 → close $2.64 +91.44; RRC×143 09:30 $41.44 → close $41.64 +28.60; PGY×259 09:30 $22.93 → close $23.26 +85.47; CRK×411 09:30 $14.42 → close $14.62 +82.20 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.56 | ▲ 09:30 equity $12,666.35 vs yday $12,556.76 (+109.59) | 09:30 open · cash $6.56 (unchanged overnight, no fees) · equity $12,666.35 vs prior close $12,556.76 (+109.59) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 142 | $13.90 | $2.42 | $-43.97 | $-1,969.66 | ▼ -43.97 after sell → book $12,663.93; vs 09:30 mark -2.42 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 35 | $52.49 | $2.10 | $+66.43 | $-3,808.90 | ▲ +66.43 after sell → book $12,661.84; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 11 | $172.76 | $2.02 | $+20.63 | $-5,711.29 | ▲ +20.63 after sell → book $12,659.81; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 2286 | $2.68 | $29.92 | $+123.47 | $385.27 | ▲ +123.47 after sell → book $12,629.89; vs 09:30 mark -29.92 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 143 | $41.74 | $2.49 | $+37.99 | $6,351.60 | ▲ +37.99 after sell → book $12,627.40; vs 09:30 mark -2.49 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 259 | $23.21 | $3.43 | $+65.75 | $12,359.56 | ▲ +65.75 after sell → book $12,623.97; vs 09:30 mark -3.43 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 411 | $14.63 | $5.42 | $+75.59 | $18,367.07 | ▲ +75.59 after sell → book $12,618.55; vs 09:30 mark -5.42 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 163 | $14.00 | $2.48 | — | $16,082.59 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $2295.88; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 235 | $9.73 | $3.03 | — | $13,793.01 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+47.1; combo leftover $2295.88; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 16 | $137.19 | $2.04 | — | $11,595.93 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+7.1; combo leftover $2295.88; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 15 | $146.07 | $2.04 | — | $9,402.85 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $2295.88; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 12 | $252.24 | $2.14 | — | $12,427.59 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $3152.24; owner short_news_r_h3 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 104 | $30.18 | $2.43 | — | $15,563.88 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $3152.24; owner short_news_r_h3 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,563.88 | ▲ close $13,035.89 vs 09:30 $12,666.35 (session +431.49) | 16:00 close · cash $15,563.88 · equity $13,035.89 vs 09:30 $12,666.35 (+369.54; session marks +431.49) · 10 name(s) marked open→close (per-name table). BE×6 09:30 $215.71 → close $210.77 +29.61; ABCL×122 09:30 $12.30 → close $11.35 +116.51; AQST×293 09:30 $5.11 → close $5.02 +26.37; NEM×11 09:30 $132.35 → close $127.98 +48.07; BYND×163 09:30 $14.00 → close $13.86 -22.82; CAPR×235 09:30 $9.73 → close $9.59 -32.90; MRNA×16 09:30 $137.19 → close $137.99 +12.80; ANF×15 09:30 $146.07 → close $148.42 +35.25; SIMO×12 09:30 $252.24 → close $245.81 +77.16; FIG×104 09:30 $30.18 → close $28.82 +141.44 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,563.88 | ▲ 09:30 equity $13,111.35 vs yday $13,035.89 (+75.46) | 09:30 open · cash $15,563.88 (unchanged overnight, no fees) · equity $13,111.35 vs prior close $13,035.89 (+75.46) | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 6 | $208.88 | $2.01 | $+26.29 | $14,308.59 | ▲ +26.29 after sell → book $13,109.34; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 122 | $11.10 | $2.36 | $+131.85 | $12,952.03 | ▲ +131.85 after sell → book $13,106.99; vs 09:30 mark -2.35 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 293 | $4.97 | $3.78 | $+23.10 | $11,490.58 | ▲ +23.10 after sell → book $13,103.21; vs 09:30 mark -3.78 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 11 | $127.45 | $2.02 | $+52.98 | $10,086.60 | ▲ +52.98 after sell → book $13,101.18; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 235 | $9.50 | $3.09 | $-60.17 | $12,316.02 | ▼ -60.17 after sell → book $13,098.10; vs 09:30 mark -3.08 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 16 | $134.10 | $2.07 | $-53.54 | $14,459.55 | ▼ -53.54 after sell → book $13,096.03; vs 09:30 mark -2.07 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 15 | $148.03 | $2.06 | $+25.30 | $16,677.94 | ▲ +25.30 after sell → book $13,093.97; vs 09:30 mark -2.06 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,677.94 | ▼ close $13,024.80 vs 09:30 $13,111.35 (session -69.17) | 16:00 close · cash $16,677.94 · equity $13,024.80 vs 09:30 $13,111.35 (-86.55; session marks -69.17) · 3 name(s) marked open→close (per-name table). BYND×163 09:30 $13.81 → close $13.30 -83.13; SIMO×12 09:30 $247.05 → close $246.84 +2.52; FIG×104 09:30 $27.60 → close $27.49 +11.44 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,677.94 | ▲ 09:30 equity $13,108.14 vs yday $13,024.80 (+83.34) | 09:30 open · cash $16,677.94 (unchanged overnight, no fees) · equity $13,108.14 vs prior close $13,024.80 (+83.34) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 163 | $13.04 | $2.52 | $-161.48 | $18,800.94 | ▼ -161.48 after sell → book $13,105.62; vs 09:30 mark -2.52 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,800.94 | ▲ close $13,123.94 vs 09:30 $13,108.14 (session +18.32) | 16:00 close · cash $18,800.94 · equity $13,123.94 vs 09:30 $13,108.14 (+15.80; session marks +18.32) · 2 name(s) marked open→close (per-name table). SIMO×12 09:30 $240.09 → close $237.35 +32.88; FIG×104 09:30 $27.06 → close $27.20 -14.56 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,800.94 | ▲ 09:30 equity $13,187.30 vs yday $13,123.94 (+63.36) | 09:30 open · cash $18,800.94 (unchanged overnight, no fees) · equity $13,187.30 vs prior close $13,123.94 (+63.36) | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 12 | $235.71 | $2.03 | $+194.19 | $15,970.39 | ▲ +194.19 after sell → book $13,185.27; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 104 | $26.78 | $2.30 | $+348.87 | $13,182.97 | ▲ +348.87 after sell → book $13,182.97; vs 09:30 mark -2.30 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,182.97 | ▲ close $13,182.97 vs 09:30 $13,187.30 (session +0.00) | 16:00 close · cash $13,182.97 · no lots left · equity $13,182.97. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,182.97 | ▲ 09:30 equity $13,182.97 vs yday $13,182.97 (-0.00) | 09:30 open · cash $13,182.97 (unchanged overnight, no fees) · equity $13,182.97 vs prior close $13,182.97 (-0.00) | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 925 | $1.78 | $11.93 | — | $11,524.53 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $1647.87; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 89 | $18.40 | $2.26 | — | $9,884.68 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $1647.87; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 120 | $13.71 | $2.35 | — | $8,237.13 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $1647.87; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 69 | $23.88 | $2.20 | — | $6,587.21 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $1647.87; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 221 | $14.85 | $3.00 | — | $9,866.06 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $3291.06; owner short_news_r_h3 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1924 | $1.71 | $25.26 | — | $13,130.84 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $3291.06; owner short_news_r_h3 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,130.84 | ▼ close $12,993.72 vs 09:30 $13,182.97 (session -142.25) | 16:00 close · cash $13,130.84 · equity $12,993.72 vs 09:30 $13,182.97 (-189.25; session marks -142.25) · 6 name(s) marked open→close (per-name table). GPRO×925 09:30 $1.78 → close $1.39 -360.75; REAX×89 09:30 $18.40 → close $18.40 +0.00; CNH×120 09:30 $13.71 → close $13.84 +15.60; MMED×69 09:30 $23.88 → close $23.84 -2.76; SLN×221 09:30 $14.85 → close $14.79 +13.26; OPK×1924 09:30 $1.71 → close $1.61 +192.40 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,130.84 | ▲ 09:30 equity $13,134.56 vs yday $12,993.72 (+140.84) | 09:30 open · cash $13,130.84 (unchanged overnight, no fees) · equity $13,134.56 vs prior close $12,993.72 (+140.84) | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 89 | $18.15 | $2.28 | $-26.79 | $14,743.91 | ▼ -26.79 after sell → book $13,132.28; vs 09:30 mark -2.28 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 120 | $13.89 | $2.38 | $+16.87 | $16,408.32 | ▲ +16.87 after sell → book $13,129.89; vs 09:30 mark -2.39 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 69 | $23.84 | $2.22 | $-7.18 | $18,051.06 | ▼ -7.18 after sell → book $13,127.67; vs 09:30 mark -2.22 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 119 | $25.18 | $2.35 | — | $15,052.29 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $3008.51; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 382 | $7.87 | $4.93 | — | $12,041.03 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $3008.51; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 519 | $5.79 | $6.70 | — | $9,029.32 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $3008.51; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 702 | $4.67 | $9.29 | — | $12,298.37 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $3278.43; owner short_news_r_h3 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 42 | $76.55 | $2.24 | — | $15,511.23 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $3278.43; owner short_news_r_h3 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,511.23 | ▲ close $13,495.41 vs 09:30 $13,134.56 (session +393.24) | 16:00 close · cash $15,511.23 · equity $13,495.41 vs 09:30 $13,134.56 (+360.85; session marks +393.24) · 8 name(s) marked open→close (per-name table). GPRO×925 09:30 $1.48 → close $1.70 +203.50; SLN×221 09:30 $14.63 → close $14.59 +8.84; OPK×1924 09:30 $1.59 → close $1.64 -96.20; ASST×119 09:30 $25.18 → close $27.14 +233.24; USDE×382 09:30 $7.87 → close $7.93 +22.92; DFDV×519 09:30 $5.79 → close $5.87 +41.52; GSM×702 09:30 $4.67 → close $4.67 -0.00; PIPR×42 09:30 $76.55 → close $77.04 -20.58 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,511.23 | ▼ 09:30 equity $13,248.39 vs yday $13,495.41 (-247.02) | 09:30 open · cash $15,511.23 (unchanged overnight, no fees) · equity $13,248.39 vs prior close $13,495.41 (-247.02) | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 925 | $1.56 | $12.10 | $-222.91 | $16,946.76 | ▼ -222.91 after sell → book $13,236.29; vs 09:30 mark -12.10 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 119 | $26.44 | $2.39 | $+145.20 | $20,090.73 | ▲ +145.20 after sell → book $13,233.90; vs 09:30 mark -2.39 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 382 | $7.76 | $5.01 | $-51.96 | $23,050.03 | ▼ -51.96 after sell → book $13,228.88; vs 09:30 mark -5.02 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 519 | $5.81 | $6.81 | $-3.12 | $26,058.62 | ▼ -3.12 after sell → book $13,222.08; vs 09:30 mark -6.80 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26,058.62 | ▲ close $13,552.65 vs 09:30 $13,248.39 (session +330.57) | 16:00 close · cash $26,058.62 · equity $13,552.65 vs 09:30 $13,248.39 (+304.26; session marks +330.57) · 4 name(s) marked open→close (per-name table). SLN×221 09:30 $14.24 → close $13.69 +121.55; OPK×1924 09:30 $1.63 → close $1.59 +76.96; GSM×702 09:30 $4.75 → close $4.52 +161.46; PIPR×42 09:30 $76.64 → close $77.34 -29.40 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26,058.62 | ▲ 09:30 equity $13,595.98 vs yday $13,552.65 (+43.33) | 09:30 open · cash $26,058.62 (unchanged overnight, no fees) · equity $13,595.98 vs prior close $13,552.65 (+43.33) | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 221 | $13.60 | $2.85 | $+270.40 | $23,050.17 | ▲ +270.40 after sell → book $13,593.13; vs 09:30 mark -2.85 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1924 | $1.58 | $24.82 | $+200.04 | $19,985.43 | ▲ +200.04 after sell → book $13,568.31; vs 09:30 mark -24.82 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,985.43 | ▲ close $13,601.13 vs 09:30 $13,595.98 (session +32.82) | 16:00 close · cash $19,985.43 · equity $13,601.13 vs 09:30 $13,595.98 (+5.15; session marks +32.82) · 2 name(s) marked open→close (per-name table). GSM×702 09:30 $4.52 → close $4.49 +21.06; PIPR×42 09:30 $77.24 → close $76.96 +11.76 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `EU` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `LUNR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `EU` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `LUNR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new short_news_r_h3 |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new short_news_r_h3 |
| 2026-08-18 | `RNW` | hard_red | hard-red S=-6.20 sit; no new short_news_r_h3 |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new union_hot_n4_h1 |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new union_hot_n4_h1 |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new union_hot_n4_h1 |
| 2026-08-19 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new short_news_r_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new short_news_r_h3 |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new short_news_r_h3 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new union_hot_n4_h1 |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new union_hot_n4_h1 |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new union_hot_n4_h1 |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new union_hot_n4_h1 |
| 2026-08-21 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TOYO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AAP` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TOYO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AAP` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AUGO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `SSRM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `ARIS` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new short_news_r_h3 |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new short_news_r_h3 |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new short_news_r_h3 |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new short_news_r_h3 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-25 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `AUGO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `ARIS` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `NOG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-26 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BMO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BMO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `NEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `BE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `NEM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new union_hot_n4_h1 |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new union_hot_n4_h1 |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new union_hot_n4_h1 |
| 2026-09-01 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new union_hot_n4_h1 |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new union_hot_n4_h1 |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new union_hot_n4_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new union_hot_n4_h1 |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new short_news_r_h3 |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new short_news_r_h3 |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new union_hot_n4_h1 |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new union_hot_n4_h1 |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new union_hot_n4_h1 |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new union_hot_n4_h1 |
| 2026-09-04 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `OPK` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new short_news_r_h3 |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new short_news_r_h3 |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-09 | `GSM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GSM` | 702 | 2026-09-04 @ $4.67 | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $3278.43; owner short_news_r_h3 |
| `PIPR` | 42 | 2026-09-04 @ $76.55 | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $3278.43; owner short_news_r_h3 |
