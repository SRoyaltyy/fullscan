# Factor mine action — `combo_sh_3070_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared short_news_r_h3/union_hot_n4_h1 w=0.3,0.7 net=priority

Cash book **+30.81%** ($13,081) · signal-only (no cash/fees) was —. Starts YES **17/20**. Fills 144 · skips 110 · realized $+3080.79.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: short_news_r_h3 30%, union_hot_n4_h1 70%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: short_news_r_h3 30%, union_hot_n4_h1 70%.
- Member: short_news_r_h3 (30% · short · hold 3).
- Member: union_hot_n4_h1 (70% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $13,080.80.

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
| 2026-08-14 | `QMCO` | 73 | — | $24.68 | +0.00 | $26.11 | +104.39 | +104.39 | +0.00 | +104.39 |
| 2026-08-14 | `ARX` | 92 | — | $19.57 | +0.00 | $19.58 | +0.92 | +0.92 | +0.00 | +0.92 |
| 2026-08-14 | `ZENA` | 824 | — | $2.20 | +0.00 | $2.14 | -49.44 | -49.44 | +0.00 | -49.44 |
| 2026-08-14 | `AIRO` | 163 | — | $11.12 | +0.00 | $9.57 | -252.65 | -252.65 | +0.00 | -252.65 |
| 2026-08-14 | `EU` | 881 | — | $1.18 | +0.00 | $1.21 | -26.43 | -26.43 | -0.00 | -26.43 |
| 2026-08-14 | `LUNR` | 54 | — | $19.17 | +0.00 | $19.01 | +8.64 | +8.64 | -0.00 | +8.64 |
| 2026-08-14 | `OWL` | 81 | — | $12.70 | +0.00 | $12.22 | +38.47 | +38.47 | -0.00 | +38.47 |
| 2026-08-17 | `QMCO` | 73 | $26.11 | $24.83 | -93.44 | — | +0.00 | -93.44 | +10.95 | — |
| 2026-08-17 | `ARX` | 92 | $19.58 | $19.57 | -0.92 | — | +0.00 | -0.92 | +0.00 | — |
| 2026-08-17 | `ZENA` | 824 | $2.14 | $2.08 | -45.32 | — | +0.00 | -45.32 | -94.76 | — |
| 2026-08-17 | `AIRO` | 163 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -252.65 | — |
| 2026-08-17 | `EU` | 881 | $1.21 | $1.21 | +0.00 | $1.13 | +70.48 | +70.48 | -26.43 | +44.05 |
| 2026-08-17 | `LUNR` | 54 | $19.01 | $20.25 | -66.96 | $20.38 | -7.02 | -73.98 | -58.32 | -65.34 |
| 2026-08-17 | `OWL` | 81 | $12.22 | $12.12 | +8.10 | $11.66 | +37.26 | +45.36 | +46.58 | +83.84 |
| 2026-08-17 | `XHG` | 546 | — | $4.19 | +0.00 | $3.91 | -152.88 | -152.88 | +0.00 | -152.88 |
| 2026-08-17 | `CAPR` | 333 | — | $6.87 | +0.00 | $7.45 | +193.14 | +193.14 | +0.00 | +193.14 |
| 2026-08-17 | `STDN` | 167 | — | $13.64 | +0.00 | $13.31 | -55.11 | -55.11 | +0.00 | -55.11 |
| 2026-08-17 | `HTFL` | 55 | — | $41.23 | +0.00 | $41.94 | +39.05 | +39.05 | +0.00 | +39.05 |
| 2026-08-17 | `VERI` | 857 | — | $1.15 | +0.00 | $1.08 | +55.70 | +55.70 | -0.00 | +55.70 |
| 2026-08-17 | `ZNTL` | 277 | — | $3.56 | +0.00 | $3.71 | -40.17 | -40.17 | -0.00 | -40.17 |
| 2026-08-17 | `APMD` | 31 | — | $31.70 | +0.00 | $32.55 | -26.35 | -26.35 | -0.00 | -26.35 |
| 2026-08-17 | `HIVE` | 327 | — | $3.01 | +0.00 | $3.07 | -19.62 | -19.62 | -0.00 | -19.62 |
| 2026-08-18 | `EU` | 881 | $1.13 | $1.13 | +0.00 | $1.07 | +52.86 | +52.86 | +44.05 | +96.91 |
| 2026-08-18 | `LUNR` | 54 | $20.38 | $19.31 | +57.78 | $19.31 | +0.00 | +57.78 | -7.56 | -7.56 |
| 2026-08-18 | `OWL` | 81 | $11.66 | $11.54 | +9.72 | $11.59 | -4.05 | +5.67 | +93.56 | +89.51 |
| 2026-08-18 | `XHG` | 546 | $3.91 | $3.94 | +16.38 | — | +0.00 | +16.38 | -136.50 | — |
| 2026-08-18 | `CAPR` | 333 | $7.45 | $7.50 | +16.65 | $7.08 | -139.86 | -123.21 | +209.79 | +69.93 |
| 2026-08-18 | `STDN` | 167 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -55.11 | — |
| 2026-08-18 | `HTFL` | 55 | $41.94 | $41.50 | -24.20 | — | +0.00 | -24.20 | +14.85 | — |
| 2026-08-18 | `VERI` | 857 | $1.08 | $1.05 | +29.99 | $0.99 | +47.14 | +77.13 | +85.70 | +132.83 |
| 2026-08-18 | `ZNTL` | 277 | $3.71 | $3.75 | -12.46 | $3.68 | +19.39 | +6.93 | -52.63 | -33.24 |
| 2026-08-18 | `APMD` | 31 | $32.55 | $32.85 | -9.30 | $31.81 | +32.24 | +22.94 | -35.65 | -3.41 |
| 2026-08-18 | `HIVE` | 327 | $3.07 | $2.96 | +35.97 | $2.78 | +58.86 | +94.83 | +16.35 | +75.21 |
| 2026-08-19 | `EU` | 881 | $1.07 | $1.07 | +0.00 | — | +0.00 | +0.00 | +96.91 | — |
| 2026-08-19 | `LUNR` | 54 | $19.31 | $18.98 | +17.82 | $18.52 | +24.84 | +42.66 | +10.26 | +35.10 |
| 2026-08-19 | `OWL` | 81 | $11.59 | $11.75 | -12.96 | — | +0.00 | -12.96 | +76.55 | — |
| 2026-08-19 | `CAPR` | 333 | $7.08 | $7.19 | +36.63 | — | +0.00 | +36.63 | +106.56 | — |
| 2026-08-19 | `VERI` | 857 | $0.99 | $1.00 | -4.29 | $0.97 | +29.14 | +24.85 | +128.55 | +157.69 |
| 2026-08-19 | `ZNTL` | 277 | $3.68 | $3.76 | -22.16 | $3.82 | -16.62 | -38.78 | -55.40 | -72.02 |
| 2026-08-19 | `APMD` | 31 | $31.81 | $32.13 | -9.92 | $32.03 | +3.10 | -6.82 | -13.33 | -10.23 |
| 2026-08-19 | `HIVE` | 327 | $2.78 | $2.78 | +0.00 | $2.82 | -13.08 | -13.08 | +75.21 | +62.13 |
| 2026-08-20 | `LUNR` | 54 | $18.52 | $18.13 | +21.06 | — | +0.00 | +21.06 | +56.16 | — |
| 2026-08-20 | `VERI` | 857 | $0.97 | $0.96 | +2.57 | — | +0.00 | +2.57 | +160.26 | — |
| 2026-08-20 | `ZNTL` | 277 | $3.82 | $4.01 | -54.01 | — | +0.00 | -54.01 | -126.03 | — |
| 2026-08-20 | `APMD` | 31 | $32.03 | $31.87 | +4.96 | — | +0.00 | +4.96 | -5.27 | — |
| 2026-08-20 | `HIVE` | 327 | $2.82 | $2.95 | -42.51 | — | +0.00 | -42.51 | +19.62 | — |
| 2026-08-20 | `ABCL` | 149 | — | $11.81 | +0.00 | $11.57 | -36.50 | -36.50 | +0.00 | -36.50 |
| 2026-08-20 | `MRNA` | 11 | — | $150.14 | +0.00 | $133.32 | -185.02 | -185.02 | +0.00 | -185.02 |
| 2026-08-20 | `CYPH` | 1536 | — | $1.15 | +0.00 | $1.19 | +61.44 | +61.44 | +0.00 | +61.44 |
| 2026-08-20 | `AZI` | 1289 | — | $1.37 | +0.00 | $1.44 | +90.23 | +90.23 | +0.00 | +90.23 |
| 2026-08-20 | `AEM` | 2 | — | $204.45 | +0.00 | $212.04 | -15.18 | -15.18 | -0.00 | -15.18 |
| 2026-08-20 | `WYFI` | 20 | — | $21.40 | +0.00 | $21.16 | +4.80 | +4.80 | -0.00 | +4.80 |
| 2026-08-20 | `TOYO` | 100 | — | $4.43 | +0.00 | $4.51 | -8.50 | -8.50 | -0.00 | -8.50 |
| 2026-08-20 | `TEAM` | 2 | — | $173.90 | +0.00 | $174.91 | -2.02 | -2.02 | -0.00 | -2.02 |
| 2026-08-20 | `AAP` | 9 | — | $46.85 | +0.00 | $42.39 | +40.14 | +40.14 | -0.00 | +40.14 |
| 2026-08-20 | `WMT` | 4 | — | $106.38 | +0.00 | $103.84 | +10.16 | +10.16 | -0.00 | +10.16 |
| 2026-08-20 | `AQST` | 96 | — | $4.61 | +0.00 | $4.50 | +11.04 | +11.04 | -0.00 | +11.04 |
| 2026-08-21 | `ABCL` | 149 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -36.50 | — |
| 2026-08-21 | `MRNA` | 11 | $133.32 | $133.11 | -2.31 | $145.13 | +132.22 | +129.91 | -187.33 | -55.11 |
| 2026-08-21 | `CYPH` | 1536 | $1.19 | $1.32 | +199.68 | $1.42 | +153.60 | +353.28 | +261.12 | +414.72 |
| 2026-08-21 | `AZI` | 1289 | $1.44 | $1.46 | +25.78 | — | +0.00 | +25.78 | +116.01 | — |
| 2026-08-21 | `AEM` | 2 | $212.04 | $216.30 | -8.52 | $216.06 | +0.48 | -8.04 | -23.70 | -23.22 |
| 2026-08-21 | `WYFI` | 20 | $21.16 | $21.54 | -7.60 | $20.72 | +16.40 | +8.80 | -2.80 | +13.60 |
| 2026-08-21 | `TOYO` | 100 | $4.51 | $4.68 | -16.50 | $4.82 | -14.00 | -30.50 | -25.00 | -39.00 |
| 2026-08-21 | `TEAM` | 2 | $174.91 | $174.22 | +1.38 | $171.81 | +4.82 | +6.20 | -0.64 | +4.18 |
| 2026-08-21 | `AAP` | 9 | $42.39 | $42.41 | -0.18 | $42.58 | -1.53 | -1.71 | +39.96 | +38.43 |
| 2026-08-21 | `WMT` | 4 | $103.84 | $103.69 | +0.60 | $103.70 | -0.04 | +0.56 | +10.76 | +10.72 |
| 2026-08-21 | `AQST` | 96 | $4.50 | $4.54 | -4.32 | $4.66 | -11.52 | -15.84 | +6.72 | -4.80 |
| 2026-08-21 | `XHG` | 748 | — | $4.49 | +0.00 | $4.41 | -59.84 | -59.84 | +0.00 | -59.84 |
| 2026-08-21 | `CAPR` | 493 | — | $6.81 | +0.00 | $6.29 | -256.36 | -256.36 | +0.00 | -256.36 |
| 2026-08-21 | `QTRX` | 184 | — | $3.11 | +0.00 | $2.99 | +22.08 | +22.08 | -0.00 | +22.08 |
| 2026-08-21 | `AUGO` | 6 | — | $89.10 | +0.00 | $87.26 | +11.04 | +11.04 | -0.00 | +11.04 |
| 2026-08-21 | `SSRM` | 14 | — | $38.40 | +0.00 | $37.77 | +8.82 | +8.82 | -0.00 | +8.82 |
| 2026-08-21 | `ARIS` | 27 | — | $20.90 | +0.00 | $20.86 | +1.08 | +1.08 | -0.00 | +1.08 |
| 2026-08-21 | `NOG` | 21 | — | $27.00 | +0.00 | $27.34 | -7.14 | -7.14 | -0.00 | -7.14 |
| 2026-08-24 | `MRNA` | 11 | $145.13 | $142.70 | -26.73 | — | +0.00 | -26.73 | -81.84 | — |
| 2026-08-24 | `CYPH` | 1536 | $1.42 | $1.83 | +629.76 | — | +0.00 | +629.76 | +1044.48 | — |
| 2026-08-24 | `AEM` | 2 | $216.06 | $217.03 | -1.94 | $217.89 | -1.72 | -3.66 | -25.16 | -26.88 |
| 2026-08-24 | `WYFI` | 20 | $20.72 | $20.01 | +14.20 | $20.78 | -15.40 | -1.20 | +27.80 | +12.40 |
| 2026-08-24 | `TOYO` | 100 | $4.82 | $4.58 | +24.00 | $4.38 | +20.00 | +44.00 | -15.00 | +5.00 |
| 2026-08-24 | `TEAM` | 2 | $171.81 | $169.30 | +5.02 | $171.33 | -4.06 | +0.96 | +9.20 | +5.14 |
| 2026-08-24 | `AAP` | 9 | $42.58 | $43.05 | -4.23 | $43.63 | -5.22 | -9.45 | +34.20 | +28.98 |
| 2026-08-24 | `WMT` | 4 | $103.70 | $104.14 | -1.76 | $106.49 | -9.40 | -11.16 | +8.96 | -0.44 |
| 2026-08-24 | `AQST` | 96 | $4.66 | $4.67 | -0.96 | $4.80 | -12.48 | -13.44 | -5.76 | -18.24 |
| 2026-08-24 | `XHG` | 748 | $4.41 | $4.32 | -67.32 | — | +0.00 | -67.32 | -127.16 | — |
| 2026-08-24 | `CAPR` | 493 | $6.29 | $8.03 | +857.82 | — | +0.00 | +857.82 | +601.46 | — |
| 2026-08-24 | `QTRX` | 184 | $2.99 | $2.99 | +0.00 | $2.80 | +34.96 | +34.96 | +22.08 | +57.04 |
| 2026-08-24 | `AUGO` | 6 | $87.26 | $88.60 | -8.04 | $87.37 | +7.38 | -0.66 | +3.00 | +10.38 |
| 2026-08-24 | `SSRM` | 14 | $37.77 | $38.32 | -7.70 | $38.61 | -4.06 | -11.76 | +1.12 | -2.94 |
| 2026-08-24 | `ARIS` | 27 | $20.86 | $20.98 | -3.24 | $20.81 | +4.59 | +1.35 | -2.16 | +2.43 |
| 2026-08-24 | `NOG` | 21 | $27.34 | $27.12 | +4.62 | $26.84 | +5.88 | +10.50 | -2.52 | +3.36 |
| 2026-08-25 | `AEM` | 2 | $217.89 | $212.00 | +11.78 | — | +0.00 | +11.78 | -15.10 | — |
| 2026-08-25 | `WYFI` | 20 | $20.78 | $20.90 | -2.40 | — | +0.00 | -2.40 | +10.00 | — |
| 2026-08-25 | `TOYO` | 100 | $4.38 | $4.42 | -4.00 | — | +0.00 | -4.00 | +1.00 | — |
| 2026-08-25 | `TEAM` | 2 | $171.33 | $170.64 | +1.38 | — | +0.00 | +1.38 | +6.52 | — |
| 2026-08-25 | `AAP` | 9 | $43.63 | $43.63 | +0.00 | — | +0.00 | +0.00 | +28.98 | — |
| 2026-08-25 | `WMT` | 4 | $106.49 | $105.58 | +3.64 | — | +0.00 | +3.64 | +3.20 | — |
| 2026-08-25 | `AQST` | 96 | $4.80 | $4.77 | +2.88 | — | +0.00 | +2.88 | -15.36 | — |
| 2026-08-25 | `QTRX` | 184 | $2.80 | $2.80 | +0.00 | $2.79 | +1.84 | +1.84 | +57.04 | +58.88 |
| 2026-08-25 | `AUGO` | 6 | $87.37 | $85.78 | +9.54 | $90.47 | -28.14 | -18.60 | +19.92 | -8.22 |
| 2026-08-25 | `SSRM` | 14 | $38.61 | $37.75 | +12.04 | $39.21 | -20.44 | -8.40 | +9.10 | -11.34 |
| 2026-08-25 | `ARIS` | 27 | $20.81 | $20.45 | +9.72 | $21.18 | -19.71 | -9.99 | +12.15 | -7.56 |
| 2026-08-25 | `NOG` | 21 | $26.84 | $26.06 | +16.38 | $26.42 | -7.56 | +8.82 | +19.74 | +12.18 |
| 2026-08-25 | `REAX` | 103 | — | $24.11 | +0.00 | $28.43 | +444.96 | +444.96 | +0.00 | +444.96 |
| 2026-08-25 | `CYPH` | 1598 | — | $1.56 | +0.00 | $1.64 | +127.84 | +127.84 | +0.00 | +127.84 |
| 2026-08-25 | `XHG` | 612 | — | $4.07 | +0.00 | $4.02 | -30.60 | -30.60 | +0.00 | -30.60 |
| 2026-08-25 | `ASST` | 131 | — | $19.04 | +0.00 | $21.39 | +307.85 | +307.85 | +0.00 | +307.85 |
| 2026-08-25 | `AVAH` | 104 | — | $13.62 | +0.00 | $13.59 | +3.64 | +3.64 | -0.00 | +3.64 |
| 2026-08-25 | `ARE` | 26 | — | $54.51 | +0.00 | $52.90 | +41.86 | +41.86 | -0.00 | +41.86 |
| 2026-08-25 | `BMO` | 8 | — | $175.01 | +0.00 | $173.46 | +12.40 | +12.40 | -0.00 | +12.40 |
| 2026-08-26 | `QTRX` | 184 | $2.79 | $2.83 | -7.36 | — | +0.00 | -7.36 | +51.52 | — |
| 2026-08-26 | `AUGO` | 6 | $90.47 | $88.24 | +13.38 | — | +0.00 | +13.38 | +5.16 | — |
| 2026-08-26 | `SSRM` | 14 | $39.21 | $38.41 | +11.20 | — | +0.00 | +11.20 | -0.14 | — |
| 2026-08-26 | `ARIS` | 27 | $21.18 | $20.50 | +18.36 | — | +0.00 | +18.36 | +10.80 | — |
| 2026-08-26 | `NOG` | 21 | $26.42 | $26.00 | +8.82 | — | +0.00 | +8.82 | +21.00 | — |
| 2026-08-26 | `REAX` | 103 | $28.43 | $26.61 | -187.46 | — | +0.00 | -187.46 | +257.50 | — |
| 2026-08-26 | `CYPH` | 1598 | $1.64 | $1.60 | -63.92 | — | +0.00 | -63.92 | +63.92 | — |
| 2026-08-26 | `XHG` | 612 | $4.02 | $3.81 | -128.52 | $4.06 | +153.00 | +24.48 | -159.12 | -6.12 |
| 2026-08-26 | `ASST` | 131 | $21.39 | $20.72 | -87.77 | — | +0.00 | -87.77 | +220.08 | — |
| 2026-08-26 | `AVAH` | 104 | $13.59 | $13.65 | -6.24 | $13.62 | +3.12 | -3.12 | -2.60 | +0.52 |
| 2026-08-26 | `ARE` | 26 | $52.90 | $52.77 | +3.38 | $52.97 | -5.20 | -1.82 | +45.24 | +40.04 |
| 2026-08-26 | `BMO` | 8 | $173.46 | $173.22 | +1.92 | $172.90 | +2.56 | +4.48 | +14.32 | +16.88 |
| 2026-08-26 | `BYND` | 227 | — | $14.11 | +0.00 | $14.25 | +31.78 | +31.78 | +0.00 | +31.78 |
| 2026-08-26 | `USDE` | 553 | — | $5.81 | +0.00 | $5.98 | +94.01 | +94.01 | +0.00 | +94.01 |
| 2026-08-26 | `SUJA` | 342 | — | $9.39 | +0.00 | $9.44 | +17.10 | +17.10 | +0.00 | +17.10 |
| 2026-08-26 | `BE` | 4 | — | $213.94 | +0.00 | $218.21 | -17.08 | -17.08 | -0.00 | -17.08 |
| 2026-08-26 | `ABCL` | 84 | — | $12.22 | +0.00 | $12.24 | -1.68 | -1.68 | -0.00 | -1.68 |
| 2026-08-26 | `AQST` | 203 | — | $5.08 | +0.00 | $5.39 | -62.93 | -62.93 | -0.00 | -62.93 |
| 2026-08-26 | `NEM` | 7 | — | $132.64 | +0.00 | $131.60 | +7.28 | +7.28 | -0.00 | +7.28 |
| 2026-08-27 | `XHG` | 612 | $4.06 | $4.06 | +0.00 | — | +0.00 | +0.00 | -6.12 | — |
| 2026-08-27 | `AVAH` | 104 | $13.62 | $13.62 | +0.00 | $13.82 | -20.80 | -20.80 | +0.52 | -20.28 |
| 2026-08-27 | `ARE` | 26 | $52.97 | $52.45 | +13.52 | $52.28 | +4.42 | +17.94 | +53.56 | +57.98 |
| 2026-08-27 | `BMO` | 8 | $172.90 | $172.85 | +0.40 | $172.13 | +5.76 | +6.16 | +17.28 | +23.04 |
| 2026-08-27 | `BYND` | 227 | $14.25 | $14.20 | -11.35 | — | +0.00 | -11.35 | +20.43 | — |
| 2026-08-27 | `USDE` | 553 | $5.98 | $6.50 | +287.56 | — | +0.00 | +287.56 | +381.57 | — |
| 2026-08-27 | `SUJA` | 342 | $9.44 | $9.41 | -10.26 | — | +0.00 | -10.26 | +6.84 | — |
| 2026-08-27 | `BE` | 4 | $218.21 | $227.10 | -35.56 | $217.83 | +37.08 | +1.52 | -52.64 | -15.56 |
| 2026-08-27 | `ABCL` | 84 | $12.24 | $12.25 | -0.84 | $12.40 | -12.60 | -13.44 | -2.52 | -15.12 |
| 2026-08-27 | `AQST` | 203 | $5.39 | $5.39 | +0.00 | $5.16 | +46.69 | +46.69 | -62.93 | -16.24 |
| 2026-08-27 | `NEM` | 7 | $131.60 | $131.02 | +4.06 | $132.29 | -8.89 | -4.83 | +11.34 | +2.45 |
| 2026-08-27 | `SLI` | 1967 | — | $2.60 | +0.00 | $2.64 | +78.68 | +78.68 | +0.00 | +78.68 |
| 2026-08-27 | `RRC` | 123 | — | $41.44 | +0.00 | $41.64 | +24.60 | +24.60 | +0.00 | +24.60 |
| 2026-08-27 | `PGY` | 223 | — | $22.93 | +0.00 | $23.26 | +73.59 | +73.59 | +0.00 | +73.59 |
| 2026-08-27 | `CRK` | 353 | — | $14.42 | +0.00 | $14.62 | +70.60 | +70.60 | +0.00 | +70.60 |
| 2026-08-28 | `AVAH` | 104 | $13.82 | $13.90 | -8.32 | — | +0.00 | -8.32 | -28.60 | — |
| 2026-08-28 | `ARE` | 26 | $52.28 | $52.49 | -5.46 | — | +0.00 | -5.46 | +52.52 | — |
| 2026-08-28 | `BMO` | 8 | $172.13 | $172.76 | -5.04 | — | +0.00 | -5.04 | +18.00 | — |
| 2026-08-28 | `BE` | 4 | $217.83 | $215.71 | +8.50 | $210.77 | +19.74 | +28.24 | -7.06 | +12.68 |
| 2026-08-28 | `ABCL` | 84 | $12.40 | $12.30 | +7.98 | $11.35 | +80.22 | +88.20 | -7.14 | +73.08 |
| 2026-08-28 | `AQST` | 203 | $5.16 | $5.11 | +10.15 | $5.02 | +18.27 | +28.42 | -6.09 | +12.18 |
| 2026-08-28 | `NEM` | 7 | $132.29 | $132.35 | -0.42 | $127.98 | +30.59 | +30.17 | +2.03 | +32.62 |
| 2026-08-28 | `SLI` | 1967 | $2.64 | $2.68 | +78.68 | — | +0.00 | +78.68 | +157.36 | — |
| 2026-08-28 | `RRC` | 123 | $41.64 | $41.74 | +12.30 | — | +0.00 | +12.30 | +36.90 | — |
| 2026-08-28 | `PGY` | 223 | $23.26 | $23.21 | -11.15 | — | +0.00 | -11.15 | +62.44 | — |
| 2026-08-28 | `CRK` | 353 | $14.62 | $14.63 | +3.53 | — | +0.00 | +3.53 | +74.13 | — |
| 2026-08-28 | `BYND` | 206 | — | $14.00 | +0.00 | $13.86 | -28.84 | -28.84 | +0.00 | -28.84 |
| 2026-08-28 | `CAPR` | 297 | — | $9.73 | +0.00 | $9.59 | -41.58 | -41.58 | +0.00 | -41.58 |
| 2026-08-28 | `MRNA` | 21 | — | $137.19 | +0.00 | $137.99 | +16.80 | +16.80 | +0.00 | +16.80 |
| 2026-08-28 | `ANF` | 19 | — | $146.07 | +0.00 | $148.42 | +44.65 | +44.65 | +0.00 | +44.65 |
| 2026-08-28 | `SIMO` | 10 | — | $252.24 | +0.00 | $245.81 | +64.30 | +64.30 | -0.00 | +64.30 |
| 2026-08-28 | `FIG` | 84 | — | $30.18 | +0.00 | $28.82 | +114.24 | +114.24 | -0.00 | +114.24 |
| 2026-08-31 | `BE` | 4 | $210.77 | $208.88 | +7.56 | — | +0.00 | +7.56 | +20.24 | — |
| 2026-08-31 | `ABCL` | 84 | $11.35 | $11.10 | +21.00 | — | +0.00 | +21.00 | +94.08 | — |
| 2026-08-31 | `AQST` | 203 | $5.02 | $4.97 | +9.13 | — | +0.00 | +9.13 | +21.32 | — |
| 2026-08-31 | `NEM` | 7 | $127.98 | $127.45 | +3.71 | — | +0.00 | +3.71 | +36.33 | — |
| 2026-08-31 | `BYND` | 206 | $13.86 | $13.81 | -10.30 | $13.30 | -105.06 | -115.36 | -39.14 | -144.20 |
| 2026-08-31 | `CAPR` | 297 | $9.59 | $9.50 | -26.73 | — | +0.00 | -26.73 | -68.31 | — |
| 2026-08-31 | `MRNA` | 21 | $137.99 | $134.10 | -81.69 | — | +0.00 | -81.69 | -64.89 | — |
| 2026-08-31 | `ANF` | 19 | $148.42 | $148.03 | -7.41 | — | +0.00 | -7.41 | +37.24 | — |
| 2026-08-31 | `SIMO` | 10 | $245.81 | $247.05 | -12.40 | $246.84 | +2.10 | -10.30 | +51.90 | +54.00 |
| 2026-08-31 | `FIG` | 84 | $28.82 | $27.60 | +102.48 | $27.49 | +9.24 | +111.72 | +216.72 | +225.96 |
| 2026-09-01 | `BYND` | 206 | $13.30 | $13.04 | -53.56 | — | +0.00 | -53.56 | -197.76 | — |
| 2026-09-01 | `SIMO` | 10 | $246.84 | $240.09 | +67.50 | $237.35 | +27.40 | +94.90 | +121.50 | +148.90 |
| 2026-09-01 | `FIG` | 84 | $27.49 | $27.06 | +36.12 | $27.20 | -11.76 | +24.36 | +262.08 | +250.32 |
| 2026-09-02 | `SIMO` | 10 | $237.35 | $235.71 | +16.40 | — | +0.00 | +16.40 | +165.30 | — |
| 2026-09-02 | `FIG` | 84 | $27.20 | $26.78 | +35.28 | — | +0.00 | +35.28 | +285.60 | — |
| 2026-09-03 | `GPRO` | 1275 | — | $1.78 | +0.00 | $1.39 | -497.25 | -497.25 | +0.00 | -497.25 |
| 2026-09-03 | `REAX` | 123 | — | $18.40 | +0.00 | $18.40 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `CNH` | 165 | — | $13.71 | +0.00 | $13.84 | +21.45 | +21.45 | +0.00 | +21.45 |
| 2026-09-03 | `MMED` | 95 | — | $23.88 | +0.00 | $23.84 | -3.80 | -3.80 | +0.00 | -3.80 |
| 2026-09-03 | `SLN` | 130 | — | $14.85 | +0.00 | $14.79 | +7.80 | +7.80 | -0.00 | +7.80 |
| 2026-09-03 | `OPK` | 1136 | — | $1.71 | +0.00 | $1.61 | +113.60 | +113.60 | -0.00 | +113.60 |
| 2026-09-04 | `GPRO` | 1275 | $1.39 | $1.48 | +114.75 | $1.70 | +280.50 | +395.25 | -382.50 | -102.00 |
| 2026-09-04 | `REAX` | 123 | $18.40 | $18.15 | -30.75 | — | +0.00 | -30.75 | -30.75 | — |
| 2026-09-04 | `CNH` | 165 | $13.84 | $13.89 | +8.25 | — | +0.00 | +8.25 | +29.70 | — |
| 2026-09-04 | `MMED` | 95 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -3.80 | — |
| 2026-09-04 | `SLN` | 130 | $14.79 | $14.63 | +20.80 | $14.59 | +5.20 | +26.00 | +28.60 | +33.80 |
| 2026-09-04 | `OPK` | 1136 | $1.61 | $1.59 | +22.72 | $1.64 | -56.80 | -34.08 | +136.32 | +79.52 |
| 2026-09-04 | `ASST` | 134 | — | $25.18 | +0.00 | $27.14 | +262.64 | +262.64 | +0.00 | +262.64 |
| 2026-09-04 | `USDE` | 430 | — | $7.87 | +0.00 | $7.93 | +25.80 | +25.80 | +0.00 | +25.80 |
| 2026-09-04 | `DFDV` | 585 | — | $5.79 | +0.00 | $5.87 | +46.80 | +46.80 | +0.00 | +46.80 |
| 2026-09-04 | `GSM` | 467 | — | $4.67 | +0.00 | $4.67 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-09-04 | `PIPR` | 28 | — | $76.55 | +0.00 | $77.04 | -13.72 | -13.72 | -0.00 | -13.72 |
| 2026-09-08 | `GPRO` | 1275 | $1.70 | $1.56 | -172.12 | — | +0.00 | -172.12 | -274.13 | — |
| 2026-09-08 | `SLN` | 130 | $14.59 | $14.24 | +45.50 | $13.69 | +71.50 | +117.00 | +79.30 | +150.80 |
| 2026-09-08 | `OPK` | 1136 | $1.64 | $1.63 | +11.36 | $1.59 | +45.44 | +56.80 | +90.88 | +136.32 |
| 2026-09-08 | `ASST` | 134 | $27.14 | $26.44 | -93.80 | — | +0.00 | -93.80 | +168.84 | — |
| 2026-09-08 | `USDE` | 430 | $7.93 | $7.76 | -73.10 | — | +0.00 | -73.10 | -47.30 | — |
| 2026-09-08 | `DFDV` | 585 | $5.87 | $5.81 | -35.10 | — | +0.00 | -35.10 | +11.70 | — |
| 2026-09-08 | `GSM` | 467 | $4.67 | $4.75 | -37.36 | $4.52 | +107.41 | +70.05 | -37.36 | +70.05 |
| 2026-09-08 | `PIPR` | 28 | $77.04 | $76.64 | +11.20 | $77.34 | -19.60 | -8.40 | -2.52 | -22.12 |
| 2026-09-09 | `SLN` | 130 | $13.69 | $13.60 | +11.70 | — | +0.00 | +11.70 | +162.50 | — |
| 2026-09-09 | `OPK` | 1136 | $1.59 | $1.58 | +11.36 | — | +0.00 | +11.36 | +147.68 | — |
| 2026-09-09 | `GSM` | 467 | $4.52 | $4.52 | +0.00 | $4.49 | +14.01 | +14.01 | +70.05 | +84.06 |
| 2026-09-09 | `PIPR` | 28 | $77.34 | $77.24 | +2.80 | $76.96 | +7.84 | +10.64 | -19.32 | -11.48 |
| 2026-09-10 | `GSM` | 467 | $4.49 | $4.49 | +0.00 | — | +0.00 | +0.00 | +84.06 | — |
| 2026-09-10 | `PIPR` | 28 | $76.96 | $76.96 | +0.00 | — | +0.00 | +0.00 | -11.48 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +386.21 | IREN, TNDM, TPG, INO | — | $0.54 | $10,345.37 | IREN×54, TNDM×107, TPG×49, INO×3085 |
| 2026-08-14 | +5.50 | $0.54 | IREN×54, TNDM×107, TPG×49, INO×3085 | $10,412.10 | +66.73 | -176.10 | QMCO, ARX, ZENA, AIRO, EU, LUNR, OWL | IREN, TNDM, TPG, INO | $6,208.92 | $10,157.21 | QMCO×73, ARX×92, ZENA×824, AIRO×163, EU×881, LUNR×54, OWL×81 |
| 2026-08-17 | +2.25 | $6,208.92 | QMCO×73, ARX×92, ZENA×824, AIRO×163, EU×881, LUNR×54, OWL×81 | $9,958.67 | -198.54 | +94.48 | XHG, CAPR, STDN, HTFL, VERI, ZNTL, APMD, HIVE | QMCO, ARX, ZENA, AIRO | $7,862.41 | $9,998.01 | EU×881, LUNR×54, OWL×81, XHG×546, CAPR×333, STDN×167, HTFL×55, VERI×857, ZNTL×277, APMD×31, HIVE×327 |
| 2026-08-18 | -6.20 | $7,862.41 | EU×881, LUNR×54, OWL×81, XHG×546, CAPR×333, STDN×167, HTFL×55, VERI×857, ZNTL×277, APMD×31, HIVE×327 | $10,118.54 | +120.53 | +66.58 | — | XHG, STDN, HTFL | $14,507.05 | $10,173.25 | EU×881, LUNR×54, OWL×81, CAPR×333, VERI×857, ZNTL×277, APMD×31, HIVE×327 |
| 2026-08-19 | -7.20 | $14,507.05 | EU×881, LUNR×54, OWL×81, CAPR×333, VERI×857, ZNTL×277, APMD×31, HIVE×327 | $10,178.37 | +5.12 | +27.38 | — | EU, OWL, CAPR | $14,988.93 | $10,187.78 | LUNR×54, VERI×857, ZNTL×277, APMD×31, HIVE×327 |
| 2026-08-20 | +1.12 | $14,988.93 | LUNR×54, VERI×857, ZNTL×277, APMD×31, HIVE×327 | $10,119.85 | -67.93 | -29.41 | ABCL, MRNA, CYPH, AZI, AEM, WYFI, TOYO, TEAM, AAP, WMT, AQST | LUNR, VERI, ZNTL, APMD, HIVE | $6,014.36 | $10,011.82 | ABCL×149, MRNA×11, CYPH×1536, AZI×1289, AEM×2, WYFI×20, TOYO×100, TEAM×2, AAP×9, WMT×4, AQST×96 |
| 2026-08-21 | +3.25 | $6,014.36 | ABCL×149, MRNA×11, CYPH×1536, AZI×1289, AEM×2, WYFI×20, TOYO×100, TEAM×2, AAP×9, WMT×4, AQST×96 | $10,199.83 | +188.01 | +0.11 | XHG, CAPR, QTRX, AUGO, SSRM, ARIS, NOG | ABCL, AZI | $5,633.87 | $10,153.69 | MRNA×11, CYPH×1536, AEM×2, WYFI×20, TOYO×100, TEAM×2, AAP×9, WMT×4, AQST×96, XHG×748, CAPR×493, QTRX×184, AUGO×6, SSRM×14, ARIS×27, NOG×21 |
| 2026-08-24 | -5.17 | $5,633.87 | MRNA×11, CYPH×1536, AEM×2, WYFI×20, TOYO×100, TEAM×2, AAP×9, WMT×4, AQST×96, XHG×748, CAPR×493, QTRX×184, AUGO×6, SSRM×14, ARIS×27, NOG×21 | $11,567.19 | +1,413.50 | +20.47 | — | MRNA, CYPH, XHG, CAPR | $17,166.19 | $11,549.25 | AEM×2, WYFI×20, TOYO×100, TEAM×2, AAP×9, WMT×4, AQST×96, QTRX×184, AUGO×6, SSRM×14, ARIS×27, NOG×21 |
| 2026-08-25 | +1.80 | $17,166.19 | AEM×2, WYFI×20, TOYO×100, TEAM×2, AAP×9, WMT×4, AQST×96, QTRX×184, AUGO×6, SSRM×14, ARIS×27, NOG×21 | $11,610.21 | +60.96 | +833.94 | REAX, CYPH, XHG, ASST, AVAH, ARE, BMO | AEM, WYFI, TOYO, TEAM, AAP, WMT, AQST | $8,486.66 | $12,389.76 | QTRX×184, AUGO×6, SSRM×14, ARIS×27, NOG×21, REAX×103, CYPH×1598, XHG×612, ASST×131, AVAH×104, ARE×26, BMO×8 |
| 2026-08-26 | +2.02 | $8,486.66 | QTRX×184, AUGO×6, SSRM×14, ARIS×27, NOG×21, REAX×103, CYPH×1598, XHG×612, ASST×131, AVAH×104, ARE×26, BMO×8 | $11,965.55 | -424.21 | +221.96 | BYND, USDE, SUJA, BE, ABCL, AQST, NEM | QTRX, AUGO, SSRM, ARIS, NOG, REAX, CYPH, ASST | $7,965.96 | $12,127.58 | XHG×612, AVAH×104, ARE×26, BMO×8, BYND×227, USDE×553, SUJA×342, BE×4, ABCL×84, AQST×203, NEM×7 |
| 2026-08-27 | — | $7,965.96 | XHG×612, AVAH×104, ARE×26, BMO×8, BYND×227, USDE×553, SUJA×342, BE×4, ABCL×84, AQST×203, NEM×7 | $12,375.11 | +247.53 | +299.13 | SLI, RRC, PGY, CRK | XHG, BYND, USDE, SUJA | $13.91 | $12,616.32 | AVAH×104, ARE×26, BMO×8, BE×4, ABCL×84, AQST×203, NEM×7, SLI×1967, RRC×123, PGY×223, CRK×353 |
| 2026-08-28 | +0.75 | $13.91 | AVAH×104, ARE×26, BMO×8, BE×4, ABCL×84, AQST×203, NEM×7, SLI×1967, RRC×123, PGY×223, CRK×353 | $12,707.07 | +90.75 | +318.39 | BYND, CAPR, MRNA, ANF, SIMO, FIG | AVAH, ARE, BMO, SLI, RRC, PGY, CRK | $10,137.47 | $12,968.25 | BE×4, ABCL×84, AQST×203, NEM×7, BYND×206, CAPR×297, MRNA×21, ANF×19, SIMO×10, FIG×84 |
| 2026-08-31 | -5.85 | $10,137.47 | BE×4, ABCL×84, AQST×203, NEM×7, BYND×206, CAPR×297, MRNA×21, ANF×19, SIMO×10, FIG×84 | $12,973.61 | +5.36 | -93.72 | — | BE, ABCL, AQST, NEM, CAPR, MRNA, ANF | $14,900.71 | $12,862.95 | BYND×206, SIMO×10, FIG×84 |
| 2026-09-01 | -6.30 | $14,900.71 | BYND×206, SIMO×10, FIG×84 | $12,913.01 | +50.06 | +15.64 | — | BYND | $17,584.23 | $12,925.93 | SIMO×10, FIG×84 |
| 2026-09-02 | -3.83 | $17,584.23 | SIMO×10, FIG×84 | $12,977.61 | +51.68 | +0.00 | — | SIMO, FIG | $12,973.35 | $12,973.35 | — |
| 2026-09-03 | -0.90 | $12,973.35 | — | $12,973.35 | -0.00 | -358.20 | GPRO, REAX, CNH, MMED, SLN, OPK | — | $7,742.01 | $12,574.20 | GPRO×1275, REAX×123, CNH×165, MMED×95, SLN×130, OPK×1136 |
| 2026-09-04 | +2.25 | $7,742.01 | GPRO×1275, REAX×123, CNH×165, MMED×95, SLN×130, OPK×1136 | $12,709.97 | +135.77 | +550.42 | ASST, USDE, DFDV, GSM, PIPR | REAX, CNH, MMED | $8,678.97 | $13,229.33 | GPRO×1275, SLN×130, OPK×1136, ASST×134, USDE×430, DFDV×585, GSM×467, PIPR×28 |
| 2026-09-08 | -11.47 | $8,678.97 | GPRO×1275, SLN×130, OPK×1136, ASST×134, USDE×430, DFDV×585, GSM×467, PIPR×28 | $12,885.91 | -343.42 | +204.75 | — | GPRO, ASST, USDE, DFDV | $20,920.52 | $13,058.22 | SLN×130, OPK×1136, GSM×467, PIPR×28 |
| 2026-09-09 | -13.95 | $20,920.52 | SLN×130, OPK×1136, GSM×467, PIPR×28 | $13,084.08 | +25.86 | +21.85 | — | SLN, OPK | $17,340.61 | $13,088.90 | GSM×467, PIPR×28 |
| 2026-09-10 | -13.28 | $17,340.61 | GSM×467, PIPR×28 | $13,088.90 | -0.00 | +0.00 | — | GSM, PIPR | $13,080.80 | $13,080.80 | — |

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
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 73 | $24.68 | $2.21 | — | $8,563.07 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; combo leftover $1814.21; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 92 | $19.57 | $2.27 | — | $6,760.37 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $1814.21; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 824 | $2.20 | $10.63 | — | $4,936.94 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; combo leftover $1814.21; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 163 | $11.12 | $2.48 | — | $3,121.90 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $1814.21; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 881 | $1.18 | $11.55 | — | $4,149.93 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $1040.63; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 54 | $19.17 | $2.20 | — | $5,182.91 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $1040.63; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 81 | $12.70 | $2.28 | — | $6,208.92 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $1040.63; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,208.92 | ▼ close $10,157.21 vs 09:30 $10,412.10 (session -176.10) | 16:00 close · cash $6,208.92 · equity $10,157.21 vs 09:30 $10,412.10 (-254.89; session marks -176.10) · 7 name(s) marked open→close (per-name table). QMCO×73 09:30 $24.68 → close $26.11 +104.39; ARX×92 09:30 $19.57 → close $19.58 +0.92; ZENA×824 09:30 $2.20 → close $2.14 -49.44; AIRO×163 09:30 $11.12 → close $9.57 -252.65; EU×881 09:30 $1.18 → close $1.21 -26.43; LUNR×54 09:30 $19.17 → close $19.01 +8.64; OWL×81 09:30 $12.70 → close $12.22 +38.47 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,208.92 | ▼ 09:30 equity $9,958.67 vs yday $10,157.21 (-198.54) | 09:30 open · cash $6,208.92 (unchanged overnight, no fees) · equity $9,958.67 vs prior close $10,157.21 (-198.54) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 73 | $24.83 | $2.24 | $+6.51 | $8,019.27 | ▲ +6.51 after sell → book $9,956.43; vs 09:30 mark -2.24 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 92 | $19.57 | $2.30 | $-4.56 | $9,817.42 | ▼ -4.56 after sell → book $9,954.14; vs 09:30 mark -2.29 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 824 | $2.08 | $10.78 | $-116.17 | $11,524.68 | ▼ -116.17 after sell → book $9,943.36; vs 09:30 mark -10.78 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 163 | $9.57 | $2.52 | $-257.65 | $13,082.07 | ▼ -257.65 after sell → book $9,940.84; vs 09:30 mark -2.52 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 546 | $4.19 | $7.04 | — | $10,787.28 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $2289.36; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 333 | $6.87 | $4.30 | — | $8,495.28 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $2289.36; owner union_hot_n4_h1 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 167 | $13.64 | $2.49 | — | $6,214.91 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $2289.36; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 55 | $41.23 | $2.15 | — | $3,945.10 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $2289.36; owner union_hot_n4_h1 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 857 | $1.15 | $11.23 | — | $4,919.42 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $986.28; owner short_news_r_h3 | join🟡 sector🟢 gen🟢 news🔴 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 277 | $3.56 | $3.66 | — | $5,901.88 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $986.28; owner short_news_r_h3 | join🟡 sector🔴 gen🟢 news🔴 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 31 | $31.70 | $2.13 | — | $6,882.45 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; combo leftover $986.28; owner short_news_r_h3 | join🟡 sector🔴 gen🟢 news🔴 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 327 | $3.01 | $4.31 | — | $7,862.41 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $986.28; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,862.41 | ▲ close $9,998.01 vs 09:30 $9,958.67 (session +94.48) | 16:00 close · cash $7,862.41 · equity $9,998.01 vs 09:30 $9,958.67 (+39.34; session marks +94.48) · 11 name(s) marked open→close (per-name table). EU×881 09:30 $1.21 → close $1.13 +70.48; LUNR×54 09:30 $20.25 → close $20.38 -7.02; OWL×81 09:30 $12.12 → close $11.66 +37.26; XHG×546 09:30 $4.19 → close $3.91 -152.88; CAPR×333 09:30 $6.87 → close $7.45 +193.14; STDN×167 09:30 $13.64 → close $13.31 -55.11; HTFL×55 09:30 $41.23 → close $41.94 +39.05; VERI×857 09:30 $1.15 → close $1.08 +55.70; ZNTL×277 09:30 $3.56 → close $3.71 -40.17; APMD×31 09:30 $31.70 → close $32.55 -26.35; HIVE×327 09:30 $3.01 → close $3.07 -19.62 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,862.41 | ▲ 09:30 equity $10,118.54 vs yday $9,998.01 (+120.53) | 09:30 open · cash $7,862.41 (unchanged overnight, no fees) · equity $10,118.54 vs prior close $9,998.01 (+120.53) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 546 | $3.94 | $7.15 | $-150.69 | $10,006.50 | ▼ -150.69 after sell → book $10,111.39; vs 09:30 mark -7.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 167 | $13.31 | $2.54 | $-60.14 | $12,226.73 | ▼ -60.14 after sell → book $10,108.85; vs 09:30 mark -2.54 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 55 | $41.50 | $2.18 | $+10.51 | $14,507.05 | ▲ +10.51 after sell → book $10,106.67; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,507.05 | ▲ close $10,173.25 vs 09:30 $10,118.54 (session +66.58) | 16:00 close · cash $14,507.05 · equity $10,173.25 vs 09:30 $10,118.54 (+54.71; session marks +66.58) · 8 name(s) marked open→close (per-name table). EU×881 09:30 $1.13 → close $1.07 +52.86; LUNR×54 09:30 $19.31 → close $19.31 -0.00; OWL×81 09:30 $11.54 → close $11.59 -4.05; CAPR×333 09:30 $7.50 → close $7.08 -139.86; VERI×857 09:30 $1.05 → close $0.99 +47.14; ZNTL×277 09:30 $3.75 → close $3.68 +19.39; APMD×31 09:30 $32.85 → close $31.81 +32.24; HIVE×327 09:30 $2.96 → close $2.78 +58.86 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,507.05 | ▲ 09:30 equity $10,178.37 vs yday $10,173.25 (+5.12) | 09:30 open · cash $14,507.05 (unchanged overnight, no fees) · equity $10,178.37 vs prior close $10,173.25 (+5.12) | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 881 | $1.07 | $11.36 | $+74.00 | $13,553.02 | ▲ +74.00 after sell → book $10,167.01; vs 09:30 mark -11.36 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 81 | $11.75 | $2.23 | $+72.03 | $12,599.03 | ▲ +72.03 after sell → book $10,164.77; vs 09:30 mark -2.24 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 333 | $7.19 | $4.37 | $+97.89 | $14,988.93 | ▲ +97.89 after sell → book $10,160.40; vs 09:30 mark -4.37 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,988.93 | ▲ close $10,187.78 vs 09:30 $10,178.37 (session +27.38) | 16:00 close · cash $14,988.93 · equity $10,187.78 vs 09:30 $10,178.37 (+9.41; session marks +27.38) · 5 name(s) marked open→close (per-name table). LUNR×54 09:30 $18.98 → close $18.52 +24.84; VERI×857 09:30 $1.00 → close $0.97 +29.14; ZNTL×277 09:30 $3.76 → close $3.82 -16.62; APMD×31 09:30 $32.13 → close $32.03 +3.10; HIVE×327 09:30 $2.78 → close $2.82 -13.08 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,988.93 | ▼ 09:30 equity $10,119.85 vs yday $10,187.78 (-67.93) | 09:30 open · cash $14,988.93 (unchanged overnight, no fees) · equity $10,119.85 vs prior close $10,187.78 (-67.93) | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 54 | $18.13 | $2.15 | $+51.81 | $14,007.76 | ▲ +51.81 after sell → book $10,117.70; vs 09:30 mark -2.15 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 857 | $0.96 | $10.82 | $+138.20 | $13,171.65 | ▲ +138.20 after sell → book $10,106.87; vs 09:30 mark -10.83 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 277 | $4.01 | $3.57 | $-133.26 | $12,055.92 | ▼ -133.26 after sell → book $10,103.30; vs 09:30 mark -3.57 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 31 | $31.87 | $2.08 | $-9.48 | $11,065.87 | ▼ -9.48 after sell → book $10,101.22; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 327 | $2.95 | $4.22 | $+11.09 | $10,097.00 | ▲ +11.09 after sell → book $10,097.00; vs 09:30 mark -4.22 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 149 | $11.81 | $2.44 | — | $8,334.12 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1766.97; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 11 | $150.14 | $2.02 | — | $6,680.56 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $1766.97; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1536 | $1.15 | $19.81 | — | $4,894.35 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $1766.97; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 1289 | $1.37 | $16.63 | — | $3,111.79 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $1766.97; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 2 | $204.45 | $2.03 | — | $3,518.66 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $444.54; owner short_news_r_h3 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 20 | $21.40 | $2.08 | — | $3,944.58 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $444.54; owner short_news_r_h3 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 100 | $4.43 | $2.33 | — | $4,385.25 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; combo leftover $444.54; owner short_news_r_h3 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 2 | $173.90 | $2.03 | — | $4,731.03 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $444.54; owner short_news_r_h3 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 9 | $46.85 | $2.05 | — | $5,150.63 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; combo leftover $444.54; owner short_news_r_h3 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 4 | $106.38 | $2.03 | — | $5,574.11 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $444.54; owner short_news_r_h3 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 96 | $4.61 | $2.32 | — | $6,014.36 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $444.54; owner short_news_r_h3 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,014.36 | ▼ close $10,011.82 vs 09:30 $10,119.85 (session -29.41) | 16:00 close · cash $6,014.36 · equity $10,011.82 vs 09:30 $10,119.85 (-108.03; session marks -29.41) · 11 name(s) marked open→close (per-name table). ABCL×149 09:30 $11.81 → close $11.57 -36.50; MRNA×11 09:30 $150.14 → close $133.32 -185.02; CYPH×1536 09:30 $1.15 → close $1.19 +61.44; AZI×1289 09:30 $1.37 → close $1.44 +90.23; AEM×2 09:30 $204.45 → close $212.04 -15.18; WYFI×20 09:30 $21.40 → close $21.16 +4.80; TOYO×100 09:30 $4.43 → close $4.51 -8.50; TEAM×2 09:30 $173.90 → close $174.91 -2.02; AAP×9 09:30 $46.85 → close $42.39 +40.14; WMT×4 09:30 $106.38 → close $103.84 +10.16; AQST×96 09:30 $4.61 → close $4.50 +11.04 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,014.36 | ▲ 09:30 equity $10,199.83 vs yday $10,011.82 (+188.01) | 09:30 open · cash $6,014.36 (unchanged overnight, no fees) · equity $10,199.83 vs prior close $10,011.82 (+188.01) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 149 | $11.57 | $2.48 | $-41.42 | $7,735.81 | ▼ -41.42 after sell → book $10,197.35; vs 09:30 mark -2.48 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 1289 | $1.46 | $16.86 | $+82.52 | $9,600.90 | ▲ +82.52 after sell → book $10,180.50; vs 09:30 mark -16.85 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 748 | $4.49 | $9.65 | — | $6,232.73 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $3360.31; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 493 | $6.81 | $6.36 | — | $2,869.04 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $3360.31; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 184 | $3.11 | $2.60 | — | $3,438.68 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $573.81; owner short_news_r_h3 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 6 | $89.10 | $2.04 | — | $3,971.24 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $573.81; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 14 | $38.40 | $2.07 | — | $4,506.77 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $573.81; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 27 | $20.90 | $2.11 | — | $5,068.96 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $573.81; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 21 | $27.00 | $2.09 | — | $5,633.87 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $573.81; owner short_news_r_h3 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,633.87 | ▲ close $10,153.69 vs 09:30 $10,199.83 (session +0.11) | 16:00 close · cash $5,633.87 · equity $10,153.69 vs 09:30 $10,199.83 (-46.14; session marks +0.11) · 16 name(s) marked open→close (per-name table). MRNA×11 09:30 $133.11 → close $145.13 +132.22; CYPH×1536 09:30 $1.32 → close $1.42 +153.60; AEM×2 09:30 $216.30 → close $216.06 +0.48; WYFI×20 09:30 $21.54 → close $20.72 +16.40; TOYO×100 09:30 $4.68 → close $4.82 -14.00; TEAM×2 09:30 $174.22 → close $171.81 +4.82; AAP×9 09:30 $42.41 → close $42.58 -1.53; WMT×4 09:30 $103.69 → close $103.70 -0.04; AQST×96 09:30 $4.54 → close $4.66 -11.52; XHG×748 09:30 $4.49 → close $4.41 -59.84; CAPR×493 09:30 $6.81 → close $6.29 -256.36; QTRX×184 09:30 $3.11 → close $2.99 +22.08; AUGO×6 09:30 $89.10 → close $87.26 +11.04; SSRM×14 09:30 $38.40 → close $37.77 +8.82; ARIS×27 09:30 $20.90 → close $20.86 +1.08; NOG×21 09:30 $27.00 → close $27.34 -7.14 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,633.87 | ▲ 09:30 equity $11,567.19 vs yday $10,153.69 (+1,413.50) | 09:30 open · cash $5,633.87 (unchanged overnight, no fees) · equity $11,567.19 vs prior close $10,153.69 (+1413.50) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 11 | $142.70 | $2.05 | $-85.91 | $7,201.53 | ▼ -85.91 after sell → book $11,565.15; vs 09:30 mark -2.04 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1536 | $1.83 | $20.09 | $+1004.57 | $9,992.32 | ▲ +1,004.57 after sell → book $11,545.06; vs 09:30 mark -20.09 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 748 | $4.32 | $9.80 | $-146.61 | $13,213.88 | ▼ -146.61 after sell → book $11,535.26; vs 09:30 mark -9.80 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 493 | $8.03 | $6.47 | $+588.63 | $17,166.19 | ▲ +588.63 after sell → book $11,528.78; vs 09:30 mark -6.48 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,166.19 | ▲ close $11,549.25 vs 09:30 $11,567.19 (session +20.47) | 16:00 close · cash $17,166.19 · equity $11,549.25 vs 09:30 $11,567.19 (-17.94; session marks +20.47) · 12 name(s) marked open→close (per-name table). AEM×2 09:30 $217.03 → close $217.89 -1.72; WYFI×20 09:30 $20.01 → close $20.78 -15.40; TOYO×100 09:30 $4.58 → close $4.38 +20.00; TEAM×2 09:30 $169.30 → close $171.33 -4.06; AAP×9 09:30 $43.05 → close $43.63 -5.22; WMT×4 09:30 $104.14 → close $106.49 -9.40; AQST×96 09:30 $4.67 → close $4.80 -12.48; QTRX×184 09:30 $2.99 → close $2.80 +34.96; AUGO×6 09:30 $88.60 → close $87.37 +7.38; SSRM×14 09:30 $38.32 → close $38.61 -4.06; ARIS×27 09:30 $20.98 → close $20.81 +4.59; NOG×21 09:30 $27.12 → close $26.84 +5.88 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,166.19 | ▲ 09:30 equity $11,610.21 vs yday $11,549.25 (+60.96) | 09:30 open · cash $17,166.19 (unchanged overnight, no fees) · equity $11,610.21 vs prior close $11,549.25 (+60.96) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 2 | $212.00 | $2.00 | $-19.12 | $16,740.20 | ▼ -19.12 after sell → book $11,608.22; vs 09:30 mark -1.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 20 | $20.90 | $2.05 | $+5.87 | $16,320.15 | ▲ +5.87 after sell → book $11,606.17; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 100 | $4.42 | $2.29 | $-3.62 | $15,875.86 | ▼ -3.62 after sell → book $11,603.88; vs 09:30 mark -2.29 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 2 | $170.64 | $2.00 | $+2.50 | $15,532.58 | ▲ +2.50 after sell → book $11,601.88; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 9 | $43.63 | $2.02 | $+24.91 | $15,137.90 | ▲ +24.91 after sell → book $11,599.87; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 4 | $105.58 | $2.00 | $-0.84 | $14,713.57 | ▼ -0.84 after sell → book $11,597.86; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 96 | $4.77 | $2.28 | $-19.95 | $14,253.38 | ▼ -19.95 after sell → book $11,595.59; vs 09:30 mark -2.27 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 103 | $24.11 | $2.30 | — | $11,767.75 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $2494.34; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1598 | $1.56 | $20.61 | — | $9,254.25 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $2494.34; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 612 | $4.07 | $7.89 | — | $6,755.52 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $2494.34; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 131 | $19.04 | $2.38 | — | $4,258.89 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $2494.34; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 104 | $13.62 | $2.37 | — | $5,673.52 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1419.63; owner short_news_r_h3 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 26 | $54.51 | $2.13 | — | $7,088.66 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $1419.63; owner short_news_r_h3 | join🔴 sector🟡 gen🟡 news🔴 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 8 | $175.01 | $2.07 | — | $8,486.66 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; combo leftover $1419.63; owner short_news_r_h3 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,486.66 | ▲ close $12,389.76 vs 09:30 $11,610.21 (session +833.94) | 16:00 close · cash $8,486.66 · equity $12,389.76 vs 09:30 $11,610.21 (+779.55; session marks +833.94) · 12 name(s) marked open→close (per-name table). QTRX×184 09:30 $2.80 → close $2.79 +1.84; AUGO×6 09:30 $85.78 → close $90.47 -28.14; SSRM×14 09:30 $37.75 → close $39.21 -20.44; ARIS×27 09:30 $20.45 → close $21.18 -19.71; NOG×21 09:30 $26.06 → close $26.42 -7.56; REAX×103 09:30 $24.11 → close $28.43 +444.96; CYPH×1598 09:30 $1.56 → close $1.64 +127.84; XHG×612 09:30 $4.07 → close $4.02 -30.60; ASST×131 09:30 $19.04 → close $21.39 +307.85; AVAH×104 09:30 $13.62 → close $13.59 +3.64; ARE×26 09:30 $54.51 → close $52.90 +41.86; BMO×8 09:30 $175.01 → close $173.46 +12.40 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,486.66 | ▼ 09:30 equity $11,965.55 vs yday $12,389.76 (-424.21) | 09:30 open · cash $8,486.66 (unchanged overnight, no fees) · equity $11,965.55 vs prior close $12,389.76 (-424.21) | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 184 | $2.83 | $2.54 | $+46.38 | $7,963.40 | ▲ +46.38 after sell → book $11,963.01; vs 09:30 mark -2.54 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 6 | $88.24 | $2.01 | $+1.11 | $7,431.95 | ▲ +1.11 after sell → book $11,961.00; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 14 | $38.41 | $2.03 | $-4.24 | $6,892.18 | ▼ -4.24 after sell → book $11,958.97; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 27 | $20.50 | $2.07 | $+6.62 | $6,336.61 | ▲ +6.62 after sell → book $11,956.90; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 21 | $26.00 | $2.05 | $+16.86 | $5,788.56 | ▲ +16.86 after sell → book $11,954.85; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 103 | $26.61 | $2.34 | $+252.86 | $8,527.05 | ▲ +252.86 after sell → book $11,952.51; vs 09:30 mark -2.34 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 1598 | $1.60 | $20.90 | $+22.41 | $11,062.95 | ▲ +22.41 after sell → book $11,931.61; vs 09:30 mark -20.90 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 131 | $20.72 | $2.43 | $+215.27 | $13,774.84 | ▲ +215.27 after sell → book $11,929.18; vs 09:30 mark -2.43 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 227 | $14.11 | $2.93 | — | $10,568.94 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $3214.13; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 553 | $5.81 | $7.13 | — | $7,348.88 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $3214.13; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SUJA` | 342 | $9.39 | $4.41 | — | $4,133.09 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+39.0; combo leftover $3214.13; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 4 | $213.94 | $2.05 | — | $4,986.80 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $1033.27; owner short_news_r_h3 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 84 | $12.22 | $2.29 | — | $6,010.99 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $1033.27; owner short_news_r_h3 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 203 | $5.08 | $2.69 | — | $7,039.54 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $1033.27; owner short_news_r_h3 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 7 | $132.64 | $2.06 | — | $7,965.96 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $1033.27; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,965.96 | ▲ close $12,127.58 vs 09:30 $11,965.55 (session +221.96) | 16:00 close · cash $7,965.96 · equity $12,127.58 vs 09:30 $11,965.55 (+162.03; session marks +221.96) · 11 name(s) marked open→close (per-name table). XHG×612 09:30 $3.81 → close $4.06 +153.00; AVAH×104 09:30 $13.65 → close $13.62 +3.12; ARE×26 09:30 $52.77 → close $52.97 -5.20; BMO×8 09:30 $173.22 → close $172.90 +2.56; BYND×227 09:30 $14.11 → close $14.25 +31.78; USDE×553 09:30 $5.81 → close $5.98 +94.01; SUJA×342 09:30 $9.39 → close $9.44 +17.10; BE×4 09:30 $213.94 → close $218.21 -17.08; ABCL×84 09:30 $12.22 → close $12.24 -1.68; AQST×203 09:30 $5.08 → close $5.39 -62.93; NEM×7 09:30 $132.64 → close $131.60 +7.28 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,965.96 | ▲ 09:30 equity $12,375.11 vs yday $12,127.58 (+247.53) | 09:30 open · cash $7,965.96 (unchanged overnight, no fees) · equity $12,375.11 vs prior close $12,127.58 (+247.53) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 612 | $4.06 | $8.02 | $-22.03 | $10,442.67 | ▼ -22.03 after sell → book $12,367.10; vs 09:30 mark -8.01 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 227 | $14.20 | $2.99 | $+14.51 | $13,663.07 | ▲ +14.51 after sell → book $12,364.10; vs 09:30 mark -3.00 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 553 | $6.50 | $7.25 | $+367.18 | $17,250.32 | ▲ +367.18 after sell → book $12,356.85; vs 09:30 mark -7.25 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 342 | $9.41 | $4.49 | $-2.07 | $20,464.05 | ▼ -2.07 after sell → book $12,352.36; vs 09:30 mark -4.49 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1967 | $2.60 | $25.37 | — | $15,324.47 | — | top 4 by hot; rank hot_score; list flatten; ret5=+13.0; combo leftover $5116.01; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 123 | $41.44 | $2.36 | — | $10,224.99 | — | top 4 by hot; rank hot_score; list flatten; ret5=+3.1; combo leftover $5116.01; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 223 | $22.93 | $2.88 | — | $5,108.73 | — | top 4 by hot; rank hot_score; list mover_buy; 🔵; ret5=+9.5; combo leftover $5116.01; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 353 | $14.42 | $4.55 | — | $13.91 | — | top 4 by hot; rank hot_score; list flatten; ret5=+7.1; combo leftover $5116.01; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.91 | ▲ close $12,616.32 vs 09:30 $12,375.11 (session +299.13) | 16:00 close · cash $13.91 · equity $12,616.32 vs 09:30 $12,375.11 (+241.21; session marks +299.13) · 11 name(s) marked open→close (per-name table). AVAH×104 09:30 $13.62 → close $13.82 -20.80; ARE×26 09:30 $52.45 → close $52.28 +4.42; BMO×8 09:30 $172.85 → close $172.13 +5.76; BE×4 09:30 $227.10 → close $217.83 +37.08; ABCL×84 09:30 $12.25 → close $12.40 -12.60; AQST×203 09:30 $5.39 → close $5.16 +46.69; NEM×7 09:30 $131.02 → close $132.29 -8.89; SLI×1967 09:30 $2.60 → close $2.64 +78.68; RRC×123 09:30 $41.44 → close $41.64 +24.60; PGY×223 09:30 $22.93 → close $23.26 +73.59; CRK×353 09:30 $14.42 → close $14.62 +70.60 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.91 | ▲ 09:30 equity $12,707.07 vs yday $12,616.32 (+90.75) | 09:30 open · cash $13.91 (unchanged overnight, no fees) · equity $12,707.07 vs prior close $12,616.32 (+90.75) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 104 | $13.90 | $2.30 | $-33.27 | $-1,433.99 | ▼ -33.27 after sell → book $12,704.77; vs 09:30 mark -2.30 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 26 | $52.49 | $2.07 | $+48.32 | $-2,800.80 | ▲ +48.32 after sell → book $12,702.70; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 8 | $172.76 | $2.01 | $+13.91 | $-4,184.89 | ▲ +13.91 after sell → book $12,700.69; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 1967 | $2.68 | $25.74 | $+106.24 | $1,060.92 | ▲ +106.24 after sell → book $12,674.94; vs 09:30 mark -25.75 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 123 | $41.74 | $2.42 | $+32.12 | $6,192.52 | ▲ +32.12 after sell → book $12,672.52; vs 09:30 mark -2.42 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 223 | $23.21 | $2.96 | $+56.61 | $11,365.40 | ▲ +56.61 after sell → book $12,669.57; vs 09:30 mark -2.95 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 353 | $14.63 | $4.65 | $+64.92 | $16,525.14 | ▲ +64.92 after sell → book $12,664.92; vs 09:30 mark -4.65 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 206 | $14.00 | $2.66 | — | $13,638.48 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $2891.90; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 297 | $9.73 | $3.83 | — | $10,744.84 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+47.1; combo leftover $2891.90; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 21 | $137.19 | $2.05 | — | $7,861.79 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+7.1; combo leftover $2891.90; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 19 | $146.07 | $2.05 | — | $5,084.42 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $2891.90; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 10 | $252.24 | $2.12 | — | $7,604.70 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $2542.21; owner short_news_r_h3 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 84 | $30.18 | $2.35 | — | $10,137.47 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $2542.21; owner short_news_r_h3 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,137.47 | ▲ close $12,968.25 vs 09:30 $12,707.07 (session +318.39) | 16:00 close · cash $10,137.47 · equity $12,968.25 vs 09:30 $12,707.07 (+261.18; session marks +318.39) · 10 name(s) marked open→close (per-name table). BE×4 09:30 $215.71 → close $210.77 +19.74; ABCL×84 09:30 $12.30 → close $11.35 +80.22; AQST×203 09:30 $5.11 → close $5.02 +18.27; NEM×7 09:30 $132.35 → close $127.98 +30.59; BYND×206 09:30 $14.00 → close $13.86 -28.84; CAPR×297 09:30 $9.73 → close $9.59 -41.58; MRNA×21 09:30 $137.19 → close $137.99 +16.80; ANF×19 09:30 $146.07 → close $148.42 +44.65; SIMO×10 09:30 $252.24 → close $245.81 +64.30; FIG×84 09:30 $30.18 → close $28.82 +114.24 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,137.47 | ▲ 09:30 equity $12,973.61 vs yday $12,968.25 (+5.36) | 09:30 open · cash $10,137.47 (unchanged overnight, no fees) · equity $12,973.61 vs prior close $12,968.25 (+5.36) | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 4 | $208.88 | $2.00 | $+16.19 | $9,299.95 | ▲ +16.19 after sell → book $12,971.60; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 84 | $11.10 | $2.24 | $+89.54 | $8,365.31 | ▲ +89.54 after sell → book $12,969.36; vs 09:30 mark -2.24 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 203 | $4.97 | $2.62 | $+16.01 | $7,352.76 | ▲ +16.01 after sell → book $12,966.74; vs 09:30 mark -2.62 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 7 | $127.45 | $2.01 | $+32.26 | $6,458.60 | ▲ +32.26 after sell → book $12,964.73; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 297 | $9.50 | $3.90 | $-76.04 | $9,276.20 | ▼ -76.04 after sell → book $12,960.83; vs 09:30 mark -3.90 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 21 | $134.10 | $2.09 | $-69.03 | $12,090.21 | ▼ -69.03 after sell → book $12,958.74; vs 09:30 mark -2.09 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 19 | $148.03 | $2.08 | $+33.11 | $14,900.71 | ▲ +33.11 after sell → book $12,956.67; vs 09:30 mark -2.07 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,900.71 | ▼ close $12,862.95 vs 09:30 $12,973.61 (session -93.72) | 16:00 close · cash $14,900.71 · equity $12,862.95 vs 09:30 $12,973.61 (-110.66; session marks -93.72) · 3 name(s) marked open→close (per-name table). BYND×206 09:30 $13.81 → close $13.30 -105.06; SIMO×10 09:30 $247.05 → close $246.84 +2.10; FIG×84 09:30 $27.60 → close $27.49 +9.24 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,900.71 | ▲ 09:30 equity $12,913.01 vs yday $12,862.95 (+50.06) | 09:30 open · cash $14,900.71 (unchanged overnight, no fees) · equity $12,913.01 vs prior close $12,862.95 (+50.06) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 206 | $13.04 | $2.71 | $-203.13 | $17,584.23 | ▼ -203.13 after sell → book $12,910.29; vs 09:30 mark -2.72 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,584.23 | ▲ close $12,925.93 vs 09:30 $12,913.01 (session +15.64) | 16:00 close · cash $17,584.23 · equity $12,925.93 vs 09:30 $12,913.01 (+12.92; session marks +15.64) · 2 name(s) marked open→close (per-name table). SIMO×10 09:30 $240.09 → close $237.35 +27.40; FIG×84 09:30 $27.06 → close $27.20 -11.76 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,584.23 | ▲ 09:30 equity $12,977.61 vs yday $12,925.93 (+51.68) | 09:30 open · cash $17,584.23 (unchanged overnight, no fees) · equity $12,977.61 vs prior close $12,925.93 (+51.68) | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 10 | $235.71 | $2.02 | $+161.16 | $15,225.11 | ▲ +161.16 after sell → book $12,975.59; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 84 | $26.78 | $2.24 | $+281.01 | $12,973.35 | ▲ +281.01 after sell → book $12,973.35; vs 09:30 mark -2.24 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,973.35 | ▲ close $12,973.35 vs 09:30 $12,977.61 (session +0.00) | 16:00 close · cash $12,973.35 · no lots left · equity $12,973.35. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,973.35 | ▲ 09:30 equity $12,973.35 vs yday $12,973.35 (-0.00) | 09:30 open · cash $12,973.35 (unchanged overnight, no fees) · equity $12,973.35 vs prior close $12,973.35 (-0.00) | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1275 | $1.78 | $16.45 | — | $10,687.40 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $2270.34; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 123 | $18.40 | $2.36 | — | $8,421.84 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $2270.34; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 165 | $13.71 | $2.48 | — | $6,157.21 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $2270.34; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 95 | $23.88 | $2.27 | — | $3,886.33 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $2270.34; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 130 | $14.85 | $2.47 | — | $5,814.36 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $1943.17; owner short_news_r_h3 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1136 | $1.71 | $14.91 | — | $7,742.01 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $1943.17; owner short_news_r_h3 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,742.01 | ▼ close $12,574.20 vs 09:30 $12,973.35 (session -358.20) | 16:00 close · cash $7,742.01 · equity $12,574.20 vs 09:30 $12,973.35 (-399.15; session marks -358.20) · 6 name(s) marked open→close (per-name table). GPRO×1275 09:30 $1.78 → close $1.39 -497.25; REAX×123 09:30 $18.40 → close $18.40 +0.00; CNH×165 09:30 $13.71 → close $13.84 +21.45; MMED×95 09:30 $23.88 → close $23.84 -3.80; SLN×130 09:30 $14.85 → close $14.79 +7.80; OPK×1136 09:30 $1.71 → close $1.61 +113.60 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,742.01 | ▲ 09:30 equity $12,709.97 vs yday $12,574.20 (+135.77) | 09:30 open · cash $7,742.01 (unchanged overnight, no fees) · equity $12,709.97 vs prior close $12,574.20 (+135.77) | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 123 | $18.15 | $2.40 | $-35.51 | $9,972.06 | ▼ -35.51 after sell → book $12,707.57; vs 09:30 mark -2.40 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 165 | $13.89 | $2.53 | $+24.68 | $12,261.38 | ▲ +24.68 after sell → book $12,705.04; vs 09:30 mark -2.53 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 95 | $23.84 | $2.31 | $-8.38 | $14,523.87 | ▼ -8.38 after sell → book $12,702.73; vs 09:30 mark -2.31 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 134 | $25.18 | $2.39 | — | $11,147.36 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $3388.90; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 430 | $7.87 | $5.55 | — | $7,757.72 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $3388.90; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 585 | $5.79 | $7.55 | — | $4,363.02 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $3388.90; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 467 | $4.67 | $6.18 | — | $6,537.73 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $2181.51; owner short_news_r_h3 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 28 | $76.55 | $2.16 | — | $8,678.97 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $2181.51; owner short_news_r_h3 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,678.97 | ▲ close $13,229.33 vs 09:30 $12,709.97 (session +550.42) | 16:00 close · cash $8,678.97 · equity $13,229.33 vs 09:30 $12,709.97 (+519.36; session marks +550.42) · 8 name(s) marked open→close (per-name table). GPRO×1275 09:30 $1.48 → close $1.70 +280.50; SLN×130 09:30 $14.63 → close $14.59 +5.20; OPK×1136 09:30 $1.59 → close $1.64 -56.80; ASST×134 09:30 $25.18 → close $27.14 +262.64; USDE×430 09:30 $7.87 → close $7.93 +25.80; DFDV×585 09:30 $5.79 → close $5.87 +46.80; GSM×467 09:30 $4.67 → close $4.67 -0.00; PIPR×28 09:30 $76.55 → close $77.04 -13.72 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,678.97 | ▼ 09:30 equity $12,885.91 vs yday $13,229.33 (-343.42) | 09:30 open · cash $8,678.97 (unchanged overnight, no fees) · equity $12,885.91 vs prior close $13,229.33 (-343.42) | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 1275 | $1.56 | $16.68 | $-307.25 | $10,657.67 | ▼ -307.25 after sell → book $12,869.23; vs 09:30 mark -16.68 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 134 | $26.44 | $2.44 | $+164.01 | $14,198.19 | ▲ +164.01 after sell → book $12,866.79; vs 09:30 mark -2.44 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 430 | $7.76 | $5.65 | $-58.49 | $17,529.34 | ▼ -58.49 after sell → book $12,861.14; vs 09:30 mark -5.65 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 585 | $5.81 | $7.67 | $-3.52 | $20,920.52 | ▼ -3.52 after sell → book $12,853.47; vs 09:30 mark -7.67 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,920.52 | ▲ close $13,058.22 vs 09:30 $12,885.91 (session +204.75) | 16:00 close · cash $20,920.52 · equity $13,058.22 vs 09:30 $12,885.91 (+172.31; session marks +204.75) · 4 name(s) marked open→close (per-name table). SLN×130 09:30 $14.24 → close $13.69 +71.50; OPK×1136 09:30 $1.63 → close $1.59 +45.44; GSM×467 09:30 $4.75 → close $4.52 +107.41; PIPR×28 09:30 $76.64 → close $77.34 -19.60 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,920.52 | ▲ 09:30 equity $13,084.08 vs yday $13,058.22 (+25.86) | 09:30 open · cash $20,920.52 (unchanged overnight, no fees) · equity $13,084.08 vs prior close $13,058.22 (+25.86) | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 130 | $13.60 | $2.38 | $+157.65 | $19,150.14 | ▲ +157.65 after sell → book $13,081.70; vs 09:30 mark -2.38 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1136 | $1.58 | $14.65 | $+118.11 | $17,340.61 | ▲ +118.11 after sell → book $13,067.05; vs 09:30 mark -14.65 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,340.61 | ▲ close $13,088.90 vs 09:30 $13,084.08 (session +21.85) | 16:00 close · cash $17,340.61 · equity $13,088.90 vs 09:30 $13,084.08 (+4.82; session marks +21.85) · 2 name(s) marked open→close (per-name table). GSM×467 09:30 $4.52 → close $4.49 +14.01; PIPR×28 09:30 $77.24 → close $76.96 +7.84 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,340.61 | ▲ 09:30 equity $13,088.90 vs yday $13,088.90 (-0.00) | 09:30 open · cash $17,340.61 (unchanged overnight, no fees) · equity $13,088.90 vs prior close $13,088.90 (-0.00) | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 467 | $4.49 | $6.02 | $+71.86 | $15,237.75 | ▲ +71.86 after sell → book $13,082.87; vs 09:30 mark -6.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 28 | $76.96 | $2.07 | $-15.71 | $13,080.80 | ▼ -15.71 after sell → book $13,080.80; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,080.80 | ▲ close $13,080.80 vs 09:30 $13,088.90 (session +0.00) | 16:00 close · cash $13,080.80 · no lots left · equity $13,080.80. | — |

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
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new short_news_r_h3 |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new short_news_r_h3 |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new union_hot_n4_h1 |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new union_hot_n4_h1 |
| 2026-09-10 | `PAYP` | hard_red | hard-red S=-13.28 sit; no new union_hot_n4_h1 |
| 2026-09-10 | `CRWV` | hard_red | hard-red S=-13.28 sit; no new union_hot_n4_h1 |
