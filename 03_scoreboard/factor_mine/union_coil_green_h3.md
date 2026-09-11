# Factor mine action — `union_coil_green_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-5.53%** ($9,447) · signal-only (no cash/fees) was -9.82%. Starts YES **0/20**. Fills 80 · skips 177 · realized $-553.32.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the last finished bar was green (closed up).
- Must-have: prior 5-session return is at least 0%.
- Must-have: prior 5-session return is at most 10% (not already exploded).
- Must-have: prior relative volume is at least 0.7.
- Must-have: prior relative volume is at most 2.2 (not a blow-off).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,446.67.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TPG` | 197 | — | $50.62 | +0.00 | $54.62 | +787.37 | +787.37 | +0.00 | +787.37 |
| 2026-08-14 | `TPG` | 197 | $54.62 | $55.29 | +131.99 | $53.03 | -445.22 | -313.23 | +919.36 | +474.14 |
| 2026-08-14 | `LDI` | 3 | — | $0.94 | +0.00 | $0.90 | -0.12 | -0.12 | +0.00 | -0.12 |
| 2026-08-14 | `BTBT` | 2 | — | $1.50 | +0.00 | $1.57 | +0.14 | +0.14 | +0.00 | +0.14 |
| 2026-08-17 | `TPG` | 197 | $53.03 | $52.67 | -70.92 | $51.77 | -177.30 | -248.22 | +403.22 | +225.92 |
| 2026-08-17 | `LDI` | 3 | $0.90 | $0.91 | +0.03 | $0.88 | -0.10 | -0.07 | -0.09 | -0.19 |
| 2026-08-17 | `BTBT` | 2 | $1.57 | $1.52 | -0.10 | $1.60 | +0.16 | +0.06 | +0.04 | +0.20 |
| 2026-08-18 | `TPG` | 197 | $51.77 | $51.77 | +0.00 | — | +0.00 | +0.00 | +225.92 | — |
| 2026-08-18 | `LDI` | 3 | $0.88 | $0.87 | -0.02 | $0.86 | -0.04 | -0.06 | -0.20 | -0.24 |
| 2026-08-18 | `BTBT` | 2 | $1.60 | $1.54 | -0.12 | $1.45 | -0.18 | -0.30 | +0.08 | -0.10 |
| 2026-08-19 | `LDI` | 3 | $0.86 | $0.88 | +0.07 | — | +0.00 | +0.07 | -0.17 | — |
| 2026-08-19 | `BTBT` | 2 | $1.45 | $1.42 | -0.06 | — | +0.00 | -0.06 | -0.16 | — |
| 2026-08-20 | `AG` | 62 | — | $20.55 | +0.00 | $21.19 | +39.68 | +39.68 | +0.00 | +39.68 |
| 2026-08-20 | `HDSN` | 221 | — | $5.77 | +0.00 | $5.57 | -44.20 | -44.20 | +0.00 | -44.20 |
| 2026-08-20 | `IAG` | 65 | — | $19.63 | +0.00 | $20.50 | +56.55 | +56.55 | +0.00 | +56.55 |
| 2026-08-20 | `KGC` | 43 | — | $29.63 | +0.00 | $31.43 | +77.40 | +77.40 | +0.00 | +77.40 |
| 2026-08-20 | `NFGC` | 730 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `DNA` | 171 | — | $7.45 | +0.00 | $6.96 | -83.79 | -83.79 | +0.00 | -83.79 |
| 2026-08-20 | `EXK` | 118 | — | $10.77 | +0.00 | $10.97 | +23.60 | +23.60 | +0.00 | +23.60 |
| 2026-08-20 | `SCZM` | 134 | — | $9.46 | +0.00 | $9.76 | +40.20 | +40.20 | +0.00 | +40.20 |
| 2026-08-21 | `AG` | 62 | $21.19 | $21.90 | +44.02 | $21.09 | -50.22 | -6.20 | +83.70 | +33.48 |
| 2026-08-21 | `HDSN` | 221 | $5.57 | $5.67 | +22.10 | $5.63 | -8.84 | +13.26 | -22.10 | -30.94 |
| 2026-08-21 | `IAG` | 65 | $20.50 | $21.17 | +43.55 | $21.14 | -1.95 | +41.60 | +100.10 | +98.15 |
| 2026-08-21 | `KGC` | 43 | $31.43 | $32.17 | +31.82 | $32.76 | +25.37 | +57.19 | +109.22 | +134.59 |
| 2026-08-21 | `NFGC` | 730 | $1.75 | $1.79 | +29.20 | $1.84 | +36.50 | +65.70 | +29.20 | +65.70 |
| 2026-08-21 | `DNA` | 171 | $6.96 | $7.09 | +22.23 | $7.40 | +53.01 | +75.24 | -61.56 | -8.55 |
| 2026-08-21 | `EXK` | 118 | $10.97 | $11.34 | +43.66 | $10.62 | -84.96 | -41.30 | +67.26 | -17.70 |
| 2026-08-21 | `SCZM` | 134 | $9.76 | $10.26 | +67.00 | $9.68 | -78.39 | -11.39 | +107.20 | +28.81 |
| 2026-08-24 | `AG` | 62 | $21.09 | $21.30 | +13.02 | $20.83 | -29.14 | -16.12 | +46.50 | +17.36 |
| 2026-08-24 | `HDSN` | 221 | $5.63 | $5.69 | +13.26 | $5.52 | -37.57 | -24.31 | -17.68 | -55.25 |
| 2026-08-24 | `IAG` | 65 | $21.14 | $21.38 | +15.60 | $21.80 | +27.30 | +42.90 | +113.75 | +141.05 |
| 2026-08-24 | `KGC` | 43 | $32.76 | $33.03 | +11.61 | $32.98 | -2.15 | +9.46 | +146.20 | +144.05 |
| 2026-08-24 | `NFGC` | 730 | $1.84 | $1.86 | +14.60 | $1.90 | +29.20 | +43.80 | +80.30 | +109.50 |
| 2026-08-24 | `DNA` | 171 | $7.40 | $7.25 | -25.65 | $6.78 | -80.37 | -106.02 | -34.20 | -114.57 |
| 2026-08-24 | `EXK` | 118 | $10.62 | $10.97 | +41.30 | $10.76 | -24.78 | +16.52 | +23.60 | -1.18 |
| 2026-08-24 | `SCZM` | 134 | $9.68 | $9.76 | +10.72 | $9.57 | -24.79 | -14.07 | +39.53 | +14.74 |
| 2026-08-25 | `AG` | 62 | $20.83 | $20.32 | -31.62 | — | +0.00 | -31.62 | -14.26 | — |
| 2026-08-25 | `HDSN` | 221 | $5.52 | $5.53 | +2.21 | — | +0.00 | +2.21 | -53.04 | — |
| 2026-08-25 | `IAG` | 65 | $21.80 | $21.21 | -38.35 | — | +0.00 | -38.35 | +102.70 | — |
| 2026-08-25 | `KGC` | 43 | $32.98 | $32.32 | -28.38 | — | +0.00 | -28.38 | +115.67 | — |
| 2026-08-25 | `NFGC` | 730 | $1.90 | $1.90 | +0.00 | — | +0.00 | +0.00 | +109.50 | — |
| 2026-08-25 | `DNA` | 171 | $6.78 | $6.94 | +27.36 | — | +0.00 | +27.36 | -87.21 | — |
| 2026-08-25 | `EXK` | 118 | $10.76 | $10.44 | -37.76 | — | +0.00 | -37.76 | -38.94 | — |
| 2026-08-25 | `SCZM` | 134 | $9.57 | $9.45 | -16.08 | — | +0.00 | -16.08 | -1.34 | — |
| 2026-08-25 | `KURA` | 151 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `LIFE` | 55 | — | $36.96 | +0.00 | $38.56 | +88.00 | +88.00 | +0.00 | +88.00 |
| 2026-08-25 | `ETON` | 31 | — | $64.55 | +0.00 | $63.05 | -46.50 | -46.50 | +0.00 | -46.50 |
| 2026-08-25 | `ANRO` | 56 | — | $36.52 | +0.00 | $36.31 | -11.76 | -11.76 | +0.00 | -11.76 |
| 2026-08-25 | `VALE` | 137 | — | $15.01 | +0.00 | $15.33 | +43.84 | +43.84 | +0.00 | +43.84 |
| 2026-08-26 | `KURA` | 151 | $13.59 | $13.63 | +6.04 | $13.06 | -86.07 | -80.03 | +6.04 | -80.03 |
| 2026-08-26 | `LIFE` | 55 | $38.56 | $38.24 | -17.60 | $39.11 | +47.85 | +30.25 | +70.40 | +118.25 |
| 2026-08-26 | `ETON` | 31 | $63.05 | $63.60 | +17.05 | $62.62 | -30.38 | -13.33 | -29.45 | -59.83 |
| 2026-08-26 | `ANRO` | 56 | $36.31 | $35.80 | -28.56 | $36.53 | +40.88 | +12.32 | -40.32 | +0.56 |
| 2026-08-26 | `VALE` | 137 | $15.33 | $15.37 | +5.48 | $15.16 | -28.77 | -23.29 | +49.32 | +20.55 |
| 2026-08-26 | `CRMD` | 1 | — | $8.60 | +0.00 | $8.39 | -0.21 | -0.21 | +0.00 | -0.21 |
| 2026-08-26 | `RZLT` | 2 | — | $5.01 | +0.00 | $5.04 | +0.06 | +0.06 | +0.00 | +0.06 |
| 2026-08-26 | `SENS` | 1 | — | $9.48 | +0.00 | $9.34 | -0.14 | -0.14 | +0.00 | -0.14 |
| 2026-08-26 | `ACRS` | 1 | — | $6.53 | +0.00 | $6.19 | -0.34 | -0.34 | +0.00 | -0.34 |
| 2026-08-26 | `TMCI` | 2 | — | $4.78 | +0.00 | $4.72 | -0.12 | -0.12 | +0.00 | -0.12 |
| 2026-08-26 | `CRDL` | 6 | — | $2.03 | +0.00 | $2.14 | +0.66 | +0.66 | +0.00 | +0.66 |
| 2026-08-26 | `LI` | 1 | — | $12.14 | +0.00 | $12.14 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-27 | `KURA` | 151 | $13.06 | $12.98 | -12.08 | $13.18 | +30.20 | +18.12 | -92.11 | -61.91 |
| 2026-08-27 | `LIFE` | 55 | $39.11 | $39.40 | +15.95 | $39.44 | +2.20 | +18.15 | +134.20 | +136.40 |
| 2026-08-27 | `ETON` | 31 | $62.62 | $62.50 | -3.72 | $62.67 | +5.27 | +1.55 | -63.55 | -58.28 |
| 2026-08-27 | `ANRO` | 56 | $36.53 | $36.08 | -25.20 | $36.30 | +12.32 | -12.88 | -24.64 | -12.32 |
| 2026-08-27 | `VALE` | 137 | $15.16 | $15.17 | +1.37 | $15.31 | +19.18 | +20.55 | +21.92 | +41.10 |
| 2026-08-27 | `CRMD` | 1 | $8.39 | $8.49 | +0.10 | $8.31 | -0.18 | -0.08 | -0.11 | -0.29 |
| 2026-08-27 | `RZLT` | 2 | $5.04 | $5.07 | +0.06 | $4.98 | -0.18 | -0.12 | +0.12 | -0.06 |
| 2026-08-27 | `SENS` | 1 | $9.34 | $9.33 | -0.01 | $9.40 | +0.07 | +0.06 | -0.15 | -0.08 |
| 2026-08-27 | `ACRS` | 1 | $6.19 | $6.15 | -0.04 | $6.16 | +0.01 | -0.03 | -0.38 | -0.37 |
| 2026-08-27 | `TMCI` | 2 | $4.72 | $4.72 | +0.00 | $4.60 | -0.24 | -0.24 | -0.12 | -0.36 |
| 2026-08-27 | `CRDL` | 6 | $2.14 | $2.09 | -0.30 | $2.06 | -0.18 | -0.48 | +0.36 | +0.18 |
| 2026-08-27 | `LI` | 1 | $12.14 | $12.35 | +0.21 | $12.18 | -0.17 | +0.04 | +0.21 | +0.04 |
| 2026-08-28 | `KURA` | 151 | $13.18 | $13.05 | -19.63 | — | +0.00 | -19.63 | -81.54 | — |
| 2026-08-28 | `LIFE` | 55 | $39.44 | $39.60 | +8.80 | — | +0.00 | +8.80 | +145.20 | — |
| 2026-08-28 | `ETON` | 31 | $62.67 | $61.98 | -21.39 | — | +0.00 | -21.39 | -79.67 | — |
| 2026-08-28 | `ANRO` | 56 | $36.30 | $35.00 | -72.80 | — | +0.00 | -72.80 | -85.12 | — |
| 2026-08-28 | `VALE` | 137 | $15.31 | $15.28 | -4.11 | — | +0.00 | -4.11 | +36.99 | — |
| 2026-08-28 | `CRMD` | 1 | $8.31 | $8.28 | -0.03 | $8.30 | +0.02 | -0.01 | -0.32 | -0.30 |
| 2026-08-28 | `RZLT` | 2 | $4.98 | $4.95 | -0.06 | $4.62 | -0.66 | -0.72 | -0.12 | -0.78 |
| 2026-08-28 | `SENS` | 1 | $9.40 | $9.39 | -0.01 | $9.33 | -0.06 | -0.07 | -0.09 | -0.15 |
| 2026-08-28 | `ACRS` | 1 | $6.16 | $6.10 | -0.06 | $6.01 | -0.09 | -0.15 | -0.43 | -0.52 |
| 2026-08-28 | `TMCI` | 2 | $4.60 | $4.65 | +0.10 | $4.65 | +0.00 | +0.10 | -0.26 | -0.26 |
| 2026-08-28 | `CRDL` | 6 | $2.06 | $2.06 | +0.00 | $1.94 | -0.72 | -0.72 | +0.18 | -0.54 |
| 2026-08-28 | `LI` | 1 | $12.18 | $12.32 | +0.14 | $12.23 | -0.09 | +0.05 | +0.18 | +0.09 |
| 2026-08-28 | `CRK` | 86 | — | $14.63 | +0.00 | $14.29 | -29.24 | -29.24 | +0.00 | -29.24 |
| 2026-08-28 | `EQ` | 515 | — | $2.46 | +0.00 | $2.39 | -36.05 | -36.05 | +0.00 | -36.05 |
| 2026-08-28 | `FIGR` | 33 | — | $37.49 | +0.00 | $36.05 | -47.52 | -47.52 | +0.00 | -47.52 |
| 2026-08-28 | `TH` | 66 | — | $19.00 | +0.00 | $18.55 | -29.70 | -29.70 | +0.00 | -29.70 |
| 2026-08-28 | `FSM` | 98 | — | $12.84 | +0.00 | $12.26 | -56.84 | -56.84 | +0.00 | -56.84 |
| 2026-08-28 | `ADSK` | 4 | — | $261.16 | +0.00 | $260.66 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-08-28 | `FRO` | 28 | — | $44.40 | +0.00 | $44.19 | -5.88 | -5.88 | +0.00 | -5.88 |
| 2026-08-28 | `HAFN` | 151 | — | $8.35 | +0.00 | $8.47 | +18.12 | +18.12 | +0.00 | +18.12 |
| 2026-08-31 | `CRMD` | 1 | $8.30 | $8.26 | -0.04 | — | +0.00 | -0.04 | -0.34 | — |
| 2026-08-31 | `RZLT` | 2 | $4.62 | $4.65 | +0.06 | — | +0.00 | +0.06 | -0.72 | — |
| 2026-08-31 | `SENS` | 1 | $9.33 | $9.29 | -0.04 | — | +0.00 | -0.04 | -0.19 | — |
| 2026-08-31 | `ACRS` | 1 | $6.01 | $5.97 | -0.04 | — | +0.00 | -0.04 | -0.56 | — |
| 2026-08-31 | `TMCI` | 2 | $4.65 | $4.60 | -0.10 | — | +0.00 | -0.10 | -0.36 | — |
| 2026-08-31 | `CRDL` | 6 | $1.94 | $1.92 | -0.12 | — | +0.00 | -0.12 | -0.66 | — |
| 2026-08-31 | `LI` | 1 | $12.23 | $12.28 | +0.05 | — | +0.00 | +0.05 | +0.14 | — |
| 2026-08-31 | `CRK` | 86 | $14.29 | $14.54 | +21.50 | $14.43 | -9.46 | +12.04 | -7.74 | -17.20 |
| 2026-08-31 | `EQ` | 515 | $2.39 | $2.39 | +0.00 | $2.27 | -61.80 | -61.80 | -36.05 | -97.85 |
| 2026-08-31 | `FIGR` | 33 | $36.05 | $35.77 | -9.24 | $36.69 | +30.36 | +21.12 | -56.76 | -26.40 |
| 2026-08-31 | `TH` | 66 | $18.55 | $18.12 | -28.05 | $18.52 | +26.07 | -1.98 | -57.75 | -31.68 |
| 2026-08-31 | `FSM` | 98 | $12.26 | $12.26 | +0.00 | $12.08 | -17.64 | -17.64 | -56.84 | -74.48 |
| 2026-08-31 | `ADSK` | 4 | $260.66 | $257.71 | -11.80 | $258.53 | +3.28 | -8.52 | -13.80 | -10.52 |
| 2026-08-31 | `FRO` | 28 | $44.19 | $44.85 | +18.48 | $43.78 | -29.96 | -11.48 | +12.60 | -17.36 |
| 2026-08-31 | `HAFN` | 151 | $8.47 | $8.53 | +9.06 | $8.44 | -13.59 | -4.53 | +27.18 | +13.59 |
| 2026-09-01 | `CRK` | 86 | $14.43 | $15.82 | +119.54 | $16.02 | +17.20 | +136.74 | +102.34 | +119.54 |
| 2026-09-01 | `EQ` | 515 | $2.27 | $2.25 | -10.30 | $2.21 | -20.60 | -30.90 | -108.15 | -128.75 |
| 2026-09-01 | `FIGR` | 33 | $36.69 | $35.46 | -40.59 | $33.70 | -58.08 | -98.67 | -66.99 | -125.07 |
| 2026-09-01 | `TH` | 66 | $18.52 | $18.45 | -4.62 | $18.07 | -25.08 | -29.70 | -36.30 | -61.38 |
| 2026-09-01 | `FSM` | 98 | $12.08 | $11.67 | -40.18 | $11.75 | +7.84 | -32.34 | -114.66 | -106.82 |
| 2026-09-01 | `ADSK` | 4 | $258.53 | $253.48 | -20.20 | $247.69 | -23.16 | -43.36 | -30.72 | -53.88 |
| 2026-09-01 | `FRO` | 28 | $43.78 | $44.39 | +17.08 | $44.32 | -1.96 | +15.12 | -0.28 | -2.24 |
| 2026-09-01 | `HAFN` | 151 | $8.44 | $8.56 | +18.12 | $8.59 | +4.53 | +22.65 | +31.71 | +36.24 |
| 2026-09-02 | `CRK` | 86 | $16.02 | $15.70 | -27.52 | — | +0.00 | -27.52 | +92.02 | — |
| 2026-09-02 | `EQ` | 515 | $2.21 | $2.20 | -5.15 | — | +0.00 | -5.15 | -133.90 | — |
| 2026-09-02 | `FIGR` | 33 | $33.70 | $33.31 | -12.87 | — | +0.00 | -12.87 | -137.94 | — |
| 2026-09-02 | `TH` | 66 | $18.07 | $17.98 | -5.94 | — | +0.00 | -5.94 | -67.32 | — |
| 2026-09-02 | `FSM` | 98 | $11.75 | $12.08 | +32.34 | — | +0.00 | +32.34 | -74.48 | — |
| 2026-09-02 | `ADSK` | 4 | $247.69 | $246.70 | -3.96 | — | +0.00 | -3.96 | -57.84 | — |
| 2026-09-02 | `FRO` | 28 | $44.32 | $44.17 | -4.20 | — | +0.00 | -4.20 | -6.44 | — |
| 2026-09-02 | `HAFN` | 151 | $8.59 | $8.58 | -1.51 | — | +0.00 | -1.51 | +34.73 | — |
| 2026-09-03 | `ATRC` | 23 | — | $52.88 | +0.00 | $52.46 | -9.66 | -9.66 | +0.00 | -9.66 |
| 2026-09-03 | `HRMY` | 28 | — | $42.93 | +0.00 | $41.86 | -29.96 | -29.96 | +0.00 | -29.96 |
| 2026-09-03 | `CABA` | 337 | — | $3.63 | +0.00 | $3.48 | -50.55 | -50.55 | +0.00 | -50.55 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `ARCT` | 73 | — | $16.77 | +0.00 | $15.56 | -88.33 | -88.33 | +0.00 | -88.33 |
| 2026-09-03 | `CRDL` | 562 | — | $2.18 | +0.00 | $2.16 | -11.24 | -11.24 | +0.00 | -11.24 |
| 2026-09-03 | `SDGR` | 58 | — | $21.03 | +0.00 | $20.71 | -18.56 | -18.56 | +0.00 | -18.56 |
| 2026-09-03 | `NEOV` | 325 | — | $3.77 | +0.00 | $3.82 | +16.25 | +16.25 | +0.00 | +16.25 |
| 2026-09-04 | `ATRC` | 23 | $52.46 | $52.03 | -9.89 | $51.52 | -11.73 | -21.62 | -19.55 | -31.28 |
| 2026-09-04 | `HRMY` | 28 | $41.86 | $41.50 | -10.08 | $42.25 | +21.00 | +10.92 | -40.04 | -19.04 |
| 2026-09-04 | `CABA` | 337 | $3.48 | $3.46 | -6.74 | $3.47 | +3.37 | -3.37 | -57.29 | -53.92 |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | $130.22 | +1.71 | -3.69 | -21.78 | -20.07 |
| 2026-09-04 | `ARCT` | 73 | $15.56 | $15.61 | +3.65 | $15.82 | +15.33 | +18.98 | -84.68 | -69.35 |
| 2026-09-04 | `CRDL` | 562 | $2.16 | $2.16 | +0.00 | $2.20 | +22.48 | +22.48 | -11.24 | +11.24 |
| 2026-09-04 | `SDGR` | 58 | $20.71 | $20.58 | -7.54 | $20.09 | -28.42 | -35.96 | -26.10 | -54.52 |
| 2026-09-04 | `NEOV` | 325 | $3.82 | $3.83 | +3.25 | $3.86 | +9.75 | +13.00 | +19.50 | +29.25 |
| 2026-09-04 | `GORO` | 1 | — | $3.95 | +0.00 | $4.15 | +0.20 | +0.20 | +0.00 | +0.20 |
| 2026-09-08 | `ATRC` | 23 | $51.52 | $54.31 | +64.17 | $53.73 | -13.34 | +50.83 | +32.89 | +19.55 |
| 2026-09-08 | `HRMY` | 28 | $42.25 | $42.20 | -1.40 | $42.07 | -3.64 | -5.04 | -20.44 | -24.08 |
| 2026-09-08 | `CABA` | 337 | $3.47 | $3.43 | -13.48 | $3.27 | -53.92 | -67.40 | -67.40 | -121.32 |
| 2026-09-08 | `RVTY` | 9 | $130.22 | $128.50 | -15.48 | $127.08 | -12.78 | -28.26 | -35.55 | -48.33 |
| 2026-09-08 | `ARCT` | 73 | $15.82 | $15.47 | -25.55 | $15.63 | +11.68 | -13.87 | -94.90 | -83.22 |
| 2026-09-08 | `CRDL` | 562 | $2.20 | $2.20 | +0.00 | $2.22 | +11.24 | +11.24 | +11.24 | +22.48 |
| 2026-09-08 | `SDGR` | 58 | $20.09 | $19.87 | -12.76 | $20.03 | +9.28 | -3.48 | -67.28 | -58.00 |
| 2026-09-08 | `NEOV` | 325 | $3.86 | $3.86 | +0.00 | $3.92 | +19.50 | +19.50 | +29.25 | +48.75 |
| 2026-09-08 | `GORO` | 1 | $4.15 | $4.13 | -0.02 | $3.84 | -0.29 | -0.31 | +0.18 | -0.11 |
| 2026-09-09 | `ATRC` | 23 | $53.73 | $53.16 | -13.11 | — | +0.00 | -13.11 | +6.44 | — |
| 2026-09-09 | `HRMY` | 28 | $42.07 | $42.01 | -1.68 | — | +0.00 | -1.68 | -25.76 | — |
| 2026-09-09 | `CABA` | 337 | $3.27 | $3.28 | +3.37 | — | +0.00 | +3.37 | -117.95 | — |
| 2026-09-09 | `RVTY` | 9 | $127.08 | $125.77 | -11.79 | — | +0.00 | -11.79 | -60.12 | — |
| 2026-09-09 | `ARCT` | 73 | $15.63 | $15.46 | -12.41 | — | +0.00 | -12.41 | -95.63 | — |
| 2026-09-09 | `CRDL` | 562 | $2.22 | $2.22 | +0.00 | — | +0.00 | +0.00 | +22.48 | — |
| 2026-09-09 | `SDGR` | 58 | $20.03 | $19.88 | -8.70 | — | +0.00 | -8.70 | -66.70 | — |
| 2026-09-09 | `NEOV` | 325 | $3.92 | $3.84 | -26.00 | — | +0.00 | -26.00 | +22.75 | — |
| 2026-09-09 | `GORO` | 1 | $3.84 | $3.91 | +0.07 | $3.62 | -0.29 | -0.22 | -0.04 | -0.33 |
| 2026-09-10 | `GORO` | 1 | $3.62 | $3.69 | +0.07 | — | +0.00 | +0.07 | -0.26 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +787.37 | TPG | — | $24.65 | $10,784.79 | TPG×197 |
| 2026-08-14 | +5.50 | $24.65 | TPG×197 | $10,916.78 | +131.99 | -445.20 | LDI, BTBT | — | $18.76 | $10,471.51 | TPG×197, LDI×3, BTBT×2 |
| 2026-08-17 | +2.25 | $18.76 | TPG×197, LDI×3, BTBT×2 | $10,400.52 | -70.99 | -177.24 | — | — | $18.76 | $10,223.28 | TPG×197, LDI×3, BTBT×2 |
| 2026-08-18 | -6.20 | $18.76 | TPG×197, LDI×3, BTBT×2 | $10,223.14 | -0.14 | -0.22 | — | TPG | $10,214.76 | $10,220.23 | LDI×3, BTBT×2 |
| 2026-08-19 | -7.20 | $10,214.76 | LDI×3, BTBT×2 | $10,220.24 | +0.01 | +0.00 | — | LDI, BTBT | $10,220.13 | $10,220.13 | — |
| 2026-08-20 | +1.12 | $10,220.13 | — | $10,220.13 | -0.00 | +109.44 | AG, HDSN, IAG, KGC, NFGC, DNA, EXK, SCZM | — | $4.88 | $10,303.58 | AG×62, HDSN×221, IAG×65, KGC×43, NFGC×730, DNA×171, EXK×118, SCZM×134 |
| 2026-08-21 | +3.25 | $4.88 | AG×62, HDSN×221, IAG×65, KGC×43, NFGC×730, DNA×171, EXK×118, SCZM×134 | $10,607.16 | +303.58 | -109.48 | — | — | $4.88 | $10,497.68 | AG×62, HDSN×221, IAG×65, KGC×43, NFGC×730, DNA×171, EXK×118, SCZM×134 |
| 2026-08-24 | -5.17 | $4.88 | AG×62, HDSN×221, IAG×65, KGC×43, NFGC×730, DNA×171, EXK×118, SCZM×134 | $10,592.14 | +94.46 | -142.30 | — | — | $4.88 | $10,449.84 | AG×62, HDSN×221, IAG×65, KGC×43, NFGC×730, DNA×171, EXK×118, SCZM×134 |
| 2026-08-25 | +1.80 | $4.88 | AG×62, HDSN×221, IAG×65, KGC×43, NFGC×730, DNA×171, EXK×118, SCZM×134 | $10,327.22 | -122.62 | +73.58 | KURA, LIFE, ETON, ANRO, VALE | AG, HDSN, IAG, KGC, NFGC, DNA, EXK, SCZM | $102.22 | $10,363.23 | KURA×151, LIFE×55, ETON×31, ANRO×56, VALE×137 |
| 2026-08-26 | +2.02 | $102.22 | KURA×151, LIFE×55, ETON×31, ANRO×56, VALE×137 | $10,345.64 | -17.59 | -56.58 | CRMD, RZLT, SENS, ACRS, TMCI, CRDL, LI | — | $32.99 | $10,288.34 | KURA×151, LIFE×55, ETON×31, ANRO×56, VALE×137, CRMD×1, RZLT×2, SENS×1, ACRS×1, TMCI×2, CRDL×6, LI×1 |
| 2026-08-27 | — | $32.99 | KURA×151, LIFE×55, ETON×31, ANRO×56, VALE×137, CRMD×1, RZLT×2, SENS×1, ACRS×1, TMCI×2, CRDL×6, LI×1 | $10,264.68 | -23.66 | +68.30 | — | — | $32.99 | $10,332.98 | KURA×151, LIFE×55, ETON×31, ANRO×56, VALE×137, CRMD×1, RZLT×2, SENS×1, ACRS×1, TMCI×2, CRDL×6, LI×1 |
| 2026-08-28 | +0.75 | $32.99 | KURA×151, LIFE×55, ETON×31, ANRO×56, VALE×137, CRMD×1, RZLT×2, SENS×1, ACRS×1, TMCI×2, CRDL×6, LI×1 | $10,223.93 | -109.05 | -190.71 | CRK, EQ, FIGR, TH, FSM, ADSK, FRO, HAFN | KURA, LIFE, ETON, ANRO, VALE | $299.65 | $9,999.85 | CRMD×1, RZLT×2, SENS×1, ACRS×1, TMCI×2, CRDL×6, LI×1, CRK×86, EQ×515, FIGR×33, TH×66, FSM×98, ADSK×4, FRO×28, HAFN×151 |
| 2026-08-31 | -5.85 | $299.65 | CRMD×1, RZLT×2, SENS×1, ACRS×1, TMCI×2, CRDL×6, LI×1, CRK×86, EQ×515, FIGR×33, TH×66, FSM×98, ADSK×4, FRO×28, HAFN×151 | $9,999.57 | -0.28 | -72.74 | — | CRMD, RZLT, SENS, ACRS, TMCI, CRDL, LI | $364.63 | $9,925.99 | CRK×86, EQ×515, FIGR×33, TH×66, FSM×98, ADSK×4, FRO×28, HAFN×151 |
| 2026-09-01 | -6.30 | $364.63 | CRK×86, EQ×515, FIGR×33, TH×66, FSM×98, ADSK×4, FRO×28, HAFN×151 | $9,964.84 | +38.85 | -99.31 | — | — | $364.63 | $9,865.53 | CRK×86, EQ×515, FIGR×33, TH×66, FSM×98, ADSK×4, FRO×28, HAFN×151 |
| 2026-09-02 | -3.83 | $364.63 | CRK×86, EQ×515, FIGR×33, TH×66, FSM×98, ADSK×4, FRO×28, HAFN×151 | $9,836.72 | -28.81 | +0.00 | — | CRK, EQ, FIGR, TH, FSM, ADSK, FRO, HAFN | $9,814.48 | $9,814.48 | — |
| 2026-09-03 | -0.90 | $9,814.48 | — | $9,814.48 | +0.00 | -208.43 | ATRC, HRMY, CABA, RVTY, ARCT, CRDL, SDGR, NEOV | — | $60.17 | $9,579.74 | ATRC×23, HRMY×28, CABA×337, RVTY×9, ARCT×73, CRDL×562, SDGR×58, NEOV×325 |
| 2026-09-04 | +2.25 | $60.17 | ATRC×23, HRMY×28, CABA×337, RVTY×9, ARCT×73, CRDL×562, SDGR×58, NEOV×325 | $9,546.99 | -32.75 | +33.69 | GORO | — | $56.18 | $9,580.64 | ATRC×23, HRMY×28, CABA×337, RVTY×9, ARCT×73, CRDL×562, SDGR×58, NEOV×325, GORO×1 |
| 2026-09-08 | -11.47 | $56.18 | ATRC×23, HRMY×28, CABA×337, RVTY×9, ARCT×73, CRDL×562, SDGR×58, NEOV×325, GORO×1 | $9,576.12 | -4.52 | -32.27 | — | — | $56.18 | $9,543.85 | ATRC×23, HRMY×28, CABA×337, RVTY×9, ARCT×73, CRDL×562, SDGR×58, NEOV×325, GORO×1 |
| 2026-09-09 | -13.95 | $56.18 | ATRC×23, HRMY×28, CABA×337, RVTY×9, ARCT×73, CRDL×562, SDGR×58, NEOV×325, GORO×1 | $9,473.60 | -70.25 | -0.29 | — | ATRC, HRMY, CABA, RVTY, ARCT, CRDL, SDGR, NEOV | $9,443.04 | $9,446.66 | GORO×1 |
| 2026-09-10 | -13.28 | $9,443.04 | GORO×1 | $9,446.73 | +0.07 | +0.00 | — | GORO | $9,446.67 | $9,446.67 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $10000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | 16:00 close · cash $24.65 · equity $10,784.79 vs 09:30 $10,000.00 (+784.79; session marks +787.37) · 1 name(s) marked open→close (per-name table). TPG×197 09:30 $50.62 → close $54.62 +787.37 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | 09:30 open · cash $24.65 (unchanged overnight, no fees) · equity $10,916.78 vs prior close $10,784.79 (+131.99) · 1 name(s) re-marked at the open (per-name table). TPG×197 yday $54.62 → 09:30 $55.29 +131.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 3 | $0.94 | $0.04 | — | $21.80 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $3.08 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 2 | $1.50 | $0.04 | — | $18.76 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $3.08 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.76 | ▼ close $10,471.51 vs 09:30 $10,916.78 (session -445.20) | 16:00 close · cash $18.76 · equity $10,471.51 vs 09:30 $10,916.78 (-445.27; session marks -445.20) · 3 name(s) marked open→close (per-name table). TPG×197 09:30 $55.29 → close $53.03 -445.22; LDI×3 09:30 $0.94 → close $0.90 -0.12; BTBT×2 09:30 $1.50 → close $1.57 +0.14 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.76 | ▼ 09:30 equity $10,400.52 vs yday $10,471.51 (-70.99) | 09:30 open · cash $18.76 (unchanged overnight, no fees) · equity $10,400.52 vs prior close $10,471.51 (-70.99) · 3 name(s) re-marked at the open (per-name table). TPG×197 yday $53.03 → 09:30 $52.67 -70.92; LDI×3 yday $0.90 → 09:30 $0.91 +0.03; BTBT×2 yday $1.57 → 09:30 $1.52 -0.10 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.76 | ▼ close $10,223.28 vs 09:30 $10,400.52 (session -177.24) | 16:00 close · cash $18.76 · equity $10,223.28 vs 09:30 $10,400.52 (-177.24; session marks -177.24) · 3 name(s) marked open→close (per-name table). TPG×197 09:30 $52.67 → close $51.77 -177.30; LDI×3 09:30 $0.91 → close $0.88 -0.10; BTBT×2 09:30 $1.52 → close $1.60 +0.16 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.76 | ▼ 09:30 equity $10,223.14 vs yday $10,223.28 (-0.14) | 09:30 open · cash $18.76 (unchanged overnight, no fees) · equity $10,223.14 vs prior close $10,223.28 (-0.14) · 3 name(s) re-marked at the open (per-name table). TPG×197 yday $51.77 → 09:30 $51.77 +0.00; LDI×3 yday $0.88 → 09:30 $0.87 -0.02; BTBT×2 yday $1.60 → 09:30 $1.54 -0.12 | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 197 | $51.77 | $2.70 | $+220.64 | $10,214.76 | ▲ +220.64 after sell → book $10,220.45; vs 09:30 mark -2.69 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,214.76 | ▼ close $10,220.23 vs 09:30 $10,223.14 (session -0.22) | 16:00 close · cash $10,214.76 · equity $10,220.23 vs 09:30 $10,223.14 (-2.91; session marks -0.22) · 2 name(s) marked open→close (per-name table). LDI×3 09:30 $0.87 → close $0.86 -0.04; BTBT×2 09:30 $1.54 → close $1.45 -0.18 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,214.76 | ▲ 09:30 equity $10,220.24 vs yday $10,220.23 (+0.01) | 09:30 open · cash $10,214.76 (unchanged overnight, no fees) · equity $10,220.24 vs prior close $10,220.23 (+0.01) · 2 name(s) re-marked at the open (per-name table). LDI×3 yday $0.86 → 09:30 $0.88 +0.07; BTBT×2 yday $1.45 → 09:30 $1.42 -0.06 | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 3 | $0.88 | $0.06 | $-0.26 | $10,217.34 | ▼ -0.26 after sell → book $10,220.18; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 2 | $1.42 | $0.05 | $-0.25 | $10,220.13 | ▼ -0.25 after sell → book $10,220.13; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,220.13 | ▲ close $10,220.13 vs 09:30 $10,220.24 (session +0.00) | 16:00 close · cash $10,220.13 · no lots left · equity $10,220.13. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,220.13 | ▲ 09:30 equity $10,220.13 vs yday $10,220.13 (-0.00) | 09:30 open · cash $10,220.13 · no holdings · equity $10,220.13 vs prior close $10,220.13 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $8,943.85 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1277.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 221 | $5.77 | $2.85 | — | $7,665.83 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1277.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $6,387.70 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1277.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $5,111.49 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1277.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 730 | $1.75 | $9.42 | — | $3,824.57 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1277.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 171 | $7.45 | $2.50 | — | $2,548.12 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1277.52 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 118 | $10.77 | $2.34 | — | $1,274.91 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1277.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 134 | $9.46 | $2.39 | — | $4.88 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1277.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.88 | ▲ close $10,303.58 vs 09:30 $10,220.13 (session +109.44) | 16:00 close · cash $4.88 · equity $10,303.58 vs 09:30 $10,220.13 (+83.45; session marks +109.44) · 8 name(s) marked open→close (per-name table). AG×62 09:30 $20.55 → close $21.19 +39.68; HDSN×221 09:30 $5.77 → close $5.57 -44.20; IAG×65 09:30 $19.63 → close $20.50 +56.55; KGC×43 09:30 $29.63 → close $31.43 +77.40; NFGC×730 09:30 $1.75 → close $1.75 +0.00; DNA×171 09:30 $7.45 → close $6.96 -83.79; EXK×118 09:30 $10.77 → close $10.97 +23.60; SCZM×134 09:30 $9.46 → close $9.76 +40.20 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.88 | ▲ 09:30 equity $10,607.16 vs yday $10,303.58 (+303.58) | 09:30 open · cash $4.88 (unchanged overnight, no fees) · equity $10,607.16 vs prior close $10,303.58 (+303.58) · 8 name(s) re-marked at the open (per-name table). AG×62 yday $21.19 → 09:30 $21.90 +44.02; HDSN×221 yday $5.57 → 09:30 $5.67 +22.10; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×730 yday $1.75 → 09:30 $1.79 +29.20; DNA×171 yday $6.96 → 09:30 $7.09 +22.23; EXK×118 yday $10.97 → 09:30 $11.34 +43.66; SCZM×134 yday $9.76 → 09:30 $10.26 +67.00 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.88 | ▼ close $10,497.68 vs 09:30 $10,607.16 (session -109.48) | 16:00 close · cash $4.88 · equity $10,497.68 vs 09:30 $10,607.16 (-109.48; session marks -109.48) · 8 name(s) marked open→close (per-name table). AG×62 09:30 $21.90 → close $21.09 -50.22; HDSN×221 09:30 $5.67 → close $5.63 -8.84; IAG×65 09:30 $21.17 → close $21.14 -1.95; KGC×43 09:30 $32.17 → close $32.76 +25.37; NFGC×730 09:30 $1.79 → close $1.84 +36.50; DNA×171 09:30 $7.09 → close $7.40 +53.01; EXK×118 09:30 $11.34 → close $10.62 -84.96; SCZM×134 09:30 $10.26 → close $9.68 -78.39 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.88 | ▲ 09:30 equity $10,592.14 vs yday $10,497.68 (+94.46) | 09:30 open · cash $4.88 (unchanged overnight, no fees) · equity $10,592.14 vs prior close $10,497.68 (+94.46) · 8 name(s) re-marked at the open (per-name table). AG×62 yday $21.09 → 09:30 $21.30 +13.02; HDSN×221 yday $5.63 → 09:30 $5.69 +13.26; IAG×65 yday $21.14 → 09:30 $21.38 +15.60; KGC×43 yday $32.76 → 09:30 $33.03 +11.61; NFGC×730 yday $1.84 → 09:30 $1.86 +14.60; DNA×171 yday $7.40 → 09:30 $7.25 -25.65; EXK×118 yday $10.62 → 09:30 $10.97 +41.30; SCZM×134 yday $9.68 → 09:30 $9.76 +10.72 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.88 | ▼ close $10,449.84 vs 09:30 $10,592.14 (session -142.30) | 16:00 close · cash $4.88 · equity $10,449.84 vs 09:30 $10,592.14 (-142.30; session marks -142.30) · 8 name(s) marked open→close (per-name table). AG×62 09:30 $21.30 → close $20.83 -29.14; HDSN×221 09:30 $5.69 → close $5.52 -37.57; IAG×65 09:30 $21.38 → close $21.80 +27.30; KGC×43 09:30 $33.03 → close $32.98 -2.15; NFGC×730 09:30 $1.86 → close $1.90 +29.20; DNA×171 09:30 $7.25 → close $6.78 -80.37; EXK×118 09:30 $10.97 → close $10.76 -24.78; SCZM×134 09:30 $9.76 → close $9.57 -24.79 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.88 | ▼ 09:30 equity $10,327.22 vs yday $10,449.84 (-122.62) | 09:30 open · cash $4.88 (unchanged overnight, no fees) · equity $10,327.22 vs prior close $10,449.84 (-122.62) · 8 name(s) re-marked at the open (per-name table). AG×62 yday $20.83 → 09:30 $20.32 -31.62; HDSN×221 yday $5.52 → 09:30 $5.53 +2.21; IAG×65 yday $21.80 → 09:30 $21.21 -38.35; KGC×43 yday $32.98 → 09:30 $32.32 -28.38; NFGC×730 yday $1.90 → 09:30 $1.90 +0.00; DNA×171 yday $6.78 → 09:30 $6.94 +27.36; EXK×118 yday $10.76 → 09:30 $10.44 -37.76; SCZM×134 yday $9.57 → 09:30 $9.45 -16.08 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.32 | $2.20 | $-18.63 | $1,262.53 | ▼ -18.63 after sell → book $10,325.03; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 221 | $5.53 | $2.90 | $-58.79 | $2,481.76 | ▼ -58.79 after sell → book $10,322.13; vs 09:30 mark -2.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.21 | $2.21 | $+98.31 | $3,858.20 | ▲ +98.31 after sell → book $10,319.92; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.32 | $2.14 | $+111.41 | $5,245.82 | ▲ +111.41 after sell → book $10,317.78; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 730 | $1.90 | $9.55 | $+90.53 | $6,623.27 | ▲ +90.53 after sell → book $10,308.23; vs 09:30 mark -9.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 171 | $6.94 | $2.54 | $-92.25 | $7,807.47 | ▼ -92.25 after sell → book $10,305.69; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 118 | $10.44 | $2.37 | $-43.66 | $9,037.02 | ▼ -43.66 after sell → book $10,303.32; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SCZM` | 134 | $9.45 | $2.42 | $-6.16 | $10,300.89 | ▼ -6.16 after sell → book $10,300.89; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 151 | $13.59 | $2.44 | — | $8,246.36 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $2060.18 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 55 | $36.96 | $2.15 | — | $6,211.40 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $2060.18 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 31 | $64.55 | $2.08 | — | $4,208.27 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+4.4; leftover $2060.18 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ANRO` | 56 | $36.52 | $2.16 | — | $2,160.99 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.9; leftover $2060.18 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VALE` | 137 | $15.01 | $2.40 | — | $102.22 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; ⚪; ret5=+9.4; leftover $2060.18 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $102.22 | ▲ close $10,363.23 vs 09:30 $10,327.22 (session +73.58) | 16:00 close · cash $102.22 · equity $10,363.23 vs 09:30 $10,327.22 (+36.01; session marks +73.58) · 5 name(s) marked open→close (per-name table). KURA×151 09:30 $13.59 → close $13.59 +0.00; LIFE×55 09:30 $36.96 → close $38.56 +88.00; ETON×31 09:30 $64.55 → close $63.05 -46.50; ANRO×56 09:30 $36.52 → close $36.31 -11.76; VALE×137 09:30 $15.01 → close $15.33 +43.84 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $102.22 | ▼ 09:30 equity $10,345.64 vs yday $10,363.23 (-17.59) | 09:30 open · cash $102.22 (unchanged overnight, no fees) · equity $10,345.64 vs prior close $10,363.23 (-17.59) · 5 name(s) re-marked at the open (per-name table). KURA×151 yday $13.59 → 09:30 $13.63 +6.04; LIFE×55 yday $38.56 → 09:30 $38.24 -17.60; ETON×31 yday $63.05 → 09:30 $63.60 +17.05; ANRO×56 yday $36.31 → 09:30 $35.80 -28.56; VALE×137 yday $15.33 → 09:30 $15.37 +5.48 | — |
| 2026-08-26 09:30 ET | **BUY** | `CRMD` | 1 | $8.60 | $0.09 | — | $93.53 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+4.8; leftover $12.78 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `RZLT` | 2 | $5.01 | $0.11 | — | $83.41 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,yday_gainer; 🔵; ret5=+7.5; leftover $12.78 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SENS` | 1 | $9.48 | $0.10 | — | $73.83 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $12.78 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ACRS` | 1 | $6.53 | $0.07 | — | $67.23 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.6; leftover $12.78 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TMCI` | 2 | $4.78 | $0.10 | — | $57.57 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+8.1; leftover $12.78 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CRDL` | 6 | $2.03 | $0.14 | — | $45.25 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+5.5; leftover $12.78 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟡 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `LI` | 1 | $12.14 | $0.12 | — | $32.99 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+1.2; leftover $12.78 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.99 | ▼ close $10,288.34 vs 09:30 $10,345.64 (session -56.58) | 16:00 close · cash $32.99 · equity $10,288.34 vs 09:30 $10,345.64 (-57.30; session marks -56.58) · 12 name(s) marked open→close (per-name table). KURA×151 09:30 $13.63 → close $13.06 -86.07; LIFE×55 09:30 $38.24 → close $39.11 +47.85; ETON×31 09:30 $63.60 → close $62.62 -30.38; ANRO×56 09:30 $35.80 → close $36.53 +40.88; VALE×137 09:30 $15.37 → close $15.16 -28.77; CRMD×1 09:30 $8.60 → close $8.39 -0.21; RZLT×2 09:30 $5.01 → close $5.04 +0.06; SENS×1 09:30 $9.48 → close $9.34 -0.14; ACRS×1 09:30 $6.53 → close $6.19 -0.34; TMCI×2 09:30 $4.78 → close $4.72 -0.12; CRDL×6 09:30 $2.03 → close $2.14 +0.66; LI×1 09:30 $12.14 → close $12.14 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.99 | ▼ 09:30 equity $10,264.68 vs yday $10,288.34 (-23.66) | 09:30 open · cash $32.99 (unchanged overnight, no fees) · equity $10,264.68 vs prior close $10,288.34 (-23.66) · 12 name(s) re-marked at the open (per-name table). KURA×151 yday $13.06 → 09:30 $12.98 -12.08; LIFE×55 yday $39.11 → 09:30 $39.40 +15.95; ETON×31 yday $62.62 → 09:30 $62.50 -3.72; ANRO×56 yday $36.53 → 09:30 $36.08 -25.20; VALE×137 yday $15.16 → 09:30 $15.17 +1.37; CRMD×1 yday $8.39 → 09:30 $8.49 +0.10; RZLT×2 yday $5.04 → 09:30 $5.07 +0.06; SENS×1 yday $9.34 → 09:30 $9.33 -0.01; ACRS×1 yday $6.19 → 09:30 $6.15 -0.04; TMCI×2 yday $4.72 → 09:30 $4.72 +0.00; CRDL×6 yday $2.14 → 09:30 $2.09 -0.30; LI×1 yday $12.14 → 09:30 $12.35 +0.21 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.99 | ▲ close $10,332.98 vs 09:30 $10,264.68 (session +68.30) | 16:00 close · cash $32.99 · equity $10,332.98 vs 09:30 $10,264.68 (+68.30; session marks +68.30) · 12 name(s) marked open→close (per-name table). KURA×151 09:30 $12.98 → close $13.18 +30.20; LIFE×55 09:30 $39.40 → close $39.44 +2.20; ETON×31 09:30 $62.50 → close $62.67 +5.27; ANRO×56 09:30 $36.08 → close $36.30 +12.32; VALE×137 09:30 $15.17 → close $15.31 +19.18; CRMD×1 09:30 $8.49 → close $8.31 -0.18; RZLT×2 09:30 $5.07 → close $4.98 -0.18; SENS×1 09:30 $9.33 → close $9.40 +0.07; ACRS×1 09:30 $6.15 → close $6.16 +0.01; TMCI×2 09:30 $4.72 → close $4.60 -0.24; CRDL×6 09:30 $2.09 → close $2.06 -0.18; LI×1 09:30 $12.35 → close $12.18 -0.17 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.99 | ▼ 09:30 equity $10,223.93 vs yday $10,332.98 (-109.05) | 09:30 open · cash $32.99 (unchanged overnight, no fees) · equity $10,223.93 vs prior close $10,332.98 (-109.05) · 12 name(s) re-marked at the open (per-name table). KURA×151 yday $13.18 → 09:30 $13.05 -19.63; LIFE×55 yday $39.44 → 09:30 $39.60 +8.80; ETON×31 yday $62.67 → 09:30 $61.98 -21.39; ANRO×56 yday $36.30 → 09:30 $35.00 -72.80; VALE×137 yday $15.31 → 09:30 $15.28 -4.11; CRMD×1 yday $8.31 → 09:30 $8.28 -0.03; RZLT×2 yday $4.98 → 09:30 $4.95 -0.06; SENS×1 yday $9.40 → 09:30 $9.39 -0.01; ACRS×1 yday $6.16 → 09:30 $6.10 -0.06; TMCI×2 yday $4.60 → 09:30 $4.65 +0.10; CRDL×6 yday $2.06 → 09:30 $2.06 +0.00; LI×1 yday $12.18 → 09:30 $12.32 +0.14 | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 151 | $13.05 | $2.48 | $-86.47 | $2,001.05 | ▼ -86.47 after sell → book $10,221.44; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 55 | $39.60 | $2.18 | $+140.86 | $4,176.87 | ▲ +140.86 after sell → book $10,219.26; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ETON` | 31 | $61.98 | $2.11 | $-83.86 | $6,096.14 | ▼ -83.86 after sell → book $10,217.15; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANRO` | 56 | $35.00 | $2.18 | $-89.46 | $8,053.96 | ▼ -89.46 after sell → book $10,214.97; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `VALE` | 137 | $15.28 | $2.44 | $+32.15 | $10,144.88 | ▲ +32.15 after sell → book $10,212.53; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 86 | $14.63 | $2.25 | — | $8,884.45 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+5.8; leftover $1268.11 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `EQ` | 515 | $2.46 | $6.64 | — | $7,610.91 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+7.9; leftover $1268.11 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 33 | $37.49 | $2.09 | — | $6,371.65 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+5.4; leftover $1268.11 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 66 | $19.00 | $2.19 | — | $5,115.46 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+7.5; leftover $1268.11 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FSM` | 98 | $12.84 | $2.28 | — | $3,854.85 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+7.6; leftover $1268.11 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $2,808.21 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+7.8; leftover $1268.11 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 28 | $44.40 | $2.07 | — | $1,562.94 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+0.4; leftover $1268.11 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 151 | $8.35 | $2.44 | — | $299.65 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+5.1; leftover $1268.11 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $299.65 | ▼ close $9,999.85 vs 09:30 $10,223.93 (session -190.71) | 16:00 close · cash $299.65 · equity $9,999.85 vs 09:30 $10,223.93 (-224.08; session marks -190.71) · 15 name(s) marked open→close (per-name table). CRMD×1 09:30 $8.28 → close $8.30 +0.02; RZLT×2 09:30 $4.95 → close $4.62 -0.66; SENS×1 09:30 $9.39 → close $9.33 -0.06; ACRS×1 09:30 $6.10 → close $6.01 -0.09; TMCI×2 09:30 $4.65 → close $4.65 +0.00; CRDL×6 09:30 $2.06 → close $1.94 -0.72; LI×1 09:30 $12.32 → close $12.23 -0.09; CRK×86 09:30 $14.63 → close $14.29 -29.24; EQ×515 09:30 $2.46 → close $2.39 -36.05; FIGR×33 09:30 $37.49 → close $36.05 -47.52; TH×66 09:30 $19.00 → close $18.55 -29.70; FSM×98 09:30 $12.84 → close $12.26 -56.84; ADSK×4 09:30 $261.16 → close $260.66 -2.00; FRO×28 09:30 $44.40 → close $44.19 -5.88; HAFN×151 09:30 $8.35 → close $8.47 +18.12 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $299.65 | ▼ 09:30 equity $9,999.57 vs yday $9,999.85 (-0.28) | 09:30 open · cash $299.65 (unchanged overnight, no fees) · equity $9,999.57 vs prior close $9,999.85 (-0.28) · 15 name(s) re-marked at the open (per-name table). CRMD×1 yday $8.30 → 09:30 $8.26 -0.04; RZLT×2 yday $4.62 → 09:30 $4.65 +0.06; SENS×1 yday $9.33 → 09:30 $9.29 -0.04; ACRS×1 yday $6.01 → 09:30 $5.97 -0.04; TMCI×2 yday $4.65 → 09:30 $4.60 -0.10; CRDL×6 yday $1.94 → 09:30 $1.92 -0.12; LI×1 yday $12.23 → 09:30 $12.28 +0.05; CRK×86 yday $14.29 → 09:30 $14.54 +21.50; EQ×515 yday $2.39 → 09:30 $2.39 +0.00; FIGR×33 yday $36.05 → 09:30 $35.77 -9.24; TH×66 yday $18.55 → 09:30 $18.12 -28.05; FSM×98 yday $12.26 → 09:30 $12.26 +0.00; ADSK×4 yday $260.66 → 09:30 $257.71 -11.80; FRO×28 yday $44.19 → 09:30 $44.85 +18.48; HAFN×151 yday $8.47 → 09:30 $8.53 +9.06 | — |
| 2026-08-31 09:30 ET | **SELL** | `CRMD` | 1 | $8.26 | $0.11 | $-0.53 | $307.80 | ▼ -0.53 after sell → book $9,999.46; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `RZLT` | 2 | $4.65 | $0.12 | $-0.95 | $316.98 | ▼ -0.95 after sell → book $9,999.34; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `SENS` | 1 | $9.29 | $0.12 | $-0.40 | $326.16 | ▼ -0.40 after sell → book $9,999.23; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ACRS` | 1 | $5.97 | $0.08 | $-0.71 | $332.04 | ▼ -0.71 after sell → book $9,999.14; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TMCI` | 2 | $4.60 | $0.12 | $-0.58 | $341.12 | ▼ -0.58 after sell → book $9,999.02; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRDL` | 6 | $1.92 | $0.15 | $-0.95 | $352.49 | ▼ -0.95 after sell → book $9,998.87; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `LI` | 1 | $12.28 | $0.15 | $-0.13 | $364.63 | ▼ -0.13 after sell → book $9,998.73; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $364.63 | ▼ close $9,925.99 vs 09:30 $9,999.57 (session -72.74) | 16:00 close · cash $364.63 · equity $9,925.99 vs 09:30 $9,999.57 (-73.58; session marks -72.74) · 8 name(s) marked open→close (per-name table). CRK×86 09:30 $14.54 → close $14.43 -9.46; EQ×515 09:30 $2.39 → close $2.27 -61.80; FIGR×33 09:30 $35.77 → close $36.69 +30.36; TH×66 09:30 $18.12 → close $18.52 +26.07; FSM×98 09:30 $12.26 → close $12.08 -17.64; ADSK×4 09:30 $257.71 → close $258.53 +3.28; FRO×28 09:30 $44.85 → close $43.78 -29.96; HAFN×151 09:30 $8.53 → close $8.44 -13.59 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $364.63 | ▲ 09:30 equity $9,964.84 vs yday $9,925.99 (+38.85) | 09:30 open · cash $364.63 (unchanged overnight, no fees) · equity $9,964.84 vs prior close $9,925.99 (+38.85) · 8 name(s) re-marked at the open (per-name table). CRK×86 yday $14.43 → 09:30 $15.82 +119.54; EQ×515 yday $2.27 → 09:30 $2.25 -10.30; FIGR×33 yday $36.69 → 09:30 $35.46 -40.59; TH×66 yday $18.52 → 09:30 $18.45 -4.62; FSM×98 yday $12.08 → 09:30 $11.67 -40.18; ADSK×4 yday $258.53 → 09:30 $253.48 -20.20; FRO×28 yday $43.78 → 09:30 $44.39 +17.08; HAFN×151 yday $8.44 → 09:30 $8.56 +18.12 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $364.63 | ▼ close $9,865.53 vs 09:30 $9,964.84 (session -99.31) | 16:00 close · cash $364.63 · equity $9,865.53 vs 09:30 $9,964.84 (-99.31; session marks -99.31) · 8 name(s) marked open→close (per-name table). CRK×86 09:30 $15.82 → close $16.02 +17.20; EQ×515 09:30 $2.25 → close $2.21 -20.60; FIGR×33 09:30 $35.46 → close $33.70 -58.08; TH×66 09:30 $18.45 → close $18.07 -25.08; FSM×98 09:30 $11.67 → close $11.75 +7.84; ADSK×4 09:30 $253.48 → close $247.69 -23.16; FRO×28 09:30 $44.39 → close $44.32 -1.96; HAFN×151 09:30 $8.56 → close $8.59 +4.53 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $364.63 | ▼ 09:30 equity $9,836.72 vs yday $9,865.53 (-28.81) | 09:30 open · cash $364.63 (unchanged overnight, no fees) · equity $9,836.72 vs prior close $9,865.53 (-28.81) · 8 name(s) re-marked at the open (per-name table). CRK×86 yday $16.02 → 09:30 $15.70 -27.52; EQ×515 yday $2.21 → 09:30 $2.20 -5.15; FIGR×33 yday $33.70 → 09:30 $33.31 -12.87; TH×66 yday $18.07 → 09:30 $17.98 -5.94; FSM×98 yday $11.75 → 09:30 $12.08 +32.34; ADSK×4 yday $247.69 → 09:30 $246.70 -3.96; FRO×28 yday $44.32 → 09:30 $44.17 -4.20; HAFN×151 yday $8.59 → 09:30 $8.58 -1.51 | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 86 | $15.70 | $2.27 | $+87.50 | $1,712.55 | ▲ +87.50 after sell → book $9,834.44; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `EQ` | 515 | $2.20 | $6.74 | $-147.28 | $2,838.81 | ▼ -147.28 after sell → book $9,827.70; vs 09:30 mark -6.74 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FIGR` | 33 | $33.31 | $2.11 | $-142.14 | $3,935.93 | ▼ -142.14 after sell → book $9,825.59; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TH` | 66 | $17.98 | $2.21 | $-71.72 | $5,120.41 | ▼ -71.72 after sell → book $9,823.39; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FSM` | 98 | $12.08 | $2.31 | $-79.07 | $6,301.93 | ▼ -79.07 after sell → book $9,821.07; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 4 | $246.70 | $2.02 | $-61.86 | $7,286.71 | ▼ -61.86 after sell → book $9,819.05; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 28 | $44.17 | $2.09 | $-10.61 | $8,521.38 | ▼ -10.61 after sell → book $9,816.96; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 151 | $8.58 | $2.48 | $+29.81 | $9,814.48 | ▲ +29.81 after sell → book $9,814.48; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,814.48 | ▲ close $9,814.48 vs 09:30 $9,836.72 (session +0.00) | 16:00 close · cash $9,814.48 · no lots left · equity $9,814.48. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,814.48 | ▲ 09:30 equity $9,814.48 vs yday $9,814.48 (+0.00) | 09:30 open · cash $9,814.48 · no holdings · equity $9,814.48 vs prior close $9,814.48 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $8,596.18 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1226.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 28 | $42.93 | $2.07 | — | $7,392.07 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1226.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 337 | $3.63 | $4.35 | — | $6,164.41 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1226.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $4,970.34 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1226.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 73 | $16.77 | $2.21 | — | $3,743.92 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1226.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 562 | $2.18 | $7.25 | — | $2,511.51 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1226.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SDGR` | 58 | $21.03 | $2.16 | — | $1,289.61 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $1226.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NEOV` | 325 | $3.77 | $4.19 | — | $60.17 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; 🔵; ⚪; ret5=+8.6; leftover $1226.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.17 | ▼ close $9,579.74 vs 09:30 $9,814.48 (session -208.43) | 16:00 close · cash $60.17 · equity $9,579.74 vs 09:30 $9,814.48 (-234.74; session marks -208.43) · 8 name(s) marked open→close (per-name table). ATRC×23 09:30 $52.88 → close $52.46 -9.66; HRMY×28 09:30 $42.93 → close $41.86 -29.96; CABA×337 09:30 $3.63 → close $3.48 -50.55; RVTY×9 09:30 $132.45 → close $130.63 -16.38; ARCT×73 09:30 $16.77 → close $15.56 -88.33; CRDL×562 09:30 $2.18 → close $2.16 -11.24; SDGR×58 09:30 $21.03 → close $20.71 -18.56; NEOV×325 09:30 $3.77 → close $3.82 +16.25 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.17 | ▼ 09:30 equity $9,546.99 vs yday $9,579.74 (-32.75) | 09:30 open · cash $60.17 (unchanged overnight, no fees) · equity $9,546.99 vs prior close $9,579.74 (-32.75) · 8 name(s) re-marked at the open (per-name table). ATRC×23 yday $52.46 → 09:30 $52.03 -9.89; HRMY×28 yday $41.86 → 09:30 $41.50 -10.08; CABA×337 yday $3.48 → 09:30 $3.46 -6.74; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; ARCT×73 yday $15.56 → 09:30 $15.61 +3.65; CRDL×562 yday $2.16 → 09:30 $2.16 +0.00; SDGR×58 yday $20.71 → 09:30 $20.58 -7.54; NEOV×325 yday $3.82 → 09:30 $3.83 +3.25 | — |
| 2026-09-04 09:30 ET | **BUY** | `GORO` | 1 | $3.95 | $0.04 | — | $56.18 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+6.9; leftover $7.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.18 | ▲ close $9,580.64 vs 09:30 $9,546.99 (session +33.69) | 16:00 close · cash $56.18 · equity $9,580.64 vs 09:30 $9,546.99 (+33.65; session marks +33.69) · 9 name(s) marked open→close (per-name table). ATRC×23 09:30 $52.03 → close $51.52 -11.73; HRMY×28 09:30 $41.50 → close $42.25 +21.00; CABA×337 09:30 $3.46 → close $3.47 +3.37; RVTY×9 09:30 $130.03 → close $130.22 +1.71; ARCT×73 09:30 $15.61 → close $15.82 +15.33; CRDL×562 09:30 $2.16 → close $2.20 +22.48; SDGR×58 09:30 $20.58 → close $20.09 -28.42; NEOV×325 09:30 $3.83 → close $3.86 +9.75; GORO×1 09:30 $3.95 → close $4.15 +0.20 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.18 | ▼ 09:30 equity $9,576.12 vs yday $9,580.64 (-4.52) | 09:30 open · cash $56.18 (unchanged overnight, no fees) · equity $9,576.12 vs prior close $9,580.64 (-4.52) · 9 name(s) re-marked at the open (per-name table). ATRC×23 yday $51.52 → 09:30 $54.31 +64.17; HRMY×28 yday $42.25 → 09:30 $42.20 -1.40; CABA×337 yday $3.47 → 09:30 $3.43 -13.48; RVTY×9 yday $130.22 → 09:30 $128.50 -15.48; ARCT×73 yday $15.82 → 09:30 $15.47 -25.55; CRDL×562 yday $2.20 → 09:30 $2.20 +0.00; SDGR×58 yday $20.09 → 09:30 $19.87 -12.76; NEOV×325 yday $3.86 → 09:30 $3.86 +0.00; GORO×1 yday $4.15 → 09:30 $4.13 -0.02 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.18 | ▼ close $9,543.85 vs 09:30 $9,576.12 (session -32.27) | 16:00 close · cash $56.18 · equity $9,543.85 vs 09:30 $9,576.12 (-32.27; session marks -32.27) · 9 name(s) marked open→close (per-name table). ATRC×23 09:30 $54.31 → close $53.73 -13.34; HRMY×28 09:30 $42.20 → close $42.07 -3.64; CABA×337 09:30 $3.43 → close $3.27 -53.92; RVTY×9 09:30 $128.50 → close $127.08 -12.78; ARCT×73 09:30 $15.47 → close $15.63 +11.68; CRDL×562 09:30 $2.20 → close $2.22 +11.24; SDGR×58 09:30 $19.87 → close $20.03 +9.28; NEOV×325 09:30 $3.86 → close $3.92 +19.50; GORO×1 09:30 $4.13 → close $3.84 -0.29 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.18 | ▼ 09:30 equity $9,473.60 vs yday $9,543.85 (-70.25) | 09:30 open · cash $56.18 (unchanged overnight, no fees) · equity $9,473.60 vs prior close $9,543.85 (-70.25) · 9 name(s) re-marked at the open (per-name table). ATRC×23 yday $53.73 → 09:30 $53.16 -13.11; HRMY×28 yday $42.07 → 09:30 $42.01 -1.68; CABA×337 yday $3.27 → 09:30 $3.28 +3.37; RVTY×9 yday $127.08 → 09:30 $125.77 -11.79; ARCT×73 yday $15.63 → 09:30 $15.46 -12.41; CRDL×562 yday $2.22 → 09:30 $2.22 +0.00; SDGR×58 yday $20.03 → 09:30 $19.88 -8.70; NEOV×325 yday $3.92 → 09:30 $3.84 -26.00; GORO×1 yday $3.84 → 09:30 $3.91 +0.07 | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 23 | $53.16 | $2.08 | $+2.30 | $1,276.78 | ▲ +2.30 after sell → book $9,471.52; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 28 | $42.01 | $2.09 | $-29.93 | $2,450.96 | ▼ -29.93 after sell → book $9,469.42; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 337 | $3.28 | $4.41 | $-126.71 | $3,551.91 | ▼ -126.71 after sell → book $9,465.01; vs 09:30 mark -4.41 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $4,681.80 | ▼ -64.17 after sell → book $9,462.97; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 73 | $15.46 | $2.23 | $-100.07 | $5,808.15 | ▼ -100.07 after sell → book $9,460.74; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 562 | $2.22 | $7.35 | $+7.88 | $7,048.44 | ▲ +7.88 after sell → book $9,453.39; vs 09:30 mark -7.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SDGR` | 58 | $19.88 | $2.18 | $-71.05 | $8,199.29 | ▼ -71.05 after sell → book $9,451.20; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `NEOV` | 325 | $3.84 | $4.26 | $+14.30 | $9,443.04 | ▲ +14.30 after sell → book $9,446.95; vs 09:30 mark -4.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,443.04 | ▼ close $9,446.66 vs 09:30 $9,473.60 (session -0.29) | 16:00 close · cash $9,443.04 · equity $9,446.66 vs 09:30 $9,473.60 (-26.94; session marks -0.29) · 1 name(s) marked open→close (per-name table). GORO×1 09:30 $3.91 → close $3.62 -0.29 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,443.04 | ▲ 09:30 equity $9,446.73 vs yday $9,446.66 (+0.07) | 09:30 open · cash $9,443.04 (unchanged overnight, no fees) · equity $9,446.73 vs prior close $9,446.66 (+0.07) · 1 name(s) re-marked at the open (per-name table). GORO×1 yday $3.62 → 09:30 $3.69 +0.07 | — |
| 2026-09-10 09:30 ET | **SELL** | `GORO` | 1 | $3.69 | $0.06 | $-0.36 | $9,446.67 | ▼ -0.36 after sell → book $9,446.67; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,446.67 | ▲ close $9,446.67 vs 09:30 $9,446.73 (session +0.00) | 16:00 close · cash $9,446.67 · no lots left · equity $9,446.67. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLG` | cash | leftover split 3.08 < 1 share @ 57.61 |
| 2026-08-14 | `ANGX` | cash | leftover split 3.08 < 1 share @ 4.31 |
| 2026-08-14 | `HYLN` | cash | leftover split 3.08 < 1 share @ 4.18 |
| 2026-08-14 | `WDC` | cash | leftover split 3.08 < 1 share @ 503.50 |
| 2026-08-14 | `ADUR` | cash | leftover split 3.08 < 1 share @ 16.50 |
| 2026-08-14 | `ALGM` | cash | leftover split 3.08 < 1 share @ 44.06 |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 4.69 < 1 share @ 46.18 |
| 2026-08-17 | `OCC` | cash | leftover split 4.69 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 4.69 < 1 share @ 16.20 |
| 2026-08-17 | `NEWP` | cash | leftover split 4.69 < 1 share @ 6.94 |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBNX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BETA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `U` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `VSTM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BTBT` | cash | leftover split 0.81 < 1 share @ 1.66 |
| 2026-08-21 | `ORBS` | cash | leftover split 0.81 < 1 share @ 0.86 |
| 2026-08-21 | `CF` | cash | leftover split 0.81 < 1 share @ 127.43 |
| 2026-08-21 | `EMBC` | cash | leftover split 0.81 < 1 share @ 5.43 |
| 2026-08-21 | `TXG` | cash | leftover split 0.81 < 1 share @ 64.39 |
| 2026-08-21 | `DXYZ` | cash | leftover split 0.81 < 1 share @ 34.89 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABAT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ETON` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ANRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `VALE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NCNO` | cash | leftover split 12.78 < 1 share @ 19.33 |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ETON` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ANRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `VALE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `SENS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ACRS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `TMCI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `LI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 4.71 < 1 share @ 41.44 |
| 2026-08-27 | `CRK` | cash | leftover split 4.71 < 1 share @ 14.42 |
| 2026-08-27 | `ANET` | cash | leftover split 4.71 < 1 share @ 205.90 |
| 2026-08-27 | `GEN` | cash | leftover split 4.71 < 1 share @ 29.83 |
| 2026-08-27 | `MRVL` | cash | leftover split 4.71 < 1 share @ 253.44 |
| 2026-08-27 | `NUE` | cash | leftover split 4.71 < 1 share @ 252.00 |
| 2026-08-27 | `PGY` | cash | leftover split 4.71 < 1 share @ 22.93 |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ACRS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `TMCI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `LI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `EQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FSM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RCKT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GWRE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TII` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `YEXT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FNKO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `EQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FSM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AGCO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VIR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SDGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NEOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 7.52 < 1 share @ 263.36 |
| 2026-09-04 | `USDE` | cash | leftover split 7.52 < 1 share @ 7.87 |
| 2026-09-04 | `CRCL` | cash | leftover split 7.52 < 1 share @ 97.98 |
| 2026-09-04 | `MSTR` | cash | leftover split 7.52 < 1 share @ 137.35 |
| 2026-09-04 | `BLSH` | cash | leftover split 7.52 < 1 share @ 34.69 |
| 2026-09-04 | `ZETA` | cash | leftover split 7.52 < 1 share @ 32.65 |
| 2026-09-04 | `HAFN` | cash | leftover split 7.52 < 1 share @ 8.94 |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SDGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `NEOV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LAND` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `XRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `VNT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `WDS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HELP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AVXL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `FATE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `STX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `HELP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TJGC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new buys |
