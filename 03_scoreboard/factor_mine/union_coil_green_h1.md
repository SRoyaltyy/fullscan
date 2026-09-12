# Factor mine action — `union_coil_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-1.78%** ($9,822) · signal-only (no cash/fees) was +0.85%. Starts YES **0/21**. Fills 141 · skips 72 · realized $-153.57.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,616.55.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TPG` | 197 | — | $50.62 | +0.00 | $54.62 | +787.37 | +787.37 | +0.00 | +787.37 |
| 2026-08-14 | `TPG` | 197 | $54.62 | $55.29 | +131.99 | — | +0.00 | +131.99 | +919.36 | — |
| 2026-08-14 | `SLG` | 23 | — | $57.61 | +0.00 | $56.09 | -34.96 | -34.96 | +0.00 | -34.96 |
| 2026-08-14 | `LDI` | 1455 | — | $0.94 | +0.00 | $0.90 | -58.20 | -58.20 | +0.00 | -58.20 |
| 2026-08-14 | `BTBT` | 909 | — | $1.50 | +0.00 | $1.57 | +63.63 | +63.63 | +0.00 | +63.63 |
| 2026-08-14 | `ANGX` | 316 | — | $4.31 | +0.00 | $4.37 | +18.96 | +18.96 | +0.00 | +18.96 |
| 2026-08-14 | `HYLN` | 326 | — | $4.18 | +0.00 | $4.06 | -39.12 | -39.12 | +0.00 | -39.12 |
| 2026-08-14 | `WDC` | 2 | — | $503.50 | +0.00 | $508.80 | +10.60 | +10.60 | +0.00 | +10.60 |
| 2026-08-14 | `ADUR` | 82 | — | $16.50 | +0.00 | $16.17 | -27.06 | -27.06 | +0.00 | -27.06 |
| 2026-08-14 | `ALGM` | 30 | — | $44.06 | +0.00 | $44.39 | +9.90 | +9.90 | +0.00 | +9.90 |
| 2026-08-17 | `SLG` | 23 | $56.09 | $55.37 | -16.56 | — | +0.00 | -16.56 | -51.52 | — |
| 2026-08-17 | `LDI` | 1455 | $0.90 | $0.91 | +14.55 | — | +0.00 | +14.55 | -43.65 | — |
| 2026-08-17 | `BTBT` | 909 | $1.57 | $1.52 | -45.45 | — | +0.00 | -45.45 | +18.18 | — |
| 2026-08-17 | `ANGX` | 316 | $4.37 | $4.60 | +72.68 | — | +0.00 | +72.68 | +91.64 | — |
| 2026-08-17 | `HYLN` | 326 | $4.06 | $4.10 | +13.04 | — | +0.00 | +13.04 | -26.08 | — |
| 2026-08-17 | `WDC` | 2 | $508.80 | $525.53 | +33.46 | — | +0.00 | +33.46 | +44.06 | — |
| 2026-08-17 | `ADUR` | 82 | $16.17 | $15.73 | -36.08 | — | +0.00 | -36.08 | -63.14 | — |
| 2026-08-17 | `ALGM` | 30 | $44.39 | $45.32 | +27.90 | — | +0.00 | +27.90 | +37.80 | — |
| 2026-08-17 | `DVN` | 58 | — | $46.18 | +0.00 | $47.57 | +80.62 | +80.62 | +0.00 | +80.62 |
| 2026-08-17 | `OCC` | 148 | — | $18.24 | +0.00 | $17.12 | -165.76 | -165.76 | +0.00 | -165.76 |
| 2026-08-17 | `ALM` | 167 | — | $16.20 | +0.00 | $16.36 | +26.72 | +26.72 | +0.00 | +26.72 |
| 2026-08-17 | `NEWP` | 390 | — | $6.94 | +0.00 | $6.66 | -109.20 | -109.20 | +0.00 | -109.20 |
| 2026-08-18 | `DVN` | 58 | $47.57 | $48.00 | +24.94 | — | +0.00 | +24.94 | +105.56 | — |
| 2026-08-18 | `OCC` | 148 | $17.12 | $16.20 | -136.16 | — | +0.00 | -136.16 | -301.92 | — |
| 2026-08-18 | `ALM` | 167 | $16.36 | $15.78 | -96.86 | — | +0.00 | -96.86 | -70.14 | — |
| 2026-08-18 | `NEWP` | 390 | $6.66 | $6.51 | -58.50 | — | +0.00 | -58.50 | -167.70 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 63 | — | $20.55 | +0.00 | $21.19 | +40.32 | +40.32 | +0.00 | +40.32 |
| 2026-08-20 | `HDSN` | 224 | — | $5.77 | +0.00 | $5.57 | -44.80 | -44.80 | +0.00 | -44.80 |
| 2026-08-20 | `IAG` | 66 | — | $19.63 | +0.00 | $20.50 | +57.42 | +57.42 | +0.00 | +57.42 |
| 2026-08-20 | `KGC` | 43 | — | $29.63 | +0.00 | $31.43 | +77.40 | +77.40 | +0.00 | +77.40 |
| 2026-08-20 | `NFGC` | 740 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `DNA` | 173 | — | $7.45 | +0.00 | $6.96 | -84.77 | -84.77 | +0.00 | -84.77 |
| 2026-08-20 | `EXK` | 120 | — | $10.77 | +0.00 | $10.97 | +24.00 | +24.00 | +0.00 | +24.00 |
| 2026-08-20 | `SCZM` | 137 | — | $9.46 | +0.00 | $9.76 | +41.10 | +41.10 | +0.00 | +41.10 |
| 2026-08-21 | `AG` | 63 | $21.19 | $21.90 | +44.73 | — | +0.00 | +44.73 | +85.05 | — |
| 2026-08-21 | `HDSN` | 224 | $5.57 | $5.67 | +22.40 | — | +0.00 | +22.40 | -22.40 | — |
| 2026-08-21 | `IAG` | 66 | $20.50 | $21.17 | +44.22 | — | +0.00 | +44.22 | +101.64 | — |
| 2026-08-21 | `KGC` | 43 | $31.43 | $32.17 | +31.82 | — | +0.00 | +31.82 | +109.22 | — |
| 2026-08-21 | `NFGC` | 740 | $1.75 | $1.79 | +29.60 | — | +0.00 | +29.60 | +29.60 | — |
| 2026-08-21 | `DNA` | 173 | $6.96 | $7.09 | +22.49 | — | +0.00 | +22.49 | -62.28 | — |
| 2026-08-21 | `EXK` | 120 | $10.97 | $11.34 | +44.40 | — | +0.00 | +44.40 | +68.40 | — |
| 2026-08-21 | `SCZM` | 137 | $9.76 | $10.26 | +68.50 | — | +0.00 | +68.50 | +109.60 | — |
| 2026-08-21 | `BTBT` | 1077 | — | $1.66 | +0.00 | $1.53 | -140.01 | -140.01 | +0.00 | -140.01 |
| 2026-08-21 | `ORBS` | 2070 | — | $0.86 | +0.00 | $0.88 | +33.12 | +33.12 | +0.00 | +33.12 |
| 2026-08-21 | `CF` | 14 | — | $127.43 | +0.00 | $129.60 | +30.38 | +30.38 | +0.00 | +30.38 |
| 2026-08-21 | `EMBC` | 329 | — | $5.43 | +0.00 | $5.23 | -65.80 | -65.80 | +0.00 | -65.80 |
| 2026-08-21 | `TXG` | 27 | — | $64.39 | +0.00 | $65.12 | +19.71 | +19.71 | +0.00 | +19.71 |
| 2026-08-21 | `DXYZ` | 51 | — | $34.89 | +0.00 | $34.43 | -23.46 | -23.46 | +0.00 | -23.46 |
| 2026-08-24 | `BTBT` | 1077 | $1.53 | $1.55 | +21.54 | — | +0.00 | +21.54 | -118.47 | — |
| 2026-08-24 | `ORBS` | 2070 | $0.88 | $0.89 | +20.70 | — | +0.00 | +20.70 | +53.82 | — |
| 2026-08-24 | `CF` | 14 | $129.60 | $129.99 | +5.46 | — | +0.00 | +5.46 | +35.84 | — |
| 2026-08-24 | `EMBC` | 329 | $5.23 | $5.20 | -11.52 | — | +0.00 | -11.52 | -77.31 | — |
| 2026-08-24 | `TXG` | 27 | $65.12 | $63.15 | -53.19 | — | +0.00 | -53.19 | -33.48 | — |
| 2026-08-24 | `DXYZ` | 51 | $34.43 | $33.10 | -67.83 | — | +0.00 | -67.83 | -91.29 | — |
| 2026-08-25 | `KURA` | 153 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `LIFE` | 56 | — | $36.96 | +0.00 | $38.56 | +89.60 | +89.60 | +0.00 | +89.60 |
| 2026-08-25 | `ETON` | 32 | — | $64.55 | +0.00 | $63.05 | -48.00 | -48.00 | +0.00 | -48.00 |
| 2026-08-25 | `ANRO` | 56 | — | $36.52 | +0.00 | $36.31 | -11.76 | -11.76 | +0.00 | -11.76 |
| 2026-08-25 | `VALE` | 138 | — | $15.01 | +0.00 | $15.33 | +44.16 | +44.16 | +0.00 | +44.16 |
| 2026-08-26 | `KURA` | 153 | $13.59 | $13.63 | +6.12 | — | +0.00 | +6.12 | +6.12 | — |
| 2026-08-26 | `LIFE` | 56 | $38.56 | $38.24 | -17.92 | — | +0.00 | -17.92 | +71.68 | — |
| 2026-08-26 | `ETON` | 32 | $63.05 | $63.60 | +17.60 | — | +0.00 | +17.60 | -30.40 | — |
| 2026-08-26 | `ANRO` | 56 | $36.31 | $35.80 | -28.56 | — | +0.00 | -28.56 | -40.32 | — |
| 2026-08-26 | `VALE` | 138 | $15.33 | $15.37 | +5.52 | — | +0.00 | +5.52 | +49.68 | — |
| 2026-08-26 | `CRMD` | 151 | — | $8.60 | +0.00 | $8.39 | -31.71 | -31.71 | +0.00 | -31.71 |
| 2026-08-26 | `RZLT` | 260 | — | $5.01 | +0.00 | $5.04 | +7.80 | +7.80 | +0.00 | +7.80 |
| 2026-08-26 | `SENS` | 137 | — | $9.48 | +0.00 | $9.34 | -19.18 | -19.18 | +0.00 | -19.18 |
| 2026-08-26 | `ACRS` | 199 | — | $6.53 | +0.00 | $6.19 | -67.66 | -67.66 | +0.00 | -67.66 |
| 2026-08-26 | `TMCI` | 273 | — | $4.78 | +0.00 | $4.72 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-08-26 | `CRDL` | 642 | — | $2.03 | +0.00 | $2.14 | +70.62 | +70.62 | +0.00 | +70.62 |
| 2026-08-26 | `LI` | 107 | — | $12.14 | +0.00 | $12.14 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `NCNO` | 67 | — | $19.33 | +0.00 | $21.51 | +146.06 | +146.06 | +0.00 | +146.06 |
| 2026-08-27 | `CRMD` | 151 | $8.39 | $8.49 | +15.10 | — | +0.00 | +15.10 | -16.61 | — |
| 2026-08-27 | `RZLT` | 260 | $5.04 | $5.07 | +7.80 | — | +0.00 | +7.80 | +15.60 | — |
| 2026-08-27 | `SENS` | 137 | $9.34 | $9.33 | -1.37 | — | +0.00 | -1.37 | -20.55 | — |
| 2026-08-27 | `ACRS` | 199 | $6.19 | $6.15 | -7.96 | — | +0.00 | -7.96 | -75.62 | — |
| 2026-08-27 | `TMCI` | 273 | $4.72 | $4.72 | +0.00 | — | +0.00 | +0.00 | -16.38 | — |
| 2026-08-27 | `CRDL` | 642 | $2.14 | $2.09 | -32.10 | — | +0.00 | -32.10 | +38.52 | — |
| 2026-08-27 | `LI` | 107 | $12.14 | $12.35 | +22.47 | — | +0.00 | +22.47 | +22.47 | — |
| 2026-08-27 | `NCNO` | 67 | $21.51 | $22.03 | +34.84 | — | +0.00 | +34.84 | +180.90 | — |
| 2026-08-27 | `RRC` | 36 | — | $41.44 | +0.00 | $41.64 | +7.20 | +7.20 | +0.00 | +7.20 |
| 2026-08-27 | `CRK` | 104 | — | $14.42 | +0.00 | $14.62 | +20.80 | +20.80 | +0.00 | +20.80 |
| 2026-08-27 | `ANET` | 7 | — | $205.90 | +0.00 | $201.09 | -33.67 | -33.67 | +0.00 | -33.67 |
| 2026-08-27 | `GEN` | 50 | — | $29.83 | +0.00 | $30.50 | +33.50 | +33.50 | +0.00 | +33.50 |
| 2026-08-27 | `MRVL` | 5 | — | $253.44 | +0.00 | $241.45 | -59.95 | -59.95 | +0.00 | -59.95 |
| 2026-08-27 | `NUE` | 5 | — | $252.00 | +0.00 | $252.37 | +1.85 | +1.85 | +0.00 | +1.85 |
| 2026-08-27 | `PGY` | 65 | — | $22.93 | +0.00 | $23.26 | +21.45 | +21.45 | +0.00 | +21.45 |
| 2026-08-28 | `RRC` | 36 | $41.64 | $41.74 | +3.60 | — | +0.00 | +3.60 | +10.80 | — |
| 2026-08-28 | `CRK` | 104 | $14.62 | $14.63 | +1.04 | $14.29 | -35.36 | -34.32 | +21.84 | -13.52 |
| 2026-08-28 | `ANET` | 7 | $201.09 | $200.00 | -7.63 | — | +0.00 | -7.63 | -41.30 | — |
| 2026-08-28 | `GEN` | 50 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +33.50 | — |
| 2026-08-28 | `MRVL` | 5 | $241.45 | $225.26 | -80.95 | — | +0.00 | -80.95 | -140.90 | — |
| 2026-08-28 | `NUE` | 5 | $252.37 | $252.76 | +1.95 | — | +0.00 | +1.95 | +3.80 | — |
| 2026-08-28 | `PGY` | 65 | $23.26 | $23.21 | -3.25 | — | +0.00 | -3.25 | +18.20 | — |
| 2026-08-28 | `EQ` | 515 | — | $2.46 | +0.00 | $2.39 | -36.05 | -36.05 | +0.00 | -36.05 |
| 2026-08-28 | `FIGR` | 33 | — | $37.49 | +0.00 | $36.05 | -47.52 | -47.52 | +0.00 | -47.52 |
| 2026-08-28 | `TH` | 66 | — | $19.00 | +0.00 | $18.55 | -29.70 | -29.70 | +0.00 | -29.70 |
| 2026-08-28 | `FSM` | 98 | — | $12.84 | +0.00 | $12.26 | -56.84 | -56.84 | +0.00 | -56.84 |
| 2026-08-28 | `ADSK` | 4 | — | $261.16 | +0.00 | $260.66 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-08-28 | `FRO` | 28 | — | $44.40 | +0.00 | $44.19 | -5.88 | -5.88 | +0.00 | -5.88 |
| 2026-08-28 | `HAFN` | 151 | — | $8.35 | +0.00 | $8.47 | +18.12 | +18.12 | +0.00 | +18.12 |
| 2026-08-31 | `CRK` | 104 | $14.29 | $14.54 | +26.00 | — | +0.00 | +26.00 | +12.48 | — |
| 2026-08-31 | `EQ` | 515 | $2.39 | $2.39 | +0.00 | — | +0.00 | +0.00 | -36.05 | — |
| 2026-08-31 | `FIGR` | 33 | $36.05 | $35.77 | -9.24 | — | +0.00 | -9.24 | -56.76 | — |
| 2026-08-31 | `TH` | 66 | $18.55 | $18.12 | -28.05 | — | +0.00 | -28.05 | -57.75 | — |
| 2026-08-31 | `FSM` | 98 | $12.26 | $12.26 | +0.00 | — | +0.00 | +0.00 | -56.84 | — |
| 2026-08-31 | `ADSK` | 4 | $260.66 | $257.71 | -11.80 | — | +0.00 | -11.80 | -13.80 | — |
| 2026-08-31 | `FRO` | 28 | $44.19 | $44.85 | +18.48 | — | +0.00 | +18.48 | +12.60 | — |
| 2026-08-31 | `HAFN` | 151 | $8.47 | $8.53 | +9.06 | — | +0.00 | +9.06 | +27.18 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 24 | — | $52.88 | +0.00 | $52.46 | -10.08 | -10.08 | +0.00 | -10.08 |
| 2026-09-03 | `HRMY` | 29 | — | $42.93 | +0.00 | $41.86 | -31.03 | -31.03 | +0.00 | -31.03 |
| 2026-09-03 | `CABA` | 349 | — | $3.63 | +0.00 | $3.48 | -52.35 | -52.35 | +0.00 | -52.35 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `ARCT` | 75 | — | $16.77 | +0.00 | $15.56 | -90.75 | -90.75 | +0.00 | -90.75 |
| 2026-09-03 | `CRDL` | 582 | — | $2.18 | +0.00 | $2.16 | -11.64 | -11.64 | +0.00 | -11.64 |
| 2026-09-03 | `SDGR` | 60 | — | $21.03 | +0.00 | $20.71 | -19.20 | -19.20 | +0.00 | -19.20 |
| 2026-09-03 | `NEOV` | 336 | — | $3.77 | +0.00 | $3.82 | +16.80 | +16.80 | +0.00 | +16.80 |
| 2026-09-04 | `ATRC` | 24 | $52.46 | $52.03 | -10.32 | — | +0.00 | -10.32 | -20.40 | — |
| 2026-09-04 | `HRMY` | 29 | $41.86 | $41.50 | -10.44 | — | +0.00 | -10.44 | -41.47 | — |
| 2026-09-04 | `CABA` | 349 | $3.48 | $3.46 | -6.98 | — | +0.00 | -6.98 | -59.33 | — |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `ARCT` | 75 | $15.56 | $15.61 | +3.75 | — | +0.00 | +3.75 | -87.00 | — |
| 2026-09-04 | `CRDL` | 582 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -11.64 | — |
| 2026-09-04 | `SDGR` | 60 | $20.71 | $20.58 | -7.80 | — | +0.00 | -7.80 | -27.00 | — |
| 2026-09-04 | `NEOV` | 336 | $3.82 | $3.83 | +3.36 | — | +0.00 | +3.36 | +20.16 | — |
| 2026-09-04 | `CRM` | 4 | — | $263.36 | +0.00 | $259.23 | -16.52 | -16.52 | +0.00 | -16.52 |
| 2026-09-04 | `USDE` | 156 | — | $7.87 | +0.00 | $7.93 | +9.36 | +9.36 | +0.00 | +9.36 |
| 2026-09-04 | `GORO` | 311 | — | $3.95 | +0.00 | $4.15 | +62.20 | +62.20 | +0.00 | +62.20 |
| 2026-09-04 | `CRCL` | 12 | — | $97.98 | +0.00 | $102.05 | +48.84 | +48.84 | +0.00 | +48.84 |
| 2026-09-04 | `MSTR` | 8 | — | $137.35 | +0.00 | $142.80 | +43.60 | +43.60 | +0.00 | +43.60 |
| 2026-09-04 | `BLSH` | 35 | — | $34.69 | +0.00 | $36.00 | +45.85 | +45.85 | +0.00 | +45.85 |
| 2026-09-04 | `ZETA` | 37 | — | $32.65 | +0.00 | $31.35 | -48.10 | -48.10 | +0.00 | -48.10 |
| 2026-09-04 | `HAFN` | 137 | — | $8.94 | +0.00 | $9.22 | +38.36 | +38.36 | +0.00 | +38.36 |
| 2026-09-08 | `CRM` | 4 | $259.23 | $253.72 | -22.04 | — | +0.00 | -22.04 | -38.56 | — |
| 2026-09-08 | `USDE` | 156 | $7.93 | $7.76 | -26.52 | — | +0.00 | -26.52 | -17.16 | — |
| 2026-09-08 | `GORO` | 311 | $4.15 | $4.13 | -6.22 | — | +0.00 | -6.22 | +55.98 | — |
| 2026-09-08 | `CRCL` | 12 | $102.05 | $100.65 | -16.80 | — | +0.00 | -16.80 | +32.04 | — |
| 2026-09-08 | `MSTR` | 8 | $142.80 | $137.62 | -41.44 | — | +0.00 | -41.44 | +2.16 | — |
| 2026-09-08 | `BLSH` | 35 | $36.00 | $35.90 | -3.50 | — | +0.00 | -3.50 | +42.35 | — |
| 2026-09-08 | `ZETA` | 37 | $31.35 | $31.08 | -9.99 | — | +0.00 | -9.99 | -58.09 | — |
| 2026-09-08 | `HAFN` | 137 | $9.22 | $8.81 | -56.17 | $8.96 | +20.55 | -35.62 | -17.81 | +2.74 |
| 2026-09-09 | `HAFN` | 137 | $8.96 | $9.00 | +5.48 | — | +0.00 | +5.48 | +8.22 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `LPTH` | 131 | — | $9.37 | +0.00 | $9.20 | -22.27 | -22.27 | +0.00 | -22.27 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +787.37 | TPG | — | $24.65 | $10,784.79 | TPG×197 |
| 2026-08-14 | +5.50 | $24.65 | TPG×197 | $10,916.78 | +131.99 | -56.25 | SLG, LDI, BTBT, ANGX, HYLN, WDC, ADUR, ALGM | TPG | $409.40 | $10,811.45 | SLG×23, LDI×1455, BTBT×909, ANGX×316, HYLN×326, WDC×2, ADUR×82, ALGM×30 |
| 2026-08-17 | +2.25 | $409.40 | SLG×23, LDI×1455, BTBT×909, ANGX×316, HYLN×326, WDC×2, ADUR×82, ALGM×30 | $10,874.99 | +63.54 | -167.62 | DVN, OCC, ALM, NEWP | SLG, LDI, BTBT, ANGX, HYLN, WDC, ADUR, ALGM | $26.34 | $10,648.68 | DVN×58, OCC×148, ALM×167, NEWP×390 |
| 2026-08-18 | -6.20 | $26.34 | DVN×58, OCC×148, ALM×167, NEWP×390 | $10,382.10 | -266.58 | +0.00 | — | DVN, OCC, ALM, NEWP | $10,369.77 | $10,369.77 | — |
| 2026-08-19 | -7.20 | $10,369.77 | — | $10,369.77 | +0.00 | +0.00 | — | — | $10,369.77 | $10,369.77 | — |
| 2026-08-20 | +1.12 | $10,369.77 | — | $10,369.77 | +0.00 | +110.67 | AG, HDSN, IAG, KGC, NFGC, DNA, EXK, SCZM | — | $14.52 | $10,454.26 | AG×63, HDSN×224, IAG×66, KGC×43, NFGC×740, DNA×173, EXK×120, SCZM×137 |
| 2026-08-21 | +3.25 | $14.52 | AG×63, HDSN×224, IAG×66, KGC×43, NFGC×740, DNA×173, EXK×120, SCZM×137 | $10,762.42 | +308.16 | -146.06 | BTBT, ORBS, CF, EMBC, TXG, DXYZ | AG, HDSN, IAG, KGC, NFGC, DNA, EXK, SCZM | $22.70 | $10,541.35 | BTBT×1077, ORBS×2070, CF×14, EMBC×329, TXG×27, DXYZ×51 |
| 2026-08-24 | -5.17 | $22.70 | BTBT×1077, ORBS×2070, CF×14, EMBC×329, TXG×27, DXYZ×51 | $10,456.52 | -84.83 | +0.00 | — | BTBT, ORBS, CF, EMBC, TXG, DXYZ | $10,406.81 | $10,406.81 | — |
| 2026-08-25 | +1.80 | $10,406.81 | — | $10,406.81 | -0.00 | +74.00 | KURA, LIFE, ETON, ANRO, VALE | — | $64.42 | $10,469.55 | KURA×153, LIFE×56, ETON×32, ANRO×56, VALE×138 |
| 2026-08-26 | +2.02 | $64.42 | KURA×153, LIFE×56, ETON×32, ANRO×56, VALE×138 | $10,452.31 | -17.24 | +89.55 | CRMD, RZLT, SENS, ACRS, TMCI, CRDL, LI, NCNO | KURA, LIFE, ETON, ANRO, VALE | $12.09 | $10,503.36 | CRMD×151, RZLT×260, SENS×137, ACRS×199, TMCI×273, CRDL×642, LI×107, NCNO×67 |
| 2026-08-27 | — | $12.09 | CRMD×151, RZLT×260, SENS×137, ACRS×199, TMCI×273, CRDL×642, LI×107, NCNO×67 | $10,542.14 | +38.78 | -8.82 | RRC, CRK, ANET, GEN, MRVL, NUE, PGY | CRMD, RZLT, SENS, ACRS, TMCI, CRDL, LI, NCNO | $557.94 | $10,491.09 | RRC×36, CRK×104, ANET×7, GEN×50, MRVL×5, NUE×5, PGY×65 |
| 2026-08-28 | +0.75 | $557.94 | RRC×36, CRK×104, ANET×7, GEN×50, MRVL×5, NUE×5, PGY×65 | $10,405.85 | -85.24 | -195.23 | EQ, FIGR, TH, FSM, ADSK, FRO, HAFN | RRC, ANET, GEN, MRVL, NUE, PGY | $286.96 | $10,178.33 | CRK×104, EQ×515, FIGR×33, TH×66, FSM×98, ADSK×4, FRO×28, HAFN×151 |
| 2026-08-31 | -5.85 | $286.96 | CRK×104, EQ×515, FIGR×33, TH×66, FSM×98, ADSK×4, FRO×28, HAFN×151 | $10,182.78 | +4.45 | +0.00 | — | CRK, EQ, FIGR, TH, FSM, ADSK, FRO, HAFN | $10,160.48 | $10,160.48 | — |
| 2026-09-01 | -6.30 | $10,160.48 | — | $10,160.48 | +0.00 | +0.00 | — | — | $10,160.48 | $10,160.48 | — |
| 2026-09-02 | -3.83 | $10,160.48 | — | $10,160.48 | +0.00 | +0.00 | — | — | $10,160.48 | $10,160.48 | — |
| 2026-09-03 | -0.90 | $10,160.48 | — | $10,160.48 | +0.00 | -214.63 | ATRC, HRMY, CABA, RVTY, ARCT, CRDL, SDGR, NEOV | — | $105.56 | $9,918.97 | ATRC×24, HRMY×29, CABA×349, RVTY×9, ARCT×75, CRDL×582, SDGR×60, NEOV×336 |
| 2026-09-04 | +2.25 | $105.56 | ATRC×24, HRMY×29, CABA×349, RVTY×9, ARCT×75, CRDL×582, SDGR×60, NEOV×336 | $9,885.14 | -33.83 | +183.59 | CRM, USDE, GORO, CRCL, MSTR, BLSH, ZETA, HAFN | ATRC, HRMY, CABA, RVTY, ARCT, CRDL, SDGR, NEOV | $407.65 | $10,022.39 | CRM×4, USDE×156, GORO×311, CRCL×12, MSTR×8, BLSH×35, ZETA×37, HAFN×137 |
| 2026-09-08 | -11.47 | $407.65 | CRM×4, USDE×156, GORO×311, CRCL×12, MSTR×8, BLSH×35, ZETA×37, HAFN×137 | $9,839.71 | -182.68 | +20.55 | — | CRM, USDE, GORO, CRCL, MSTR, BLSH, ZETA | $8,615.83 | $9,843.35 | HAFN×137 |
| 2026-09-09 | -13.95 | $8,615.83 | HAFN×137 | $9,848.83 | +5.48 | +0.00 | — | HAFN | $9,846.40 | $9,846.40 | — |
| 2026-09-10 | -13.28 | $9,846.40 | — | $9,846.40 | +0.00 | +0.00 | — | — | $9,846.40 | $9,846.40 | — |
| 2026-09-11 | +0.50 | $9,846.40 | — | $9,846.40 | +0.00 | -22.27 | LPTH | — | $8,616.55 | $9,821.75 | LPTH×131 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $10000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | 16:00 close · cash $24.65 · equity $10,784.79 vs 09:30 $10,000.00 (+784.79; session marks +787.37) · 1 name(s) marked open→close (per-name table). TPG×197 09:30 $50.62 → close $54.62 +787.37 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | 09:30 open · cash $24.65 (unchanged overnight, no fees) · equity $10,916.78 vs prior close $10,784.79 (+131.99) · 1 name(s) re-marked at the open (per-name table). TPG×197 yday $54.62 → 09:30 $55.29 +131.99 | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 197 | $55.29 | $2.70 | $+914.08 | $10,914.08 | ▲ +914.08 after sell → book $10,914.08; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 23 | $57.61 | $2.06 | — | $9,586.99 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1455 | $0.94 | $18.00 | — | $8,205.66 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 909 | $1.50 | $11.73 | — | $6,830.43 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 316 | $4.31 | $4.08 | — | $5,464.39 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 326 | $4.18 | $4.21 | — | $4,097.51 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $3,088.51 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable; 🔵; ⚪; ret5=+7.9; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 82 | $16.50 | $2.24 | — | $1,733.28 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 30 | $44.06 | $2.08 | — | $409.40 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable; 🔵; ret5=+3.9; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $409.40 | ▼ close $10,811.45 vs 09:30 $10,916.78 (session -56.25) | 16:00 close · cash $409.40 · equity $10,811.45 vs 09:30 $10,916.78 (-105.33; session marks -56.25) · 8 name(s) marked open→close (per-name table). SLG×23 09:30 $57.61 → close $56.09 -34.96; LDI×1455 09:30 $0.94 → close $0.90 -58.20; BTBT×909 09:30 $1.50 → close $1.57 +63.63; ANGX×316 09:30 $4.31 → close $4.37 +18.96; HYLN×326 09:30 $4.18 → close $4.06 -39.12; WDC×2 09:30 $503.50 → close $508.80 +10.60; ADUR×82 09:30 $16.50 → close $16.17 -27.06; ALGM×30 09:30 $44.06 → close $44.39 +9.90 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $409.40 | ▲ 09:30 equity $10,874.99 vs yday $10,811.45 (+63.54) | 09:30 open · cash $409.40 (unchanged overnight, no fees) · equity $10,874.99 vs prior close $10,811.45 (+63.54) · 8 name(s) re-marked at the open (per-name table). SLG×23 yday $56.09 → 09:30 $55.37 -16.56; LDI×1455 yday $0.90 → 09:30 $0.91 +14.55; BTBT×909 yday $1.57 → 09:30 $1.52 -45.45; ANGX×316 yday $4.37 → 09:30 $4.60 +72.68; HYLN×326 yday $4.06 → 09:30 $4.10 +13.04; WDC×2 yday $508.80 → 09:30 $525.53 +33.46; ADUR×82 yday $16.17 → 09:30 $15.73 -36.08; ALGM×30 yday $44.39 → 09:30 $45.32 +27.90 | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 23 | $55.37 | $2.08 | $-55.66 | $1,680.83 | ▼ -55.66 after sell → book $10,872.91; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1455 | $0.91 | $17.81 | $-79.46 | $2,982.70 | ▼ -79.46 after sell → book $10,855.10; vs 09:30 mark -17.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 909 | $1.52 | $11.89 | $-5.43 | $4,352.49 | ▼ -5.43 after sell → book $10,843.21; vs 09:30 mark -11.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 316 | $4.60 | $4.14 | $+83.42 | $5,801.95 | ▲ +83.42 after sell → book $10,839.07; vs 09:30 mark -4.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 326 | $4.10 | $4.27 | $-34.56 | $7,134.28 | ▼ -34.56 after sell → book $10,834.80; vs 09:30 mark -4.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $8,183.32 | ▲ +40.05 after sell → book $10,832.78; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 82 | $15.73 | $2.26 | $-67.64 | $9,470.92 | ▼ -67.64 after sell → book $10,830.52; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ALGM` | 30 | $45.32 | $2.10 | $+33.62 | $10,828.42 | ▲ +33.62 after sell → book $10,828.42; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 58 | $46.18 | $2.16 | — | $8,147.82 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+6.7; leftover $2707.11 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 148 | $18.24 | $2.43 | — | $5,445.86 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $2707.11 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 167 | $16.20 | $2.49 | — | $2,737.97 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $2707.11 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NEWP` | 390 | $6.94 | $5.03 | — | $26.34 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.1; leftover $2707.11 | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.34 | ▼ close $10,648.68 vs 09:30 $10,874.99 (session -167.62) | 16:00 close · cash $26.34 · equity $10,648.68 vs 09:30 $10,874.99 (-226.31; session marks -167.62) · 4 name(s) marked open→close (per-name table). DVN×58 09:30 $46.18 → close $47.57 +80.62; OCC×148 09:30 $18.24 → close $17.12 -165.76; ALM×167 09:30 $16.20 → close $16.36 +26.72; NEWP×390 09:30 $6.94 → close $6.66 -109.20 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.34 | ▼ 09:30 equity $10,382.10 vs yday $10,648.68 (-266.58) | 09:30 open · cash $26.34 (unchanged overnight, no fees) · equity $10,382.10 vs prior close $10,648.68 (-266.58) · 4 name(s) re-marked at the open (per-name table). DVN×58 yday $47.57 → 09:30 $48.00 +24.94; OCC×148 yday $17.12 → 09:30 $16.20 -136.16; ALM×167 yday $16.36 → 09:30 $15.78 -96.86; NEWP×390 yday $6.66 → 09:30 $6.51 -58.50 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 58 | $48.00 | $2.20 | $+101.20 | $2,808.15 | ▲ +101.20 after sell → book $10,379.91; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 148 | $16.20 | $2.48 | $-306.83 | $5,203.27 | ▼ -306.83 after sell → book $10,377.43; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 167 | $15.78 | $2.54 | $-75.17 | $7,835.99 | ▼ -75.17 after sell → book $10,374.89; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NEWP` | 390 | $6.51 | $5.12 | $-177.85 | $10,369.77 | ▼ -177.85 after sell → book $10,369.77; vs 09:30 mark -5.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,369.77 | ▲ close $10,369.77 vs 09:30 $10,382.10 (session +0.00) | 16:00 close · cash $10,369.77 · no lots left · equity $10,369.77. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,369.77 | ▲ 09:30 equity $10,369.77 vs yday $10,369.77 (+0.00) | 09:30 open · cash $10,369.77 · no holdings · equity $10,369.77 vs prior close $10,369.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,369.77 | ▲ close $10,369.77 vs 09:30 $10,369.77 (session +0.00) | 16:00 close · cash $10,369.77 · no lots left · equity $10,369.77. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,369.77 | ▲ 09:30 equity $10,369.77 vs yday $10,369.77 (+0.00) | 09:30 open · cash $10,369.77 · no holdings · equity $10,369.77 vs prior close $10,369.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 63 | $20.55 | $2.18 | — | $9,072.94 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1296.22 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 224 | $5.77 | $2.89 | — | $7,777.57 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1296.22 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 66 | $19.63 | $2.19 | — | $6,479.81 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1296.22 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $5,203.60 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1296.22 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 740 | $1.75 | $9.55 | — | $3,899.05 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1296.22 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 173 | $7.45 | $2.51 | — | $2,607.69 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1296.22 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 120 | $10.77 | $2.35 | — | $1,312.94 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1296.22 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 137 | $9.46 | $2.40 | — | $14.52 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1296.22 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.52 | ▲ close $10,454.26 vs 09:30 $10,369.77 (session +110.67) | 16:00 close · cash $14.52 · equity $10,454.26 vs 09:30 $10,369.77 (+84.49; session marks +110.67) · 8 name(s) marked open→close (per-name table). AG×63 09:30 $20.55 → close $21.19 +40.32; HDSN×224 09:30 $5.77 → close $5.57 -44.80; IAG×66 09:30 $19.63 → close $20.50 +57.42; KGC×43 09:30 $29.63 → close $31.43 +77.40; NFGC×740 09:30 $1.75 → close $1.75 +0.00; DNA×173 09:30 $7.45 → close $6.96 -84.77; EXK×120 09:30 $10.77 → close $10.97 +24.00; SCZM×137 09:30 $9.46 → close $9.76 +41.10 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.52 | ▲ 09:30 equity $10,762.42 vs yday $10,454.26 (+308.16) | 09:30 open · cash $14.52 (unchanged overnight, no fees) · equity $10,762.42 vs prior close $10,454.26 (+308.16) · 8 name(s) re-marked at the open (per-name table). AG×63 yday $21.19 → 09:30 $21.90 +44.73; HDSN×224 yday $5.57 → 09:30 $5.67 +22.40; IAG×66 yday $20.50 → 09:30 $21.17 +44.22; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×740 yday $1.75 → 09:30 $1.79 +29.60; DNA×173 yday $6.96 → 09:30 $7.09 +22.49; EXK×120 yday $10.97 → 09:30 $11.34 +44.40; SCZM×137 yday $9.76 → 09:30 $10.26 +68.50 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 63 | $21.90 | $2.20 | $+80.67 | $1,392.02 | ▲ +80.67 after sell → book $10,760.22; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 224 | $5.67 | $2.94 | $-28.23 | $2,659.16 | ▼ -28.23 after sell → book $10,757.28; vs 09:30 mark -2.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 66 | $21.17 | $2.21 | $+97.24 | $4,054.17 | ▲ +97.24 after sell → book $10,755.07; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 43 | $32.17 | $2.14 | $+104.96 | $5,435.34 | ▲ +104.96 after sell → book $10,752.93; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 740 | $1.79 | $9.68 | $+10.37 | $6,750.26 | ▲ +10.37 after sell → book $10,743.25; vs 09:30 mark -9.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 173 | $7.09 | $2.55 | $-67.34 | $7,974.29 | ▼ -67.34 after sell → book $10,740.71; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `EXK` | 120 | $11.34 | $2.38 | $+63.67 | $9,332.70 | ▲ +63.67 after sell → book $10,738.32; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 137 | $10.26 | $2.44 | $+104.76 | $10,735.89 | ▲ +104.76 after sell → book $10,735.89; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 1077 | $1.66 | $13.89 | — | $8,934.18 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1789.31 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 2070 | $0.86 | $24.09 | — | $7,121.60 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1789.31 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 14 | $127.43 | $2.03 | — | $5,335.55 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable; 🔵; ⚪; ret5=+7.9; leftover $1789.31 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 329 | $5.43 | $4.24 | — | $3,544.84 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $1789.31 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TXG` | 27 | $64.39 | $2.07 | — | $1,804.23 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1789.31 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DXYZ` | 51 | $34.89 | $2.14 | — | $22.70 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.6; leftover $1789.31 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🔴 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.70 | ▼ close $10,541.35 vs 09:30 $10,762.42 (session -146.06) | 16:00 close · cash $22.70 · equity $10,541.35 vs 09:30 $10,762.42 (-221.07; session marks -146.06) · 6 name(s) marked open→close (per-name table). BTBT×1077 09:30 $1.66 → close $1.53 -140.01; ORBS×2070 09:30 $0.86 → close $0.88 +33.12; CF×14 09:30 $127.43 → close $129.60 +30.38; EMBC×329 09:30 $5.43 → close $5.23 -65.80; TXG×27 09:30 $64.39 → close $65.12 +19.71; DXYZ×51 09:30 $34.89 → close $34.43 -23.46 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.70 | ▼ 09:30 equity $10,456.52 vs yday $10,541.35 (-84.83) | 09:30 open · cash $22.70 (unchanged overnight, no fees) · equity $10,456.52 vs prior close $10,541.35 (-84.83) · 6 name(s) re-marked at the open (per-name table). BTBT×1077 yday $1.53 → 09:30 $1.55 +21.54; ORBS×2070 yday $0.88 → 09:30 $0.89 +20.70; CF×14 yday $129.60 → 09:30 $129.99 +5.46; EMBC×329 yday $5.23 → 09:30 $5.20 -11.52; TXG×27 yday $65.12 → 09:30 $63.15 -53.19; DXYZ×51 yday $34.43 → 09:30 $33.10 -67.83 | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 1077 | $1.55 | $14.09 | $-146.45 | $1,677.97 | ▼ -146.45 after sell → book $10,442.43; vs 09:30 mark -14.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 2070 | $0.89 | $24.99 | $+4.73 | $3,495.27 | ▲ +4.73 after sell → book $10,417.44; vs 09:30 mark -24.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 14 | $129.99 | $2.06 | $+31.75 | $5,313.08 | ▲ +31.75 after sell → book $10,415.38; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `EMBC` | 329 | $5.20 | $4.31 | $-85.87 | $7,017.92 | ▼ -85.87 after sell → book $10,411.07; vs 09:30 mark -4.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TXG` | 27 | $63.15 | $2.09 | $-37.65 | $8,720.88 | ▼ -37.65 after sell → book $10,408.98; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DXYZ` | 51 | $33.10 | $2.17 | $-95.60 | $10,406.81 | ▼ -95.60 after sell → book $10,406.81; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,406.81 | ▲ close $10,406.81 vs 09:30 $10,456.52 (session +0.00) | 16:00 close · cash $10,406.81 · no lots left · equity $10,406.81. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,406.81 | ▲ 09:30 equity $10,406.81 vs yday $10,406.81 (-0.00) | 09:30 open · cash $10,406.81 · no holdings · equity $10,406.81 vs prior close $10,406.81 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 153 | $13.59 | $2.45 | — | $8,325.09 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $2081.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 56 | $36.96 | $2.16 | — | $6,253.17 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $2081.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 32 | $64.55 | $2.09 | — | $4,185.49 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+4.4; leftover $2081.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ANRO` | 56 | $36.52 | $2.16 | — | $2,138.21 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.9; leftover $2081.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VALE` | 138 | $15.01 | $2.40 | — | $64.42 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; ⚪; ret5=+9.4; leftover $2081.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $64.42 | ▲ close $10,469.55 vs 09:30 $10,406.81 (session +74.00) | 16:00 close · cash $64.42 · equity $10,469.55 vs 09:30 $10,406.81 (+62.74; session marks +74.00) · 5 name(s) marked open→close (per-name table). KURA×153 09:30 $13.59 → close $13.59 +0.00; LIFE×56 09:30 $36.96 → close $38.56 +89.60; ETON×32 09:30 $64.55 → close $63.05 -48.00; ANRO×56 09:30 $36.52 → close $36.31 -11.76; VALE×138 09:30 $15.01 → close $15.33 +44.16 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.42 | ▼ 09:30 equity $10,452.31 vs yday $10,469.55 (-17.24) | 09:30 open · cash $64.42 (unchanged overnight, no fees) · equity $10,452.31 vs prior close $10,469.55 (-17.24) · 5 name(s) re-marked at the open (per-name table). KURA×153 yday $13.59 → 09:30 $13.63 +6.12; LIFE×56 yday $38.56 → 09:30 $38.24 -17.92; ETON×32 yday $63.05 → 09:30 $63.60 +17.60; ANRO×56 yday $36.31 → 09:30 $35.80 -28.56; VALE×138 yday $15.33 → 09:30 $15.37 +5.52 | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 153 | $13.63 | $2.49 | $+1.18 | $2,147.32 | ▲ +1.18 after sell → book $10,449.82; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 56 | $38.24 | $2.19 | $+67.34 | $4,286.58 | ▲ +67.34 after sell → book $10,447.64; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 32 | $63.60 | $2.11 | $-34.60 | $6,319.67 | ▼ -34.60 after sell → book $10,445.53; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ANRO` | 56 | $35.80 | $2.18 | $-44.66 | $8,322.28 | ▼ -44.66 after sell → book $10,443.34; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `VALE` | 138 | $15.37 | $2.44 | $+44.83 | $10,440.90 | ▲ +44.83 after sell → book $10,440.90; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `CRMD` | 151 | $8.60 | $2.44 | — | $9,139.86 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+4.8; leftover $1305.11 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `RZLT` | 260 | $5.01 | $3.35 | — | $7,833.90 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,yday_gainer; 🔵; ret5=+7.5; leftover $1305.11 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SENS` | 137 | $9.48 | $2.40 | — | $6,532.74 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $1305.11 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ACRS` | 199 | $6.53 | $2.59 | — | $5,230.68 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.6; leftover $1305.11 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TMCI` | 273 | $4.78 | $3.52 | — | $3,922.22 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+8.1; leftover $1305.11 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CRDL` | 642 | $2.03 | $8.28 | — | $2,610.68 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+5.5; leftover $1305.11 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟡 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `LI` | 107 | $12.14 | $2.31 | — | $1,309.39 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+1.2; leftover $1305.11 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NCNO` | 67 | $19.33 | $2.19 | — | $12.09 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+3.0; leftover $1305.11 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.09 | ▲ close $10,503.36 vs 09:30 $10,452.31 (session +89.55) | 16:00 close · cash $12.09 · equity $10,503.36 vs 09:30 $10,452.31 (+51.05; session marks +89.55) · 8 name(s) marked open→close (per-name table). CRMD×151 09:30 $8.60 → close $8.39 -31.71; RZLT×260 09:30 $5.01 → close $5.04 +7.80; SENS×137 09:30 $9.48 → close $9.34 -19.18; ACRS×199 09:30 $6.53 → close $6.19 -67.66; TMCI×273 09:30 $4.78 → close $4.72 -16.38; CRDL×642 09:30 $2.03 → close $2.14 +70.62; LI×107 09:30 $12.14 → close $12.14 +0.00; NCNO×67 09:30 $19.33 → close $21.51 +146.06 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.09 | ▲ 09:30 equity $10,542.14 vs yday $10,503.36 (+38.78) | 09:30 open · cash $12.09 (unchanged overnight, no fees) · equity $10,542.14 vs prior close $10,503.36 (+38.78) · 8 name(s) re-marked at the open (per-name table). CRMD×151 yday $8.39 → 09:30 $8.49 +15.10; RZLT×260 yday $5.04 → 09:30 $5.07 +7.80; SENS×137 yday $9.34 → 09:30 $9.33 -1.37; ACRS×199 yday $6.19 → 09:30 $6.15 -7.96; TMCI×273 yday $4.72 → 09:30 $4.72 +0.00; CRDL×642 yday $2.14 → 09:30 $2.09 -32.10; LI×107 yday $12.14 → 09:30 $12.35 +22.47; NCNO×67 yday $21.51 → 09:30 $22.03 +34.84 | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 151 | $8.49 | $2.48 | $-21.53 | $1,291.60 | ▼ -21.53 after sell → book $10,539.66; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 260 | $5.07 | $3.41 | $+8.84 | $2,606.39 | ▲ +8.84 after sell → book $10,536.25; vs 09:30 mark -3.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SENS` | 137 | $9.33 | $2.43 | $-25.38 | $3,882.17 | ▼ -25.38 after sell → book $10,533.82; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ACRS` | 199 | $6.15 | $2.63 | $-80.84 | $5,103.39 | ▼ -80.84 after sell → book $10,531.19; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TMCI` | 273 | $4.72 | $3.58 | $-23.48 | $6,388.37 | ▼ -23.48 after sell → book $10,527.61; vs 09:30 mark -3.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 642 | $2.09 | $8.40 | $+21.84 | $7,721.75 | ▲ +21.84 after sell → book $10,519.21; vs 09:30 mark -8.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `LI` | 107 | $12.35 | $2.34 | $+17.82 | $9,040.86 | ▲ +17.82 after sell → book $10,516.87; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NCNO` | 67 | $22.03 | $2.21 | $+176.50 | $10,514.66 | ▲ +176.50 after sell → book $10,514.66; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 36 | $41.44 | $2.10 | — | $9,020.72 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+3.1; leftover $1502.09 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 104 | $14.42 | $2.30 | — | $7,518.74 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+7.1; leftover $1502.09 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 7 | $205.90 | $2.01 | — | $6,075.43 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+8.5; leftover $1502.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 50 | $29.83 | $2.14 | — | $4,581.79 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+7.6; leftover $1502.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $253.44 | $2.00 | — | $3,312.58 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+3.3; leftover $1502.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NUE` | 5 | $252.00 | $2.00 | — | $2,050.58 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+1.6; leftover $1502.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 65 | $22.93 | $2.19 | — | $557.94 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+9.5; leftover $1502.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $557.94 | ▼ close $10,491.09 vs 09:30 $10,542.14 (session -8.82) | 16:00 close · cash $557.94 · equity $10,491.09 vs 09:30 $10,542.14 (-51.05; session marks -8.82) · 7 name(s) marked open→close (per-name table). RRC×36 09:30 $41.44 → close $41.64 +7.20; CRK×104 09:30 $14.42 → close $14.62 +20.80; ANET×7 09:30 $205.90 → close $201.09 -33.67; GEN×50 09:30 $29.83 → close $30.50 +33.50; MRVL×5 09:30 $253.44 → close $241.45 -59.95; NUE×5 09:30 $252.00 → close $252.37 +1.85; PGY×65 09:30 $22.93 → close $23.26 +21.45 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $557.94 | ▼ 09:30 equity $10,405.85 vs yday $10,491.09 (-85.24) | 09:30 open · cash $557.94 (unchanged overnight, no fees) · equity $10,405.85 vs prior close $10,491.09 (-85.24) · 7 name(s) re-marked at the open (per-name table). RRC×36 yday $41.64 → 09:30 $41.74 +3.60; CRK×104 yday $14.62 → 09:30 $14.63 +1.04; ANET×7 yday $201.09 → 09:30 $200.00 -7.63; GEN×50 yday $30.50 → 09:30 $30.50 +0.00; MRVL×5 yday $241.45 → 09:30 $225.26 -80.95; NUE×5 yday $252.37 → 09:30 $252.76 +1.95; PGY×65 yday $23.26 → 09:30 $23.21 -3.25 | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 36 | $41.74 | $2.12 | $+6.58 | $2,058.46 | ▲ +6.58 after sell → book $10,403.73; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 7 | $200.00 | $2.03 | $-45.34 | $3,456.43 | ▼ -45.34 after sell → book $10,401.70; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 50 | $30.50 | $2.16 | $+29.20 | $4,979.27 | ▲ +29.20 after sell → book $10,399.54; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $225.26 | $2.02 | $-144.93 | $6,103.54 | ▼ -144.93 after sell → book $10,397.51; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `NUE` | 5 | $252.76 | $2.03 | $-0.23 | $7,365.32 | ▼ -0.23 after sell → book $10,395.49; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 65 | $23.21 | $2.21 | $+13.81 | $8,871.76 | ▲ +13.81 after sell → book $10,393.28; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `EQ` | 515 | $2.46 | $6.64 | — | $7,598.22 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+7.9; leftover $1267.39 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 33 | $37.49 | $2.09 | — | $6,358.96 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+5.4; leftover $1267.39 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 66 | $19.00 | $2.19 | — | $5,102.77 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+7.5; leftover $1267.39 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FSM` | 98 | $12.84 | $2.28 | — | $3,842.17 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+7.6; leftover $1267.39 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $2,795.52 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+7.8; leftover $1267.39 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 28 | $44.40 | $2.07 | — | $1,550.25 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+0.4; leftover $1267.39 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 151 | $8.35 | $2.44 | — | $286.96 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+5.1; leftover $1267.39 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $286.96 | ▼ close $10,178.33 vs 09:30 $10,405.85 (session -195.23) | 16:00 close · cash $286.96 · equity $10,178.33 vs 09:30 $10,405.85 (-227.52; session marks -195.23) · 8 name(s) marked open→close (per-name table). CRK×104 09:30 $14.63 → close $14.29 -35.36; EQ×515 09:30 $2.46 → close $2.39 -36.05; FIGR×33 09:30 $37.49 → close $36.05 -47.52; TH×66 09:30 $19.00 → close $18.55 -29.70; FSM×98 09:30 $12.84 → close $12.26 -56.84; ADSK×4 09:30 $261.16 → close $260.66 -2.00; FRO×28 09:30 $44.40 → close $44.19 -5.88; HAFN×151 09:30 $8.35 → close $8.47 +18.12 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $286.96 | ▲ 09:30 equity $10,182.78 vs yday $10,178.33 (+4.45) | 09:30 open · cash $286.96 (unchanged overnight, no fees) · equity $10,182.78 vs prior close $10,178.33 (+4.45) · 8 name(s) re-marked at the open (per-name table). CRK×104 yday $14.29 → 09:30 $14.54 +26.00; EQ×515 yday $2.39 → 09:30 $2.39 +0.00; FIGR×33 yday $36.05 → 09:30 $35.77 -9.24; TH×66 yday $18.55 → 09:30 $18.12 -28.05; FSM×98 yday $12.26 → 09:30 $12.26 +0.00; ADSK×4 yday $260.66 → 09:30 $257.71 -11.80; FRO×28 yday $44.19 → 09:30 $44.85 +18.48; HAFN×151 yday $8.47 → 09:30 $8.53 +9.06 | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 104 | $14.54 | $2.33 | $+7.85 | $1,796.78 | ▲ +7.85 after sell → book $10,180.44; vs 09:30 mark -2.34 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `EQ` | 515 | $2.39 | $6.74 | $-49.43 | $3,020.90 | ▼ -49.43 after sell → book $10,173.71; vs 09:30 mark -6.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 33 | $35.77 | $2.11 | $-60.96 | $4,199.20 | ▼ -60.96 after sell → book $10,171.60; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 66 | $18.12 | $2.21 | $-62.15 | $5,393.24 | ▼ -62.15 after sell → book $10,169.39; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `FSM` | 98 | $12.26 | $2.31 | $-61.43 | $6,592.41 | ▼ -61.43 after sell → book $10,167.08; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-17.82 | $7,621.23 | ▼ -17.82 after sell → book $10,165.06; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 28 | $44.85 | $2.09 | $+8.43 | $8,874.93 | ▲ +8.43 after sell → book $10,162.96; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 151 | $8.53 | $2.48 | $+22.26 | $10,160.48 | ▲ +22.26 after sell → book $10,160.48; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,160.48 | ▲ close $10,160.48 vs 09:30 $10,182.78 (session +0.00) | 16:00 close · cash $10,160.48 · no lots left · equity $10,160.48. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,160.48 | ▲ 09:30 equity $10,160.48 vs yday $10,160.48 (+0.00) | 09:30 open · cash $10,160.48 · no holdings · equity $10,160.48 vs prior close $10,160.48 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,160.48 | ▲ close $10,160.48 vs 09:30 $10,160.48 (session +0.00) | 16:00 close · cash $10,160.48 · no lots left · equity $10,160.48. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,160.48 | ▲ 09:30 equity $10,160.48 vs yday $10,160.48 (+0.00) | 09:30 open · cash $10,160.48 · no holdings · equity $10,160.48 vs prior close $10,160.48 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,160.48 | ▲ close $10,160.48 vs 09:30 $10,160.48 (session +0.00) | 16:00 close · cash $10,160.48 · no lots left · equity $10,160.48. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,160.48 | ▲ 09:30 equity $10,160.48 vs yday $10,160.48 (+0.00) | 09:30 open · cash $10,160.48 · no holdings · equity $10,160.48 vs prior close $10,160.48 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $52.88 | $2.06 | — | $8,889.30 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1270.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $7,642.25 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1270.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 349 | $3.63 | $4.50 | — | $6,370.88 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1270.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $5,176.82 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1270.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 75 | $16.77 | $2.21 | — | $3,916.85 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1270.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 582 | $2.18 | $7.51 | — | $2,640.58 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1270.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SDGR` | 60 | $21.03 | $2.17 | — | $1,376.61 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $1270.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NEOV` | 336 | $3.77 | $4.33 | — | $105.56 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; 🔵; ⚪; ret5=+8.6; leftover $1270.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $105.56 | ▼ close $9,918.97 vs 09:30 $10,160.48 (session -214.63) | 16:00 close · cash $105.56 · equity $9,918.97 vs 09:30 $10,160.48 (-241.51; session marks -214.63) · 8 name(s) marked open→close (per-name table). ATRC×24 09:30 $52.88 → close $52.46 -10.08; HRMY×29 09:30 $42.93 → close $41.86 -31.03; CABA×349 09:30 $3.63 → close $3.48 -52.35; RVTY×9 09:30 $132.45 → close $130.63 -16.38; ARCT×75 09:30 $16.77 → close $15.56 -90.75; CRDL×582 09:30 $2.18 → close $2.16 -11.64; SDGR×60 09:30 $21.03 → close $20.71 -19.20; NEOV×336 09:30 $3.77 → close $3.82 +16.80 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $105.56 | ▼ 09:30 equity $9,885.14 vs yday $9,918.97 (-33.83) | 09:30 open · cash $105.56 (unchanged overnight, no fees) · equity $9,885.14 vs prior close $9,918.97 (-33.83) · 8 name(s) re-marked at the open (per-name table). ATRC×24 yday $52.46 → 09:30 $52.03 -10.32; HRMY×29 yday $41.86 → 09:30 $41.50 -10.44; CABA×349 yday $3.48 → 09:30 $3.46 -6.98; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; ARCT×75 yday $15.56 → 09:30 $15.61 +3.75; CRDL×582 yday $2.16 → 09:30 $2.16 +0.00; SDGR×60 yday $20.71 → 09:30 $20.58 -7.80; NEOV×336 yday $3.82 → 09:30 $3.83 +3.36 | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 24 | $52.03 | $2.08 | $-24.54 | $1,352.20 | ▼ -24.54 after sell → book $9,883.06; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 29 | $41.50 | $2.10 | $-45.64 | $2,553.60 | ▼ -45.64 after sell → book $9,880.96; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 349 | $3.46 | $4.57 | $-68.40 | $3,756.57 | ▼ -68.40 after sell → book $9,876.39; vs 09:30 mark -4.57 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $4,924.80 | ▼ -25.83 after sell → book $9,874.35; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 75 | $15.61 | $2.24 | $-91.45 | $6,093.31 | ▼ -91.45 after sell → book $9,872.11; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 582 | $2.16 | $7.61 | $-26.76 | $7,342.82 | ▼ -26.76 after sell → book $9,864.50; vs 09:30 mark -7.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SDGR` | 60 | $20.58 | $2.19 | $-31.36 | $8,575.43 | ▼ -31.36 after sell → book $9,862.31; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NEOV` | 336 | $3.83 | $4.40 | $+11.43 | $9,857.91 | ▲ +11.43 after sell → book $9,857.91; vs 09:30 mark -4.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $8,802.47 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1232.24 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 156 | $7.87 | $2.46 | — | $7,572.29 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.7; leftover $1232.24 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GORO` | 311 | $3.95 | $4.01 | — | $6,339.83 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+6.9; leftover $1232.24 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRCL` | 12 | $97.98 | $2.03 | — | $5,162.04 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.5; leftover $1232.24 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 8 | $137.35 | $2.01 | — | $4,061.23 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+5.4; leftover $1232.24 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BLSH` | 35 | $34.69 | $2.10 | — | $2,844.98 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.9; leftover $1232.24 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ZETA` | 37 | $32.65 | $2.10 | — | $1,634.83 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+8.1; leftover $1232.24 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 137 | $8.94 | $2.40 | — | $407.65 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+7.7; leftover $1232.24 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $407.65 | ▲ close $10,022.39 vs 09:30 $9,885.14 (session +183.59) | 16:00 close · cash $407.65 · equity $10,022.39 vs 09:30 $9,885.14 (+137.25; session marks +183.59) · 8 name(s) marked open→close (per-name table). CRM×4 09:30 $263.36 → close $259.23 -16.52; USDE×156 09:30 $7.87 → close $7.93 +9.36; GORO×311 09:30 $3.95 → close $4.15 +62.20; CRCL×12 09:30 $97.98 → close $102.05 +48.84; MSTR×8 09:30 $137.35 → close $142.80 +43.60; BLSH×35 09:30 $34.69 → close $36.00 +45.85; ZETA×37 09:30 $32.65 → close $31.35 -48.10; HAFN×137 09:30 $8.94 → close $9.22 +38.36 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $407.65 | ▼ 09:30 equity $9,839.71 vs yday $10,022.39 (-182.68) | 09:30 open · cash $407.65 (unchanged overnight, no fees) · equity $9,839.71 vs prior close $10,022.39 (-182.68) · 8 name(s) re-marked at the open (per-name table). CRM×4 yday $259.23 → 09:30 $253.72 -22.04; USDE×156 yday $7.93 → 09:30 $7.76 -26.52; GORO×311 yday $4.15 → 09:30 $4.13 -6.22; CRCL×12 yday $102.05 → 09:30 $100.65 -16.80; MSTR×8 yday $142.80 → 09:30 $137.62 -41.44; BLSH×35 yday $36.00 → 09:30 $35.90 -3.50; ZETA×37 yday $31.35 → 09:30 $31.08 -9.99; HAFN×137 yday $9.22 → 09:30 $8.81 -56.17 | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $1,420.51 | ▼ -42.58 after sell → book $9,837.69; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 156 | $7.76 | $2.49 | $-22.11 | $2,628.57 | ▼ -22.11 after sell → book $9,835.19; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `GORO` | 311 | $4.13 | $4.07 | $+47.89 | $3,908.93 | ▲ +47.89 after sell → book $9,831.12; vs 09:30 mark -4.07 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `CRCL` | 12 | $100.65 | $2.05 | $+27.97 | $5,114.68 | ▲ +27.97 after sell → book $9,829.07; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MSTR` | 8 | $137.62 | $2.03 | $-1.89 | $6,213.61 | ▼ -1.89 after sell → book $9,827.04; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `BLSH` | 35 | $35.90 | $2.12 | $+38.14 | $7,468.00 | ▲ +38.14 after sell → book $9,824.93; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ZETA` | 37 | $31.08 | $2.12 | $-62.31 | $8,615.83 | ▼ -62.31 after sell → book $9,822.80; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,615.83 | ▲ close $9,843.35 vs 09:30 $9,839.71 (session +20.55) | 16:00 close · cash $8,615.83 · equity $9,843.35 vs 09:30 $9,839.71 (+3.64; session marks +20.55) · 1 name(s) marked open→close (per-name table). HAFN×137 09:30 $8.81 → close $8.96 +20.55 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,615.83 | ▲ 09:30 equity $9,848.83 vs yday $9,843.35 (+5.48) | 09:30 open · cash $8,615.83 (unchanged overnight, no fees) · equity $9,848.83 vs prior close $9,843.35 (+5.48) · 1 name(s) re-marked at the open (per-name table). HAFN×137 yday $8.96 → 09:30 $9.00 +5.48 | — |
| 2026-09-09 09:30 ET | **SELL** | `HAFN` | 137 | $9.00 | $2.43 | $+3.39 | $9,846.40 | ▲ +3.39 after sell → book $9,846.40; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,846.40 | ▲ close $9,846.40 vs 09:30 $9,848.83 (session +0.00) | 16:00 close · cash $9,846.40 · no lots left · equity $9,846.40. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,846.40 | ▲ 09:30 equity $9,846.40 vs yday $9,846.40 (+0.00) | 09:30 open · cash $9,846.40 · no holdings · equity $9,846.40 vs prior close $9,846.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,846.40 | ▲ close $9,846.40 vs 09:30 $9,846.40 (session +0.00) | 16:00 close · cash $9,846.40 · no lots left · equity $9,846.40. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,846.40 | ▲ 09:30 equity $9,846.40 vs yday $9,846.40 (+0.00) | 09:30 open · cash $9,846.40 · no holdings · equity $9,846.40 vs prior close $9,846.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 131 | $9.37 | $2.38 | — | $8,616.55 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; 🔵; ret5=+6.9; leftover $1230.80 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,616.55 | ▼ close $9,821.75 vs 09:30 $9,846.40 (session -22.27) | 16:00 close · cash $8,616.55 · equity $9,821.75 vs 09:30 $9,846.40 (-24.65; session marks -22.27) · 1 name(s) marked open→close (per-name table). LPTH×131 09:30 $9.37 → close $9.20 -22.27 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
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
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABAT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RCKT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GWRE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TII` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `YEXT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FNKO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WK` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LAND` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `XRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `VNT` | hard_red | hard-red S=-11.47 sit; no new buys |
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
| 2026-09-11 | `SANM` | no_price | no 09:30 open |
| 2026-09-11 | `NVT` | no_price | no 09:30 open |
| 2026-09-11 | `CLOV` | no_price | no 09:30 open |
| 2026-09-11 | `APPS` | no_price | no 09:30 open |
| 2026-09-11 | `VIST` | no_price | no 09:30 open |
| 2026-09-11 | `ZSQR` | no_price | no 09:30 open |
| 2026-09-11 | `TJGC` | no_price | no 09:30 open |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `LPTH` | 131 | 2026-09-11 @ $9.37 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; 🔵; ret5=+6.9; leftover $1230.80 |
