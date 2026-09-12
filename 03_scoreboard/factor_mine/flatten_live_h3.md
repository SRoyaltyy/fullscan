# Factor mine action — `flatten_live_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · 09:30 tickets only when flatten_robust gate fires (mover)

Cash book **-6.24%** ($9,376) · signal-only (no cash/fees) was +6.87%. Starts YES **0/21**. Fills 42 · skips 45 · realized $-623.92.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the flatten wish-list (names the flatten board wanted that morning) and only buy names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the flatten wish-list (names the flatten board wanted that morning).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Live flatten gate: new buys only when flatten_robust would actually send 09:30 tickets.

### When it buys

- At 09:30, take names on the flatten wish-list (names the flatten board wanted that morning) that pass the must-haves.
- If the live flatten gate is HOLD / io that morning, buy nobody new.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
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

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,376.07.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 60 | — | $20.55 | +0.00 | $21.19 | +38.40 | +38.40 | +0.00 | +38.40 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 60 | — | $20.65 | +0.00 | $21.11 | +27.60 | +27.60 | +0.00 | +27.60 |
| 2026-08-20 | `HDSN` | 216 | — | $5.77 | +0.00 | $5.57 | -43.20 | -43.20 | +0.00 | -43.20 |
| 2026-08-20 | `IAG` | 63 | — | $19.63 | +0.00 | $20.50 | +54.81 | +54.81 | +0.00 | +54.81 |
| 2026-08-20 | `KGC` | 42 | — | $29.63 | +0.00 | $31.43 | +75.60 | +75.60 | +0.00 | +75.60 |
| 2026-08-20 | `NFGC` | 714 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 60 | $21.19 | $21.90 | +42.60 | $21.09 | -48.60 | -6.00 | +81.00 | +32.40 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | $97.03 | +17.03 | +44.20 | +61.23 | +78.26 |
| 2026-08-21 | `CDE` | 60 | $21.11 | $21.75 | +38.40 | $20.97 | -46.80 | -8.40 | +66.00 | +19.20 |
| 2026-08-21 | `HDSN` | 216 | $5.57 | $5.67 | +21.60 | $5.63 | -8.64 | +12.96 | -21.60 | -30.24 |
| 2026-08-21 | `IAG` | 63 | $20.50 | $21.17 | +42.21 | $21.14 | -1.89 | +40.32 | +97.02 | +95.13 |
| 2026-08-21 | `KGC` | 42 | $31.43 | $32.17 | +31.08 | $32.76 | +24.78 | +55.86 | +106.68 | +131.46 |
| 2026-08-21 | `NFGC` | 714 | $1.75 | $1.79 | +28.56 | $1.84 | +35.70 | +64.26 | +28.56 | +64.26 |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | $157.78 | +24.64 | +60.24 | +81.28 | +105.92 |
| 2026-08-21 | `AUPH` | 1 | — | $17.20 | +0.00 | $16.65 | -0.55 | -0.55 | +0.00 | -0.55 |
| 2026-08-21 | `ARCT` | 2 | — | $11.13 | +0.00 | $13.45 | +4.64 | +4.64 | +0.00 | +4.64 |
| 2026-08-21 | `AUTL` | 9 | — | $2.47 | +0.00 | $2.41 | -0.54 | -0.54 | +0.00 | -0.54 |
| 2026-08-21 | `CRDL` | 12 | — | $1.93 | +0.00 | $1.86 | -0.84 | -0.84 | +0.00 | -0.84 |
| 2026-08-21 | `CYPH` | 17 | — | $1.32 | +0.00 | $1.42 | +1.70 | +1.70 | +0.00 | +1.70 |
| 2026-08-24 | `AG` | 60 | $21.09 | $21.30 | +12.60 | $20.83 | -28.20 | -15.60 | +45.00 | +16.80 |
| 2026-08-24 | `BHP` | 13 | $97.03 | $97.31 | +3.64 | $97.13 | -2.34 | +1.30 | +81.90 | +79.56 |
| 2026-08-24 | `CDE` | 60 | $20.97 | $21.26 | +17.40 | $20.88 | -22.80 | -5.40 | +36.60 | +13.80 |
| 2026-08-24 | `HDSN` | 216 | $5.63 | $5.69 | +12.96 | $5.52 | -36.72 | -23.76 | -17.28 | -54.00 |
| 2026-08-24 | `IAG` | 63 | $21.14 | $21.38 | +15.12 | $21.80 | +26.46 | +41.58 | +110.25 | +136.71 |
| 2026-08-24 | `KGC` | 42 | $32.76 | $33.03 | +11.34 | $32.98 | -2.10 | +9.24 | +142.80 | +140.70 |
| 2026-08-24 | `NFGC` | 714 | $1.84 | $1.86 | +14.28 | $1.90 | +28.56 | +42.84 | +78.54 | +107.10 |
| 2026-08-24 | `WPM` | 8 | $157.78 | $159.50 | +13.76 | $160.19 | +5.52 | +19.28 | +119.68 | +125.20 |
| 2026-08-24 | `AUPH` | 1 | $16.65 | $16.57 | -0.08 | $16.57 | +0.00 | -0.08 | -0.63 | -0.63 |
| 2026-08-24 | `ARCT` | 2 | $13.45 | $13.33 | -0.24 | $14.34 | +2.02 | +1.78 | +4.40 | +6.42 |
| 2026-08-24 | `AUTL` | 9 | $2.41 | $2.40 | -0.09 | $2.34 | -0.54 | -0.63 | -0.63 | -1.17 |
| 2026-08-24 | `CRDL` | 12 | $1.86 | $1.88 | +0.24 | $1.86 | -0.24 | +0.00 | -0.60 | -0.84 |
| 2026-08-24 | `CYPH` | 17 | $1.42 | $1.83 | +6.97 | $1.68 | -2.55 | +4.42 | +8.67 | +6.12 |
| 2026-08-25 | `AG` | 60 | $20.83 | $20.32 | -30.60 | — | +0.00 | -30.60 | -13.80 | — |
| 2026-08-25 | `BHP` | 13 | $97.13 | $95.86 | -16.51 | — | +0.00 | -16.51 | +63.05 | — |
| 2026-08-25 | `CDE` | 60 | $20.88 | $20.47 | -24.60 | — | +0.00 | -24.60 | -10.80 | — |
| 2026-08-25 | `HDSN` | 216 | $5.52 | $5.53 | +2.16 | — | +0.00 | +2.16 | -51.84 | — |
| 2026-08-25 | `IAG` | 63 | $21.80 | $21.21 | -37.17 | — | +0.00 | -37.17 | +99.54 | — |
| 2026-08-25 | `KGC` | 42 | $32.98 | $32.32 | -27.72 | — | +0.00 | -27.72 | +112.98 | — |
| 2026-08-25 | `NFGC` | 714 | $1.90 | $1.90 | +0.00 | — | +0.00 | +0.00 | +107.10 | — |
| 2026-08-25 | `WPM` | 8 | $160.19 | $156.51 | -29.44 | — | +0.00 | -29.44 | +95.76 | — |
| 2026-08-25 | `AUPH` | 1 | $16.57 | $16.63 | +0.06 | $16.75 | +0.12 | +0.18 | -0.57 | -0.45 |
| 2026-08-25 | `ARCT` | 2 | $14.34 | $14.12 | -0.44 | $15.44 | +2.64 | +2.20 | +5.98 | +8.62 |
| 2026-08-25 | `AUTL` | 9 | $2.34 | $2.38 | +0.36 | $2.44 | +0.54 | +0.90 | -0.81 | -0.27 |
| 2026-08-25 | `CRDL` | 12 | $1.86 | $1.89 | +0.36 | $2.00 | +1.32 | +1.68 | -0.48 | +0.84 |
| 2026-08-25 | `CYPH` | 17 | $1.68 | $1.56 | -2.04 | $1.64 | +1.36 | -0.68 | +4.08 | +5.44 |
| 2026-08-26 | `AUPH` | 1 | $16.75 | $16.60 | -0.15 | — | +0.00 | -0.15 | -0.60 | — |
| 2026-08-26 | `ARCT` | 2 | $15.44 | $15.35 | -0.18 | — | +0.00 | -0.18 | +8.44 | — |
| 2026-08-26 | `AUTL` | 9 | $2.44 | $2.41 | -0.27 | — | +0.00 | -0.27 | -0.54 | — |
| 2026-08-26 | `CRDL` | 12 | $2.00 | $2.03 | +0.36 | — | +0.00 | +0.36 | +1.20 | — |
| 2026-08-26 | `CYPH` | 17 | $1.64 | $1.60 | -0.68 | — | +0.00 | -0.68 | +4.76 | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-31 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | `CABA` | 374 | — | $3.46 | +0.00 | $3.47 | +3.74 | +3.74 | +0.00 | +3.74 |
| 2026-09-04 | `ALEC` | 514 | — | $2.52 | +0.00 | $2.46 | -30.84 | -30.84 | +0.00 | -30.84 |
| 2026-09-04 | `BHC` | 193 | — | $6.71 | +0.00 | $6.56 | -28.95 | -28.95 | +0.00 | -28.95 |
| 2026-09-04 | `BMEA` | 681 | — | $1.90 | +0.00 | $2.03 | +88.53 | +88.53 | +0.00 | +88.53 |
| 2026-09-04 | `OABI` | 270 | — | $4.78 | +0.00 | $4.33 | -121.50 | -121.50 | +0.00 | -121.50 |
| 2026-09-04 | `OPK` | 814 | — | $1.59 | +0.00 | $1.64 | +40.70 | +40.70 | +0.00 | +40.70 |
| 2026-09-04 | `VIR` | 114 | — | $11.31 | +0.00 | $11.38 | +8.55 | +8.55 | +0.00 | +8.55 |
| 2026-09-04 | `ATRC` | 24 | — | $52.03 | +0.00 | $51.52 | -12.24 | -12.24 | +0.00 | -12.24 |
| 2026-09-08 | `CABA` | 374 | $3.47 | $3.43 | -14.96 | $3.27 | -59.84 | -74.80 | -11.22 | -71.06 |
| 2026-09-08 | `ALEC` | 514 | $2.46 | $2.38 | -41.12 | $2.47 | +46.26 | +5.14 | -71.96 | -25.70 |
| 2026-09-08 | `BHC` | 193 | $6.56 | $6.57 | +1.93 | $6.43 | -27.02 | -25.09 | -27.02 | -54.04 |
| 2026-09-08 | `BMEA` | 681 | $2.03 | $2.00 | -20.43 | $1.93 | -47.67 | -68.10 | +68.10 | +20.43 |
| 2026-09-08 | `OABI` | 270 | $4.33 | $4.30 | -8.10 | $4.24 | -16.20 | -24.30 | -129.60 | -145.80 |
| 2026-09-08 | `OPK` | 814 | $1.64 | $1.63 | -8.14 | $1.59 | -32.56 | -40.70 | +32.56 | +0.00 |
| 2026-09-08 | `VIR` | 114 | $11.38 | $11.22 | -18.81 | $11.18 | -4.56 | -23.37 | -10.26 | -14.82 |
| 2026-09-08 | `ATRC` | 24 | $51.52 | $54.31 | +66.96 | $53.73 | -13.92 | +53.04 | +54.72 | +40.80 |
| 2026-09-09 | `CABA` | 374 | $3.27 | $3.28 | +3.74 | $2.91 | -138.38 | -134.64 | -67.32 | -205.70 |
| 2026-09-09 | `ALEC` | 514 | $2.47 | $2.47 | +0.00 | $2.27 | -102.80 | -102.80 | -25.70 | -128.50 |
| 2026-09-09 | `BHC` | 193 | $6.43 | $6.38 | -9.65 | $6.16 | -42.46 | -52.11 | -63.69 | -106.15 |
| 2026-09-09 | `BMEA` | 681 | $1.93 | $1.94 | +6.81 | $1.84 | -64.69 | -57.88 | +27.24 | -37.45 |
| 2026-09-09 | `OABI` | 270 | $4.24 | $4.21 | -8.10 | $4.01 | -52.65 | -60.75 | -153.90 | -206.55 |
| 2026-09-09 | `OPK` | 814 | $1.59 | $1.58 | -8.14 | $1.54 | -32.56 | -40.70 | -8.14 | -40.70 |
| 2026-09-09 | `VIR` | 114 | $11.18 | $11.04 | -15.96 | $10.81 | -26.22 | -42.18 | -30.78 | -57.00 |
| 2026-09-09 | `ATRC` | 24 | $53.73 | $53.16 | -13.68 | $53.03 | -3.12 | -16.80 | +27.12 | +24.00 |
| 2026-09-10 | `CABA` | 374 | $2.91 | $2.85 | -22.44 | — | +0.00 | -22.44 | -228.14 | — |
| 2026-09-10 | `ALEC` | 514 | $2.27 | $2.22 | -25.70 | — | +0.00 | -25.70 | -154.20 | — |
| 2026-09-10 | `BHC` | 193 | $6.16 | $6.11 | -9.65 | — | +0.00 | -9.65 | -115.80 | — |
| 2026-09-10 | `BMEA` | 681 | $1.84 | $1.83 | -10.21 | — | +0.00 | -10.21 | -47.67 | — |
| 2026-09-10 | `OABI` | 270 | $4.01 | $3.92 | -25.11 | — | +0.00 | -25.11 | -231.66 | — |
| 2026-09-10 | `OPK` | 814 | $1.54 | $1.53 | -8.14 | — | +0.00 | -8.14 | -48.84 | — |
| 2026-09-10 | `VIR` | 114 | $10.81 | $10.57 | -27.36 | — | +0.00 | -27.36 | -84.36 | — |
| 2026-09-10 | `ATRC` | 24 | $53.03 | $52.31 | -17.28 | — | +0.00 | -17.28 | +6.72 | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +232.95 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $186.91 | $10,208.28 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8 |
| 2026-08-21 | +3.25 | $186.91 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8 | $10,475.50 | +267.22 | +0.63 | AUPH, ARCT, AUTL, CRDL, CYPH | — | $78.42 | $10,474.93 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 |
| 2026-08-24 | -5.17 | $78.42 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | $10,582.83 | +107.90 | -32.93 | — | — | $78.42 | $10,549.90 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 |
| 2026-08-25 | +1.80 | $78.42 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | $10,384.32 | -165.58 | +5.98 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $10,243.86 | $10,365.33 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 |
| 2026-08-26 | +2.02 | $10,243.86 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | $10,364.41 | -0.92 | +0.00 | — | AUPH, ARCT, AUTL, CRDL, CYPH | $10,362.98 | $10,362.98 | — |
| 2026-08-27 | — | $10,362.98 | — | $10,362.98 | -0.00 | +0.00 | — | — | $10,362.98 | $10,362.98 | — |
| 2026-08-28 | +0.75 | $10,362.98 | — | $10,362.98 | -0.00 | +0.00 | — | — | $10,362.98 | $10,362.98 | — |
| 2026-08-31 | -5.85 | $10,362.98 | — | $10,362.98 | -0.00 | +0.00 | — | — | $10,362.98 | $10,362.98 | — |
| 2026-09-01 | -6.30 | $10,362.98 | — | $10,362.98 | -0.00 | +0.00 | — | — | $10,362.98 | $10,362.98 | — |
| 2026-09-02 | -3.83 | $10,362.98 | — | $10,362.98 | -0.00 | +0.00 | — | — | $10,362.98 | $10,362.98 | — |
| 2026-09-03 | -0.90 | $10,362.98 | — | $10,362.98 | -0.00 | +0.00 | — | — | $10,362.98 | $10,362.98 | — |
| 2026-09-04 | +2.25 | $10,362.98 | — | $10,362.98 | -0.00 | -52.01 | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC | — | $20.62 | $10,269.78 | CABA×374, ALEC×514, BHC×193, BMEA×681, OABI×270, OPK×814, VIR×114, ATRC×24 |
| 2026-09-08 | -11.47 | $20.62 | CABA×374, ALEC×514, BHC×193, BMEA×681, OABI×270, OPK×814, VIR×114, ATRC×24 | $10,227.11 | -42.67 | -155.51 | — | — | $20.62 | $10,071.60 | CABA×374, ALEC×514, BHC×193, BMEA×681, OABI×270, OPK×814, VIR×114, ATRC×24 |
| 2026-09-09 | -13.95 | $20.62 | CABA×374, ALEC×514, BHC×193, BMEA×681, OABI×270, OPK×814, VIR×114, ATRC×24 | $10,026.62 | -44.98 | -462.88 | — | — | $20.62 | $9,563.74 | CABA×374, ALEC×514, BHC×193, BMEA×681, OABI×270, OPK×814, VIR×114, ATRC×24 |
| 2026-09-10 | -13.28 | $20.62 | CABA×374, ALEC×514, BHC×193, BMEA×681, OABI×270, OPK×814, VIR×114, ATRC×24 | $9,417.84 | -145.90 | +0.00 | — | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC | $9,376.07 | $9,376.07 | — |
| 2026-09-11 | +0.50 | $9,376.07 | — | $9,376.07 | +0.00 | +0.00 | — | — | $9,376.07 | $9,376.07 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,764.83 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,579.67 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,338.50 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 216 | $5.77 | $2.79 | — | $5,089.39 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,850.53 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,603.95 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 714 | $1.75 | $9.21 | — | $1,345.24 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $186.91 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $186.91 | ▲ close $10,208.28 vs 09:30 $10,000.00 (session +232.95) | 16:00 close · cash $186.91 · equity $10,208.28 vs 09:30 $10,000.00 (+208.28; session marks +232.95) · 8 name(s) marked open→close (per-name table). AG×60 09:30 $20.55 → close $21.19 +38.40; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×60 09:30 $20.65 → close $21.11 +27.60; HDSN×216 09:30 $5.77 → close $5.57 -43.20; IAG×63 09:30 $19.63 → close $20.50 +54.81; KGC×42 09:30 $29.63 → close $31.43 +75.60; NFGC×714 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.91 | ▲ 09:30 equity $10,475.50 vs yday $10,208.28 (+267.22) | 09:30 open · cash $186.91 (unchanged overnight, no fees) · equity $10,475.50 vs prior close $10,208.28 (+267.22) · 8 name(s) re-marked at the open (per-name table). AG×60 yday $21.19 → 09:30 $21.90 +42.60; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×60 yday $21.11 → 09:30 $21.75 +38.40; HDSN×216 yday $5.57 → 09:30 $5.67 +21.60; IAG×63 yday $20.50 → 09:30 $21.17 +42.21; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×714 yday $1.75 → 09:30 $1.79 +28.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $169.53 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $147.04 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $124.56 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 12 | $1.93 | $0.27 | — | $101.13 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 17 | $1.32 | $0.28 | — | $78.42 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.42 | ▲ close $10,474.93 vs 09:30 $10,475.50 (session +0.63) | 16:00 close · cash $78.42 · equity $10,474.93 vs 09:30 $10,475.50 (-0.57; session marks +0.63) · 13 name(s) marked open→close (per-name table). AG×60 09:30 $21.90 → close $21.09 -48.60; BHP×13 09:30 $95.72 → close $97.03 +17.03; CDE×60 09:30 $21.75 → close $20.97 -46.80; HDSN×216 09:30 $5.67 → close $5.63 -8.64; IAG×63 09:30 $21.17 → close $21.14 -1.89; KGC×42 09:30 $32.17 → close $32.76 +24.78; NFGC×714 09:30 $1.79 → close $1.84 +35.70; WPM×8 09:30 $154.70 → close $157.78 +24.64; AUPH×1 09:30 $17.20 → close $16.65 -0.55; ARCT×2 09:30 $11.13 → close $13.45 +4.64; AUTL×9 09:30 $2.47 → close $2.41 -0.54; CRDL×12 09:30 $1.93 → close $1.86 -0.84; CYPH×17 09:30 $1.32 → close $1.42 +1.70 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.42 | ▲ 09:30 equity $10,582.83 vs yday $10,474.93 (+107.90) | 09:30 open · cash $78.42 (unchanged overnight, no fees) · equity $10,582.83 vs prior close $10,474.93 (+107.90) · 13 name(s) re-marked at the open (per-name table). AG×60 yday $21.09 → 09:30 $21.30 +12.60; BHP×13 yday $97.03 → 09:30 $97.31 +3.64; CDE×60 yday $20.97 → 09:30 $21.26 +17.40; HDSN×216 yday $5.63 → 09:30 $5.69 +12.96; IAG×63 yday $21.14 → 09:30 $21.38 +15.12; KGC×42 yday $32.76 → 09:30 $33.03 +11.34; NFGC×714 yday $1.84 → 09:30 $1.86 +14.28; WPM×8 yday $157.78 → 09:30 $159.50 +13.76; AUPH×1 yday $16.65 → 09:30 $16.57 -0.08; ARCT×2 yday $13.45 → 09:30 $13.33 -0.24; AUTL×9 yday $2.41 → 09:30 $2.40 -0.09; CRDL×12 yday $1.86 → 09:30 $1.88 +0.24; CYPH×17 yday $1.42 → 09:30 $1.83 +6.97 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.42 | ▼ close $10,549.90 vs 09:30 $10,582.83 (session -32.93) | 16:00 close · cash $78.42 · equity $10,549.90 vs 09:30 $10,582.83 (-32.93; session marks -32.93) · 13 name(s) marked open→close (per-name table). AG×60 09:30 $21.30 → close $20.83 -28.20; BHP×13 09:30 $97.31 → close $97.13 -2.34; CDE×60 09:30 $21.26 → close $20.88 -22.80; HDSN×216 09:30 $5.69 → close $5.52 -36.72; IAG×63 09:30 $21.38 → close $21.80 +26.46; KGC×42 09:30 $33.03 → close $32.98 -2.10; NFGC×714 09:30 $1.86 → close $1.90 +28.56; WPM×8 09:30 $159.50 → close $160.19 +5.52; AUPH×1 09:30 $16.57 → close $16.57 +0.00; ARCT×2 09:30 $13.33 → close $14.34 +2.02; AUTL×9 09:30 $2.40 → close $2.34 -0.54; CRDL×12 09:30 $1.88 → close $1.86 -0.24; CYPH×17 09:30 $1.83 → close $1.68 -2.55 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.42 | ▼ 09:30 equity $10,384.32 vs yday $10,549.90 (-165.58) | 09:30 open · cash $78.42 (unchanged overnight, no fees) · equity $10,384.32 vs prior close $10,549.90 (-165.58) · 13 name(s) re-marked at the open (per-name table). AG×60 yday $20.83 → 09:30 $20.32 -30.60; BHP×13 yday $97.13 → 09:30 $95.86 -16.51; CDE×60 yday $20.88 → 09:30 $20.47 -24.60; HDSN×216 yday $5.52 → 09:30 $5.53 +2.16; IAG×63 yday $21.80 → 09:30 $21.21 -37.17; KGC×42 yday $32.98 → 09:30 $32.32 -27.72; NFGC×714 yday $1.90 → 09:30 $1.90 +0.00; WPM×8 yday $160.19 → 09:30 $156.51 -29.44; AUPH×1 yday $16.57 → 09:30 $16.63 +0.06; ARCT×2 yday $14.34 → 09:30 $14.12 -0.44; AUTL×9 yday $2.34 → 09:30 $2.38 +0.36; CRDL×12 yday $1.86 → 09:30 $1.89 +0.36; CYPH×17 yday $1.68 → 09:30 $1.56 -2.04 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 60 | $20.32 | $2.19 | $-18.16 | $1,295.43 | ▼ -18.16 after sell → book $10,382.13; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $2,539.56 | ▲ +58.97 after sell → book $10,380.08; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 60 | $20.47 | $2.19 | $-15.16 | $3,765.57 | ▼ -15.16 after sell → book $10,377.89; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 216 | $5.53 | $2.83 | $-57.46 | $4,957.22 | ▼ -57.46 after sell → book $10,375.06; vs 09:30 mark -2.83 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 63 | $21.21 | $2.20 | $+95.16 | $6,291.25 | ▲ +95.16 after sell → book $10,372.86; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 42 | $32.32 | $2.14 | $+108.73 | $7,646.55 | ▲ +108.73 after sell → book $10,370.72; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 714 | $1.90 | $9.34 | $+88.55 | $8,993.81 | ▲ +88.55 after sell → book $10,361.38; vs 09:30 mark -9.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $10,243.86 | ▲ +91.71 after sell → book $10,359.35; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,243.86 | ▲ close $10,365.33 vs 09:30 $10,384.32 (session +5.98) | 16:00 close · cash $10,243.86 · equity $10,365.33 vs 09:30 $10,384.32 (-18.99; session marks +5.98) · 5 name(s) marked open→close (per-name table). AUPH×1 09:30 $16.63 → close $16.75 +0.12; ARCT×2 09:30 $14.12 → close $15.44 +2.64; AUTL×9 09:30 $2.38 → close $2.44 +0.54; CRDL×12 09:30 $1.89 → close $2.00 +1.32; CYPH×17 09:30 $1.56 → close $1.64 +1.36 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,243.86 | ▼ 09:30 equity $10,364.41 vs yday $10,365.33 (-0.92) | 09:30 open · cash $10,243.86 (unchanged overnight, no fees) · equity $10,364.41 vs prior close $10,365.33 (-0.92) · 5 name(s) re-marked at the open (per-name table). AUPH×1 yday $16.75 → 09:30 $16.60 -0.15; ARCT×2 yday $15.44 → 09:30 $15.35 -0.18; AUTL×9 yday $2.44 → 09:30 $2.41 -0.27; CRDL×12 yday $2.00 → 09:30 $2.03 +0.36; CYPH×17 yday $1.64 → 09:30 $1.60 -0.68 | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $10,260.27 | ▼ -0.96 after sell → book $10,364.22; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $10,290.63 | ▲ +7.88 after sell → book $10,363.88; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 9 | $2.41 | $0.26 | $-1.05 | $10,312.06 | ▼ -1.05 after sell → book $10,363.62; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 12 | $2.03 | $0.30 | $+0.63 | $10,336.12 | ▲ +0.63 after sell → book $10,363.32; vs 09:30 mark -0.30 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟡 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 17 | $1.60 | $0.34 | $+4.14 | $10,362.98 | ▲ +4.14 after sell → book $10,362.98; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,362.98 | ▲ close $10,362.98 vs 09:30 $10,364.41 (session +0.00) | 16:00 close · cash $10,362.98 · no lots left · equity $10,362.98. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,362.98 | ▲ 09:30 equity $10,362.98 vs yday $10,362.98 (-0.00) | 09:30 open · cash $10,362.98 · no holdings · equity $10,362.98 vs prior close $10,362.98 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,362.98 | ▲ close $10,362.98 vs 09:30 $10,362.98 (session +0.00) | 16:00 close · cash $10,362.98 · no lots left · equity $10,362.98. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,362.98 | ▲ 09:30 equity $10,362.98 vs yday $10,362.98 (-0.00) | 09:30 open · cash $10,362.98 · no holdings · equity $10,362.98 vs prior close $10,362.98 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,362.98 | ▲ close $10,362.98 vs 09:30 $10,362.98 (session +0.00) | 16:00 close · cash $10,362.98 · no lots left · equity $10,362.98. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,362.98 | ▲ 09:30 equity $10,362.98 vs yday $10,362.98 (-0.00) | 09:30 open · cash $10,362.98 · no holdings · equity $10,362.98 vs prior close $10,362.98 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,362.98 | ▲ close $10,362.98 vs 09:30 $10,362.98 (session +0.00) | 16:00 close · cash $10,362.98 · no lots left · equity $10,362.98. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,362.98 | ▲ 09:30 equity $10,362.98 vs yday $10,362.98 (-0.00) | 09:30 open · cash $10,362.98 · no holdings · equity $10,362.98 vs prior close $10,362.98 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,362.98 | ▲ close $10,362.98 vs 09:30 $10,362.98 (session +0.00) | 16:00 close · cash $10,362.98 · no lots left · equity $10,362.98. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,362.98 | ▲ 09:30 equity $10,362.98 vs yday $10,362.98 (-0.00) | 09:30 open · cash $10,362.98 · no holdings · equity $10,362.98 vs prior close $10,362.98 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,362.98 | ▲ close $10,362.98 vs 09:30 $10,362.98 (session +0.00) | 16:00 close · cash $10,362.98 · no lots left · equity $10,362.98. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,362.98 | ▲ 09:30 equity $10,362.98 vs yday $10,362.98 (-0.00) | 09:30 open · cash $10,362.98 · no holdings · equity $10,362.98 vs prior close $10,362.98 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,362.98 | ▲ close $10,362.98 vs 09:30 $10,362.98 (session +0.00) | 16:00 close · cash $10,362.98 · no lots left · equity $10,362.98. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,362.98 | ▲ 09:30 equity $10,362.98 vs yday $10,362.98 (-0.00) | 09:30 open · cash $10,362.98 · no holdings · equity $10,362.98 vs prior close $10,362.98 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 374 | $3.46 | $4.82 | — | $9,064.11 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $1295.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 514 | $2.52 | $6.63 | — | $7,762.20 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $1295.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 193 | $6.71 | $2.57 | — | $6,464.60 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $1295.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 681 | $1.90 | $8.78 | — | $5,161.92 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $1295.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 270 | $4.78 | $3.48 | — | $3,867.84 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $1295.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 814 | $1.59 | $10.50 | — | $2,563.08 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $1295.37 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 114 | $11.31 | $2.33 | — | $1,271.40 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $1295.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 24 | $52.03 | $2.06 | — | $20.62 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+6.5; leftover $1295.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.62 | ▼ close $10,269.78 vs 09:30 $10,362.98 (session -52.01) | 16:00 close · cash $20.62 · equity $10,269.78 vs 09:30 $10,362.98 (-93.20; session marks -52.01) · 8 name(s) marked open→close (per-name table). CABA×374 09:30 $3.46 → close $3.47 +3.74; ALEC×514 09:30 $2.52 → close $2.46 -30.84; BHC×193 09:30 $6.71 → close $6.56 -28.95; BMEA×681 09:30 $1.90 → close $2.03 +88.53; OABI×270 09:30 $4.78 → close $4.33 -121.50; OPK×814 09:30 $1.59 → close $1.64 +40.70; VIR×114 09:30 $11.31 → close $11.38 +8.55; ATRC×24 09:30 $52.03 → close $51.52 -12.24 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.62 | ▼ 09:30 equity $10,227.11 vs yday $10,269.78 (-42.67) | 09:30 open · cash $20.62 (unchanged overnight, no fees) · equity $10,227.11 vs prior close $10,269.78 (-42.67) · 8 name(s) re-marked at the open (per-name table). CABA×374 yday $3.47 → 09:30 $3.43 -14.96; ALEC×514 yday $2.46 → 09:30 $2.38 -41.12; BHC×193 yday $6.56 → 09:30 $6.57 +1.93; BMEA×681 yday $2.03 → 09:30 $2.00 -20.43; OABI×270 yday $4.33 → 09:30 $4.30 -8.10; OPK×814 yday $1.64 → 09:30 $1.63 -8.14; VIR×114 yday $11.38 → 09:30 $11.22 -18.81; ATRC×24 yday $51.52 → 09:30 $54.31 +66.96 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.62 | ▼ close $10,071.60 vs 09:30 $10,227.11 (session -155.51) | 16:00 close · cash $20.62 · equity $10,071.60 vs 09:30 $10,227.11 (-155.51; session marks -155.51) · 8 name(s) marked open→close (per-name table). CABA×374 09:30 $3.43 → close $3.27 -59.84; ALEC×514 09:30 $2.38 → close $2.47 +46.26; BHC×193 09:30 $6.57 → close $6.43 -27.02; BMEA×681 09:30 $2.00 → close $1.93 -47.67; OABI×270 09:30 $4.30 → close $4.24 -16.20; OPK×814 09:30 $1.63 → close $1.59 -32.56; VIR×114 09:30 $11.22 → close $11.18 -4.56; ATRC×24 09:30 $54.31 → close $53.73 -13.92 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.62 | ▼ 09:30 equity $10,026.62 vs yday $10,071.60 (-44.98) | 09:30 open · cash $20.62 (unchanged overnight, no fees) · equity $10,026.62 vs prior close $10,071.60 (-44.98) · 8 name(s) re-marked at the open (per-name table). CABA×374 yday $3.27 → 09:30 $3.28 +3.74; ALEC×514 yday $2.47 → 09:30 $2.47 +0.00; BHC×193 yday $6.43 → 09:30 $6.38 -9.65; BMEA×681 yday $1.93 → 09:30 $1.94 +6.81; OABI×270 yday $4.24 → 09:30 $4.21 -8.10; OPK×814 yday $1.59 → 09:30 $1.58 -8.14; VIR×114 yday $11.18 → 09:30 $11.04 -15.96; ATRC×24 yday $53.73 → 09:30 $53.16 -13.68 | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.62 | ▼ close $9,563.74 vs 09:30 $10,026.62 (session -462.88) | 16:00 close · cash $20.62 · equity $9,563.74 vs 09:30 $10,026.62 (-462.88; session marks -462.88) · 8 name(s) marked open→close (per-name table). CABA×374 09:30 $3.28 → close $2.91 -138.38; ALEC×514 09:30 $2.47 → close $2.27 -102.80; BHC×193 09:30 $6.38 → close $6.16 -42.46; BMEA×681 09:30 $1.94 → close $1.84 -64.69; OABI×270 09:30 $4.21 → close $4.01 -52.65; OPK×814 09:30 $1.58 → close $1.54 -32.56; VIR×114 09:30 $11.04 → close $10.81 -26.22; ATRC×24 09:30 $53.16 → close $53.03 -3.12 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.62 | ▼ 09:30 equity $9,417.84 vs yday $9,563.74 (-145.90) | 09:30 open · cash $20.62 (unchanged overnight, no fees) · equity $9,417.84 vs prior close $9,563.74 (-145.90) · 8 name(s) re-marked at the open (per-name table). CABA×374 yday $2.91 → 09:30 $2.85 -22.44; ALEC×514 yday $2.27 → 09:30 $2.22 -25.70; BHC×193 yday $6.16 → 09:30 $6.11 -9.65; BMEA×681 yday $1.84 → 09:30 $1.83 -10.21; OABI×270 yday $4.01 → 09:30 $3.92 -25.11; OPK×814 yday $1.54 → 09:30 $1.53 -8.14; VIR×114 yday $10.81 → 09:30 $10.57 -27.36; ATRC×24 yday $53.03 → 09:30 $52.31 -17.28 | — |
| 2026-09-10 09:30 ET | **SELL** | `CABA` | 374 | $2.85 | $4.90 | $-237.86 | $1,081.62 | ▼ -237.86 after sell → book $9,412.94; vs 09:30 mark -4.90 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 514 | $2.22 | $6.73 | $-167.56 | $2,215.98 | ▼ -167.56 after sell → book $9,406.22; vs 09:30 mark -6.72 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 193 | $6.11 | $2.61 | $-120.98 | $3,392.60 | ▼ -120.98 after sell → book $9,403.61; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 681 | $1.83 | $8.91 | $-65.36 | $4,629.92 | ▼ -65.36 after sell → book $9,394.70; vs 09:30 mark -8.91 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 270 | $3.92 | $3.54 | $-238.68 | $5,685.32 | ▼ -238.68 after sell → book $9,391.16; vs 09:30 mark -3.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 814 | $1.53 | $10.65 | $-69.99 | $6,920.10 | ▼ -69.99 after sell → book $9,380.52; vs 09:30 mark -10.64 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 114 | $10.57 | $2.36 | $-89.05 | $8,122.72 | ▼ -89.05 after sell → book $9,378.16; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ATRC` | 24 | $52.31 | $2.08 | $+2.58 | $9,376.07 | ▲ +2.58 after sell → book $9,376.07; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,376.07 | ▲ close $9,376.07 vs 09:30 $9,417.84 (session +0.00) | 16:00 close · cash $9,376.07 · no lots left · equity $9,376.07. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,376.07 | ▲ 09:30 equity $9,376.07 vs yday $9,376.07 (+0.00) | 09:30 open · cash $9,376.07 · no holdings · equity $9,376.07 vs prior close $9,376.07 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,376.07 | ▲ close $9,376.07 vs 09:30 $9,376.07 (session +0.00) | 16:00 close · cash $9,376.07 · no lots left · equity $9,376.07. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 23.36 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 23.36 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 23.36 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
