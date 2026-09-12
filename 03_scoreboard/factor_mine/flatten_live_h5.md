# Factor mine action — `flatten_live_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · 09:30 tickets only when flatten_robust gate fires (mover)

Cash book **-5.59%** ($9,441) · signal-only (no cash/fees) was +5.13%. Starts YES **0/21**. Fills 34 · skips 87 · realized $+459.00.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the flatten wish-list (names the flatten board wanted that morning) and only buy names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $37.48.

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
| 2026-08-25 | `AG` | 60 | $20.83 | $20.32 | -30.60 | $21.24 | +55.20 | +24.60 | -13.80 | +41.40 |
| 2026-08-25 | `BHP` | 13 | $97.13 | $95.86 | -16.51 | $98.69 | +36.79 | +20.28 | +63.05 | +99.84 |
| 2026-08-25 | `CDE` | 60 | $20.88 | $20.47 | -24.60 | $21.59 | +67.20 | +42.60 | -10.80 | +56.40 |
| 2026-08-25 | `HDSN` | 216 | $5.52 | $5.53 | +2.16 | $5.47 | -12.96 | -10.80 | -51.84 | -64.80 |
| 2026-08-25 | `IAG` | 63 | $21.80 | $21.21 | -37.17 | $22.17 | +60.48 | +23.31 | +99.54 | +160.02 |
| 2026-08-25 | `KGC` | 42 | $32.98 | $32.32 | -27.72 | $33.48 | +48.72 | +21.00 | +112.98 | +161.70 |
| 2026-08-25 | `NFGC` | 714 | $1.90 | $1.90 | +0.00 | $2.04 | +99.96 | +99.96 | +107.10 | +207.06 |
| 2026-08-25 | `WPM` | 8 | $160.19 | $156.51 | -29.44 | $163.72 | +57.68 | +28.24 | +95.76 | +153.44 |
| 2026-08-25 | `AUPH` | 1 | $16.57 | $16.63 | +0.06 | $16.75 | +0.12 | +0.18 | -0.57 | -0.45 |
| 2026-08-25 | `ARCT` | 2 | $14.34 | $14.12 | -0.44 | $15.44 | +2.64 | +2.20 | +5.98 | +8.62 |
| 2026-08-25 | `AUTL` | 9 | $2.34 | $2.38 | +0.36 | $2.44 | +0.54 | +0.90 | -0.81 | -0.27 |
| 2026-08-25 | `CRDL` | 12 | $1.86 | $1.89 | +0.36 | $2.00 | +1.32 | +1.68 | -0.48 | +0.84 |
| 2026-08-25 | `CYPH` | 17 | $1.68 | $1.56 | -2.04 | $1.64 | +1.36 | -0.68 | +4.08 | +5.44 |
| 2026-08-26 | `AG` | 60 | $21.24 | $20.63 | -36.60 | $20.99 | +21.60 | -15.00 | +4.80 | +26.40 |
| 2026-08-26 | `BHP` | 13 | $98.69 | $96.99 | -22.10 | $96.33 | -8.58 | -30.68 | +77.74 | +69.16 |
| 2026-08-26 | `CDE` | 60 | $21.59 | $21.00 | -35.40 | $21.44 | +26.40 | -9.00 | +21.00 | +47.40 |
| 2026-08-26 | `HDSN` | 216 | $5.47 | $5.51 | +8.64 | $5.29 | -47.52 | -38.88 | -56.16 | -103.68 |
| 2026-08-26 | `IAG` | 63 | $22.17 | $21.64 | -33.39 | $21.54 | -6.30 | -39.69 | +126.63 | +120.33 |
| 2026-08-26 | `KGC` | 42 | $33.48 | $32.90 | -24.36 | $32.32 | -24.36 | -48.72 | +137.34 | +112.98 |
| 2026-08-26 | `NFGC` | 714 | $2.04 | $2.00 | -28.56 | $1.90 | -71.40 | -99.96 | +178.50 | +107.10 |
| 2026-08-26 | `WPM` | 8 | $163.72 | $160.93 | -22.32 | $156.02 | -39.28 | -61.60 | +131.12 | +91.84 |
| 2026-08-26 | `AUPH` | 1 | $16.75 | $16.60 | -0.15 | $16.54 | -0.06 | -0.21 | -0.60 | -0.66 |
| 2026-08-26 | `ARCT` | 2 | $15.44 | $15.35 | -0.18 | $15.83 | +0.96 | +0.78 | +8.44 | +9.40 |
| 2026-08-26 | `AUTL` | 9 | $2.44 | $2.41 | -0.27 | $2.33 | -0.72 | -0.99 | -0.54 | -1.26 |
| 2026-08-26 | `CRDL` | 12 | $2.00 | $2.03 | +0.36 | $2.14 | +1.32 | +1.68 | +1.20 | +2.52 |
| 2026-08-26 | `CYPH` | 17 | $1.64 | $1.60 | -0.68 | $1.63 | +0.51 | -0.17 | +4.76 | +5.27 |
| 2026-08-27 | `AG` | 60 | $20.99 | $20.93 | -3.60 | — | +0.00 | -3.60 | +22.80 | — |
| 2026-08-27 | `BHP` | 13 | $96.33 | $95.52 | -10.53 | — | +0.00 | -10.53 | +58.63 | — |
| 2026-08-27 | `CDE` | 60 | $21.44 | $21.31 | -7.80 | — | +0.00 | -7.80 | +39.60 | — |
| 2026-08-27 | `HDSN` | 216 | $5.29 | $5.49 | +43.20 | — | +0.00 | +43.20 | -60.48 | — |
| 2026-08-27 | `IAG` | 63 | $21.54 | $21.47 | -4.41 | — | +0.00 | -4.41 | +115.92 | — |
| 2026-08-27 | `KGC` | 42 | $32.32 | $32.32 | +0.00 | — | +0.00 | +0.00 | +112.98 | — |
| 2026-08-27 | `NFGC` | 714 | $1.90 | $1.91 | +7.14 | — | +0.00 | +7.14 | +114.24 | — |
| 2026-08-27 | `WPM` | 8 | $156.02 | $155.89 | -1.04 | — | +0.00 | -1.04 | +90.80 | — |
| 2026-08-27 | `AUPH` | 1 | $16.54 | $16.47 | -0.07 | $16.48 | +0.01 | -0.06 | -0.73 | -0.72 |
| 2026-08-27 | `ARCT` | 2 | $15.83 | $15.74 | -0.18 | $16.17 | +0.86 | +0.68 | +9.22 | +10.08 |
| 2026-08-27 | `AUTL` | 9 | $2.33 | $2.32 | -0.09 | $2.36 | +0.36 | +0.27 | -1.35 | -0.99 |
| 2026-08-27 | `CRDL` | 12 | $2.14 | $2.09 | -0.60 | $2.06 | -0.36 | -0.96 | +1.92 | +1.56 |
| 2026-08-27 | `CYPH` | 17 | $1.63 | $1.75 | +2.04 | $1.89 | +2.38 | +4.42 | +7.31 | +9.69 |
| 2026-08-28 | `AUPH` | 1 | $16.48 | $16.44 | -0.04 | — | +0.00 | -0.04 | -0.76 | — |
| 2026-08-28 | `ARCT` | 2 | $16.17 | $15.43 | -1.48 | — | +0.00 | -1.48 | +8.60 | — |
| 2026-08-28 | `AUTL` | 9 | $2.36 | $2.35 | -0.09 | — | +0.00 | -0.09 | -1.08 | — |
| 2026-08-28 | `CRDL` | 12 | $2.06 | $2.06 | +0.00 | — | +0.00 | +0.00 | +1.56 | — |
| 2026-08-28 | `CYPH` | 17 | $1.89 | $1.82 | -1.19 | — | +0.00 | -1.19 | +8.50 | — |
| 2026-08-31 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | `CABA` | 377 | — | $3.46 | +0.00 | $3.47 | +3.77 | +3.77 | +0.00 | +3.77 |
| 2026-09-04 | `ALEC` | 518 | — | $2.52 | +0.00 | $2.46 | -31.08 | -31.08 | +0.00 | -31.08 |
| 2026-09-04 | `BHC` | 194 | — | $6.71 | +0.00 | $6.56 | -29.10 | -29.10 | +0.00 | -29.10 |
| 2026-09-04 | `BMEA` | 688 | — | $1.90 | +0.00 | $2.03 | +89.44 | +89.44 | +0.00 | +89.44 |
| 2026-09-04 | `OABI` | 273 | — | $4.78 | +0.00 | $4.33 | -122.85 | -122.85 | +0.00 | -122.85 |
| 2026-09-04 | `OPK` | 822 | — | $1.59 | +0.00 | $1.64 | +41.10 | +41.10 | +0.00 | +41.10 |
| 2026-09-04 | `VIR` | 115 | — | $11.31 | +0.00 | $11.38 | +8.62 | +8.62 | +0.00 | +8.62 |
| 2026-09-04 | `ATRC` | 24 | — | $52.03 | +0.00 | $51.52 | -12.24 | -12.24 | +0.00 | -12.24 |
| 2026-09-08 | `CABA` | 377 | $3.47 | $3.43 | -15.08 | $3.27 | -60.32 | -75.40 | -11.31 | -71.63 |
| 2026-09-08 | `ALEC` | 518 | $2.46 | $2.38 | -41.44 | $2.47 | +46.62 | +5.18 | -72.52 | -25.90 |
| 2026-09-08 | `BHC` | 194 | $6.56 | $6.57 | +1.94 | $6.43 | -27.16 | -25.22 | -27.16 | -54.32 |
| 2026-09-08 | `BMEA` | 688 | $2.03 | $2.00 | -20.64 | $1.93 | -48.16 | -68.80 | +68.80 | +20.64 |
| 2026-09-08 | `OABI` | 273 | $4.33 | $4.30 | -8.19 | $4.24 | -16.38 | -24.57 | -131.04 | -147.42 |
| 2026-09-08 | `OPK` | 822 | $1.64 | $1.63 | -8.22 | $1.59 | -32.88 | -41.10 | +32.88 | +0.00 |
| 2026-09-08 | `VIR` | 115 | $11.38 | $11.22 | -18.97 | $11.18 | -4.60 | -23.57 | -10.35 | -14.95 |
| 2026-09-08 | `ATRC` | 24 | $51.52 | $54.31 | +66.96 | $53.73 | -13.92 | +53.04 | +54.72 | +40.80 |
| 2026-09-09 | `CABA` | 377 | $3.27 | $3.28 | +3.77 | $2.91 | -139.49 | -135.72 | -67.86 | -207.35 |
| 2026-09-09 | `ALEC` | 518 | $2.47 | $2.47 | +0.00 | $2.27 | -103.60 | -103.60 | -25.90 | -129.50 |
| 2026-09-09 | `BHC` | 194 | $6.43 | $6.38 | -9.70 | $6.16 | -42.68 | -52.38 | -64.02 | -106.70 |
| 2026-09-09 | `BMEA` | 688 | $1.93 | $1.94 | +6.88 | $1.84 | -65.36 | -58.48 | +27.52 | -37.84 |
| 2026-09-09 | `OABI` | 273 | $4.24 | $4.21 | -8.19 | $4.01 | -53.24 | -61.43 | -155.61 | -208.85 |
| 2026-09-09 | `OPK` | 822 | $1.59 | $1.58 | -8.22 | $1.54 | -32.88 | -41.10 | -8.22 | -41.10 |
| 2026-09-09 | `VIR` | 115 | $11.18 | $11.04 | -16.10 | $10.81 | -26.45 | -42.55 | -31.05 | -57.50 |
| 2026-09-09 | `ATRC` | 24 | $53.73 | $53.16 | -13.68 | $53.03 | -3.12 | -16.80 | +27.12 | +24.00 |
| 2026-09-10 | `CABA` | 377 | $2.91 | $2.85 | -22.62 | $2.74 | -41.47 | -64.09 | -229.97 | -271.44 |
| 2026-09-10 | `ALEC` | 518 | $2.27 | $2.22 | -25.90 | $2.14 | -41.44 | -67.34 | -155.40 | -196.84 |
| 2026-09-10 | `BHC` | 194 | $6.16 | $6.11 | -9.70 | $6.07 | -7.76 | -17.46 | -116.40 | -124.16 |
| 2026-09-10 | `BMEA` | 688 | $1.84 | $1.83 | -10.32 | $1.70 | -89.44 | -99.76 | -48.16 | -137.60 |
| 2026-09-10 | `OABI` | 273 | $4.01 | $3.92 | -25.39 | $3.95 | +7.64 | -17.75 | -234.23 | -226.59 |
| 2026-09-10 | `OPK` | 822 | $1.54 | $1.53 | -8.22 | $1.49 | -32.88 | -41.10 | -49.32 | -82.20 |
| 2026-09-10 | `VIR` | 115 | $10.81 | $10.57 | -27.60 | $10.58 | +1.15 | -26.45 | -85.10 | -83.95 |
| 2026-09-10 | `ATRC` | 24 | $53.03 | $52.31 | -17.28 | $52.96 | +15.60 | -1.68 | +6.72 | +22.32 |
| 2026-09-11 | `CABA` | 377 | $2.74 | $2.77 | +11.31 | $2.73 | -15.08 | -3.77 | -260.13 | -275.21 |
| 2026-09-11 | `ALEC` | 518 | $2.14 | $2.17 | +15.54 | $2.14 | -15.54 | +0.00 | -181.30 | -196.84 |
| 2026-09-11 | `BHC` | 194 | $6.07 | $6.12 | +9.70 | $5.86 | -50.44 | -40.74 | -114.46 | -164.90 |
| 2026-09-11 | `BMEA` | 688 | $1.70 | $1.75 | +34.40 | $1.74 | -6.88 | +27.52 | -103.20 | -110.08 |
| 2026-09-11 | `OABI` | 273 | $3.95 | $3.97 | +5.46 | $4.07 | +27.30 | +32.76 | -221.13 | -193.83 |
| 2026-09-11 | `OPK` | 822 | $1.49 | $1.49 | +0.00 | $1.57 | +65.76 | +65.76 | -82.20 | -16.44 |
| 2026-09-11 | `VIR` | 115 | $10.58 | $10.79 | +24.15 | $10.62 | -19.55 | +4.60 | -59.80 | -79.35 |
| 2026-09-11 | `ATRC` | 24 | $52.96 | $53.53 | +13.68 | $54.54 | +24.24 | +37.92 | +36.00 | +60.24 |

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
| 2026-08-25 | +1.80 | $78.42 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | $10,384.32 | -165.58 | +419.05 | — | — | $78.42 | $10,803.37 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 |
| 2026-08-26 | +2.02 | $78.42 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | $10,608.36 | -195.01 | -147.43 | — | — | $78.42 | $10,460.93 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 |
| 2026-08-27 | — | $78.42 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | $10,484.99 | +24.06 | +3.25 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $10,336.36 | $10,463.26 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 |
| 2026-08-28 | +0.75 | $10,336.36 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | $10,460.47 | -2.79 | +0.00 | — | AUPH, ARCT, AUTL, CRDL, CYPH | $10,459.00 | $10,459.00 | — |
| 2026-08-31 | -5.85 | $10,459.00 | — | $10,459.00 | +0.00 | +0.00 | — | — | $10,459.00 | $10,459.00 | — |
| 2026-09-01 | -6.30 | $10,459.00 | — | $10,459.00 | +0.00 | +0.00 | — | — | $10,459.00 | $10,459.00 | — |
| 2026-09-02 | -3.83 | $10,459.00 | — | $10,459.00 | +0.00 | +0.00 | — | — | $10,459.00 | $10,459.00 | — |
| 2026-09-03 | -0.90 | $10,459.00 | — | $10,459.00 | +0.00 | +0.00 | — | — | $10,459.00 | $10,459.00 | — |
| 2026-09-04 | +2.25 | $10,459.00 | — | $10,459.00 | +0.00 | -52.34 | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC | — | $37.48 | $10,365.15 | CABA×377, ALEC×518, BHC×194, BMEA×688, OABI×273, OPK×822, VIR×115, ATRC×24 |
| 2026-09-08 | -11.47 | $37.48 | CABA×377, ALEC×518, BHC×194, BMEA×688, OABI×273, OPK×822, VIR×115, ATRC×24 | $10,321.51 | -43.64 | -156.80 | — | — | $37.48 | $10,164.71 | CABA×377, ALEC×518, BHC×194, BMEA×688, OABI×273, OPK×822, VIR×115, ATRC×24 |
| 2026-09-09 | -13.95 | $37.48 | CABA×377, ALEC×518, BHC×194, BMEA×688, OABI×273, OPK×822, VIR×115, ATRC×24 | $10,119.47 | -45.24 | -466.82 | — | — | $37.48 | $9,652.65 | CABA×377, ALEC×518, BHC×194, BMEA×688, OABI×273, OPK×822, VIR×115, ATRC×24 |
| 2026-09-10 | -13.28 | $37.48 | CABA×377, ALEC×518, BHC×194, BMEA×688, OABI×273, OPK×822, VIR×115, ATRC×24 | $9,505.62 | -147.03 | -188.60 | — | — | $37.48 | $9,317.03 | CABA×377, ALEC×518, BHC×194, BMEA×688, OABI×273, OPK×822, VIR×115, ATRC×24 |
| 2026-09-11 | +0.50 | $37.48 | CABA×377, ALEC×518, BHC×194, BMEA×688, OABI×273, OPK×822, VIR×115, ATRC×24 | $9,431.27 | +114.24 | +9.81 | — | — | $37.48 | $9,441.08 | CABA×377, ALEC×518, BHC×194, BMEA×688, OABI×273, OPK×822, VIR×115, ATRC×24 |

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
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.42 | ▲ close $10,803.37 vs 09:30 $10,384.32 (session +419.05) | 16:00 close · cash $78.42 · equity $10,803.37 vs 09:30 $10,384.32 (+419.05; session marks +419.05) · 13 name(s) marked open→close (per-name table). AG×60 09:30 $20.32 → close $21.24 +55.20; BHP×13 09:30 $95.86 → close $98.69 +36.79; CDE×60 09:30 $20.47 → close $21.59 +67.20; HDSN×216 09:30 $5.53 → close $5.47 -12.96; IAG×63 09:30 $21.21 → close $22.17 +60.48; KGC×42 09:30 $32.32 → close $33.48 +48.72; NFGC×714 09:30 $1.90 → close $2.04 +99.96; WPM×8 09:30 $156.51 → close $163.72 +57.68; AUPH×1 09:30 $16.63 → close $16.75 +0.12; ARCT×2 09:30 $14.12 → close $15.44 +2.64; AUTL×9 09:30 $2.38 → close $2.44 +0.54; CRDL×12 09:30 $1.89 → close $2.00 +1.32; CYPH×17 09:30 $1.56 → close $1.64 +1.36 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.42 | ▼ 09:30 equity $10,608.36 vs yday $10,803.37 (-195.01) | 09:30 open · cash $78.42 (unchanged overnight, no fees) · equity $10,608.36 vs prior close $10,803.37 (-195.01) · 13 name(s) re-marked at the open (per-name table). AG×60 yday $21.24 → 09:30 $20.63 -36.60; BHP×13 yday $98.69 → 09:30 $96.99 -22.10; CDE×60 yday $21.59 → 09:30 $21.00 -35.40; HDSN×216 yday $5.47 → 09:30 $5.51 +8.64; IAG×63 yday $22.17 → 09:30 $21.64 -33.39; KGC×42 yday $33.48 → 09:30 $32.90 -24.36; NFGC×714 yday $2.04 → 09:30 $2.00 -28.56; WPM×8 yday $163.72 → 09:30 $160.93 -22.32; AUPH×1 yday $16.75 → 09:30 $16.60 -0.15; ARCT×2 yday $15.44 → 09:30 $15.35 -0.18; AUTL×9 yday $2.44 → 09:30 $2.41 -0.27; CRDL×12 yday $2.00 → 09:30 $2.03 +0.36; CYPH×17 yday $1.64 → 09:30 $1.60 -0.68 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.42 | ▼ close $10,460.93 vs 09:30 $10,608.36 (session -147.43) | 16:00 close · cash $78.42 · equity $10,460.93 vs 09:30 $10,608.36 (-147.43; session marks -147.43) · 13 name(s) marked open→close (per-name table). AG×60 09:30 $20.63 → close $20.99 +21.60; BHP×13 09:30 $96.99 → close $96.33 -8.58; CDE×60 09:30 $21.00 → close $21.44 +26.40; HDSN×216 09:30 $5.51 → close $5.29 -47.52; IAG×63 09:30 $21.64 → close $21.54 -6.30; KGC×42 09:30 $32.90 → close $32.32 -24.36; NFGC×714 09:30 $2.00 → close $1.90 -71.40; WPM×8 09:30 $160.93 → close $156.02 -39.28; AUPH×1 09:30 $16.60 → close $16.54 -0.06; ARCT×2 09:30 $15.35 → close $15.83 +0.96; AUTL×9 09:30 $2.41 → close $2.33 -0.72; CRDL×12 09:30 $2.03 → close $2.14 +1.32; CYPH×17 09:30 $1.60 → close $1.63 +0.51 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.42 | ▲ 09:30 equity $10,484.99 vs yday $10,460.93 (+24.06) | 09:30 open · cash $78.42 (unchanged overnight, no fees) · equity $10,484.99 vs prior close $10,460.93 (+24.06) · 13 name(s) re-marked at the open (per-name table). AG×60 yday $20.99 → 09:30 $20.93 -3.60; BHP×13 yday $96.33 → 09:30 $95.52 -10.53; CDE×60 yday $21.44 → 09:30 $21.31 -7.80; HDSN×216 yday $5.29 → 09:30 $5.49 +43.20; IAG×63 yday $21.54 → 09:30 $21.47 -4.41; KGC×42 yday $32.32 → 09:30 $32.32 +0.00; NFGC×714 yday $1.90 → 09:30 $1.91 +7.14; WPM×8 yday $156.02 → 09:30 $155.89 -1.04; AUPH×1 yday $16.54 → 09:30 $16.47 -0.07; ARCT×2 yday $15.83 → 09:30 $15.74 -0.18; AUTL×9 yday $2.33 → 09:30 $2.32 -0.09; CRDL×12 yday $2.14 → 09:30 $2.09 -0.60; CYPH×17 yday $1.63 → 09:30 $1.75 +2.04 | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 60 | $20.93 | $2.19 | $+18.44 | $1,332.03 | ▲ +18.44 after sell → book $10,482.80; vs 09:30 mark -2.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 13 | $95.52 | $2.05 | $+54.55 | $2,571.74 | ▲ +54.55 after sell → book $10,480.75; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 60 | $21.31 | $2.19 | $+35.24 | $3,848.15 | ▲ +35.24 after sell → book $10,478.56; vs 09:30 mark -2.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 216 | $5.49 | $2.83 | $-66.10 | $5,031.16 | ▼ -66.10 after sell → book $10,475.73; vs 09:30 mark -2.83 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 63 | $21.47 | $2.20 | $+111.54 | $6,381.57 | ▲ +111.54 after sell → book $10,473.53; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 42 | $32.32 | $2.14 | $+108.73 | $7,736.87 | ▲ +108.73 after sell → book $10,471.39; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 714 | $1.91 | $9.34 | $+95.69 | $9,091.27 | ▲ +95.69 after sell → book $10,462.05; vs 09:30 mark -9.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 8 | $155.89 | $2.03 | $+86.75 | $10,336.36 | ▲ +86.75 after sell → book $10,460.02; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,336.36 | ▲ close $10,463.26 vs 09:30 $10,484.99 (session +3.25) | 16:00 close · cash $10,336.36 · equity $10,463.26 vs 09:30 $10,484.99 (-21.73; session marks +3.25) · 5 name(s) marked open→close (per-name table). AUPH×1 09:30 $16.47 → close $16.48 +0.01; ARCT×2 09:30 $15.74 → close $16.17 +0.86; AUTL×9 09:30 $2.32 → close $2.36 +0.36; CRDL×12 09:30 $2.09 → close $2.06 -0.36; CYPH×17 09:30 $1.75 → close $1.89 +2.38 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,336.36 | ▼ 09:30 equity $10,460.47 vs yday $10,463.26 (-2.79) | 09:30 open · cash $10,336.36 (unchanged overnight, no fees) · equity $10,460.47 vs prior close $10,463.26 (-2.79) · 5 name(s) re-marked at the open (per-name table). AUPH×1 yday $16.48 → 09:30 $16.44 -0.04; ARCT×2 yday $16.17 → 09:30 $15.43 -1.48; AUTL×9 yday $2.36 → 09:30 $2.35 -0.09; CRDL×12 yday $2.06 → 09:30 $2.06 +0.00; CYPH×17 yday $1.89 → 09:30 $1.82 -1.19 | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.44 | $0.19 | $-1.12 | $10,352.61 | ▼ -1.12 after sell → book $10,460.28; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.43 | $0.33 | $+8.04 | $10,383.13 | ▲ +8.04 after sell → book $10,459.94; vs 09:30 mark -0.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 9 | $2.35 | $0.26 | $-1.59 | $10,404.03 | ▼ -1.59 after sell → book $10,459.69; vs 09:30 mark -0.25 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 12 | $2.06 | $0.30 | $+0.99 | $10,428.44 | ▲ +0.99 after sell → book $10,459.38; vs 09:30 mark -0.31 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 17 | $1.82 | $0.38 | $+7.84 | $10,459.00 | ▲ +7.84 after sell → book $10,459.00; vs 09:30 mark -0.38 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,459.00 | ▲ close $10,459.00 vs 09:30 $10,460.47 (session +0.00) | 16:00 close · cash $10,459.00 · no lots left · equity $10,459.00. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,459.00 | ▲ 09:30 equity $10,459.00 vs yday $10,459.00 (+0.00) | 09:30 open · cash $10,459.00 · no holdings · equity $10,459.00 vs prior close $10,459.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,459.00 | ▲ close $10,459.00 vs 09:30 $10,459.00 (session +0.00) | 16:00 close · cash $10,459.00 · no lots left · equity $10,459.00. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,459.00 | ▲ 09:30 equity $10,459.00 vs yday $10,459.00 (+0.00) | 09:30 open · cash $10,459.00 · no holdings · equity $10,459.00 vs prior close $10,459.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,459.00 | ▲ close $10,459.00 vs 09:30 $10,459.00 (session +0.00) | 16:00 close · cash $10,459.00 · no lots left · equity $10,459.00. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,459.00 | ▲ 09:30 equity $10,459.00 vs yday $10,459.00 (+0.00) | 09:30 open · cash $10,459.00 · no holdings · equity $10,459.00 vs prior close $10,459.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,459.00 | ▲ close $10,459.00 vs 09:30 $10,459.00 (session +0.00) | 16:00 close · cash $10,459.00 · no lots left · equity $10,459.00. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,459.00 | ▲ 09:30 equity $10,459.00 vs yday $10,459.00 (+0.00) | 09:30 open · cash $10,459.00 · no holdings · equity $10,459.00 vs prior close $10,459.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,459.00 | ▲ close $10,459.00 vs 09:30 $10,459.00 (session +0.00) | 16:00 close · cash $10,459.00 · no lots left · equity $10,459.00. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,459.00 | ▲ 09:30 equity $10,459.00 vs yday $10,459.00 (+0.00) | 09:30 open · cash $10,459.00 · no holdings · equity $10,459.00 vs prior close $10,459.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 377 | $3.46 | $4.86 | — | $9,149.72 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $1307.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 518 | $2.52 | $6.68 | — | $7,837.68 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $1307.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 194 | $6.71 | $2.57 | — | $6,533.36 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $1307.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 688 | $1.90 | $8.88 | — | $5,217.29 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $1307.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 273 | $4.78 | $3.52 | — | $3,908.83 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $1307.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 822 | $1.59 | $10.60 | — | $2,591.24 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $1307.38 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 115 | $11.31 | $2.33 | — | $1,288.26 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $1307.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 24 | $52.03 | $2.06 | — | $37.48 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+6.5; leftover $1307.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.48 | ▼ close $10,365.15 vs 09:30 $10,459.00 (session -52.34) | 16:00 close · cash $37.48 · equity $10,365.15 vs 09:30 $10,459.00 (-93.85; session marks -52.34) · 8 name(s) marked open→close (per-name table). CABA×377 09:30 $3.46 → close $3.47 +3.77; ALEC×518 09:30 $2.52 → close $2.46 -31.08; BHC×194 09:30 $6.71 → close $6.56 -29.10; BMEA×688 09:30 $1.90 → close $2.03 +89.44; OABI×273 09:30 $4.78 → close $4.33 -122.85; OPK×822 09:30 $1.59 → close $1.64 +41.10; VIR×115 09:30 $11.31 → close $11.38 +8.62; ATRC×24 09:30 $52.03 → close $51.52 -12.24 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.48 | ▼ 09:30 equity $10,321.51 vs yday $10,365.15 (-43.64) | 09:30 open · cash $37.48 (unchanged overnight, no fees) · equity $10,321.51 vs prior close $10,365.15 (-43.64) · 8 name(s) re-marked at the open (per-name table). CABA×377 yday $3.47 → 09:30 $3.43 -15.08; ALEC×518 yday $2.46 → 09:30 $2.38 -41.44; BHC×194 yday $6.56 → 09:30 $6.57 +1.94; BMEA×688 yday $2.03 → 09:30 $2.00 -20.64; OABI×273 yday $4.33 → 09:30 $4.30 -8.19; OPK×822 yday $1.64 → 09:30 $1.63 -8.22; VIR×115 yday $11.38 → 09:30 $11.22 -18.97; ATRC×24 yday $51.52 → 09:30 $54.31 +66.96 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.48 | ▼ close $10,164.71 vs 09:30 $10,321.51 (session -156.80) | 16:00 close · cash $37.48 · equity $10,164.71 vs 09:30 $10,321.51 (-156.80; session marks -156.80) · 8 name(s) marked open→close (per-name table). CABA×377 09:30 $3.43 → close $3.27 -60.32; ALEC×518 09:30 $2.38 → close $2.47 +46.62; BHC×194 09:30 $6.57 → close $6.43 -27.16; BMEA×688 09:30 $2.00 → close $1.93 -48.16; OABI×273 09:30 $4.30 → close $4.24 -16.38; OPK×822 09:30 $1.63 → close $1.59 -32.88; VIR×115 09:30 $11.22 → close $11.18 -4.60; ATRC×24 09:30 $54.31 → close $53.73 -13.92 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.48 | ▼ 09:30 equity $10,119.47 vs yday $10,164.71 (-45.24) | 09:30 open · cash $37.48 (unchanged overnight, no fees) · equity $10,119.47 vs prior close $10,164.71 (-45.24) · 8 name(s) re-marked at the open (per-name table). CABA×377 yday $3.27 → 09:30 $3.28 +3.77; ALEC×518 yday $2.47 → 09:30 $2.47 +0.00; BHC×194 yday $6.43 → 09:30 $6.38 -9.70; BMEA×688 yday $1.93 → 09:30 $1.94 +6.88; OABI×273 yday $4.24 → 09:30 $4.21 -8.19; OPK×822 yday $1.59 → 09:30 $1.58 -8.22; VIR×115 yday $11.18 → 09:30 $11.04 -16.10; ATRC×24 yday $53.73 → 09:30 $53.16 -13.68 | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.48 | ▼ close $9,652.65 vs 09:30 $10,119.47 (session -466.82) | 16:00 close · cash $37.48 · equity $9,652.65 vs 09:30 $10,119.47 (-466.82; session marks -466.82) · 8 name(s) marked open→close (per-name table). CABA×377 09:30 $3.28 → close $2.91 -139.49; ALEC×518 09:30 $2.47 → close $2.27 -103.60; BHC×194 09:30 $6.38 → close $6.16 -42.68; BMEA×688 09:30 $1.94 → close $1.84 -65.36; OABI×273 09:30 $4.21 → close $4.01 -53.24; OPK×822 09:30 $1.58 → close $1.54 -32.88; VIR×115 09:30 $11.04 → close $10.81 -26.45; ATRC×24 09:30 $53.16 → close $53.03 -3.12 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.48 | ▼ 09:30 equity $9,505.62 vs yday $9,652.65 (-147.03) | 09:30 open · cash $37.48 (unchanged overnight, no fees) · equity $9,505.62 vs prior close $9,652.65 (-147.03) · 8 name(s) re-marked at the open (per-name table). CABA×377 yday $2.91 → 09:30 $2.85 -22.62; ALEC×518 yday $2.27 → 09:30 $2.22 -25.90; BHC×194 yday $6.16 → 09:30 $6.11 -9.70; BMEA×688 yday $1.84 → 09:30 $1.83 -10.32; OABI×273 yday $4.01 → 09:30 $3.92 -25.39; OPK×822 yday $1.54 → 09:30 $1.53 -8.22; VIR×115 yday $10.81 → 09:30 $10.57 -27.60; ATRC×24 yday $53.03 → 09:30 $52.31 -17.28 | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.48 | ▼ close $9,317.03 vs 09:30 $9,505.62 (session -188.60) | 16:00 close · cash $37.48 · equity $9,317.03 vs 09:30 $9,505.62 (-188.59; session marks -188.60) · 8 name(s) marked open→close (per-name table). CABA×377 09:30 $2.85 → close $2.74 -41.47; ALEC×518 09:30 $2.22 → close $2.14 -41.44; BHC×194 09:30 $6.11 → close $6.07 -7.76; BMEA×688 09:30 $1.83 → close $1.70 -89.44; OABI×273 09:30 $3.92 → close $3.95 +7.64; OPK×822 09:30 $1.53 → close $1.49 -32.88; VIR×115 09:30 $10.57 → close $10.58 +1.15; ATRC×24 09:30 $52.31 → close $52.96 +15.60 | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.48 | ▲ 09:30 equity $9,431.27 vs yday $9,317.03 (+114.24) | 09:30 open · cash $37.48 (unchanged overnight, no fees) · equity $9,431.27 vs prior close $9,317.03 (+114.24) · 8 name(s) re-marked at the open (per-name table). CABA×377 yday $2.74 → 09:30 $2.77 +11.31; ALEC×518 yday $2.14 → 09:30 $2.17 +15.54; BHC×194 yday $6.07 → 09:30 $6.12 +9.70; BMEA×688 yday $1.70 → 09:30 $1.75 +34.40; OABI×273 yday $3.95 → 09:30 $3.97 +5.46; OPK×822 yday $1.49 → 09:30 $1.49 +0.00; VIR×115 yday $10.58 → 09:30 $10.79 +24.15; ATRC×24 yday $52.96 → 09:30 $53.53 +13.68 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.48 | ▲ close $9,441.08 vs 09:30 $9,431.27 (session +9.81) | 16:00 close · cash $37.48 · equity $9,441.08 vs 09:30 $9,431.27 (+9.81; session marks +9.81) · 8 name(s) marked open→close (per-name table). CABA×377 09:30 $2.77 → close $2.73 -15.08; ALEC×518 09:30 $2.17 → close $2.14 -15.54; BHC×194 09:30 $6.12 → close $5.86 -50.44; BMEA×688 09:30 $1.75 → close $1.74 -6.88; OABI×273 09:30 $3.97 → close $4.07 +27.30; OPK×822 09:30 $1.49 → close $1.57 +65.76; VIR×115 09:30 $10.79 → close $10.62 -19.55; ATRC×24 09:30 $53.53 → close $54.54 +24.24 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 23.36 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 23.36 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 23.36 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-25 | `AG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BHP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `CDE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `HDSN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `IAG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NFGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `WPM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-10 | `CABA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `ALEC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BHC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `OABI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `OPK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `VIR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `ATRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-11 | `CABA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `ALEC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BHC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OABI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OPK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `VIR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `ATRC` | min_hold | dropped but min-hold 4/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CABA` | 377 | 2026-09-04 @ $3.46 | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $1307.38 |
| `ALEC` | 518 | 2026-09-04 @ $2.52 | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $1307.38 |
| `BHC` | 194 | 2026-09-04 @ $6.71 | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $1307.38 |
| `BMEA` | 688 | 2026-09-04 @ $1.90 | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $1307.38 |
| `OABI` | 273 | 2026-09-04 @ $4.78 | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $1307.38 |
| `OPK` | 822 | 2026-09-04 @ $1.59 | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $1307.38 |
| `VIR` | 115 | 2026-09-04 @ $11.31 | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $1307.38 |
| `ATRC` | 24 | 2026-09-04 @ $52.03 | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+6.5; leftover $1307.38 |
