# Factor mine action — `union_ab_g_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ ab_g, no 🚨

Cash book **+4.09%** ($10,409) · signal-only (no cash/fees) was -0.48%. Starts YES **10/21**. Fills 110 · skips 56 · realized $+321.01.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the A/B camera (does our A/B score like this name?) is green.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

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
- **Gate** `ab=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $193.26.

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
| 2026-08-21 | `AG` | 60 | $21.19 | $21.90 | +42.60 | — | +0.00 | +42.60 | +81.00 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `CDE` | 60 | $21.11 | $21.75 | +38.40 | — | +0.00 | +38.40 | +66.00 | — |
| 2026-08-21 | `HDSN` | 216 | $5.57 | $5.67 | +21.60 | — | +0.00 | +21.60 | -21.60 | — |
| 2026-08-21 | `IAG` | 63 | $20.50 | $21.17 | +42.21 | — | +0.00 | +42.21 | +97.02 | — |
| 2026-08-21 | `KGC` | 42 | $31.43 | $32.17 | +31.08 | — | +0.00 | +31.08 | +106.68 | — |
| 2026-08-21 | `NFGC` | 714 | $1.75 | $1.79 | +28.56 | — | +0.00 | +28.56 | +28.56 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 75 | — | $17.20 | +0.00 | $16.65 | -41.25 | -41.25 | +0.00 | -41.25 |
| 2026-08-21 | `AEM` | 6 | — | $216.30 | +0.00 | $216.06 | -1.44 | -1.44 | +0.00 | -1.44 |
| 2026-08-21 | `ARCT` | 117 | — | $11.13 | +0.00 | $13.45 | +271.44 | +271.44 | +0.00 | +271.44 |
| 2026-08-21 | `AUTL` | 528 | — | $2.47 | +0.00 | $2.41 | -31.68 | -31.68 | +0.00 | -31.68 |
| 2026-08-21 | `CRDL` | 676 | — | $1.93 | +0.00 | $1.86 | -47.32 | -47.32 | +0.00 | -47.32 |
| 2026-08-21 | `CRSP` | 21 | — | $59.72 | +0.00 | $59.50 | -4.62 | -4.62 | +0.00 | -4.62 |
| 2026-08-21 | `CYPH` | 989 | — | $1.32 | +0.00 | $1.42 | +98.90 | +98.90 | +0.00 | +98.90 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 75 | $16.65 | $16.57 | -6.00 | — | +0.00 | -6.00 | -47.25 | — |
| 2026-08-24 | `AEM` | 6 | $216.06 | $217.03 | +5.82 | — | +0.00 | +5.82 | +4.38 | — |
| 2026-08-24 | `ARCT` | 117 | $13.45 | $13.33 | -14.04 | — | +0.00 | -14.04 | +257.40 | — |
| 2026-08-24 | `AUTL` | 528 | $2.41 | $2.40 | -5.28 | — | +0.00 | -5.28 | -36.96 | — |
| 2026-08-24 | `CRDL` | 676 | $1.86 | $1.88 | +13.52 | — | +0.00 | +13.52 | -33.80 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | — | +0.00 | -15.75 | -20.37 | — |
| 2026-08-24 | `CYPH` | 989 | $1.42 | $1.83 | +405.49 | — | +0.00 | +405.49 | +504.39 | — |
| 2026-08-25 | `MOS` | 57 | — | $23.77 | +0.00 | $24.27 | +28.50 | +28.50 | +0.00 | +28.50 |
| 2026-08-25 | `OCUL` | 125 | — | $10.98 | +0.00 | $10.88 | -12.50 | -12.50 | +0.00 | -12.50 |
| 2026-08-25 | `INSP` | 22 | — | $61.19 | +0.00 | $61.07 | -2.64 | -2.64 | +0.00 | -2.64 |
| 2026-08-25 | `CRMD` | 164 | — | $8.35 | +0.00 | $8.56 | +34.44 | +34.44 | +0.00 | +34.44 |
| 2026-08-25 | `RZLT` | 278 | — | $4.94 | +0.00 | $5.01 | +19.46 | +19.46 | +0.00 | +19.46 |
| 2026-08-25 | `HCA` | 3 | — | $426.97 | +0.00 | $428.76 | +5.37 | +5.37 | +0.00 | +5.37 |
| 2026-08-25 | `VITL` | 123 | — | $11.12 | +0.00 | $11.11 | -1.23 | -1.23 | +0.00 | -1.23 |
| 2026-08-25 | `KURA` | 101 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `MOS` | 57 | $24.27 | $24.84 | +32.49 | $24.16 | -38.76 | -6.27 | +60.99 | +22.23 |
| 2026-08-26 | `OCUL` | 125 | $10.88 | $10.79 | -11.25 | $10.77 | -2.50 | -13.75 | -23.75 | -26.25 |
| 2026-08-26 | `INSP` | 22 | $61.07 | $60.07 | -22.00 | $61.80 | +38.06 | +16.06 | -24.64 | +13.42 |
| 2026-08-26 | `CRMD` | 164 | $8.56 | $8.60 | +6.56 | $8.39 | -34.44 | -27.88 | +41.00 | +6.56 |
| 2026-08-26 | `RZLT` | 278 | $5.01 | $5.01 | +0.00 | $5.04 | +8.34 | +8.34 | +19.46 | +27.80 |
| 2026-08-26 | `HCA` | 3 | $428.76 | $427.50 | -3.78 | $427.16 | -1.02 | -4.80 | +1.59 | +0.57 |
| 2026-08-26 | `VITL` | 123 | $11.11 | $11.03 | -9.84 | — | +0.00 | -9.84 | -11.07 | — |
| 2026-08-26 | `KURA` | 101 | $13.59 | $13.63 | +4.04 | — | +0.00 | +4.04 | +4.04 | — |
| 2026-08-26 | `AVBP` | 46 | — | $31.21 | +0.00 | $31.14 | -3.22 | -3.22 | +0.00 | -3.22 |
| 2026-08-26 | `ABX` | 146 | — | $9.83 | +0.00 | $9.78 | -7.30 | -7.30 | +0.00 | -7.30 |
| 2026-08-27 | `MOS` | 57 | $24.16 | $24.00 | -9.12 | $23.76 | -13.68 | -22.80 | +13.11 | -0.57 |
| 2026-08-27 | `OCUL` | 125 | $10.77 | $10.63 | -17.50 | — | +0.00 | -17.50 | -43.75 | — |
| 2026-08-27 | `INSP` | 22 | $61.80 | $62.10 | +6.60 | — | +0.00 | +6.60 | +20.02 | — |
| 2026-08-27 | `CRMD` | 164 | $8.39 | $8.49 | +16.40 | — | +0.00 | +16.40 | +22.96 | — |
| 2026-08-27 | `RZLT` | 278 | $5.04 | $5.07 | +8.34 | — | +0.00 | +8.34 | +36.14 | — |
| 2026-08-27 | `HCA` | 3 | $427.16 | $424.61 | -7.65 | — | +0.00 | -7.65 | -7.08 | — |
| 2026-08-27 | `AVBP` | 46 | $31.14 | $30.79 | -16.10 | — | +0.00 | -16.10 | -19.32 | — |
| 2026-08-27 | `ABX` | 146 | $9.78 | $9.68 | -14.60 | — | +0.00 | -14.60 | -21.90 | — |
| 2026-08-27 | `RRC` | 33 | — | $41.44 | +0.00 | $41.64 | +6.60 | +6.60 | +0.00 | +6.60 |
| 2026-08-27 | `CRK` | 95 | — | $14.42 | +0.00 | $14.62 | +19.00 | +19.00 | +0.00 | +19.00 |
| 2026-08-27 | `SLI` | 526 | — | $2.60 | +0.00 | $2.64 | +21.04 | +21.04 | +0.00 | +21.04 |
| 2026-08-27 | `ACMR` | 16 | — | $81.65 | +0.00 | $80.49 | -18.56 | -18.56 | +0.00 | -18.56 |
| 2026-08-27 | `GGB` | 299 | — | $4.57 | +0.00 | $4.70 | +38.87 | +38.87 | +0.00 | +38.87 |
| 2026-08-27 | `MT` | 18 | — | $74.54 | +0.00 | $74.63 | +1.62 | +1.62 | +0.00 | +1.62 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-28 | `MOS` | 57 | $23.76 | $23.95 | +10.83 | $23.60 | -19.95 | -9.12 | +10.26 | -9.69 |
| 2026-08-28 | `RRC` | 33 | $41.64 | $41.74 | +3.30 | $41.46 | -9.24 | -5.94 | +9.90 | +0.66 |
| 2026-08-28 | `CRK` | 95 | $14.62 | $14.63 | +0.95 | $14.29 | -32.30 | -31.35 | +19.95 | -12.35 |
| 2026-08-28 | `SLI` | 526 | $2.64 | $2.68 | +21.04 | $2.55 | -68.38 | -47.34 | +42.08 | -26.30 |
| 2026-08-28 | `ACMR` | 16 | $80.49 | $79.27 | -19.52 | — | +0.00 | -19.52 | -38.08 | — |
| 2026-08-28 | `GGB` | 299 | $4.70 | $4.67 | -8.97 | — | +0.00 | -8.97 | +29.90 | — |
| 2026-08-28 | `MT` | 18 | $74.63 | $75.39 | +13.68 | — | +0.00 | +13.68 | +15.30 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `GRRR` | 86 | — | $15.66 | +0.00 | $14.41 | -107.50 | -107.50 | +0.00 | -107.50 |
| 2026-08-28 | `URBN` | 17 | — | $79.42 | +0.00 | $81.09 | +28.39 | +28.39 | +0.00 | +28.39 |
| 2026-08-28 | `SIMO` | 5 | — | $252.24 | +0.00 | $245.81 | -32.15 | -32.15 | +0.00 | -32.15 |
| 2026-08-28 | `ANF` | 9 | — | $146.07 | +0.00 | $148.42 | +21.15 | +21.15 | +0.00 | +21.15 |
| 2026-08-31 | `MOS` | 57 | $23.60 | $23.68 | +4.56 | — | +0.00 | +4.56 | -5.13 | — |
| 2026-08-31 | `RRC` | 33 | $41.46 | $42.00 | +17.82 | — | +0.00 | +17.82 | +18.48 | — |
| 2026-08-31 | `CRK` | 95 | $14.29 | $14.54 | +23.75 | — | +0.00 | +23.75 | +11.40 | — |
| 2026-08-31 | `SLI` | 526 | $2.55 | $2.58 | +15.78 | — | +0.00 | +15.78 | -10.52 | — |
| 2026-08-31 | `GRRR` | 86 | $14.41 | $14.44 | +2.58 | — | +0.00 | +2.58 | -104.92 | — |
| 2026-08-31 | `URBN` | 17 | $81.09 | $80.44 | -11.05 | — | +0.00 | -11.05 | +17.34 | — |
| 2026-08-31 | `SIMO` | 5 | $245.81 | $247.05 | +6.20 | — | +0.00 | +6.20 | -25.95 | — |
| 2026-08-31 | `ANF` | 9 | $148.42 | $148.03 | -3.51 | — | +0.00 | -3.51 | +17.64 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 25 | — | $52.88 | +0.00 | $52.46 | -10.50 | -10.50 | +0.00 | -10.50 |
| 2026-09-03 | `HRMY` | 31 | — | $42.93 | +0.00 | $41.86 | -33.17 | -33.17 | +0.00 | -33.17 |
| 2026-09-03 | `CABA` | 370 | — | $3.63 | +0.00 | $3.48 | -55.50 | -55.50 | +0.00 | -55.50 |
| 2026-09-03 | `VSTM` | 167 | — | $8.03 | +0.00 | $7.98 | -8.35 | -8.35 | +0.00 | -8.35 |
| 2026-09-03 | `RVTY` | 10 | — | $132.45 | +0.00 | $130.63 | -18.20 | -18.20 | +0.00 | -18.20 |
| 2026-09-03 | `CRK` | 87 | — | $15.45 | +0.00 | $14.95 | -43.50 | -43.50 | +0.00 | -43.50 |
| 2026-09-03 | `MRNA` | 9 | — | $145.94 | +0.00 | $148.87 | +26.33 | +26.33 | +0.00 | +26.33 |
| 2026-09-03 | `ARCT` | 80 | — | $16.77 | +0.00 | $15.56 | -96.80 | -96.80 | +0.00 | -96.80 |
| 2026-09-04 | `ATRC` | 25 | $52.46 | $52.03 | -10.75 | $51.52 | -12.75 | -23.50 | -21.25 | -34.00 |
| 2026-09-04 | `HRMY` | 31 | $41.86 | $41.50 | -11.16 | — | +0.00 | -11.16 | -44.33 | — |
| 2026-09-04 | `CABA` | 370 | $3.48 | $3.46 | -7.40 | $3.47 | +3.70 | -3.70 | -62.90 | -59.20 |
| 2026-09-04 | `VSTM` | 167 | $7.98 | $7.91 | -11.69 | — | +0.00 | -11.69 | -20.04 | — |
| 2026-09-04 | `RVTY` | 10 | $130.63 | $130.03 | -6.00 | — | +0.00 | -6.00 | -24.20 | — |
| 2026-09-04 | `CRK` | 87 | $14.95 | $15.00 | +4.35 | — | +0.00 | +4.35 | -39.15 | — |
| 2026-09-04 | `MRNA` | 9 | $148.87 | $153.62 | +42.75 | — | +0.00 | +42.75 | +69.08 | — |
| 2026-09-04 | `ARCT` | 80 | $15.56 | $15.61 | +4.00 | — | +0.00 | +4.00 | -92.80 | — |
| 2026-09-04 | `ALEC` | 523 | — | $2.52 | +0.00 | $2.46 | -31.38 | -31.38 | +0.00 | -31.38 |
| 2026-09-04 | `BHC` | 196 | — | $6.71 | +0.00 | $6.56 | -29.40 | -29.40 | +0.00 | -29.40 |
| 2026-09-04 | `BMEA` | 694 | — | $1.90 | +0.00 | $2.03 | +90.22 | +90.22 | +0.00 | +90.22 |
| 2026-09-04 | `OABI` | 275 | — | $4.78 | +0.00 | $4.33 | -123.75 | -123.75 | +0.00 | -123.75 |
| 2026-09-04 | `OPK` | 829 | — | $1.59 | +0.00 | $1.64 | +41.45 | +41.45 | +0.00 | +41.45 |
| 2026-09-04 | `VIR` | 114 | — | $11.31 | +0.00 | $11.38 | +8.55 | +8.55 | +0.00 | +8.55 |
| 2026-09-08 | `ATRC` | 25 | $51.52 | $54.31 | +69.75 | — | +0.00 | +69.75 | +35.75 | — |
| 2026-09-08 | `CABA` | 370 | $3.47 | $3.43 | -14.80 | — | +0.00 | -14.80 | -74.00 | — |
| 2026-09-08 | `ALEC` | 523 | $2.46 | $2.38 | -41.84 | — | +0.00 | -41.84 | -73.22 | — |
| 2026-09-08 | `BHC` | 196 | $6.56 | $6.57 | +1.96 | — | +0.00 | +1.96 | -27.44 | — |
| 2026-09-08 | `BMEA` | 694 | $2.03 | $2.00 | -20.82 | — | +0.00 | -20.82 | +69.40 | — |
| 2026-09-08 | `OABI` | 275 | $4.33 | $4.30 | -8.25 | — | +0.00 | -8.25 | -132.00 | — |
| 2026-09-08 | `OPK` | 829 | $1.64 | $1.63 | -8.29 | — | +0.00 | -8.29 | +33.16 | — |
| 2026-09-08 | `VIR` | 114 | $11.38 | $11.22 | -18.81 | — | +0.00 | -18.81 | -10.26 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `AUPH` | 79 | — | $16.28 | +0.00 | $16.10 | -14.22 | -14.22 | +0.00 | -14.22 |
| 2026-09-11 | `OVID` | 472 | — | $2.73 | +0.00 | $2.69 | -18.88 | -18.88 | +0.00 | -18.88 |
| 2026-09-11 | `SANM` | 6 | — | $206.84 | +0.00 | $216.00 | +54.96 | +54.96 | +0.00 | +54.96 |
| 2026-09-11 | `ORCL` | 7 | — | $164.43 | +0.00 | $150.28 | -99.05 | -99.05 | +0.00 | -99.05 |
| 2026-09-11 | `NVT` | 8 | — | $157.78 | +0.00 | $162.38 | +36.80 | +36.80 | +0.00 | +36.80 |
| 2026-09-11 | `COHU` | 23 | — | $56.09 | +0.00 | $57.08 | +22.77 | +22.77 | +0.00 | +22.77 |
| 2026-09-11 | `CMRC` | 412 | — | $3.13 | +0.00 | $3.50 | +154.50 | +154.50 | +0.00 | +154.50 |
| 2026-09-11 | `AMTX` | 632 | — | $2.04 | +0.00 | $2.01 | -18.96 | -18.96 | +0.00 | -18.96 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +232.95 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $186.91 | $10,208.28 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8 |
| 2026-08-21 | +3.25 | $186.91 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8 | $10,475.50 | +267.22 | +261.93 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $158.85 | $10,673.53 | AU×10, AUPH×75, AEM×6, ARCT×117, AUTL×528, CRDL×676, CRSP×21, CYPH×989 |
| 2026-08-24 | -5.17 | $158.85 | AU×10, AUPH×75, AEM×6, ARCT×117, AUTL×528, CRDL×676, CRSP×21, CYPH×989 | $11,050.19 | +376.66 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $11,010.75 | $11,010.75 | — |
| 2026-08-25 | +1.80 | $11,010.75 | — | $11,010.75 | -0.00 | +71.40 | MOS, OCUL, INSP, CRMD, RZLT, HCA, VITL, KURA | — | $153.89 | $11,062.84 | MOS×57, OCUL×125, INSP×22, CRMD×164, RZLT×278, HCA×3, VITL×123, KURA×101 |
| 2026-08-26 | +2.02 | $153.89 | MOS×57, OCUL×125, INSP×22, CRMD×164, RZLT×278, HCA×3, VITL×123, KURA×101 | $11,059.06 | -3.78 | -40.84 | AVBP, ABX | VITL, KURA | $7.11 | $11,008.96 | MOS×57, OCUL×125, INSP×22, CRMD×164, RZLT×278, HCA×3, AVBP×46, ABX×146 |
| 2026-08-27 | — | $7.11 | MOS×57, OCUL×125, INSP×22, CRMD×164, RZLT×278, HCA×3, AVBP×46, ABX×146 | $10,975.33 | -33.63 | +23.27 | RRC, CRK, SLI, ACMR, GGB, MT, MU | OCUL, INSP, CRMD, RZLT, HCA, AVBP, ABX | $482.40 | $10,960.25 | MOS×57, RRC×33, CRK×95, SLI×526, ACMR×16, GGB×299, MT×18, MU×1 |
| 2026-08-28 | +0.75 | $482.40 | MOS×57, RRC×33, CRK×95, SLI×526, ACMR×16, GGB×299, MT×18, MU×1 | $10,965.46 | +5.21 | -219.98 | GRRR, URBN, SIMO, ANF | ACMR, GGB, MT, MU | $132.26 | $10,727.11 | MOS×57, RRC×33, CRK×95, SLI×526, GRRR×86, URBN×17, SIMO×5, ANF×9 |
| 2026-08-31 | -5.85 | $132.26 | MOS×57, RRC×33, CRK×95, SLI×526, GRRR×86, URBN×17, SIMO×5, ANF×9 | $10,783.24 | +56.13 | +0.00 | — | MOS, RRC, CRK, SLI, GRRR, URBN, SIMO, ANF | $10,761.37 | $10,761.37 | — |
| 2026-09-01 | -6.30 | $10,761.37 | — | $10,761.37 | -0.00 | +0.00 | — | — | $10,761.37 | $10,761.37 | — |
| 2026-09-02 | -3.83 | $10,761.37 | — | $10,761.37 | -0.00 | +0.00 | — | — | $10,761.37 | $10,761.37 | — |
| 2026-09-03 | -0.90 | $10,761.37 | — | $10,761.37 | -0.00 | -239.69 | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MRNA, ARCT | — | $80.74 | $10,501.74 | ATRC×25, HRMY×31, CABA×370, VSTM×167, RVTY×10, CRK×87, MRNA×9, ARCT×80 |
| 2026-09-04 | +2.25 | $80.74 | ATRC×25, HRMY×31, CABA×370, VSTM×167, RVTY×10, CRK×87, MRNA×9, ARCT×80 | $10,505.84 | +4.10 | -53.36 | ALEC, BHC, BMEA, OABI, OPK, VIR | HRMY, VSTM, RVTY, CRK, MRNA, ARCT | $3.13 | $10,404.39 | ATRC×25, CABA×370, ALEC×523, BHC×196, BMEA×694, OABI×275, OPK×829, VIR×114 |
| 2026-09-08 | -11.47 | $3.13 | ATRC×25, CABA×370, ALEC×523, BHC×196, BMEA×694, OABI×275, OPK×829, VIR×114 | $10,363.29 | -41.10 | +0.00 | — | ATRC, CABA, ALEC, BHC, BMEA, OABI, OPK, VIR | $10,321.01 | $10,321.01 | — |
| 2026-09-09 | -13.95 | $10,321.01 | — | $10,321.01 | +0.00 | +0.00 | — | — | $10,321.01 | $10,321.01 | — |
| 2026-09-10 | -13.28 | $10,321.01 | — | $10,321.01 | +0.00 | +0.00 | — | — | $10,321.01 | $10,321.01 | — |
| 2026-09-11 | +0.50 | $10,321.01 | — | $10,321.01 | +0.00 | +117.92 | AUPH, OVID, SANM, ORCL, NVT, COHU, CMRC, AMTX | — | $193.26 | $10,409.06 | AUPH×79, OVID×472, SANM×6, ORCL×7, NVT×8, COHU×23, CMRC×412, AMTX×632 |

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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,764.83 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,579.67 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,338.50 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 216 | $5.77 | $2.79 | — | $5,089.39 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,850.53 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,603.95 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 714 | $1.75 | $9.21 | — | $1,345.24 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $186.91 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $186.91 | ▲ close $10,208.28 vs 09:30 $10,000.00 (session +232.95) | 16:00 close · cash $186.91 · equity $10,208.28 vs 09:30 $10,000.00 (+208.28; session marks +232.95) · 8 name(s) marked open→close (per-name table). AG×60 09:30 $20.55 → close $21.19 +38.40; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×60 09:30 $20.65 → close $21.11 +27.60; HDSN×216 09:30 $5.77 → close $5.57 -43.20; IAG×63 09:30 $19.63 → close $20.50 +54.81; KGC×42 09:30 $29.63 → close $31.43 +75.60; NFGC×714 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.91 | ▲ 09:30 equity $10,475.50 vs yday $10,208.28 (+267.22) | 09:30 open · cash $186.91 (unchanged overnight, no fees) · equity $10,475.50 vs prior close $10,208.28 (+267.22) · 8 name(s) re-marked at the open (per-name table). AG×60 yday $21.19 → 09:30 $21.90 +42.60; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×60 yday $21.11 → 09:30 $21.75 +38.40; HDSN×216 yday $5.57 → 09:30 $5.67 +21.60; IAG×63 yday $20.50 → 09:30 $21.17 +42.21; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×714 yday $1.75 → 09:30 $1.79 +28.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 60 | $21.90 | $2.19 | $+76.64 | $1,498.71 | ▲ +76.64 after sell → book $10,473.30; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,741.03 | ▲ +57.15 after sell → book $10,471.26; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 60 | $21.75 | $2.19 | $+61.64 | $4,043.84 | ▲ +61.64 after sell → book $10,469.07; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 216 | $5.67 | $2.83 | $-27.22 | $5,265.72 | ▼ -27.22 after sell → book $10,466.23; vs 09:30 mark -2.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 63 | $21.17 | $2.20 | $+92.64 | $6,597.23 | ▲ +92.64 after sell → book $10,464.03; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $7,946.24 | ▲ +102.43 after sell → book $10,461.90; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 714 | $1.79 | $9.34 | $+10.01 | $9,214.96 | ▲ +10.01 after sell → book $10,452.56; vs 09:30 mark -9.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,450.52 | ▲ +77.23 after sell → book $10,450.52; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,254.20 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 75 | $17.20 | $2.21 | — | $7,961.99 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,662.18 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 117 | $11.13 | $2.34 | — | $5,357.63 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 528 | $2.47 | $6.81 | — | $4,046.66 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 676 | $1.93 | $8.72 | — | $2,733.26 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,477.08 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 989 | $1.32 | $12.76 | — | $158.85 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.85 | ▲ close $10,673.53 vs 09:30 $10,475.50 (session +261.93) | 16:00 close · cash $158.85 · equity $10,673.53 vs 09:30 $10,475.50 (+198.03; session marks +261.93) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×75 09:30 $17.20 → close $16.65 -41.25; AEM×6 09:30 $216.30 → close $216.06 -1.44; ARCT×117 09:30 $11.13 → close $13.45 +271.44; AUTL×528 09:30 $2.47 → close $2.41 -31.68; CRDL×676 09:30 $1.93 → close $1.86 -47.32; CRSP×21 09:30 $59.72 → close $59.50 -4.62; CYPH×989 09:30 $1.32 → close $1.42 +98.90 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.85 | ▲ 09:30 equity $11,050.19 vs yday $10,673.53 (+376.66) | 09:30 open · cash $158.85 (unchanged overnight, no fees) · equity $11,050.19 vs prior close $10,673.53 (+376.66) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×75 yday $16.65 → 09:30 $16.57 -6.00; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ARCT×117 yday $13.45 → 09:30 $13.33 -14.04; AUTL×528 yday $2.41 → 09:30 $2.40 -5.28; CRDL×676 yday $1.86 → 09:30 $1.88 +13.52; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; CYPH×989 yday $1.42 → 09:30 $1.83 +405.49 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,361.91 | ▲ +6.74 after sell → book $11,048.15; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 75 | $16.57 | $2.24 | $-51.70 | $2,602.42 | ▼ -51.70 after sell → book $11,045.91; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,902.57 | ▲ +0.34 after sell → book $11,043.88; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 117 | $13.33 | $2.37 | $+252.69 | $5,459.81 | ▲ +252.69 after sell → book $11,041.51; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 528 | $2.40 | $6.91 | $-50.68 | $6,720.10 | ▼ -50.68 after sell → book $11,034.60; vs 09:30 mark -6.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 676 | $1.88 | $8.84 | $-51.36 | $7,982.14 | ▼ -51.36 after sell → book $11,025.76; vs 09:30 mark -8.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $9,213.81 | ▼ -24.50 after sell → book $11,023.68; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 989 | $1.83 | $12.94 | $+478.70 | $11,010.75 | ▲ +478.70 after sell → book $11,010.75; vs 09:30 mark -12.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,010.75 | ▲ close $11,010.75 vs 09:30 $11,050.19 (session +0.00) | 16:00 close · cash $11,010.75 · no lots left · equity $11,010.75. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,010.75 | ▲ 09:30 equity $11,010.75 vs yday $11,010.75 (-0.00) | 09:30 open · cash $11,010.75 · no holdings · equity $11,010.75 vs prior close $11,010.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 57 | $23.77 | $2.16 | — | $9,653.69 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ⚪; ret5=+13.0; leftover $1376.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 125 | $10.98 | $2.37 | — | $8,278.83 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+1.2; leftover $1376.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.19 | $2.06 | — | $6,930.59 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+7.4; leftover $1376.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 164 | $8.35 | $2.48 | — | $5,558.71 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1376.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 278 | $4.94 | $3.59 | — | $4,181.81 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+7.1; leftover $1376.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $2,898.90 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+6.0; leftover $1376.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 123 | $11.12 | $2.36 | — | $1,528.78 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; ret5=-0.7; leftover $1376.34 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 101 | $13.59 | $2.29 | — | $153.89 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1376.34 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $153.89 | ▲ close $11,062.84 vs 09:30 $11,010.75 (session +71.40) | 16:00 close · cash $153.89 · equity $11,062.84 vs 09:30 $11,010.75 (+52.09; session marks +71.40) · 8 name(s) marked open→close (per-name table). MOS×57 09:30 $23.77 → close $24.27 +28.50; OCUL×125 09:30 $10.98 → close $10.88 -12.50; INSP×22 09:30 $61.19 → close $61.07 -2.64; CRMD×164 09:30 $8.35 → close $8.56 +34.44; RZLT×278 09:30 $4.94 → close $5.01 +19.46; HCA×3 09:30 $426.97 → close $428.76 +5.37; VITL×123 09:30 $11.12 → close $11.11 -1.23; KURA×101 09:30 $13.59 → close $13.59 +0.00 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $153.89 | ▼ 09:30 equity $11,059.06 vs yday $11,062.84 (-3.78) | 09:30 open · cash $153.89 (unchanged overnight, no fees) · equity $11,059.06 vs prior close $11,062.84 (-3.78) · 8 name(s) re-marked at the open (per-name table). MOS×57 yday $24.27 → 09:30 $24.84 +32.49; OCUL×125 yday $10.88 → 09:30 $10.79 -11.25; INSP×22 yday $61.07 → 09:30 $60.07 -22.00; CRMD×164 yday $8.56 → 09:30 $8.60 +6.56; RZLT×278 yday $5.01 → 09:30 $5.01 +0.00; HCA×3 yday $428.76 → 09:30 $427.50 -3.78; VITL×123 yday $11.11 → 09:30 $11.03 -9.84; KURA×101 yday $13.59 → 09:30 $13.63 +4.04 | — |
| 2026-08-26 09:30 ET | **SELL** | `VITL` | 123 | $11.03 | $2.39 | $-15.82 | $1,508.19 | ▼ -15.82 after sell → book $11,056.67; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 101 | $13.63 | $2.32 | $-0.57 | $2,882.50 | ▼ -0.57 after sell → book $11,054.35; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 46 | $31.21 | $2.13 | — | $1,444.72 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1441.25 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 146 | $9.83 | $2.43 | — | $7.11 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1441.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.11 | ▼ close $11,008.96 vs 09:30 $11,059.06 (session -40.84) | 16:00 close · cash $7.11 · equity $11,008.96 vs 09:30 $11,059.06 (-50.10; session marks -40.84) · 8 name(s) marked open→close (per-name table). MOS×57 09:30 $24.84 → close $24.16 -38.76; OCUL×125 09:30 $10.79 → close $10.77 -2.50; INSP×22 09:30 $60.07 → close $61.80 +38.06; CRMD×164 09:30 $8.60 → close $8.39 -34.44; RZLT×278 09:30 $5.01 → close $5.04 +8.34; HCA×3 09:30 $427.50 → close $427.16 -1.02; AVBP×46 09:30 $31.21 → close $31.14 -3.22; ABX×146 09:30 $9.83 → close $9.78 -7.30 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.11 | ▼ 09:30 equity $10,975.33 vs yday $11,008.96 (-33.63) | 09:30 open · cash $7.11 (unchanged overnight, no fees) · equity $10,975.33 vs prior close $11,008.96 (-33.63) · 8 name(s) re-marked at the open (per-name table). MOS×57 yday $24.16 → 09:30 $24.00 -9.12; OCUL×125 yday $10.77 → 09:30 $10.63 -17.50; INSP×22 yday $61.80 → 09:30 $62.10 +6.60; CRMD×164 yday $8.39 → 09:30 $8.49 +16.40; RZLT×278 yday $5.04 → 09:30 $5.07 +8.34; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; AVBP×46 yday $31.14 → 09:30 $30.79 -16.10; ABX×146 yday $9.78 → 09:30 $9.68 -14.60 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 125 | $10.63 | $2.40 | $-48.51 | $1,333.46 | ▼ -48.51 after sell → book $10,972.93; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $62.10 | $2.08 | $+15.89 | $2,697.58 | ▲ +15.89 after sell → book $10,970.85; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 164 | $8.49 | $2.52 | $+17.96 | $4,087.42 | ▲ +17.96 after sell → book $10,968.33; vs 09:30 mark -2.52 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 278 | $5.07 | $3.64 | $+28.91 | $5,493.24 | ▲ +28.91 after sell → book $10,964.69; vs 09:30 mark -3.64 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-11.10 | $6,765.05 | ▼ -11.10 after sell → book $10,962.67; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 46 | $30.79 | $2.15 | $-23.60 | $8,179.24 | ▼ -23.60 after sell → book $10,960.52; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ABX` | 146 | $9.68 | $2.46 | $-26.79 | $9,590.06 | ▼ -26.79 after sell → book $10,958.06; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $41.44 | $2.09 | — | $8,220.45 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+3.1; leftover $1370.01 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 95 | $14.42 | $2.27 | — | $6,848.27 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+7.1; leftover $1370.01 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 526 | $2.60 | $6.79 | — | $5,473.89 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+13.0; leftover $1370.01 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 16 | $81.65 | $2.04 | — | $4,165.45 | — | union ∩ ab_g, no 🚨; gate ab=good; list mover_buy; 🔵; ret5=+2.0; leftover $1370.01 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 299 | $4.57 | $3.86 | — | $2,795.16 | — | union ∩ ab_g, no 🚨; gate ab=good; list mover_buy; 🔵; ret5=+1.1; leftover $1370.01 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 18 | $74.54 | $2.04 | — | $1,451.40 | — | union ∩ ab_g, no 🚨; gate ab=good; list mover_buy; 🔵; ret5=-0.1; leftover $1370.01 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $482.40 | — | union ∩ ab_g, no 🚨; gate ab=good; list mover_buy; 🔵; ret5=+0.1; leftover $1370.01 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $482.40 | ▲ close $10,960.25 vs 09:30 $10,975.33 (session +23.27) | 16:00 close · cash $482.40 · equity $10,960.25 vs 09:30 $10,975.33 (-15.08; session marks +23.27) · 8 name(s) marked open→close (per-name table). MOS×57 09:30 $24.00 → close $23.76 -13.68; RRC×33 09:30 $41.44 → close $41.64 +6.60; CRK×95 09:30 $14.42 → close $14.62 +19.00; SLI×526 09:30 $2.60 → close $2.64 +21.04; ACMR×16 09:30 $81.65 → close $80.49 -18.56; GGB×299 09:30 $4.57 → close $4.70 +38.87; MT×18 09:30 $74.54 → close $74.63 +1.62; MU×1 09:30 $967.01 → close $935.39 -31.62 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $482.40 | ▲ 09:30 equity $10,965.46 vs yday $10,960.25 (+5.21) | 09:30 open · cash $482.40 (unchanged overnight, no fees) · equity $10,965.46 vs prior close $10,960.25 (+5.21) · 8 name(s) re-marked at the open (per-name table). MOS×57 yday $23.76 → 09:30 $23.95 +10.83; RRC×33 yday $41.64 → 09:30 $41.74 +3.30; CRK×95 yday $14.62 → 09:30 $14.63 +0.95; SLI×526 yday $2.64 → 09:30 $2.68 +21.04; ACMR×16 yday $80.49 → 09:30 $79.27 -19.52; GGB×299 yday $4.70 → 09:30 $4.67 -8.97; MT×18 yday $74.63 → 09:30 $75.39 +13.68; MU×1 yday $935.39 → 09:30 $919.29 -16.10 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 16 | $79.27 | $2.06 | $-42.18 | $1,748.66 | ▼ -42.18 after sell → book $10,963.40; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 299 | $4.67 | $3.92 | $+22.12 | $3,141.07 | ▲ +22.12 after sell → book $10,959.48; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 18 | $75.39 | $2.06 | $+11.19 | $4,496.03 | ▲ +11.19 after sell → book $10,957.42; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $5,413.30 | ▼ -51.73 after sell → book $10,955.40; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 86 | $15.66 | $2.25 | — | $4,064.29 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1353.33 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 17 | $79.42 | $2.04 | — | $2,712.11 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1353.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $1,448.91 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1353.33 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $132.26 | — | union ∩ ab_g, no 🚨; gate ab=good; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1353.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.26 | ▼ close $10,727.11 vs 09:30 $10,965.46 (session -219.98) | 16:00 close · cash $132.26 · equity $10,727.11 vs 09:30 $10,965.46 (-238.35; session marks -219.98) · 8 name(s) marked open→close (per-name table). MOS×57 09:30 $23.95 → close $23.60 -19.95; RRC×33 09:30 $41.74 → close $41.46 -9.24; CRK×95 09:30 $14.63 → close $14.29 -32.30; SLI×526 09:30 $2.68 → close $2.55 -68.38; GRRR×86 09:30 $15.66 → close $14.41 -107.50; URBN×17 09:30 $79.42 → close $81.09 +28.39; SIMO×5 09:30 $252.24 → close $245.81 -32.15; ANF×9 09:30 $146.07 → close $148.42 +21.15 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.26 | ▲ 09:30 equity $10,783.24 vs yday $10,727.11 (+56.13) | 09:30 open · cash $132.26 (unchanged overnight, no fees) · equity $10,783.24 vs prior close $10,727.11 (+56.13) · 8 name(s) re-marked at the open (per-name table). MOS×57 yday $23.60 → 09:30 $23.68 +4.56; RRC×33 yday $41.46 → 09:30 $42.00 +17.82; CRK×95 yday $14.29 → 09:30 $14.54 +23.75; SLI×526 yday $2.55 → 09:30 $2.58 +15.78; GRRR×86 yday $14.41 → 09:30 $14.44 +2.58; URBN×17 yday $81.09 → 09:30 $80.44 -11.05; SIMO×5 yday $245.81 → 09:30 $247.05 +6.20; ANF×9 yday $148.42 → 09:30 $148.03 -3.51 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 57 | $23.68 | $2.18 | $-9.47 | $1,479.84 | ▼ -9.47 after sell → book $10,781.06; vs 09:30 mark -2.18 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 33 | $42.00 | $2.11 | $+14.28 | $2,863.73 | ▲ +14.28 after sell → book $10,778.95; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 95 | $14.54 | $2.30 | $+6.82 | $4,242.73 | ▲ +6.82 after sell → book $10,776.65; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 526 | $2.58 | $6.88 | $-24.19 | $5,592.92 | ▼ -24.19 after sell → book $10,769.76; vs 09:30 mark -6.89 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 86 | $14.44 | $2.27 | $-109.44 | $6,832.49 | ▼ -109.44 after sell → book $10,767.49; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 17 | $80.44 | $2.06 | $+13.24 | $8,197.91 | ▲ +13.24 after sell → book $10,765.43; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $9,431.14 | ▼ -29.98 after sell → book $10,763.41; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $10,761.37 | ▲ +13.59 after sell → book $10,761.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,761.37 | ▲ close $10,761.37 vs 09:30 $10,783.24 (session +0.00) | 16:00 close · cash $10,761.37 · no lots left · equity $10,761.37. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,761.37 | ▲ 09:30 equity $10,761.37 vs yday $10,761.37 (-0.00) | 09:30 open · cash $10,761.37 · no holdings · equity $10,761.37 vs prior close $10,761.37 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,761.37 | ▲ close $10,761.37 vs 09:30 $10,761.37 (session +0.00) | 16:00 close · cash $10,761.37 · no lots left · equity $10,761.37. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,761.37 | ▲ 09:30 equity $10,761.37 vs yday $10,761.37 (-0.00) | 09:30 open · cash $10,761.37 · no holdings · equity $10,761.37 vs prior close $10,761.37 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,761.37 | ▲ close $10,761.37 vs 09:30 $10,761.37 (session +0.00) | 16:00 close · cash $10,761.37 · no lots left · equity $10,761.37. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,761.37 | ▲ 09:30 equity $10,761.37 vs yday $10,761.37 (-0.00) | 09:30 open · cash $10,761.37 · no holdings · equity $10,761.37 vs prior close $10,761.37 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,437.30 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1345.17 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $42.93 | $2.08 | — | $8,104.39 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1345.17 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 370 | $3.63 | $4.77 | — | $6,756.52 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1345.17 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 167 | $8.03 | $2.49 | — | $5,413.02 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1345.17 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,086.50 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1345.17 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 87 | $15.45 | $2.25 | — | $2,740.09 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1345.17 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,424.57 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1345.17 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 80 | $16.77 | $2.23 | — | $80.74 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1345.17 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $80.74 | ▼ close $10,501.74 vs 09:30 $10,761.37 (session -239.69) | 16:00 close · cash $80.74 · equity $10,501.74 vs 09:30 $10,761.37 (-259.63; session marks -239.69) · 8 name(s) marked open→close (per-name table). ATRC×25 09:30 $52.88 → close $52.46 -10.50; HRMY×31 09:30 $42.93 → close $41.86 -33.17; CABA×370 09:30 $3.63 → close $3.48 -55.50; VSTM×167 09:30 $8.03 → close $7.98 -8.35; RVTY×10 09:30 $132.45 → close $130.63 -18.20; CRK×87 09:30 $15.45 → close $14.95 -43.50; MRNA×9 09:30 $145.94 → close $148.87 +26.33; ARCT×80 09:30 $16.77 → close $15.56 -96.80 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.74 | ▲ 09:30 equity $10,505.84 vs yday $10,501.74 (+4.10) | 09:30 open · cash $80.74 (unchanged overnight, no fees) · equity $10,505.84 vs prior close $10,501.74 (+4.10) · 8 name(s) re-marked at the open (per-name table). ATRC×25 yday $52.46 → 09:30 $52.03 -10.75; HRMY×31 yday $41.86 → 09:30 $41.50 -11.16; CABA×370 yday $3.48 → 09:30 $3.46 -7.40; VSTM×167 yday $7.98 → 09:30 $7.91 -11.69; RVTY×10 yday $130.63 → 09:30 $130.03 -6.00; CRK×87 yday $14.95 → 09:30 $15.00 +4.35; MRNA×9 yday $148.87 → 09:30 $153.62 +42.75; ARCT×80 yday $15.56 → 09:30 $15.61 +4.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 31 | $41.50 | $2.10 | $-48.52 | $1,365.14 | ▼ -48.52 after sell → book $10,503.74; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 167 | $7.91 | $2.53 | $-25.06 | $2,683.58 | ▼ -25.06 after sell → book $10,501.21; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $3,981.84 | ▼ -28.26 after sell → book $10,499.17; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 87 | $15.00 | $2.28 | $-43.68 | $5,284.56 | ▼ -43.68 after sell → book $10,496.89; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $6,665.11 | ▲ +65.02 after sell → book $10,494.86; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 80 | $15.61 | $2.25 | $-97.28 | $7,911.65 | ▼ -97.28 after sell → book $10,492.60; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 523 | $2.52 | $6.75 | — | $6,586.95 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1318.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 196 | $6.71 | $2.58 | — | $5,269.21 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1318.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 694 | $1.90 | $8.95 | — | $3,941.65 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1318.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 275 | $4.78 | $3.55 | — | $2,623.61 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1318.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 829 | $1.59 | $10.69 | — | $1,294.80 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1318.61 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 114 | $11.31 | $2.33 | — | $3.13 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1318.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.13 | ▼ close $10,404.39 vs 09:30 $10,505.84 (session -53.36) | 16:00 close · cash $3.13 · equity $10,404.39 vs 09:30 $10,505.84 (-101.45; session marks -53.36) · 8 name(s) marked open→close (per-name table). ATRC×25 09:30 $52.03 → close $51.52 -12.75; CABA×370 09:30 $3.46 → close $3.47 +3.70; ALEC×523 09:30 $2.52 → close $2.46 -31.38; BHC×196 09:30 $6.71 → close $6.56 -29.40; BMEA×694 09:30 $1.90 → close $2.03 +90.22; OABI×275 09:30 $4.78 → close $4.33 -123.75; OPK×829 09:30 $1.59 → close $1.64 +41.45; VIR×114 09:30 $11.31 → close $11.38 +8.55 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.13 | ▼ 09:30 equity $10,363.29 vs yday $10,404.39 (-41.10) | 09:30 open · cash $3.13 (unchanged overnight, no fees) · equity $10,363.29 vs prior close $10,404.39 (-41.10) · 8 name(s) re-marked at the open (per-name table). ATRC×25 yday $51.52 → 09:30 $54.31 +69.75; CABA×370 yday $3.47 → 09:30 $3.43 -14.80; ALEC×523 yday $2.46 → 09:30 $2.38 -41.84; BHC×196 yday $6.56 → 09:30 $6.57 +1.96; BMEA×694 yday $2.03 → 09:30 $2.00 -20.82; OABI×275 yday $4.33 → 09:30 $4.30 -8.25; OPK×829 yday $1.64 → 09:30 $1.63 -8.29; VIR×114 yday $11.38 → 09:30 $11.22 -18.81 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 25 | $54.31 | $2.09 | $+31.60 | $1,358.80 | ▲ +31.60 after sell → book $10,361.21; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 370 | $3.43 | $4.84 | $-83.62 | $2,623.05 | ▼ -83.62 after sell → book $10,356.36; vs 09:30 mark -4.85 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 523 | $2.38 | $6.84 | $-86.81 | $3,860.95 | ▼ -86.81 after sell → book $10,349.52; vs 09:30 mark -6.84 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 196 | $6.57 | $2.62 | $-32.64 | $5,146.05 | ▼ -32.64 after sell → book $10,346.90; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 694 | $2.00 | $9.08 | $+51.37 | $6,524.97 | ▲ +51.37 after sell → book $10,337.82; vs 09:30 mark -9.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 275 | $4.30 | $3.60 | $-139.15 | $7,703.86 | ▼ -139.15 after sell → book $10,334.21; vs 09:30 mark -3.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 829 | $1.63 | $10.84 | $+11.62 | $9,044.29 | ▲ +11.62 after sell → book $10,323.37; vs 09:30 mark -10.84 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 114 | $11.22 | $2.36 | $-14.95 | $10,321.01 | ▼ -14.95 after sell → book $10,321.01; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,321.01 | ▲ close $10,321.01 vs 09:30 $10,363.29 (session +0.00) | 16:00 close · cash $10,321.01 · no lots left · equity $10,321.01. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,321.01 | ▲ 09:30 equity $10,321.01 vs yday $10,321.01 (+0.00) | 09:30 open · cash $10,321.01 · no holdings · equity $10,321.01 vs prior close $10,321.01 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,321.01 | ▲ close $10,321.01 vs 09:30 $10,321.01 (session +0.00) | 16:00 close · cash $10,321.01 · no lots left · equity $10,321.01. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,321.01 | ▲ 09:30 equity $10,321.01 vs yday $10,321.01 (+0.00) | 09:30 open · cash $10,321.01 · no holdings · equity $10,321.01 vs prior close $10,321.01 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,321.01 | ▲ close $10,321.01 vs 09:30 $10,321.01 (session +0.00) | 16:00 close · cash $10,321.01 · no lots left · equity $10,321.01. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,321.01 | ▲ 09:30 equity $10,321.01 vs yday $10,321.01 (+0.00) | 09:30 open · cash $10,321.01 · no holdings · equity $10,321.01 vs prior close $10,321.01 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 79 | $16.28 | $2.23 | — | $9,032.66 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=-1.1; leftover $1290.13 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 472 | $2.73 | $6.09 | — | $7,738.01 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+3.0; leftover $1290.13 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $6,494.97 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+4.4; leftover $1290.13 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $5,341.95 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,earn_react; 🔵; ⚪; ret5=+9.0; leftover $1290.13 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $4,077.69 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+7.8; leftover $1290.13 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 23 | $56.09 | $2.06 | — | $2,785.56 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+15.3; leftover $1290.13 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 412 | $3.13 | $5.31 | — | $1,490.69 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+6.2; leftover $1290.13 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 632 | $2.04 | $8.15 | — | $193.26 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.8; leftover $1290.13 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $193.26 | ▲ close $10,409.06 vs 09:30 $10,321.01 (session +117.92) | 16:00 close · cash $193.26 · equity $10,409.06 vs 09:30 $10,321.01 (+88.05; session marks +117.92) · 8 name(s) marked open→close (per-name table). AUPH×79 09:30 $16.28 → close $16.10 -14.22; OVID×472 09:30 $2.73 → close $2.69 -18.88; SANM×6 09:30 $206.84 → close $216.00 +54.96; ORCL×7 09:30 $164.43 → close $150.28 -99.05; NVT×8 09:30 $157.78 → close $162.38 +36.80; COHU×23 09:30 $56.09 → close $57.08 +22.77; CMRC×412 09:30 $3.13 → close $3.50 +154.50; AMTX×632 09:30 $2.04 → close $2.01 -18.96 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `QSI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CNTB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HAS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XHLD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NET` | hard_red | hard-red S=-13.28 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `AUPH` | 79 | 2026-09-11 @ $16.28 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=-1.1; leftover $1290.13 |
| `OVID` | 472 | 2026-09-11 @ $2.73 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+3.0; leftover $1290.13 |
| `SANM` | 6 | 2026-09-11 @ $206.84 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+4.4; leftover $1290.13 |
| `ORCL` | 7 | 2026-09-11 @ $164.43 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,earn_react; 🔵; ⚪; ret5=+9.0; leftover $1290.13 |
| `NVT` | 8 | 2026-09-11 @ $157.78 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+7.8; leftover $1290.13 |
| `COHU` | 23 | 2026-09-11 @ $56.09 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+15.3; leftover $1290.13 |
| `CMRC` | 412 | 2026-09-11 @ $3.13 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+6.2; leftover $1290.13 |
| `AMTX` | 632 | 2026-09-11 @ $2.04 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.8; leftover $1290.13 |
