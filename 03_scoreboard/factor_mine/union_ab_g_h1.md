# Factor mine action — `union_ab_g_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ ab_g, no 🚨

Cash book **+0.53%** ($10,053) · signal-only (no cash/fees) was -0.92%. Starts YES **7/26**. Fills 150 · skips 71 · realized $+65.78.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $0.59.

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
| 2026-08-27 | `AVBP` | 46 | $31.14 | $30.79 | -16.10 | $31.00 | +9.66 | -6.44 | -19.32 | -9.66 |
| 2026-08-27 | `ABX` | 146 | $9.78 | $9.68 | -14.60 | $9.83 | +21.90 | +7.30 | -21.90 | +0.00 |
| 2026-08-27 | `RRC` | 32 | — | $41.44 | +0.00 | $41.64 | +6.40 | +6.40 | +0.00 | +6.40 |
| 2026-08-27 | `CRK` | 93 | — | $14.42 | +0.00 | $14.62 | +18.60 | +18.60 | +0.00 | +18.60 |
| 2026-08-27 | `SLI` | 520 | — | $2.60 | +0.00 | $2.64 | +20.80 | +20.80 | +0.00 | +20.80 |
| 2026-08-27 | `KURA` | 104 | — | $12.98 | +0.00 | $13.18 | +20.80 | +20.80 | +0.00 | +20.80 |
| 2026-08-27 | `ITG` | 109 | — | $12.36 | +0.00 | $12.87 | +55.59 | +55.59 | +0.00 | +55.59 |
| 2026-08-28 | `MOS` | 57 | $23.76 | $23.95 | +10.83 | $23.60 | -19.95 | -9.12 | +10.26 | -9.69 |
| 2026-08-28 | `AVBP` | 46 | $31.00 | $30.53 | -21.62 | — | +0.00 | -21.62 | -31.28 | — |
| 2026-08-28 | `ABX` | 146 | $9.83 | $9.88 | +7.30 | — | +0.00 | +7.30 | +7.30 | — |
| 2026-08-28 | `RRC` | 32 | $41.64 | $41.74 | +3.20 | $41.46 | -8.96 | -5.76 | +9.60 | +0.64 |
| 2026-08-28 | `CRK` | 93 | $14.62 | $14.63 | +0.93 | $14.29 | -31.62 | -30.69 | +19.53 | -12.09 |
| 2026-08-28 | `SLI` | 520 | $2.64 | $2.68 | +20.80 | $2.55 | -67.60 | -46.80 | +41.60 | -26.00 |
| 2026-08-28 | `KURA` | 104 | $13.18 | $13.05 | -13.52 | — | +0.00 | -13.52 | +7.28 | — |
| 2026-08-28 | `ITG` | 109 | $12.87 | $12.79 | -8.72 | — | +0.00 | -8.72 | +46.87 | — |
| 2026-08-28 | `GRRR` | 89 | — | $15.66 | +0.00 | $14.41 | -111.25 | -111.25 | +0.00 | -111.25 |
| 2026-08-28 | `URBN` | 17 | — | $79.42 | +0.00 | $81.09 | +28.39 | +28.39 | +0.00 | +28.39 |
| 2026-08-28 | `SIMO` | 5 | — | $252.24 | +0.00 | $245.81 | -32.15 | -32.15 | +0.00 | -32.15 |
| 2026-08-28 | `ANF` | 9 | — | $146.07 | +0.00 | $148.42 | +21.15 | +21.15 | +0.00 | +21.15 |
| 2026-08-31 | `MOS` | 57 | $23.60 | $23.68 | +4.56 | — | +0.00 | +4.56 | -5.13 | — |
| 2026-08-31 | `RRC` | 32 | $41.46 | $42.00 | +17.28 | — | +0.00 | +17.28 | +17.92 | — |
| 2026-08-31 | `CRK` | 93 | $14.29 | $14.54 | +23.25 | — | +0.00 | +23.25 | +11.16 | — |
| 2026-08-31 | `SLI` | 520 | $2.55 | $2.58 | +15.60 | — | +0.00 | +15.60 | -10.40 | — |
| 2026-08-31 | `GRRR` | 89 | $14.41 | $14.44 | +2.67 | — | +0.00 | +2.67 | -108.58 | — |
| 2026-08-31 | `URBN` | 17 | $81.09 | $80.44 | -11.05 | — | +0.00 | -11.05 | +17.34 | — |
| 2026-08-31 | `SIMO` | 5 | $245.81 | $247.05 | +6.20 | — | +0.00 | +6.20 | -25.95 | — |
| 2026-08-31 | `ANF` | 9 | $148.42 | $148.03 | -3.51 | — | +0.00 | -3.51 | +17.64 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 25 | — | $52.88 | +0.00 | $52.46 | -10.50 | -10.50 | +0.00 | -10.50 |
| 2026-09-03 | `HRMY` | 31 | — | $42.93 | +0.00 | $41.86 | -33.17 | -33.17 | +0.00 | -33.17 |
| 2026-09-03 | `CABA` | 374 | — | $3.63 | +0.00 | $3.48 | -56.10 | -56.10 | +0.00 | -56.10 |
| 2026-09-03 | `VSTM` | 169 | — | $8.03 | +0.00 | $7.98 | -8.45 | -8.45 | +0.00 | -8.45 |
| 2026-09-03 | `RVTY` | 10 | — | $132.45 | +0.00 | $130.63 | -18.20 | -18.20 | +0.00 | -18.20 |
| 2026-09-03 | `CRK` | 88 | — | $15.45 | +0.00 | $14.95 | -44.00 | -44.00 | +0.00 | -44.00 |
| 2026-09-03 | `MRNA` | 9 | — | $145.94 | +0.00 | $148.87 | +26.33 | +26.33 | +0.00 | +26.33 |
| 2026-09-03 | `ARCT` | 81 | — | $16.77 | +0.00 | $15.56 | -98.01 | -98.01 | +0.00 | -98.01 |
| 2026-09-04 | `ATRC` | 25 | $52.46 | $52.03 | -10.75 | $51.52 | -12.75 | -23.50 | -21.25 | -34.00 |
| 2026-09-04 | `HRMY` | 31 | $41.86 | $41.50 | -11.16 | — | +0.00 | -11.16 | -44.33 | — |
| 2026-09-04 | `CABA` | 374 | $3.48 | $3.46 | -7.48 | $3.47 | +3.74 | -3.74 | -63.58 | -59.84 |
| 2026-09-04 | `VSTM` | 169 | $7.98 | $7.91 | -11.83 | — | +0.00 | -11.83 | -20.28 | — |
| 2026-09-04 | `RVTY` | 10 | $130.63 | $130.03 | -6.00 | — | +0.00 | -6.00 | -24.20 | — |
| 2026-09-04 | `CRK` | 88 | $14.95 | $15.00 | +4.40 | — | +0.00 | +4.40 | -39.60 | — |
| 2026-09-04 | `MRNA` | 9 | $148.87 | $153.62 | +42.75 | — | +0.00 | +42.75 | +69.08 | — |
| 2026-09-04 | `ARCT` | 81 | $15.56 | $15.61 | +4.05 | — | +0.00 | +4.05 | -93.96 | — |
| 2026-09-04 | `ALEC` | 530 | — | $2.52 | +0.00 | $2.46 | -31.80 | -31.80 | +0.00 | -31.80 |
| 2026-09-04 | `BHC` | 199 | — | $6.71 | +0.00 | $6.56 | -29.85 | -29.85 | +0.00 | -29.85 |
| 2026-09-04 | `BMEA` | 702 | — | $1.90 | +0.00 | $2.03 | +91.26 | +91.26 | +0.00 | +91.26 |
| 2026-09-04 | `OABI` | 279 | — | $4.78 | +0.00 | $4.33 | -125.55 | -125.55 | +0.00 | -125.55 |
| 2026-09-04 | `OPK` | 840 | — | $1.59 | +0.00 | $1.64 | +42.00 | +42.00 | +0.00 | +42.00 |
| 2026-09-04 | `VIR` | 115 | — | $11.31 | +0.00 | $11.38 | +8.62 | +8.62 | +0.00 | +8.62 |
| 2026-09-08 | `ATRC` | 25 | $51.52 | $54.31 | +69.75 | — | +0.00 | +69.75 | +35.75 | — |
| 2026-09-08 | `CABA` | 374 | $3.47 | $3.43 | -14.96 | — | +0.00 | -14.96 | -74.80 | — |
| 2026-09-08 | `ALEC` | 530 | $2.46 | $2.38 | -42.40 | — | +0.00 | -42.40 | -74.20 | — |
| 2026-09-08 | `BHC` | 199 | $6.56 | $6.57 | +1.99 | — | +0.00 | +1.99 | -27.86 | — |
| 2026-09-08 | `BMEA` | 702 | $2.03 | $2.00 | -21.06 | — | +0.00 | -21.06 | +70.20 | — |
| 2026-09-08 | `OABI` | 279 | $4.33 | $4.30 | -8.37 | — | +0.00 | -8.37 | -133.92 | — |
| 2026-09-08 | `OPK` | 840 | $1.64 | $1.63 | -8.40 | — | +0.00 | -8.40 | +33.60 | — |
| 2026-09-08 | `VIR` | 115 | $11.38 | $11.22 | -18.97 | — | +0.00 | -18.97 | -10.35 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `AUPH` | 80 | — | $16.28 | +0.00 | $16.10 | -14.40 | -14.40 | +0.00 | -14.40 |
| 2026-09-11 | `OVID` | 477 | — | $2.73 | +0.00 | $2.69 | -19.08 | -19.08 | +0.00 | -19.08 |
| 2026-09-11 | `SANM` | 6 | — | $206.84 | +0.00 | $216.00 | +54.96 | +54.96 | +0.00 | +54.96 |
| 2026-09-11 | `ORCL` | 7 | — | $164.43 | +0.00 | $150.28 | -99.05 | -99.05 | +0.00 | -99.05 |
| 2026-09-11 | `NVT` | 8 | — | $157.78 | +0.00 | $162.38 | +36.80 | +36.80 | +0.00 | +36.80 |
| 2026-09-11 | `COHU` | 23 | — | $56.09 | +0.00 | $57.08 | +22.77 | +22.77 | +0.00 | +22.77 |
| 2026-09-11 | `AMTX` | 639 | — | $2.04 | +0.00 | $2.01 | -19.17 | -19.17 | +0.00 | -19.17 |
| 2026-09-11 | `CLOV` | 274 | — | $4.75 | +0.00 | $4.82 | +19.18 | +19.18 | +0.00 | +19.18 |
| 2026-09-14 | `AUPH` | 80 | $16.10 | $16.03 | -5.60 | — | +0.00 | -5.60 | -20.00 | — |
| 2026-09-14 | `OVID` | 477 | $2.69 | $2.75 | +31.00 | — | +0.00 | +31.00 | +11.92 | — |
| 2026-09-14 | `SANM` | 6 | $216.00 | $206.50 | -57.00 | — | +0.00 | -57.00 | -2.04 | — |
| 2026-09-14 | `ORCL` | 7 | $150.28 | $141.42 | -62.02 | — | +0.00 | -62.02 | -161.07 | — |
| 2026-09-14 | `NVT` | 8 | $162.38 | $150.00 | -99.04 | $146.64 | -26.88 | -125.92 | -62.24 | -89.12 |
| 2026-09-14 | `COHU` | 23 | $57.08 | $52.23 | -111.55 | — | +0.00 | -111.55 | -88.78 | — |
| 2026-09-14 | `AMTX` | 639 | $2.01 | $2.01 | +0.00 | — | +0.00 | +0.00 | -19.17 | — |
| 2026-09-14 | `CLOV` | 274 | $4.82 | $4.82 | +0.00 | — | +0.00 | +0.00 | +19.18 | — |
| 2026-09-15 | `NVT` | 8 | $146.64 | $151.12 | +35.84 | — | +0.00 | +35.84 | -53.28 | — |
| 2026-09-16 | `IQV` | 4 | — | $270.89 | +0.00 | $268.82 | -8.28 | -8.28 | +0.00 | -8.28 |
| 2026-09-16 | `RDNT` | 16 | — | $77.12 | +0.00 | $75.78 | -21.44 | -21.44 | +0.00 | -21.44 |
| 2026-09-16 | `AVAH` | 87 | — | $14.31 | +0.00 | $14.26 | -4.35 | -4.35 | +0.00 | -4.35 |
| 2026-09-16 | `BLFS` | 34 | — | $36.46 | +0.00 | $36.11 | -11.90 | -11.90 | +0.00 | -11.90 |
| 2026-09-16 | `TEM` | 18 | — | $68.79 | +0.00 | $69.97 | +21.24 | +21.24 | +0.00 | +21.24 |
| 2026-09-16 | `RIG` | 214 | — | $5.87 | +0.00 | $5.54 | -70.62 | -70.62 | +0.00 | -70.62 |
| 2026-09-16 | `VAL` | 14 | — | $87.40 | +0.00 | $82.52 | -68.32 | -68.32 | +0.00 | -68.32 |
| 2026-09-16 | `KRMN` | 33 | — | $38.01 | +0.00 | $36.94 | -35.31 | -35.31 | +0.00 | -35.31 |
| 2026-09-17 | `IQV` | 4 | $268.82 | $273.15 | +17.32 | — | +0.00 | +17.32 | +9.04 | — |
| 2026-09-17 | `RDNT` | 16 | $75.78 | $76.44 | +10.56 | — | +0.00 | +10.56 | -10.88 | — |
| 2026-09-17 | `AVAH` | 87 | $14.26 | $14.33 | +6.09 | — | +0.00 | +6.09 | +1.74 | — |
| 2026-09-17 | `BLFS` | 34 | $36.11 | $36.67 | +19.04 | — | +0.00 | +19.04 | +7.14 | — |
| 2026-09-17 | `TEM` | 18 | $69.97 | $72.70 | +49.14 | — | +0.00 | +49.14 | +70.38 | — |
| 2026-09-17 | `RIG` | 214 | $5.54 | $5.58 | +8.56 | — | +0.00 | +8.56 | -62.06 | — |
| 2026-09-17 | `VAL` | 14 | $82.52 | $83.20 | +9.52 | — | +0.00 | +9.52 | -58.80 | — |
| 2026-09-17 | `KRMN` | 33 | $36.94 | $37.89 | +31.35 | — | +0.00 | +31.35 | -3.96 | — |
| 2026-09-17 | `ILMN` | 5 | — | $233.85 | +0.00 | $245.18 | +56.65 | +56.65 | +0.00 | +56.65 |
| 2026-09-17 | `TWST` | 8 | — | $151.43 | +0.00 | $155.56 | +33.04 | +33.04 | +0.00 | +33.04 |
| 2026-09-17 | `RVTY` | 8 | — | $147.61 | +0.00 | $146.73 | -7.04 | -7.04 | +0.00 | -7.04 |
| 2026-09-17 | `IOVA` | 121 | — | $10.25 | +0.00 | $10.02 | -27.83 | -27.83 | +0.00 | -27.83 |
| 2026-09-17 | `PGEN` | 164 | — | $7.59 | +0.00 | $7.87 | +45.92 | +45.92 | +0.00 | +45.92 |
| 2026-09-17 | `AMN` | 35 | — | $34.93 | +0.00 | $34.55 | -13.30 | -13.30 | +0.00 | -13.30 |
| 2026-09-17 | `AXTI` | 18 | — | $67.91 | +0.00 | $67.75 | -2.88 | -2.88 | +0.00 | -2.88 |
| 2026-09-17 | `ARQT` | 48 | — | $25.95 | +0.00 | $26.46 | +24.48 | +24.48 | +0.00 | +24.48 |
| 2026-09-18 | `ILMN` | 5 | $245.18 | $249.13 | +19.75 | $239.62 | -47.55 | -27.80 | +76.40 | +28.85 |
| 2026-09-18 | `TWST` | 8 | $155.56 | $158.04 | +19.84 | — | +0.00 | +19.84 | +52.88 | — |
| 2026-09-18 | `RVTY` | 8 | $146.73 | $146.50 | -1.84 | — | +0.00 | -1.84 | -8.88 | — |
| 2026-09-18 | `IOVA` | 121 | $10.02 | $10.12 | +12.10 | — | +0.00 | +12.10 | -15.73 | — |
| 2026-09-18 | `PGEN` | 164 | $7.87 | $7.98 | +18.04 | — | +0.00 | +18.04 | +63.96 | — |
| 2026-09-18 | `AMN` | 35 | $34.55 | $34.52 | -1.05 | — | +0.00 | -1.05 | -14.35 | — |
| 2026-09-18 | `AXTI` | 18 | $67.75 | $69.72 | +35.46 | — | +0.00 | +35.46 | +32.58 | — |
| 2026-09-18 | `ARQT` | 48 | $26.46 | $26.14 | -15.36 | $25.38 | -36.48 | -51.84 | +9.12 | -27.36 |
| 2026-09-18 | `SDGR` | 43 | — | $29.32 | +0.00 | $29.02 | -12.90 | -12.90 | +0.00 | -12.90 |
| 2026-09-18 | `FTRE` | 63 | — | $20.10 | +0.00 | $19.93 | -10.71 | -10.71 | +0.00 | -10.71 |
| 2026-09-18 | `BNC` | 218 | — | $5.83 | +0.00 | $5.98 | +32.70 | +32.70 | +0.00 | +32.70 |
| 2026-09-18 | `DDD` | 355 | — | $3.58 | +0.00 | $3.63 | +17.75 | +17.75 | +0.00 | +17.75 |
| 2026-09-18 | `RANI` | 1499 | — | $0.85 | +0.00 | $0.86 | +17.99 | +17.99 | +0.00 | +17.99 |
| 2026-09-18 | `RARE` | 86 | — | $14.79 | +0.00 | $14.51 | -24.08 | -24.08 | +0.00 | -24.08 |

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
| 2026-08-27 | — | $7.11 | MOS×57, OCUL×125, INSP×22, CRMD×164, RZLT×278, HCA×3, AVBP×46, ABX×146 | $10,975.33 | -33.63 | +140.07 | RRC, CRK, SLI, KURA, ITG | OCUL, INSP, CRMD, RZLT, HCA | $33.07 | $11,087.06 | MOS×57, AVBP×46, ABX×146, RRC×32, CRK×93, SLI×520, KURA×104, ITG×109 |
| 2026-08-28 | +0.75 | $33.07 | MOS×57, AVBP×46, ABX×146, RRC×32, CRK×93, SLI×520, KURA×104, ITG×109 | $11,086.26 | -0.80 | -221.99 | GRRR, URBN, SIMO, ANF | AVBP, ABX, KURA, ITG | $293.92 | $10,846.66 | MOS×57, RRC×32, CRK×93, SLI×520, GRRR×89, URBN×17, SIMO×5, ANF×9 |
| 2026-08-31 | -5.85 | $293.92 | MOS×57, RRC×32, CRK×93, SLI×520, GRRR×89, URBN×17, SIMO×5, ANF×9 | $10,901.66 | +55.00 | +0.00 | — | MOS, RRC, CRK, SLI, GRRR, URBN, SIMO, ANF | $10,879.86 | $10,879.86 | — |
| 2026-09-01 | -6.30 | $10,879.86 | — | $10,879.86 | +0.00 | +0.00 | — | — | $10,879.86 | $10,879.86 | — |
| 2026-09-02 | -3.83 | $10,879.86 | — | $10,879.86 | +0.00 | +0.00 | — | — | $10,879.86 | $10,879.86 | — |
| 2026-09-03 | -0.90 | $10,879.86 | — | $10,879.86 | +0.00 | -242.10 | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MRNA, ARCT | — | $136.38 | $10,617.77 | ATRC×25, HRMY×31, CABA×374, VSTM×169, RVTY×10, CRK×88, MRNA×9, ARCT×81 |
| 2026-09-04 | +2.25 | $136.38 | ATRC×25, HRMY×31, CABA×374, VSTM×169, RVTY×10, CRK×88, MRNA×9, ARCT×81 | $10,621.75 | +3.98 | -54.33 | ALEC, BHC, BMEA, OABI, OPK, VIR | HRMY, VSTM, RVTY, CRK, MRNA, ARCT | $3.89 | $10,518.92 | ATRC×25, CABA×374, ALEC×530, BHC×199, BMEA×702, OABI×279, OPK×840, VIR×115 |
| 2026-09-08 | -11.47 | $3.89 | ATRC×25, CABA×374, ALEC×530, BHC×199, BMEA×702, OABI×279, OPK×840, VIR×115 | $10,476.49 | -42.43 | +0.00 | — | ATRC, CABA, ALEC, BHC, BMEA, OABI, OPK, VIR | $10,433.75 | $10,433.75 | — |
| 2026-09-09 | -13.95 | $10,433.75 | — | $10,433.75 | +0.00 | +0.00 | — | — | $10,433.75 | $10,433.75 | — |
| 2026-09-10 | -13.28 | $10,433.75 | — | $10,433.75 | +0.00 | +0.00 | — | — | $10,433.75 | $10,433.75 | — |
| 2026-09-11 | +0.50 | $10,433.75 | — | $10,433.75 | +0.00 | -17.99 | AUPH, OVID, SANM, ORCL, NVT, COHU, AMTX, CLOV | — | $251.47 | $10,387.51 | AUPH×80, OVID×477, SANM×6, ORCL×7, NVT×8, COHU×23, AMTX×639, CLOV×274 |
| 2026-09-14 | -11.00 | $251.47 | AUPH×80, OVID×477, SANM×6, ORCL×7, NVT×8, COHU×23, AMTX×639, CLOV×274 | $10,083.31 | -304.20 | -26.88 | — | AUPH, OVID, SANM, ORCL, COHU, AMTX, CLOV | $8,856.72 | $10,029.84 | NVT×8 |
| 2026-09-15 | -3.84 | $8,856.72 | NVT×8 | $10,065.68 | +35.84 | +0.00 | — | NVT | $10,063.65 | $10,063.65 | — |
| 2026-09-16 | +5.30 | $10,063.65 | — | $10,063.65 | -0.00 | -198.98 | IQV, RDNT, AVAH, BLFS, TEM, RIG, VAL, KRMN | — | $271.92 | $9,847.36 | IQV×4, RDNT×16, AVAH×87, BLFS×34, TEM×18, RIG×214, VAL×14, KRMN×33 |
| 2026-09-17 | +7.38 | $271.92 | IQV×4, RDNT×16, AVAH×87, BLFS×34, TEM×18, RIG×214, VAL×14, KRMN×33 | $9,998.94 | +151.58 | +109.04 | ILMN, TWST, RVTY, IOVA, PGEN, AMN, AXTI, ARQT | IQV, RDNT, AVAH, BLFS, TEM, RIG, VAL, KRMN | $227.19 | $10,073.34 | ILMN×5, TWST×8, RVTY×8, IOVA×121, PGEN×164, AMN×35, AXTI×18, ARQT×48 |
| 2026-09-18 | +4.86 | $227.19 | ILMN×5, TWST×8, RVTY×8, IOVA×121, PGEN×164, AMN×35, AXTI×18, ARQT×48 | $10,160.28 | +86.94 | -63.28 | SDGR, FTRE, BNC, DDD, RANI, RARE | TWST, RVTY, IOVA, PGEN, AMN, AXTI | $0.59 | $10,052.67 | ILMN×5, ARQT×48, SDGR×43, FTRE×63, BNC×218, DDD×355, RANI×1499, RARE×86 |

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
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $186.91 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy,oppset; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
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
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 117 | $11.13 | $2.34 | — | $5,357.63 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,mover_buy,oppset; 🔵; ⚪; ret5=+39.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
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
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 278 | $5.07 | $3.64 | $+28.91 | $5,493.24 | ▲ +28.91 after sell → book $10,964.69; vs 09:30 mark -3.64 | dropped from list after 2 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-11.10 | $6,765.05 | ▼ -11.10 after sell → book $10,962.67; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 32 | $41.44 | $2.09 | — | $5,436.88 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+3.1; leftover $1353.01 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 93 | $14.42 | $2.27 | — | $4,093.56 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+7.1; leftover $1353.01 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 520 | $2.60 | $6.71 | — | $2,734.85 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; ret5=+13.0; leftover $1353.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 104 | $12.98 | $2.30 | — | $1,382.63 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $1353.01 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ITG` | 109 | $12.36 | $2.32 | — | $33.07 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=-3.0; leftover $1353.01 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.07 | ▲ close $11,087.06 vs 09:30 $10,975.33 (session +140.07) | 16:00 close · cash $33.07 · equity $11,087.06 vs 09:30 $10,975.33 (+111.73; session marks +140.07) · 8 name(s) marked open→close (per-name table). MOS×57 09:30 $24.00 → close $23.76 -13.68; AVBP×46 09:30 $30.79 → close $31.00 +9.66; ABX×146 09:30 $9.68 → close $9.83 +21.90; RRC×32 09:30 $41.44 → close $41.64 +6.40; CRK×93 09:30 $14.42 → close $14.62 +18.60; SLI×520 09:30 $2.60 → close $2.64 +20.80; KURA×104 09:30 $12.98 → close $13.18 +20.80; ITG×109 09:30 $12.36 → close $12.87 +55.59 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.07 | ▼ 09:30 equity $11,086.26 vs yday $11,087.06 (-0.80) | 09:30 open · cash $33.07 (unchanged overnight, no fees) · equity $11,086.26 vs prior close $11,087.06 (-0.80) · 8 name(s) re-marked at the open (per-name table). MOS×57 yday $23.76 → 09:30 $23.95 +10.83; AVBP×46 yday $31.00 → 09:30 $30.53 -21.62; ABX×146 yday $9.83 → 09:30 $9.88 +7.30; RRC×32 yday $41.64 → 09:30 $41.74 +3.20; CRK×93 yday $14.62 → 09:30 $14.63 +0.93; SLI×520 yday $2.64 → 09:30 $2.68 +20.80; KURA×104 yday $13.18 → 09:30 $13.05 -13.52; ITG×109 yday $12.87 → 09:30 $12.79 -8.72 | — |
| 2026-08-28 09:30 ET | **SELL** | `AVBP` | 46 | $30.53 | $2.15 | $-35.56 | $1,435.30 | ▼ -35.56 after sell → book $11,084.11; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ABX` | 146 | $9.88 | $2.46 | $+2.41 | $2,875.32 | ▲ +2.41 after sell → book $11,081.65; vs 09:30 mark -2.46 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 104 | $13.05 | $2.33 | $+2.65 | $4,230.19 | ▲ +2.65 after sell → book $11,079.32; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ITG` | 109 | $12.79 | $2.35 | $+42.21 | $5,621.95 | ▲ +42.21 after sell → book $11,076.97; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 89 | $15.66 | $2.26 | — | $4,225.95 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1405.49 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 17 | $79.42 | $2.04 | — | $2,873.77 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,oppset; 🔵; ret5=+8.5; leftover $1405.49 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $1,610.57 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1405.49 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $293.92 | — | union ∩ ab_g, no 🚨; gate ab=good; list yday_gainer,yday_mover,oppset; 🔵; ret5=+38.8; leftover $1405.49 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $293.92 | ▼ close $10,846.66 vs 09:30 $11,086.26 (session -221.99) | 16:00 close · cash $293.92 · equity $10,846.66 vs 09:30 $11,086.26 (-239.60; session marks -221.99) · 8 name(s) marked open→close (per-name table). MOS×57 09:30 $23.95 → close $23.60 -19.95; RRC×32 09:30 $41.74 → close $41.46 -8.96; CRK×93 09:30 $14.63 → close $14.29 -31.62; SLI×520 09:30 $2.68 → close $2.55 -67.60; GRRR×89 09:30 $15.66 → close $14.41 -111.25; URBN×17 09:30 $79.42 → close $81.09 +28.39; SIMO×5 09:30 $252.24 → close $245.81 -32.15; ANF×9 09:30 $146.07 → close $148.42 +21.15 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $293.92 | ▲ 09:30 equity $10,901.66 vs yday $10,846.66 (+55.00) | 09:30 open · cash $293.92 (unchanged overnight, no fees) · equity $10,901.66 vs prior close $10,846.66 (+55.00) · 8 name(s) re-marked at the open (per-name table). MOS×57 yday $23.60 → 09:30 $23.68 +4.56; RRC×32 yday $41.46 → 09:30 $42.00 +17.28; CRK×93 yday $14.29 → 09:30 $14.54 +23.25; SLI×520 yday $2.55 → 09:30 $2.58 +15.60; GRRR×89 yday $14.41 → 09:30 $14.44 +2.67; URBN×17 yday $81.09 → 09:30 $80.44 -11.05; SIMO×5 yday $245.81 → 09:30 $247.05 +6.20; ANF×9 yday $148.42 → 09:30 $148.03 -3.51 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 57 | $23.68 | $2.18 | $-9.47 | $1,641.50 | ▼ -9.47 after sell → book $10,899.48; vs 09:30 mark -2.18 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 32 | $42.00 | $2.11 | $+13.73 | $2,983.39 | ▲ +13.73 after sell → book $10,897.37; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 93 | $14.54 | $2.30 | $+6.60 | $4,333.32 | ▲ +6.60 after sell → book $10,895.08; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 520 | $2.58 | $6.81 | $-23.91 | $5,668.11 | ▼ -23.91 after sell → book $10,888.27; vs 09:30 mark -6.81 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 89 | $14.44 | $2.28 | $-113.12 | $6,950.99 | ▼ -113.12 after sell → book $10,885.99; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 17 | $80.44 | $2.06 | $+13.24 | $8,316.41 | ▲ +13.24 after sell → book $10,883.93; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $9,549.63 | ▼ -29.98 after sell → book $10,881.90; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $10,879.86 | ▲ +13.59 after sell → book $10,879.86; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,879.86 | ▲ close $10,879.86 vs 09:30 $10,901.66 (session +0.00) | 16:00 close · cash $10,879.86 · no lots left · equity $10,879.86. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,879.86 | ▲ 09:30 equity $10,879.86 vs yday $10,879.86 (+0.00) | 09:30 open · cash $10,879.86 · no holdings · equity $10,879.86 vs prior close $10,879.86 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,879.86 | ▲ close $10,879.86 vs 09:30 $10,879.86 (session +0.00) | 16:00 close · cash $10,879.86 · no lots left · equity $10,879.86. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,879.86 | ▲ 09:30 equity $10,879.86 vs yday $10,879.86 (+0.00) | 09:30 open · cash $10,879.86 · no holdings · equity $10,879.86 vs prior close $10,879.86 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,879.86 | ▲ close $10,879.86 vs 09:30 $10,879.86 (session +0.00) | 16:00 close · cash $10,879.86 · no lots left · equity $10,879.86. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,879.86 | ▲ 09:30 equity $10,879.86 vs yday $10,879.86 (+0.00) | 09:30 open · cash $10,879.86 · no holdings · equity $10,879.86 vs prior close $10,879.86 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,555.80 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1359.98 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $42.93 | $2.08 | — | $8,222.89 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1359.98 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 374 | $3.63 | $4.82 | — | $6,860.44 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1359.98 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 169 | $8.03 | $2.50 | — | $5,500.87 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1359.98 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,174.35 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1359.98 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 88 | $15.45 | $2.25 | — | $2,812.50 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1359.98 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,496.98 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1359.98 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 81 | $16.77 | $2.23 | — | $136.38 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy,oppset; 🔵; ⚪; ret5=+5.7; leftover $1359.98 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $136.38 | ▼ close $10,617.77 vs 09:30 $10,879.86 (session -242.10) | 16:00 close · cash $136.38 · equity $10,617.77 vs 09:30 $10,879.86 (-262.09; session marks -242.10) · 8 name(s) marked open→close (per-name table). ATRC×25 09:30 $52.88 → close $52.46 -10.50; HRMY×31 09:30 $42.93 → close $41.86 -33.17; CABA×374 09:30 $3.63 → close $3.48 -56.10; VSTM×169 09:30 $8.03 → close $7.98 -8.45; RVTY×10 09:30 $132.45 → close $130.63 -18.20; CRK×88 09:30 $15.45 → close $14.95 -44.00; MRNA×9 09:30 $145.94 → close $148.87 +26.33; ARCT×81 09:30 $16.77 → close $15.56 -98.01 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $136.38 | ▲ 09:30 equity $10,621.75 vs yday $10,617.77 (+3.98) | 09:30 open · cash $136.38 (unchanged overnight, no fees) · equity $10,621.75 vs prior close $10,617.77 (+3.98) · 8 name(s) re-marked at the open (per-name table). ATRC×25 yday $52.46 → 09:30 $52.03 -10.75; HRMY×31 yday $41.86 → 09:30 $41.50 -11.16; CABA×374 yday $3.48 → 09:30 $3.46 -7.48; VSTM×169 yday $7.98 → 09:30 $7.91 -11.83; RVTY×10 yday $130.63 → 09:30 $130.03 -6.00; CRK×88 yday $14.95 → 09:30 $15.00 +4.40; MRNA×9 yday $148.87 → 09:30 $153.62 +42.75; ARCT×81 yday $15.56 → 09:30 $15.61 +4.05 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 31 | $41.50 | $2.10 | $-48.52 | $1,420.77 | ▼ -48.52 after sell → book $10,619.64; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 169 | $7.91 | $2.54 | $-25.31 | $2,755.03 | ▼ -25.31 after sell → book $10,617.11; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $4,053.29 | ▼ -28.26 after sell → book $10,615.07; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 88 | $15.00 | $2.28 | $-44.13 | $5,371.01 | ▼ -44.13 after sell → book $10,612.79; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $6,751.55 | ▲ +65.02 after sell → book $10,610.75; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 81 | $15.61 | $2.26 | $-98.45 | $8,013.70 | ▼ -98.45 after sell → book $10,608.49; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 530 | $2.52 | $6.84 | — | $6,671.26 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1335.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 199 | $6.71 | $2.59 | — | $5,333.39 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1335.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 702 | $1.90 | $9.06 | — | $3,990.53 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1335.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 279 | $4.78 | $3.60 | — | $2,653.31 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1335.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 840 | $1.59 | $10.84 | — | $1,306.88 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1335.62 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 115 | $11.31 | $2.33 | — | $3.89 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1335.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.89 | ▼ close $10,518.92 vs 09:30 $10,621.75 (session -54.33) | 16:00 close · cash $3.89 · equity $10,518.92 vs 09:30 $10,621.75 (-102.83; session marks -54.33) · 8 name(s) marked open→close (per-name table). ATRC×25 09:30 $52.03 → close $51.52 -12.75; CABA×374 09:30 $3.46 → close $3.47 +3.74; ALEC×530 09:30 $2.52 → close $2.46 -31.80; BHC×199 09:30 $6.71 → close $6.56 -29.85; BMEA×702 09:30 $1.90 → close $2.03 +91.26; OABI×279 09:30 $4.78 → close $4.33 -125.55; OPK×840 09:30 $1.59 → close $1.64 +42.00; VIR×115 09:30 $11.31 → close $11.38 +8.62 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.89 | ▼ 09:30 equity $10,476.49 vs yday $10,518.92 (-42.43) | 09:30 open · cash $3.89 (unchanged overnight, no fees) · equity $10,476.49 vs prior close $10,518.92 (-42.43) · 8 name(s) re-marked at the open (per-name table). ATRC×25 yday $51.52 → 09:30 $54.31 +69.75; CABA×374 yday $3.47 → 09:30 $3.43 -14.96; ALEC×530 yday $2.46 → 09:30 $2.38 -42.40; BHC×199 yday $6.56 → 09:30 $6.57 +1.99; BMEA×702 yday $2.03 → 09:30 $2.00 -21.06; OABI×279 yday $4.33 → 09:30 $4.30 -8.37; OPK×840 yday $1.64 → 09:30 $1.63 -8.40; VIR×115 yday $11.38 → 09:30 $11.22 -18.97 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 25 | $54.31 | $2.09 | $+31.60 | $1,359.56 | ▲ +31.60 after sell → book $10,474.41; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 374 | $3.43 | $4.90 | $-84.52 | $2,637.48 | ▼ -84.52 after sell → book $10,469.51; vs 09:30 mark -4.90 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 530 | $2.38 | $6.94 | $-87.97 | $3,891.94 | ▼ -87.97 after sell → book $10,462.57; vs 09:30 mark -6.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 199 | $6.57 | $2.63 | $-33.08 | $5,196.74 | ▼ -33.08 after sell → book $10,459.94; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 702 | $2.00 | $9.18 | $+51.96 | $6,591.56 | ▲ +51.96 after sell → book $10,450.76; vs 09:30 mark -9.18 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 279 | $4.30 | $3.66 | $-141.17 | $7,787.60 | ▼ -141.17 after sell → book $10,447.10; vs 09:30 mark -3.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 840 | $1.63 | $10.99 | $+11.78 | $9,145.82 | ▲ +11.78 after sell → book $10,436.12; vs 09:30 mark -10.98 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 115 | $11.22 | $2.36 | $-15.05 | $10,433.75 | ▼ -15.05 after sell → book $10,433.75; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,433.75 | ▲ close $10,433.75 vs 09:30 $10,476.49 (session +0.00) | 16:00 close · cash $10,433.75 · no lots left · equity $10,433.75. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,433.75 | ▲ 09:30 equity $10,433.75 vs yday $10,433.75 (+0.00) | 09:30 open · cash $10,433.75 · no holdings · equity $10,433.75 vs prior close $10,433.75 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,433.75 | ▲ close $10,433.75 vs 09:30 $10,433.75 (session +0.00) | 16:00 close · cash $10,433.75 · no lots left · equity $10,433.75. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,433.75 | ▲ 09:30 equity $10,433.75 vs yday $10,433.75 (+0.00) | 09:30 open · cash $10,433.75 · no holdings · equity $10,433.75 vs prior close $10,433.75 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,433.75 | ▲ close $10,433.75 vs 09:30 $10,433.75 (session +0.00) | 16:00 close · cash $10,433.75 · no lots left · equity $10,433.75. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,433.75 | ▲ 09:30 equity $10,433.75 vs yday $10,433.75 (+0.00) | 09:30 open · cash $10,433.75 · no holdings · equity $10,433.75 vs prior close $10,433.75 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 80 | $16.28 | $2.23 | — | $9,129.12 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=-1.1; leftover $1304.22 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 477 | $2.73 | $6.15 | — | $7,820.76 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=-3.0; leftover $1304.22 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $6,577.71 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+8.3; leftover $1304.22 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $5,424.69 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1304.22 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $4,160.44 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+4.7; leftover $1304.22 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 23 | $56.09 | $2.06 | — | $2,868.31 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+19.6; leftover $1304.22 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 639 | $2.04 | $8.24 | — | $1,556.51 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1304.22 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 274 | $4.75 | $3.53 | — | $251.47 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1304.22 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $251.47 | ▼ close $10,387.51 vs 09:30 $10,433.75 (session -17.99) | 16:00 close · cash $251.47 · equity $10,387.51 vs 09:30 $10,433.75 (-46.24; session marks -17.99) · 8 name(s) marked open→close (per-name table). AUPH×80 09:30 $16.28 → close $16.10 -14.40; OVID×477 09:30 $2.73 → close $2.69 -19.08; SANM×6 09:30 $206.84 → close $216.00 +54.96; ORCL×7 09:30 $164.43 → close $150.28 -99.05; NVT×8 09:30 $157.78 → close $162.38 +36.80; COHU×23 09:30 $56.09 → close $57.08 +22.77; AMTX×639 09:30 $2.04 → close $2.01 -19.17; CLOV×274 09:30 $4.75 → close $4.82 +19.18 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $251.47 | ▼ 09:30 equity $10,083.31 vs yday $10,387.51 (-304.20) | 09:30 open · cash $251.47 (unchanged overnight, no fees) · equity $10,083.31 vs prior close $10,387.51 (-304.20) · 8 name(s) re-marked at the open (per-name table). AUPH×80 yday $16.10 → 09:30 $16.03 -5.60; OVID×477 yday $2.69 → 09:30 $2.75 +31.00; SANM×6 yday $216.00 → 09:30 $206.50 -57.00; ORCL×7 yday $150.28 → 09:30 $141.42 -62.02; NVT×8 yday $162.38 → 09:30 $150.00 -99.04; COHU×23 yday $57.08 → 09:30 $52.23 -111.55; AMTX×639 yday $2.01 → 09:30 $2.01 +0.00; CLOV×274 yday $4.82 → 09:30 $4.82 +0.00 | — |
| 2026-09-14 09:30 ET | **SELL** | `AUPH` | 80 | $16.03 | $2.25 | $-24.48 | $1,531.62 | ▼ -24.48 after sell → book $10,081.05; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `OVID` | 477 | $2.75 | $6.24 | $-0.47 | $2,839.51 | ▼ -0.47 after sell → book $10,074.81; vs 09:30 mark -6.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SANM` | 6 | $206.50 | $2.03 | $-6.08 | $4,076.48 | ▼ -6.08 after sell → book $10,072.78; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 7 | $141.42 | $2.03 | $-165.11 | $5,064.39 | ▼ -165.11 after sell → book $10,070.75; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-14 09:30 ET | **SELL** | `COHU` | 23 | $52.23 | $2.08 | $-92.92 | $6,263.60 | ▼ -92.92 after sell → book $10,068.67; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 639 | $2.01 | $8.36 | $-35.77 | $7,539.63 | ▼ -35.77 after sell → book $10,060.31; vs 09:30 mark -8.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 274 | $4.82 | $3.59 | $+12.05 | $8,856.72 | ▲ +12.05 after sell → book $10,056.72; vs 09:30 mark -3.59 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,856.72 | ▼ close $10,029.84 vs 09:30 $10,083.31 (session -26.88) | 16:00 close · cash $8,856.72 · equity $10,029.84 vs 09:30 $10,083.31 (-53.47; session marks -26.88) · 1 name(s) marked open→close (per-name table). NVT×8 09:30 $150.00 → close $146.64 -26.88 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,856.72 | ▲ 09:30 equity $10,065.68 vs yday $10,029.84 (+35.84) | 09:30 open · cash $8,856.72 (unchanged overnight, no fees) · equity $10,065.68 vs prior close $10,029.84 (+35.84) · 1 name(s) re-marked at the open (per-name table). NVT×8 yday $146.64 → 09:30 $151.12 +35.84 | — |
| 2026-09-15 09:30 ET | **SELL** | `NVT` | 8 | $151.12 | $2.03 | $-57.33 | $10,063.65 | ▼ -57.33 after sell → book $10,063.65; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,063.65 | ▲ close $10,063.65 vs 09:30 $10,065.68 (session +0.00) | 16:00 close · cash $10,063.65 · no lots left · equity $10,063.65. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,063.65 | ▲ 09:30 equity $10,063.65 vs yday $10,063.65 (-0.00) | 09:30 open · cash $10,063.65 · no holdings · equity $10,063.65 vs prior close $10,063.65 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $8,978.09 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+4.0; leftover $1257.96 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 16 | $77.12 | $2.04 | — | $7,742.13 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; ret5=+7.2; leftover $1257.96 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 87 | $14.31 | $2.25 | — | $6,494.91 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+4.8; leftover $1257.96 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 34 | $36.46 | $2.09 | — | $5,253.17 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+2.9; leftover $1257.96 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 18 | $68.79 | $2.04 | — | $4,012.91 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1257.96 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 214 | $5.87 | $2.76 | — | $2,753.97 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1257.96 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 catal🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 14 | $87.40 | $2.03 | — | $1,528.34 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1257.96 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 33 | $38.01 | $2.09 | — | $271.92 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $1257.96 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $271.92 | ▼ close $9,847.36 vs 09:30 $10,063.65 (session -198.98) | 16:00 close · cash $271.92 · equity $9,847.36 vs 09:30 $10,063.65 (-216.29; session marks -198.98) · 8 name(s) marked open→close (per-name table). IQV×4 09:30 $270.89 → close $268.82 -8.28; RDNT×16 09:30 $77.12 → close $75.78 -21.44; AVAH×87 09:30 $14.31 → close $14.26 -4.35; BLFS×34 09:30 $36.46 → close $36.11 -11.90; TEM×18 09:30 $68.79 → close $69.97 +21.24; RIG×214 09:30 $5.87 → close $5.54 -70.62; VAL×14 09:30 $87.40 → close $82.52 -68.32; KRMN×33 09:30 $38.01 → close $36.94 -35.31 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $271.92 | ▲ 09:30 equity $9,998.94 vs yday $9,847.36 (+151.58) | 09:30 open · cash $271.92 (unchanged overnight, no fees) · equity $9,998.94 vs prior close $9,847.36 (+151.58) · 8 name(s) re-marked at the open (per-name table). IQV×4 yday $268.82 → 09:30 $273.15 +17.32; RDNT×16 yday $75.78 → 09:30 $76.44 +10.56; AVAH×87 yday $14.26 → 09:30 $14.33 +6.09; BLFS×34 yday $36.11 → 09:30 $36.67 +19.04; TEM×18 yday $69.97 → 09:30 $72.70 +49.14; RIG×214 yday $5.54 → 09:30 $5.58 +8.56; VAL×14 yday $82.52 → 09:30 $83.20 +9.52; KRMN×33 yday $36.94 → 09:30 $37.89 +31.35 | — |
| 2026-09-17 09:30 ET | **SELL** | `IQV` | 4 | $273.15 | $2.02 | $+5.02 | $1,362.50 | ▲ +5.02 after sell → book $9,996.92; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 16 | $76.44 | $2.06 | $-14.98 | $2,583.48 | ▼ -14.98 after sell → book $9,994.86; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AVAH` | 87 | $14.33 | $2.28 | $-2.79 | $3,827.91 | ▼ -2.79 after sell → book $9,992.58; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BLFS` | 34 | $36.67 | $2.11 | $+2.94 | $5,072.58 | ▲ +2.94 after sell → book $9,990.47; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 18 | $72.70 | $2.06 | $+66.27 | $6,379.12 | ▲ +66.27 after sell → book $9,988.41; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 214 | $5.58 | $2.81 | $-67.63 | $7,570.43 | ▼ -67.63 after sell → book $9,985.60; vs 09:30 mark -2.81 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 14 | $83.20 | $2.05 | $-62.88 | $8,733.18 | ▼ -62.88 after sell → book $9,983.55; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `KRMN` | 33 | $37.89 | $2.11 | $-8.16 | $9,981.44 | ▼ -8.16 after sell → book $9,981.44; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 5 | $233.85 | $2.00 | — | $8,810.18 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; ret5=+11.7; leftover $1247.68 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 8 | $151.43 | $2.01 | — | $7,596.73 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $1247.68 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 8 | $147.61 | $2.01 | — | $6,413.84 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; ret5=+17.7; leftover $1247.68 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 121 | $10.25 | $2.35 | — | $5,171.23 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1247.68 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 164 | $7.59 | $2.48 | — | $3,923.99 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1247.68 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 35 | $34.93 | $2.10 | — | $2,699.35 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+1.6; leftover $1247.68 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AXTI` | 18 | $67.91 | $2.04 | — | $1,474.92 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.7; leftover $1247.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 48 | $25.95 | $2.13 | — | $227.19 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,ohlc_hot,oppset; 🔵; ret5=+9.6; leftover $1247.68 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $227.19 | ▲ close $10,073.34 vs 09:30 $9,998.94 (session +109.04) | 16:00 close · cash $227.19 · equity $10,073.34 vs 09:30 $9,998.94 (+74.40; session marks +109.04) · 8 name(s) marked open→close (per-name table). ILMN×5 09:30 $233.85 → close $245.18 +56.65; TWST×8 09:30 $151.43 → close $155.56 +33.04; RVTY×8 09:30 $147.61 → close $146.73 -7.04; IOVA×121 09:30 $10.25 → close $10.02 -27.83; PGEN×164 09:30 $7.59 → close $7.87 +45.92; AMN×35 09:30 $34.93 → close $34.55 -13.30; AXTI×18 09:30 $67.91 → close $67.75 -2.88; ARQT×48 09:30 $25.95 → close $26.46 +24.48 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $227.19 | ▲ 09:30 equity $10,160.28 vs yday $10,073.34 (+86.94) | 09:30 open · cash $227.19 (unchanged overnight, no fees) · equity $10,160.28 vs prior close $10,073.34 (+86.94) · 8 name(s) re-marked at the open (per-name table). ILMN×5 yday $245.18 → 09:30 $249.13 +19.75; TWST×8 yday $155.56 → 09:30 $158.04 +19.84; RVTY×8 yday $146.73 → 09:30 $146.50 -1.84; IOVA×121 yday $10.02 → 09:30 $10.12 +12.10; PGEN×164 yday $7.87 → 09:30 $7.98 +18.04; AMN×35 yday $34.55 → 09:30 $34.52 -1.05; AXTI×18 yday $67.75 → 09:30 $69.72 +35.46; ARQT×48 yday $26.46 → 09:30 $26.14 -15.36 | — |
| 2026-09-18 09:30 ET | **SELL** | `TWST` | 8 | $158.04 | $2.03 | $+48.83 | $1,489.47 | ▲ +48.83 after sell → book $10,158.24; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 8 | $146.50 | $2.03 | $-12.93 | $2,659.44 | ▼ -12.93 after sell → book $10,156.21; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 121 | $10.12 | $2.38 | $-20.47 | $3,881.58 | ▼ -20.47 after sell → book $10,153.83; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 164 | $7.98 | $2.52 | $+58.96 | $5,187.78 | ▲ +58.96 after sell → book $10,151.31; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 35 | $34.52 | $2.12 | $-18.56 | $6,393.86 | ▼ -18.56 after sell → book $10,149.19; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AXTI` | 18 | $69.72 | $2.06 | $+28.47 | $7,646.76 | ▲ +28.47 after sell → book $10,147.13; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 43 | $29.32 | $2.12 | — | $6,383.88 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $1274.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FTRE` | 63 | $20.10 | $2.18 | — | $5,115.40 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+19.2; leftover $1274.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 218 | $5.83 | $2.81 | — | $3,841.65 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1274.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 355 | $3.58 | $4.58 | — | $2,566.17 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1274.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RANI` | 1499 | $0.85 | $17.24 | — | $1,274.78 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; ret5=+3.6; leftover $1274.46 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 86 | $14.79 | $2.25 | — | $0.59 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1274.46 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.59 | ▼ close $10,052.67 vs 09:30 $10,160.28 (session -63.28) | 16:00 close · cash $0.59 · equity $10,052.67 vs 09:30 $10,160.28 (-107.61; session marks -63.28) · 8 name(s) marked open→close (per-name table). ILMN×5 09:30 $249.13 → close $239.62 -47.55; ARQT×48 09:30 $26.14 → close $25.38 -36.48; SDGR×43 09:30 $29.32 → close $29.02 -12.90; FTRE×63 09:30 $20.10 → close $19.93 -10.71; BNC×218 09:30 $5.83 → close $5.98 +32.70; DDD×355 09:30 $3.58 → close $3.63 +17.75; RANI×1499 09:30 $0.85 → close $0.86 +17.99; RARE×86 09:30 $14.79 → close $14.51 -24.08 | — |

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
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VICR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RPD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ILMN` | 5 | 2026-09-17 @ $233.85 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; ret5=+11.7; leftover $1247.68 |
| `ARQT` | 48 | 2026-09-17 @ $25.95 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,ohlc_hot,oppset; 🔵; ret5=+9.6; leftover $1247.68 |
| `SDGR` | 43 | 2026-09-18 @ $29.32 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $1274.46 |
| `FTRE` | 63 | 2026-09-18 @ $20.10 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+19.2; leftover $1274.46 |
| `BNC` | 218 | 2026-09-18 @ $5.83 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1274.46 |
| `DDD` | 355 | 2026-09-18 @ $3.58 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1274.46 |
| `RANI` | 1499 | 2026-09-18 @ $0.85 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; ret5=+3.6; leftover $1274.46 |
| `RARE` | 86 | 2026-09-18 @ $14.79 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1274.46 |
