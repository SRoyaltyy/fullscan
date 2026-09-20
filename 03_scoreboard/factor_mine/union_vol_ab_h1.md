# Factor mine action — `union_vol_ab_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+9.20%** ($10,920) · signal-only (no cash/fees) was +63.81%. Starts YES **24/26**. Fills 148 · skips 48 · realized $+540.80.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the volume camera (is this name unusually active?) is green.
- Must-have: the A/B camera (does our A/B score like this name?) is green.
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
- **Gate** `vol=good,ab=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $164.08.

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
| 2026-08-25 | `KURA` | 101 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `LIFE` | 37 | — | $36.96 | +0.00 | $38.56 | +59.20 | +59.20 | +0.00 | +59.20 |
| 2026-08-25 | `ZIP` | 302 | — | $4.55 | +0.00 | $4.35 | -60.40 | -60.40 | +0.00 | -60.40 |
| 2026-08-25 | `BMEA` | 844 | — | $1.63 | +0.00 | $1.73 | +84.40 | +84.40 | +0.00 | +84.40 |
| 2026-08-25 | `ALVO` | 262 | — | $5.24 | +0.00 | $5.05 | -49.78 | -49.78 | +0.00 | -49.78 |
| 2026-08-25 | `CYPH` | 882 | — | $1.56 | +0.00 | $1.64 | +70.56 | +70.56 | +0.00 | +70.56 |
| 2026-08-25 | `DEFT` | 2219 | — | $0.62 | +0.00 | $0.60 | -35.50 | -35.50 | +0.00 | -35.50 |
| 2026-08-25 | `ZURA` | 210 | — | $6.37 | +0.00 | $6.32 | -10.50 | -10.50 | +0.00 | -10.50 |
| 2026-08-26 | `KURA` | 101 | $13.59 | $13.63 | +4.04 | — | +0.00 | +4.04 | +4.04 | — |
| 2026-08-26 | `LIFE` | 37 | $38.56 | $38.24 | -11.84 | — | +0.00 | -11.84 | +47.36 | — |
| 2026-08-26 | `ZIP` | 302 | $4.35 | $4.31 | -12.08 | — | +0.00 | -12.08 | -72.48 | — |
| 2026-08-26 | `BMEA` | 844 | $1.73 | $1.75 | +21.10 | — | +0.00 | +21.10 | +105.50 | — |
| 2026-08-26 | `ALVO` | 262 | $5.05 | $4.98 | -18.34 | — | +0.00 | -18.34 | -68.12 | — |
| 2026-08-26 | `CYPH` | 882 | $1.64 | $1.60 | -35.28 | — | +0.00 | -35.28 | +35.28 | — |
| 2026-08-26 | `DEFT` | 2219 | $0.60 | $0.60 | -13.31 | — | +0.00 | -13.31 | -48.82 | — |
| 2026-08-26 | `ZURA` | 210 | $6.32 | $6.13 | -39.90 | — | +0.00 | -39.90 | -50.40 | — |
| 2026-08-26 | `SLQT` | 9304 | — | $0.58 | +0.00 | $0.55 | -307.03 | -307.03 | +0.00 | -307.03 |
| 2026-08-26 | `DKS` | 43 | — | $121.87 | +0.00 | $129.66 | +334.97 | +334.97 | +0.00 | +334.97 |
| 2026-08-27 | `SLQT` | 9304 | $0.55 | $0.53 | -186.08 | $0.54 | +93.04 | -93.04 | -493.11 | -400.07 |
| 2026-08-27 | `DKS` | 43 | $129.66 | $128.73 | -39.99 | $131.77 | +130.72 | +90.73 | +294.98 | +425.70 |
| 2026-08-28 | `SLQT` | 9304 | $0.54 | $0.53 | -83.74 | — | +0.00 | -83.74 | -483.81 | — |
| 2026-08-28 | `DKS` | 43 | $131.77 | $132.80 | +44.29 | — | +0.00 | +44.29 | +469.99 | — |
| 2026-08-28 | `URBN` | 16 | — | $79.42 | +0.00 | $81.09 | +26.72 | +26.72 | +0.00 | +26.72 |
| 2026-08-28 | `ANF` | 9 | — | $146.07 | +0.00 | $148.42 | +21.15 | +21.15 | +0.00 | +21.15 |
| 2026-08-28 | `BZ` | 73 | — | $18.15 | +0.00 | $17.80 | -25.55 | -25.55 | +0.00 | -25.55 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `BBWI` | 71 | — | $18.75 | +0.00 | $19.22 | +33.37 | +33.37 | +0.00 | +33.37 |
| 2026-08-28 | `CRDL` | 647 | — | $2.06 | +0.00 | $1.94 | -77.64 | -77.64 | +0.00 | -77.64 |
| 2026-08-28 | `NCNO` | 57 | — | $23.30 | +0.00 | $22.99 | -17.67 | -17.67 | +0.00 | -17.67 |
| 2026-08-28 | `TH` | 70 | — | $19.00 | +0.00 | $18.55 | -31.50 | -31.50 | +0.00 | -31.50 |
| 2026-08-31 | `URBN` | 16 | $81.09 | $80.44 | -10.40 | — | +0.00 | -10.40 | +16.32 | — |
| 2026-08-31 | `ANF` | 9 | $148.42 | $148.03 | -3.51 | — | +0.00 | -3.51 | +17.64 | — |
| 2026-08-31 | `BZ` | 73 | $17.80 | $17.70 | -7.30 | — | +0.00 | -7.30 | -32.85 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `BBWI` | 71 | $19.22 | $19.25 | +2.13 | — | +0.00 | +2.13 | +35.50 | — |
| 2026-08-31 | `CRDL` | 647 | $1.94 | $1.92 | -12.94 | — | +0.00 | -12.94 | -90.58 | — |
| 2026-08-31 | `NCNO` | 57 | $22.99 | $22.66 | -18.81 | — | +0.00 | -18.81 | -36.48 | — |
| 2026-08-31 | `TH` | 70 | $18.55 | $18.12 | -29.75 | $18.52 | +27.65 | -2.10 | -61.25 | -33.60 |
| 2026-09-01 | `TH` | 70 | $18.52 | $18.45 | -4.90 | — | +0.00 | -4.90 | -38.50 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `CRK` | 84 | — | $15.45 | +0.00 | $14.95 | -42.00 | -42.00 | +0.00 | -42.00 |
| 2026-09-03 | `MRNA` | 8 | — | $145.94 | +0.00 | $148.87 | +23.40 | +23.40 | +0.00 | +23.40 |
| 2026-09-03 | `ARCT` | 77 | — | $16.77 | +0.00 | $15.56 | -93.17 | -93.17 | +0.00 | -93.17 |
| 2026-09-03 | `EIX` | 23 | — | $55.42 | +0.00 | $56.30 | +20.24 | +20.24 | +0.00 | +20.24 |
| 2026-09-03 | `CRDL` | 596 | — | $2.18 | +0.00 | $2.16 | -11.92 | -11.92 | +0.00 | -11.92 |
| 2026-09-03 | `MMED` | 54 | — | $23.88 | +0.00 | $23.84 | -2.16 | -2.16 | +0.00 | -2.16 |
| 2026-09-03 | `NVAX` | 124 | — | $10.42 | +0.00 | $10.34 | -9.92 | -9.92 | +0.00 | -9.92 |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `CRK` | 84 | $14.95 | $15.00 | +4.20 | — | +0.00 | +4.20 | -37.80 | — |
| 2026-09-04 | `MRNA` | 8 | $148.87 | $153.62 | +38.00 | — | +0.00 | +38.00 | +61.40 | — |
| 2026-09-04 | `ARCT` | 77 | $15.56 | $15.61 | +3.85 | — | +0.00 | +3.85 | -89.32 | — |
| 2026-09-04 | `EIX` | 23 | $56.30 | $55.79 | -11.73 | — | +0.00 | -11.73 | +8.51 | — |
| 2026-09-04 | `CRDL` | 596 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -11.92 | — |
| 2026-09-04 | `MMED` | 54 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.16 | — |
| 2026-09-04 | `NVAX` | 124 | $10.34 | $10.50 | +19.84 | — | +0.00 | +19.84 | +9.92 | — |
| 2026-09-04 | `CABA` | 371 | — | $3.46 | +0.00 | $3.47 | +3.71 | +3.71 | +0.00 | +3.71 |
| 2026-09-04 | `ALEC` | 509 | — | $2.52 | +0.00 | $2.46 | -30.54 | -30.54 | +0.00 | -30.54 |
| 2026-09-04 | `BHC` | 191 | — | $6.71 | +0.00 | $6.56 | -28.65 | -28.65 | +0.00 | -28.65 |
| 2026-09-04 | `BMEA` | 676 | — | $1.90 | +0.00 | $2.03 | +87.88 | +87.88 | +0.00 | +87.88 |
| 2026-09-04 | `OABI` | 268 | — | $4.78 | +0.00 | $4.33 | -120.60 | -120.60 | +0.00 | -120.60 |
| 2026-09-04 | `VIR` | 113 | — | $11.31 | +0.00 | $11.38 | +8.47 | +8.47 | +0.00 | +8.47 |
| 2026-09-04 | `DELL` | 2 | — | $513.78 | +0.00 | $524.14 | +20.72 | +20.72 | +0.00 | +20.72 |
| 2026-09-04 | `MLYS` | 45 | — | $28.00 | +0.00 | $28.21 | +9.45 | +9.45 | +0.00 | +9.45 |
| 2026-09-08 | `CABA` | 371 | $3.47 | $3.43 | -14.84 | — | +0.00 | -14.84 | -11.13 | — |
| 2026-09-08 | `ALEC` | 509 | $2.46 | $2.38 | -40.72 | — | +0.00 | -40.72 | -71.26 | — |
| 2026-09-08 | `BHC` | 191 | $6.56 | $6.57 | +1.91 | — | +0.00 | +1.91 | -26.74 | — |
| 2026-09-08 | `BMEA` | 676 | $2.03 | $2.00 | -20.28 | — | +0.00 | -20.28 | +67.60 | — |
| 2026-09-08 | `OABI` | 268 | $4.33 | $4.30 | -8.04 | — | +0.00 | -8.04 | -128.64 | — |
| 2026-09-08 | `VIR` | 113 | $11.38 | $11.22 | -18.64 | — | +0.00 | -18.64 | -10.17 | — |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | — | +0.00 | -5.98 | +14.74 | — |
| 2026-09-08 | `MLYS` | 45 | $28.21 | $28.03 | -8.10 | — | +0.00 | -8.10 | +1.35 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 7 | — | $164.43 | +0.00 | $150.28 | -99.05 | -99.05 | +0.00 | -99.05 |
| 2026-09-11 | `VIST` | 16 | — | $77.33 | +0.00 | $76.27 | -16.96 | -16.96 | +0.00 | -16.96 |
| 2026-09-11 | `INDP` | 465 | — | $2.70 | +0.00 | $2.77 | +32.55 | +32.55 | +0.00 | +32.55 |
| 2026-09-11 | `CMRC` | 401 | — | $3.13 | +0.00 | $3.50 | +150.38 | +150.38 | +0.00 | +150.38 |
| 2026-09-11 | `WLTH` | 114 | — | $10.95 | +0.00 | $10.38 | -64.98 | -64.98 | +0.00 | -64.98 |
| 2026-09-11 | `BNC` | 255 | — | $4.91 | +0.00 | $4.80 | -28.05 | -28.05 | +0.00 | -28.05 |
| 2026-09-11 | `SWKS` | 14 | — | $84.27 | +0.00 | $88.35 | +57.12 | +57.12 | +0.00 | +57.12 |
| 2026-09-11 | `ANGX` | 233 | — | $5.38 | +0.00 | $5.45 | +16.31 | +16.31 | +0.00 | +16.31 |
| 2026-09-14 | `ORCL` | 7 | $150.28 | $141.42 | -62.02 | — | +0.00 | -62.02 | -161.07 | — |
| 2026-09-14 | `VIST` | 16 | $76.27 | $77.10 | +13.28 | — | +0.00 | +13.28 | -3.68 | — |
| 2026-09-14 | `INDP` | 465 | $2.77 | $2.80 | +13.95 | $3.14 | +158.10 | +172.05 | +46.50 | +204.60 |
| 2026-09-14 | `CMRC` | 401 | $3.50 | $3.51 | +2.00 | — | +0.00 | +2.00 | +152.38 | — |
| 2026-09-14 | `WLTH` | 114 | $10.38 | $10.29 | -10.26 | — | +0.00 | -10.26 | -75.24 | — |
| 2026-09-14 | `BNC` | 255 | $4.80 | $5.03 | +58.65 | — | +0.00 | +58.65 | +30.60 | — |
| 2026-09-14 | `SWKS` | 14 | $88.35 | $86.06 | -32.06 | — | +0.00 | -32.06 | +25.06 | — |
| 2026-09-14 | `ANGX` | 233 | $5.45 | $5.57 | +27.96 | — | +0.00 | +27.96 | +44.27 | — |
| 2026-09-15 | `INDP` | 465 | $3.14 | $3.40 | +120.90 | — | +0.00 | +120.90 | +325.50 | — |
| 2026-09-16 | `RDNT` | 16 | — | $77.12 | +0.00 | $75.78 | -21.44 | -21.44 | +0.00 | -21.44 |
| 2026-09-16 | `RIG` | 220 | — | $5.87 | +0.00 | $5.54 | -72.60 | -72.60 | +0.00 | -72.60 |
| 2026-09-16 | `VAL` | 14 | — | $87.40 | +0.00 | $82.52 | -68.32 | -68.32 | +0.00 | -68.32 |
| 2026-09-16 | `ADPT` | 47 | — | $27.09 | +0.00 | $27.67 | +27.26 | +27.26 | +0.00 | +27.26 |
| 2026-09-16 | `SWKS` | 14 | — | $89.38 | +0.00 | $85.59 | -53.06 | -53.06 | +0.00 | -53.06 |
| 2026-09-16 | `SDGR` | 55 | — | $23.29 | +0.00 | $23.93 | +35.20 | +35.20 | +0.00 | +35.20 |
| 2026-09-16 | `FPS` | 38 | — | $33.14 | +0.00 | $34.84 | +64.60 | +64.60 | +0.00 | +64.60 |
| 2026-09-16 | `CAI` | 45 | — | $28.16 | +0.00 | $28.21 | +2.25 | +2.25 | +0.00 | +2.25 |
| 2026-09-17 | `RDNT` | 16 | $75.78 | $76.44 | +10.56 | — | +0.00 | +10.56 | -10.88 | — |
| 2026-09-17 | `RIG` | 220 | $5.54 | $5.58 | +8.80 | — | +0.00 | +8.80 | -63.80 | — |
| 2026-09-17 | `VAL` | 14 | $82.52 | $83.20 | +9.52 | — | +0.00 | +9.52 | -58.80 | — |
| 2026-09-17 | `ADPT` | 47 | $27.67 | $28.23 | +26.32 | — | +0.00 | +26.32 | +53.58 | — |
| 2026-09-17 | `SWKS` | 14 | $85.59 | $86.76 | +16.38 | — | +0.00 | +16.38 | -36.68 | — |
| 2026-09-17 | `SDGR` | 55 | $23.93 | $24.09 | +8.80 | — | +0.00 | +8.80 | +44.00 | — |
| 2026-09-17 | `FPS` | 38 | $34.84 | $36.76 | +72.96 | $38.06 | +49.40 | +122.36 | +137.56 | +186.96 |
| 2026-09-17 | `CAI` | 45 | $28.21 | $28.59 | +17.32 | — | +0.00 | +17.32 | +19.57 | — |
| 2026-09-17 | `ILMN` | 5 | — | $233.85 | +0.00 | $245.18 | +56.65 | +56.65 | +0.00 | +56.65 |
| 2026-09-17 | `RVTY` | 8 | — | $147.61 | +0.00 | $146.73 | -7.04 | -7.04 | +0.00 | -7.04 |
| 2026-09-17 | `PGEN` | 169 | — | $7.59 | +0.00 | $7.87 | +47.32 | +47.32 | +0.00 | +47.32 |
| 2026-09-17 | `ARQT` | 49 | — | $25.95 | +0.00 | $26.46 | +24.99 | +24.99 | +0.00 | +24.99 |
| 2026-09-17 | `SMTC` | 7 | — | $170.85 | +0.00 | $178.19 | +51.38 | +51.38 | +0.00 | +51.38 |
| 2026-09-17 | `SABR` | 535 | — | $2.40 | +0.00 | $2.32 | -42.80 | -42.80 | +0.00 | -42.80 |
| 2026-09-17 | `CYPH` | 480 | — | $2.67 | +0.00 | $3.07 | +189.60 | +189.60 | +0.00 | +189.60 |
| 2026-09-18 | `FPS` | 38 | $38.06 | $39.50 | +54.72 | — | +0.00 | +54.72 | +241.68 | — |
| 2026-09-18 | `ILMN` | 5 | $245.18 | $249.13 | +19.75 | $239.62 | -47.55 | -27.80 | +76.40 | +28.85 |
| 2026-09-18 | `RVTY` | 8 | $146.73 | $146.50 | -1.84 | — | +0.00 | -1.84 | -8.88 | — |
| 2026-09-18 | `PGEN` | 169 | $7.87 | $7.98 | +18.59 | — | +0.00 | +18.59 | +65.91 | — |
| 2026-09-18 | `ARQT` | 49 | $26.46 | $26.14 | -15.68 | $25.38 | -37.24 | -52.92 | +9.31 | -27.93 |
| 2026-09-18 | `SMTC` | 7 | $178.19 | $182.33 | +28.98 | — | +0.00 | +28.98 | +80.36 | — |
| 2026-09-18 | `SABR` | 535 | $2.32 | $2.29 | -16.05 | — | +0.00 | -16.05 | -58.85 | — |
| 2026-09-18 | `CYPH` | 480 | $3.07 | $3.04 | -16.80 | $3.60 | +271.20 | +254.40 | +172.80 | +444.00 |
| 2026-09-18 | `SDGR` | 46 | — | $29.32 | +0.00 | $29.02 | -13.80 | -13.80 | +0.00 | -13.80 |
| 2026-09-18 | `FTRE` | 67 | — | $20.10 | +0.00 | $19.93 | -11.39 | -11.39 | +0.00 | -11.39 |
| 2026-09-18 | `RARE` | 92 | — | $14.79 | +0.00 | $14.51 | -25.76 | -25.76 | +0.00 | -25.76 |
| 2026-09-18 | `GNRC` | 6 | — | $209.52 | +0.00 | $207.44 | -12.48 | -12.48 | +0.00 | -12.48 |
| 2026-09-18 | `VICR` | 6 | — | $219.62 | +0.00 | $222.72 | +18.60 | +18.60 | +0.00 | +18.60 |

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
| 2026-08-25 | +1.80 | $11,010.75 | — | $11,010.75 | -0.00 | +57.98 | KURA, LIFE, ZIP, BMEA, ALVO, CYPH, DEFT, ZURA | — | $1.48 | $11,011.66 | KURA×101, LIFE×37, ZIP×302, BMEA×844, ALVO×262, CYPH×882, DEFT×2219, ZURA×210 |
| 2026-08-26 | +2.02 | $1.48 | KURA×101, LIFE×37, ZIP×302, BMEA×844, ALVO×262, CYPH×882, DEFT×2219, ZURA×210 | $10,906.05 | -105.61 | +27.94 | SLQT, DKS | KURA, LIFE, ZIP, BMEA, ALVO, CYPH, DEFT, ZURA | $99.67 | $10,792.25 | SLQT×9304, DKS×43 |
| 2026-08-27 | — | $99.67 | SLQT×9304, DKS×43 | $10,566.18 | -226.07 | +223.76 | — | — | $99.67 | $10,789.94 | SLQT×9304, DKS×43 |
| 2026-08-28 | +0.75 | $99.67 | SLQT×9304, DKS×43 | $10,750.49 | -39.45 | -166.43 | URBN, ANF, BZ, SMTC, BBWI, CRDL, NCNO, TH | SLQT, DKS | $137.91 | $10,479.79 | URBN×16, ANF×9, BZ×73, SMTC×9, BBWI×71, CRDL×647, NCNO×57, TH×70 |
| 2026-08-31 | -5.85 | $137.91 | URBN×16, ANF×9, BZ×73, SMTC×9, BBWI×71, CRDL×647, NCNO×57, TH×70 | $10,409.38 | -70.41 | +27.65 | — | URBN, ANF, BZ, SMTC, BBWI, CRDL, NCNO | $9,119.40 | $10,415.80 | TH×70 |
| 2026-09-01 | -6.30 | $9,119.40 | TH×70 | $10,410.90 | -4.90 | +0.00 | — | TH | $10,408.68 | $10,408.68 | — |
| 2026-09-02 | -3.83 | $10,408.68 | — | $10,408.68 | -0.00 | +0.00 | — | — | $10,408.68 | $10,408.68 | — |
| 2026-09-03 | -0.90 | $10,408.68 | — | $10,408.68 | -0.00 | -131.91 | RVTY, CRK, MRNA, ARCT, EIX, CRDL, MMED, NVAX | — | $281.68 | $10,254.01 | RVTY×9, CRK×84, MRNA×8, ARCT×77, EIX×23, CRDL×596, MMED×54, NVAX×124 |
| 2026-09-04 | +2.25 | $281.68 | RVTY×9, CRK×84, MRNA×8, ARCT×77, EIX×23, CRDL×596, MMED×54, NVAX×124 | $10,302.77 | +48.76 | -49.56 | CABA, ALEC, BHC, BMEA, OABI, VIR, DELL, MLYS | RVTY, CRK, MRNA, ARCT, EIX, CRDL, MMED, NVAX | $268.23 | $10,197.65 | CABA×371, ALEC×509, BHC×191, BMEA×676, OABI×268, VIR×113, DELL×2, MLYS×45 |
| 2026-09-08 | -11.47 | $268.23 | CABA×371, ALEC×509, BHC×191, BMEA×676, OABI×268, VIR×113, DELL×2, MLYS×45 | $10,082.96 | -114.69 | +0.00 | — | CABA, ALEC, BHC, BMEA, OABI, VIR, DELL, MLYS | $10,049.96 | $10,049.96 | — |
| 2026-09-09 | -13.95 | $10,049.96 | — | $10,049.96 | -0.00 | +0.00 | — | — | $10,049.96 | $10,049.96 | — |
| 2026-09-10 | -13.28 | $10,049.96 | — | $10,049.96 | -0.00 | +0.00 | — | — | $10,049.96 | $10,049.96 | — |
| 2026-09-11 | +0.50 | $10,049.96 | — | $10,049.96 | -0.00 | +47.32 | ORCL, VIST, INDP, CMRC, WLTH, BNC, SWKS, ANGX | — | $191.49 | $10,071.39 | ORCL×7, VIST×16, INDP×465, CMRC×401, WLTH×114, BNC×255, SWKS×14, ANGX×233 |
| 2026-09-14 | -11.00 | $191.49 | ORCL×7, VIST×16, INDP×465, CMRC×401, WLTH×114, BNC×255, SWKS×14, ANGX×233 | $10,082.90 | +11.51 | +158.10 | — | ORCL, VIST, CMRC, WLTH, BNC, SWKS, ANGX | $8,760.75 | $10,220.85 | INDP×465 |
| 2026-09-15 | -3.84 | $8,760.75 | INDP×465 | $10,341.75 | +120.90 | +0.00 | — | INDP | $10,335.66 | $10,335.66 | — |
| 2026-09-16 | +5.30 | $10,335.66 | — | $10,335.66 | +0.00 | -86.11 | RDNT, RIG, VAL, ADPT, SWKS, SDGR, FPS, CAI | — | $237.27 | $10,232.10 | RDNT×16, RIG×220, VAL×14, ADPT×47, SWKS×14, SDGR×55, FPS×38, CAI×45 |
| 2026-09-17 | +7.38 | $237.27 | RDNT×16, RIG×220, VAL×14, ADPT×47, SWKS×14, SDGR×55, FPS×38, CAI×45 | $10,402.76 | +170.66 | +369.50 | ILMN, RVTY, PGEN, ARQT, SMTC, SABR, CYPH | RDNT, RIG, VAL, ADPT, SWKS, SDGR, CAI | $298.26 | $10,732.98 | FPS×38, ILMN×5, RVTY×8, PGEN×169, ARQT×49, SMTC×7, SABR×535, CYPH×480 |
| 2026-09-18 | +4.86 | $298.26 | FPS×38, ILMN×5, RVTY×8, PGEN×169, ARQT×49, SMTC×7, SABR×535, CYPH×480 | $10,804.65 | +71.67 | +141.58 | SDGR, FTRE, RARE, GNRC, VICR | FPS, RVTY, PGEN, SMTC, SABR | $164.08 | $10,919.91 | ILMN×5, ARQT×49, CYPH×480, SDGR×46, FTRE×67, RARE×92, GNRC×6, VICR×6 |

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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,764.83 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,579.67 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,338.50 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 216 | $5.77 | $2.79 | — | $5,089.39 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,850.53 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,603.95 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 714 | $1.75 | $9.21 | — | $1,345.24 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $186.91 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy,oppset; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
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
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,254.20 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 75 | $17.20 | $2.21 | — | $7,961.99 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,662.18 | — | combo gate; gate vol=good,ab=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 117 | $11.13 | $2.34 | — | $5,357.63 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,mover_buy,oppset; 🔵; ⚪; ret5=+39.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 528 | $2.47 | $6.81 | — | $4,046.66 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 676 | $1.93 | $8.72 | — | $2,733.26 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,477.08 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 989 | $1.32 | $12.76 | — | $158.85 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
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
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 101 | $13.59 | $2.29 | — | $9,635.86 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1376.34 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 37 | $36.96 | $2.10 | — | $8,266.24 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,oppset; 🔵; ret5=+4.4; leftover $1376.34 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 302 | $4.55 | $3.90 | — | $6,888.25 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1376.34 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 844 | $1.63 | $10.89 | — | $5,501.64 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1376.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 262 | $5.24 | $3.38 | — | $4,125.38 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+35.3; leftover $1376.34 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 882 | $1.56 | $11.38 | — | $2,738.08 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1376.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2219 | $0.62 | $20.41 | — | $1,341.89 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $1376.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 210 | $6.37 | $2.71 | — | $1.48 | — | combo gate; gate vol=good,ab=good; list yday_gainer,oppset; 🔵; ⚪; ret5=+10.9; leftover $1376.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.48 | ▲ close $11,011.66 vs 09:30 $11,010.75 (session +57.98) | 16:00 close · cash $1.48 · equity $11,011.66 vs 09:30 $11,010.75 (+0.91; session marks +57.98) · 8 name(s) marked open→close (per-name table). KURA×101 09:30 $13.59 → close $13.59 +0.00; LIFE×37 09:30 $36.96 → close $38.56 +59.20; ZIP×302 09:30 $4.55 → close $4.35 -60.40; BMEA×844 09:30 $1.63 → close $1.73 +84.40; ALVO×262 09:30 $5.24 → close $5.05 -49.78; CYPH×882 09:30 $1.56 → close $1.64 +70.56; DEFT×2219 09:30 $0.62 → close $0.60 -35.50; ZURA×210 09:30 $6.37 → close $6.32 -10.50 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.48 | ▼ 09:30 equity $10,906.05 vs yday $11,011.66 (-105.61) | 09:30 open · cash $1.48 (unchanged overnight, no fees) · equity $10,906.05 vs prior close $11,011.66 (-105.61) · 8 name(s) re-marked at the open (per-name table). KURA×101 yday $13.59 → 09:30 $13.63 +4.04; LIFE×37 yday $38.56 → 09:30 $38.24 -11.84; ZIP×302 yday $4.35 → 09:30 $4.31 -12.08; BMEA×844 yday $1.73 → 09:30 $1.75 +21.10; ALVO×262 yday $5.05 → 09:30 $4.98 -18.34; CYPH×882 yday $1.64 → 09:30 $1.60 -35.28; DEFT×2219 yday $0.60 → 09:30 $0.60 -13.31; ZURA×210 yday $6.32 → 09:30 $6.13 -39.90 | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 101 | $13.63 | $2.32 | $-0.57 | $1,375.79 | ▼ -0.57 after sell → book $10,903.73; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 37 | $38.24 | $2.12 | $+43.14 | $2,788.54 | ▲ +43.14 after sell → book $10,901.61; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 302 | $4.31 | $3.96 | $-80.33 | $4,086.21 | ▼ -80.33 after sell → book $10,897.65; vs 09:30 mark -3.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 844 | $1.75 | $11.04 | $+83.57 | $5,556.39 | ▲ +83.57 after sell → book $10,886.61; vs 09:30 mark -11.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 262 | $4.98 | $3.43 | $-74.93 | $6,857.71 | ▼ -74.93 after sell → book $10,883.18; vs 09:30 mark -3.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 882 | $1.60 | $11.54 | $+12.37 | $8,257.38 | ▲ +12.37 after sell → book $10,871.64; vs 09:30 mark -11.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DEFT` | 2219 | $0.60 | $20.31 | $-89.54 | $9,564.04 | ▼ -89.54 after sell → book $10,851.34; vs 09:30 mark -20.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 210 | $6.13 | $2.75 | $-55.86 | $10,848.58 | ▼ -55.86 after sell → book $10,848.58; vs 09:30 mark -2.76 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 9304 | $0.58 | $82.15 | — | $5,342.19 | — | combo gate; gate vol=good,ab=good; list yday_mover; 🔵; ret5=-27.5; leftover $5424.29 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `DKS` | 43 | $121.87 | $2.12 | — | $99.67 | — | combo gate; gate vol=good,ab=good; list yday_mover,oppset; 🔵; ret5=-35.1; leftover $5424.29 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.67 | ▲ close $10,792.25 vs 09:30 $10,906.05 (session +27.94) | 16:00 close · cash $99.67 · equity $10,792.25 vs 09:30 $10,906.05 (-113.80; session marks +27.94) · 2 name(s) marked open→close (per-name table). SLQT×9304 09:30 $0.58 → close $0.55 -307.03; DKS×43 09:30 $121.87 → close $129.66 +334.97 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.67 | ▼ 09:30 equity $10,566.18 vs yday $10,792.25 (-226.07) | 09:30 open · cash $99.67 (unchanged overnight, no fees) · equity $10,566.18 vs prior close $10,792.25 (-226.07) · 2 name(s) re-marked at the open (per-name table). SLQT×9304 yday $0.55 → 09:30 $0.53 -186.08; DKS×43 yday $129.66 → 09:30 $128.73 -39.99 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.67 | ▲ close $10,789.94 vs 09:30 $10,566.18 (session +223.76) | 16:00 close · cash $99.67 · equity $10,789.94 vs 09:30 $10,566.18 (+223.76; session marks +223.76) · 2 name(s) marked open→close (per-name table). SLQT×9304 09:30 $0.53 → close $0.54 +93.04; DKS×43 09:30 $128.73 → close $131.77 +130.72 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.67 | ▼ 09:30 equity $10,750.49 vs yday $10,789.94 (-39.45) | 09:30 open · cash $99.67 (unchanged overnight, no fees) · equity $10,750.49 vs prior close $10,789.94 (-39.45) · 2 name(s) re-marked at the open (per-name table). SLQT×9304 yday $0.54 → 09:30 $0.53 -83.74; DKS×43 yday $131.77 → 09:30 $132.80 +44.29 | — |
| 2026-08-28 09:30 ET | **SELL** | `SLQT` | 9304 | $0.53 | $78.90 | $-644.86 | $4,961.19 | ▼ -644.86 after sell → book $10,671.59; vs 09:30 mark -78.90 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 43 | $132.80 | $2.17 | $+465.70 | $10,669.41 | ▲ +465.70 after sell → book $10,669.41; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 1) | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $79.42 | $2.04 | — | $9,396.66 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,oppset; 🔵; ret5=+8.5; leftover $1333.68 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $8,080.01 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,oppset; 🔵; ret5=+38.8; leftover $1333.68 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 73 | $18.15 | $2.21 | — | $6,752.85 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,oppset; ret5=+14.1; leftover $1333.68 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $5,474.99 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $1333.68 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 71 | $18.75 | $2.20 | — | $4,141.54 | — | combo gate; gate vol=good,ab=good; list yday_gainer,oppset; ret5=-5.0; leftover $1333.68 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CRDL` | 647 | $2.06 | $8.35 | — | $2,800.37 | — | combo gate; gate vol=good,ab=good; list yday_gainer; ret5=+9.3; leftover $1333.68 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 57 | $23.30 | $2.16 | — | $1,470.11 | — | combo gate; gate vol=good,ab=good; list ohlc_hot; 🔵; ret5=+14.5; leftover $1333.68 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 70 | $19.00 | $2.20 | — | $137.91 | — | combo gate; gate vol=good,ab=good; list ohlc_hot; ret5=+7.5; leftover $1333.68 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $137.91 | ▼ close $10,479.79 vs 09:30 $10,750.49 (session -166.43) | 16:00 close · cash $137.91 · equity $10,479.79 vs 09:30 $10,750.49 (-270.70; session marks -166.43) · 8 name(s) marked open→close (per-name table). URBN×16 09:30 $79.42 → close $81.09 +26.72; ANF×9 09:30 $146.07 → close $148.42 +21.15; BZ×73 09:30 $18.15 → close $17.80 -25.55; SMTC×9 09:30 $141.76 → close $131.17 -95.31; BBWI×71 09:30 $18.75 → close $19.22 +33.37; CRDL×647 09:30 $2.06 → close $1.94 -77.64; NCNO×57 09:30 $23.30 → close $22.99 -17.67; TH×70 09:30 $19.00 → close $18.55 -31.50 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.91 | ▼ 09:30 equity $10,409.38 vs yday $10,479.79 (-70.41) | 09:30 open · cash $137.91 (unchanged overnight, no fees) · equity $10,409.38 vs prior close $10,479.79 (-70.41) · 8 name(s) re-marked at the open (per-name table). URBN×16 yday $81.09 → 09:30 $80.44 -10.40; ANF×9 yday $148.42 → 09:30 $148.03 -3.51; BZ×73 yday $17.80 → 09:30 $17.70 -7.30; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; BBWI×71 yday $19.22 → 09:30 $19.25 +2.13; CRDL×647 yday $1.94 → 09:30 $1.92 -12.94; NCNO×57 yday $22.99 → 09:30 $22.66 -18.81; TH×70 yday $18.55 → 09:30 $18.12 -29.75 | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $80.44 | $2.06 | $+12.22 | $1,422.90 | ▲ +12.22 after sell → book $10,407.33; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $2,753.13 | ▲ +13.59 after sell → book $10,405.29; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 73 | $17.70 | $2.23 | $-37.29 | $4,043.00 | ▼ -37.29 after sell → book $10,403.06; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $5,231.66 | ▼ -89.19 after sell → book $10,401.02; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 71 | $19.25 | $2.23 | $+31.07 | $6,596.18 | ▲ +31.07 after sell → book $10,398.79; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRDL` | 647 | $1.92 | $8.46 | $-107.39 | $7,829.96 | ▼ -107.39 after sell → book $10,390.33; vs 09:30 mark -8.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 57 | $22.66 | $2.18 | $-40.82 | $9,119.40 | ▼ -40.82 after sell → book $10,388.15; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,119.40 | ▲ close $10,415.80 vs 09:30 $10,409.38 (session +27.65) | 16:00 close · cash $9,119.40 · equity $10,415.80 vs 09:30 $10,409.38 (+6.42; session marks +27.65) · 1 name(s) marked open→close (per-name table). TH×70 09:30 $18.12 → close $18.52 +27.65 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,119.40 | ▼ 09:30 equity $10,410.90 vs yday $10,415.80 (-4.90) | 09:30 open · cash $9,119.40 (unchanged overnight, no fees) · equity $10,410.90 vs prior close $10,415.80 (-4.90) · 1 name(s) re-marked at the open (per-name table). TH×70 yday $18.52 → 09:30 $18.45 -4.90 | — |
| 2026-09-01 09:30 ET | **SELL** | `TH` | 70 | $18.45 | $2.22 | $-42.92 | $10,408.68 | ▼ -42.92 after sell → book $10,408.68; vs 09:30 mark -2.22 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,408.68 | ▲ close $10,408.68 vs 09:30 $10,410.90 (session +0.00) | 16:00 close · cash $10,408.68 · no lots left · equity $10,408.68. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,408.68 | ▲ 09:30 equity $10,408.68 vs yday $10,408.68 (-0.00) | 09:30 open · cash $10,408.68 · no holdings · equity $10,408.68 vs prior close $10,408.68 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,408.68 | ▲ close $10,408.68 vs 09:30 $10,408.68 (session +0.00) | 16:00 close · cash $10,408.68 · no lots left · equity $10,408.68. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,408.68 | ▲ 09:30 equity $10,408.68 vs yday $10,408.68 (-0.00) | 09:30 open · cash $10,408.68 · no holdings · equity $10,408.68 vs prior close $10,408.68 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $9,214.61 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1301.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 84 | $15.45 | $2.24 | — | $7,914.57 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1301.08 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $6,744.99 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1301.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 77 | $16.77 | $2.22 | — | $5,451.48 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy,oppset; 🔵; ⚪; ret5=+5.7; leftover $1301.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 23 | $55.42 | $2.06 | — | $4,174.76 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,oppset; ret5=-25.9; leftover $1301.08 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 596 | $2.18 | $7.69 | — | $2,867.79 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1301.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 54 | $23.88 | $2.15 | — | $1,576.12 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+21.9; leftover $1301.08 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 124 | $10.42 | $2.36 | — | $281.68 | — | combo gate; gate vol=good,ab=good; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1301.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $281.68 | ▼ close $10,254.01 vs 09:30 $10,408.68 (session -131.91) | 16:00 close · cash $281.68 · equity $10,254.01 vs 09:30 $10,408.68 (-154.67; session marks -131.91) · 8 name(s) marked open→close (per-name table). RVTY×9 09:30 $132.45 → close $130.63 -16.38; CRK×84 09:30 $15.45 → close $14.95 -42.00; MRNA×8 09:30 $145.94 → close $148.87 +23.40; ARCT×77 09:30 $16.77 → close $15.56 -93.17; EIX×23 09:30 $55.42 → close $56.30 +20.24; CRDL×596 09:30 $2.18 → close $2.16 -11.92; MMED×54 09:30 $23.88 → close $23.84 -2.16; NVAX×124 09:30 $10.42 → close $10.34 -9.92 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $281.68 | ▲ 09:30 equity $10,302.77 vs yday $10,254.01 (+48.76) | 09:30 open · cash $281.68 (unchanged overnight, no fees) · equity $10,302.77 vs prior close $10,254.01 (+48.76) · 8 name(s) re-marked at the open (per-name table). RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; CRK×84 yday $14.95 → 09:30 $15.00 +4.20; MRNA×8 yday $148.87 → 09:30 $153.62 +38.00; ARCT×77 yday $15.56 → 09:30 $15.61 +3.85; EIX×23 yday $56.30 → 09:30 $55.79 -11.73; CRDL×596 yday $2.16 → 09:30 $2.16 +0.00; MMED×54 yday $23.84 → 09:30 $23.84 +0.00; NVAX×124 yday $10.34 → 09:30 $10.50 +19.84 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $1,449.91 | ▼ -25.83 after sell → book $10,300.73; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 84 | $15.00 | $2.27 | $-42.31 | $2,707.65 | ▼ -42.31 after sell → book $10,298.47; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $153.62 | $2.03 | $+57.35 | $3,934.57 | ▲ +57.35 after sell → book $10,296.43; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 77 | $15.61 | $2.24 | $-93.78 | $5,134.30 | ▼ -93.78 after sell → book $10,294.19; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 23 | $55.79 | $2.08 | $+4.37 | $6,415.39 | ▲ +4.37 after sell → book $10,292.11; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 596 | $2.16 | $7.80 | $-27.41 | $7,694.95 | ▼ -27.41 after sell → book $10,284.31; vs 09:30 mark -7.80 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 54 | $23.84 | $2.17 | $-6.48 | $8,980.14 | ▼ -6.48 after sell → book $10,282.14; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 124 | $10.50 | $2.39 | $+5.16 | $10,279.75 | ▲ +5.16 after sell → book $10,279.75; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 371 | $3.46 | $4.79 | — | $8,991.30 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1284.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 509 | $2.52 | $6.57 | — | $7,702.06 | — | combo gate; gate vol=good,ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1284.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 191 | $6.71 | $2.56 | — | $6,417.88 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1284.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 676 | $1.90 | $8.72 | — | $5,124.76 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1284.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 268 | $4.78 | $3.46 | — | $3,840.27 | — | combo gate; gate vol=good,ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1284.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 113 | $11.31 | $2.33 | — | $2,559.91 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1284.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $1,530.35 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy,oppset; 🔵; ⚪; ret5=+9.3; leftover $1284.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 45 | $28.00 | $2.12 | — | $268.23 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+8.7; leftover $1284.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $268.23 | ▼ close $10,197.65 vs 09:30 $10,302.77 (session -49.56) | 16:00 close · cash $268.23 · equity $10,197.65 vs 09:30 $10,302.77 (-105.12; session marks -49.56) · 8 name(s) marked open→close (per-name table). CABA×371 09:30 $3.46 → close $3.47 +3.71; ALEC×509 09:30 $2.52 → close $2.46 -30.54; BHC×191 09:30 $6.71 → close $6.56 -28.65; BMEA×676 09:30 $1.90 → close $2.03 +87.88; OABI×268 09:30 $4.78 → close $4.33 -120.60; VIR×113 09:30 $11.31 → close $11.38 +8.47; DELL×2 09:30 $513.78 → close $524.14 +20.72; MLYS×45 09:30 $28.00 → close $28.21 +9.45 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $268.23 | ▼ 09:30 equity $10,082.96 vs yday $10,197.65 (-114.69) | 09:30 open · cash $268.23 (unchanged overnight, no fees) · equity $10,082.96 vs prior close $10,197.65 (-114.69) · 8 name(s) re-marked at the open (per-name table). CABA×371 yday $3.47 → 09:30 $3.43 -14.84; ALEC×509 yday $2.46 → 09:30 $2.38 -40.72; BHC×191 yday $6.56 → 09:30 $6.57 +1.91; BMEA×676 yday $2.03 → 09:30 $2.00 -20.28; OABI×268 yday $4.33 → 09:30 $4.30 -8.04; VIR×113 yday $11.38 → 09:30 $11.22 -18.64; DELL×2 yday $524.14 → 09:30 $521.15 -5.98; MLYS×45 yday $28.21 → 09:30 $28.03 -8.10 | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 371 | $3.43 | $4.86 | $-20.77 | $1,535.90 | ▼ -20.77 after sell → book $10,078.10; vs 09:30 mark -4.86 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 509 | $2.38 | $6.66 | $-84.49 | $2,740.66 | ▼ -84.49 after sell → book $10,071.44; vs 09:30 mark -6.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 191 | $6.57 | $2.60 | $-31.91 | $3,992.92 | ▼ -31.91 after sell → book $10,068.83; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 676 | $2.00 | $8.84 | $+50.04 | $5,336.08 | ▲ +50.04 after sell → book $10,059.99; vs 09:30 mark -8.84 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 268 | $4.30 | $3.51 | $-135.61 | $6,484.97 | ▼ -135.61 after sell → book $10,056.48; vs 09:30 mark -3.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 113 | $11.22 | $2.36 | $-14.86 | $7,750.47 | ▼ -14.86 after sell → book $10,054.12; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $8,790.75 | ▲ +10.73 after sell → book $10,052.10; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MLYS` | 45 | $28.03 | $2.15 | $-2.92 | $10,049.96 | ▼ -2.92 after sell → book $10,049.96; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,049.96 | ▲ close $10,049.96 vs 09:30 $10,082.96 (session +0.00) | 16:00 close · cash $10,049.96 · no lots left · equity $10,049.96. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,049.96 | ▲ 09:30 equity $10,049.96 vs yday $10,049.96 (-0.00) | 09:30 open · cash $10,049.96 · no holdings · equity $10,049.96 vs prior close $10,049.96 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,049.96 | ▲ close $10,049.96 vs 09:30 $10,049.96 (session +0.00) | 16:00 close · cash $10,049.96 · no lots left · equity $10,049.96. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,049.96 | ▲ 09:30 equity $10,049.96 vs yday $10,049.96 (-0.00) | 09:30 open · cash $10,049.96 · no holdings · equity $10,049.96 vs prior close $10,049.96 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,049.96 | ▲ close $10,049.96 vs 09:30 $10,049.96 (session +0.00) | 16:00 close · cash $10,049.96 · no lots left · equity $10,049.96. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,049.96 | ▲ 09:30 equity $10,049.96 vs yday $10,049.96 (-0.00) | 09:30 open · cash $10,049.96 · no holdings · equity $10,049.96 vs prior close $10,049.96 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $8,896.94 | — | combo gate; gate vol=good,ab=good; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1256.24 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 16 | $77.33 | $2.04 | — | $7,657.62 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; ret5=+2.5; leftover $1256.24 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 465 | $2.70 | $6.00 | — | $6,396.12 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1256.24 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 401 | $3.13 | $5.17 | — | $5,135.82 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+24.2; leftover $1256.24 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 114 | $10.95 | $2.33 | — | $3,885.19 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,oppset; 🔵; ret5=+20.8; leftover $1256.24 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 255 | $4.91 | $3.29 | — | $2,629.85 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1256.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 14 | $84.27 | $2.03 | — | $1,448.03 | — | combo gate; gate vol=good,ab=good; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1256.24 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ANGX` | 233 | $5.38 | $3.01 | — | $191.49 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=+19.8; leftover $1256.24 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $191.49 | ▲ close $10,071.39 vs 09:30 $10,049.96 (session +47.32) | 16:00 close · cash $191.49 · equity $10,071.39 vs 09:30 $10,049.96 (+21.43; session marks +47.32) · 8 name(s) marked open→close (per-name table). ORCL×7 09:30 $164.43 → close $150.28 -99.05; VIST×16 09:30 $77.33 → close $76.27 -16.96; INDP×465 09:30 $2.70 → close $2.77 +32.55; CMRC×401 09:30 $3.13 → close $3.50 +150.38; WLTH×114 09:30 $10.95 → close $10.38 -64.98; BNC×255 09:30 $4.91 → close $4.80 -28.05; SWKS×14 09:30 $84.27 → close $88.35 +57.12; ANGX×233 09:30 $5.38 → close $5.45 +16.31 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $191.49 | ▲ 09:30 equity $10,082.90 vs yday $10,071.39 (+11.51) | 09:30 open · cash $191.49 (unchanged overnight, no fees) · equity $10,082.90 vs prior close $10,071.39 (+11.51) · 8 name(s) re-marked at the open (per-name table). ORCL×7 yday $150.28 → 09:30 $141.42 -62.02; VIST×16 yday $76.27 → 09:30 $77.10 +13.28; INDP×465 yday $2.77 → 09:30 $2.80 +13.95; CMRC×401 yday $3.50 → 09:30 $3.51 +2.00; WLTH×114 yday $10.38 → 09:30 $10.29 -10.26; BNC×255 yday $4.80 → 09:30 $5.03 +58.65; SWKS×14 yday $88.35 → 09:30 $86.06 -32.06; ANGX×233 yday $5.45 → 09:30 $5.57 +27.96 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 7 | $141.42 | $2.03 | $-165.11 | $1,179.40 | ▼ -165.11 after sell → book $10,080.87; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-14 09:30 ET | **SELL** | `VIST` | 16 | $77.10 | $2.06 | $-7.78 | $2,410.94 | ▼ -7.78 after sell → book $10,078.81; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CMRC` | 401 | $3.51 | $5.25 | $+141.96 | $3,813.20 | ▲ +141.96 after sell → book $10,073.56; vs 09:30 mark -5.25 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 114 | $10.29 | $2.36 | $-79.93 | $4,983.90 | ▼ -79.93 after sell → book $10,071.20; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 255 | $5.03 | $3.34 | $+23.97 | $6,263.21 | ▲ +23.97 after sell → book $10,067.86; vs 09:30 mark -3.34 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 14 | $86.06 | $2.05 | $+20.98 | $7,465.99 | ▲ +20.98 after sell → book $10,065.80; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ANGX` | 233 | $5.57 | $3.05 | $+38.21 | $8,760.75 | ▲ +38.21 after sell → book $10,062.75; vs 09:30 mark -3.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,760.75 | ▲ close $10,220.85 vs 09:30 $10,082.90 (session +158.10) | 16:00 close · cash $8,760.75 · equity $10,220.85 vs 09:30 $10,082.90 (+137.95; session marks +158.10) · 1 name(s) marked open→close (per-name table). INDP×465 09:30 $2.80 → close $3.14 +158.10 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,760.75 | ▲ 09:30 equity $10,341.75 vs yday $10,220.85 (+120.90) | 09:30 open · cash $8,760.75 (unchanged overnight, no fees) · equity $10,341.75 vs prior close $10,220.85 (+120.90) · 1 name(s) re-marked at the open (per-name table). INDP×465 yday $3.14 → 09:30 $3.40 +120.90 | — |
| 2026-09-15 09:30 ET | **SELL** | `INDP` | 465 | $3.40 | $6.09 | $+313.41 | $10,335.66 | ▲ +313.41 after sell → book $10,335.66; vs 09:30 mark -6.09 | dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,335.66 | ▲ close $10,335.66 vs 09:30 $10,341.75 (session +0.00) | 16:00 close · cash $10,335.66 · no lots left · equity $10,335.66. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,335.66 | ▲ 09:30 equity $10,335.66 vs yday $10,335.66 (+0.00) | 09:30 open · cash $10,335.66 · no holdings · equity $10,335.66 vs prior close $10,335.66 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 16 | $77.12 | $2.04 | — | $9,099.70 | — | combo gate; gate vol=good,ab=good; list flatten,ohlc_hot; ret5=+7.2; leftover $1291.96 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 220 | $5.87 | $2.84 | — | $7,805.46 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1291.96 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 catal🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 14 | $87.40 | $2.03 | — | $6,579.83 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1291.96 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 47 | $27.09 | $2.13 | — | $5,304.47 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1291.96 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 14 | $89.38 | $2.03 | — | $4,051.12 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1291.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 55 | $23.29 | $2.15 | — | $2,768.01 | — | combo gate; gate vol=good,ab=good; list yday_gainer,oppset; 🔵; ret5=+16.1; leftover $1291.96 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 38 | $33.14 | $2.10 | — | $1,506.59 | — | combo gate; gate vol=good,ab=good; list yday_gainer,oppset; 🔵; ret5=-2.9; leftover $1291.96 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 45 | $28.16 | $2.12 | — | $237.27 | — | combo gate; gate vol=good,ab=good; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $1291.96 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $237.27 | ▼ close $10,232.10 vs 09:30 $10,335.66 (session -86.11) | 16:00 close · cash $237.27 · equity $10,232.10 vs 09:30 $10,335.66 (-103.56; session marks -86.11) · 8 name(s) marked open→close (per-name table). RDNT×16 09:30 $77.12 → close $75.78 -21.44; RIG×220 09:30 $5.87 → close $5.54 -72.60; VAL×14 09:30 $87.40 → close $82.52 -68.32; ADPT×47 09:30 $27.09 → close $27.67 +27.26; SWKS×14 09:30 $89.38 → close $85.59 -53.06; SDGR×55 09:30 $23.29 → close $23.93 +35.20; FPS×38 09:30 $33.14 → close $34.84 +64.60; CAI×45 09:30 $28.16 → close $28.21 +2.25 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $237.27 | ▲ 09:30 equity $10,402.76 vs yday $10,232.10 (+170.66) | 09:30 open · cash $237.27 (unchanged overnight, no fees) · equity $10,402.76 vs prior close $10,232.10 (+170.66) · 8 name(s) re-marked at the open (per-name table). RDNT×16 yday $75.78 → 09:30 $76.44 +10.56; RIG×220 yday $5.54 → 09:30 $5.58 +8.80; VAL×14 yday $82.52 → 09:30 $83.20 +9.52; ADPT×47 yday $27.67 → 09:30 $28.23 +26.32; SWKS×14 yday $85.59 → 09:30 $86.76 +16.38; SDGR×55 yday $23.93 → 09:30 $24.09 +8.80; FPS×38 yday $34.84 → 09:30 $36.76 +72.96; CAI×45 yday $28.21 → 09:30 $28.59 +17.32 | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 16 | $76.44 | $2.06 | $-14.98 | $1,458.25 | ▼ -14.98 after sell → book $10,400.70; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 220 | $5.58 | $2.88 | $-69.52 | $2,682.96 | ▼ -69.52 after sell → book $10,397.82; vs 09:30 mark -2.88 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 14 | $83.20 | $2.05 | $-62.88 | $3,845.71 | ▼ -62.88 after sell → book $10,395.77; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 47 | $28.23 | $2.15 | $+49.30 | $5,170.37 | ▲ +49.30 after sell → book $10,393.61; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 14 | $86.76 | $2.05 | $-40.76 | $6,382.96 | ▼ -40.76 after sell → book $10,391.56; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 55 | $24.09 | $2.18 | $+39.67 | $7,705.73 | ▲ +39.67 after sell → book $10,389.39; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CAI` | 45 | $28.59 | $2.15 | $+15.30 | $8,990.36 | ▲ +15.30 after sell → book $10,387.24; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 5 | $233.85 | $2.00 | — | $7,819.11 | — | combo gate; gate vol=good,ab=good; list flatten,ohlc_hot; ret5=+11.7; leftover $1284.34 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 8 | $147.61 | $2.01 | — | $6,636.21 | — | combo gate; gate vol=good,ab=good; list flatten,ohlc_hot; ret5=+17.7; leftover $1284.34 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 169 | $7.59 | $2.50 | — | $5,351.01 | — | combo gate; gate vol=good,ab=good; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1284.34 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 49 | $25.95 | $2.14 | — | $4,077.32 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,ohlc_hot,oppset; 🔵; ret5=+9.6; leftover $1284.34 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $2,879.36 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1284.34 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 535 | $2.40 | $6.90 | — | $1,588.46 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1284.34 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `CYPH` | 480 | $2.67 | $6.19 | — | $298.26 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=-0.4; leftover $1284.34 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $298.26 | ▲ close $10,732.98 vs 09:30 $10,402.76 (session +369.50) | 16:00 close · cash $298.26 · equity $10,732.98 vs 09:30 $10,402.76 (+330.22; session marks +369.50) · 8 name(s) marked open→close (per-name table). FPS×38 09:30 $36.76 → close $38.06 +49.40; ILMN×5 09:30 $233.85 → close $245.18 +56.65; RVTY×8 09:30 $147.61 → close $146.73 -7.04; PGEN×169 09:30 $7.59 → close $7.87 +47.32; ARQT×49 09:30 $25.95 → close $26.46 +24.99; SMTC×7 09:30 $170.85 → close $178.19 +51.38; SABR×535 09:30 $2.40 → close $2.32 -42.80; CYPH×480 09:30 $2.67 → close $3.07 +189.60 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $298.26 | ▲ 09:30 equity $10,804.65 vs yday $10,732.98 (+71.67) | 09:30 open · cash $298.26 (unchanged overnight, no fees) · equity $10,804.65 vs prior close $10,732.98 (+71.67) · 8 name(s) re-marked at the open (per-name table). FPS×38 yday $38.06 → 09:30 $39.50 +54.72; ILMN×5 yday $245.18 → 09:30 $249.13 +19.75; RVTY×8 yday $146.73 → 09:30 $146.50 -1.84; PGEN×169 yday $7.87 → 09:30 $7.98 +18.59; ARQT×49 yday $26.46 → 09:30 $26.14 -15.68; SMTC×7 yday $178.19 → 09:30 $182.33 +28.98; SABR×535 yday $2.32 → 09:30 $2.29 -16.05; CYPH×480 yday $3.07 → 09:30 $3.04 -16.80 | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 38 | $39.50 | $2.13 | $+237.45 | $1,797.14 | ▲ +237.45 after sell → book $10,802.53; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 8 | $146.50 | $2.03 | $-12.93 | $2,967.10 | ▼ -12.93 after sell → book $10,800.49; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 169 | $7.98 | $2.54 | $+60.88 | $4,313.19 | ▲ +60.88 after sell → book $10,797.96; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $5,587.47 | ▲ +76.32 after sell → book $10,795.93; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 535 | $2.29 | $7.00 | $-72.75 | $6,805.62 | ▼ -72.75 after sell → book $10,788.93; vs 09:30 mark -7.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 46 | $29.32 | $2.13 | — | $5,454.77 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $1361.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FTRE` | 67 | $20.10 | $2.19 | — | $4,105.88 | — | combo gate; gate vol=good,ab=good; list flatten; 🔵; ⚪; ret5=+19.2; leftover $1361.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 92 | $14.79 | $2.27 | — | $2,742.93 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1361.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 6 | $209.52 | $2.01 | — | $1,483.80 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,oppset; 🔵; ret5=+14.1; leftover $1361.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 6 | $219.62 | $2.01 | — | $164.08 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1361.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $164.08 | ▲ close $10,919.91 vs 09:30 $10,804.65 (session +141.58) | 16:00 close · cash $164.08 · equity $10,919.91 vs 09:30 $10,804.65 (+115.26; session marks +141.58) · 8 name(s) marked open→close (per-name table). ILMN×5 09:30 $249.13 → close $239.62 -47.55; ARQT×49 09:30 $26.14 → close $25.38 -37.24; CYPH×480 09:30 $3.04 → close $3.60 +271.20; SDGR×46 09:30 $29.32 → close $29.02 -13.80; FTRE×67 09:30 $20.10 → close $19.93 -11.39; RARE×92 09:30 $14.79 → close $14.51 -25.76; GNRC×6 09:30 $209.52 → close $207.44 -12.48; VICR×6 09:30 $219.62 → close $222.72 +18.60 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `QSI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BTBT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SID` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RPD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ILMN` | 5 | 2026-09-17 @ $233.85 | combo gate; gate vol=good,ab=good; list flatten,ohlc_hot; ret5=+11.7; leftover $1284.34 |
| `ARQT` | 49 | 2026-09-17 @ $25.95 | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,ohlc_hot,oppset; 🔵; ret5=+9.6; leftover $1284.34 |
| `CYPH` | 480 | 2026-09-17 @ $2.67 | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=-0.4; leftover $1284.34 |
| `SDGR` | 46 | 2026-09-18 @ $29.32 | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $1361.12 |
| `FTRE` | 67 | 2026-09-18 @ $20.10 | combo gate; gate vol=good,ab=good; list flatten; 🔵; ⚪; ret5=+19.2; leftover $1361.12 |
| `RARE` | 92 | 2026-09-18 @ $14.79 | combo gate; gate vol=good,ab=good; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1361.12 |
| `GNRC` | 6 | 2026-09-18 @ $209.52 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,oppset; 🔵; ret5=+14.1; leftover $1361.12 |
| `VICR` | 6 | 2026-09-18 @ $219.62 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1361.12 |
