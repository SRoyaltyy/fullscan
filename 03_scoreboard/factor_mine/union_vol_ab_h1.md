# Factor mine action — `union_vol_ab_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-0.97%** ($9,903) · signal-only (no cash/fees) was +45.27%. Starts YES **4/21**. Fills 108 · skips 33 · realized $-127.05.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $127.99.

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
| 2026-08-27 | `SLQT` | 9304 | $0.55 | $0.53 | -186.08 | — | +0.00 | -186.08 | -493.11 | — |
| 2026-08-27 | `DKS` | 43 | $129.66 | $128.73 | -39.99 | — | +0.00 | -39.99 | +294.98 | — |
| 2026-08-28 | `URBN` | 16 | — | $79.42 | +0.00 | $81.09 | +26.72 | +26.72 | +0.00 | +26.72 |
| 2026-08-28 | `ANF` | 8 | — | $146.07 | +0.00 | $148.42 | +18.80 | +18.80 | +0.00 | +18.80 |
| 2026-08-28 | `BZ` | 72 | — | $18.15 | +0.00 | $17.80 | -25.20 | -25.20 | +0.00 | -25.20 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `BBWI` | 69 | — | $18.75 | +0.00 | $19.22 | +32.43 | +32.43 | +0.00 | +32.43 |
| 2026-08-28 | `CRDL` | 636 | — | $2.06 | +0.00 | $1.94 | -76.32 | -76.32 | +0.00 | -76.32 |
| 2026-08-28 | `NCNO` | 56 | — | $23.30 | +0.00 | $22.99 | -17.36 | -17.36 | +0.00 | -17.36 |
| 2026-08-28 | `TH` | 68 | — | $19.00 | +0.00 | $18.55 | -30.60 | -30.60 | +0.00 | -30.60 |
| 2026-08-31 | `URBN` | 16 | $81.09 | $80.44 | -10.40 | — | +0.00 | -10.40 | +16.32 | — |
| 2026-08-31 | `ANF` | 8 | $148.42 | $148.03 | -3.12 | — | +0.00 | -3.12 | +15.68 | — |
| 2026-08-31 | `BZ` | 72 | $17.80 | $17.70 | -7.20 | — | +0.00 | -7.20 | -32.40 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `BBWI` | 69 | $19.22 | $19.25 | +2.07 | — | +0.00 | +2.07 | +34.50 | — |
| 2026-08-31 | `CRDL` | 636 | $1.94 | $1.92 | -12.72 | — | +0.00 | -12.72 | -89.04 | — |
| 2026-08-31 | `NCNO` | 56 | $22.99 | $22.66 | -18.48 | — | +0.00 | -18.48 | -35.84 | — |
| 2026-08-31 | `TH` | 68 | $18.55 | $18.12 | -28.90 | $18.52 | +26.86 | -2.04 | -59.50 | -32.64 |
| 2026-09-01 | `TH` | 68 | $18.52 | $18.45 | -4.76 | — | +0.00 | -4.76 | -37.40 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `CRK` | 82 | — | $15.45 | +0.00 | $14.95 | -41.00 | -41.00 | +0.00 | -41.00 |
| 2026-09-03 | `MRNA` | 8 | — | $145.94 | +0.00 | $148.87 | +23.40 | +23.40 | +0.00 | +23.40 |
| 2026-09-03 | `ARCT` | 76 | — | $16.77 | +0.00 | $15.56 | -91.96 | -91.96 | +0.00 | -91.96 |
| 2026-09-03 | `EIX` | 23 | — | $55.42 | +0.00 | $56.30 | +20.24 | +20.24 | +0.00 | +20.24 |
| 2026-09-03 | `CRDL` | 586 | — | $2.18 | +0.00 | $2.16 | -11.72 | -11.72 | +0.00 | -11.72 |
| 2026-09-03 | `MMED` | 53 | — | $23.88 | +0.00 | $23.84 | -2.12 | -2.12 | +0.00 | -2.12 |
| 2026-09-03 | `NVAX` | 122 | — | $10.42 | +0.00 | $10.34 | -9.76 | -9.76 | +0.00 | -9.76 |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `CRK` | 82 | $14.95 | $15.00 | +4.10 | — | +0.00 | +4.10 | -36.90 | — |
| 2026-09-04 | `MRNA` | 8 | $148.87 | $153.62 | +38.00 | — | +0.00 | +38.00 | +61.40 | — |
| 2026-09-04 | `ARCT` | 76 | $15.56 | $15.61 | +3.80 | — | +0.00 | +3.80 | -88.16 | — |
| 2026-09-04 | `EIX` | 23 | $56.30 | $55.79 | -11.73 | — | +0.00 | -11.73 | +8.51 | — |
| 2026-09-04 | `CRDL` | 586 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -11.72 | — |
| 2026-09-04 | `MMED` | 53 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.12 | — |
| 2026-09-04 | `NVAX` | 122 | $10.34 | $10.50 | +19.52 | — | +0.00 | +19.52 | +9.76 | — |
| 2026-09-04 | `CABA` | 364 | — | $3.46 | +0.00 | $3.47 | +3.64 | +3.64 | +0.00 | +3.64 |
| 2026-09-04 | `ALEC` | 500 | — | $2.52 | +0.00 | $2.46 | -30.00 | -30.00 | +0.00 | -30.00 |
| 2026-09-04 | `BHC` | 188 | — | $6.71 | +0.00 | $6.56 | -28.20 | -28.20 | +0.00 | -28.20 |
| 2026-09-04 | `BMEA` | 664 | — | $1.90 | +0.00 | $2.03 | +86.32 | +86.32 | +0.00 | +86.32 |
| 2026-09-04 | `OABI` | 264 | — | $4.78 | +0.00 | $4.33 | -118.80 | -118.80 | +0.00 | -118.80 |
| 2026-09-04 | `VIR` | 111 | — | $11.31 | +0.00 | $11.38 | +8.32 | +8.32 | +0.00 | +8.32 |
| 2026-09-04 | `DELL` | 2 | — | $513.78 | +0.00 | $524.14 | +20.72 | +20.72 | +0.00 | +20.72 |
| 2026-09-04 | `MLYS` | 45 | — | $28.00 | +0.00 | $28.21 | +9.45 | +9.45 | +0.00 | +9.45 |
| 2026-09-08 | `CABA` | 364 | $3.47 | $3.43 | -14.56 | — | +0.00 | -14.56 | -10.92 | — |
| 2026-09-08 | `ALEC` | 500 | $2.46 | $2.38 | -40.00 | — | +0.00 | -40.00 | -70.00 | — |
| 2026-09-08 | `BHC` | 188 | $6.56 | $6.57 | +1.88 | — | +0.00 | +1.88 | -26.32 | — |
| 2026-09-08 | `BMEA` | 664 | $2.03 | $2.00 | -19.92 | — | +0.00 | -19.92 | +66.40 | — |
| 2026-09-08 | `OABI` | 264 | $4.33 | $4.30 | -7.92 | — | +0.00 | -7.92 | -126.72 | — |
| 2026-09-08 | `VIR` | 111 | $11.38 | $11.22 | -18.31 | — | +0.00 | -18.31 | -9.99 | — |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | — | +0.00 | -5.98 | +14.74 | — |
| 2026-09-08 | `MLYS` | 45 | $28.21 | $28.03 | -8.10 | — | +0.00 | -8.10 | +1.35 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 7 | — | $164.43 | +0.00 | $150.28 | -99.05 | -99.05 | +0.00 | -99.05 |
| 2026-09-11 | `CMRC` | 394 | — | $3.13 | +0.00 | $3.50 | +147.75 | +147.75 | +0.00 | +147.75 |
| 2026-09-11 | `INDP` | 457 | — | $2.70 | +0.00 | $2.77 | +31.99 | +31.99 | +0.00 | +31.99 |
| 2026-09-11 | `WLTH` | 112 | — | $10.95 | +0.00 | $10.38 | -63.84 | -63.84 | +0.00 | -63.84 |
| 2026-09-11 | `BNC` | 251 | — | $4.91 | +0.00 | $4.80 | -27.61 | -27.61 | +0.00 | -27.61 |
| 2026-09-11 | `SWKS` | 14 | — | $84.27 | +0.00 | $88.35 | +57.12 | +57.12 | +0.00 | +57.12 |
| 2026-09-11 | `ANGX` | 229 | — | $5.38 | +0.00 | $5.45 | +16.03 | +16.03 | +0.00 | +16.03 |
| 2026-09-11 | `TSSI` | 137 | — | $8.98 | +0.00 | $8.93 | -6.85 | -6.85 | +0.00 | -6.85 |

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
| 2026-08-27 | — | $99.67 | SLQT×9304, DKS×43 | $10,566.18 | -226.07 | +0.00 | — | SLQT, DKS | $10,485.20 | $10,485.20 | — |
| 2026-08-28 | +0.75 | $10,485.20 | — | $10,485.20 | -0.00 | -166.84 | URBN, ANF, BZ, SMTC, BBWI, CRDL, NCNO, TH | — | $239.54 | $10,295.33 | URBN×16, ANF×8, BZ×72, SMTC×9, BBWI×69, CRDL×636, NCNO×56, TH×68 |
| 2026-08-31 | -5.85 | $239.54 | URBN×16, ANF×8, BZ×72, SMTC×9, BBWI×69, CRDL×636, NCNO×56, TH×68 | $10,226.75 | -68.58 | +26.86 | — | URBN, ANF, BZ, SMTC, BBWI, CRDL, NCNO | $8,973.17 | $10,232.53 | TH×68 |
| 2026-09-01 | -6.30 | $8,973.17 | TH×68 | $10,227.77 | -4.76 | +0.00 | — | TH | $10,225.56 | $10,225.56 | — |
| 2026-09-02 | -3.83 | $10,225.56 | — | $10,225.56 | -0.00 | +0.00 | — | — | $10,225.56 | $10,225.56 | — |
| 2026-09-03 | -0.90 | $10,225.56 | — | $10,225.56 | -0.00 | -129.30 | RVTY, CRK, MRNA, ARCT, EIX, CRDL, MMED, NVAX | — | $212.90 | $10,073.65 | RVTY×9, CRK×82, MRNA×8, ARCT×76, EIX×23, CRDL×586, MMED×53, NVAX×122 |
| 2026-09-04 | +2.25 | $212.90 | RVTY×9, CRK×82, MRNA×8, ARCT×76, EIX×23, CRDL×586, MMED×53, NVAX×122 | $10,121.94 | +48.29 | -48.55 | CABA, ALEC, BHC, BMEA, OABI, VIR, DELL, MLYS | RVTY, CRK, MRNA, ARCT, EIX, CRDL, MMED, NVAX | $219.54 | $10,018.41 | CABA×364, ALEC×500, BHC×188, BMEA×664, OABI×264, VIR×111, DELL×2, MLYS×45 |
| 2026-09-08 | -11.47 | $219.54 | CABA×364, ALEC×500, BHC×188, BMEA×664, OABI×264, VIR×111, DELL×2, MLYS×45 | $9,905.49 | -112.92 | +0.00 | — | CABA, ALEC, BHC, BMEA, OABI, VIR, DELL, MLYS | $9,872.93 | $9,872.93 | — |
| 2026-09-09 | -13.95 | $9,872.93 | — | $9,872.93 | -0.00 | +0.00 | — | — | $9,872.93 | $9,872.93 | — |
| 2026-09-10 | -13.28 | $9,872.93 | — | $9,872.93 | -0.00 | +0.00 | — | — | $9,872.93 | $9,872.93 | — |
| 2026-09-11 | +0.50 | $9,872.93 | — | $9,872.93 | -0.00 | +55.54 | ORCL, CMRC, INDP, WLTH, BNC, SWKS, ANGX, TSSI | — | $127.99 | $9,902.53 | ORCL×7, CMRC×394, INDP×457, WLTH×112, BNC×251, SWKS×14, ANGX×229, TSSI×137 |

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
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $186.91 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
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
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 117 | $11.13 | $2.34 | — | $5,357.63 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
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
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 37 | $36.96 | $2.10 | — | $8,266.24 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1376.34 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 302 | $4.55 | $3.90 | — | $6,888.25 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1376.34 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 844 | $1.63 | $10.89 | — | $5,501.64 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1376.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 262 | $5.24 | $3.38 | — | $4,125.38 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1376.34 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 882 | $1.56 | $11.38 | — | $2,738.08 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1376.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2219 | $0.62 | $20.41 | — | $1,341.89 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $1376.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 210 | $6.37 | $2.71 | — | $1.48 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1376.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.48 | ▲ close $11,011.66 vs 09:30 $11,010.75 (session +57.98) | 16:00 close · cash $1.48 · equity $11,011.66 vs 09:30 $11,010.75 (+0.91; session marks +57.98) · 8 name(s) marked open→close (per-name table). KURA×101 09:30 $13.59 → close $13.59 +0.00; LIFE×37 09:30 $36.96 → close $38.56 +59.20; ZIP×302 09:30 $4.55 → close $4.35 -60.40; BMEA×844 09:30 $1.63 → close $1.73 +84.40; ALVO×262 09:30 $5.24 → close $5.05 -49.78; CYPH×882 09:30 $1.56 → close $1.64 +70.56; DEFT×2219 09:30 $0.62 → close $0.60 -35.50; ZURA×210 09:30 $6.37 → close $6.32 -10.50 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.48 | ▼ 09:30 equity $10,906.05 vs yday $11,011.66 (-105.61) | 09:30 open · cash $1.48 (unchanged overnight, no fees) · equity $10,906.05 vs prior close $11,011.66 (-105.61) · 8 name(s) re-marked at the open (per-name table). KURA×101 yday $13.59 → 09:30 $13.63 +4.04; LIFE×37 yday $38.56 → 09:30 $38.24 -11.84; ZIP×302 yday $4.35 → 09:30 $4.31 -12.08; BMEA×844 yday $1.73 → 09:30 $1.75 +21.10; ALVO×262 yday $5.05 → 09:30 $4.98 -18.34; CYPH×882 yday $1.64 → 09:30 $1.60 -35.28; DEFT×2219 yday $0.60 → 09:30 $0.60 -13.31; ZURA×210 yday $6.32 → 09:30 $6.13 -39.90 | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 101 | $13.63 | $2.32 | $-0.57 | $1,375.79 | ▼ -0.57 after sell → book $10,903.73; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 37 | $38.24 | $2.12 | $+43.14 | $2,788.54 | ▲ +43.14 after sell → book $10,901.61; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 302 | $4.31 | $3.96 | $-80.33 | $4,086.21 | ▼ -80.33 after sell → book $10,897.65; vs 09:30 mark -3.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 844 | $1.75 | $11.04 | $+83.57 | $5,556.39 | ▲ +83.57 after sell → book $10,886.61; vs 09:30 mark -11.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 262 | $4.98 | $3.43 | $-74.93 | $6,857.71 | ▼ -74.93 after sell → book $10,883.18; vs 09:30 mark -3.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 882 | $1.60 | $11.54 | $+12.37 | $8,257.38 | ▲ +12.37 after sell → book $10,871.64; vs 09:30 mark -11.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DEFT` | 2219 | $0.60 | $20.31 | $-89.54 | $9,564.04 | ▼ -89.54 after sell → book $10,851.34; vs 09:30 mark -20.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 210 | $6.13 | $2.75 | $-55.86 | $10,848.58 | ▼ -55.86 after sell → book $10,848.58; vs 09:30 mark -2.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 9304 | $0.58 | $82.15 | — | $5,342.19 | — | combo gate; gate vol=good,ab=good; list yday_mover; 🔵; ret5=-27.5; leftover $5424.29 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `DKS` | 43 | $121.87 | $2.12 | — | $99.67 | — | combo gate; gate vol=good,ab=good; list yday_mover; 🔵; ret5=-35.1; leftover $5424.29 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.67 | ▲ close $10,792.25 vs 09:30 $10,906.05 (session +27.94) | 16:00 close · cash $99.67 · equity $10,792.25 vs 09:30 $10,906.05 (-113.80; session marks +27.94) · 2 name(s) marked open→close (per-name table). SLQT×9304 09:30 $0.58 → close $0.55 -307.03; DKS×43 09:30 $121.87 → close $129.66 +334.97 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.67 | ▼ 09:30 equity $10,566.18 vs yday $10,792.25 (-226.07) | 09:30 open · cash $99.67 (unchanged overnight, no fees) · equity $10,566.18 vs prior close $10,792.25 (-226.07) · 2 name(s) re-marked at the open (per-name table). SLQT×9304 yday $0.55 → 09:30 $0.53 -186.08; DKS×43 yday $129.66 → 09:30 $128.73 -39.99 | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 9304 | $0.53 | $78.81 | $-654.07 | $4,951.98 | ▼ -654.07 after sell → book $10,487.37; vs 09:30 mark -78.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 43 | $128.73 | $2.17 | $+290.69 | $10,485.20 | ▲ +290.69 after sell → book $10,485.20; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,485.20 | ▲ close $10,485.20 vs 09:30 $10,566.18 (session +0.00) | 16:00 close · cash $10,485.20 · no lots left · equity $10,485.20. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,485.20 | ▲ 09:30 equity $10,485.20 vs yday $10,485.20 (-0.00) | 09:30 open · cash $10,485.20 · no holdings · equity $10,485.20 vs prior close $10,485.20 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $79.42 | $2.04 | — | $9,212.44 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1310.65 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $8,041.86 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1310.65 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 72 | $18.15 | $2.21 | — | $6,732.86 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+14.1; leftover $1310.65 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $5,455.00 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1310.65 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 69 | $18.75 | $2.20 | — | $4,159.05 | — | combo gate; gate vol=good,ab=good; list yday_gainer; ret5=-5.0; leftover $1310.65 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CRDL` | 636 | $2.06 | $8.20 | — | $2,840.69 | — | combo gate; gate vol=good,ab=good; list yday_gainer; ret5=+9.3; leftover $1310.65 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 56 | $23.30 | $2.16 | — | $1,533.73 | — | combo gate; gate vol=good,ab=good; list ohlc_hot; 🔵; ret5=+14.5; leftover $1310.65 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 68 | $19.00 | $2.19 | — | $239.54 | — | combo gate; gate vol=good,ab=good; list ohlc_hot; ret5=+7.5; leftover $1310.65 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $239.54 | ▼ close $10,295.33 vs 09:30 $10,485.20 (session -166.84) | 16:00 close · cash $239.54 · equity $10,295.33 vs 09:30 $10,485.20 (-189.87; session marks -166.84) · 8 name(s) marked open→close (per-name table). URBN×16 09:30 $79.42 → close $81.09 +26.72; ANF×8 09:30 $146.07 → close $148.42 +18.80; BZ×72 09:30 $18.15 → close $17.80 -25.20; SMTC×9 09:30 $141.76 → close $131.17 -95.31; BBWI×69 09:30 $18.75 → close $19.22 +32.43; CRDL×636 09:30 $2.06 → close $1.94 -76.32; NCNO×56 09:30 $23.30 → close $22.99 -17.36; TH×68 09:30 $19.00 → close $18.55 -30.60 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $239.54 | ▼ 09:30 equity $10,226.75 vs yday $10,295.33 (-68.58) | 09:30 open · cash $239.54 (unchanged overnight, no fees) · equity $10,226.75 vs prior close $10,295.33 (-68.58) · 8 name(s) re-marked at the open (per-name table). URBN×16 yday $81.09 → 09:30 $80.44 -10.40; ANF×8 yday $148.42 → 09:30 $148.03 -3.12; BZ×72 yday $17.80 → 09:30 $17.70 -7.20; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; BBWI×69 yday $19.22 → 09:30 $19.25 +2.07; CRDL×636 yday $1.94 → 09:30 $1.92 -12.72; NCNO×56 yday $22.99 → 09:30 $22.66 -18.48; TH×68 yday $18.55 → 09:30 $18.12 -28.90 | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $80.44 | $2.06 | $+12.22 | $1,524.52 | ▲ +12.22 after sell → book $10,224.69; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.03 | $2.03 | $+11.63 | $2,706.72 | ▲ +11.63 after sell → book $10,222.65; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 72 | $17.70 | $2.23 | $-36.83 | $3,978.90 | ▼ -36.83 after sell → book $10,220.43; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $5,167.56 | ▼ -89.19 after sell → book $10,218.39; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 69 | $19.25 | $2.22 | $+30.08 | $6,493.59 | ▲ +30.08 after sell → book $10,216.17; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRDL` | 636 | $1.92 | $8.32 | $-105.56 | $7,706.39 | ▼ -105.56 after sell → book $10,207.85; vs 09:30 mark -8.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 56 | $22.66 | $2.18 | $-40.18 | $8,973.17 | ▼ -40.18 after sell → book $10,205.67; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,973.17 | ▲ close $10,232.53 vs 09:30 $10,226.75 (session +26.86) | 16:00 close · cash $8,973.17 · equity $10,232.53 vs 09:30 $10,226.75 (+5.78; session marks +26.86) · 1 name(s) marked open→close (per-name table). TH×68 09:30 $18.12 → close $18.52 +26.86 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,973.17 | ▼ 09:30 equity $10,227.77 vs yday $10,232.53 (-4.76) | 09:30 open · cash $8,973.17 (unchanged overnight, no fees) · equity $10,227.77 vs prior close $10,232.53 (-4.76) · 1 name(s) re-marked at the open (per-name table). TH×68 yday $18.52 → 09:30 $18.45 -4.76 | — |
| 2026-09-01 09:30 ET | **SELL** | `TH` | 68 | $18.45 | $2.22 | $-41.81 | $10,225.56 | ▼ -41.81 after sell → book $10,225.56; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,225.56 | ▲ close $10,225.56 vs 09:30 $10,227.77 (session +0.00) | 16:00 close · cash $10,225.56 · no lots left · equity $10,225.56. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,225.56 | ▲ 09:30 equity $10,225.56 vs yday $10,225.56 (-0.00) | 09:30 open · cash $10,225.56 · no holdings · equity $10,225.56 vs prior close $10,225.56 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,225.56 | ▲ close $10,225.56 vs 09:30 $10,225.56 (session +0.00) | 16:00 close · cash $10,225.56 · no lots left · equity $10,225.56. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,225.56 | ▲ 09:30 equity $10,225.56 vs yday $10,225.56 (-0.00) | 09:30 open · cash $10,225.56 · no holdings · equity $10,225.56 vs prior close $10,225.56 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $9,031.49 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1278.19 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 82 | $15.45 | $2.24 | — | $7,762.35 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1278.19 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $6,592.78 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1278.19 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 76 | $16.77 | $2.22 | — | $5,316.04 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1278.19 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 23 | $55.42 | $2.06 | — | $4,039.32 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; ret5=-25.9; leftover $1278.19 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 586 | $2.18 | $7.56 | — | $2,754.28 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1278.19 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 53 | $23.88 | $2.15 | — | $1,486.49 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1278.19 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 122 | $10.42 | $2.36 | — | $212.90 | — | combo gate; gate vol=good,ab=good; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1278.19 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $212.90 | ▼ close $10,073.65 vs 09:30 $10,225.56 (session -129.30) | 16:00 close · cash $212.90 · equity $10,073.65 vs 09:30 $10,225.56 (-151.91; session marks -129.30) · 8 name(s) marked open→close (per-name table). RVTY×9 09:30 $132.45 → close $130.63 -16.38; CRK×82 09:30 $15.45 → close $14.95 -41.00; MRNA×8 09:30 $145.94 → close $148.87 +23.40; ARCT×76 09:30 $16.77 → close $15.56 -91.96; EIX×23 09:30 $55.42 → close $56.30 +20.24; CRDL×586 09:30 $2.18 → close $2.16 -11.72; MMED×53 09:30 $23.88 → close $23.84 -2.12; NVAX×122 09:30 $10.42 → close $10.34 -9.76 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $212.90 | ▲ 09:30 equity $10,121.94 vs yday $10,073.65 (+48.29) | 09:30 open · cash $212.90 (unchanged overnight, no fees) · equity $10,121.94 vs prior close $10,073.65 (+48.29) · 8 name(s) re-marked at the open (per-name table). RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; CRK×82 yday $14.95 → 09:30 $15.00 +4.10; MRNA×8 yday $148.87 → 09:30 $153.62 +38.00; ARCT×76 yday $15.56 → 09:30 $15.61 +3.80; EIX×23 yday $56.30 → 09:30 $55.79 -11.73; CRDL×586 yday $2.16 → 09:30 $2.16 +0.00; MMED×53 yday $23.84 → 09:30 $23.84 +0.00; NVAX×122 yday $10.34 → 09:30 $10.50 +19.52 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $1,381.13 | ▼ -25.83 after sell → book $10,119.90; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 82 | $15.00 | $2.26 | $-41.40 | $2,608.87 | ▼ -41.40 after sell → book $10,117.64; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $153.62 | $2.03 | $+57.35 | $3,835.80 | ▲ +57.35 after sell → book $10,115.61; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 76 | $15.61 | $2.24 | $-92.62 | $5,019.92 | ▼ -92.62 after sell → book $10,113.37; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 23 | $55.79 | $2.08 | $+4.37 | $6,301.01 | ▲ +4.37 after sell → book $10,111.29; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 586 | $2.16 | $7.67 | $-26.95 | $7,559.10 | ▼ -26.95 after sell → book $10,103.62; vs 09:30 mark -7.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 53 | $23.84 | $2.17 | $-6.44 | $8,820.45 | ▼ -6.44 after sell → book $10,101.45; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 122 | $10.50 | $2.39 | $+5.02 | $10,099.07 | ▲ +5.02 after sell → book $10,099.07; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 364 | $3.46 | $4.70 | — | $8,834.93 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1262.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 500 | $2.52 | $6.45 | — | $7,568.48 | — | combo gate; gate vol=good,ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1262.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 188 | $6.71 | $2.55 | — | $6,304.45 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1262.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 664 | $1.90 | $8.57 | — | $5,034.28 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1262.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 264 | $4.78 | $3.41 | — | $3,768.95 | — | combo gate; gate vol=good,ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1262.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 111 | $11.31 | $2.32 | — | $2,511.22 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1262.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $1,481.67 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1262.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 45 | $28.00 | $2.12 | — | $219.54 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+8.7; leftover $1262.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $219.54 | ▼ close $10,018.41 vs 09:30 $10,121.94 (session -48.55) | 16:00 close · cash $219.54 · equity $10,018.41 vs 09:30 $10,121.94 (-103.53; session marks -48.55) · 8 name(s) marked open→close (per-name table). CABA×364 09:30 $3.46 → close $3.47 +3.64; ALEC×500 09:30 $2.52 → close $2.46 -30.00; BHC×188 09:30 $6.71 → close $6.56 -28.20; BMEA×664 09:30 $1.90 → close $2.03 +86.32; OABI×264 09:30 $4.78 → close $4.33 -118.80; VIR×111 09:30 $11.31 → close $11.38 +8.32; DELL×2 09:30 $513.78 → close $524.14 +20.72; MLYS×45 09:30 $28.00 → close $28.21 +9.45 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $219.54 | ▼ 09:30 equity $9,905.49 vs yday $10,018.41 (-112.92) | 09:30 open · cash $219.54 (unchanged overnight, no fees) · equity $9,905.49 vs prior close $10,018.41 (-112.92) · 8 name(s) re-marked at the open (per-name table). CABA×364 yday $3.47 → 09:30 $3.43 -14.56; ALEC×500 yday $2.46 → 09:30 $2.38 -40.00; BHC×188 yday $6.56 → 09:30 $6.57 +1.88; BMEA×664 yday $2.03 → 09:30 $2.00 -19.92; OABI×264 yday $4.33 → 09:30 $4.30 -7.92; VIR×111 yday $11.38 → 09:30 $11.22 -18.31; DELL×2 yday $524.14 → 09:30 $521.15 -5.98; MLYS×45 yday $28.21 → 09:30 $28.03 -8.10 | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 364 | $3.43 | $4.77 | $-20.38 | $1,463.29 | ▼ -20.38 after sell → book $9,900.72; vs 09:30 mark -4.77 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 500 | $2.38 | $6.54 | $-82.99 | $2,646.75 | ▼ -82.99 after sell → book $9,894.18; vs 09:30 mark -6.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 188 | $6.57 | $2.60 | $-31.47 | $3,879.32 | ▼ -31.47 after sell → book $9,891.59; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 664 | $2.00 | $8.69 | $+49.15 | $5,198.63 | ▲ +49.15 after sell → book $9,882.90; vs 09:30 mark -8.69 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 264 | $4.30 | $3.46 | $-133.59 | $6,330.37 | ▼ -133.59 after sell → book $9,879.44; vs 09:30 mark -3.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 111 | $11.22 | $2.35 | $-14.66 | $7,573.44 | ▼ -14.66 after sell → book $9,877.09; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $8,613.72 | ▲ +10.73 after sell → book $9,875.07; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MLYS` | 45 | $28.03 | $2.15 | $-2.92 | $9,872.93 | ▼ -2.92 after sell → book $9,872.93; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,872.93 | ▲ close $9,872.93 vs 09:30 $9,905.49 (session +0.00) | 16:00 close · cash $9,872.93 · no lots left · equity $9,872.93. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,872.93 | ▲ 09:30 equity $9,872.93 vs yday $9,872.93 (-0.00) | 09:30 open · cash $9,872.93 · no holdings · equity $9,872.93 vs prior close $9,872.93 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,872.93 | ▲ close $9,872.93 vs 09:30 $9,872.93 (session +0.00) | 16:00 close · cash $9,872.93 · no lots left · equity $9,872.93. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,872.93 | ▲ 09:30 equity $9,872.93 vs yday $9,872.93 (-0.00) | 09:30 open · cash $9,872.93 · no holdings · equity $9,872.93 vs prior close $9,872.93 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,872.93 | ▲ close $9,872.93 vs 09:30 $9,872.93 (session +0.00) | 16:00 close · cash $9,872.93 · no lots left · equity $9,872.93. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,872.93 | ▲ 09:30 equity $9,872.93 vs yday $9,872.93 (-0.00) | 09:30 open · cash $9,872.93 · no holdings · equity $9,872.93 vs prior close $9,872.93 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $8,719.91 | — | combo gate; gate vol=good,ab=good; list flatten,earn_react; 🔵; ⚪; ret5=+9.0; leftover $1234.12 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 394 | $3.13 | $5.08 | — | $7,481.60 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; ret5=+6.2; leftover $1234.12 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 457 | $2.70 | $5.90 | — | $6,241.81 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1234.12 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 112 | $10.95 | $2.33 | — | $5,013.08 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+4.6; leftover $1234.12 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 251 | $4.91 | $3.24 | — | $3,777.44 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+69.9; leftover $1234.12 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 14 | $84.27 | $2.03 | — | $2,595.62 | — | combo gate; gate vol=good,ab=good; list yday_gainer,ohlc_hot; ⚪; ret5=+12.5; leftover $1234.12 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ANGX` | 229 | $5.38 | $2.95 | — | $1,360.65 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=+19.8; leftover $1234.12 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `TSSI` | 137 | $8.98 | $2.40 | — | $127.99 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=+10.0; leftover $1234.12 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.99 | ▲ close $9,902.53 vs 09:30 $9,872.93 (session +55.54) | 16:00 close · cash $127.99 · equity $9,902.53 vs 09:30 $9,872.93 (+29.60; session marks +55.54) · 8 name(s) marked open→close (per-name table). ORCL×7 09:30 $164.43 → close $150.28 -99.05; CMRC×394 09:30 $3.13 → close $3.50 +147.75; INDP×457 09:30 $2.70 → close $2.77 +31.99; WLTH×112 09:30 $10.95 → close $10.38 -63.84; BNC×251 09:30 $4.91 → close $4.80 -27.61; SWKS×14 09:30 $84.27 → close $88.35 +57.12; ANGX×229 09:30 $5.38 → close $5.45 +16.03; TSSI×137 09:30 $8.98 → close $8.93 -6.85 | — |

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
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SID` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ORCL` | 7 | 2026-09-11 @ $164.43 | combo gate; gate vol=good,ab=good; list flatten,earn_react; 🔵; ⚪; ret5=+9.0; leftover $1234.12 |
| `CMRC` | 394 | 2026-09-11 @ $3.13 | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; ret5=+6.2; leftover $1234.12 |
| `INDP` | 457 | 2026-09-11 @ $2.70 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1234.12 |
| `WLTH` | 112 | 2026-09-11 @ $10.95 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+4.6; leftover $1234.12 |
| `BNC` | 251 | 2026-09-11 @ $4.91 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+69.9; leftover $1234.12 |
| `SWKS` | 14 | 2026-09-11 @ $84.27 | combo gate; gate vol=good,ab=good; list yday_gainer,ohlc_hot; ⚪; ret5=+12.5; leftover $1234.12 |
| `ANGX` | 229 | 2026-09-11 @ $5.38 | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=+19.8; leftover $1234.12 |
| `TSSI` | 137 | 2026-09-11 @ $8.98 | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=+10.0; leftover $1234.12 |
