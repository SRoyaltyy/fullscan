# Factor mine action — `union_vol_ab_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-4.08%** ($9,592) · signal-only (no cash/fees) was +46.48%. Starts YES **1/18**. Fills 72 · skips 108 · realized $-238.08.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=good,ab=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $32.60.

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
| 2026-08-25 | `KURA` | 107 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `LIFE` | 39 | — | $36.96 | +0.00 | $38.56 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-08-25 | `ZIP` | 321 | — | $4.55 | +0.00 | $4.35 | -64.20 | -64.20 | +0.00 | -64.20 |
| 2026-08-25 | `BMEA` | 897 | — | $1.63 | +0.00 | $1.73 | +89.70 | +89.70 | +0.00 | +89.70 |
| 2026-08-25 | `ALVO` | 279 | — | $5.24 | +0.00 | $5.05 | -53.01 | -53.01 | +0.00 | -53.01 |
| 2026-08-25 | `DEFT` | 2360 | — | $0.62 | +0.00 | $0.60 | -37.76 | -37.76 | +0.00 | -37.76 |
| 2026-08-25 | `ZURA` | 227 | — | $6.37 | +0.00 | $6.32 | -11.35 | -11.35 | +0.00 | -11.35 |
| 2026-08-26 | `AUPH` | 1 | $16.75 | $16.60 | -0.15 | — | +0.00 | -0.15 | -0.60 | — |
| 2026-08-26 | `ARCT` | 2 | $15.44 | $15.35 | -0.18 | — | +0.00 | -0.18 | +8.44 | — |
| 2026-08-26 | `AUTL` | 9 | $2.44 | $2.41 | -0.27 | — | +0.00 | -0.27 | -0.54 | — |
| 2026-08-26 | `CRDL` | 12 | $2.00 | $2.03 | +0.36 | — | +0.00 | +0.36 | +1.20 | — |
| 2026-08-26 | `CYPH` | 17 | $1.64 | $1.60 | -0.68 | — | +0.00 | -0.68 | +4.76 | — |
| 2026-08-26 | `KURA` | 107 | $13.59 | $13.63 | +4.28 | $13.06 | -60.99 | -56.71 | +4.28 | -56.71 |
| 2026-08-26 | `LIFE` | 39 | $38.56 | $38.24 | -12.48 | $39.11 | +33.93 | +21.45 | +49.92 | +83.85 |
| 2026-08-26 | `ZIP` | 321 | $4.35 | $4.31 | -12.84 | $4.31 | +0.00 | -12.84 | -77.04 | -77.04 |
| 2026-08-26 | `BMEA` | 897 | $1.73 | $1.75 | +22.42 | $1.71 | -40.36 | -17.94 | +112.12 | +71.76 |
| 2026-08-26 | `ALVO` | 279 | $5.05 | $4.98 | -19.53 | $4.91 | -19.53 | -39.06 | -72.54 | -92.07 |
| 2026-08-26 | `DEFT` | 2360 | $0.60 | $0.60 | -14.16 | $0.59 | -21.24 | -35.40 | -51.92 | -73.16 |
| 2026-08-26 | `ZURA` | 227 | $6.32 | $6.13 | -43.13 | $5.99 | -31.78 | -74.91 | -54.48 | -86.26 |
| 2026-08-26 | `SLQT` | 107 | — | $0.58 | +0.00 | $0.55 | -3.53 | -3.53 | +0.00 | -3.53 |
| 2026-08-27 | `KURA` | 107 | $13.06 | $12.98 | -8.56 | $13.18 | +21.40 | +12.84 | -65.27 | -43.87 |
| 2026-08-27 | `LIFE` | 39 | $39.11 | $39.40 | +11.31 | $39.44 | +1.56 | +12.87 | +95.16 | +96.72 |
| 2026-08-27 | `ZIP` | 321 | $4.31 | $4.30 | -3.21 | $4.29 | -3.21 | -6.42 | -80.25 | -83.46 |
| 2026-08-27 | `BMEA` | 897 | $1.71 | $1.74 | +26.91 | $1.68 | -53.82 | -26.91 | +98.67 | +44.85 |
| 2026-08-27 | `ALVO` | 279 | $4.91 | $4.88 | -8.37 | $4.88 | +0.00 | -8.37 | -100.44 | -100.44 |
| 2026-08-27 | `DEFT` | 2360 | $0.59 | $0.60 | +16.52 | $0.65 | +129.80 | +146.32 | -56.64 | +73.16 |
| 2026-08-27 | `ZURA` | 227 | $5.99 | $6.02 | +6.81 | $5.85 | -38.59 | -31.78 | -79.45 | -118.04 |
| 2026-08-27 | `SLQT` | 107 | $0.55 | $0.53 | -2.14 | $0.54 | +1.07 | -1.07 | -5.67 | -4.60 |
| 2026-08-28 | `KURA` | 107 | $13.18 | $13.05 | -13.91 | — | +0.00 | -13.91 | -57.78 | — |
| 2026-08-28 | `LIFE` | 39 | $39.44 | $39.60 | +6.24 | — | +0.00 | +6.24 | +102.96 | — |
| 2026-08-28 | `ZIP` | 321 | $4.29 | $4.21 | -25.68 | — | +0.00 | -25.68 | -109.14 | — |
| 2026-08-28 | `BMEA` | 897 | $1.68 | $1.69 | +8.97 | — | +0.00 | +8.97 | +53.82 | — |
| 2026-08-28 | `ALVO` | 279 | $4.88 | $4.84 | -11.16 | — | +0.00 | -11.16 | -111.60 | — |
| 2026-08-28 | `DEFT` | 2360 | $0.65 | $0.64 | -37.76 | — | +0.00 | -37.76 | +35.40 | — |
| 2026-08-28 | `ZURA` | 227 | $5.85 | $5.88 | +6.81 | — | +0.00 | +6.81 | -111.23 | — |
| 2026-08-28 | `SLQT` | 107 | $0.54 | $0.53 | -0.96 | $0.52 | -1.18 | -2.14 | -5.56 | -6.74 |
| 2026-08-28 | `URBN` | 15 | — | $79.42 | +0.00 | $81.09 | +25.05 | +25.05 | +0.00 | +25.05 |
| 2026-08-28 | `ANF` | 8 | — | $146.07 | +0.00 | $148.42 | +18.80 | +18.80 | +0.00 | +18.80 |
| 2026-08-28 | `BZ` | 68 | — | $18.15 | +0.00 | $17.80 | -23.80 | -23.80 | +0.00 | -23.80 |
| 2026-08-28 | `SMTC` | 8 | — | $141.76 | +0.00 | $131.17 | -84.72 | -84.72 | +0.00 | -84.72 |
| 2026-08-28 | `BBWI` | 66 | — | $18.75 | +0.00 | $19.22 | +31.02 | +31.02 | +0.00 | +31.02 |
| 2026-08-28 | `CRDL` | 607 | — | $2.06 | +0.00 | $1.94 | -72.84 | -72.84 | +0.00 | -72.84 |
| 2026-08-28 | `NCNO` | 53 | — | $23.30 | +0.00 | $22.99 | -16.43 | -16.43 | +0.00 | -16.43 |
| 2026-08-28 | `TH` | 65 | — | $19.00 | +0.00 | $18.55 | -29.25 | -29.25 | +0.00 | -29.25 |
| 2026-08-31 | `SLQT` | 107 | $0.52 | $0.51 | -1.07 | — | +0.00 | -1.07 | -7.81 | — |
| 2026-08-31 | `URBN` | 15 | $81.09 | $80.44 | -9.75 | $80.69 | +3.75 | -6.00 | +15.30 | +19.05 |
| 2026-08-31 | `ANF` | 8 | $148.42 | $148.03 | -3.12 | $143.08 | -39.60 | -42.72 | +15.68 | -23.92 |
| 2026-08-31 | `BZ` | 68 | $17.80 | $17.70 | -6.80 | $17.36 | -23.12 | -29.92 | -30.60 | -53.72 |
| 2026-08-31 | `SMTC` | 8 | $131.17 | $132.30 | +9.04 | $132.96 | +5.28 | +14.32 | -75.68 | -70.40 |
| 2026-08-31 | `BBWI` | 66 | $19.22 | $19.25 | +1.98 | $19.25 | +0.00 | +1.98 | +33.00 | +33.00 |
| 2026-08-31 | `CRDL` | 607 | $1.94 | $1.92 | -12.14 | $1.98 | +33.39 | +21.25 | -84.98 | -51.59 |
| 2026-08-31 | `NCNO` | 53 | $22.99 | $22.66 | -17.49 | $22.60 | -3.18 | -20.67 | -33.92 | -37.10 |
| 2026-08-31 | `TH` | 65 | $18.55 | $18.12 | -27.63 | $18.52 | +25.67 | -1.96 | -56.88 | -31.20 |
| 2026-09-01 | `URBN` | 15 | $80.69 | $79.12 | -23.55 | $79.29 | +2.55 | -21.00 | -4.50 | -1.95 |
| 2026-09-01 | `ANF` | 8 | $143.08 | $142.00 | -8.64 | $140.68 | -10.56 | -19.20 | -32.56 | -43.12 |
| 2026-09-01 | `BZ` | 68 | $17.36 | $17.29 | -4.76 | $17.55 | +17.68 | +12.92 | -58.48 | -40.80 |
| 2026-09-01 | `SMTC` | 8 | $132.96 | $127.63 | -42.64 | $132.27 | +37.12 | -5.52 | -113.04 | -75.92 |
| 2026-09-01 | `BBWI` | 66 | $19.25 | $18.77 | -31.68 | $18.61 | -10.56 | -42.24 | +1.32 | -9.24 |
| 2026-09-01 | `CRDL` | 607 | $1.98 | $1.94 | -21.25 | $2.15 | +127.47 | +106.22 | -72.84 | +54.63 |
| 2026-09-01 | `NCNO` | 53 | $22.60 | $22.15 | -23.85 | $22.30 | +7.95 | -15.90 | -60.95 | -53.00 |
| 2026-09-01 | `TH` | 65 | $18.52 | $18.45 | -4.55 | $18.07 | -24.70 | -29.25 | -35.75 | -60.45 |
| 2026-09-02 | `URBN` | 15 | $79.29 | $78.84 | -6.75 | — | +0.00 | -6.75 | -8.70 | — |
| 2026-09-02 | `ANF` | 8 | $140.68 | $139.65 | -8.24 | — | +0.00 | -8.24 | -51.36 | — |
| 2026-09-02 | `BZ` | 68 | $17.55 | $17.65 | +6.80 | — | +0.00 | +6.80 | -34.00 | — |
| 2026-09-02 | `SMTC` | 8 | $132.27 | $133.00 | +5.84 | — | +0.00 | +5.84 | -70.08 | — |
| 2026-09-02 | `BBWI` | 66 | $18.61 | $18.41 | -13.20 | — | +0.00 | -13.20 | -22.44 | — |
| 2026-09-02 | `CRDL` | 607 | $2.15 | $2.16 | +6.07 | — | +0.00 | +6.07 | +60.70 | — |
| 2026-09-02 | `NCNO` | 53 | $22.30 | $22.20 | -5.30 | — | +0.00 | -5.30 | -58.30 | — |
| 2026-09-02 | `TH` | 65 | $18.07 | $17.98 | -5.85 | — | +0.00 | -5.85 | -66.30 | — |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `CRK` | 78 | — | $15.45 | +0.00 | $14.95 | -39.00 | -39.00 | +0.00 | -39.00 |
| 2026-09-03 | `MRNA` | 8 | — | $145.94 | +0.00 | $148.87 | +23.40 | +23.40 | +0.00 | +23.40 |
| 2026-09-03 | `ARCT` | 72 | — | $16.77 | +0.00 | $15.56 | -87.12 | -87.12 | +0.00 | -87.12 |
| 2026-09-03 | `EIX` | 22 | — | $55.42 | +0.00 | $56.30 | +19.36 | +19.36 | +0.00 | +19.36 |
| 2026-09-03 | `CRDL` | 559 | — | $2.18 | +0.00 | $2.16 | -11.18 | -11.18 | +0.00 | -11.18 |
| 2026-09-03 | `MMED` | 51 | — | $23.88 | +0.00 | $23.84 | -2.04 | -2.04 | +0.00 | -2.04 |
| 2026-09-03 | `NVAX` | 117 | — | $10.42 | +0.00 | $10.34 | -9.36 | -9.36 | +0.00 | -9.36 |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | $130.22 | +1.71 | -3.69 | -21.78 | -20.07 |
| 2026-09-04 | `CRK` | 78 | $14.95 | $15.00 | +3.90 | $15.26 | +20.28 | +24.18 | -35.10 | -14.82 |
| 2026-09-04 | `MRNA` | 8 | $148.87 | $153.62 | +38.00 | $145.55 | -64.56 | -26.56 | +61.40 | -3.16 |
| 2026-09-04 | `ARCT` | 72 | $15.56 | $15.61 | +3.60 | $15.82 | +15.12 | +18.72 | -83.52 | -68.40 |
| 2026-09-04 | `EIX` | 22 | $56.30 | $55.79 | -11.22 | $56.77 | +21.56 | +10.34 | +8.14 | +29.70 |
| 2026-09-04 | `CRDL` | 559 | $2.16 | $2.16 | +0.00 | $2.20 | +22.36 | +22.36 | -11.18 | +11.18 |
| 2026-09-04 | `MMED` | 51 | $23.84 | $23.84 | +0.00 | $23.29 | -28.05 | -28.05 | -2.04 | -30.09 |
| 2026-09-04 | `NVAX` | 117 | $10.34 | $10.50 | +18.72 | $10.22 | -32.76 | -14.04 | +9.36 | -23.40 |
| 2026-09-04 | `CABA` | 3 | — | $3.46 | +0.00 | $3.47 | +0.03 | +0.03 | +0.00 | +0.03 |
| 2026-09-04 | `ALEC` | 4 | — | $2.52 | +0.00 | $2.46 | -0.24 | -0.24 | +0.00 | -0.24 |
| 2026-09-04 | `BHC` | 1 | — | $6.71 | +0.00 | $6.56 | -0.15 | -0.15 | +0.00 | -0.15 |
| 2026-09-04 | `BMEA` | 6 | — | $1.90 | +0.00 | $2.03 | +0.78 | +0.78 | +0.00 | +0.78 |
| 2026-09-04 | `OABI` | 2 | — | $4.78 | +0.00 | $4.33 | -0.90 | -0.90 | +0.00 | -0.90 |
| 2026-09-04 | `VIR` | 1 | — | $11.31 | +0.00 | $11.38 | +0.07 | +0.07 | +0.00 | +0.07 |
| 2026-09-08 | `RVTY` | 9 | $130.22 | $128.50 | -15.48 | $127.08 | -12.78 | -28.26 | -35.55 | -48.33 |
| 2026-09-08 | `CRK` | 78 | $15.26 | $15.50 | +18.72 | $15.16 | -26.52 | -7.80 | +3.90 | -22.62 |
| 2026-09-08 | `MRNA` | 8 | $145.55 | $145.98 | +3.44 | $140.33 | -45.20 | -41.76 | +0.28 | -44.92 |
| 2026-09-08 | `ARCT` | 72 | $15.82 | $15.47 | -25.20 | $15.63 | +11.52 | -13.68 | -93.60 | -82.08 |
| 2026-09-08 | `EIX` | 22 | $56.77 | $56.53 | -5.28 | $59.33 | +61.60 | +56.32 | +24.42 | +86.02 |
| 2026-09-08 | `CRDL` | 559 | $2.20 | $2.20 | +0.00 | $2.22 | +11.18 | +11.18 | +11.18 | +22.36 |
| 2026-09-08 | `MMED` | 51 | $23.29 | $23.16 | -6.63 | $23.32 | +8.16 | +1.53 | -36.72 | -28.56 |
| 2026-09-08 | `NVAX` | 117 | $10.22 | $10.10 | -14.04 | $10.19 | +10.53 | -3.51 | -37.44 | -26.91 |
| 2026-09-08 | `CABA` | 3 | $3.47 | $3.43 | -0.12 | $3.27 | -0.48 | -0.60 | -0.09 | -0.57 |
| 2026-09-08 | `ALEC` | 4 | $2.46 | $2.38 | -0.32 | $2.47 | +0.36 | +0.04 | -0.56 | -0.20 |
| 2026-09-08 | `BHC` | 1 | $6.56 | $6.57 | +0.01 | $6.43 | -0.14 | -0.13 | -0.14 | -0.28 |
| 2026-09-08 | `BMEA` | 6 | $2.03 | $2.00 | -0.18 | $1.93 | -0.42 | -0.60 | +0.60 | +0.18 |
| 2026-09-08 | `OABI` | 2 | $4.33 | $4.30 | -0.06 | $4.24 | -0.12 | -0.18 | -0.96 | -1.08 |
| 2026-09-08 | `VIR` | 1 | $11.38 | $11.22 | -0.16 | $11.18 | -0.04 | -0.20 | -0.09 | -0.13 |

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
| 2026-08-25 | +1.80 | $78.42 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | $10,384.32 | -165.58 | -8.24 | KURA, LIFE, ZIP, BMEA, ALVO, DEFT, ZURA | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $6.11 | $10,302.74 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17, KURA×107, LIFE×39, ZIP×321, BMEA×897, ALVO×279, DEFT×2360, ZURA×227 |
| 2026-08-26 | +2.02 | $6.11 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17, KURA×107, LIFE×39, ZIP×321, BMEA×897, ALVO×279, DEFT×2360, ZURA×227 | $10,226.38 | -76.36 | -143.50 | SLQT | AUPH, ARCT, AUTL, CRDL, CYPH | $61.90 | $10,080.50 | KURA×107, LIFE×39, ZIP×321, BMEA×897, ALVO×279, DEFT×2360, ZURA×227, SLQT×107 |
| 2026-08-27 | — | $61.90 | KURA×107, LIFE×39, ZIP×321, BMEA×897, ALVO×279, DEFT×2360, ZURA×227, SLQT×107 | $10,119.77 | +39.27 | +58.21 | — | — | $61.90 | $10,177.98 | KURA×107, LIFE×39, ZIP×321, BMEA×897, ALVO×279, DEFT×2360, ZURA×227, SLQT×107 |
| 2026-08-28 | +0.75 | $61.90 | KURA×107, LIFE×39, ZIP×321, BMEA×897, ALVO×279, DEFT×2360, ZURA×227, SLQT×107 | $10,110.53 | -67.45 | -153.35 | URBN, ANF, BZ, SMTC, BBWI, CRDL, NCNO, TH | KURA, LIFE, ZIP, BMEA, ALVO, DEFT, ZURA | $295.63 | $9,885.06 | SLQT×107, URBN×15, ANF×8, BZ×68, SMTC×8, BBWI×66, CRDL×607, NCNO×53, TH×65 |
| 2026-08-31 | -5.85 | $295.63 | SLQT×107, URBN×15, ANF×8, BZ×68, SMTC×8, BBWI×66, CRDL×607, NCNO×53, TH×65 | $9,818.09 | -66.97 | +2.19 | — | SLQT | $349.31 | $9,819.38 | URBN×15, ANF×8, BZ×68, SMTC×8, BBWI×66, CRDL×607, NCNO×53, TH×65 |
| 2026-09-01 | -6.30 | $349.31 | URBN×15, ANF×8, BZ×68, SMTC×8, BBWI×66, CRDL×607, NCNO×53, TH×65 | $9,658.47 | -160.91 | +146.95 | — | — | $349.31 | $9,805.42 | URBN×15, ANF×8, BZ×68, SMTC×8, BBWI×66, CRDL×607, NCNO×53, TH×65 |
| 2026-09-02 | -3.83 | $349.31 | URBN×15, ANF×8, BZ×68, SMTC×8, BBWI×66, CRDL×607, NCNO×53, TH×65 | $9,784.79 | -20.63 | +0.00 | — | URBN, ANF, BZ, SMTC, BBWI, CRDL, NCNO, TH | $9,761.93 | $9,761.93 | — |
| 2026-09-03 | -0.90 | $9,761.93 | — | $9,761.93 | -0.00 | -122.32 | RVTY, CRK, MRNA, ARCT, EIX, CRDL, MMED, NVAX | — | $92.68 | $9,617.39 | RVTY×9, CRK×78, MRNA×8, ARCT×72, EIX×22, CRDL×559, MMED×51, NVAX×117 |
| 2026-09-04 | +2.25 | $92.68 | RVTY×9, CRK×78, MRNA×8, ARCT×72, EIX×22, CRDL×559, MMED×51, NVAX×117 | $9,664.99 | +47.60 | -44.75 | CABA, ALEC, BHC, BMEA, OABI, VIR | — | $32.60 | $9,619.60 | RVTY×9, CRK×78, MRNA×8, ARCT×72, EIX×22, CRDL×559, MMED×51, NVAX×117, CABA×3, ALEC×4, BHC×1, BMEA×6, OABI×2, VIR×1 |
| 2026-09-08 | -11.47 | $32.60 | RVTY×9, CRK×78, MRNA×8, ARCT×72, EIX×22, CRDL×559, MMED×51, NVAX×117, CABA×3, ALEC×4, BHC×1, BMEA×6, OABI×2, VIR×1 | $9,574.30 | -45.30 | +17.65 | — | — | $32.60 | $9,591.95 | RVTY×9, CRK×78, MRNA×8, ARCT×72, EIX×22, CRDL×559, MMED×51, NVAX×117, CABA×3, ALEC×4, BHC×1, BMEA×6, OABI×2, VIR×1 |

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
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $169.53 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $147.04 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $124.56 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 12 | $1.93 | $0.27 | — | $101.13 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 17 | $1.32 | $0.28 | — | $78.42 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
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
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 107 | $13.59 | $2.31 | — | $8,787.42 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1463.41 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 39 | $36.96 | $2.11 | — | $7,343.87 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1463.41 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 321 | $4.55 | $4.14 | — | $5,879.18 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1463.41 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 897 | $1.63 | $11.57 | — | $4,405.50 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1463.41 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 279 | $5.24 | $3.60 | — | $2,939.94 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1463.41 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2360 | $0.62 | $21.71 | — | $1,455.03 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $1463.41 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 227 | $6.37 | $2.93 | — | $6.11 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1463.41 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.11 | ▼ close $10,302.74 vs 09:30 $10,384.32 (session -8.24) | 16:00 close · cash $6.11 · equity $10,302.74 vs 09:30 $10,384.32 (-81.58; session marks -8.24) · 12 name(s) marked open→close (per-name table). AUPH×1 09:30 $16.63 → close $16.75 +0.12; ARCT×2 09:30 $14.12 → close $15.44 +2.64; AUTL×9 09:30 $2.38 → close $2.44 +0.54; CRDL×12 09:30 $1.89 → close $2.00 +1.32; CYPH×17 09:30 $1.56 → close $1.64 +1.36; KURA×107 09:30 $13.59 → close $13.59 +0.00; LIFE×39 09:30 $36.96 → close $38.56 +62.40; ZIP×321 09:30 $4.55 → close $4.35 -64.20; BMEA×897 09:30 $1.63 → close $1.73 +89.70; ALVO×279 09:30 $5.24 → close $5.05 -53.01; DEFT×2360 09:30 $0.62 → close $0.60 -37.76; ZURA×227 09:30 $6.37 → close $6.32 -11.35 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.11 | ▼ 09:30 equity $10,226.38 vs yday $10,302.74 (-76.36) | 09:30 open · cash $6.11 (unchanged overnight, no fees) · equity $10,226.38 vs prior close $10,302.74 (-76.36) · 12 name(s) re-marked at the open (per-name table). AUPH×1 yday $16.75 → 09:30 $16.60 -0.15; ARCT×2 yday $15.44 → 09:30 $15.35 -0.18; AUTL×9 yday $2.44 → 09:30 $2.41 -0.27; CRDL×12 yday $2.00 → 09:30 $2.03 +0.36; CYPH×17 yday $1.64 → 09:30 $1.60 -0.68; KURA×107 yday $13.59 → 09:30 $13.63 +4.28; LIFE×39 yday $38.56 → 09:30 $38.24 -12.48; ZIP×321 yday $4.35 → 09:30 $4.31 -12.84; BMEA×897 yday $1.73 → 09:30 $1.75 +22.42; ALVO×279 yday $5.05 → 09:30 $4.98 -19.53; DEFT×2360 yday $0.60 → 09:30 $0.60 -14.16; ZURA×227 yday $6.32 → 09:30 $6.13 -43.13 | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $22.52 | ▼ -0.96 after sell → book $10,226.19; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $52.89 | ▲ +7.88 after sell → book $10,225.86; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 9 | $2.41 | $0.26 | $-1.05 | $74.31 | ▼ -1.05 after sell → book $10,225.60; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 12 | $2.03 | $0.30 | $+0.63 | $98.37 | ▲ +0.63 after sell → book $10,225.30; vs 09:30 mark -0.30 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟡 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 17 | $1.60 | $0.34 | $+4.14 | $125.23 | ▲ +4.14 after sell → book $10,224.95; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 107 | $0.58 | $0.94 | — | $61.90 | — | combo gate; gate vol=good,ab=good; list yday_mover; 🔵; ret5=-27.5; leftover $62.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.90 | ▼ close $10,080.50 vs 09:30 $10,226.38 (session -143.50) | 16:00 close · cash $61.90 · equity $10,080.50 vs 09:30 $10,226.38 (-145.88; session marks -143.50) · 8 name(s) marked open→close (per-name table). KURA×107 09:30 $13.63 → close $13.06 -60.99; LIFE×39 09:30 $38.24 → close $39.11 +33.93; ZIP×321 09:30 $4.31 → close $4.31 +0.00; BMEA×897 09:30 $1.75 → close $1.71 -40.36; ALVO×279 09:30 $4.98 → close $4.91 -19.53; DEFT×2360 09:30 $0.60 → close $0.59 -21.24; ZURA×227 09:30 $6.13 → close $5.99 -31.78; SLQT×107 09:30 $0.58 → close $0.55 -3.53 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.90 | ▲ 09:30 equity $10,119.77 vs yday $10,080.50 (+39.27) | 09:30 open · cash $61.90 (unchanged overnight, no fees) · equity $10,119.77 vs prior close $10,080.50 (+39.27) · 8 name(s) re-marked at the open (per-name table). KURA×107 yday $13.06 → 09:30 $12.98 -8.56; LIFE×39 yday $39.11 → 09:30 $39.40 +11.31; ZIP×321 yday $4.31 → 09:30 $4.30 -3.21; BMEA×897 yday $1.71 → 09:30 $1.74 +26.91; ALVO×279 yday $4.91 → 09:30 $4.88 -8.37; DEFT×2360 yday $0.59 → 09:30 $0.60 +16.52; ZURA×227 yday $5.99 → 09:30 $6.02 +6.81; SLQT×107 yday $0.55 → 09:30 $0.53 -2.14 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.90 | ▲ close $10,177.98 vs 09:30 $10,119.77 (session +58.21) | 16:00 close · cash $61.90 · equity $10,177.98 vs 09:30 $10,119.77 (+58.21; session marks +58.21) · 8 name(s) marked open→close (per-name table). KURA×107 09:30 $12.98 → close $13.18 +21.40; LIFE×39 09:30 $39.40 → close $39.44 +1.56; ZIP×321 09:30 $4.30 → close $4.29 -3.21; BMEA×897 09:30 $1.74 → close $1.68 -53.82; ALVO×279 09:30 $4.88 → close $4.88 +0.00; DEFT×2360 09:30 $0.60 → close $0.65 +129.80; ZURA×227 09:30 $6.02 → close $5.85 -38.59; SLQT×107 09:30 $0.53 → close $0.54 +1.07 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.90 | ▼ 09:30 equity $10,110.53 vs yday $10,177.98 (-67.45) | 09:30 open · cash $61.90 (unchanged overnight, no fees) · equity $10,110.53 vs prior close $10,177.98 (-67.45) · 8 name(s) re-marked at the open (per-name table). KURA×107 yday $13.18 → 09:30 $13.05 -13.91; LIFE×39 yday $39.44 → 09:30 $39.60 +6.24; ZIP×321 yday $4.29 → 09:30 $4.21 -25.68; BMEA×897 yday $1.68 → 09:30 $1.69 +8.97; ALVO×279 yday $4.88 → 09:30 $4.84 -11.16; DEFT×2360 yday $0.65 → 09:30 $0.64 -37.76; ZURA×227 yday $5.85 → 09:30 $5.88 +6.81; SLQT×107 yday $0.54 → 09:30 $0.53 -0.96 | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 107 | $13.05 | $2.34 | $-62.43 | $1,455.91 | ▼ -62.43 after sell → book $10,108.19; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 39 | $39.60 | $2.13 | $+98.72 | $2,998.18 | ▲ +98.72 after sell → book $10,106.06; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZIP` | 321 | $4.21 | $4.21 | $-117.49 | $4,345.39 | ▼ -117.49 after sell → book $10,101.86; vs 09:30 mark -4.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 897 | $1.69 | $11.73 | $+30.52 | $5,849.59 | ▲ +30.52 after sell → book $10,090.12; vs 09:30 mark -11.74 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 279 | $4.84 | $3.66 | $-118.86 | $7,196.29 | ▼ -118.86 after sell → book $10,086.47; vs 09:30 mark -3.65 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DEFT` | 2360 | $0.64 | $22.47 | $-8.78 | $8,672.42 | ▼ -8.78 after sell → book $10,064.00; vs 09:30 mark -22.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 227 | $5.88 | $2.98 | $-117.13 | $10,004.20 | ▼ -117.13 after sell → book $10,061.02; vs 09:30 mark -2.98 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 15 | $79.42 | $2.04 | — | $8,810.87 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1250.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $7,640.29 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1250.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 68 | $18.15 | $2.19 | — | $6,403.90 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+14.1; leftover $1250.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $5,267.81 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1250.53 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 66 | $18.75 | $2.19 | — | $4,028.12 | — | combo gate; gate vol=good,ab=good; list yday_gainer; ret5=-5.0; leftover $1250.53 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CRDL` | 607 | $2.06 | $7.83 | — | $2,769.87 | — | combo gate; gate vol=good,ab=good; list yday_gainer; ret5=+9.3; leftover $1250.53 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 53 | $23.30 | $2.15 | — | $1,532.82 | — | combo gate; gate vol=good,ab=good; list ohlc_hot; 🔵; ret5=+14.5; leftover $1250.53 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 65 | $19.00 | $2.19 | — | $295.63 | — | combo gate; gate vol=good,ab=good; list ohlc_hot; ret5=+7.5; leftover $1250.53 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $295.63 | ▼ close $9,885.06 vs 09:30 $10,110.53 (session -153.35) | 16:00 close · cash $295.63 · equity $9,885.06 vs 09:30 $10,110.53 (-225.47; session marks -153.35) · 9 name(s) marked open→close (per-name table). SLQT×107 09:30 $0.53 → close $0.52 -1.18; URBN×15 09:30 $79.42 → close $81.09 +25.05; ANF×8 09:30 $146.07 → close $148.42 +18.80; BZ×68 09:30 $18.15 → close $17.80 -23.80; SMTC×8 09:30 $141.76 → close $131.17 -84.72; BBWI×66 09:30 $18.75 → close $19.22 +31.02; CRDL×607 09:30 $2.06 → close $1.94 -72.84; NCNO×53 09:30 $23.30 → close $22.99 -16.43; TH×65 09:30 $19.00 → close $18.55 -29.25 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $295.63 | ▼ 09:30 equity $9,818.09 vs yday $9,885.06 (-66.97) | 09:30 open · cash $295.63 (unchanged overnight, no fees) · equity $9,818.09 vs prior close $9,885.06 (-66.97) · 9 name(s) re-marked at the open (per-name table). SLQT×107 yday $0.52 → 09:30 $0.51 -1.07; URBN×15 yday $81.09 → 09:30 $80.44 -9.75; ANF×8 yday $148.42 → 09:30 $148.03 -3.12; BZ×68 yday $17.80 → 09:30 $17.70 -6.80; SMTC×8 yday $131.17 → 09:30 $132.30 +9.04; BBWI×66 yday $19.22 → 09:30 $19.25 +1.98; CRDL×607 yday $1.94 → 09:30 $1.92 -12.14; NCNO×53 yday $22.99 → 09:30 $22.66 -17.49; TH×65 yday $18.55 → 09:30 $18.12 -27.63 | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 107 | $0.51 | $0.89 | $-9.65 | $349.31 | ▼ -9.65 after sell → book $9,817.19; vs 09:30 mark -0.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $349.31 | ▲ close $9,819.38 vs 09:30 $9,818.09 (session +2.19) | 16:00 close · cash $349.31 · equity $9,819.38 vs 09:30 $9,818.09 (+1.29; session marks +2.19) · 8 name(s) marked open→close (per-name table). URBN×15 09:30 $80.44 → close $80.69 +3.75; ANF×8 09:30 $148.03 → close $143.08 -39.60; BZ×68 09:30 $17.70 → close $17.36 -23.12; SMTC×8 09:30 $132.30 → close $132.96 +5.28; BBWI×66 09:30 $19.25 → close $19.25 +0.00; CRDL×607 09:30 $1.92 → close $1.98 +33.39; NCNO×53 09:30 $22.66 → close $22.60 -3.18; TH×65 09:30 $18.12 → close $18.52 +25.67 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $349.31 | ▼ 09:30 equity $9,658.47 vs yday $9,819.38 (-160.91) | 09:30 open · cash $349.31 (unchanged overnight, no fees) · equity $9,658.47 vs prior close $9,819.38 (-160.91) · 8 name(s) re-marked at the open (per-name table). URBN×15 yday $80.69 → 09:30 $79.12 -23.55; ANF×8 yday $143.08 → 09:30 $142.00 -8.64; BZ×68 yday $17.36 → 09:30 $17.29 -4.76; SMTC×8 yday $132.96 → 09:30 $127.63 -42.64; BBWI×66 yday $19.25 → 09:30 $18.77 -31.68; CRDL×607 yday $1.98 → 09:30 $1.94 -21.25; NCNO×53 yday $22.60 → 09:30 $22.15 -23.85; TH×65 yday $18.52 → 09:30 $18.45 -4.55 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $349.31 | ▲ close $9,805.42 vs 09:30 $9,658.47 (session +146.95) | 16:00 close · cash $349.31 · equity $9,805.42 vs 09:30 $9,658.47 (+146.95; session marks +146.95) · 8 name(s) marked open→close (per-name table). URBN×15 09:30 $79.12 → close $79.29 +2.55; ANF×8 09:30 $142.00 → close $140.68 -10.56; BZ×68 09:30 $17.29 → close $17.55 +17.68; SMTC×8 09:30 $127.63 → close $132.27 +37.12; BBWI×66 09:30 $18.77 → close $18.61 -10.56; CRDL×607 09:30 $1.94 → close $2.15 +127.47; NCNO×53 09:30 $22.15 → close $22.30 +7.95; TH×65 09:30 $18.45 → close $18.07 -24.70 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $349.31 | ▼ 09:30 equity $9,784.79 vs yday $9,805.42 (-20.63) | 09:30 open · cash $349.31 (unchanged overnight, no fees) · equity $9,784.79 vs prior close $9,805.42 (-20.63) · 8 name(s) re-marked at the open (per-name table). URBN×15 yday $79.29 → 09:30 $78.84 -6.75; ANF×8 yday $140.68 → 09:30 $139.65 -8.24; BZ×68 yday $17.55 → 09:30 $17.65 +6.80; SMTC×8 yday $132.27 → 09:30 $133.00 +5.84; BBWI×66 yday $18.61 → 09:30 $18.41 -13.20; CRDL×607 yday $2.15 → 09:30 $2.16 +6.07; NCNO×53 yday $22.30 → 09:30 $22.20 -5.30; TH×65 yday $18.07 → 09:30 $17.98 -5.85 | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 15 | $78.84 | $2.06 | $-12.79 | $1,529.85 | ▼ -12.79 after sell → book $9,782.73; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 8 | $139.65 | $2.03 | $-55.41 | $2,645.02 | ▼ -55.41 after sell → book $9,780.70; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 68 | $17.65 | $2.22 | $-38.41 | $3,843.01 | ▼ -38.41 after sell → book $9,778.49; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $133.00 | $2.03 | $-74.13 | $4,904.97 | ▼ -74.13 after sell → book $9,776.45; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 66 | $18.41 | $2.21 | $-26.84 | $6,117.82 | ▼ -26.84 after sell → book $9,774.24; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRDL` | 607 | $2.16 | $7.94 | $+44.93 | $7,421.00 | ▲ +44.93 after sell → book $9,766.30; vs 09:30 mark -7.94 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `NCNO` | 53 | $22.20 | $2.17 | $-62.62 | $8,595.43 | ▼ -62.62 after sell → book $9,764.13; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TH` | 65 | $17.98 | $2.21 | $-70.69 | $9,761.93 | ▼ -70.69 after sell → book $9,761.93; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,761.93 | ▲ close $9,761.93 vs 09:30 $9,784.79 (session +0.00) | 16:00 close · cash $9,761.93 · no lots left · equity $9,761.93. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,761.93 | ▲ 09:30 equity $9,761.93 vs yday $9,761.93 (-0.00) | 09:30 open · cash $9,761.93 · no holdings · equity $9,761.93 vs prior close $9,761.93 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $8,567.86 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1220.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 78 | $15.45 | $2.22 | — | $7,360.54 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1220.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $6,190.96 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1220.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 72 | $16.77 | $2.21 | — | $4,981.32 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1220.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $55.42 | $2.06 | — | $3,760.02 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; ret5=-25.9; leftover $1220.24 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 559 | $2.18 | $7.21 | — | $2,534.19 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1220.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 51 | $23.88 | $2.14 | — | $1,314.17 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1220.24 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 117 | $10.42 | $2.34 | — | $92.68 | — | combo gate; gate vol=good,ab=good; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1220.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $92.68 | ▼ close $9,617.39 vs 09:30 $9,761.93 (session -122.32) | 16:00 close · cash $92.68 · equity $9,617.39 vs 09:30 $9,761.93 (-144.54; session marks -122.32) · 8 name(s) marked open→close (per-name table). RVTY×9 09:30 $132.45 → close $130.63 -16.38; CRK×78 09:30 $15.45 → close $14.95 -39.00; MRNA×8 09:30 $145.94 → close $148.87 +23.40; ARCT×72 09:30 $16.77 → close $15.56 -87.12; EIX×22 09:30 $55.42 → close $56.30 +19.36; CRDL×559 09:30 $2.18 → close $2.16 -11.18; MMED×51 09:30 $23.88 → close $23.84 -2.04; NVAX×117 09:30 $10.42 → close $10.34 -9.36 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.68 | ▲ 09:30 equity $9,664.99 vs yday $9,617.39 (+47.60) | 09:30 open · cash $92.68 (unchanged overnight, no fees) · equity $9,664.99 vs prior close $9,617.39 (+47.60) · 8 name(s) re-marked at the open (per-name table). RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; CRK×78 yday $14.95 → 09:30 $15.00 +3.90; MRNA×8 yday $148.87 → 09:30 $153.62 +38.00; ARCT×72 yday $15.56 → 09:30 $15.61 +3.60; EIX×22 yday $56.30 → 09:30 $55.79 -11.22; CRDL×559 yday $2.16 → 09:30 $2.16 +0.00; MMED×51 yday $23.84 → 09:30 $23.84 +0.00; NVAX×117 yday $10.34 → 09:30 $10.50 +18.72 | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 3 | $3.46 | $0.11 | — | $82.19 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $11.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 4 | $2.52 | $0.11 | — | $72.00 | — | combo gate; gate vol=good,ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $11.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 1 | $6.71 | $0.07 | — | $65.22 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $11.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 6 | $1.90 | $0.13 | — | $53.69 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $11.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 2 | $4.78 | $0.10 | — | $44.02 | — | combo gate; gate vol=good,ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $11.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $32.60 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $11.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.60 | ▼ close $9,619.60 vs 09:30 $9,664.99 (session -44.75) | 16:00 close · cash $32.60 · equity $9,619.60 vs 09:30 $9,664.99 (-45.39; session marks -44.75) · 14 name(s) marked open→close (per-name table). RVTY×9 09:30 $130.03 → close $130.22 +1.71; CRK×78 09:30 $15.00 → close $15.26 +20.28; MRNA×8 09:30 $153.62 → close $145.55 -64.56; ARCT×72 09:30 $15.61 → close $15.82 +15.12; EIX×22 09:30 $55.79 → close $56.77 +21.56; CRDL×559 09:30 $2.16 → close $2.20 +22.36; MMED×51 09:30 $23.84 → close $23.29 -28.05; NVAX×117 09:30 $10.50 → close $10.22 -32.76; CABA×3 09:30 $3.46 → close $3.47 +0.03; ALEC×4 09:30 $2.52 → close $2.46 -0.24; BHC×1 09:30 $6.71 → close $6.56 -0.15; BMEA×6 09:30 $1.90 → close $2.03 +0.78; OABI×2 09:30 $4.78 → close $4.33 -0.90; VIR×1 09:30 $11.31 → close $11.38 +0.07 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.60 | ▼ 09:30 equity $9,574.30 vs yday $9,619.60 (-45.30) | 09:30 open · cash $32.60 (unchanged overnight, no fees) · equity $9,574.30 vs prior close $9,619.60 (-45.30) · 14 name(s) re-marked at the open (per-name table). RVTY×9 yday $130.22 → 09:30 $128.50 -15.48; CRK×78 yday $15.26 → 09:30 $15.50 +18.72; MRNA×8 yday $145.55 → 09:30 $145.98 +3.44; ARCT×72 yday $15.82 → 09:30 $15.47 -25.20; EIX×22 yday $56.77 → 09:30 $56.53 -5.28; CRDL×559 yday $2.20 → 09:30 $2.20 +0.00; MMED×51 yday $23.29 → 09:30 $23.16 -6.63; NVAX×117 yday $10.22 → 09:30 $10.10 -14.04; CABA×3 yday $3.47 → 09:30 $3.43 -0.12; ALEC×4 yday $2.46 → 09:30 $2.38 -0.32; BHC×1 yday $6.56 → 09:30 $6.57 +0.01; BMEA×6 yday $2.03 → 09:30 $2.00 -0.18; OABI×2 yday $4.33 → 09:30 $4.30 -0.06; VIR×1 yday $11.38 → 09:30 $11.22 -0.16 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.60 | ▲ close $9,591.95 vs 09:30 $9,574.30 (session +17.65) | 16:00 close · cash $32.60 · equity $9,591.95 vs 09:30 $9,574.30 (+17.65; session marks +17.65) · 14 name(s) marked open→close (per-name table). RVTY×9 09:30 $128.50 → close $127.08 -12.78; CRK×78 09:30 $15.50 → close $15.16 -26.52; MRNA×8 09:30 $145.98 → close $140.33 -45.20; ARCT×72 09:30 $15.47 → close $15.63 +11.52; EIX×22 09:30 $56.53 → close $59.33 +61.60; CRDL×559 09:30 $2.20 → close $2.22 +11.18; MMED×51 09:30 $23.16 → close $23.32 +8.16; NVAX×117 09:30 $10.10 → close $10.19 +10.53; CABA×3 09:30 $3.43 → close $3.27 -0.48; ALEC×4 09:30 $2.38 → close $2.47 +0.36; BHC×1 09:30 $6.57 → close $6.43 -0.14; BMEA×6 09:30 $2.00 → close $1.93 -0.42; OABI×2 09:30 $4.30 → close $4.24 -0.12; VIR×1 09:30 $11.22 → close $11.18 -0.04 | — |

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
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `QSI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZIP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DKS` | cash | leftover split 62.61 < 1 share @ 121.87 |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZIP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SLQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SLQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NCNO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NCNO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 11.59 < 1 share @ 513.78 |
| 2026-09-04 | `MLYS` | cash | leftover split 11.59 < 1 share @ 28.00 |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `EIX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `NVAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BTBT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RVTY` | 9 | 2026-09-03 @ $132.45 | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1220.24 |
| `CRK` | 78 | 2026-09-03 @ $15.45 | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1220.24 |
| `MRNA` | 8 | 2026-09-03 @ $145.94 | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1220.24 |
| `ARCT` | 72 | 2026-09-03 @ $16.77 | combo gate; gate vol=good,ab=good; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1220.24 |
| `EIX` | 22 | 2026-09-03 @ $55.42 | combo gate; gate vol=good,ab=good; list probable,yday_gainer; ret5=-25.9; leftover $1220.24 |
| `CRDL` | 559 | 2026-09-03 @ $2.18 | combo gate; gate vol=good,ab=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1220.24 |
| `MMED` | 51 | 2026-09-03 @ $23.88 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1220.24 |
| `NVAX` | 117 | 2026-09-03 @ $10.42 | combo gate; gate vol=good,ab=good; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1220.24 |
| `CABA` | 3 | 2026-09-04 @ $3.46 | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $11.59 |
| `ALEC` | 4 | 2026-09-04 @ $2.52 | combo gate; gate vol=good,ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $11.59 |
| `BHC` | 1 | 2026-09-04 @ $6.71 | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $11.59 |
| `BMEA` | 6 | 2026-09-04 @ $1.90 | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $11.59 |
| `OABI` | 2 | 2026-09-04 @ $4.78 | combo gate; gate vol=good,ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $11.59 |
| `VIR` | 1 | 2026-09-04 @ $11.31 | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $11.59 |
