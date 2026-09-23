# Factor mine action — `union_oppset_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `opp_rvol` · size `leftover` · sell `list` · S-boost `none` · Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP)

Cash book **+8.29%** ($10,829) · signal-only (no cash/fees) was +18.00%. Starts YES **26/29**. Fills 186 · skips 87 · realized $+829.46.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: Theme Radar T−1 relative volume (Clock-B opportunity-set; not same-day RelVol).
- Must-have: Theme Radar Clock-B opportunity-set: T−1 gap or RelVol (or week move) flagged — not today's Gap/RelVol.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by Theme Radar T−1 relative volume (Clock-B opportunity-set; not same-day RelVol) and keep the top 8.
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
- **Gate** `oppset=True` · **rank** `opp_rvol` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,829.43.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ARX` | 63 | — | $19.57 | +0.00 | $19.58 | +0.63 | +0.63 | +0.00 | +0.63 |
| 2026-08-14 | `CLBT` | 115 | — | $10.83 | +0.00 | $11.14 | +35.65 | +35.65 | +0.00 | +35.65 |
| 2026-08-14 | `AIRO` | 112 | — | $11.12 | +0.00 | $9.57 | -173.60 | -173.60 | +0.00 | -173.60 |
| 2026-08-14 | `SECZ` | 214 | — | $5.84 | +0.00 | $5.61 | -49.22 | -49.22 | +0.00 | -49.22 |
| 2026-08-14 | `TBBB` | 25 | — | $48.82 | +0.00 | $47.79 | -25.75 | -25.75 | +0.00 | -25.75 |
| 2026-08-14 | `REZI` | 60 | — | $20.56 | +0.00 | $20.50 | -3.60 | -3.60 | +0.00 | -3.60 |
| 2026-08-14 | `STUB` | 163 | — | $7.66 | +0.00 | $8.08 | +68.46 | +68.46 | +0.00 | +68.46 |
| 2026-08-14 | `QMCO` | 50 | — | $24.68 | +0.00 | $26.11 | +71.50 | +71.50 | +0.00 | +71.50 |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | — | +0.00 | -0.63 | +0.00 | — |
| 2026-08-17 | `CLBT` | 115 | $11.14 | $11.19 | +5.75 | — | +0.00 | +5.75 | +41.40 | — |
| 2026-08-17 | `AIRO` | 112 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -173.60 | — |
| 2026-08-17 | `SECZ` | 214 | $5.61 | $5.45 | -34.24 | — | +0.00 | -34.24 | -83.46 | — |
| 2026-08-17 | `TBBB` | 25 | $47.79 | $47.39 | -10.00 | — | +0.00 | -10.00 | -35.75 | — |
| 2026-08-17 | `REZI` | 60 | $20.50 | $20.83 | +19.80 | — | +0.00 | +19.80 | +16.20 | — |
| 2026-08-17 | `STUB` | 163 | $8.08 | $7.91 | -27.71 | — | +0.00 | -27.71 | +40.75 | — |
| 2026-08-17 | `QMCO` | 50 | $26.11 | $24.83 | -64.00 | — | +0.00 | -64.00 | +7.50 | — |
| 2026-08-17 | `CAPR` | 177 | — | $6.87 | +0.00 | $7.45 | +102.66 | +102.66 | +0.00 | +102.66 |
| 2026-08-17 | `HTFL` | 29 | — | $41.23 | +0.00 | $41.94 | +20.59 | +20.59 | +0.00 | +20.59 |
| 2026-08-17 | `NMAX` | 111 | — | $10.97 | +0.00 | $10.36 | -67.71 | -67.71 | +0.00 | -67.71 |
| 2026-08-17 | `RDDT` | 6 | — | $177.51 | +0.00 | $164.50 | -78.06 | -78.06 | +0.00 | -78.06 |
| 2026-08-17 | `VERA` | 39 | — | $31.30 | +0.00 | $31.63 | +12.87 | +12.87 | +0.00 | +12.87 |
| 2026-08-17 | `TSSI` | 127 | — | $9.61 | +0.00 | $9.48 | -16.51 | -16.51 | +0.00 | -16.51 |
| 2026-08-17 | `UMAC` | 37 | — | $32.55 | +0.00 | $30.15 | -88.80 | -88.80 | +0.00 | -88.80 |
| 2026-08-17 | `NU` | 79 | — | $15.40 | +0.00 | $14.74 | -52.14 | -52.14 | +0.00 | -52.14 |
| 2026-08-18 | `CAPR` | 177 | $7.45 | $7.50 | +8.85 | $7.08 | -74.34 | -65.49 | +111.51 | +37.17 |
| 2026-08-18 | `HTFL` | 29 | $41.94 | $41.50 | -12.76 | — | +0.00 | -12.76 | +7.83 | — |
| 2026-08-18 | `NMAX` | 111 | $10.36 | $10.31 | -5.55 | — | +0.00 | -5.55 | -73.26 | — |
| 2026-08-18 | `RDDT` | 6 | $164.50 | $166.10 | +9.60 | — | +0.00 | +9.60 | -68.46 | — |
| 2026-08-18 | `VERA` | 39 | $31.63 | $31.31 | -12.48 | — | +0.00 | -12.48 | +0.39 | — |
| 2026-08-18 | `TSSI` | 127 | $9.48 | $9.22 | -33.02 | — | +0.00 | -33.02 | -49.53 | — |
| 2026-08-18 | `UMAC` | 37 | $30.15 | $28.59 | -57.72 | — | +0.00 | -57.72 | -146.52 | — |
| 2026-08-18 | `NU` | 79 | $14.74 | $14.53 | -16.59 | — | +0.00 | -16.59 | -68.73 | — |
| 2026-08-19 | `CAPR` | 177 | $7.08 | $7.19 | +19.47 | — | +0.00 | +19.47 | +56.64 | — |
| 2026-08-20 | `MRNA` | 7 | — | $150.14 | +0.00 | $133.32 | -117.74 | -117.74 | +0.00 | -117.74 |
| 2026-08-20 | `BNTX` | 10 | — | $109.06 | +0.00 | $110.89 | +18.30 | +18.30 | +0.00 | +18.30 |
| 2026-08-20 | `WYFI` | 54 | — | $21.40 | +0.00 | $21.16 | -12.96 | -12.96 | +0.00 | -12.96 |
| 2026-08-20 | `MRVI` | 157 | — | $7.44 | +0.00 | $8.29 | +133.45 | +133.45 | +0.00 | +133.45 |
| 2026-08-20 | `LZB` | 34 | — | $33.61 | +0.00 | $33.65 | +1.36 | +1.36 | +0.00 | +1.36 |
| 2026-08-20 | `EL` | 12 | — | $97.43 | +0.00 | $96.15 | -15.36 | -15.36 | +0.00 | -15.36 |
| 2026-08-20 | `TEM` | 19 | — | $61.83 | +0.00 | $66.65 | +91.58 | +91.58 | +0.00 | +91.58 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `MRNA` | 7 | $133.32 | $133.11 | -1.47 | $145.13 | +84.14 | +82.67 | -119.21 | -35.07 |
| 2026-08-21 | `BNTX` | 10 | $110.89 | $110.92 | +0.30 | — | +0.00 | +0.30 | +18.60 | — |
| 2026-08-21 | `WYFI` | 54 | $21.16 | $21.54 | +20.52 | — | +0.00 | +20.52 | +7.56 | — |
| 2026-08-21 | `MRVI` | 157 | $8.29 | $8.28 | -1.57 | $8.64 | +56.52 | +54.95 | +131.88 | +188.40 |
| 2026-08-21 | `LZB` | 34 | $33.65 | $33.63 | -0.68 | — | +0.00 | -0.68 | +0.68 | — |
| 2026-08-21 | `EL` | 12 | $96.15 | $96.75 | +7.20 | — | +0.00 | +7.20 | -8.16 | — |
| 2026-08-21 | `TEM` | 19 | $66.65 | $65.60 | -19.95 | $72.69 | +134.71 | +114.76 | +71.63 | +206.34 |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AAP` | 28 | — | $42.41 | +0.00 | $42.58 | +4.76 | +4.76 | +0.00 | +4.76 |
| 2026-08-21 | `ARCT` | 109 | — | $11.13 | +0.00 | $13.45 | +252.88 | +252.88 | +0.00 | +252.88 |
| 2026-08-21 | `WMT` | 11 | — | $103.69 | +0.00 | $103.70 | +0.11 | +0.11 | +0.00 | +0.11 |
| 2026-08-21 | `AMRC` | 53 | — | $22.51 | +0.00 | $21.38 | -59.89 | -59.89 | +0.00 | -59.89 |
| 2026-08-21 | `GMAB` | 36 | — | $33.36 | +0.00 | $33.45 | +3.24 | +3.24 | +0.00 | +3.24 |
| 2026-08-24 | `MRNA` | 7 | $145.13 | $142.70 | -17.01 | — | +0.00 | -17.01 | -52.08 | — |
| 2026-08-24 | `MRVI` | 157 | $8.64 | $8.59 | -7.85 | — | +0.00 | -7.85 | +180.55 | — |
| 2026-08-24 | `TEM` | 19 | $72.69 | $70.08 | -49.68 | — | +0.00 | -49.68 | +156.66 | — |
| 2026-08-24 | `AAP` | 28 | $42.58 | $43.05 | +13.16 | — | +0.00 | +13.16 | +17.92 | — |
| 2026-08-24 | `ARCT` | 109 | $13.45 | $13.33 | -13.08 | — | +0.00 | -13.08 | +239.80 | — |
| 2026-08-24 | `WMT` | 11 | $103.70 | $104.14 | +4.84 | — | +0.00 | +4.84 | +4.95 | — |
| 2026-08-24 | `AMRC` | 53 | $21.38 | $21.19 | -10.07 | — | +0.00 | -10.07 | -69.96 | — |
| 2026-08-24 | `GMAB` | 36 | $33.45 | $32.82 | -22.68 | — | +0.00 | -22.68 | -19.44 | — |
| 2026-08-25 | `ABUS` | 235 | — | $5.25 | +0.00 | $5.20 | -11.75 | -11.75 | +0.00 | -11.75 |
| 2026-08-25 | `ZURA` | 194 | — | $6.37 | +0.00 | $6.32 | -9.70 | -9.70 | +0.00 | -9.70 |
| 2026-08-25 | `CAPR` | 170 | — | $7.25 | +0.00 | $8.29 | +176.80 | +176.80 | +0.00 | +176.80 |
| 2026-08-25 | `FWDI` | 216 | — | $5.71 | +0.00 | $6.05 | +73.44 | +73.44 | +0.00 | +73.44 |
| 2026-08-25 | `ALVO` | 236 | — | $5.24 | +0.00 | $5.05 | -44.84 | -44.84 | +0.00 | -44.84 |
| 2026-08-25 | `ASST` | 65 | — | $19.04 | +0.00 | $21.39 | +152.75 | +152.75 | +0.00 | +152.75 |
| 2026-08-25 | `LIFE` | 33 | — | $36.96 | +0.00 | $38.56 | +52.80 | +52.80 | +0.00 | +52.80 |
| 2026-08-25 | `QFIN` | 111 | — | $11.09 | +0.00 | $11.53 | +48.84 | +48.84 | +0.00 | +48.84 |
| 2026-08-26 | `ABUS` | 235 | $5.20 | $5.19 | -2.35 | — | +0.00 | -2.35 | -14.10 | — |
| 2026-08-26 | `ZURA` | 194 | $6.32 | $6.13 | -36.86 | — | +0.00 | -36.86 | -46.56 | — |
| 2026-08-26 | `CAPR` | 170 | $8.29 | $8.29 | +0.00 | $9.36 | +181.90 | +181.90 | +176.80 | +358.70 |
| 2026-08-26 | `FWDI` | 216 | $6.05 | $5.97 | -17.28 | — | +0.00 | -17.28 | +56.16 | — |
| 2026-08-26 | `ALVO` | 236 | $5.05 | $4.98 | -16.52 | — | +0.00 | -16.52 | -61.36 | — |
| 2026-08-26 | `ASST` | 65 | $21.39 | $20.72 | -43.55 | — | +0.00 | -43.55 | +109.20 | — |
| 2026-08-26 | `LIFE` | 33 | $38.56 | $38.24 | -10.56 | — | +0.00 | -10.56 | +42.24 | — |
| 2026-08-26 | `QFIN` | 111 | $11.53 | $9.76 | -196.47 | $9.35 | -45.51 | -241.98 | -147.63 | -193.14 |
| 2026-08-26 | `DKS` | 10 | — | $121.87 | +0.00 | $129.66 | +77.90 | +77.90 | +0.00 | +77.90 |
| 2026-08-26 | `BZ` | 74 | — | $16.77 | +0.00 | $18.84 | +153.18 | +153.18 | +0.00 | +153.18 |
| 2026-08-26 | `MAIR` | 45 | — | $27.59 | +0.00 | $28.51 | +41.40 | +41.40 | +0.00 | +41.40 |
| 2026-08-26 | `DY` | 3 | — | $326.91 | +0.00 | $310.91 | -48.00 | -48.00 | +0.00 | -48.00 |
| 2026-08-26 | `KURA` | 91 | — | $13.63 | +0.00 | $13.06 | -51.87 | -51.87 | +0.00 | -51.87 |
| 2026-08-26 | `SMTC` | 9 | — | $130.90 | +0.00 | $140.80 | +89.10 | +89.10 | +0.00 | +89.10 |
| 2026-08-27 | `CAPR` | 170 | $9.36 | $9.19 | -28.90 | — | +0.00 | -28.90 | +329.80 | — |
| 2026-08-27 | `QFIN` | 111 | $9.35 | $9.42 | +7.77 | — | +0.00 | +7.77 | -185.37 | — |
| 2026-08-27 | `DKS` | 10 | $129.66 | $128.73 | -9.30 | — | +0.00 | -9.30 | +68.60 | — |
| 2026-08-27 | `BZ` | 74 | $18.84 | $18.50 | -25.16 | — | +0.00 | -25.16 | +128.02 | — |
| 2026-08-27 | `MAIR` | 45 | $28.51 | $28.76 | +11.25 | — | +0.00 | +11.25 | +52.65 | — |
| 2026-08-27 | `DY` | 3 | $310.91 | $314.90 | +11.97 | — | +0.00 | +11.97 | -36.03 | — |
| 2026-08-27 | `KURA` | 91 | $13.06 | $12.98 | -7.28 | — | +0.00 | -7.28 | -59.15 | — |
| 2026-08-27 | `SMTC` | 9 | $140.80 | $149.40 | +77.40 | — | +0.00 | +77.40 | +166.50 | — |
| 2026-08-28 | `ANF` | 8 | — | $146.07 | +0.00 | $148.42 | +18.80 | +18.80 | +0.00 | +18.80 |
| 2026-08-28 | `BZ` | 71 | — | $18.15 | +0.00 | $17.80 | -24.85 | -24.85 | +0.00 | -24.85 |
| 2026-08-28 | `QFIN` | 141 | — | $9.15 | +0.00 | $8.80 | -49.35 | -49.35 | +0.00 | -49.35 |
| 2026-08-28 | `BHVN` | 81 | — | $15.88 | +0.00 | $15.41 | -38.07 | -38.07 | +0.00 | -38.07 |
| 2026-08-28 | `DY` | 4 | — | $306.34 | +0.00 | $294.34 | -48.00 | -48.00 | +0.00 | -48.00 |
| 2026-08-28 | `GENB` | 82 | — | $15.77 | +0.00 | $15.33 | -36.08 | -36.08 | +0.00 | -36.08 |
| 2026-08-28 | `URBN` | 16 | — | $79.42 | +0.00 | $81.09 | +26.72 | +26.72 | +0.00 | +26.72 |
| 2026-08-28 | `JKS` | 97 | — | $13.37 | +0.00 | $13.54 | +16.49 | +16.49 | +0.00 | +16.49 |
| 2026-08-31 | `ANF` | 8 | $148.42 | $148.03 | -3.12 | — | +0.00 | -3.12 | +15.68 | — |
| 2026-08-31 | `BZ` | 71 | $17.80 | $17.70 | -7.10 | — | +0.00 | -7.10 | -31.95 | — |
| 2026-08-31 | `QFIN` | 141 | $8.80 | $8.70 | -14.10 | — | +0.00 | -14.10 | -63.45 | — |
| 2026-08-31 | `BHVN` | 81 | $15.41 | $15.46 | +4.05 | — | +0.00 | +4.05 | -34.02 | — |
| 2026-08-31 | `DY` | 4 | $294.34 | $298.01 | +14.68 | — | +0.00 | +14.68 | -33.32 | — |
| 2026-08-31 | `GENB` | 82 | $15.33 | $15.27 | -4.92 | — | +0.00 | -4.92 | -41.00 | — |
| 2026-08-31 | `URBN` | 16 | $81.09 | $80.44 | -10.40 | — | +0.00 | -10.40 | +16.32 | — |
| 2026-08-31 | `JKS` | 97 | $13.54 | $13.54 | +0.00 | — | +0.00 | +0.00 | +16.49 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ALMS` | 122 | — | $10.38 | +0.00 | $11.36 | +120.17 | +120.17 | +0.00 | +120.17 |
| 2026-09-03 | `FRVO` | 69 | — | $18.28 | +0.00 | $17.16 | -77.28 | -77.28 | +0.00 | -77.28 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `CNH` | 92 | — | $13.71 | +0.00 | $13.84 | +11.96 | +11.96 | +0.00 | +11.96 |
| 2026-09-03 | `EIX` | 22 | — | $55.42 | +0.00 | $56.30 | +19.36 | +19.36 | +0.00 | +19.36 |
| 2026-09-03 | `MMED` | 53 | — | $23.88 | +0.00 | $23.84 | -2.12 | -2.12 | +0.00 | -2.12 |
| 2026-09-03 | `RSKD` | 190 | — | $6.68 | +0.00 | $6.93 | +47.50 | +47.50 | +0.00 | +47.50 |
| 2026-09-03 | `AGCO` | 9 | — | $127.91 | +0.00 | $125.82 | -18.81 | -18.81 | +0.00 | -18.81 |
| 2026-09-04 | `ALMS` | 122 | $11.36 | $11.23 | -15.86 | — | +0.00 | -15.86 | +104.31 | — |
| 2026-09-04 | `FRVO` | 69 | $17.16 | $17.27 | +7.59 | — | +0.00 | +7.59 | -69.69 | — |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | $524.14 | +20.72 | +15.50 | +54.94 | +75.66 |
| 2026-09-04 | `CNH` | 92 | $13.84 | $13.89 | +4.60 | — | +0.00 | +4.60 | +16.56 | — |
| 2026-09-04 | `EIX` | 22 | $56.30 | $55.79 | -11.22 | — | +0.00 | -11.22 | +8.14 | — |
| 2026-09-04 | `MMED` | 53 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.12 | — |
| 2026-09-04 | `RSKD` | 190 | $6.93 | $6.84 | -17.10 | $6.51 | -62.70 | -79.80 | +30.40 | -32.30 |
| 2026-09-04 | `AGCO` | 9 | $125.82 | $125.22 | -5.40 | — | +0.00 | -5.40 | -24.21 | — |
| 2026-09-04 | `LULU` | 13 | — | $98.15 | +0.00 | $100.61 | +31.98 | +31.98 | +0.00 | +31.98 |
| 2026-09-04 | `ASST` | 52 | — | $25.18 | +0.00 | $27.14 | +101.92 | +101.92 | +0.00 | +101.92 |
| 2026-09-04 | `PL` | 67 | — | $19.64 | +0.00 | $18.12 | -101.84 | -101.84 | +0.00 | -101.84 |
| 2026-09-04 | `ZS` | 7 | — | $166.15 | +0.00 | $169.80 | +25.55 | +25.55 | +0.00 | +25.55 |
| 2026-09-04 | `IOT` | 29 | — | $44.90 | +0.00 | $40.20 | -136.30 | -136.30 | +0.00 | -136.30 |
| 2026-09-04 | `MRX` | 17 | — | $75.65 | +0.00 | $78.27 | +44.54 | +44.54 | +0.00 | +44.54 |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | — | +0.00 | -5.98 | +69.68 | — |
| 2026-09-08 | `RSKD` | 190 | $6.51 | $6.46 | -9.50 | — | +0.00 | -9.50 | -41.80 | — |
| 2026-09-08 | `LULU` | 13 | $100.61 | $100.58 | -0.39 | — | +0.00 | -0.39 | +31.59 | — |
| 2026-09-08 | `ASST` | 52 | $27.14 | $26.44 | -36.40 | — | +0.00 | -36.40 | +65.52 | — |
| 2026-09-08 | `PL` | 67 | $18.12 | $17.85 | -18.09 | — | +0.00 | -18.09 | -119.93 | — |
| 2026-09-08 | `ZS` | 7 | $169.80 | $165.62 | -29.30 | — | +0.00 | -29.30 | -3.74 | — |
| 2026-09-08 | `IOT` | 29 | $40.20 | $39.56 | -18.56 | — | +0.00 | -18.56 | -154.86 | — |
| 2026-09-08 | `MRX` | 17 | $78.27 | $78.84 | +9.69 | — | +0.00 | +9.69 | +54.23 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `COO` | 23 | — | $54.66 | +0.00 | $53.91 | -17.25 | -17.25 | +0.00 | -17.25 |
| 2026-09-11 | `AEO` | 85 | — | $14.71 | +0.00 | $15.02 | +26.35 | +26.35 | +0.00 | +26.35 |
| 2026-09-11 | `WLTH` | 114 | — | $10.95 | +0.00 | $10.38 | -64.98 | -64.98 | +0.00 | -64.98 |
| 2026-09-11 | `NAVN` | 61 | — | $20.61 | +0.00 | $21.02 | +25.01 | +25.01 | +0.00 | +25.01 |
| 2026-09-11 | `TSSI` | 140 | — | $8.98 | +0.00 | $8.93 | -7.00 | -7.00 | +0.00 | -7.00 |
| 2026-09-11 | `IRD` | 204 | — | $6.16 | +0.00 | $6.04 | -24.48 | -24.48 | +0.00 | -24.48 |
| 2026-09-11 | `AXGN` | 29 | — | $42.48 | +0.00 | $42.16 | -9.28 | -9.28 | +0.00 | -9.28 |
| 2026-09-11 | `TYRA` | 53 | — | $23.63 | +0.00 | $22.03 | -84.80 | -84.80 | +0.00 | -84.80 |
| 2026-09-14 | `COO` | 23 | $53.91 | $54.78 | +20.01 | — | +0.00 | +20.01 | +2.76 | — |
| 2026-09-14 | `AEO` | 85 | $15.02 | $14.85 | -14.45 | — | +0.00 | -14.45 | +11.90 | — |
| 2026-09-14 | `WLTH` | 114 | $10.38 | $10.29 | -10.26 | — | +0.00 | -10.26 | -75.24 | — |
| 2026-09-14 | `NAVN` | 61 | $21.02 | $21.10 | +4.88 | — | +0.00 | +4.88 | +29.89 | — |
| 2026-09-14 | `TSSI` | 140 | $8.93 | $8.57 | -50.40 | — | +0.00 | -50.40 | -57.40 | — |
| 2026-09-14 | `IRD` | 204 | $6.04 | $6.02 | -4.08 | — | +0.00 | -4.08 | -28.56 | — |
| 2026-09-14 | `AXGN` | 29 | $42.16 | $41.55 | -17.69 | — | +0.00 | -17.69 | -26.97 | — |
| 2026-09-14 | `TYRA` | 53 | $22.03 | $23.20 | +62.01 | — | +0.00 | +62.01 | -22.79 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `PLAY` | 179 | — | $6.86 | +0.00 | $6.86 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-16 | `ALHC` | 119 | — | $10.30 | +0.00 | $8.71 | -189.21 | -189.21 | +0.00 | -189.21 |
| 2026-09-16 | `FPS` | 37 | — | $33.14 | +0.00 | $34.84 | +62.90 | +62.90 | +0.00 | +62.90 |
| 2026-09-16 | `GFR` | 180 | — | $6.83 | +0.00 | $6.49 | -61.20 | -61.20 | +0.00 | -61.20 |
| 2026-09-16 | `HQ` | 95 | — | $12.89 | +0.00 | $13.56 | +63.65 | +63.65 | +0.00 | +63.65 |
| 2026-09-16 | `SDGR` | 52 | — | $23.29 | +0.00 | $23.93 | +33.28 | +33.28 | +0.00 | +33.28 |
| 2026-09-16 | `DMRA` | 49 | — | $24.88 | +0.00 | $24.53 | -17.15 | -17.15 | +0.00 | -17.15 |
| 2026-09-16 | `RVTY` | 8 | — | $140.88 | +0.00 | $145.73 | +38.80 | +38.80 | +0.00 | +38.80 |
| 2026-09-17 | `PLAY` | 179 | $6.86 | $6.96 | +17.90 | — | +0.00 | +17.90 | +17.90 | — |
| 2026-09-17 | `ALHC` | 119 | $8.71 | $8.58 | -15.47 | $8.70 | +14.28 | -1.19 | -204.68 | -190.40 |
| 2026-09-17 | `FPS` | 37 | $34.84 | $36.76 | +71.04 | $38.06 | +48.10 | +119.14 | +133.94 | +182.04 |
| 2026-09-17 | `GFR` | 180 | $6.49 | $6.48 | -1.80 | — | +0.00 | -1.80 | -63.00 | — |
| 2026-09-17 | `HQ` | 95 | $13.56 | $13.56 | +0.00 | — | +0.00 | +0.00 | +63.65 | — |
| 2026-09-17 | `SDGR` | 52 | $23.93 | $24.09 | +8.32 | — | +0.00 | +8.32 | +41.60 | — |
| 2026-09-17 | `DMRA` | 49 | $24.53 | $24.96 | +21.07 | — | +0.00 | +21.07 | +3.92 | — |
| 2026-09-17 | `RVTY` | 8 | $145.73 | $147.61 | +15.04 | — | +0.00 | +15.04 | +53.84 | — |
| 2026-09-17 | `BBNX` | 55 | — | $22.46 | +0.00 | $21.43 | -56.65 | -56.65 | +0.00 | -56.65 |
| 2026-09-17 | `JBHT` | 5 | — | $238.60 | +0.00 | $236.80 | -9.00 | -9.00 | +0.00 | -9.00 |
| 2026-09-17 | `ALMU` | 111 | — | $11.21 | +0.00 | $11.54 | +37.18 | +37.18 | +0.00 | +37.18 |
| 2026-09-17 | `ARQT` | 48 | — | $25.95 | +0.00 | $26.46 | +24.48 | +24.48 | +0.00 | +24.48 |
| 2026-09-17 | `AMRX` | 67 | — | $18.56 | +0.00 | $18.28 | -18.76 | -18.76 | +0.00 | -18.76 |
| 2026-09-17 | `BTGO` | 190 | — | $6.56 | +0.00 | $6.76 | +38.00 | +38.00 | +0.00 | +38.00 |
| 2026-09-18 | `ALHC` | 119 | $8.70 | $8.68 | -2.38 | — | +0.00 | -2.38 | -192.78 | — |
| 2026-09-18 | `FPS` | 37 | $38.06 | $39.50 | +53.28 | — | +0.00 | +53.28 | +235.32 | — |
| 2026-09-18 | `BBNX` | 55 | $21.43 | $21.30 | -7.15 | — | +0.00 | -7.15 | -63.80 | — |
| 2026-09-18 | `JBHT` | 5 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -9.00 | — |
| 2026-09-18 | `ALMU` | 111 | $11.54 | $11.64 | +10.55 | $12.72 | +120.43 | +130.98 | +47.73 | +168.16 |
| 2026-09-18 | `ARQT` | 48 | $26.46 | $26.14 | -15.36 | $25.38 | -36.48 | -51.84 | +9.12 | -27.36 |
| 2026-09-18 | `AMRX` | 67 | $18.28 | $18.12 | -10.72 | — | +0.00 | -10.72 | -29.48 | — |
| 2026-09-18 | `BTGO` | 190 | $6.76 | $6.94 | +34.20 | — | +0.00 | +34.20 | +72.20 | — |
| 2026-09-18 | `GNRC` | 5 | — | $209.52 | +0.00 | $207.44 | -10.40 | -10.40 | +0.00 | -10.40 |
| 2026-09-18 | `SDGR` | 42 | — | $29.32 | +0.00 | $29.02 | -12.60 | -12.60 | +0.00 | -12.60 |
| 2026-09-18 | `FLNC` | 164 | — | $7.54 | +0.00 | $7.32 | -35.26 | -35.26 | +0.00 | -35.26 |
| 2026-09-18 | `RARE` | 83 | — | $14.79 | +0.00 | $14.51 | -23.24 | -23.24 | +0.00 | -23.24 |
| 2026-09-18 | `SECZ` | 133 | — | $9.32 | +0.00 | $10.86 | +204.82 | +204.82 | +0.00 | +204.82 |
| 2026-09-18 | `USDE` | 130 | — | $9.54 | +0.00 | $10.19 | +84.50 | +84.50 | +0.00 | +84.50 |
| 2026-09-21 | `ALMU` | 111 | $12.72 | $13.12 | +44.40 | — | +0.00 | +44.40 | +212.56 | — |
| 2026-09-21 | `ARQT` | 48 | $25.38 | $25.57 | +9.12 | — | +0.00 | +9.12 | -18.24 | — |
| 2026-09-21 | `GNRC` | 5 | $207.44 | $210.00 | +12.80 | — | +0.00 | +12.80 | +2.40 | — |
| 2026-09-21 | `SDGR` | 42 | $29.02 | $29.43 | +17.22 | — | +0.00 | +17.22 | +4.62 | — |
| 2026-09-21 | `FLNC` | 164 | $7.32 | $7.36 | +6.56 | — | +0.00 | +6.56 | -28.70 | — |
| 2026-09-21 | `RARE` | 83 | $14.51 | $14.58 | +5.81 | — | +0.00 | +5.81 | -17.43 | — |
| 2026-09-21 | `SECZ` | 133 | $10.86 | $11.67 | +107.73 | — | +0.00 | +107.73 | +312.55 | — |
| 2026-09-21 | `USDE` | 130 | $10.19 | $13.05 | +371.80 | — | +0.00 | +371.80 | +456.30 | — |
| 2026-09-22 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-23 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -75.93 | ARX, CLBT, AIRO, SECZ, TBBB, REZI, STUB, QMCO | — | $71.31 | $9,905.62 | ARX×63, CLBT×115, AIRO×112, SECZ×214, TBBB×25, REZI×60, STUB×163, QMCO×50 |
| 2026-08-17 | +2.25 | $71.31 | ARX×63, CLBT×115, AIRO×112, SECZ×214, TBBB×25, REZI×60, STUB×163, QMCO×50 | $9,794.59 | -111.03 | -167.10 | CAPR, HTFL, NMAX, RDDT, VERA, TSSI, UMAC, NU | ARX, CLBT, AIRO, SECZ, TBBB, REZI, STUB, QMCO | $201.66 | $9,591.07 | CAPR×177, HTFL×29, NMAX×111, RDDT×6, VERA×39, TSSI×127, UMAC×37, NU×79 |
| 2026-08-18 | -6.20 | $201.66 | CAPR×177, HTFL×29, NMAX×111, RDDT×6, VERA×39, TSSI×127, UMAC×37, NU×79 | $9,471.40 | -119.67 | -74.34 | — | HTFL, NMAX, RDDT, VERA, TSSI, UMAC, NU | $8,128.53 | $9,381.69 | CAPR×177 |
| 2026-08-19 | -7.20 | $8,128.53 | CAPR×177 | $9,401.16 | +19.47 | +0.00 | — | CAPR | $9,398.60 | $9,398.60 | — |
| 2026-08-20 | +1.12 | $9,398.60 | — | $9,398.60 | -0.00 | +144.31 | MRNA, BNTX, WYFI, MRVI, LZB, EL, TEM, WPM | — | $273.52 | $9,526.08 | MRNA×7, BNTX×10, WYFI×54, MRVI×157, LZB×34, EL×12, TEM×19, WPM×8 |
| 2026-08-21 | +3.25 | $273.52 | MRNA×7, BNTX×10, WYFI×54, MRVI×157, LZB×34, EL×12, TEM×19, WPM×8 | $9,566.03 | +39.95 | +476.47 | AAP, ARCT, WMT, AMRC, GMAB | BNTX, WYFI, LZB, EL, WPM | $131.61 | $10,021.44 | MRNA×7, MRVI×157, TEM×19, AAP×28, ARCT×109, WMT×11, AMRC×53, GMAB×36 |
| 2026-08-24 | -5.17 | $131.61 | MRNA×7, MRVI×157, TEM×19, AAP×28, ARCT×109, WMT×11, AMRC×53, GMAB×36 | $9,919.06 | -102.38 | +0.00 | — | MRNA, MRVI, TEM, AAP, ARCT, WMT, AMRC, GMAB | $9,901.70 | $9,901.70 | — |
| 2026-08-25 | +1.80 | $9,901.70 | — | $9,901.70 | -0.00 | +438.34 | ABUS, ZURA, CAPR, FWDI, ALVO, ASST, LIFE, QFIN | — | $20.87 | $10,319.51 | ABUS×235, ZURA×194, CAPR×170, FWDI×216, ALVO×236, ASST×65, LIFE×33, QFIN×111 |
| 2026-08-26 | +2.02 | $20.87 | ABUS×235, ZURA×194, CAPR×170, FWDI×216, ALVO×236, ASST×65, LIFE×33, QFIN×111 | $9,995.92 | -323.59 | +398.10 | DKS, BZ, MAIR, DY, KURA, SMTC | ABUS, ZURA, FWDI, ALVO, ASST, LIFE | $374.29 | $10,365.44 | CAPR×170, QFIN×111, DKS×10, BZ×74, MAIR×45, DY×3, KURA×91, SMTC×9 |
| 2026-08-27 | — | $374.29 | CAPR×170, QFIN×111, DKS×10, BZ×74, MAIR×45, DY×3, KURA×91, SMTC×9 | $10,403.19 | +37.75 | +0.00 | — | CAPR, QFIN, DKS, BZ, MAIR, DY, KURA, SMTC | $10,385.54 | $10,385.54 | — |
| 2026-08-28 | +0.75 | $10,385.54 | — | $10,385.54 | -0.00 | -134.34 | ANF, BZ, QFIN, BHVN, DY, GENB, URBN, JKS | — | $248.37 | $10,233.78 | ANF×8, BZ×71, QFIN×141, BHVN×81, DY×4, GENB×82, URBN×16, JKS×97 |
| 2026-08-31 | -5.85 | $248.37 | ANF×8, BZ×71, QFIN×141, BHVN×81, DY×4, GENB×82, URBN×16, JKS×97 | $10,212.87 | -20.91 | +0.00 | — | ANF, BZ, QFIN, BHVN, DY, GENB, URBN, JKS | $10,195.26 | $10,195.26 | — |
| 2026-09-01 | -6.30 | $10,195.26 | — | $10,195.26 | -0.00 | +0.00 | — | — | $10,195.26 | $10,195.26 | — |
| 2026-09-02 | -3.83 | $10,195.26 | — | $10,195.26 | -0.00 | +0.00 | — | — | $10,195.26 | $10,195.26 | — |
| 2026-09-03 | -0.90 | $10,195.26 | — | $10,195.26 | -0.00 | +160.94 | ALMS, FRVO, DELL, CNH, EIX, MMED, RSKD, AGCO | — | $511.38 | $10,338.60 | ALMS×122, FRVO×69, DELL×2, CNH×92, EIX×22, MMED×53, RSKD×190, AGCO×9 |
| 2026-09-04 | +2.25 | $511.38 | ALMS×122, FRVO×69, DELL×2, CNH×92, EIX×22, MMED×53, RSKD×190, AGCO×9 | $10,295.99 | -42.61 | -76.13 | LULU, ASST, PL, ZS, IOT, MRX | ALMS, FRVO, CNH, EIX, MMED, AGCO | $290.76 | $10,194.18 | DELL×2, RSKD×190, LULU×13, ASST×52, PL×67, ZS×7, IOT×29, MRX×17 |
| 2026-09-08 | -11.47 | $290.76 | DELL×2, RSKD×190, LULU×13, ASST×52, PL×67, ZS×7, IOT×29, MRX×17 | $10,085.66 | -108.52 | +0.00 | — | DELL, RSKD, LULU, ASST, PL, ZS, IOT, MRX | $10,068.42 | $10,068.42 | — |
| 2026-09-09 | -13.95 | $10,068.42 | — | $10,068.42 | +0.00 | +0.00 | — | — | $10,068.42 | $10,068.42 | — |
| 2026-09-10 | -13.28 | $10,068.42 | — | $10,068.42 | +0.00 | +0.00 | — | — | $10,068.42 | $10,068.42 | — |
| 2026-09-11 | +0.50 | $10,068.42 | — | $10,068.42 | +0.00 | -156.43 | COO, AEO, WLTH, NAVN, TSSI, IRD, AXGN, TYRA | — | $39.16 | $9,893.92 | COO×23, AEO×85, WLTH×114, NAVN×61, TSSI×140, IRD×204, AXGN×29, TYRA×53 |
| 2026-09-14 | -11.00 | $39.16 | COO×23, AEO×85, WLTH×114, NAVN×61, TSSI×140, IRD×204, AXGN×29, TYRA×53 | $9,883.94 | -9.98 | +0.00 | — | COO, AEO, WLTH, NAVN, TSSI, IRD, AXGN, TYRA | $9,865.65 | $9,865.65 | — |
| 2026-09-15 | -3.84 | $9,865.65 | — | $9,865.65 | -0.00 | +0.00 | — | — | $9,865.65 | $9,865.65 | — |
| 2026-09-16 | +5.30 | $9,865.65 | — | $9,865.65 | -0.00 | -68.93 | PLAY, ALHC, FPS, GFR, HQ, SDGR, DMRA, RVTY | — | $156.56 | $9,778.64 | PLAY×179, ALHC×119, FPS×37, GFR×180, HQ×95, SDGR×52, DMRA×49, RVTY×8 |
| 2026-09-17 | +7.38 | $156.56 | PLAY×179, ALHC×119, FPS×37, GFR×180, HQ×95, SDGR×52, DMRA×49, RVTY×8 | $9,894.74 | +116.10 | +77.63 | BBNX, JBHT, ALMU, ARQT, AMRX, BTGO | PLAY, GFR, HQ, SDGR, DMRA, RVTY | $78.31 | $9,945.22 | ALHC×119, FPS×37, BBNX×55, JBHT×5, ALMU×111, ARQT×48, AMRX×67, BTGO×190 |
| 2026-09-18 | +4.86 | $78.31 | ALHC×119, FPS×37, BBNX×55, JBHT×5, ALMU×111, ARQT×48, AMRX×67, BTGO×190 | $10,007.63 | +62.41 | +291.77 | GNRC, SDGR, FLNC, RARE, SECZ, USDE | ALHC, FPS, BBNX, JBHT, AMRX, BTGO | $211.64 | $10,272.28 | ALMU×111, ARQT×48, GNRC×5, SDGR×42, FLNC×164, RARE×83, SECZ×133, USDE×130 |
| 2026-09-21 | +12.87 | $211.64 | ALMU×111, ARQT×48, GNRC×5, SDGR×42, FLNC×164, RARE×83, SECZ×133, USDE×130 | $10,847.72 | +575.44 | +0.00 | — | ALMU, ARQT, GNRC, SDGR, FLNC, RARE, SECZ, USDE | $10,829.43 | $10,829.43 | — |
| 2026-09-22 | -0.50 | $10,829.43 | — | $10,829.43 | +0.00 | +0.00 | — | — | $10,829.43 | $10,829.43 | — |
| 2026-09-23 | +2.29 | $10,829.43 | — | $10,829.43 | +0.00 | +0.00 | — | — | $10,829.43 | $10,829.43 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $8,764.91 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 115 | $10.83 | $2.33 | — | $7,517.13 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $6,269.36 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SECZ` | 214 | $5.84 | $2.76 | — | $5,016.84 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ⚪; ret5=-20.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 25 | $48.82 | $2.06 | — | $3,794.27 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `REZI` | 60 | $20.56 | $2.17 | — | $2,558.50 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ⚪; ret5=-21.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `STUB` | 163 | $7.66 | $2.48 | — | $1,307.45 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ⚪; ret5=-13.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 50 | $24.68 | $2.14 | — | $71.31 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.31 | ▼ close $9,905.62 vs 09:30 $10,000.00 (session -75.93) | 16:00 close · cash $71.31 · equity $9,905.62 vs 09:30 $10,000.00 (-94.38; session marks -75.93) · 8 name(s) marked open→close (per-name table). ARX×63 09:30 $19.57 → close $19.58 +0.63; CLBT×115 09:30 $10.83 → close $11.14 +35.65; AIRO×112 09:30 $11.12 → close $9.57 -173.60; SECZ×214 09:30 $5.84 → close $5.61 -49.22; TBBB×25 09:30 $48.82 → close $47.79 -25.75; REZI×60 09:30 $20.56 → close $20.50 -3.60; STUB×163 09:30 $7.66 → close $8.08 +68.46; QMCO×50 09:30 $24.68 → close $26.11 +71.50 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.31 | ▼ 09:30 equity $9,794.59 vs yday $9,905.62 (-111.03) | 09:30 open · cash $71.31 (unchanged overnight, no fees) · equity $9,794.59 vs prior close $9,905.62 (-111.03) · 8 name(s) re-marked at the open (per-name table). ARX×63 yday $19.58 → 09:30 $19.57 -0.63; CLBT×115 yday $11.14 → 09:30 $11.19 +5.75; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; SECZ×214 yday $5.61 → 09:30 $5.45 -34.24; TBBB×25 yday $47.79 → 09:30 $47.39 -10.00; REZI×60 yday $20.50 → 09:30 $20.83 +19.80; STUB×163 yday $8.08 → 09:30 $7.91 -27.71; QMCO×50 yday $26.11 → 09:30 $24.83 -64.00 | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $1,302.02 | ▼ -4.38 after sell → book $9,792.39; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 115 | $11.19 | $2.36 | $+36.70 | $2,586.50 | ▲ +36.70 after sell → book $9,790.02; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $3,655.99 | ▼ -178.28 after sell → book $9,787.67; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SECZ` | 214 | $5.45 | $2.81 | $-89.03 | $4,819.48 | ▼ -89.03 after sell → book $9,784.86; vs 09:30 mark -2.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 25 | $47.39 | $2.08 | $-39.90 | $6,002.15 | ▼ -39.90 after sell → book $9,782.78; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `REZI` | 60 | $20.83 | $2.19 | $+11.84 | $7,249.76 | ▲ +11.84 after sell → book $9,780.59; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `STUB` | 163 | $7.91 | $2.52 | $+35.75 | $8,536.57 | ▲ +35.75 after sell → book $9,778.07; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 50 | $24.83 | $2.16 | $+3.20 | $9,775.91 | ▲ +3.20 after sell → book $9,775.91; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 177 | $6.87 | $2.52 | — | $8,557.40 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+62.6; leftover $1221.99 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $7,359.65 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+46.0; leftover $1221.99 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 111 | $10.97 | $2.32 | — | $6,139.66 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $1221.99 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `RDDT` | 6 | $177.51 | $2.01 | — | $5,072.59 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ⚪; ret5=+10.1; leftover $1221.99 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 39 | $31.30 | $2.11 | — | $3,849.78 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-3.8; leftover $1221.99 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TSSI` | 127 | $9.61 | $2.37 | — | $2,626.94 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ⚪; ret5=-14.0; leftover $1221.99 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $1,420.49 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1221.99 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NU` | 79 | $15.40 | $2.23 | — | $201.66 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $1221.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $201.66 | ▼ close $9,591.07 vs 09:30 $9,794.59 (session -167.10) | 16:00 close · cash $201.66 · equity $9,591.07 vs 09:30 $9,794.59 (-203.52; session marks -167.10) · 8 name(s) marked open→close (per-name table). CAPR×177 09:30 $6.87 → close $7.45 +102.66; HTFL×29 09:30 $41.23 → close $41.94 +20.59; NMAX×111 09:30 $10.97 → close $10.36 -67.71; RDDT×6 09:30 $177.51 → close $164.50 -78.06; VERA×39 09:30 $31.30 → close $31.63 +12.87; TSSI×127 09:30 $9.61 → close $9.48 -16.51; UMAC×37 09:30 $32.55 → close $30.15 -88.80; NU×79 09:30 $15.40 → close $14.74 -52.14 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $201.66 | ▼ 09:30 equity $9,471.40 vs yday $9,591.07 (-119.67) | 09:30 open · cash $201.66 (unchanged overnight, no fees) · equity $9,471.40 vs prior close $9,591.07 (-119.67) · 8 name(s) re-marked at the open (per-name table). CAPR×177 yday $7.45 → 09:30 $7.50 +8.85; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; NMAX×111 yday $10.36 → 09:30 $10.31 -5.55; RDDT×6 yday $164.50 → 09:30 $166.10 +9.60; VERA×39 yday $31.63 → 09:30 $31.31 -12.48; TSSI×127 yday $9.48 → 09:30 $9.22 -33.02; UMAC×37 yday $30.15 → 09:30 $28.59 -57.72; NU×79 yday $14.74 → 09:30 $14.53 -16.59 | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $1,403.07 | ▲ +3.66 after sell → book $9,469.31; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 111 | $10.31 | $2.35 | $-77.93 | $2,545.13 | ▼ -77.93 after sell → book $9,466.96; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `RDDT` | 6 | $166.10 | $2.03 | $-72.50 | $3,539.70 | ▼ -72.50 after sell → book $9,464.93; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 39 | $31.31 | $2.13 | $-3.84 | $4,758.66 | ▼ -3.84 after sell → book $9,462.80; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TSSI` | 127 | $9.22 | $2.40 | $-54.30 | $5,927.20 | ▼ -54.30 after sell → book $9,460.40; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $6,982.91 | ▼ -150.74 after sell → book $9,458.28; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NU` | 79 | $14.53 | $2.25 | $-73.21 | $8,128.53 | ▼ -73.21 after sell → book $9,456.03; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,128.53 | ▼ close $9,381.69 vs 09:30 $9,471.40 (session -74.34) | 16:00 close · cash $8,128.53 · equity $9,381.69 vs 09:30 $9,471.40 (-89.71; session marks -74.34) · 1 name(s) marked open→close (per-name table). CAPR×177 09:30 $7.50 → close $7.08 -74.34 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,128.53 | ▲ 09:30 equity $9,401.16 vs yday $9,381.69 (+19.47) | 09:30 open · cash $8,128.53 (unchanged overnight, no fees) · equity $9,401.16 vs prior close $9,381.69 (+19.47) · 1 name(s) re-marked at the open (per-name table). CAPR×177 yday $7.08 → 09:30 $7.19 +19.47 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 177 | $7.19 | $2.56 | $+51.56 | $9,398.60 | ▲ +51.56 after sell → book $9,398.60; vs 09:30 mark -2.56 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,398.60 | ▲ close $9,398.60 vs 09:30 $9,401.16 (session +0.00) | 16:00 close · cash $9,398.60 · no lots left · equity $9,398.60. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,398.60 | ▲ 09:30 equity $9,398.60 vs yday $9,398.60 (-0.00) | 09:30 open · cash $9,398.60 · no holdings · equity $9,398.60 vs prior close $9,398.60 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $8,345.61 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1174.82 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 10 | $109.06 | $2.02 | — | $7,252.99 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $1174.82 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WYFI` | 54 | $21.40 | $2.15 | — | $6,095.23 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-25.2; leftover $1174.82 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 157 | $7.44 | $2.46 | — | $4,924.69 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1174.82 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `LZB` | 34 | $33.61 | $2.09 | — | $3,779.86 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-17.4; leftover $1174.82 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 12 | $97.43 | $2.03 | — | $2,608.68 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; leftover $1174.82 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TEM` | 19 | $61.83 | $2.05 | — | $1,431.86 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1174.82 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $273.52 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1174.82 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $273.52 | ▲ close $9,526.08 vs 09:30 $9,398.60 (session +144.31) | 16:00 close · cash $273.52 · equity $9,526.08 vs 09:30 $9,398.60 (+127.48; session marks +144.31) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $150.14 → close $133.32 -117.74; BNTX×10 09:30 $109.06 → close $110.89 +18.30; WYFI×54 09:30 $21.40 → close $21.16 -12.96; MRVI×157 09:30 $7.44 → close $8.29 +133.45; LZB×34 09:30 $33.61 → close $33.65 +1.36; EL×12 09:30 $97.43 → close $96.15 -15.36; TEM×19 09:30 $61.83 → close $66.65 +91.58; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $273.52 | ▲ 09:30 equity $9,566.03 vs yday $9,526.08 (+39.95) | 09:30 open · cash $273.52 (unchanged overnight, no fees) · equity $9,566.03 vs prior close $9,526.08 (+39.95) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $133.32 → 09:30 $133.11 -1.47; BNTX×10 yday $110.89 → 09:30 $110.92 +0.30; WYFI×54 yday $21.16 → 09:30 $21.54 +20.52; MRVI×157 yday $8.29 → 09:30 $8.28 -1.57; LZB×34 yday $33.65 → 09:30 $33.63 -0.68; EL×12 yday $96.15 → 09:30 $96.75 +7.20; TEM×19 yday $66.65 → 09:30 $65.60 -19.95; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `BNTX` | 10 | $110.92 | $2.04 | $+14.54 | $1,380.68 | ▲ +14.54 after sell → book $9,563.99; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WYFI` | 54 | $21.54 | $2.17 | $+3.24 | $2,541.67 | ▲ +3.24 after sell → book $9,561.82; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `LZB` | 34 | $33.63 | $2.11 | $-3.52 | $3,682.98 | ▼ -3.52 after sell → book $9,559.71; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 12 | $96.75 | $2.05 | $-12.23 | $4,841.93 | ▼ -12.23 after sell → book $9,557.66; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $6,077.50 | ▲ +77.23 after sell → book $9,555.63; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AAP` | 28 | $42.41 | $2.07 | — | $4,887.95 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-26.1; leftover $1215.50 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 109 | $11.13 | $2.32 | — | $3,672.46 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1215.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 11 | $103.69 | $2.02 | — | $2,529.85 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-10.3; leftover $1215.50 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AMRC` | 53 | $22.51 | $2.15 | — | $1,334.67 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-20.2; leftover $1215.50 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 36 | $33.36 | $2.10 | — | $131.61 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $1215.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.61 | ▲ close $10,021.44 vs 09:30 $9,566.03 (session +476.47) | 16:00 close · cash $131.61 · equity $10,021.44 vs 09:30 $9,566.03 (+455.41; session marks +476.47) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $133.11 → close $145.13 +84.14; MRVI×157 09:30 $8.28 → close $8.64 +56.52; TEM×19 09:30 $65.60 → close $72.69 +134.71; AAP×28 09:30 $42.41 → close $42.58 +4.76; ARCT×109 09:30 $11.13 → close $13.45 +252.88; WMT×11 09:30 $103.69 → close $103.70 +0.11; AMRC×53 09:30 $22.51 → close $21.38 -59.89; GMAB×36 09:30 $33.36 → close $33.45 +3.24 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.61 | ▼ 09:30 equity $9,919.06 vs yday $10,021.44 (-102.38) | 09:30 open · cash $131.61 (unchanged overnight, no fees) · equity $9,919.06 vs prior close $10,021.44 (-102.38) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $145.13 → 09:30 $142.70 -17.01; MRVI×157 yday $8.64 → 09:30 $8.59 -7.85; TEM×19 yday $72.69 → 09:30 $70.08 -49.68; AAP×28 yday $42.58 → 09:30 $43.05 +13.16; ARCT×109 yday $13.45 → 09:30 $13.33 -13.08; WMT×11 yday $103.70 → 09:30 $104.14 +4.84; AMRC×53 yday $21.38 → 09:30 $21.19 -10.07; GMAB×36 yday $33.45 → 09:30 $32.82 -22.68 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $1,128.48 | ▼ -56.12 after sell → book $9,917.03; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 157 | $8.59 | $2.50 | $+175.59 | $2,474.61 | ▲ +175.59 after sell → book $9,914.54; vs 09:30 mark -2.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TEM` | 19 | $70.08 | $2.07 | $+152.54 | $3,803.97 | ▲ +152.54 after sell → book $9,912.47; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 28 | $43.05 | $2.09 | $+13.75 | $5,007.27 | ▲ +13.75 after sell → book $9,910.37; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 109 | $13.33 | $2.35 | $+235.14 | $6,457.90 | ▲ +235.14 after sell → book $9,908.03; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 11 | $104.14 | $2.04 | $+0.88 | $7,601.39 | ▲ +0.88 after sell → book $9,905.98; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AMRC` | 53 | $21.19 | $2.17 | $-74.28 | $8,722.29 | ▼ -74.28 after sell → book $9,903.81; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 36 | $32.82 | $2.12 | $-23.66 | $9,901.70 | ▼ -23.66 after sell → book $9,901.70; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,901.70 | ▲ close $9,901.70 vs 09:30 $9,919.06 (session +0.00) | 16:00 close · cash $9,901.70 · no lots left · equity $9,901.70. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,901.70 | ▲ 09:30 equity $9,901.70 vs yday $9,901.70 (-0.00) | 09:30 open · cash $9,901.70 · no holdings · equity $9,901.70 vs prior close $9,901.70 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `ABUS` | 235 | $5.25 | $3.03 | — | $8,664.92 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list mover_buy; 🔵; ⚪; ret5=+10.4; leftover $1237.71 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 194 | $6.37 | $2.57 | — | $7,426.56 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1237.71 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 170 | $7.25 | $2.50 | — | $6,191.56 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1237.71 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 216 | $5.71 | $2.79 | — | $4,955.42 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $1237.71 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 236 | $5.24 | $3.04 | — | $3,715.73 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1237.71 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 65 | $19.04 | $2.19 | — | $2,475.95 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+49.5; leftover $1237.71 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 33 | $36.96 | $2.09 | — | $1,254.18 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1237.71 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `QFIN` | 111 | $11.09 | $2.32 | — | $20.87 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list overnight; 🔵; ret5=-8.0; leftover $1237.71 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.87 | ▲ close $10,319.51 vs 09:30 $9,901.70 (session +438.34) | 16:00 close · cash $20.87 · equity $10,319.51 vs 09:30 $9,901.70 (+417.81; session marks +438.34) · 8 name(s) marked open→close (per-name table). ABUS×235 09:30 $5.25 → close $5.20 -11.75; ZURA×194 09:30 $6.37 → close $6.32 -9.70; CAPR×170 09:30 $7.25 → close $8.29 +176.80; FWDI×216 09:30 $5.71 → close $6.05 +73.44; ALVO×236 09:30 $5.24 → close $5.05 -44.84; ASST×65 09:30 $19.04 → close $21.39 +152.75; LIFE×33 09:30 $36.96 → close $38.56 +52.80; QFIN×111 09:30 $11.09 → close $11.53 +48.84 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.87 | ▼ 09:30 equity $9,995.92 vs yday $10,319.51 (-323.59) | 09:30 open · cash $20.87 (unchanged overnight, no fees) · equity $9,995.92 vs prior close $10,319.51 (-323.59) · 8 name(s) re-marked at the open (per-name table). ABUS×235 yday $5.20 → 09:30 $5.19 -2.35; ZURA×194 yday $6.32 → 09:30 $6.13 -36.86; CAPR×170 yday $8.29 → 09:30 $8.29 +0.00; FWDI×216 yday $6.05 → 09:30 $5.97 -17.28; ALVO×236 yday $5.05 → 09:30 $4.98 -16.52; ASST×65 yday $21.39 → 09:30 $20.72 -43.55; LIFE×33 yday $38.56 → 09:30 $38.24 -10.56; QFIN×111 yday $11.53 → 09:30 $9.76 -196.47 | — |
| 2026-08-26 09:30 ET | **SELL** | `ABUS` | 235 | $5.19 | $3.08 | $-20.21 | $1,237.44 | ▼ -20.21 after sell → book $9,992.84; vs 09:30 mark -3.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 194 | $6.13 | $2.61 | $-51.75 | $2,424.04 | ▼ -51.75 after sell → book $9,990.22; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FWDI` | 216 | $5.97 | $2.83 | $+50.54 | $3,710.73 | ▲ +50.54 after sell → book $9,987.39; vs 09:30 mark -2.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 236 | $4.98 | $3.09 | $-67.50 | $4,882.91 | ▼ -67.50 after sell → book $9,984.29; vs 09:30 mark -3.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 65 | $20.72 | $2.21 | $+104.81 | $6,227.51 | ▲ +104.81 after sell → book $9,982.09; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 33 | $38.24 | $2.11 | $+38.04 | $7,487.32 | ▲ +38.04 after sell → book $9,979.98; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `DKS` | 10 | $121.87 | $2.02 | — | $6,266.60 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-35.1; leftover $1247.89 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BZ` | 74 | $16.77 | $2.21 | — | $5,023.41 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1247.89 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `MAIR` | 45 | $27.59 | $2.12 | — | $3,779.73 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; ret5=+2.0; leftover $1247.89 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 3 | $326.91 | $2.00 | — | $2,797.00 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=-15.2; leftover $1247.89 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `KURA` | 91 | $13.63 | $2.26 | — | $1,554.41 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+10.2; leftover $1247.89 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SMTC` | 9 | $130.90 | $2.02 | — | $374.29 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-5.7; leftover $1247.89 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $374.29 | ▲ close $10,365.44 vs 09:30 $9,995.92 (session +398.10) | 16:00 close · cash $374.29 · equity $10,365.44 vs 09:30 $9,995.92 (+369.52; session marks +398.10) · 8 name(s) marked open→close (per-name table). CAPR×170 09:30 $8.29 → close $9.36 +181.90; QFIN×111 09:30 $9.76 → close $9.35 -45.51; DKS×10 09:30 $121.87 → close $129.66 +77.90; BZ×74 09:30 $16.77 → close $18.84 +153.18; MAIR×45 09:30 $27.59 → close $28.51 +41.40; DY×3 09:30 $326.91 → close $310.91 -48.00; KURA×91 09:30 $13.63 → close $13.06 -51.87; SMTC×9 09:30 $130.90 → close $140.80 +89.10 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $374.29 | ▲ 09:30 equity $10,403.19 vs yday $10,365.44 (+37.75) | 09:30 open · cash $374.29 (unchanged overnight, no fees) · equity $10,403.19 vs prior close $10,365.44 (+37.75) · 8 name(s) re-marked at the open (per-name table). CAPR×170 yday $9.36 → 09:30 $9.19 -28.90; QFIN×111 yday $9.35 → 09:30 $9.42 +7.77; DKS×10 yday $129.66 → 09:30 $128.73 -9.30; BZ×74 yday $18.84 → 09:30 $18.50 -25.16; MAIR×45 yday $28.51 → 09:30 $28.76 +11.25; DY×3 yday $310.91 → 09:30 $314.90 +11.97; KURA×91 yday $13.06 → 09:30 $12.98 -7.28; SMTC×9 yday $140.80 → 09:30 $149.40 +77.40 | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 170 | $9.19 | $2.54 | $+324.76 | $1,934.05 | ▲ +324.76 after sell → book $10,400.65; vs 09:30 mark -2.54 | dropped from list after 2 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `QFIN` | 111 | $9.42 | $2.35 | $-190.04 | $2,977.32 | ▼ -190.04 after sell → book $10,398.30; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 10 | $128.73 | $2.04 | $+64.54 | $4,262.58 | ▲ +64.54 after sell → book $10,396.26; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 74 | $18.50 | $2.24 | $+123.57 | $5,629.35 | ▲ +123.57 after sell → book $10,394.03; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **SELL** | `MAIR` | 45 | $28.76 | $2.15 | $+48.38 | $6,921.40 | ▲ +48.38 after sell → book $10,391.88; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `DY` | 3 | $314.90 | $2.02 | $-40.05 | $7,864.08 | ▼ -40.05 after sell → book $10,389.86; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `KURA` | 91 | $12.98 | $2.29 | $-63.70 | $9,042.97 | ▼ -63.70 after sell → book $10,387.57; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `SMTC` | 9 | $149.40 | $2.04 | $+162.45 | $10,385.54 | ▲ +162.45 after sell → book $10,385.54; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,385.54 | ▲ close $10,385.54 vs 09:30 $10,403.19 (session +0.00) | 16:00 close · cash $10,385.54 · no lots left · equity $10,385.54. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,385.54 | ▲ 09:30 equity $10,385.54 vs yday $10,385.54 (-0.00) | 09:30 open · cash $10,385.54 · no holdings · equity $10,385.54 vs prior close $10,385.54 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $9,214.96 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1298.19 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 71 | $18.15 | $2.20 | — | $7,924.11 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+14.1; leftover $1298.19 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `QFIN` | 141 | $9.15 | $2.41 | — | $6,631.55 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-19.9; leftover $1298.19 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 81 | $15.88 | $2.23 | — | $5,343.03 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+19.4; leftover $1298.19 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 4 | $306.34 | $2.00 | — | $4,115.67 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-23.0; leftover $1298.19 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GENB` | 82 | $15.77 | $2.24 | — | $2,820.29 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-1.4; leftover $1298.19 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $79.42 | $2.04 | — | $1,547.54 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1298.19 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `JKS` | 97 | $13.37 | $2.28 | — | $248.37 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-14.9; leftover $1298.19 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $248.37 | ▼ close $10,233.78 vs 09:30 $10,385.54 (session -134.34) | 16:00 close · cash $248.37 · equity $10,233.78 vs 09:30 $10,385.54 (-151.76; session marks -134.34) · 8 name(s) marked open→close (per-name table). ANF×8 09:30 $146.07 → close $148.42 +18.80; BZ×71 09:30 $18.15 → close $17.80 -24.85; QFIN×141 09:30 $9.15 → close $8.80 -49.35; BHVN×81 09:30 $15.88 → close $15.41 -38.07; DY×4 09:30 $306.34 → close $294.34 -48.00; GENB×82 09:30 $15.77 → close $15.33 -36.08; URBN×16 09:30 $79.42 → close $81.09 +26.72; JKS×97 09:30 $13.37 → close $13.54 +16.49 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $248.37 | ▼ 09:30 equity $10,212.87 vs yday $10,233.78 (-20.91) | 09:30 open · cash $248.37 (unchanged overnight, no fees) · equity $10,212.87 vs prior close $10,233.78 (-20.91) · 8 name(s) re-marked at the open (per-name table). ANF×8 yday $148.42 → 09:30 $148.03 -3.12; BZ×71 yday $17.80 → 09:30 $17.70 -7.10; QFIN×141 yday $8.80 → 09:30 $8.70 -14.10; BHVN×81 yday $15.41 → 09:30 $15.46 +4.05; DY×4 yday $294.34 → 09:30 $298.01 +14.68; GENB×82 yday $15.33 → 09:30 $15.27 -4.92; URBN×16 yday $81.09 → 09:30 $80.44 -10.40; JKS×97 yday $13.54 → 09:30 $13.54 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.03 | $2.03 | $+11.63 | $1,430.57 | ▲ +11.63 after sell → book $10,210.83; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 71 | $17.70 | $2.22 | $-36.38 | $2,685.05 | ▼ -36.38 after sell → book $10,208.61; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `QFIN` | 141 | $8.70 | $2.45 | $-68.31 | $3,909.30 | ▼ -68.31 after sell → book $10,206.16; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 81 | $15.46 | $2.26 | $-38.51 | $5,159.30 | ▼ -38.51 after sell → book $10,203.90; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 4 | $298.01 | $2.02 | $-37.34 | $6,349.32 | ▼ -37.34 after sell → book $10,201.88; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GENB` | 82 | $15.27 | $2.26 | $-45.50 | $7,599.20 | ▼ -45.50 after sell → book $10,199.62; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $80.44 | $2.06 | $+12.22 | $8,884.18 | ▲ +12.22 after sell → book $10,197.56; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `JKS` | 97 | $13.54 | $2.31 | $+11.90 | $10,195.26 | ▲ +11.90 after sell → book $10,195.26; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,195.26 | ▲ close $10,195.26 vs 09:30 $10,212.87 (session +0.00) | 16:00 close · cash $10,195.26 · no lots left · equity $10,195.26. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,195.26 | ▲ 09:30 equity $10,195.26 vs yday $10,195.26 (-0.00) | 09:30 open · cash $10,195.26 · no holdings · equity $10,195.26 vs prior close $10,195.26 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,195.26 | ▲ close $10,195.26 vs 09:30 $10,195.26 (session +0.00) | 16:00 close · cash $10,195.26 · no lots left · equity $10,195.26. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,195.26 | ▲ 09:30 equity $10,195.26 vs yday $10,195.26 (-0.00) | 09:30 open · cash $10,195.26 · no holdings · equity $10,195.26 vs prior close $10,195.26 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,195.26 | ▲ close $10,195.26 vs 09:30 $10,195.26 (session +0.00) | 16:00 close · cash $10,195.26 · no lots left · equity $10,195.26. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,195.26 | ▲ 09:30 equity $10,195.26 vs yday $10,195.26 (-0.00) | 09:30 open · cash $10,195.26 · no holdings · equity $10,195.26 vs prior close $10,195.26 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 122 | $10.38 | $2.36 | — | $8,927.15 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-56.2; leftover $1274.41 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 69 | $18.28 | $2.20 | — | $7,663.63 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+16.5; leftover $1274.41 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $6,689.02 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list mover_buy; 🔵; ret5=+6.1; leftover $1274.41 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 92 | $13.71 | $2.27 | — | $5,425.43 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+17.5; leftover $1274.41 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $55.42 | $2.06 | — | $4,204.13 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-25.9; leftover $1274.41 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 53 | $23.88 | $2.15 | — | $2,936.35 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1274.41 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RSKD` | 190 | $6.68 | $2.56 | — | $1,664.59 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+11.4; leftover $1274.41 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 9 | $127.91 | $2.02 | — | $511.38 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1274.41 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $511.38 | ▲ close $10,338.60 vs 09:30 $10,195.26 (session +160.94) | 16:00 close · cash $511.38 · equity $10,338.60 vs 09:30 $10,195.26 (+143.34; session marks +160.94) · 8 name(s) marked open→close (per-name table). ALMS×122 09:30 $10.38 → close $11.36 +120.17; FRVO×69 09:30 $18.28 → close $17.16 -77.28; DELL×2 09:30 $486.31 → close $516.39 +60.16; CNH×92 09:30 $13.71 → close $13.84 +11.96; EIX×22 09:30 $55.42 → close $56.30 +19.36; MMED×53 09:30 $23.88 → close $23.84 -2.12; RSKD×190 09:30 $6.68 → close $6.93 +47.50; AGCO×9 09:30 $127.91 → close $125.82 -18.81 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $511.38 | ▼ 09:30 equity $10,295.99 vs yday $10,338.60 (-42.61) | 09:30 open · cash $511.38 (unchanged overnight, no fees) · equity $10,295.99 vs prior close $10,338.60 (-42.61) · 8 name(s) re-marked at the open (per-name table). ALMS×122 yday $11.36 → 09:30 $11.23 -15.86; FRVO×69 yday $17.16 → 09:30 $17.27 +7.59; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CNH×92 yday $13.84 → 09:30 $13.89 +4.60; EIX×22 yday $56.30 → 09:30 $55.79 -11.22; MMED×53 yday $23.84 → 09:30 $23.84 +0.00; RSKD×190 yday $6.93 → 09:30 $6.84 -17.10; AGCO×9 yday $125.82 → 09:30 $125.22 -5.40 | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 122 | $11.23 | $2.39 | $+99.57 | $1,879.05 | ▲ +99.57 after sell → book $10,293.60; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 69 | $17.27 | $2.22 | $-74.11 | $3,068.46 | ▼ -74.11 after sell → book $10,291.38; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 92 | $13.89 | $2.29 | $+12.00 | $4,344.05 | ▲ +12.00 after sell → book $10,289.09; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 22 | $55.79 | $2.08 | $+4.01 | $5,569.36 | ▲ +4.01 after sell → book $10,287.02; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 53 | $23.84 | $2.17 | $-6.44 | $6,830.71 | ▼ -6.44 after sell → book $10,284.85; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AGCO` | 9 | $125.22 | $2.04 | $-28.26 | $7,955.65 | ▼ -28.26 after sell → book $10,282.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 13 | $98.15 | $2.03 | — | $6,677.67 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=+5.9; leftover $1325.94 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 52 | $25.18 | $2.15 | — | $5,366.16 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+16.0; leftover $1325.94 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `PL` | 67 | $19.64 | $2.19 | — | $4,048.09 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=-13.3; leftover $1325.94 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ZS` | 7 | $166.15 | $2.01 | — | $2,883.03 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=-5.1; leftover $1325.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 29 | $44.90 | $2.08 | — | $1,578.86 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=-7.5; leftover $1325.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 17 | $75.65 | $2.04 | — | $290.76 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1325.94 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $290.76 | ▼ close $10,194.18 vs 09:30 $10,295.99 (session -76.13) | 16:00 close · cash $290.76 · equity $10,194.18 vs 09:30 $10,295.99 (-101.81; session marks -76.13) · 8 name(s) marked open→close (per-name table). DELL×2 09:30 $513.78 → close $524.14 +20.72; RSKD×190 09:30 $6.84 → close $6.51 -62.70; LULU×13 09:30 $98.15 → close $100.61 +31.98; ASST×52 09:30 $25.18 → close $27.14 +101.92; PL×67 09:30 $19.64 → close $18.12 -101.84; ZS×7 09:30 $166.15 → close $169.80 +25.55; IOT×29 09:30 $44.90 → close $40.20 -136.30; MRX×17 09:30 $75.65 → close $78.27 +44.54 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $290.76 | ▼ 09:30 equity $10,085.66 vs yday $10,194.18 (-108.52) | 09:30 open · cash $290.76 (unchanged overnight, no fees) · equity $10,085.66 vs prior close $10,194.18 (-108.52) · 8 name(s) re-marked at the open (per-name table). DELL×2 yday $524.14 → 09:30 $521.15 -5.98; RSKD×190 yday $6.51 → 09:30 $6.46 -9.50; LULU×13 yday $100.61 → 09:30 $100.58 -0.39; ASST×52 yday $27.14 → 09:30 $26.44 -36.40; PL×67 yday $18.12 → 09:30 $17.85 -18.09; ZS×7 yday $169.80 → 09:30 $165.62 -29.30; IOT×29 yday $40.20 → 09:30 $39.56 -18.56; MRX×17 yday $78.27 → 09:30 $78.84 +9.69 | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+65.67 | $1,331.05 | ▲ +65.67 after sell → book $10,083.64; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `RSKD` | 190 | $6.46 | $2.60 | $-46.96 | $2,555.85 | ▼ -46.96 after sell → book $10,081.04; vs 09:30 mark -2.60 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 13 | $100.58 | $2.05 | $+27.51 | $3,861.34 | ▲ +27.51 after sell → book $10,078.99; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 52 | $26.44 | $2.17 | $+61.21 | $5,234.05 | ▲ +61.21 after sell → book $10,076.83; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `PL` | 67 | $17.85 | $2.21 | $-124.33 | $6,427.79 | ▼ -124.33 after sell → book $10,074.61; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ZS` | 7 | $165.62 | $2.03 | $-7.79 | $7,585.06 | ▼ -7.79 after sell → book $10,072.58; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 29 | $39.56 | $2.10 | $-159.03 | $8,730.21 | ▼ -159.03 after sell → book $10,070.49; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 17 | $78.84 | $2.06 | $+50.13 | $10,068.42 | ▲ +50.13 after sell → book $10,068.42; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,068.42 | ▲ close $10,068.42 vs 09:30 $10,085.66 (session +0.00) | 16:00 close · cash $10,068.42 · no lots left · equity $10,068.42. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,068.42 | ▲ 09:30 equity $10,068.42 vs yday $10,068.42 (+0.00) | 09:30 open · cash $10,068.42 · no holdings · equity $10,068.42 vs prior close $10,068.42 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,068.42 | ▲ close $10,068.42 vs 09:30 $10,068.42 (session +0.00) | 16:00 close · cash $10,068.42 · no lots left · equity $10,068.42. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,068.42 | ▲ 09:30 equity $10,068.42 vs yday $10,068.42 (+0.00) | 09:30 open · cash $10,068.42 · no holdings · equity $10,068.42 vs prior close $10,068.42 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,068.42 | ▲ close $10,068.42 vs 09:30 $10,068.42 (session +0.00) | 16:00 close · cash $10,068.42 · no lots left · equity $10,068.42. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,068.42 | ▲ 09:30 equity $10,068.42 vs yday $10,068.42 (+0.00) | 09:30 open · cash $10,068.42 · no holdings · equity $10,068.42 vs prior close $10,068.42 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 23 | $54.66 | $2.06 | — | $8,809.18 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-22.3; leftover $1258.55 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AEO` | 85 | $14.71 | $2.25 | — | $7,556.59 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-12.8; leftover $1258.55 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 114 | $10.95 | $2.33 | — | $6,305.96 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1258.55 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `NAVN` | 61 | $20.61 | $2.17 | — | $5,046.57 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-24.7; leftover $1258.55 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `TSSI` | 140 | $8.98 | $2.41 | — | $3,786.96 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+14.1; leftover $1258.55 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 204 | $6.16 | $2.63 | — | $2,527.69 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+36.4; leftover $1258.55 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AXGN` | 29 | $42.48 | $2.08 | — | $1,293.70 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-13.4; leftover $1258.55 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 53 | $23.63 | $2.15 | — | $39.16 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-6.3; leftover $1258.55 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.16 | ▼ close $9,893.92 vs 09:30 $10,068.42 (session -156.43) | 16:00 close · cash $39.16 · equity $9,893.92 vs 09:30 $10,068.42 (-174.50; session marks -156.43) · 8 name(s) marked open→close (per-name table). COO×23 09:30 $54.66 → close $53.91 -17.25; AEO×85 09:30 $14.71 → close $15.02 +26.35; WLTH×114 09:30 $10.95 → close $10.38 -64.98; NAVN×61 09:30 $20.61 → close $21.02 +25.01; TSSI×140 09:30 $8.98 → close $8.93 -7.00; IRD×204 09:30 $6.16 → close $6.04 -24.48; AXGN×29 09:30 $42.48 → close $42.16 -9.28; TYRA×53 09:30 $23.63 → close $22.03 -84.80 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.16 | ▼ 09:30 equity $9,883.94 vs yday $9,893.92 (-9.98) | 09:30 open · cash $39.16 (unchanged overnight, no fees) · equity $9,883.94 vs prior close $9,893.92 (-9.98) · 8 name(s) re-marked at the open (per-name table). COO×23 yday $53.91 → 09:30 $54.78 +20.01; AEO×85 yday $15.02 → 09:30 $14.85 -14.45; WLTH×114 yday $10.38 → 09:30 $10.29 -10.26; NAVN×61 yday $21.02 → 09:30 $21.10 +4.88; TSSI×140 yday $8.93 → 09:30 $8.57 -50.40; IRD×204 yday $6.04 → 09:30 $6.02 -4.08; AXGN×29 yday $42.16 → 09:30 $41.55 -17.69; TYRA×53 yday $22.03 → 09:30 $23.20 +62.01 | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 23 | $54.78 | $2.08 | $-1.38 | $1,297.02 | ▼ -1.38 after sell → book $9,881.86; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AEO` | 85 | $14.85 | $2.27 | $+7.39 | $2,557.00 | ▲ +7.39 after sell → book $9,879.59; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 114 | $10.29 | $2.36 | $-79.93 | $3,727.70 | ▼ -79.93 after sell → book $9,877.23; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `NAVN` | 61 | $21.10 | $2.19 | $+25.52 | $5,012.60 | ▲ +25.52 after sell → book $9,875.03; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `TSSI` | 140 | $8.57 | $2.44 | $-62.25 | $6,209.96 | ▼ -62.25 after sell → book $9,872.59; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 204 | $6.02 | $2.68 | $-33.87 | $7,435.37 | ▼ -33.87 after sell → book $9,869.92; vs 09:30 mark -2.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AXGN` | 29 | $41.55 | $2.10 | $-31.14 | $8,638.22 | ▼ -31.14 after sell → book $9,867.82; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 53 | $23.20 | $2.17 | $-27.11 | $9,865.65 | ▼ -27.11 after sell → book $9,865.65; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,865.65 | ▲ close $9,865.65 vs 09:30 $9,883.94 (session +0.00) | 16:00 close · cash $9,865.65 · no lots left · equity $9,865.65. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,865.65 | ▲ 09:30 equity $9,865.65 vs yday $9,865.65 (-0.00) | 09:30 open · cash $9,865.65 · no holdings · equity $9,865.65 vs prior close $9,865.65 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,865.65 | ▲ close $9,865.65 vs 09:30 $9,865.65 (session +0.00) | 16:00 close · cash $9,865.65 · no lots left · equity $9,865.65. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,865.65 | ▲ 09:30 equity $9,865.65 vs yday $9,865.65 (-0.00) | 09:30 open · cash $9,865.65 · no holdings · equity $9,865.65 vs prior close $9,865.65 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `PLAY` | 179 | $6.86 | $2.53 | — | $8,635.18 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-22.4; leftover $1233.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ALHC` | 119 | $10.30 | $2.35 | — | $7,407.14 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-23.0; leftover $1233.21 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 37 | $33.14 | $2.10 | — | $6,178.85 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=-2.9; leftover $1233.21 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `GFR` | 180 | $6.83 | $2.53 | — | $4,946.92 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; ret5=+11.2; leftover $1233.21 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `HQ` | 95 | $12.89 | $2.27 | — | $3,720.10 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=-18.2; leftover $1233.21 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 52 | $23.29 | $2.15 | — | $2,506.87 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+16.1; leftover $1233.21 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `DMRA` | 49 | $24.88 | $2.14 | — | $1,285.62 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-15.2; leftover $1233.21 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RVTY` | 8 | $140.88 | $2.01 | — | $156.56 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,ohlc_hot; 🔵; ret5=+10.3; leftover $1233.21 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $156.56 | ▼ close $9,778.64 vs 09:30 $9,865.65 (session -68.93) | 16:00 close · cash $156.56 · equity $9,778.64 vs 09:30 $9,865.65 (-87.01; session marks -68.93) · 8 name(s) marked open→close (per-name table). PLAY×179 09:30 $6.86 → close $6.86 +0.00; ALHC×119 09:30 $10.30 → close $8.71 -189.21; FPS×37 09:30 $33.14 → close $34.84 +62.90; GFR×180 09:30 $6.83 → close $6.49 -61.20; HQ×95 09:30 $12.89 → close $13.56 +63.65; SDGR×52 09:30 $23.29 → close $23.93 +33.28; DMRA×49 09:30 $24.88 → close $24.53 -17.15; RVTY×8 09:30 $140.88 → close $145.73 +38.80 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $156.56 | ▲ 09:30 equity $9,894.74 vs yday $9,778.64 (+116.10) | 09:30 open · cash $156.56 (unchanged overnight, no fees) · equity $9,894.74 vs prior close $9,778.64 (+116.10) · 8 name(s) re-marked at the open (per-name table). PLAY×179 yday $6.86 → 09:30 $6.96 +17.90; ALHC×119 yday $8.71 → 09:30 $8.58 -15.47; FPS×37 yday $34.84 → 09:30 $36.76 +71.04; GFR×180 yday $6.49 → 09:30 $6.48 -1.80; HQ×95 yday $13.56 → 09:30 $13.56 +0.00; SDGR×52 yday $23.93 → 09:30 $24.09 +8.32; DMRA×49 yday $24.53 → 09:30 $24.96 +21.07; RVTY×8 yday $145.73 → 09:30 $147.61 +15.04 | — |
| 2026-09-17 09:30 ET | **SELL** | `PLAY` | 179 | $6.96 | $2.57 | $+12.81 | $1,399.84 | ▲ +12.81 after sell → book $9,892.18; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `GFR` | 180 | $6.48 | $2.57 | $-68.10 | $2,563.67 | ▼ -68.10 after sell → book $9,889.61; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `HQ` | 95 | $13.56 | $2.30 | $+59.07 | $3,849.57 | ▲ +59.07 after sell → book $9,887.31; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 52 | $24.09 | $2.17 | $+37.29 | $5,100.08 | ▲ +37.29 after sell → book $9,885.14; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `DMRA` | 49 | $24.96 | $2.16 | $-0.37 | $6,320.96 | ▼ -0.37 after sell → book $9,882.98; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RVTY` | 8 | $147.61 | $2.03 | $+49.79 | $7,499.81 | ▲ +49.79 after sell → book $9,880.95; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 55 | $22.46 | $2.15 | — | $6,262.35 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+27.3; leftover $1249.97 | join🟢 sector🟢 gen🟢 news🔴 digest🔴 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 5 | $238.60 | $2.00 | — | $5,067.35 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-11.6; leftover $1249.97 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 111 | $11.21 | $2.32 | — | $3,820.72 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=+1.0; leftover $1249.97 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 48 | $25.95 | $2.13 | — | $2,572.98 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1249.97 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AMRX` | 67 | $18.56 | $2.19 | — | $1,327.27 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+4.8; leftover $1249.97 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `BTGO` | 190 | $6.56 | $2.56 | — | $78.31 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-14.6; leftover $1249.97 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.31 | ▲ close $9,945.22 vs 09:30 $9,894.74 (session +77.63) | 16:00 close · cash $78.31 · equity $9,945.22 vs 09:30 $9,894.74 (+50.48; session marks +77.63) · 8 name(s) marked open→close (per-name table). ALHC×119 09:30 $8.58 → close $8.70 +14.28; FPS×37 09:30 $36.76 → close $38.06 +48.10; BBNX×55 09:30 $22.46 → close $21.43 -56.65; JBHT×5 09:30 $238.60 → close $236.80 -9.00; ALMU×111 09:30 $11.21 → close $11.54 +37.18; ARQT×48 09:30 $25.95 → close $26.46 +24.48; AMRX×67 09:30 $18.56 → close $18.28 -18.76; BTGO×190 09:30 $6.56 → close $6.76 +38.00 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.31 | ▲ 09:30 equity $10,007.63 vs yday $9,945.22 (+62.41) | 09:30 open · cash $78.31 (unchanged overnight, no fees) · equity $10,007.63 vs prior close $9,945.22 (+62.41) · 8 name(s) re-marked at the open (per-name table). ALHC×119 yday $8.70 → 09:30 $8.68 -2.38; FPS×37 yday $38.06 → 09:30 $39.50 +53.28; BBNX×55 yday $21.43 → 09:30 $21.30 -7.15; JBHT×5 yday $236.80 → 09:30 $236.80 +0.00; ALMU×111 yday $11.54 → 09:30 $11.64 +10.55; ARQT×48 yday $26.46 → 09:30 $26.14 -15.36; AMRX×67 yday $18.28 → 09:30 $18.12 -10.72; BTGO×190 yday $6.76 → 09:30 $6.94 +34.20 | — |
| 2026-09-18 09:30 ET | **SELL** | `ALHC` | 119 | $8.68 | $2.38 | $-197.50 | $1,108.85 | ▼ -197.50 after sell → book $10,005.25; vs 09:30 mark -2.38 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 37 | $39.50 | $2.12 | $+231.10 | $2,568.23 | ▲ +231.10 after sell → book $10,003.13; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 55 | $21.30 | $2.17 | $-68.13 | $3,737.56 | ▼ -68.13 after sell → book $10,000.96; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 5 | $236.80 | $2.02 | $-13.03 | $4,919.53 | ▼ -13.03 after sell → book $9,998.93; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMRX` | 67 | $18.12 | $2.21 | $-33.88 | $6,131.36 | ▼ -33.88 after sell → book $9,996.72; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BTGO` | 190 | $6.94 | $2.60 | $+67.04 | $7,447.36 | ▲ +67.04 after sell → book $9,994.12; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 5 | $209.52 | $2.00 | — | $6,397.75 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1241.23 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 42 | $29.32 | $2.12 | — | $5,164.20 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $1241.23 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 164 | $7.54 | $2.48 | — | $3,925.97 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-20.9; leftover $1241.23 | join🟡 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 83 | $14.79 | $2.24 | — | $2,696.16 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1241.23 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `SECZ` | 133 | $9.32 | $2.39 | — | $1,454.22 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+11.1; leftover $1241.23 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `USDE` | 130 | $9.54 | $2.38 | — | $211.64 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+15.8; leftover $1241.23 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $211.64 | ▲ close $10,272.28 vs 09:30 $10,007.63 (session +291.77) | 16:00 close · cash $211.64 · equity $10,272.28 vs 09:30 $10,007.63 (+264.65; session marks +291.77) · 8 name(s) marked open→close (per-name table). ALMU×111 09:30 $11.64 → close $12.72 +120.43; ARQT×48 09:30 $26.14 → close $25.38 -36.48; GNRC×5 09:30 $209.52 → close $207.44 -10.40; SDGR×42 09:30 $29.32 → close $29.02 -12.60; FLNC×164 09:30 $7.54 → close $7.32 -35.26; RARE×83 09:30 $14.79 → close $14.51 -23.24; SECZ×133 09:30 $9.32 → close $10.86 +204.82; USDE×130 09:30 $9.54 → close $10.19 +84.50 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $211.64 | ▲ 09:30 equity $10,847.72 vs yday $10,272.28 (+575.44) | 09:30 open · cash $211.64 (unchanged overnight, no fees) · equity $10,847.72 vs prior close $10,272.28 (+575.44) · 8 name(s) re-marked at the open (per-name table). ALMU×111 yday $12.72 → 09:30 $13.12 +44.40; ARQT×48 yday $25.38 → 09:30 $25.57 +9.12; GNRC×5 yday $207.44 → 09:30 $210.00 +12.80; SDGR×42 yday $29.02 → 09:30 $29.43 +17.22; FLNC×164 yday $7.32 → 09:30 $7.36 +6.56; RARE×83 yday $14.51 → 09:30 $14.58 +5.81; SECZ×133 yday $10.86 → 09:30 $11.67 +107.73; USDE×130 yday $10.19 → 09:30 $13.05 +371.80 | — |
| 2026-09-21 09:30 ET | **SELL** | `ALMU` | 111 | $13.12 | $2.35 | $+207.89 | $1,666.16 | ▲ +207.89 after sell → book $10,845.37; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARQT` | 48 | $25.57 | $2.15 | $-22.53 | $2,891.36 | ▼ -22.53 after sell → book $10,843.21; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 5 | $210.00 | $2.02 | $-1.63 | $3,939.34 | ▼ -1.63 after sell → book $10,841.19; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 42 | $29.43 | $2.14 | $+0.37 | $5,173.26 | ▲ +0.37 after sell → book $10,839.05; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 164 | $7.36 | $2.52 | $-33.70 | $6,377.78 | ▼ -33.70 after sell → book $10,836.53; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 83 | $14.58 | $2.26 | $-21.93 | $7,585.66 | ▼ -21.93 after sell → book $10,834.27; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SECZ` | 133 | $11.67 | $2.42 | $+307.74 | $9,135.35 | ▲ +307.74 after sell → book $10,831.85; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `USDE` | 130 | $13.05 | $2.42 | $+451.50 | $10,829.43 | ▲ +451.50 after sell → book $10,829.43; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,829.43 | ▲ close $10,829.43 vs 09:30 $10,847.72 (session +0.00) | 16:00 close · cash $10,829.43 · no lots left · equity $10,829.43. | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,829.43 | ▲ 09:30 equity $10,829.43 vs yday $10,829.43 (+0.00) | 09:30 open · cash $10,829.43 · no holdings · equity $10,829.43 vs prior close $10,829.43 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,829.43 | ▲ close $10,829.43 vs 09:30 $10,829.43 (session +0.00) | 16:00 close · cash $10,829.43 · no lots left · equity $10,829.43. | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,829.43 | ▲ 09:30 equity $10,829.43 vs yday $10,829.43 (+0.00) | 09:30 open · cash $10,829.43 · no holdings · equity $10,829.43 vs prior close $10,829.43 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,829.43 | ▲ close $10,829.43 vs 09:30 $10,829.43 (session +0.00) | 16:00 close · cash $10,829.43 · no lots left · equity $10,829.43. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OCUL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `FN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PAAS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AGCO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `METC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `HAL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TSLA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ENOV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUNB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `PURR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TWI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `DYN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CHA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `IONS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SGML` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TTAN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TYRA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `EVMN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BRZE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SMR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NMAX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `QRVO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `RUM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `HQ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
