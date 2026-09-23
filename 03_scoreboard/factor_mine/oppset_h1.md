# Factor mine action — `oppset_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `oppset` · top 8 · rank `opp_rvol` · size `leftover` · sell `list` · S-boost `none` · Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP)

Cash book **+6.35%** ($10,635) · signal-only (no cash/fees) was +17.09%. Starts YES **26/28**. Fills 186 · skips 85 · realized $+634.68.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at Theme Radar Clock-B opportunity-set (T−1 gap + RelVol flagged; optional feed) and only buy names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: Theme Radar Clock-B opportunity-set (T−1 gap + RelVol flagged; optional feed).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: Theme Radar T−1 relative volume (Clock-B opportunity-set; not same-day RelVol).
- Must-have: Theme Radar Clock-B opportunity-set: T−1 gap or RelVol (or week move) flagged — not today's Gap/RelVol.

### When it buys

- At 09:30, take names on Theme Radar Clock-B opportunity-set (T−1 gap + RelVol flagged; optional feed) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
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

- **Universe** `oppset` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `oppset=True` · **rank** `opp_rvol` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,634.66.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ARX` | 63 | — | $19.57 | +0.00 | $19.58 | +0.63 | +0.63 | +0.00 | +0.63 |
| 2026-08-14 | `CLBT` | 115 | — | $10.83 | +0.00 | $11.14 | +35.65 | +35.65 | +0.00 | +35.65 |
| 2026-08-14 | `OMER` | 72 | — | $17.35 | +0.00 | $17.19 | -11.52 | -11.52 | +0.00 | -11.52 |
| 2026-08-14 | `AIRO` | 112 | — | $11.12 | +0.00 | $9.57 | -173.60 | -173.60 | +0.00 | -173.60 |
| 2026-08-14 | `SECZ` | 214 | — | $5.84 | +0.00 | $5.61 | -49.22 | -49.22 | +0.00 | -49.22 |
| 2026-08-14 | `AVAH` | 104 | — | $11.91 | +0.00 | $12.32 | +42.64 | +42.64 | +0.00 | +42.64 |
| 2026-08-14 | `CRMD` | 155 | — | $8.05 | +0.00 | $7.54 | -79.05 | -79.05 | +0.00 | -79.05 |
| 2026-08-14 | `TBBB` | 25 | — | $48.82 | +0.00 | $47.79 | -25.75 | -25.75 | +0.00 | -25.75 |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | — | +0.00 | -0.63 | +0.00 | — |
| 2026-08-17 | `CLBT` | 115 | $11.14 | $11.19 | +5.75 | — | +0.00 | +5.75 | +41.40 | — |
| 2026-08-17 | `OMER` | 72 | $17.19 | $17.17 | -1.44 | — | +0.00 | -1.44 | -12.96 | — |
| 2026-08-17 | `AIRO` | 112 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -173.60 | — |
| 2026-08-17 | `SECZ` | 214 | $5.61 | $5.45 | -34.24 | — | +0.00 | -34.24 | -83.46 | — |
| 2026-08-17 | `AVAH` | 104 | $12.32 | $12.21 | -11.44 | — | +0.00 | -11.44 | +31.20 | — |
| 2026-08-17 | `CRMD` | 155 | $7.54 | $7.55 | +1.55 | — | +0.00 | +1.55 | -77.50 | — |
| 2026-08-17 | `TBBB` | 25 | $47.79 | $47.39 | -10.00 | — | +0.00 | -10.00 | -35.75 | — |
| 2026-08-17 | `CAPR` | 175 | — | $6.87 | +0.00 | $7.45 | +101.50 | +101.50 | +0.00 | +101.50 |
| 2026-08-17 | `HTFL` | 29 | — | $41.23 | +0.00 | $41.94 | +20.59 | +20.59 | +0.00 | +20.59 |
| 2026-08-17 | `NMAX` | 109 | — | $10.97 | +0.00 | $10.36 | -66.49 | -66.49 | +0.00 | -66.49 |
| 2026-08-17 | `RDDT` | 6 | — | $177.51 | +0.00 | $164.50 | -78.06 | -78.06 | +0.00 | -78.06 |
| 2026-08-17 | `VERA` | 38 | — | $31.30 | +0.00 | $31.63 | +12.54 | +12.54 | +0.00 | +12.54 |
| 2026-08-17 | `TSSI` | 125 | — | $9.61 | +0.00 | $9.48 | -16.25 | -16.25 | +0.00 | -16.25 |
| 2026-08-17 | `UMAC` | 37 | — | $32.55 | +0.00 | $30.15 | -88.80 | -88.80 | +0.00 | -88.80 |
| 2026-08-17 | `BYND` | 94 | — | $12.83 | +0.00 | $11.63 | -112.80 | -112.80 | +0.00 | -112.80 |
| 2026-08-18 | `CAPR` | 175 | $7.45 | $7.50 | +8.75 | $7.08 | -73.50 | -64.75 | +110.25 | +36.75 |
| 2026-08-18 | `HTFL` | 29 | $41.94 | $41.50 | -12.76 | — | +0.00 | -12.76 | +7.83 | — |
| 2026-08-18 | `NMAX` | 109 | $10.36 | $10.31 | -5.45 | — | +0.00 | -5.45 | -71.94 | — |
| 2026-08-18 | `RDDT` | 6 | $164.50 | $166.10 | +9.60 | — | +0.00 | +9.60 | -68.46 | — |
| 2026-08-18 | `VERA` | 38 | $31.63 | $31.31 | -12.16 | — | +0.00 | -12.16 | +0.38 | — |
| 2026-08-18 | `TSSI` | 125 | $9.48 | $9.22 | -32.50 | — | +0.00 | -32.50 | -48.75 | — |
| 2026-08-18 | `UMAC` | 37 | $30.15 | $28.59 | -57.72 | — | +0.00 | -57.72 | -146.52 | — |
| 2026-08-18 | `BYND` | 94 | $11.63 | $11.12 | -47.94 | — | +0.00 | -47.94 | -160.74 | — |
| 2026-08-19 | `CAPR` | 175 | $7.08 | $7.19 | +19.25 | — | +0.00 | +19.25 | +56.00 | — |
| 2026-08-20 | `MRNA` | 7 | — | $150.14 | +0.00 | $133.32 | -117.74 | -117.74 | +0.00 | -117.74 |
| 2026-08-20 | `BNTX` | 10 | — | $109.06 | +0.00 | $110.89 | +18.30 | +18.30 | +0.00 | +18.30 |
| 2026-08-20 | `WYFI` | 53 | — | $21.40 | +0.00 | $21.16 | -12.72 | -12.72 | +0.00 | -12.72 |
| 2026-08-20 | `MRVI` | 154 | — | $7.44 | +0.00 | $8.29 | +130.90 | +130.90 | +0.00 | +130.90 |
| 2026-08-20 | `LZB` | 34 | — | $33.61 | +0.00 | $33.65 | +1.36 | +1.36 | +0.00 | +1.36 |
| 2026-08-20 | `EL` | 11 | — | $97.43 | +0.00 | $96.15 | -14.08 | -14.08 | +0.00 | -14.08 |
| 2026-08-20 | `TEM` | 18 | — | $61.83 | +0.00 | $66.65 | +86.76 | +86.76 | +0.00 | +86.76 |
| 2026-08-20 | `WPM` | 7 | — | $144.54 | +0.00 | $150.25 | +39.97 | +39.97 | +0.00 | +39.97 |
| 2026-08-21 | `MRNA` | 7 | $133.32 | $133.11 | -1.47 | $145.13 | +84.14 | +82.67 | -119.21 | -35.07 |
| 2026-08-21 | `BNTX` | 10 | $110.89 | $110.92 | +0.30 | — | +0.00 | +0.30 | +18.60 | — |
| 2026-08-21 | `WYFI` | 53 | $21.16 | $21.54 | +20.14 | — | +0.00 | +20.14 | +7.42 | — |
| 2026-08-21 | `MRVI` | 154 | $8.29 | $8.28 | -1.54 | $8.64 | +55.44 | +53.90 | +129.36 | +184.80 |
| 2026-08-21 | `LZB` | 34 | $33.65 | $33.63 | -0.68 | — | +0.00 | -0.68 | +0.68 | — |
| 2026-08-21 | `EL` | 11 | $96.15 | $96.75 | +6.60 | — | +0.00 | +6.60 | -7.48 | — |
| 2026-08-21 | `TEM` | 18 | $66.65 | $65.60 | -18.90 | $72.69 | +127.62 | +108.72 | +67.86 | +195.48 |
| 2026-08-21 | `WPM` | 7 | $150.25 | $154.70 | +31.15 | — | +0.00 | +31.15 | +71.12 | — |
| 2026-08-21 | `AAP` | 27 | — | $42.41 | +0.00 | $42.58 | +4.59 | +4.59 | +0.00 | +4.59 |
| 2026-08-21 | `ARCT` | 106 | — | $11.13 | +0.00 | $13.45 | +245.92 | +245.92 | +0.00 | +245.92 |
| 2026-08-21 | `WMT` | 11 | — | $103.69 | +0.00 | $103.70 | +0.11 | +0.11 | +0.00 | +0.11 |
| 2026-08-21 | `AMRC` | 52 | — | $22.51 | +0.00 | $21.38 | -58.76 | -58.76 | +0.00 | -58.76 |
| 2026-08-21 | `GMAB` | 35 | — | $33.36 | +0.00 | $33.45 | +3.15 | +3.15 | +0.00 | +3.15 |
| 2026-08-24 | `MRNA` | 7 | $145.13 | $142.70 | -17.01 | — | +0.00 | -17.01 | -52.08 | — |
| 2026-08-24 | `MRVI` | 154 | $8.64 | $8.59 | -7.70 | — | +0.00 | -7.70 | +177.10 | — |
| 2026-08-24 | `TEM` | 18 | $72.69 | $70.08 | -47.07 | — | +0.00 | -47.07 | +148.41 | — |
| 2026-08-24 | `AAP` | 27 | $42.58 | $43.05 | +12.69 | — | +0.00 | +12.69 | +17.28 | — |
| 2026-08-24 | `ARCT` | 106 | $13.45 | $13.33 | -12.72 | $14.34 | +107.06 | +94.34 | +233.20 | +340.26 |
| 2026-08-24 | `WMT` | 11 | $103.70 | $104.14 | +4.84 | — | +0.00 | +4.84 | +4.95 | — |
| 2026-08-24 | `AMRC` | 52 | $21.38 | $21.19 | -9.88 | — | +0.00 | -9.88 | -68.64 | — |
| 2026-08-24 | `GMAB` | 35 | $33.45 | $32.82 | -22.05 | — | +0.00 | -22.05 | -18.90 | — |
| 2026-08-25 | `ARCT` | 106 | $14.34 | $14.12 | -23.32 | — | +0.00 | -23.32 | +316.94 | — |
| 2026-08-25 | `ABUS` | 232 | — | $5.25 | +0.00 | $5.20 | -11.60 | -11.60 | +0.00 | -11.60 |
| 2026-08-25 | `ZURA` | 191 | — | $6.37 | +0.00 | $6.32 | -9.55 | -9.55 | +0.00 | -9.55 |
| 2026-08-25 | `CAPR` | 168 | — | $7.25 | +0.00 | $8.29 | +174.72 | +174.72 | +0.00 | +174.72 |
| 2026-08-25 | `FWDI` | 213 | — | $5.71 | +0.00 | $6.05 | +72.42 | +72.42 | +0.00 | +72.42 |
| 2026-08-25 | `ALVO` | 232 | — | $5.24 | +0.00 | $5.05 | -44.08 | -44.08 | +0.00 | -44.08 |
| 2026-08-25 | `ASST` | 63 | — | $19.04 | +0.00 | $21.39 | +148.05 | +148.05 | +0.00 | +148.05 |
| 2026-08-25 | `LIFE` | 32 | — | $36.96 | +0.00 | $38.56 | +51.20 | +51.20 | +0.00 | +51.20 |
| 2026-08-25 | `QFIN` | 109 | — | $11.09 | +0.00 | $11.53 | +47.96 | +47.96 | +0.00 | +47.96 |
| 2026-08-26 | `ABUS` | 232 | $5.20 | $5.19 | -2.32 | — | +0.00 | -2.32 | -13.92 | — |
| 2026-08-26 | `ZURA` | 191 | $6.32 | $6.13 | -36.29 | — | +0.00 | -36.29 | -45.84 | — |
| 2026-08-26 | `CAPR` | 168 | $8.29 | $8.29 | +0.00 | $9.36 | +179.76 | +179.76 | +174.72 | +354.48 |
| 2026-08-26 | `FWDI` | 213 | $6.05 | $5.97 | -17.04 | — | +0.00 | -17.04 | +55.38 | — |
| 2026-08-26 | `ALVO` | 232 | $5.05 | $4.98 | -16.24 | — | +0.00 | -16.24 | -60.32 | — |
| 2026-08-26 | `ASST` | 63 | $21.39 | $20.72 | -42.21 | — | +0.00 | -42.21 | +105.84 | — |
| 2026-08-26 | `LIFE` | 32 | $38.56 | $38.24 | -10.24 | — | +0.00 | -10.24 | +40.96 | — |
| 2026-08-26 | `QFIN` | 109 | $11.53 | $9.76 | -192.93 | $9.35 | -44.69 | -237.62 | -144.97 | -189.66 |
| 2026-08-26 | `DKS` | 10 | — | $121.87 | +0.00 | $129.66 | +77.90 | +77.90 | +0.00 | +77.90 |
| 2026-08-26 | `BZ` | 73 | — | $16.77 | +0.00 | $18.84 | +151.11 | +151.11 | +0.00 | +151.11 |
| 2026-08-26 | `MAIR` | 44 | — | $27.59 | +0.00 | $28.51 | +40.48 | +40.48 | +0.00 | +40.48 |
| 2026-08-26 | `DY` | 3 | — | $326.91 | +0.00 | $310.91 | -48.00 | -48.00 | +0.00 | -48.00 |
| 2026-08-26 | `KURA` | 90 | — | $13.63 | +0.00 | $13.06 | -51.30 | -51.30 | +0.00 | -51.30 |
| 2026-08-26 | `SMTC` | 9 | — | $130.90 | +0.00 | $140.80 | +89.10 | +89.10 | +0.00 | +89.10 |
| 2026-08-27 | `CAPR` | 168 | $9.36 | $9.19 | -28.56 | — | +0.00 | -28.56 | +325.92 | — |
| 2026-08-27 | `QFIN` | 109 | $9.35 | $9.42 | +7.63 | — | +0.00 | +7.63 | -182.03 | — |
| 2026-08-27 | `DKS` | 10 | $129.66 | $128.73 | -9.30 | — | +0.00 | -9.30 | +68.60 | — |
| 2026-08-27 | `BZ` | 73 | $18.84 | $18.50 | -24.82 | — | +0.00 | -24.82 | +126.29 | — |
| 2026-08-27 | `MAIR` | 44 | $28.51 | $28.76 | +11.00 | — | +0.00 | +11.00 | +51.48 | — |
| 2026-08-27 | `DY` | 3 | $310.91 | $314.90 | +11.97 | — | +0.00 | +11.97 | -36.03 | — |
| 2026-08-27 | `KURA` | 90 | $13.06 | $12.98 | -7.20 | — | +0.00 | -7.20 | -58.50 | — |
| 2026-08-27 | `SMTC` | 9 | $140.80 | $149.40 | +77.40 | — | +0.00 | +77.40 | +166.50 | — |
| 2026-08-28 | `ANF` | 8 | — | $146.07 | +0.00 | $148.42 | +18.80 | +18.80 | +0.00 | +18.80 |
| 2026-08-28 | `BZ` | 70 | — | $18.15 | +0.00 | $17.80 | -24.50 | -24.50 | +0.00 | -24.50 |
| 2026-08-28 | `QFIN` | 139 | — | $9.15 | +0.00 | $8.80 | -48.65 | -48.65 | +0.00 | -48.65 |
| 2026-08-28 | `BHVN` | 80 | — | $15.88 | +0.00 | $15.41 | -37.60 | -37.60 | +0.00 | -37.60 |
| 2026-08-28 | `DY` | 4 | — | $306.34 | +0.00 | $294.34 | -48.00 | -48.00 | +0.00 | -48.00 |
| 2026-08-28 | `GENB` | 81 | — | $15.77 | +0.00 | $15.33 | -35.64 | -35.64 | +0.00 | -35.64 |
| 2026-08-28 | `URBN` | 16 | — | $79.42 | +0.00 | $81.09 | +26.72 | +26.72 | +0.00 | +26.72 |
| 2026-08-28 | `JKS` | 95 | — | $13.37 | +0.00 | $13.54 | +16.15 | +16.15 | +0.00 | +16.15 |
| 2026-08-31 | `ANF` | 8 | $148.42 | $148.03 | -3.12 | — | +0.00 | -3.12 | +15.68 | — |
| 2026-08-31 | `BZ` | 70 | $17.80 | $17.70 | -7.00 | — | +0.00 | -7.00 | -31.50 | — |
| 2026-08-31 | `QFIN` | 139 | $8.80 | $8.70 | -13.90 | — | +0.00 | -13.90 | -62.55 | — |
| 2026-08-31 | `BHVN` | 80 | $15.41 | $15.46 | +4.00 | — | +0.00 | +4.00 | -33.60 | — |
| 2026-08-31 | `DY` | 4 | $294.34 | $298.01 | +14.68 | — | +0.00 | +14.68 | -33.32 | — |
| 2026-08-31 | `GENB` | 81 | $15.33 | $15.27 | -4.86 | — | +0.00 | -4.86 | -40.50 | — |
| 2026-08-31 | `URBN` | 16 | $81.09 | $80.44 | -10.40 | — | +0.00 | -10.40 | +16.32 | — |
| 2026-08-31 | `JKS` | 95 | $13.54 | $13.54 | +0.00 | — | +0.00 | +0.00 | +16.15 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ALMS` | 120 | — | $10.38 | +0.00 | $11.36 | +118.20 | +118.20 | +0.00 | +118.20 |
| 2026-09-03 | `FRVO` | 68 | — | $18.28 | +0.00 | $17.16 | -76.16 | -76.16 | +0.00 | -76.16 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `CNH` | 91 | — | $13.71 | +0.00 | $13.84 | +11.83 | +11.83 | +0.00 | +11.83 |
| 2026-09-03 | `EIX` | 22 | — | $55.42 | +0.00 | $56.30 | +19.36 | +19.36 | +0.00 | +19.36 |
| 2026-09-03 | `MMED` | 52 | — | $23.88 | +0.00 | $23.84 | -2.08 | -2.08 | +0.00 | -2.08 |
| 2026-09-03 | `RSKD` | 187 | — | $6.68 | +0.00 | $6.93 | +46.75 | +46.75 | +0.00 | +46.75 |
| 2026-09-03 | `AGCO` | 9 | — | $127.91 | +0.00 | $125.82 | -18.81 | -18.81 | +0.00 | -18.81 |
| 2026-09-04 | `ALMS` | 120 | $11.36 | $11.23 | -15.60 | — | +0.00 | -15.60 | +102.60 | — |
| 2026-09-04 | `FRVO` | 68 | $17.16 | $17.27 | +7.48 | — | +0.00 | +7.48 | -68.68 | — |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | $524.14 | +20.72 | +15.50 | +54.94 | +75.66 |
| 2026-09-04 | `CNH` | 91 | $13.84 | $13.89 | +4.55 | — | +0.00 | +4.55 | +16.38 | — |
| 2026-09-04 | `EIX` | 22 | $56.30 | $55.79 | -11.22 | — | +0.00 | -11.22 | +8.14 | — |
| 2026-09-04 | `MMED` | 52 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.08 | — |
| 2026-09-04 | `RSKD` | 187 | $6.93 | $6.84 | -16.83 | $6.51 | -61.71 | -78.54 | +29.92 | -31.79 |
| 2026-09-04 | `AGCO` | 9 | $125.82 | $125.22 | -5.40 | — | +0.00 | -5.40 | -24.21 | — |
| 2026-09-04 | `LULU` | 13 | — | $98.15 | +0.00 | $100.61 | +31.98 | +31.98 | +0.00 | +31.98 |
| 2026-09-04 | `ASST` | 51 | — | $25.18 | +0.00 | $27.14 | +99.96 | +99.96 | +0.00 | +99.96 |
| 2026-09-04 | `PL` | 66 | — | $19.64 | +0.00 | $18.12 | -100.32 | -100.32 | +0.00 | -100.32 |
| 2026-09-04 | `ZS` | 7 | — | $166.15 | +0.00 | $169.80 | +25.55 | +25.55 | +0.00 | +25.55 |
| 2026-09-04 | `IOT` | 29 | — | $44.90 | +0.00 | $40.20 | -136.30 | -136.30 | +0.00 | -136.30 |
| 2026-09-04 | `MRX` | 17 | — | $75.65 | +0.00 | $78.27 | +44.54 | +44.54 | +0.00 | +44.54 |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | — | +0.00 | -5.98 | +69.68 | — |
| 2026-09-08 | `RSKD` | 187 | $6.51 | $6.46 | -9.35 | — | +0.00 | -9.35 | -41.14 | — |
| 2026-09-08 | `LULU` | 13 | $100.61 | $100.58 | -0.39 | — | +0.00 | -0.39 | +31.59 | — |
| 2026-09-08 | `ASST` | 51 | $27.14 | $26.44 | -35.70 | — | +0.00 | -35.70 | +64.26 | — |
| 2026-09-08 | `PL` | 66 | $18.12 | $17.85 | -17.82 | — | +0.00 | -17.82 | -118.14 | — |
| 2026-09-08 | `ZS` | 7 | $169.80 | $165.62 | -29.30 | — | +0.00 | -29.30 | -3.74 | — |
| 2026-09-08 | `IOT` | 29 | $40.20 | $39.56 | -18.56 | — | +0.00 | -18.56 | -154.86 | — |
| 2026-09-08 | `MRX` | 17 | $78.27 | $78.84 | +9.69 | — | +0.00 | +9.69 | +54.23 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `BKV` | 49 | — | $24.97 | +0.00 | $24.23 | -36.26 | -36.26 | +0.00 | -36.26 |
| 2026-09-11 | `COO` | 22 | — | $54.66 | +0.00 | $53.91 | -16.50 | -16.50 | +0.00 | -16.50 |
| 2026-09-11 | `AEO` | 84 | — | $14.71 | +0.00 | $15.02 | +26.04 | +26.04 | +0.00 | +26.04 |
| 2026-09-11 | `WLTH` | 113 | — | $10.95 | +0.00 | $10.38 | -64.41 | -64.41 | +0.00 | -64.41 |
| 2026-09-11 | `NAVN` | 60 | — | $20.61 | +0.00 | $21.02 | +24.60 | +24.60 | +0.00 | +24.60 |
| 2026-09-11 | `TSSI` | 137 | — | $8.98 | +0.00 | $8.93 | -6.85 | -6.85 | +0.00 | -6.85 |
| 2026-09-11 | `IRD` | 201 | — | $6.16 | +0.00 | $6.04 | -24.12 | -24.12 | +0.00 | -24.12 |
| 2026-09-11 | `AXGN` | 29 | — | $42.48 | +0.00 | $42.16 | -9.28 | -9.28 | +0.00 | -9.28 |
| 2026-09-14 | `BKV` | 49 | $24.23 | $24.26 | +1.47 | $23.82 | -21.56 | -20.09 | -34.79 | -56.35 |
| 2026-09-14 | `COO` | 22 | $53.91 | $54.78 | +19.14 | — | +0.00 | +19.14 | +2.64 | — |
| 2026-09-14 | `AEO` | 84 | $15.02 | $14.85 | -14.28 | — | +0.00 | -14.28 | +11.76 | — |
| 2026-09-14 | `WLTH` | 113 | $10.38 | $10.29 | -10.17 | — | +0.00 | -10.17 | -74.58 | — |
| 2026-09-14 | `NAVN` | 60 | $21.02 | $21.10 | +4.80 | — | +0.00 | +4.80 | +29.40 | — |
| 2026-09-14 | `TSSI` | 137 | $8.93 | $8.57 | -49.32 | — | +0.00 | -49.32 | -56.17 | — |
| 2026-09-14 | `IRD` | 201 | $6.04 | $6.02 | -4.02 | — | +0.00 | -4.02 | -28.14 | — |
| 2026-09-14 | `AXGN` | 29 | $42.16 | $41.55 | -17.69 | — | +0.00 | -17.69 | -26.97 | — |
| 2026-09-15 | `BKV` | 49 | $23.82 | $24.25 | +21.07 | — | +0.00 | +21.07 | -35.28 | — |
| 2026-09-16 | `PLAY` | 176 | — | $6.86 | +0.00 | $6.86 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-16 | `ALHC` | 117 | — | $10.30 | +0.00 | $8.71 | -186.03 | -186.03 | +0.00 | -186.03 |
| 2026-09-16 | `FPS` | 36 | — | $33.14 | +0.00 | $34.84 | +61.20 | +61.20 | +0.00 | +61.20 |
| 2026-09-16 | `GFR` | 177 | — | $6.83 | +0.00 | $6.49 | -60.18 | -60.18 | +0.00 | -60.18 |
| 2026-09-16 | `HQ` | 94 | — | $12.89 | +0.00 | $13.56 | +62.98 | +62.98 | +0.00 | +62.98 |
| 2026-09-16 | `SDGR` | 52 | — | $23.29 | +0.00 | $23.93 | +33.28 | +33.28 | +0.00 | +33.28 |
| 2026-09-16 | `DMRA` | 48 | — | $24.88 | +0.00 | $24.53 | -16.80 | -16.80 | +0.00 | -16.80 |
| 2026-09-16 | `RVTY` | 8 | — | $140.88 | +0.00 | $145.73 | +38.80 | +38.80 | +0.00 | +38.80 |
| 2026-09-17 | `PLAY` | 176 | $6.86 | $6.96 | +17.60 | — | +0.00 | +17.60 | +17.60 | — |
| 2026-09-17 | `ALHC` | 117 | $8.71 | $8.58 | -15.21 | $8.70 | +14.04 | -1.17 | -201.24 | -187.20 |
| 2026-09-17 | `FPS` | 36 | $34.84 | $36.76 | +69.12 | $38.06 | +46.80 | +115.92 | +130.32 | +177.12 |
| 2026-09-17 | `GFR` | 177 | $6.49 | $6.48 | -1.77 | — | +0.00 | -1.77 | -61.95 | — |
| 2026-09-17 | `HQ` | 94 | $13.56 | $13.56 | +0.00 | — | +0.00 | +0.00 | +62.98 | — |
| 2026-09-17 | `SDGR` | 52 | $23.93 | $24.09 | +8.32 | — | +0.00 | +8.32 | +41.60 | — |
| 2026-09-17 | `DMRA` | 48 | $24.53 | $24.96 | +20.64 | — | +0.00 | +20.64 | +3.84 | — |
| 2026-09-17 | `RVTY` | 8 | $145.73 | $147.61 | +15.04 | — | +0.00 | +15.04 | +53.84 | — |
| 2026-09-17 | `BBNX` | 54 | — | $22.46 | +0.00 | $21.43 | -55.62 | -55.62 | +0.00 | -55.62 |
| 2026-09-17 | `JBHT` | 5 | — | $238.60 | +0.00 | $236.80 | -9.00 | -9.00 | +0.00 | -9.00 |
| 2026-09-17 | `ALMU` | 109 | — | $11.21 | +0.00 | $11.54 | +36.51 | +36.51 | +0.00 | +36.51 |
| 2026-09-17 | `ARQT` | 47 | — | $25.95 | +0.00 | $26.46 | +23.97 | +23.97 | +0.00 | +23.97 |
| 2026-09-17 | `AMRX` | 66 | — | $18.56 | +0.00 | $18.28 | -18.48 | -18.48 | +0.00 | -18.48 |
| 2026-09-17 | `BTGO` | 187 | — | $6.56 | +0.00 | $6.76 | +37.40 | +37.40 | +0.00 | +37.40 |
| 2026-09-18 | `ALHC` | 117 | $8.70 | $8.68 | -2.34 | — | +0.00 | -2.34 | -189.54 | — |
| 2026-09-18 | `FPS` | 36 | $38.06 | $39.50 | +51.84 | — | +0.00 | +51.84 | +228.96 | — |
| 2026-09-18 | `BBNX` | 54 | $21.43 | $21.30 | -7.02 | — | +0.00 | -7.02 | -62.64 | — |
| 2026-09-18 | `JBHT` | 5 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -9.00 | — |
| 2026-09-18 | `ALMU` | 109 | $11.54 | $11.64 | +10.36 | $12.72 | +118.26 | +128.62 | +46.87 | +165.13 |
| 2026-09-18 | `ARQT` | 47 | $26.46 | $26.14 | -15.04 | $25.38 | -35.72 | -50.76 | +8.93 | -26.79 |
| 2026-09-18 | `AMRX` | 66 | $18.28 | $18.12 | -10.56 | — | +0.00 | -10.56 | -29.04 | — |
| 2026-09-18 | `BTGO` | 187 | $6.76 | $6.94 | +33.66 | — | +0.00 | +33.66 | +71.06 | — |
| 2026-09-18 | `GNRC` | 5 | — | $209.52 | +0.00 | $207.44 | -10.40 | -10.40 | +0.00 | -10.40 |
| 2026-09-18 | `SDGR` | 41 | — | $29.32 | +0.00 | $29.02 | -12.30 | -12.30 | +0.00 | -12.30 |
| 2026-09-18 | `FLNC` | 161 | — | $7.54 | +0.00 | $7.32 | -34.61 | -34.61 | +0.00 | -34.61 |
| 2026-09-18 | `RARE` | 82 | — | $14.79 | +0.00 | $14.51 | -22.96 | -22.96 | +0.00 | -22.96 |
| 2026-09-18 | `SECZ` | 130 | — | $9.32 | +0.00 | $10.86 | +200.20 | +200.20 | +0.00 | +200.20 |
| 2026-09-18 | `USDE` | 127 | — | $9.54 | +0.00 | $10.19 | +82.55 | +82.55 | +0.00 | +82.55 |
| 2026-09-21 | `ALMU` | 109 | $12.72 | $13.12 | +43.60 | — | +0.00 | +43.60 | +208.73 | — |
| 2026-09-21 | `ARQT` | 47 | $25.38 | $25.57 | +8.93 | — | +0.00 | +8.93 | -17.86 | — |
| 2026-09-21 | `GNRC` | 5 | $207.44 | $210.00 | +12.80 | — | +0.00 | +12.80 | +2.40 | — |
| 2026-09-21 | `SDGR` | 41 | $29.02 | $29.43 | +16.81 | — | +0.00 | +16.81 | +4.51 | — |
| 2026-09-21 | `FLNC` | 161 | $7.32 | $7.36 | +6.44 | — | +0.00 | +6.44 | -28.17 | — |
| 2026-09-21 | `RARE` | 82 | $14.51 | $14.58 | +5.74 | — | +0.00 | +5.74 | -17.22 | — |
| 2026-09-21 | `SECZ` | 130 | $10.86 | $11.67 | +105.30 | — | +0.00 | +105.30 | +305.50 | — |
| 2026-09-21 | `USDE` | 127 | $10.19 | $13.05 | +363.22 | — | +0.00 | +363.22 | +445.77 | — |
| 2026-09-22 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -260.22 | ARX, CLBT, OMER, AIRO, SECZ, AVAH, CRMD, TBBB | — | $51.72 | $9,721.15 | ARX×63, CLBT×115, OMER×72, AIRO×112, SECZ×214, AVAH×104, CRMD×155, TBBB×25 |
| 2026-08-17 | +2.25 | $51.72 | ARX×63, CLBT×115, OMER×72, AIRO×112, SECZ×214, AVAH×104, CRMD×155, TBBB×25 | $9,670.70 | -50.45 | -227.77 | CAPR, HTFL, NMAX, RDDT, VERA, TSSI, UMAC, BYND | ARX, CLBT, OMER, AIRO, SECZ, AVAH, CRMD, TBBB | $174.35 | $9,406.31 | CAPR×175, HTFL×29, NMAX×109, RDDT×6, VERA×38, TSSI×125, UMAC×37, BYND×94 |
| 2026-08-18 | -6.20 | $174.35 | CAPR×175, HTFL×29, NMAX×109, RDDT×6, VERA×38, TSSI×125, UMAC×37, BYND×94 | $9,256.13 | -150.18 | -73.50 | — | HTFL, NMAX, RDDT, VERA, TSSI, UMAC, BYND | $7,928.23 | $9,167.23 | CAPR×175 |
| 2026-08-19 | -7.20 | $7,928.23 | CAPR×175 | $9,186.48 | +19.25 | +0.00 | — | CAPR | $9,183.92 | $9,183.92 | — |
| 2026-08-20 | +1.12 | $9,183.92 | — | $9,183.92 | +0.00 | +132.75 | MRNA, BNTX, WYFI, MRVI, LZB, EL, TEM, WPM | — | $406.39 | $9,299.87 | MRNA×7, BNTX×10, WYFI×53, MRVI×154, LZB×34, EL×11, TEM×18, WPM×7 |
| 2026-08-21 | +3.25 | $406.39 | MRNA×7, BNTX×10, WYFI×53, MRVI×154, LZB×34, EL×11, TEM×18, WPM×7 | $9,335.47 | +35.60 | +462.21 | AAP, ARCT, WMT, AMRC, GMAB | BNTX, WYFI, LZB, EL, WPM | $123.18 | $9,776.64 | MRNA×7, MRVI×154, TEM×18, AAP×27, ARCT×106, WMT×11, AMRC×52, GMAB×35 |
| 2026-08-24 | -5.17 | $123.18 | MRNA×7, MRVI×154, TEM×18, AAP×27, ARCT×106, WMT×11, AMRC×52, GMAB×35 | $9,677.74 | -98.90 | +107.06 | — | MRNA, MRVI, TEM, AAP, WMT, AMRC, GMAB | $8,249.76 | $9,769.80 | ARCT×106 |
| 2026-08-25 | +1.80 | $8,249.76 | ARCT×106 | $9,746.48 | -23.32 | +429.12 | ABUS, ZURA, CAPR, FWDI, ALVO, ASST, LIFE, QFIN | ARCT | $48.14 | $10,152.89 | ABUS×232, ZURA×191, CAPR×168, FWDI×213, ALVO×232, ASST×63, LIFE×32, QFIN×109 |
| 2026-08-26 | +2.02 | $48.14 | ABUS×232, ZURA×191, CAPR×168, FWDI×213, ALVO×232, ASST×63, LIFE×32, QFIN×109 | $9,835.62 | -317.27 | +394.36 | DKS, BZ, MAIR, DY, KURA, SMTC | ABUS, ZURA, FWDI, ALVO, ASST, LIFE | $308.25 | $10,201.57 | CAPR×168, QFIN×109, DKS×10, BZ×73, MAIR×44, DY×3, KURA×90, SMTC×9 |
| 2026-08-27 | — | $308.25 | CAPR×168, QFIN×109, DKS×10, BZ×73, MAIR×44, DY×3, KURA×90, SMTC×9 | $10,239.69 | +38.12 | +0.00 | — | CAPR, QFIN, DKS, BZ, MAIR, DY, KURA, SMTC | $10,222.06 | $10,222.06 | — |
| 2026-08-28 | +0.75 | $10,222.06 | — | $10,222.06 | -0.00 | -132.72 | ANF, BZ, QFIN, BHVN, DY, GENB, URBN, JKS | — | $179.75 | $10,071.94 | ANF×8, BZ×70, QFIN×139, BHVN×80, DY×4, GENB×81, URBN×16, JKS×95 |
| 2026-08-31 | -5.85 | $179.75 | ANF×8, BZ×70, QFIN×139, BHVN×80, DY×4, GENB×81, URBN×16, JKS×95 | $10,051.34 | -20.60 | +0.00 | — | ANF, BZ, QFIN, BHVN, DY, GENB, URBN, JKS | $10,033.75 | $10,033.75 | — |
| 2026-09-01 | -6.30 | $10,033.75 | — | $10,033.75 | -0.00 | +0.00 | — | — | $10,033.75 | $10,033.75 | — |
| 2026-09-02 | -3.83 | $10,033.75 | — | $10,033.75 | -0.00 | +0.00 | — | — | $10,033.75 | $10,033.75 | — |
| 2026-09-03 | -0.90 | $10,033.75 | — | $10,033.75 | -0.00 | +159.25 | ALMS, FRVO, DELL, CNH, EIX, MMED, RSKD, AGCO | — | $446.56 | $10,175.43 | ALMS×120, FRVO×68, DELL×2, CNH×91, EIX×22, MMED×52, RSKD×187, AGCO×9 |
| 2026-09-04 | +2.25 | $446.56 | ALMS×120, FRVO×68, DELL×2, CNH×91, EIX×22, MMED×52, RSKD×187, AGCO×9 | $10,133.19 | -42.24 | -75.58 | LULU, ASST, PL, ZS, IOT, MRX | ALMS, FRVO, CNH, EIX, MMED, AGCO | $193.32 | $10,031.95 | DELL×2, RSKD×187, LULU×13, ASST×51, PL×66, ZS×7, IOT×29, MRX×17 |
| 2026-09-08 | -11.47 | $193.32 | DELL×2, RSKD×187, LULU×13, ASST×51, PL×66, ZS×7, IOT×29, MRX×17 | $9,924.55 | -107.40 | +0.00 | — | DELL, RSKD, LULU, ASST, PL, ZS, IOT, MRX | $9,907.33 | $9,907.33 | — |
| 2026-09-09 | -13.95 | $9,907.33 | — | $9,907.33 | -0.00 | +0.00 | — | — | $9,907.33 | $9,907.33 | — |
| 2026-09-10 | -13.28 | $9,907.33 | — | $9,907.33 | -0.00 | +0.00 | — | — | $9,907.33 | $9,907.33 | — |
| 2026-09-11 | +0.50 | $9,907.33 | — | $9,907.33 | -0.00 | -106.78 | BKV, COO, AEO, WLTH, NAVN, TSSI, IRD, AXGN | — | $53.34 | $9,782.54 | BKV×49, COO×22, AEO×84, WLTH×113, NAVN×60, TSSI×137, IRD×201, AXGN×29 |
| 2026-09-14 | -11.00 | $53.34 | BKV×49, COO×22, AEO×84, WLTH×113, NAVN×60, TSSI×137, IRD×201, AXGN×29 | $9,712.47 | -70.07 | -21.56 | — | COO, AEO, WLTH, NAVN, TSSI, IRD, AXGN | $8,507.67 | $9,674.85 | BKV×49 |
| 2026-09-15 | -3.84 | $8,507.67 | BKV×49 | $9,695.92 | +21.07 | +0.00 | — | BKV | $9,693.76 | $9,693.76 | — |
| 2026-09-16 | +5.30 | $9,693.76 | — | $9,693.76 | +0.00 | -66.75 | PLAY, ALHC, FPS, GFR, HQ, SDGR, DMRA, RVTY | — | $117.29 | $9,608.97 | PLAY×176, ALHC×117, FPS×36, GFR×177, HQ×94, SDGR×52, DMRA×48, RVTY×8 |
| 2026-09-17 | +7.38 | $117.29 | PLAY×176, ALHC×117, FPS×36, GFR×177, HQ×94, SDGR×52, DMRA×48, RVTY×8 | $9,722.71 | +113.74 | +75.62 | BBNX, JBHT, ALMU, ARQT, AMRX, BTGO | PLAY, GFR, HQ, SDGR, DMRA, RVTY | $69.31 | $9,771.22 | ALHC×117, FPS×36, BBNX×54, JBHT×5, ALMU×109, ARQT×47, AMRX×66, BTGO×187 |
| 2026-09-18 | +4.86 | $69.31 | ALHC×117, FPS×36, BBNX×54, JBHT×5, ALMU×109, ARQT×47, AMRX×66, BTGO×187 | $9,832.11 | +60.89 | +285.02 | GNRC, SDGR, FLNC, RARE, SECZ, USDE | ALHC, FPS, BBNX, JBHT, AMRX, BTGO | $208.89 | $10,090.07 | ALMU×109, ARQT×47, GNRC×5, SDGR×41, FLNC×161, RARE×82, SECZ×130, USDE×127 |
| 2026-09-21 | +12.87 | $208.89 | ALMU×109, ARQT×47, GNRC×5, SDGR×41, FLNC×161, RARE×82, SECZ×130, USDE×127 | $10,652.91 | +562.84 | +0.00 | — | ALMU, ARQT, GNRC, SDGR, FLNC, RARE, SECZ, USDE | $10,634.66 | $10,634.66 | — |
| 2026-09-22 | -0.50 | $10,634.66 | — | $10,634.66 | +0.00 | +0.00 | — | — | $10,634.66 | $10,634.66 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $8,764.91 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 115 | $10.83 | $2.33 | — | $7,517.13 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `OMER` | 72 | $17.35 | $2.21 | — | $6,265.72 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $5,017.95 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SECZ` | 214 | $5.84 | $2.76 | — | $3,765.43 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ⚪; ret5=-20.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AVAH` | 104 | $11.91 | $2.30 | — | $2,524.49 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+21.3; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `CRMD` | 155 | $8.05 | $2.46 | — | $1,274.29 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 25 | $48.82 | $2.06 | — | $51.72 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.72 | ▼ close $9,721.15 vs 09:30 $10,000.00 (session -260.22) | 16:00 close · cash $51.72 · equity $9,721.15 vs 09:30 $10,000.00 (-278.85; session marks -260.22) · 8 name(s) marked open→close (per-name table). ARX×63 09:30 $19.57 → close $19.58 +0.63; CLBT×115 09:30 $10.83 → close $11.14 +35.65; OMER×72 09:30 $17.35 → close $17.19 -11.52; AIRO×112 09:30 $11.12 → close $9.57 -173.60; SECZ×214 09:30 $5.84 → close $5.61 -49.22; AVAH×104 09:30 $11.91 → close $12.32 +42.64; CRMD×155 09:30 $8.05 → close $7.54 -79.05; TBBB×25 09:30 $48.82 → close $47.79 -25.75 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.72 | ▼ 09:30 equity $9,670.70 vs yday $9,721.15 (-50.45) | 09:30 open · cash $51.72 (unchanged overnight, no fees) · equity $9,670.70 vs prior close $9,721.15 (-50.45) · 8 name(s) re-marked at the open (per-name table). ARX×63 yday $19.58 → 09:30 $19.57 -0.63; CLBT×115 yday $11.14 → 09:30 $11.19 +5.75; OMER×72 yday $17.19 → 09:30 $17.17 -1.44; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; SECZ×214 yday $5.61 → 09:30 $5.45 -34.24; AVAH×104 yday $12.32 → 09:30 $12.21 -11.44; CRMD×155 yday $7.54 → 09:30 $7.55 +1.55; TBBB×25 yday $47.79 → 09:30 $47.39 -10.00 | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $1,282.43 | ▼ -4.38 after sell → book $9,668.50; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 115 | $11.19 | $2.36 | $+36.70 | $2,566.92 | ▲ +36.70 after sell → book $9,666.14; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `OMER` | 72 | $17.17 | $2.23 | $-17.39 | $3,800.93 | ▼ -17.39 after sell → book $9,663.91; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $4,870.41 | ▼ -178.28 after sell → book $9,661.55; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SECZ` | 214 | $5.45 | $2.81 | $-89.03 | $6,033.91 | ▼ -89.03 after sell → book $9,658.75; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AVAH` | 104 | $12.21 | $2.33 | $+26.57 | $7,301.42 | ▲ +26.57 after sell → book $9,656.42; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CRMD` | 155 | $7.55 | $2.49 | $-82.45 | $8,469.18 | ▼ -82.45 after sell → book $9,653.93; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 25 | $47.39 | $2.08 | $-39.90 | $9,651.84 | ▼ -39.90 after sell → book $9,651.84; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 175 | $6.87 | $2.52 | — | $8,447.08 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+62.6; leftover $1206.48 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $7,249.33 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+46.0; leftover $1206.48 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 109 | $10.97 | $2.32 | — | $6,051.28 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $1206.48 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `RDDT` | 6 | $177.51 | $2.01 | — | $4,984.22 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ⚪; ret5=+10.1; leftover $1206.48 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 38 | $31.30 | $2.10 | — | $3,792.71 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-3.8; leftover $1206.48 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TSSI` | 125 | $9.61 | $2.37 | — | $2,589.10 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ⚪; ret5=-14.0; leftover $1206.48 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $1,382.65 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1206.48 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BYND` | 94 | $12.83 | $2.27 | — | $174.35 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ⚪; ret5=-34.1; leftover $1206.48 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.35 | ▼ close $9,406.31 vs 09:30 $9,670.70 (session -227.77) | 16:00 close · cash $174.35 · equity $9,406.31 vs 09:30 $9,670.70 (-264.39; session marks -227.77) · 8 name(s) marked open→close (per-name table). CAPR×175 09:30 $6.87 → close $7.45 +101.50; HTFL×29 09:30 $41.23 → close $41.94 +20.59; NMAX×109 09:30 $10.97 → close $10.36 -66.49; RDDT×6 09:30 $177.51 → close $164.50 -78.06; VERA×38 09:30 $31.30 → close $31.63 +12.54; TSSI×125 09:30 $9.61 → close $9.48 -16.25; UMAC×37 09:30 $32.55 → close $30.15 -88.80; BYND×94 09:30 $12.83 → close $11.63 -112.80 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.35 | ▼ 09:30 equity $9,256.13 vs yday $9,406.31 (-150.18) | 09:30 open · cash $174.35 (unchanged overnight, no fees) · equity $9,256.13 vs prior close $9,406.31 (-150.18) · 8 name(s) re-marked at the open (per-name table). CAPR×175 yday $7.45 → 09:30 $7.50 +8.75; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; NMAX×109 yday $10.36 → 09:30 $10.31 -5.45; RDDT×6 yday $164.50 → 09:30 $166.10 +9.60; VERA×38 yday $31.63 → 09:30 $31.31 -12.16; TSSI×125 yday $9.48 → 09:30 $9.22 -32.50; UMAC×37 yday $30.15 → 09:30 $28.59 -57.72; BYND×94 yday $11.63 → 09:30 $11.12 -47.94 | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $1,375.76 | ▲ +3.66 after sell → book $9,254.04; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 109 | $10.31 | $2.35 | $-76.60 | $2,497.20 | ▼ -76.60 after sell → book $9,251.69; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `RDDT` | 6 | $166.10 | $2.03 | $-72.50 | $3,491.77 | ▼ -72.50 after sell → book $9,249.66; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 38 | $31.31 | $2.12 | $-3.85 | $4,679.43 | ▼ -3.85 after sell → book $9,247.54; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TSSI` | 125 | $9.22 | $2.40 | $-53.51 | $5,829.53 | ▼ -53.51 after sell → book $9,245.14; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $6,885.24 | ▼ -150.74 after sell → book $9,243.02; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BYND` | 94 | $11.12 | $2.30 | $-165.31 | $7,928.23 | ▼ -165.31 after sell → book $9,240.73; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,928.23 | ▼ close $9,167.23 vs 09:30 $9,256.13 (session -73.50) | 16:00 close · cash $7,928.23 · equity $9,167.23 vs 09:30 $9,256.13 (-88.90; session marks -73.50) · 1 name(s) marked open→close (per-name table). CAPR×175 09:30 $7.50 → close $7.08 -73.50 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,928.23 | ▲ 09:30 equity $9,186.48 vs yday $9,167.23 (+19.25) | 09:30 open · cash $7,928.23 (unchanged overnight, no fees) · equity $9,186.48 vs prior close $9,167.23 (+19.25) · 1 name(s) re-marked at the open (per-name table). CAPR×175 yday $7.08 → 09:30 $7.19 +19.25 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 175 | $7.19 | $2.55 | $+50.93 | $9,183.92 | ▲ +50.93 after sell → book $9,183.92; vs 09:30 mark -2.56 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,183.92 | ▲ close $9,183.92 vs 09:30 $9,186.48 (session +0.00) | 16:00 close · cash $9,183.92 · no lots left · equity $9,183.92. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,183.92 | ▲ 09:30 equity $9,183.92 vs yday $9,183.92 (+0.00) | 09:30 open · cash $9,183.92 · no holdings · equity $9,183.92 vs prior close $9,183.92 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $8,130.93 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1147.99 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 10 | $109.06 | $2.02 | — | $7,038.31 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $1147.99 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WYFI` | 53 | $21.40 | $2.15 | — | $5,901.96 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-25.2; leftover $1147.99 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 154 | $7.44 | $2.45 | — | $4,753.75 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1147.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `LZB` | 34 | $33.61 | $2.09 | — | $3,608.92 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-17.4; leftover $1147.99 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 11 | $97.43 | $2.02 | — | $2,535.17 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; leftover $1147.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TEM` | 18 | $61.83 | $2.04 | — | $1,420.18 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1147.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $406.39 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1147.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $406.39 | ▲ close $9,299.87 vs 09:30 $9,183.92 (session +132.75) | 16:00 close · cash $406.39 · equity $9,299.87 vs 09:30 $9,183.92 (+115.95; session marks +132.75) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $150.14 → close $133.32 -117.74; BNTX×10 09:30 $109.06 → close $110.89 +18.30; WYFI×53 09:30 $21.40 → close $21.16 -12.72; MRVI×154 09:30 $7.44 → close $8.29 +130.90; LZB×34 09:30 $33.61 → close $33.65 +1.36; EL×11 09:30 $97.43 → close $96.15 -14.08; TEM×18 09:30 $61.83 → close $66.65 +86.76; WPM×7 09:30 $144.54 → close $150.25 +39.97 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $406.39 | ▲ 09:30 equity $9,335.47 vs yday $9,299.87 (+35.60) | 09:30 open · cash $406.39 (unchanged overnight, no fees) · equity $9,335.47 vs prior close $9,299.87 (+35.60) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $133.32 → 09:30 $133.11 -1.47; BNTX×10 yday $110.89 → 09:30 $110.92 +0.30; WYFI×53 yday $21.16 → 09:30 $21.54 +20.14; MRVI×154 yday $8.29 → 09:30 $8.28 -1.54; LZB×34 yday $33.65 → 09:30 $33.63 -0.68; EL×11 yday $96.15 → 09:30 $96.75 +6.60; TEM×18 yday $66.65 → 09:30 $65.60 -18.90; WPM×7 yday $150.25 → 09:30 $154.70 +31.15 | — |
| 2026-08-21 09:30 ET | **SELL** | `BNTX` | 10 | $110.92 | $2.04 | $+14.54 | $1,513.55 | ▲ +14.54 after sell → book $9,333.43; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WYFI` | 53 | $21.54 | $2.17 | $+3.10 | $2,653.00 | ▲ +3.10 after sell → book $9,331.26; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `LZB` | 34 | $33.63 | $2.11 | $-3.52 | $3,794.31 | ▼ -3.52 after sell → book $9,329.15; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 11 | $96.75 | $2.04 | $-11.55 | $4,856.52 | ▼ -11.55 after sell → book $9,327.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 7 | $154.70 | $2.03 | $+67.08 | $5,937.39 | ▲ +67.08 after sell → book $9,325.08; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AAP` | 27 | $42.41 | $2.07 | — | $4,790.24 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-26.1; leftover $1187.48 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 106 | $11.13 | $2.31 | — | $3,608.16 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1187.48 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 11 | $103.69 | $2.02 | — | $2,465.54 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-10.3; leftover $1187.48 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AMRC` | 52 | $22.51 | $2.15 | — | $1,292.88 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-20.2; leftover $1187.48 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 35 | $33.36 | $2.10 | — | $123.18 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $1187.48 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.18 | ▲ close $9,776.64 vs 09:30 $9,335.47 (session +462.21) | 16:00 close · cash $123.18 · equity $9,776.64 vs 09:30 $9,335.47 (+441.17; session marks +462.21) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $133.11 → close $145.13 +84.14; MRVI×154 09:30 $8.28 → close $8.64 +55.44; TEM×18 09:30 $65.60 → close $72.69 +127.62; AAP×27 09:30 $42.41 → close $42.58 +4.59; ARCT×106 09:30 $11.13 → close $13.45 +245.92; WMT×11 09:30 $103.69 → close $103.70 +0.11; AMRC×52 09:30 $22.51 → close $21.38 -58.76; GMAB×35 09:30 $33.36 → close $33.45 +3.15 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.18 | ▼ 09:30 equity $9,677.74 vs yday $9,776.64 (-98.90) | 09:30 open · cash $123.18 (unchanged overnight, no fees) · equity $9,677.74 vs prior close $9,776.64 (-98.90) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $145.13 → 09:30 $142.70 -17.01; MRVI×154 yday $8.64 → 09:30 $8.59 -7.70; TEM×18 yday $72.69 → 09:30 $70.08 -47.07; AAP×27 yday $42.58 → 09:30 $43.05 +12.69; ARCT×106 yday $13.45 → 09:30 $13.33 -12.72; WMT×11 yday $103.70 → 09:30 $104.14 +4.84; AMRC×52 yday $21.38 → 09:30 $21.19 -9.88; GMAB×35 yday $33.45 → 09:30 $32.82 -22.05 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $1,120.05 | ▼ -56.12 after sell → book $9,675.71; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 154 | $8.59 | $2.49 | $+172.16 | $2,440.42 | ▲ +172.16 after sell → book $9,673.22; vs 09:30 mark -2.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TEM` | 18 | $70.08 | $2.06 | $+144.30 | $3,699.71 | ▲ +144.30 after sell → book $9,671.16; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 27 | $43.05 | $2.09 | $+13.12 | $4,859.97 | ▲ +13.12 after sell → book $9,669.07; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 11 | $104.14 | $2.04 | $+0.88 | $6,003.46 | ▲ +0.88 after sell → book $9,667.02; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AMRC` | 52 | $21.19 | $2.17 | $-72.95 | $7,103.18 | ▼ -72.95 after sell → book $9,664.86; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 35 | $32.82 | $2.12 | $-23.11 | $8,249.76 | ▼ -23.11 after sell → book $9,662.74; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,249.76 | ▲ close $9,769.80 vs 09:30 $9,677.74 (session +107.06) | 16:00 close · cash $8,249.76 · equity $9,769.80 vs 09:30 $9,677.74 (+92.06; session marks +107.06) · 1 name(s) marked open→close (per-name table). ARCT×106 09:30 $13.33 → close $14.34 +107.06 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,249.76 | ▼ 09:30 equity $9,746.48 vs yday $9,769.80 (-23.32) | 09:30 open · cash $8,249.76 (unchanged overnight, no fees) · equity $9,746.48 vs prior close $9,769.80 (-23.32) · 1 name(s) re-marked at the open (per-name table). ARCT×106 yday $14.34 → 09:30 $14.12 -23.32 | — |
| 2026-08-25 09:30 ET | **SELL** | `ARCT` | 106 | $14.12 | $2.34 | $+312.29 | $9,744.15 | ▲ +312.29 after sell → book $9,744.15; vs 09:30 mark -2.33 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `ABUS` | 232 | $5.25 | $2.99 | — | $8,523.15 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list mover_buy; 🔵; ⚪; ret5=+10.4; leftover $1218.02 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 191 | $6.37 | $2.56 | — | $7,303.92 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1218.02 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 168 | $7.25 | $2.49 | — | $6,083.43 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1218.02 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 213 | $5.71 | $2.75 | — | $4,864.45 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $1218.02 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 232 | $5.24 | $2.99 | — | $3,645.78 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1218.02 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 63 | $19.04 | $2.18 | — | $2,444.08 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+49.5; leftover $1218.02 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 32 | $36.96 | $2.09 | — | $1,259.27 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1218.02 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `QFIN` | 109 | $11.09 | $2.32 | — | $48.14 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list overnight; 🔵; ret5=-8.0; leftover $1218.02 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.14 | ▲ close $10,152.89 vs 09:30 $9,746.48 (session +429.12) | 16:00 close · cash $48.14 · equity $10,152.89 vs 09:30 $9,746.48 (+406.41; session marks +429.12) · 8 name(s) marked open→close (per-name table). ABUS×232 09:30 $5.25 → close $5.20 -11.60; ZURA×191 09:30 $6.37 → close $6.32 -9.55; CAPR×168 09:30 $7.25 → close $8.29 +174.72; FWDI×213 09:30 $5.71 → close $6.05 +72.42; ALVO×232 09:30 $5.24 → close $5.05 -44.08; ASST×63 09:30 $19.04 → close $21.39 +148.05; LIFE×32 09:30 $36.96 → close $38.56 +51.20; QFIN×109 09:30 $11.09 → close $11.53 +47.96 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.14 | ▼ 09:30 equity $9,835.62 vs yday $10,152.89 (-317.27) | 09:30 open · cash $48.14 (unchanged overnight, no fees) · equity $9,835.62 vs prior close $10,152.89 (-317.27) · 8 name(s) re-marked at the open (per-name table). ABUS×232 yday $5.20 → 09:30 $5.19 -2.32; ZURA×191 yday $6.32 → 09:30 $6.13 -36.29; CAPR×168 yday $8.29 → 09:30 $8.29 +0.00; FWDI×213 yday $6.05 → 09:30 $5.97 -17.04; ALVO×232 yday $5.05 → 09:30 $4.98 -16.24; ASST×63 yday $21.39 → 09:30 $20.72 -42.21; LIFE×32 yday $38.56 → 09:30 $38.24 -10.24; QFIN×109 yday $11.53 → 09:30 $9.76 -192.93 | — |
| 2026-08-26 09:30 ET | **SELL** | `ABUS` | 232 | $5.19 | $3.04 | $-19.95 | $1,249.18 | ▼ -19.95 after sell → book $9,832.58; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 191 | $6.13 | $2.60 | $-51.01 | $2,417.41 | ▼ -51.01 after sell → book $9,829.98; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FWDI` | 213 | $5.97 | $2.79 | $+49.84 | $3,686.22 | ▲ +49.84 after sell → book $9,827.18; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 232 | $4.98 | $3.04 | $-66.35 | $4,838.54 | ▼ -66.35 after sell → book $9,824.14; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 63 | $20.72 | $2.20 | $+101.46 | $6,141.70 | ▲ +101.46 after sell → book $9,821.94; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 32 | $38.24 | $2.11 | $+36.77 | $7,363.28 | ▲ +36.77 after sell → book $9,819.84; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `DKS` | 10 | $121.87 | $2.02 | — | $6,142.56 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-35.1; leftover $1227.21 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BZ` | 73 | $16.77 | $2.21 | — | $4,916.14 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1227.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `MAIR` | 44 | $27.59 | $2.12 | — | $3,700.06 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; ret5=+2.0; leftover $1227.21 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 3 | $326.91 | $2.00 | — | $2,717.33 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=-15.2; leftover $1227.21 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `KURA` | 90 | $13.63 | $2.26 | — | $1,488.37 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+10.2; leftover $1227.21 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SMTC` | 9 | $130.90 | $2.02 | — | $308.25 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-5.7; leftover $1227.21 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $308.25 | ▲ close $10,201.57 vs 09:30 $9,835.62 (session +394.36) | 16:00 close · cash $308.25 · equity $10,201.57 vs 09:30 $9,835.62 (+365.95; session marks +394.36) · 8 name(s) marked open→close (per-name table). CAPR×168 09:30 $8.29 → close $9.36 +179.76; QFIN×109 09:30 $9.76 → close $9.35 -44.69; DKS×10 09:30 $121.87 → close $129.66 +77.90; BZ×73 09:30 $16.77 → close $18.84 +151.11; MAIR×44 09:30 $27.59 → close $28.51 +40.48; DY×3 09:30 $326.91 → close $310.91 -48.00; KURA×90 09:30 $13.63 → close $13.06 -51.30; SMTC×9 09:30 $130.90 → close $140.80 +89.10 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $308.25 | ▲ 09:30 equity $10,239.69 vs yday $10,201.57 (+38.12) | 09:30 open · cash $308.25 (unchanged overnight, no fees) · equity $10,239.69 vs prior close $10,201.57 (+38.12) · 8 name(s) re-marked at the open (per-name table). CAPR×168 yday $9.36 → 09:30 $9.19 -28.56; QFIN×109 yday $9.35 → 09:30 $9.42 +7.63; DKS×10 yday $129.66 → 09:30 $128.73 -9.30; BZ×73 yday $18.84 → 09:30 $18.50 -24.82; MAIR×44 yday $28.51 → 09:30 $28.76 +11.00; DY×3 yday $310.91 → 09:30 $314.90 +11.97; KURA×90 yday $13.06 → 09:30 $12.98 -7.20; SMTC×9 yday $140.80 → 09:30 $149.40 +77.40 | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 168 | $9.19 | $2.53 | $+320.89 | $1,849.64 | ▲ +320.89 after sell → book $10,237.16; vs 09:30 mark -2.53 | dropped from list after 2 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `QFIN` | 109 | $9.42 | $2.35 | $-186.69 | $2,874.07 | ▼ -186.69 after sell → book $10,234.81; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 10 | $128.73 | $2.04 | $+64.54 | $4,159.33 | ▲ +64.54 after sell → book $10,232.77; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 73 | $18.50 | $2.23 | $+121.85 | $5,507.60 | ▲ +121.85 after sell → book $10,230.54; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **SELL** | `MAIR` | 44 | $28.76 | $2.14 | $+47.22 | $6,770.90 | ▲ +47.22 after sell → book $10,228.40; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `DY` | 3 | $314.90 | $2.02 | $-40.05 | $7,713.58 | ▼ -40.05 after sell → book $10,226.38; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `KURA` | 90 | $12.98 | $2.28 | $-63.04 | $8,879.49 | ▼ -63.04 after sell → book $10,224.09; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `SMTC` | 9 | $149.40 | $2.04 | $+162.45 | $10,222.06 | ▲ +162.45 after sell → book $10,222.06; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,222.06 | ▲ close $10,222.06 vs 09:30 $10,239.69 (session +0.00) | 16:00 close · cash $10,222.06 · no lots left · equity $10,222.06. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,222.06 | ▲ 09:30 equity $10,222.06 vs yday $10,222.06 (-0.00) | 09:30 open · cash $10,222.06 · no holdings · equity $10,222.06 vs prior close $10,222.06 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $9,051.48 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1277.76 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 70 | $18.15 | $2.20 | — | $7,778.78 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+14.1; leftover $1277.76 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `QFIN` | 139 | $9.15 | $2.41 | — | $6,504.52 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-19.9; leftover $1277.76 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 80 | $15.88 | $2.23 | — | $5,231.89 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+19.4; leftover $1277.76 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 4 | $306.34 | $2.00 | — | $4,004.53 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-23.0; leftover $1277.76 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GENB` | 81 | $15.77 | $2.23 | — | $2,724.93 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-1.4; leftover $1277.76 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $79.42 | $2.04 | — | $1,452.17 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1277.76 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `JKS` | 95 | $13.37 | $2.27 | — | $179.75 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-14.9; leftover $1277.76 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $179.75 | ▼ close $10,071.94 vs 09:30 $10,222.06 (session -132.72) | 16:00 close · cash $179.75 · equity $10,071.94 vs 09:30 $10,222.06 (-150.12; session marks -132.72) · 8 name(s) marked open→close (per-name table). ANF×8 09:30 $146.07 → close $148.42 +18.80; BZ×70 09:30 $18.15 → close $17.80 -24.50; QFIN×139 09:30 $9.15 → close $8.80 -48.65; BHVN×80 09:30 $15.88 → close $15.41 -37.60; DY×4 09:30 $306.34 → close $294.34 -48.00; GENB×81 09:30 $15.77 → close $15.33 -35.64; URBN×16 09:30 $79.42 → close $81.09 +26.72; JKS×95 09:30 $13.37 → close $13.54 +16.15 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $179.75 | ▼ 09:30 equity $10,051.34 vs yday $10,071.94 (-20.60) | 09:30 open · cash $179.75 (unchanged overnight, no fees) · equity $10,051.34 vs prior close $10,071.94 (-20.60) · 8 name(s) re-marked at the open (per-name table). ANF×8 yday $148.42 → 09:30 $148.03 -3.12; BZ×70 yday $17.80 → 09:30 $17.70 -7.00; QFIN×139 yday $8.80 → 09:30 $8.70 -13.90; BHVN×80 yday $15.41 → 09:30 $15.46 +4.00; DY×4 yday $294.34 → 09:30 $298.01 +14.68; GENB×81 yday $15.33 → 09:30 $15.27 -4.86; URBN×16 yday $81.09 → 09:30 $80.44 -10.40; JKS×95 yday $13.54 → 09:30 $13.54 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.03 | $2.03 | $+11.63 | $1,361.95 | ▲ +11.63 after sell → book $10,049.30; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 70 | $17.70 | $2.22 | $-35.92 | $2,598.73 | ▼ -35.92 after sell → book $10,047.08; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `QFIN` | 139 | $8.70 | $2.44 | $-67.40 | $3,805.59 | ▼ -67.40 after sell → book $10,044.64; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 80 | $15.46 | $2.25 | $-38.08 | $5,040.14 | ▼ -38.08 after sell → book $10,042.39; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 4 | $298.01 | $2.02 | $-37.34 | $6,230.16 | ▼ -37.34 after sell → book $10,040.37; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GENB` | 81 | $15.27 | $2.26 | $-44.99 | $7,464.77 | ▼ -44.99 after sell → book $10,038.11; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $80.44 | $2.06 | $+12.22 | $8,749.75 | ▲ +12.22 after sell → book $10,036.05; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `JKS` | 95 | $13.54 | $2.30 | $+11.57 | $10,033.75 | ▲ +11.57 after sell → book $10,033.75; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,033.75 | ▲ close $10,033.75 vs 09:30 $10,051.34 (session +0.00) | 16:00 close · cash $10,033.75 · no lots left · equity $10,033.75. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,033.75 | ▲ 09:30 equity $10,033.75 vs yday $10,033.75 (-0.00) | 09:30 open · cash $10,033.75 · no holdings · equity $10,033.75 vs prior close $10,033.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,033.75 | ▲ close $10,033.75 vs 09:30 $10,033.75 (session +0.00) | 16:00 close · cash $10,033.75 · no lots left · equity $10,033.75. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,033.75 | ▲ 09:30 equity $10,033.75 vs yday $10,033.75 (-0.00) | 09:30 open · cash $10,033.75 · no holdings · equity $10,033.75 vs prior close $10,033.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,033.75 | ▲ close $10,033.75 vs 09:30 $10,033.75 (session +0.00) | 16:00 close · cash $10,033.75 · no lots left · equity $10,033.75. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,033.75 | ▲ 09:30 equity $10,033.75 vs yday $10,033.75 (-0.00) | 09:30 open · cash $10,033.75 · no holdings · equity $10,033.75 vs prior close $10,033.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 120 | $10.38 | $2.35 | — | $8,786.40 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-56.2; leftover $1254.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 68 | $18.28 | $2.19 | — | $7,541.17 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+16.5; leftover $1254.22 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $6,566.55 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list mover_buy; 🔵; ret5=+6.1; leftover $1254.22 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 91 | $13.71 | $2.26 | — | $5,316.68 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+17.5; leftover $1254.22 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $55.42 | $2.06 | — | $4,095.38 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-25.9; leftover $1254.22 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 52 | $23.88 | $2.15 | — | $2,851.47 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1254.22 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RSKD` | 187 | $6.68 | $2.55 | — | $1,599.76 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+11.4; leftover $1254.22 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 9 | $127.91 | $2.02 | — | $446.56 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1254.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $446.56 | ▲ close $10,175.43 vs 09:30 $10,033.75 (session +159.25) | 16:00 close · cash $446.56 · equity $10,175.43 vs 09:30 $10,033.75 (+141.68; session marks +159.25) · 8 name(s) marked open→close (per-name table). ALMS×120 09:30 $10.38 → close $11.36 +118.20; FRVO×68 09:30 $18.28 → close $17.16 -76.16; DELL×2 09:30 $486.31 → close $516.39 +60.16; CNH×91 09:30 $13.71 → close $13.84 +11.83; EIX×22 09:30 $55.42 → close $56.30 +19.36; MMED×52 09:30 $23.88 → close $23.84 -2.08; RSKD×187 09:30 $6.68 → close $6.93 +46.75; AGCO×9 09:30 $127.91 → close $125.82 -18.81 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $446.56 | ▼ 09:30 equity $10,133.19 vs yday $10,175.43 (-42.24) | 09:30 open · cash $446.56 (unchanged overnight, no fees) · equity $10,133.19 vs prior close $10,175.43 (-42.24) · 8 name(s) re-marked at the open (per-name table). ALMS×120 yday $11.36 → 09:30 $11.23 -15.60; FRVO×68 yday $17.16 → 09:30 $17.27 +7.48; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CNH×91 yday $13.84 → 09:30 $13.89 +4.55; EIX×22 yday $56.30 → 09:30 $55.79 -11.22; MMED×52 yday $23.84 → 09:30 $23.84 +0.00; RSKD×187 yday $6.93 → 09:30 $6.84 -16.83; AGCO×9 yday $125.82 → 09:30 $125.22 -5.40 | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 120 | $11.23 | $2.38 | $+97.87 | $1,791.78 | ▲ +97.87 after sell → book $10,130.81; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 68 | $17.27 | $2.22 | $-73.09 | $2,963.92 | ▼ -73.09 after sell → book $10,128.59; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 91 | $13.89 | $2.29 | $+11.83 | $4,225.62 | ▲ +11.83 after sell → book $10,126.30; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 22 | $55.79 | $2.08 | $+4.01 | $5,450.93 | ▲ +4.01 after sell → book $10,124.23; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 52 | $23.84 | $2.17 | $-6.39 | $6,688.44 | ▼ -6.39 after sell → book $10,122.06; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AGCO` | 9 | $125.22 | $2.04 | $-28.26 | $7,813.38 | ▼ -28.26 after sell → book $10,120.02; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 13 | $98.15 | $2.03 | — | $6,535.40 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=+5.9; leftover $1302.23 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 51 | $25.18 | $2.14 | — | $5,249.08 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+16.0; leftover $1302.23 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `PL` | 66 | $19.64 | $2.19 | — | $3,950.65 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=-13.3; leftover $1302.23 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ZS` | 7 | $166.15 | $2.01 | — | $2,785.59 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=-5.1; leftover $1302.23 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 29 | $44.90 | $2.08 | — | $1,481.42 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=-7.5; leftover $1302.23 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 17 | $75.65 | $2.04 | — | $193.32 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1302.23 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $193.32 | ▼ close $10,031.95 vs 09:30 $10,133.19 (session -75.58) | 16:00 close · cash $193.32 · equity $10,031.95 vs 09:30 $10,133.19 (-101.24; session marks -75.58) · 8 name(s) marked open→close (per-name table). DELL×2 09:30 $513.78 → close $524.14 +20.72; RSKD×187 09:30 $6.84 → close $6.51 -61.71; LULU×13 09:30 $98.15 → close $100.61 +31.98; ASST×51 09:30 $25.18 → close $27.14 +99.96; PL×66 09:30 $19.64 → close $18.12 -100.32; ZS×7 09:30 $166.15 → close $169.80 +25.55; IOT×29 09:30 $44.90 → close $40.20 -136.30; MRX×17 09:30 $75.65 → close $78.27 +44.54 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $193.32 | ▼ 09:30 equity $9,924.55 vs yday $10,031.95 (-107.40) | 09:30 open · cash $193.32 (unchanged overnight, no fees) · equity $9,924.55 vs prior close $10,031.95 (-107.40) · 8 name(s) re-marked at the open (per-name table). DELL×2 yday $524.14 → 09:30 $521.15 -5.98; RSKD×187 yday $6.51 → 09:30 $6.46 -9.35; LULU×13 yday $100.61 → 09:30 $100.58 -0.39; ASST×51 yday $27.14 → 09:30 $26.44 -35.70; PL×66 yday $18.12 → 09:30 $17.85 -17.82; ZS×7 yday $169.80 → 09:30 $165.62 -29.30; IOT×29 yday $40.20 → 09:30 $39.56 -18.56; MRX×17 yday $78.27 → 09:30 $78.84 +9.69 | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+65.67 | $1,233.61 | ▲ +65.67 after sell → book $9,922.53; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `RSKD` | 187 | $6.46 | $2.59 | $-46.28 | $2,439.04 | ▼ -46.28 after sell → book $9,919.94; vs 09:30 mark -2.59 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 13 | $100.58 | $2.05 | $+27.51 | $3,744.53 | ▲ +27.51 after sell → book $9,917.89; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 51 | $26.44 | $2.16 | $+59.95 | $5,090.80 | ▲ +59.95 after sell → book $9,915.73; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `PL` | 66 | $17.85 | $2.21 | $-122.54 | $6,266.69 | ▼ -122.54 after sell → book $9,913.52; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ZS` | 7 | $165.62 | $2.03 | $-7.79 | $7,423.97 | ▼ -7.79 after sell → book $9,911.49; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 29 | $39.56 | $2.10 | $-159.03 | $8,569.11 | ▼ -159.03 after sell → book $9,909.39; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 17 | $78.84 | $2.06 | $+50.13 | $9,907.33 | ▲ +50.13 after sell → book $9,907.33; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,907.33 | ▲ close $9,907.33 vs 09:30 $9,924.55 (session +0.00) | 16:00 close · cash $9,907.33 · no lots left · equity $9,907.33. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,907.33 | ▲ 09:30 equity $9,907.33 vs yday $9,907.33 (-0.00) | 09:30 open · cash $9,907.33 · no holdings · equity $9,907.33 vs prior close $9,907.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,907.33 | ▲ close $9,907.33 vs 09:30 $9,907.33 (session +0.00) | 16:00 close · cash $9,907.33 · no lots left · equity $9,907.33. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,907.33 | ▲ 09:30 equity $9,907.33 vs yday $9,907.33 (-0.00) | 09:30 open · cash $9,907.33 · no holdings · equity $9,907.33 vs prior close $9,907.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,907.33 | ▲ close $9,907.33 vs 09:30 $9,907.33 (session +0.00) | 16:00 close · cash $9,907.33 · no lots left · equity $9,907.33. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,907.33 | ▲ 09:30 equity $9,907.33 vs yday $9,907.33 (-0.00) | 09:30 open · cash $9,907.33 · no holdings · equity $9,907.33 vs prior close $9,907.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `BKV` | 49 | $24.97 | $2.14 | — | $8,681.66 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; ret5=+10.8; leftover $1238.42 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 22 | $54.66 | $2.06 | — | $7,477.09 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-22.3; leftover $1238.42 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AEO` | 84 | $14.71 | $2.24 | — | $6,239.20 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-12.8; leftover $1238.42 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 113 | $10.95 | $2.33 | — | $4,999.53 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1238.42 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `NAVN` | 60 | $20.61 | $2.17 | — | $3,760.76 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-24.7; leftover $1238.42 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `TSSI` | 137 | $8.98 | $2.40 | — | $2,528.09 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+14.1; leftover $1238.42 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 201 | $6.16 | $2.60 | — | $1,287.34 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+36.4; leftover $1238.42 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AXGN` | 29 | $42.48 | $2.08 | — | $53.34 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-13.4; leftover $1238.42 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.34 | ▼ close $9,782.54 vs 09:30 $9,907.33 (session -106.78) | 16:00 close · cash $53.34 · equity $9,782.54 vs 09:30 $9,907.33 (-124.79; session marks -106.78) · 8 name(s) marked open→close (per-name table). BKV×49 09:30 $24.97 → close $24.23 -36.26; COO×22 09:30 $54.66 → close $53.91 -16.50; AEO×84 09:30 $14.71 → close $15.02 +26.04; WLTH×113 09:30 $10.95 → close $10.38 -64.41; NAVN×60 09:30 $20.61 → close $21.02 +24.60; TSSI×137 09:30 $8.98 → close $8.93 -6.85; IRD×201 09:30 $6.16 → close $6.04 -24.12; AXGN×29 09:30 $42.48 → close $42.16 -9.28 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.34 | ▼ 09:30 equity $9,712.47 vs yday $9,782.54 (-70.07) | 09:30 open · cash $53.34 (unchanged overnight, no fees) · equity $9,712.47 vs prior close $9,782.54 (-70.07) · 8 name(s) re-marked at the open (per-name table). BKV×49 yday $24.23 → 09:30 $24.26 +1.47; COO×22 yday $53.91 → 09:30 $54.78 +19.14; AEO×84 yday $15.02 → 09:30 $14.85 -14.28; WLTH×113 yday $10.38 → 09:30 $10.29 -10.17; NAVN×60 yday $21.02 → 09:30 $21.10 +4.80; TSSI×137 yday $8.93 → 09:30 $8.57 -49.32; IRD×201 yday $6.04 → 09:30 $6.02 -4.02; AXGN×29 yday $42.16 → 09:30 $41.55 -17.69 | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 22 | $54.78 | $2.08 | $-1.49 | $1,256.42 | ▼ -1.49 after sell → book $9,710.39; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AEO` | 84 | $14.85 | $2.27 | $+7.25 | $2,501.56 | ▲ +7.25 after sell → book $9,708.13; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 113 | $10.29 | $2.36 | $-79.27 | $3,661.97 | ▼ -79.27 after sell → book $9,705.77; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `NAVN` | 60 | $21.10 | $2.19 | $+25.04 | $4,925.78 | ▲ +25.04 after sell → book $9,703.58; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `TSSI` | 137 | $8.57 | $2.43 | $-61.00 | $6,097.44 | ▼ -61.00 after sell → book $9,701.15; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 201 | $6.02 | $2.64 | $-33.38 | $7,304.81 | ▼ -33.38 after sell → book $9,698.50; vs 09:30 mark -2.65 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AXGN` | 29 | $41.55 | $2.10 | $-31.14 | $8,507.67 | ▼ -31.14 after sell → book $9,696.41; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,507.67 | ▼ close $9,674.85 vs 09:30 $9,712.47 (session -21.56) | 16:00 close · cash $8,507.67 · equity $9,674.85 vs 09:30 $9,712.47 (-37.62; session marks -21.56) · 1 name(s) marked open→close (per-name table). BKV×49 09:30 $24.26 → close $23.82 -21.56 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,507.67 | ▲ 09:30 equity $9,695.92 vs yday $9,674.85 (+21.07) | 09:30 open · cash $8,507.67 (unchanged overnight, no fees) · equity $9,695.92 vs prior close $9,674.85 (+21.07) · 1 name(s) re-marked at the open (per-name table). BKV×49 yday $23.82 → 09:30 $24.25 +21.07 | — |
| 2026-09-15 09:30 ET | **SELL** | `BKV` | 49 | $24.25 | $2.16 | $-39.57 | $9,693.76 | ▼ -39.57 after sell → book $9,693.76; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | join🟢 sector🔴 gen🔴 news🔴 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,693.76 | ▲ close $9,693.76 vs 09:30 $9,695.92 (session +0.00) | 16:00 close · cash $9,693.76 · no lots left · equity $9,693.76. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,693.76 | ▲ 09:30 equity $9,693.76 vs yday $9,693.76 (+0.00) | 09:30 open · cash $9,693.76 · no holdings · equity $9,693.76 vs prior close $9,693.76 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `PLAY` | 176 | $6.86 | $2.52 | — | $8,483.88 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-22.4; leftover $1211.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ALHC` | 117 | $10.30 | $2.34 | — | $7,276.44 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-23.0; leftover $1211.72 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 36 | $33.14 | $2.10 | — | $6,081.30 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=-2.9; leftover $1211.72 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `GFR` | 177 | $6.83 | $2.52 | — | $4,869.87 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; ret5=+11.2; leftover $1211.72 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `HQ` | 94 | $12.89 | $2.27 | — | $3,655.94 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=-18.2; leftover $1211.72 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 52 | $23.29 | $2.15 | — | $2,442.71 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+16.1; leftover $1211.72 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `DMRA` | 48 | $24.88 | $2.13 | — | $1,246.34 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-15.2; leftover $1211.72 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RVTY` | 8 | $140.88 | $2.01 | — | $117.29 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,ohlc_hot; 🔵; ret5=+10.3; leftover $1211.72 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $117.29 | ▼ close $9,608.97 vs 09:30 $9,693.76 (session -66.75) | 16:00 close · cash $117.29 · equity $9,608.97 vs 09:30 $9,693.76 (-84.79; session marks -66.75) · 8 name(s) marked open→close (per-name table). PLAY×176 09:30 $6.86 → close $6.86 +0.00; ALHC×117 09:30 $10.30 → close $8.71 -186.03; FPS×36 09:30 $33.14 → close $34.84 +61.20; GFR×177 09:30 $6.83 → close $6.49 -60.18; HQ×94 09:30 $12.89 → close $13.56 +62.98; SDGR×52 09:30 $23.29 → close $23.93 +33.28; DMRA×48 09:30 $24.88 → close $24.53 -16.80; RVTY×8 09:30 $140.88 → close $145.73 +38.80 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $117.29 | ▲ 09:30 equity $9,722.71 vs yday $9,608.97 (+113.74) | 09:30 open · cash $117.29 (unchanged overnight, no fees) · equity $9,722.71 vs prior close $9,608.97 (+113.74) · 8 name(s) re-marked at the open (per-name table). PLAY×176 yday $6.86 → 09:30 $6.96 +17.60; ALHC×117 yday $8.71 → 09:30 $8.58 -15.21; FPS×36 yday $34.84 → 09:30 $36.76 +69.12; GFR×177 yday $6.49 → 09:30 $6.48 -1.77; HQ×94 yday $13.56 → 09:30 $13.56 +0.00; SDGR×52 yday $23.93 → 09:30 $24.09 +8.32; DMRA×48 yday $24.53 → 09:30 $24.96 +20.64; RVTY×8 yday $145.73 → 09:30 $147.61 +15.04 | — |
| 2026-09-17 09:30 ET | **SELL** | `PLAY` | 176 | $6.96 | $2.56 | $+12.52 | $1,339.69 | ▲ +12.52 after sell → book $9,720.15; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `GFR` | 177 | $6.48 | $2.56 | $-67.03 | $2,484.09 | ▼ -67.03 after sell → book $9,717.59; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `HQ` | 94 | $13.56 | $2.30 | $+58.41 | $3,756.43 | ▲ +58.41 after sell → book $9,715.29; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 52 | $24.09 | $2.17 | $+37.29 | $5,006.95 | ▲ +37.29 after sell → book $9,713.13; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `DMRA` | 48 | $24.96 | $2.15 | $-0.45 | $6,202.87 | ▼ -0.45 after sell → book $9,710.97; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RVTY` | 8 | $147.61 | $2.03 | $+49.79 | $7,381.72 | ▲ +49.79 after sell → book $9,708.94; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 54 | $22.46 | $2.15 | — | $6,166.73 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+27.3; leftover $1230.29 | join🟢 sector🟢 gen🟢 news🔴 digest🔴 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 5 | $238.60 | $2.00 | — | $4,971.72 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-11.6; leftover $1230.29 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 109 | $11.21 | $2.32 | — | $3,747.51 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=+1.0; leftover $1230.29 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 47 | $25.95 | $2.13 | — | $2,525.73 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1230.29 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AMRX` | 66 | $18.56 | $2.19 | — | $1,298.58 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+4.8; leftover $1230.29 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `BTGO` | 187 | $6.56 | $2.55 | — | $69.31 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-14.6; leftover $1230.29 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $69.31 | ▲ close $9,771.22 vs 09:30 $9,722.71 (session +75.62) | 16:00 close · cash $69.31 · equity $9,771.22 vs 09:30 $9,722.71 (+48.51; session marks +75.62) · 8 name(s) marked open→close (per-name table). ALHC×117 09:30 $8.58 → close $8.70 +14.04; FPS×36 09:30 $36.76 → close $38.06 +46.80; BBNX×54 09:30 $22.46 → close $21.43 -55.62; JBHT×5 09:30 $238.60 → close $236.80 -9.00; ALMU×109 09:30 $11.21 → close $11.54 +36.51; ARQT×47 09:30 $25.95 → close $26.46 +23.97; AMRX×66 09:30 $18.56 → close $18.28 -18.48; BTGO×187 09:30 $6.56 → close $6.76 +37.40 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.31 | ▲ 09:30 equity $9,832.11 vs yday $9,771.22 (+60.89) | 09:30 open · cash $69.31 (unchanged overnight, no fees) · equity $9,832.11 vs prior close $9,771.22 (+60.89) · 8 name(s) re-marked at the open (per-name table). ALHC×117 yday $8.70 → 09:30 $8.68 -2.34; FPS×36 yday $38.06 → 09:30 $39.50 +51.84; BBNX×54 yday $21.43 → 09:30 $21.30 -7.02; JBHT×5 yday $236.80 → 09:30 $236.80 +0.00; ALMU×109 yday $11.54 → 09:30 $11.64 +10.36; ARQT×47 yday $26.46 → 09:30 $26.14 -15.04; AMRX×66 yday $18.28 → 09:30 $18.12 -10.56; BTGO×187 yday $6.76 → 09:30 $6.94 +33.66 | — |
| 2026-09-18 09:30 ET | **SELL** | `ALHC` | 117 | $8.68 | $2.37 | $-194.25 | $1,082.50 | ▼ -194.25 after sell → book $9,829.74; vs 09:30 mark -2.37 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 36 | $39.50 | $2.12 | $+224.74 | $2,502.38 | ▲ +224.74 after sell → book $9,827.62; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 54 | $21.30 | $2.17 | $-66.96 | $3,650.41 | ▼ -66.96 after sell → book $9,825.45; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 5 | $236.80 | $2.02 | $-13.03 | $4,832.39 | ▼ -13.03 after sell → book $9,823.43; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMRX` | 66 | $18.12 | $2.21 | $-33.44 | $6,026.10 | ▼ -33.44 after sell → book $9,821.22; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BTGO` | 187 | $6.94 | $2.59 | $+65.92 | $7,321.28 | ▲ +65.92 after sell → book $9,818.62; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 5 | $209.52 | $2.00 | — | $6,271.68 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1220.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 41 | $29.32 | $2.11 | — | $5,067.45 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $1220.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 161 | $7.54 | $2.47 | — | $3,851.84 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-20.9; leftover $1220.21 | join🟡 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 82 | $14.79 | $2.24 | — | $2,636.82 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1220.21 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `SECZ` | 130 | $9.32 | $2.38 | — | $1,422.84 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+11.1; leftover $1220.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `USDE` | 127 | $9.54 | $2.37 | — | $208.89 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+15.8; leftover $1220.21 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $208.89 | ▲ close $10,090.07 vs 09:30 $9,832.11 (session +285.02) | 16:00 close · cash $208.89 · equity $10,090.07 vs 09:30 $9,832.11 (+257.96; session marks +285.02) · 8 name(s) marked open→close (per-name table). ALMU×109 09:30 $11.64 → close $12.72 +118.26; ARQT×47 09:30 $26.14 → close $25.38 -35.72; GNRC×5 09:30 $209.52 → close $207.44 -10.40; SDGR×41 09:30 $29.32 → close $29.02 -12.30; FLNC×161 09:30 $7.54 → close $7.32 -34.61; RARE×82 09:30 $14.79 → close $14.51 -22.96; SECZ×130 09:30 $9.32 → close $10.86 +200.20; USDE×127 09:30 $9.54 → close $10.19 +82.55 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $208.89 | ▲ 09:30 equity $10,652.91 vs yday $10,090.07 (+562.84) | 09:30 open · cash $208.89 (unchanged overnight, no fees) · equity $10,652.91 vs prior close $10,090.07 (+562.84) · 8 name(s) re-marked at the open (per-name table). ALMU×109 yday $12.72 → 09:30 $13.12 +43.60; ARQT×47 yday $25.38 → 09:30 $25.57 +8.93; GNRC×5 yday $207.44 → 09:30 $210.00 +12.80; SDGR×41 yday $29.02 → 09:30 $29.43 +16.81; FLNC×161 yday $7.32 → 09:30 $7.36 +6.44; RARE×82 yday $14.51 → 09:30 $14.58 +5.74; SECZ×130 yday $10.86 → 09:30 $11.67 +105.30; USDE×127 yday $10.19 → 09:30 $13.05 +363.22 | — |
| 2026-09-21 09:30 ET | **SELL** | `ALMU` | 109 | $13.12 | $2.35 | $+204.07 | $1,637.17 | ▲ +204.07 after sell → book $10,650.56; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARQT` | 47 | $25.57 | $2.15 | $-22.14 | $2,836.81 | ▼ -22.14 after sell → book $10,648.41; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 5 | $210.00 | $2.02 | $-1.63 | $3,884.78 | ▼ -1.63 after sell → book $10,646.38; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 41 | $29.43 | $2.13 | $+0.26 | $5,089.28 | ▲ +0.26 after sell → book $10,644.25; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 161 | $7.36 | $2.51 | $-33.16 | $6,271.73 | ▼ -33.16 after sell → book $10,641.74; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 82 | $14.58 | $2.26 | $-21.72 | $7,465.03 | ▼ -21.72 after sell → book $10,639.48; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SECZ` | 130 | $11.67 | $2.41 | $+300.71 | $8,979.72 | ▲ +300.71 after sell → book $10,637.07; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `USDE` | 127 | $13.05 | $2.41 | $+440.99 | $10,634.66 | ▲ +440.99 after sell → book $10,634.66; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,634.66 | ▲ close $10,634.66 vs 09:30 $10,652.91 (session +0.00) | 16:00 close · cash $10,634.66 · no lots left · equity $10,634.66. | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,634.66 | ▲ 09:30 equity $10,634.66 vs yday $10,634.66 (+0.00) | 09:30 open · cash $10,634.66 · no holdings · equity $10,634.66 vs prior close $10,634.66 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,634.66 | ▲ close $10,634.66 vs 09:30 $10,634.66 (session +0.00) | 16:00 close · cash $10,634.66 · no lots left · equity $10,634.66. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OCUL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZIM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `EYPT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `FN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `USDE` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ASST` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PAAS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ARCT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PCG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MNSO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `REAX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AGCO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ENOV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CNH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUNB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `RARE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `DPRO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `PURR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `DYN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CHA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `IONS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `WLTH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TTAN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TYRA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `EVMN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SMR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NMAX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `RUM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `DBI` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `HQ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
