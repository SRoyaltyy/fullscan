# Factor mine action — `oppset_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `oppset` · top 8 · rank `opp_rvol` · size `leftover` · sell `list` · S-boost `none` · Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP)

Cash book **-4.37%** ($9,563) · signal-only (no cash/fees) was +3.59%. Starts YES **1/26**. Fills 182 · skips 88 · realized $-141.34.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $263.94.

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
| 2026-08-14 | `YETI` | 27 | — | $45.51 | +0.00 | $44.56 | -25.65 | -25.65 | +0.00 | -25.65 |
| 2026-08-14 | `AVAH` | 104 | — | $11.91 | +0.00 | $12.32 | +42.64 | +42.64 | +0.00 | +42.64 |
| 2026-08-14 | `CRMD` | 155 | — | $8.05 | +0.00 | $7.54 | -79.05 | -79.05 | +0.00 | -79.05 |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | $19.54 | -1.89 | -2.52 | +0.00 | -1.89 |
| 2026-08-17 | `CLBT` | 115 | $11.14 | $11.19 | +5.75 | — | +0.00 | +5.75 | +41.40 | — |
| 2026-08-17 | `OMER` | 72 | $17.19 | $17.17 | -1.44 | — | +0.00 | -1.44 | -12.96 | — |
| 2026-08-17 | `AIRO` | 112 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -173.60 | — |
| 2026-08-17 | `SECZ` | 214 | $5.61 | $5.45 | -34.24 | — | +0.00 | -34.24 | -83.46 | — |
| 2026-08-17 | `YETI` | 27 | $44.56 | $43.80 | -20.52 | — | +0.00 | -20.52 | -46.17 | — |
| 2026-08-17 | `AVAH` | 104 | $12.32 | $12.21 | -11.44 | — | +0.00 | -11.44 | +31.20 | — |
| 2026-08-17 | `CRMD` | 155 | $7.54 | $7.55 | +1.55 | — | +0.00 | +1.55 | -77.50 | — |
| 2026-08-17 | `CAPR` | 174 | — | $6.87 | +0.00 | $7.45 | +100.92 | +100.92 | +0.00 | +100.92 |
| 2026-08-17 | `HTFL` | 29 | — | $41.23 | +0.00 | $41.94 | +20.59 | +20.59 | +0.00 | +20.59 |
| 2026-08-17 | `BIRK` | 30 | — | $39.48 | +0.00 | $37.86 | -48.60 | -48.60 | +0.00 | -48.60 |
| 2026-08-17 | `NMAX` | 109 | — | $10.97 | +0.00 | $10.36 | -66.49 | -66.49 | +0.00 | -66.49 |
| 2026-08-17 | `CHEF` | 11 | — | $108.18 | +0.00 | $110.57 | +26.29 | +26.29 | +0.00 | +26.29 |
| 2026-08-17 | `VIV` | 104 | — | $11.55 | +0.00 | $11.40 | -15.60 | -15.60 | +0.00 | -15.60 |
| 2026-08-17 | `ABEO` | 205 | — | $5.86 | +0.00 | $5.94 | +16.40 | +16.40 | +0.00 | +16.40 |
| 2026-08-18 | `ARX` | 63 | $19.54 | $19.57 | +1.89 | — | +0.00 | +1.89 | +0.00 | — |
| 2026-08-18 | `CAPR` | 174 | $7.45 | $7.50 | +8.70 | $7.08 | -73.08 | -64.38 | +109.62 | +36.54 |
| 2026-08-18 | `HTFL` | 29 | $41.94 | $41.50 | -12.76 | — | +0.00 | -12.76 | +7.83 | — |
| 2026-08-18 | `BIRK` | 30 | $37.86 | $38.07 | +6.30 | — | +0.00 | +6.30 | -42.30 | — |
| 2026-08-18 | `NMAX` | 109 | $10.36 | $10.31 | -5.45 | — | +0.00 | -5.45 | -71.94 | — |
| 2026-08-18 | `CHEF` | 11 | $110.57 | $110.91 | +3.74 | — | +0.00 | +3.74 | +30.03 | — |
| 2026-08-18 | `VIV` | 104 | $11.40 | $11.45 | +5.20 | — | +0.00 | +5.20 | -10.40 | — |
| 2026-08-18 | `ABEO` | 205 | $5.94 | $5.90 | -8.20 | — | +0.00 | -8.20 | +8.20 | — |
| 2026-08-19 | `CAPR` | 174 | $7.08 | $7.19 | +19.14 | — | +0.00 | +19.14 | +55.68 | — |
| 2026-08-20 | `MRNA` | 7 | — | $150.14 | +0.00 | $133.32 | -117.74 | -117.74 | +0.00 | -117.74 |
| 2026-08-20 | `WBS` | 15 | — | $77.57 | +0.00 | $77.57 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `ALH` | 50 | — | $23.72 | +0.00 | $23.18 | -27.00 | -27.00 | +0.00 | -27.00 |
| 2026-08-20 | `LBRDK` | 33 | — | $36.02 | +0.00 | $36.02 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `SUI` | 9 | — | $121.21 | +0.00 | $122.29 | +9.72 | +9.72 | +0.00 | +9.72 |
| 2026-08-20 | `NTST` | 58 | — | $20.47 | +0.00 | $20.61 | +8.12 | +8.12 | +0.00 | +8.12 |
| 2026-08-20 | `BNTX` | 10 | — | $109.06 | +0.00 | $110.89 | +18.30 | +18.30 | +0.00 | +18.30 |
| 2026-08-20 | `ADC` | 16 | — | $74.37 | +0.00 | $74.45 | +1.28 | +1.28 | +0.00 | +1.28 |
| 2026-08-21 | `MRNA` | 7 | $133.32 | $133.11 | -1.47 | $145.13 | +84.14 | +82.67 | -119.21 | -35.07 |
| 2026-08-21 | `WBS` | 15 | $77.57 | $77.57 | +0.00 | — | +0.00 | +0.00 | +0.00 | — |
| 2026-08-21 | `ALH` | 50 | $23.18 | $23.33 | +7.50 | $23.47 | +7.00 | +14.50 | -19.50 | -12.50 |
| 2026-08-21 | `LBRDK` | 33 | $36.02 | $36.02 | +0.00 | $36.02 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-21 | `SUI` | 9 | $122.29 | $122.41 | +1.08 | — | +0.00 | +1.08 | +10.80 | — |
| 2026-08-21 | `NTST` | 58 | $20.61 | $20.66 | +2.90 | — | +0.00 | +2.90 | +11.02 | — |
| 2026-08-21 | `BNTX` | 10 | $110.89 | $110.92 | +0.30 | — | +0.00 | +0.30 | +18.60 | — |
| 2026-08-21 | `ADC` | 16 | $74.45 | $74.60 | +2.40 | — | +0.00 | +2.40 | +3.68 | — |
| 2026-08-21 | `AAP` | 24 | — | $42.41 | +0.00 | $42.58 | +4.08 | +4.08 | +0.00 | +4.08 |
| 2026-08-21 | `MFC` | 24 | — | $42.48 | +0.00 | $42.51 | +0.72 | +0.72 | +0.00 | +0.72 |
| 2026-08-21 | `MRVI` | 124 | — | $8.28 | +0.00 | $8.64 | +44.64 | +44.64 | +0.00 | +44.64 |
| 2026-08-21 | `BULL` | 114 | — | $8.99 | +0.00 | $8.78 | -23.94 | -23.94 | +0.00 | -23.94 |
| 2026-08-21 | `SGRY` | 73 | — | $14.10 | +0.00 | $14.52 | +30.66 | +30.66 | +0.00 | +30.66 |
| 2026-08-21 | `ARCT` | 92 | — | $11.13 | +0.00 | $13.45 | +213.44 | +213.44 | +0.00 | +213.44 |
| 2026-08-24 | `MRNA` | 7 | $145.13 | $142.70 | -17.01 | $138.89 | -26.67 | -43.68 | -52.08 | -78.75 |
| 2026-08-24 | `ALH` | 50 | $23.47 | $23.66 | +9.50 | — | +0.00 | +9.50 | -3.00 | — |
| 2026-08-24 | `LBRDK` | 33 | $36.02 | $36.02 | +0.00 | $36.02 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-24 | `AAP` | 24 | $42.58 | $43.05 | +11.28 | — | +0.00 | +11.28 | +15.36 | — |
| 2026-08-24 | `MFC` | 24 | $42.51 | $42.31 | -4.80 | — | +0.00 | -4.80 | -4.08 | — |
| 2026-08-24 | `MRVI` | 124 | $8.64 | $8.59 | -6.20 | — | +0.00 | -6.20 | +38.44 | — |
| 2026-08-24 | `BULL` | 114 | $8.78 | $8.58 | -22.80 | — | +0.00 | -22.80 | -46.74 | — |
| 2026-08-24 | `SGRY` | 73 | $14.52 | $14.55 | +2.19 | — | +0.00 | +2.19 | +32.85 | — |
| 2026-08-24 | `ARCT` | 92 | $13.45 | $13.33 | -11.04 | $14.34 | +92.92 | +81.88 | +202.40 | +295.32 |
| 2026-08-25 | `MRNA` | 7 | $138.89 | $143.50 | +32.27 | — | +0.00 | +32.27 | -46.48 | — |
| 2026-08-25 | `LBRDK` | 33 | $36.02 | $36.02 | +0.00 | $36.02 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `ARCT` | 92 | $14.34 | $14.12 | -20.24 | $15.44 | +121.44 | +101.20 | +275.08 | +396.52 |
| 2026-08-25 | `ABUS` | 199 | — | $5.25 | +0.00 | $5.20 | -9.95 | -9.95 | +0.00 | -9.95 |
| 2026-08-25 | `ZURA` | 164 | — | $6.37 | +0.00 | $6.32 | -8.20 | -8.20 | +0.00 | -8.20 |
| 2026-08-25 | `ALH` | 43 | — | $24.16 | +0.00 | $23.85 | -13.33 | -13.33 | +0.00 | -13.33 |
| 2026-08-25 | `CAPR` | 144 | — | $7.25 | +0.00 | $8.29 | +149.76 | +149.76 | +0.00 | +149.76 |
| 2026-08-25 | `FWDI` | 183 | — | $5.71 | +0.00 | $6.05 | +62.22 | +62.22 | +0.00 | +62.22 |
| 2026-08-25 | `XPEV` | 93 | — | $11.19 | +0.00 | $11.60 | +38.59 | +38.59 | +0.00 | +38.59 |
| 2026-08-26 | `LBRDK` | 33 | $36.02 | $36.02 | +0.00 | $36.02 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `ARCT` | 92 | $15.44 | $15.35 | -8.28 | — | +0.00 | -8.28 | +388.24 | — |
| 2026-08-26 | `ABUS` | 199 | $5.20 | $5.19 | -1.99 | — | +0.00 | -1.99 | -11.94 | — |
| 2026-08-26 | `ZURA` | 164 | $6.32 | $6.13 | -31.16 | — | +0.00 | -31.16 | -39.36 | — |
| 2026-08-26 | `ALH` | 43 | $23.85 | $24.00 | +6.45 | — | +0.00 | +6.45 | -6.88 | — |
| 2026-08-26 | `CAPR` | 144 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | +149.76 | — |
| 2026-08-26 | `FWDI` | 183 | $6.05 | $5.97 | -14.64 | — | +0.00 | -14.64 | +47.58 | — |
| 2026-08-26 | `XPEV` | 93 | $11.60 | $11.90 | +27.90 | — | +0.00 | +27.90 | +66.49 | — |
| 2026-08-26 | `DKS` | 9 | — | $121.87 | +0.00 | $129.66 | +70.11 | +70.11 | +0.00 | +70.11 |
| 2026-08-26 | `SLF` | 14 | — | $79.20 | +0.00 | $79.05 | -2.10 | -2.10 | +0.00 | -2.10 |
| 2026-08-26 | `QFIN` | 114 | — | $9.76 | +0.00 | $9.35 | -46.74 | -46.74 | +0.00 | -46.74 |
| 2026-08-26 | `GENB` | 55 | — | $20.00 | +0.00 | $16.14 | -212.30 | -212.30 | +0.00 | -212.30 |
| 2026-08-26 | `VIPS` | 79 | — | $14.00 | +0.00 | $14.08 | +6.32 | +6.32 | +0.00 | +6.32 |
| 2026-08-26 | `BZ` | 66 | — | $16.77 | +0.00 | $18.84 | +136.62 | +136.62 | +0.00 | +136.62 |
| 2026-08-26 | `MRNA` | 7 | — | $154.20 | +0.00 | $149.66 | -31.78 | -31.78 | +0.00 | -31.78 |
| 2026-08-26 | `ATHM` | 51 | — | $21.74 | +0.00 | $22.17 | +21.93 | +21.93 | +0.00 | +21.93 |
| 2026-08-27 | `LBRDK` | 33 | $36.02 | $36.02 | +0.00 | $36.02 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-27 | `DKS` | 9 | $129.66 | $128.73 | -8.37 | — | +0.00 | -8.37 | +61.74 | — |
| 2026-08-27 | `SLF` | 14 | $79.05 | $78.45 | -8.40 | — | +0.00 | -8.40 | -10.50 | — |
| 2026-08-27 | `QFIN` | 114 | $9.35 | $9.42 | +7.98 | — | +0.00 | +7.98 | -38.76 | — |
| 2026-08-27 | `GENB` | 55 | $16.14 | $17.11 | +53.35 | — | +0.00 | +53.35 | -158.95 | — |
| 2026-08-27 | `VIPS` | 79 | $14.08 | $14.00 | -6.32 | — | +0.00 | -6.32 | +0.00 | — |
| 2026-08-27 | `BZ` | 66 | $18.84 | $18.50 | -22.44 | — | +0.00 | -22.44 | +114.18 | — |
| 2026-08-27 | `MRNA` | 7 | $149.66 | $144.18 | -38.36 | — | +0.00 | -38.36 | -70.14 | — |
| 2026-08-27 | `ATHM` | 51 | $22.17 | $22.08 | -4.59 | — | +0.00 | -4.59 | +17.34 | — |
| 2026-08-28 | `LBRDK` | 33 | $36.02 | $36.02 | +0.00 | — | +0.00 | +0.00 | +0.00 | — |
| 2026-08-28 | `ANF` | 8 | — | $146.07 | +0.00 | $148.42 | +18.80 | +18.80 | +0.00 | +18.80 |
| 2026-08-28 | `DKS` | 9 | — | $132.80 | +0.00 | $135.09 | +20.61 | +20.61 | +0.00 | +20.61 |
| 2026-08-28 | `BZ` | 68 | — | $18.15 | +0.00 | $17.80 | -23.80 | -23.80 | +0.00 | -23.80 |
| 2026-08-28 | `QFIN` | 136 | — | $9.15 | +0.00 | $8.80 | -47.60 | -47.60 | +0.00 | -47.60 |
| 2026-08-28 | `LEG` | 135 | — | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-28 | `BHVN` | 78 | — | $15.88 | +0.00 | $15.41 | -36.66 | -36.66 | +0.00 | -36.66 |
| 2026-08-28 | `PLAB` | 41 | — | $30.01 | +0.00 | $27.73 | -93.48 | -93.48 | +0.00 | -93.48 |
| 2026-08-28 | `DY` | 4 | — | $306.34 | +0.00 | $294.34 | -48.00 | -48.00 | +0.00 | -48.00 |
| 2026-08-31 | `ANF` | 8 | $148.42 | $148.03 | -3.12 | — | +0.00 | -3.12 | +15.68 | — |
| 2026-08-31 | `DKS` | 9 | $135.09 | $136.75 | +14.94 | — | +0.00 | +14.94 | +35.55 | — |
| 2026-08-31 | `BZ` | 68 | $17.80 | $17.70 | -6.80 | — | +0.00 | -6.80 | -30.60 | — |
| 2026-08-31 | `QFIN` | 136 | $8.80 | $8.70 | -13.60 | — | +0.00 | -13.60 | -61.20 | — |
| 2026-08-31 | `LEG` | 135 | $9.20 | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-31 | `BHVN` | 78 | $15.41 | $15.46 | +3.90 | — | +0.00 | +3.90 | -32.76 | — |
| 2026-08-31 | `PLAB` | 41 | $27.73 | $28.04 | +12.71 | — | +0.00 | +12.71 | -80.77 | — |
| 2026-08-31 | `DY` | 4 | $294.34 | $298.01 | +14.68 | — | +0.00 | +14.68 | -33.32 | — |
| 2026-09-01 | `LEG` | 135 | $9.20 | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-02 | `LEG` | 135 | $9.20 | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `LEG` | 135 | $9.20 | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `GBTG` | 112 | — | $9.49 | +0.00 | $9.49 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `ALMS` | 102 | — | $10.38 | +0.00 | $11.36 | +100.47 | +100.47 | +0.00 | +100.47 |
| 2026-09-03 | `DAKT` | 55 | — | $19.08 | +0.00 | $19.48 | +22.00 | +22.00 | +0.00 | +22.00 |
| 2026-09-03 | `GTLB` | 21 | — | $49.98 | +0.00 | $49.31 | -14.07 | -14.07 | +0.00 | -14.07 |
| 2026-09-03 | `FRVO` | 58 | — | $18.28 | +0.00 | $17.16 | -64.96 | -64.96 | +0.00 | -64.96 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `MDB` | 2 | — | $378.76 | +0.00 | $384.45 | +11.38 | +11.38 | +0.00 | +11.38 |
| 2026-09-03 | `OABI` | 210 | — | $5.08 | +0.00 | $4.75 | -69.30 | -69.30 | +0.00 | -69.30 |
| 2026-09-04 | `LEG` | 135 | $9.20 | $9.20 | +0.00 | — | +0.00 | +0.00 | +0.00 | — |
| 2026-09-04 | `GBTG` | 112 | $9.49 | $9.49 | +0.00 | — | +0.00 | +0.00 | +0.00 | — |
| 2026-09-04 | `ALMS` | 102 | $11.36 | $11.23 | -13.26 | $11.10 | -13.26 | -26.52 | +87.21 | +73.95 |
| 2026-09-04 | `DAKT` | 55 | $19.48 | $19.47 | -0.55 | — | +0.00 | -0.55 | +21.45 | — |
| 2026-09-04 | `GTLB` | 21 | $49.31 | $48.94 | -7.77 | — | +0.00 | -7.77 | -21.84 | — |
| 2026-09-04 | `FRVO` | 58 | $17.16 | $17.27 | +6.38 | — | +0.00 | +6.38 | -58.58 | — |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | — | +0.00 | -5.22 | +54.94 | — |
| 2026-09-04 | `MDB` | 2 | $384.45 | $378.34 | -12.22 | — | +0.00 | -12.22 | -0.84 | — |
| 2026-09-04 | `OABI` | 210 | $4.75 | $4.78 | +6.30 | — | +0.00 | +6.30 | -63.00 | — |
| 2026-09-04 | `CHPT` | 132 | — | $9.28 | +0.00 | $9.89 | +80.52 | +80.52 | +0.00 | +80.52 |
| 2026-09-04 | `RARE` | 79 | — | $15.47 | +0.00 | $15.30 | -13.82 | -13.82 | +0.00 | -13.82 |
| 2026-09-04 | `DPRO` | 193 | — | $6.36 | +0.00 | $6.12 | -46.32 | -46.32 | +0.00 | -46.32 |
| 2026-09-04 | `CPB` | 55 | — | $22.10 | +0.00 | $21.38 | -39.60 | -39.60 | +0.00 | -39.60 |
| 2026-09-04 | `LULU` | 12 | — | $98.15 | +0.00 | $100.61 | +29.52 | +29.52 | +0.00 | +29.52 |
| 2026-09-04 | `VSXY` | 16 | — | $73.63 | +0.00 | $75.56 | +30.88 | +30.88 | +0.00 | +30.88 |
| 2026-09-04 | `CLYM` | 84 | — | $14.49 | +0.00 | $15.52 | +86.52 | +86.52 | +0.00 | +86.52 |
| 2026-09-08 | `ALMS` | 102 | $11.10 | $11.05 | -5.10 | — | +0.00 | -5.10 | +68.85 | — |
| 2026-09-08 | `CHPT` | 132 | $9.89 | $9.91 | +2.64 | $9.37 | -71.28 | -68.64 | +83.16 | +11.88 |
| 2026-09-08 | `RARE` | 79 | $15.30 | $15.10 | -16.04 | $14.88 | -17.14 | -33.18 | -29.86 | -47.00 |
| 2026-09-08 | `DPRO` | 193 | $6.12 | $6.07 | -9.65 | — | +0.00 | -9.65 | -55.97 | — |
| 2026-09-08 | `CPB` | 55 | $21.38 | $21.30 | -4.40 | — | +0.00 | -4.40 | -44.00 | — |
| 2026-09-08 | `LULU` | 12 | $100.61 | $100.58 | -0.36 | $103.19 | +31.32 | +30.96 | +29.16 | +60.48 |
| 2026-09-08 | `VSXY` | 16 | $75.56 | $73.51 | -32.80 | — | +0.00 | -32.80 | -1.92 | — |
| 2026-09-08 | `CLYM` | 84 | $15.52 | $15.62 | +8.40 | — | +0.00 | +8.40 | +94.92 | — |
| 2026-09-09 | `CHPT` | 132 | $9.37 | $9.39 | +2.64 | — | +0.00 | +2.64 | +14.52 | — |
| 2026-09-09 | `RARE` | 79 | $14.88 | $14.77 | -8.69 | — | +0.00 | -8.69 | -55.70 | — |
| 2026-09-09 | `LULU` | 12 | $103.19 | $101.90 | -15.48 | — | +0.00 | -15.48 | +45.00 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `BKV` | 48 | — | $24.97 | +0.00 | $24.23 | -35.52 | -35.52 | +0.00 | -35.52 |
| 2026-09-11 | `COO` | 22 | — | $54.66 | +0.00 | $53.91 | -16.50 | -16.50 | +0.00 | -16.50 |
| 2026-09-11 | `GFR` | 196 | — | $6.19 | +0.00 | $6.52 | +64.68 | +64.68 | +0.00 | +64.68 |
| 2026-09-11 | `SHOE` | 97 | — | $12.51 | +0.00 | $12.71 | +19.40 | +19.40 | +0.00 | +19.40 |
| 2026-09-11 | `AEO` | 82 | — | $14.71 | +0.00 | $15.02 | +25.42 | +25.42 | +0.00 | +25.42 |
| 2026-09-11 | `ACVA` | 116 | — | $10.46 | +0.00 | $10.41 | -5.22 | -5.22 | +0.00 | -5.22 |
| 2026-09-11 | `AVAV` | 8 | — | $145.91 | +0.00 | $146.71 | +6.40 | +6.40 | +0.00 | +6.40 |
| 2026-09-11 | `WLTH` | 110 | — | $10.95 | +0.00 | $10.38 | -62.70 | -62.70 | +0.00 | -62.70 |
| 2026-09-14 | `BKV` | 48 | $24.23 | $24.26 | +1.44 | — | +0.00 | +1.44 | -34.08 | — |
| 2026-09-14 | `COO` | 22 | $53.91 | $54.78 | +19.14 | — | +0.00 | +19.14 | +2.64 | — |
| 2026-09-14 | `GFR` | 196 | $6.52 | $6.60 | +15.68 | $6.62 | +3.92 | +19.60 | +80.36 | +84.28 |
| 2026-09-14 | `SHOE` | 97 | $12.71 | $12.55 | -15.52 | — | +0.00 | -15.52 | +3.88 | — |
| 2026-09-14 | `AEO` | 82 | $15.02 | $14.85 | -13.94 | — | +0.00 | -13.94 | +11.48 | — |
| 2026-09-14 | `ACVA` | 116 | $10.41 | $10.42 | +1.16 | $10.41 | -1.16 | +0.00 | -4.06 | -5.22 |
| 2026-09-14 | `AVAV` | 8 | $146.71 | $145.80 | -7.28 | — | +0.00 | -7.28 | -0.88 | — |
| 2026-09-14 | `WLTH` | 110 | $10.38 | $10.29 | -9.90 | $10.65 | +39.60 | +29.70 | -72.60 | -33.00 |
| 2026-09-15 | `GFR` | 196 | $6.62 | $6.61 | -1.96 | — | +0.00 | -1.96 | +82.32 | — |
| 2026-09-15 | `ACVA` | 116 | $10.41 | $10.43 | +2.32 | $10.43 | +0.00 | +2.32 | -2.90 | -2.90 |
| 2026-09-15 | `WLTH` | 110 | $10.65 | $10.55 | -11.00 | — | +0.00 | -11.00 | -44.00 | — |
| 2026-09-16 | `ACVA` | 116 | $10.43 | $10.44 | +1.16 | — | +0.00 | +1.16 | -1.74 | — |
| 2026-09-16 | `TRMD` | 33 | — | $35.90 | +0.00 | $36.60 | +23.10 | +23.10 | +0.00 | +23.10 |
| 2026-09-16 | `PLAY` | 176 | — | $6.86 | +0.00 | $6.86 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-16 | `BWIN` | 37 | — | $32.25 | +0.00 | $32.04 | -7.77 | -7.77 | +0.00 | -7.77 |
| 2026-09-16 | `ALHC` | 117 | — | $10.30 | +0.00 | $8.71 | -186.03 | -186.03 | +0.00 | -186.03 |
| 2026-09-16 | `VERA` | 36 | — | $33.22 | +0.00 | $31.77 | -52.20 | -52.20 | +0.00 | -52.20 |
| 2026-09-16 | `ASND` | 5 | — | $239.70 | +0.00 | $247.69 | +39.95 | +39.95 | +0.00 | +39.95 |
| 2026-09-16 | `HLMN` | 166 | — | $7.26 | +0.00 | $7.32 | +9.96 | +9.96 | +0.00 | +9.96 |
| 2026-09-16 | `FPS` | 36 | — | $33.14 | +0.00 | $34.84 | +61.20 | +61.20 | +0.00 | +61.20 |
| 2026-09-17 | `TRMD` | 33 | $36.60 | $36.52 | -2.64 | — | +0.00 | -2.64 | +20.46 | — |
| 2026-09-17 | `PLAY` | 176 | $6.86 | $6.96 | +17.60 | — | +0.00 | +17.60 | +17.60 | — |
| 2026-09-17 | `BWIN` | 37 | $32.04 | $32.06 | +0.74 | $31.95 | -4.07 | -3.33 | -7.03 | -11.10 |
| 2026-09-17 | `ALHC` | 117 | $8.71 | $8.58 | -15.21 | $8.70 | +14.04 | -1.17 | -201.24 | -187.20 |
| 2026-09-17 | `VERA` | 36 | $31.77 | $32.50 | +26.28 | — | +0.00 | +26.28 | -25.92 | — |
| 2026-09-17 | `ASND` | 5 | $247.69 | $249.23 | +7.70 | — | +0.00 | +7.70 | +47.65 | — |
| 2026-09-17 | `HLMN` | 166 | $7.32 | $7.51 | +31.54 | — | +0.00 | +31.54 | +41.50 | — |
| 2026-09-17 | `FPS` | 36 | $34.84 | $36.76 | +69.12 | — | +0.00 | +69.12 | +130.32 | — |
| 2026-09-17 | `FANG` | 6 | — | $191.08 | +0.00 | $196.94 | +35.16 | +35.16 | +0.00 | +35.16 |
| 2026-09-17 | `BBNX` | 55 | — | $22.46 | +0.00 | $21.43 | -56.65 | -56.65 | +0.00 | -56.65 |
| 2026-09-17 | `JBHT` | 5 | — | $238.60 | +0.00 | $236.80 | -9.00 | -9.00 | +0.00 | -9.00 |
| 2026-09-17 | `ALVO` | 239 | — | $5.22 | +0.00 | $5.34 | +28.68 | +28.68 | +0.00 | +28.68 |
| 2026-09-17 | `MNR` | 113 | — | $11.00 | +0.00 | $10.97 | -3.39 | -3.39 | +0.00 | -3.39 |
| 2026-09-17 | `CBC` | 39 | — | $31.60 | +0.00 | $31.67 | +2.73 | +2.73 | +0.00 | +2.73 |
| 2026-09-18 | `BWIN` | 37 | $31.95 | $31.98 | +1.11 | — | +0.00 | +1.11 | -9.99 | — |
| 2026-09-18 | `ALHC` | 117 | $8.70 | $8.68 | -2.34 | $8.35 | -38.61 | -40.95 | -189.54 | -228.15 |
| 2026-09-18 | `FANG` | 6 | $196.94 | $196.94 | +0.00 | — | +0.00 | +0.00 | +35.16 | — |
| 2026-09-18 | `BBNX` | 55 | $21.43 | $21.30 | -7.15 | — | +0.00 | -7.15 | -63.80 | — |
| 2026-09-18 | `JBHT` | 5 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -9.00 | — |
| 2026-09-18 | `ALVO` | 239 | $5.34 | $5.40 | +14.34 | — | +0.00 | +14.34 | +43.02 | — |
| 2026-09-18 | `MNR` | 113 | $10.97 | $10.95 | -2.26 | — | +0.00 | -2.26 | -5.65 | — |
| 2026-09-18 | `CBC` | 39 | $31.67 | $31.64 | -1.17 | — | +0.00 | -1.17 | +1.56 | — |
| 2026-09-18 | `GNRC` | 5 | — | $209.52 | +0.00 | $207.44 | -10.40 | -10.40 | +0.00 | -10.40 |
| 2026-09-18 | `SDGR` | 42 | — | $29.32 | +0.00 | $29.02 | -12.60 | -12.60 | +0.00 | -12.60 |
| 2026-09-18 | `QSR` | 16 | — | $73.00 | +0.00 | $72.88 | -1.92 | -1.92 | +0.00 | -1.92 |
| 2026-09-18 | `FLNC` | 164 | — | $7.54 | +0.00 | $7.32 | -35.26 | -35.26 | +0.00 | -35.26 |
| 2026-09-18 | `GBTG` | 130 | — | $9.46 | +0.00 | $9.46 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-18 | `HLN` | 134 | — | $9.20 | +0.00 | $9.28 | +10.72 | +10.72 | +0.00 | +10.72 |
| 2026-09-18 | `ACVA` | 117 | — | $10.48 | +0.00 | $10.48 | +0.00 | +0.00 | +0.00 | +0.00 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -260.12 | ARX, CLBT, OMER, AIRO, SECZ, YETI, AVAH, CRMD | — | $43.45 | $9,721.25 | ARX×63, CLBT×115, OMER×72, AIRO×112, SECZ×214, YETI×27, AVAH×104, CRMD×155 |
| 2026-08-17 | +2.25 | $43.45 | ARX×63, CLBT×115, OMER×72, AIRO×112, SECZ×214, YETI×27, AVAH×104, CRMD×155 | $9,660.28 | -60.97 | +31.62 | CAPR, HTFL, BIRK, NMAX, CHEF, VIV, ABEO | CLBT, OMER, AIRO, SECZ, YETI, AVAH, CRMD | $31.09 | $9,659.28 | ARX×63, CAPR×174, HTFL×29, BIRK×30, NMAX×109, CHEF×11, VIV×104, ABEO×205 |
| 2026-08-18 | -6.20 | $31.09 | ARX×63, CAPR×174, HTFL×29, BIRK×30, NMAX×109, CHEF×11, VIV×104, ABEO×205 | $9,658.70 | -0.58 | -73.08 | — | ARX, HTFL, BIRK, NMAX, CHEF, VIV, ABEO | $8,337.89 | $9,569.81 | CAPR×174 |
| 2026-08-19 | -7.20 | $8,337.89 | CAPR×174 | $9,588.95 | +19.14 | +0.00 | — | CAPR | $9,586.40 | $9,586.40 | — |
| 2026-08-20 | +1.12 | $9,586.40 | — | $9,586.40 | +0.00 | -107.32 | MRNA, WBS, ALH, LBRDK, SUI, NTST, BNTX, ADC | — | $422.03 | $9,462.57 | MRNA×7, WBS×15, ALH×50, LBRDK×33, SUI×9, NTST×58, BNTX×10, ADC×16 |
| 2026-08-21 | +3.25 | $422.03 | MRNA×7, WBS×15, ALH×50, LBRDK×33, SUI×9, NTST×58, BNTX×10, ADC×16 | $9,475.28 | +12.71 | +360.74 | AAP, MFC, MRVI, BULL, SGRY, ARCT | WBS, SUI, NTST, BNTX, ADC | $22.48 | $9,812.35 | MRNA×7, ALH×50, LBRDK×33, AAP×24, MFC×24, MRVI×124, BULL×114, SGRY×73, ARCT×92 |
| 2026-08-24 | -5.17 | $22.48 | MRNA×7, ALH×50, LBRDK×33, AAP×24, MFC×24, MRVI×124, BULL×114, SGRY×73, ARCT×92 | $9,773.47 | -38.88 | +66.25 | — | ALH, AAP, MFC, MRVI, BULL, SGRY | $6,346.24 | $9,826.41 | MRNA×7, LBRDK×33, ARCT×92 |
| 2026-08-25 | +1.80 | $6,346.24 | MRNA×7, LBRDK×33, ARCT×92 | $9,838.44 | +12.03 | +340.53 | ABUS, ZURA, ALH, CAPR, FWDI, XPEV | MRNA | $1,076.85 | $10,162.53 | LBRDK×33, ARCT×92, ABUS×199, ZURA×164, ALH×43, CAPR×144, FWDI×183, XPEV×93 |
| 2026-08-26 | +2.02 | $1,076.85 | LBRDK×33, ARCT×92, ABUS×199, ZURA×164, ALH×43, CAPR×144, FWDI×183, XPEV×93 | $10,140.81 | -21.72 | -57.94 | DKS, SLF, QFIN, GENB, VIPS, BZ, MRNA, ATHM | ARCT, ABUS, ZURA, ALH, CAPR, FWDI, XPEV | $98.90 | $10,048.85 | LBRDK×33, DKS×9, SLF×14, QFIN×114, GENB×55, VIPS×79, BZ×66, MRNA×7, ATHM×51 |
| 2026-08-27 | — | $98.90 | LBRDK×33, DKS×9, SLF×14, QFIN×114, GENB×55, VIPS×79, BZ×66, MRNA×7, ATHM×51 | $10,021.70 | -27.15 | +0.00 | — | DKS, SLF, QFIN, GENB, VIPS, BZ, MRNA, ATHM | $8,815.77 | $10,004.43 | LBRDK×33 |
| 2026-08-28 | +0.75 | $8,815.77 | LBRDK×33 | $10,004.43 | -0.00 | -210.13 | ANF, DKS, BZ, QFIN, LEG, BHVN, PLAB, DY | LBRDK | $206.19 | $9,774.83 | ANF×8, DKS×9, BZ×68, QFIN×136, LEG×135, BHVN×78, PLAB×41, DY×4 |
| 2026-08-31 | -5.85 | $206.19 | ANF×8, DKS×9, BZ×68, QFIN×136, LEG×135, BHVN×78, PLAB×41, DY×4 | $9,797.54 | +22.71 | +0.00 | — | ANF, DKS, BZ, QFIN, BHVN, PLAB, DY | $8,540.42 | $9,782.42 | LEG×135 |
| 2026-09-01 | -6.30 | $8,540.42 | LEG×135 | $9,782.42 | +0.00 | +0.00 | — | — | $8,540.42 | $9,782.42 | LEG×135 |
| 2026-09-02 | -3.83 | $8,540.42 | LEG×135 | $9,782.42 | +0.00 | +0.00 | — | — | $8,540.42 | $9,782.42 | LEG×135 |
| 2026-09-03 | -0.90 | $8,540.42 | LEG×135 | $9,782.42 | +0.00 | +45.68 | GBTG, ALMS, DAKT, GTLB, FRVO, DELL, MDB, OABI | — | $445.44 | $9,810.41 | LEG×135, GBTG×112, ALMS×102, DAKT×55, GTLB×21, FRVO×58, DELL×2, MDB×2, OABI×210 |
| 2026-09-04 | +2.25 | $445.44 | LEG×135, GBTG×112, ALMS×102, DAKT×55, GTLB×21, FRVO×58, DELL×2, MDB×2, OABI×210 | $9,784.07 | -26.34 | +114.44 | CHPT, RARE, DPRO, CPB, LULU, VSXY, CLYM | LEG, GBTG, DAKT, GTLB, FRVO, DELL, MDB, OABI | $141.46 | $9,864.86 | ALMS×102, CHPT×132, RARE×79, DPRO×193, CPB×55, LULU×12, VSXY×16, CLYM×84 |
| 2026-09-08 | -11.47 | $141.46 | ALMS×102, CHPT×132, RARE×79, DPRO×193, CPB×55, LULU×12, VSXY×16, CLYM×84 | $9,807.55 | -57.31 | -57.10 | — | ALMS, DPRO, CPB, VSXY, CLYM | $6,088.37 | $9,739.01 | CHPT×132, RARE×79, LULU×12 |
| 2026-09-09 | -13.95 | $6,088.37 | CHPT×132, RARE×79, LULU×12 | $9,717.48 | -21.53 | +0.00 | — | CHPT, RARE, LULU | $9,710.77 | $9,710.77 | — |
| 2026-09-10 | -13.28 | $9,710.77 | — | $9,710.77 | +0.00 | +0.00 | — | — | $9,710.77 | $9,710.77 | — |
| 2026-09-11 | +0.50 | $9,710.77 | — | $9,710.77 | +0.00 | -4.04 | BKV, COO, GFR, SHOE, AEO, ACVA, AVAV, WLTH | — | $74.24 | $9,688.77 | BKV×48, COO×22, GFR×196, SHOE×97, AEO×82, ACVA×116, AVAV×8, WLTH×110 |
| 2026-09-14 | -11.00 | $74.24 | BKV×48, COO×22, GFR×196, SHOE×97, AEO×82, ACVA×116, AVAV×8, WLTH×110 | $9,679.55 | -9.22 | +42.36 | — | BKV, COO, SHOE, AEO, AVAV | $6,034.50 | $9,711.08 | GFR×196, ACVA×116, WLTH×110 |
| 2026-09-15 | -3.84 | $6,034.50 | GFR×196, ACVA×116, WLTH×110 | $9,700.44 | -10.64 | +0.00 | — | GFR, WLTH | $8,485.59 | $9,695.47 | ACVA×116 |
| 2026-09-16 | +5.30 | $8,485.59 | ACVA×116 | $9,696.63 | +1.16 | -111.79 | TRMD, PLAY, BWIN, ALHC, VERA, ASND, HLMN, FPS | ACVA | $93.50 | $9,564.74 | TRMD×33, PLAY×176, BWIN×37, ALHC×117, VERA×36, ASND×5, HLMN×166, FPS×36 |
| 2026-09-17 | +7.38 | $93.50 | TRMD×33, PLAY×176, BWIN×37, ALHC×117, VERA×36, ASND×5, HLMN×166, FPS×36 | $9,699.87 | +135.13 | +7.50 | FANG, BBNX, JBHT, ALVO, MNR, CBC | TRMD, PLAY, VERA, ASND, HLMN, FPS | $184.89 | $9,680.23 | BWIN×37, ALHC×117, FANG×6, BBNX×55, JBHT×5, ALVO×239, MNR×113, CBC×39 |
| 2026-09-18 | +4.86 | $184.89 | BWIN×37, ALHC×117, FANG×6, BBNX×55, JBHT×5, ALVO×239, MNR×113, CBC×39 | $9,682.76 | +2.53 | -88.07 | GNRC, SDGR, QSR, FLNC, GBTG, HLN, ACVA | BWIN, FANG, BBNX, JBHT, ALVO, MNR, CBC | $263.94 | $9,562.97 | ALHC×117, GNRC×5, SDGR×42, QSR×16, FLNC×164, GBTG×130, HLN×134, ACVA×117 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $8,764.91 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 115 | $10.83 | $2.33 | — | $7,517.13 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; 🔵; ⚪; ret5=-30.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `OMER` | 72 | $17.35 | $2.21 | — | $6,265.72 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+31.9; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $5,017.95 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SECZ` | 214 | $5.84 | $2.76 | — | $3,765.43 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; 🔵; ⚪; ret5=-20.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `YETI` | 27 | $45.51 | $2.07 | — | $2,534.59 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-12.4; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AVAH` | 104 | $11.91 | $2.30 | — | $1,293.65 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+21.3; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `CRMD` | 155 | $8.05 | $2.46 | — | $43.45 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+8.4; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.45 | ▼ close $9,721.25 vs 09:30 $10,000.00 (session -260.12) | 16:00 close · cash $43.45 · equity $9,721.25 vs 09:30 $10,000.00 (-278.75; session marks -260.12) · 8 name(s) marked open→close (per-name table). ARX×63 09:30 $19.57 → close $19.58 +0.63; CLBT×115 09:30 $10.83 → close $11.14 +35.65; OMER×72 09:30 $17.35 → close $17.19 -11.52; AIRO×112 09:30 $11.12 → close $9.57 -173.60; SECZ×214 09:30 $5.84 → close $5.61 -49.22; YETI×27 09:30 $45.51 → close $44.56 -25.65; AVAH×104 09:30 $11.91 → close $12.32 +42.64; CRMD×155 09:30 $8.05 → close $7.54 -79.05 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.45 | ▼ 09:30 equity $9,660.28 vs yday $9,721.25 (-60.97) | 09:30 open · cash $43.45 (unchanged overnight, no fees) · equity $9,660.28 vs prior close $9,721.25 (-60.97) · 8 name(s) re-marked at the open (per-name table). ARX×63 yday $19.58 → 09:30 $19.57 -0.63; CLBT×115 yday $11.14 → 09:30 $11.19 +5.75; OMER×72 yday $17.19 → 09:30 $17.17 -1.44; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; SECZ×214 yday $5.61 → 09:30 $5.45 -34.24; YETI×27 yday $44.56 → 09:30 $43.80 -20.52; AVAH×104 yday $12.32 → 09:30 $12.21 -11.44; CRMD×155 yday $7.54 → 09:30 $7.55 +1.55 | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 115 | $11.19 | $2.36 | $+36.70 | $1,327.93 | ▲ +36.70 after sell → book $9,657.91; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `OMER` | 72 | $17.17 | $2.23 | $-17.39 | $2,561.94 | ▼ -17.39 after sell → book $9,655.68; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $3,631.43 | ▼ -178.28 after sell → book $9,653.33; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `SECZ` | 214 | $5.45 | $2.81 | $-89.03 | $4,794.92 | ▼ -89.03 after sell → book $9,650.52; vs 09:30 mark -2.81 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `YETI` | 27 | $43.80 | $2.09 | $-50.33 | $5,975.43 | ▼ -50.33 after sell → book $9,648.43; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AVAH` | 104 | $12.21 | $2.33 | $+26.57 | $7,242.94 | ▲ +26.57 after sell → book $9,646.10; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `CRMD` | 155 | $7.55 | $2.49 | $-82.45 | $8,410.70 | ▼ -82.45 after sell → book $9,643.61; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 174 | $6.87 | $2.51 | — | $7,212.81 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; ret5=+62.6; leftover $1201.53 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $6,015.06 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; ret5=+46.0; leftover $1201.53 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BIRK` | 30 | $39.48 | $2.08 | — | $4,828.58 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=+2.3; leftover $1201.53 | join🟡 sector🔴 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 109 | $10.97 | $2.32 | — | $3,630.54 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; ⚪; ret5=+21.2; leftover $1201.53 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CHEF` | 11 | $108.18 | $2.02 | — | $2,438.53 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ⚪; ret5=-1.0; leftover $1201.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VIV` | 104 | $11.55 | $2.30 | — | $1,235.03 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ⚪; ret5=-5.0; leftover $1201.53 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABEO` | 205 | $5.86 | $2.64 | — | $31.09 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-17.3; leftover $1201.53 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $31.09 | ▲ close $9,659.28 vs 09:30 $9,660.28 (session +31.62) | 16:00 close · cash $31.09 · equity $9,659.28 vs 09:30 $9,660.28 (-1.00; session marks +31.62) · 8 name(s) marked open→close (per-name table). ARX×63 09:30 $19.57 → close $19.54 -1.89; CAPR×174 09:30 $6.87 → close $7.45 +100.92; HTFL×29 09:30 $41.23 → close $41.94 +20.59; BIRK×30 09:30 $39.48 → close $37.86 -48.60; NMAX×109 09:30 $10.97 → close $10.36 -66.49; CHEF×11 09:30 $108.18 → close $110.57 +26.29; VIV×104 09:30 $11.55 → close $11.40 -15.60; ABEO×205 09:30 $5.86 → close $5.94 +16.40 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.09 | ▼ 09:30 equity $9,658.70 vs yday $9,659.28 (-0.58) | 09:30 open · cash $31.09 (unchanged overnight, no fees) · equity $9,658.70 vs prior close $9,659.28 (-0.58) · 8 name(s) re-marked at the open (per-name table). ARX×63 yday $19.54 → 09:30 $19.57 +1.89; CAPR×174 yday $7.45 → 09:30 $7.50 +8.70; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; BIRK×30 yday $37.86 → 09:30 $38.07 +6.30; NMAX×109 yday $10.36 → 09:30 $10.31 -5.45; CHEF×11 yday $110.57 → 09:30 $110.91 +3.74; VIV×104 yday $11.40 → 09:30 $11.45 +5.20; ABEO×205 yday $5.94 → 09:30 $5.90 -8.20 | — |
| 2026-08-18 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $1,261.80 | ▼ -4.38 after sell → book $9,656.50; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $2,463.20 | ▲ +3.66 after sell → book $9,654.40; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `BIRK` | 30 | $38.07 | $2.10 | $-46.48 | $3,603.20 | ▼ -46.48 after sell → book $9,652.30; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 109 | $10.31 | $2.35 | $-76.60 | $4,724.64 | ▼ -76.60 after sell → book $9,649.95; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `CHEF` | 11 | $110.91 | $2.04 | $+25.96 | $5,942.61 | ▲ +25.96 after sell → book $9,647.91; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VIV` | 104 | $11.45 | $2.33 | $-15.03 | $7,131.08 | ▼ -15.03 after sell → book $9,645.58; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABEO` | 205 | $5.90 | $2.69 | $+2.87 | $8,337.89 | ▲ +2.87 after sell → book $9,642.89; vs 09:30 mark -2.69 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,337.89 | ▼ close $9,569.81 vs 09:30 $9,658.70 (session -73.08) | 16:00 close · cash $8,337.89 · equity $9,569.81 vs 09:30 $9,658.70 (-88.89; session marks -73.08) · 1 name(s) marked open→close (per-name table). CAPR×174 09:30 $7.50 → close $7.08 -73.08 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,337.89 | ▲ 09:30 equity $9,588.95 vs yday $9,569.81 (+19.14) | 09:30 open · cash $8,337.89 (unchanged overnight, no fees) · equity $9,588.95 vs prior close $9,569.81 (+19.14) · 1 name(s) re-marked at the open (per-name table). CAPR×174 yday $7.08 → 09:30 $7.19 +19.14 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 174 | $7.19 | $2.55 | $+50.62 | $9,586.40 | ▲ +50.62 after sell → book $9,586.40; vs 09:30 mark -2.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,586.40 | ▲ close $9,586.40 vs 09:30 $9,588.95 (session +0.00) | 16:00 close · cash $9,586.40 · no lots left · equity $9,586.40. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,586.40 | ▲ 09:30 equity $9,586.40 vs yday $9,586.40 (+0.00) | 09:30 open · cash $9,586.40 · no holdings · equity $9,586.40 vs prior close $9,586.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $8,533.41 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+173.9; leftover $1198.30 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WBS` | 15 | $77.57 | $2.04 | — | $7,367.83 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=-1.9; leftover $1198.30 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALH` | 50 | $23.72 | $2.14 | — | $6,179.69 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-7.6; leftover $1198.30 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `LBRDK` | 33 | $36.02 | $2.09 | — | $4,988.94 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-0.2; leftover $1198.30 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SUI` | 9 | $121.21 | $2.02 | — | $3,896.03 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+3.1; leftover $1198.30 | join🔴 sector🔴 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NTST` | 58 | $20.47 | $2.16 | — | $2,706.61 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.0; leftover $1198.30 | join🟢 sector🔴 gen🟢 news🟡 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 10 | $109.06 | $2.02 | — | $1,613.99 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+22.0; leftover $1198.30 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ADC` | 16 | $74.37 | $2.04 | — | $422.03 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.0; leftover $1198.30 | join🟡 sector🔴 gen🟢 news🟡 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $422.03 | ▼ close $9,462.57 vs 09:30 $9,586.40 (session -107.32) | 16:00 close · cash $422.03 · equity $9,462.57 vs 09:30 $9,586.40 (-123.83; session marks -107.32) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $150.14 → close $133.32 -117.74; WBS×15 09:30 $77.57 → close $77.57 +0.00; ALH×50 09:30 $23.72 → close $23.18 -27.00; LBRDK×33 09:30 $36.02 → close $36.02 +0.00; SUI×9 09:30 $121.21 → close $122.29 +9.72; NTST×58 09:30 $20.47 → close $20.61 +8.12; BNTX×10 09:30 $109.06 → close $110.89 +18.30; ADC×16 09:30 $74.37 → close $74.45 +1.28 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $422.03 | ▲ 09:30 equity $9,475.28 vs yday $9,462.57 (+12.71) | 09:30 open · cash $422.03 (unchanged overnight, no fees) · equity $9,475.28 vs prior close $9,462.57 (+12.71) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $133.32 → 09:30 $133.11 -1.47; WBS×15 yday $77.57 → 09:30 $77.57 +0.00; ALH×50 yday $23.18 → 09:30 $23.33 +7.50; LBRDK×33 yday $36.02 → 09:30 $36.02 +0.00; SUI×9 yday $122.29 → 09:30 $122.41 +1.08; NTST×58 yday $20.61 → 09:30 $20.66 +2.90; BNTX×10 yday $110.89 → 09:30 $110.92 +0.30; ADC×16 yday $74.45 → 09:30 $74.60 +2.40 | — |
| 2026-08-21 09:30 ET | **SELL** | `WBS` | 15 | $77.57 | $2.06 | $-4.09 | $1,583.52 | ▼ -4.09 after sell → book $9,473.22; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SUI` | 9 | $122.41 | $2.04 | $+6.75 | $2,683.18 | ▲ +6.75 after sell → book $9,471.19; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NTST` | 58 | $20.66 | $2.18 | $+6.67 | $3,879.27 | ▲ +6.67 after sell → book $9,469.00; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BNTX` | 10 | $110.92 | $2.04 | $+14.54 | $4,986.43 | ▲ +14.54 after sell → book $9,466.96; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ADC` | 16 | $74.60 | $2.06 | $-0.42 | $6,177.97 | ▼ -0.42 after sell → book $9,464.90; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AAP` | 24 | $42.41 | $2.06 | — | $5,158.07 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-26.1; leftover $1029.66 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MFC` | 24 | $42.48 | $2.06 | — | $4,136.49 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=-3.3; leftover $1029.66 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 124 | $8.28 | $2.36 | — | $3,107.41 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+36.6; leftover $1029.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BULL` | 114 | $8.99 | $2.33 | — | $2,080.22 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=+11.3; leftover $1029.66 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `SGRY` | 73 | $14.10 | $2.21 | — | $1,048.71 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-8.2; leftover $1029.66 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 92 | $11.13 | $2.27 | — | $22.48 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,yday_gainer,mover_buy,oppset; 🔵; ⚪; ret5=+39.8; leftover $1029.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.48 | ▲ close $9,812.35 vs 09:30 $9,475.28 (session +360.74) | 16:00 close · cash $22.48 · equity $9,812.35 vs 09:30 $9,475.28 (+337.07; session marks +360.74) · 9 name(s) marked open→close (per-name table). MRNA×7 09:30 $133.11 → close $145.13 +84.14; ALH×50 09:30 $23.33 → close $23.47 +7.00; LBRDK×33 09:30 $36.02 → close $36.02 +0.00; AAP×24 09:30 $42.41 → close $42.58 +4.08; MFC×24 09:30 $42.48 → close $42.51 +0.72; MRVI×124 09:30 $8.28 → close $8.64 +44.64; BULL×114 09:30 $8.99 → close $8.78 -23.94; SGRY×73 09:30 $14.10 → close $14.52 +30.66; ARCT×92 09:30 $11.13 → close $13.45 +213.44 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.48 | ▼ 09:30 equity $9,773.47 vs yday $9,812.35 (-38.88) | 09:30 open · cash $22.48 (unchanged overnight, no fees) · equity $9,773.47 vs prior close $9,812.35 (-38.88) · 9 name(s) re-marked at the open (per-name table). MRNA×7 yday $145.13 → 09:30 $142.70 -17.01; ALH×50 yday $23.47 → 09:30 $23.66 +9.50; LBRDK×33 yday $36.02 → 09:30 $36.02 +0.00; AAP×24 yday $42.58 → 09:30 $43.05 +11.28; MFC×24 yday $42.51 → 09:30 $42.31 -4.80; MRVI×124 yday $8.64 → 09:30 $8.59 -6.20; BULL×114 yday $8.78 → 09:30 $8.58 -22.80; SGRY×73 yday $14.52 → 09:30 $14.55 +2.19; ARCT×92 yday $13.45 → 09:30 $13.33 -11.04 | — |
| 2026-08-24 09:30 ET | **SELL** | `ALH` | 50 | $23.66 | $2.16 | $-7.30 | $1,203.32 | ▼ -7.30 after sell → book $9,771.31; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | join🔴 sector🔴 gen🔴 news🔴 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 24 | $43.05 | $2.08 | $+11.22 | $2,234.44 | ▲ +11.22 after sell → book $9,769.23; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MFC` | 24 | $42.31 | $2.08 | $-8.22 | $3,247.80 | ▼ -8.22 after sell → book $9,767.15; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 124 | $8.59 | $2.39 | $+33.69 | $4,310.56 | ▲ +33.69 after sell → book $9,764.75; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BULL` | 114 | $8.58 | $2.36 | $-51.43 | $5,286.32 | ▼ -51.43 after sell → book $9,762.39; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `SGRY` | 73 | $14.55 | $2.23 | $+28.41 | $6,346.24 | ▲ +28.41 after sell → book $9,760.16; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,346.24 | ▲ close $9,826.41 vs 09:30 $9,773.47 (session +66.25) | 16:00 close · cash $6,346.24 · equity $9,826.41 vs 09:30 $9,773.47 (+52.94; session marks +66.25) · 3 name(s) marked open→close (per-name table). MRNA×7 09:30 $142.70 → close $138.89 -26.67; LBRDK×33 09:30 $36.02 → close $36.02 +0.00; ARCT×92 09:30 $13.33 → close $14.34 +92.92 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,346.24 | ▲ 09:30 equity $9,838.44 vs yday $9,826.41 (+12.03) | 09:30 open · cash $6,346.24 (unchanged overnight, no fees) · equity $9,838.44 vs prior close $9,826.41 (+12.03) · 3 name(s) re-marked at the open (per-name table). MRNA×7 yday $138.89 → 09:30 $143.50 +32.27; LBRDK×33 yday $36.02 → 09:30 $36.02 +0.00; ARCT×92 yday $14.34 → 09:30 $14.12 -20.24 | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 7 | $143.50 | $2.03 | $-50.52 | $7,348.71 | ▼ -50.52 after sell → book $9,836.41; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 1) | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ABUS` | 199 | $5.25 | $2.59 | — | $6,301.37 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list mover_buy,oppset; 🔵; ⚪; ret5=+10.4; leftover $1049.82 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 164 | $6.37 | $2.48 | — | $5,254.21 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,oppset; 🔵; ⚪; ret5=+10.9; leftover $1049.82 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALH` | 43 | $24.16 | $2.12 | — | $4,213.21 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-3.8; leftover $1049.82 | join🔴 sector🟡 gen🟡 news🔴 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 144 | $7.25 | $2.42 | — | $3,166.79 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,ohlc_hot,oppset; 🔵; ret5=-8.7; leftover $1049.82 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 183 | $5.71 | $2.54 | — | $2,119.32 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+33.9; leftover $1049.82 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XPEV` | 93 | $11.19 | $2.27 | — | $1,076.85 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-8.6; leftover $1049.82 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,076.85 | ▲ close $10,162.53 vs 09:30 $9,838.44 (session +340.53) | 16:00 close · cash $1,076.85 · equity $10,162.53 vs 09:30 $9,838.44 (+324.09; session marks +340.53) · 8 name(s) marked open→close (per-name table). LBRDK×33 09:30 $36.02 → close $36.02 +0.00; ARCT×92 09:30 $14.12 → close $15.44 +121.44; ABUS×199 09:30 $5.25 → close $5.20 -9.95; ZURA×164 09:30 $6.37 → close $6.32 -8.20; ALH×43 09:30 $24.16 → close $23.85 -13.33; CAPR×144 09:30 $7.25 → close $8.29 +149.76; FWDI×183 09:30 $5.71 → close $6.05 +62.22; XPEV×93 09:30 $11.19 → close $11.60 +38.59 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,076.85 | ▼ 09:30 equity $10,140.81 vs yday $10,162.53 (-21.72) | 09:30 open · cash $1,076.85 (unchanged overnight, no fees) · equity $10,140.81 vs prior close $10,162.53 (-21.72) · 8 name(s) re-marked at the open (per-name table). LBRDK×33 yday $36.02 → 09:30 $36.02 +0.00; ARCT×92 yday $15.44 → 09:30 $15.35 -8.28; ABUS×199 yday $5.20 → 09:30 $5.19 -1.99; ZURA×164 yday $6.32 → 09:30 $6.13 -31.16; ALH×43 yday $23.85 → 09:30 $24.00 +6.45; CAPR×144 yday $8.29 → 09:30 $8.29 +0.00; FWDI×183 yday $6.05 → 09:30 $5.97 -14.64; XPEV×93 yday $11.60 → 09:30 $11.90 +27.90 | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 92 | $15.35 | $2.29 | $+383.68 | $2,486.76 | ▲ +383.68 after sell → book $10,138.52; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `ABUS` | 199 | $5.19 | $2.63 | $-17.16 | $3,516.94 | ▼ -17.16 after sell → book $10,135.89; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 164 | $6.13 | $2.52 | $-44.36 | $4,519.74 | ▼ -44.36 after sell → book $10,133.37; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `ALH` | 43 | $24.00 | $2.14 | $-11.14 | $5,549.60 | ▼ -11.14 after sell → book $10,131.23; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 144 | $8.29 | $2.46 | $+144.88 | $6,740.90 | ▲ +144.88 after sell → book $10,128.77; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `FWDI` | 183 | $5.97 | $2.58 | $+42.46 | $7,830.83 | ▲ +42.46 after sell → book $10,126.19; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `XPEV` | 93 | $11.90 | $2.29 | $+61.93 | $8,935.24 | ▲ +61.93 after sell → book $10,123.90; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `DKS` | 9 | $121.87 | $2.02 | — | $7,836.39 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; 🔵; ret5=-35.1; leftover $1116.90 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SLF` | 14 | $79.20 | $2.03 | — | $6,725.56 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-0.5; leftover $1116.90 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `QFIN` | 114 | $9.76 | $2.33 | — | $5,610.59 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react,oppset; 🔵; ret5=-4.4; leftover $1116.90 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `GENB` | 55 | $20.00 | $2.15 | — | $4,508.43 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=+26.7; leftover $1116.90 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `VIPS` | 79 | $14.00 | $2.23 | — | $3,400.21 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-0.4; leftover $1116.90 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BZ` | 66 | $16.77 | $2.19 | — | $2,291.20 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+3.1; leftover $1116.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `MRNA` | 7 | $154.20 | $2.01 | — | $1,209.79 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=+152.3; leftover $1116.90 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ATHM` | 51 | $21.74 | $2.14 | — | $98.90 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-1.1; leftover $1116.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.90 | ▼ close $10,048.85 vs 09:30 $10,140.81 (session -57.94) | 16:00 close · cash $98.90 · equity $10,048.85 vs 09:30 $10,140.81 (-91.96; session marks -57.94) · 9 name(s) marked open→close (per-name table). LBRDK×33 09:30 $36.02 → close $36.02 +0.00; DKS×9 09:30 $121.87 → close $129.66 +70.11; SLF×14 09:30 $79.20 → close $79.05 -2.10; QFIN×114 09:30 $9.76 → close $9.35 -46.74; GENB×55 09:30 $20.00 → close $16.14 -212.30; VIPS×79 09:30 $14.00 → close $14.08 +6.32; BZ×66 09:30 $16.77 → close $18.84 +136.62; MRNA×7 09:30 $154.20 → close $149.66 -31.78; ATHM×51 09:30 $21.74 → close $22.17 +21.93 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.90 | ▼ 09:30 equity $10,021.70 vs yday $10,048.85 (-27.15) | 09:30 open · cash $98.90 (unchanged overnight, no fees) · equity $10,021.70 vs prior close $10,048.85 (-27.15) · 9 name(s) re-marked at the open (per-name table). LBRDK×33 yday $36.02 → 09:30 $36.02 +0.00; DKS×9 yday $129.66 → 09:30 $128.73 -8.37; SLF×14 yday $79.05 → 09:30 $78.45 -8.40; QFIN×114 yday $9.35 → 09:30 $9.42 +7.98; GENB×55 yday $16.14 → 09:30 $17.11 +53.35; VIPS×79 yday $14.08 → 09:30 $14.00 -6.32; BZ×66 yday $18.84 → 09:30 $18.50 -22.44; MRNA×7 yday $149.66 → 09:30 $144.18 -38.36; ATHM×51 yday $22.17 → 09:30 $22.08 -4.59 | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 9 | $128.73 | $2.04 | $+57.69 | $1,255.44 | ▲ +57.69 after sell → book $10,019.67; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `SLF` | 14 | $78.45 | $2.05 | $-14.58 | $2,351.68 | ▼ -14.58 after sell → book $10,017.61; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `QFIN` | 114 | $9.42 | $2.36 | $-43.45 | $3,423.20 | ▼ -43.45 after sell → book $10,015.25; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `GENB` | 55 | $17.11 | $2.17 | $-163.28 | $4,362.08 | ▼ -163.28 after sell → book $10,013.08; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `VIPS` | 79 | $14.00 | $2.25 | $-4.48 | $5,465.83 | ▼ -4.48 after sell → book $10,010.83; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 66 | $18.50 | $2.21 | $+109.78 | $6,684.62 | ▲ +109.78 after sell → book $10,008.62; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **SELL** | `MRNA` | 7 | $144.18 | $2.03 | $-74.18 | $7,691.85 | ▼ -74.18 after sell → book $10,006.59; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `ATHM` | 51 | $22.08 | $2.16 | $+13.03 | $8,815.77 | ▲ +13.03 after sell → book $10,004.43; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,815.77 | ▲ close $10,004.43 vs 09:30 $10,021.70 (session +0.00) | 16:00 close · cash $8,815.77 · equity $10,004.43 vs 09:30 $10,021.70 (-17.27; session marks +0.00) · 1 name(s) marked open→close (per-name table). LBRDK×33 09:30 $36.02 → close $36.02 +0.00 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,815.77 | ▲ 09:30 equity $10,004.43 vs yday $10,004.43 (-0.00) | 09:30 open · cash $8,815.77 (unchanged overnight, no fees) · equity $10,004.43 vs prior close $10,004.43 (-0.00) · 1 name(s) re-marked at the open (per-name table). LBRDK×33 yday $36.02 → 09:30 $36.02 +0.00 | — |
| 2026-08-28 09:30 ET | **SELL** | `LBRDK` | 33 | $36.02 | $2.11 | $-4.20 | $10,002.32 | ▼ -4.20 after sell → book $10,002.32; vs 09:30 mark -2.11 | dropped from list after 6 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $8,831.74 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+38.8; leftover $1250.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DKS` | 9 | $132.80 | $2.02 | — | $7,634.53 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-26.5; leftover $1250.29 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 68 | $18.15 | $2.19 | — | $6,398.13 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; ret5=+14.1; leftover $1250.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `QFIN` | 136 | $9.15 | $2.40 | — | $5,151.33 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-19.9; leftover $1250.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LEG` | 135 | $9.20 | $2.40 | — | $3,906.94 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-2.6; leftover $1250.29 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 78 | $15.88 | $2.22 | — | $2,666.07 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; ret5=+19.4; leftover $1250.29 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PLAB` | 41 | $30.01 | $2.11 | — | $1,433.55 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-0.9; leftover $1250.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 4 | $306.34 | $2.00 | — | $206.19 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-23.0; leftover $1250.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $206.19 | ▼ close $9,774.83 vs 09:30 $10,004.43 (session -210.13) | 16:00 close · cash $206.19 · equity $9,774.83 vs 09:30 $10,004.43 (-229.60; session marks -210.13) · 8 name(s) marked open→close (per-name table). ANF×8 09:30 $146.07 → close $148.42 +18.80; DKS×9 09:30 $132.80 → close $135.09 +20.61; BZ×68 09:30 $18.15 → close $17.80 -23.80; QFIN×136 09:30 $9.15 → close $8.80 -47.60; LEG×135 09:30 $9.20 → close $9.20 +0.00; BHVN×78 09:30 $15.88 → close $15.41 -36.66; PLAB×41 09:30 $30.01 → close $27.73 -93.48; DY×4 09:30 $306.34 → close $294.34 -48.00 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $206.19 | ▲ 09:30 equity $9,797.54 vs yday $9,774.83 (+22.71) | 09:30 open · cash $206.19 (unchanged overnight, no fees) · equity $9,797.54 vs prior close $9,774.83 (+22.71) · 8 name(s) re-marked at the open (per-name table). ANF×8 yday $148.42 → 09:30 $148.03 -3.12; DKS×9 yday $135.09 → 09:30 $136.75 +14.94; BZ×68 yday $17.80 → 09:30 $17.70 -6.80; QFIN×136 yday $8.80 → 09:30 $8.70 -13.60; LEG×135 yday $9.20 → 09:30 $9.20 +0.00; BHVN×78 yday $15.41 → 09:30 $15.46 +3.90; PLAB×41 yday $27.73 → 09:30 $28.04 +12.71; DY×4 yday $294.34 → 09:30 $298.01 +14.68 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.03 | $2.03 | $+11.63 | $1,388.40 | ▲ +11.63 after sell → book $9,795.51; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DKS` | 9 | $136.75 | $2.04 | $+31.50 | $2,617.11 | ▲ +31.50 after sell → book $9,793.47; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 68 | $17.70 | $2.22 | $-35.01 | $3,818.49 | ▼ -35.01 after sell → book $9,791.25; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `QFIN` | 136 | $8.70 | $2.43 | $-66.03 | $4,999.26 | ▼ -66.03 after sell → book $9,788.82; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 78 | $15.46 | $2.25 | $-37.23 | $6,202.90 | ▼ -37.23 after sell → book $9,786.58; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PLAB` | 41 | $28.04 | $2.13 | $-85.02 | $7,350.40 | ▼ -85.02 after sell → book $9,784.44; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 4 | $298.01 | $2.02 | $-37.34 | $8,540.42 | ▼ -37.34 after sell → book $9,782.42; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,540.42 | ▲ close $9,782.42 vs 09:30 $9,797.54 (session +0.00) | 16:00 close · cash $8,540.42 · equity $9,782.42 vs 09:30 $9,797.54 (-15.12; session marks +0.00) · 1 name(s) marked open→close (per-name table). LEG×135 09:30 $9.20 → close $9.20 +0.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,540.42 | ▲ 09:30 equity $9,782.42 vs yday $9,782.42 (+0.00) | 09:30 open · cash $8,540.42 (unchanged overnight, no fees) · equity $9,782.42 vs prior close $9,782.42 (+0.00) · 1 name(s) re-marked at the open (per-name table). LEG×135 yday $9.20 → 09:30 $9.20 +0.00 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,540.42 | ▲ close $9,782.42 vs 09:30 $9,782.42 (session +0.00) | 16:00 close · cash $8,540.42 · equity $9,782.42 vs 09:30 $9,782.42 (+0.00; session marks +0.00) · 1 name(s) marked open→close (per-name table). LEG×135 09:30 $9.20 → close $9.20 +0.00 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,540.42 | ▲ 09:30 equity $9,782.42 vs yday $9,782.42 (+0.00) | 09:30 open · cash $8,540.42 (unchanged overnight, no fees) · equity $9,782.42 vs prior close $9,782.42 (+0.00) · 1 name(s) re-marked at the open (per-name table). LEG×135 yday $9.20 → 09:30 $9.20 +0.00 | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,540.42 | ▲ close $9,782.42 vs 09:30 $9,782.42 (session +0.00) | 16:00 close · cash $8,540.42 · equity $9,782.42 vs 09:30 $9,782.42 (+0.00; session marks +0.00) · 1 name(s) marked open→close (per-name table). LEG×135 09:30 $9.20 → close $9.20 +0.00 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,540.42 | ▲ 09:30 equity $9,782.42 vs yday $9,782.42 (+0.00) | 09:30 open · cash $8,540.42 (unchanged overnight, no fees) · equity $9,782.42 vs prior close $9,782.42 (+0.00) · 1 name(s) re-marked at the open (per-name table). LEG×135 yday $9.20 → 09:30 $9.20 +0.00 | — |
| 2026-09-03 09:30 ET | **BUY** | `GBTG` | 112 | $9.49 | $2.33 | — | $7,475.21 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.2; leftover $1067.55 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 102 | $10.38 | $2.30 | — | $6,414.67 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; 🔵; ret5=-56.2; leftover $1067.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DAKT` | 55 | $19.08 | $2.15 | — | $5,363.11 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-1.9; leftover $1067.55 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GTLB` | 21 | $49.98 | $2.05 | — | $4,311.48 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+20.0; leftover $1067.55 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 58 | $18.28 | $2.16 | — | $3,249.08 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; ret5=+16.5; leftover $1067.55 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $2,274.46 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $1067.55 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MDB` | 2 | $378.76 | $2.00 | — | $1,514.94 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-7.6; leftover $1067.55 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OABI` | 210 | $5.08 | $2.71 | — | $445.44 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+4.8; leftover $1067.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $445.44 | ▲ close $9,810.41 vs 09:30 $9,782.42 (session +45.68) | 16:00 close · cash $445.44 · equity $9,810.41 vs 09:30 $9,782.42 (+27.99; session marks +45.68) · 9 name(s) marked open→close (per-name table). LEG×135 09:30 $9.20 → close $9.20 +0.00; GBTG×112 09:30 $9.49 → close $9.49 +0.00; ALMS×102 09:30 $10.38 → close $11.36 +100.47; DAKT×55 09:30 $19.08 → close $19.48 +22.00; GTLB×21 09:30 $49.98 → close $49.31 -14.07; FRVO×58 09:30 $18.28 → close $17.16 -64.96; DELL×2 09:30 $486.31 → close $516.39 +60.16; MDB×2 09:30 $378.76 → close $384.45 +11.38; OABI×210 09:30 $5.08 → close $4.75 -69.30 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $445.44 | ▼ 09:30 equity $9,784.07 vs yday $9,810.41 (-26.34) | 09:30 open · cash $445.44 (unchanged overnight, no fees) · equity $9,784.07 vs prior close $9,810.41 (-26.34) · 9 name(s) re-marked at the open (per-name table). LEG×135 yday $9.20 → 09:30 $9.20 +0.00; GBTG×112 yday $9.49 → 09:30 $9.49 +0.00; ALMS×102 yday $11.36 → 09:30 $11.23 -13.26; DAKT×55 yday $19.48 → 09:30 $19.47 -0.55; GTLB×21 yday $49.31 → 09:30 $48.94 -7.77; FRVO×58 yday $17.16 → 09:30 $17.27 +6.38; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; MDB×2 yday $384.45 → 09:30 $378.34 -12.22; OABI×210 yday $4.75 → 09:30 $4.78 +6.30 | — |
| 2026-09-04 09:30 ET | **SELL** | `LEG` | 135 | $9.20 | $2.43 | $-4.82 | $1,685.01 | ▼ -4.82 after sell → book $9,781.64; vs 09:30 mark -2.43 | dropped from list after 5 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `GBTG` | 112 | $9.49 | $2.35 | $-4.68 | $2,745.53 | ▼ -4.68 after sell → book $9,779.28; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DAKT` | 55 | $19.47 | $2.17 | $+17.12 | $3,814.21 | ▲ +17.12 after sell → book $9,777.11; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `GTLB` | 21 | $48.94 | $2.07 | $-25.97 | $4,839.88 | ▼ -25.97 after sell → book $9,775.04; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 58 | $17.27 | $2.18 | $-62.93 | $5,839.35 | ▼ -62.93 after sell → book $9,772.85; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $6,864.90 | ▲ +50.93 after sell → book $9,770.84; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `MDB` | 2 | $378.34 | $2.02 | $-4.85 | $7,619.56 | ▼ -4.85 after sell → book $9,768.82; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `OABI` | 210 | $4.78 | $2.75 | $-68.46 | $8,620.61 | ▼ -68.46 after sell → book $9,766.07; vs 09:30 mark -2.75 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CHPT` | 132 | $9.28 | $2.39 | — | $7,393.26 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+55.7; leftover $1231.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `RARE` | 79 | $15.47 | $2.23 | — | $6,168.51 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-43.5; leftover $1231.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DPRO` | 193 | $6.36 | $2.57 | — | $4,938.46 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+47.1; leftover $1231.52 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CPB` | 55 | $22.10 | $2.15 | — | $3,720.80 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-4.9; leftover $1231.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 12 | $98.15 | $2.03 | — | $2,540.98 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react,oppset; ret5=+5.9; leftover $1231.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VSXY` | 16 | $73.63 | $2.04 | — | $1,360.86 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-17.9; leftover $1231.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CLYM` | 84 | $14.49 | $2.24 | — | $141.46 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-3.1; leftover $1231.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $141.46 | ▲ close $9,864.86 vs 09:30 $9,784.07 (session +114.44) | 16:00 close · cash $141.46 · equity $9,864.86 vs 09:30 $9,784.07 (+80.79; session marks +114.44) · 8 name(s) marked open→close (per-name table). ALMS×102 09:30 $11.23 → close $11.10 -13.26; CHPT×132 09:30 $9.28 → close $9.89 +80.52; RARE×79 09:30 $15.47 → close $15.30 -13.82; DPRO×193 09:30 $6.36 → close $6.12 -46.32; CPB×55 09:30 $22.10 → close $21.38 -39.60; LULU×12 09:30 $98.15 → close $100.61 +29.52; VSXY×16 09:30 $73.63 → close $75.56 +30.88; CLYM×84 09:30 $14.49 → close $15.52 +86.52 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $141.46 | ▼ 09:30 equity $9,807.55 vs yday $9,864.86 (-57.31) | 09:30 open · cash $141.46 (unchanged overnight, no fees) · equity $9,807.55 vs prior close $9,864.86 (-57.31) · 8 name(s) re-marked at the open (per-name table). ALMS×102 yday $11.10 → 09:30 $11.05 -5.10; CHPT×132 yday $9.89 → 09:30 $9.91 +2.64; RARE×79 yday $15.30 → 09:30 $15.10 -16.04; DPRO×193 yday $6.12 → 09:30 $6.07 -9.65; CPB×55 yday $21.38 → 09:30 $21.30 -4.40; LULU×12 yday $100.61 → 09:30 $100.58 -0.36; VSXY×16 yday $75.56 → 09:30 $73.51 -32.80; CLYM×84 yday $15.52 → 09:30 $15.62 +8.40 | — |
| 2026-09-08 09:30 ET | **SELL** | `ALMS` | 102 | $11.05 | $2.32 | $+64.23 | $1,266.23 | ▲ +64.23 after sell → book $9,805.23; vs 09:30 mark -2.32 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DPRO` | 193 | $6.07 | $2.61 | $-61.15 | $2,435.13 | ▼ -61.15 after sell → book $9,802.62; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `CPB` | 55 | $21.30 | $2.17 | $-48.33 | $3,604.46 | ▼ -48.33 after sell → book $9,800.44; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VSXY` | 16 | $73.51 | $2.06 | $-6.02 | $4,778.56 | ▼ -6.02 after sell → book $9,798.38; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CLYM` | 84 | $15.62 | $2.27 | $+90.41 | $6,088.37 | ▲ +90.41 after sell → book $9,796.12; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,088.37 | ▼ close $9,739.01 vs 09:30 $9,807.55 (session -57.10) | 16:00 close · cash $6,088.37 · equity $9,739.01 vs 09:30 $9,807.55 (-68.54; session marks -57.10) · 3 name(s) marked open→close (per-name table). CHPT×132 09:30 $9.91 → close $9.37 -71.28; RARE×79 09:30 $15.10 → close $14.88 -17.14; LULU×12 09:30 $100.58 → close $103.19 +31.32 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,088.37 | ▼ 09:30 equity $9,717.48 vs yday $9,739.01 (-21.53) | 09:30 open · cash $6,088.37 (unchanged overnight, no fees) · equity $9,717.48 vs prior close $9,739.01 (-21.53) · 3 name(s) re-marked at the open (per-name table). CHPT×132 yday $9.37 → 09:30 $9.39 +2.64; RARE×79 yday $14.88 → 09:30 $14.77 -8.69; LULU×12 yday $103.19 → 09:30 $101.90 -15.48 | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 132 | $9.39 | $2.42 | $+9.72 | $7,325.44 | ▲ +9.72 after sell → book $9,715.07; vs 09:30 mark -2.41 | dropped from list after 2 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-09 09:30 ET | **SELL** | `RARE` | 79 | $14.77 | $2.25 | $-60.17 | $8,490.02 | ▼ -60.17 after sell → book $9,712.82; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `LULU` | 12 | $101.90 | $2.05 | $+40.93 | $9,710.77 | ▲ +40.93 after sell → book $9,710.77; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,710.77 | ▲ close $9,710.77 vs 09:30 $9,717.48 (session +0.00) | 16:00 close · cash $9,710.77 · no lots left · equity $9,710.77. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,710.77 | ▲ 09:30 equity $9,710.77 vs yday $9,710.77 (+0.00) | 09:30 open · cash $9,710.77 · no holdings · equity $9,710.77 vs prior close $9,710.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,710.77 | ▲ close $9,710.77 vs 09:30 $9,710.77 (session +0.00) | 16:00 close · cash $9,710.77 · no lots left · equity $9,710.77. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,710.77 | ▲ 09:30 equity $9,710.77 vs yday $9,710.77 (+0.00) | 09:30 open · cash $9,710.77 · no holdings · equity $9,710.77 vs prior close $9,710.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `BKV` | 48 | $24.97 | $2.13 | — | $8,510.08 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-0.6; leftover $1213.85 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 22 | $54.66 | $2.06 | — | $7,305.50 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-22.3; leftover $1213.85 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `GFR` | 196 | $6.19 | $2.58 | — | $6,089.68 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=+1.1; leftover $1213.85 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SHOE` | 97 | $12.51 | $2.28 | — | $4,873.93 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-9.0; leftover $1213.85 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AEO` | 82 | $14.71 | $2.24 | — | $3,665.48 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-12.8; leftover $1213.85 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ACVA` | 116 | $10.46 | $2.34 | — | $2,450.36 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.0; leftover $1213.85 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AVAV` | 8 | $145.91 | $2.01 | — | $1,281.06 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+1.2; leftover $1213.85 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 110 | $10.95 | $2.32 | — | $74.24 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+20.8; leftover $1213.85 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $74.24 | ▼ close $9,688.77 vs 09:30 $9,710.77 (session -4.04) | 16:00 close · cash $74.24 · equity $9,688.77 vs 09:30 $9,710.77 (-22.00; session marks -4.04) · 8 name(s) marked open→close (per-name table). BKV×48 09:30 $24.97 → close $24.23 -35.52; COO×22 09:30 $54.66 → close $53.91 -16.50; GFR×196 09:30 $6.19 → close $6.52 +64.68; SHOE×97 09:30 $12.51 → close $12.71 +19.40; AEO×82 09:30 $14.71 → close $15.02 +25.42; ACVA×116 09:30 $10.46 → close $10.41 -5.22; AVAV×8 09:30 $145.91 → close $146.71 +6.40; WLTH×110 09:30 $10.95 → close $10.38 -62.70 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.24 | ▼ 09:30 equity $9,679.55 vs yday $9,688.77 (-9.22) | 09:30 open · cash $74.24 (unchanged overnight, no fees) · equity $9,679.55 vs prior close $9,688.77 (-9.22) · 8 name(s) re-marked at the open (per-name table). BKV×48 yday $24.23 → 09:30 $24.26 +1.44; COO×22 yday $53.91 → 09:30 $54.78 +19.14; GFR×196 yday $6.52 → 09:30 $6.60 +15.68; SHOE×97 yday $12.71 → 09:30 $12.55 -15.52; AEO×82 yday $15.02 → 09:30 $14.85 -13.94; ACVA×116 yday $10.41 → 09:30 $10.42 +1.16; AVAV×8 yday $146.71 → 09:30 $145.80 -7.28; WLTH×110 yday $10.38 → 09:30 $10.29 -9.90 | — |
| 2026-09-14 09:30 ET | **SELL** | `BKV` | 48 | $24.26 | $2.15 | $-38.37 | $1,236.57 | ▼ -38.37 after sell → book $9,677.40; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 22 | $54.78 | $2.08 | $-1.49 | $2,439.65 | ▼ -1.49 after sell → book $9,675.32; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `SHOE` | 97 | $12.55 | $2.31 | $-0.71 | $3,654.70 | ▼ -0.71 after sell → book $9,673.02; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AEO` | 82 | $14.85 | $2.26 | $+6.98 | $4,870.14 | ▲ +6.98 after sell → book $9,670.76; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AVAV` | 8 | $145.80 | $2.03 | $-4.93 | $6,034.50 | ▼ -4.93 after sell → book $9,668.72; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,034.50 | ▲ close $9,711.08 vs 09:30 $9,679.55 (session +42.36) | 16:00 close · cash $6,034.50 · equity $9,711.08 vs 09:30 $9,679.55 (+31.53; session marks +42.36) · 3 name(s) marked open→close (per-name table). GFR×196 09:30 $6.60 → close $6.62 +3.92; ACVA×116 09:30 $10.42 → close $10.41 -1.16; WLTH×110 09:30 $10.29 → close $10.65 +39.60 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,034.50 | ▼ 09:30 equity $9,700.44 vs yday $9,711.08 (-10.64) | 09:30 open · cash $6,034.50 (unchanged overnight, no fees) · equity $9,700.44 vs prior close $9,711.08 (-10.64) · 3 name(s) re-marked at the open (per-name table). GFR×196 yday $6.62 → 09:30 $6.61 -1.96; ACVA×116 yday $10.41 → 09:30 $10.43 +2.32; WLTH×110 yday $10.65 → 09:30 $10.55 -11.00 | — |
| 2026-09-15 09:30 ET | **SELL** | `GFR` | 196 | $6.61 | $2.62 | $+77.12 | $7,327.44 | ▲ +77.12 after sell → book $9,697.82; vs 09:30 mark -2.62 | dropped from list after 2 sess (min 1) | join🟢 sector🔴 gen🔴 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-15 09:30 ET | **SELL** | `WLTH` | 110 | $10.55 | $2.35 | $-48.67 | $8,485.59 | ▼ -48.67 after sell → book $9,695.47; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,485.59 | ▲ close $9,695.47 vs 09:30 $9,700.44 (session +0.00) | 16:00 close · cash $8,485.59 · equity $9,695.47 vs 09:30 $9,700.44 (-4.97; session marks +0.00) · 1 name(s) marked open→close (per-name table). ACVA×116 09:30 $10.43 → close $10.43 +0.00 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,485.59 | ▲ 09:30 equity $9,696.63 vs yday $9,695.47 (+1.16) | 09:30 open · cash $8,485.59 (unchanged overnight, no fees) · equity $9,696.63 vs prior close $9,695.47 (+1.16) · 1 name(s) re-marked at the open (per-name table). ACVA×116 yday $10.43 → 09:30 $10.44 +1.16 | — |
| 2026-09-16 09:30 ET | **SELL** | `ACVA` | 116 | $10.44 | $2.37 | $-6.45 | $9,694.27 | ▼ -6.45 after sell → book $9,694.27; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 1) | — |
| 2026-09-16 09:30 ET | **BUY** | `TRMD` | 33 | $35.90 | $2.09 | — | $8,507.48 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+2.6; leftover $1211.78 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `PLAY` | 176 | $6.86 | $2.52 | — | $7,297.60 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-22.4; leftover $1211.78 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `BWIN` | 37 | $32.25 | $2.10 | — | $6,102.25 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-3.7; leftover $1211.78 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `ALHC` | 117 | $10.30 | $2.34 | — | $4,894.81 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-23.0; leftover $1211.78 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `VERA` | 36 | $33.22 | $2.10 | — | $3,696.79 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-4.6; leftover $1211.78 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ASND` | 5 | $239.70 | $2.00 | — | $2,496.28 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-11.2; leftover $1211.78 | join🟡 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `HLMN` | 166 | $7.26 | $2.49 | — | $1,288.64 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-1.9; leftover $1211.78 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 36 | $33.14 | $2.10 | — | $93.50 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,oppset; 🔵; ret5=-2.9; leftover $1211.78 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $93.50 | ▼ close $9,564.74 vs 09:30 $9,696.63 (session -111.79) | 16:00 close · cash $93.50 · equity $9,564.74 vs 09:30 $9,696.63 (-131.89; session marks -111.79) · 8 name(s) marked open→close (per-name table). TRMD×33 09:30 $35.90 → close $36.60 +23.10; PLAY×176 09:30 $6.86 → close $6.86 +0.00; BWIN×37 09:30 $32.25 → close $32.04 -7.77; ALHC×117 09:30 $10.30 → close $8.71 -186.03; VERA×36 09:30 $33.22 → close $31.77 -52.20; ASND×5 09:30 $239.70 → close $247.69 +39.95; HLMN×166 09:30 $7.26 → close $7.32 +9.96; FPS×36 09:30 $33.14 → close $34.84 +61.20 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.50 | ▲ 09:30 equity $9,699.87 vs yday $9,564.74 (+135.13) | 09:30 open · cash $93.50 (unchanged overnight, no fees) · equity $9,699.87 vs prior close $9,564.74 (+135.13) · 8 name(s) re-marked at the open (per-name table). TRMD×33 yday $36.60 → 09:30 $36.52 -2.64; PLAY×176 yday $6.86 → 09:30 $6.96 +17.60; BWIN×37 yday $32.04 → 09:30 $32.06 +0.74; ALHC×117 yday $8.71 → 09:30 $8.58 -15.21; VERA×36 yday $31.77 → 09:30 $32.50 +26.28; ASND×5 yday $247.69 → 09:30 $249.23 +7.70; HLMN×166 yday $7.32 → 09:30 $7.51 +31.54; FPS×36 yday $34.84 → 09:30 $36.76 +69.12 | — |
| 2026-09-17 09:30 ET | **SELL** | `TRMD` | 33 | $36.52 | $2.11 | $+16.26 | $1,296.55 | ▲ +16.26 after sell → book $9,697.76; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `PLAY` | 176 | $6.96 | $2.56 | $+12.52 | $2,518.95 | ▲ +12.52 after sell → book $9,695.20; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `VERA` | 36 | $32.50 | $2.12 | $-30.14 | $3,686.83 | ▼ -30.14 after sell → book $9,693.08; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ASND` | 5 | $249.23 | $2.02 | $+43.62 | $4,930.96 | ▲ +43.62 after sell → book $9,691.06; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `HLMN` | 166 | $7.51 | $2.53 | $+36.49 | $6,175.09 | ▲ +36.49 after sell → book $9,688.53; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FPS` | 36 | $36.76 | $2.12 | $+126.10 | $7,496.33 | ▲ +126.10 after sell → book $9,686.41; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `FANG` | 6 | $191.08 | $2.01 | — | $6,347.85 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-4.0; leftover $1249.39 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 55 | $22.46 | $2.15 | — | $5,110.39 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+27.3; leftover $1249.39 | join🟢 sector🟢 gen🟢 news🔴 digest🔴 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 5 | $238.60 | $2.00 | — | $3,915.39 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-11.6; leftover $1249.39 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ALVO` | 239 | $5.22 | $3.08 | — | $2,664.72 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-2.1; leftover $1249.39 | join🔴 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `MNR` | 113 | $11.00 | $2.33 | — | $1,419.39 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=+1.7; leftover $1249.39 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `CBC` | 39 | $31.60 | $2.11 | — | $184.89 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=+1.3; leftover $1249.39 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $184.89 | ▲ close $9,680.23 vs 09:30 $9,699.87 (session +7.50) | 16:00 close · cash $184.89 · equity $9,680.23 vs 09:30 $9,699.87 (-19.64; session marks +7.50) · 8 name(s) marked open→close (per-name table). BWIN×37 09:30 $32.06 → close $31.95 -4.07; ALHC×117 09:30 $8.58 → close $8.70 +14.04; FANG×6 09:30 $191.08 → close $196.94 +35.16; BBNX×55 09:30 $22.46 → close $21.43 -56.65; JBHT×5 09:30 $238.60 → close $236.80 -9.00; ALVO×239 09:30 $5.22 → close $5.34 +28.68; MNR×113 09:30 $11.00 → close $10.97 -3.39; CBC×39 09:30 $31.60 → close $31.67 +2.73 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $184.89 | ▲ 09:30 equity $9,682.76 vs yday $9,680.23 (+2.53) | 09:30 open · cash $184.89 (unchanged overnight, no fees) · equity $9,682.76 vs prior close $9,680.23 (+2.53) · 8 name(s) re-marked at the open (per-name table). BWIN×37 yday $31.95 → 09:30 $31.98 +1.11; ALHC×117 yday $8.70 → 09:30 $8.68 -2.34; FANG×6 yday $196.94 → 09:30 $196.94 +0.00; BBNX×55 yday $21.43 → 09:30 $21.30 -7.15; JBHT×5 yday $236.80 → 09:30 $236.80 +0.00; ALVO×239 yday $5.34 → 09:30 $5.40 +14.34; MNR×113 yday $10.97 → 09:30 $10.95 -2.26; CBC×39 yday $31.67 → 09:30 $31.64 -1.17 | — |
| 2026-09-18 09:30 ET | **SELL** | `BWIN` | 37 | $31.98 | $2.12 | $-14.21 | $1,366.03 | ▼ -14.21 after sell → book $9,680.64; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 1) | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `FANG` | 6 | $196.94 | $2.03 | $+31.12 | $2,545.64 | ▲ +31.12 after sell → book $9,678.61; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 55 | $21.30 | $2.17 | $-68.13 | $3,714.96 | ▼ -68.13 after sell → book $9,676.43; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 5 | $236.80 | $2.02 | $-13.03 | $4,896.94 | ▼ -13.03 after sell → book $9,674.41; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ALVO` | 239 | $5.40 | $3.13 | $+36.80 | $6,184.41 | ▲ +36.80 after sell → book $9,671.28; vs 09:30 mark -3.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `MNR` | 113 | $10.95 | $2.36 | $-10.34 | $7,419.40 | ▼ -10.34 after sell → book $9,668.92; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `CBC` | 39 | $31.64 | $2.13 | $-2.67 | $8,651.23 | ▼ -2.67 after sell → book $9,666.79; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 5 | $209.52 | $2.00 | — | $7,601.63 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+14.1; leftover $1235.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 42 | $29.32 | $2.12 | — | $6,368.07 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $1235.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `QSR` | 16 | $73.00 | $2.04 | — | $5,198.03 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+0.5; leftover $1235.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 164 | $7.54 | $2.48 | — | $3,959.81 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; 🔵; ret5=-20.9; leftover $1235.89 | join🟡 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `GBTG` | 130 | $9.46 | $2.38 | — | $2,727.63 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=+0.2; leftover $1235.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `HLN` | 134 | $9.20 | $2.39 | — | $1,492.44 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-7.8; leftover $1235.89 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `ACVA` | 117 | $10.48 | $2.34 | — | $263.94 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.0; leftover $1235.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $263.94 | ▼ close $9,562.97 vs 09:30 $9,682.76 (session -88.07) | 16:00 close · cash $263.94 · equity $9,562.97 vs 09:30 $9,682.76 (-119.79; session marks -88.07) · 8 name(s) marked open→close (per-name table). ALHC×117 09:30 $8.68 → close $8.35 -38.61; GNRC×5 09:30 $209.52 → close $207.44 -10.40; SDGR×42 09:30 $29.32 → close $29.02 -12.60; QSR×16 09:30 $73.00 → close $72.88 -1.92; FLNC×164 09:30 $7.54 → close $7.32 -35.26; GBTG×130 09:30 $9.46 → close $9.46 +0.00; HLN×134 09:30 $9.20 → close $9.28 +10.72; ACVA×117 09:30 $10.48 → close $10.48 +0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `RDDT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OCUL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HDSN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KRNY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `EYPT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WBS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `LBRDK` | no_price | no 09:30 open — carry |
| 2026-08-24 | `LBRDK` | no_price | no 09:30 open — carry |
| 2026-08-24 | `USDE` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AMX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `IQMX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `WSC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `LBRDK` | no_price | no 09:30 open — carry |
| 2026-08-25 | `TWO` | no_price | no 09:30 open |
| 2026-08-26 | `LBRDK` | no_price | no 09:30 open — carry |
| 2026-08-27 | `LBRDK` | no_price | no 09:30 open — carry |
| 2026-08-31 | `LEG` | no_price | no 09:30 open — carry |
| 2026-08-31 | `AFRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SOLS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PCG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ESI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ESTC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SUNB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BEKE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `LEG` | no_price | no 09:30 open — carry |
| 2026-09-01 | `GGG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RPM` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AVY` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRMB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `EIX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `GLPI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `LEG` | no_price | no 09:30 open — carry |
| 2026-09-02 | `ALMS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EIX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PCG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MMED` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HASI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CRK` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUNB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `LEG` | no_price | no 09:30 open — carry |
| 2026-09-08 | `SUNB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BILI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `IOT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `IMO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `PHVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ATAI` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GPRK` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DYN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `WAFD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GPCR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HWM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TTAN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TYRA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INGM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `KHC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TENB` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `INGM` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DSGX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RCUS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `BWIN` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `BZ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RUM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `DV` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `AYA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `BNC` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ALHC` | 117 | 2026-09-16 @ $10.30 | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-23.0; leftover $1211.78 |
| `GNRC` | 5 | 2026-09-18 @ $209.52 | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+14.1; leftover $1235.89 |
| `SDGR` | 42 | 2026-09-18 @ $29.32 | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $1235.89 |
| `QSR` | 16 | 2026-09-18 @ $73.00 | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+0.5; leftover $1235.89 |
| `FLNC` | 164 | 2026-09-18 @ $7.54 | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; 🔵; ret5=-20.9; leftover $1235.89 |
| `GBTG` | 130 | 2026-09-18 @ $9.46 | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=+0.2; leftover $1235.89 |
| `HLN` | 134 | 2026-09-18 @ $9.20 | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-7.8; leftover $1235.89 |
| `ACVA` | 117 | 2026-09-18 @ $10.48 | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.0; leftover $1235.89 |
