# Factor mine action — `union_oppset_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `opp_rvol` · size `leftover` · sell `list` · S-boost `none` · Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP)

Cash book **-4.51%** ($9,549) · signal-only (no cash/fees) was +3.68%. Starts YES **1/26**. Fills 188 · skips 94 · realized $-157.96.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $265.19.

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
| 2026-08-14 | `EROC` | 77 | — | $16.05 | +0.00 | $16.72 | +51.59 | +51.59 | +0.00 | +51.59 |
| 2026-08-14 | `TBBB` | 25 | — | $48.82 | +0.00 | $47.79 | -25.75 | -25.75 | +0.00 | -25.75 |
| 2026-08-14 | `FA` | 56 | — | $22.15 | +0.00 | $21.58 | -31.92 | -31.92 | +0.00 | -31.92 |
| 2026-08-14 | `ZIM` | 45 | — | $27.25 | +0.00 | $28.14 | +40.05 | +40.05 | +0.00 | +40.05 |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | — | +0.00 | -0.63 | +0.00 | — |
| 2026-08-17 | `CLBT` | 115 | $11.14 | $11.19 | +5.75 | — | +0.00 | +5.75 | +41.40 | — |
| 2026-08-17 | `AIRO` | 112 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -173.60 | — |
| 2026-08-17 | `SECZ` | 214 | $5.61 | $5.45 | -34.24 | — | +0.00 | -34.24 | -83.46 | — |
| 2026-08-17 | `EROC` | 77 | $16.72 | $17.15 | +33.11 | — | +0.00 | +33.11 | +84.70 | — |
| 2026-08-17 | `TBBB` | 25 | $47.79 | $47.39 | -10.00 | — | +0.00 | -10.00 | -35.75 | — |
| 2026-08-17 | `FA` | 56 | $21.58 | $21.35 | -12.88 | — | +0.00 | -12.88 | -44.80 | — |
| 2026-08-17 | `ZIM` | 45 | $28.14 | $28.83 | +31.05 | — | +0.00 | +31.05 | +71.10 | — |
| 2026-08-17 | `CAPR` | 178 | — | $6.87 | +0.00 | $7.45 | +103.24 | +103.24 | +0.00 | +103.24 |
| 2026-08-17 | `HTFL` | 29 | — | $41.23 | +0.00 | $41.94 | +20.59 | +20.59 | +0.00 | +20.59 |
| 2026-08-17 | `NMAX` | 111 | — | $10.97 | +0.00 | $10.36 | -67.71 | -67.71 | +0.00 | -67.71 |
| 2026-08-17 | `CHEF` | 11 | — | $108.18 | +0.00 | $110.57 | +26.29 | +26.29 | +0.00 | +26.29 |
| 2026-08-17 | `VIV` | 106 | — | $11.55 | +0.00 | $11.40 | -15.90 | -15.90 | +0.00 | -15.90 |
| 2026-08-17 | `RDDT` | 6 | — | $177.51 | +0.00 | $164.50 | -78.06 | -78.06 | +0.00 | -78.06 |
| 2026-08-17 | `DIOD` | 11 | — | $104.00 | +0.00 | $107.56 | +39.16 | +39.16 | +0.00 | +39.16 |
| 2026-08-17 | `YSS` | 118 | — | $10.36 | +0.00 | $10.66 | +35.40 | +35.40 | +0.00 | +35.40 |
| 2026-08-18 | `CAPR` | 178 | $7.45 | $7.50 | +8.90 | $7.08 | -74.76 | -65.86 | +112.14 | +37.38 |
| 2026-08-18 | `HTFL` | 29 | $41.94 | $41.50 | -12.76 | — | +0.00 | -12.76 | +7.83 | — |
| 2026-08-18 | `NMAX` | 111 | $10.36 | $10.31 | -5.55 | — | +0.00 | -5.55 | -73.26 | — |
| 2026-08-18 | `CHEF` | 11 | $110.57 | $110.91 | +3.74 | — | +0.00 | +3.74 | +30.03 | — |
| 2026-08-18 | `VIV` | 106 | $11.40 | $11.45 | +5.30 | — | +0.00 | +5.30 | -10.60 | — |
| 2026-08-18 | `RDDT` | 6 | $164.50 | $166.10 | +9.60 | — | +0.00 | +9.60 | -68.46 | — |
| 2026-08-18 | `DIOD` | 11 | $107.56 | $103.01 | -50.05 | — | +0.00 | -50.05 | -10.89 | — |
| 2026-08-18 | `YSS` | 118 | $10.66 | $10.24 | -49.56 | — | +0.00 | -49.56 | -14.16 | — |
| 2026-08-19 | `CAPR` | 178 | $7.08 | $7.19 | +19.58 | — | +0.00 | +19.58 | +56.96 | — |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `WBS` | 15 | — | $77.57 | +0.00 | $77.57 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `ALH` | 51 | — | $23.72 | +0.00 | $23.18 | -27.54 | -27.54 | +0.00 | -27.54 |
| 2026-08-20 | `LBRDK` | 33 | — | $36.02 | +0.00 | $36.02 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `SUI` | 10 | — | $121.21 | +0.00 | $122.29 | +10.80 | +10.80 | +0.00 | +10.80 |
| 2026-08-20 | `NTST` | 59 | — | $20.47 | +0.00 | $20.61 | +8.26 | +8.26 | +0.00 | +8.26 |
| 2026-08-20 | `BNTX` | 11 | — | $109.06 | +0.00 | $110.89 | +20.13 | +20.13 | +0.00 | +20.13 |
| 2026-08-20 | `ADC` | 16 | — | $74.37 | +0.00 | $74.45 | +1.28 | +1.28 | +0.00 | +1.28 |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | $145.13 | +96.16 | +94.48 | -136.24 | -40.08 |
| 2026-08-21 | `WBS` | 15 | $77.57 | $77.57 | +0.00 | — | +0.00 | +0.00 | +0.00 | — |
| 2026-08-21 | `ALH` | 51 | $23.18 | $23.33 | +7.65 | $23.47 | +7.14 | +14.79 | -19.89 | -12.75 |
| 2026-08-21 | `LBRDK` | 33 | $36.02 | $36.02 | +0.00 | $36.02 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-21 | `SUI` | 10 | $122.29 | $122.41 | +1.20 | — | +0.00 | +1.20 | +12.00 | — |
| 2026-08-21 | `NTST` | 59 | $20.61 | $20.66 | +2.95 | — | +0.00 | +2.95 | +11.21 | — |
| 2026-08-21 | `BNTX` | 11 | $110.89 | $110.92 | +0.33 | — | +0.00 | +0.33 | +20.46 | — |
| 2026-08-21 | `ADC` | 16 | $74.45 | $74.60 | +2.40 | — | +0.00 | +2.40 | +3.68 | — |
| 2026-08-21 | `AAP` | 24 | — | $42.41 | +0.00 | $42.58 | +4.08 | +4.08 | +0.00 | +4.08 |
| 2026-08-21 | `MFC` | 24 | — | $42.48 | +0.00 | $42.51 | +0.72 | +0.72 | +0.00 | +0.72 |
| 2026-08-21 | `MRVI` | 123 | — | $8.28 | +0.00 | $8.64 | +44.28 | +44.28 | +0.00 | +44.28 |
| 2026-08-21 | `BULL` | 113 | — | $8.99 | +0.00 | $8.78 | -23.73 | -23.73 | +0.00 | -23.73 |
| 2026-08-21 | `SGRY` | 72 | — | $14.10 | +0.00 | $14.52 | +30.24 | +30.24 | +0.00 | +30.24 |
| 2026-08-21 | `ARCT` | 91 | — | $11.13 | +0.00 | $13.45 | +211.12 | +211.12 | +0.00 | +211.12 |
| 2026-08-24 | `MRNA` | 8 | $145.13 | $142.70 | -19.44 | — | +0.00 | -19.44 | -59.52 | — |
| 2026-08-24 | `ALH` | 51 | $23.47 | $23.66 | +9.69 | — | +0.00 | +9.69 | -3.06 | — |
| 2026-08-24 | `LBRDK` | 33 | $36.02 | $36.02 | +0.00 | $36.02 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-24 | `AAP` | 24 | $42.58 | $43.05 | +11.28 | — | +0.00 | +11.28 | +15.36 | — |
| 2026-08-24 | `MFC` | 24 | $42.51 | $42.31 | -4.80 | — | +0.00 | -4.80 | -4.08 | — |
| 2026-08-24 | `MRVI` | 123 | $8.64 | $8.59 | -6.15 | — | +0.00 | -6.15 | +38.13 | — |
| 2026-08-24 | `BULL` | 113 | $8.78 | $8.58 | -22.60 | — | +0.00 | -22.60 | -46.33 | — |
| 2026-08-24 | `SGRY` | 72 | $14.52 | $14.55 | +2.16 | — | +0.00 | +2.16 | +32.40 | — |
| 2026-08-24 | `ARCT` | 91 | $13.45 | $13.33 | -10.92 | — | +0.00 | -10.92 | +200.20 | — |
| 2026-08-25 | `LBRDK` | 33 | $36.02 | $36.02 | +0.00 | $36.02 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `ABUS` | 206 | — | $5.25 | +0.00 | $5.20 | -10.30 | -10.30 | +0.00 | -10.30 |
| 2026-08-25 | `ZURA` | 170 | — | $6.37 | +0.00 | $6.32 | -8.50 | -8.50 | +0.00 | -8.50 |
| 2026-08-25 | `ALH` | 44 | — | $24.16 | +0.00 | $23.85 | -13.64 | -13.64 | +0.00 | -13.64 |
| 2026-08-25 | `CAPR` | 149 | — | $7.25 | +0.00 | $8.29 | +154.96 | +154.96 | +0.00 | +154.96 |
| 2026-08-25 | `FWDI` | 189 | — | $5.71 | +0.00 | $6.05 | +64.26 | +64.26 | +0.00 | +64.26 |
| 2026-08-25 | `ARCT` | 76 | — | $14.12 | +0.00 | $15.44 | +100.32 | +100.32 | +0.00 | +100.32 |
| 2026-08-25 | `XPEV` | 96 | — | $11.19 | +0.00 | $11.60 | +39.84 | +39.84 | +0.00 | +39.84 |
| 2026-08-26 | `LBRDK` | 33 | $36.02 | $36.02 | +0.00 | $36.02 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `ABUS` | 206 | $5.20 | $5.19 | -2.06 | — | +0.00 | -2.06 | -12.36 | — |
| 2026-08-26 | `ZURA` | 170 | $6.32 | $6.13 | -32.30 | — | +0.00 | -32.30 | -40.80 | — |
| 2026-08-26 | `ALH` | 44 | $23.85 | $24.00 | +6.60 | — | +0.00 | +6.60 | -7.04 | — |
| 2026-08-26 | `CAPR` | 149 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | +154.96 | — |
| 2026-08-26 | `FWDI` | 189 | $6.05 | $5.97 | -15.12 | — | +0.00 | -15.12 | +49.14 | — |
| 2026-08-26 | `ARCT` | 76 | $15.44 | $15.35 | -6.84 | — | +0.00 | -6.84 | +93.48 | — |
| 2026-08-26 | `XPEV` | 96 | $11.60 | $11.90 | +28.80 | — | +0.00 | +28.80 | +68.64 | — |
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
| 2026-08-28 | `BZ` | 68 | — | $18.15 | +0.00 | $17.80 | -23.80 | -23.80 | +0.00 | -23.80 |
| 2026-08-28 | `QFIN` | 136 | — | $9.15 | +0.00 | $8.80 | -47.60 | -47.60 | +0.00 | -47.60 |
| 2026-08-28 | `LEG` | 136 | — | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-28 | `BHVN` | 78 | — | $15.88 | +0.00 | $15.41 | -36.66 | -36.66 | +0.00 | -36.66 |
| 2026-08-28 | `PLAB` | 41 | — | $30.01 | +0.00 | $27.73 | -93.48 | -93.48 | +0.00 | -93.48 |
| 2026-08-28 | `DY` | 4 | — | $306.34 | +0.00 | $294.34 | -48.00 | -48.00 | +0.00 | -48.00 |
| 2026-08-28 | `GENB` | 79 | — | $15.77 | +0.00 | $15.33 | -34.76 | -34.76 | +0.00 | -34.76 |
| 2026-08-31 | `ANF` | 8 | $148.42 | $148.03 | -3.12 | — | +0.00 | -3.12 | +15.68 | — |
| 2026-08-31 | `BZ` | 68 | $17.80 | $17.70 | -6.80 | — | +0.00 | -6.80 | -30.60 | — |
| 2026-08-31 | `QFIN` | 136 | $8.80 | $8.70 | -13.60 | — | +0.00 | -13.60 | -61.20 | — |
| 2026-08-31 | `LEG` | 136 | $9.20 | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-31 | `BHVN` | 78 | $15.41 | $15.46 | +3.90 | — | +0.00 | +3.90 | -32.76 | — |
| 2026-08-31 | `PLAB` | 41 | $27.73 | $28.04 | +12.71 | — | +0.00 | +12.71 | -80.77 | — |
| 2026-08-31 | `DY` | 4 | $294.34 | $298.01 | +14.68 | — | +0.00 | +14.68 | -33.32 | — |
| 2026-08-31 | `GENB` | 79 | $15.33 | $15.27 | -4.74 | — | +0.00 | -4.74 | -39.50 | — |
| 2026-09-01 | `LEG` | 136 | $9.20 | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-02 | `LEG` | 136 | $9.20 | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `LEG` | 136 | $9.20 | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `GBTG` | 111 | — | $9.49 | +0.00 | $9.49 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `ALMS` | 102 | — | $10.38 | +0.00 | $11.36 | +100.47 | +100.47 | +0.00 | +100.47 |
| 2026-09-03 | `DAKT` | 55 | — | $19.08 | +0.00 | $19.48 | +22.00 | +22.00 | +0.00 | +22.00 |
| 2026-09-03 | `GTLB` | 21 | — | $49.98 | +0.00 | $49.31 | -14.07 | -14.07 | +0.00 | -14.07 |
| 2026-09-03 | `FRVO` | 57 | — | $18.28 | +0.00 | $17.16 | -63.84 | -63.84 | +0.00 | -63.84 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `MDB` | 2 | — | $378.76 | +0.00 | $384.45 | +11.38 | +11.38 | +0.00 | +11.38 |
| 2026-09-03 | `OABI` | 208 | — | $5.08 | +0.00 | $4.75 | -68.64 | -68.64 | +0.00 | -68.64 |
| 2026-09-04 | `LEG` | 136 | $9.20 | $9.20 | +0.00 | — | +0.00 | +0.00 | +0.00 | — |
| 2026-09-04 | `GBTG` | 111 | $9.49 | $9.49 | +0.00 | — | +0.00 | +0.00 | +0.00 | — |
| 2026-09-04 | `ALMS` | 102 | $11.36 | $11.23 | -13.26 | $11.10 | -13.26 | -26.52 | +87.21 | +73.95 |
| 2026-09-04 | `DAKT` | 55 | $19.48 | $19.47 | -0.55 | — | +0.00 | -0.55 | +21.45 | — |
| 2026-09-04 | `GTLB` | 21 | $49.31 | $48.94 | -7.77 | — | +0.00 | -7.77 | -21.84 | — |
| 2026-09-04 | `FRVO` | 57 | $17.16 | $17.27 | +6.27 | — | +0.00 | +6.27 | -57.57 | — |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | — | +0.00 | -5.22 | +54.94 | — |
| 2026-09-04 | `MDB` | 2 | $384.45 | $378.34 | -12.22 | — | +0.00 | -12.22 | -0.84 | — |
| 2026-09-04 | `OABI` | 208 | $4.75 | $4.78 | +6.24 | — | +0.00 | +6.24 | -62.40 | — |
| 2026-09-04 | `CHPT` | 131 | — | $9.28 | +0.00 | $9.89 | +79.91 | +79.91 | +0.00 | +79.91 |
| 2026-09-04 | `RARE` | 79 | — | $15.47 | +0.00 | $15.30 | -13.82 | -13.82 | +0.00 | -13.82 |
| 2026-09-04 | `DPRO` | 192 | — | $6.36 | +0.00 | $6.12 | -46.08 | -46.08 | +0.00 | -46.08 |
| 2026-09-04 | `CPB` | 55 | — | $22.10 | +0.00 | $21.38 | -39.60 | -39.60 | +0.00 | -39.60 |
| 2026-09-04 | `LULU` | 12 | — | $98.15 | +0.00 | $100.61 | +29.52 | +29.52 | +0.00 | +29.52 |
| 2026-09-04 | `VSXY` | 16 | — | $73.63 | +0.00 | $75.56 | +30.88 | +30.88 | +0.00 | +30.88 |
| 2026-09-04 | `CLYM` | 84 | — | $14.49 | +0.00 | $15.52 | +86.52 | +86.52 | +0.00 | +86.52 |
| 2026-09-08 | `ALMS` | 102 | $11.10 | $11.05 | -5.10 | — | +0.00 | -5.10 | +68.85 | — |
| 2026-09-08 | `CHPT` | 131 | $9.89 | $9.91 | +2.62 | $9.37 | -70.74 | -68.12 | +82.53 | +11.79 |
| 2026-09-08 | `RARE` | 79 | $15.30 | $15.10 | -16.04 | — | +0.00 | -16.04 | -29.86 | — |
| 2026-09-08 | `DPRO` | 192 | $6.12 | $6.07 | -9.60 | — | +0.00 | -9.60 | -55.68 | — |
| 2026-09-08 | `CPB` | 55 | $21.38 | $21.30 | -4.40 | — | +0.00 | -4.40 | -44.00 | — |
| 2026-09-08 | `LULU` | 12 | $100.61 | $100.58 | -0.36 | $103.19 | +31.32 | +30.96 | +29.16 | +60.48 |
| 2026-09-08 | `VSXY` | 16 | $75.56 | $73.51 | -32.80 | — | +0.00 | -32.80 | -1.92 | — |
| 2026-09-08 | `CLYM` | 84 | $15.52 | $15.62 | +8.40 | — | +0.00 | +8.40 | +94.92 | — |
| 2026-09-09 | `CHPT` | 131 | $9.37 | $9.39 | +2.62 | — | +0.00 | +2.62 | +14.41 | — |
| 2026-09-09 | `LULU` | 12 | $103.19 | $101.90 | -15.48 | — | +0.00 | -15.48 | +45.00 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `BKV` | 48 | — | $24.97 | +0.00 | $24.23 | -35.52 | -35.52 | +0.00 | -35.52 |
| 2026-09-11 | `COO` | 22 | — | $54.66 | +0.00 | $53.91 | -16.50 | -16.50 | +0.00 | -16.50 |
| 2026-09-11 | `GFR` | 195 | — | $6.19 | +0.00 | $6.52 | +64.35 | +64.35 | +0.00 | +64.35 |
| 2026-09-11 | `SHOE` | 96 | — | $12.51 | +0.00 | $12.71 | +19.20 | +19.20 | +0.00 | +19.20 |
| 2026-09-11 | `AEO` | 82 | — | $14.71 | +0.00 | $15.02 | +25.42 | +25.42 | +0.00 | +25.42 |
| 2026-09-11 | `ACVA` | 115 | — | $10.46 | +0.00 | $10.41 | -5.17 | -5.17 | +0.00 | -5.17 |
| 2026-09-11 | `AVAV` | 8 | — | $145.91 | +0.00 | $146.71 | +6.40 | +6.40 | +0.00 | +6.40 |
| 2026-09-11 | `WLTH` | 110 | — | $10.95 | +0.00 | $10.38 | -62.70 | -62.70 | +0.00 | -62.70 |
| 2026-09-14 | `BKV` | 48 | $24.23 | $24.26 | +1.44 | — | +0.00 | +1.44 | -34.08 | — |
| 2026-09-14 | `COO` | 22 | $53.91 | $54.78 | +19.14 | — | +0.00 | +19.14 | +2.64 | — |
| 2026-09-14 | `GFR` | 195 | $6.52 | $6.60 | +15.60 | $6.62 | +3.90 | +19.50 | +79.95 | +83.85 |
| 2026-09-14 | `SHOE` | 96 | $12.71 | $12.55 | -15.36 | — | +0.00 | -15.36 | +3.84 | — |
| 2026-09-14 | `AEO` | 82 | $15.02 | $14.85 | -13.94 | — | +0.00 | -13.94 | +11.48 | — |
| 2026-09-14 | `ACVA` | 115 | $10.41 | $10.42 | +1.15 | — | +0.00 | +1.15 | -4.03 | — |
| 2026-09-14 | `AVAV` | 8 | $146.71 | $145.80 | -7.28 | — | +0.00 | -7.28 | -0.88 | — |
| 2026-09-14 | `WLTH` | 110 | $10.38 | $10.29 | -9.90 | — | +0.00 | -9.90 | -72.60 | — |
| 2026-09-15 | `GFR` | 195 | $6.62 | $6.61 | -1.95 | — | +0.00 | -1.95 | +81.90 | — |
| 2026-09-16 | `TRMD` | 33 | — | $35.90 | +0.00 | $36.60 | +23.10 | +23.10 | +0.00 | +23.10 |
| 2026-09-16 | `PLAY` | 175 | — | $6.86 | +0.00 | $6.86 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-16 | `BWIN` | 37 | — | $32.25 | +0.00 | $32.04 | -7.77 | -7.77 | +0.00 | -7.77 |
| 2026-09-16 | `ALHC` | 116 | — | $10.30 | +0.00 | $8.71 | -184.44 | -184.44 | +0.00 | -184.44 |
| 2026-09-16 | `VERA` | 36 | — | $33.22 | +0.00 | $31.77 | -52.20 | -52.20 | +0.00 | -52.20 |
| 2026-09-16 | `ASND` | 5 | — | $239.70 | +0.00 | $247.69 | +39.95 | +39.95 | +0.00 | +39.95 |
| 2026-09-16 | `HLMN` | 165 | — | $7.26 | +0.00 | $7.32 | +9.90 | +9.90 | +0.00 | +9.90 |
| 2026-09-16 | `FPS` | 36 | — | $33.14 | +0.00 | $34.84 | +61.20 | +61.20 | +0.00 | +61.20 |
| 2026-09-17 | `TRMD` | 33 | $36.60 | $36.52 | -2.64 | — | +0.00 | -2.64 | +20.46 | — |
| 2026-09-17 | `PLAY` | 175 | $6.86 | $6.96 | +17.50 | — | +0.00 | +17.50 | +17.50 | — |
| 2026-09-17 | `BWIN` | 37 | $32.04 | $32.06 | +0.74 | — | +0.00 | +0.74 | -7.03 | — |
| 2026-09-17 | `ALHC` | 116 | $8.71 | $8.58 | -15.08 | $8.70 | +13.92 | -1.16 | -199.52 | -185.60 |
| 2026-09-17 | `VERA` | 36 | $31.77 | $32.50 | +26.28 | — | +0.00 | +26.28 | -25.92 | — |
| 2026-09-17 | `ASND` | 5 | $247.69 | $249.23 | +7.70 | — | +0.00 | +7.70 | +47.65 | — |
| 2026-09-17 | `HLMN` | 165 | $7.32 | $7.51 | +31.35 | — | +0.00 | +31.35 | +41.25 | — |
| 2026-09-17 | `FPS` | 36 | $34.84 | $36.76 | +69.12 | — | +0.00 | +69.12 | +130.32 | — |
| 2026-09-17 | `FANG` | 6 | — | $191.08 | +0.00 | $196.94 | +35.16 | +35.16 | +0.00 | +35.16 |
| 2026-09-17 | `BBNX` | 54 | — | $22.46 | +0.00 | $21.43 | -55.62 | -55.62 | +0.00 | -55.62 |
| 2026-09-17 | `JBHT` | 5 | — | $238.60 | +0.00 | $236.80 | -9.00 | -9.00 | +0.00 | -9.00 |
| 2026-09-17 | `ALVO` | 236 | — | $5.22 | +0.00 | $5.34 | +28.32 | +28.32 | +0.00 | +28.32 |
| 2026-09-17 | `CBC` | 39 | — | $31.60 | +0.00 | $31.67 | +2.73 | +2.73 | +0.00 | +2.73 |
| 2026-09-17 | `WWD` | 3 | — | $329.36 | +0.00 | $319.56 | -29.40 | -29.40 | +0.00 | -29.40 |
| 2026-09-17 | `ASAN` | 129 | — | $9.55 | +0.00 | $10.09 | +69.66 | +69.66 | +0.00 | +69.66 |
| 2026-09-18 | `ALHC` | 116 | $8.70 | $8.68 | -2.32 | $8.35 | -38.28 | -40.60 | -187.92 | -226.20 |
| 2026-09-18 | `FANG` | 6 | $196.94 | $196.94 | +0.00 | — | +0.00 | +0.00 | +35.16 | — |
| 2026-09-18 | `BBNX` | 54 | $21.43 | $21.30 | -7.02 | — | +0.00 | -7.02 | -62.64 | — |
| 2026-09-18 | `JBHT` | 5 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -9.00 | — |
| 2026-09-18 | `ALVO` | 236 | $5.34 | $5.40 | +14.16 | — | +0.00 | +14.16 | +42.48 | — |
| 2026-09-18 | `CBC` | 39 | $31.67 | $31.64 | -1.17 | — | +0.00 | -1.17 | +1.56 | — |
| 2026-09-18 | `WWD` | 3 | $319.56 | $320.02 | +1.38 | — | +0.00 | +1.38 | -28.02 | — |
| 2026-09-18 | `ASAN` | 129 | $10.09 | $10.09 | +0.00 | — | +0.00 | +0.00 | +69.66 | — |
| 2026-09-18 | `GNRC` | 5 | — | $209.52 | +0.00 | $207.44 | -10.40 | -10.40 | +0.00 | -10.40 |
| 2026-09-18 | `SDGR` | 42 | — | $29.32 | +0.00 | $29.02 | -12.60 | -12.60 | +0.00 | -12.60 |
| 2026-09-18 | `QSR` | 16 | — | $73.00 | +0.00 | $72.88 | -1.92 | -1.92 | +0.00 | -1.92 |
| 2026-09-18 | `FLNC` | 163 | — | $7.54 | +0.00 | $7.32 | -35.04 | -35.04 | +0.00 | -35.04 |
| 2026-09-18 | `GBTG` | 130 | — | $9.46 | +0.00 | $9.46 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-18 | `HLN` | 134 | — | $9.20 | +0.00 | $9.28 | +10.72 | +10.72 | +0.00 | +10.72 |
| 2026-09-18 | `ACVA` | 117 | — | $10.48 | +0.00 | $10.48 | +0.00 | +0.00 | +0.00 | +0.00 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -152.57 | ARX, CLBT, AIRO, SECZ, EROC, TBBB, FA, ZIM | — | $85.27 | $9,829.26 | ARX×63, CLBT×115, AIRO×112, SECZ×214, EROC×77, TBBB×25, FA×56, ZIM×45 |
| 2026-08-17 | +2.25 | $85.27 | ARX×63, CLBT×115, AIRO×112, SECZ×214, EROC×77, TBBB×25, FA×56, ZIM×45 | $9,841.42 | +12.16 | +63.01 | CAPR, HTFL, NMAX, CHEF, VIV, RDDT, DIOD, YSS | ARX, CLBT, AIRO, SECZ, EROC, TBBB, FA, ZIM | $323.39 | $9,868.42 | CAPR×178, HTFL×29, NMAX×111, CHEF×11, VIV×106, RDDT×6, DIOD×11, YSS×118 |
| 2026-08-18 | -6.20 | $323.39 | CAPR×178, HTFL×29, NMAX×111, CHEF×11, VIV×106, RDDT×6, DIOD×11, YSS×118 | $9,778.04 | -90.38 | -74.76 | — | HTFL, NMAX, CHEF, VIV, RDDT, DIOD, YSS | $8,427.77 | $9,688.01 | CAPR×178 |
| 2026-08-19 | -7.20 | $8,427.77 | CAPR×178 | $9,707.59 | +19.58 | +0.00 | — | CAPR | $9,705.03 | $9,705.03 | — |
| 2026-08-20 | +1.12 | $9,705.03 | — | $9,705.03 | -0.00 | -121.63 | MRNA, WBS, ALH, LBRDK, SUI, NTST, BNTX, ADC | — | $116.04 | $9,566.87 | MRNA×8, WBS×15, ALH×51, LBRDK×33, SUI×10, NTST×59, BNTX×11, ADC×16 |
| 2026-08-21 | +3.25 | $116.04 | MRNA×8, WBS×15, ALH×51, LBRDK×33, SUI×10, NTST×59, BNTX×11, ADC×16 | $9,579.72 | +12.85 | +370.01 | AAP, MFC, MRVI, BULL, SGRY, ARCT | WBS, SUI, NTST, BNTX, ADC | $12.98 | $9,926.06 | MRNA×8, ALH×51, LBRDK×33, AAP×24, MFC×24, MRVI×123, BULL×113, SGRY×72, ARCT×91 |
| 2026-08-24 | -5.17 | $12.98 | MRNA×8, ALH×51, LBRDK×33, AAP×24, MFC×24, MRVI×123, BULL×113, SGRY×72, ARCT×91 | $9,885.28 | -40.78 | +0.00 | — | MRNA, ALH, AAP, MFC, MRVI, BULL, SGRY, ARCT | $8,679.00 | $9,867.66 | LBRDK×33 |
| 2026-08-25 | +1.80 | $8,679.00 | LBRDK×33 | $9,867.66 | +0.00 | +326.94 | ABUS, ZURA, ALH, CAPR, FWDI, ARCT, XPEV | — | $1,128.47 | $10,177.83 | LBRDK×33, ABUS×206, ZURA×170, ALH×44, CAPR×149, FWDI×189, ARCT×76, XPEV×96 |
| 2026-08-26 | +2.02 | $1,128.47 | LBRDK×33, ABUS×206, ZURA×170, ALH×44, CAPR×149, FWDI×189, ARCT×76, XPEV×96 | $10,156.91 | -20.92 | -57.94 | DKS, SLF, QFIN, GENB, VIPS, BZ, MRNA, ATHM | ABUS, ZURA, ALH, CAPR, FWDI, ARCT, XPEV | $114.92 | $10,064.87 | LBRDK×33, DKS×9, SLF×14, QFIN×114, GENB×55, VIPS×79, BZ×66, MRNA×7, ATHM×51 |
| 2026-08-27 | — | $114.92 | LBRDK×33, DKS×9, SLF×14, QFIN×114, GENB×55, VIPS×79, BZ×66, MRNA×7, ATHM×51 | $10,037.72 | -27.15 | +0.00 | — | DKS, SLF, QFIN, GENB, VIPS, BZ, MRNA, ATHM | $8,831.78 | $10,020.44 | LBRDK×33 |
| 2026-08-28 | +0.75 | $8,831.78 | LBRDK×33 | $10,020.44 | +0.00 | -265.50 | ANF, BZ, QFIN, LEG, BHVN, PLAB, DY, GENB | LBRDK | $162.16 | $9,735.26 | ANF×8, BZ×68, QFIN×136, LEG×136, BHVN×78, PLAB×41, DY×4, GENB×79 |
| 2026-08-31 | -5.85 | $162.16 | ANF×8, BZ×68, QFIN×136, LEG×136, BHVN×78, PLAB×41, DY×4, GENB×79 | $9,738.29 | +3.03 | +0.00 | — | ANF, BZ, QFIN, BHVN, PLAB, DY, GENB | $8,471.76 | $9,722.96 | LEG×136 |
| 2026-09-01 | -6.30 | $8,471.76 | LEG×136 | $9,722.96 | +0.00 | +0.00 | — | — | $8,471.76 | $9,722.96 | LEG×136 |
| 2026-09-02 | -3.83 | $8,471.76 | LEG×136 | $9,722.96 | +0.00 | +0.00 | — | — | $8,471.76 | $9,722.96 | LEG×136 |
| 2026-09-03 | -0.90 | $8,471.76 | LEG×136 | $9,722.96 | +0.00 | +47.46 | GBTG, ALMS, DAKT, GTLB, FRVO, DELL, MDB, OABI | — | $414.74 | $9,752.76 | LEG×136, GBTG×111, ALMS×102, DAKT×55, GTLB×21, FRVO×57, DELL×2, MDB×2, OABI×208 |
| 2026-09-04 | +2.25 | $414.74 | LEG×136, GBTG×111, ALMS×102, DAKT×55, GTLB×21, FRVO×57, DELL×2, MDB×2, OABI×208 | $9,726.25 | -26.51 | +114.07 | CHPT, RARE, DPRO, CPB, LULU, VSXY, CLYM | LEG, GBTG, DAKT, GTLB, FRVO, DELL, MDB, OABI | $99.31 | $9,806.70 | ALMS×102, CHPT×131, RARE×79, DPRO×192, CPB×55, LULU×12, VSXY×16, CLYM×84 |
| 2026-09-08 | -11.47 | $99.31 | ALMS×102, CHPT×131, RARE×79, DPRO×192, CPB×55, LULU×12, VSXY×16, CLYM×84 | $9,749.43 | -57.27 | -39.42 | — | ALMS, RARE, DPRO, CPB, VSXY, CLYM | $7,230.58 | $9,696.33 | CHPT×131, LULU×12 |
| 2026-09-09 | -13.95 | $7,230.58 | CHPT×131, LULU×12 | $9,683.47 | -12.86 | +0.00 | — | CHPT, LULU | $9,679.01 | $9,679.01 | — |
| 2026-09-10 | -13.28 | $9,679.01 | — | $9,679.01 | -0.00 | +0.00 | — | — | $9,679.01 | $9,679.01 | — |
| 2026-09-11 | +0.50 | $9,679.01 | — | $9,679.01 | -0.00 | -4.52 | BKV, COO, GFR, SHOE, AEO, ACVA, AVAV, WLTH | — | $71.64 | $9,656.53 | BKV×48, COO×22, GFR×195, SHOE×96, AEO×82, ACVA×115, AVAV×8, WLTH×110 |
| 2026-09-14 | -11.00 | $71.64 | BKV×48, COO×22, GFR×195, SHOE×96, AEO×82, ACVA×115, AVAV×8, WLTH×110 | $9,647.38 | -9.15 | +3.90 | — | BKV, COO, SHOE, AEO, ACVA, AVAV, WLTH | $8,344.84 | $9,635.74 | GFR×195 |
| 2026-09-15 | -3.84 | $8,344.84 | GFR×195 | $9,633.79 | -1.95 | +0.00 | — | GFR | $9,631.18 | $9,631.18 | — |
| 2026-09-16 | +5.30 | $9,631.18 | — | $9,631.18 | -0.00 | -110.26 | TRMD, PLAY, BWIN, ALHC, VERA, ASND, HLMN, FPS | — | $54.84 | $9,503.19 | TRMD×33, PLAY×175, BWIN×37, ALHC×116, VERA×36, ASND×5, HLMN×165, FPS×36 |
| 2026-09-17 | +7.38 | $54.84 | TRMD×33, PLAY×175, BWIN×37, ALHC×116, VERA×36, ASND×5, HLMN×165, FPS×36 | $9,638.16 | +134.97 | +55.77 | FANG, BBNX, JBHT, ALVO, CBC, WWD, ASAN | TRMD, PLAY, BWIN, VERA, ASND, HLMN, FPS | $374.95 | $9,662.67 | ALHC×116, FANG×6, BBNX×54, JBHT×5, ALVO×236, CBC×39, WWD×3, ASAN×129 |
| 2026-09-18 | +4.86 | $374.95 | ALHC×116, FANG×6, BBNX×54, JBHT×5, ALVO×236, CBC×39, WWD×3, ASAN×129 | $9,667.70 | +5.03 | -87.52 | GNRC, SDGR, QSR, FLNC, GBTG, HLN, ACVA | FANG, BBNX, JBHT, ALVO, CBC, WWD, ASAN | $265.19 | $9,548.55 | ALHC×116, GNRC×5, SDGR×42, QSR×16, FLNC×163, GBTG×130, HLN×134, ACVA×117 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $8,764.91 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 115 | $10.83 | $2.33 | — | $7,517.13 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; 🔵; ⚪; ret5=-30.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $6,269.36 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SECZ` | 214 | $5.84 | $2.76 | — | $5,016.84 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; 🔵; ⚪; ret5=-20.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `EROC` | 77 | $16.05 | $2.22 | — | $3,778.77 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+50.4; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 25 | $48.82 | $2.06 | — | $2,556.20 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+21.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FA` | 56 | $22.15 | $2.16 | — | $1,313.65 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=-8.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZIM` | 45 | $27.25 | $2.12 | — | $85.27 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+1.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.27 | ▼ close $9,829.26 vs 09:30 $10,000.00 (session -152.57) | 16:00 close · cash $85.27 · equity $9,829.26 vs 09:30 $10,000.00 (-170.74; session marks -152.57) · 8 name(s) marked open→close (per-name table). ARX×63 09:30 $19.57 → close $19.58 +0.63; CLBT×115 09:30 $10.83 → close $11.14 +35.65; AIRO×112 09:30 $11.12 → close $9.57 -173.60; SECZ×214 09:30 $5.84 → close $5.61 -49.22; EROC×77 09:30 $16.05 → close $16.72 +51.59; TBBB×25 09:30 $48.82 → close $47.79 -25.75; FA×56 09:30 $22.15 → close $21.58 -31.92; ZIM×45 09:30 $27.25 → close $28.14 +40.05 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.27 | ▲ 09:30 equity $9,841.42 vs yday $9,829.26 (+12.16) | 09:30 open · cash $85.27 (unchanged overnight, no fees) · equity $9,841.42 vs prior close $9,829.26 (+12.16) · 8 name(s) re-marked at the open (per-name table). ARX×63 yday $19.58 → 09:30 $19.57 -0.63; CLBT×115 yday $11.14 → 09:30 $11.19 +5.75; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; SECZ×214 yday $5.61 → 09:30 $5.45 -34.24; EROC×77 yday $16.72 → 09:30 $17.15 +33.11; TBBB×25 yday $47.79 → 09:30 $47.39 -10.00; FA×56 yday $21.58 → 09:30 $21.35 -12.88; ZIM×45 yday $28.14 → 09:30 $28.83 +31.05 | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $1,315.98 | ▼ -4.38 after sell → book $9,839.22; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 115 | $11.19 | $2.36 | $+36.70 | $2,600.47 | ▲ +36.70 after sell → book $9,836.86; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $3,669.95 | ▼ -178.28 after sell → book $9,834.50; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `SECZ` | 214 | $5.45 | $2.81 | $-89.03 | $4,833.45 | ▼ -89.03 after sell → book $9,831.70; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `EROC` | 77 | $17.15 | $2.24 | $+80.23 | $6,151.75 | ▲ +80.23 after sell → book $9,829.45; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 25 | $47.39 | $2.08 | $-39.90 | $7,334.42 | ▼ -39.90 after sell → book $9,827.37; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `FA` | 56 | $21.35 | $2.18 | $-49.14 | $8,527.84 | ▼ -49.14 after sell → book $9,825.19; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZIM` | 45 | $28.83 | $2.15 | $+66.83 | $9,823.04 | ▲ +66.83 after sell → book $9,823.04; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 178 | $6.87 | $2.52 | — | $8,597.66 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; ret5=+62.6; leftover $1227.88 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $7,399.91 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; ret5=+46.0; leftover $1227.88 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 111 | $10.97 | $2.32 | — | $6,179.92 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; ⚪; ret5=+21.2; leftover $1227.88 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CHEF` | 11 | $108.18 | $2.02 | — | $4,987.92 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ⚪; ret5=-1.0; leftover $1227.88 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VIV` | 106 | $11.55 | $2.31 | — | $3,761.31 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ⚪; ret5=-5.0; leftover $1227.88 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `RDDT` | 6 | $177.51 | $2.01 | — | $2,694.24 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; ⚪; ret5=+10.1; leftover $1227.88 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DIOD` | 11 | $104.00 | $2.02 | — | $1,548.22 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ⚪; ret5=-1.6; leftover $1227.88 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `YSS` | 118 | $10.36 | $2.34 | — | $323.39 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ⚪; ret5=-4.2; leftover $1227.88 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $323.39 | ▲ close $9,868.42 vs 09:30 $9,841.42 (session +63.01) | 16:00 close · cash $323.39 · equity $9,868.42 vs 09:30 $9,841.42 (+27.00; session marks +63.01) · 8 name(s) marked open→close (per-name table). CAPR×178 09:30 $6.87 → close $7.45 +103.24; HTFL×29 09:30 $41.23 → close $41.94 +20.59; NMAX×111 09:30 $10.97 → close $10.36 -67.71; CHEF×11 09:30 $108.18 → close $110.57 +26.29; VIV×106 09:30 $11.55 → close $11.40 -15.90; RDDT×6 09:30 $177.51 → close $164.50 -78.06; DIOD×11 09:30 $104.00 → close $107.56 +39.16; YSS×118 09:30 $10.36 → close $10.66 +35.40 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $323.39 | ▼ 09:30 equity $9,778.04 vs yday $9,868.42 (-90.38) | 09:30 open · cash $323.39 (unchanged overnight, no fees) · equity $9,778.04 vs prior close $9,868.42 (-90.38) · 8 name(s) re-marked at the open (per-name table). CAPR×178 yday $7.45 → 09:30 $7.50 +8.90; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; NMAX×111 yday $10.36 → 09:30 $10.31 -5.55; CHEF×11 yday $110.57 → 09:30 $110.91 +3.74; VIV×106 yday $11.40 → 09:30 $11.45 +5.30; RDDT×6 yday $164.50 → 09:30 $166.10 +9.60; DIOD×11 yday $107.56 → 09:30 $103.01 -50.05; YSS×118 yday $10.66 → 09:30 $10.24 -49.56 | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $1,524.80 | ▲ +3.66 after sell → book $9,775.95; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 111 | $10.31 | $2.35 | $-77.93 | $2,666.85 | ▼ -77.93 after sell → book $9,773.59; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `CHEF` | 11 | $110.91 | $2.04 | $+25.96 | $3,884.82 | ▲ +25.96 after sell → book $9,771.55; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VIV` | 106 | $11.45 | $2.34 | $-15.24 | $5,096.19 | ▼ -15.24 after sell → book $9,769.22; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `RDDT` | 6 | $166.10 | $2.03 | $-72.50 | $6,090.76 | ▼ -72.50 after sell → book $9,767.19; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `DIOD` | 11 | $103.01 | $2.04 | $-14.96 | $7,221.83 | ▼ -14.96 after sell → book $9,765.15; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `YSS` | 118 | $10.24 | $2.37 | $-18.88 | $8,427.77 | ▼ -18.88 after sell → book $9,762.77; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,427.77 | ▼ close $9,688.01 vs 09:30 $9,778.04 (session -74.76) | 16:00 close · cash $8,427.77 · equity $9,688.01 vs 09:30 $9,778.04 (-90.03; session marks -74.76) · 1 name(s) marked open→close (per-name table). CAPR×178 09:30 $7.50 → close $7.08 -74.76 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,427.77 | ▲ 09:30 equity $9,707.59 vs yday $9,688.01 (+19.58) | 09:30 open · cash $8,427.77 (unchanged overnight, no fees) · equity $9,707.59 vs prior close $9,688.01 (+19.58) · 1 name(s) re-marked at the open (per-name table). CAPR×178 yday $7.08 → 09:30 $7.19 +19.58 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 178 | $7.19 | $2.56 | $+51.87 | $9,705.03 | ▲ +51.87 after sell → book $9,705.03; vs 09:30 mark -2.56 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,705.03 | ▲ close $9,705.03 vs 09:30 $9,707.59 (session +0.00) | 16:00 close · cash $9,705.03 · no lots left · equity $9,705.03. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,705.03 | ▲ 09:30 equity $9,705.03 vs yday $9,705.03 (-0.00) | 09:30 open · cash $9,705.03 · no holdings · equity $9,705.03 vs prior close $9,705.03 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $8,501.89 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+173.9; leftover $1213.13 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WBS` | 15 | $77.57 | $2.04 | — | $7,336.31 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=-1.9; leftover $1213.13 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALH` | 51 | $23.72 | $2.14 | — | $6,124.45 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-7.6; leftover $1213.13 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `LBRDK` | 33 | $36.02 | $2.09 | — | $4,933.70 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-0.2; leftover $1213.13 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SUI` | 10 | $121.21 | $2.02 | — | $3,719.58 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+3.1; leftover $1213.13 | join🔴 sector🔴 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NTST` | 59 | $20.47 | $2.17 | — | $2,509.68 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.0; leftover $1213.13 | join🟢 sector🔴 gen🟢 news🟡 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 11 | $109.06 | $2.02 | — | $1,308.00 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+22.0; leftover $1213.13 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ADC` | 16 | $74.37 | $2.04 | — | $116.04 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.0; leftover $1213.13 | join🟡 sector🔴 gen🟢 news🟡 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.04 | ▼ close $9,566.87 vs 09:30 $9,705.03 (session -121.63) | 16:00 close · cash $116.04 · equity $9,566.87 vs 09:30 $9,705.03 (-138.16; session marks -121.63) · 8 name(s) marked open→close (per-name table). MRNA×8 09:30 $150.14 → close $133.32 -134.56; WBS×15 09:30 $77.57 → close $77.57 +0.00; ALH×51 09:30 $23.72 → close $23.18 -27.54; LBRDK×33 09:30 $36.02 → close $36.02 +0.00; SUI×10 09:30 $121.21 → close $122.29 +10.80; NTST×59 09:30 $20.47 → close $20.61 +8.26; BNTX×11 09:30 $109.06 → close $110.89 +20.13; ADC×16 09:30 $74.37 → close $74.45 +1.28 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.04 | ▲ 09:30 equity $9,579.72 vs yday $9,566.87 (+12.85) | 09:30 open · cash $116.04 (unchanged overnight, no fees) · equity $9,579.72 vs prior close $9,566.87 (+12.85) · 8 name(s) re-marked at the open (per-name table). MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; WBS×15 yday $77.57 → 09:30 $77.57 +0.00; ALH×51 yday $23.18 → 09:30 $23.33 +7.65; LBRDK×33 yday $36.02 → 09:30 $36.02 +0.00; SUI×10 yday $122.29 → 09:30 $122.41 +1.20; NTST×59 yday $20.61 → 09:30 $20.66 +2.95; BNTX×11 yday $110.89 → 09:30 $110.92 +0.33; ADC×16 yday $74.45 → 09:30 $74.60 +2.40 | — |
| 2026-08-21 09:30 ET | **SELL** | `WBS` | 15 | $77.57 | $2.06 | $-4.09 | $1,277.53 | ▼ -4.09 after sell → book $9,577.66; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SUI` | 10 | $122.41 | $2.04 | $+7.94 | $2,499.59 | ▲ +7.94 after sell → book $9,575.62; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NTST` | 59 | $20.66 | $2.19 | $+6.86 | $3,716.35 | ▲ +6.86 after sell → book $9,573.44; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BNTX` | 11 | $110.92 | $2.04 | $+16.39 | $4,934.42 | ▲ +16.39 after sell → book $9,571.39; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ADC` | 16 | $74.60 | $2.06 | $-0.42 | $6,125.97 | ▼ -0.42 after sell → book $9,569.34; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AAP` | 24 | $42.41 | $2.06 | — | $5,106.06 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-26.1; leftover $1020.99 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MFC` | 24 | $42.48 | $2.06 | — | $4,084.48 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=-3.3; leftover $1020.99 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 123 | $8.28 | $2.36 | — | $3,063.68 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+36.6; leftover $1020.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BULL` | 113 | $8.99 | $2.33 | — | $2,045.48 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=+11.3; leftover $1020.99 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `SGRY` | 72 | $14.10 | $2.21 | — | $1,028.08 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-8.2; leftover $1020.99 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 91 | $11.13 | $2.26 | — | $12.98 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,yday_gainer,mover_buy,oppset; 🔵; ⚪; ret5=+39.8; leftover $1020.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.98 | ▲ close $9,926.06 vs 09:30 $9,579.72 (session +370.01) | 16:00 close · cash $12.98 · equity $9,926.06 vs 09:30 $9,579.72 (+346.34; session marks +370.01) · 9 name(s) marked open→close (per-name table). MRNA×8 09:30 $133.11 → close $145.13 +96.16; ALH×51 09:30 $23.33 → close $23.47 +7.14; LBRDK×33 09:30 $36.02 → close $36.02 +0.00; AAP×24 09:30 $42.41 → close $42.58 +4.08; MFC×24 09:30 $42.48 → close $42.51 +0.72; MRVI×123 09:30 $8.28 → close $8.64 +44.28; BULL×113 09:30 $8.99 → close $8.78 -23.73; SGRY×72 09:30 $14.10 → close $14.52 +30.24; ARCT×91 09:30 $11.13 → close $13.45 +211.12 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.98 | ▼ 09:30 equity $9,885.28 vs yday $9,926.06 (-40.78) | 09:30 open · cash $12.98 (unchanged overnight, no fees) · equity $9,885.28 vs prior close $9,926.06 (-40.78) · 9 name(s) re-marked at the open (per-name table). MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; ALH×51 yday $23.47 → 09:30 $23.66 +9.69; LBRDK×33 yday $36.02 → 09:30 $36.02 +0.00; AAP×24 yday $42.58 → 09:30 $43.05 +11.28; MFC×24 yday $42.51 → 09:30 $42.31 -4.80; MRVI×123 yday $8.64 → 09:30 $8.59 -6.15; BULL×113 yday $8.78 → 09:30 $8.58 -22.60; SGRY×72 yday $14.52 → 09:30 $14.55 +2.16; ARCT×91 yday $13.45 → 09:30 $13.33 -10.92 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 8 | $142.70 | $2.03 | $-63.57 | $1,152.55 | ▼ -63.57 after sell → book $9,883.25; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ALH` | 51 | $23.66 | $2.16 | $-7.37 | $2,357.05 | ▼ -7.37 after sell → book $9,881.09; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | join🔴 sector🔴 gen🔴 news🔴 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 24 | $43.05 | $2.08 | $+11.22 | $3,388.17 | ▲ +11.22 after sell → book $9,879.01; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MFC` | 24 | $42.31 | $2.08 | $-8.22 | $4,401.52 | ▼ -8.22 after sell → book $9,876.92; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 123 | $8.59 | $2.39 | $+33.38 | $5,455.70 | ▲ +33.38 after sell → book $9,874.53; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BULL` | 113 | $8.58 | $2.36 | $-51.02 | $6,422.89 | ▼ -51.02 after sell → book $9,872.18; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `SGRY` | 72 | $14.55 | $2.23 | $+27.97 | $7,468.26 | ▲ +27.97 after sell → book $9,869.95; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 91 | $13.33 | $2.29 | $+195.65 | $8,679.00 | ▲ +195.65 after sell → book $9,867.66; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,679.00 | ▲ close $9,867.66 vs 09:30 $9,885.28 (session +0.00) | 16:00 close · cash $8,679.00 · equity $9,867.66 vs 09:30 $9,885.28 (-17.62; session marks +0.00) · 1 name(s) marked open→close (per-name table). LBRDK×33 09:30 $36.02 → close $36.02 +0.00 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,679.00 | ▲ 09:30 equity $9,867.66 vs yday $9,867.66 (+0.00) | 09:30 open · cash $8,679.00 (unchanged overnight, no fees) · equity $9,867.66 vs prior close $9,867.66 (+0.00) · 1 name(s) re-marked at the open (per-name table). LBRDK×33 yday $36.02 → 09:30 $36.02 +0.00 | — |
| 2026-08-25 09:30 ET | **BUY** | `ABUS` | 206 | $5.25 | $2.66 | — | $7,594.84 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list mover_buy,oppset; 🔵; ⚪; ret5=+10.4; leftover $1084.88 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 170 | $6.37 | $2.50 | — | $6,509.44 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,oppset; 🔵; ⚪; ret5=+10.9; leftover $1084.88 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALH` | 44 | $24.16 | $2.12 | — | $5,444.28 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-3.8; leftover $1084.88 | join🔴 sector🟡 gen🟡 news🔴 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 149 | $7.25 | $2.44 | — | $4,361.59 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,ohlc_hot,oppset; 🔵; ret5=-8.7; leftover $1084.88 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 189 | $5.71 | $2.56 | — | $3,279.85 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+33.9; leftover $1084.88 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ARCT` | 76 | $14.12 | $2.22 | — | $2,204.51 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+76.2; leftover $1084.88 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XPEV` | 96 | $11.19 | $2.28 | — | $1,128.47 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-8.6; leftover $1084.88 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,128.47 | ▲ close $10,177.83 vs 09:30 $9,867.66 (session +326.94) | 16:00 close · cash $1,128.47 · equity $10,177.83 vs 09:30 $9,867.66 (+310.17; session marks +326.94) · 8 name(s) marked open→close (per-name table). LBRDK×33 09:30 $36.02 → close $36.02 +0.00; ABUS×206 09:30 $5.25 → close $5.20 -10.30; ZURA×170 09:30 $6.37 → close $6.32 -8.50; ALH×44 09:30 $24.16 → close $23.85 -13.64; CAPR×149 09:30 $7.25 → close $8.29 +154.96; FWDI×189 09:30 $5.71 → close $6.05 +64.26; ARCT×76 09:30 $14.12 → close $15.44 +100.32; XPEV×96 09:30 $11.19 → close $11.60 +39.84 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,128.47 | ▼ 09:30 equity $10,156.91 vs yday $10,177.83 (-20.92) | 09:30 open · cash $1,128.47 (unchanged overnight, no fees) · equity $10,156.91 vs prior close $10,177.83 (-20.92) · 8 name(s) re-marked at the open (per-name table). LBRDK×33 yday $36.02 → 09:30 $36.02 +0.00; ABUS×206 yday $5.20 → 09:30 $5.19 -2.06; ZURA×170 yday $6.32 → 09:30 $6.13 -32.30; ALH×44 yday $23.85 → 09:30 $24.00 +6.60; CAPR×149 yday $8.29 → 09:30 $8.29 +0.00; FWDI×189 yday $6.05 → 09:30 $5.97 -15.12; ARCT×76 yday $15.44 → 09:30 $15.35 -6.84; XPEV×96 yday $11.60 → 09:30 $11.90 +28.80 | — |
| 2026-08-26 09:30 ET | **SELL** | `ABUS` | 206 | $5.19 | $2.70 | $-17.72 | $2,194.91 | ▼ -17.72 after sell → book $10,154.21; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 170 | $6.13 | $2.54 | $-45.84 | $3,234.47 | ▼ -45.84 after sell → book $10,151.67; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `ALH` | 44 | $24.00 | $2.14 | $-11.30 | $4,288.33 | ▼ -11.30 after sell → book $10,149.53; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 149 | $8.29 | $2.47 | $+150.05 | $5,521.07 | ▲ +150.05 after sell → book $10,147.06; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `FWDI` | 189 | $5.97 | $2.60 | $+43.98 | $6,646.80 | ▲ +43.98 after sell → book $10,144.46; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 76 | $15.35 | $2.24 | $+89.02 | $7,811.16 | ▲ +89.02 after sell → book $10,142.22; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `XPEV` | 96 | $11.90 | $2.30 | $+64.06 | $8,951.25 | ▲ +64.06 after sell → book $10,139.91; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `DKS` | 9 | $121.87 | $2.02 | — | $7,852.41 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; 🔵; ret5=-35.1; leftover $1118.91 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SLF` | 14 | $79.20 | $2.03 | — | $6,741.58 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-0.5; leftover $1118.91 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `QFIN` | 114 | $9.76 | $2.33 | — | $5,626.60 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react,oppset; 🔵; ret5=-4.4; leftover $1118.91 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `GENB` | 55 | $20.00 | $2.15 | — | $4,524.45 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=+26.7; leftover $1118.91 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `VIPS` | 79 | $14.00 | $2.23 | — | $3,416.22 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-0.4; leftover $1118.91 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BZ` | 66 | $16.77 | $2.19 | — | $2,307.21 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+3.1; leftover $1118.91 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `MRNA` | 7 | $154.20 | $2.01 | — | $1,225.80 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=+152.3; leftover $1118.91 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ATHM` | 51 | $21.74 | $2.14 | — | $114.92 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-1.1; leftover $1118.91 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $114.92 | ▼ close $10,064.87 vs 09:30 $10,156.91 (session -57.94) | 16:00 close · cash $114.92 · equity $10,064.87 vs 09:30 $10,156.91 (-92.04; session marks -57.94) · 9 name(s) marked open→close (per-name table). LBRDK×33 09:30 $36.02 → close $36.02 +0.00; DKS×9 09:30 $121.87 → close $129.66 +70.11; SLF×14 09:30 $79.20 → close $79.05 -2.10; QFIN×114 09:30 $9.76 → close $9.35 -46.74; GENB×55 09:30 $20.00 → close $16.14 -212.30; VIPS×79 09:30 $14.00 → close $14.08 +6.32; BZ×66 09:30 $16.77 → close $18.84 +136.62; MRNA×7 09:30 $154.20 → close $149.66 -31.78; ATHM×51 09:30 $21.74 → close $22.17 +21.93 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $114.92 | ▼ 09:30 equity $10,037.72 vs yday $10,064.87 (-27.15) | 09:30 open · cash $114.92 (unchanged overnight, no fees) · equity $10,037.72 vs prior close $10,064.87 (-27.15) · 9 name(s) re-marked at the open (per-name table). LBRDK×33 yday $36.02 → 09:30 $36.02 +0.00; DKS×9 yday $129.66 → 09:30 $128.73 -8.37; SLF×14 yday $79.05 → 09:30 $78.45 -8.40; QFIN×114 yday $9.35 → 09:30 $9.42 +7.98; GENB×55 yday $16.14 → 09:30 $17.11 +53.35; VIPS×79 yday $14.08 → 09:30 $14.00 -6.32; BZ×66 yday $18.84 → 09:30 $18.50 -22.44; MRNA×7 yday $149.66 → 09:30 $144.18 -38.36; ATHM×51 yday $22.17 → 09:30 $22.08 -4.59 | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 9 | $128.73 | $2.04 | $+57.69 | $1,271.45 | ▲ +57.69 after sell → book $10,035.68; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `SLF` | 14 | $78.45 | $2.05 | $-14.58 | $2,367.70 | ▼ -14.58 after sell → book $10,033.63; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `QFIN` | 114 | $9.42 | $2.36 | $-43.45 | $3,439.22 | ▼ -43.45 after sell → book $10,031.27; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `GENB` | 55 | $17.11 | $2.17 | $-163.28 | $4,378.09 | ▼ -163.28 after sell → book $10,029.09; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `VIPS` | 79 | $14.00 | $2.25 | $-4.48 | $5,481.84 | ▼ -4.48 after sell → book $10,026.84; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 66 | $18.50 | $2.21 | $+109.78 | $6,700.64 | ▲ +109.78 after sell → book $10,024.64; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **SELL** | `MRNA` | 7 | $144.18 | $2.03 | $-74.18 | $7,707.86 | ▼ -74.18 after sell → book $10,022.60; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `ATHM` | 51 | $22.08 | $2.16 | $+13.03 | $8,831.78 | ▲ +13.03 after sell → book $10,020.44; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,831.78 | ▲ close $10,020.44 vs 09:30 $10,037.72 (session +0.00) | 16:00 close · cash $8,831.78 · equity $10,020.44 vs 09:30 $10,037.72 (-17.28; session marks +0.00) · 1 name(s) marked open→close (per-name table). LBRDK×33 09:30 $36.02 → close $36.02 +0.00 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,831.78 | ▲ 09:30 equity $10,020.44 vs yday $10,020.44 (+0.00) | 09:30 open · cash $8,831.78 (unchanged overnight, no fees) · equity $10,020.44 vs prior close $10,020.44 (+0.00) · 1 name(s) re-marked at the open (per-name table). LBRDK×33 yday $36.02 → 09:30 $36.02 +0.00 | — |
| 2026-08-28 09:30 ET | **SELL** | `LBRDK` | 33 | $36.02 | $2.11 | $-4.20 | $10,018.33 | ▼ -4.20 after sell → book $10,018.33; vs 09:30 mark -2.11 | dropped from list after 6 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $8,847.76 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+38.8; leftover $1252.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 68 | $18.15 | $2.19 | — | $7,611.36 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; ret5=+14.1; leftover $1252.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `QFIN` | 136 | $9.15 | $2.40 | — | $6,364.57 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-19.9; leftover $1252.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LEG` | 136 | $9.20 | $2.40 | — | $5,110.97 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-2.6; leftover $1252.29 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 78 | $15.88 | $2.22 | — | $3,870.10 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; ret5=+19.4; leftover $1252.29 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PLAB` | 41 | $30.01 | $2.11 | — | $2,637.58 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-0.9; leftover $1252.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 4 | $306.34 | $2.00 | — | $1,410.22 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-23.0; leftover $1252.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GENB` | 79 | $15.77 | $2.23 | — | $162.16 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-1.4; leftover $1252.29 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $162.16 | ▼ close $9,735.26 vs 09:30 $10,020.44 (session -265.50) | 16:00 close · cash $162.16 · equity $9,735.26 vs 09:30 $10,020.44 (-285.18; session marks -265.50) · 8 name(s) marked open→close (per-name table). ANF×8 09:30 $146.07 → close $148.42 +18.80; BZ×68 09:30 $18.15 → close $17.80 -23.80; QFIN×136 09:30 $9.15 → close $8.80 -47.60; LEG×136 09:30 $9.20 → close $9.20 +0.00; BHVN×78 09:30 $15.88 → close $15.41 -36.66; PLAB×41 09:30 $30.01 → close $27.73 -93.48; DY×4 09:30 $306.34 → close $294.34 -48.00; GENB×79 09:30 $15.77 → close $15.33 -34.76 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $162.16 | ▲ 09:30 equity $9,738.29 vs yday $9,735.26 (+3.03) | 09:30 open · cash $162.16 (unchanged overnight, no fees) · equity $9,738.29 vs prior close $9,735.26 (+3.03) · 8 name(s) re-marked at the open (per-name table). ANF×8 yday $148.42 → 09:30 $148.03 -3.12; BZ×68 yday $17.80 → 09:30 $17.70 -6.80; QFIN×136 yday $8.80 → 09:30 $8.70 -13.60; LEG×136 yday $9.20 → 09:30 $9.20 +0.00; BHVN×78 yday $15.41 → 09:30 $15.46 +3.90; PLAB×41 yday $27.73 → 09:30 $28.04 +12.71; DY×4 yday $294.34 → 09:30 $298.01 +14.68; GENB×79 yday $15.33 → 09:30 $15.27 -4.74 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.03 | $2.03 | $+11.63 | $1,344.37 | ▲ +11.63 after sell → book $9,736.26; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 68 | $17.70 | $2.22 | $-35.01 | $2,545.75 | ▼ -35.01 after sell → book $9,734.04; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `QFIN` | 136 | $8.70 | $2.43 | $-66.03 | $3,726.52 | ▼ -66.03 after sell → book $9,731.61; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 78 | $15.46 | $2.25 | $-37.23 | $4,930.16 | ▼ -37.23 after sell → book $9,729.37; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PLAB` | 41 | $28.04 | $2.13 | $-85.02 | $6,077.66 | ▼ -85.02 after sell → book $9,727.23; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 4 | $298.01 | $2.02 | $-37.34 | $7,267.68 | ▼ -37.34 after sell → book $9,725.21; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GENB` | 79 | $15.27 | $2.25 | $-43.98 | $8,471.76 | ▼ -43.98 after sell → book $9,722.96; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,471.76 | ▲ close $9,722.96 vs 09:30 $9,738.29 (session +0.00) | 16:00 close · cash $8,471.76 · equity $9,722.96 vs 09:30 $9,738.29 (-15.33; session marks +0.00) · 1 name(s) marked open→close (per-name table). LEG×136 09:30 $9.20 → close $9.20 +0.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,471.76 | ▲ 09:30 equity $9,722.96 vs yday $9,722.96 (+0.00) | 09:30 open · cash $8,471.76 (unchanged overnight, no fees) · equity $9,722.96 vs prior close $9,722.96 (+0.00) · 1 name(s) re-marked at the open (per-name table). LEG×136 yday $9.20 → 09:30 $9.20 +0.00 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,471.76 | ▲ close $9,722.96 vs 09:30 $9,722.96 (session +0.00) | 16:00 close · cash $8,471.76 · equity $9,722.96 vs 09:30 $9,722.96 (+0.00; session marks +0.00) · 1 name(s) marked open→close (per-name table). LEG×136 09:30 $9.20 → close $9.20 +0.00 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,471.76 | ▲ 09:30 equity $9,722.96 vs yday $9,722.96 (+0.00) | 09:30 open · cash $8,471.76 (unchanged overnight, no fees) · equity $9,722.96 vs prior close $9,722.96 (+0.00) · 1 name(s) re-marked at the open (per-name table). LEG×136 yday $9.20 → 09:30 $9.20 +0.00 | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,471.76 | ▲ close $9,722.96 vs 09:30 $9,722.96 (session +0.00) | 16:00 close · cash $8,471.76 · equity $9,722.96 vs 09:30 $9,722.96 (+0.00; session marks +0.00) · 1 name(s) marked open→close (per-name table). LEG×136 09:30 $9.20 → close $9.20 +0.00 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,471.76 | ▲ 09:30 equity $9,722.96 vs yday $9,722.96 (+0.00) | 09:30 open · cash $8,471.76 (unchanged overnight, no fees) · equity $9,722.96 vs prior close $9,722.96 (+0.00) · 1 name(s) re-marked at the open (per-name table). LEG×136 yday $9.20 → 09:30 $9.20 +0.00 | — |
| 2026-09-03 09:30 ET | **BUY** | `GBTG` | 111 | $9.49 | $2.32 | — | $7,416.05 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.2; leftover $1058.97 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 102 | $10.38 | $2.30 | — | $6,355.50 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; 🔵; ret5=-56.2; leftover $1058.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DAKT` | 55 | $19.08 | $2.15 | — | $5,303.95 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-1.9; leftover $1058.97 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GTLB` | 21 | $49.98 | $2.05 | — | $4,252.31 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+20.0; leftover $1058.97 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 57 | $18.28 | $2.16 | — | $3,208.19 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; ret5=+16.5; leftover $1058.97 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $2,233.58 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $1058.97 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MDB` | 2 | $378.76 | $2.00 | — | $1,474.06 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-7.6; leftover $1058.97 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OABI` | 208 | $5.08 | $2.68 | — | $414.74 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+4.8; leftover $1058.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $414.74 | ▲ close $9,752.76 vs 09:30 $9,722.96 (session +47.46) | 16:00 close · cash $414.74 · equity $9,752.76 vs 09:30 $9,722.96 (+29.80; session marks +47.46) · 9 name(s) marked open→close (per-name table). LEG×136 09:30 $9.20 → close $9.20 +0.00; GBTG×111 09:30 $9.49 → close $9.49 +0.00; ALMS×102 09:30 $10.38 → close $11.36 +100.47; DAKT×55 09:30 $19.08 → close $19.48 +22.00; GTLB×21 09:30 $49.98 → close $49.31 -14.07; FRVO×57 09:30 $18.28 → close $17.16 -63.84; DELL×2 09:30 $486.31 → close $516.39 +60.16; MDB×2 09:30 $378.76 → close $384.45 +11.38; OABI×208 09:30 $5.08 → close $4.75 -68.64 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $414.74 | ▼ 09:30 equity $9,726.25 vs yday $9,752.76 (-26.51) | 09:30 open · cash $414.74 (unchanged overnight, no fees) · equity $9,726.25 vs prior close $9,752.76 (-26.51) · 9 name(s) re-marked at the open (per-name table). LEG×136 yday $9.20 → 09:30 $9.20 +0.00; GBTG×111 yday $9.49 → 09:30 $9.49 +0.00; ALMS×102 yday $11.36 → 09:30 $11.23 -13.26; DAKT×55 yday $19.48 → 09:30 $19.47 -0.55; GTLB×21 yday $49.31 → 09:30 $48.94 -7.77; FRVO×57 yday $17.16 → 09:30 $17.27 +6.27; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; MDB×2 yday $384.45 → 09:30 $378.34 -12.22; OABI×208 yday $4.75 → 09:30 $4.78 +6.24 | — |
| 2026-09-04 09:30 ET | **SELL** | `LEG` | 136 | $9.20 | $2.43 | $-4.83 | $1,663.51 | ▼ -4.83 after sell → book $9,723.82; vs 09:30 mark -2.43 | dropped from list after 5 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `GBTG` | 111 | $9.49 | $2.35 | $-4.67 | $2,714.55 | ▼ -4.67 after sell → book $9,721.47; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DAKT` | 55 | $19.47 | $2.17 | $+17.12 | $3,783.22 | ▲ +17.12 after sell → book $9,719.29; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `GTLB` | 21 | $48.94 | $2.07 | $-25.97 | $4,808.89 | ▼ -25.97 after sell → book $9,717.22; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 57 | $17.27 | $2.18 | $-61.91 | $5,791.10 | ▼ -61.91 after sell → book $9,715.04; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $6,816.64 | ▲ +50.93 after sell → book $9,713.02; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `MDB` | 2 | $378.34 | $2.02 | $-4.85 | $7,571.30 | ▼ -4.85 after sell → book $9,711.00; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `OABI` | 208 | $4.78 | $2.73 | $-67.81 | $8,562.82 | ▼ -67.81 after sell → book $9,708.28; vs 09:30 mark -2.72 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CHPT` | 131 | $9.28 | $2.38 | — | $7,344.75 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+55.7; leftover $1223.26 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `RARE` | 79 | $15.47 | $2.23 | — | $6,120.00 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-43.5; leftover $1223.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DPRO` | 192 | $6.36 | $2.57 | — | $4,896.32 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+47.1; leftover $1223.26 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CPB` | 55 | $22.10 | $2.15 | — | $3,678.66 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-4.9; leftover $1223.26 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 12 | $98.15 | $2.03 | — | $2,498.83 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react,oppset; ret5=+5.9; leftover $1223.26 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VSXY` | 16 | $73.63 | $2.04 | — | $1,318.72 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-17.9; leftover $1223.26 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CLYM` | 84 | $14.49 | $2.24 | — | $99.31 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-3.1; leftover $1223.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.31 | ▲ close $9,806.70 vs 09:30 $9,726.25 (session +114.07) | 16:00 close · cash $99.31 · equity $9,806.70 vs 09:30 $9,726.25 (+80.45; session marks +114.07) · 8 name(s) marked open→close (per-name table). ALMS×102 09:30 $11.23 → close $11.10 -13.26; CHPT×131 09:30 $9.28 → close $9.89 +79.91; RARE×79 09:30 $15.47 → close $15.30 -13.82; DPRO×192 09:30 $6.36 → close $6.12 -46.08; CPB×55 09:30 $22.10 → close $21.38 -39.60; LULU×12 09:30 $98.15 → close $100.61 +29.52; VSXY×16 09:30 $73.63 → close $75.56 +30.88; CLYM×84 09:30 $14.49 → close $15.52 +86.52 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.31 | ▼ 09:30 equity $9,749.43 vs yday $9,806.70 (-57.27) | 09:30 open · cash $99.31 (unchanged overnight, no fees) · equity $9,749.43 vs prior close $9,806.70 (-57.27) · 8 name(s) re-marked at the open (per-name table). ALMS×102 yday $11.10 → 09:30 $11.05 -5.10; CHPT×131 yday $9.89 → 09:30 $9.91 +2.62; RARE×79 yday $15.30 → 09:30 $15.10 -16.04; DPRO×192 yday $6.12 → 09:30 $6.07 -9.60; CPB×55 yday $21.38 → 09:30 $21.30 -4.40; LULU×12 yday $100.61 → 09:30 $100.58 -0.36; VSXY×16 yday $75.56 → 09:30 $73.51 -32.80; CLYM×84 yday $15.52 → 09:30 $15.62 +8.40 | — |
| 2026-09-08 09:30 ET | **SELL** | `ALMS` | 102 | $11.05 | $2.32 | $+64.23 | $1,224.09 | ▲ +64.23 after sell → book $9,747.10; vs 09:30 mark -2.33 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `RARE` | 79 | $15.10 | $2.25 | $-34.34 | $2,414.50 | ▼ -34.34 after sell → book $9,744.85; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DPRO` | 192 | $6.07 | $2.61 | $-60.85 | $3,577.34 | ▼ -60.85 after sell → book $9,742.25; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `CPB` | 55 | $21.30 | $2.17 | $-48.33 | $4,746.66 | ▼ -48.33 after sell → book $9,740.07; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VSXY` | 16 | $73.51 | $2.06 | $-6.02 | $5,920.76 | ▼ -6.02 after sell → book $9,738.01; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CLYM` | 84 | $15.62 | $2.27 | $+90.41 | $7,230.58 | ▲ +90.41 after sell → book $9,735.75; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,230.58 | ▼ close $9,696.33 vs 09:30 $9,749.43 (session -39.42) | 16:00 close · cash $7,230.58 · equity $9,696.33 vs 09:30 $9,749.43 (-53.10; session marks -39.42) · 2 name(s) marked open→close (per-name table). CHPT×131 09:30 $9.91 → close $9.37 -70.74; LULU×12 09:30 $100.58 → close $103.19 +31.32 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,230.58 | ▼ 09:30 equity $9,683.47 vs yday $9,696.33 (-12.86) | 09:30 open · cash $7,230.58 (unchanged overnight, no fees) · equity $9,683.47 vs prior close $9,696.33 (-12.86) · 2 name(s) re-marked at the open (per-name table). CHPT×131 yday $9.37 → 09:30 $9.39 +2.62; LULU×12 yday $103.19 → 09:30 $101.90 -15.48 | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 131 | $9.39 | $2.41 | $+9.61 | $8,458.25 | ▲ +9.61 after sell → book $9,681.05; vs 09:30 mark -2.42 | dropped from list after 2 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-09 09:30 ET | **SELL** | `LULU` | 12 | $101.90 | $2.05 | $+40.93 | $9,679.01 | ▲ +40.93 after sell → book $9,679.01; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,679.01 | ▲ close $9,679.01 vs 09:30 $9,683.47 (session +0.00) | 16:00 close · cash $9,679.01 · no lots left · equity $9,679.01. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,679.01 | ▲ 09:30 equity $9,679.01 vs yday $9,679.01 (-0.00) | 09:30 open · cash $9,679.01 · no holdings · equity $9,679.01 vs prior close $9,679.01 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,679.01 | ▲ close $9,679.01 vs 09:30 $9,679.01 (session +0.00) | 16:00 close · cash $9,679.01 · no lots left · equity $9,679.01. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,679.01 | ▲ 09:30 equity $9,679.01 vs yday $9,679.01 (-0.00) | 09:30 open · cash $9,679.01 · no holdings · equity $9,679.01 vs prior close $9,679.01 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `BKV` | 48 | $24.97 | $2.13 | — | $8,478.31 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-0.6; leftover $1209.88 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 22 | $54.66 | $2.06 | — | $7,273.74 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-22.3; leftover $1209.88 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `GFR` | 195 | $6.19 | $2.58 | — | $6,064.11 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=+1.1; leftover $1209.88 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SHOE` | 96 | $12.51 | $2.28 | — | $4,860.87 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-9.0; leftover $1209.88 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AEO` | 82 | $14.71 | $2.24 | — | $3,652.42 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-12.8; leftover $1209.88 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ACVA` | 115 | $10.46 | $2.33 | — | $2,447.76 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.0; leftover $1209.88 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AVAV` | 8 | $145.91 | $2.01 | — | $1,278.46 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+1.2; leftover $1209.88 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 110 | $10.95 | $2.32 | — | $71.64 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+20.8; leftover $1209.88 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.64 | ▼ close $9,656.53 vs 09:30 $9,679.01 (session -4.52) | 16:00 close · cash $71.64 · equity $9,656.53 vs 09:30 $9,679.01 (-22.48; session marks -4.52) · 8 name(s) marked open→close (per-name table). BKV×48 09:30 $24.97 → close $24.23 -35.52; COO×22 09:30 $54.66 → close $53.91 -16.50; GFR×195 09:30 $6.19 → close $6.52 +64.35; SHOE×96 09:30 $12.51 → close $12.71 +19.20; AEO×82 09:30 $14.71 → close $15.02 +25.42; ACVA×115 09:30 $10.46 → close $10.41 -5.17; AVAV×8 09:30 $145.91 → close $146.71 +6.40; WLTH×110 09:30 $10.95 → close $10.38 -62.70 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.64 | ▼ 09:30 equity $9,647.38 vs yday $9,656.53 (-9.15) | 09:30 open · cash $71.64 (unchanged overnight, no fees) · equity $9,647.38 vs prior close $9,656.53 (-9.15) · 8 name(s) re-marked at the open (per-name table). BKV×48 yday $24.23 → 09:30 $24.26 +1.44; COO×22 yday $53.91 → 09:30 $54.78 +19.14; GFR×195 yday $6.52 → 09:30 $6.60 +15.60; SHOE×96 yday $12.71 → 09:30 $12.55 -15.36; AEO×82 yday $15.02 → 09:30 $14.85 -13.94; ACVA×115 yday $10.41 → 09:30 $10.42 +1.15; AVAV×8 yday $146.71 → 09:30 $145.80 -7.28; WLTH×110 yday $10.38 → 09:30 $10.29 -9.90 | — |
| 2026-09-14 09:30 ET | **SELL** | `BKV` | 48 | $24.26 | $2.15 | $-38.37 | $1,233.97 | ▼ -38.37 after sell → book $9,645.23; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 22 | $54.78 | $2.08 | $-1.49 | $2,437.05 | ▼ -1.49 after sell → book $9,643.15; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `SHOE` | 96 | $12.55 | $2.30 | $-0.74 | $3,639.55 | ▼ -0.74 after sell → book $9,640.85; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AEO` | 82 | $14.85 | $2.26 | $+6.98 | $4,854.99 | ▲ +6.98 after sell → book $9,638.59; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ACVA` | 115 | $10.42 | $2.36 | $-8.72 | $6,050.93 | ▼ -8.72 after sell → book $9,636.23; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `AVAV` | 8 | $145.80 | $2.03 | $-4.93 | $7,215.29 | ▼ -4.93 after sell → book $9,634.19; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 110 | $10.29 | $2.35 | $-77.27 | $8,344.84 | ▼ -77.27 after sell → book $9,631.84; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,344.84 | ▲ close $9,635.74 vs 09:30 $9,647.38 (session +3.90) | 16:00 close · cash $8,344.84 · equity $9,635.74 vs 09:30 $9,647.38 (-11.64; session marks +3.90) · 1 name(s) marked open→close (per-name table). GFR×195 09:30 $6.60 → close $6.62 +3.90 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,344.84 | ▼ 09:30 equity $9,633.79 vs yday $9,635.74 (-1.95) | 09:30 open · cash $8,344.84 (unchanged overnight, no fees) · equity $9,633.79 vs prior close $9,635.74 (-1.95) · 1 name(s) re-marked at the open (per-name table). GFR×195 yday $6.62 → 09:30 $6.61 -1.95 | — |
| 2026-09-15 09:30 ET | **SELL** | `GFR` | 195 | $6.61 | $2.62 | $+76.71 | $9,631.18 | ▲ +76.71 after sell → book $9,631.18; vs 09:30 mark -2.61 | dropped from list after 2 sess (min 1) | join🟢 sector🔴 gen🔴 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,631.18 | ▲ close $9,631.18 vs 09:30 $9,633.79 (session +0.00) | 16:00 close · cash $9,631.18 · no lots left · equity $9,631.18. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,631.18 | ▲ 09:30 equity $9,631.18 vs yday $9,631.18 (-0.00) | 09:30 open · cash $9,631.18 · no holdings · equity $9,631.18 vs prior close $9,631.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `TRMD` | 33 | $35.90 | $2.09 | — | $8,444.39 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+2.6; leftover $1203.90 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `PLAY` | 175 | $6.86 | $2.52 | — | $7,241.37 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-22.4; leftover $1203.90 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `BWIN` | 37 | $32.25 | $2.10 | — | $6,046.02 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-3.7; leftover $1203.90 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `ALHC` | 116 | $10.30 | $2.34 | — | $4,848.88 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-23.0; leftover $1203.90 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `VERA` | 36 | $33.22 | $2.10 | — | $3,650.87 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-4.6; leftover $1203.90 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ASND` | 5 | $239.70 | $2.00 | — | $2,450.36 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-11.2; leftover $1203.90 | join🟡 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `HLMN` | 165 | $7.26 | $2.48 | — | $1,249.98 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-1.9; leftover $1203.90 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 36 | $33.14 | $2.10 | — | $54.84 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,oppset; 🔵; ret5=-2.9; leftover $1203.90 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.84 | ▼ close $9,503.19 vs 09:30 $9,631.18 (session -110.26) | 16:00 close · cash $54.84 · equity $9,503.19 vs 09:30 $9,631.18 (-127.99; session marks -110.26) · 8 name(s) marked open→close (per-name table). TRMD×33 09:30 $35.90 → close $36.60 +23.10; PLAY×175 09:30 $6.86 → close $6.86 +0.00; BWIN×37 09:30 $32.25 → close $32.04 -7.77; ALHC×116 09:30 $10.30 → close $8.71 -184.44; VERA×36 09:30 $33.22 → close $31.77 -52.20; ASND×5 09:30 $239.70 → close $247.69 +39.95; HLMN×165 09:30 $7.26 → close $7.32 +9.90; FPS×36 09:30 $33.14 → close $34.84 +61.20 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.84 | ▲ 09:30 equity $9,638.16 vs yday $9,503.19 (+134.97) | 09:30 open · cash $54.84 (unchanged overnight, no fees) · equity $9,638.16 vs prior close $9,503.19 (+134.97) · 8 name(s) re-marked at the open (per-name table). TRMD×33 yday $36.60 → 09:30 $36.52 -2.64; PLAY×175 yday $6.86 → 09:30 $6.96 +17.50; BWIN×37 yday $32.04 → 09:30 $32.06 +0.74; ALHC×116 yday $8.71 → 09:30 $8.58 -15.08; VERA×36 yday $31.77 → 09:30 $32.50 +26.28; ASND×5 yday $247.69 → 09:30 $249.23 +7.70; HLMN×165 yday $7.32 → 09:30 $7.51 +31.35; FPS×36 yday $34.84 → 09:30 $36.76 +69.12 | — |
| 2026-09-17 09:30 ET | **SELL** | `TRMD` | 33 | $36.52 | $2.11 | $+16.26 | $1,257.89 | ▲ +16.26 after sell → book $9,636.05; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `PLAY` | 175 | $6.96 | $2.55 | $+12.43 | $2,473.33 | ▲ +12.43 after sell → book $9,633.49; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `BWIN` | 37 | $32.06 | $2.12 | $-11.25 | $3,657.43 | ▼ -11.25 after sell → book $9,631.37; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `VERA` | 36 | $32.50 | $2.12 | $-30.14 | $4,825.32 | ▼ -30.14 after sell → book $9,629.26; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ASND` | 5 | $249.23 | $2.02 | $+43.62 | $6,069.44 | ▲ +43.62 after sell → book $9,627.23; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `HLMN` | 165 | $7.51 | $2.52 | $+36.24 | $7,306.07 | ▲ +36.24 after sell → book $9,624.71; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FPS` | 36 | $36.76 | $2.12 | $+126.10 | $8,627.31 | ▲ +126.10 after sell → book $9,622.59; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `FANG` | 6 | $191.08 | $2.01 | — | $7,478.82 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=-4.0; leftover $1232.47 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 54 | $22.46 | $2.15 | — | $6,263.83 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+27.3; leftover $1232.47 | join🟢 sector🟢 gen🟢 news🔴 digest🔴 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 5 | $238.60 | $2.00 | — | $5,068.82 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-11.6; leftover $1232.47 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ALVO` | 236 | $5.22 | $3.04 | — | $3,833.86 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-2.1; leftover $1232.47 | join🔴 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `CBC` | 39 | $31.60 | $2.11 | — | $2,599.35 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=+1.3; leftover $1232.47 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `WWD` | 3 | $329.36 | $2.00 | — | $1,609.27 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-0.2; leftover $1232.47 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ASAN` | 129 | $9.55 | $2.38 | — | $374.95 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+17.0; leftover $1232.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $374.95 | ▲ close $9,662.67 vs 09:30 $9,638.16 (session +55.77) | 16:00 close · cash $374.95 · equity $9,662.67 vs 09:30 $9,638.16 (+24.51; session marks +55.77) · 8 name(s) marked open→close (per-name table). ALHC×116 09:30 $8.58 → close $8.70 +13.92; FANG×6 09:30 $191.08 → close $196.94 +35.16; BBNX×54 09:30 $22.46 → close $21.43 -55.62; JBHT×5 09:30 $238.60 → close $236.80 -9.00; ALVO×236 09:30 $5.22 → close $5.34 +28.32; CBC×39 09:30 $31.60 → close $31.67 +2.73; WWD×3 09:30 $329.36 → close $319.56 -29.40; ASAN×129 09:30 $9.55 → close $10.09 +69.66 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $374.95 | ▲ 09:30 equity $9,667.70 vs yday $9,662.67 (+5.03) | 09:30 open · cash $374.95 (unchanged overnight, no fees) · equity $9,667.70 vs prior close $9,662.67 (+5.03) · 8 name(s) re-marked at the open (per-name table). ALHC×116 yday $8.70 → 09:30 $8.68 -2.32; FANG×6 yday $196.94 → 09:30 $196.94 +0.00; BBNX×54 yday $21.43 → 09:30 $21.30 -7.02; JBHT×5 yday $236.80 → 09:30 $236.80 +0.00; ALVO×236 yday $5.34 → 09:30 $5.40 +14.16; CBC×39 yday $31.67 → 09:30 $31.64 -1.17; WWD×3 yday $319.56 → 09:30 $320.02 +1.38; ASAN×129 yday $10.09 → 09:30 $10.09 +0.00 | — |
| 2026-09-18 09:30 ET | **SELL** | `FANG` | 6 | $196.94 | $2.03 | $+31.12 | $1,554.56 | ▲ +31.12 after sell → book $9,665.67; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 54 | $21.30 | $2.17 | $-66.96 | $2,702.59 | ▼ -66.96 after sell → book $9,663.50; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 5 | $236.80 | $2.02 | $-13.03 | $3,884.56 | ▼ -13.03 after sell → book $9,661.47; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ALVO` | 236 | $5.40 | $3.09 | $+36.34 | $5,155.87 | ▲ +36.34 after sell → book $9,658.38; vs 09:30 mark -3.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CBC` | 39 | $31.64 | $2.13 | $-2.67 | $6,387.70 | ▼ -2.67 after sell → book $9,656.25; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `WWD` | 3 | $320.02 | $2.02 | $-32.04 | $7,345.74 | ▼ -32.04 after sell → book $9,654.23; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ASAN` | 129 | $10.09 | $2.41 | $+64.87 | $8,644.94 | ▲ +64.87 after sell → book $9,651.82; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 5 | $209.52 | $2.00 | — | $7,595.34 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+14.1; leftover $1234.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 42 | $29.32 | $2.12 | — | $6,361.78 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $1234.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `QSR` | 16 | $73.00 | $2.04 | — | $5,191.74 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+0.5; leftover $1234.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 163 | $7.54 | $2.48 | — | $3,961.06 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; 🔵; ret5=-20.9; leftover $1234.99 | join🟡 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `GBTG` | 130 | $9.46 | $2.38 | — | $2,728.88 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=+0.2; leftover $1234.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `HLN` | 134 | $9.20 | $2.39 | — | $1,493.69 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-7.8; leftover $1234.99 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `ACVA` | 117 | $10.48 | $2.34 | — | $265.19 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.0; leftover $1234.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $265.19 | ▼ close $9,548.55 vs 09:30 $9,667.70 (session -87.52) | 16:00 close · cash $265.19 · equity $9,548.55 vs 09:30 $9,667.70 (-119.15; session marks -87.52) · 8 name(s) marked open→close (per-name table). ALHC×116 09:30 $8.68 → close $8.35 -38.28; GNRC×5 09:30 $209.52 → close $207.44 -10.40; SDGR×42 09:30 $29.32 → close $29.02 -12.60; QSR×16 09:30 $73.00 → close $72.88 -1.92; FLNC×163 09:30 $7.54 → close $7.32 -35.04; GBTG×130 09:30 $9.46 → close $9.46 +0.00; HLN×134 09:30 $9.20 → close $9.28 +10.72; ACVA×117 09:30 $10.48 → close $10.48 +0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OCUL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KRNY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `BHF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MAIR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WBS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ALH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `LBRDK` | no_price | no 09:30 open — carry |
| 2026-08-24 | `LBRDK` | no_price | no 09:30 open — carry |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AMX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `IQMX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `WSC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BVN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `LBRDK` | no_price | no 09:30 open — carry |
| 2026-08-25 | `TWO` | no_price | no 09:30 open |
| 2026-08-26 | `LBRDK` | no_price | no 09:30 open — carry |
| 2026-08-27 | `LBRDK` | no_price | no 09:30 open — carry |
| 2026-08-31 | `LEG` | no_price | no 09:30 open — carry |
| 2026-08-31 | `AFRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SOLS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ESI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ESTC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BEKE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MNSO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `LEG` | no_price | no 09:30 open — carry |
| 2026-09-01 | `GGG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AVY` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRMB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `GLPI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUNB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `STNE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `LEG` | no_price | no 09:30 open — carry |
| 2026-09-02 | `ALMS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EIX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PCG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MMED` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HASI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUNB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `LEG` | no_price | no 09:30 open — carry |
| 2026-09-08 | `SUNB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `IOT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `IMO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ASAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AMBA` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `PHVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ATAI` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GPRK` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DYN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `WAFD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GPCR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ROIV` | hard_red | hard-red S=-13.95 sit; no new buys |
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
| 2026-09-14 | `DBRG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HDB` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `BWIN` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `BZ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RUM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `DV` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `BNC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ACVA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `AQN` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ALHC` | 116 | 2026-09-16 @ $10.30 | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-23.0; leftover $1203.90 |
| `GNRC` | 5 | 2026-09-18 @ $209.52 | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+14.1; leftover $1234.99 |
| `SDGR` | 42 | 2026-09-18 @ $29.32 | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $1234.99 |
| `QSR` | 16 | 2026-09-18 @ $73.00 | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+0.5; leftover $1234.99 |
| `FLNC` | 163 | 2026-09-18 @ $7.54 | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover,oppset; 🔵; ret5=-20.9; leftover $1234.99 |
| `GBTG` | 130 | 2026-09-18 @ $9.46 | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; ret5=+0.2; leftover $1234.99 |
| `HLN` | 134 | 2026-09-18 @ $9.20 | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-7.8; leftover $1234.99 |
| `ACVA` | 117 | 2026-09-18 @ $10.48 | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.0; leftover $1234.99 |
