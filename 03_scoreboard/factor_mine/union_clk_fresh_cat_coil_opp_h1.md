# Factor mine action — `union_clk_fresh_cat_coil_opp_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `opp_rvol` · size `leftover` · sell `list` · S-boost `none` · Clock-B #2 ∩ Theme Radar T−1 oppset

Cash book **-5.32%** ($9,468) · signal-only (no cash/fees) was -5.75%. Starts YES **1/26**. Fills 162 · skips 78 · realized $-618.38.

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
- Must-have: Clock-B #2: a fresh good catalyst (catal / EPS beat / packet or headline green) and the prior tape is not already exploded.
- Must-have: Theme Radar Clock-B opportunity-set: T−1 gap or RelVol (or week move) flagged — not today's Gap/RelVol.
- Must-not: Clock-B #5 long veto: extreme prior extension plus diminishing progress or a failed breakout.
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
- **Gate** `clk_fresh_cat_coil=True,oppset=True` · **rank** `opp_rvol` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $84.67.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ADUR` | 151 | — | $16.50 | +0.00 | $16.17 | -49.83 | -49.83 | +0.00 | -49.83 |
| 2026-08-14 | `AMAT` | 5 | — | $499.40 | +0.00 | $507.18 | +38.90 | +38.90 | +0.00 | +38.90 |
| 2026-08-14 | `WDC` | 4 | — | $503.50 | +0.00 | $508.80 | +21.20 | +21.20 | +0.00 | +21.20 |
| 2026-08-14 | `NU` | 158 | — | $15.74 | +0.00 | $15.23 | -80.58 | -80.58 | +0.00 | -80.58 |
| 2026-08-17 | `ADUR` | 151 | $16.17 | $15.73 | -66.44 | — | +0.00 | -66.44 | -116.27 | — |
| 2026-08-17 | `AMAT` | 5 | $507.18 | $517.45 | +51.33 | — | +0.00 | +51.33 | +90.23 | — |
| 2026-08-17 | `WDC` | 4 | $508.80 | $525.53 | +66.92 | — | +0.00 | +66.92 | +88.12 | — |
| 2026-08-17 | `NU` | 158 | $15.23 | $15.40 | +26.86 | — | +0.00 | +26.86 | -53.72 | — |
| 2026-08-17 | `ABX` | 365 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `ALM` | 205 | — | $16.20 | +0.00 | $16.36 | +32.80 | +32.80 | +0.00 | +32.80 |
| 2026-08-17 | `CELC` | 35 | — | $92.99 | +0.00 | $92.44 | -19.25 | -19.25 | +0.00 | -19.25 |
| 2026-08-18 | `ABX` | 365 | $9.12 | $9.03 | -32.85 | — | +0.00 | -32.85 | -32.85 | — |
| 2026-08-18 | `ALM` | 205 | $16.36 | $15.78 | -118.90 | — | +0.00 | -118.90 | -86.10 | — |
| 2026-08-18 | `CELC` | 35 | $92.44 | $92.38 | -2.10 | — | +0.00 | -2.10 | -21.35 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `MRVI` | 165 | — | $7.44 | +0.00 | $8.29 | +140.25 | +140.25 | +0.00 | +140.25 |
| 2026-08-20 | `DNA` | 164 | — | $7.45 | +0.00 | $6.96 | -80.36 | -80.36 | +0.00 | -80.36 |
| 2026-08-20 | `BILL` | 25 | — | $49.00 | +0.00 | $47.40 | -40.00 | -40.00 | +0.00 | -40.00 |
| 2026-08-20 | `CRCL` | 14 | — | $82.99 | +0.00 | $83.66 | +9.38 | +9.38 | +0.00 | +9.38 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `KGC` | 41 | — | $29.63 | +0.00 | $31.43 | +73.80 | +73.80 | +0.00 | +73.80 |
| 2026-08-20 | `WOLF` | 46 | — | $26.50 | +0.00 | $26.35 | -6.90 | -6.90 | +0.00 | -6.90 |
| 2026-08-20 | `BLSH` | 42 | — | $29.20 | +0.00 | $28.44 | -31.92 | -31.92 | +0.00 | -31.92 |
| 2026-08-21 | `MRVI` | 165 | $8.29 | $8.28 | -1.65 | — | +0.00 | -1.65 | +138.60 | — |
| 2026-08-21 | `DNA` | 164 | $6.96 | $7.09 | +21.32 | — | +0.00 | +21.32 | -59.04 | — |
| 2026-08-21 | `BILL` | 25 | $47.40 | $47.50 | +2.50 | — | +0.00 | +2.50 | -37.50 | — |
| 2026-08-21 | `CRCL` | 14 | $83.66 | $87.98 | +60.48 | — | +0.00 | +60.48 | +69.86 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `KGC` | 41 | $31.43 | $32.17 | +30.34 | — | +0.00 | +30.34 | +104.14 | — |
| 2026-08-21 | `WOLF` | 46 | $26.35 | $26.86 | +23.46 | $25.76 | -50.60 | -27.14 | +16.56 | -34.04 |
| 2026-08-21 | `BLSH` | 42 | $28.44 | $29.75 | +55.02 | — | +0.00 | +55.02 | +23.10 | — |
| 2026-08-21 | `QDEL` | 593 | — | $14.96 | +0.00 | $14.74 | -130.46 | -130.46 | +0.00 | -130.46 |
| 2026-08-24 | `WOLF` | 46 | $25.76 | $25.00 | -34.96 | — | +0.00 | -34.96 | -69.00 | — |
| 2026-08-24 | `QDEL` | 593 | $14.74 | $14.74 | +0.00 | — | +0.00 | +0.00 | -130.46 | — |
| 2026-08-25 | `AMX` | 51 | — | $23.80 | +0.00 | $23.75 | -2.55 | -2.55 | +0.00 | -2.55 |
| 2026-08-25 | `GOOS` | 150 | — | $8.22 | +0.00 | $8.22 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `AAOI` | 11 | — | $111.78 | +0.00 | $113.15 | +15.12 | +15.12 | +0.00 | +15.12 |
| 2026-08-25 | `ELMT` | 69 | — | $17.89 | +0.00 | $17.75 | -9.66 | -9.66 | +0.00 | -9.66 |
| 2026-08-25 | `GRRR` | 88 | — | $13.92 | +0.00 | $14.04 | +10.56 | +10.56 | +0.00 | +10.56 |
| 2026-08-25 | `AXTI` | 18 | — | $68.20 | +0.00 | $67.45 | -13.50 | -13.50 | +0.00 | -13.50 |
| 2026-08-25 | `EH` | 242 | — | $5.10 | +0.00 | $4.83 | -65.34 | -65.34 | +0.00 | -65.34 |
| 2026-08-26 | `AMX` | 51 | $23.75 | $23.75 | +0.00 | $23.62 | -6.63 | -6.63 | -2.55 | -9.18 |
| 2026-08-26 | `GOOS` | 150 | $8.22 | $8.25 | +4.50 | — | +0.00 | +4.50 | +4.50 | — |
| 2026-08-26 | `AAOI` | 11 | $113.15 | $110.59 | -28.16 | — | +0.00 | -28.16 | -13.04 | — |
| 2026-08-26 | `ELMT` | 69 | $17.75 | $17.82 | +4.83 | — | +0.00 | +4.83 | -4.83 | — |
| 2026-08-26 | `GRRR` | 88 | $14.04 | $14.03 | -0.88 | — | +0.00 | -0.88 | +9.68 | — |
| 2026-08-26 | `AXTI` | 18 | $67.45 | $65.34 | -37.98 | — | +0.00 | -37.98 | -51.48 | — |
| 2026-08-26 | `EH` | 242 | $4.83 | $4.77 | -14.52 | — | +0.00 | -14.52 | -79.86 | — |
| 2026-08-26 | `TAL` | 103 | — | $11.68 | +0.00 | $11.69 | +1.03 | +1.03 | +0.00 | +1.03 |
| 2026-08-26 | `TME` | 139 | — | $8.71 | +0.00 | $8.80 | +12.51 | +12.51 | +0.00 | +12.51 |
| 2026-08-26 | `BBWI` | 66 | — | $18.26 | +0.00 | $18.90 | +42.24 | +42.24 | +0.00 | +42.24 |
| 2026-08-26 | `INTU` | 3 | — | $323.47 | +0.00 | $345.88 | +67.23 | +67.23 | +0.00 | +67.23 |
| 2026-08-26 | `NCNO` | 62 | — | $19.33 | +0.00 | $21.51 | +135.16 | +135.16 | +0.00 | +135.16 |
| 2026-08-26 | `HEI` | 3 | — | $370.00 | +0.00 | $346.15 | -71.55 | -71.55 | +0.00 | -71.55 |
| 2026-08-26 | `BE` | 5 | — | $213.94 | +0.00 | $218.21 | +21.35 | +21.35 | +0.00 | +21.35 |
| 2026-08-27 | `AMX` | 51 | $23.62 | $23.77 | +7.65 | — | +0.00 | +7.65 | -1.53 | — |
| 2026-08-27 | `TAL` | 103 | $11.69 | $11.62 | -7.21 | — | +0.00 | -7.21 | -6.18 | — |
| 2026-08-27 | `TME` | 139 | $8.80 | $8.80 | +0.00 | — | +0.00 | +0.00 | +12.51 | — |
| 2026-08-27 | `BBWI` | 66 | $18.90 | $18.69 | -13.86 | — | +0.00 | -13.86 | +28.38 | — |
| 2026-08-27 | `INTU` | 3 | $345.88 | $353.54 | +22.98 | — | +0.00 | +22.98 | +90.21 | — |
| 2026-08-27 | `NCNO` | 62 | $21.51 | $22.03 | +32.24 | — | +0.00 | +32.24 | +167.40 | — |
| 2026-08-27 | `HEI` | 3 | $346.15 | $346.19 | +0.12 | — | +0.00 | +0.12 | -71.43 | — |
| 2026-08-27 | `BE` | 5 | $218.21 | $227.10 | +44.45 | — | +0.00 | +44.45 | +65.80 | — |
| 2026-08-28 | `LEG` | 135 | — | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-28 | `PLAB` | 41 | — | $30.01 | +0.00 | $27.73 | -93.48 | -93.48 | +0.00 | -93.48 |
| 2026-08-28 | `DY` | 4 | — | $306.34 | +0.00 | $294.34 | -48.00 | -48.00 | +0.00 | -48.00 |
| 2026-08-28 | `SLF` | 15 | — | $78.95 | +0.00 | $78.76 | -2.85 | -2.85 | +0.00 | -2.85 |
| 2026-08-28 | `KSS` | 68 | — | $18.25 | +0.00 | $17.50 | -51.00 | -51.00 | +0.00 | -51.00 |
| 2026-08-28 | `BBWI` | 66 | — | $18.75 | +0.00 | $19.22 | +31.02 | +31.02 | +0.00 | +31.02 |
| 2026-08-28 | `MAIR` | 45 | — | $27.36 | +0.00 | $26.33 | -46.35 | -46.35 | +0.00 | -46.35 |
| 2026-08-28 | `HEI` | 3 | — | $339.95 | +0.00 | $336.53 | -10.26 | -10.26 | +0.00 | -10.26 |
| 2026-08-31 | `LEG` | 135 | $9.20 | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-31 | `PLAB` | 41 | $27.73 | $28.04 | +12.71 | — | +0.00 | +12.71 | -80.77 | — |
| 2026-08-31 | `DY` | 4 | $294.34 | $298.01 | +14.68 | — | +0.00 | +14.68 | -33.32 | — |
| 2026-08-31 | `SLF` | 15 | $78.76 | $78.70 | -0.90 | — | +0.00 | -0.90 | -3.75 | — |
| 2026-08-31 | `KSS` | 68 | $17.50 | $17.26 | -16.32 | — | +0.00 | -16.32 | -67.32 | — |
| 2026-08-31 | `BBWI` | 66 | $19.22 | $19.25 | +1.98 | — | +0.00 | +1.98 | +33.00 | — |
| 2026-08-31 | `MAIR` | 45 | $26.33 | $26.28 | -2.25 | — | +0.00 | -2.25 | -48.60 | — |
| 2026-08-31 | `HEI` | 3 | $336.53 | $334.88 | -4.95 | — | +0.00 | -4.95 | -15.21 | — |
| 2026-09-01 | `LEG` | 135 | $9.20 | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-02 | `LEG` | 135 | $9.20 | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `LEG` | 135 | $9.20 | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `EIX` | 19 | — | $55.42 | +0.00 | $56.30 | +16.72 | +16.72 | +0.00 | +16.72 |
| 2026-09-03 | `PRMB` | 47 | — | $22.32 | +0.00 | $21.91 | -19.27 | -19.27 | +0.00 | -19.27 |
| 2026-09-03 | `SNN` | 36 | — | $29.03 | +0.00 | $28.69 | -12.24 | -12.24 | +0.00 | -12.24 |
| 2026-09-03 | `ENOV` | 52 | — | $20.28 | +0.00 | $19.40 | -45.76 | -45.76 | +0.00 | -45.76 |
| 2026-09-03 | `FIVE` | 4 | — | $257.00 | +0.00 | $239.96 | -68.16 | -68.16 | +0.00 | -68.16 |
| 2026-09-03 | `NTSK` | 68 | — | $15.51 | +0.00 | $14.34 | -79.56 | -79.56 | +0.00 | -79.56 |
| 2026-09-03 | `NTAP` | 6 | — | $161.95 | +0.00 | $185.38 | +140.58 | +140.58 | +0.00 | +140.58 |
| 2026-09-03 | `ATRC` | 20 | — | $52.88 | +0.00 | $52.46 | -8.40 | -8.40 | +0.00 | -8.40 |
| 2026-09-04 | `LEG` | 135 | $9.20 | $9.20 | +0.00 | — | +0.00 | +0.00 | +0.00 | — |
| 2026-09-04 | `EIX` | 19 | $56.30 | $55.79 | -9.69 | — | +0.00 | -9.69 | +7.03 | — |
| 2026-09-04 | `PRMB` | 47 | $21.91 | $21.87 | -1.88 | — | +0.00 | -1.88 | -21.15 | — |
| 2026-09-04 | `SNN` | 36 | $28.69 | $28.77 | +2.88 | — | +0.00 | +2.88 | -9.36 | — |
| 2026-09-04 | `ENOV` | 52 | $19.40 | $19.01 | -20.28 | — | +0.00 | -20.28 | -66.04 | — |
| 2026-09-04 | `FIVE` | 4 | $239.96 | $238.88 | -4.32 | — | +0.00 | -4.32 | -72.48 | — |
| 2026-09-04 | `NTSK` | 68 | $14.34 | $14.15 | -12.92 | — | +0.00 | -12.92 | -92.48 | — |
| 2026-09-04 | `NTAP` | 6 | $185.38 | $182.59 | -16.74 | — | +0.00 | -16.74 | +123.84 | — |
| 2026-09-04 | `ATRC` | 20 | $52.46 | $52.03 | -8.60 | — | +0.00 | -8.60 | -17.00 | — |
| 2026-09-04 | `AMX` | 51 | — | $23.03 | +0.00 | $23.00 | -1.53 | -1.53 | +0.00 | -1.53 |
| 2026-09-04 | `BULL` | 121 | — | $9.79 | +0.00 | $9.74 | -6.05 | -6.05 | +0.00 | -6.05 |
| 2026-09-04 | `ASAN` | 136 | — | $8.74 | +0.00 | $8.81 | +9.52 | +9.52 | +0.00 | +9.52 |
| 2026-09-04 | `MSTR` | 8 | — | $137.35 | +0.00 | $142.80 | +43.60 | +43.60 | +0.00 | +43.60 |
| 2026-09-04 | `CRCL` | 12 | — | $97.98 | +0.00 | $102.05 | +48.84 | +48.84 | +0.00 | +48.84 |
| 2026-09-04 | `DOCU` | 17 | — | $68.52 | +0.00 | $68.41 | -1.87 | -1.87 | +0.00 | -1.87 |
| 2026-09-04 | `GWRE` | 7 | — | $167.55 | +0.00 | $162.42 | -35.91 | -35.91 | +0.00 | -35.91 |
| 2026-09-04 | `BLSH` | 34 | — | $34.69 | +0.00 | $36.00 | +44.54 | +44.54 | +0.00 | +44.54 |
| 2026-09-08 | `AMX` | 51 | $23.00 | $23.15 | +7.65 | — | +0.00 | +7.65 | +6.12 | — |
| 2026-09-08 | `BULL` | 121 | $9.74 | $9.94 | +23.60 | — | +0.00 | +23.60 | +17.55 | — |
| 2026-09-08 | `ASAN` | 136 | $8.81 | $8.73 | -10.88 | — | +0.00 | -10.88 | -1.36 | — |
| 2026-09-08 | `MSTR` | 8 | $142.80 | $137.62 | -41.44 | — | +0.00 | -41.44 | +2.16 | — |
| 2026-09-08 | `CRCL` | 12 | $102.05 | $100.65 | -16.80 | — | +0.00 | -16.80 | +32.04 | — |
| 2026-09-08 | `DOCU` | 17 | $68.41 | $67.05 | -23.12 | — | +0.00 | -23.12 | -24.99 | — |
| 2026-09-08 | `GWRE` | 7 | $162.42 | $160.52 | -13.30 | — | +0.00 | -13.30 | -49.21 | — |
| 2026-09-08 | `BLSH` | 34 | $36.00 | $35.90 | -3.40 | — | +0.00 | -3.40 | +41.14 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ACVA` | 113 | — | $10.46 | +0.00 | $10.41 | -5.08 | -5.08 | +0.00 | -5.08 |
| 2026-09-11 | `DRVN` | 94 | — | $12.55 | +0.00 | $12.15 | -37.60 | -37.60 | +0.00 | -37.60 |
| 2026-09-11 | `ENB` | 24 | — | $48.37 | +0.00 | $47.76 | -14.64 | -14.64 | +0.00 | -14.64 |
| 2026-09-11 | `CNQ` | 23 | — | $49.94 | +0.00 | $50.07 | +2.99 | +2.99 | +0.00 | +2.99 |
| 2026-09-11 | `KMB` | 11 | — | $99.24 | +0.00 | $98.15 | -11.99 | -11.99 | +0.00 | -11.99 |
| 2026-09-11 | `BTI` | 21 | — | $56.03 | +0.00 | $55.24 | -16.59 | -16.59 | +0.00 | -16.59 |
| 2026-09-11 | `INGM` | 44 | — | $26.62 | +0.00 | $27.55 | +40.92 | +40.92 | +0.00 | +40.92 |
| 2026-09-11 | `PBR` | 56 | — | $21.21 | +0.00 | $21.20 | -0.56 | -0.56 | +0.00 | -0.56 |
| 2026-09-14 | `ACVA` | 113 | $10.41 | $10.42 | +1.13 | — | +0.00 | +1.13 | -3.96 | — |
| 2026-09-14 | `DRVN` | 94 | $12.15 | $12.33 | +16.92 | — | +0.00 | +16.92 | -20.68 | — |
| 2026-09-14 | `ENB` | 24 | $47.76 | $47.85 | +2.16 | — | +0.00 | +2.16 | -12.48 | — |
| 2026-09-14 | `CNQ` | 23 | $50.07 | $50.76 | +15.87 | — | +0.00 | +15.87 | +18.86 | — |
| 2026-09-14 | `KMB` | 11 | $98.15 | $99.18 | +11.33 | — | +0.00 | +11.33 | -0.66 | — |
| 2026-09-14 | `BTI` | 21 | $55.24 | $57.12 | +39.48 | — | +0.00 | +39.48 | +22.89 | — |
| 2026-09-14 | `INGM` | 44 | $27.55 | $26.89 | -29.04 | $26.85 | -1.76 | -30.80 | +11.88 | +10.12 |
| 2026-09-14 | `PBR` | 56 | $21.20 | $21.23 | +1.68 | — | +0.00 | +1.68 | +1.12 | — |
| 2026-09-15 | `INGM` | 44 | $26.85 | $26.91 | +2.64 | — | +0.00 | +2.64 | +12.76 | — |
| 2026-09-16 | `BWIN` | 36 | — | $32.25 | +0.00 | $32.04 | -7.56 | -7.56 | +0.00 | -7.56 |
| 2026-09-16 | `HLMN` | 163 | — | $7.26 | +0.00 | $7.32 | +9.78 | +9.78 | +0.00 | +9.78 |
| 2026-09-16 | `SYY` | 14 | — | $79.73 | +0.00 | $78.71 | -14.28 | -14.28 | +0.00 | -14.28 |
| 2026-09-16 | `SRRK` | 23 | — | $50.01 | +0.00 | $49.37 | -14.72 | -14.72 | +0.00 | -14.72 |
| 2026-09-16 | `ARVN` | 143 | — | $8.31 | +0.00 | $8.16 | -21.45 | -21.45 | +0.00 | -21.45 |
| 2026-09-16 | `AMX` | 51 | — | $23.18 | +0.00 | $22.98 | -10.20 | -10.20 | +0.00 | -10.20 |
| 2026-09-16 | `GNW` | 118 | — | $10.00 | +0.00 | $10.09 | +10.62 | +10.62 | +0.00 | +10.62 |
| 2026-09-16 | `PBF` | 16 | — | $73.02 | +0.00 | $75.93 | +46.56 | +46.56 | +0.00 | +46.56 |
| 2026-09-17 | `BWIN` | 36 | $32.04 | $32.06 | +0.72 | — | +0.00 | +0.72 | -6.84 | — |
| 2026-09-17 | `HLMN` | 163 | $7.32 | $7.51 | +30.97 | — | +0.00 | +30.97 | +40.75 | — |
| 2026-09-17 | `SYY` | 14 | $78.71 | $79.03 | +4.48 | — | +0.00 | +4.48 | -9.80 | — |
| 2026-09-17 | `SRRK` | 23 | $49.37 | $49.52 | +3.45 | $49.02 | -11.50 | -8.05 | -11.27 | -22.77 |
| 2026-09-17 | `ARVN` | 143 | $8.16 | $8.29 | +18.59 | — | +0.00 | +18.59 | -2.86 | — |
| 2026-09-17 | `AMX` | 51 | $22.98 | $23.09 | +5.61 | — | +0.00 | +5.61 | -4.59 | — |
| 2026-09-17 | `GNW` | 118 | $10.09 | $10.12 | +3.54 | — | +0.00 | +3.54 | +14.16 | — |
| 2026-09-17 | `PBF` | 16 | $75.93 | $74.28 | -26.40 | — | +0.00 | -26.40 | +20.16 | — |
| 2026-09-17 | `WWD` | 3 | — | $329.36 | +0.00 | $319.56 | -29.40 | -29.40 | +0.00 | -29.40 |
| 2026-09-17 | `KNX` | 17 | — | $66.85 | +0.00 | $66.67 | -3.06 | -3.06 | +0.00 | -3.06 |
| 2026-09-17 | `HTLD` | 99 | — | $12.03 | +0.00 | $11.92 | -10.89 | -10.89 | +0.00 | -10.89 |
| 2026-09-17 | `LIFE` | 30 | — | $39.67 | +0.00 | $36.40 | -98.10 | -98.10 | +0.00 | -98.10 |
| 2026-09-17 | `AXON` | 2 | — | $468.73 | +0.00 | $453.50 | -30.46 | -30.46 | +0.00 | -30.46 |
| 2026-09-17 | `KEY` | 57 | — | $20.98 | +0.00 | $20.95 | -1.71 | -1.71 | +0.00 | -1.71 |
| 2026-09-17 | `GRAL` | 15 | — | $75.29 | +0.00 | $79.95 | +69.90 | +69.90 | +0.00 | +69.90 |
| 2026-09-18 | `SRRK` | 23 | $49.02 | $48.02 | -23.00 | — | +0.00 | -23.00 | -45.77 | — |
| 2026-09-18 | `WWD` | 3 | $319.56 | $320.02 | +1.38 | — | +0.00 | +1.38 | -28.02 | — |
| 2026-09-18 | `KNX` | 17 | $66.67 | $66.65 | -0.34 | — | +0.00 | -0.34 | -3.40 | — |
| 2026-09-18 | `HTLD` | 99 | $11.92 | $11.87 | -4.95 | — | +0.00 | -4.95 | -15.84 | — |
| 2026-09-18 | `LIFE` | 30 | $36.40 | $36.89 | +14.70 | — | +0.00 | +14.70 | -83.40 | — |
| 2026-09-18 | `AXON` | 2 | $453.50 | $455.62 | +4.24 | — | +0.00 | +4.24 | -26.22 | — |
| 2026-09-18 | `KEY` | 57 | $20.95 | $20.90 | -2.85 | — | +0.00 | -2.85 | -4.56 | — |
| 2026-09-18 | `GRAL` | 15 | $79.95 | $81.48 | +22.95 | — | +0.00 | +22.95 | +92.85 | — |
| 2026-09-18 | `QSR` | 16 | — | $73.00 | +0.00 | $72.88 | -1.92 | -1.92 | +0.00 | -1.92 |
| 2026-09-18 | `HLN` | 127 | — | $9.20 | +0.00 | $9.28 | +10.16 | +10.16 | +0.00 | +10.16 |
| 2026-09-18 | `ACVA` | 111 | — | $10.48 | +0.00 | $10.48 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-18 | `JXN` | 9 | — | $129.00 | +0.00 | $132.67 | +33.03 | +33.03 | +0.00 | +33.03 |
| 2026-09-18 | `DRVN` | 95 | — | $12.26 | +0.00 | $12.28 | +1.90 | +1.90 | +0.00 | +1.90 |
| 2026-09-18 | `MNR` | 107 | — | $10.95 | +0.00 | $11.13 | +19.26 | +19.26 | +0.00 | +19.26 |
| 2026-09-18 | `INIO` | 56 | — | $20.80 | +0.00 | $21.15 | +19.60 | +19.60 | +0.00 | +19.60 |
| 2026-09-18 | `CRWV` | 14 | — | $79.83 | +0.00 | $81.36 | +21.42 | +21.42 | +0.00 | +21.42 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -70.31 | ADUR, AMAT, WDC, NU | — | $501.67 | $9,920.78 | ADUR×151, AMAT×5, WDC×4, NU×158 |
| 2026-08-17 | +2.25 | $501.67 | ADUR×151, AMAT×5, WDC×4, NU×158 | $9,999.45 | +78.67 | +13.55 | ABX, ALM, CELC | ADUR, AMAT, WDC, NU | $76.49 | $9,994.49 | ABX×365, ALM×205, CELC×35 |
| 2026-08-18 | -6.20 | $76.49 | ABX×365, ALM×205, CELC×35 | $9,840.64 | -153.85 | +0.00 | — | ABX, ALM, CELC | $9,831.01 | $9,831.01 | — |
| 2026-08-19 | -7.20 | $9,831.01 | — | $9,831.01 | -0.00 | +0.00 | — | — | $9,831.01 | $9,831.01 | — |
| 2026-08-20 | +1.12 | $9,831.01 | — | $9,831.01 | -0.00 | +98.31 | MRVI, DNA, BILL, CRCL, BHP, KGC, WOLF, BLSH | — | $133.94 | $9,911.87 | MRVI×165, DNA×164, BILL×25, CRCL×14, BHP×13, KGC×41, WOLF×46, BLSH×42 |
| 2026-08-21 | +3.25 | $133.94 | MRVI×165, DNA×164, BILL×25, CRCL×14, BHP×13, KGC×41, WOLF×46, BLSH×42 | $10,130.51 | +218.64 | -181.06 | QDEL | MRVI, DNA, BILL, CRCL, BHP, KGC, BLSH | $0.52 | $9,926.30 | WOLF×46, QDEL×593 |
| 2026-08-24 | -5.17 | $0.52 | WOLF×46, QDEL×593 | $9,891.34 | -34.96 | +0.00 | — | WOLF, QDEL | $9,881.38 | $9,881.38 | — |
| 2026-08-25 | +1.80 | $9,881.38 | — | $9,881.38 | -0.00 | -65.37 | AMX, GOOS, AAOI, ELMT, GRRR, AXTI, EH | — | $1,267.66 | $9,799.79 | AMX×51, GOOS×150, AAOI×11, ELMT×69, GRRR×88, AXTI×18, EH×242 |
| 2026-08-26 | +2.02 | $1,267.66 | AMX×51, GOOS×150, AAOI×11, ELMT×69, GRRR×88, AXTI×18, EH×242 | $9,727.58 | -72.21 | +201.34 | TAL, TME, BBWI, INTU, NCNO, HEI, BE | GOOS, AAOI, ELMT, GRRR, AXTI, EH | $519.54 | $9,899.59 | AMX×51, TAL×103, TME×139, BBWI×66, INTU×3, NCNO×62, HEI×3, BE×5 |
| 2026-08-27 | — | $519.54 | AMX×51, TAL×103, TME×139, BBWI×66, INTU×3, NCNO×62, HEI×3, BE×5 | $9,985.96 | +86.37 | +0.00 | — | AMX, TAL, TME, BBWI, INTU, NCNO, HEI, BE | $9,968.57 | $9,968.57 | — |
| 2026-08-28 | +0.75 | $9,968.57 | — | $9,968.57 | -0.00 | -220.92 | LEG, PLAB, DY, SLF, KSS, BBWI, MAIR, HEI | — | $339.94 | $9,730.59 | LEG×135, PLAB×41, DY×4, SLF×15, KSS×68, BBWI×66, MAIR×45, HEI×3 |
| 2026-08-31 | -5.85 | $339.94 | LEG×135, PLAB×41, DY×4, SLF×15, KSS×68, BBWI×66, MAIR×45, HEI×3 | $9,735.54 | +4.95 | +0.00 | — | PLAB, DY, SLF, KSS, BBWI, MAIR, HEI | $8,478.75 | $9,720.75 | LEG×135 |
| 2026-09-01 | -6.30 | $8,478.75 | LEG×135 | $9,720.75 | -0.00 | +0.00 | — | — | $8,478.75 | $9,720.75 | LEG×135 |
| 2026-09-02 | -3.83 | $8,478.75 | LEG×135 | $9,720.75 | -0.00 | +0.00 | — | — | $8,478.75 | $9,720.75 | LEG×135 |
| 2026-09-03 | -0.90 | $8,478.75 | LEG×135 | $9,720.75 | -0.00 | -76.09 | EIX, PRMB, SNN, ENOV, FIVE, NTSK, NTAP, ATRC | — | $148.43 | $9,627.98 | LEG×135, EIX×19, PRMB×47, SNN×36, ENOV×52, FIVE×4, NTSK×68, NTAP×6, ATRC×20 |
| 2026-09-04 | +2.25 | $148.43 | LEG×135, EIX×19, PRMB×47, SNN×36, ENOV×52, FIVE×4, NTSK×68, NTAP×6, ATRC×20 | $9,556.43 | -71.55 | +101.14 | AMX, BULL, ASAN, MSTR, CRCL, DOCU, GWRE, BLSH | LEG, EIX, PRMB, SNN, ENOV, FIVE, NTSK, NTAP, ATRC | $180.62 | $9,621.23 | AMX×51, BULL×121, ASAN×136, MSTR×8, CRCL×12, DOCU×17, GWRE×7, BLSH×34 |
| 2026-09-08 | -11.47 | $180.62 | AMX×51, BULL×121, ASAN×136, MSTR×8, CRCL×12, DOCU×17, GWRE×7, BLSH×34 | $9,543.53 | -77.70 | +0.00 | — | AMX, BULL, ASAN, MSTR, CRCL, DOCU, GWRE, BLSH | $9,526.27 | $9,526.27 | — |
| 2026-09-09 | -13.95 | $9,526.27 | — | $9,526.27 | +0.00 | +0.00 | — | — | $9,526.27 | $9,526.27 | — |
| 2026-09-10 | -13.28 | $9,526.27 | — | $9,526.27 | +0.00 | +0.00 | — | — | $9,526.27 | $9,526.27 | — |
| 2026-09-11 | +0.50 | $9,526.27 | — | $9,526.27 | +0.00 | -42.55 | ACVA, DRVN, ENB, CNQ, KMB, BTI, INGM, PBR | — | $211.27 | $9,466.64 | ACVA×113, DRVN×94, ENB×24, CNQ×23, KMB×11, BTI×21, INGM×44, PBR×56 |
| 2026-09-14 | -11.00 | $211.27 | ACVA×113, DRVN×94, ENB×24, CNQ×23, KMB×11, BTI×21, INGM×44, PBR×56 | $9,526.17 | +59.53 | -1.76 | — | ACVA, DRVN, ENB, CNQ, KMB, BTI, PBR | $8,327.90 | $9,509.30 | INGM×44 |
| 2026-09-15 | -3.84 | $8,327.90 | INGM×44 | $9,511.94 | +2.64 | +0.00 | — | INGM | $9,509.80 | $9,509.80 | — |
| 2026-09-16 | +5.30 | $9,509.80 | — | $9,509.80 | -0.00 | -1.25 | BWIN, HLMN, SYY, SRRK, ARVN, AMX, GNW, PBF | — | $162.52 | $9,490.93 | BWIN×36, HLMN×163, SYY×14, SRRK×23, ARVN×143, AMX×51, GNW×118, PBF×16 |
| 2026-09-17 | +7.38 | $162.52 | BWIN×36, HLMN×163, SYY×14, SRRK×23, ARVN×143, AMX×51, GNW×118, PBF×16 | $9,531.89 | +40.96 | -115.22 | WWD, KNX, HTLD, LIFE, AXON, KEY, GRAL | BWIN, HLMN, SYY, ARVN, AMX, GNW, PBF | $594.33 | $9,386.34 | SRRK×23, WWD×3, KNX×17, HTLD×99, LIFE×30, AXON×2, KEY×57, GRAL×15 |
| 2026-09-18 | +4.86 | $594.33 | SRRK×23, WWD×3, KNX×17, HTLD×99, LIFE×30, AXON×2, KEY×57, GRAL×15 | $9,398.47 | +12.13 | +103.45 | QSR, HLN, ACVA, JXN, DRVN, MNR, INIO, CRWV | SRRK, WWD, KNX, HTLD, LIFE, AXON, KEY, GRAL | $84.67 | $9,467.57 | QSR×16, HLN×127, ACVA×111, JXN×9, DRVN×95, MNR×107, INIO×56, CRWV×14 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 151 | $16.50 | $2.44 | — | $7,506.06 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AMAT` | 5 | $499.40 | $2.00 | — | $5,007.05 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+1.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 4 | $503.50 | $2.00 | — | $2,991.05 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable; 🔵; ⚪; ret5=+7.9; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NU` | 158 | $15.74 | $2.46 | — | $501.67 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-1.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $501.67 | ▼ close $9,920.78 vs 09:30 $10,000.00 (session -70.31) | 16:00 close · cash $501.67 · equity $9,920.78 vs 09:30 $10,000.00 (-79.22; session marks -70.31) · 4 name(s) marked open→close (per-name table). ADUR×151 09:30 $16.50 → close $16.17 -49.83; AMAT×5 09:30 $499.40 → close $507.18 +38.90; WDC×4 09:30 $503.50 → close $508.80 +21.20; NU×158 09:30 $15.74 → close $15.23 -80.58 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $501.67 | ▲ 09:30 equity $9,999.45 vs yday $9,920.78 (+78.67) | 09:30 open · cash $501.67 (unchanged overnight, no fees) · equity $9,999.45 vs prior close $9,920.78 (+78.67) · 4 name(s) re-marked at the open (per-name table). ADUR×151 yday $16.17 → 09:30 $15.73 -66.44; AMAT×5 yday $507.18 → 09:30 $517.45 +51.33; WDC×4 yday $508.80 → 09:30 $525.53 +66.92; NU×158 yday $15.23 → 09:30 $15.40 +26.86 | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 151 | $15.73 | $2.49 | $-121.20 | $2,874.41 | ▼ -121.20 after sell → book $9,996.96; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMAT` | 5 | $517.45 | $2.04 | $+86.19 | $5,459.61 | ▲ +86.19 after sell → book $9,994.93; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 4 | $525.53 | $2.03 | $+84.09 | $7,559.70 | ▲ +84.09 after sell → book $9,992.90; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NU` | 158 | $15.40 | $2.51 | $-58.69 | $9,990.39 | ▼ -58.69 after sell → book $9,990.39; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 365 | $9.12 | $4.71 | — | $6,656.88 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $3330.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 205 | $16.20 | $2.64 | — | $3,333.24 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $3330.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 35 | $92.99 | $2.10 | — | $76.49 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-0.8; leftover $3330.13 | join🟡 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.49 | ▲ close $9,994.49 vs 09:30 $9,999.45 (session +13.55) | 16:00 close · cash $76.49 · equity $9,994.49 vs 09:30 $9,999.45 (-4.96; session marks +13.55) · 3 name(s) marked open→close (per-name table). ABX×365 09:30 $9.12 → close $9.12 +0.00; ALM×205 09:30 $16.20 → close $16.36 +32.80; CELC×35 09:30 $92.99 → close $92.44 -19.25 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.49 | ▼ 09:30 equity $9,840.64 vs yday $9,994.49 (-153.85) | 09:30 open · cash $76.49 (unchanged overnight, no fees) · equity $9,840.64 vs prior close $9,994.49 (-153.85) · 3 name(s) re-marked at the open (per-name table). ABX×365 yday $9.12 → 09:30 $9.03 -32.85; ALM×205 yday $16.36 → 09:30 $15.78 -118.90; CELC×35 yday $92.44 → 09:30 $92.38 -2.10 | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 365 | $9.03 | $4.80 | $-42.35 | $3,367.64 | ▼ -42.35 after sell → book $9,835.84; vs 09:30 mark -4.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 205 | $15.78 | $2.70 | $-91.45 | $6,599.84 | ▼ -91.45 after sell → book $9,833.14; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 35 | $92.38 | $2.13 | $-25.58 | $9,831.01 | ▼ -25.58 after sell → book $9,831.01; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,831.01 | ▲ close $9,831.01 vs 09:30 $9,840.64 (session +0.00) | 16:00 close · cash $9,831.01 · no lots left · equity $9,831.01. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,831.01 | ▲ 09:30 equity $9,831.01 vs yday $9,831.01 (-0.00) | 09:30 open · cash $9,831.01 · no holdings · equity $9,831.01 vs prior close $9,831.01 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,831.01 | ▲ close $9,831.01 vs 09:30 $9,831.01 (session +0.00) | 16:00 close · cash $9,831.01 · no lots left · equity $9,831.01. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,831.01 | ▲ 09:30 equity $9,831.01 vs yday $9,831.01 (-0.00) | 09:30 open · cash $9,831.01 · no holdings · equity $9,831.01 vs prior close $9,831.01 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 165 | $7.44 | $2.48 | — | $8,600.92 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=-8.5; leftover $1228.88 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 164 | $7.45 | $2.48 | — | $7,376.64 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1228.88 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BILL` | 25 | $49.00 | $2.06 | — | $6,149.58 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-2.0; leftover $1228.88 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRCL` | 14 | $82.99 | $2.03 | — | $4,985.69 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable; 🔵; ⚪; ret5=+7.4; leftover $1228.88 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $3,800.53 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1228.88 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $2,583.58 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1228.88 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WOLF` | 46 | $26.50 | $2.13 | — | $1,362.46 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-8.3; leftover $1228.88 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 42 | $29.20 | $2.12 | — | $133.94 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1228.88 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.94 | ▲ close $9,911.87 vs 09:30 $9,831.01 (session +98.31) | 16:00 close · cash $133.94 · equity $9,911.87 vs 09:30 $9,831.01 (+80.86; session marks +98.31) · 8 name(s) marked open→close (per-name table). MRVI×165 09:30 $7.44 → close $8.29 +140.25; DNA×164 09:30 $7.45 → close $6.96 -80.36; BILL×25 09:30 $49.00 → close $47.40 -40.00; CRCL×14 09:30 $82.99 → close $83.66 +9.38; BHP×13 09:30 $91.01 → close $93.63 +34.06; KGC×41 09:30 $29.63 → close $31.43 +73.80; WOLF×46 09:30 $26.50 → close $26.35 -6.90; BLSH×42 09:30 $29.20 → close $28.44 -31.92 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.94 | ▲ 09:30 equity $10,130.51 vs yday $9,911.87 (+218.64) | 09:30 open · cash $133.94 (unchanged overnight, no fees) · equity $10,130.51 vs prior close $9,911.87 (+218.64) · 8 name(s) re-marked at the open (per-name table). MRVI×165 yday $8.29 → 09:30 $8.28 -1.65; DNA×164 yday $6.96 → 09:30 $7.09 +21.32; BILL×25 yday $47.40 → 09:30 $47.50 +2.50; CRCL×14 yday $83.66 → 09:30 $87.98 +60.48; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; KGC×41 yday $31.43 → 09:30 $32.17 +30.34; WOLF×46 yday $26.35 → 09:30 $26.86 +23.46; BLSH×42 yday $28.44 → 09:30 $29.75 +55.02 | — |
| 2026-08-21 09:30 ET | **SELL** | `MRVI` | 165 | $8.28 | $2.52 | $+133.59 | $1,497.62 | ▲ +133.59 after sell → book $10,127.99; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 164 | $7.09 | $2.52 | $-64.04 | $2,657.86 | ▼ -64.04 after sell → book $10,125.47; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BILL` | 25 | $47.50 | $2.08 | $-41.65 | $3,843.27 | ▼ -41.65 after sell → book $10,123.38; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CRCL` | 14 | $87.98 | $2.05 | $+65.78 | $5,072.94 | ▲ +65.78 after sell → book $10,121.33; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $6,315.25 | ▲ +57.15 after sell → book $10,119.28; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $7,632.09 | ▲ +99.89 after sell → book $10,117.15; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BLSH` | 42 | $29.75 | $2.14 | $+18.85 | $8,879.45 | ▲ +18.85 after sell → book $10,115.01; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 593 | $14.96 | $7.65 | — | $0.52 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-1.6; leftover $8879.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.52 | ▼ close $9,926.30 vs 09:30 $10,130.51 (session -181.06) | 16:00 close · cash $0.52 · equity $9,926.30 vs 09:30 $10,130.51 (-204.21; session marks -181.06) · 2 name(s) marked open→close (per-name table). WOLF×46 09:30 $26.86 → close $25.76 -50.60; QDEL×593 09:30 $14.96 → close $14.74 -130.46 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.52 | ▼ 09:30 equity $9,891.34 vs yday $9,926.30 (-34.96) | 09:30 open · cash $0.52 (unchanged overnight, no fees) · equity $9,891.34 vs prior close $9,926.30 (-34.96) · 2 name(s) re-marked at the open (per-name table). WOLF×46 yday $25.76 → 09:30 $25.00 -34.96; QDEL×593 yday $14.74 → 09:30 $14.74 +0.00 | — |
| 2026-08-24 09:30 ET | **SELL** | `WOLF` | 46 | $25.00 | $2.15 | $-73.28 | $1,148.37 | ▼ -73.28 after sell → book $9,889.19; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 593 | $14.74 | $7.82 | $-145.93 | $9,881.38 | ▼ -145.93 after sell → book $9,881.38; vs 09:30 mark -7.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,881.38 | ▲ close $9,881.38 vs 09:30 $9,891.34 (session +0.00) | 16:00 close · cash $9,881.38 · no lots left · equity $9,881.38. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,881.38 | ▲ 09:30 equity $9,881.38 vs yday $9,881.38 (-0.00) | 09:30 open · cash $9,881.38 · no holdings · equity $9,881.38 vs prior close $9,881.38 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `AMX` | 51 | $23.80 | $2.14 | — | $8,665.43 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.5; leftover $1235.17 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GOOS` | 150 | $8.22 | $2.44 | — | $7,429.99 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-2.1; leftover $1235.17 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AAOI` | 11 | $111.78 | $2.02 | — | $6,198.44 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-30.5; leftover $1235.17 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ELMT` | 69 | $17.89 | $2.20 | — | $4,961.84 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ⚪; ret5=-7.5; leftover $1235.17 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 88 | $13.92 | $2.25 | — | $3,734.62 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+5.9; leftover $1235.17 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AXTI` | 18 | $68.20 | $2.04 | — | $2,504.98 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-31.9; leftover $1235.17 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 242 | $5.10 | $3.12 | — | $1,267.66 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-8.9; leftover $1235.17 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,267.66 | ▼ close $9,799.79 vs 09:30 $9,881.38 (session -65.37) | 16:00 close · cash $1,267.66 · equity $9,799.79 vs 09:30 $9,881.38 (-81.59; session marks -65.37) · 7 name(s) marked open→close (per-name table). AMX×51 09:30 $23.80 → close $23.75 -2.55; GOOS×150 09:30 $8.22 → close $8.22 +0.00; AAOI×11 09:30 $111.78 → close $113.15 +15.12; ELMT×69 09:30 $17.89 → close $17.75 -9.66; GRRR×88 09:30 $13.92 → close $14.04 +10.56; AXTI×18 09:30 $68.20 → close $67.45 -13.50; EH×242 09:30 $5.10 → close $4.83 -65.34 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,267.66 | ▼ 09:30 equity $9,727.58 vs yday $9,799.79 (-72.21) | 09:30 open · cash $1,267.66 (unchanged overnight, no fees) · equity $9,727.58 vs prior close $9,799.79 (-72.21) · 7 name(s) re-marked at the open (per-name table). AMX×51 yday $23.75 → 09:30 $23.75 +0.00; GOOS×150 yday $8.22 → 09:30 $8.25 +4.50; AAOI×11 yday $113.15 → 09:30 $110.59 -28.16; ELMT×69 yday $17.75 → 09:30 $17.82 +4.83; GRRR×88 yday $14.04 → 09:30 $14.03 -0.88; AXTI×18 yday $67.45 → 09:30 $65.34 -37.98; EH×242 yday $4.83 → 09:30 $4.77 -14.52 | — |
| 2026-08-26 09:30 ET | **SELL** | `GOOS` | 150 | $8.25 | $2.47 | $-0.41 | $2,502.68 | ▼ -0.41 after sell → book $9,725.10; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AAOI` | 11 | $110.59 | $2.04 | $-17.10 | $3,717.13 | ▼ -17.10 after sell → book $9,723.06; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `ELMT` | 69 | $17.82 | $2.22 | $-9.25 | $4,944.49 | ▼ -9.25 after sell → book $9,720.84; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 88 | $14.03 | $2.28 | $+5.15 | $6,176.85 | ▲ +5.15 after sell → book $9,718.56; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `AXTI` | 18 | $65.34 | $2.06 | $-55.59 | $7,350.91 | ▼ -55.59 after sell → book $9,716.50; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `EH` | 242 | $4.77 | $3.17 | $-86.15 | $8,502.08 | ▼ -86.15 after sell → book $9,713.33; vs 09:30 mark -3.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `TAL` | 103 | $11.68 | $2.30 | — | $7,296.74 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-3.7; leftover $1214.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TME` | 139 | $8.71 | $2.41 | — | $6,083.64 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-0.6; leftover $1214.58 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 66 | $18.26 | $2.19 | — | $4,876.29 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; ret5=-11.4; leftover $1214.58 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `INTU` | 3 | $323.47 | $2.00 | — | $3,903.88 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; ret5=+2.0; leftover $1214.58 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NCNO` | 62 | $19.33 | $2.18 | — | $2,703.25 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; ret5=+3.0; leftover $1214.58 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `HEI` | 3 | $370.00 | $2.00 | — | $1,591.25 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; ret5=-4.6; leftover $1214.58 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 5 | $213.94 | $2.00 | — | $519.54 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1214.58 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $519.54 | ▲ close $9,899.59 vs 09:30 $9,727.58 (session +201.34) | 16:00 close · cash $519.54 · equity $9,899.59 vs 09:30 $9,727.58 (+172.01; session marks +201.34) · 8 name(s) marked open→close (per-name table). AMX×51 09:30 $23.75 → close $23.62 -6.63; TAL×103 09:30 $11.68 → close $11.69 +1.03; TME×139 09:30 $8.71 → close $8.80 +12.51; BBWI×66 09:30 $18.26 → close $18.90 +42.24; INTU×3 09:30 $323.47 → close $345.88 +67.23; NCNO×62 09:30 $19.33 → close $21.51 +135.16; HEI×3 09:30 $370.00 → close $346.15 -71.55; BE×5 09:30 $213.94 → close $218.21 +21.35 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $519.54 | ▲ 09:30 equity $9,985.96 vs yday $9,899.59 (+86.37) | 09:30 open · cash $519.54 (unchanged overnight, no fees) · equity $9,985.96 vs prior close $9,899.59 (+86.37) · 8 name(s) re-marked at the open (per-name table). AMX×51 yday $23.62 → 09:30 $23.77 +7.65; TAL×103 yday $11.69 → 09:30 $11.62 -7.21; TME×139 yday $8.80 → 09:30 $8.80 +0.00; BBWI×66 yday $18.90 → 09:30 $18.69 -13.86; INTU×3 yday $345.88 → 09:30 $353.54 +22.98; NCNO×62 yday $21.51 → 09:30 $22.03 +32.24; HEI×3 yday $346.15 → 09:30 $346.19 +0.12; BE×5 yday $218.21 → 09:30 $227.10 +44.45 | — |
| 2026-08-27 09:30 ET | **SELL** | `AMX` | 51 | $23.77 | $2.16 | $-5.84 | $1,729.65 | ▼ -5.84 after sell → book $9,983.80; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TAL` | 103 | $11.62 | $2.33 | $-10.81 | $2,924.18 | ▼ -10.81 after sell → book $9,981.47; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TME` | 139 | $8.80 | $2.44 | $+7.66 | $4,144.94 | ▲ +7.66 after sell → book $9,979.03; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBWI` | 66 | $18.69 | $2.21 | $+23.98 | $5,376.28 | ▲ +23.98 after sell → book $9,976.83; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INTU` | 3 | $353.54 | $2.02 | $+86.19 | $6,434.88 | ▲ +86.19 after sell → book $9,974.81; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `NCNO` | 62 | $22.03 | $2.20 | $+163.03 | $7,798.54 | ▲ +163.03 after sell → book $9,972.61; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `HEI` | 3 | $346.19 | $2.02 | $-75.45 | $8,835.09 | ▼ -75.45 after sell → book $9,970.59; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `BE` | 5 | $227.10 | $2.02 | $+61.77 | $9,968.57 | ▲ +61.77 after sell → book $9,968.57; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,968.57 | ▲ close $9,968.57 vs 09:30 $9,985.96 (session +0.00) | 16:00 close · cash $9,968.57 · no lots left · equity $9,968.57. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,968.57 | ▲ 09:30 equity $9,968.57 vs yday $9,968.57 (-0.00) | 09:30 open · cash $9,968.57 · no holdings · equity $9,968.57 vs prior close $9,968.57 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `LEG` | 135 | $9.20 | $2.40 | — | $8,724.17 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; ret5=-2.6; leftover $1246.07 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PLAB` | 41 | $30.01 | $2.11 | — | $7,491.65 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-0.9; leftover $1246.07 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 4 | $306.34 | $2.00 | — | $6,264.29 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-23.0; leftover $1246.07 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SLF` | 15 | $78.95 | $2.04 | — | $5,078.00 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; ret5=+0.4; leftover $1246.07 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KSS` | 68 | $18.25 | $2.19 | — | $3,834.81 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; ret5=+4.7; leftover $1246.07 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟡 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 66 | $18.75 | $2.19 | — | $2,595.12 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer,oppset; ret5=-5.0; leftover $1246.07 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MAIR` | 45 | $27.36 | $2.12 | — | $1,361.79 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+3.5; leftover $1246.07 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HEI` | 3 | $339.95 | $2.00 | — | $339.94 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; ret5=-3.9; leftover $1246.07 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $339.94 | ▼ close $9,730.59 vs 09:30 $9,968.57 (session -220.92) | 16:00 close · cash $339.94 · equity $9,730.59 vs 09:30 $9,968.57 (-237.98; session marks -220.92) · 8 name(s) marked open→close (per-name table). LEG×135 09:30 $9.20 → close $9.20 +0.00; PLAB×41 09:30 $30.01 → close $27.73 -93.48; DY×4 09:30 $306.34 → close $294.34 -48.00; SLF×15 09:30 $78.95 → close $78.76 -2.85; KSS×68 09:30 $18.25 → close $17.50 -51.00; BBWI×66 09:30 $18.75 → close $19.22 +31.02; MAIR×45 09:30 $27.36 → close $26.33 -46.35; HEI×3 09:30 $339.95 → close $336.53 -10.26 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $339.94 | ▲ 09:30 equity $9,735.54 vs yday $9,730.59 (+4.95) | 09:30 open · cash $339.94 (unchanged overnight, no fees) · equity $9,735.54 vs prior close $9,730.59 (+4.95) · 8 name(s) re-marked at the open (per-name table). LEG×135 yday $9.20 → 09:30 $9.20 +0.00; PLAB×41 yday $27.73 → 09:30 $28.04 +12.71; DY×4 yday $294.34 → 09:30 $298.01 +14.68; SLF×15 yday $78.76 → 09:30 $78.70 -0.90; KSS×68 yday $17.50 → 09:30 $17.26 -16.32; BBWI×66 yday $19.22 → 09:30 $19.25 +1.98; MAIR×45 yday $26.33 → 09:30 $26.28 -2.25; HEI×3 yday $336.53 → 09:30 $334.88 -4.95 | — |
| 2026-08-31 09:30 ET | **SELL** | `PLAB` | 41 | $28.04 | $2.13 | $-85.02 | $1,487.45 | ▼ -85.02 after sell → book $9,733.41; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 4 | $298.01 | $2.02 | $-37.34 | $2,677.47 | ▼ -37.34 after sell → book $9,731.39; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLF` | 15 | $78.70 | $2.06 | $-7.84 | $3,855.91 | ▼ -7.84 after sell → book $9,729.33; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KSS` | 68 | $17.26 | $2.22 | $-71.73 | $5,027.38 | ▼ -71.73 after sell → book $9,727.12; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 66 | $19.25 | $2.21 | $+28.60 | $6,295.67 | ▲ +28.60 after sell → book $9,724.91; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MAIR` | 45 | $26.28 | $2.15 | $-52.87 | $7,476.12 | ▼ -52.87 after sell → book $9,722.76; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HEI` | 3 | $334.88 | $2.02 | $-19.23 | $8,478.75 | ▼ -19.23 after sell → book $9,720.75; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,478.75 | ▲ close $9,720.75 vs 09:30 $9,735.54 (session +0.00) | 16:00 close · cash $8,478.75 · equity $9,720.75 vs 09:30 $9,735.54 (-14.79; session marks +0.00) · 1 name(s) marked open→close (per-name table). LEG×135 09:30 $9.20 → close $9.20 +0.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,478.75 | ▲ 09:30 equity $9,720.75 vs yday $9,720.75 (-0.00) | 09:30 open · cash $8,478.75 (unchanged overnight, no fees) · equity $9,720.75 vs prior close $9,720.75 (-0.00) · 1 name(s) re-marked at the open (per-name table). LEG×135 yday $9.20 → 09:30 $9.20 +0.00 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,478.75 | ▲ close $9,720.75 vs 09:30 $9,720.75 (session +0.00) | 16:00 close · cash $8,478.75 · equity $9,720.75 vs 09:30 $9,720.75 (-0.00; session marks +0.00) · 1 name(s) marked open→close (per-name table). LEG×135 09:30 $9.20 → close $9.20 +0.00 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,478.75 | ▲ 09:30 equity $9,720.75 vs yday $9,720.75 (-0.00) | 09:30 open · cash $8,478.75 (unchanged overnight, no fees) · equity $9,720.75 vs prior close $9,720.75 (-0.00) · 1 name(s) re-marked at the open (per-name table). LEG×135 yday $9.20 → 09:30 $9.20 +0.00 | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,478.75 | ▲ close $9,720.75 vs 09:30 $9,720.75 (session +0.00) | 16:00 close · cash $8,478.75 · equity $9,720.75 vs 09:30 $9,720.75 (-0.00; session marks +0.00) · 1 name(s) marked open→close (per-name table). LEG×135 09:30 $9.20 → close $9.20 +0.00 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,478.75 | ▲ 09:30 equity $9,720.75 vs yday $9,720.75 (-0.00) | 09:30 open · cash $8,478.75 (unchanged overnight, no fees) · equity $9,720.75 vs prior close $9,720.75 (-0.00) · 1 name(s) re-marked at the open (per-name table). LEG×135 yday $9.20 → 09:30 $9.20 +0.00 | — |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 19 | $55.42 | $2.05 | — | $7,423.72 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,oppset; ret5=-25.9; leftover $1059.84 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PRMB` | 47 | $22.32 | $2.13 | — | $6,372.55 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; ret5=-4.0; leftover $1059.84 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SNN` | 36 | $29.03 | $2.10 | — | $5,325.37 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-2.2; leftover $1059.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ENOV` | 52 | $20.28 | $2.15 | — | $4,268.66 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-20.0; leftover $1059.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 4 | $257.00 | $2.00 | — | $3,238.66 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-5.5; leftover $1059.84 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NTSK` | 68 | $15.51 | $2.19 | — | $2,181.79 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-1.2; leftover $1059.84 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NTAP` | 6 | $161.95 | $2.01 | — | $1,208.08 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-6.7; leftover $1059.84 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 20 | $52.88 | $2.05 | — | $148.43 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1059.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $148.43 | ▼ close $9,627.98 vs 09:30 $9,720.75 (session -76.09) | 16:00 close · cash $148.43 · equity $9,627.98 vs 09:30 $9,720.75 (-92.77; session marks -76.09) · 9 name(s) marked open→close (per-name table). LEG×135 09:30 $9.20 → close $9.20 +0.00; EIX×19 09:30 $55.42 → close $56.30 +16.72; PRMB×47 09:30 $22.32 → close $21.91 -19.27; SNN×36 09:30 $29.03 → close $28.69 -12.24; ENOV×52 09:30 $20.28 → close $19.40 -45.76; FIVE×4 09:30 $257.00 → close $239.96 -68.16; NTSK×68 09:30 $15.51 → close $14.34 -79.56; NTAP×6 09:30 $161.95 → close $185.38 +140.58; ATRC×20 09:30 $52.88 → close $52.46 -8.40 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $148.43 | ▼ 09:30 equity $9,556.43 vs yday $9,627.98 (-71.55) | 09:30 open · cash $148.43 (unchanged overnight, no fees) · equity $9,556.43 vs prior close $9,627.98 (-71.55) · 9 name(s) re-marked at the open (per-name table). LEG×135 yday $9.20 → 09:30 $9.20 +0.00; EIX×19 yday $56.30 → 09:30 $55.79 -9.69; PRMB×47 yday $21.91 → 09:30 $21.87 -1.88; SNN×36 yday $28.69 → 09:30 $28.77 +2.88; ENOV×52 yday $19.40 → 09:30 $19.01 -20.28; FIVE×4 yday $239.96 → 09:30 $238.88 -4.32; NTSK×68 yday $14.34 → 09:30 $14.15 -12.92; NTAP×6 yday $185.38 → 09:30 $182.59 -16.74; ATRC×20 yday $52.46 → 09:30 $52.03 -8.60 | — |
| 2026-09-04 09:30 ET | **SELL** | `LEG` | 135 | $9.20 | $2.43 | $-4.82 | $1,388.00 | ▼ -4.82 after sell → book $9,554.00; vs 09:30 mark -2.43 | dropped from list after 5 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 19 | $55.79 | $2.07 | $+2.92 | $2,445.95 | ▲ +2.92 after sell → book $9,551.94; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PRMB` | 47 | $21.87 | $2.15 | $-25.43 | $3,471.68 | ▼ -25.43 after sell → book $9,549.78; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SNN` | 36 | $28.77 | $2.12 | $-13.58 | $4,505.29 | ▼ -13.58 after sell → book $9,547.67; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ENOV` | 52 | $19.01 | $2.17 | $-70.35 | $5,491.64 | ▼ -70.35 after sell → book $9,545.50; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 4 | $238.88 | $2.02 | $-76.50 | $6,445.14 | ▼ -76.50 after sell → book $9,543.48; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NTSK` | 68 | $14.15 | $2.22 | $-96.89 | $7,405.12 | ▼ -96.89 after sell → book $9,541.26; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NTAP` | 6 | $182.59 | $2.03 | $+119.80 | $8,498.64 | ▲ +119.80 after sell → book $9,539.24; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 20 | $52.03 | $2.07 | $-21.12 | $9,537.17 | ▼ -21.12 after sell → book $9,537.17; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `AMX` | 51 | $23.03 | $2.14 | — | $8,360.49 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-1.4; leftover $1192.15 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BULL` | 121 | $9.79 | $2.35 | — | $7,173.55 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+4.5; leftover $1192.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 136 | $8.74 | $2.40 | — | $5,982.51 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-0.8; leftover $1192.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 8 | $137.35 | $2.01 | — | $4,881.70 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+5.4; leftover $1192.15 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRCL` | 12 | $97.98 | $2.03 | — | $3,703.91 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+9.5; leftover $1192.15 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 17 | $68.52 | $2.04 | — | $2,537.03 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+3.4; leftover $1192.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 7 | $167.55 | $2.01 | — | $1,362.17 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; ret5=+0.9; leftover $1192.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BLSH` | 34 | $34.69 | $2.09 | — | $180.62 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+7.9; leftover $1192.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $180.62 | ▲ close $9,621.23 vs 09:30 $9,556.43 (session +101.14) | 16:00 close · cash $180.62 · equity $9,621.23 vs 09:30 $9,556.43 (+64.80; session marks +101.14) · 8 name(s) marked open→close (per-name table). AMX×51 09:30 $23.03 → close $23.00 -1.53; BULL×121 09:30 $9.79 → close $9.74 -6.05; ASAN×136 09:30 $8.74 → close $8.81 +9.52; MSTR×8 09:30 $137.35 → close $142.80 +43.60; CRCL×12 09:30 $97.98 → close $102.05 +48.84; DOCU×17 09:30 $68.52 → close $68.41 -1.87; GWRE×7 09:30 $167.55 → close $162.42 -35.91; BLSH×34 09:30 $34.69 → close $36.00 +44.54 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $180.62 | ▼ 09:30 equity $9,543.53 vs yday $9,621.23 (-77.70) | 09:30 open · cash $180.62 (unchanged overnight, no fees) · equity $9,543.53 vs prior close $9,621.23 (-77.70) · 8 name(s) re-marked at the open (per-name table). AMX×51 yday $23.00 → 09:30 $23.15 +7.65; BULL×121 yday $9.74 → 09:30 $9.94 +23.60; ASAN×136 yday $8.81 → 09:30 $8.73 -10.88; MSTR×8 yday $142.80 → 09:30 $137.62 -41.44; CRCL×12 yday $102.05 → 09:30 $100.65 -16.80; DOCU×17 yday $68.41 → 09:30 $67.05 -23.12; GWRE×7 yday $162.42 → 09:30 $160.52 -13.30; BLSH×34 yday $36.00 → 09:30 $35.90 -3.40 | — |
| 2026-09-08 09:30 ET | **SELL** | `AMX` | 51 | $23.15 | $2.16 | $+1.81 | $1,359.10 | ▲ +1.81 after sell → book $9,541.37; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BULL` | 121 | $9.94 | $2.38 | $+12.81 | $2,558.86 | ▲ +12.81 after sell → book $9,538.99; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASAN` | 136 | $8.73 | $2.43 | $-6.19 | $3,743.71 | ▼ -6.19 after sell → book $9,536.56; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MSTR` | 8 | $137.62 | $2.03 | $-1.89 | $4,842.63 | ▼ -1.89 after sell → book $9,534.52; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `CRCL` | 12 | $100.65 | $2.05 | $+27.97 | $6,048.39 | ▲ +27.97 after sell → book $9,532.48; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 17 | $67.05 | $2.06 | $-29.09 | $7,186.17 | ▼ -29.09 after sell → book $9,530.41; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 7 | $160.52 | $2.03 | $-53.25 | $8,307.78 | ▼ -53.25 after sell → book $9,528.38; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `BLSH` | 34 | $35.90 | $2.11 | $+36.94 | $9,526.27 | ▲ +36.94 after sell → book $9,526.27; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,526.27 | ▲ close $9,526.27 vs 09:30 $9,543.53 (session +0.00) | 16:00 close · cash $9,526.27 · no lots left · equity $9,526.27. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,526.27 | ▲ 09:30 equity $9,526.27 vs yday $9,526.27 (+0.00) | 09:30 open · cash $9,526.27 · no holdings · equity $9,526.27 vs prior close $9,526.27 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,526.27 | ▲ close $9,526.27 vs 09:30 $9,526.27 (session +0.00) | 16:00 close · cash $9,526.27 · no lots left · equity $9,526.27. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,526.27 | ▲ 09:30 equity $9,526.27 vs yday $9,526.27 (+0.00) | 09:30 open · cash $9,526.27 · no holdings · equity $9,526.27 vs prior close $9,526.27 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,526.27 | ▲ close $9,526.27 vs 09:30 $9,526.27 (session +0.00) | 16:00 close · cash $9,526.27 · no lots left · equity $9,526.27. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,526.27 | ▲ 09:30 equity $9,526.27 vs yday $9,526.27 (+0.00) | 09:30 open · cash $9,526.27 · no holdings · equity $9,526.27 vs prior close $9,526.27 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ACVA` | 113 | $10.46 | $2.33 | — | $8,342.53 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.0; leftover $1190.78 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `DRVN` | 94 | $12.55 | $2.27 | — | $7,160.56 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; ret5=+6.4; leftover $1190.78 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ENB` | 24 | $48.37 | $2.06 | — | $5,997.61 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; ret5=-0.2; leftover $1190.78 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CNQ` | 23 | $49.94 | $2.06 | — | $4,846.93 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; ret5=+1.7; leftover $1190.78 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `KMB` | 11 | $99.24 | $2.02 | — | $3,753.27 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-4.7; leftover $1190.78 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BTI` | 21 | $56.03 | $2.05 | — | $2,574.59 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-0.8; leftover $1190.78 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INGM` | 44 | $26.62 | $2.12 | — | $1,401.19 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; ret5=+0.7; leftover $1190.78 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PBR` | 56 | $21.21 | $2.16 | — | $211.27 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+2.5; leftover $1190.78 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $211.27 | ▼ close $9,466.64 vs 09:30 $9,526.27 (session -42.55) | 16:00 close · cash $211.27 · equity $9,466.64 vs 09:30 $9,526.27 (-59.63; session marks -42.55) · 8 name(s) marked open→close (per-name table). ACVA×113 09:30 $10.46 → close $10.41 -5.08; DRVN×94 09:30 $12.55 → close $12.15 -37.60; ENB×24 09:30 $48.37 → close $47.76 -14.64; CNQ×23 09:30 $49.94 → close $50.07 +2.99; KMB×11 09:30 $99.24 → close $98.15 -11.99; BTI×21 09:30 $56.03 → close $55.24 -16.59; INGM×44 09:30 $26.62 → close $27.55 +40.92; PBR×56 09:30 $21.21 → close $21.20 -0.56 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $211.27 | ▲ 09:30 equity $9,526.17 vs yday $9,466.64 (+59.53) | 09:30 open · cash $211.27 (unchanged overnight, no fees) · equity $9,526.17 vs prior close $9,466.64 (+59.53) · 8 name(s) re-marked at the open (per-name table). ACVA×113 yday $10.41 → 09:30 $10.42 +1.13; DRVN×94 yday $12.15 → 09:30 $12.33 +16.92; ENB×24 yday $47.76 → 09:30 $47.85 +2.16; CNQ×23 yday $50.07 → 09:30 $50.76 +15.87; KMB×11 yday $98.15 → 09:30 $99.18 +11.33; BTI×21 yday $55.24 → 09:30 $57.12 +39.48; INGM×44 yday $27.55 → 09:30 $26.89 -29.04; PBR×56 yday $21.20 → 09:30 $21.23 +1.68 | — |
| 2026-09-14 09:30 ET | **SELL** | `ACVA` | 113 | $10.42 | $2.36 | $-8.64 | $1,386.37 | ▼ -8.64 after sell → book $9,523.81; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `DRVN` | 94 | $12.33 | $2.30 | $-25.25 | $2,543.09 | ▼ -25.25 after sell → book $9,521.51; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ENB` | 24 | $47.85 | $2.08 | $-16.62 | $3,689.41 | ▼ -16.62 after sell → book $9,519.43; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CNQ` | 23 | $50.76 | $2.08 | $+14.72 | $4,854.81 | ▲ +14.72 after sell → book $9,517.35; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `KMB` | 11 | $99.18 | $2.04 | $-4.73 | $5,943.75 | ▼ -4.73 after sell → book $9,515.31; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BTI` | 21 | $57.12 | $2.07 | $+18.76 | $7,141.20 | ▲ +18.76 after sell → book $9,513.24; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PBR` | 56 | $21.23 | $2.18 | $-3.22 | $8,327.90 | ▼ -3.22 after sell → book $9,511.06; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,327.90 | ▼ close $9,509.30 vs 09:30 $9,526.17 (session -1.76) | 16:00 close · cash $8,327.90 · equity $9,509.30 vs 09:30 $9,526.17 (-16.87; session marks -1.76) · 1 name(s) marked open→close (per-name table). INGM×44 09:30 $26.89 → close $26.85 -1.76 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,327.90 | ▲ 09:30 equity $9,511.94 vs yday $9,509.30 (+2.64) | 09:30 open · cash $8,327.90 (unchanged overnight, no fees) · equity $9,511.94 vs prior close $9,509.30 (+2.64) · 1 name(s) re-marked at the open (per-name table). INGM×44 yday $26.85 → 09:30 $26.91 +2.64 | — |
| 2026-09-15 09:30 ET | **SELL** | `INGM` | 44 | $26.91 | $2.14 | $+8.50 | $9,509.80 | ▲ +8.50 after sell → book $9,509.80; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,509.80 | ▲ close $9,509.80 vs 09:30 $9,511.94 (session +0.00) | 16:00 close · cash $9,509.80 · no lots left · equity $9,509.80. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,509.80 | ▲ 09:30 equity $9,509.80 vs yday $9,509.80 (-0.00) | 09:30 open · cash $9,509.80 · no holdings · equity $9,509.80 vs prior close $9,509.80 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `BWIN` | 36 | $32.25 | $2.10 | — | $8,346.70 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-3.7; leftover $1188.72 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `HLMN` | 163 | $7.26 | $2.48 | — | $7,160.84 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-1.9; leftover $1188.72 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SYY` | 14 | $79.73 | $2.03 | — | $6,042.59 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; ret5=-1.5; leftover $1188.72 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SRRK` | 23 | $50.01 | $2.06 | — | $4,890.30 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; ret5=+1.1; leftover $1188.72 | join🟡 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ARVN` | 143 | $8.31 | $2.42 | — | $3,699.55 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; ret5=-1.3; leftover $1188.72 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `AMX` | 51 | $23.18 | $2.14 | — | $2,515.23 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-0.2; leftover $1188.72 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `GNW` | 118 | $10.00 | $2.34 | — | $1,332.88 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+4.8; leftover $1188.72 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `PBF` | 16 | $73.02 | $2.04 | — | $162.52 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; ret5=+3.9; leftover $1188.72 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $162.52 | ▼ close $9,490.93 vs 09:30 $9,509.80 (session -1.25) | 16:00 close · cash $162.52 · equity $9,490.93 vs 09:30 $9,509.80 (-18.87; session marks -1.25) · 8 name(s) marked open→close (per-name table). BWIN×36 09:30 $32.25 → close $32.04 -7.56; HLMN×163 09:30 $7.26 → close $7.32 +9.78; SYY×14 09:30 $79.73 → close $78.71 -14.28; SRRK×23 09:30 $50.01 → close $49.37 -14.72; ARVN×143 09:30 $8.31 → close $8.16 -21.45; AMX×51 09:30 $23.18 → close $22.98 -10.20; GNW×118 09:30 $10.00 → close $10.09 +10.62; PBF×16 09:30 $73.02 → close $75.93 +46.56 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $162.52 | ▲ 09:30 equity $9,531.89 vs yday $9,490.93 (+40.96) | 09:30 open · cash $162.52 (unchanged overnight, no fees) · equity $9,531.89 vs prior close $9,490.93 (+40.96) · 8 name(s) re-marked at the open (per-name table). BWIN×36 yday $32.04 → 09:30 $32.06 +0.72; HLMN×163 yday $7.32 → 09:30 $7.51 +30.97; SYY×14 yday $78.71 → 09:30 $79.03 +4.48; SRRK×23 yday $49.37 → 09:30 $49.52 +3.45; ARVN×143 yday $8.16 → 09:30 $8.29 +18.59; AMX×51 yday $22.98 → 09:30 $23.09 +5.61; GNW×118 yday $10.09 → 09:30 $10.12 +3.54; PBF×16 yday $75.93 → 09:30 $74.28 -26.40 | — |
| 2026-09-17 09:30 ET | **SELL** | `BWIN` | 36 | $32.06 | $2.12 | $-11.06 | $1,314.57 | ▼ -11.06 after sell → book $9,529.78; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `HLMN` | 163 | $7.51 | $2.52 | $+35.75 | $2,536.18 | ▲ +35.75 after sell → book $9,527.26; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SYY` | 14 | $79.03 | $2.05 | $-13.88 | $3,640.55 | ▼ -13.88 after sell → book $9,525.21; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ARVN` | 143 | $8.29 | $2.45 | $-7.73 | $4,823.57 | ▼ -7.73 after sell → book $9,522.76; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AMX` | 51 | $23.09 | $2.16 | $-8.90 | $5,998.99 | ▼ -8.90 after sell → book $9,520.59; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `GNW` | 118 | $10.12 | $2.37 | $+9.44 | $7,190.78 | ▲ +9.44 after sell → book $9,518.22; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `PBF` | 16 | $74.28 | $2.06 | $+16.06 | $8,377.20 | ▲ +16.06 after sell → book $9,516.16; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `WWD` | 3 | $329.36 | $2.00 | — | $7,387.12 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-0.2; leftover $1196.74 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `KNX` | 17 | $66.85 | $2.04 | — | $6,248.63 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+3.3; leftover $1196.74 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `HTLD` | 99 | $12.03 | $2.29 | — | $5,055.37 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-0.3; leftover $1196.74 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `LIFE` | 30 | $39.67 | $2.08 | — | $3,863.19 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-2.9; leftover $1196.74 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AXON` | 2 | $468.73 | $2.00 | — | $2,923.74 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; ret5=-10.8; leftover $1196.74 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `KEY` | 57 | $20.98 | $2.16 | — | $1,725.72 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.8; leftover $1196.74 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GRAL` | 15 | $75.29 | $2.04 | — | $594.33 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; ret5=-3.1; leftover $1196.74 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $594.33 | ▼ close $9,386.34 vs 09:30 $9,531.89 (session -115.22) | 16:00 close · cash $594.33 · equity $9,386.34 vs 09:30 $9,531.89 (-145.55; session marks -115.22) · 8 name(s) marked open→close (per-name table). SRRK×23 09:30 $49.52 → close $49.02 -11.50; WWD×3 09:30 $329.36 → close $319.56 -29.40; KNX×17 09:30 $66.85 → close $66.67 -3.06; HTLD×99 09:30 $12.03 → close $11.92 -10.89; LIFE×30 09:30 $39.67 → close $36.40 -98.10; AXON×2 09:30 $468.73 → close $453.50 -30.46; KEY×57 09:30 $20.98 → close $20.95 -1.71; GRAL×15 09:30 $75.29 → close $79.95 +69.90 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $594.33 | ▲ 09:30 equity $9,398.47 vs yday $9,386.34 (+12.13) | 09:30 open · cash $594.33 (unchanged overnight, no fees) · equity $9,398.47 vs prior close $9,386.34 (+12.13) · 8 name(s) re-marked at the open (per-name table). SRRK×23 yday $49.02 → 09:30 $48.02 -23.00; WWD×3 yday $319.56 → 09:30 $320.02 +1.38; KNX×17 yday $66.67 → 09:30 $66.65 -0.34; HTLD×99 yday $11.92 → 09:30 $11.87 -4.95; LIFE×30 yday $36.40 → 09:30 $36.89 +14.70; AXON×2 yday $453.50 → 09:30 $455.62 +4.24; KEY×57 yday $20.95 → 09:30 $20.90 -2.85; GRAL×15 yday $79.95 → 09:30 $81.48 +22.95 | — |
| 2026-09-18 09:30 ET | **SELL** | `SRRK` | 23 | $48.02 | $2.08 | $-49.91 | $1,696.71 | ▼ -49.91 after sell → book $9,396.39; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `WWD` | 3 | $320.02 | $2.02 | $-32.04 | $2,654.75 | ▼ -32.04 after sell → book $9,394.37; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `KNX` | 17 | $66.65 | $2.06 | $-7.50 | $3,785.74 | ▼ -7.50 after sell → book $9,392.31; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `HTLD` | 99 | $11.87 | $2.31 | $-20.44 | $4,958.56 | ▼ -20.44 after sell → book $9,390.00; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LIFE` | 30 | $36.89 | $2.10 | $-87.58 | $6,063.16 | ▼ -87.58 after sell → book $9,387.90; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `AXON` | 2 | $455.62 | $2.02 | $-30.23 | $6,972.38 | ▼ -30.23 after sell → book $9,385.88; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `KEY` | 57 | $20.90 | $2.18 | $-8.90 | $8,161.50 | ▼ -8.90 after sell → book $9,383.70; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `GRAL` | 15 | $81.48 | $2.06 | $+88.76 | $9,381.65 | ▲ +88.76 after sell → book $9,381.65; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `QSR` | 16 | $73.00 | $2.04 | — | $8,211.61 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+0.5; leftover $1172.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `HLN` | 127 | $9.20 | $2.37 | — | $7,040.84 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-7.8; leftover $1172.71 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `ACVA` | 111 | $10.48 | $2.32 | — | $5,875.24 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.0; leftover $1172.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `JXN` | 9 | $129.00 | $2.02 | — | $4,712.22 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; ret5=+2.4; leftover $1172.71 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `DRVN` | 95 | $12.26 | $2.27 | — | $3,545.24 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+6.4; leftover $1172.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `MNR` | 107 | $10.95 | $2.31 | — | $2,371.28 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; ret5=+1.7; leftover $1172.71 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `INIO` | 56 | $20.80 | $2.16 | — | $1,204.32 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+8.0; leftover $1172.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `CRWV` | 14 | $79.83 | $2.03 | — | $84.67 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+5.2; leftover $1172.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $84.67 | ▲ close $9,467.57 vs 09:30 $9,398.47 (session +103.45) | 16:00 close · cash $84.67 · equity $9,467.57 vs 09:30 $9,398.47 (+69.10; session marks +103.45) · 8 name(s) marked open→close (per-name table). QSR×16 09:30 $73.00 → close $72.88 -1.92; HLN×127 09:30 $9.20 → close $9.28 +10.16; ACVA×111 09:30 $10.48 → close $10.48 +0.00; JXN×9 09:30 $129.00 → close $132.67 +33.03; DRVN×95 09:30 $12.26 → close $12.28 +1.90; MNR×107 09:30 $10.95 → close $11.13 +19.26; INIO×56 09:30 $20.80 → close $21.15 +19.60; CRWV×14 09:30 $79.83 → close $81.36 +21.42 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `MAIR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `EIX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDAY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OPLN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `CBRS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBNX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `COHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AAOI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEVA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `CRCT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EROC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `TWO` | no_price | no 09:30 open |
| 2026-08-31 | `LEG` | no_price | no 09:30 open — carry |
| 2026-08-31 | `BNTX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GPRE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FNKO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `LEG` | no_price | no 09:30 open — carry |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `HAL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KRMN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OBE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `LEG` | no_price | no 09:30 open — carry |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `COLL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CAE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VIR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SDGR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `LEG` | no_price | no 09:30 open — carry |
| 2026-09-08 | `CHA` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GPGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `KMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BILI` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `STX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `INGM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XYL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `APG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LFST` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NEOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CGNT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `KHC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RCUS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SPSC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TTAN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `KIM` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `COO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `BWIN` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ACVA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `AQN` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SRRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TTAN` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TAC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CVE` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `QSR` | 16 | 2026-09-18 @ $73.00 | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+0.5; leftover $1172.71 |
| `HLN` | 127 | 2026-09-18 @ $9.20 | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-7.8; leftover $1172.71 |
| `ACVA` | 111 | 2026-09-18 @ $10.48 | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.0; leftover $1172.71 |
| `JXN` | 9 | 2026-09-18 @ $129.00 | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; ret5=+2.4; leftover $1172.71 |
| `DRVN` | 95 | 2026-09-18 @ $12.26 | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+6.4; leftover $1172.71 |
| `MNR` | 107 | 2026-09-18 @ $10.95 | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; ret5=+1.7; leftover $1172.71 |
| `INIO` | 56 | 2026-09-18 @ $20.80 | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+8.0; leftover $1172.71 |
| `CRWV` | 14 | 2026-09-18 @ $79.83 | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+5.2; leftover $1172.71 |
