# Factor mine action — `short_rsi_ob_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · RSI overbought

Cash book **-10.13%** ($8,987) · signal-only (no cash/fees) was -11.11%. Starts YES **0/26**. Fills 158 · skips 81 · realized $-1012.85.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior RSI is overbought (≥70).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a SHORT sleeve: it borrows the name and profits if the price falls. Equity treats the short as a liability (must keep enough to cover).

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `rsi_ob=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,987.16.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TPG` | 49 | — | $50.62 | +0.00 | $54.62 | -195.84 | -195.84 | -0.00 | -195.84 |
| 2026-08-13 | `TNDM` | 107 | — | $23.33 | +0.00 | $23.13 | +21.40 | +21.40 | -0.00 | +21.40 |
| 2026-08-14 | `TPG` | 49 | $54.62 | $55.29 | -32.83 | — | +0.00 | -32.83 | -228.67 | — |
| 2026-08-14 | `TNDM` | 107 | $23.13 | $22.92 | +22.47 | — | +0.00 | +22.47 | +43.87 | — |
| 2026-08-14 | `ARX` | 31 | — | $19.57 | +0.00 | $19.58 | -0.31 | -0.31 | -0.00 | -0.31 |
| 2026-08-14 | `OMER` | 35 | — | $17.35 | +0.00 | $17.19 | +5.60 | +5.60 | -0.00 | +5.60 |
| 2026-08-14 | `AIRO` | 55 | — | $11.12 | +0.00 | $9.57 | +85.25 | +85.25 | -0.00 | +85.25 |
| 2026-08-14 | `MXCT` | 440 | — | $1.39 | +0.00 | $1.32 | +30.80 | +30.80 | -0.00 | +30.80 |
| 2026-08-14 | `TBBB` | 12 | — | $48.82 | +0.00 | $47.79 | +12.36 | +12.36 | -0.00 | +12.36 |
| 2026-08-14 | `AMPY` | 124 | — | $4.94 | +0.00 | $4.78 | +19.84 | +19.84 | -0.00 | +19.84 |
| 2026-08-14 | `MH` | 45 | — | $13.55 | +0.00 | $13.10 | +20.25 | +20.25 | -0.00 | +20.25 |
| 2026-08-14 | `CRDL` | 354 | — | $1.73 | +0.00 | $1.80 | -24.78 | -24.78 | -0.00 | -24.78 |
| 2026-08-17 | `ARX` | 31 | $19.58 | $19.57 | +0.31 | — | +0.00 | +0.31 | -0.00 | — |
| 2026-08-17 | `OMER` | 35 | $17.19 | $17.17 | +0.70 | — | +0.00 | +0.70 | +6.30 | — |
| 2026-08-17 | `AIRO` | 55 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | +85.25 | — |
| 2026-08-17 | `MXCT` | 440 | $1.32 | $1.32 | +0.00 | — | +0.00 | +0.00 | +30.80 | — |
| 2026-08-17 | `TBBB` | 12 | $47.79 | $47.39 | +4.80 | — | +0.00 | +4.80 | +17.16 | — |
| 2026-08-17 | `AMPY` | 124 | $4.78 | $4.86 | -9.92 | — | +0.00 | -9.92 | +9.92 | — |
| 2026-08-17 | `MH` | 45 | $13.10 | $13.16 | -2.70 | — | +0.00 | -2.70 | +17.55 | — |
| 2026-08-17 | `CRDL` | 354 | $1.80 | $1.73 | +24.78 | — | +0.00 | +24.78 | -0.00 | — |
| 2026-08-17 | `HTFL` | 15 | — | $41.23 | +0.00 | $41.94 | -10.65 | -10.65 | -0.00 | -10.65 |
| 2026-08-17 | `UMAC` | 19 | — | $32.55 | +0.00 | $30.15 | +45.60 | +45.60 | -0.00 | +45.60 |
| 2026-08-17 | `NPWR` | 323 | — | $1.92 | +0.00 | $1.73 | +61.37 | +61.37 | -0.00 | +61.37 |
| 2026-08-17 | `NMAX` | 56 | — | $10.97 | +0.00 | $10.36 | +34.16 | +34.16 | -0.00 | +34.16 |
| 2026-08-17 | `CLYM` | 38 | — | $16.25 | +0.00 | $17.44 | -45.22 | -45.22 | -0.00 | -45.22 |
| 2026-08-17 | `XHG` | 148 | — | $4.19 | +0.00 | $3.91 | +41.44 | +41.44 | -0.00 | +41.44 |
| 2026-08-17 | `SGMT` | 59 | — | $10.45 | +0.00 | $10.43 | +1.18 | +1.18 | -0.00 | +1.18 |
| 2026-08-17 | `U` | 13 | — | $46.21 | +0.00 | $45.45 | +9.88 | +9.88 | -0.00 | +9.88 |
| 2026-08-18 | `HTFL` | 15 | $41.94 | $41.50 | +6.60 | — | +0.00 | +6.60 | -4.05 | — |
| 2026-08-18 | `UMAC` | 19 | $30.15 | $28.59 | +29.64 | — | +0.00 | +29.64 | +75.24 | — |
| 2026-08-18 | `NPWR` | 323 | $1.73 | $1.70 | +9.69 | — | +0.00 | +9.69 | +71.06 | — |
| 2026-08-18 | `NMAX` | 56 | $10.36 | $10.31 | +2.80 | — | +0.00 | +2.80 | +36.96 | — |
| 2026-08-18 | `CLYM` | 38 | $17.44 | $16.90 | +20.52 | — | +0.00 | +20.52 | -24.70 | — |
| 2026-08-18 | `XHG` | 148 | $3.91 | $3.94 | -4.44 | — | +0.00 | -4.44 | +37.00 | — |
| 2026-08-18 | `SGMT` | 59 | $10.43 | $10.41 | +1.18 | — | +0.00 | +1.18 | +2.36 | — |
| 2026-08-18 | `U` | 13 | $45.45 | $45.50 | -0.65 | — | +0.00 | -0.65 | +9.23 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `KGC` | 21 | — | $29.63 | +0.00 | $31.43 | -37.80 | -37.80 | -0.00 | -37.80 |
| 2026-08-20 | `WPM` | 4 | — | $144.54 | +0.00 | $150.25 | -22.84 | -22.84 | -0.00 | -22.84 |
| 2026-08-20 | `AEM` | 3 | — | $204.45 | +0.00 | $212.04 | -22.77 | -22.77 | -0.00 | -22.77 |
| 2026-08-20 | `SCZM` | 66 | — | $9.46 | +0.00 | $9.76 | -19.80 | -19.80 | -0.00 | -19.80 |
| 2026-08-20 | `MRNA` | 4 | — | $150.14 | +0.00 | $133.32 | +67.28 | +67.28 | -0.00 | +67.28 |
| 2026-08-20 | `CYPH` | 548 | — | $1.15 | +0.00 | $1.19 | -21.92 | -21.92 | -0.00 | -21.92 |
| 2026-08-20 | `BNTX` | 5 | — | $109.06 | +0.00 | $110.89 | -9.15 | -9.15 | -0.00 | -9.15 |
| 2026-08-20 | `EL` | 6 | — | $97.43 | +0.00 | $96.15 | +7.68 | +7.68 | -0.00 | +7.68 |
| 2026-08-21 | `KGC` | 21 | $31.43 | $32.17 | -15.54 | — | +0.00 | -15.54 | -53.34 | — |
| 2026-08-21 | `WPM` | 4 | $150.25 | $154.70 | -17.80 | — | +0.00 | -17.80 | -40.64 | — |
| 2026-08-21 | `AEM` | 3 | $212.04 | $216.30 | -12.78 | $216.06 | +0.72 | -12.06 | -35.55 | -34.83 |
| 2026-08-21 | `SCZM` | 66 | $9.76 | $10.26 | -33.00 | — | +0.00 | -33.00 | -52.80 | — |
| 2026-08-21 | `MRNA` | 4 | $133.32 | $133.11 | +0.84 | — | +0.00 | +0.84 | +68.12 | — |
| 2026-08-21 | `CYPH` | 548 | $1.19 | $1.32 | -71.24 | $1.42 | -54.80 | -126.04 | -93.16 | -147.96 |
| 2026-08-21 | `BNTX` | 5 | $110.89 | $110.92 | -0.15 | — | +0.00 | -0.15 | -9.30 | — |
| 2026-08-21 | `EL` | 6 | $96.15 | $96.75 | -3.60 | — | +0.00 | -3.60 | +4.08 | — |
| 2026-08-21 | `AU` | 6 | — | $119.43 | +0.00 | $121.22 | -10.74 | -10.74 | -0.00 | -10.74 |
| 2026-08-21 | `ARCT` | 73 | — | $11.13 | +0.00 | $13.45 | -169.36 | -169.36 | -0.00 | -169.36 |
| 2026-08-21 | `CRDL` | 425 | — | $1.93 | +0.00 | $1.86 | +29.75 | +29.75 | -0.00 | +29.75 |
| 2026-08-21 | `GMAB` | 24 | — | $33.36 | +0.00 | $33.45 | -2.16 | -2.16 | -0.00 | -2.16 |
| 2026-08-21 | `MRVI` | 99 | — | $8.28 | +0.00 | $8.64 | -35.64 | -35.64 | -0.00 | -35.64 |
| 2026-08-21 | `DFDV` | 203 | — | $4.04 | +0.00 | $3.94 | +20.30 | +20.30 | -0.00 | +20.30 |
| 2026-08-24 | `AEM` | 3 | $216.06 | $217.03 | -2.91 | — | +0.00 | -2.91 | -37.74 | — |
| 2026-08-24 | `CYPH` | 548 | $1.42 | $1.83 | -224.68 | — | +0.00 | -224.68 | -372.64 | — |
| 2026-08-24 | `AU` | 6 | $121.22 | $120.51 | +4.26 | — | +0.00 | +4.26 | -6.48 | — |
| 2026-08-24 | `ARCT` | 73 | $13.45 | $13.33 | +8.76 | $14.34 | -73.73 | -64.97 | -160.60 | -234.33 |
| 2026-08-24 | `CRDL` | 425 | $1.86 | $1.88 | -8.50 | — | +0.00 | -8.50 | +21.25 | — |
| 2026-08-24 | `GMAB` | 24 | $33.45 | $32.82 | +15.12 | — | +0.00 | +15.12 | +12.96 | — |
| 2026-08-24 | `MRVI` | 99 | $8.64 | $8.59 | +4.95 | — | +0.00 | +4.95 | -30.69 | — |
| 2026-08-24 | `DFDV` | 203 | $3.94 | $4.16 | -44.66 | — | +0.00 | -44.66 | -24.36 | — |
| 2026-08-25 | `ARCT` | 73 | $14.34 | $14.12 | +16.06 | — | +0.00 | +16.06 | -218.27 | — |
| 2026-08-25 | `LIFE` | 15 | — | $36.96 | +0.00 | $38.56 | -24.00 | -24.00 | -0.00 | -24.00 |
| 2026-08-25 | `BMEA` | 355 | — | $1.63 | +0.00 | $1.73 | -35.50 | -35.50 | -0.00 | -35.50 |
| 2026-08-25 | `ALVO` | 110 | — | $5.24 | +0.00 | $5.05 | +20.90 | +20.90 | -0.00 | +20.90 |
| 2026-08-25 | `CYPH` | 371 | — | $1.56 | +0.00 | $1.64 | -29.68 | -29.68 | -0.00 | -29.68 |
| 2026-08-25 | `FWDI` | 101 | — | $5.71 | +0.00 | $6.05 | -34.34 | -34.34 | -0.00 | -34.34 |
| 2026-08-25 | `ASST` | 30 | — | $19.04 | +0.00 | $21.39 | -70.50 | -70.50 | -0.00 | -70.50 |
| 2026-08-25 | `JANX` | 30 | — | $18.72 | +0.00 | $18.68 | +1.20 | +1.20 | -0.00 | +1.20 |
| 2026-08-25 | `BMNR` | 24 | — | $23.80 | +0.00 | $24.82 | -24.48 | -24.48 | -0.00 | -24.48 |
| 2026-08-26 | `LIFE` | 15 | $38.56 | $38.24 | +4.80 | — | +0.00 | +4.80 | -19.20 | — |
| 2026-08-26 | `BMEA` | 355 | $1.73 | $1.75 | -8.87 | — | +0.00 | -8.87 | -44.38 | — |
| 2026-08-26 | `ALVO` | 110 | $5.05 | $4.98 | +7.70 | — | +0.00 | +7.70 | +28.60 | — |
| 2026-08-26 | `CYPH` | 371 | $1.64 | $1.60 | +14.84 | — | +0.00 | +14.84 | -14.84 | — |
| 2026-08-26 | `FWDI` | 101 | $6.05 | $5.97 | +8.08 | — | +0.00 | +8.08 | -26.26 | — |
| 2026-08-26 | `ASST` | 30 | $21.39 | $20.72 | +20.10 | — | +0.00 | +20.10 | -50.40 | — |
| 2026-08-26 | `JANX` | 30 | $18.68 | $18.59 | +2.70 | — | +0.00 | +2.70 | +3.90 | — |
| 2026-08-26 | `BMNR` | 24 | $24.82 | $24.24 | +13.92 | — | +0.00 | +13.92 | -10.56 | — |
| 2026-08-26 | `SENS` | 59 | — | $9.48 | +0.00 | $9.34 | +8.26 | +8.26 | -0.00 | +8.26 |
| 2026-08-26 | `KURA` | 41 | — | $13.63 | +0.00 | $13.06 | +23.37 | +23.37 | -0.00 | +23.37 |
| 2026-08-26 | `XHG` | 149 | — | $3.81 | +0.00 | $4.06 | -37.25 | -37.25 | -0.00 | -37.25 |
| 2026-08-26 | `BTG` | 98 | — | $5.75 | +0.00 | $5.74 | +0.98 | +0.98 | -0.00 | +0.98 |
| 2026-08-26 | `MRK` | 3 | — | $154.35 | +0.00 | $153.10 | +3.75 | +3.75 | -0.00 | +3.75 |
| 2026-08-26 | `B` | 11 | — | $48.18 | +0.00 | $47.00 | +12.98 | +12.98 | -0.00 | +12.98 |
| 2026-08-26 | `PEPG` | 166 | — | $3.41 | +0.00 | $3.22 | +31.54 | +31.54 | -0.00 | +31.54 |
| 2026-08-26 | `CRDL` | 280 | — | $2.03 | +0.00 | $2.14 | -30.80 | -30.80 | -0.00 | -30.80 |
| 2026-08-27 | `SENS` | 59 | $9.34 | $9.33 | +0.59 | — | +0.00 | +0.59 | +8.85 | — |
| 2026-08-27 | `KURA` | 41 | $13.06 | $12.98 | +3.28 | — | +0.00 | +3.28 | +26.65 | — |
| 2026-08-27 | `XHG` | 149 | $4.06 | $4.06 | +0.00 | — | +0.00 | +0.00 | -37.25 | — |
| 2026-08-27 | `BTG` | 98 | $5.74 | $5.73 | +0.98 | — | +0.00 | +0.98 | +1.96 | — |
| 2026-08-27 | `MRK` | 3 | $153.10 | $149.53 | +10.71 | — | +0.00 | +10.71 | +14.46 | — |
| 2026-08-27 | `B` | 11 | $47.00 | $47.07 | -0.77 | — | +0.00 | -0.77 | +12.21 | — |
| 2026-08-27 | `PEPG` | 166 | $3.22 | $3.22 | +0.00 | — | +0.00 | +0.00 | +31.54 | — |
| 2026-08-27 | `CRDL` | 280 | $2.14 | $2.09 | +14.00 | — | +0.00 | +14.00 | -16.80 | — |
| 2026-08-28 | `ANF` | 3 | — | $146.07 | +0.00 | $148.42 | -7.05 | -7.05 | -0.00 | -7.05 |
| 2026-08-28 | `BZ` | 31 | — | $18.15 | +0.00 | $17.80 | +10.85 | +10.85 | -0.00 | +10.85 |
| 2026-08-28 | `CRDL` | 276 | — | $2.06 | +0.00 | $1.94 | +33.12 | +33.12 | -0.00 | +33.12 |
| 2026-08-28 | `SBET` | 65 | — | $8.65 | +0.00 | $8.20 | +29.25 | +29.25 | -0.00 | +29.25 |
| 2026-08-28 | `EL` | 5 | — | $106.99 | +0.00 | $103.39 | +18.00 | +18.00 | -0.00 | +18.00 |
| 2026-08-28 | `CXM` | 72 | — | $7.88 | +0.00 | $8.16 | -20.16 | -20.16 | -0.00 | -20.16 |
| 2026-08-28 | `PATH` | 31 | — | $18.12 | +0.00 | $18.15 | -0.77 | -0.77 | -0.00 | -0.77 |
| 2026-08-28 | `FSM` | 44 | — | $12.84 | +0.00 | $12.26 | +25.52 | +25.52 | -0.00 | +25.52 |
| 2026-08-31 | `ANF` | 3 | $148.42 | $148.03 | +1.17 | — | +0.00 | +1.17 | -5.88 | — |
| 2026-08-31 | `BZ` | 31 | $17.80 | $17.70 | +3.10 | — | +0.00 | +3.10 | +13.95 | — |
| 2026-08-31 | `CRDL` | 276 | $1.94 | $1.92 | +5.52 | — | +0.00 | +5.52 | +38.64 | — |
| 2026-08-31 | `SBET` | 65 | $8.20 | $8.24 | -2.60 | — | +0.00 | -2.60 | +26.65 | — |
| 2026-08-31 | `EL` | 5 | $103.39 | $102.70 | +3.45 | — | +0.00 | +3.45 | +21.45 | — |
| 2026-08-31 | `CXM` | 72 | $8.16 | $8.17 | -0.72 | — | +0.00 | -0.72 | -20.88 | — |
| 2026-08-31 | `PATH` | 31 | $18.15 | $18.09 | +1.86 | — | +0.00 | +1.86 | +1.09 | — |
| 2026-08-31 | `FSM` | 44 | $12.26 | $12.26 | +0.00 | — | +0.00 | +0.00 | +25.52 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 10 | — | $52.88 | +0.00 | $52.46 | +4.20 | +4.20 | -0.00 | +4.20 |
| 2026-09-03 | `CRK` | 37 | — | $15.45 | +0.00 | $14.95 | +18.50 | +18.50 | -0.00 | +18.50 |
| 2026-09-03 | `ARCT` | 34 | — | $16.77 | +0.00 | $15.56 | +41.14 | +41.14 | -0.00 | +41.14 |
| 2026-09-03 | `CRDL` | 262 | — | $2.18 | +0.00 | $2.16 | +5.24 | +5.24 | -0.00 | +5.24 |
| 2026-09-03 | `GPRO` | 321 | — | $1.78 | +0.00 | $1.39 | +125.19 | +125.19 | -0.00 | +125.19 |
| 2026-09-03 | `MMED` | 23 | — | $23.88 | +0.00 | $23.84 | +0.92 | +0.92 | -0.00 | +0.92 |
| 2026-09-03 | `NVAX` | 54 | — | $10.42 | +0.00 | $10.34 | +4.32 | +4.32 | -0.00 | +4.32 |
| 2026-09-03 | `CNXC` | 17 | — | $32.88 | +0.00 | $32.85 | +0.51 | +0.51 | -0.00 | +0.51 |
| 2026-09-04 | `ATRC` | 10 | $52.46 | $52.03 | +4.30 | $51.52 | +5.10 | +9.40 | +8.50 | +13.60 |
| 2026-09-04 | `CRK` | 37 | $14.95 | $15.00 | -1.85 | — | +0.00 | -1.85 | +16.65 | — |
| 2026-09-04 | `ARCT` | 34 | $15.56 | $15.61 | -1.70 | — | +0.00 | -1.70 | +39.44 | — |
| 2026-09-04 | `CRDL` | 262 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | +5.24 | — |
| 2026-09-04 | `GPRO` | 321 | $1.39 | $1.48 | -28.89 | $1.70 | -70.62 | -99.51 | +96.30 | +25.68 |
| 2026-09-04 | `MMED` | 23 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | +0.92 | — |
| 2026-09-04 | `NVAX` | 54 | $10.34 | $10.50 | -8.64 | — | +0.00 | -8.64 | -4.32 | — |
| 2026-09-04 | `CNXC` | 17 | $32.85 | $32.48 | +6.29 | — | +0.00 | +6.29 | +6.80 | — |
| 2026-09-04 | `BMEA` | 407 | — | $1.90 | +0.00 | $2.03 | -52.91 | -52.91 | -0.00 | -52.91 |
| 2026-09-04 | `OABI` | 162 | — | $4.78 | +0.00 | $4.33 | +72.90 | +72.90 | -0.00 | +72.90 |
| 2026-09-04 | `OPK` | 487 | — | $1.59 | +0.00 | $1.64 | -24.35 | -24.35 | -0.00 | -24.35 |
| 2026-09-04 | `CRM` | 2 | — | $263.36 | +0.00 | $259.23 | +8.26 | +8.26 | -0.00 | +8.26 |
| 2026-09-04 | `HRMY` | 18 | — | $41.50 | +0.00 | $42.25 | -13.50 | -13.50 | -0.00 | -13.50 |
| 2026-09-04 | `FMC` | 59 | — | $12.95 | +0.00 | $12.97 | -1.18 | -1.18 | -0.00 | -1.18 |
| 2026-09-08 | `ATRC` | 10 | $51.52 | $54.31 | -27.90 | — | +0.00 | -27.90 | -14.30 | — |
| 2026-09-08 | `GPRO` | 321 | $1.70 | $1.56 | +43.34 | — | +0.00 | +43.34 | +69.02 | — |
| 2026-09-08 | `BMEA` | 407 | $2.03 | $2.00 | +12.21 | — | +0.00 | +12.21 | -40.70 | — |
| 2026-09-08 | `OABI` | 162 | $4.33 | $4.30 | +4.86 | — | +0.00 | +4.86 | +77.76 | — |
| 2026-09-08 | `OPK` | 487 | $1.64 | $1.63 | +4.87 | — | +0.00 | +4.87 | -19.48 | — |
| 2026-09-08 | `CRM` | 2 | $259.23 | $253.72 | +11.02 | — | +0.00 | +11.02 | +19.28 | — |
| 2026-09-08 | `HRMY` | 18 | $42.25 | $42.20 | +0.90 | — | +0.00 | +0.90 | -12.60 | — |
| 2026-09-08 | `FMC` | 59 | $12.97 | $13.11 | -8.26 | — | +0.00 | -8.26 | -9.44 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `INDP` | 213 | — | $2.70 | +0.00 | $2.77 | -14.91 | -14.91 | -0.00 | -14.91 |
| 2026-09-11 | `WLTH` | 52 | — | $10.95 | +0.00 | $10.38 | +29.64 | +29.64 | -0.00 | +29.64 |
| 2026-09-11 | `BNC` | 117 | — | $4.91 | +0.00 | $4.80 | +12.87 | +12.87 | -0.00 | +12.87 |
| 2026-09-11 | `SWKS` | 6 | — | $84.27 | +0.00 | $88.35 | -24.48 | -24.48 | -0.00 | -24.48 |
| 2026-09-11 | `ANGX` | 107 | — | $5.38 | +0.00 | $5.45 | -7.49 | -7.49 | -0.00 | -7.49 |
| 2026-09-11 | `QRVO` | 5 | — | $112.83 | +0.00 | $116.65 | -19.08 | -19.08 | -0.00 | -19.08 |
| 2026-09-11 | `ASO` | 10 | — | $54.91 | +0.00 | $55.36 | -4.50 | -4.50 | -0.00 | -4.50 |
| 2026-09-11 | `IRD` | 93 | — | $6.16 | +0.00 | $6.04 | +11.16 | +11.16 | -0.00 | +11.16 |
| 2026-09-14 | `INDP` | 213 | $2.77 | $2.80 | -6.39 | $3.14 | -72.42 | -78.81 | -21.30 | -93.72 |
| 2026-09-14 | `WLTH` | 52 | $10.38 | $10.29 | +4.68 | — | +0.00 | +4.68 | +34.32 | — |
| 2026-09-14 | `BNC` | 117 | $4.80 | $5.03 | -26.91 | — | +0.00 | -26.91 | -14.04 | — |
| 2026-09-14 | `SWKS` | 6 | $88.35 | $86.06 | +13.74 | — | +0.00 | +13.74 | -10.74 | — |
| 2026-09-14 | `ANGX` | 107 | $5.45 | $5.57 | -12.84 | — | +0.00 | -12.84 | -20.33 | — |
| 2026-09-14 | `QRVO` | 5 | $116.65 | $114.11 | +12.70 | $107.98 | +30.65 | +43.35 | -6.38 | +24.27 |
| 2026-09-14 | `ASO` | 10 | $55.36 | $54.75 | +6.10 | — | +0.00 | +6.10 | +1.60 | — |
| 2026-09-14 | `IRD` | 93 | $6.04 | $6.02 | +1.86 | — | +0.00 | +1.86 | +13.02 | — |
| 2026-09-15 | `INDP` | 213 | $3.14 | $3.40 | -55.38 | $3.64 | -51.12 | -106.50 | -149.10 | -200.22 |
| 2026-09-15 | `QRVO` | 5 | $107.98 | $108.40 | -2.10 | — | +0.00 | -2.10 | +22.17 | — |
| 2026-09-16 | `INDP` | 213 | $3.64 | $3.66 | -4.26 | — | +0.00 | -4.26 | -204.48 | — |
| 2026-09-16 | `AVAH` | 314 | — | $14.31 | +0.00 | $14.26 | +15.70 | +15.70 | -0.00 | +15.70 |
| 2026-09-17 | `AVAH` | 314 | $14.26 | $14.33 | -21.98 | — | +0.00 | -21.98 | -6.28 | — |
| 2026-09-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | -174.44 | TPG, TNDM | — | $14,972.19 | $9,820.90 | TPG×49, TNDM×107 |
| 2026-08-14 | +5.50 | $14,972.19 | TPG×49, TNDM×107 | $9,810.54 | -10.36 | +149.01 | ARX, OMER, AIRO, MXCT, TBBB, AMPY, MH, CRDL | TPG, TNDM | $14,640.28 | $9,931.60 | ARX×31, OMER×35, AIRO×55, MXCT×440, TBBB×12, AMPY×124, MH×45, CRDL×354 |
| 2026-08-17 | +2.25 | $14,640.28 | ARX×31, OMER×35, AIRO×55, MXCT×440, TBBB×12, AMPY×124, MH×45, CRDL×354 | $9,949.57 | +17.97 | +137.76 | HTFL, UMAC, NPWR, NMAX, CLYM, XHG, SGMT, U | ARX, OMER, AIRO, MXCT, TBBB, AMPY, MH, CRDL | $14,833.27 | $10,044.75 | HTFL×15, UMAC×19, NPWR×323, NMAX×56, CLYM×38, XHG×148, SGMT×59, U×13 |
| 2026-08-18 | -6.20 | $14,833.27 | HTFL×15, UMAC×19, NPWR×323, NMAX×56, CLYM×38, XHG×148, SGMT×59, U×13 | $10,110.09 | +65.34 | +0.00 | — | HTFL, UMAC, NPWR, NMAX, CLYM, XHG, SGMT, U | $10,090.95 | $10,090.95 | — |
| 2026-08-19 | -7.20 | $10,090.95 | — | $10,090.95 | -0.00 | +0.00 | — | — | $10,090.95 | $10,090.95 | — |
| 2026-08-20 | +1.12 | $10,090.95 | — | $10,090.95 | -0.00 | -59.32 | KGC, WPM, AEM, SCZM, MRNA, CYPH, BNTX, EL | — | $14,867.99 | $10,009.93 | KGC×21, WPM×4, AEM×3, SCZM×66, MRNA×4, CYPH×548, BNTX×5, EL×6 |
| 2026-08-21 | +3.25 | $14,867.99 | KGC×21, WPM×4, AEM×3, SCZM×66, MRNA×4, CYPH×548, BNTX×5, EL×6 | $9,856.66 | -153.27 | -221.93 | AU, ARCT, CRDL, GMAB, MRVI, DFDV | KGC, WPM, SCZM, MRNA, BNTX, EL | $15,989.45 | $9,605.46 | AEM×3, CYPH×548, AU×6, ARCT×73, CRDL×425, GMAB×24, MRVI×99, DFDV×203 |
| 2026-08-24 | -5.17 | $15,989.45 | AEM×3, CYPH×548, AU×6, ARCT×73, CRDL×425, GMAB×24, MRVI×99, DFDV×203 | $9,357.80 | -247.66 | -73.73 | — | AEM, CYPH, AU, CRDL, GMAB, MRVI, DFDV | $10,307.36 | $9,260.54 | ARCT×73 |
| 2026-08-25 | +1.80 | $10,307.36 | ARCT×73 | $9,276.60 | +16.06 | -196.40 | LIFE, BMEA, ALVO, CYPH, FWDI, ASST, JANX, BMNR | ARCT | $13,820.68 | $9,055.36 | LIFE×15, BMEA×355, ALVO×110, CYPH×371, FWDI×101, ASST×30, JANX×30, BMNR×24 |
| 2026-08-26 | +2.02 | $13,820.68 | LIFE×15, BMEA×355, ALVO×110, CYPH×371, FWDI×101, ASST×30, JANX×30, BMNR×24 | $9,118.62 | +63.26 | +12.83 | SENS, KURA, XHG, BTG, MRK, B, PEPG, CRDL | LIFE, BMEA, ALVO, CYPH, FWDI, ASST, JANX, BMNR | $13,453.74 | $9,089.74 | SENS×59, KURA×41, XHG×149, BTG×98, MRK×3, B×11, PEPG×166, CRDL×280 |
| 2026-08-27 | — | $13,453.74 | SENS×59, KURA×41, XHG×149, BTG×98, MRK×3, B×11, PEPG×166, CRDL×280 | $9,118.53 | +28.79 | +0.00 | — | SENS, KURA, XHG, BTG, MRK, B, PEPG, CRDL | $9,099.41 | $9,099.41 | — |
| 2026-08-28 | +0.75 | $9,099.41 | — | $9,099.41 | -0.00 | +88.76 | ANF, BZ, CRDL, SBET, EL, CXM, PATH, FSM | — | $13,441.66 | $9,169.60 | ANF×3, BZ×31, CRDL×276, SBET×65, EL×5, CXM×72, PATH×31, FSM×44 |
| 2026-08-31 | -5.85 | $13,441.66 | ANF×3, BZ×31, CRDL×276, SBET×65, EL×5, CXM×72, PATH×31, FSM×44 | $9,181.38 | +11.78 | +0.00 | — | ANF, BZ, CRDL, SBET, EL, CXM, PATH, FSM | $9,163.14 | $9,163.14 | — |
| 2026-09-01 | -6.30 | $9,163.14 | — | $9,163.14 | -0.00 | +0.00 | — | — | $9,163.14 | $9,163.14 | — |
| 2026-09-02 | -3.83 | $9,163.14 | — | $9,163.14 | -0.00 | +0.00 | — | — | $9,163.14 | $9,163.14 | — |
| 2026-09-03 | -0.90 | $9,163.14 | — | $9,163.14 | -0.00 | +200.02 | ATRC, CRK, ARCT, CRDL, GPRO, MMED, NVAX, CNXC | — | $13,626.84 | $9,342.81 | ATRC×10, CRK×37, ARCT×34, CRDL×262, GPRO×321, MMED×23, NVAX×54, CNXC×17 |
| 2026-09-04 | +2.25 | $13,626.84 | ATRC×10, CRK×37, ARCT×34, CRDL×262, GPRO×321, MMED×23, NVAX×54, CNXC×17 | $9,312.32 | -30.49 | -76.30 | BMEA, OABI, OPK, CRM, HRMY, FMC | CRK, ARCT, CRDL, MMED, NVAX, CNXC | $14,633.04 | $9,201.60 | ATRC×10, GPRO×321, BMEA×407, OABI×162, OPK×487, CRM×2, HRMY×18, FMC×59 |
| 2026-09-08 | -11.47 | $14,633.04 | ATRC×10, GPRO×321, BMEA×407, OABI×162, OPK×487, CRM×2, HRMY×18, FMC×59 | $9,242.63 | +41.03 | +0.00 | — | ATRC, GPRO, BMEA, OABI, OPK, CRM, HRMY, FMC | $9,216.26 | $9,216.26 | — |
| 2026-09-09 | -13.95 | $9,216.26 | — | $9,216.26 | -0.00 | +0.00 | — | — | $9,216.26 | $9,216.26 | — |
| 2026-09-10 | -13.28 | $9,216.26 | — | $9,216.26 | -0.00 | +0.00 | — | — | $9,216.26 | $9,216.26 | — |
| 2026-09-11 | +0.50 | $9,216.26 | — | $9,216.26 | -0.00 | -16.79 | INDP, WLTH, BNC, SWKS, ANGX, QRVO, ASO, IRD | — | $13,684.48 | $9,181.29 | INDP×213, WLTH×52, BNC×117, SWKS×6, ANGX×107, QRVO×5, ASO×10, IRD×93 |
| 2026-09-14 | -11.00 | $13,684.48 | INDP×213, WLTH×52, BNC×117, SWKS×6, ANGX×107, QRVO×5, ASO×10, IRD×93 | $9,174.23 | -7.06 | -41.77 | — | WLTH, BNC, SWKS, ANGX, ASO, IRD | $10,328.09 | $9,119.37 | INDP×213, QRVO×5 |
| 2026-09-15 | -3.84 | $10,328.09 | INDP×213, QRVO×5 | $9,061.89 | -57.48 | -51.12 | — | QRVO | $9,784.08 | $9,008.76 | INDP×213 |
| 2026-09-16 | +5.30 | $9,784.08 | INDP×213 | $9,004.50 | -4.26 | +15.70 | AVAH | INDP | $13,490.83 | $9,013.19 | AVAH×314 |
| 2026-09-17 | +7.38 | $13,490.83 | AVAH×314 | $8,991.21 | -21.98 | +0.00 | — | AVAH | $8,987.16 | $8,987.16 | — |
| 2026-09-18 | +4.86 | $8,987.16 | — | $8,987.16 | +0.00 | +0.00 | — | — | $8,987.16 | $8,987.16 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **SHORT** | `TPG` | 49 | $50.62 | $2.23 | — | $12,478.30 | — | RSI overbought; gate rsi_ob=True; list flatten; ret5=+6.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🔴 |
| 2026-08-13 09:30 ET | **SHORT** | `TNDM` | 107 | $23.33 | $2.42 | — | $14,972.19 | — | RSI overbought; gate rsi_ob=True; list flatten; ⚪; ret5=+19.7; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,972.19 | ▼ close $9,820.90 vs 09:30 $10,000.00 (session -174.44) | 16:00 close · cash $14,972.19 · equity $9,820.90 vs 09:30 $10,000.00 (-179.10; session marks -174.44) · 2 name(s) marked open→close (per-name table). TPG×49 09:30 $50.62 → close $54.62 -195.84; TNDM×107 09:30 $23.33 → close $23.13 +21.40 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,972.19 | ▼ 09:30 equity $9,810.54 vs yday $9,820.90 (-10.36) | 09:30 open · cash $14,972.19 (unchanged overnight, no fees) · equity $9,810.54 vs prior close $9,820.90 (-10.36) · 2 name(s) re-marked at the open (per-name table). TPG×49 yday $54.62 → 09:30 $55.29 -32.83; TNDM×107 yday $23.13 → 09:30 $22.92 +22.47 | — |
| 2026-08-14 09:30 ET | **COVER** | `TPG` | 49 | $55.29 | $2.14 | $-233.04 | $12,260.85 | ▼ -233.04 after sell → book $9,808.41; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **COVER** | `TNDM` | 107 | $22.92 | $2.31 | $+39.14 | $9,806.10 | ▲ +39.14 after sell → book $9,806.10; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SHORT** | `ARX` | 31 | $19.57 | $2.12 | — | $10,410.65 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+58.7; leftover $612.88 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OMER` | 35 | $17.35 | $2.13 | — | $11,015.77 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $612.88 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AIRO` | 55 | $11.12 | $2.19 | — | $11,625.17 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $612.88 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `MXCT` | 440 | $1.39 | $5.78 | — | $12,231.00 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $612.88 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `TBBB` | 12 | $48.82 | $2.06 | — | $12,814.78 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $612.88 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AMPY` | 124 | $4.94 | $2.41 | — | $13,424.93 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.4; leftover $612.88 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `MH` | 45 | $13.55 | $2.16 | — | $14,032.52 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $612.88 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `CRDL` | 354 | $1.73 | $4.65 | — | $14,640.28 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+25.7; leftover $612.88 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,640.28 | ▲ close $9,931.60 vs 09:30 $9,810.54 (session +149.01) | 16:00 close · cash $14,640.28 · equity $9,931.60 vs 09:30 $9,810.54 (+121.06; session marks +149.01) · 8 name(s) marked open→close (per-name table). ARX×31 09:30 $19.57 → close $19.58 -0.31; OMER×35 09:30 $17.35 → close $17.19 +5.60; AIRO×55 09:30 $11.12 → close $9.57 +85.25; MXCT×440 09:30 $1.39 → close $1.32 +30.80; TBBB×12 09:30 $48.82 → close $47.79 +12.36; AMPY×124 09:30 $4.94 → close $4.78 +19.84; MH×45 09:30 $13.55 → close $13.10 +20.25; CRDL×354 09:30 $1.73 → close $1.80 -24.78 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,640.28 | ▲ 09:30 equity $9,949.57 vs yday $9,931.60 (+17.97) | 09:30 open · cash $14,640.28 (unchanged overnight, no fees) · equity $9,949.57 vs prior close $9,931.60 (+17.97) · 8 name(s) re-marked at the open (per-name table). ARX×31 yday $19.58 → 09:30 $19.57 +0.31; OMER×35 yday $17.19 → 09:30 $17.17 +0.70; AIRO×55 yday $9.57 → 09:30 $9.57 -0.00; MXCT×440 yday $1.32 → 09:30 $1.32 -0.00; TBBB×12 yday $47.79 → 09:30 $47.39 +4.80; AMPY×124 yday $4.78 → 09:30 $4.86 -9.92; MH×45 yday $13.10 → 09:30 $13.16 -2.70; CRDL×354 yday $1.80 → 09:30 $1.73 +24.78 | — |
| 2026-08-17 09:30 ET | **COVER** | `ARX` | 31 | $19.57 | $2.08 | $-4.20 | $14,031.53 | ▼ -4.20 after sell → book $9,947.49; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `OMER` | 35 | $17.17 | $2.10 | $+2.07 | $13,428.48 | ▲ +2.07 after sell → book $9,945.39; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AIRO` | 55 | $9.57 | $2.15 | $+80.90 | $12,899.98 | ▲ +80.90 after sell → book $9,943.24; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `MXCT` | 440 | $1.32 | $5.68 | $+19.35 | $12,313.50 | ▲ +19.35 after sell → book $9,937.56; vs 09:30 mark -5.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `TBBB` | 12 | $47.39 | $2.03 | $+13.07 | $11,742.80 | ▲ +13.07 after sell → book $9,935.54; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AMPY` | 124 | $4.86 | $2.36 | $+5.15 | $11,137.80 | ▲ +5.15 after sell → book $9,933.18; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `MH` | 45 | $13.16 | $2.12 | $+13.26 | $10,543.47 | ▲ +13.26 after sell → book $9,931.05; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `CRDL` | 354 | $1.73 | $4.57 | $-9.22 | $9,926.48 | ▼ -9.22 after sell → book $9,926.48; vs 09:30 mark -4.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `HTFL` | 15 | $41.23 | $2.07 | — | $10,542.86 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+46.0; leftover $620.41 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `UMAC` | 19 | $32.55 | $2.08 | — | $11,159.23 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $620.41 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `NPWR` | 323 | $1.92 | $4.25 | — | $11,775.14 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $620.41 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `NMAX` | 56 | $10.97 | $2.19 | — | $12,387.27 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $620.41 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `CLYM` | 38 | $16.25 | $2.14 | — | $13,002.63 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot; ret5=+16.6; leftover $620.41 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `XHG` | 148 | $4.19 | $2.49 | — | $13,620.26 | — | RSI overbought; gate rsi_ob=True; list yday_mover; ret5=+291.8; leftover $620.41 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `SGMT` | 59 | $10.45 | $2.20 | — | $14,234.61 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+16.4; leftover $620.41 | join🟡 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `U` | 13 | $46.21 | $2.07 | — | $14,833.27 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ⚪; ret5=+7.6; leftover $620.41 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,833.27 | ▲ close $10,044.75 vs 09:30 $9,949.57 (session +137.76) | 16:00 close · cash $14,833.27 · equity $10,044.75 vs 09:30 $9,949.57 (+95.18; session marks +137.76) · 8 name(s) marked open→close (per-name table). HTFL×15 09:30 $41.23 → close $41.94 -10.65; UMAC×19 09:30 $32.55 → close $30.15 +45.60; NPWR×323 09:30 $1.92 → close $1.73 +61.37; NMAX×56 09:30 $10.97 → close $10.36 +34.16; CLYM×38 09:30 $16.25 → close $17.44 -45.22; XHG×148 09:30 $4.19 → close $3.91 +41.44; SGMT×59 09:30 $10.45 → close $10.43 +1.18; U×13 09:30 $46.21 → close $45.45 +9.88 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,833.27 | ▲ 09:30 equity $10,110.09 vs yday $10,044.75 (+65.34) | 09:30 open · cash $14,833.27 (unchanged overnight, no fees) · equity $10,110.09 vs prior close $10,044.75 (+65.34) · 8 name(s) re-marked at the open (per-name table). HTFL×15 yday $41.94 → 09:30 $41.50 +6.60; UMAC×19 yday $30.15 → 09:30 $28.59 +29.64; NPWR×323 yday $1.73 → 09:30 $1.70 +9.69; NMAX×56 yday $10.36 → 09:30 $10.31 +2.80; CLYM×38 yday $17.44 → 09:30 $16.90 +20.52; XHG×148 yday $3.91 → 09:30 $3.94 -4.44; SGMT×59 yday $10.43 → 09:30 $10.41 +1.18; U×13 yday $45.45 → 09:30 $45.50 -0.65 | — |
| 2026-08-18 09:30 ET | **COVER** | `HTFL` | 15 | $41.50 | $2.04 | $-8.16 | $14,208.74 | ▼ -8.16 after sell → book $10,108.06; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `UMAC` | 19 | $28.59 | $2.05 | $+71.11 | $13,663.48 | ▲ +71.11 after sell → book $10,106.01; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `NPWR` | 323 | $1.70 | $4.17 | $+62.65 | $13,110.21 | ▲ +62.65 after sell → book $10,101.84; vs 09:30 mark -4.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `NMAX` | 56 | $10.31 | $2.16 | $+32.61 | $12,530.69 | ▲ +32.61 after sell → book $10,099.68; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `CLYM` | 38 | $16.90 | $2.10 | $-28.94 | $11,886.39 | ▼ -28.94 after sell → book $10,097.58; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `XHG` | 148 | $3.94 | $2.43 | $+32.08 | $11,300.84 | ▲ +32.08 after sell → book $10,095.15; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `SGMT` | 59 | $10.41 | $2.17 | $-2.01 | $10,684.48 | ▼ -2.01 after sell → book $10,092.98; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `U` | 13 | $45.50 | $2.03 | $+5.14 | $10,090.95 | ▲ +5.14 after sell → book $10,090.95; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,090.95 | ▲ close $10,090.95 vs 09:30 $10,110.09 (session +0.00) | 16:00 close · cash $10,090.95 · no lots left · equity $10,090.95. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,090.95 | ▲ 09:30 equity $10,090.95 vs yday $10,090.95 (-0.00) | 09:30 open · cash $10,090.95 · no holdings · equity $10,090.95 vs prior close $10,090.95 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,090.95 | ▲ close $10,090.95 vs 09:30 $10,090.95 (session +0.00) | 16:00 close · cash $10,090.95 · no lots left · equity $10,090.95. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,090.95 | ▲ 09:30 equity $10,090.95 vs yday $10,090.95 (-0.00) | 09:30 open · cash $10,090.95 · no holdings · equity $10,090.95 vs prior close $10,090.95 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **SHORT** | `KGC` | 21 | $29.63 | $2.09 | — | $10,711.09 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $630.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WPM` | 4 | $144.54 | $2.04 | — | $11,287.21 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $630.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $11,898.53 | — | RSI overbought; gate rsi_ob=True; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $630.68 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `SCZM` | 66 | $9.46 | $2.23 | — | $12,520.66 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $630.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `MRNA` | 4 | $150.14 | $2.04 | — | $13,119.18 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $630.68 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `CYPH` | 548 | $1.15 | $7.19 | — | $13,742.19 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $630.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `BNTX` | 5 | $109.06 | $2.04 | — | $14,285.45 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $630.68 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `EL` | 6 | $97.43 | $2.04 | — | $14,867.99 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; leftover $630.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,867.99 | ▼ close $10,009.93 vs 09:30 $10,090.95 (session -59.32) | 16:00 close · cash $14,867.99 · equity $10,009.93 vs 09:30 $10,090.95 (-81.02; session marks -59.32) · 8 name(s) marked open→close (per-name table). KGC×21 09:30 $29.63 → close $31.43 -37.80; WPM×4 09:30 $144.54 → close $150.25 -22.84; AEM×3 09:30 $204.45 → close $212.04 -22.77; SCZM×66 09:30 $9.46 → close $9.76 -19.80; MRNA×4 09:30 $150.14 → close $133.32 +67.28; CYPH×548 09:30 $1.15 → close $1.19 -21.92; BNTX×5 09:30 $109.06 → close $110.89 -9.15; EL×6 09:30 $97.43 → close $96.15 +7.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,867.99 | ▼ 09:30 equity $9,856.66 vs yday $10,009.93 (-153.27) | 09:30 open · cash $14,867.99 (unchanged overnight, no fees) · equity $9,856.66 vs prior close $10,009.93 (-153.27) · 8 name(s) re-marked at the open (per-name table). KGC×21 yday $31.43 → 09:30 $32.17 -15.54; WPM×4 yday $150.25 → 09:30 $154.70 -17.80; AEM×3 yday $212.04 → 09:30 $216.30 -12.78; SCZM×66 yday $9.76 → 09:30 $10.26 -33.00; MRNA×4 yday $133.32 → 09:30 $133.11 +0.84; CYPH×548 yday $1.19 → 09:30 $1.32 -71.24; BNTX×5 yday $110.89 → 09:30 $110.92 -0.15; EL×6 yday $96.15 → 09:30 $96.75 -3.60 | — |
| 2026-08-21 09:30 ET | **COVER** | `KGC` | 21 | $32.17 | $2.05 | $-57.48 | $14,190.37 | ▼ -57.48 after sell → book $9,854.61; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `WPM` | 4 | $154.70 | $2.00 | $-44.68 | $13,569.57 | ▼ -44.68 after sell → book $9,852.61; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `SCZM` | 66 | $10.26 | $2.19 | $-57.21 | $12,890.22 | ▼ -57.21 after sell → book $9,850.42; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `MRNA` | 4 | $133.11 | $2.00 | $+64.08 | $12,355.78 | ▲ +64.08 after sell → book $9,848.42; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `BNTX` | 5 | $110.92 | $2.00 | $-13.34 | $11,799.17 | ▼ -13.34 after sell → book $9,846.41; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `EL` | 6 | $96.75 | $2.01 | $+0.03 | $11,216.66 | ▲ +0.03 after sell → book $9,844.40; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `AU` | 6 | $119.43 | $2.05 | — | $11,931.19 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $820.37 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARCT` | 73 | $11.13 | $2.25 | — | $12,741.43 | — | RSI overbought; gate rsi_ob=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $820.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CRDL` | 425 | $1.93 | $5.59 | — | $13,556.10 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $820.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `GMAB` | 24 | $33.36 | $2.10 | — | $14,354.63 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $820.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `MRVI` | 99 | $8.28 | $2.34 | — | $15,172.02 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $820.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `DFDV` | 203 | $4.04 | $2.68 | — | $15,989.45 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $820.37 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,989.45 | ▼ close $9,605.46 vs 09:30 $9,856.66 (session -221.93) | 16:00 close · cash $15,989.45 · equity $9,605.46 vs 09:30 $9,856.66 (-251.20; session marks -221.93) · 8 name(s) marked open→close (per-name table). AEM×3 09:30 $216.30 → close $216.06 +0.72; CYPH×548 09:30 $1.32 → close $1.42 -54.80; AU×6 09:30 $119.43 → close $121.22 -10.74; ARCT×73 09:30 $11.13 → close $13.45 -169.36; CRDL×425 09:30 $1.93 → close $1.86 +29.75; GMAB×24 09:30 $33.36 → close $33.45 -2.16; MRVI×99 09:30 $8.28 → close $8.64 -35.64; DFDV×203 09:30 $4.04 → close $3.94 +20.30 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,989.45 | ▼ 09:30 equity $9,357.80 vs yday $9,605.46 (-247.66) | 09:30 open · cash $15,989.45 (unchanged overnight, no fees) · equity $9,357.80 vs prior close $9,605.46 (-247.66) · 8 name(s) re-marked at the open (per-name table). AEM×3 yday $216.06 → 09:30 $217.03 -2.91; CYPH×548 yday $1.42 → 09:30 $1.83 -224.68; AU×6 yday $121.22 → 09:30 $120.51 +4.26; ARCT×73 yday $13.45 → 09:30 $13.33 +8.76; CRDL×425 yday $1.86 → 09:30 $1.88 -8.50; GMAB×24 yday $33.45 → 09:30 $32.82 +15.12; MRVI×99 yday $8.64 → 09:30 $8.59 +4.95; DFDV×203 yday $3.94 → 09:30 $4.16 -44.66 | — |
| 2026-08-24 09:30 ET | **COVER** | `AEM` | 3 | $217.03 | $2.00 | $-41.77 | $15,336.36 | ▼ -41.77 after sell → book $9,355.80; vs 09:30 mark -2.00 | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **COVER** | `CYPH` | 548 | $1.83 | $7.07 | $-386.90 | $14,326.45 | ▼ -386.90 after sell → book $9,348.73; vs 09:30 mark -7.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AU` | 6 | $120.51 | $2.01 | $-10.54 | $13,601.38 | ▼ -10.54 after sell → book $9,346.72; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `CRDL` | 425 | $1.88 | $5.48 | $+10.18 | $12,796.90 | ▲ +10.18 after sell → book $9,341.24; vs 09:30 mark -5.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `GMAB` | 24 | $32.82 | $2.06 | $+8.79 | $12,007.16 | ▲ +8.79 after sell → book $9,339.18; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `MRVI` | 99 | $8.59 | $2.29 | $-35.31 | $11,154.46 | ▼ -35.31 after sell → book $9,336.89; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `DFDV` | 203 | $4.16 | $2.62 | $-29.66 | $10,307.36 | ▼ -29.66 after sell → book $9,334.27; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,307.36 | ▼ close $9,260.54 vs 09:30 $9,357.80 (session -73.73) | 16:00 close · cash $10,307.36 · equity $9,260.54 vs 09:30 $9,357.80 (-97.26; session marks -73.73) · 1 name(s) marked open→close (per-name table). ARCT×73 09:30 $13.33 → close $14.34 -73.73 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,307.36 | ▲ 09:30 equity $9,276.60 vs yday $9,260.54 (+16.06) | 09:30 open · cash $10,307.36 (unchanged overnight, no fees) · equity $9,276.60 vs prior close $9,260.54 (+16.06) · 1 name(s) re-marked at the open (per-name table). ARCT×73 yday $14.34 → 09:30 $14.12 +16.06 | — |
| 2026-08-25 09:30 ET | **COVER** | `ARCT` | 73 | $14.12 | $2.21 | $-222.73 | $9,274.40 | ▼ -222.73 after sell → book $9,274.40; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **SHORT** | `LIFE` | 15 | $36.96 | $2.07 | — | $9,826.73 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $579.65 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `BMEA` | 355 | $1.63 | $4.66 | — | $10,400.71 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $579.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `ALVO` | 110 | $5.24 | $2.36 | — | $10,974.75 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $579.65 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `CYPH` | 371 | $1.56 | $4.87 | — | $11,548.63 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $579.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `FWDI` | 101 | $5.71 | $2.34 | — | $12,123.01 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $579.65 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `ASST` | 30 | $19.04 | $2.12 | — | $12,692.09 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+49.5; leftover $579.65 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `JANX` | 30 | $18.72 | $2.12 | — | $13,251.58 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot; 🔵; ret5=+14.4; leftover $579.65 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `BMNR` | 24 | $23.80 | $2.10 | — | $13,820.68 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; ret5=+28.9; leftover $579.65 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,820.68 | ▼ close $9,055.36 vs 09:30 $9,276.60 (session -196.40) | 16:00 close · cash $13,820.68 · equity $9,055.36 vs 09:30 $9,276.60 (-221.24; session marks -196.40) · 8 name(s) marked open→close (per-name table). LIFE×15 09:30 $36.96 → close $38.56 -24.00; BMEA×355 09:30 $1.63 → close $1.73 -35.50; ALVO×110 09:30 $5.24 → close $5.05 +20.90; CYPH×371 09:30 $1.56 → close $1.64 -29.68; FWDI×101 09:30 $5.71 → close $6.05 -34.34; ASST×30 09:30 $19.04 → close $21.39 -70.50; JANX×30 09:30 $18.72 → close $18.68 +1.20; BMNR×24 09:30 $23.80 → close $24.82 -24.48 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,820.68 | ▲ 09:30 equity $9,118.62 vs yday $9,055.36 (+63.26) | 09:30 open · cash $13,820.68 (unchanged overnight, no fees) · equity $9,118.62 vs prior close $9,055.36 (+63.26) · 8 name(s) re-marked at the open (per-name table). LIFE×15 yday $38.56 → 09:30 $38.24 +4.80; BMEA×355 yday $1.73 → 09:30 $1.75 -8.87; ALVO×110 yday $5.05 → 09:30 $4.98 +7.70; CYPH×371 yday $1.64 → 09:30 $1.60 +14.84; FWDI×101 yday $6.05 → 09:30 $5.97 +8.08; ASST×30 yday $21.39 → 09:30 $20.72 +20.10; JANX×30 yday $18.68 → 09:30 $18.59 +2.70; BMNR×24 yday $24.82 → 09:30 $24.24 +13.92 | — |
| 2026-08-26 09:30 ET | **COVER** | `LIFE` | 15 | $38.24 | $2.04 | $-23.31 | $13,245.04 | ▼ -23.31 after sell → book $9,116.59; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `BMEA` | 355 | $1.75 | $4.58 | $-53.62 | $12,617.44 | ▼ -53.62 after sell → book $9,112.01; vs 09:30 mark -4.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `ALVO` | 110 | $4.98 | $2.32 | $+23.92 | $12,067.32 | ▲ +23.92 after sell → book $9,109.69; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `CYPH` | 371 | $1.60 | $4.79 | $-24.50 | $11,468.93 | ▼ -24.50 after sell → book $9,104.90; vs 09:30 mark -4.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `FWDI` | 101 | $5.97 | $2.29 | $-30.89 | $10,863.67 | ▼ -30.89 after sell → book $9,102.61; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `ASST` | 30 | $20.72 | $2.08 | $-54.60 | $10,239.99 | ▼ -54.60 after sell → book $9,100.53; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `JANX` | 30 | $18.59 | $2.08 | $-0.30 | $9,680.21 | ▼ -0.30 after sell → book $9,098.45; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `BMNR` | 24 | $24.24 | $2.06 | $-14.72 | $9,096.39 | ▼ -14.72 after sell → book $9,096.39; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SHORT** | `SENS` | 59 | $9.48 | $2.20 | — | $9,653.51 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $568.52 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `KURA` | 41 | $13.63 | $2.15 | — | $10,210.19 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+10.2; leftover $568.52 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `XHG` | 149 | $3.81 | $2.49 | — | $10,775.39 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=-6.1; leftover $568.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `BTG` | 98 | $5.75 | $2.33 | — | $11,336.57 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=+17.9; leftover $568.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `MRK` | 3 | $154.35 | $2.03 | — | $11,797.58 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+15.7; leftover $568.52 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `B` | 11 | $48.18 | $2.06 | — | $12,325.51 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=+16.1; leftover $568.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `PEPG` | 166 | $3.41 | $2.54 | — | $12,889.02 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=+17.7; leftover $568.52 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `CRDL` | 280 | $2.03 | $3.68 | — | $13,453.74 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+5.5; leftover $568.52 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟡 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,453.74 | ▲ close $9,089.74 vs 09:30 $9,118.62 (session +12.83) | 16:00 close · cash $13,453.74 · equity $9,089.74 vs 09:30 $9,118.62 (-28.88; session marks +12.83) · 8 name(s) marked open→close (per-name table). SENS×59 09:30 $9.48 → close $9.34 +8.26; KURA×41 09:30 $13.63 → close $13.06 +23.37; XHG×149 09:30 $3.81 → close $4.06 -37.25; BTG×98 09:30 $5.75 → close $5.74 +0.98; MRK×3 09:30 $154.35 → close $153.10 +3.75; B×11 09:30 $48.18 → close $47.00 +12.98; PEPG×166 09:30 $3.41 → close $3.22 +31.54; CRDL×280 09:30 $2.03 → close $2.14 -30.80 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,453.74 | ▲ 09:30 equity $9,118.53 vs yday $9,089.74 (+28.79) | 09:30 open · cash $13,453.74 (unchanged overnight, no fees) · equity $9,118.53 vs prior close $9,089.74 (+28.79) · 8 name(s) re-marked at the open (per-name table). SENS×59 yday $9.34 → 09:30 $9.33 +0.59; KURA×41 yday $13.06 → 09:30 $12.98 +3.28; XHG×149 yday $4.06 → 09:30 $4.06 -0.00; BTG×98 yday $5.74 → 09:30 $5.73 +0.98; MRK×3 yday $153.10 → 09:30 $149.53 +10.71; B×11 yday $47.00 → 09:30 $47.07 -0.77; PEPG×166 yday $3.22 → 09:30 $3.22 -0.00; CRDL×280 yday $2.14 → 09:30 $2.09 +14.00 | — |
| 2026-08-27 09:30 ET | **COVER** | `SENS` | 59 | $9.33 | $2.17 | $+4.48 | $12,901.10 | ▲ +4.48 after sell → book $9,116.36; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `KURA` | 41 | $12.98 | $2.11 | $+22.39 | $12,366.81 | ▲ +22.39 after sell → book $9,114.25; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `XHG` | 149 | $4.06 | $2.44 | $-42.17 | $11,759.43 | ▼ -42.17 after sell → book $9,111.81; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `BTG` | 98 | $5.73 | $2.28 | $-2.65 | $11,195.61 | ▼ -2.65 after sell → book $9,109.53; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `MRK` | 3 | $149.53 | $2.00 | $+10.43 | $10,745.02 | ▲ +10.43 after sell → book $9,107.53; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `B` | 11 | $47.07 | $2.02 | $+8.13 | $10,225.23 | ▲ +8.13 after sell → book $9,105.51; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `PEPG` | 166 | $3.22 | $2.49 | $+26.51 | $9,688.22 | ▲ +26.51 after sell → book $9,103.02; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `CRDL` | 280 | $2.09 | $3.61 | $-24.10 | $9,099.41 | ▼ -24.10 after sell → book $9,099.41; vs 09:30 mark -3.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,099.41 | ▲ close $9,099.41 vs 09:30 $9,118.53 (session +0.00) | 16:00 close · cash $9,099.41 · no lots left · equity $9,099.41. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,099.41 | ▲ 09:30 equity $9,099.41 vs yday $9,099.41 (-0.00) | 09:30 open · cash $9,099.41 · no holdings · equity $9,099.41 vs prior close $9,099.41 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **SHORT** | `ANF` | 3 | $146.07 | $2.03 | — | $9,535.59 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $568.71 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `BZ` | 31 | $18.15 | $2.12 | — | $10,096.12 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+14.1; leftover $568.71 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **SHORT** | `CRDL` | 276 | $2.06 | $3.63 | — | $10,661.05 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; ret5=+9.3; leftover $568.71 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SBET` | 65 | $8.65 | $2.22 | — | $11,221.08 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+17.0; leftover $568.71 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `EL` | 5 | $106.99 | $2.04 | — | $11,753.99 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+10.5; leftover $568.71 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `CXM` | 72 | $7.88 | $2.24 | — | $12,319.10 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=+10.3; leftover $568.71 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `PATH` | 31 | $18.12 | $2.12 | — | $12,878.86 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=+15.1; leftover $568.71 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `FSM` | 44 | $12.84 | $2.16 | — | $13,441.66 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+7.6; leftover $568.71 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,441.66 | ▲ close $9,169.60 vs 09:30 $9,099.41 (session +88.76) | 16:00 close · cash $13,441.66 · equity $9,169.60 vs 09:30 $9,099.41 (+70.19; session marks +88.76) · 8 name(s) marked open→close (per-name table). ANF×3 09:30 $146.07 → close $148.42 -7.05; BZ×31 09:30 $18.15 → close $17.80 +10.85; CRDL×276 09:30 $2.06 → close $1.94 +33.12; SBET×65 09:30 $8.65 → close $8.20 +29.25; EL×5 09:30 $106.99 → close $103.39 +18.00; CXM×72 09:30 $7.88 → close $8.16 -20.16; PATH×31 09:30 $18.12 → close $18.15 -0.77; FSM×44 09:30 $12.84 → close $12.26 +25.52 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,441.66 | ▲ 09:30 equity $9,181.38 vs yday $9,169.60 (+11.78) | 09:30 open · cash $13,441.66 (unchanged overnight, no fees) · equity $9,181.38 vs prior close $9,169.60 (+11.78) · 8 name(s) re-marked at the open (per-name table). ANF×3 yday $148.42 → 09:30 $148.03 +1.17; BZ×31 yday $17.80 → 09:30 $17.70 +3.10; CRDL×276 yday $1.94 → 09:30 $1.92 +5.52; SBET×65 yday $8.20 → 09:30 $8.24 -2.60; EL×5 yday $103.39 → 09:30 $102.70 +3.45; CXM×72 yday $8.16 → 09:30 $8.17 -0.72; PATH×31 yday $18.15 → 09:30 $18.09 +1.86; FSM×44 yday $12.26 → 09:30 $12.26 -0.00 | — |
| 2026-08-31 09:30 ET | **COVER** | `ANF` | 3 | $148.03 | $2.00 | $-9.91 | $12,995.57 | ▼ -9.91 after sell → book $9,179.38; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `BZ` | 31 | $17.70 | $2.08 | $+9.75 | $12,444.79 | ▲ +9.75 after sell → book $9,177.30; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRDL` | 276 | $1.92 | $3.56 | $+31.45 | $11,911.31 | ▲ +31.45 after sell → book $9,173.74; vs 09:30 mark -3.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SBET` | 65 | $8.24 | $2.19 | $+22.24 | $11,373.52 | ▲ +22.24 after sell → book $9,171.55; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `EL` | 5 | $102.70 | $2.00 | $+17.41 | $10,858.02 | ▲ +17.41 after sell → book $9,169.55; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `CXM` | 72 | $8.17 | $2.21 | $-25.33 | $10,267.57 | ▼ -25.33 after sell → book $9,167.34; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **COVER** | `PATH` | 31 | $18.09 | $2.08 | $-3.12 | $9,704.70 | ▼ -3.12 after sell → book $9,165.26; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `FSM` | 44 | $12.26 | $2.12 | $+21.24 | $9,163.14 | ▲ +21.24 after sell → book $9,163.14; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,163.14 | ▲ close $9,163.14 vs 09:30 $9,181.38 (session +0.00) | 16:00 close · cash $9,163.14 · no lots left · equity $9,163.14. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,163.14 | ▲ 09:30 equity $9,163.14 vs yday $9,163.14 (-0.00) | 09:30 open · cash $9,163.14 · no holdings · equity $9,163.14 vs prior close $9,163.14 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,163.14 | ▲ close $9,163.14 vs 09:30 $9,163.14 (session +0.00) | 16:00 close · cash $9,163.14 · no lots left · equity $9,163.14. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,163.14 | ▲ 09:30 equity $9,163.14 vs yday $9,163.14 (-0.00) | 09:30 open · cash $9,163.14 · no holdings · equity $9,163.14 vs prior close $9,163.14 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,163.14 | ▲ close $9,163.14 vs 09:30 $9,163.14 (session +0.00) | 16:00 close · cash $9,163.14 · no lots left · equity $9,163.14. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,163.14 | ▲ 09:30 equity $9,163.14 vs yday $9,163.14 (-0.00) | 09:30 open · cash $9,163.14 · no holdings · equity $9,163.14 vs prior close $9,163.14 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **SHORT** | `ATRC` | 10 | $52.88 | $2.05 | — | $9,689.88 | — | RSI overbought; gate rsi_ob=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $572.70 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `CRK` | 37 | $15.45 | $2.14 | — | $10,259.40 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $572.70 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **SHORT** | `ARCT` | 34 | $16.77 | $2.13 | — | $10,827.45 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $572.70 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `CRDL` | 262 | $2.18 | $3.45 | — | $11,395.16 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $572.70 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `GPRO` | 321 | $1.78 | $4.22 | — | $11,962.32 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+183.1; leftover $572.70 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `MMED` | 23 | $23.88 | $2.09 | — | $12,509.47 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $572.70 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `NVAX` | 54 | $10.42 | $2.19 | — | $13,069.96 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $572.70 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `CNXC` | 17 | $32.88 | $2.08 | — | $13,626.84 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+16.2; leftover $572.70 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,626.84 | ▲ close $9,342.81 vs 09:30 $9,163.14 (session +200.02) | 16:00 close · cash $13,626.84 · equity $9,342.81 vs 09:30 $9,163.14 (+179.67; session marks +200.02) · 8 name(s) marked open→close (per-name table). ATRC×10 09:30 $52.88 → close $52.46 +4.20; CRK×37 09:30 $15.45 → close $14.95 +18.50; ARCT×34 09:30 $16.77 → close $15.56 +41.14; CRDL×262 09:30 $2.18 → close $2.16 +5.24; GPRO×321 09:30 $1.78 → close $1.39 +125.19; MMED×23 09:30 $23.88 → close $23.84 +0.92; NVAX×54 09:30 $10.42 → close $10.34 +4.32; CNXC×17 09:30 $32.88 → close $32.85 +0.51 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,626.84 | ▼ 09:30 equity $9,312.32 vs yday $9,342.81 (-30.49) | 09:30 open · cash $13,626.84 (unchanged overnight, no fees) · equity $9,312.32 vs prior close $9,342.81 (-30.49) · 8 name(s) re-marked at the open (per-name table). ATRC×10 yday $52.46 → 09:30 $52.03 +4.30; CRK×37 yday $14.95 → 09:30 $15.00 -1.85; ARCT×34 yday $15.56 → 09:30 $15.61 -1.70; CRDL×262 yday $2.16 → 09:30 $2.16 -0.00; GPRO×321 yday $1.39 → 09:30 $1.48 -28.89; MMED×23 yday $23.84 → 09:30 $23.84 -0.00; NVAX×54 yday $10.34 → 09:30 $10.50 -8.64; CNXC×17 yday $32.85 → 09:30 $32.48 +6.29 | — |
| 2026-09-04 09:30 ET | **COVER** | `CRK` | 37 | $15.00 | $2.10 | $+12.41 | $13,069.74 | ▲ +12.41 after sell → book $9,310.22; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `ARCT` | 34 | $15.61 | $2.09 | $+35.22 | $12,536.91 | ▲ +35.22 after sell → book $9,308.13; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `CRDL` | 262 | $2.16 | $3.38 | $-1.59 | $11,967.61 | ▼ -1.59 after sell → book $9,304.75; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `MMED` | 23 | $23.84 | $2.06 | $-3.23 | $11,417.23 | ▼ -3.23 after sell → book $9,302.69; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `NVAX` | 54 | $10.50 | $2.15 | $-8.66 | $10,848.08 | ▼ -8.66 after sell → book $9,300.54; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `CNXC` | 17 | $32.48 | $2.04 | $+2.68 | $10,293.88 | ▲ +2.68 after sell → book $9,298.50; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `BMEA` | 407 | $1.90 | $5.35 | — | $11,061.83 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $774.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `OABI` | 162 | $4.78 | $2.53 | — | $11,833.65 | — | RSI overbought; gate rsi_ob=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $774.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `OPK` | 487 | $1.59 | $6.39 | — | $12,601.59 | — | RSI overbought; gate rsi_ob=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $774.87 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `CRM` | 2 | $263.36 | $2.03 | — | $13,126.28 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $774.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SHORT** | `HRMY` | 18 | $41.50 | $2.08 | — | $13,871.20 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $774.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SHORT** | `FMC` | 59 | $12.95 | $2.21 | — | $14,633.04 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+21.8; leftover $774.87 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,633.04 | ▼ close $9,201.60 vs 09:30 $9,312.32 (session -76.30) | 16:00 close · cash $14,633.04 · equity $9,201.60 vs 09:30 $9,312.32 (-110.72; session marks -76.30) · 8 name(s) marked open→close (per-name table). ATRC×10 09:30 $52.03 → close $51.52 +5.10; GPRO×321 09:30 $1.48 → close $1.70 -70.62; BMEA×407 09:30 $1.90 → close $2.03 -52.91; OABI×162 09:30 $4.78 → close $4.33 +72.90; OPK×487 09:30 $1.59 → close $1.64 -24.35; CRM×2 09:30 $263.36 → close $259.23 +8.26; HRMY×18 09:30 $41.50 → close $42.25 -13.50; FMC×59 09:30 $12.95 → close $12.97 -1.18 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,633.04 | ▲ 09:30 equity $9,242.63 vs yday $9,201.60 (+41.03) | 09:30 open · cash $14,633.04 (unchanged overnight, no fees) · equity $9,242.63 vs prior close $9,201.60 (+41.03) · 8 name(s) re-marked at the open (per-name table). ATRC×10 yday $51.52 → 09:30 $54.31 -27.90; GPRO×321 yday $1.70 → 09:30 $1.56 +43.34; BMEA×407 yday $2.03 → 09:30 $2.00 +12.21; OABI×162 yday $4.33 → 09:30 $4.30 +4.86; OPK×487 yday $1.64 → 09:30 $1.63 +4.87; CRM×2 yday $259.23 → 09:30 $253.72 +11.02; HRMY×18 yday $42.25 → 09:30 $42.20 +0.90; FMC×59 yday $12.97 → 09:30 $13.11 -8.26 | — |
| 2026-09-08 09:30 ET | **COVER** | `ATRC` | 10 | $54.31 | $2.02 | $-18.37 | $14,087.92 | ▼ -18.37 after sell → book $9,240.61; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `GPRO` | 321 | $1.56 | $4.14 | $+60.65 | $13,581.41 | ▲ +60.65 after sell → book $9,236.47; vs 09:30 mark -4.14 | dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **COVER** | `BMEA` | 407 | $2.00 | $5.25 | $-51.30 | $12,762.16 | ▼ -51.30 after sell → book $9,231.22; vs 09:30 mark -5.25 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **COVER** | `OABI` | 162 | $4.30 | $2.48 | $+72.75 | $12,063.09 | ▲ +72.75 after sell → book $9,228.75; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `OPK` | 487 | $1.63 | $6.28 | $-32.16 | $11,262.99 | ▼ -32.16 after sell → book $9,222.46; vs 09:30 mark -6.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `CRM` | 2 | $253.72 | $2.00 | $+15.25 | $10,753.56 | ▲ +15.25 after sell → book $9,220.47; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `HRMY` | 18 | $42.20 | $2.04 | $-16.73 | $9,991.91 | ▼ -16.73 after sell → book $9,218.42; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `FMC` | 59 | $13.11 | $2.17 | $-13.81 | $9,216.26 | ▼ -13.81 after sell → book $9,216.26; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,216.26 | ▲ close $9,216.26 vs 09:30 $9,242.63 (session +0.00) | 16:00 close · cash $9,216.26 · no lots left · equity $9,216.26. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,216.26 | ▲ 09:30 equity $9,216.26 vs yday $9,216.26 (-0.00) | 09:30 open · cash $9,216.26 · no holdings · equity $9,216.26 vs prior close $9,216.26 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,216.26 | ▲ close $9,216.26 vs 09:30 $9,216.26 (session +0.00) | 16:00 close · cash $9,216.26 · no lots left · equity $9,216.26. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,216.26 | ▲ 09:30 equity $9,216.26 vs yday $9,216.26 (-0.00) | 09:30 open · cash $9,216.26 · no holdings · equity $9,216.26 vs prior close $9,216.26 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,216.26 | ▲ close $9,216.26 vs 09:30 $9,216.26 (session +0.00) | 16:00 close · cash $9,216.26 · no lots left · equity $9,216.26. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,216.26 | ▲ 09:30 equity $9,216.26 vs yday $9,216.26 (-0.00) | 09:30 open · cash $9,216.26 · no holdings · equity $9,216.26 vs prior close $9,216.26 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **SHORT** | `INDP` | 213 | $2.70 | $2.81 | — | $9,788.55 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $576.02 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `WLTH` | 52 | $10.95 | $2.18 | — | $10,355.77 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $576.02 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `BNC` | 117 | $4.91 | $2.39 | — | $10,927.85 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $576.02 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `SWKS` | 6 | $84.27 | $2.04 | — | $11,431.43 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $576.02 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `ANGX` | 107 | $5.38 | $2.35 | — | $12,004.73 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+19.8; leftover $576.02 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 5 | $112.83 | $2.04 | — | $12,566.87 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $576.02 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `ASO` | 10 | $54.91 | $2.06 | — | $13,113.91 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+24.3; leftover $576.02 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `IRD` | 93 | $6.16 | $2.31 | — | $13,684.48 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+36.4; leftover $576.02 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,684.48 | ▼ close $9,181.29 vs 09:30 $9,216.26 (session -16.79) | 16:00 close · cash $13,684.48 · equity $9,181.29 vs 09:30 $9,216.26 (-34.97; session marks -16.79) · 8 name(s) marked open→close (per-name table). INDP×213 09:30 $2.70 → close $2.77 -14.91; WLTH×52 09:30 $10.95 → close $10.38 +29.64; BNC×117 09:30 $4.91 → close $4.80 +12.87; SWKS×6 09:30 $84.27 → close $88.35 -24.48; ANGX×107 09:30 $5.38 → close $5.45 -7.49; QRVO×5 09:30 $112.83 → close $116.65 -19.08; ASO×10 09:30 $54.91 → close $55.36 -4.50; IRD×93 09:30 $6.16 → close $6.04 +11.16 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,684.48 | ▼ 09:30 equity $9,174.23 vs yday $9,181.29 (-7.06) | 09:30 open · cash $13,684.48 (unchanged overnight, no fees) · equity $9,174.23 vs prior close $9,181.29 (-7.06) · 8 name(s) re-marked at the open (per-name table). INDP×213 yday $2.77 → 09:30 $2.80 -6.39; WLTH×52 yday $10.38 → 09:30 $10.29 +4.68; BNC×117 yday $4.80 → 09:30 $5.03 -26.91; SWKS×6 yday $88.35 → 09:30 $86.06 +13.74; ANGX×107 yday $5.45 → 09:30 $5.57 -12.84; QRVO×5 yday $116.65 → 09:30 $114.11 +12.70; ASO×10 yday $55.36 → 09:30 $54.75 +6.10; IRD×93 yday $6.04 → 09:30 $6.02 +1.86 | — |
| 2026-09-14 09:30 ET | **COVER** | `WLTH` | 52 | $10.29 | $2.15 | $+29.99 | $13,147.26 | ▲ +29.99 after sell → book $9,172.09; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `BNC` | 117 | $5.03 | $2.34 | $-18.77 | $12,556.41 | ▼ -18.77 after sell → book $9,169.75; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **COVER** | `SWKS` | 6 | $86.06 | $2.01 | $-14.79 | $12,038.04 | ▼ -14.79 after sell → book $9,167.74; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `ANGX` | 107 | $5.57 | $2.31 | $-25.00 | $11,439.74 | ▼ -25.00 after sell → book $9,165.43; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `ASO` | 10 | $54.75 | $2.02 | $-2.48 | $10,890.22 | ▼ -2.48 after sell → book $9,163.41; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `IRD` | 93 | $6.02 | $2.27 | $+8.44 | $10,328.09 | ▲ +8.44 after sell → book $9,161.14; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,328.09 | ▼ close $9,119.37 vs 09:30 $9,174.23 (session -41.77) | 16:00 close · cash $10,328.09 · equity $9,119.37 vs 09:30 $9,174.23 (-54.86; session marks -41.77) · 2 name(s) marked open→close (per-name table). INDP×213 09:30 $2.80 → close $3.14 -72.42; QRVO×5 09:30 $114.11 → close $107.98 +30.65 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,328.09 | ▼ 09:30 equity $9,061.89 vs yday $9,119.37 (-57.48) | 09:30 open · cash $10,328.09 (unchanged overnight, no fees) · equity $9,061.89 vs prior close $9,119.37 (-57.48) · 2 name(s) re-marked at the open (per-name table). INDP×213 yday $3.14 → 09:30 $3.40 -55.38; QRVO×5 yday $107.98 → 09:30 $108.40 -2.10 | — |
| 2026-09-15 09:30 ET | **COVER** | `QRVO` | 5 | $108.40 | $2.00 | $+18.13 | $9,784.08 | ▲ +18.13 after sell → book $9,059.88; vs 09:30 mark -2.01 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,784.08 | ▼ close $9,008.76 vs 09:30 $9,061.89 (session -51.12) | 16:00 close · cash $9,784.08 · equity $9,008.76 vs 09:30 $9,061.89 (-53.13; session marks -51.12) · 1 name(s) marked open→close (per-name table). INDP×213 09:30 $3.40 → close $3.64 -51.12 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,784.08 | ▼ 09:30 equity $9,004.50 vs yday $9,008.76 (-4.26) | 09:30 open · cash $9,784.08 (unchanged overnight, no fees) · equity $9,004.50 vs prior close $9,008.76 (-4.26) · 1 name(s) re-marked at the open (per-name table). INDP×213 yday $3.64 → 09:30 $3.66 -4.26 | — |
| 2026-09-16 09:30 ET | **COVER** | `INDP` | 213 | $3.66 | $2.75 | $-210.04 | $9,001.75 | ▼ -210.04 after sell → book $9,001.75; vs 09:30 mark -2.75 | dropped from list after 3 sess (min 1) | — |
| 2026-09-16 09:30 ET | **SHORT** | `AVAH` | 314 | $14.31 | $4.26 | — | $13,490.83 | — | RSI overbought; gate rsi_ob=True; list flatten; ⚪; ret5=+3.2; leftover $4500.88 | join🟢 sector🟢 gen🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,490.83 | ▲ close $9,013.19 vs 09:30 $9,004.50 (session +15.70) | 16:00 close · cash $13,490.83 · equity $9,013.19 vs 09:30 $9,004.50 (+8.69; session marks +15.70) · 1 name(s) marked open→close (per-name table). AVAH×314 09:30 $14.31 → close $14.26 +15.70 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,490.83 | ▼ 09:30 equity $8,991.21 vs yday $9,013.19 (-21.98) | 09:30 open · cash $13,490.83 (unchanged overnight, no fees) · equity $8,991.21 vs prior close $9,013.19 (-21.98) · 1 name(s) re-marked at the open (per-name table). AVAH×314 yday $14.26 → 09:30 $14.33 -21.98 | — |
| 2026-09-17 09:30 ET | **COVER** | `AVAH` | 314 | $14.33 | $4.05 | $-14.59 | $8,987.16 | ▼ -14.59 after sell → book $8,987.16; vs 09:30 mark -4.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,987.16 | ▲ close $8,987.16 vs 09:30 $8,991.21 (session +0.00) | 16:00 close · cash $8,987.16 · no lots left · equity $8,987.16. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,987.16 | ▲ 09:30 equity $8,987.16 vs yday $8,987.16 (+0.00) | 09:30 open · cash $8,987.16 · no holdings · equity $8,987.16 vs prior close $8,987.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,987.16 | ▲ close $8,987.16 vs 09:30 $8,987.16 (session +0.00) | 16:00 close · cash $8,987.16 · no lots left · equity $8,987.16. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `WFF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DSX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `REAX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `USDE` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CAN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ASST` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `XHG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NIQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `STT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ARCT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PATH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `LAND` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DINO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `DFDV` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BRR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `DPRO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `PURR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CABA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LIFE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ATRC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XHLD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SID` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CVI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `XHLD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CMRC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TJGC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBLX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GME` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `FRO` | hard_red | hard-red S=-3.84 sit; no new buys |
