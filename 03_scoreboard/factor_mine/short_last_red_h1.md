# Factor mine action — `short_last_red_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · last bar red

Cash book **-8.82%** ($9,118) · signal-only (no cash/fees) was -8.01%. Starts YES **1/18**. Fills 150 · skips 59 · realized $-794.91.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the last finished bar was red (closed down).

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
- **Gate** `last_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $14,152.44.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TGTX` | 25 | — | $49.70 | +0.00 | $47.94 | +44.00 | +44.00 | -0.00 | +44.00 |
| 2026-08-13 | `SLS` | 106 | — | $11.70 | +0.00 | $12.36 | -69.96 | -69.96 | -0.00 | -69.96 |
| 2026-08-13 | `HIMS` | 42 | — | $29.74 | +0.00 | $28.77 | +40.74 | +40.74 | -0.00 | +40.74 |
| 2026-08-13 | `VOR` | 56 | — | $22.01 | +0.00 | $23.29 | -71.68 | -71.68 | -0.00 | -71.68 |
| 2026-08-14 | `TGTX` | 25 | $47.94 | $47.27 | +16.75 | — | +0.00 | +16.75 | +60.75 | — |
| 2026-08-14 | `SLS` | 106 | $12.36 | $12.40 | -4.24 | — | +0.00 | -4.24 | -74.20 | — |
| 2026-08-14 | `HIMS` | 42 | $28.77 | $29.15 | -15.96 | — | +0.00 | -15.96 | +24.78 | — |
| 2026-08-14 | `VOR` | 56 | $23.29 | $23.33 | -2.24 | — | +0.00 | -2.24 | -73.92 | — |
| 2026-08-14 | `TLN` | 1 | — | $359.83 | +0.00 | $362.74 | -2.91 | -2.91 | -0.00 | -2.91 |
| 2026-08-14 | `NRG` | 5 | — | $120.00 | +0.00 | $126.24 | -31.20 | -31.20 | -0.00 | -31.20 |
| 2026-08-14 | `MARA` | 68 | — | $9.01 | +0.00 | $9.20 | -12.92 | -12.92 | -0.00 | -12.92 |
| 2026-08-14 | `FOSL` | 109 | — | $5.64 | +0.00 | $5.57 | +7.63 | +7.63 | -0.00 | +7.63 |
| 2026-08-14 | `ARX` | 31 | — | $19.57 | +0.00 | $19.58 | -0.31 | -0.31 | -0.00 | -0.31 |
| 2026-08-14 | `CRMD` | 77 | — | $8.05 | +0.00 | $7.54 | +39.27 | +39.27 | -0.00 | +39.27 |
| 2026-08-14 | `BIRK` | 15 | — | $39.75 | +0.00 | $39.35 | +6.00 | +6.00 | -0.00 | +6.00 |
| 2026-08-14 | `HLIT` | 47 | — | $13.18 | +0.00 | $13.92 | -34.78 | -34.78 | -0.00 | -34.78 |
| 2026-08-17 | `TLN` | 1 | $362.74 | $367.88 | -5.14 | — | +0.00 | -5.14 | -8.05 | — |
| 2026-08-17 | `NRG` | 5 | $126.24 | $127.40 | -5.80 | — | +0.00 | -5.80 | -37.00 | — |
| 2026-08-17 | `MARA` | 68 | $9.20 | $9.22 | -1.36 | — | +0.00 | -1.36 | -14.28 | — |
| 2026-08-17 | `FOSL` | 109 | $5.57 | $5.50 | +7.63 | — | +0.00 | +7.63 | +15.26 | — |
| 2026-08-17 | `ARX` | 31 | $19.58 | $19.57 | +0.31 | — | +0.00 | +0.31 | -0.00 | — |
| 2026-08-17 | `CRMD` | 77 | $7.54 | $7.55 | -0.77 | — | +0.00 | -0.77 | +38.50 | — |
| 2026-08-17 | `BIRK` | 15 | $39.35 | $39.48 | -1.95 | — | +0.00 | -1.95 | +4.05 | — |
| 2026-08-17 | `HLIT` | 47 | $13.92 | $13.84 | +3.76 | — | +0.00 | +3.76 | -31.02 | — |
| 2026-08-17 | `TMC` | 152 | — | $4.05 | +0.00 | $3.77 | +42.56 | +42.56 | -0.00 | +42.56 |
| 2026-08-17 | `TGB` | 72 | — | $8.46 | +0.00 | $8.77 | -22.32 | -22.32 | -0.00 | -22.32 |
| 2026-08-17 | `ELF` | 6 | — | $90.54 | +0.00 | $93.66 | -18.72 | -18.72 | -0.00 | -18.72 |
| 2026-08-17 | `DNN` | 190 | — | $3.24 | +0.00 | $3.19 | +9.50 | +9.50 | -0.00 | +9.50 |
| 2026-08-17 | `HNST` | 128 | — | $4.81 | +0.00 | $4.70 | +14.08 | +14.08 | -0.00 | +14.08 |
| 2026-08-17 | `CAPR` | 89 | — | $6.87 | +0.00 | $7.45 | -51.62 | -51.62 | -0.00 | -51.62 |
| 2026-08-17 | `BYND` | 47 | — | $12.83 | +0.00 | $11.63 | +56.40 | +56.40 | -0.00 | +56.40 |
| 2026-08-17 | `NU` | 39 | — | $15.40 | +0.00 | $14.74 | +25.74 | +25.74 | -0.00 | +25.74 |
| 2026-08-18 | `TMC` | 152 | $3.77 | $3.72 | +7.60 | — | +0.00 | +7.60 | +50.16 | — |
| 2026-08-18 | `TGB` | 72 | $8.77 | $8.55 | +15.84 | — | +0.00 | +15.84 | -6.48 | — |
| 2026-08-18 | `ELF` | 6 | $93.66 | $93.44 | +1.32 | — | +0.00 | +1.32 | -17.40 | — |
| 2026-08-18 | `DNN` | 190 | $3.19 | $3.11 | +15.20 | — | +0.00 | +15.20 | +24.70 | — |
| 2026-08-18 | `HNST` | 128 | $4.70 | $4.67 | +3.84 | — | +0.00 | +3.84 | +17.92 | — |
| 2026-08-18 | `CAPR` | 89 | $7.45 | $7.50 | -4.45 | — | +0.00 | -4.45 | -56.07 | — |
| 2026-08-18 | `BYND` | 47 | $11.63 | $11.12 | +23.97 | — | +0.00 | +23.97 | +80.37 | — |
| 2026-08-18 | `NU` | 39 | $14.74 | $14.53 | +8.19 | — | +0.00 | +8.19 | +33.93 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 6 | — | $91.01 | +0.00 | $93.63 | -15.72 | -15.72 | -0.00 | -15.72 |
| 2026-08-20 | `MRVI` | 84 | — | $7.38 | +0.00 | $8.26 | -73.92 | -73.92 | -0.00 | -73.92 |
| 2026-08-20 | `CRCL` | 7 | — | $83.29 | +0.00 | $83.99 | -4.90 | -4.90 | -0.00 | -4.90 |
| 2026-08-20 | `WYFI` | 29 | — | $21.40 | +0.00 | $21.16 | +6.96 | +6.96 | -0.00 | +6.96 |
| 2026-08-20 | `TOYO` | 140 | — | $4.43 | +0.00 | $4.51 | -11.90 | -11.90 | -0.00 | -11.90 |
| 2026-08-20 | `DVLT` | 2071 | — | $0.30 | +0.00 | $0.32 | -41.42 | -41.42 | -0.00 | -41.42 |
| 2026-08-20 | `SAFX` | 1755 | — | $0.35 | +0.00 | $0.34 | +19.30 | +19.30 | -0.00 | +19.30 |
| 2026-08-20 | `AAP` | 13 | — | $46.85 | +0.00 | $42.39 | +57.98 | +57.98 | -0.00 | +57.98 |
| 2026-08-21 | `BHP` | 6 | $93.63 | $95.72 | -12.54 | — | +0.00 | -12.54 | -28.26 | — |
| 2026-08-21 | `MRVI` | 84 | $8.26 | $8.20 | +5.04 | $8.70 | -42.00 | -36.96 | -68.88 | -110.88 |
| 2026-08-21 | `CRCL` | 7 | $83.99 | $87.65 | -25.62 | — | +0.00 | -25.62 | -30.52 | — |
| 2026-08-21 | `WYFI` | 29 | $21.16 | $21.54 | -11.02 | — | +0.00 | -11.02 | -4.06 | — |
| 2026-08-21 | `TOYO` | 140 | $4.51 | $4.68 | -23.10 | — | +0.00 | -23.10 | -35.00 | — |
| 2026-08-21 | `DVLT` | 2071 | $0.32 | $0.31 | +20.71 | — | +0.00 | +20.71 | -20.71 | — |
| 2026-08-21 | `SAFX` | 1755 | $0.34 | $0.35 | -12.28 | — | +0.00 | -12.28 | +7.02 | — |
| 2026-08-21 | `AAP` | 13 | $42.39 | $42.41 | -0.26 | — | +0.00 | -0.26 | +57.72 | — |
| 2026-08-21 | `AUTL` | 281 | — | $2.47 | +0.00 | $2.41 | +16.86 | +16.86 | -0.00 | +16.86 |
| 2026-08-21 | `CRDL` | 360 | — | $1.93 | +0.00 | $1.86 | +25.20 | +25.20 | -0.00 | +25.20 |
| 2026-08-21 | `CRSP` | 11 | — | $59.72 | +0.00 | $59.50 | +2.42 | +2.42 | -0.00 | +2.42 |
| 2026-08-21 | `FUTU` | 6 | — | $115.18 | +0.00 | $123.64 | -50.76 | -50.76 | -0.00 | -50.76 |
| 2026-08-21 | `GMAB` | 20 | — | $33.36 | +0.00 | $33.45 | -1.80 | -1.80 | -0.00 | -1.80 |
| 2026-08-21 | `ENHA` | 407 | — | $1.71 | +0.00 | $1.72 | -4.07 | -4.07 | -0.00 | -4.07 |
| 2026-08-21 | `CAN` | 2368 | — | $0.29 | +0.00 | $0.35 | -144.45 | -144.45 | -0.00 | -144.45 |
| 2026-08-24 | `MRVI` | 84 | $8.70 | $8.59 | +9.24 | — | +0.00 | +9.24 | -101.64 | — |
| 2026-08-24 | `AUTL` | 281 | $2.41 | $2.36 | +14.05 | — | +0.00 | +14.05 | +30.91 | — |
| 2026-08-24 | `CRDL` | 360 | $1.86 | $1.87 | -3.60 | — | +0.00 | -3.60 | +21.60 | — |
| 2026-08-24 | `CRSP` | 11 | $59.50 | $58.79 | +7.81 | — | +0.00 | +7.81 | +10.23 | — |
| 2026-08-24 | `FUTU` | 6 | $123.64 | $120.87 | +16.62 | — | +0.00 | +16.62 | -34.14 | — |
| 2026-08-24 | `GMAB` | 20 | $33.45 | $32.82 | +12.60 | — | +0.00 | +12.60 | +10.80 | — |
| 2026-08-24 | `ENHA` | 407 | $1.72 | $1.74 | -8.14 | — | +0.00 | -8.14 | -12.21 | — |
| 2026-08-24 | `CAN` | 2368 | $0.35 | $0.38 | -59.20 | — | +0.00 | -59.20 | -203.65 | — |
| 2026-08-25 | `OCUL` | 54 | — | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-08-25 | `CRMD` | 71 | — | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-08-25 | `PUSA` | 159 | — | $3.70 | +0.00 | $3.91 | -33.39 | -33.39 | -0.00 | -33.39 |
| 2026-08-25 | `CAPR` | 87 | — | $6.79 | +0.00 | $7.19 | -34.80 | -34.80 | -0.00 | -34.80 |
| 2026-08-25 | `SAFX` | 1599 | — | $0.37 | +0.00 | $0.37 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-08-25 | `SUJA` | 67 | — | $8.79 | +0.00 | $8.54 | +16.75 | +16.75 | -0.00 | +16.75 |
| 2026-08-25 | `FWDI` | 98 | — | $5.99 | +0.00 | $5.86 | +12.74 | +12.74 | -0.00 | +12.74 |
| 2026-08-25 | `JANX` | 31 | — | $18.52 | +0.00 | $18.99 | -14.57 | -14.57 | -0.00 | -14.57 |
| 2026-08-26 | `OCUL` | 54 | $10.92 | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-08-26 | `CRMD` | 71 | $8.28 | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-08-26 | `PUSA` | 159 | $3.91 | $3.91 | +0.00 | $3.91 | +0.00 | +0.00 | -33.39 | -33.39 |
| 2026-08-26 | `CAPR` | 87 | $7.19 | $7.19 | +0.00 | $7.19 | +0.00 | +0.00 | -34.80 | -34.80 |
| 2026-08-26 | `SAFX` | 1599 | $0.37 | $0.37 | +0.00 | $0.37 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-08-26 | `SUJA` | 67 | $8.54 | $8.54 | +0.00 | $8.54 | +0.00 | +0.00 | +16.75 | +16.75 |
| 2026-08-26 | `FWDI` | 98 | $5.86 | $5.86 | +0.00 | $5.86 | +0.00 | +0.00 | +12.74 | +12.74 |
| 2026-08-26 | `JANX` | 31 | $18.99 | $18.99 | +0.00 | $18.99 | +0.00 | +0.00 | -14.57 | -14.57 |
| 2026-08-27 | `OCUL` | 54 | $10.92 | $10.79 | +7.02 | — | +0.00 | +7.02 | +7.02 | — |
| 2026-08-27 | `CRMD` | 71 | $8.28 | $8.60 | -22.72 | — | +0.00 | -22.72 | -22.72 | — |
| 2026-08-27 | `PUSA` | 159 | $3.91 | $3.84 | +11.13 | — | +0.00 | +11.13 | -22.26 | — |
| 2026-08-27 | `CAPR` | 87 | $7.19 | $8.29 | -95.70 | — | +0.00 | -95.70 | -130.50 | — |
| 2026-08-27 | `SAFX` | 1599 | $0.37 | $0.35 | +31.98 | — | +0.00 | +31.98 | +31.98 | — |
| 2026-08-27 | `SUJA` | 67 | $8.54 | $9.39 | -56.95 | — | +0.00 | -56.95 | -40.20 | — |
| 2026-08-27 | `FWDI` | 98 | $5.86 | $5.97 | -10.78 | — | +0.00 | -10.78 | +1.96 | — |
| 2026-08-27 | `JANX` | 31 | $18.99 | $18.59 | +12.40 | — | +0.00 | +12.40 | -2.17 | — |
| 2026-08-27 | `ACMR` | 7 | — | $80.97 | +0.00 | $79.11 | +13.02 | +13.02 | -0.00 | +13.02 |
| 2026-08-27 | `GGB` | 130 | — | $4.42 | +0.00 | $4.46 | -5.20 | -5.20 | -0.00 | -5.20 |
| 2026-08-27 | `MT` | 7 | — | $75.12 | +0.00 | $74.53 | +4.13 | +4.13 | -0.00 | +4.13 |
| 2026-08-27 | `TX` | 10 | — | $55.20 | +0.00 | $55.13 | +0.70 | +0.70 | -0.00 | +0.70 |
| 2026-08-27 | `LRCX` | 1 | — | $314.61 | +0.00 | $312.88 | +1.73 | +1.73 | -0.00 | +1.73 |
| 2026-08-27 | `MRVL` | 2 | — | $240.00 | +0.00 | $245.11 | -10.22 | -10.22 | -0.00 | -10.22 |
| 2026-08-27 | `NUE` | 2 | — | $248.91 | +0.00 | $252.80 | -7.78 | -7.78 | -0.00 | -7.78 |
| 2026-08-28 | `ACMR` | 7 | $79.11 | $81.65 | -17.78 | — | +0.00 | -17.78 | -4.76 | — |
| 2026-08-28 | `GGB` | 130 | $4.46 | $4.57 | -14.30 | — | +0.00 | -14.30 | -19.50 | — |
| 2026-08-28 | `MT` | 7 | $74.53 | $74.54 | -0.07 | — | +0.00 | -0.07 | +4.06 | — |
| 2026-08-28 | `TX` | 10 | $55.13 | $55.25 | -1.20 | — | +0.00 | -1.20 | -0.50 | — |
| 2026-08-28 | `LRCX` | 1 | $312.88 | $318.88 | -6.00 | — | +0.00 | -6.00 | -4.27 | — |
| 2026-08-28 | `MRVL` | 2 | $245.11 | $253.44 | -16.66 | — | +0.00 | -16.66 | -26.88 | — |
| 2026-08-28 | `NUE` | 2 | $252.80 | $252.00 | +1.60 | — | +0.00 | +1.60 | -6.18 | — |
| 2026-08-28 | `CAPR` | 62 | — | $9.19 | +0.00 | $10.06 | -53.94 | -53.94 | -0.00 | -53.94 |
| 2026-08-28 | `SEDG` | 16 | — | $33.78 | +0.00 | $33.51 | +4.32 | +4.32 | -0.00 | +4.32 |
| 2026-08-28 | `SMTC` | 3 | — | $149.40 | +0.00 | $142.43 | +20.91 | +20.91 | -0.00 | +20.91 |
| 2026-08-28 | `PYXS` | 172 | — | $3.31 | +0.00 | $3.32 | -1.72 | -1.72 | -0.00 | -1.72 |
| 2026-08-28 | `SAFX` | 1466 | — | $0.39 | +0.00 | $0.37 | +29.32 | +29.32 | -0.00 | +29.32 |
| 2026-08-28 | `OPTX` | 66 | — | $8.57 | +0.00 | $8.73 | -10.56 | -10.56 | -0.00 | -10.56 |
| 2026-08-28 | `TTMI` | 4 | — | $127.07 | +0.00 | $124.73 | +9.36 | +9.36 | -0.00 | +9.36 |
| 2026-08-28 | `APMD` | 19 | — | $29.50 | +0.00 | $28.72 | +14.82 | +14.82 | -0.00 | +14.82 |
| 2026-08-31 | `CAPR` | 62 | $10.06 | $9.44 | +38.44 | — | +0.00 | +38.44 | -15.50 | — |
| 2026-08-31 | `SEDG` | 16 | $33.51 | $31.50 | +32.16 | — | +0.00 | +32.16 | +36.48 | — |
| 2026-08-31 | `SMTC` | 3 | $142.43 | $133.04 | +28.17 | — | +0.00 | +28.17 | +49.08 | — |
| 2026-08-31 | `PYXS` | 172 | $3.32 | $3.23 | +15.48 | — | +0.00 | +15.48 | +13.76 | — |
| 2026-08-31 | `SAFX` | 1466 | $0.37 | $0.38 | -14.66 | — | +0.00 | -14.66 | +14.66 | — |
| 2026-08-31 | `OPTX` | 66 | $8.73 | $8.52 | +13.86 | — | +0.00 | +13.86 | +3.30 | — |
| 2026-08-31 | `TTMI` | 4 | $124.73 | $117.20 | +30.12 | — | +0.00 | +30.12 | +39.48 | — |
| 2026-08-31 | `APMD` | 19 | $28.72 | $29.80 | -20.52 | — | +0.00 | -20.52 | -5.70 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `CABA` | 176 | — | $3.27 | +0.00 | $3.57 | -52.80 | -52.80 | -0.00 | -52.80 |
| 2026-09-03 | `FRVO` | 31 | — | $18.40 | +0.00 | $17.98 | +13.02 | +13.02 | -0.00 | +13.02 |
| 2026-09-03 | `CTMX` | 155 | — | $3.72 | +0.00 | $3.72 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-09-03 | `EIX` | 10 | — | $56.78 | +0.00 | $55.19 | +15.90 | +15.90 | -0.00 | +15.90 |
| 2026-09-03 | `CRDL` | 267 | — | $2.16 | +0.00 | $2.17 | -2.67 | -2.67 | -0.00 | -2.67 |
| 2026-09-03 | `SION` | 87 | — | $6.63 | +0.00 | $7.31 | -59.16 | -59.16 | -0.00 | -59.16 |
| 2026-09-03 | `DUOL` | 3 | — | $156.24 | +0.00 | $157.85 | -4.83 | -4.83 | -0.00 | -4.83 |
| 2026-09-03 | `SAFX` | 1480 | — | $0.39 | +0.00 | $0.38 | +14.80 | +14.80 | -0.00 | +14.80 |
| 2026-09-04 | `CABA` | 176 | $3.57 | $3.63 | -10.56 | $3.48 | +26.40 | +15.84 | -63.36 | -36.96 |
| 2026-09-04 | `FRVO` | 31 | $17.98 | $18.27 | -8.99 | — | +0.00 | -8.99 | +4.03 | — |
| 2026-09-04 | `CTMX` | 155 | $3.72 | $3.73 | -1.55 | — | +0.00 | -1.55 | -1.55 | — |
| 2026-09-04 | `EIX` | 10 | $55.19 | $55.42 | -2.30 | — | +0.00 | -2.30 | +13.60 | — |
| 2026-09-04 | `CRDL` | 267 | $2.17 | $2.18 | -2.67 | — | +0.00 | -2.67 | -5.34 | — |
| 2026-09-04 | `SION` | 87 | $7.31 | $7.31 | +0.00 | $6.75 | +48.72 | +48.72 | -59.16 | -10.44 |
| 2026-09-04 | `DUOL` | 3 | $157.85 | $161.54 | -11.07 | — | +0.00 | -11.07 | -15.90 | — |
| 2026-09-04 | `SAFX` | 1480 | $0.38 | $0.38 | +0.00 | — | +0.00 | +0.00 | +14.80 | — |
| 2026-09-04 | `ASND` | 2 | — | $266.94 | +0.00 | $271.12 | -8.36 | -8.36 | -0.00 | -8.36 |
| 2026-09-04 | `SLBT` | 246 | — | $3.07 | +0.00 | $3.15 | -19.68 | -19.68 | -0.00 | -19.68 |
| 2026-09-04 | `MLYS` | 25 | — | $29.15 | +0.00 | $28.27 | +22.00 | +22.00 | -0.00 | +22.00 |
| 2026-09-04 | `CCOI` | 73 | — | $10.22 | +0.00 | $9.98 | +17.52 | +17.52 | -0.00 | +17.52 |
| 2026-09-04 | `IRD` | 162 | — | $4.66 | +0.00 | $4.60 | +9.72 | +9.72 | -0.00 | +9.72 |
| 2026-09-04 | `JLHL` | 121 | — | $6.20 | +0.00 | $6.18 | +2.42 | +2.42 | -0.00 | +2.42 |
| 2026-09-07 | `CABA` | 176 | $3.48 | $3.46 | +3.52 | $3.47 | -1.76 | +1.76 | -33.44 | -35.20 |
| 2026-09-07 | `SION` | 87 | $6.75 | $6.68 | +6.09 | — | +0.00 | +6.09 | -4.35 | — |
| 2026-09-07 | `ASND` | 2 | $271.12 | $267.96 | +6.32 | — | +0.00 | +6.32 | -2.04 | — |
| 2026-09-07 | `SLBT` | 246 | $3.15 | $3.15 | +0.00 | — | +0.00 | +0.00 | -19.68 | — |
| 2026-09-07 | `MLYS` | 25 | $28.27 | $28.00 | +6.75 | — | +0.00 | +6.75 | +28.75 | — |
| 2026-09-07 | `CCOI` | 73 | $9.98 | $10.02 | -2.92 | — | +0.00 | -2.92 | +14.60 | — |
| 2026-09-07 | `IRD` | 162 | $4.60 | $4.53 | +11.34 | — | +0.00 | +11.34 | +21.06 | — |
| 2026-09-07 | `JLHL` | 121 | $6.18 | $6.20 | -2.42 | — | +0.00 | -2.42 | -0.00 | — |
| 2026-09-07 | `ABTC` | 73 | — | $8.95 | +0.00 | $7.99 | +70.08 | +70.08 | -0.00 | +70.08 |
| 2026-09-07 | `MRLN` | 199 | — | $3.28 | +0.00 | $3.35 | -13.93 | -13.93 | -0.00 | -13.93 |
| 2026-09-07 | `BTBT` | 409 | — | $1.60 | +0.00 | $1.64 | -16.36 | -16.36 | -0.00 | -16.36 |
| 2026-09-07 | `CRCL` | 6 | — | $97.98 | +0.00 | $102.05 | -24.42 | -24.42 | -0.00 | -24.42 |
| 2026-09-07 | `MSTR` | 4 | — | $137.35 | +0.00 | $142.80 | -21.80 | -21.80 | -0.00 | -21.80 |
| 2026-09-07 | `DFDV` | 113 | — | $5.79 | +0.00 | $5.87 | -9.04 | -9.04 | -0.00 | -9.04 |
| 2026-09-07 | `CIFR` | 37 | — | $17.33 | +0.00 | $17.74 | -15.17 | -15.17 | -0.00 | -15.17 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | -56.90 | TGTX, SLS, HIMS, VOR | — | $14,955.47 | $9,934.23 | TGTX×25, SLS×106, HIMS×42, VOR×56 |
| 2026-08-14 | +5.50 | $14,955.47 | TGTX×25, SLS×106, HIMS×42, VOR×56 | $9,928.54 | -5.69 | -29.22 | TLN, NRG, MARA, FOSL, ARX, CRMD, BIRK, HLIT | TGTX, SLS, HIMS, VOR | $14,532.11 | $9,873.39 | TLN×1, NRG×5, MARA×68, FOSL×109, ARX×31, CRMD×77, BIRK×15, HLIT×47 |
| 2026-08-17 | +2.25 | $14,532.11 | TLN×1, NRG×5, MARA×68, FOSL×109, ARX×31, CRMD×77, BIRK×15, HLIT×47 | $9,870.07 | -3.32 | +55.62 | TMC, TGB, ELF, DNN, HNST, CAPR, BYND, NU | TLN, NRG, MARA, FOSL, ARX, CRMD, BIRK, HLIT | $14,648.94 | $9,890.28 | TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×47, NU×39 |
| 2026-08-18 | -6.20 | $14,648.94 | TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×47, NU×39 | $9,961.79 | +71.51 | +0.00 | — | TMC, TGB, ELF, DNN, HNST, CAPR, BYND, NU | $9,943.70 | $9,943.70 | — |
| 2026-08-19 | -7.20 | $9,943.70 | — | $9,943.70 | +0.00 | +0.00 | — | — | $9,943.70 | $9,943.70 | — |
| 2026-08-20 | +1.12 | $9,943.70 | — | $9,943.70 | +0.00 | -63.62 | BHP, MRVI, CRCL, WYFI, TOYO, DVLT, SAFX, AAP | — | $14,747.53 | $9,842.48 | BHP×6, MRVI×84, CRCL×7, WYFI×29, TOYO×140, DVLT×2071, SAFX×1755, AAP×13 |
| 2026-08-21 | +3.25 | $14,747.53 | BHP×6, MRVI×84, CRCL×7, WYFI×29, TOYO×140, DVLT×2071, SAFX×1755, AAP×13 | $9,783.41 | -59.07 | -198.60 | AUTL, CRDL, CRSP, FUTU, GMAB, ENHA, CAN | BHP, CRCL, WYFI, TOYO, DVLT, SAFX, AAP | $15,199.40 | $9,515.77 | MRVI×84, AUTL×281, CRDL×360, CRSP×11, FUTU×6, GMAB×20, ENHA×407, CAN×2368 |
| 2026-08-24 | -5.17 | $15,199.40 | MRVI×84, AUTL×281, CRDL×360, CRSP×11, FUTU×6, GMAB×20, ENHA×407, CAN×2368 | $9,505.15 | -10.62 | +0.00 | — | MRVI, AUTL, CRDL, CRSP, FUTU, GMAB, ENHA, CAN | $9,467.20 | $9,467.20 | — |
| 2026-08-25 | +1.80 | $9,467.20 | — | $9,467.20 | +0.00 | -53.27 | OCUL, CRMD, PUSA, CAPR, SAFX, SUJA, FWDI, JANX | — | $14,138.57 | $9,387.01 | OCUL×54, CRMD×71, PUSA×159, CAPR×87, SAFX×1599, SUJA×67, FWDI×98, JANX×31 |
| 2026-08-26 | +2.02 | $14,138.57 | OCUL×54, CRMD×71, PUSA×159, CAPR×87, SAFX×1599, SUJA×67, FWDI×98, JANX×31 | $9,387.01 | +0.00 | +0.00 | — | — | $14,138.57 | $9,387.01 | OCUL×54, CRMD×71, PUSA×159, CAPR×87, SAFX×1599, SUJA×67, FWDI×98, JANX×31 |
| 2026-08-27 | — | $14,138.57 | OCUL×54, CRMD×71, PUSA×159, CAPR×87, SAFX×1599, SUJA×67, FWDI×98, JANX×31 | $9,263.39 | -123.62 | -3.62 | ACMR, GGB, MT, TX, LRCX, MRVL, NUE | OCUL, CRMD, PUSA, CAPR, SAFX, SUJA, FWDI, JANX | $12,734.37 | $9,219.09 | ACMR×7, GGB×130, MT×7, TX×10, LRCX×1, MRVL×2, NUE×2 |
| 2026-08-28 | +0.75 | $12,734.37 | ACMR×7, GGB×130, MT×7, TX×10, LRCX×1, MRVL×2, NUE×2 | $9,164.68 | -54.41 | +12.51 | CAPR, SEDG, SMTC, PYXS, SAFX, OPTX, TTMI, APMD | ACMR, GGB, MT, TX, LRCX, MRVL, NUE | $13,458.59 | $9,137.18 | CAPR×62, SEDG×16, SMTC×3, PYXS×172, SAFX×1466, OPTX×66, TTMI×4, APMD×19 |
| 2026-08-31 | -5.85 | $13,458.59 | CAPR×62, SEDG×16, SMTC×3, PYXS×172, SAFX×1466, OPTX×66, TTMI×4, APMD×19 | $9,260.23 | +123.05 | +0.00 | — | CAPR, SEDG, SMTC, PYXS, SAFX, OPTX, TTMI, APMD | $9,235.31 | $9,235.31 | — |
| 2026-09-01 | -6.30 | $9,235.31 | — | $9,235.31 | -0.00 | +0.00 | — | — | $9,235.31 | $9,235.31 | — |
| 2026-09-02 | -3.83 | $9,235.31 | — | $9,235.31 | -0.00 | +0.00 | — | — | $9,235.31 | $9,235.31 | — |
| 2026-09-03 | -0.90 | $9,235.31 | — | $9,235.31 | -0.00 | -75.74 | CABA, FRVO, CTMX, EIX, CRDL, SION, DUOL, SAFX | — | $13,697.50 | $9,131.99 | CABA×176, FRVO×31, CTMX×155, EIX×10, CRDL×267, SION×87, DUOL×3, SAFX×1480 |
| 2026-09-04 | — | $13,697.50 | CABA×176, FRVO×31, CTMX×155, EIX×10, CRDL×267, SION×87, DUOL×3, SAFX×1480 | $9,094.85 | -37.14 | +98.74 | ASND, SLBT, MLYS, CCOI, IRD, JLHL | FRVO, CTMX, EIX, CRDL, DUOL, SAFX | $14,602.10 | $9,156.96 | CABA×176, SION×87, ASND×2, SLBT×246, MLYS×25, CCOI×73, IRD×162, JLHL×121 |
| 2026-09-07 | — | $14,602.10 | CABA×176, SION×87, ASND×2, SLBT×246, MLYS×25, CCOI×73, IRD×162, JLHL×121 | $9,185.64 | +28.68 | -32.40 | ABTC, MRLN, BTBT, CRCL, MSTR, DFDV, CIFR | SION, ASND, SLBT, MLYS, CCOI, IRD, JLHL | $14,152.44 | $9,117.85 | CABA×176, ABTC×73, MRLN×199, BTBT×409, CRCL×6, MSTR×4, DFDV×113, CIFR×37 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **SHORT** | `TGTX` | 25 | $49.70 | $2.12 | — | $11,240.38 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **SHORT** | `SLS` | 106 | $11.70 | $2.37 | — | $12,478.21 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **SHORT** | `HIMS` | 42 | $29.74 | $2.17 | — | $13,725.12 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **SHORT** | `VOR` | 56 | $22.01 | $2.21 | — | $14,955.47 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,955.47 | ▼ close $9,934.23 vs 09:30 $10,000.00 (session -56.90) | 16:00 close · cash $14,955.47 · equity $9,934.23 vs 09:30 $10,000.00 (-65.77; session marks -56.90) · 4 name(s) marked open→close (per-name table). TGTX×25 09:30 $49.70 → close $47.94 +44.00; SLS×106 09:30 $11.70 → close $12.36 -69.96; HIMS×42 09:30 $29.74 → close $28.77 +40.74; VOR×56 09:30 $22.01 → close $23.29 -71.68 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,955.47 | ▼ 09:30 equity $9,928.54 vs yday $9,934.23 (-5.69) | 09:30 open · cash $14,955.47 (unchanged overnight, no fees) · equity $9,928.54 vs prior close $9,934.23 (-5.69) · 4 name(s) re-marked at the open (per-name table). TGTX×25 yday $47.94 → 09:30 $47.27 +16.75; SLS×106 yday $12.36 → 09:30 $12.40 -4.24; HIMS×42 yday $28.77 → 09:30 $29.15 -15.96; VOR×56 yday $23.29 → 09:30 $23.33 -2.24 | — |
| 2026-08-14 09:30 ET | **COVER** | `TGTX` | 25 | $47.27 | $2.06 | $+56.57 | $13,771.65 | ▲ +56.57 after sell → book $9,926.47; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **COVER** | `SLS` | 106 | $12.40 | $2.31 | $-78.88 | $12,454.95 | ▼ -78.88 after sell → book $9,924.17; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **COVER** | `HIMS` | 42 | $29.15 | $2.12 | $+20.49 | $11,228.53 | ▲ +20.49 after sell → book $9,922.05; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **COVER** | `VOR` | 56 | $23.33 | $2.16 | $-78.29 | $9,919.89 | ▼ -78.29 after sell → book $9,919.89; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SHORT** | `TLN` | 1 | $359.83 | $2.02 | — | $10,277.70 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+5.9; leftover $619.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `NRG` | 5 | $120.00 | $2.04 | — | $10,875.66 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+0.6; leftover $619.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `MARA` | 68 | $9.01 | $2.23 | — | $11,486.11 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $619.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `FOSL` | 109 | $5.64 | $2.36 | — | $12,098.50 | — | last bar red; gate last_red=True; list probable; 🔵; ret5=-4.1; leftover $619.99 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `ARX` | 31 | $19.57 | $2.12 | — | $12,703.05 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $619.99 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `CRMD` | 77 | $8.05 | $2.26 | — | $13,320.64 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $619.99 | join🟢 sector🔴 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `BIRK` | 15 | $39.75 | $2.07 | — | $13,914.82 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+10.2; leftover $619.99 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `HLIT` | 47 | $13.18 | $2.17 | — | $14,532.11 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $619.99 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,532.11 | ▼ close $9,873.39 vs 09:30 $9,928.54 (session -29.22) | 16:00 close · cash $14,532.11 · equity $9,873.39 vs 09:30 $9,928.54 (-55.15; session marks -29.22) · 8 name(s) marked open→close (per-name table). TLN×1 09:30 $359.83 → close $362.74 -2.91; NRG×5 09:30 $120.00 → close $126.24 -31.20; MARA×68 09:30 $9.01 → close $9.20 -12.92; FOSL×109 09:30 $5.64 → close $5.57 +7.63; ARX×31 09:30 $19.57 → close $19.58 -0.31; CRMD×77 09:30 $8.05 → close $7.54 +39.27; BIRK×15 09:30 $39.75 → close $39.35 +6.00; HLIT×47 09:30 $13.18 → close $13.92 -34.78 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,532.11 | ▼ 09:30 equity $9,870.07 vs yday $9,873.39 (-3.32) | 09:30 open · cash $14,532.11 (unchanged overnight, no fees) · equity $9,870.07 vs prior close $9,873.39 (-3.32) · 8 name(s) re-marked at the open (per-name table). TLN×1 yday $362.74 → 09:30 $367.88 -5.14; NRG×5 yday $126.24 → 09:30 $127.40 -5.80; MARA×68 yday $9.20 → 09:30 $9.22 -1.36; FOSL×109 yday $5.57 → 09:30 $5.50 +7.63; ARX×31 yday $19.58 → 09:30 $19.57 +0.31; CRMD×77 yday $7.54 → 09:30 $7.55 -0.77; BIRK×15 yday $39.35 → 09:30 $39.48 -1.95; HLIT×47 yday $13.92 → 09:30 $13.84 +3.76 | — |
| 2026-08-17 09:30 ET | **COVER** | `TLN` | 1 | $367.88 | $1.99 | $-12.07 | $14,162.24 | ▼ -12.07 after sell → book $9,868.08; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `NRG` | 5 | $127.40 | $2.00 | $-41.05 | $13,523.24 | ▼ -41.05 after sell → book $9,866.08; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `MARA` | 68 | $9.22 | $2.19 | $-18.71 | $12,894.08 | ▼ -18.71 after sell → book $9,863.88; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `FOSL` | 109 | $5.50 | $2.32 | $+10.58 | $12,292.27 | ▲ +10.58 after sell → book $9,861.57; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `ARX` | 31 | $19.57 | $2.08 | $-4.20 | $11,683.51 | ▼ -4.20 after sell → book $9,859.48; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `CRMD` | 77 | $7.55 | $2.22 | $+34.02 | $11,099.94 | ▲ +34.02 after sell → book $9,857.26; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `BIRK` | 15 | $39.48 | $2.04 | $-0.06 | $10,505.71 | ▼ -0.06 after sell → book $9,855.23; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `HLIT` | 47 | $13.84 | $2.13 | $-35.32 | $9,853.10 | ▼ -35.32 after sell → book $9,853.10; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `TMC` | 152 | $4.05 | $2.50 | — | $10,466.20 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $615.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `TGB` | 72 | $8.46 | $2.24 | — | $11,073.07 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $615.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `ELF` | 6 | $90.54 | $2.04 | — | $11,614.27 | — | last bar red; gate last_red=True; list flatten; ret5=-7.2; leftover $615.82 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `DNN` | 190 | $3.24 | $2.62 | — | $12,227.25 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $615.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HNST` | 128 | $4.81 | $2.42 | — | $12,840.51 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=-11.4; leftover $615.82 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `CAPR` | 89 | $6.87 | $2.30 | — | $13,449.64 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+62.6; leftover $615.82 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `BYND` | 47 | $12.83 | $2.17 | — | $14,050.48 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ⚪; ret5=-34.1; leftover $615.82 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `NU` | 39 | $15.40 | $2.14 | — | $14,648.94 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $615.82 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,648.94 | ▲ close $9,890.28 vs 09:30 $9,870.07 (session +55.62) | 16:00 close · cash $14,648.94 · equity $9,890.28 vs 09:30 $9,870.07 (+20.21; session marks +55.62) · 8 name(s) marked open→close (per-name table). TMC×152 09:30 $4.05 → close $3.77 +42.56; TGB×72 09:30 $8.46 → close $8.77 -22.32; ELF×6 09:30 $90.54 → close $93.66 -18.72; DNN×190 09:30 $3.24 → close $3.19 +9.50; HNST×128 09:30 $4.81 → close $4.70 +14.08; CAPR×89 09:30 $6.87 → close $7.45 -51.62; BYND×47 09:30 $12.83 → close $11.63 +56.40; NU×39 09:30 $15.40 → close $14.74 +25.74 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,648.94 | ▲ 09:30 equity $9,961.79 vs yday $9,890.28 (+71.51) | 09:30 open · cash $14,648.94 (unchanged overnight, no fees) · equity $9,961.79 vs prior close $9,890.28 (+71.51) · 8 name(s) re-marked at the open (per-name table). TMC×152 yday $3.77 → 09:30 $3.72 +7.60; TGB×72 yday $8.77 → 09:30 $8.55 +15.84; ELF×6 yday $93.66 → 09:30 $93.44 +1.32; DNN×190 yday $3.19 → 09:30 $3.11 +15.20; HNST×128 yday $4.70 → 09:30 $4.67 +3.84; CAPR×89 yday $7.45 → 09:30 $7.50 -4.45; BYND×47 yday $11.63 → 09:30 $11.12 +23.97; NU×39 yday $14.74 → 09:30 $14.53 +8.19 | — |
| 2026-08-18 09:30 ET | **COVER** | `TMC` | 152 | $3.72 | $2.45 | $+45.22 | $14,081.05 | ▲ +45.22 after sell → book $9,959.34; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `TGB` | 72 | $8.55 | $2.21 | $-10.93 | $13,463.25 | ▼ -10.93 after sell → book $9,957.14; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `ELF` | 6 | $93.44 | $2.01 | $-21.45 | $12,900.60 | ▼ -21.45 after sell → book $9,955.13; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `DNN` | 190 | $3.11 | $2.56 | $+19.52 | $12,307.14 | ▲ +19.52 after sell → book $9,952.57; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **COVER** | `HNST` | 128 | $4.67 | $2.37 | $+13.12 | $11,707.01 | ▲ +13.12 after sell → book $9,950.20; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `CAPR` | 89 | $7.50 | $2.26 | $-60.63 | $11,037.25 | ▼ -60.63 after sell → book $9,947.94; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `BYND` | 47 | $11.12 | $2.13 | $+76.07 | $10,512.48 | ▲ +76.07 after sell → book $9,945.81; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `NU` | 39 | $14.53 | $2.11 | $+29.68 | $9,943.70 | ▲ +29.68 after sell → book $9,943.70; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,943.70 | ▲ close $9,943.70 vs 09:30 $9,961.79 (session +0.00) | 16:00 close · cash $9,943.70 · no lots left · equity $9,943.70. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,943.70 | ▲ 09:30 equity $9,943.70 vs yday $9,943.70 (+0.00) | 09:30 open · cash $9,943.70 · no holdings · equity $9,943.70 vs prior close $9,943.70 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,943.70 | ▲ close $9,943.70 vs 09:30 $9,943.70 (session +0.00) | 16:00 close · cash $9,943.70 · no lots left · equity $9,943.70. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,943.70 | ▲ 09:30 equity $9,943.70 vs yday $9,943.70 (+0.00) | 09:30 open · cash $9,943.70 · no holdings · equity $9,943.70 vs prior close $9,943.70 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **SHORT** | `BHP` | 6 | $91.01 | $2.04 | — | $10,487.72 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $621.48 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `MRVI` | 84 | $7.38 | $2.28 | — | $11,105.35 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $621.48 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `CRCL` | 7 | $83.29 | $2.05 | — | $11,686.34 | — | last bar red; gate last_red=True; list probable; 🔵; ⚪; ret5=+7.4; leftover $621.48 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 29 | $21.40 | $2.11 | — | $12,304.82 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-25.2; leftover $621.48 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 140 | $4.43 | $2.46 | — | $12,922.56 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-23.1; leftover $621.48 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `DVLT` | 2071 | $0.30 | $12.80 | — | $13,531.07 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-3.2; leftover $621.48 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `SAFX` | 1755 | $0.35 | $11.80 | — | $14,140.54 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-29.4; leftover $621.48 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $14,747.53 | — | last bar red; gate last_red=True; list earn_react; 🔵; ret5=+5.0; leftover $621.48 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,747.53 | ▼ close $9,842.48 vs 09:30 $9,943.70 (session -63.62) | 16:00 close · cash $14,747.53 · equity $9,842.48 vs 09:30 $9,943.70 (-101.22; session marks -63.62) · 8 name(s) marked open→close (per-name table). BHP×6 09:30 $91.01 → close $93.63 -15.72; MRVI×84 09:30 $7.38 → close $8.26 -73.92; CRCL×7 09:30 $83.29 → close $83.99 -4.90; WYFI×29 09:30 $21.40 → close $21.16 +6.96; TOYO×140 09:30 $4.43 → close $4.51 -11.90; DVLT×2071 09:30 $0.30 → close $0.32 -41.42; SAFX×1755 09:30 $0.35 → close $0.34 +19.30; AAP×13 09:30 $46.85 → close $42.39 +57.98 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,747.53 | ▼ 09:30 equity $9,783.41 vs yday $9,842.48 (-59.07) | 09:30 open · cash $14,747.53 (unchanged overnight, no fees) · equity $9,783.41 vs prior close $9,842.48 (-59.07) · 8 name(s) re-marked at the open (per-name table). BHP×6 yday $93.63 → 09:30 $95.72 -12.54; MRVI×84 yday $8.26 → 09:30 $8.20 +5.04; CRCL×7 yday $83.99 → 09:30 $87.65 -25.62; WYFI×29 yday $21.16 → 09:30 $21.54 -11.02; TOYO×140 yday $4.51 → 09:30 $4.68 -23.10; DVLT×2071 yday $0.32 → 09:30 $0.31 +20.71; SAFX×1755 yday $0.34 → 09:30 $0.35 -12.28; AAP×13 yday $42.39 → 09:30 $42.41 -0.26 | — |
| 2026-08-21 09:30 ET | **COVER** | `BHP` | 6 | $95.72 | $2.01 | $-32.31 | $14,171.20 | ▼ -32.31 after sell → book $9,781.40; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `CRCL` | 7 | $87.65 | $2.01 | $-34.58 | $13,555.64 | ▼ -34.58 after sell → book $9,779.39; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `WYFI` | 29 | $21.54 | $2.08 | $-8.25 | $12,928.90 | ▼ -8.25 after sell → book $9,777.31; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `TOYO` | 140 | $4.68 | $2.41 | $-39.87 | $12,271.29 | ▼ -39.87 after sell → book $9,774.90; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `DVLT` | 2071 | $0.31 | $12.63 | $-46.14 | $11,616.65 | ▼ -46.14 after sell → book $9,762.27; vs 09:30 mark -12.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `SAFX` | 1755 | $0.35 | $11.41 | $-16.18 | $10,990.99 | ▼ -16.18 after sell → book $9,750.86; vs 09:30 mark -11.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `AAP` | 13 | $42.41 | $2.03 | $+53.63 | $10,437.63 | ▲ +53.63 after sell → book $9,748.83; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AUTL` | 281 | $2.47 | $3.70 | — | $11,128.00 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $696.34 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CRDL` | 360 | $1.93 | $4.73 | — | $11,818.07 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $696.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CRSP` | 11 | $59.72 | $2.06 | — | $12,472.93 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $696.34 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `FUTU` | 6 | $115.18 | $2.05 | — | $13,161.96 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $696.34 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `GMAB` | 20 | $33.36 | $2.09 | — | $13,827.07 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $696.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ENHA` | 407 | $1.71 | $5.35 | — | $14,517.69 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $696.34 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CAN` | 2368 | $0.29 | $14.49 | — | $15,199.40 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $696.34 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,199.40 | ▼ close $9,515.77 vs 09:30 $9,783.41 (session -198.60) | 16:00 close · cash $15,199.40 · equity $9,515.77 vs 09:30 $9,783.41 (-267.64; session marks -198.60) · 8 name(s) marked open→close (per-name table). MRVI×84 09:30 $8.20 → close $8.70 -42.00; AUTL×281 09:30 $2.47 → close $2.41 +16.86; CRDL×360 09:30 $1.93 → close $1.86 +25.20; CRSP×11 09:30 $59.72 → close $59.50 +2.42; FUTU×6 09:30 $115.18 → close $123.64 -50.76; GMAB×20 09:30 $33.36 → close $33.45 -1.80; ENHA×407 09:30 $1.71 → close $1.72 -4.07; CAN×2368 09:30 $0.29 → close $0.35 -144.45 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,199.40 | ▼ 09:30 equity $9,505.15 vs yday $9,515.77 (-10.62) | 09:30 open · cash $15,199.40 (unchanged overnight, no fees) · equity $9,505.15 vs prior close $9,515.77 (-10.62) · 8 name(s) re-marked at the open (per-name table). MRVI×84 yday $8.70 → 09:30 $8.59 +9.24; AUTL×281 yday $2.41 → 09:30 $2.36 +14.05; CRDL×360 yday $1.86 → 09:30 $1.87 -3.60; CRSP×11 yday $59.50 → 09:30 $58.79 +7.81; FUTU×6 yday $123.64 → 09:30 $120.87 +16.62; GMAB×20 yday $33.45 → 09:30 $32.82 +12.60; ENHA×407 yday $1.72 → 09:30 $1.74 -8.14; CAN×2368 yday $0.35 → 09:30 $0.38 -59.20 | — |
| 2026-08-24 09:30 ET | **COVER** | `MRVI` | 84 | $8.59 | $2.24 | $-106.16 | $14,475.60 | ▼ -106.16 after sell → book $9,502.91; vs 09:30 mark -2.24 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AUTL` | 281 | $2.36 | $3.62 | $+23.58 | $13,808.81 | ▲ +23.58 after sell → book $9,499.28; vs 09:30 mark -3.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `CRDL` | 360 | $1.87 | $4.64 | $+12.22 | $13,130.97 | ▲ +12.22 after sell → book $9,494.64; vs 09:30 mark -4.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `CRSP` | 11 | $58.79 | $2.02 | $+6.15 | $12,482.25 | ▲ +6.15 after sell → book $9,492.61; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **COVER** | `FUTU` | 6 | $120.87 | $2.01 | $-38.19 | $11,755.03 | ▼ -38.19 after sell → book $9,490.61; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **COVER** | `GMAB` | 20 | $32.82 | $2.05 | $+6.66 | $11,096.58 | ▲ +6.66 after sell → book $9,488.56; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `ENHA` | 407 | $1.74 | $5.25 | $-22.81 | $10,383.14 | ▼ -22.81 after sell → book $9,483.30; vs 09:30 mark -5.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `CAN` | 2368 | $0.38 | $16.10 | $-234.24 | $9,467.20 | ▼ -234.24 after sell → book $9,467.20; vs 09:30 mark -16.10 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,467.20 | ▲ close $9,467.20 vs 09:30 $9,505.15 (session +0.00) | 16:00 close · cash $9,467.20 · no lots left · equity $9,467.20. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,467.20 | ▲ 09:30 equity $9,467.20 vs yday $9,467.20 (+0.00) | 09:30 open · cash $9,467.20 · no holdings · equity $9,467.20 vs prior close $9,467.20 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **SHORT** | `OCUL` | 54 | $10.92 | $2.19 | — | $10,054.69 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+10.4; leftover $591.70 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **SHORT** | `CRMD` | 71 | $8.28 | $2.24 | — | $10,640.33 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $591.70 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `PUSA` | 159 | $3.70 | $2.52 | — | $11,226.11 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $591.70 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `CAPR` | 87 | $6.79 | $2.29 | — | $11,814.55 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $591.70 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `SAFX` | 1599 | $0.37 | $11.00 | — | $12,395.18 | — | last bar red; gate last_red=True; list probable,yday_gainer; ret5=-26.5; leftover $591.70 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `SUJA` | 67 | $8.79 | $2.23 | — | $12,981.88 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $591.70 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `FWDI` | 98 | $5.99 | $2.33 | — | $13,566.57 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+20.7; leftover $591.70 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `JANX` | 31 | $18.52 | $2.12 | — | $14,138.57 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+7.9; leftover $591.70 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,138.57 | ▼ close $9,387.01 vs 09:30 $9,467.20 (session -53.27) | 16:00 close · cash $14,138.57 · equity $9,387.01 vs 09:30 $9,467.20 (-80.19; session marks -53.27) · 8 name(s) marked open→close (per-name table). OCUL×54 09:30 $10.92 → close $10.92 -0.00; CRMD×71 09:30 $8.28 → close $8.28 -0.00; PUSA×159 09:30 $3.70 → close $3.91 -33.39; CAPR×87 09:30 $6.79 → close $7.19 -34.80; SAFX×1599 09:30 $0.37 → close $0.37 -0.00; SUJA×67 09:30 $8.79 → close $8.54 +16.75; FWDI×98 09:30 $5.99 → close $5.86 +12.74; JANX×31 09:30 $18.52 → close $18.99 -14.57 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,138.57 | ▲ 09:30 equity $9,387.01 vs yday $9,387.01 (+0.00) | 09:30 open · cash $14,138.57 (unchanged overnight, no fees) · equity $9,387.01 vs prior close $9,387.01 (+0.00) · 8 name(s) re-marked at the open (per-name table). OCUL×54 yday $10.92 → 09:30 $10.92 -0.00; CRMD×71 yday $8.28 → 09:30 $8.28 -0.00; PUSA×159 yday $3.91 → 09:30 $3.91 -0.00; CAPR×87 yday $7.19 → 09:30 $7.19 -0.00; SAFX×1599 yday $0.37 → 09:30 $0.37 -0.00; SUJA×67 yday $8.54 → 09:30 $8.54 -0.00; FWDI×98 yday $5.86 → 09:30 $5.86 -0.00; JANX×31 yday $18.99 → 09:30 $18.99 -0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,138.57 | ▲ close $9,387.01 vs 09:30 $9,387.01 (session +0.00) | 16:00 close · cash $14,138.57 · equity $9,387.01 vs 09:30 $9,387.01 (+0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). OCUL×54 09:30 $10.92 → close $10.92 -0.00; CRMD×71 09:30 $8.28 → close $8.28 -0.00; PUSA×159 09:30 $3.91 → close $3.91 -0.00; CAPR×87 09:30 $7.19 → close $7.19 -0.00; SAFX×1599 09:30 $0.37 → close $0.37 -0.00; SUJA×67 09:30 $8.54 → close $8.54 -0.00; FWDI×98 09:30 $5.86 → close $5.86 -0.00; JANX×31 09:30 $18.99 → close $18.99 -0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,138.57 | ▼ 09:30 equity $9,263.39 vs yday $9,387.01 (-123.62) | 09:30 open · cash $14,138.57 (unchanged overnight, no fees) · equity $9,263.39 vs prior close $9,387.01 (-123.62) · 8 name(s) re-marked at the open (per-name table). OCUL×54 yday $10.92 → 09:30 $10.79 +7.02; CRMD×71 yday $8.28 → 09:30 $8.60 -22.72; PUSA×159 yday $3.91 → 09:30 $3.84 +11.13; CAPR×87 yday $7.19 → 09:30 $8.29 -95.70; SAFX×1599 yday $0.37 → 09:30 $0.35 +31.98; SUJA×67 yday $8.54 → 09:30 $9.39 -56.95; FWDI×98 yday $5.86 → 09:30 $5.97 -10.78; JANX×31 yday $18.99 → 09:30 $18.59 +12.40 | — |
| 2026-08-27 09:30 ET | **COVER** | `OCUL` | 54 | $10.79 | $2.15 | $+2.68 | $13,553.76 | ▲ +2.68 after sell → book $9,261.24; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `CRMD` | 71 | $8.60 | $2.20 | $-27.16 | $12,940.96 | ▼ -27.16 after sell → book $9,259.04; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `PUSA` | 159 | $3.84 | $2.47 | $-27.25 | $12,327.93 | ▼ -27.25 after sell → book $9,256.57; vs 09:30 mark -2.47 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `CAPR` | 87 | $8.29 | $2.25 | $-135.04 | $11,604.45 | ▼ -135.04 after sell → book $9,254.32; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `SAFX` | 1599 | $0.35 | $10.39 | $+10.58 | $11,034.41 | ▲ +10.58 after sell → book $9,243.93; vs 09:30 mark -10.39 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `SUJA` | 67 | $9.39 | $2.19 | $-44.62 | $10,403.09 | ▼ -44.62 after sell → book $9,241.74; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `FWDI` | 98 | $5.97 | $2.28 | $-2.65 | $9,815.74 | ▼ -2.65 after sell → book $9,239.45; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `JANX` | 31 | $18.59 | $2.08 | $-6.37 | $9,237.37 | ▼ -6.37 after sell → book $9,237.37; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SHORT** | `ACMR` | 7 | $80.97 | $2.05 | — | $9,802.11 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-1.3; leftover $577.34 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `GGB` | 130 | $4.42 | $2.43 | — | $10,374.29 | — | last bar red; gate last_red=True; list mover_buy; ret5=-8.6; leftover $577.34 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 7 | $75.12 | $2.05 | — | $10,898.08 | — | last bar red; gate last_red=True; list mover_buy; ret5=-2.2; leftover $577.34 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 10 | $55.20 | $2.06 | — | $11,448.03 | — | last bar red; gate last_red=True; list mover_buy; ret5=+3.0; leftover $577.34 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `LRCX` | 1 | $314.61 | $2.02 | — | $11,760.61 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-5.5; leftover $577.34 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `MRVL` | 2 | $240.00 | $2.03 | — | $12,238.58 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=+6.8; leftover $577.34 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `NUE` | 2 | $248.91 | $2.03 | — | $12,734.37 | — | last bar red; gate last_red=True; list mover_buy; ret5=-9.4; leftover $577.34 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,734.37 | ▼ close $9,219.09 vs 09:30 $9,263.39 (session -3.62) | 16:00 close · cash $12,734.37 · equity $9,219.09 vs 09:30 $9,263.39 (-44.30; session marks -3.62) · 7 name(s) marked open→close (per-name table). ACMR×7 09:30 $80.97 → close $79.11 +13.02; GGB×130 09:30 $4.42 → close $4.46 -5.20; MT×7 09:30 $75.12 → close $74.53 +4.13; TX×10 09:30 $55.20 → close $55.13 +0.70; LRCX×1 09:30 $314.61 → close $312.88 +1.73; MRVL×2 09:30 $240.00 → close $245.11 -10.22; NUE×2 09:30 $248.91 → close $252.80 -7.78 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,734.37 | ▼ 09:30 equity $9,164.68 vs yday $9,219.09 (-54.41) | 09:30 open · cash $12,734.37 (unchanged overnight, no fees) · equity $9,164.68 vs prior close $9,219.09 (-54.41) · 7 name(s) re-marked at the open (per-name table). ACMR×7 yday $79.11 → 09:30 $81.65 -17.78; GGB×130 yday $4.46 → 09:30 $4.57 -14.30; MT×7 yday $74.53 → 09:30 $74.54 -0.07; TX×10 yday $55.13 → 09:30 $55.25 -1.20; LRCX×1 yday $312.88 → 09:30 $318.88 -6.00; MRVL×2 yday $245.11 → 09:30 $253.44 -16.66; NUE×2 yday $252.80 → 09:30 $252.00 +1.60 | — |
| 2026-08-28 09:30 ET | **COVER** | `ACMR` | 7 | $81.65 | $2.01 | $-8.82 | $12,160.81 | ▼ -8.82 after sell → book $9,162.67; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `GGB` | 130 | $4.57 | $2.38 | $-24.31 | $11,564.33 | ▼ -24.31 after sell → book $9,160.29; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `MT` | 7 | $74.54 | $2.01 | $+0.00 | $11,040.54 | ▼ +0.00 after sell → book $9,158.28; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `TX` | 10 | $55.25 | $2.02 | $-4.58 | $10,486.02 | ▼ -4.58 after sell → book $9,156.26; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `LRCX` | 1 | $318.88 | $1.99 | $-8.28 | $10,165.15 | ▼ -8.28 after sell → book $9,154.27; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `MRVL` | 2 | $253.44 | $2.00 | $-30.91 | $9,656.27 | ▼ -30.91 after sell → book $9,152.27; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **COVER** | `NUE` | 2 | $252.00 | $2.00 | $-10.21 | $9,150.28 | ▼ -10.21 after sell → book $9,150.28; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SHORT** | `CAPR` | 62 | $9.19 | $2.21 | — | $9,717.85 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $571.89 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SEDG` | 16 | $33.78 | $2.07 | — | $10,256.25 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $571.89 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SMTC` | 3 | $149.40 | $2.03 | — | $10,702.42 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $571.89 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `PYXS` | 172 | $3.31 | $2.56 | — | $11,269.18 | — | last bar red; gate last_red=True; list yday_gainer; ret5=+2.3; leftover $571.89 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SAFX` | 1466 | $0.39 | $10.38 | — | $11,830.54 | — | last bar red; gate last_red=True; list yday_gainer; ret5=-26.5; leftover $571.89 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `OPTX` | 66 | $8.57 | $2.22 | — | $12,393.93 | — | last bar red; gate last_red=True; list yday_gainer; ret5=-3.4; leftover $571.89 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `TTMI` | 4 | $127.07 | $2.04 | — | $12,900.18 | — | last bar red; gate last_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $571.89 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `APMD` | 19 | $29.50 | $2.08 | — | $13,458.59 | — | last bar red; gate last_red=True; list yday_gainer; ret5=-11.7; leftover $571.89 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,458.59 | ▲ close $9,137.18 vs 09:30 $9,164.68 (session +12.51) | 16:00 close · cash $13,458.59 · equity $9,137.18 vs 09:30 $9,164.68 (-27.50; session marks +12.51) · 8 name(s) marked open→close (per-name table). CAPR×62 09:30 $9.19 → close $10.06 -53.94; SEDG×16 09:30 $33.78 → close $33.51 +4.32; SMTC×3 09:30 $149.40 → close $142.43 +20.91; PYXS×172 09:30 $3.31 → close $3.32 -1.72; SAFX×1466 09:30 $0.39 → close $0.37 +29.32; OPTX×66 09:30 $8.57 → close $8.73 -10.56; TTMI×4 09:30 $127.07 → close $124.73 +9.36; APMD×19 09:30 $29.50 → close $28.72 +14.82 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,458.59 | ▲ 09:30 equity $9,260.23 vs yday $9,137.18 (+123.05) | 09:30 open · cash $13,458.59 (unchanged overnight, no fees) · equity $9,260.23 vs prior close $9,137.18 (+123.05) · 8 name(s) re-marked at the open (per-name table). CAPR×62 yday $10.06 → 09:30 $9.44 +38.44; SEDG×16 yday $33.51 → 09:30 $31.50 +32.16; SMTC×3 yday $142.43 → 09:30 $133.04 +28.17; PYXS×172 yday $3.32 → 09:30 $3.23 +15.48; SAFX×1466 yday $0.37 → 09:30 $0.38 -14.66; OPTX×66 yday $8.73 → 09:30 $8.52 +13.86; TTMI×4 yday $124.73 → 09:30 $117.20 +30.12; APMD×19 yday $28.72 → 09:30 $29.80 -20.52 | — |
| 2026-08-31 09:30 ET | **COVER** | `CAPR` | 62 | $9.44 | $2.18 | $-19.89 | $12,871.14 | ▼ -19.89 after sell → book $9,258.06; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SEDG` | 16 | $31.50 | $2.04 | $+32.37 | $12,365.10 | ▲ +32.37 after sell → book $9,256.02; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SMTC` | 3 | $133.04 | $2.00 | $+45.05 | $11,963.98 | ▲ +45.05 after sell → book $9,254.02; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `PYXS` | 172 | $3.23 | $2.51 | $+8.69 | $11,405.92 | ▲ +8.69 after sell → book $9,251.52; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SAFX` | 1466 | $0.38 | $9.97 | $-5.69 | $10,838.87 | ▼ -5.69 after sell → book $9,241.55; vs 09:30 mark -9.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `OPTX` | 66 | $8.52 | $2.19 | $-1.11 | $10,274.36 | ▼ -1.11 after sell → book $9,239.36; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `TTMI` | 4 | $117.20 | $2.00 | $+35.44 | $9,803.56 | ▲ +35.44 after sell → book $9,237.36; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `APMD` | 19 | $29.80 | $2.05 | $-9.83 | $9,235.31 | ▼ -9.83 after sell → book $9,235.31; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,235.31 | ▲ close $9,235.31 vs 09:30 $9,260.23 (session +0.00) | 16:00 close · cash $9,235.31 · no lots left · equity $9,235.31. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,235.31 | ▲ 09:30 equity $9,235.31 vs yday $9,235.31 (-0.00) | 09:30 open · cash $9,235.31 · no holdings · equity $9,235.31 vs prior close $9,235.31 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,235.31 | ▲ close $9,235.31 vs 09:30 $9,235.31 (session +0.00) | 16:00 close · cash $9,235.31 · no lots left · equity $9,235.31. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,235.31 | ▲ 09:30 equity $9,235.31 vs yday $9,235.31 (-0.00) | 09:30 open · cash $9,235.31 · no holdings · equity $9,235.31 vs prior close $9,235.31 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,235.31 | ▲ close $9,235.31 vs 09:30 $9,235.31 (session +0.00) | 16:00 close · cash $9,235.31 · no lots left · equity $9,235.31. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,235.31 | ▲ 09:30 equity $9,235.31 vs yday $9,235.31 (-0.00) | 09:30 open · cash $9,235.31 · no holdings · equity $9,235.31 vs prior close $9,235.31 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **SHORT** | `CABA` | 176 | $3.27 | $2.57 | — | $9,808.26 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $577.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `FRVO` | 31 | $18.40 | $2.12 | — | $10,376.54 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $577.21 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `CTMX` | 155 | $3.72 | $2.51 | — | $10,950.63 | — | last bar red; gate last_red=True; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $577.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `EIX` | 10 | $56.78 | $2.06 | — | $11,516.38 | — | last bar red; gate last_red=True; list probable,yday_gainer; ret5=+0.3; leftover $577.21 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `CRDL` | 267 | $2.16 | $3.51 | — | $12,089.58 | — | last bar red; gate last_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $577.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `SION` | 87 | $6.63 | $2.29 | — | $12,664.10 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-18.1; leftover $577.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `DUOL` | 3 | $156.24 | $2.03 | — | $13,130.79 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+10.0; leftover $577.21 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `SAFX` | 1480 | $0.39 | $10.48 | — | $13,697.50 | — | last bar red; gate last_red=True; list yday_gainer; ret5=-26.5; leftover $577.21 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,697.50 | ▼ close $9,131.99 vs 09:30 $9,235.31 (session -75.74) | 16:00 close · cash $13,697.50 · equity $9,131.99 vs 09:30 $9,235.31 (-103.32; session marks -75.74) · 8 name(s) marked open→close (per-name table). CABA×176 09:30 $3.27 → close $3.57 -52.80; FRVO×31 09:30 $18.40 → close $17.98 +13.02; CTMX×155 09:30 $3.72 → close $3.72 -0.00; EIX×10 09:30 $56.78 → close $55.19 +15.90; CRDL×267 09:30 $2.16 → close $2.17 -2.67; SION×87 09:30 $6.63 → close $7.31 -59.16; DUOL×3 09:30 $156.24 → close $157.85 -4.83; SAFX×1480 09:30 $0.39 → close $0.38 +14.80 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,697.50 | ▼ 09:30 equity $9,094.85 vs yday $9,131.99 (-37.14) | 09:30 open · cash $13,697.50 (unchanged overnight, no fees) · equity $9,094.85 vs prior close $9,131.99 (-37.14) · 8 name(s) re-marked at the open (per-name table). CABA×176 yday $3.57 → 09:30 $3.63 -10.56; FRVO×31 yday $17.98 → 09:30 $18.27 -8.99; CTMX×155 yday $3.72 → 09:30 $3.73 -1.55; EIX×10 yday $55.19 → 09:30 $55.42 -2.30; CRDL×267 yday $2.17 → 09:30 $2.18 -2.67; SION×87 yday $7.31 → 09:30 $7.31 -0.00; DUOL×3 yday $157.85 → 09:30 $161.54 -11.07; SAFX×1480 yday $0.38 → 09:30 $0.38 -0.00 | — |
| 2026-09-04 09:30 ET | **COVER** | `FRVO` | 31 | $18.27 | $2.08 | $-0.17 | $13,129.05 | ▼ -0.17 after sell → book $9,092.77; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `CTMX` | 155 | $3.73 | $2.46 | $-6.51 | $12,548.45 | ▼ -6.51 after sell → book $9,090.32; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `EIX` | 10 | $55.42 | $2.02 | $+9.52 | $11,992.23 | ▲ +9.52 after sell → book $9,088.30; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `CRDL` | 267 | $2.18 | $3.44 | $-12.30 | $11,406.72 | ▼ -12.30 after sell → book $9,084.85; vs 09:30 mark -3.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `DUOL` | 3 | $161.54 | $2.00 | $-19.93 | $10,920.10 | ▼ -19.93 after sell → book $9,082.85; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `SAFX` | 1480 | $0.38 | $10.06 | $-5.75 | $10,347.64 | ▼ -5.75 after sell → book $9,072.79; vs 09:30 mark -10.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `ASND` | 2 | $266.94 | $2.03 | — | $10,879.49 | — | last bar red; gate last_red=True; list flatten; ret5=+1.9; leftover $756.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `SLBT` | 246 | $3.07 | $3.24 | — | $11,631.46 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $756.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `MLYS` | 25 | $29.15 | $2.10 | — | $12,358.11 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $756.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `CCOI` | 73 | $10.22 | $2.25 | — | $13,101.92 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $756.07 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `IRD` | 162 | $4.66 | $2.53 | — | $13,854.30 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $756.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `JLHL` | 121 | $6.20 | $2.40 | — | $14,602.10 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=-8.2; leftover $756.07 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,602.10 | ▲ close $9,156.96 vs 09:30 $9,094.85 (session +98.74) | 16:00 close · cash $14,602.10 · equity $9,156.96 vs 09:30 $9,094.85 (+62.11; session marks +98.74) · 8 name(s) marked open→close (per-name table). CABA×176 09:30 $3.63 → close $3.48 +26.40; SION×87 09:30 $7.31 → close $6.75 +48.72; ASND×2 09:30 $266.94 → close $271.12 -8.36; SLBT×246 09:30 $3.07 → close $3.15 -19.68; MLYS×25 09:30 $29.15 → close $28.27 +22.00; CCOI×73 09:30 $10.22 → close $9.98 +17.52; IRD×162 09:30 $4.66 → close $4.60 +9.72; JLHL×121 09:30 $6.20 → close $6.18 +2.42 | — |
| 2026-09-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,602.10 | ▲ 09:30 equity $9,185.64 vs yday $9,156.96 (+28.68) | 09:30 open · cash $14,602.10 (unchanged overnight, no fees) · equity $9,185.64 vs prior close $9,156.96 (+28.68) · 8 name(s) re-marked at the open (per-name table). CABA×176 yday $3.48 → 09:30 $3.46 +3.52; SION×87 yday $6.75 → 09:30 $6.68 +6.09; ASND×2 yday $271.12 → 09:30 $267.96 +6.32; SLBT×246 yday $3.15 → 09:30 $3.15 -0.00; MLYS×25 yday $28.27 → 09:30 $28.00 +6.75; CCOI×73 yday $9.98 → 09:30 $10.02 -2.92; IRD×162 yday $4.60 → 09:30 $4.53 +11.34; JLHL×121 yday $6.18 → 09:30 $6.20 -2.42 | — |
| 2026-09-07 09:30 ET | **COVER** | `SION` | 87 | $6.68 | $2.25 | $-8.89 | $14,018.69 | ▼ -8.89 after sell → book $9,183.39; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-09-07 09:30 ET | **COVER** | `ASND` | 2 | $267.96 | $2.00 | $-6.07 | $13,480.77 | ▼ -6.07 after sell → book $9,181.39; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **COVER** | `SLBT` | 246 | $3.15 | $3.17 | $-26.10 | $12,702.70 | ▼ -26.10 after sell → book $9,178.22; vs 09:30 mark -3.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **COVER** | `MLYS` | 25 | $28.00 | $2.06 | $+24.58 | $12,000.63 | ▲ +24.58 after sell → book $9,176.15; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **COVER** | `CCOI` | 73 | $10.02 | $2.21 | $+10.14 | $11,266.97 | ▲ +10.14 after sell → book $9,173.95; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **COVER** | `IRD` | 162 | $4.53 | $2.48 | $+16.05 | $10,530.63 | ▲ +16.05 after sell → book $9,171.47; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **COVER** | `JLHL` | 121 | $6.20 | $2.35 | $-4.76 | $9,778.08 | ▼ -4.76 after sell → book $9,169.12; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SHORT** | `ABTC` | 73 | $8.95 | $2.25 | — | $10,429.18 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $654.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **SHORT** | `MRLN` | 199 | $3.28 | $2.65 | — | $11,079.25 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.6; leftover $654.94 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-07 09:30 ET | **SHORT** | `BTBT` | 409 | $1.60 | $5.37 | — | $11,728.28 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-2.5; leftover $654.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **SHORT** | `CRCL` | 6 | $97.98 | $2.04 | — | $12,314.11 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+7.4; leftover $654.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **SHORT** | `MSTR` | 4 | $137.35 | $2.04 | — | $12,861.48 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+28.2; leftover $654.94 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **SHORT** | `DFDV` | 113 | $5.79 | $2.38 | — | $13,513.37 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+28.3; leftover $654.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **SHORT** | `CIFR` | 37 | $17.33 | $2.14 | — | $14,152.44 | — | last bar red; gate last_red=True; list yday_gainer; ret5=-11.7; leftover $654.94 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,152.44 | ▼ close $9,117.85 vs 09:30 $9,185.64 (session -32.40) | 16:00 close · cash $14,152.44 · equity $9,117.85 vs 09:30 $9,185.64 (-67.79; session marks -32.40) · 8 name(s) marked open→close (per-name table). CABA×176 09:30 $3.46 → close $3.47 -1.76; ABTC×73 09:30 $8.95 → close $7.99 +70.08; MRLN×199 09:30 $3.28 → close $3.35 -13.93; BTBT×409 09:30 $1.60 → close $1.64 -16.36; CRCL×6 09:30 $97.98 → close $102.05 -24.42; MSTR×4 09:30 $137.35 → close $142.80 -21.80; DFDV×113 09:30 $5.79 → close $5.87 -9.04; CIFR×37 09:30 $17.33 → close $17.74 -15.17 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `TBPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENHA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENVX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INV` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `INMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `XHG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AEM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `PUSA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SAFX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SUJA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `FWDI` | no_price | no 09:30 open — carry |
| 2026-08-26 | `JANX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `BE` | no_price | no 09:30 open |
| 2026-08-26 | `INDP` | no_price | no 09:30 open |
| 2026-08-26 | `AXTI` | no_price | no 09:30 open |
| 2026-08-27 | `MU` | cash | leftover split 577.34 < 1 share @ 925.74 |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `IRD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BEP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `STIM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VLRS` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CABA` | 176 | 2026-09-03 @ $3.27 | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $577.21 |
| `ABTC` | 73 | 2026-09-07 @ $8.95 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $654.94 |
| `MRLN` | 199 | 2026-09-07 @ $3.28 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.6; leftover $654.94 |
| `BTBT` | 409 | 2026-09-07 @ $1.60 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-2.5; leftover $654.94 |
| `CRCL` | 6 | 2026-09-07 @ $97.98 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+7.4; leftover $654.94 |
| `MSTR` | 4 | 2026-09-07 @ $137.35 | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+28.2; leftover $654.94 |
| `DFDV` | 113 | 2026-09-07 @ $5.79 | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+28.3; leftover $654.94 |
| `CIFR` | 37 | 2026-09-07 @ $17.33 | last bar red; gate last_red=True; list yday_gainer; ret5=-11.7; leftover $654.94 |
