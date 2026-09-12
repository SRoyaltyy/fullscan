# Factor mine action — `short_last_red_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · last bar red

Cash book **-9.62%** ($9,038) · signal-only (no cash/fees) was -10.04%. Starts YES **5/21**. Fills 163 · skips 78 · realized $-976.25.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,699.42.

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
| 2026-08-20 | `MRVI` | 83 | — | $7.44 | +0.00 | $8.29 | -70.55 | -70.55 | -0.00 | -70.55 |
| 2026-08-20 | `CRCL` | 7 | — | $82.99 | +0.00 | $83.66 | -4.69 | -4.69 | -0.00 | -4.69 |
| 2026-08-20 | `WYFI` | 29 | — | $21.40 | +0.00 | $21.16 | +6.96 | +6.96 | -0.00 | +6.96 |
| 2026-08-20 | `TOYO` | 140 | — | $4.43 | +0.00 | $4.51 | -11.90 | -11.90 | -0.00 | -11.90 |
| 2026-08-20 | `DVLT` | 2071 | — | $0.30 | +0.00 | $0.32 | -41.42 | -41.42 | -0.00 | -41.42 |
| 2026-08-20 | `SAFX` | 1755 | — | $0.35 | +0.00 | $0.34 | +19.30 | +19.30 | -0.00 | +19.30 |
| 2026-08-20 | `AAP` | 13 | — | $46.85 | +0.00 | $42.39 | +57.98 | +57.98 | -0.00 | +57.98 |
| 2026-08-21 | `BHP` | 6 | $93.63 | $95.72 | -12.54 | — | +0.00 | -12.54 | -28.26 | — |
| 2026-08-21 | `MRVI` | 83 | $8.29 | $8.28 | +0.83 | — | +0.00 | +0.83 | -69.72 | — |
| 2026-08-21 | `CRCL` | 7 | $83.66 | $87.98 | -30.24 | — | +0.00 | -30.24 | -34.93 | — |
| 2026-08-21 | `WYFI` | 29 | $21.16 | $21.54 | -11.02 | — | +0.00 | -11.02 | -4.06 | — |
| 2026-08-21 | `TOYO` | 140 | $4.51 | $4.68 | -23.10 | — | +0.00 | -23.10 | -35.00 | — |
| 2026-08-21 | `DVLT` | 2071 | $0.32 | $0.31 | +20.71 | — | +0.00 | +20.71 | -20.71 | — |
| 2026-08-21 | `SAFX` | 1755 | $0.34 | $0.35 | -12.28 | — | +0.00 | -12.28 | +7.02 | — |
| 2026-08-21 | `AAP` | 13 | $42.39 | $42.41 | -0.26 | — | +0.00 | -0.26 | +57.72 | — |
| 2026-08-21 | `AUTL` | 246 | — | $2.47 | +0.00 | $2.41 | +14.76 | +14.76 | -0.00 | +14.76 |
| 2026-08-21 | `CRDL` | 315 | — | $1.93 | +0.00 | $1.86 | +22.05 | +22.05 | -0.00 | +22.05 |
| 2026-08-21 | `CRSP` | 10 | — | $59.72 | +0.00 | $59.50 | +2.20 | +2.20 | -0.00 | +2.20 |
| 2026-08-21 | `FUTU` | 5 | — | $115.18 | +0.00 | $123.64 | -42.30 | -42.30 | -0.00 | -42.30 |
| 2026-08-21 | `GMAB` | 18 | — | $33.36 | +0.00 | $33.45 | -1.62 | -1.62 | -0.00 | -1.62 |
| 2026-08-21 | `ENHA` | 356 | — | $1.71 | +0.00 | $1.72 | -3.56 | -3.56 | -0.00 | -3.56 |
| 2026-08-21 | `CAN` | 2070 | — | $0.29 | +0.00 | $0.35 | -126.27 | -126.27 | -0.00 | -126.27 |
| 2026-08-21 | `PRQR` | 267 | — | $2.28 | +0.00 | $2.34 | -16.02 | -16.02 | -0.00 | -16.02 |
| 2026-08-24 | `AUTL` | 246 | $2.41 | $2.40 | +2.46 | — | +0.00 | +2.46 | +17.22 | — |
| 2026-08-24 | `CRDL` | 315 | $1.86 | $1.88 | -6.30 | — | +0.00 | -6.30 | +15.75 | — |
| 2026-08-24 | `CRSP` | 10 | $59.50 | $58.75 | +7.50 | — | +0.00 | +7.50 | +9.70 | — |
| 2026-08-24 | `FUTU` | 5 | $123.64 | $121.00 | +13.20 | — | +0.00 | +13.20 | -29.10 | — |
| 2026-08-24 | `GMAB` | 18 | $33.45 | $32.82 | +11.34 | — | +0.00 | +11.34 | +9.72 | — |
| 2026-08-24 | `ENHA` | 356 | $1.72 | $1.74 | -7.12 | — | +0.00 | -7.12 | -10.68 | — |
| 2026-08-24 | `CAN` | 2070 | $0.35 | $0.38 | -57.96 | — | +0.00 | -57.96 | -184.23 | — |
| 2026-08-24 | `PRQR` | 267 | $2.34 | $2.35 | -2.67 | — | +0.00 | -2.67 | -18.69 | — |
| 2026-08-25 | `MOS` | 24 | — | $23.77 | +0.00 | $24.27 | -12.00 | -12.00 | -0.00 | -12.00 |
| 2026-08-25 | `OCUL` | 53 | — | $10.98 | +0.00 | $10.88 | +5.30 | +5.30 | -0.00 | +5.30 |
| 2026-08-25 | `INSP` | 9 | — | $61.19 | +0.00 | $61.07 | +1.08 | +1.08 | -0.00 | +1.08 |
| 2026-08-25 | `RZLT` | 119 | — | $4.94 | +0.00 | $5.01 | -8.33 | -8.33 | -0.00 | -8.33 |
| 2026-08-25 | `HCA` | 1 | — | $426.97 | +0.00 | $428.76 | -1.79 | -1.79 | -0.00 | -1.79 |
| 2026-08-25 | `CAPR` | 81 | — | $7.25 | +0.00 | $8.29 | -84.24 | -84.24 | -0.00 | -84.24 |
| 2026-08-25 | `PUSA` | 155 | — | $3.80 | +0.00 | $3.78 | +3.10 | +3.10 | -0.00 | +3.10 |
| 2026-08-25 | `CYPH` | 379 | — | $1.56 | +0.00 | $1.64 | -30.32 | -30.32 | -0.00 | -30.32 |
| 2026-08-26 | `MOS` | 24 | $24.27 | $24.84 | -13.68 | — | +0.00 | -13.68 | -25.68 | — |
| 2026-08-26 | `OCUL` | 53 | $10.88 | $10.79 | +4.77 | $10.77 | +1.06 | +5.83 | +10.07 | +11.13 |
| 2026-08-26 | `INSP` | 9 | $61.07 | $60.07 | +9.00 | $61.80 | -15.57 | -6.57 | +10.08 | -5.49 |
| 2026-08-26 | `RZLT` | 119 | $5.01 | $5.01 | +0.00 | — | +0.00 | +0.00 | -8.33 | — |
| 2026-08-26 | `HCA` | 1 | $428.76 | $427.50 | +1.26 | — | +0.00 | +1.26 | -0.53 | — |
| 2026-08-26 | `CAPR` | 81 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | -84.24 | — |
| 2026-08-26 | `PUSA` | 155 | $3.78 | $3.83 | -8.53 | — | +0.00 | -8.53 | -5.43 | — |
| 2026-08-26 | `CYPH` | 379 | $1.64 | $1.60 | +15.16 | — | +0.00 | +15.16 | -15.16 | — |
| 2026-08-26 | `FLNC` | 69 | — | $11.12 | +0.00 | $11.08 | +2.76 | +2.76 | -0.00 | +2.76 |
| 2026-08-26 | `AVEX` | 44 | — | $17.51 | +0.00 | $18.34 | -36.52 | -36.52 | -0.00 | -36.52 |
| 2026-08-26 | `AXTI` | 11 | — | $65.34 | +0.00 | $65.18 | +1.76 | +1.76 | -0.00 | +1.76 |
| 2026-08-26 | `INDP` | 712 | — | $1.09 | +0.00 | $1.14 | -35.60 | -35.60 | -0.00 | -35.60 |
| 2026-08-26 | `NVTS` | 61 | — | $12.60 | +0.00 | $12.67 | -4.27 | -4.27 | -0.00 | -4.27 |
| 2026-08-26 | `IRDM` | 16 | — | $46.96 | +0.00 | $47.20 | -3.84 | -3.84 | -0.00 | -3.84 |
| 2026-08-27 | `OCUL` | 53 | $10.77 | $10.63 | +7.42 | — | +0.00 | +7.42 | +18.55 | — |
| 2026-08-27 | `INSP` | 9 | $61.80 | $62.10 | -2.70 | — | +0.00 | -2.70 | -8.19 | — |
| 2026-08-27 | `FLNC` | 69 | $11.08 | $11.52 | -30.36 | — | +0.00 | -30.36 | -27.60 | — |
| 2026-08-27 | `AVEX` | 44 | $18.34 | $18.43 | -3.96 | — | +0.00 | -3.96 | -40.48 | — |
| 2026-08-27 | `AXTI` | 11 | $65.18 | $70.30 | -56.32 | — | +0.00 | -56.32 | -54.56 | — |
| 2026-08-27 | `INDP` | 712 | $1.14 | $1.13 | +7.12 | — | +0.00 | +7.12 | -28.48 | — |
| 2026-08-27 | `NVTS` | 61 | $12.67 | $13.18 | -31.11 | — | +0.00 | -31.11 | -35.38 | — |
| 2026-08-27 | `IRDM` | 16 | $47.20 | $47.46 | -4.16 | — | +0.00 | -4.16 | -8.00 | — |
| 2026-08-27 | `MOS` | 23 | — | $24.00 | +0.00 | $23.76 | +5.52 | +5.52 | -0.00 | +5.52 |
| 2026-08-27 | `ACMR` | 6 | — | $81.65 | +0.00 | $80.49 | +6.96 | +6.96 | -0.00 | +6.96 |
| 2026-08-27 | `MT` | 7 | — | $74.54 | +0.00 | $74.63 | -0.63 | -0.63 | -0.00 | -0.63 |
| 2026-08-27 | `TX` | 10 | — | $55.25 | +0.00 | $55.83 | -5.80 | -5.80 | -0.00 | -5.80 |
| 2026-08-27 | `DLO` | 37 | — | $15.33 | +0.00 | $15.14 | +7.03 | +7.03 | -0.00 | +7.03 |
| 2026-08-27 | `LRCX` | 1 | — | $318.88 | +0.00 | $318.58 | +0.30 | +0.30 | -0.00 | +0.30 |
| 2026-08-27 | `NVDA` | 2 | — | $222.86 | +0.00 | $227.98 | -10.24 | -10.24 | -0.00 | -10.24 |
| 2026-08-28 | `MOS` | 23 | $23.76 | $23.95 | -4.37 | $23.60 | +8.05 | +3.68 | +1.15 | +9.20 |
| 2026-08-28 | `ACMR` | 6 | $80.49 | $79.27 | +7.32 | — | +0.00 | +7.32 | +14.28 | — |
| 2026-08-28 | `MT` | 7 | $74.63 | $75.39 | -5.32 | — | +0.00 | -5.32 | -5.95 | — |
| 2026-08-28 | `TX` | 10 | $55.83 | $55.97 | -1.40 | — | +0.00 | -1.40 | -7.20 | — |
| 2026-08-28 | `DLO` | 37 | $15.14 | $15.19 | -1.85 | — | +0.00 | -1.85 | +5.18 | — |
| 2026-08-28 | `LRCX` | 1 | $318.58 | $318.03 | +0.55 | — | +0.00 | +0.55 | +0.85 | — |
| 2026-08-28 | `NVDA` | 2 | $227.98 | $227.36 | +1.24 | — | +0.00 | +1.24 | -9.00 | — |
| 2026-08-28 | `SEDG` | 19 | — | $32.90 | +0.00 | $31.41 | +28.31 | +28.31 | -0.00 | +28.31 |
| 2026-08-28 | `GRRR` | 41 | — | $15.66 | +0.00 | $14.41 | +51.25 | +51.25 | -0.00 | +51.25 |
| 2026-08-28 | `URBN` | 8 | — | $79.42 | +0.00 | $81.09 | -13.36 | -13.36 | -0.00 | -13.36 |
| 2026-08-28 | `SAFX` | 1771 | — | $0.36 | +0.00 | $0.36 | +10.63 | +10.63 | -0.00 | +10.63 |
| 2026-08-28 | `SIMO` | 2 | — | $252.24 | +0.00 | $245.81 | +12.86 | +12.86 | -0.00 | +12.86 |
| 2026-08-28 | `XPOF` | 120 | — | $5.38 | +0.00 | $5.43 | -6.00 | -6.00 | -0.00 | -6.00 |
| 2026-08-28 | `BHVN` | 40 | — | $15.88 | +0.00 | $15.41 | +18.80 | +18.80 | -0.00 | +18.80 |
| 2026-08-31 | `MOS` | 23 | $23.60 | $23.68 | -1.84 | — | +0.00 | -1.84 | +7.36 | — |
| 2026-08-31 | `SEDG` | 19 | $31.41 | $31.15 | +4.94 | — | +0.00 | +4.94 | +33.25 | — |
| 2026-08-31 | `GRRR` | 41 | $14.41 | $14.44 | -1.23 | — | +0.00 | -1.23 | +50.02 | — |
| 2026-08-31 | `URBN` | 8 | $81.09 | $80.44 | +5.20 | — | +0.00 | +5.20 | -8.16 | — |
| 2026-08-31 | `SAFX` | 1771 | $0.36 | $0.36 | -5.31 | — | +0.00 | -5.31 | +5.31 | — |
| 2026-08-31 | `SIMO` | 2 | $245.81 | $247.05 | -2.48 | — | +0.00 | -2.48 | +10.38 | — |
| 2026-08-31 | `XPOF` | 120 | $5.43 | $5.37 | +7.20 | — | +0.00 | +7.20 | +1.20 | — |
| 2026-08-31 | `BHVN` | 40 | $15.41 | $15.46 | -2.00 | — | +0.00 | -2.00 | +16.80 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `CRK` | 36 | — | $15.45 | +0.00 | $14.95 | +18.00 | +18.00 | -0.00 | +18.00 |
| 2026-09-03 | `MRNA` | 3 | — | $145.94 | +0.00 | $148.87 | -8.78 | -8.78 | -0.00 | -8.78 |
| 2026-09-03 | `EIX` | 10 | — | $55.42 | +0.00 | $56.30 | -8.80 | -8.80 | -0.00 | -8.80 |
| 2026-09-03 | `SAFX` | 1510 | — | $0.38 | +0.00 | $0.38 | -3.02 | -3.02 | -0.00 | -3.02 |
| 2026-09-03 | `FRVO` | 31 | — | $18.28 | +0.00 | $17.16 | +34.72 | +34.72 | -0.00 | +34.72 |
| 2026-09-03 | `DEFT` | 876 | — | $0.65 | +0.00 | $0.68 | -25.40 | -25.40 | -0.00 | -25.40 |
| 2026-09-03 | `GMRS` | 44 | — | $12.83 | +0.00 | $13.41 | -25.52 | -25.52 | -0.00 | -25.52 |
| 2026-09-03 | `KLRA` | 35 | — | $15.95 | +0.00 | $15.74 | +7.35 | +7.35 | -0.00 | +7.35 |
| 2026-09-04 | `CRK` | 36 | $14.95 | $15.00 | -1.80 | — | +0.00 | -1.80 | +16.20 | — |
| 2026-09-04 | `MRNA` | 3 | $148.87 | $153.62 | -14.25 | — | +0.00 | -14.25 | -23.03 | — |
| 2026-09-04 | `EIX` | 10 | $56.30 | $55.79 | +5.10 | — | +0.00 | +5.10 | -3.70 | — |
| 2026-09-04 | `SAFX` | 1510 | $0.38 | $0.38 | +1.51 | — | +0.00 | +1.51 | -1.51 | — |
| 2026-09-04 | `FRVO` | 31 | $17.16 | $17.27 | -3.41 | — | +0.00 | -3.41 | +31.31 | — |
| 2026-09-04 | `DEFT` | 876 | $0.68 | $0.69 | -9.64 | — | +0.00 | -9.64 | -35.04 | — |
| 2026-09-04 | `GMRS` | 44 | $13.41 | $13.29 | +5.28 | — | +0.00 | +5.28 | -20.24 | — |
| 2026-09-04 | `KLRA` | 35 | $15.74 | $15.60 | +4.90 | — | +0.00 | +4.90 | +12.25 | — |
| 2026-09-04 | `CABA` | 163 | — | $3.46 | +0.00 | $3.47 | -1.63 | -1.63 | -0.00 | -1.63 |
| 2026-09-04 | `ALEC` | 223 | — | $2.52 | +0.00 | $2.46 | +13.38 | +13.38 | -0.00 | +13.38 |
| 2026-09-04 | `BHC` | 84 | — | $6.71 | +0.00 | $6.56 | +12.60 | +12.60 | -0.00 | +12.60 |
| 2026-09-04 | `BMEA` | 296 | — | $1.90 | +0.00 | $2.03 | -38.48 | -38.48 | -0.00 | -38.48 |
| 2026-09-04 | `OABI` | 118 | — | $4.78 | +0.00 | $4.33 | +53.10 | +53.10 | -0.00 | +53.10 |
| 2026-09-04 | `OPK` | 354 | — | $1.59 | +0.00 | $1.64 | -17.70 | -17.70 | -0.00 | -17.70 |
| 2026-09-04 | `VIR` | 49 | — | $11.31 | +0.00 | $11.38 | -3.67 | -3.67 | -0.00 | -3.67 |
| 2026-09-04 | `ATRC` | 10 | — | $52.03 | +0.00 | $51.52 | +5.10 | +5.10 | -0.00 | +5.10 |
| 2026-09-08 | `CABA` | 163 | $3.47 | $3.43 | +6.52 | — | +0.00 | +6.52 | +4.89 | — |
| 2026-09-08 | `ALEC` | 223 | $2.46 | $2.38 | +17.84 | — | +0.00 | +17.84 | +31.22 | — |
| 2026-09-08 | `BHC` | 84 | $6.56 | $6.57 | -0.84 | — | +0.00 | -0.84 | +11.76 | — |
| 2026-09-08 | `BMEA` | 296 | $2.03 | $2.00 | +8.88 | — | +0.00 | +8.88 | -29.60 | — |
| 2026-09-08 | `OABI` | 118 | $4.33 | $4.30 | +3.54 | — | +0.00 | +3.54 | +56.64 | — |
| 2026-09-08 | `OPK` | 354 | $1.64 | $1.63 | +3.54 | — | +0.00 | +3.54 | -14.16 | — |
| 2026-09-08 | `VIR` | 49 | $11.38 | $11.22 | +8.08 | — | +0.00 | +8.08 | +4.41 | — |
| 2026-09-08 | `ATRC` | 10 | $51.52 | $54.31 | -27.90 | — | +0.00 | -27.90 | -22.80 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `AMTX` | 276 | — | $2.04 | +0.00 | $2.01 | +8.28 | +8.28 | -0.00 | +8.28 |
| 2026-09-11 | `LDI` | 663 | — | $0.85 | +0.00 | $0.83 | +9.95 | +9.95 | -0.00 | +9.95 |
| 2026-09-11 | `BAK` | 266 | — | $2.12 | +0.00 | $2.08 | +10.64 | +10.64 | -0.00 | +10.64 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | -56.90 | TGTX, SLS, HIMS, VOR | — | $14,955.47 | $9,934.23 | TGTX×25, SLS×106, HIMS×42, VOR×56 |
| 2026-08-14 | +5.50 | $14,955.47 | TGTX×25, SLS×106, HIMS×42, VOR×56 | $9,928.54 | -5.69 | -29.22 | TLN, NRG, MARA, FOSL, ARX, CRMD, BIRK, HLIT | TGTX, SLS, HIMS, VOR | $14,532.11 | $9,873.39 | TLN×1, NRG×5, MARA×68, FOSL×109, ARX×31, CRMD×77, BIRK×15, HLIT×47 |
| 2026-08-17 | +2.25 | $14,532.11 | TLN×1, NRG×5, MARA×68, FOSL×109, ARX×31, CRMD×77, BIRK×15, HLIT×47 | $9,870.07 | -3.32 | +55.62 | TMC, TGB, ELF, DNN, HNST, CAPR, BYND, NU | TLN, NRG, MARA, FOSL, ARX, CRMD, BIRK, HLIT | $14,648.94 | $9,890.28 | TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×47, NU×39 |
| 2026-08-18 | -6.20 | $14,648.94 | TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×47, NU×39 | $9,961.79 | +71.51 | +0.00 | — | TMC, TGB, ELF, DNN, HNST, CAPR, BYND, NU | $9,943.70 | $9,943.70 | — |
| 2026-08-19 | -7.20 | $9,943.70 | — | $9,943.70 | +0.00 | +0.00 | — | — | $9,943.70 | $9,943.70 | — |
| 2026-08-20 | +1.12 | $9,943.70 | — | $9,943.70 | +0.00 | -60.04 | BHP, MRVI, CRCL, WYFI, TOYO, DVLT, SAFX, AAP | — | $14,743.03 | $9,846.06 | BHP×6, MRVI×83, CRCL×7, WYFI×29, TOYO×140, DVLT×2071, SAFX×1755, AAP×13 |
| 2026-08-21 | +3.25 | $14,743.03 | BHP×6, MRVI×83, CRCL×7, WYFI×29, TOYO×140, DVLT×2071, SAFX×1755, AAP×13 | $9,778.16 | -67.90 | -150.76 | AUTL, CRDL, CRSP, FUTU, GMAB, ENHA, CAN, PRQR | BHP, MRVI, CRCL, WYFI, TOYO, DVLT, SAFX, AAP | $14,522.17 | $9,556.16 | AUTL×246, CRDL×315, CRSP×10, FUTU×5, GMAB×18, ENHA×356, CAN×2070, PRQR×267 |
| 2026-08-24 | -5.17 | $14,522.17 | AUTL×246, CRDL×315, CRSP×10, FUTU×5, GMAB×18, ENHA×356, CAN×2070, PRQR×267 | $9,516.61 | -39.55 | +0.00 | — | AUTL, CRDL, CRSP, FUTU, GMAB, ENHA, CAN, PRQR | $9,481.13 | $9,481.13 | — |
| 2026-08-25 | +1.80 | $9,481.13 | — | $9,481.13 | +0.00 | -127.20 | MOS, OCUL, INSP, RZLT, HCA, CAPR, PUSA, CYPH | — | $13,946.07 | $9,333.42 | MOS×24, OCUL×53, INSP×9, RZLT×119, HCA×1, CAPR×81, PUSA×155, CYPH×379 |
| 2026-08-26 | +2.02 | $13,946.07 | MOS×24, OCUL×53, INSP×9, RZLT×119, HCA×1, CAPR×81, PUSA×155, CYPH×379 | $9,341.41 | +7.99 | -90.22 | FLNC, AVEX, AXTI, INDP, NVTS, IRDM | MOS, RZLT, HCA, CAPR, PUSA, CYPH | $14,970.34 | $9,215.12 | OCUL×53, INSP×9, FLNC×69, AVEX×44, AXTI×11, INDP×712, NVTS×61, IRDM×16 |
| 2026-08-27 | — | $14,970.34 | OCUL×53, INSP×9, FLNC×69, AVEX×44, AXTI×11, INDP×712, NVTS×61, IRDM×16 | $9,101.05 | -114.07 | +3.14 | MOS, ACMR, MT, TX, DLO, LRCX, NVDA | OCUL, INSP, FLNC, AVEX, AXTI, INDP, NVTS, IRDM | $12,510.71 | $9,065.86 | MOS×23, ACMR×6, MT×7, TX×10, DLO×37, LRCX×1, NVDA×2 |
| 2026-08-28 | +0.75 | $12,510.71 | MOS×23, ACMR×6, MT×7, TX×10, DLO×37, LRCX×1, NVDA×2 | $9,062.03 | -3.83 | +110.54 | SEDG, GRRR, URBN, SAFX, SIMO, XPOF, BHVN | ACMR, MT, TX, DLO, LRCX, NVDA | $13,910.01 | $9,135.48 | MOS×23, SEDG×19, GRRR×41, URBN×8, SAFX×1771, SIMO×2, XPOF×120, BHVN×40 |
| 2026-08-31 | -5.85 | $13,910.01 | MOS×23, SEDG×19, GRRR×41, URBN×8, SAFX×1771, SIMO×2, XPOF×120, BHVN×40 | $9,139.95 | +4.47 | +0.00 | — | MOS, SEDG, GRRR, URBN, SAFX, SIMO, XPOF, BHVN | $9,113.54 | $9,113.54 | — |
| 2026-09-01 | -6.30 | $9,113.54 | — | $9,113.54 | +0.00 | +0.00 | — | — | $9,113.54 | $9,113.54 | — |
| 2026-09-02 | -3.83 | $9,113.54 | — | $9,113.54 | +0.00 | +0.00 | — | — | $9,113.54 | $9,113.54 | — |
| 2026-09-03 | -0.90 | $9,113.54 | — | $9,113.54 | +0.00 | -11.45 | CRK, MRNA, EIX, SAFX, FRVO, DEFT, GMRS, KLRA | — | $13,458.28 | $9,070.47 | CRK×36, MRNA×3, EIX×10, SAFX×1510, FRVO×31, DEFT×876, GMRS×44, KLRA×35 |
| 2026-09-04 | +2.25 | $13,458.28 | CRK×36, MRNA×3, EIX×10, SAFX×1510, FRVO×31, DEFT×876, GMRS×44, KLRA×35 | $9,058.17 | -12.30 | +22.70 | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC | CRK, MRNA, EIX, SAFX, FRVO, DEFT, GMRS, KLRA | $13,457.30 | $9,026.63 | CABA×163, ALEC×223, BHC×84, BMEA×296, OABI×118, OPK×354, VIR×49, ATRC×10 |
| 2026-09-08 | -11.47 | $13,457.30 | CABA×163, ALEC×223, BHC×84, BMEA×296, OABI×118, OPK×354, VIR×49, ATRC×10 | $9,046.29 | +19.66 | +0.00 | — | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC | $9,023.81 | $9,023.81 | — |
| 2026-09-09 | -13.95 | $9,023.81 | — | $9,023.81 | -0.00 | +0.00 | — | — | $9,023.81 | $9,023.81 | — |
| 2026-09-10 | -13.28 | $9,023.81 | — | $9,023.81 | -0.00 | +0.00 | — | — | $9,023.81 | $9,023.81 | — |
| 2026-09-11 | +0.50 | $9,023.81 | — | $9,023.81 | -0.00 | +28.87 | AMTX, LDI, BAK | — | $10,699.42 | $9,037.78 | AMTX×276, LDI×663, BAK×266 |

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
| 2026-08-14 09:30 ET | **SHORT** | `MARA` | 68 | $9.01 | $2.23 | — | $11,486.11 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $619.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `FOSL` | 109 | $5.64 | $2.36 | — | $12,098.50 | — | last bar red; gate last_red=True; list probable; 🔵; ret5=-4.1; leftover $619.99 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `ARX` | 31 | $19.57 | $2.12 | — | $12,703.05 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $619.99 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `CRMD` | 77 | $8.05 | $2.26 | — | $13,320.64 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $619.99 | join🟢 sector🔴 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `BIRK` | 15 | $39.75 | $2.07 | — | $13,914.82 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+10.2; leftover $619.99 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `HLIT` | 47 | $13.18 | $2.17 | — | $14,532.11 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $619.99 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
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
| 2026-08-17 09:30 ET | **SHORT** | `NU` | 39 | $15.40 | $2.14 | — | $14,648.94 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $615.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
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
| 2026-08-20 09:30 ET | **SHORT** | `MRVI` | 83 | $7.44 | $2.28 | — | $11,102.96 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $621.48 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `CRCL` | 7 | $82.99 | $2.05 | — | $11,681.84 | — | last bar red; gate last_red=True; list probable; 🔵; ⚪; ret5=+7.4; leftover $621.48 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 29 | $21.40 | $2.11 | — | $12,300.33 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-25.2; leftover $621.48 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 140 | $4.43 | $2.46 | — | $12,918.07 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-23.1; leftover $621.48 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `DVLT` | 2071 | $0.30 | $12.80 | — | $13,526.57 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-3.2; leftover $621.48 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `SAFX` | 1755 | $0.35 | $11.80 | — | $14,136.04 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-29.4; leftover $621.48 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $14,743.03 | — | last bar red; gate last_red=True; list earn_react; 🔵; ret5=+5.0; leftover $621.48 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,743.03 | ▼ close $9,846.06 vs 09:30 $9,943.70 (session -60.04) | 16:00 close · cash $14,743.03 · equity $9,846.06 vs 09:30 $9,943.70 (-97.64; session marks -60.04) · 8 name(s) marked open→close (per-name table). BHP×6 09:30 $91.01 → close $93.63 -15.72; MRVI×83 09:30 $7.44 → close $8.29 -70.55; CRCL×7 09:30 $82.99 → close $83.66 -4.69; WYFI×29 09:30 $21.40 → close $21.16 +6.96; TOYO×140 09:30 $4.43 → close $4.51 -11.90; DVLT×2071 09:30 $0.30 → close $0.32 -41.42; SAFX×1755 09:30 $0.35 → close $0.34 +19.30; AAP×13 09:30 $46.85 → close $42.39 +57.98 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,743.03 | ▼ 09:30 equity $9,778.16 vs yday $9,846.06 (-67.90) | 09:30 open · cash $14,743.03 (unchanged overnight, no fees) · equity $9,778.16 vs prior close $9,846.06 (-67.90) · 8 name(s) re-marked at the open (per-name table). BHP×6 yday $93.63 → 09:30 $95.72 -12.54; MRVI×83 yday $8.29 → 09:30 $8.28 +0.83; CRCL×7 yday $83.66 → 09:30 $87.98 -30.24; WYFI×29 yday $21.16 → 09:30 $21.54 -11.02; TOYO×140 yday $4.51 → 09:30 $4.68 -23.10; DVLT×2071 yday $0.32 → 09:30 $0.31 +20.71; SAFX×1755 yday $0.34 → 09:30 $0.35 -12.28; AAP×13 yday $42.39 → 09:30 $42.41 -0.26 | — |
| 2026-08-21 09:30 ET | **COVER** | `BHP` | 6 | $95.72 | $2.01 | $-32.31 | $14,166.70 | ▼ -32.31 after sell → book $9,776.15; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `MRVI` | 83 | $8.28 | $2.24 | $-74.24 | $13,477.22 | ▼ -74.24 after sell → book $9,773.91; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `CRCL` | 7 | $87.98 | $2.01 | $-38.99 | $12,859.35 | ▼ -38.99 after sell → book $9,771.90; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `WYFI` | 29 | $21.54 | $2.08 | $-8.25 | $12,232.61 | ▼ -8.25 after sell → book $9,769.82; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `TOYO` | 140 | $4.68 | $2.41 | $-39.87 | $11,575.00 | ▼ -39.87 after sell → book $9,767.41; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `DVLT` | 2071 | $0.31 | $12.63 | $-46.14 | $10,920.36 | ▼ -46.14 after sell → book $9,754.78; vs 09:30 mark -12.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `SAFX` | 1755 | $0.35 | $11.41 | $-16.18 | $10,294.70 | ▼ -16.18 after sell → book $9,743.37; vs 09:30 mark -11.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `AAP` | 13 | $42.41 | $2.03 | $+53.63 | $9,741.34 | ▲ +53.63 after sell → book $9,741.34; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AUTL` | 246 | $2.47 | $3.24 | — | $10,345.72 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $608.83 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CRDL` | 315 | $1.93 | $4.14 | — | $10,949.53 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $608.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CRSP` | 10 | $59.72 | $2.06 | — | $11,544.67 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $608.83 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `FUTU` | 5 | $115.18 | $2.04 | — | $12,118.53 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $608.83 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `GMAB` | 18 | $33.36 | $2.08 | — | $12,716.93 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $608.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ENHA` | 356 | $1.71 | $4.68 | — | $13,321.01 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $608.83 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CAN` | 2070 | $0.29 | $12.67 | — | $13,916.93 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $608.83 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `PRQR` | 267 | $2.28 | $3.52 | — | $14,522.17 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+22.7; leftover $608.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,522.17 | ▼ close $9,556.16 vs 09:30 $9,778.16 (session -150.76) | 16:00 close · cash $14,522.17 · equity $9,556.16 vs 09:30 $9,778.16 (-222.00; session marks -150.76) · 8 name(s) marked open→close (per-name table). AUTL×246 09:30 $2.47 → close $2.41 +14.76; CRDL×315 09:30 $1.93 → close $1.86 +22.05; CRSP×10 09:30 $59.72 → close $59.50 +2.20; FUTU×5 09:30 $115.18 → close $123.64 -42.30; GMAB×18 09:30 $33.36 → close $33.45 -1.62; ENHA×356 09:30 $1.71 → close $1.72 -3.56; CAN×2070 09:30 $0.29 → close $0.35 -126.27; PRQR×267 09:30 $2.28 → close $2.34 -16.02 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,522.17 | ▼ 09:30 equity $9,516.61 vs yday $9,556.16 (-39.55) | 09:30 open · cash $14,522.17 (unchanged overnight, no fees) · equity $9,516.61 vs prior close $9,556.16 (-39.55) · 8 name(s) re-marked at the open (per-name table). AUTL×246 yday $2.41 → 09:30 $2.40 +2.46; CRDL×315 yday $1.86 → 09:30 $1.88 -6.30; CRSP×10 yday $59.50 → 09:30 $58.75 +7.50; FUTU×5 yday $123.64 → 09:30 $121.00 +13.20; GMAB×18 yday $33.45 → 09:30 $32.82 +11.34; ENHA×356 yday $1.72 → 09:30 $1.74 -7.12; CAN×2070 yday $0.35 → 09:30 $0.38 -57.96; PRQR×267 yday $2.34 → 09:30 $2.35 -2.67 | — |
| 2026-08-24 09:30 ET | **COVER** | `AUTL` | 246 | $2.40 | $3.17 | $+10.81 | $13,928.60 | ▲ +10.81 after sell → book $9,513.44; vs 09:30 mark -3.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `CRDL` | 315 | $1.88 | $4.06 | $+7.54 | $13,332.34 | ▲ +7.54 after sell → book $9,509.38; vs 09:30 mark -4.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `CRSP` | 10 | $58.75 | $2.02 | $+5.62 | $12,742.82 | ▲ +5.62 after sell → book $9,507.36; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **COVER** | `FUTU` | 5 | $121.00 | $2.00 | $-33.15 | $12,135.81 | ▼ -33.15 after sell → book $9,505.35; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **COVER** | `GMAB` | 18 | $32.82 | $2.04 | $+5.60 | $11,543.01 | ▲ +5.60 after sell → book $9,503.31; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `ENHA` | 356 | $1.74 | $4.59 | $-19.95 | $10,918.98 | ▼ -19.95 after sell → book $9,498.72; vs 09:30 mark -4.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `CAN` | 2070 | $0.38 | $14.14 | $-211.03 | $10,112.03 | ▼ -211.03 after sell → book $9,484.58; vs 09:30 mark -14.14 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **COVER** | `PRQR` | 267 | $2.35 | $3.44 | $-25.65 | $9,481.13 | ▼ -25.65 after sell → book $9,481.13; vs 09:30 mark -3.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,481.13 | ▲ close $9,481.13 vs 09:30 $9,516.61 (session +0.00) | 16:00 close · cash $9,481.13 · no lots left · equity $9,481.13. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,481.13 | ▲ 09:30 equity $9,481.13 vs yday $9,481.13 (+0.00) | 09:30 open · cash $9,481.13 · no holdings · equity $9,481.13 vs prior close $9,481.13 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **SHORT** | `MOS` | 24 | $23.77 | $2.10 | — | $10,049.52 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=+13.0; leftover $592.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **SHORT** | `OCUL` | 53 | $10.98 | $2.18 | — | $10,629.27 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+1.2; leftover $592.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **SHORT** | `INSP` | 9 | $61.19 | $2.05 | — | $11,177.93 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+7.4; leftover $592.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **SHORT** | `RZLT` | 119 | $4.94 | $2.39 | — | $11,763.40 | — | last bar red; gate last_red=True; list flatten; ret5=+7.1; leftover $592.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `HCA` | 1 | $426.97 | $2.02 | — | $12,188.34 | — | last bar red; gate last_red=True; list flatten; ret5=+6.0; leftover $592.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `CAPR` | 81 | $7.25 | $2.27 | — | $12,773.32 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $592.57 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `PUSA` | 155 | $3.80 | $2.51 | — | $13,359.81 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $592.57 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `CYPH` | 379 | $1.56 | $4.98 | — | $13,946.07 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $592.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,946.07 | ▼ close $9,333.42 vs 09:30 $9,481.13 (session -127.20) | 16:00 close · cash $13,946.07 · equity $9,333.42 vs 09:30 $9,481.13 (-147.71; session marks -127.20) · 8 name(s) marked open→close (per-name table). MOS×24 09:30 $23.77 → close $24.27 -12.00; OCUL×53 09:30 $10.98 → close $10.88 +5.30; INSP×9 09:30 $61.19 → close $61.07 +1.08; RZLT×119 09:30 $4.94 → close $5.01 -8.33; HCA×1 09:30 $426.97 → close $428.76 -1.79; CAPR×81 09:30 $7.25 → close $8.29 -84.24; PUSA×155 09:30 $3.80 → close $3.78 +3.10; CYPH×379 09:30 $1.56 → close $1.64 -30.32 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,946.07 | ▲ 09:30 equity $9,341.41 vs yday $9,333.42 (+7.99) | 09:30 open · cash $13,946.07 (unchanged overnight, no fees) · equity $9,341.41 vs prior close $9,333.42 (+7.99) · 8 name(s) re-marked at the open (per-name table). MOS×24 yday $24.27 → 09:30 $24.84 -13.68; OCUL×53 yday $10.88 → 09:30 $10.79 +4.77; INSP×9 yday $61.07 → 09:30 $60.07 +9.00; RZLT×119 yday $5.01 → 09:30 $5.01 -0.00; HCA×1 yday $428.76 → 09:30 $427.50 +1.26; CAPR×81 yday $8.29 → 09:30 $8.29 -0.00; PUSA×155 yday $3.78 → 09:30 $3.83 -8.53; CYPH×379 yday $1.64 → 09:30 $1.60 +15.16 | — |
| 2026-08-26 09:30 ET | **COVER** | `MOS` | 24 | $24.84 | $2.06 | $-29.84 | $13,347.85 | ▼ -29.84 after sell → book $9,339.35; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **COVER** | `RZLT` | 119 | $5.01 | $2.35 | $-13.07 | $12,749.31 | ▼ -13.07 after sell → book $9,337.00; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **COVER** | `HCA` | 1 | $427.50 | $1.99 | $-4.55 | $12,319.82 | ▼ -4.55 after sell → book $9,335.01; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **COVER** | `CAPR` | 81 | $8.29 | $2.23 | $-88.75 | $11,646.10 | ▼ -88.75 after sell → book $9,332.77; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **COVER** | `PUSA` | 155 | $3.83 | $2.46 | $-10.39 | $11,049.22 | ▼ -10.39 after sell → book $9,330.32; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `CYPH` | 379 | $1.60 | $4.89 | $-25.03 | $10,437.93 | ▼ -25.03 after sell → book $9,325.43; vs 09:30 mark -4.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SHORT** | `FLNC` | 69 | $11.12 | $2.24 | — | $11,202.97 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $777.12 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `AVEX` | 44 | $17.51 | $2.16 | — | $11,971.25 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $777.12 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `AXTI` | 11 | $65.34 | $2.06 | — | $12,687.92 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $777.12 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `INDP` | 712 | $1.09 | $9.33 | — | $13,454.67 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+17.0; leftover $777.12 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `NVTS` | 61 | $12.60 | $2.21 | — | $14,221.06 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-5.5; leftover $777.12 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `IRDM` | 16 | $46.96 | $2.08 | — | $14,970.34 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-3.9; leftover $777.12 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,970.34 | ▼ close $9,215.12 vs 09:30 $9,341.41 (session -90.22) | 16:00 close · cash $14,970.34 · equity $9,215.12 vs 09:30 $9,341.41 (-126.29; session marks -90.22) · 8 name(s) marked open→close (per-name table). OCUL×53 09:30 $10.79 → close $10.77 +1.06; INSP×9 09:30 $60.07 → close $61.80 -15.57; FLNC×69 09:30 $11.12 → close $11.08 +2.76; AVEX×44 09:30 $17.51 → close $18.34 -36.52; AXTI×11 09:30 $65.34 → close $65.18 +1.76; INDP×712 09:30 $1.09 → close $1.14 -35.60; NVTS×61 09:30 $12.60 → close $12.67 -4.27; IRDM×16 09:30 $46.96 → close $47.20 -3.84 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,970.34 | ▼ 09:30 equity $9,101.05 vs yday $9,215.12 (-114.07) | 09:30 open · cash $14,970.34 (unchanged overnight, no fees) · equity $9,101.05 vs prior close $9,215.12 (-114.07) · 8 name(s) re-marked at the open (per-name table). OCUL×53 yday $10.77 → 09:30 $10.63 +7.42; INSP×9 yday $61.80 → 09:30 $62.10 -2.70; FLNC×69 yday $11.08 → 09:30 $11.52 -30.36; AVEX×44 yday $18.34 → 09:30 $18.43 -3.96; AXTI×11 yday $65.18 → 09:30 $70.30 -56.32; INDP×712 yday $1.14 → 09:30 $1.13 +7.12; NVTS×61 yday $12.67 → 09:30 $13.18 -31.11; IRDM×16 yday $47.20 → 09:30 $47.46 -4.16 | — |
| 2026-08-27 09:30 ET | **COVER** | `OCUL` | 53 | $10.63 | $2.15 | $+14.22 | $14,404.80 | ▲ +14.22 after sell → book $9,098.90; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `INSP` | 9 | $62.10 | $2.02 | $-12.26 | $13,843.88 | ▼ -12.26 after sell → book $9,096.88; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `FLNC` | 69 | $11.52 | $2.20 | $-32.04 | $13,046.80 | ▼ -32.04 after sell → book $9,094.68; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `AVEX` | 44 | $18.43 | $2.12 | $-44.77 | $12,233.76 | ▼ -44.77 after sell → book $9,092.56; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `AXTI` | 11 | $70.30 | $2.02 | $-58.65 | $11,458.44 | ▼ -58.65 after sell → book $9,090.54; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `INDP` | 712 | $1.13 | $9.18 | $-47.00 | $10,644.69 | ▼ -47.00 after sell → book $9,081.35; vs 09:30 mark -9.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `NVTS` | 61 | $13.18 | $2.17 | $-39.77 | $9,838.54 | ▼ -39.77 after sell → book $9,079.18; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `IRDM` | 16 | $47.46 | $2.04 | $-12.12 | $9,077.14 | ▼ -12.12 after sell → book $9,077.14; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SHORT** | `MOS` | 23 | $24.00 | $2.09 | — | $9,627.05 | — | last bar red; gate last_red=True; list flatten; ret5=+8.7; leftover $567.32 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **SHORT** | `ACMR` | 6 | $81.65 | $2.04 | — | $10,114.91 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=+2.0; leftover $567.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 7 | $74.54 | $2.05 | — | $10,634.64 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-0.1; leftover $567.32 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 10 | $55.25 | $2.06 | — | $11,185.09 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=+2.1; leftover $567.32 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `DLO` | 37 | $15.33 | $2.14 | — | $11,750.16 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=+7.4; leftover $567.32 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `LRCX` | 1 | $318.88 | $2.02 | — | $12,067.02 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=+1.9; leftover $567.32 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `NVDA` | 2 | $222.86 | $2.03 | — | $12,510.71 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-3.6; leftover $567.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,510.71 | ▲ close $9,065.86 vs 09:30 $9,101.05 (session +3.14) | 16:00 close · cash $12,510.71 · equity $9,065.86 vs 09:30 $9,101.05 (-35.19; session marks +3.14) · 7 name(s) marked open→close (per-name table). MOS×23 09:30 $24.00 → close $23.76 +5.52; ACMR×6 09:30 $81.65 → close $80.49 +6.96; MT×7 09:30 $74.54 → close $74.63 -0.63; TX×10 09:30 $55.25 → close $55.83 -5.80; DLO×37 09:30 $15.33 → close $15.14 +7.03; LRCX×1 09:30 $318.88 → close $318.58 +0.30; NVDA×2 09:30 $222.86 → close $227.98 -10.24 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,510.71 | ▼ 09:30 equity $9,062.03 vs yday $9,065.86 (-3.83) | 09:30 open · cash $12,510.71 (unchanged overnight, no fees) · equity $9,062.03 vs prior close $9,065.86 (-3.83) · 7 name(s) re-marked at the open (per-name table). MOS×23 yday $23.76 → 09:30 $23.95 -4.37; ACMR×6 yday $80.49 → 09:30 $79.27 +7.32; MT×7 yday $74.63 → 09:30 $75.39 -5.32; TX×10 yday $55.83 → 09:30 $55.97 -1.40; DLO×37 yday $15.14 → 09:30 $15.19 -1.85; LRCX×1 yday $318.58 → 09:30 $318.03 +0.55; NVDA×2 yday $227.98 → 09:30 $227.36 +1.24 | — |
| 2026-08-28 09:30 ET | **COVER** | `ACMR` | 6 | $79.27 | $2.01 | $+10.23 | $12,033.08 | ▲ +10.23 after sell → book $9,060.02; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `MT` | 7 | $75.39 | $2.01 | $-10.01 | $11,503.34 | ▼ -10.01 after sell → book $9,058.01; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `TX` | 10 | $55.97 | $2.02 | $-11.28 | $10,941.62 | ▼ -11.28 after sell → book $9,055.99; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `DLO` | 37 | $15.19 | $2.10 | $+0.94 | $10,377.49 | ▲ +0.94 after sell → book $9,053.89; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `LRCX` | 1 | $318.03 | $1.99 | $-3.16 | $10,057.47 | ▼ -3.16 after sell → book $9,051.90; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `NVDA` | 2 | $227.36 | $2.00 | $-13.02 | $9,600.75 | ▼ -13.02 after sell → book $9,049.90; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SEDG` | 19 | $32.90 | $2.08 | — | $10,223.77 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $646.42 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `GRRR` | 41 | $15.66 | $2.15 | — | $10,863.68 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $646.42 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `URBN` | 8 | $79.42 | $2.05 | — | $11,496.99 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $646.42 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SAFX` | 1771 | $0.36 | $12.10 | — | $12,131.30 | — | last bar red; gate last_red=True; list probable,yday_gainer; ret5=+7.6; leftover $646.42 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 2 | $252.24 | $2.03 | — | $12,633.75 | — | last bar red; gate last_red=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $646.42 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `XPOF` | 120 | $5.38 | $2.40 | — | $13,276.95 | — | last bar red; gate last_red=True; list probable,yday_gainer; ret5=+6.5; leftover $646.42 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `BHVN` | 40 | $15.88 | $2.15 | — | $13,910.01 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+19.4; leftover $646.42 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,910.01 | ▲ close $9,135.48 vs 09:30 $9,062.03 (session +110.54) | 16:00 close · cash $13,910.01 · equity $9,135.48 vs 09:30 $9,062.03 (+73.45; session marks +110.54) · 8 name(s) marked open→close (per-name table). MOS×23 09:30 $23.95 → close $23.60 +8.05; SEDG×19 09:30 $32.90 → close $31.41 +28.31; GRRR×41 09:30 $15.66 → close $14.41 +51.25; URBN×8 09:30 $79.42 → close $81.09 -13.36; SAFX×1771 09:30 $0.36 → close $0.36 +10.63; SIMO×2 09:30 $252.24 → close $245.81 +12.86; XPOF×120 09:30 $5.38 → close $5.43 -6.00; BHVN×40 09:30 $15.88 → close $15.41 +18.80 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,910.01 | ▲ 09:30 equity $9,139.95 vs yday $9,135.48 (+4.47) | 09:30 open · cash $13,910.01 (unchanged overnight, no fees) · equity $9,139.95 vs prior close $9,135.48 (+4.47) · 8 name(s) re-marked at the open (per-name table). MOS×23 yday $23.60 → 09:30 $23.68 -1.84; SEDG×19 yday $31.41 → 09:30 $31.15 +4.94; GRRR×41 yday $14.41 → 09:30 $14.44 -1.23; URBN×8 yday $81.09 → 09:30 $80.44 +5.20; SAFX×1771 yday $0.36 → 09:30 $0.36 -5.31; SIMO×2 yday $245.81 → 09:30 $247.05 -2.48; XPOF×120 yday $5.43 → 09:30 $5.37 +7.20; BHVN×40 yday $15.41 → 09:30 $15.46 -2.00 | — |
| 2026-08-31 09:30 ET | **COVER** | `MOS` | 23 | $23.68 | $2.06 | $+3.21 | $13,363.31 | ▲ +3.21 after sell → book $9,137.90; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SEDG` | 19 | $31.15 | $2.05 | $+29.12 | $12,769.41 | ▲ +29.12 after sell → book $9,135.85; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `GRRR` | 41 | $14.44 | $2.11 | $+45.76 | $12,175.26 | ▲ +45.76 after sell → book $9,133.74; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `URBN` | 8 | $80.44 | $2.01 | $-12.23 | $11,529.72 | ▼ -12.23 after sell → book $9,131.72; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SAFX` | 1771 | $0.36 | $11.72 | $-18.51 | $10,876.90 | ▼ -18.51 after sell → book $9,120.00; vs 09:30 mark -11.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SIMO` | 2 | $247.05 | $2.00 | $+6.35 | $10,380.80 | ▲ +6.35 after sell → book $9,118.00; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `XPOF` | 120 | $5.37 | $2.35 | $-3.55 | $9,734.05 | ▼ -3.55 after sell → book $9,115.65; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `BHVN` | 40 | $15.46 | $2.11 | $+12.54 | $9,113.54 | ▲ +12.54 after sell → book $9,113.54; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.54 | ▲ close $9,113.54 vs 09:30 $9,139.95 (session +0.00) | 16:00 close · cash $9,113.54 · no lots left · equity $9,113.54. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.54 | ▲ 09:30 equity $9,113.54 vs yday $9,113.54 (+0.00) | 09:30 open · cash $9,113.54 · no holdings · equity $9,113.54 vs prior close $9,113.54 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.54 | ▲ close $9,113.54 vs 09:30 $9,113.54 (session +0.00) | 16:00 close · cash $9,113.54 · no lots left · equity $9,113.54. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.54 | ▲ 09:30 equity $9,113.54 vs yday $9,113.54 (+0.00) | 09:30 open · cash $9,113.54 · no holdings · equity $9,113.54 vs prior close $9,113.54 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.54 | ▲ close $9,113.54 vs 09:30 $9,113.54 (session +0.00) | 16:00 close · cash $9,113.54 · no lots left · equity $9,113.54. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.54 | ▲ 09:30 equity $9,113.54 vs yday $9,113.54 (+0.00) | 09:30 open · cash $9,113.54 · no holdings · equity $9,113.54 vs prior close $9,113.54 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **SHORT** | `CRK` | 36 | $15.45 | $2.13 | — | $9,667.61 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $569.60 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **SHORT** | `MRNA` | 3 | $145.94 | $2.03 | — | $10,103.41 | — | last bar red; gate last_red=True; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $569.60 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `EIX` | 10 | $55.42 | $2.06 | — | $10,655.56 | — | last bar red; gate last_red=True; list probable,yday_gainer; ret5=-25.9; leftover $569.60 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `SAFX` | 1510 | $0.38 | $10.50 | — | $11,214.33 | — | last bar red; gate last_red=True; list probable,yday_gainer; ret5=-2.3; leftover $569.60 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `FRVO` | 31 | $18.28 | $2.12 | — | $11,778.89 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+16.5; leftover $569.60 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `DEFT` | 876 | $0.65 | $8.49 | — | $12,339.80 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+10.2; leftover $569.60 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `GMRS` | 44 | $12.83 | $2.16 | — | $12,902.16 | — | last bar red; gate last_red=True; list yday_gainer; ret5=-0.2; leftover $569.60 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `KLRA` | 35 | $15.95 | $2.13 | — | $13,458.28 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-14.0; leftover $569.60 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,458.28 | ▼ close $9,070.47 vs 09:30 $9,113.54 (session -11.45) | 16:00 close · cash $13,458.28 · equity $9,070.47 vs 09:30 $9,113.54 (-43.07; session marks -11.45) · 8 name(s) marked open→close (per-name table). CRK×36 09:30 $15.45 → close $14.95 +18.00; MRNA×3 09:30 $145.94 → close $148.87 -8.78; EIX×10 09:30 $55.42 → close $56.30 -8.80; SAFX×1510 09:30 $0.38 → close $0.38 -3.02; FRVO×31 09:30 $18.28 → close $17.16 +34.72; DEFT×876 09:30 $0.65 → close $0.68 -25.40; GMRS×44 09:30 $12.83 → close $13.41 -25.52; KLRA×35 09:30 $15.95 → close $15.74 +7.35 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,458.28 | ▼ 09:30 equity $9,058.17 vs yday $9,070.47 (-12.30) | 09:30 open · cash $13,458.28 (unchanged overnight, no fees) · equity $9,058.17 vs prior close $9,070.47 (-12.30) · 8 name(s) re-marked at the open (per-name table). CRK×36 yday $14.95 → 09:30 $15.00 -1.80; MRNA×3 yday $148.87 → 09:30 $153.62 -14.25; EIX×10 yday $56.30 → 09:30 $55.79 +5.10; SAFX×1510 yday $0.38 → 09:30 $0.38 +1.51; FRVO×31 yday $17.16 → 09:30 $17.27 -3.41; DEFT×876 yday $0.68 → 09:30 $0.69 -9.64; GMRS×44 yday $13.41 → 09:30 $13.29 +5.28; KLRA×35 yday $15.74 → 09:30 $15.60 +4.90 | — |
| 2026-09-04 09:30 ET | **COVER** | `CRK` | 36 | $15.00 | $2.10 | $+11.97 | $12,916.18 | ▲ +11.97 after sell → book $9,056.07; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `MRNA` | 3 | $153.62 | $2.00 | $-27.05 | $12,453.32 | ▼ -27.05 after sell → book $9,054.07; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `EIX` | 10 | $55.79 | $2.02 | $-7.78 | $11,893.40 | ▼ -7.78 after sell → book $9,052.05; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `SAFX` | 1510 | $0.38 | $10.24 | $-22.25 | $11,312.38 | ▼ -22.25 after sell → book $9,041.81; vs 09:30 mark -10.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `FRVO` | 31 | $17.27 | $2.08 | $+27.11 | $10,774.93 | ▲ +27.11 after sell → book $9,039.73; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `DEFT` | 876 | $0.69 | $8.67 | $-52.21 | $10,161.82 | ▼ -52.21 after sell → book $9,031.06; vs 09:30 mark -8.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `GMRS` | 44 | $13.29 | $2.12 | $-24.52 | $9,574.94 | ▼ -24.52 after sell → book $9,028.94; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `KLRA` | 35 | $15.60 | $2.10 | $+8.02 | $9,026.84 | ▲ +8.02 after sell → book $9,026.84; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `CABA` | 163 | $3.46 | $2.53 | — | $9,588.29 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $564.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **SHORT** | `ALEC` | 223 | $2.52 | $2.94 | — | $10,147.31 | — | last bar red; gate last_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $564.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `BHC` | 84 | $6.71 | $2.28 | — | $10,708.67 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $564.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `BMEA` | 296 | $1.90 | $3.89 | — | $11,267.18 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $564.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `OABI` | 118 | $4.78 | $2.39 | — | $11,828.83 | — | last bar red; gate last_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $564.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `OPK` | 354 | $1.59 | $4.65 | — | $12,387.04 | — | last bar red; gate last_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $564.18 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `VIR` | 49 | $11.31 | $2.17 | — | $12,939.05 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $564.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `ATRC` | 10 | $52.03 | $2.05 | — | $13,457.30 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $564.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,457.30 | ▲ close $9,026.63 vs 09:30 $9,058.17 (session +22.70) | 16:00 close · cash $13,457.30 · equity $9,026.63 vs 09:30 $9,058.17 (-31.54; session marks +22.70) · 8 name(s) marked open→close (per-name table). CABA×163 09:30 $3.46 → close $3.47 -1.63; ALEC×223 09:30 $2.52 → close $2.46 +13.38; BHC×84 09:30 $6.71 → close $6.56 +12.60; BMEA×296 09:30 $1.90 → close $2.03 -38.48; OABI×118 09:30 $4.78 → close $4.33 +53.10; OPK×354 09:30 $1.59 → close $1.64 -17.70; VIR×49 09:30 $11.31 → close $11.38 -3.67; ATRC×10 09:30 $52.03 → close $51.52 +5.10 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,457.30 | ▲ 09:30 equity $9,046.29 vs yday $9,026.63 (+19.66) | 09:30 open · cash $13,457.30 (unchanged overnight, no fees) · equity $9,046.29 vs prior close $9,026.63 (+19.66) · 8 name(s) re-marked at the open (per-name table). CABA×163 yday $3.47 → 09:30 $3.43 +6.52; ALEC×223 yday $2.46 → 09:30 $2.38 +17.84; BHC×84 yday $6.56 → 09:30 $6.57 -0.84; BMEA×296 yday $2.03 → 09:30 $2.00 +8.88; OABI×118 yday $4.33 → 09:30 $4.30 +3.54; OPK×354 yday $1.64 → 09:30 $1.63 +3.54; VIR×49 yday $11.38 → 09:30 $11.22 +8.08; ATRC×10 yday $51.52 → 09:30 $54.31 -27.90 | — |
| 2026-09-08 09:30 ET | **COVER** | `CABA` | 163 | $3.43 | $2.48 | $-0.12 | $12,895.73 | ▼ -0.12 after sell → book $9,043.81; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `ALEC` | 223 | $2.38 | $2.88 | $+25.40 | $12,362.11 | ▲ +25.40 after sell → book $9,040.93; vs 09:30 mark -2.88 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `BHC` | 84 | $6.57 | $2.24 | $+7.24 | $11,807.99 | ▲ +7.24 after sell → book $9,038.69; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `BMEA` | 296 | $2.00 | $3.82 | $-37.31 | $11,212.17 | ▼ -37.31 after sell → book $9,034.87; vs 09:30 mark -3.82 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **COVER** | `OABI` | 118 | $4.30 | $2.34 | $+51.91 | $10,702.43 | ▲ +51.91 after sell → book $9,032.53; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `OPK` | 354 | $1.63 | $4.57 | $-23.38 | $10,120.84 | ▼ -23.38 after sell → book $9,027.96; vs 09:30 mark -4.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `VIR` | 49 | $11.22 | $2.14 | $+0.10 | $9,568.93 | ▲ +0.10 after sell → book $9,025.83; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `ATRC` | 10 | $54.31 | $2.02 | $-26.87 | $9,023.81 | ▼ -26.87 after sell → book $9,023.81; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,023.81 | ▲ close $9,023.81 vs 09:30 $9,046.29 (session +0.00) | 16:00 close · cash $9,023.81 · no lots left · equity $9,023.81. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,023.81 | ▲ 09:30 equity $9,023.81 vs yday $9,023.81 (-0.00) | 09:30 open · cash $9,023.81 · no holdings · equity $9,023.81 vs prior close $9,023.81 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,023.81 | ▲ close $9,023.81 vs 09:30 $9,023.81 (session +0.00) | 16:00 close · cash $9,023.81 · no lots left · equity $9,023.81. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,023.81 | ▲ 09:30 equity $9,023.81 vs yday $9,023.81 (-0.00) | 09:30 open · cash $9,023.81 · no holdings · equity $9,023.81 vs prior close $9,023.81 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,023.81 | ▲ close $9,023.81 vs 09:30 $9,023.81 (session +0.00) | 16:00 close · cash $9,023.81 · no lots left · equity $9,023.81. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,023.81 | ▲ 09:30 equity $9,023.81 vs yday $9,023.81 (-0.00) | 09:30 open · cash $9,023.81 · no holdings · equity $9,023.81 vs prior close $9,023.81 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **SHORT** | `AMTX` | 276 | $2.04 | $3.63 | — | $9,583.22 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.9; leftover $563.99 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `LDI` | 663 | $0.85 | $7.76 | — | $10,139.01 | — | last bar red; gate last_red=True; list probable,yday_gainer; 🔵; ret5=-12.5; leftover $563.99 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `BAK` | 266 | $2.12 | $3.50 | — | $10,699.42 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+19.4; leftover $563.99 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,699.42 | ▲ close $9,037.78 vs 09:30 $9,023.81 (session +28.87) | 16:00 close · cash $10,699.42 · equity $9,037.78 vs 09:30 $9,023.81 (+13.97; session marks +28.87) · 3 name(s) marked open→close (per-name table). AMTX×276 09:30 $2.04 → close $2.01 +8.28; LDI×663 09:30 $0.85 → close $0.83 +9.95; BAK×266 09:30 $2.12 → close $2.08 +10.64 | — |

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
| 2026-08-27 | `ASML` | cash | leftover split 567.32 < 1 share @ 1746.53 |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ELMT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FOX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BEP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VLRS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `XLAB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CABA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CNTB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HAS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ZJYL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `ORCL` | no_price | no 09:30 open |
| 2026-09-11 | `DBI` | no_price | no 09:30 open |
| 2026-09-11 | `TYRA` | no_price | no 09:30 open |
| 2026-09-11 | `WLTH` | no_price | no 09:30 open |
| 2026-09-11 | `BNC` | no_price | no 09:30 open |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `AMTX` | 276 | 2026-09-11 @ $2.04 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.9; leftover $563.99 |
| `LDI` | 663 | 2026-09-11 @ $0.85 | last bar red; gate last_red=True; list probable,yday_gainer; 🔵; ret5=-12.5; leftover $563.99 |
| `BAK` | 266 | 2026-09-11 @ $2.12 | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+19.4; leftover $563.99 |
