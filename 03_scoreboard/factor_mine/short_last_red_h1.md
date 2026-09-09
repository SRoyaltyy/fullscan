# Factor mine action — `short_last_red_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · last bar red

Cash book **-9.47%** ($9,053) · signal-only (no cash/fees) was -16.80%. Starts YES **0/18**. Fills 160 · skips 57 · realized $-947.35.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,052.69.

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
| 2026-08-20 | `WYFI` | 29 | — | $21.40 | +0.00 | $21.16 | +6.96 | +6.96 | -0.00 | +6.96 |
| 2026-08-20 | `TOYO` | 140 | — | $4.43 | +0.00 | $4.51 | -11.90 | -11.90 | -0.00 | -11.90 |
| 2026-08-20 | `DVLT` | 2071 | — | $0.30 | +0.00 | $0.32 | -41.42 | -41.42 | -0.00 | -41.42 |
| 2026-08-20 | `SAFX` | 1755 | — | $0.35 | +0.00 | $0.34 | +19.30 | +19.30 | -0.00 | +19.30 |
| 2026-08-20 | `AAP` | 13 | — | $46.85 | +0.00 | $42.39 | +57.98 | +57.98 | -0.00 | +57.98 |
| 2026-08-20 | `AEG` | 68 | — | $9.01 | +0.00 | $9.01 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-08-21 | `BHP` | 6 | $93.63 | $95.72 | -12.54 | — | +0.00 | -12.54 | -28.26 | — |
| 2026-08-21 | `MRVI` | 83 | $8.29 | $8.28 | +0.83 | — | +0.00 | +0.83 | -69.72 | — |
| 2026-08-21 | `WYFI` | 29 | $21.16 | $21.54 | -11.02 | — | +0.00 | -11.02 | -4.06 | — |
| 2026-08-21 | `TOYO` | 140 | $4.51 | $4.68 | -23.10 | — | +0.00 | -23.10 | -35.00 | — |
| 2026-08-21 | `DVLT` | 2071 | $0.32 | $0.31 | +20.71 | — | +0.00 | +20.71 | -20.71 | — |
| 2026-08-21 | `SAFX` | 1755 | $0.34 | $0.35 | -12.28 | — | +0.00 | -12.28 | +7.02 | — |
| 2026-08-21 | `AAP` | 13 | $42.39 | $42.41 | -0.26 | — | +0.00 | -0.26 | +57.72 | — |
| 2026-08-21 | `AEG` | 68 | $9.01 | $9.04 | -2.04 | — | +0.00 | -2.04 | -2.04 | — |
| 2026-08-21 | `AUTL` | 247 | — | $2.47 | +0.00 | $2.41 | +14.82 | +14.82 | -0.00 | +14.82 |
| 2026-08-21 | `CRDL` | 316 | — | $1.93 | +0.00 | $1.86 | +22.12 | +22.12 | -0.00 | +22.12 |
| 2026-08-21 | `CRSP` | 10 | — | $59.72 | +0.00 | $59.50 | +2.20 | +2.20 | -0.00 | +2.20 |
| 2026-08-21 | `FUTU` | 5 | — | $115.18 | +0.00 | $123.64 | -42.30 | -42.30 | -0.00 | -42.30 |
| 2026-08-21 | `GMAB` | 18 | — | $33.36 | +0.00 | $33.45 | -1.62 | -1.62 | -0.00 | -1.62 |
| 2026-08-21 | `ENHA` | 357 | — | $1.71 | +0.00 | $1.72 | -3.57 | -3.57 | -0.00 | -3.57 |
| 2026-08-21 | `CAN` | 2077 | — | $0.29 | +0.00 | $0.35 | -126.70 | -126.70 | -0.00 | -126.70 |
| 2026-08-21 | `PRQR` | 267 | — | $2.28 | +0.00 | $2.34 | -16.02 | -16.02 | -0.00 | -16.02 |
| 2026-08-24 | `AUTL` | 247 | $2.41 | $2.40 | +2.47 | — | +0.00 | +2.47 | +17.29 | — |
| 2026-08-24 | `CRDL` | 316 | $1.86 | $1.88 | -6.32 | — | +0.00 | -6.32 | +15.80 | — |
| 2026-08-24 | `CRSP` | 10 | $59.50 | $58.75 | +7.50 | — | +0.00 | +7.50 | +9.70 | — |
| 2026-08-24 | `FUTU` | 5 | $123.64 | $121.00 | +13.20 | — | +0.00 | +13.20 | -29.10 | — |
| 2026-08-24 | `GMAB` | 18 | $33.45 | $32.82 | +11.34 | — | +0.00 | +11.34 | +9.72 | — |
| 2026-08-24 | `ENHA` | 357 | $1.72 | $1.74 | -7.14 | — | +0.00 | -7.14 | -10.71 | — |
| 2026-08-24 | `CAN` | 2077 | $0.35 | $0.38 | -58.16 | — | +0.00 | -58.16 | -184.85 | — |
| 2026-08-24 | `PRQR` | 267 | $2.34 | $2.35 | -2.67 | — | +0.00 | -2.67 | -18.69 | — |
| 2026-08-25 | `MOS` | 25 | — | $23.77 | +0.00 | $24.27 | -12.50 | -12.50 | -0.00 | -12.50 |
| 2026-08-25 | `OCUL` | 54 | — | $10.98 | +0.00 | $10.88 | +5.40 | +5.40 | -0.00 | +5.40 |
| 2026-08-25 | `INSP` | 9 | — | $61.19 | +0.00 | $61.07 | +1.08 | +1.08 | -0.00 | +1.08 |
| 2026-08-25 | `RZLT` | 120 | — | $4.94 | +0.00 | $5.01 | -8.40 | -8.40 | -0.00 | -8.40 |
| 2026-08-25 | `HCA` | 1 | — | $426.97 | +0.00 | $428.76 | -1.79 | -1.79 | -0.00 | -1.79 |
| 2026-08-25 | `CAPR` | 82 | — | $7.25 | +0.00 | $8.29 | -85.28 | -85.28 | -0.00 | -85.28 |
| 2026-08-25 | `PUSA` | 156 | — | $3.80 | +0.00 | $3.78 | +3.12 | +3.12 | -0.00 | +3.12 |
| 2026-08-25 | `CYPH` | 381 | — | $1.56 | +0.00 | $1.64 | -30.48 | -30.48 | -0.00 | -30.48 |
| 2026-08-26 | `MOS` | 25 | $24.27 | $24.84 | -14.25 | — | +0.00 | -14.25 | -26.75 | — |
| 2026-08-26 | `OCUL` | 54 | $10.88 | $10.79 | +4.86 | $10.77 | +1.08 | +5.94 | +10.26 | +11.34 |
| 2026-08-26 | `INSP` | 9 | $61.07 | $60.07 | +9.00 | $61.80 | -15.57 | -6.57 | +10.08 | -5.49 |
| 2026-08-26 | `RZLT` | 120 | $5.01 | $5.01 | +0.00 | — | +0.00 | +0.00 | -8.40 | — |
| 2026-08-26 | `HCA` | 1 | $428.76 | $427.50 | +1.26 | — | +0.00 | +1.26 | -0.53 | — |
| 2026-08-26 | `CAPR` | 82 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | -85.28 | — |
| 2026-08-26 | `PUSA` | 156 | $3.78 | $3.83 | -8.58 | — | +0.00 | -8.58 | -5.46 | — |
| 2026-08-26 | `CYPH` | 381 | $1.64 | $1.60 | +15.24 | — | +0.00 | +15.24 | -15.24 | — |
| 2026-08-26 | `FLNC` | 70 | — | $11.12 | +0.00 | $11.08 | +2.80 | +2.80 | -0.00 | +2.80 |
| 2026-08-26 | `AVEX` | 44 | — | $17.51 | +0.00 | $18.34 | -36.52 | -36.52 | -0.00 | -36.52 |
| 2026-08-26 | `AXTI` | 11 | — | $65.34 | +0.00 | $65.18 | +1.76 | +1.76 | -0.00 | +1.76 |
| 2026-08-26 | `INDP` | 715 | — | $1.09 | +0.00 | $1.14 | -35.75 | -35.75 | -0.00 | -35.75 |
| 2026-08-26 | `NVTS` | 61 | — | $12.60 | +0.00 | $12.67 | -4.27 | -4.27 | -0.00 | -4.27 |
| 2026-08-26 | `IRDM` | 16 | — | $46.96 | +0.00 | $47.20 | -3.84 | -3.84 | -0.00 | -3.84 |
| 2026-08-27 | `OCUL` | 54 | $10.77 | $10.63 | +7.56 | — | +0.00 | +7.56 | +18.90 | — |
| 2026-08-27 | `INSP` | 9 | $61.80 | $62.10 | -2.70 | — | +0.00 | -2.70 | -8.19 | — |
| 2026-08-27 | `FLNC` | 70 | $11.08 | $11.52 | -30.80 | — | +0.00 | -30.80 | -28.00 | — |
| 2026-08-27 | `AVEX` | 44 | $18.34 | $18.43 | -3.96 | — | +0.00 | -3.96 | -40.48 | — |
| 2026-08-27 | `AXTI` | 11 | $65.18 | $70.30 | -56.32 | — | +0.00 | -56.32 | -54.56 | — |
| 2026-08-27 | `INDP` | 715 | $1.14 | $1.13 | +7.15 | — | +0.00 | +7.15 | -28.60 | — |
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
| 2026-08-28 | `SAFX` | 1776 | — | $0.36 | +0.00 | $0.36 | +10.66 | +10.66 | -0.00 | +10.66 |
| 2026-08-28 | `SIMO` | 2 | — | $252.24 | +0.00 | $245.81 | +12.86 | +12.86 | -0.00 | +12.86 |
| 2026-08-28 | `XPOF` | 120 | — | $5.38 | +0.00 | $5.43 | -6.00 | -6.00 | -0.00 | -6.00 |
| 2026-08-28 | `BHVN` | 40 | — | $15.88 | +0.00 | $15.41 | +18.80 | +18.80 | -0.00 | +18.80 |
| 2026-08-31 | `MOS` | 23 | $23.60 | $23.68 | -1.84 | — | +0.00 | -1.84 | +7.36 | — |
| 2026-08-31 | `SEDG` | 19 | $31.41 | $31.15 | +4.94 | — | +0.00 | +4.94 | +33.25 | — |
| 2026-08-31 | `GRRR` | 41 | $14.41 | $14.44 | -1.23 | — | +0.00 | -1.23 | +50.02 | — |
| 2026-08-31 | `URBN` | 8 | $81.09 | $80.44 | +5.20 | — | +0.00 | +5.20 | -8.16 | — |
| 2026-08-31 | `SAFX` | 1776 | $0.36 | $0.36 | -5.33 | — | +0.00 | -5.33 | +5.33 | — |
| 2026-08-31 | `SIMO` | 2 | $245.81 | $247.05 | -2.48 | — | +0.00 | -2.48 | +10.38 | — |
| 2026-08-31 | `XPOF` | 120 | $5.43 | $5.37 | +7.20 | — | +0.00 | +7.20 | +1.20 | — |
| 2026-08-31 | `BHVN` | 40 | $15.41 | $15.46 | -2.00 | — | +0.00 | -2.00 | +16.80 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `CRK` | 36 | — | $15.45 | +0.00 | $14.95 | +18.00 | +18.00 | -0.00 | +18.00 |
| 2026-09-03 | `MRNA` | 3 | — | $145.94 | +0.00 | $148.87 | -8.78 | -8.78 | -0.00 | -8.78 |
| 2026-09-03 | `EIX` | 10 | — | $55.42 | +0.00 | $56.30 | -8.80 | -8.80 | -0.00 | -8.80 |
| 2026-09-03 | `SAFX` | 1515 | — | $0.38 | +0.00 | $0.38 | -3.03 | -3.03 | -0.00 | -3.03 |
| 2026-09-03 | `FRVO` | 31 | — | $18.28 | +0.00 | $17.16 | +34.72 | +34.72 | -0.00 | +34.72 |
| 2026-09-03 | `DEFT` | 879 | — | $0.65 | +0.00 | $0.68 | -25.49 | -25.49 | -0.00 | -25.49 |
| 2026-09-03 | `GMRS` | 44 | — | $12.83 | +0.00 | $13.41 | -25.52 | -25.52 | -0.00 | -25.52 |
| 2026-09-03 | `KLRA` | 35 | — | $15.95 | +0.00 | $15.74 | +7.35 | +7.35 | -0.00 | +7.35 |
| 2026-09-04 | `CRK` | 36 | $14.95 | $15.00 | -1.80 | — | +0.00 | -1.80 | +16.20 | — |
| 2026-09-04 | `MRNA` | 3 | $148.87 | $153.62 | -14.25 | — | +0.00 | -14.25 | -23.03 | — |
| 2026-09-04 | `EIX` | 10 | $56.30 | $55.79 | +5.10 | — | +0.00 | +5.10 | -3.70 | — |
| 2026-09-04 | `SAFX` | 1515 | $0.38 | $0.38 | +1.52 | — | +0.00 | +1.52 | -1.52 | — |
| 2026-09-04 | `FRVO` | 31 | $17.16 | $17.27 | -3.41 | — | +0.00 | -3.41 | +31.31 | — |
| 2026-09-04 | `DEFT` | 879 | $0.68 | $0.69 | -9.67 | — | +0.00 | -9.67 | -35.16 | — |
| 2026-09-04 | `GMRS` | 44 | $13.41 | $13.29 | +5.28 | — | +0.00 | +5.28 | -20.24 | — |
| 2026-09-04 | `KLRA` | 35 | $15.74 | $15.60 | +4.90 | — | +0.00 | +4.90 | +12.25 | — |
| 2026-09-04 | `CABA` | 163 | — | $3.46 | +0.00 | $3.47 | -1.63 | -1.63 | -0.00 | -1.63 |
| 2026-09-04 | `ALEC` | 224 | — | $2.52 | +0.00 | $2.46 | +13.44 | +13.44 | -0.00 | +13.44 |
| 2026-09-04 | `BHC` | 84 | — | $6.71 | +0.00 | $6.56 | +12.60 | +12.60 | -0.00 | +12.60 |
| 2026-09-04 | `BMEA` | 297 | — | $1.90 | +0.00 | $2.03 | -38.61 | -38.61 | -0.00 | -38.61 |
| 2026-09-04 | `OABI` | 118 | — | $4.78 | +0.00 | $4.33 | +53.10 | +53.10 | -0.00 | +53.10 |
| 2026-09-04 | `OPK` | 355 | — | $1.59 | +0.00 | $1.64 | -17.75 | -17.75 | -0.00 | -17.75 |
| 2026-09-04 | `VIR` | 50 | — | $11.31 | +0.00 | $11.38 | -3.75 | -3.75 | -0.00 | -3.75 |
| 2026-09-04 | `ATRC` | 10 | — | $52.03 | +0.00 | $51.52 | +5.10 | +5.10 | -0.00 | +5.10 |
| 2026-09-08 | `CABA` | 163 | $3.47 | $3.43 | +6.52 | — | +0.00 | +6.52 | +4.89 | — |
| 2026-09-08 | `ALEC` | 224 | $2.46 | $2.38 | +17.92 | — | +0.00 | +17.92 | +31.36 | — |
| 2026-09-08 | `BHC` | 84 | $6.56 | $6.57 | -0.84 | — | +0.00 | -0.84 | +11.76 | — |
| 2026-09-08 | `BMEA` | 297 | $2.03 | $2.00 | +8.91 | — | +0.00 | +8.91 | -29.70 | — |
| 2026-09-08 | `OABI` | 118 | $4.33 | $4.30 | +3.54 | — | +0.00 | +3.54 | +56.64 | — |
| 2026-09-08 | `OPK` | 355 | $1.64 | $1.63 | +3.55 | — | +0.00 | +3.55 | -14.20 | — |
| 2026-09-08 | `VIR` | 50 | $11.38 | $11.22 | +8.25 | — | +0.00 | +8.25 | +4.50 | — |
| 2026-09-08 | `ATRC` | 10 | $51.52 | $54.31 | -27.90 | — | +0.00 | -27.90 | -22.80 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | -56.90 | TGTX, SLS, HIMS, VOR | — | $14,955.47 | $9,934.23 | TGTX×25, SLS×106, HIMS×42, VOR×56 |
| 2026-08-14 | +5.50 | $14,955.47 | TGTX×25, SLS×106, HIMS×42, VOR×56 | $9,928.54 | -5.69 | -29.22 | TLN, NRG, MARA, FOSL, ARX, CRMD, BIRK, HLIT | TGTX, SLS, HIMS, VOR | $14,532.11 | $9,873.39 | TLN×1, NRG×5, MARA×68, FOSL×109, ARX×31, CRMD×77, BIRK×15, HLIT×47 |
| 2026-08-17 | +2.25 | $14,532.11 | TLN×1, NRG×5, MARA×68, FOSL×109, ARX×31, CRMD×77, BIRK×15, HLIT×47 | $9,870.07 | -3.32 | +55.62 | TMC, TGB, ELF, DNN, HNST, CAPR, BYND, NU | TLN, NRG, MARA, FOSL, ARX, CRMD, BIRK, HLIT | $14,648.94 | $9,890.28 | TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×47, NU×39 |
| 2026-08-18 | -6.20 | $14,648.94 | TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×47, NU×39 | $9,961.79 | +71.51 | +0.00 | — | TMC, TGB, ELF, DNN, HNST, CAPR, BYND, NU | $9,943.70 | $9,943.70 | — |
| 2026-08-19 | -7.20 | $9,943.70 | — | $9,943.70 | +0.00 | +0.00 | — | — | $9,943.70 | $9,943.70 | — |
| 2026-08-20 | +1.12 | $9,943.70 | — | $9,943.70 | +0.00 | -55.35 | BHP, MRVI, WYFI, TOYO, DVLT, SAFX, AAP, AEG | — | $14,774.59 | $9,850.57 | BHP×6, MRVI×83, WYFI×29, TOYO×140, DVLT×2071, SAFX×1755, AAP×13, AEG×68 |
| 2026-08-21 | +3.25 | $14,774.59 | BHP×6, MRVI×83, WYFI×29, TOYO×140, DVLT×2071, SAFX×1755, AAP×13, AEG×68 | $9,810.86 | -39.71 | -151.07 | AUTL, CRDL, CRSP, FUTU, GMAB, ENHA, CAN, PRQR | BHP, MRVI, WYFI, TOYO, DVLT, SAFX, AAP, AEG | $14,562.78 | $9,588.30 | AUTL×247, CRDL×316, CRSP×10, FUTU×5, GMAB×18, ENHA×357, CAN×2077, PRQR×267 |
| 2026-08-24 | -5.17 | $14,562.78 | AUTL×247, CRDL×316, CRSP×10, FUTU×5, GMAB×18, ENHA×357, CAN×2077, PRQR×267 | $9,548.52 | -39.78 | +0.00 | — | AUTL, CRDL, CRSP, FUTU, GMAB, ENHA, CAN, PRQR | $9,512.95 | $9,512.95 | — |
| 2026-08-25 | +1.80 | $9,512.95 | — | $9,512.95 | +0.00 | -128.85 | MOS, OCUL, INSP, RZLT, HCA, CAPR, PUSA, CYPH | — | $14,031.71 | $9,363.55 | MOS×25, OCUL×54, INSP×9, RZLT×120, HCA×1, CAPR×82, PUSA×156, CYPH×381 |
| 2026-08-26 | +2.02 | $14,031.71 | MOS×25, OCUL×54, INSP×9, RZLT×120, HCA×1, CAPR×82, PUSA×156, CYPH×381 | $9,371.08 | +7.53 | -90.31 | FLNC, AVEX, AXTI, INDP, NVTS, IRDM | MOS, RZLT, HCA, CAPR, PUSA, CYPH | $15,025.11 | $9,244.62 | OCUL×54, INSP×9, FLNC×70, AVEX×44, AXTI×11, INDP×715, NVTS×61, IRDM×16 |
| 2026-08-27 | — | $15,025.11 | OCUL×54, INSP×9, FLNC×70, AVEX×44, AXTI×11, INDP×715, NVTS×61, IRDM×16 | $9,130.28 | -114.34 | +3.14 | MOS, ACMR, MT, TX, DLO, LRCX, NVDA | OCUL, INSP, FLNC, AVEX, AXTI, INDP, NVTS, IRDM | $12,539.90 | $9,095.05 | MOS×23, ACMR×6, MT×7, TX×10, DLO×37, LRCX×1, NVDA×2 |
| 2026-08-28 | +0.75 | $12,539.90 | MOS×23, ACMR×6, MT×7, TX×10, DLO×37, LRCX×1, NVDA×2 | $9,091.22 | -3.83 | +110.57 | SEDG, GRRR, URBN, SAFX, SIMO, XPOF, BHVN | ACMR, MT, TX, DLO, LRCX, NVDA | $13,940.98 | $9,164.66 | MOS×23, SEDG×19, GRRR×41, URBN×8, SAFX×1776, SIMO×2, XPOF×120, BHVN×40 |
| 2026-08-31 | -5.85 | $13,940.98 | MOS×23, SEDG×19, GRRR×41, URBN×8, SAFX×1776, SIMO×2, XPOF×120, BHVN×40 | $9,169.12 | +4.46 | +0.00 | — | MOS, SEDG, GRRR, URBN, SAFX, SIMO, XPOF, BHVN | $9,142.68 | $9,142.68 | — |
| 2026-09-01 | -6.30 | $9,142.68 | — | $9,142.68 | -0.00 | +0.00 | — | — | $9,142.68 | $9,142.68 | — |
| 2026-09-02 | -3.83 | $9,142.68 | — | $9,142.68 | -0.00 | +0.00 | — | — | $9,142.68 | $9,142.68 | — |
| 2026-09-03 | -0.90 | $9,142.68 | — | $9,142.68 | -0.00 | -11.55 | CRK, MRNA, EIX, SAFX, FRVO, DEFT, GMRS, KLRA | — | $13,491.18 | $9,099.45 | CRK×36, MRNA×3, EIX×10, SAFX×1515, FRVO×31, DEFT×879, GMRS×44, KLRA×35 |
| 2026-09-04 | +2.25 | $13,491.18 | CRK×36, MRNA×3, EIX×10, SAFX×1515, FRVO×31, DEFT×879, GMRS×44, KLRA×35 | $9,087.11 | -12.34 | +22.50 | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC | CRK, MRNA, EIX, SAFX, FRVO, DEFT, GMRS, KLRA | $13,503.46 | $9,055.27 | CABA×163, ALEC×224, BHC×84, BMEA×297, OABI×118, OPK×355, VIR×50, ATRC×10 |
| 2026-09-08 | -11.47 | $13,503.46 | CABA×163, ALEC×224, BHC×84, BMEA×297, OABI×118, OPK×355, VIR×50, ATRC×10 | $9,075.22 | +19.95 | +0.00 | — | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC | $9,052.69 | $9,052.69 | — |

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
| 2026-08-20 09:30 ET | **SHORT** | `MRVI` | 83 | $7.44 | $2.28 | — | $11,102.96 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $621.48 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 29 | $21.40 | $2.11 | — | $11,721.44 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-25.2; leftover $621.48 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 140 | $4.43 | $2.46 | — | $12,339.18 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-23.1; leftover $621.48 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `DVLT` | 2071 | $0.30 | $12.80 | — | $12,947.69 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-3.2; leftover $621.48 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `SAFX` | 1755 | $0.35 | $11.80 | — | $13,557.16 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-29.4; leftover $621.48 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $14,164.15 | — | last bar red; gate last_red=True; list earn_react; 🔵; ret5=+5.0; leftover $621.48 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AEG` | 68 | $9.01 | $2.23 | — | $14,774.59 | — | last bar red; gate last_red=True; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $621.48 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,774.59 | ▼ close $9,850.57 vs 09:30 $9,943.70 (session -55.35) | 16:00 close · cash $14,774.59 · equity $9,850.57 vs 09:30 $9,943.70 (-93.13; session marks -55.35) · 8 name(s) marked open→close (per-name table). BHP×6 09:30 $91.01 → close $93.63 -15.72; MRVI×83 09:30 $7.44 → close $8.29 -70.55; WYFI×29 09:30 $21.40 → close $21.16 +6.96; TOYO×140 09:30 $4.43 → close $4.51 -11.90; DVLT×2071 09:30 $0.30 → close $0.32 -41.42; SAFX×1755 09:30 $0.35 → close $0.34 +19.30; AAP×13 09:30 $46.85 → close $42.39 +57.98; AEG×68 09:30 $9.01 → close $9.01 -0.00 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,774.59 | ▼ 09:30 equity $9,810.86 vs yday $9,850.57 (-39.71) | 09:30 open · cash $14,774.59 (unchanged overnight, no fees) · equity $9,810.86 vs prior close $9,850.57 (-39.71) · 8 name(s) re-marked at the open (per-name table). BHP×6 yday $93.63 → 09:30 $95.72 -12.54; MRVI×83 yday $8.29 → 09:30 $8.28 +0.83; WYFI×29 yday $21.16 → 09:30 $21.54 -11.02; TOYO×140 yday $4.51 → 09:30 $4.68 -23.10; DVLT×2071 yday $0.32 → 09:30 $0.31 +20.71; SAFX×1755 yday $0.34 → 09:30 $0.35 -12.28; AAP×13 yday $42.39 → 09:30 $42.41 -0.26; AEG×68 yday $9.01 → 09:30 $9.04 -2.04 | — |
| 2026-08-21 09:30 ET | **COVER** | `BHP` | 6 | $95.72 | $2.01 | $-32.31 | $14,198.27 | ▼ -32.31 after sell → book $9,808.86; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `MRVI` | 83 | $8.28 | $2.24 | $-74.24 | $13,508.79 | ▼ -74.24 after sell → book $9,806.62; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `WYFI` | 29 | $21.54 | $2.08 | $-8.25 | $12,882.05 | ▼ -8.25 after sell → book $9,804.54; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `TOYO` | 140 | $4.68 | $2.41 | $-39.87 | $12,224.44 | ▼ -39.87 after sell → book $9,802.13; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `DVLT` | 2071 | $0.31 | $12.63 | $-46.14 | $11,569.80 | ▼ -46.14 after sell → book $9,789.50; vs 09:30 mark -12.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `SAFX` | 1755 | $0.35 | $11.41 | $-16.18 | $10,944.14 | ▼ -16.18 after sell → book $9,778.09; vs 09:30 mark -11.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `AAP` | 13 | $42.41 | $2.03 | $+53.63 | $10,390.78 | ▲ +53.63 after sell → book $9,776.06; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `AEG` | 68 | $9.04 | $2.19 | $-6.47 | $9,773.87 | ▼ -6.47 after sell → book $9,773.87; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUTL` | 247 | $2.47 | $3.25 | — | $10,380.70 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $610.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CRDL` | 316 | $1.93 | $4.16 | — | $10,986.43 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $610.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CRSP` | 10 | $59.72 | $2.06 | — | $11,581.57 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $610.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `FUTU` | 5 | $115.18 | $2.04 | — | $12,155.43 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $610.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `GMAB` | 18 | $33.36 | $2.08 | — | $12,753.83 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $610.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ENHA` | 357 | $1.71 | $4.69 | — | $13,359.61 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $610.87 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CAN` | 2077 | $0.29 | $12.71 | — | $13,957.54 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $610.87 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `PRQR` | 267 | $2.28 | $3.52 | — | $14,562.78 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+22.7; leftover $610.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,562.78 | ▼ close $9,588.30 vs 09:30 $9,810.86 (session -151.07) | 16:00 close · cash $14,562.78 · equity $9,588.30 vs 09:30 $9,810.86 (-222.56; session marks -151.07) · 8 name(s) marked open→close (per-name table). AUTL×247 09:30 $2.47 → close $2.41 +14.82; CRDL×316 09:30 $1.93 → close $1.86 +22.12; CRSP×10 09:30 $59.72 → close $59.50 +2.20; FUTU×5 09:30 $115.18 → close $123.64 -42.30; GMAB×18 09:30 $33.36 → close $33.45 -1.62; ENHA×357 09:30 $1.71 → close $1.72 -3.57; CAN×2077 09:30 $0.29 → close $0.35 -126.70; PRQR×267 09:30 $2.28 → close $2.34 -16.02 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,562.78 | ▼ 09:30 equity $9,548.52 vs yday $9,588.30 (-39.78) | 09:30 open · cash $14,562.78 (unchanged overnight, no fees) · equity $9,548.52 vs prior close $9,588.30 (-39.78) · 8 name(s) re-marked at the open (per-name table). AUTL×247 yday $2.41 → 09:30 $2.40 +2.47; CRDL×316 yday $1.86 → 09:30 $1.88 -6.32; CRSP×10 yday $59.50 → 09:30 $58.75 +7.50; FUTU×5 yday $123.64 → 09:30 $121.00 +13.20; GMAB×18 yday $33.45 → 09:30 $32.82 +11.34; ENHA×357 yday $1.72 → 09:30 $1.74 -7.14; CAN×2077 yday $0.35 → 09:30 $0.38 -58.16; PRQR×267 yday $2.34 → 09:30 $2.35 -2.67 | — |
| 2026-08-24 09:30 ET | **COVER** | `AUTL` | 247 | $2.40 | $3.19 | $+10.85 | $13,966.79 | ▲ +10.85 after sell → book $9,545.33; vs 09:30 mark -3.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `CRDL` | 316 | $1.88 | $4.08 | $+7.57 | $13,368.64 | ▲ +7.57 after sell → book $9,541.26; vs 09:30 mark -4.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `CRSP` | 10 | $58.75 | $2.02 | $+5.62 | $12,779.12 | ▲ +5.62 after sell → book $9,539.24; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **COVER** | `FUTU` | 5 | $121.00 | $2.00 | $-33.15 | $12,172.11 | ▼ -33.15 after sell → book $9,537.23; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **COVER** | `GMAB` | 18 | $32.82 | $2.04 | $+5.60 | $11,579.31 | ▲ +5.60 after sell → book $9,535.19; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `ENHA` | 357 | $1.74 | $4.61 | $-20.01 | $10,953.52 | ▼ -20.01 after sell → book $9,530.58; vs 09:30 mark -4.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `CAN` | 2077 | $0.38 | $14.19 | $-211.75 | $10,143.85 | ▼ -211.75 after sell → book $9,516.40; vs 09:30 mark -14.18 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **COVER** | `PRQR` | 267 | $2.35 | $3.44 | $-25.65 | $9,512.95 | ▼ -25.65 after sell → book $9,512.95; vs 09:30 mark -3.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,512.95 | ▲ close $9,512.95 vs 09:30 $9,548.52 (session +0.00) | 16:00 close · cash $9,512.95 · no lots left · equity $9,512.95. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,512.95 | ▲ 09:30 equity $9,512.95 vs yday $9,512.95 (+0.00) | 09:30 open · cash $9,512.95 · no holdings · equity $9,512.95 vs prior close $9,512.95 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **SHORT** | `MOS` | 25 | $23.77 | $2.10 | — | $10,105.10 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=+13.0; leftover $594.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **SHORT** | `OCUL` | 54 | $10.98 | $2.19 | — | $10,695.83 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+1.2; leftover $594.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **SHORT** | `INSP` | 9 | $61.19 | $2.05 | — | $11,244.49 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+7.4; leftover $594.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **SHORT** | `RZLT` | 120 | $4.94 | $2.40 | — | $11,834.89 | — | last bar red; gate last_red=True; list flatten; ret5=+7.1; leftover $594.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `HCA` | 1 | $426.97 | $2.02 | — | $12,259.84 | — | last bar red; gate last_red=True; list flatten; ret5=+6.0; leftover $594.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `CAPR` | 82 | $7.25 | $2.28 | — | $12,852.06 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $594.56 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `PUSA` | 156 | $3.80 | $2.51 | — | $13,442.35 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $594.56 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `CYPH` | 381 | $1.56 | $5.00 | — | $14,031.71 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $594.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,031.71 | ▼ close $9,363.55 vs 09:30 $9,512.95 (session -128.85) | 16:00 close · cash $14,031.71 · equity $9,363.55 vs 09:30 $9,512.95 (-149.40; session marks -128.85) · 8 name(s) marked open→close (per-name table). MOS×25 09:30 $23.77 → close $24.27 -12.50; OCUL×54 09:30 $10.98 → close $10.88 +5.40; INSP×9 09:30 $61.19 → close $61.07 +1.08; RZLT×120 09:30 $4.94 → close $5.01 -8.40; HCA×1 09:30 $426.97 → close $428.76 -1.79; CAPR×82 09:30 $7.25 → close $8.29 -85.28; PUSA×156 09:30 $3.80 → close $3.78 +3.12; CYPH×381 09:30 $1.56 → close $1.64 -30.48 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,031.71 | ▲ 09:30 equity $9,371.08 vs yday $9,363.55 (+7.53) | 09:30 open · cash $14,031.71 (unchanged overnight, no fees) · equity $9,371.08 vs prior close $9,363.55 (+7.53) · 8 name(s) re-marked at the open (per-name table). MOS×25 yday $24.27 → 09:30 $24.84 -14.25; OCUL×54 yday $10.88 → 09:30 $10.79 +4.86; INSP×9 yday $61.07 → 09:30 $60.07 +9.00; RZLT×120 yday $5.01 → 09:30 $5.01 -0.00; HCA×1 yday $428.76 → 09:30 $427.50 +1.26; CAPR×82 yday $8.29 → 09:30 $8.29 -0.00; PUSA×156 yday $3.78 → 09:30 $3.83 -8.58; CYPH×381 yday $1.64 → 09:30 $1.60 +15.24 | — |
| 2026-08-26 09:30 ET | **COVER** | `MOS` | 25 | $24.84 | $2.06 | $-30.92 | $13,408.64 | ▼ -30.92 after sell → book $9,369.01; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **COVER** | `RZLT` | 120 | $5.01 | $2.35 | $-13.15 | $12,805.09 | ▼ -13.15 after sell → book $9,366.66; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **COVER** | `HCA` | 1 | $427.50 | $1.99 | $-4.55 | $12,375.60 | ▼ -4.55 after sell → book $9,364.67; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **COVER** | `CAPR` | 82 | $8.29 | $2.24 | $-89.79 | $11,693.59 | ▼ -89.79 after sell → book $9,362.44; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **COVER** | `PUSA` | 156 | $3.83 | $2.46 | $-10.43 | $11,092.87 | ▼ -10.43 after sell → book $9,359.98; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `CYPH` | 381 | $1.60 | $4.91 | $-25.16 | $10,478.35 | ▼ -25.16 after sell → book $9,355.06; vs 09:30 mark -4.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SHORT** | `FLNC` | 70 | $11.12 | $2.24 | — | $11,254.51 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $779.59 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `AVEX` | 44 | $17.51 | $2.16 | — | $12,022.79 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $779.59 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `AXTI` | 11 | $65.34 | $2.06 | — | $12,739.46 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $779.59 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `INDP` | 715 | $1.09 | $9.37 | — | $13,509.44 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+17.0; leftover $779.59 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `NVTS` | 61 | $12.60 | $2.21 | — | $14,275.83 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-5.5; leftover $779.59 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `IRDM` | 16 | $46.96 | $2.08 | — | $15,025.11 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-3.9; leftover $779.59 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,025.11 | ▼ close $9,244.62 vs 09:30 $9,371.08 (session -90.31) | 16:00 close · cash $15,025.11 · equity $9,244.62 vs 09:30 $9,371.08 (-126.46; session marks -90.31) · 8 name(s) marked open→close (per-name table). OCUL×54 09:30 $10.79 → close $10.77 +1.08; INSP×9 09:30 $60.07 → close $61.80 -15.57; FLNC×70 09:30 $11.12 → close $11.08 +2.80; AVEX×44 09:30 $17.51 → close $18.34 -36.52; AXTI×11 09:30 $65.34 → close $65.18 +1.76; INDP×715 09:30 $1.09 → close $1.14 -35.75; NVTS×61 09:30 $12.60 → close $12.67 -4.27; IRDM×16 09:30 $46.96 → close $47.20 -3.84 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,025.11 | ▼ 09:30 equity $9,130.28 vs yday $9,244.62 (-114.34) | 09:30 open · cash $15,025.11 (unchanged overnight, no fees) · equity $9,130.28 vs prior close $9,244.62 (-114.34) · 8 name(s) re-marked at the open (per-name table). OCUL×54 yday $10.77 → 09:30 $10.63 +7.56; INSP×9 yday $61.80 → 09:30 $62.10 -2.70; FLNC×70 yday $11.08 → 09:30 $11.52 -30.80; AVEX×44 yday $18.34 → 09:30 $18.43 -3.96; AXTI×11 yday $65.18 → 09:30 $70.30 -56.32; INDP×715 yday $1.14 → 09:30 $1.13 +7.15; NVTS×61 yday $12.67 → 09:30 $13.18 -31.11; IRDM×16 yday $47.20 → 09:30 $47.46 -4.16 | — |
| 2026-08-27 09:30 ET | **COVER** | `OCUL` | 54 | $10.63 | $2.15 | $+14.56 | $14,448.94 | ▲ +14.56 after sell → book $9,128.13; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `INSP` | 9 | $62.10 | $2.02 | $-12.26 | $13,888.02 | ▼ -12.26 after sell → book $9,126.11; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `FLNC` | 70 | $11.52 | $2.20 | $-32.44 | $13,079.42 | ▼ -32.44 after sell → book $9,123.91; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `AVEX` | 44 | $18.43 | $2.12 | $-44.77 | $12,266.38 | ▼ -44.77 after sell → book $9,121.79; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `AXTI` | 11 | $70.30 | $2.02 | $-58.65 | $11,491.05 | ▼ -58.65 after sell → book $9,119.76; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `INDP` | 715 | $1.13 | $9.22 | $-47.20 | $10,673.88 | ▼ -47.20 after sell → book $9,110.54; vs 09:30 mark -9.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `NVTS` | 61 | $13.18 | $2.17 | $-39.77 | $9,867.73 | ▼ -39.77 after sell → book $9,108.37; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `IRDM` | 16 | $47.46 | $2.04 | $-12.12 | $9,106.33 | ▼ -12.12 after sell → book $9,106.33; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SHORT** | `MOS` | 23 | $24.00 | $2.09 | — | $9,656.24 | — | last bar red; gate last_red=True; list flatten; ret5=+8.7; leftover $569.15 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **SHORT** | `ACMR` | 6 | $81.65 | $2.04 | — | $10,144.09 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=+2.0; leftover $569.15 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 7 | $74.54 | $2.05 | — | $10,663.83 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-0.1; leftover $569.15 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 10 | $55.25 | $2.06 | — | $11,214.27 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=+2.1; leftover $569.15 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `DLO` | 37 | $15.33 | $2.14 | — | $11,779.35 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=+7.4; leftover $569.15 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `LRCX` | 1 | $318.88 | $2.02 | — | $12,096.21 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=+1.9; leftover $569.15 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `NVDA` | 2 | $222.86 | $2.03 | — | $12,539.90 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-3.6; leftover $569.15 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,539.90 | ▲ close $9,095.05 vs 09:30 $9,130.28 (session +3.14) | 16:00 close · cash $12,539.90 · equity $9,095.05 vs 09:30 $9,130.28 (-35.23; session marks +3.14) · 7 name(s) marked open→close (per-name table). MOS×23 09:30 $24.00 → close $23.76 +5.52; ACMR×6 09:30 $81.65 → close $80.49 +6.96; MT×7 09:30 $74.54 → close $74.63 -0.63; TX×10 09:30 $55.25 → close $55.83 -5.80; DLO×37 09:30 $15.33 → close $15.14 +7.03; LRCX×1 09:30 $318.88 → close $318.58 +0.30; NVDA×2 09:30 $222.86 → close $227.98 -10.24 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,539.90 | ▼ 09:30 equity $9,091.22 vs yday $9,095.05 (-3.83) | 09:30 open · cash $12,539.90 (unchanged overnight, no fees) · equity $9,091.22 vs prior close $9,095.05 (-3.83) · 7 name(s) re-marked at the open (per-name table). MOS×23 yday $23.76 → 09:30 $23.95 -4.37; ACMR×6 yday $80.49 → 09:30 $79.27 +7.32; MT×7 yday $74.63 → 09:30 $75.39 -5.32; TX×10 yday $55.83 → 09:30 $55.97 -1.40; DLO×37 yday $15.14 → 09:30 $15.19 -1.85; LRCX×1 yday $318.58 → 09:30 $318.03 +0.55; NVDA×2 yday $227.98 → 09:30 $227.36 +1.24 | — |
| 2026-08-28 09:30 ET | **COVER** | `ACMR` | 6 | $79.27 | $2.01 | $+10.23 | $12,062.27 | ▲ +10.23 after sell → book $9,089.21; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `MT` | 7 | $75.39 | $2.01 | $-10.01 | $11,532.53 | ▼ -10.01 after sell → book $9,087.20; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `TX` | 10 | $55.97 | $2.02 | $-11.28 | $10,970.81 | ▼ -11.28 after sell → book $9,085.18; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `DLO` | 37 | $15.19 | $2.10 | $+0.94 | $10,406.68 | ▲ +0.94 after sell → book $9,083.08; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `LRCX` | 1 | $318.03 | $1.99 | $-3.16 | $10,086.65 | ▼ -3.16 after sell → book $9,081.08; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `NVDA` | 2 | $227.36 | $2.00 | $-13.02 | $9,629.94 | ▼ -13.02 after sell → book $9,079.09; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SEDG` | 19 | $32.90 | $2.08 | — | $10,252.95 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $648.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `GRRR` | 41 | $15.66 | $2.15 | — | $10,892.86 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $648.51 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `URBN` | 8 | $79.42 | $2.05 | — | $11,526.17 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $648.51 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SAFX` | 1776 | $0.36 | $12.13 | — | $12,162.28 | — | last bar red; gate last_red=True; list probable,yday_gainer; ret5=+7.6; leftover $648.51 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 2 | $252.24 | $2.03 | — | $12,664.73 | — | last bar red; gate last_red=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $648.51 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `XPOF` | 120 | $5.38 | $2.40 | — | $13,307.93 | — | last bar red; gate last_red=True; list probable,yday_gainer; ret5=+6.5; leftover $648.51 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `BHVN` | 40 | $15.88 | $2.15 | — | $13,940.98 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+19.4; leftover $648.51 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,940.98 | ▲ close $9,164.66 vs 09:30 $9,091.22 (session +110.57) | 16:00 close · cash $13,940.98 · equity $9,164.66 vs 09:30 $9,091.22 (+73.44; session marks +110.57) · 8 name(s) marked open→close (per-name table). MOS×23 09:30 $23.95 → close $23.60 +8.05; SEDG×19 09:30 $32.90 → close $31.41 +28.31; GRRR×41 09:30 $15.66 → close $14.41 +51.25; URBN×8 09:30 $79.42 → close $81.09 -13.36; SAFX×1776 09:30 $0.36 → close $0.36 +10.66; SIMO×2 09:30 $252.24 → close $245.81 +12.86; XPOF×120 09:30 $5.38 → close $5.43 -6.00; BHVN×40 09:30 $15.88 → close $15.41 +18.80 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,940.98 | ▲ 09:30 equity $9,169.12 vs yday $9,164.66 (+4.46) | 09:30 open · cash $13,940.98 (unchanged overnight, no fees) · equity $9,169.12 vs prior close $9,164.66 (+4.46) · 8 name(s) re-marked at the open (per-name table). MOS×23 yday $23.60 → 09:30 $23.68 -1.84; SEDG×19 yday $31.41 → 09:30 $31.15 +4.94; GRRR×41 yday $14.41 → 09:30 $14.44 -1.23; URBN×8 yday $81.09 → 09:30 $80.44 +5.20; SAFX×1776 yday $0.36 → 09:30 $0.36 -5.33; SIMO×2 yday $245.81 → 09:30 $247.05 -2.48; XPOF×120 yday $5.43 → 09:30 $5.37 +7.20; BHVN×40 yday $15.41 → 09:30 $15.46 -2.00 | — |
| 2026-08-31 09:30 ET | **COVER** | `MOS` | 23 | $23.68 | $2.06 | $+3.21 | $13,394.29 | ▲ +3.21 after sell → book $9,167.06; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SEDG` | 19 | $31.15 | $2.05 | $+29.12 | $12,800.39 | ▲ +29.12 after sell → book $9,165.02; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `GRRR` | 41 | $14.44 | $2.11 | $+45.76 | $12,206.24 | ▲ +45.76 after sell → book $9,162.90; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `URBN` | 8 | $80.44 | $2.01 | $-12.23 | $11,560.70 | ▼ -12.23 after sell → book $9,160.89; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SAFX` | 1776 | $0.36 | $11.76 | $-18.56 | $10,906.03 | ▼ -18.56 after sell → book $9,149.13; vs 09:30 mark -11.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SIMO` | 2 | $247.05 | $2.00 | $+6.35 | $10,409.94 | ▲ +6.35 after sell → book $9,147.14; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `XPOF` | 120 | $5.37 | $2.35 | $-3.55 | $9,763.19 | ▼ -3.55 after sell → book $9,144.79; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `BHVN` | 40 | $15.46 | $2.11 | $+12.54 | $9,142.68 | ▲ +12.54 after sell → book $9,142.68; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,142.68 | ▲ close $9,142.68 vs 09:30 $9,169.12 (session +0.00) | 16:00 close · cash $9,142.68 · no lots left · equity $9,142.68. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,142.68 | ▲ 09:30 equity $9,142.68 vs yday $9,142.68 (-0.00) | 09:30 open · cash $9,142.68 · no holdings · equity $9,142.68 vs prior close $9,142.68 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,142.68 | ▲ close $9,142.68 vs 09:30 $9,142.68 (session +0.00) | 16:00 close · cash $9,142.68 · no lots left · equity $9,142.68. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,142.68 | ▲ 09:30 equity $9,142.68 vs yday $9,142.68 (-0.00) | 09:30 open · cash $9,142.68 · no holdings · equity $9,142.68 vs prior close $9,142.68 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,142.68 | ▲ close $9,142.68 vs 09:30 $9,142.68 (session +0.00) | 16:00 close · cash $9,142.68 · no lots left · equity $9,142.68. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,142.68 | ▲ 09:30 equity $9,142.68 vs yday $9,142.68 (-0.00) | 09:30 open · cash $9,142.68 · no holdings · equity $9,142.68 vs prior close $9,142.68 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **SHORT** | `CRK` | 36 | $15.45 | $2.13 | — | $9,696.74 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $571.42 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **SHORT** | `MRNA` | 3 | $145.94 | $2.03 | — | $10,132.55 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $571.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `EIX` | 10 | $55.42 | $2.06 | — | $10,684.69 | — | last bar red; gate last_red=True; list probable,yday_gainer; ret5=-25.9; leftover $571.42 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `SAFX` | 1515 | $0.38 | $10.53 | — | $11,245.31 | — | last bar red; gate last_red=True; list probable,yday_gainer; ret5=-2.3; leftover $571.42 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `FRVO` | 31 | $18.28 | $2.12 | — | $11,809.87 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+16.5; leftover $571.42 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `DEFT` | 879 | $0.65 | $8.52 | — | $12,372.70 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+10.2; leftover $571.42 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `GMRS` | 44 | $12.83 | $2.16 | — | $12,935.06 | — | last bar red; gate last_red=True; list yday_gainer; ret5=-0.2; leftover $571.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `KLRA` | 35 | $15.95 | $2.13 | — | $13,491.18 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-14.0; leftover $571.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,491.18 | ▼ close $9,099.45 vs 09:30 $9,142.68 (session -11.55) | 16:00 close · cash $13,491.18 · equity $9,099.45 vs 09:30 $9,142.68 (-43.23; session marks -11.55) · 8 name(s) marked open→close (per-name table). CRK×36 09:30 $15.45 → close $14.95 +18.00; MRNA×3 09:30 $145.94 → close $148.87 -8.78; EIX×10 09:30 $55.42 → close $56.30 -8.80; SAFX×1515 09:30 $0.38 → close $0.38 -3.03; FRVO×31 09:30 $18.28 → close $17.16 +34.72; DEFT×879 09:30 $0.65 → close $0.68 -25.49; GMRS×44 09:30 $12.83 → close $13.41 -25.52; KLRA×35 09:30 $15.95 → close $15.74 +7.35 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,491.18 | ▼ 09:30 equity $9,087.11 vs yday $9,099.45 (-12.34) | 09:30 open · cash $13,491.18 (unchanged overnight, no fees) · equity $9,087.11 vs prior close $9,099.45 (-12.34) · 8 name(s) re-marked at the open (per-name table). CRK×36 yday $14.95 → 09:30 $15.00 -1.80; MRNA×3 yday $148.87 → 09:30 $153.62 -14.25; EIX×10 yday $56.30 → 09:30 $55.79 +5.10; SAFX×1515 yday $0.38 → 09:30 $0.38 +1.52; FRVO×31 yday $17.16 → 09:30 $17.27 -3.41; DEFT×879 yday $0.68 → 09:30 $0.69 -9.67; GMRS×44 yday $13.41 → 09:30 $13.29 +5.28; KLRA×35 yday $15.74 → 09:30 $15.60 +4.90 | — |
| 2026-09-04 09:30 ET | **COVER** | `CRK` | 36 | $15.00 | $2.10 | $+11.97 | $12,949.09 | ▲ +11.97 after sell → book $9,085.02; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `MRNA` | 3 | $153.62 | $2.00 | $-27.05 | $12,486.23 | ▼ -27.05 after sell → book $9,083.02; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `EIX` | 10 | $55.79 | $2.02 | $-7.78 | $11,926.31 | ▼ -7.78 after sell → book $9,081.00; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `SAFX` | 1515 | $0.38 | $10.27 | $-22.32 | $11,343.37 | ▼ -22.32 after sell → book $9,070.73; vs 09:30 mark -10.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `FRVO` | 31 | $17.27 | $2.08 | $+27.11 | $10,805.91 | ▲ +27.11 after sell → book $9,068.64; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `DEFT` | 879 | $0.69 | $8.70 | $-52.38 | $10,190.70 | ▼ -52.38 after sell → book $9,059.94; vs 09:30 mark -8.70 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `GMRS` | 44 | $13.29 | $2.12 | $-24.52 | $9,603.82 | ▼ -24.52 after sell → book $9,057.82; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `KLRA` | 35 | $15.60 | $2.10 | $+8.02 | $9,055.72 | ▲ +8.02 after sell → book $9,055.72; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `CABA` | 163 | $3.46 | $2.53 | — | $9,617.17 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $565.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **SHORT** | `ALEC` | 224 | $2.52 | $2.95 | — | $10,178.70 | — | last bar red; gate last_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $565.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `BHC` | 84 | $6.71 | $2.28 | — | $10,740.06 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $565.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `BMEA` | 297 | $1.90 | $3.91 | — | $11,300.45 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $565.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `OABI` | 118 | $4.78 | $2.39 | — | $11,862.10 | — | last bar red; gate last_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $565.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `OPK` | 355 | $1.59 | $4.66 | — | $12,421.89 | — | last bar red; gate last_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $565.98 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `VIR` | 50 | $11.31 | $2.18 | — | $12,985.21 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $565.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `ATRC` | 10 | $52.03 | $2.05 | — | $13,503.46 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $565.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,503.46 | ▲ close $9,055.27 vs 09:30 $9,087.11 (session +22.50) | 16:00 close · cash $13,503.46 · equity $9,055.27 vs 09:30 $9,087.11 (-31.84; session marks +22.50) · 8 name(s) marked open→close (per-name table). CABA×163 09:30 $3.46 → close $3.47 -1.63; ALEC×224 09:30 $2.52 → close $2.46 +13.44; BHC×84 09:30 $6.71 → close $6.56 +12.60; BMEA×297 09:30 $1.90 → close $2.03 -38.61; OABI×118 09:30 $4.78 → close $4.33 +53.10; OPK×355 09:30 $1.59 → close $1.64 -17.75; VIR×50 09:30 $11.31 → close $11.38 -3.75; ATRC×10 09:30 $52.03 → close $51.52 +5.10 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,503.46 | ▲ 09:30 equity $9,075.22 vs yday $9,055.27 (+19.95) | 09:30 open · cash $13,503.46 (unchanged overnight, no fees) · equity $9,075.22 vs prior close $9,055.27 (+19.95) · 8 name(s) re-marked at the open (per-name table). CABA×163 yday $3.47 → 09:30 $3.43 +6.52; ALEC×224 yday $2.46 → 09:30 $2.38 +17.92; BHC×84 yday $6.56 → 09:30 $6.57 -0.84; BMEA×297 yday $2.03 → 09:30 $2.00 +8.91; OABI×118 yday $4.33 → 09:30 $4.30 +3.54; OPK×355 yday $1.64 → 09:30 $1.63 +3.55; VIR×50 yday $11.38 → 09:30 $11.22 +8.25; ATRC×10 yday $51.52 → 09:30 $54.31 -27.90 | — |
| 2026-09-08 09:30 ET | **COVER** | `CABA` | 163 | $3.43 | $2.48 | $-0.12 | $12,941.89 | ▼ -0.12 after sell → book $9,072.74; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `ALEC` | 224 | $2.38 | $2.89 | $+25.52 | $12,405.88 | ▲ +25.52 after sell → book $9,069.85; vs 09:30 mark -2.89 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `BHC` | 84 | $6.57 | $2.24 | $+7.24 | $11,851.76 | ▲ +7.24 after sell → book $9,067.61; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `BMEA` | 297 | $2.00 | $3.83 | $-37.44 | $11,253.93 | ▼ -37.44 after sell → book $9,063.78; vs 09:30 mark -3.83 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **COVER** | `OABI` | 118 | $4.30 | $2.34 | $+51.91 | $10,744.18 | ▲ +51.91 after sell → book $9,061.43; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `OPK` | 355 | $1.63 | $4.58 | $-23.44 | $10,160.95 | ▼ -23.44 after sell → book $9,056.85; vs 09:30 mark -4.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `VIR` | 50 | $11.22 | $2.14 | $+0.18 | $9,597.81 | ▲ +0.18 after sell → book $9,054.71; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `ATRC` | 10 | $54.31 | $2.02 | $-26.87 | $9,052.69 | ▼ -26.87 after sell → book $9,052.69; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,052.69 | ▲ close $9,052.69 vs 09:30 $9,075.22 (session +0.00) | 16:00 close · cash $9,052.69 · no lots left · equity $9,052.69. | — |

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
| 2026-08-27 | `ASML` | cash | leftover split 569.15 < 1 share @ 1746.53 |
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
