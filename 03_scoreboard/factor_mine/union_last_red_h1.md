# Factor mine action — `union_last_red_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ last_red, no 🚨

Cash book **+0.35%** ($10,035) · signal-only (no cash/fees) was +15.68%. Starts YES **5/20**. Fills 160 · skips 70 · realized $+35.42.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the last finished bar was red (closed down).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,035.41.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TGTX` | 50 | — | $49.70 | +0.00 | $47.94 | -88.00 | -88.00 | +0.00 | -88.00 |
| 2026-08-13 | `SLS` | 213 | — | $11.70 | +0.00 | $12.36 | +140.58 | +140.58 | +0.00 | +140.58 |
| 2026-08-13 | `HIMS` | 84 | — | $29.74 | +0.00 | $28.77 | -81.48 | -81.48 | +0.00 | -81.48 |
| 2026-08-13 | `VOR` | 113 | — | $22.01 | +0.00 | $23.29 | +144.64 | +144.64 | +0.00 | +144.64 |
| 2026-08-14 | `TGTX` | 50 | $47.94 | $47.27 | -33.50 | — | +0.00 | -33.50 | -121.50 | — |
| 2026-08-14 | `SLS` | 213 | $12.36 | $12.40 | +8.52 | — | +0.00 | +8.52 | +149.10 | — |
| 2026-08-14 | `HIMS` | 84 | $28.77 | $29.15 | +31.92 | — | +0.00 | +31.92 | -49.56 | — |
| 2026-08-14 | `VOR` | 113 | $23.29 | $23.33 | +4.52 | — | +0.00 | +4.52 | +149.16 | — |
| 2026-08-14 | `TLN` | 3 | — | $359.83 | +0.00 | $362.74 | +8.73 | +8.73 | +0.00 | +8.73 |
| 2026-08-14 | `NRG` | 10 | — | $120.00 | +0.00 | $126.24 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-08-14 | `MARA` | 140 | — | $9.01 | +0.00 | $9.20 | +26.60 | +26.60 | +0.00 | +26.60 |
| 2026-08-14 | `ARX` | 64 | — | $19.57 | +0.00 | $19.58 | +0.64 | +0.64 | +0.00 | +0.64 |
| 2026-08-14 | `HLIT` | 95 | — | $13.18 | +0.00 | $13.92 | +70.30 | +70.30 | +0.00 | +70.30 |
| 2026-08-14 | `SECZ` | 216 | — | $5.84 | +0.00 | $5.61 | -49.68 | -49.68 | +0.00 | -49.68 |
| 2026-08-14 | `LFTO` | 61 | — | $20.57 | +0.00 | $21.61 | +63.44 | +63.44 | +0.00 | +63.44 |
| 2026-08-14 | `REZI` | 61 | — | $20.56 | +0.00 | $20.50 | -3.66 | -3.66 | +0.00 | -3.66 |
| 2026-08-17 | `TLN` | 3 | $362.74 | $367.88 | +15.42 | — | +0.00 | +15.42 | +24.15 | — |
| 2026-08-17 | `NRG` | 10 | $126.24 | $127.40 | +11.60 | — | +0.00 | +11.60 | +74.00 | — |
| 2026-08-17 | `MARA` | 140 | $9.20 | $9.22 | +2.80 | — | +0.00 | +2.80 | +29.40 | — |
| 2026-08-17 | `ARX` | 64 | $19.58 | $19.57 | -0.64 | — | +0.00 | -0.64 | +0.00 | — |
| 2026-08-17 | `HLIT` | 95 | $13.92 | $13.84 | -7.60 | — | +0.00 | -7.60 | +62.70 | — |
| 2026-08-17 | `SECZ` | 216 | $5.61 | $5.45 | -34.56 | — | +0.00 | -34.56 | -84.24 | — |
| 2026-08-17 | `LFTO` | 61 | $21.61 | $21.00 | -37.21 | — | +0.00 | -37.21 | +26.23 | — |
| 2026-08-17 | `REZI` | 61 | $20.50 | $20.83 | +20.13 | — | +0.00 | +20.13 | +16.47 | — |
| 2026-08-17 | `TMC` | 315 | — | $4.05 | +0.00 | $3.77 | -88.20 | -88.20 | +0.00 | -88.20 |
| 2026-08-17 | `TGB` | 151 | — | $8.46 | +0.00 | $8.77 | +46.81 | +46.81 | +0.00 | +46.81 |
| 2026-08-17 | `ELF` | 14 | — | $90.54 | +0.00 | $93.66 | +43.68 | +43.68 | +0.00 | +43.68 |
| 2026-08-17 | `DNN` | 394 | — | $3.24 | +0.00 | $3.19 | -19.70 | -19.70 | +0.00 | -19.70 |
| 2026-08-17 | `CAPR` | 185 | — | $6.87 | +0.00 | $7.45 | +107.30 | +107.30 | +0.00 | +107.30 |
| 2026-08-17 | `NU` | 82 | — | $15.40 | +0.00 | $14.74 | -54.12 | -54.12 | +0.00 | -54.12 |
| 2026-08-17 | `INV` | 788 | — | $1.62 | +0.00 | $1.39 | -185.18 | -185.18 | +0.00 | -185.18 |
| 2026-08-17 | `KLC` | 487 | — | $2.62 | +0.00 | $2.56 | -29.22 | -29.22 | +0.00 | -29.22 |
| 2026-08-18 | `TMC` | 315 | $3.77 | $3.72 | -15.75 | — | +0.00 | -15.75 | -103.95 | — |
| 2026-08-18 | `TGB` | 151 | $8.77 | $8.55 | -33.22 | — | +0.00 | -33.22 | +13.59 | — |
| 2026-08-18 | `ELF` | 14 | $93.66 | $93.44 | -3.08 | — | +0.00 | -3.08 | +40.60 | — |
| 2026-08-18 | `DNN` | 394 | $3.19 | $3.11 | -31.52 | — | +0.00 | -31.52 | -51.22 | — |
| 2026-08-18 | `CAPR` | 185 | $7.45 | $7.50 | +9.25 | — | +0.00 | +9.25 | +116.55 | — |
| 2026-08-18 | `NU` | 82 | $14.74 | $14.53 | -17.22 | — | +0.00 | -17.22 | -71.34 | — |
| 2026-08-18 | `INV` | 788 | $1.39 | $1.32 | -47.28 | — | +0.00 | -47.28 | -232.46 | — |
| 2026-08-18 | `KLC` | 487 | $2.56 | $2.52 | -19.48 | — | +0.00 | -19.48 | -48.70 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `MRVI` | 164 | — | $7.44 | +0.00 | $8.29 | +139.40 | +139.40 | +0.00 | +139.40 |
| 2026-08-20 | `CRCL` | 14 | — | $82.99 | +0.00 | $83.66 | +9.38 | +9.38 | +0.00 | +9.38 |
| 2026-08-20 | `WYFI` | 57 | — | $21.40 | +0.00 | $21.16 | -13.68 | -13.68 | +0.00 | -13.68 |
| 2026-08-20 | `TOYO` | 276 | — | $4.43 | +0.00 | $4.51 | +23.46 | +23.46 | +0.00 | +23.46 |
| 2026-08-20 | `DVLT` | 4088 | — | $0.30 | +0.00 | $0.32 | +81.76 | +81.76 | +0.00 | +81.76 |
| 2026-08-20 | `SAFX` | 3465 | — | $0.35 | +0.00 | $0.34 | -38.11 | -38.11 | +0.00 | -38.11 |
| 2026-08-20 | `AAP` | 26 | — | $46.85 | +0.00 | $42.39 | -115.96 | -115.96 | +0.00 | -115.96 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `MRVI` | 164 | $8.29 | $8.28 | -1.64 | — | +0.00 | -1.64 | +137.76 | — |
| 2026-08-21 | `CRCL` | 14 | $83.66 | $87.98 | +60.48 | — | +0.00 | +60.48 | +69.86 | — |
| 2026-08-21 | `WYFI` | 57 | $21.16 | $21.54 | +21.66 | — | +0.00 | +21.66 | +7.98 | — |
| 2026-08-21 | `TOYO` | 276 | $4.51 | $4.68 | +45.54 | — | +0.00 | +45.54 | +69.00 | — |
| 2026-08-21 | `DVLT` | 4088 | $0.32 | $0.31 | -40.88 | — | +0.00 | -40.88 | +40.88 | — |
| 2026-08-21 | `SAFX` | 3465 | $0.34 | $0.35 | +24.25 | — | +0.00 | +24.25 | -13.86 | — |
| 2026-08-21 | `AAP` | 26 | $42.39 | $42.41 | +0.52 | — | +0.00 | +0.52 | -115.44 | — |
| 2026-08-21 | `AUTL` | 503 | — | $2.47 | +0.00 | $2.41 | -30.18 | -30.18 | +0.00 | -30.18 |
| 2026-08-21 | `CRDL` | 644 | — | $1.93 | +0.00 | $1.86 | -45.08 | -45.08 | +0.00 | -45.08 |
| 2026-08-21 | `CRSP` | 20 | — | $59.72 | +0.00 | $59.50 | -4.40 | -4.40 | +0.00 | -4.40 |
| 2026-08-21 | `FUTU` | 10 | — | $115.18 | +0.00 | $123.64 | +84.60 | +84.60 | +0.00 | +84.60 |
| 2026-08-21 | `GMAB` | 37 | — | $33.36 | +0.00 | $33.45 | +3.33 | +3.33 | +0.00 | +3.33 |
| 2026-08-21 | `ENHA` | 727 | — | $1.71 | +0.00 | $1.72 | +7.27 | +7.27 | +0.00 | +7.27 |
| 2026-08-21 | `CAN` | 4228 | — | $0.29 | +0.00 | $0.35 | +257.91 | +257.91 | +0.00 | +257.91 |
| 2026-08-21 | `PRQR` | 545 | — | $2.28 | +0.00 | $2.34 | +32.70 | +32.70 | +0.00 | +32.70 |
| 2026-08-24 | `AUTL` | 503 | $2.41 | $2.40 | -5.03 | — | +0.00 | -5.03 | -35.21 | — |
| 2026-08-24 | `CRDL` | 644 | $1.86 | $1.88 | +12.88 | — | +0.00 | +12.88 | -32.20 | — |
| 2026-08-24 | `CRSP` | 20 | $59.50 | $58.75 | -15.00 | $57.08 | -33.50 | -48.50 | -19.40 | -52.90 |
| 2026-08-24 | `FUTU` | 10 | $123.64 | $121.00 | -26.40 | — | +0.00 | -26.40 | +58.20 | — |
| 2026-08-24 | `GMAB` | 37 | $33.45 | $32.82 | -23.31 | — | +0.00 | -23.31 | -19.98 | — |
| 2026-08-24 | `ENHA` | 727 | $1.72 | $1.74 | +14.54 | — | +0.00 | +14.54 | +21.81 | — |
| 2026-08-24 | `CAN` | 4228 | $0.35 | $0.38 | +118.38 | — | +0.00 | +118.38 | +376.29 | — |
| 2026-08-24 | `PRQR` | 545 | $2.34 | $2.35 | +5.45 | — | +0.00 | +5.45 | +38.15 | — |
| 2026-08-25 | `CRSP` | 20 | $57.08 | $57.93 | +17.10 | — | +0.00 | +17.10 | -35.80 | — |
| 2026-08-25 | `MOS` | 53 | — | $23.77 | +0.00 | $24.27 | +26.50 | +26.50 | +0.00 | +26.50 |
| 2026-08-25 | `OCUL` | 115 | — | $10.98 | +0.00 | $10.88 | -11.50 | -11.50 | +0.00 | -11.50 |
| 2026-08-25 | `INSP` | 20 | — | $61.19 | +0.00 | $61.07 | -2.40 | -2.40 | +0.00 | -2.40 |
| 2026-08-25 | `RZLT` | 257 | — | $4.94 | +0.00 | $5.01 | +17.99 | +17.99 | +0.00 | +17.99 |
| 2026-08-25 | `HCA` | 2 | — | $426.97 | +0.00 | $428.76 | +3.58 | +3.58 | +0.00 | +3.58 |
| 2026-08-25 | `CAPR` | 175 | — | $7.25 | +0.00 | $8.29 | +182.00 | +182.00 | +0.00 | +182.00 |
| 2026-08-25 | `PUSA` | 335 | — | $3.80 | +0.00 | $3.78 | -6.70 | -6.70 | +0.00 | -6.70 |
| 2026-08-25 | `CYPH` | 816 | — | $1.56 | +0.00 | $1.64 | +65.28 | +65.28 | +0.00 | +65.28 |
| 2026-08-26 | `MOS` | 53 | $24.27 | $24.84 | +30.21 | — | +0.00 | +30.21 | +56.71 | — |
| 2026-08-26 | `OCUL` | 115 | $10.88 | $10.79 | -10.35 | $10.77 | -2.30 | -12.65 | -21.85 | -24.15 |
| 2026-08-26 | `INSP` | 20 | $61.07 | $60.07 | -20.00 | $61.80 | +34.60 | +14.60 | -22.40 | +12.20 |
| 2026-08-26 | `RZLT` | 257 | $5.01 | $5.01 | +0.00 | — | +0.00 | +0.00 | +17.99 | — |
| 2026-08-26 | `HCA` | 2 | $428.76 | $427.50 | -2.52 | — | +0.00 | -2.52 | +1.06 | — |
| 2026-08-26 | `CAPR` | 175 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | +182.00 | — |
| 2026-08-26 | `PUSA` | 335 | $3.78 | $3.83 | +18.43 | — | +0.00 | +18.43 | +11.73 | — |
| 2026-08-26 | `CYPH` | 816 | $1.64 | $1.60 | -32.64 | — | +0.00 | -32.64 | +32.64 | — |
| 2026-08-26 | `FLNC` | 119 | — | $11.12 | +0.00 | $11.08 | -4.76 | -4.76 | +0.00 | -4.76 |
| 2026-08-26 | `AVEX` | 75 | — | $17.51 | +0.00 | $18.34 | +62.25 | +62.25 | +0.00 | +62.25 |
| 2026-08-26 | `AXTI` | 20 | — | $65.34 | +0.00 | $65.18 | -3.20 | -3.20 | +0.00 | -3.20 |
| 2026-08-26 | `INDP` | 1215 | — | $1.09 | +0.00 | $1.14 | +60.75 | +60.75 | +0.00 | +60.75 |
| 2026-08-26 | `NVTS` | 105 | — | $12.60 | +0.00 | $12.67 | +7.35 | +7.35 | +0.00 | +7.35 |
| 2026-08-26 | `IRDM` | 28 | — | $46.96 | +0.00 | $47.20 | +6.72 | +6.72 | +0.00 | +6.72 |
| 2026-08-27 | `OCUL` | 115 | $10.77 | $10.63 | -16.10 | — | +0.00 | -16.10 | -40.25 | — |
| 2026-08-27 | `INSP` | 20 | $61.80 | $62.10 | +6.00 | — | +0.00 | +6.00 | +18.20 | — |
| 2026-08-27 | `FLNC` | 119 | $11.08 | $11.52 | +52.36 | — | +0.00 | +52.36 | +47.60 | — |
| 2026-08-27 | `AVEX` | 75 | $18.34 | $18.43 | +6.75 | — | +0.00 | +6.75 | +69.00 | — |
| 2026-08-27 | `AXTI` | 20 | $65.18 | $70.30 | +102.40 | — | +0.00 | +102.40 | +99.20 | — |
| 2026-08-27 | `INDP` | 1215 | $1.14 | $1.13 | -12.15 | — | +0.00 | -12.15 | +48.60 | — |
| 2026-08-27 | `NVTS` | 105 | $12.67 | $13.18 | +53.55 | — | +0.00 | +53.55 | +60.90 | — |
| 2026-08-27 | `IRDM` | 28 | $47.20 | $47.46 | +7.28 | — | +0.00 | +7.28 | +14.00 | — |
| 2026-08-27 | `MOS` | 55 | — | $24.00 | +0.00 | $23.76 | -13.20 | -13.20 | +0.00 | -13.20 |
| 2026-08-27 | `ACMR` | 16 | — | $81.65 | +0.00 | $80.49 | -18.56 | -18.56 | +0.00 | -18.56 |
| 2026-08-27 | `MT` | 17 | — | $74.54 | +0.00 | $74.63 | +1.53 | +1.53 | +0.00 | +1.53 |
| 2026-08-27 | `TX` | 24 | — | $55.25 | +0.00 | $55.83 | +13.92 | +13.92 | +0.00 | +13.92 |
| 2026-08-27 | `DLO` | 87 | — | $15.33 | +0.00 | $15.14 | -16.53 | -16.53 | +0.00 | -16.53 |
| 2026-08-27 | `LRCX` | 4 | — | $318.88 | +0.00 | $318.58 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-27 | `NVDA` | 5 | — | $222.86 | +0.00 | $227.98 | +25.60 | +25.60 | +0.00 | +25.60 |
| 2026-08-28 | `MOS` | 55 | $23.76 | $23.95 | +10.45 | $23.60 | -19.25 | -8.80 | -2.75 | -22.00 |
| 2026-08-28 | `ACMR` | 16 | $80.49 | $79.27 | -19.52 | — | +0.00 | -19.52 | -38.08 | — |
| 2026-08-28 | `MT` | 17 | $74.63 | $75.39 | +12.92 | — | +0.00 | +12.92 | +14.45 | — |
| 2026-08-28 | `TX` | 24 | $55.83 | $55.97 | +3.36 | — | +0.00 | +3.36 | +17.28 | — |
| 2026-08-28 | `DLO` | 87 | $15.14 | $15.19 | +4.35 | — | +0.00 | +4.35 | -12.18 | — |
| 2026-08-28 | `LRCX` | 4 | $318.58 | $318.03 | -2.20 | — | +0.00 | -2.20 | -3.40 | — |
| 2026-08-28 | `NVDA` | 5 | $227.98 | $227.36 | -3.10 | — | +0.00 | -3.10 | +22.50 | — |
| 2026-08-28 | `SEDG` | 40 | — | $32.90 | +0.00 | $31.41 | -59.60 | -59.60 | +0.00 | -59.60 |
| 2026-08-28 | `GRRR` | 85 | — | $15.66 | +0.00 | $14.41 | -106.25 | -106.25 | +0.00 | -106.25 |
| 2026-08-28 | `URBN` | 16 | — | $79.42 | +0.00 | $81.09 | +26.72 | +26.72 | +0.00 | +26.72 |
| 2026-08-28 | `SIMO` | 5 | — | $252.24 | +0.00 | $245.81 | -32.15 | -32.15 | +0.00 | -32.15 |
| 2026-08-28 | `BHVN` | 84 | — | $15.88 | +0.00 | $15.41 | -39.48 | -39.48 | +0.00 | -39.48 |
| 2026-08-28 | `BZ` | 73 | — | $18.15 | +0.00 | $17.80 | -25.55 | -25.55 | +0.00 | -25.55 |
| 2026-08-28 | `LVWR` | 960 | — | $1.39 | +0.00 | $1.35 | -38.40 | -38.40 | +0.00 | -38.40 |
| 2026-08-31 | `MOS` | 55 | $23.60 | $23.68 | +4.40 | — | +0.00 | +4.40 | -17.60 | — |
| 2026-08-31 | `SEDG` | 40 | $31.41 | $31.15 | -10.40 | — | +0.00 | -10.40 | -70.00 | — |
| 2026-08-31 | `GRRR` | 85 | $14.41 | $14.44 | +2.55 | — | +0.00 | +2.55 | -103.70 | — |
| 2026-08-31 | `URBN` | 16 | $81.09 | $80.44 | -10.40 | — | +0.00 | -10.40 | +16.32 | — |
| 2026-08-31 | `SIMO` | 5 | $245.81 | $247.05 | +6.20 | — | +0.00 | +6.20 | -25.95 | — |
| 2026-08-31 | `BHVN` | 84 | $15.41 | $15.46 | +4.20 | — | +0.00 | +4.20 | -35.28 | — |
| 2026-08-31 | `BZ` | 73 | $17.80 | $17.70 | -7.30 | — | +0.00 | -7.30 | -32.85 | — |
| 2026-08-31 | `LVWR` | 960 | $1.35 | $1.30 | -48.00 | — | +0.00 | -48.00 | -86.40 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `CRK` | 83 | — | $15.45 | +0.00 | $14.95 | -41.50 | -41.50 | +0.00 | -41.50 |
| 2026-09-03 | `MRNA` | 8 | — | $145.94 | +0.00 | $148.87 | +23.40 | +23.40 | +0.00 | +23.40 |
| 2026-09-03 | `EIX` | 23 | — | $55.42 | +0.00 | $56.30 | +20.24 | +20.24 | +0.00 | +20.24 |
| 2026-09-03 | `SAFX` | 3401 | — | $0.38 | +0.00 | $0.38 | +6.80 | +6.80 | +0.00 | +6.80 |
| 2026-09-03 | `FRVO` | 70 | — | $18.28 | +0.00 | $17.16 | -78.40 | -78.40 | +0.00 | -78.40 |
| 2026-09-03 | `DEFT` | 1972 | — | $0.65 | +0.00 | $0.68 | +57.19 | +57.19 | +0.00 | +57.19 |
| 2026-09-03 | `GMRS` | 99 | — | $12.83 | +0.00 | $13.41 | +57.42 | +57.42 | +0.00 | +57.42 |
| 2026-09-03 | `KLRA` | 80 | — | $15.95 | +0.00 | $15.74 | -16.80 | -16.80 | +0.00 | -16.80 |
| 2026-09-04 | `CRK` | 83 | $14.95 | $15.00 | +4.15 | — | +0.00 | +4.15 | -37.35 | — |
| 2026-09-04 | `MRNA` | 8 | $148.87 | $153.62 | +38.00 | — | +0.00 | +38.00 | +61.40 | — |
| 2026-09-04 | `EIX` | 23 | $56.30 | $55.79 | -11.73 | — | +0.00 | -11.73 | +8.51 | — |
| 2026-09-04 | `SAFX` | 3401 | $0.38 | $0.38 | -3.40 | — | +0.00 | -3.40 | +3.40 | — |
| 2026-09-04 | `FRVO` | 70 | $17.16 | $17.27 | +7.70 | — | +0.00 | +7.70 | -70.70 | — |
| 2026-09-04 | `DEFT` | 1972 | $0.68 | $0.69 | +21.69 | — | +0.00 | +21.69 | +78.88 | — |
| 2026-09-04 | `GMRS` | 99 | $13.41 | $13.29 | -11.88 | — | +0.00 | -11.88 | +45.54 | — |
| 2026-09-04 | `KLRA` | 80 | $15.74 | $15.60 | -11.20 | — | +0.00 | -11.20 | -28.00 | — |
| 2026-09-04 | `CABA` | 368 | — | $3.46 | +0.00 | $3.47 | +3.68 | +3.68 | +0.00 | +3.68 |
| 2026-09-04 | `ALEC` | 506 | — | $2.52 | +0.00 | $2.46 | -30.36 | -30.36 | +0.00 | -30.36 |
| 2026-09-04 | `BHC` | 190 | — | $6.71 | +0.00 | $6.56 | -28.50 | -28.50 | +0.00 | -28.50 |
| 2026-09-04 | `BMEA` | 671 | — | $1.90 | +0.00 | $2.03 | +87.23 | +87.23 | +0.00 | +87.23 |
| 2026-09-04 | `OABI` | 266 | — | $4.78 | +0.00 | $4.33 | -119.70 | -119.70 | +0.00 | -119.70 |
| 2026-09-04 | `OPK` | 802 | — | $1.59 | +0.00 | $1.64 | +40.10 | +40.10 | +0.00 | +40.10 |
| 2026-09-04 | `VIR` | 112 | — | $11.31 | +0.00 | $11.38 | +8.40 | +8.40 | +0.00 | +8.40 |
| 2026-09-04 | `ATRC` | 24 | — | $52.03 | +0.00 | $51.52 | -12.24 | -12.24 | +0.00 | -12.24 |
| 2026-09-08 | `CABA` | 368 | $3.47 | $3.43 | -14.72 | — | +0.00 | -14.72 | -11.04 | — |
| 2026-09-08 | `ALEC` | 506 | $2.46 | $2.38 | -40.48 | — | +0.00 | -40.48 | -70.84 | — |
| 2026-09-08 | `BHC` | 190 | $6.56 | $6.57 | +1.90 | — | +0.00 | +1.90 | -26.60 | — |
| 2026-09-08 | `BMEA` | 671 | $2.03 | $2.00 | -20.13 | — | +0.00 | -20.13 | +67.10 | — |
| 2026-09-08 | `OABI` | 266 | $4.33 | $4.30 | -7.98 | — | +0.00 | -7.98 | -127.68 | — |
| 2026-09-08 | `OPK` | 802 | $1.64 | $1.63 | -8.02 | — | +0.00 | -8.02 | +32.08 | — |
| 2026-09-08 | `VIR` | 112 | $11.38 | $11.22 | -18.48 | — | +0.00 | -18.48 | -10.08 | — |
| 2026-09-08 | `ATRC` | 24 | $51.52 | $54.31 | +66.96 | — | +0.00 | +66.96 | +54.72 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +115.74 | TGTX, SLS, HIMS, VOR | — | $28.15 | $10,106.28 | TGTX×50, SLS×213, HIMS×84, VOR×113 |
| 2026-08-14 | +5.50 | $28.15 | TGTX×50, SLS×213, HIMS×84, VOR×113 | $10,117.74 | +11.46 | +178.77 | TLN, NRG, MARA, ARX, HLIT, SECZ, LFTO, REZI | TGTX, SLS, HIMS, VOR | $274.27 | $10,268.88 | TLN×3, NRG×10, MARA×140, ARX×64, HLIT×95, SECZ×216, LFTO×61, REZI×61 |
| 2026-08-17 | +2.25 | $274.27 | TLN×3, NRG×10, MARA×140, ARX×64, HLIT×95, SECZ×216, LFTO×61, REZI×61 | $10,238.82 | -30.06 | -178.63 | TMC, TGB, ELF, DNN, CAPR, NU, INV, KLC | TLN, NRG, MARA, ARX, HLIT, SECZ, LFTO, REZI | $2.16 | $10,007.11 | TMC×315, TGB×151, ELF×14, DNN×394, CAPR×185, NU×82, INV×788, KLC×487 |
| 2026-08-18 | -6.20 | $2.16 | TMC×315, TGB×151, ELF×14, DNN×394, CAPR×185, NU×82, INV×788, KLC×487 | $9,848.81 | -158.30 | +0.00 | — | TMC, TGB, ELF, DNN, CAPR, NU, INV, KLC | $9,813.47 | $9,813.47 | — |
| 2026-08-19 | -7.20 | $9,813.47 | — | $9,813.47 | -0.00 | +0.00 | — | — | $9,813.47 | $9,813.47 | — |
| 2026-08-20 | +1.12 | $9,813.47 | — | $9,813.47 | -0.00 | +120.31 | BHP, MRVI, CRCL, WYFI, TOYO, DVLT, SAFX, AAP | — | $73.21 | $9,872.25 | BHP×13, MRVI×164, CRCL×14, WYFI×57, TOYO×276, DVLT×4088, SAFX×3465, AAP×26 |
| 2026-08-21 | +3.25 | $73.21 | BHP×13, MRVI×164, CRCL×14, WYFI×57, TOYO×276, DVLT×4088, SAFX×3465, AAP×26 | $10,009.36 | +137.11 | +306.15 | AUTL, CRDL, CRSP, FUTU, GMAB, ENHA, CAN, PRQR | BHP, MRVI, CRCL, WYFI, TOYO, DVLT, SAFX, AAP | $88.98 | $10,189.78 | AUTL×503, CRDL×644, CRSP×20, FUTU×10, GMAB×37, ENHA×727, CAN×4228, PRQR×545 |
| 2026-08-24 | -5.17 | $88.98 | AUTL×503, CRDL×644, CRSP×20, FUTU×10, GMAB×37, ENHA×727, CAN×4228, PRQR×545 | $10,271.29 | +81.51 | -33.50 | — | AUTL, CRDL, FUTU, GMAB, ENHA, CAN, PRQR | $9,030.89 | $10,172.39 | CRSP×20 |
| 2026-08-25 | +1.80 | $9,030.89 | CRSP×20 | $10,189.49 | +17.10 | +274.75 | MOS, OCUL, INSP, RZLT, HCA, CAPR, PUSA, CYPH | CRSP | $473.67 | $10,432.96 | MOS×53, OCUL×115, INSP×20, RZLT×257, HCA×2, CAPR×175, PUSA×335, CYPH×816 |
| 2026-08-26 | +2.02 | $473.67 | MOS×53, OCUL×115, INSP×20, RZLT×257, HCA×2, CAPR×175, PUSA×335, CYPH×816 | $10,416.09 | -16.87 | +161.41 | FLNC, AVEX, AXTI, INDP, NVTS, IRDM | MOS, RZLT, HCA, CAPR, PUSA, CYPH | $16.44 | $10,525.66 | OCUL×115, INSP×20, FLNC×119, AVEX×75, AXTI×20, INDP×1215, NVTS×105, IRDM×28 |
| 2026-08-27 | — | $16.44 | OCUL×115, INSP×20, FLNC×119, AVEX×75, AXTI×20, INDP×1215, NVTS×105, IRDM×28 | $10,725.75 | +200.09 | -8.44 | MOS, ACMR, MT, TX, DLO, LRCX, NVDA | OCUL, INSP, FLNC, AVEX, AXTI, INDP, NVTS, IRDM | $1,736.65 | $10,671.32 | MOS×55, ACMR×16, MT×17, TX×24, DLO×87, LRCX×4, NVDA×5 |
| 2026-08-28 | +0.75 | $1,736.65 | MOS×55, ACMR×16, MT×17, TX×24, DLO×87, LRCX×4, NVDA×5 | $10,677.58 | +6.26 | -293.96 | SEDG, GRRR, URBN, SIMO, BHVN, BZ, LVWR | ACMR, MT, TX, DLO, LRCX, NVDA | $150.29 | $10,345.87 | MOS×55, SEDG×40, GRRR×85, URBN×16, SIMO×5, BHVN×84, BZ×73, LVWR×960 |
| 2026-08-31 | -5.85 | $150.29 | MOS×55, SEDG×40, GRRR×85, URBN×16, SIMO×5, BHVN×84, BZ×73, LVWR×960 | $10,287.12 | -58.75 | +0.00 | — | MOS, SEDG, GRRR, URBN, SIMO, BHVN, BZ, LVWR | $10,259.41 | $10,259.41 | — |
| 2026-09-01 | -6.30 | $10,259.41 | — | $10,259.41 | -0.00 | +0.00 | — | — | $10,259.41 | $10,259.41 | — |
| 2026-09-02 | -3.83 | $10,259.41 | — | $10,259.41 | -0.00 | +0.00 | — | — | $10,259.41 | $10,259.41 | — |
| 2026-09-03 | -0.90 | $10,259.41 | — | $10,259.41 | -0.00 | +28.35 | CRK, MRNA, EIX, SAFX, FRVO, DEFT, GMRS, KLRA | — | $90.30 | $10,232.97 | CRK×83, MRNA×8, EIX×23, SAFX×3401, FRVO×70, DEFT×1972, GMRS×99, KLRA×80 |
| 2026-09-04 | +2.25 | $90.30 | CRK×83, MRNA×8, EIX×23, SAFX×3401, FRVO×70, DEFT×1972, GMRS×99, KLRA×80 | $10,266.30 | +33.33 | -51.39 | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC | CRK, MRNA, EIX, SAFX, FRVO, DEFT, GMRS, KLRA | $8.68 | $10,117.59 | CABA×368, ALEC×506, BHC×190, BMEA×671, OABI×266, OPK×802, VIR×112, ATRC×24 |
| 2026-09-08 | -11.47 | $8.68 | CABA×368, ALEC×506, BHC×190, BMEA×671, OABI×266, OPK×802, VIR×112, ATRC×24 | $10,076.64 | -40.95 | +0.00 | — | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC | $10,035.41 | $10,035.41 | — |
| 2026-09-09 | -13.95 | $10,035.41 | — | $10,035.41 | +0.00 | +0.00 | — | — | $10,035.41 | $10,035.41 | — |
| 2026-09-10 | -13.28 | $10,035.41 | — | $10,035.41 | +0.00 | +0.00 | — | — | $10,035.41 | $10,035.41 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 50 | $49.70 | $2.14 | — | $7,512.86 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 213 | $11.70 | $2.75 | — | $5,018.01 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 84 | $29.74 | $2.24 | — | $2,517.61 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 113 | $22.01 | $2.33 | — | $28.15 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.15 | ▲ close $10,106.28 vs 09:30 $10,000.00 (session +115.74) | 16:00 close · cash $28.15 · equity $10,106.28 vs 09:30 $10,000.00 (+106.28; session marks +115.74) · 4 name(s) marked open→close (per-name table). TGTX×50 09:30 $49.70 → close $47.94 -88.00; SLS×213 09:30 $11.70 → close $12.36 +140.58; HIMS×84 09:30 $29.74 → close $28.77 -81.48; VOR×113 09:30 $22.01 → close $23.29 +144.64 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.15 | ▲ 09:30 equity $10,117.74 vs yday $10,106.28 (+11.46) | 09:30 open · cash $28.15 (unchanged overnight, no fees) · equity $10,117.74 vs prior close $10,106.28 (+11.46) · 4 name(s) re-marked at the open (per-name table). TGTX×50 yday $47.94 → 09:30 $47.27 -33.50; SLS×213 yday $12.36 → 09:30 $12.40 +8.52; HIMS×84 yday $28.77 → 09:30 $29.15 +31.92; VOR×113 yday $23.29 → 09:30 $23.33 +4.52 | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 50 | $47.27 | $2.17 | $-125.81 | $2,389.48 | ▼ -125.81 after sell → book $10,115.57; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 213 | $12.40 | $2.80 | $+143.55 | $5,027.88 | ▲ +143.55 after sell → book $10,112.77; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 84 | $29.15 | $2.28 | $-54.08 | $7,474.20 | ▼ -54.08 after sell → book $10,110.49; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 113 | $23.33 | $2.37 | $+144.46 | $10,108.12 | ▲ +144.46 after sell → book $10,108.12; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,026.63 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+5.9; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $7,824.61 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+0.6; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $6,560.80 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 64 | $19.57 | $2.18 | — | $5,306.14 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 95 | $13.18 | $2.27 | — | $4,051.77 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SECZ` | 216 | $5.84 | $2.79 | — | $2,787.54 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-20.7; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LFTO` | 61 | $20.57 | $2.17 | — | $1,530.60 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-14.0; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `REZI` | 61 | $20.56 | $2.17 | — | $274.27 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-21.5; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $274.27 | ▲ close $10,268.88 vs 09:30 $10,117.74 (session +178.77) | 16:00 close · cash $274.27 · equity $10,268.88 vs 09:30 $10,117.74 (+151.14; session marks +178.77) · 8 name(s) marked open→close (per-name table). TLN×3 09:30 $359.83 → close $362.74 +8.73; NRG×10 09:30 $120.00 → close $126.24 +62.40; MARA×140 09:30 $9.01 → close $9.20 +26.60; ARX×64 09:30 $19.57 → close $19.58 +0.64; HLIT×95 09:30 $13.18 → close $13.92 +70.30; SECZ×216 09:30 $5.84 → close $5.61 -49.68; LFTO×61 09:30 $20.57 → close $21.61 +63.44; REZI×61 09:30 $20.56 → close $20.50 -3.66 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $274.27 | ▼ 09:30 equity $10,238.82 vs yday $10,268.88 (-30.06) | 09:30 open · cash $274.27 (unchanged overnight, no fees) · equity $10,238.82 vs prior close $10,268.88 (-30.06) · 8 name(s) re-marked at the open (per-name table). TLN×3 yday $362.74 → 09:30 $367.88 +15.42; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; MARA×140 yday $9.20 → 09:30 $9.22 +2.80; ARX×64 yday $19.58 → 09:30 $19.57 -0.64; HLIT×95 yday $13.92 → 09:30 $13.84 -7.60; SECZ×216 yday $5.61 → 09:30 $5.45 -34.56; LFTO×61 yday $21.61 → 09:30 $21.00 -37.21; REZI×61 yday $20.50 → 09:30 $20.83 +20.13 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,375.89 | ▲ +20.13 after sell → book $10,236.80; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $2,647.85 | ▲ +69.94 after sell → book $10,234.76; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 140 | $9.22 | $2.44 | $+24.55 | $3,936.20 | ▲ +24.55 after sell → book $10,232.31; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 64 | $19.57 | $2.20 | $-4.38 | $5,186.48 | ▼ -4.38 after sell → book $10,230.11; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 95 | $13.84 | $2.30 | $+58.12 | $6,498.98 | ▲ +58.12 after sell → book $10,227.81; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SECZ` | 216 | $5.45 | $2.83 | $-89.86 | $7,673.35 | ▼ -89.86 after sell → book $10,224.98; vs 09:30 mark -2.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LFTO` | 61 | $21.00 | $2.19 | $+21.86 | $8,952.15 | ▲ +21.86 after sell → book $10,222.78; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `REZI` | 61 | $20.83 | $2.19 | $+12.10 | $10,220.59 | ▲ +12.10 after sell → book $10,220.59; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 315 | $4.05 | $4.06 | — | $8,940.78 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 151 | $8.46 | $2.44 | — | $7,660.87 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $6,391.28 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=-7.2; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 394 | $3.24 | $5.08 | — | $5,109.64 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 185 | $6.87 | $2.54 | — | $3,836.14 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+62.6; leftover $1277.57 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NU` | 82 | $15.40 | $2.24 | — | $2,571.11 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 788 | $1.62 | $10.17 | — | $1,284.38 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `KLC` | 487 | $2.62 | $6.28 | — | $2.16 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.16 | ▼ close $10,007.11 vs 09:30 $10,238.82 (session -178.63) | 16:00 close · cash $2.16 · equity $10,007.11 vs 09:30 $10,238.82 (-231.71; session marks -178.63) · 8 name(s) marked open→close (per-name table). TMC×315 09:30 $4.05 → close $3.77 -88.20; TGB×151 09:30 $8.46 → close $8.77 +46.81; ELF×14 09:30 $90.54 → close $93.66 +43.68; DNN×394 09:30 $3.24 → close $3.19 -19.70; CAPR×185 09:30 $6.87 → close $7.45 +107.30; NU×82 09:30 $15.40 → close $14.74 -54.12; INV×788 09:30 $1.62 → close $1.39 -185.18; KLC×487 09:30 $2.62 → close $2.56 -29.22 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.16 | ▼ 09:30 equity $9,848.81 vs yday $10,007.11 (-158.30) | 09:30 open · cash $2.16 (unchanged overnight, no fees) · equity $9,848.81 vs prior close $10,007.11 (-158.30) · 8 name(s) re-marked at the open (per-name table). TMC×315 yday $3.77 → 09:30 $3.72 -15.75; TGB×151 yday $8.77 → 09:30 $8.55 -33.22; ELF×14 yday $93.66 → 09:30 $93.44 -3.08; DNN×394 yday $3.19 → 09:30 $3.11 -31.52; CAPR×185 yday $7.45 → 09:30 $7.50 +9.25; NU×82 yday $14.74 → 09:30 $14.53 -17.22; INV×788 yday $1.39 → 09:30 $1.32 -47.28; KLC×487 yday $2.56 → 09:30 $2.52 -19.48 | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 315 | $3.72 | $4.13 | $-112.14 | $1,169.83 | ▼ -112.14 after sell → book $9,844.68; vs 09:30 mark -4.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 151 | $8.55 | $2.48 | $+8.67 | $2,458.41 | ▲ +8.67 after sell → book $9,842.21; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $3,764.51 | ▲ +36.52 after sell → book $9,840.15; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 394 | $3.11 | $5.16 | $-61.46 | $4,984.70 | ▼ -61.46 after sell → book $9,835.00; vs 09:30 mark -5.15 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `CAPR` | 185 | $7.50 | $2.59 | $+111.42 | $6,369.61 | ▲ +111.42 after sell → book $9,832.41; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NU` | 82 | $14.53 | $2.26 | $-75.84 | $7,558.81 | ▼ -75.84 after sell → book $9,830.15; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INV` | 788 | $1.32 | $10.31 | $-252.93 | $8,592.60 | ▼ -252.93 after sell → book $9,819.84; vs 09:30 mark -10.31 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `KLC` | 487 | $2.52 | $6.37 | $-61.36 | $9,813.47 | ▼ -61.36 after sell → book $9,813.47; vs 09:30 mark -6.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,813.47 | ▲ close $9,813.47 vs 09:30 $9,848.81 (session +0.00) | 16:00 close · cash $9,813.47 · no lots left · equity $9,813.47. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,813.47 | ▲ 09:30 equity $9,813.47 vs yday $9,813.47 (-0.00) | 09:30 open · cash $9,813.47 · no holdings · equity $9,813.47 vs prior close $9,813.47 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,813.47 | ▲ close $9,813.47 vs 09:30 $9,813.47 (session +0.00) | 16:00 close · cash $9,813.47 · no lots left · equity $9,813.47. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,813.47 | ▲ 09:30 equity $9,813.47 vs yday $9,813.47 (-0.00) | 09:30 open · cash $9,813.47 · no holdings · equity $9,813.47 vs prior close $9,813.47 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,628.31 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1226.68 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 164 | $7.44 | $2.48 | — | $7,405.67 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1226.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRCL` | 14 | $82.99 | $2.03 | — | $6,241.78 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable; 🔵; ⚪; ret5=+7.4; leftover $1226.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WYFI` | 57 | $21.40 | $2.16 | — | $5,019.82 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-25.2; leftover $1226.68 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 276 | $4.43 | $3.56 | — | $3,793.58 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-23.1; leftover $1226.68 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 4088 | $0.30 | $24.53 | — | $2,542.65 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-3.2; leftover $1226.68 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SAFX` | 3465 | $0.35 | $22.66 | — | $1,293.38 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-29.4; leftover $1226.68 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 26 | $46.85 | $2.07 | — | $73.21 | — | union ∩ last_red, no 🚨; gate last_red=True; list earn_react; 🔵; ret5=+5.0; leftover $1226.68 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.21 | ▲ close $9,872.25 vs 09:30 $9,813.47 (session +120.31) | 16:00 close · cash $73.21 · equity $9,872.25 vs 09:30 $9,813.47 (+58.78; session marks +120.31) · 8 name(s) marked open→close (per-name table). BHP×13 09:30 $91.01 → close $93.63 +34.06; MRVI×164 09:30 $7.44 → close $8.29 +139.40; CRCL×14 09:30 $82.99 → close $83.66 +9.38; WYFI×57 09:30 $21.40 → close $21.16 -13.68; TOYO×276 09:30 $4.43 → close $4.51 +23.46; DVLT×4088 09:30 $0.30 → close $0.32 +81.76; SAFX×3465 09:30 $0.35 → close $0.34 -38.11; AAP×26 09:30 $46.85 → close $42.39 -115.96 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.21 | ▲ 09:30 equity $10,009.36 vs yday $9,872.25 (+137.11) | 09:30 open · cash $73.21 (unchanged overnight, no fees) · equity $10,009.36 vs prior close $9,872.25 (+137.11) · 8 name(s) re-marked at the open (per-name table). BHP×13 yday $93.63 → 09:30 $95.72 +27.17; MRVI×164 yday $8.29 → 09:30 $8.28 -1.64; CRCL×14 yday $83.66 → 09:30 $87.98 +60.48; WYFI×57 yday $21.16 → 09:30 $21.54 +21.66; TOYO×276 yday $4.51 → 09:30 $4.68 +45.54; DVLT×4088 yday $0.32 → 09:30 $0.31 -40.88; SAFX×3465 yday $0.34 → 09:30 $0.35 +24.25; AAP×26 yday $42.39 → 09:30 $42.41 +0.52 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,315.52 | ▲ +57.15 after sell → book $10,007.31; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRVI` | 164 | $8.28 | $2.52 | $+132.76 | $2,670.92 | ▲ +132.76 after sell → book $10,004.79; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `CRCL` | 14 | $87.98 | $2.05 | $+65.78 | $3,900.59 | ▲ +65.78 after sell → book $10,002.74; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WYFI` | 57 | $21.54 | $2.18 | $+3.64 | $5,126.19 | ▲ +3.64 after sell → book $10,000.56; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 276 | $4.68 | $3.62 | $+61.82 | $6,414.25 | ▲ +61.82 after sell → book $9,996.94; vs 09:30 mark -3.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 4088 | $0.31 | $25.63 | $-9.27 | $7,655.90 | ▼ -9.27 after sell → book $9,971.31; vs 09:30 mark -25.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SAFX` | 3465 | $0.35 | $23.11 | $-59.63 | $8,845.55 | ▼ -59.63 after sell → book $9,948.21; vs 09:30 mark -23.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AAP` | 26 | $42.41 | $2.09 | $-119.60 | $9,946.12 | ▼ -119.60 after sell → book $9,946.12; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 503 | $2.47 | $6.49 | — | $8,697.22 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1243.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 644 | $1.93 | $8.31 | — | $7,445.99 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1243.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 20 | $59.72 | $2.05 | — | $6,249.54 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1243.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $5,095.72 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1243.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 37 | $33.36 | $2.10 | — | $3,859.30 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $1243.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 727 | $1.71 | $9.38 | — | $2,606.75 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1243.26 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 4228 | $0.29 | $25.11 | — | $1,338.61 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $1243.26 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PRQR` | 545 | $2.28 | $7.03 | — | $88.98 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+22.7; leftover $1243.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.98 | ▲ close $10,189.78 vs 09:30 $10,009.36 (session +306.15) | 16:00 close · cash $88.98 · equity $10,189.78 vs 09:30 $10,009.36 (+180.42; session marks +306.15) · 8 name(s) marked open→close (per-name table). AUTL×503 09:30 $2.47 → close $2.41 -30.18; CRDL×644 09:30 $1.93 → close $1.86 -45.08; CRSP×20 09:30 $59.72 → close $59.50 -4.40; FUTU×10 09:30 $115.18 → close $123.64 +84.60; GMAB×37 09:30 $33.36 → close $33.45 +3.33; ENHA×727 09:30 $1.71 → close $1.72 +7.27; CAN×4228 09:30 $0.29 → close $0.35 +257.91; PRQR×545 09:30 $2.28 → close $2.34 +32.70 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.98 | ▲ 09:30 equity $10,271.29 vs yday $10,189.78 (+81.51) | 09:30 open · cash $88.98 (unchanged overnight, no fees) · equity $10,271.29 vs prior close $10,189.78 (+81.51) · 8 name(s) re-marked at the open (per-name table). AUTL×503 yday $2.41 → 09:30 $2.40 -5.03; CRDL×644 yday $1.86 → 09:30 $1.88 +12.88; CRSP×20 yday $59.50 → 09:30 $58.75 -15.00; FUTU×10 yday $123.64 → 09:30 $121.00 -26.40; GMAB×37 yday $33.45 → 09:30 $32.82 -23.31; ENHA×727 yday $1.72 → 09:30 $1.74 +14.54; CAN×4228 yday $0.35 → 09:30 $0.38 +118.38; PRQR×545 yday $2.34 → 09:30 $2.35 +5.45 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 503 | $2.40 | $6.58 | $-48.28 | $1,289.59 | ▼ -48.28 after sell → book $10,264.71; vs 09:30 mark -6.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 644 | $1.88 | $8.42 | $-48.93 | $2,491.89 | ▼ -48.93 after sell → book $10,256.28; vs 09:30 mark -8.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $121.00 | $2.04 | $+54.14 | $3,699.85 | ▲ +54.14 after sell → book $10,254.24; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 37 | $32.82 | $2.12 | $-24.20 | $4,912.07 | ▼ -24.20 after sell → book $10,252.12; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ENHA` | 727 | $1.74 | $9.51 | $+2.92 | $6,167.54 | ▲ +2.92 after sell → book $10,242.61; vs 09:30 mark -9.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAN` | 4228 | $0.38 | $29.59 | $+321.59 | $7,757.27 | ▲ +321.59 after sell → book $10,213.02; vs 09:30 mark -29.59 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `PRQR` | 545 | $2.35 | $7.13 | $+23.99 | $9,030.89 | ▲ +23.99 after sell → book $10,205.89; vs 09:30 mark -7.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,030.89 | ▼ close $10,172.39 vs 09:30 $10,271.29 (session -33.50) | 16:00 close · cash $9,030.89 · equity $10,172.39 vs 09:30 $10,271.29 (-98.90; session marks -33.50) · 1 name(s) marked open→close (per-name table). CRSP×20 09:30 $58.75 → close $57.08 -33.50 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,030.89 | ▲ 09:30 equity $10,189.49 vs yday $10,172.39 (+17.10) | 09:30 open · cash $9,030.89 (unchanged overnight, no fees) · equity $10,189.49 vs prior close $10,172.39 (+17.10) · 1 name(s) re-marked at the open (per-name table). CRSP×20 yday $57.08 → 09:30 $57.93 +17.10 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.93 | $2.07 | $-39.92 | $10,187.42 | ▼ -39.92 after sell → book $10,187.42; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 53 | $23.77 | $2.15 | — | $8,925.46 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=+13.0; leftover $1273.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 115 | $10.98 | $2.33 | — | $7,660.43 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+1.2; leftover $1273.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.19 | $2.05 | — | $6,434.58 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+7.4; leftover $1273.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 257 | $4.94 | $3.32 | — | $5,161.68 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=+7.1; leftover $1273.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $426.97 | $2.00 | — | $4,305.74 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=+6.0; leftover $1273.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 175 | $7.25 | $2.52 | — | $3,034.48 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1273.43 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 335 | $3.80 | $4.32 | — | $1,757.16 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1273.43 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 816 | $1.56 | $10.53 | — | $473.67 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1273.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $473.67 | ▲ close $10,432.96 vs 09:30 $10,189.49 (session +274.75) | 16:00 close · cash $473.67 · equity $10,432.96 vs 09:30 $10,189.49 (+243.47; session marks +274.75) · 8 name(s) marked open→close (per-name table). MOS×53 09:30 $23.77 → close $24.27 +26.50; OCUL×115 09:30 $10.98 → close $10.88 -11.50; INSP×20 09:30 $61.19 → close $61.07 -2.40; RZLT×257 09:30 $4.94 → close $5.01 +17.99; HCA×2 09:30 $426.97 → close $428.76 +3.58; CAPR×175 09:30 $7.25 → close $8.29 +182.00; PUSA×335 09:30 $3.80 → close $3.78 -6.70; CYPH×816 09:30 $1.56 → close $1.64 +65.28 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $473.67 | ▼ 09:30 equity $10,416.09 vs yday $10,432.96 (-16.87) | 09:30 open · cash $473.67 (unchanged overnight, no fees) · equity $10,416.09 vs prior close $10,432.96 (-16.87) · 8 name(s) re-marked at the open (per-name table). MOS×53 yday $24.27 → 09:30 $24.84 +30.21; OCUL×115 yday $10.88 → 09:30 $10.79 -10.35; INSP×20 yday $61.07 → 09:30 $60.07 -20.00; RZLT×257 yday $5.01 → 09:30 $5.01 +0.00; HCA×2 yday $428.76 → 09:30 $427.50 -2.52; CAPR×175 yday $8.29 → 09:30 $8.29 +0.00; PUSA×335 yday $3.78 → 09:30 $3.83 +18.43; CYPH×816 yday $1.64 → 09:30 $1.60 -32.64 | — |
| 2026-08-26 09:30 ET | **SELL** | `MOS` | 53 | $24.84 | $2.17 | $+52.39 | $1,788.02 | ▲ +52.39 after sell → book $10,413.92; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `RZLT` | 257 | $5.01 | $3.37 | $+11.31 | $3,072.22 | ▲ +11.31 after sell → book $10,410.55; vs 09:30 mark -3.37 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `HCA` | 2 | $427.50 | $2.02 | $-2.95 | $3,925.21 | ▼ -2.95 after sell → book $10,408.53; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 175 | $8.29 | $2.56 | $+176.93 | $5,373.40 | ▲ +176.93 after sell → book $10,405.98; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `PUSA` | 335 | $3.83 | $4.39 | $+3.02 | $6,653.74 | ▲ +3.02 after sell → book $10,401.59; vs 09:30 mark -4.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 816 | $1.60 | $10.67 | $+11.44 | $7,948.67 | ▲ +11.44 after sell → book $10,390.92; vs 09:30 mark -10.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 119 | $11.12 | $2.35 | — | $6,623.04 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1324.78 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AVEX` | 75 | $17.51 | $2.21 | — | $5,307.58 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $1324.78 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AXTI` | 20 | $65.34 | $2.05 | — | $3,998.73 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1324.78 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `INDP` | 1215 | $1.09 | $15.67 | — | $2,658.70 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+17.0; leftover $1324.78 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NVTS` | 105 | $12.60 | $2.31 | — | $1,333.40 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=-5.5; leftover $1324.78 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `IRDM` | 28 | $46.96 | $2.07 | — | $16.44 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=-3.9; leftover $1324.78 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.44 | ▲ close $10,525.66 vs 09:30 $10,416.09 (session +161.41) | 16:00 close · cash $16.44 · equity $10,525.66 vs 09:30 $10,416.09 (+109.57; session marks +161.41) · 8 name(s) marked open→close (per-name table). OCUL×115 09:30 $10.79 → close $10.77 -2.30; INSP×20 09:30 $60.07 → close $61.80 +34.60; FLNC×119 09:30 $11.12 → close $11.08 -4.76; AVEX×75 09:30 $17.51 → close $18.34 +62.25; AXTI×20 09:30 $65.34 → close $65.18 -3.20; INDP×1215 09:30 $1.09 → close $1.14 +60.75; NVTS×105 09:30 $12.60 → close $12.67 +7.35; IRDM×28 09:30 $46.96 → close $47.20 +6.72 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.44 | ▲ 09:30 equity $10,725.75 vs yday $10,525.66 (+200.09) | 09:30 open · cash $16.44 (unchanged overnight, no fees) · equity $10,725.75 vs prior close $10,525.66 (+200.09) · 8 name(s) re-marked at the open (per-name table). OCUL×115 yday $10.77 → 09:30 $10.63 -16.10; INSP×20 yday $61.80 → 09:30 $62.10 +6.00; FLNC×119 yday $11.08 → 09:30 $11.52 +52.36; AVEX×75 yday $18.34 → 09:30 $18.43 +6.75; AXTI×20 yday $65.18 → 09:30 $70.30 +102.40; INDP×1215 yday $1.14 → 09:30 $1.13 -12.15; NVTS×105 yday $12.67 → 09:30 $13.18 +53.55; IRDM×28 yday $47.20 → 09:30 $47.46 +7.28 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 115 | $10.63 | $2.36 | $-44.95 | $1,236.53 | ▼ -44.95 after sell → book $10,723.39; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 20 | $62.10 | $2.07 | $+14.08 | $2,476.46 | ▲ +14.08 after sell → book $10,721.32; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 119 | $11.52 | $2.38 | $+42.88 | $3,844.96 | ▲ +42.88 after sell → book $10,718.94; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVEX` | 75 | $18.43 | $2.24 | $+64.55 | $5,224.97 | ▲ +64.55 after sell → book $10,716.70; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AXTI` | 20 | $70.30 | $2.07 | $+95.08 | $6,628.90 | ▲ +95.08 after sell → book $10,714.63; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INDP` | 1215 | $1.13 | $15.89 | $+17.04 | $7,985.97 | ▲ +17.04 after sell → book $10,698.75; vs 09:30 mark -15.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NVTS` | 105 | $13.18 | $2.33 | $+56.26 | $9,367.53 | ▲ +56.26 after sell → book $10,696.41; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `IRDM` | 28 | $47.46 | $2.09 | $+9.83 | $10,694.32 | ▲ +9.83 after sell → book $10,694.32; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 55 | $24.00 | $2.15 | — | $9,372.16 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=+8.7; leftover $1336.79 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 16 | $81.65 | $2.04 | — | $8,063.72 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=+2.0; leftover $1336.79 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 17 | $74.54 | $2.04 | — | $6,794.50 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-0.1; leftover $1336.79 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 24 | $55.25 | $2.06 | — | $5,466.44 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=+2.1; leftover $1336.79 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 87 | $15.33 | $2.25 | — | $4,130.48 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=+7.4; leftover $1336.79 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 4 | $318.88 | $2.00 | — | $2,852.96 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=+1.9; leftover $1336.79 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 5 | $222.86 | $2.00 | — | $1,736.65 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-3.6; leftover $1336.79 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,736.65 | ▼ close $10,671.32 vs 09:30 $10,725.75 (session -8.44) | 16:00 close · cash $1,736.65 · equity $10,671.32 vs 09:30 $10,725.75 (-54.43; session marks -8.44) · 7 name(s) marked open→close (per-name table). MOS×55 09:30 $24.00 → close $23.76 -13.20; ACMR×16 09:30 $81.65 → close $80.49 -18.56; MT×17 09:30 $74.54 → close $74.63 +1.53; TX×24 09:30 $55.25 → close $55.83 +13.92; DLO×87 09:30 $15.33 → close $15.14 -16.53; LRCX×4 09:30 $318.88 → close $318.58 -1.20; NVDA×5 09:30 $222.86 → close $227.98 +25.60 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,736.65 | ▲ 09:30 equity $10,677.58 vs yday $10,671.32 (+6.26) | 09:30 open · cash $1,736.65 (unchanged overnight, no fees) · equity $10,677.58 vs prior close $10,671.32 (+6.26) · 7 name(s) re-marked at the open (per-name table). MOS×55 yday $23.76 → 09:30 $23.95 +10.45; ACMR×16 yday $80.49 → 09:30 $79.27 -19.52; MT×17 yday $74.63 → 09:30 $75.39 +12.92; TX×24 yday $55.83 → 09:30 $55.97 +3.36; DLO×87 yday $15.14 → 09:30 $15.19 +4.35; LRCX×4 yday $318.58 → 09:30 $318.03 -2.20; NVDA×5 yday $227.98 → 09:30 $227.36 -3.10 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 16 | $79.27 | $2.06 | $-42.18 | $3,002.92 | ▼ -42.18 after sell → book $10,675.53; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 17 | $75.39 | $2.06 | $+10.35 | $4,282.48 | ▲ +10.35 after sell → book $10,673.46; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 24 | $55.97 | $2.08 | $+13.14 | $5,623.68 | ▲ +13.14 after sell → book $10,671.38; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 87 | $15.19 | $2.28 | $-16.71 | $6,942.94 | ▼ -16.71 after sell → book $10,669.11; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 4 | $318.03 | $2.02 | $-7.42 | $8,213.03 | ▼ -7.42 after sell → book $10,667.08; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 5 | $227.36 | $2.02 | $+18.47 | $9,347.81 | ▲ +18.47 after sell → book $10,665.06; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 40 | $32.90 | $2.11 | — | $8,029.70 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1335.40 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 85 | $15.66 | $2.25 | — | $6,696.35 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1335.40 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $79.42 | $2.04 | — | $5,423.60 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1335.40 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $4,160.39 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1335.40 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 84 | $15.88 | $2.24 | — | $2,824.23 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+19.4; leftover $1335.40 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 73 | $18.15 | $2.21 | — | $1,497.07 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+14.1; leftover $1335.40 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 960 | $1.39 | $12.38 | — | $150.29 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+20.4; leftover $1335.40 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $150.29 | ▼ close $10,345.87 vs 09:30 $10,677.58 (session -293.96) | 16:00 close · cash $150.29 · equity $10,345.87 vs 09:30 $10,677.58 (-331.71; session marks -293.96) · 8 name(s) marked open→close (per-name table). MOS×55 09:30 $23.95 → close $23.60 -19.25; SEDG×40 09:30 $32.90 → close $31.41 -59.60; GRRR×85 09:30 $15.66 → close $14.41 -106.25; URBN×16 09:30 $79.42 → close $81.09 +26.72; SIMO×5 09:30 $252.24 → close $245.81 -32.15; BHVN×84 09:30 $15.88 → close $15.41 -39.48; BZ×73 09:30 $18.15 → close $17.80 -25.55; LVWR×960 09:30 $1.39 → close $1.35 -38.40 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $150.29 | ▼ 09:30 equity $10,287.12 vs yday $10,345.87 (-58.75) | 09:30 open · cash $150.29 (unchanged overnight, no fees) · equity $10,287.12 vs prior close $10,345.87 (-58.75) · 8 name(s) re-marked at the open (per-name table). MOS×55 yday $23.60 → 09:30 $23.68 +4.40; SEDG×40 yday $31.41 → 09:30 $31.15 -10.40; GRRR×85 yday $14.41 → 09:30 $14.44 +2.55; URBN×16 yday $81.09 → 09:30 $80.44 -10.40; SIMO×5 yday $245.81 → 09:30 $247.05 +6.20; BHVN×84 yday $15.41 → 09:30 $15.46 +4.20; BZ×73 yday $17.80 → 09:30 $17.70 -7.30; LVWR×960 yday $1.35 → 09:30 $1.30 -48.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 55 | $23.68 | $2.18 | $-21.93 | $1,450.51 | ▼ -21.93 after sell → book $10,284.94; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 40 | $31.15 | $2.13 | $-74.24 | $2,694.38 | ▼ -74.24 after sell → book $10,282.81; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 85 | $14.44 | $2.27 | $-108.21 | $3,919.51 | ▼ -108.21 after sell → book $10,280.54; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $80.44 | $2.06 | $+12.22 | $5,204.49 | ▲ +12.22 after sell → book $10,278.48; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $6,437.72 | ▼ -29.98 after sell → book $10,276.46; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 84 | $15.46 | $2.27 | $-39.79 | $7,734.09 | ▼ -39.79 after sell → book $10,274.19; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 73 | $17.70 | $2.23 | $-37.29 | $9,023.96 | ▼ -37.29 after sell → book $10,271.96; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 960 | $1.30 | $12.55 | $-111.34 | $10,259.41 | ▼ -111.34 after sell → book $10,259.41; vs 09:30 mark -12.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,259.41 | ▲ close $10,259.41 vs 09:30 $10,287.12 (session +0.00) | 16:00 close · cash $10,259.41 · no lots left · equity $10,259.41. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,259.41 | ▲ 09:30 equity $10,259.41 vs yday $10,259.41 (-0.00) | 09:30 open · cash $10,259.41 · no holdings · equity $10,259.41 vs prior close $10,259.41 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,259.41 | ▲ close $10,259.41 vs 09:30 $10,259.41 (session +0.00) | 16:00 close · cash $10,259.41 · no lots left · equity $10,259.41. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,259.41 | ▲ 09:30 equity $10,259.41 vs yday $10,259.41 (-0.00) | 09:30 open · cash $10,259.41 · no holdings · equity $10,259.41 vs prior close $10,259.41 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,259.41 | ▲ close $10,259.41 vs 09:30 $10,259.41 (session +0.00) | 16:00 close · cash $10,259.41 · no lots left · equity $10,259.41. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,259.41 | ▲ 09:30 equity $10,259.41 vs yday $10,259.41 (-0.00) | 09:30 open · cash $10,259.41 · no holdings · equity $10,259.41 vs prior close $10,259.41 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 83 | $15.45 | $2.24 | — | $8,974.82 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1282.43 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $7,805.24 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1282.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 23 | $55.42 | $2.06 | — | $6,528.52 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; ret5=-25.9; leftover $1282.43 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SAFX` | 3401 | $0.38 | $23.02 | — | $5,223.32 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; ret5=-2.3; leftover $1282.43 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 70 | $18.28 | $2.20 | — | $3,941.52 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+16.5; leftover $1282.43 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1972 | $0.65 | $18.73 | — | $2,640.99 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+10.2; leftover $1282.43 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GMRS` | 99 | $12.83 | $2.29 | — | $1,368.53 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-0.2; leftover $1282.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `KLRA` | 80 | $15.95 | $2.23 | — | $90.30 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=-14.0; leftover $1282.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.30 | ▲ close $10,232.97 vs 09:30 $10,259.41 (session +28.35) | 16:00 close · cash $90.30 · equity $10,232.97 vs 09:30 $10,259.41 (-26.44; session marks +28.35) · 8 name(s) marked open→close (per-name table). CRK×83 09:30 $15.45 → close $14.95 -41.50; MRNA×8 09:30 $145.94 → close $148.87 +23.40; EIX×23 09:30 $55.42 → close $56.30 +20.24; SAFX×3401 09:30 $0.38 → close $0.38 +6.80; FRVO×70 09:30 $18.28 → close $17.16 -78.40; DEFT×1972 09:30 $0.65 → close $0.68 +57.19; GMRS×99 09:30 $12.83 → close $13.41 +57.42; KLRA×80 09:30 $15.95 → close $15.74 -16.80 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.30 | ▲ 09:30 equity $10,266.30 vs yday $10,232.97 (+33.33) | 09:30 open · cash $90.30 (unchanged overnight, no fees) · equity $10,266.30 vs prior close $10,232.97 (+33.33) · 8 name(s) re-marked at the open (per-name table). CRK×83 yday $14.95 → 09:30 $15.00 +4.15; MRNA×8 yday $148.87 → 09:30 $153.62 +38.00; EIX×23 yday $56.30 → 09:30 $55.79 -11.73; SAFX×3401 yday $0.38 → 09:30 $0.38 -3.40; FRVO×70 yday $17.16 → 09:30 $17.27 +7.70; DEFT×1972 yday $0.68 → 09:30 $0.69 +21.69; GMRS×99 yday $13.41 → 09:30 $13.29 -11.88; KLRA×80 yday $15.74 → 09:30 $15.60 -11.20 | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 83 | $15.00 | $2.26 | $-41.85 | $1,333.04 | ▼ -41.85 after sell → book $10,264.04; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $153.62 | $2.03 | $+57.35 | $2,559.96 | ▲ +57.35 after sell → book $10,262.00; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 23 | $55.79 | $2.08 | $+4.37 | $3,841.06 | ▲ +4.37 after sell → book $10,259.92; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SAFX` | 3401 | $0.38 | $23.63 | $-43.26 | $5,103.00 | ▼ -43.26 after sell → book $10,236.29; vs 09:30 mark -23.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 70 | $17.27 | $2.22 | $-75.12 | $6,309.68 | ▼ -75.12 after sell → book $10,234.07; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DEFT` | 1972 | $0.69 | $19.86 | $+40.28 | $7,650.50 | ▲ +40.28 after sell → book $10,214.21; vs 09:30 mark -19.86 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `GMRS` | 99 | $13.29 | $2.31 | $+40.94 | $8,963.89 | ▲ +40.94 after sell → book $10,211.89; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `KLRA` | 80 | $15.60 | $2.25 | $-32.48 | $10,209.64 | ▼ -32.48 after sell → book $10,209.64; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 368 | $3.46 | $4.75 | — | $8,931.61 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1276.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 506 | $2.52 | $6.53 | — | $7,649.97 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1276.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 190 | $6.71 | $2.56 | — | $6,372.51 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1276.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 671 | $1.90 | $8.66 | — | $5,088.95 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1276.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 266 | $4.78 | $3.43 | — | $3,814.04 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1276.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 802 | $1.59 | $10.35 | — | $2,528.51 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1276.20 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 112 | $11.31 | $2.33 | — | $1,259.47 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1276.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 24 | $52.03 | $2.06 | — | $8.68 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $1276.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.68 | ▼ close $10,117.59 vs 09:30 $10,266.30 (session -51.39) | 16:00 close · cash $8.68 · equity $10,117.59 vs 09:30 $10,266.30 (-148.71; session marks -51.39) · 8 name(s) marked open→close (per-name table). CABA×368 09:30 $3.46 → close $3.47 +3.68; ALEC×506 09:30 $2.52 → close $2.46 -30.36; BHC×190 09:30 $6.71 → close $6.56 -28.50; BMEA×671 09:30 $1.90 → close $2.03 +87.23; OABI×266 09:30 $4.78 → close $4.33 -119.70; OPK×802 09:30 $1.59 → close $1.64 +40.10; VIR×112 09:30 $11.31 → close $11.38 +8.40; ATRC×24 09:30 $52.03 → close $51.52 -12.24 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.68 | ▼ 09:30 equity $10,076.64 vs yday $10,117.59 (-40.95) | 09:30 open · cash $8.68 (unchanged overnight, no fees) · equity $10,076.64 vs prior close $10,117.59 (-40.95) · 8 name(s) re-marked at the open (per-name table). CABA×368 yday $3.47 → 09:30 $3.43 -14.72; ALEC×506 yday $2.46 → 09:30 $2.38 -40.48; BHC×190 yday $6.56 → 09:30 $6.57 +1.90; BMEA×671 yday $2.03 → 09:30 $2.00 -20.13; OABI×266 yday $4.33 → 09:30 $4.30 -7.98; OPK×802 yday $1.64 → 09:30 $1.63 -8.02; VIR×112 yday $11.38 → 09:30 $11.22 -18.48; ATRC×24 yday $51.52 → 09:30 $54.31 +66.96 | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 368 | $3.43 | $4.82 | $-20.61 | $1,266.11 | ▼ -20.61 after sell → book $10,071.83; vs 09:30 mark -4.81 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 506 | $2.38 | $6.62 | $-83.99 | $2,463.76 | ▼ -83.99 after sell → book $10,065.20; vs 09:30 mark -6.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 190 | $6.57 | $2.60 | $-31.76 | $3,709.46 | ▼ -31.76 after sell → book $10,062.60; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 671 | $2.00 | $8.78 | $+49.67 | $5,042.68 | ▲ +49.67 after sell → book $10,053.82; vs 09:30 mark -8.78 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 266 | $4.30 | $3.49 | $-134.60 | $6,183.00 | ▼ -134.60 after sell → book $10,050.34; vs 09:30 mark -3.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 802 | $1.63 | $10.49 | $+11.24 | $7,479.77 | ▲ +11.24 after sell → book $10,039.85; vs 09:30 mark -10.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 112 | $11.22 | $2.35 | $-14.76 | $8,734.06 | ▼ -14.76 after sell → book $10,037.50; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 24 | $54.31 | $2.08 | $+50.58 | $10,035.41 | ▲ +50.58 after sell → book $10,035.41; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,035.41 | ▲ close $10,035.41 vs 09:30 $10,076.64 (session +0.00) | 16:00 close · cash $10,035.41 · no lots left · equity $10,035.41. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,035.41 | ▲ 09:30 equity $10,035.41 vs yday $10,035.41 (+0.00) | 09:30 open · cash $10,035.41 · no holdings · equity $10,035.41 vs prior close $10,035.41 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,035.41 | ▲ close $10,035.41 vs 09:30 $10,035.41 (session +0.00) | 16:00 close · cash $10,035.41 · no lots left · equity $10,035.41. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,035.41 | ▲ 09:30 equity $10,035.41 vs yday $10,035.41 (+0.00) | 09:30 open · cash $10,035.41 · no holdings · equity $10,035.41 vs prior close $10,035.41 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,035.41 | ▲ close $10,035.41 vs 09:30 $10,035.41 (session +0.00) | 16:00 close · cash $10,035.41 · no lots left · equity $10,035.41. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `TBPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENVX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STUB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `FN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PAAS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1336.79 < 1 share @ 1746.53 |
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
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FOX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BEP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VLRS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ORBS` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `PURR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CNTB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HAS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BHC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ZJYL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new buys |
