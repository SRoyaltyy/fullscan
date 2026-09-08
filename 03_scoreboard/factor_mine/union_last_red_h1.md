# Factor mine action — `union_last_red_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ last_red, no 🚨

Cash book **+1.64%** ($10,164) · signal-only (no cash/fees) was +8.15%. Starts YES **9/18**. Fills 154 · skips 55 · realized $+53.60.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $84.28.

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
| 2026-08-20 | `MRVI` | 166 | — | $7.38 | +0.00 | $8.26 | +146.08 | +146.08 | +0.00 | +146.08 |
| 2026-08-20 | `CRCL` | 14 | — | $83.29 | +0.00 | $83.99 | +9.80 | +9.80 | +0.00 | +9.80 |
| 2026-08-20 | `WYFI` | 57 | — | $21.40 | +0.00 | $21.16 | -13.68 | -13.68 | +0.00 | -13.68 |
| 2026-08-20 | `TOYO` | 276 | — | $4.43 | +0.00 | $4.51 | +23.46 | +23.46 | +0.00 | +23.46 |
| 2026-08-20 | `DVLT` | 4088 | — | $0.30 | +0.00 | $0.32 | +81.76 | +81.76 | +0.00 | +81.76 |
| 2026-08-20 | `SAFX` | 3465 | — | $0.35 | +0.00 | $0.34 | -38.11 | -38.11 | +0.00 | -38.11 |
| 2026-08-20 | `AAP` | 26 | — | $46.85 | +0.00 | $42.39 | -115.96 | -115.96 | +0.00 | -115.96 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `MRVI` | 166 | $8.26 | $8.20 | -9.96 | $8.70 | +83.00 | +73.04 | +136.12 | +219.12 |
| 2026-08-21 | `CRCL` | 14 | $83.99 | $87.65 | +51.24 | — | +0.00 | +51.24 | +61.04 | — |
| 2026-08-21 | `WYFI` | 57 | $21.16 | $21.54 | +21.66 | — | +0.00 | +21.66 | +7.98 | — |
| 2026-08-21 | `TOYO` | 276 | $4.51 | $4.68 | +45.54 | — | +0.00 | +45.54 | +69.00 | — |
| 2026-08-21 | `DVLT` | 4088 | $0.32 | $0.31 | -40.88 | — | +0.00 | -40.88 | +40.88 | — |
| 2026-08-21 | `SAFX` | 3465 | $0.34 | $0.35 | +24.25 | — | +0.00 | +24.25 | -13.86 | — |
| 2026-08-21 | `AAP` | 26 | $42.39 | $42.41 | +0.52 | — | +0.00 | +0.52 | -115.44 | — |
| 2026-08-21 | `AUTL` | 496 | — | $2.47 | +0.00 | $2.41 | -29.76 | -29.76 | +0.00 | -29.76 |
| 2026-08-21 | `CRDL` | 634 | — | $1.93 | +0.00 | $1.86 | -44.38 | -44.38 | +0.00 | -44.38 |
| 2026-08-21 | `CRSP` | 20 | — | $59.72 | +0.00 | $59.50 | -4.40 | -4.40 | +0.00 | -4.40 |
| 2026-08-21 | `FUTU` | 10 | — | $115.18 | +0.00 | $123.64 | +84.60 | +84.60 | +0.00 | +84.60 |
| 2026-08-21 | `GMAB` | 36 | — | $33.36 | +0.00 | $33.45 | +3.24 | +3.24 | +0.00 | +3.24 |
| 2026-08-21 | `ENHA` | 716 | — | $1.71 | +0.00 | $1.72 | +7.16 | +7.16 | +0.00 | +7.16 |
| 2026-08-21 | `CAN` | 4167 | — | $0.29 | +0.00 | $0.35 | +254.19 | +254.19 | +0.00 | +254.19 |
| 2026-08-24 | `MRVI` | 166 | $8.70 | $8.59 | -18.26 | — | +0.00 | -18.26 | +200.86 | — |
| 2026-08-24 | `AUTL` | 496 | $2.41 | $2.36 | -24.80 | — | +0.00 | -24.80 | -54.56 | — |
| 2026-08-24 | `CRDL` | 634 | $1.86 | $1.87 | +6.34 | — | +0.00 | +6.34 | -38.04 | — |
| 2026-08-24 | `CRSP` | 20 | $59.50 | $58.79 | -14.20 | $56.91 | -37.60 | -51.80 | -18.60 | -56.20 |
| 2026-08-24 | `FUTU` | 10 | $123.64 | $120.87 | -27.70 | — | +0.00 | -27.70 | +56.90 | — |
| 2026-08-24 | `GMAB` | 36 | $33.45 | $32.82 | -22.68 | — | +0.00 | -22.68 | -19.44 | — |
| 2026-08-24 | `ENHA` | 716 | $1.72 | $1.74 | +14.32 | — | +0.00 | +14.32 | +21.48 | — |
| 2026-08-24 | `CAN` | 4167 | $0.35 | $0.38 | +104.18 | — | +0.00 | +104.18 | +358.36 | — |
| 2026-08-25 | `CRSP` | 20 | $56.91 | $57.00 | +1.80 | — | +0.00 | +1.80 | -54.40 | — |
| 2026-08-25 | `OCUL` | 116 | — | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CRMD` | 153 | — | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `PUSA` | 343 | — | $3.70 | +0.00 | $3.91 | +72.03 | +72.03 | +0.00 | +72.03 |
| 2026-08-25 | `CAPR` | 186 | — | $6.79 | +0.00 | $7.19 | +74.40 | +74.40 | +0.00 | +74.40 |
| 2026-08-25 | `SAFX` | 3431 | — | $0.37 | +0.00 | $0.37 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `SUJA` | 144 | — | $8.79 | +0.00 | $8.54 | -36.00 | -36.00 | +0.00 | -36.00 |
| 2026-08-25 | `FWDI` | 211 | — | $5.99 | +0.00 | $5.86 | -27.43 | -27.43 | +0.00 | -27.43 |
| 2026-08-25 | `JANX` | 67 | — | $18.52 | +0.00 | $18.99 | +31.49 | +31.49 | +0.00 | +31.49 |
| 2026-08-26 | `OCUL` | 116 | $10.92 | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `CRMD` | 153 | $8.28 | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `PUSA` | 343 | $3.91 | $3.91 | +0.00 | $3.91 | +0.00 | +0.00 | +72.03 | +72.03 |
| 2026-08-26 | `CAPR` | 186 | $7.19 | $7.19 | +0.00 | $7.19 | +0.00 | +0.00 | +74.40 | +74.40 |
| 2026-08-26 | `SAFX` | 3431 | $0.37 | $0.37 | +0.00 | $0.37 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `SUJA` | 144 | $8.54 | $8.54 | +0.00 | $8.54 | +0.00 | +0.00 | -36.00 | -36.00 |
| 2026-08-26 | `FWDI` | 211 | $5.86 | $5.86 | +0.00 | $5.86 | +0.00 | +0.00 | -27.43 | -27.43 |
| 2026-08-26 | `JANX` | 67 | $18.99 | $18.99 | +0.00 | $18.99 | +0.00 | +0.00 | +31.49 | +31.49 |
| 2026-08-27 | `OCUL` | 116 | $10.92 | $10.79 | -15.08 | — | +0.00 | -15.08 | -15.08 | — |
| 2026-08-27 | `CRMD` | 153 | $8.28 | $8.60 | +48.96 | — | +0.00 | +48.96 | +48.96 | — |
| 2026-08-27 | `PUSA` | 343 | $3.91 | $3.84 | -24.01 | — | +0.00 | -24.01 | +48.02 | — |
| 2026-08-27 | `CAPR` | 186 | $7.19 | $8.29 | +204.60 | — | +0.00 | +204.60 | +279.00 | — |
| 2026-08-27 | `SAFX` | 3431 | $0.37 | $0.35 | -68.62 | — | +0.00 | -68.62 | -68.62 | — |
| 2026-08-27 | `SUJA` | 144 | $8.54 | $9.39 | +122.40 | — | +0.00 | +122.40 | +86.40 | — |
| 2026-08-27 | `FWDI` | 211 | $5.86 | $5.97 | +23.21 | — | +0.00 | +23.21 | -4.22 | — |
| 2026-08-27 | `JANX` | 67 | $18.99 | $18.59 | -26.80 | — | +0.00 | -26.80 | +4.69 | — |
| 2026-08-27 | `ACMR` | 16 | — | $80.97 | +0.00 | $79.11 | -29.76 | -29.76 | +0.00 | -29.76 |
| 2026-08-27 | `GGB` | 295 | — | $4.42 | +0.00 | $4.46 | +11.80 | +11.80 | +0.00 | +11.80 |
| 2026-08-27 | `MT` | 17 | — | $75.12 | +0.00 | $74.53 | -10.03 | -10.03 | +0.00 | -10.03 |
| 2026-08-27 | `MU` | 1 | — | $925.74 | +0.00 | $938.40 | +12.66 | +12.66 | +0.00 | +12.66 |
| 2026-08-27 | `TX` | 23 | — | $55.20 | +0.00 | $55.13 | -1.61 | -1.61 | +0.00 | -1.61 |
| 2026-08-27 | `LRCX` | 4 | — | $314.61 | +0.00 | $312.88 | -6.92 | -6.92 | +0.00 | -6.92 |
| 2026-08-27 | `MRVL` | 5 | — | $240.00 | +0.00 | $245.11 | +25.55 | +25.55 | +0.00 | +25.55 |
| 2026-08-27 | `NUE` | 5 | — | $248.91 | +0.00 | $252.80 | +19.45 | +19.45 | +0.00 | +19.45 |
| 2026-08-28 | `ACMR` | 16 | $79.11 | $81.65 | +40.64 | — | +0.00 | +40.64 | +10.88 | — |
| 2026-08-28 | `GGB` | 295 | $4.46 | $4.57 | +32.45 | — | +0.00 | +32.45 | +44.25 | — |
| 2026-08-28 | `MT` | 17 | $74.53 | $74.54 | +0.17 | — | +0.00 | +0.17 | -9.86 | — |
| 2026-08-28 | `MU` | 1 | $938.40 | $967.01 | +28.61 | — | +0.00 | +28.61 | +41.27 | — |
| 2026-08-28 | `TX` | 23 | $55.13 | $55.25 | +2.76 | — | +0.00 | +2.76 | +1.15 | — |
| 2026-08-28 | `LRCX` | 4 | $312.88 | $318.88 | +24.00 | — | +0.00 | +24.00 | +17.08 | — |
| 2026-08-28 | `MRVL` | 5 | $245.11 | $253.44 | +41.65 | — | +0.00 | +41.65 | +67.20 | — |
| 2026-08-28 | `NUE` | 5 | $252.80 | $252.00 | -4.00 | — | +0.00 | -4.00 | +15.45 | — |
| 2026-08-28 | `CAPR` | 144 | — | $9.19 | +0.00 | $10.06 | +125.28 | +125.28 | +0.00 | +125.28 |
| 2026-08-28 | `SEDG` | 39 | — | $33.78 | +0.00 | $33.51 | -10.53 | -10.53 | +0.00 | -10.53 |
| 2026-08-28 | `SMTC` | 8 | — | $149.40 | +0.00 | $142.43 | -55.76 | -55.76 | +0.00 | -55.76 |
| 2026-08-28 | `OPTX` | 154 | — | $8.57 | +0.00 | $8.73 | +24.64 | +24.64 | +0.00 | +24.64 |
| 2026-08-28 | `TTMI` | 10 | — | $127.07 | +0.00 | $124.73 | -23.40 | -23.40 | +0.00 | -23.40 |
| 2026-08-28 | `BBWI` | 70 | — | $18.68 | +0.00 | $18.65 | -2.10 | -2.10 | +0.00 | -2.10 |
| 2026-08-28 | `BTSG` | 21 | — | $61.42 | +0.00 | $60.90 | -10.92 | -10.92 | +0.00 | -10.92 |
| 2026-08-28 | `CRDL` | 634 | — | $2.09 | +0.00 | $2.06 | -19.02 | -19.02 | +0.00 | -19.02 |
| 2026-08-31 | `CAPR` | 144 | $10.06 | $9.44 | -89.28 | — | +0.00 | -89.28 | +36.00 | — |
| 2026-08-31 | `SEDG` | 39 | $33.51 | $31.50 | -78.39 | — | +0.00 | -78.39 | -88.92 | — |
| 2026-08-31 | `SMTC` | 8 | $142.43 | $133.04 | -75.12 | — | +0.00 | -75.12 | -130.88 | — |
| 2026-08-31 | `OPTX` | 154 | $8.73 | $8.52 | -32.34 | — | +0.00 | -32.34 | -7.70 | — |
| 2026-08-31 | `TTMI` | 10 | $124.73 | $117.20 | -75.30 | — | +0.00 | -75.30 | -98.70 | — |
| 2026-08-31 | `BBWI` | 70 | $18.65 | $19.30 | +45.50 | — | +0.00 | +45.50 | +43.40 | — |
| 2026-08-31 | `BTSG` | 21 | $60.90 | $59.66 | -26.04 | — | +0.00 | -26.04 | -36.96 | — |
| 2026-08-31 | `CRDL` | 634 | $2.06 | $1.96 | -63.40 | — | +0.00 | -63.40 | -82.42 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `CABA` | 389 | — | $3.27 | +0.00 | $3.57 | +116.70 | +116.70 | +0.00 | +116.70 |
| 2026-09-03 | `FRVO` | 69 | — | $18.40 | +0.00 | $17.98 | -28.98 | -28.98 | +0.00 | -28.98 |
| 2026-09-03 | `CTMX` | 342 | — | $3.72 | +0.00 | $3.72 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `EIX` | 22 | — | $56.78 | +0.00 | $55.19 | -34.98 | -34.98 | +0.00 | -34.98 |
| 2026-09-03 | `CRDL` | 589 | — | $2.16 | +0.00 | $2.17 | +5.89 | +5.89 | +0.00 | +5.89 |
| 2026-09-03 | `SION` | 192 | — | $6.63 | +0.00 | $7.31 | +130.56 | +130.56 | +0.00 | +130.56 |
| 2026-09-03 | `DUOL` | 8 | — | $156.24 | +0.00 | $157.85 | +12.88 | +12.88 | +0.00 | +12.88 |
| 2026-09-03 | `SAFX` | 3265 | — | $0.39 | +0.00 | $0.38 | -32.65 | -32.65 | +0.00 | -32.65 |
| 2026-09-04 | `CABA` | 389 | $3.57 | $3.63 | +23.34 | $3.48 | -58.35 | -35.01 | +140.04 | +81.69 |
| 2026-09-04 | `FRVO` | 69 | $17.98 | $18.27 | +20.01 | — | +0.00 | +20.01 | -8.97 | — |
| 2026-09-04 | `CTMX` | 342 | $3.72 | $3.73 | +3.42 | — | +0.00 | +3.42 | +3.42 | — |
| 2026-09-04 | `EIX` | 22 | $55.19 | $55.42 | +5.06 | — | +0.00 | +5.06 | -29.92 | — |
| 2026-09-04 | `CRDL` | 589 | $2.17 | $2.18 | +5.89 | — | +0.00 | +5.89 | +11.78 | — |
| 2026-09-04 | `SION` | 192 | $7.31 | $7.31 | +0.00 | $6.75 | -107.52 | -107.52 | +130.56 | +23.04 |
| 2026-09-04 | `DUOL` | 8 | $157.85 | $161.54 | +29.52 | — | +0.00 | +29.52 | +42.40 | — |
| 2026-09-04 | `SAFX` | 3265 | $0.38 | $0.38 | +0.00 | — | +0.00 | +0.00 | -32.65 | — |
| 2026-09-04 | `ASND` | 4 | — | $266.94 | +0.00 | $271.12 | +16.72 | +16.72 | +0.00 | +16.72 |
| 2026-09-04 | `SLBT` | 409 | — | $3.07 | +0.00 | $3.15 | +32.72 | +32.72 | +0.00 | +32.72 |
| 2026-09-04 | `MLYS` | 43 | — | $29.15 | +0.00 | $28.27 | -37.84 | -37.84 | +0.00 | -37.84 |
| 2026-09-04 | `CCOI` | 122 | — | $10.22 | +0.00 | $9.98 | -29.28 | -29.28 | +0.00 | -29.28 |
| 2026-09-04 | `IRD` | 269 | — | $4.66 | +0.00 | $4.60 | -16.14 | -16.14 | +0.00 | -16.14 |
| 2026-09-04 | `JLHL` | 202 | — | $6.20 | +0.00 | $6.18 | -4.04 | -4.04 | +0.00 | -4.04 |
| 2026-09-07 | `CABA` | 389 | $3.48 | $3.46 | -7.78 | — | +0.00 | -7.78 | +73.91 | — |
| 2026-09-07 | `SION` | 192 | $6.75 | $6.68 | -13.44 | — | +0.00 | -13.44 | +9.60 | — |
| 2026-09-07 | `ASND` | 4 | $271.12 | $267.96 | -12.64 | — | +0.00 | -12.64 | +4.08 | — |
| 2026-09-07 | `SLBT` | 409 | $3.15 | $3.15 | +0.00 | — | +0.00 | +0.00 | +32.72 | — |
| 2026-09-07 | `MLYS` | 43 | $28.27 | $28.00 | -11.61 | — | +0.00 | -11.61 | -49.45 | — |
| 2026-09-07 | `CCOI` | 122 | $9.98 | $10.02 | +4.88 | — | +0.00 | +4.88 | -24.40 | — |
| 2026-09-07 | `IRD` | 269 | $4.60 | $4.53 | -18.83 | — | +0.00 | -18.83 | -34.97 | — |
| 2026-09-07 | `JLHL` | 202 | $6.18 | $6.20 | +4.04 | — | +0.00 | +4.04 | +0.00 | — |
| 2026-09-07 | `ABTC` | 140 | — | $8.95 | +0.00 | $7.99 | -134.40 | -134.40 | +0.00 | -134.40 |
| 2026-09-07 | `MRLN` | 383 | — | $3.28 | +0.00 | $3.35 | +26.81 | +26.81 | +0.00 | +26.81 |
| 2026-09-07 | `BTBT` | 785 | — | $1.60 | +0.00 | $1.64 | +31.40 | +31.40 | +0.00 | +31.40 |
| 2026-09-07 | `CRCL` | 12 | — | $97.98 | +0.00 | $102.05 | +48.84 | +48.84 | +0.00 | +48.84 |
| 2026-09-07 | `MSTR` | 9 | — | $137.35 | +0.00 | $142.80 | +49.05 | +49.05 | +0.00 | +49.05 |
| 2026-09-07 | `DFDV` | 217 | — | $5.79 | +0.00 | $5.87 | +17.36 | +17.36 | +0.00 | +17.36 |
| 2026-09-07 | `CIFR` | 72 | — | $17.33 | +0.00 | $17.74 | +29.52 | +29.52 | +0.00 | +29.52 |
| 2026-09-07 | `BRR` | 500 | — | $2.51 | +0.00 | $2.66 | +75.00 | +75.00 | +0.00 | +75.00 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +115.74 | TGTX, SLS, HIMS, VOR | — | $28.15 | $10,106.28 | TGTX×50, SLS×213, HIMS×84, VOR×113 |
| 2026-08-14 | +5.50 | $28.15 | TGTX×50, SLS×213, HIMS×84, VOR×113 | $10,117.74 | +11.46 | +178.77 | TLN, NRG, MARA, ARX, HLIT, SECZ, LFTO, REZI | TGTX, SLS, HIMS, VOR | $274.27 | $10,268.88 | TLN×3, NRG×10, MARA×140, ARX×64, HLIT×95, SECZ×216, LFTO×61, REZI×61 |
| 2026-08-17 | +2.25 | $274.27 | TLN×3, NRG×10, MARA×140, ARX×64, HLIT×95, SECZ×216, LFTO×61, REZI×61 | $10,238.82 | -30.06 | -178.63 | TMC, TGB, ELF, DNN, CAPR, NU, INV, KLC | TLN, NRG, MARA, ARX, HLIT, SECZ, LFTO, REZI | $2.16 | $10,007.11 | TMC×315, TGB×151, ELF×14, DNN×394, CAPR×185, NU×82, INV×788, KLC×487 |
| 2026-08-18 | -6.20 | $2.16 | TMC×315, TGB×151, ELF×14, DNN×394, CAPR×185, NU×82, INV×788, KLC×487 | $9,848.81 | -158.30 | +0.00 | — | TMC, TGB, ELF, DNN, CAPR, NU, INV, KLC | $9,813.47 | $9,813.47 | — |
| 2026-08-19 | -7.20 | $9,813.47 | — | $9,813.47 | -0.00 | +0.00 | — | — | $9,813.47 | $9,813.47 | — |
| 2026-08-20 | +1.12 | $9,813.47 | — | $9,813.47 | -0.00 | +127.41 | BHP, MRVI, CRCL, WYFI, TOYO, DVLT, SAFX, AAP | — | $64.08 | $9,879.35 | BHP×13, MRVI×166, CRCL×14, WYFI×57, TOYO×276, DVLT×4088, SAFX×3465, AAP×26 |
| 2026-08-21 | +3.25 | $64.08 | BHP×13, MRVI×166, CRCL×14, WYFI×57, TOYO×276, DVLT×4088, SAFX×3465, AAP×26 | $9,998.89 | +119.54 | +353.65 | AUTL, CRDL, CRSP, FUTU, GMAB, ENHA, CAN | BHP, CRCL, WYFI, TOYO, DVLT, SAFX, AAP | $76.88 | $10,237.09 | MRVI×166, AUTL×496, CRDL×634, CRSP×20, FUTU×10, GMAB×36, ENHA×716, CAN×4167 |
| 2026-08-24 | -5.17 | $76.88 | MRVI×166, AUTL×496, CRDL×634, CRSP×20, FUTU×10, GMAB×36, ENHA×716, CAN×4167 | $10,254.28 | +17.19 | -37.60 | — | MRVI, AUTL, CRDL, FUTU, GMAB, ENHA, CAN | $9,018.61 | $10,156.81 | CRSP×20 |
| 2026-08-25 | +1.80 | $9,018.61 | CRSP×20 | $10,158.61 | +1.80 | +114.49 | OCUL, CRMD, PUSA, CAPR, SAFX, SUJA, FWDI, JANX | CRSP | $8.89 | $10,228.94 | OCUL×116, CRMD×153, PUSA×343, CAPR×186, SAFX×3431, SUJA×144, FWDI×211, JANX×67 |
| 2026-08-26 | +2.02 | $8.89 | OCUL×116, CRMD×153, PUSA×343, CAPR×186, SAFX×3431, SUJA×144, FWDI×211, JANX×67 | $10,228.94 | +0.00 | +0.00 | — | — | $8.89 | $10,228.94 | OCUL×116, CRMD×153, PUSA×343, CAPR×186, SAFX×3431, SUJA×144, FWDI×211, JANX×67 |
| 2026-08-27 | — | $8.89 | OCUL×116, CRMD×153, PUSA×343, CAPR×186, SAFX×3431, SUJA×144, FWDI×211, JANX×67 | $10,493.60 | +264.66 | +21.14 | ACMR, GGB, MT, MU, TX, LRCX, MRVL, NUE | OCUL, CRMD, PUSA, CAPR, SAFX, SUJA, FWDI, JANX | $658.61 | $10,454.54 | ACMR×16, GGB×295, MT×17, MU×1, TX×23, LRCX×4, MRVL×5, NUE×5 |
| 2026-08-28 | +0.75 | $658.61 | ACMR×16, GGB×295, MT×17, MU×1, TX×23, LRCX×4, MRVL×5, NUE×5 | $10,620.82 | +166.28 | +28.19 | CAPR, SEDG, SMTC, OPTX, TTMI, BBWI, BTSG, CRDL | ACMR, GGB, MT, MU, TX, LRCX, MRVL, NUE | $230.29 | $10,607.42 | CAPR×144, SEDG×39, SMTC×8, OPTX×154, TTMI×10, BBWI×70, BTSG×21, CRDL×634 |
| 2026-08-31 | -5.85 | $230.29 | CAPR×144, SEDG×39, SMTC×8, OPTX×154, TTMI×10, BBWI×70, BTSG×21, CRDL×634 | $10,213.05 | -394.37 | +0.00 | — | CAPR, SEDG, SMTC, OPTX, TTMI, BBWI, BTSG, CRDL | $10,189.31 | $10,189.31 | — |
| 2026-09-01 | -6.30 | $10,189.31 | — | $10,189.31 | +0.00 | +0.00 | — | — | $10,189.31 | $10,189.31 | — |
| 2026-09-02 | -3.83 | $10,189.31 | — | $10,189.31 | +0.00 | +0.00 | — | — | $10,189.31 | $10,189.31 | — |
| 2026-09-03 | -0.90 | $10,189.31 | — | $10,189.31 | +0.00 | +169.42 | CABA, FRVO, CTMX, EIX, CRDL, SION, DUOL, SAFX | — | $9.42 | $10,310.34 | CABA×389, FRVO×69, CTMX×342, EIX×22, CRDL×589, SION×192, DUOL×8, SAFX×3265 |
| 2026-09-04 | — | $9.42 | CABA×389, FRVO×69, CTMX×342, EIX×22, CRDL×589, SION×192, DUOL×8, SAFX×3265 | $10,397.58 | +87.24 | -203.73 | ASND, SLBT, MLYS, CCOI, IRD, JLHL | FRVO, CTMX, EIX, CRDL, DUOL, SAFX | $193.28 | $10,134.76 | CABA×389, SION×192, ASND×4, SLBT×409, MLYS×43, CCOI×122, IRD×269, JLHL×202 |
| 2026-09-07 | — | $193.28 | CABA×389, SION×192, ASND×4, SLBT×409, MLYS×43, CCOI×122, IRD×269, JLHL×202 | $10,079.38 | -55.38 | +143.58 | ABTC, MRLN, BTBT, CRCL, MSTR, DFDV, CIFR, BRR | CABA, SION, ASND, SLBT, MLYS, CCOI, IRD, JLHL | $84.28 | $10,164.20 | ABTC×140, MRLN×383, BTBT×785, CRCL×12, MSTR×9, DFDV×217, CIFR×72, BRR×500 |

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
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $6,560.80 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 64 | $19.57 | $2.18 | — | $5,306.14 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 95 | $13.18 | $2.27 | — | $4,051.77 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SECZ` | 216 | $5.84 | $2.79 | — | $2,787.54 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-20.7; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LFTO` | 61 | $20.57 | $2.17 | — | $1,530.60 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-14.0; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `REZI` | 61 | $20.56 | $2.17 | — | $274.27 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-21.5; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
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
| 2026-08-17 09:30 ET | **BUY** | `NU` | 82 | $15.40 | $2.24 | — | $2,571.11 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 788 | $1.62 | $10.17 | — | $1,284.38 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `KLC` | 487 | $2.62 | $6.28 | — | $2.16 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.16 | ▼ close $10,007.11 vs 09:30 $10,238.82 (session -178.63) | 16:00 close · cash $2.16 · equity $10,007.11 vs 09:30 $10,238.82 (-231.71; session marks -178.63) · 8 name(s) marked open→close (per-name table). TMC×315 09:30 $4.05 → close $3.77 -88.20; TGB×151 09:30 $8.46 → close $8.77 +46.81; ELF×14 09:30 $90.54 → close $93.66 +43.68; DNN×394 09:30 $3.24 → close $3.19 -19.70; CAPR×185 09:30 $6.87 → close $7.45 +107.30; NU×82 09:30 $15.40 → close $14.74 -54.12; INV×788 09:30 $1.62 → close $1.39 -185.18; KLC×487 09:30 $2.62 → close $2.56 -29.22 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.16 | ▼ 09:30 equity $9,848.81 vs yday $10,007.11 (-158.30) | 09:30 open · cash $2.16 (unchanged overnight, no fees) · equity $9,848.81 vs prior close $10,007.11 (-158.30) · 8 name(s) re-marked at the open (per-name table). TMC×315 yday $3.77 → 09:30 $3.72 -15.75; TGB×151 yday $8.77 → 09:30 $8.55 -33.22; ELF×14 yday $93.66 → 09:30 $93.44 -3.08; DNN×394 yday $3.19 → 09:30 $3.11 -31.52; CAPR×185 yday $7.45 → 09:30 $7.50 +9.25; NU×82 yday $14.74 → 09:30 $14.53 -17.22; INV×788 yday $1.39 → 09:30 $1.32 -47.28; KLC×487 yday $2.56 → 09:30 $2.52 -19.48 | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 315 | $3.72 | $4.13 | $-112.14 | $1,169.83 | ▼ -112.14 after sell → book $9,844.68; vs 09:30 mark -4.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 151 | $8.55 | $2.48 | $+8.67 | $2,458.41 | ▲ +8.67 after sell → book $9,842.21; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $3,764.51 | ▲ +36.52 after sell → book $9,840.15; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 394 | $3.11 | $5.16 | $-61.46 | $4,984.70 | ▼ -61.46 after sell → book $9,835.00; vs 09:30 mark -5.15 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `CAPR` | 185 | $7.50 | $2.59 | $+111.42 | $6,369.61 | ▲ +111.42 after sell → book $9,832.41; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NU` | 82 | $14.53 | $2.26 | $-75.84 | $7,558.81 | ▼ -75.84 after sell → book $9,830.15; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INV` | 788 | $1.32 | $10.31 | $-252.93 | $8,592.60 | ▼ -252.93 after sell → book $9,819.84; vs 09:30 mark -10.31 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `KLC` | 487 | $2.52 | $6.37 | $-61.36 | $9,813.47 | ▼ -61.36 after sell → book $9,813.47; vs 09:30 mark -6.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,813.47 | ▲ close $9,813.47 vs 09:30 $9,848.81 (session +0.00) | 16:00 close · cash $9,813.47 · no lots left · equity $9,813.47. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,813.47 | ▲ 09:30 equity $9,813.47 vs yday $9,813.47 (-0.00) | 09:30 open · cash $9,813.47 · no holdings · equity $9,813.47 vs prior close $9,813.47 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,813.47 | ▲ close $9,813.47 vs 09:30 $9,813.47 (session +0.00) | 16:00 close · cash $9,813.47 · no lots left · equity $9,813.47. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,813.47 | ▲ 09:30 equity $9,813.47 vs yday $9,813.47 (-0.00) | 09:30 open · cash $9,813.47 · no holdings · equity $9,813.47 vs prior close $9,813.47 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,628.31 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1226.68 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 166 | $7.38 | $2.49 | — | $7,400.74 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1226.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRCL` | 14 | $83.29 | $2.03 | — | $6,232.65 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable; 🔵; ⚪; ret5=+7.4; leftover $1226.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WYFI` | 57 | $21.40 | $2.16 | — | $5,010.69 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-25.2; leftover $1226.68 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 276 | $4.43 | $3.56 | — | $3,784.45 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-23.1; leftover $1226.68 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 4088 | $0.30 | $24.53 | — | $2,533.52 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-3.2; leftover $1226.68 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SAFX` | 3465 | $0.35 | $22.66 | — | $1,284.25 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-29.4; leftover $1226.68 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 26 | $46.85 | $2.07 | — | $64.08 | — | union ∩ last_red, no 🚨; gate last_red=True; list earn_react; 🔵; ret5=+5.0; leftover $1226.68 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $64.08 | ▲ close $9,879.35 vs 09:30 $9,813.47 (session +127.41) | 16:00 close · cash $64.08 · equity $9,879.35 vs 09:30 $9,813.47 (+65.88; session marks +127.41) · 8 name(s) marked open→close (per-name table). BHP×13 09:30 $91.01 → close $93.63 +34.06; MRVI×166 09:30 $7.38 → close $8.26 +146.08; CRCL×14 09:30 $83.29 → close $83.99 +9.80; WYFI×57 09:30 $21.40 → close $21.16 -13.68; TOYO×276 09:30 $4.43 → close $4.51 +23.46; DVLT×4088 09:30 $0.30 → close $0.32 +81.76; SAFX×3465 09:30 $0.35 → close $0.34 -38.11; AAP×26 09:30 $46.85 → close $42.39 -115.96 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.08 | ▲ 09:30 equity $9,998.89 vs yday $9,879.35 (+119.54) | 09:30 open · cash $64.08 (unchanged overnight, no fees) · equity $9,998.89 vs prior close $9,879.35 (+119.54) · 8 name(s) re-marked at the open (per-name table). BHP×13 yday $93.63 → 09:30 $95.72 +27.17; MRVI×166 yday $8.26 → 09:30 $8.20 -9.96; CRCL×14 yday $83.99 → 09:30 $87.65 +51.24; WYFI×57 yday $21.16 → 09:30 $21.54 +21.66; TOYO×276 yday $4.51 → 09:30 $4.68 +45.54; DVLT×4088 yday $0.32 → 09:30 $0.31 -40.88; SAFX×3465 yday $0.34 → 09:30 $0.35 +24.25; AAP×26 yday $42.39 → 09:30 $42.41 +0.52 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,306.39 | ▲ +57.15 after sell → book $9,996.84; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CRCL` | 14 | $87.65 | $2.05 | $+56.96 | $2,531.44 | ▲ +56.96 after sell → book $9,994.79; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WYFI` | 57 | $21.54 | $2.18 | $+3.64 | $3,757.04 | ▲ +3.64 after sell → book $9,992.61; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 276 | $4.68 | $3.62 | $+61.82 | $5,045.10 | ▲ +61.82 after sell → book $9,988.99; vs 09:30 mark -3.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 4088 | $0.31 | $25.63 | $-9.27 | $6,286.76 | ▼ -9.27 after sell → book $9,963.37; vs 09:30 mark -25.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SAFX` | 3465 | $0.35 | $23.11 | $-59.63 | $7,476.40 | ▼ -59.63 after sell → book $9,940.26; vs 09:30 mark -23.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AAP` | 26 | $42.41 | $2.09 | $-119.60 | $8,576.97 | ▼ -119.60 after sell → book $9,938.17; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 496 | $2.47 | $6.40 | — | $7,345.45 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1225.28 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 634 | $1.93 | $8.18 | — | $6,113.66 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1225.28 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 20 | $59.72 | $2.05 | — | $4,917.21 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1225.28 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $3,763.39 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1225.28 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 36 | $33.36 | $2.10 | — | $2,560.33 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $1225.28 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 716 | $1.71 | $9.24 | — | $1,326.73 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1225.28 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 4167 | $0.29 | $24.75 | — | $76.88 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $1225.28 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.88 | ▲ close $10,237.09 vs 09:30 $9,998.89 (session +353.65) | 16:00 close · cash $76.88 · equity $10,237.09 vs 09:30 $9,998.89 (+238.20; session marks +353.65) · 8 name(s) marked open→close (per-name table). MRVI×166 09:30 $8.20 → close $8.70 +83.00; AUTL×496 09:30 $2.47 → close $2.41 -29.76; CRDL×634 09:30 $1.93 → close $1.86 -44.38; CRSP×20 09:30 $59.72 → close $59.50 -4.40; FUTU×10 09:30 $115.18 → close $123.64 +84.60; GMAB×36 09:30 $33.36 → close $33.45 +3.24; ENHA×716 09:30 $1.71 → close $1.72 +7.16; CAN×4167 09:30 $0.29 → close $0.35 +254.19 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.88 | ▲ 09:30 equity $10,254.28 vs yday $10,237.09 (+17.19) | 09:30 open · cash $76.88 (unchanged overnight, no fees) · equity $10,254.28 vs prior close $10,237.09 (+17.19) · 8 name(s) re-marked at the open (per-name table). MRVI×166 yday $8.70 → 09:30 $8.59 -18.26; AUTL×496 yday $2.41 → 09:30 $2.36 -24.80; CRDL×634 yday $1.86 → 09:30 $1.87 +6.34; CRSP×20 yday $59.50 → 09:30 $58.79 -14.20; FUTU×10 yday $123.64 → 09:30 $120.87 -27.70; GMAB×36 yday $33.45 → 09:30 $32.82 -22.68; ENHA×716 yday $1.72 → 09:30 $1.74 +14.32; CAN×4167 yday $0.35 → 09:30 $0.38 +104.18 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 166 | $8.59 | $2.53 | $+195.85 | $1,500.29 | ▲ +195.85 after sell → book $10,251.75; vs 09:30 mark -2.53 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 496 | $2.36 | $6.49 | $-67.45 | $2,664.36 | ▼ -67.45 after sell → book $10,245.26; vs 09:30 mark -6.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 634 | $1.87 | $8.29 | $-54.51 | $3,841.65 | ▼ -54.51 after sell → book $10,236.97; vs 09:30 mark -8.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $120.87 | $2.04 | $+52.84 | $5,048.31 | ▲ +52.84 after sell → book $10,234.93; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 36 | $32.82 | $2.12 | $-23.66 | $6,227.71 | ▼ -23.66 after sell → book $10,232.81; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ENHA` | 716 | $1.74 | $9.37 | $+2.88 | $7,464.19 | ▲ +2.88 after sell → book $10,223.45; vs 09:30 mark -9.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAN` | 4167 | $0.38 | $29.04 | $+304.57 | $9,018.61 | ▲ +304.57 after sell → book $10,194.41; vs 09:30 mark -29.04 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,018.61 | ▼ close $10,156.81 vs 09:30 $10,254.28 (session -37.60) | 16:00 close · cash $9,018.61 · equity $10,156.81 vs 09:30 $10,254.28 (-97.47; session marks -37.60) · 1 name(s) marked open→close (per-name table). CRSP×20 09:30 $58.79 → close $56.91 -37.60 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,018.61 | ▲ 09:30 equity $10,158.61 vs yday $10,156.81 (+1.80) | 09:30 open · cash $9,018.61 (unchanged overnight, no fees) · equity $10,158.61 vs prior close $10,156.81 (+1.80) · 1 name(s) re-marked at the open (per-name table). CRSP×20 yday $56.91 → 09:30 $57.00 +1.80 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.00 | $2.07 | $-58.52 | $10,156.54 | ▼ -58.52 after sell → book $10,156.54; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 116 | $10.92 | $2.34 | — | $8,887.48 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+10.4; leftover $1269.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 153 | $8.28 | $2.45 | — | $7,618.19 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1269.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 343 | $3.70 | $4.42 | — | $6,344.66 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1269.57 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 186 | $6.79 | $2.55 | — | $5,079.18 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1269.57 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3431 | $0.37 | $22.99 | — | $3,786.72 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; ret5=-26.5; leftover $1269.57 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 144 | $8.79 | $2.42 | — | $2,518.54 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $1269.57 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 211 | $5.99 | $2.72 | — | $1,251.93 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+20.7; leftover $1269.57 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `JANX` | 67 | $18.52 | $2.19 | — | $8.89 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=+7.9; leftover $1269.57 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.89 | ▲ close $10,228.94 vs 09:30 $10,158.61 (session +114.49) | 16:00 close · cash $8.89 · equity $10,228.94 vs 09:30 $10,158.61 (+70.33; session marks +114.49) · 8 name(s) marked open→close (per-name table). OCUL×116 09:30 $10.92 → close $10.92 +0.00; CRMD×153 09:30 $8.28 → close $8.28 +0.00; PUSA×343 09:30 $3.70 → close $3.91 +72.03; CAPR×186 09:30 $6.79 → close $7.19 +74.40; SAFX×3431 09:30 $0.37 → close $0.37 +0.00; SUJA×144 09:30 $8.79 → close $8.54 -36.00; FWDI×211 09:30 $5.99 → close $5.86 -27.43; JANX×67 09:30 $18.52 → close $18.99 +31.49 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.89 | ▲ 09:30 equity $10,228.94 vs yday $10,228.94 (+0.00) | 09:30 open · cash $8.89 (unchanged overnight, no fees) · equity $10,228.94 vs prior close $10,228.94 (+0.00) · 8 name(s) re-marked at the open (per-name table). OCUL×116 yday $10.92 → 09:30 $10.92 +0.00; CRMD×153 yday $8.28 → 09:30 $8.28 +0.00; PUSA×343 yday $3.91 → 09:30 $3.91 +0.00; CAPR×186 yday $7.19 → 09:30 $7.19 +0.00; SAFX×3431 yday $0.37 → 09:30 $0.37 +0.00; SUJA×144 yday $8.54 → 09:30 $8.54 +0.00; FWDI×211 yday $5.86 → 09:30 $5.86 +0.00; JANX×67 yday $18.99 → 09:30 $18.99 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.89 | ▲ close $10,228.94 vs 09:30 $10,228.94 (session +0.00) | 16:00 close · cash $8.89 · equity $10,228.94 vs 09:30 $10,228.94 (+0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). OCUL×116 09:30 $10.92 → close $10.92 +0.00; CRMD×153 09:30 $8.28 → close $8.28 +0.00; PUSA×343 09:30 $3.91 → close $3.91 +0.00; CAPR×186 09:30 $7.19 → close $7.19 +0.00; SAFX×3431 09:30 $0.37 → close $0.37 +0.00; SUJA×144 09:30 $8.54 → close $8.54 +0.00; FWDI×211 09:30 $5.86 → close $5.86 +0.00; JANX×67 09:30 $18.99 → close $18.99 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.89 | ▲ 09:30 equity $10,493.60 vs yday $10,228.94 (+264.66) | 09:30 open · cash $8.89 (unchanged overnight, no fees) · equity $10,493.60 vs prior close $10,228.94 (+264.66) · 8 name(s) re-marked at the open (per-name table). OCUL×116 yday $10.92 → 09:30 $10.79 -15.08; CRMD×153 yday $8.28 → 09:30 $8.60 +48.96; PUSA×343 yday $3.91 → 09:30 $3.84 -24.01; CAPR×186 yday $7.19 → 09:30 $8.29 +204.60; SAFX×3431 yday $0.37 → 09:30 $0.35 -68.62; SUJA×144 yday $8.54 → 09:30 $9.39 +122.40; FWDI×211 yday $5.86 → 09:30 $5.97 +23.21; JANX×67 yday $18.99 → 09:30 $18.59 -26.80 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 116 | $10.79 | $2.37 | $-19.79 | $1,258.17 | ▼ -19.79 after sell → book $10,491.24; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 153 | $8.60 | $2.48 | $+44.03 | $2,571.48 | ▲ +44.03 after sell → book $10,488.75; vs 09:30 mark -2.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PUSA` | 343 | $3.84 | $4.49 | $+39.10 | $3,884.11 | ▲ +39.10 after sell → book $10,484.26; vs 09:30 mark -4.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 186 | $8.29 | $2.59 | $+273.86 | $5,423.46 | ▲ +273.86 after sell → book $10,481.67; vs 09:30 mark -2.59 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SAFX` | 3431 | $0.35 | $22.88 | $-114.49 | $6,601.43 | ▼ -114.49 after sell → book $10,458.79; vs 09:30 mark -22.88 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 144 | $9.39 | $2.46 | $+81.52 | $7,951.13 | ▲ +81.52 after sell → book $10,456.33; vs 09:30 mark -2.46 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FWDI` | 211 | $5.97 | $2.77 | $-9.71 | $9,208.03 | ▼ -9.71 after sell → book $10,453.56; vs 09:30 mark -2.77 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `JANX` | 67 | $18.59 | $2.21 | $+0.29 | $10,451.35 | ▲ +0.29 after sell → book $10,451.35; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 16 | $80.97 | $2.04 | — | $9,153.79 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-1.3; leftover $1306.42 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 295 | $4.42 | $3.81 | — | $7,846.09 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; ret5=-8.6; leftover $1306.42 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 17 | $75.12 | $2.04 | — | $6,567.01 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; ret5=-2.2; leftover $1306.42 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $5,639.27 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-0.5; leftover $1306.42 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 23 | $55.20 | $2.06 | — | $4,367.62 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; ret5=+3.0; leftover $1306.42 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 4 | $314.61 | $2.00 | — | $3,107.17 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-5.5; leftover $1306.42 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $240.00 | $2.00 | — | $1,905.17 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=+6.8; leftover $1306.42 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NUE` | 5 | $248.91 | $2.00 | — | $658.61 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; ret5=-9.4; leftover $1306.42 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $658.61 | ▲ close $10,454.54 vs 09:30 $10,493.60 (session +21.14) | 16:00 close · cash $658.61 · equity $10,454.54 vs 09:30 $10,493.60 (-39.06; session marks +21.14) · 8 name(s) marked open→close (per-name table). ACMR×16 09:30 $80.97 → close $79.11 -29.76; GGB×295 09:30 $4.42 → close $4.46 +11.80; MT×17 09:30 $75.12 → close $74.53 -10.03; MU×1 09:30 $925.74 → close $938.40 +12.66; TX×23 09:30 $55.20 → close $55.13 -1.61; LRCX×4 09:30 $314.61 → close $312.88 -6.92; MRVL×5 09:30 $240.00 → close $245.11 +25.55; NUE×5 09:30 $248.91 → close $252.80 +19.45 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $658.61 | ▲ 09:30 equity $10,620.82 vs yday $10,454.54 (+166.28) | 09:30 open · cash $658.61 (unchanged overnight, no fees) · equity $10,620.82 vs prior close $10,454.54 (+166.28) · 8 name(s) re-marked at the open (per-name table). ACMR×16 yday $79.11 → 09:30 $81.65 +40.64; GGB×295 yday $4.46 → 09:30 $4.57 +32.45; MT×17 yday $74.53 → 09:30 $74.54 +0.17; MU×1 yday $938.40 → 09:30 $967.01 +28.61; TX×23 yday $55.13 → 09:30 $55.25 +2.76; LRCX×4 yday $312.88 → 09:30 $318.88 +24.00; MRVL×5 yday $245.11 → 09:30 $253.44 +41.65; NUE×5 yday $252.80 → 09:30 $252.00 -4.00 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 16 | $81.65 | $2.06 | $+6.78 | $1,962.95 | ▲ +6.78 after sell → book $10,618.76; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 295 | $4.57 | $3.87 | $+36.58 | $3,307.24 | ▲ +36.58 after sell → book $10,614.90; vs 09:30 mark -3.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 17 | $74.54 | $2.06 | $-13.96 | $4,572.36 | ▼ -13.96 after sell → book $10,612.84; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $5,537.36 | ▲ +37.26 after sell → book $10,610.83; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 23 | $55.25 | $2.08 | $-2.99 | $6,806.03 | ▼ -2.99 after sell → book $10,608.75; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 4 | $318.88 | $2.02 | $+13.06 | $8,079.52 | ▲ +13.06 after sell → book $10,606.72; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $253.44 | $2.03 | $+63.17 | $9,344.70 | ▲ +63.17 after sell → book $10,604.70; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `NUE` | 5 | $252.00 | $2.03 | $+11.42 | $10,602.67 | ▲ +11.42 after sell → book $10,602.67; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 144 | $9.19 | $2.42 | — | $9,276.89 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1325.33 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $33.78 | $2.11 | — | $7,957.36 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1325.33 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $6,760.15 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1325.33 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 154 | $8.57 | $2.45 | — | $5,437.92 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-3.4; leftover $1325.33 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $127.07 | $2.02 | — | $4,165.20 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $1325.33 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 70 | $18.68 | $2.20 | — | $2,855.40 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=+0.2; leftover $1325.33 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BTSG` | 21 | $61.42 | $2.05 | — | $1,563.53 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-4.6; leftover $1325.33 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CRDL` | 634 | $2.09 | $8.18 | — | $230.29 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=+3.3; leftover $1325.33 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $230.29 | ▲ close $10,607.42 vs 09:30 $10,620.82 (session +28.19) | 16:00 close · cash $230.29 · equity $10,607.42 vs 09:30 $10,620.82 (-13.40; session marks +28.19) · 8 name(s) marked open→close (per-name table). CAPR×144 09:30 $9.19 → close $10.06 +125.28; SEDG×39 09:30 $33.78 → close $33.51 -10.53; SMTC×8 09:30 $149.40 → close $142.43 -55.76; OPTX×154 09:30 $8.57 → close $8.73 +24.64; TTMI×10 09:30 $127.07 → close $124.73 -23.40; BBWI×70 09:30 $18.68 → close $18.65 -2.10; BTSG×21 09:30 $61.42 → close $60.90 -10.92; CRDL×634 09:30 $2.09 → close $2.06 -19.02 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $230.29 | ▼ 09:30 equity $10,213.05 vs yday $10,607.42 (-394.37) | 09:30 open · cash $230.29 (unchanged overnight, no fees) · equity $10,213.05 vs prior close $10,607.42 (-394.37) · 8 name(s) re-marked at the open (per-name table). CAPR×144 yday $10.06 → 09:30 $9.44 -89.28; SEDG×39 yday $33.51 → 09:30 $31.50 -78.39; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; OPTX×154 yday $8.73 → 09:30 $8.52 -32.34; TTMI×10 yday $124.73 → 09:30 $117.20 -75.30; BBWI×70 yday $18.65 → 09:30 $19.30 +45.50; BTSG×21 yday $60.90 → 09:30 $59.66 -26.04; CRDL×634 yday $2.06 → 09:30 $1.96 -63.40 | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 144 | $9.44 | $2.46 | $+31.12 | $1,587.19 | ▲ +31.12 after sell → book $10,210.59; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 39 | $31.50 | $2.13 | $-93.15 | $2,813.56 | ▼ -93.15 after sell → book $10,208.46; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $133.04 | $2.03 | $-134.93 | $3,875.85 | ▼ -134.93 after sell → book $10,206.43; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 154 | $8.52 | $2.49 | $-12.64 | $5,185.44 | ▼ -12.64 after sell → book $10,203.94; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 10 | $117.20 | $2.04 | $-102.76 | $6,355.40 | ▼ -102.76 after sell → book $10,201.90; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 70 | $19.30 | $2.22 | $+38.98 | $7,704.18 | ▲ +38.98 after sell → book $10,199.68; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BTSG` | 21 | $59.66 | $2.07 | $-41.09 | $8,954.97 | ▼ -41.09 after sell → book $10,197.61; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRDL` | 634 | $1.96 | $8.29 | $-98.89 | $10,189.31 | ▼ -98.89 after sell → book $10,189.31; vs 09:30 mark -8.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,189.31 | ▲ close $10,189.31 vs 09:30 $10,213.05 (session +0.00) | 16:00 close · cash $10,189.31 · no lots left · equity $10,189.31. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,189.31 | ▲ 09:30 equity $10,189.31 vs yday $10,189.31 (+0.00) | 09:30 open · cash $10,189.31 · no holdings · equity $10,189.31 vs prior close $10,189.31 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,189.31 | ▲ close $10,189.31 vs 09:30 $10,189.31 (session +0.00) | 16:00 close · cash $10,189.31 · no lots left · equity $10,189.31. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,189.31 | ▲ 09:30 equity $10,189.31 vs yday $10,189.31 (+0.00) | 09:30 open · cash $10,189.31 · no holdings · equity $10,189.31 vs prior close $10,189.31 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,189.31 | ▲ close $10,189.31 vs 09:30 $10,189.31 (session +0.00) | 16:00 close · cash $10,189.31 · no lots left · equity $10,189.31. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,189.31 | ▲ 09:30 equity $10,189.31 vs yday $10,189.31 (+0.00) | 09:30 open · cash $10,189.31 · no holdings · equity $10,189.31 vs prior close $10,189.31 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 389 | $3.27 | $5.02 | — | $8,912.26 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1273.66 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 69 | $18.40 | $2.20 | — | $7,640.47 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1273.66 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 342 | $3.72 | $4.41 | — | $6,363.82 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1273.66 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $56.78 | $2.06 | — | $5,112.60 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; ret5=+0.3; leftover $1273.66 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 589 | $2.16 | $7.60 | — | $3,832.76 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1273.66 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 192 | $6.63 | $2.57 | — | $2,557.24 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=-18.1; leftover $1273.66 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 8 | $156.24 | $2.01 | — | $1,305.30 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=+10.0; leftover $1273.66 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SAFX` | 3265 | $0.39 | $22.53 | — | $9.42 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-26.5; leftover $1273.66 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.42 | ▲ close $10,310.34 vs 09:30 $10,189.31 (session +169.42) | 16:00 close · cash $9.42 · equity $10,310.34 vs 09:30 $10,189.31 (+121.03; session marks +169.42) · 8 name(s) marked open→close (per-name table). CABA×389 09:30 $3.27 → close $3.57 +116.70; FRVO×69 09:30 $18.40 → close $17.98 -28.98; CTMX×342 09:30 $3.72 → close $3.72 +0.00; EIX×22 09:30 $56.78 → close $55.19 -34.98; CRDL×589 09:30 $2.16 → close $2.17 +5.89; SION×192 09:30 $6.63 → close $7.31 +130.56; DUOL×8 09:30 $156.24 → close $157.85 +12.88; SAFX×3265 09:30 $0.39 → close $0.38 -32.65 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.42 | ▲ 09:30 equity $10,397.58 vs yday $10,310.34 (+87.24) | 09:30 open · cash $9.42 (unchanged overnight, no fees) · equity $10,397.58 vs prior close $10,310.34 (+87.24) · 8 name(s) re-marked at the open (per-name table). CABA×389 yday $3.57 → 09:30 $3.63 +23.34; FRVO×69 yday $17.98 → 09:30 $18.27 +20.01; CTMX×342 yday $3.72 → 09:30 $3.73 +3.42; EIX×22 yday $55.19 → 09:30 $55.42 +5.06; CRDL×589 yday $2.17 → 09:30 $2.18 +5.89; SION×192 yday $7.31 → 09:30 $7.31 +0.00; DUOL×8 yday $157.85 → 09:30 $161.54 +29.52; SAFX×3265 yday $0.38 → 09:30 $0.38 +0.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 69 | $18.27 | $2.22 | $-13.39 | $1,267.83 | ▼ -13.39 after sell → book $10,395.36; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTMX` | 342 | $3.73 | $4.48 | $-5.47 | $2,539.02 | ▼ -5.47 after sell → book $10,390.89; vs 09:30 mark -4.47 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 22 | $55.42 | $2.08 | $-34.05 | $3,756.18 | ▼ -34.05 after sell → book $10,388.81; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 589 | $2.18 | $7.71 | $-3.52 | $5,032.49 | ▼ -3.52 after sell → book $10,381.10; vs 09:30 mark -7.71 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 8 | $161.54 | $2.03 | $+38.35 | $6,322.78 | ▲ +38.35 after sell → book $10,379.07; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SAFX` | 3265 | $0.38 | $22.75 | $-77.93 | $7,540.72 | ▼ -77.93 after sell → book $10,356.31; vs 09:30 mark -22.76 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 4 | $266.94 | $2.00 | — | $6,470.96 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=+1.9; leftover $1256.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 409 | $3.07 | $5.28 | — | $5,210.06 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1256.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 43 | $29.15 | $2.12 | — | $3,954.49 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1256.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 122 | $10.22 | $2.36 | — | $2,705.29 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1256.79 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 269 | $4.66 | $3.47 | — | $1,448.28 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1256.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `JLHL` | 202 | $6.20 | $2.61 | — | $193.28 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=-8.2; leftover $1256.79 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $193.28 | ▼ close $10,134.76 vs 09:30 $10,397.58 (session -203.73) | 16:00 close · cash $193.28 · equity $10,134.76 vs 09:30 $10,397.58 (-262.82; session marks -203.73) · 8 name(s) marked open→close (per-name table). CABA×389 09:30 $3.63 → close $3.48 -58.35; SION×192 09:30 $7.31 → close $6.75 -107.52; ASND×4 09:30 $266.94 → close $271.12 +16.72; SLBT×409 09:30 $3.07 → close $3.15 +32.72; MLYS×43 09:30 $29.15 → close $28.27 -37.84; CCOI×122 09:30 $10.22 → close $9.98 -29.28; IRD×269 09:30 $4.66 → close $4.60 -16.14; JLHL×202 09:30 $6.20 → close $6.18 -4.04 | — |
| 2026-09-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $193.28 | ▼ 09:30 equity $10,079.38 vs yday $10,134.76 (-55.38) | 09:30 open · cash $193.28 (unchanged overnight, no fees) · equity $10,079.38 vs prior close $10,134.76 (-55.38) · 8 name(s) re-marked at the open (per-name table). CABA×389 yday $3.48 → 09:30 $3.46 -7.78; SION×192 yday $6.75 → 09:30 $6.68 -13.44; ASND×4 yday $271.12 → 09:30 $267.96 -12.64; SLBT×409 yday $3.15 → 09:30 $3.15 +0.00; MLYS×43 yday $28.27 → 09:30 $28.00 -11.61; CCOI×122 yday $9.98 → 09:30 $10.02 +4.88; IRD×269 yday $4.60 → 09:30 $4.53 -18.83; JLHL×202 yday $6.18 → 09:30 $6.20 +4.04 | — |
| 2026-09-07 09:30 ET | **SELL** | `CABA` | 389 | $3.46 | $5.09 | $+63.80 | $1,534.12 | ▲ +63.80 after sell → book $10,074.28; vs 09:30 mark -5.10 | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **SELL** | `SION` | 192 | $6.68 | $2.61 | $+4.43 | $2,814.07 | ▲ +4.43 after sell → book $10,071.67; vs 09:30 mark -2.61 | dropped from list after 2 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `ASND` | 4 | $267.96 | $2.02 | $+0.06 | $3,883.89 | ▲ +0.06 after sell → book $10,069.65; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `SLBT` | 409 | $3.15 | $5.35 | $+22.09 | $5,166.89 | ▲ +22.09 after sell → book $10,064.30; vs 09:30 mark -5.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `MLYS` | 43 | $28.00 | $2.14 | $-53.71 | $6,368.75 | ▼ -53.71 after sell → book $10,062.16; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `CCOI` | 122 | $10.02 | $2.39 | $-29.14 | $7,588.80 | ▼ -29.14 after sell → book $10,059.77; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `IRD` | 269 | $4.53 | $3.52 | $-41.96 | $8,803.85 | ▼ -41.96 after sell → book $10,056.25; vs 09:30 mark -3.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `JLHL` | 202 | $6.20 | $2.65 | $-5.26 | $10,053.60 | ▼ -5.26 after sell → book $10,053.60; vs 09:30 mark -2.65 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **BUY** | `ABTC` | 140 | $8.95 | $2.41 | — | $8,798.19 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1256.70 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `MRLN` | 383 | $3.28 | $4.94 | — | $7,537.01 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.6; leftover $1256.70 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `BTBT` | 785 | $1.60 | $10.13 | — | $6,270.88 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-2.5; leftover $1256.70 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `CRCL` | 12 | $97.98 | $2.03 | — | $5,093.10 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+7.4; leftover $1256.70 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `MSTR` | 9 | $137.35 | $2.02 | — | $3,854.93 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+28.2; leftover $1256.70 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `DFDV` | 217 | $5.79 | $2.80 | — | $2,595.70 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+28.3; leftover $1256.70 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `CIFR` | 72 | $17.33 | $2.21 | — | $1,345.73 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-11.7; leftover $1256.70 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `BRR` | 500 | $2.51 | $6.45 | — | $84.28 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+28.0; leftover $1256.70 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $84.28 | ▲ close $10,164.20 vs 09:30 $10,079.38 (session +143.58) | 16:00 close · cash $84.28 · equity $10,164.20 vs 09:30 $10,079.38 (+84.82; session marks +143.58) · 8 name(s) marked open→close (per-name table). ABTC×140 09:30 $8.95 → close $7.99 -134.40; MRLN×383 09:30 $3.28 → close $3.35 +26.81; BTBT×785 09:30 $1.60 → close $1.64 +31.40; CRCL×12 09:30 $97.98 → close $102.05 +48.84; MSTR×9 09:30 $137.35 → close $142.80 +49.05; DFDV×217 09:30 $5.79 → close $5.87 +17.36; CIFR×72 09:30 $17.33 → close $17.74 +29.52; BRR×500 09:30 $2.51 → close $2.66 +75.00 | — |

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
| `ABTC` | 140 | 2026-09-07 @ $8.95 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1256.70 |
| `MRLN` | 383 | 2026-09-07 @ $3.28 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.6; leftover $1256.70 |
| `BTBT` | 785 | 2026-09-07 @ $1.60 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-2.5; leftover $1256.70 |
| `CRCL` | 12 | 2026-09-07 @ $97.98 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+7.4; leftover $1256.70 |
| `MSTR` | 9 | 2026-09-07 @ $137.35 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+28.2; leftover $1256.70 |
| `DFDV` | 217 | 2026-09-07 @ $5.79 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+28.3; leftover $1256.70 |
| `CIFR` | 72 | 2026-09-07 @ $17.33 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-11.7; leftover $1256.70 |
| `BRR` | 500 | 2026-09-07 @ $2.51 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+28.0; leftover $1256.70 |
