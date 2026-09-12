# Factor mine action — `union_coil_off_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ coil_off, no 🚨

Cash book **-9.28%** ($9,072) · signal-only (no cash/fees) was -8.84%. Starts YES **0/21**. Fills 148 · skips 80 · realized $-928.02.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior 5-session return is at least 0%.
- Must-have: prior 5-session return is at most 10% (not already exploded).
- Must-have: prior relative volume is at least 0.7.
- Must-have: prior relative volume is at most 2.2 (not a blow-off).
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
- **Gate** `ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,071.99.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TPG` | 98 | — | $50.62 | +0.00 | $54.62 | +391.69 | +391.69 | +0.00 | +391.69 |
| 2026-08-13 | `VOR` | 227 | — | $22.01 | +0.00 | $23.29 | +290.56 | +290.56 | +0.00 | +290.56 |
| 2026-08-14 | `TPG` | 98 | $54.62 | $55.29 | +65.66 | — | +0.00 | +65.66 | +457.35 | — |
| 2026-08-14 | `VOR` | 227 | $23.29 | $23.33 | +9.08 | — | +0.00 | +9.08 | +299.64 | — |
| 2026-08-14 | `TLN` | 3 | — | $359.83 | +0.00 | $362.74 | +8.73 | +8.73 | +0.00 | +8.73 |
| 2026-08-14 | `SLG` | 23 | — | $57.61 | +0.00 | $56.09 | -34.96 | -34.96 | +0.00 | -34.96 |
| 2026-08-14 | `LDI` | 1433 | — | $0.94 | +0.00 | $0.90 | -57.32 | -57.32 | +0.00 | -57.32 |
| 2026-08-14 | `BTBT` | 895 | — | $1.50 | +0.00 | $1.57 | +62.65 | +62.65 | +0.00 | +62.65 |
| 2026-08-14 | `ANGX` | 311 | — | $4.31 | +0.00 | $4.37 | +18.66 | +18.66 | +0.00 | +18.66 |
| 2026-08-14 | `HYLN` | 321 | — | $4.18 | +0.00 | $4.06 | -38.52 | -38.52 | +0.00 | -38.52 |
| 2026-08-14 | `WDC` | 2 | — | $503.50 | +0.00 | $508.80 | +10.60 | +10.60 | +0.00 | +10.60 |
| 2026-08-14 | `ADUR` | 81 | — | $16.50 | +0.00 | $16.17 | -26.73 | -26.73 | +0.00 | -26.73 |
| 2026-08-17 | `TLN` | 3 | $362.74 | $367.88 | +15.42 | — | +0.00 | +15.42 | +24.15 | — |
| 2026-08-17 | `SLG` | 23 | $56.09 | $55.37 | -16.56 | — | +0.00 | -16.56 | -51.52 | — |
| 2026-08-17 | `LDI` | 1433 | $0.90 | $0.91 | +14.33 | — | +0.00 | +14.33 | -42.99 | — |
| 2026-08-17 | `BTBT` | 895 | $1.57 | $1.52 | -44.75 | — | +0.00 | -44.75 | +17.90 | — |
| 2026-08-17 | `ANGX` | 311 | $4.37 | $4.60 | +71.53 | — | +0.00 | +71.53 | +90.19 | — |
| 2026-08-17 | `HYLN` | 321 | $4.06 | $4.10 | +12.84 | — | +0.00 | +12.84 | -25.68 | — |
| 2026-08-17 | `WDC` | 2 | $508.80 | $525.53 | +33.46 | — | +0.00 | +33.46 | +44.06 | — |
| 2026-08-17 | `ADUR` | 81 | $16.17 | $15.73 | -35.64 | — | +0.00 | -35.64 | -62.37 | — |
| 2026-08-17 | `DVN` | 46 | — | $46.18 | +0.00 | $47.57 | +63.94 | +63.94 | +0.00 | +63.94 |
| 2026-08-17 | `DNN` | 657 | — | $3.24 | +0.00 | $3.19 | -32.85 | -32.85 | +0.00 | -32.85 |
| 2026-08-17 | `OCC` | 116 | — | $18.24 | +0.00 | $17.12 | -129.92 | -129.92 | +0.00 | -129.92 |
| 2026-08-17 | `ALM` | 131 | — | $16.20 | +0.00 | $16.36 | +20.96 | +20.96 | +0.00 | +20.96 |
| 2026-08-17 | `NEWP` | 306 | — | $6.94 | +0.00 | $6.66 | -85.68 | -85.68 | +0.00 | -85.68 |
| 2026-08-18 | `DVN` | 46 | $47.57 | $48.00 | +19.78 | — | +0.00 | +19.78 | +83.72 | — |
| 2026-08-18 | `DNN` | 657 | $3.19 | $3.11 | -52.56 | — | +0.00 | -52.56 | -85.41 | — |
| 2026-08-18 | `OCC` | 116 | $17.12 | $16.20 | -106.72 | — | +0.00 | -106.72 | -236.64 | — |
| 2026-08-18 | `ALM` | 131 | $16.36 | $15.78 | -75.98 | — | +0.00 | -75.98 | -55.02 | — |
| 2026-08-18 | `NEWP` | 306 | $6.66 | $6.51 | -45.90 | — | +0.00 | -45.90 | -131.58 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 61 | — | $20.55 | +0.00 | $21.19 | +39.04 | +39.04 | +0.00 | +39.04 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `HDSN` | 220 | — | $5.77 | +0.00 | $5.57 | -44.00 | -44.00 | +0.00 | -44.00 |
| 2026-08-20 | `IAG` | 64 | — | $19.63 | +0.00 | $20.50 | +55.68 | +55.68 | +0.00 | +55.68 |
| 2026-08-20 | `KGC` | 42 | — | $29.63 | +0.00 | $31.43 | +75.60 | +75.60 | +0.00 | +75.60 |
| 2026-08-20 | `NFGC` | 727 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `DNA` | 170 | — | $7.45 | +0.00 | $6.96 | -83.30 | -83.30 | +0.00 | -83.30 |
| 2026-08-20 | `EXK` | 118 | — | $10.77 | +0.00 | $10.97 | +23.60 | +23.60 | +0.00 | +23.60 |
| 2026-08-21 | `AG` | 61 | $21.19 | $21.90 | +43.31 | — | +0.00 | +43.31 | +82.35 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `HDSN` | 220 | $5.57 | $5.67 | +22.00 | — | +0.00 | +22.00 | -22.00 | — |
| 2026-08-21 | `IAG` | 64 | $20.50 | $21.17 | +42.88 | — | +0.00 | +42.88 | +98.56 | — |
| 2026-08-21 | `KGC` | 42 | $31.43 | $32.17 | +31.08 | — | +0.00 | +31.08 | +106.68 | — |
| 2026-08-21 | `NFGC` | 727 | $1.75 | $1.79 | +29.08 | — | +0.00 | +29.08 | +29.08 | — |
| 2026-08-21 | `DNA` | 170 | $6.96 | $7.09 | +22.10 | — | +0.00 | +22.10 | -61.20 | — |
| 2026-08-21 | `EXK` | 118 | $10.97 | $11.34 | +43.66 | — | +0.00 | +43.66 | +67.26 | — |
| 2026-08-21 | `CRSP` | 21 | — | $59.72 | +0.00 | $59.50 | -4.62 | -4.62 | +0.00 | -4.62 |
| 2026-08-21 | `BTBT` | 790 | — | $1.66 | +0.00 | $1.53 | -102.70 | -102.70 | +0.00 | -102.70 |
| 2026-08-21 | `ORBS` | 1518 | — | $0.86 | +0.00 | $0.88 | +24.29 | +24.29 | +0.00 | +24.29 |
| 2026-08-21 | `CF` | 10 | — | $127.43 | +0.00 | $129.60 | +21.70 | +21.70 | +0.00 | +21.70 |
| 2026-08-21 | `EMBC` | 241 | — | $5.43 | +0.00 | $5.23 | -48.20 | -48.20 | +0.00 | -48.20 |
| 2026-08-21 | `TXG` | 20 | — | $64.39 | +0.00 | $65.12 | +14.60 | +14.60 | +0.00 | +14.60 |
| 2026-08-21 | `DXYZ` | 37 | — | $34.89 | +0.00 | $34.43 | -17.02 | -17.02 | +0.00 | -17.02 |
| 2026-08-21 | `BEKE` | 73 | — | $17.93 | +0.00 | $17.75 | -13.50 | -13.50 | +0.00 | -13.50 |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | — | +0.00 | -15.75 | -20.37 | — |
| 2026-08-24 | `BTBT` | 790 | $1.53 | $1.55 | +15.80 | — | +0.00 | +15.80 | -86.90 | — |
| 2026-08-24 | `ORBS` | 1518 | $0.88 | $0.89 | +15.18 | — | +0.00 | +15.18 | +39.47 | — |
| 2026-08-24 | `CF` | 10 | $129.60 | $129.99 | +3.90 | — | +0.00 | +3.90 | +25.60 | — |
| 2026-08-24 | `EMBC` | 241 | $5.23 | $5.20 | -8.44 | — | +0.00 | -8.44 | -56.63 | — |
| 2026-08-24 | `TXG` | 20 | $65.12 | $63.15 | -39.40 | — | +0.00 | -39.40 | -24.80 | — |
| 2026-08-24 | `DXYZ` | 37 | $34.43 | $33.10 | -49.21 | — | +0.00 | -49.21 | -66.23 | — |
| 2026-08-24 | `BEKE` | 73 | $17.75 | $18.05 | +22.26 | — | +0.00 | +22.26 | +8.76 | — |
| 2026-08-25 | `OCUL` | 116 | — | $10.98 | +0.00 | $10.88 | -11.60 | -11.60 | +0.00 | -11.60 |
| 2026-08-25 | `RZLT` | 258 | — | $4.94 | +0.00 | $5.01 | +18.06 | +18.06 | +0.00 | +18.06 |
| 2026-08-25 | `HCA` | 2 | — | $426.97 | +0.00 | $428.76 | +3.58 | +3.58 | +0.00 | +3.58 |
| 2026-08-25 | `KURA` | 94 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `LIFE` | 34 | — | $36.96 | +0.00 | $38.56 | +54.40 | +54.40 | +0.00 | +54.40 |
| 2026-08-25 | `AMTX` | 673 | — | $1.90 | +0.00 | $1.91 | +6.73 | +6.73 | +0.00 | +6.73 |
| 2026-08-25 | `AVAH` | 93 | — | $13.62 | +0.00 | $13.59 | -3.26 | -3.26 | +0.00 | -3.26 |
| 2026-08-25 | `ETON` | 19 | — | $64.55 | +0.00 | $63.05 | -28.50 | -28.50 | +0.00 | -28.50 |
| 2026-08-26 | `OCUL` | 116 | $10.88 | $10.79 | -10.44 | — | +0.00 | -10.44 | -22.04 | — |
| 2026-08-26 | `RZLT` | 258 | $5.01 | $5.01 | +0.00 | $5.04 | +7.74 | +7.74 | +18.06 | +25.80 |
| 2026-08-26 | `HCA` | 2 | $428.76 | $427.50 | -2.52 | — | +0.00 | -2.52 | +1.06 | — |
| 2026-08-26 | `KURA` | 94 | $13.59 | $13.63 | +3.76 | — | +0.00 | +3.76 | +3.76 | — |
| 2026-08-26 | `LIFE` | 34 | $38.56 | $38.24 | -10.88 | — | +0.00 | -10.88 | +43.52 | — |
| 2026-08-26 | `AMTX` | 673 | $1.91 | $1.91 | +0.00 | — | +0.00 | +0.00 | +6.73 | — |
| 2026-08-26 | `AVAH` | 93 | $13.59 | $13.65 | +5.58 | — | +0.00 | +5.58 | +2.33 | — |
| 2026-08-26 | `ETON` | 19 | $63.05 | $63.60 | +10.45 | — | +0.00 | +10.45 | -18.05 | — |
| 2026-08-26 | `INSP` | 21 | — | $60.07 | +0.00 | $61.80 | +36.33 | +36.33 | +0.00 | +36.33 |
| 2026-08-26 | `CRMD` | 148 | — | $8.60 | +0.00 | $8.39 | -31.08 | -31.08 | +0.00 | -31.08 |
| 2026-08-26 | `SENS` | 134 | — | $9.48 | +0.00 | $9.34 | -18.76 | -18.76 | +0.00 | -18.76 |
| 2026-08-26 | `BE` | 5 | — | $213.94 | +0.00 | $218.21 | +21.35 | +21.35 | +0.00 | +21.35 |
| 2026-08-26 | `ACRS` | 195 | — | $6.53 | +0.00 | $6.19 | -66.30 | -66.30 | +0.00 | -66.30 |
| 2026-08-26 | `TMCI` | 266 | — | $4.78 | +0.00 | $4.72 | -15.96 | -15.96 | +0.00 | -15.96 |
| 2026-08-26 | `CRDL` | 628 | — | $2.03 | +0.00 | $2.14 | +69.08 | +69.08 | +0.00 | +69.08 |
| 2026-08-27 | `RZLT` | 258 | $5.04 | $5.07 | +7.74 | — | +0.00 | +7.74 | +33.54 | — |
| 2026-08-27 | `INSP` | 21 | $61.80 | $62.10 | +6.30 | — | +0.00 | +6.30 | +42.63 | — |
| 2026-08-27 | `CRMD` | 148 | $8.39 | $8.49 | +14.80 | — | +0.00 | +14.80 | -16.28 | — |
| 2026-08-27 | `SENS` | 134 | $9.34 | $9.33 | -1.34 | — | +0.00 | -1.34 | -20.10 | — |
| 2026-08-27 | `BE` | 5 | $218.21 | $227.10 | +44.45 | — | +0.00 | +44.45 | +65.80 | — |
| 2026-08-27 | `ACRS` | 195 | $6.19 | $6.15 | -7.80 | — | +0.00 | -7.80 | -74.10 | — |
| 2026-08-27 | `TMCI` | 266 | $4.72 | $4.72 | +0.00 | — | +0.00 | +0.00 | -15.96 | — |
| 2026-08-27 | `CRDL` | 628 | $2.14 | $2.09 | -31.40 | — | +0.00 | -31.40 | +37.68 | — |
| 2026-08-27 | `RRC` | 30 | — | $41.44 | +0.00 | $41.64 | +6.00 | +6.00 | +0.00 | +6.00 |
| 2026-08-27 | `CRK` | 88 | — | $14.42 | +0.00 | $14.62 | +17.60 | +17.60 | +0.00 | +17.60 |
| 2026-08-27 | `MOS` | 53 | — | $24.00 | +0.00 | $23.76 | -12.72 | -12.72 | +0.00 | -12.72 |
| 2026-08-27 | `ANET` | 6 | — | $205.90 | +0.00 | $201.09 | -28.86 | -28.86 | +0.00 | -28.86 |
| 2026-08-27 | `DLO` | 83 | — | $15.33 | +0.00 | $15.14 | -15.77 | -15.77 | +0.00 | -15.77 |
| 2026-08-27 | `GEN` | 42 | — | $29.83 | +0.00 | $30.50 | +28.14 | +28.14 | +0.00 | +28.14 |
| 2026-08-27 | `MRVL` | 5 | — | $253.44 | +0.00 | $241.45 | -59.95 | -59.95 | +0.00 | -59.95 |
| 2026-08-27 | `NUE` | 5 | — | $252.00 | +0.00 | $252.37 | +1.85 | +1.85 | +0.00 | +1.85 |
| 2026-08-28 | `RRC` | 30 | $41.64 | $41.74 | +3.00 | — | +0.00 | +3.00 | +9.00 | — |
| 2026-08-28 | `CRK` | 88 | $14.62 | $14.63 | +0.88 | $14.29 | -29.92 | -29.04 | +18.48 | -11.44 |
| 2026-08-28 | `MOS` | 53 | $23.76 | $23.95 | +10.07 | — | +0.00 | +10.07 | -2.65 | — |
| 2026-08-28 | `ANET` | 6 | $201.09 | $200.00 | -6.54 | — | +0.00 | -6.54 | -35.40 | — |
| 2026-08-28 | `DLO` | 83 | $15.14 | $15.19 | +4.15 | — | +0.00 | +4.15 | -11.62 | — |
| 2026-08-28 | `GEN` | 42 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +28.14 | — |
| 2026-08-28 | `MRVL` | 5 | $241.45 | $225.26 | -80.95 | — | +0.00 | -80.95 | -140.90 | — |
| 2026-08-28 | `NUE` | 5 | $252.37 | $252.76 | +1.95 | — | +0.00 | +1.95 | +3.80 | — |
| 2026-08-28 | `GRRR` | 79 | — | $15.66 | +0.00 | $14.41 | -98.75 | -98.75 | +0.00 | -98.75 |
| 2026-08-28 | `TTMI` | 10 | — | $122.81 | +0.00 | $118.65 | -41.60 | -41.60 | +0.00 | -41.60 |
| 2026-08-28 | `EQ` | 508 | — | $2.46 | +0.00 | $2.39 | -35.56 | -35.56 | +0.00 | -35.56 |
| 2026-08-28 | `BTSG` | 20 | — | $60.54 | +0.00 | $59.13 | -28.20 | -28.20 | +0.00 | -28.20 |
| 2026-08-28 | `CRDL` | 607 | — | $2.06 | +0.00 | $1.94 | -72.84 | -72.84 | +0.00 | -72.84 |
| 2026-08-28 | `ZYME` | 43 | — | $28.91 | +0.00 | $28.27 | -27.52 | -27.52 | +0.00 | -27.52 |
| 2026-08-28 | `ADBT` | 250 | — | $4.99 | +0.00 | $4.99 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-31 | `CRK` | 88 | $14.29 | $14.54 | +22.00 | — | +0.00 | +22.00 | +10.56 | — |
| 2026-08-31 | `GRRR` | 79 | $14.41 | $14.44 | +2.37 | — | +0.00 | +2.37 | -96.38 | — |
| 2026-08-31 | `TTMI` | 10 | $118.65 | $118.83 | +1.80 | — | +0.00 | +1.80 | -39.80 | — |
| 2026-08-31 | `EQ` | 508 | $2.39 | $2.39 | +0.00 | — | +0.00 | +0.00 | -35.56 | — |
| 2026-08-31 | `BTSG` | 20 | $59.13 | $58.76 | -7.40 | — | +0.00 | -7.40 | -35.60 | — |
| 2026-08-31 | `CRDL` | 607 | $1.94 | $1.92 | -12.14 | — | +0.00 | -12.14 | -84.98 | — |
| 2026-08-31 | `ZYME` | 43 | $28.27 | $28.06 | -9.03 | — | +0.00 | -9.03 | -36.55 | — |
| 2026-08-31 | `ADBT` | 250 | $4.99 | $4.94 | -12.50 | — | +0.00 | -12.50 | -12.50 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 22 | — | $52.88 | +0.00 | $52.46 | -9.24 | -9.24 | +0.00 | -9.24 |
| 2026-09-03 | `HRMY` | 28 | — | $42.93 | +0.00 | $41.86 | -29.96 | -29.96 | +0.00 | -29.96 |
| 2026-09-03 | `CABA` | 331 | — | $3.63 | +0.00 | $3.48 | -49.65 | -49.65 | +0.00 | -49.65 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `CRK` | 77 | — | $15.45 | +0.00 | $14.95 | -38.50 | -38.50 | +0.00 | -38.50 |
| 2026-09-03 | `ARCT` | 71 | — | $16.77 | +0.00 | $15.56 | -85.91 | -85.91 | +0.00 | -85.91 |
| 2026-09-03 | `CRDL` | 552 | — | $2.18 | +0.00 | $2.16 | -11.04 | -11.04 | +0.00 | -11.04 |
| 2026-09-03 | `SDGR` | 57 | — | $21.03 | +0.00 | $20.71 | -18.24 | -18.24 | +0.00 | -18.24 |
| 2026-09-04 | `ATRC` | 22 | $52.46 | $52.03 | -9.46 | $51.52 | -11.22 | -20.68 | -18.70 | -29.92 |
| 2026-09-04 | `HRMY` | 28 | $41.86 | $41.50 | -10.08 | $42.25 | +21.00 | +10.92 | -40.04 | -19.04 |
| 2026-09-04 | `CABA` | 331 | $3.48 | $3.46 | -6.62 | $3.47 | +3.31 | -3.31 | -56.27 | -52.96 |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `CRK` | 77 | $14.95 | $15.00 | +3.85 | — | +0.00 | +3.85 | -34.65 | — |
| 2026-09-04 | `ARCT` | 71 | $15.56 | $15.61 | +3.55 | — | +0.00 | +3.55 | -82.36 | — |
| 2026-09-04 | `CRDL` | 552 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -11.04 | — |
| 2026-09-04 | `SDGR` | 57 | $20.71 | $20.58 | -7.41 | — | +0.00 | -7.41 | -25.65 | — |
| 2026-09-04 | `ALEC` | 464 | — | $2.52 | +0.00 | $2.46 | -27.84 | -27.84 | +0.00 | -27.84 |
| 2026-09-04 | `BHC` | 174 | — | $6.71 | +0.00 | $6.56 | -26.10 | -26.10 | +0.00 | -26.10 |
| 2026-09-04 | `OABI` | 244 | — | $4.78 | +0.00 | $4.33 | -109.80 | -109.80 | +0.00 | -109.80 |
| 2026-09-04 | `VIR` | 103 | — | $11.31 | +0.00 | $11.38 | +7.72 | +7.72 | +0.00 | +7.72 |
| 2026-09-04 | `CRM` | 4 | — | $263.36 | +0.00 | $259.23 | -16.52 | -16.52 | +0.00 | -16.52 |
| 2026-09-08 | `ATRC` | 22 | $51.52 | $54.31 | +61.38 | — | +0.00 | +61.38 | +31.46 | — |
| 2026-09-08 | `HRMY` | 28 | $42.25 | $42.20 | -1.40 | — | +0.00 | -1.40 | -20.44 | — |
| 2026-09-08 | `CABA` | 331 | $3.47 | $3.43 | -13.24 | — | +0.00 | -13.24 | -66.20 | — |
| 2026-09-08 | `ALEC` | 464 | $2.46 | $2.38 | -37.12 | — | +0.00 | -37.12 | -64.96 | — |
| 2026-09-08 | `BHC` | 174 | $6.56 | $6.57 | +1.74 | — | +0.00 | +1.74 | -24.36 | — |
| 2026-09-08 | `OABI` | 244 | $4.33 | $4.30 | -7.32 | — | +0.00 | -7.32 | -117.12 | — |
| 2026-09-08 | `VIR` | 103 | $11.38 | $11.22 | -16.99 | — | +0.00 | -16.99 | -9.27 | — |
| 2026-09-08 | `CRM` | 4 | $259.23 | $253.72 | -22.04 | — | +0.00 | -22.04 | -38.56 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +682.25 | TPG, VOR | — | $37.44 | $10,677.03 | TPG×98, VOR×227 |
| 2026-08-14 | +5.50 | $37.44 | TPG×98, VOR×227 | $10,751.77 | +74.74 | -56.89 | TLN, SLG, LDI, BTBT, ANGX, HYLN, WDC, ADUR | TPG, VOR | $585.28 | $10,643.82 | TLN×3, SLG×23, LDI×1433, BTBT×895, ANGX×311, HYLN×321, WDC×2, ADUR×81 |
| 2026-08-17 | +2.25 | $585.28 | TLN×3, SLG×23, LDI×1433, BTBT×895, ANGX×311, HYLN×321, WDC×2, ADUR×81 | $10,694.45 | +50.63 | -163.55 | DVN, DNN, OCC, ALM, NEWP | TLN, SLG, LDI, BTBT, ANGX, HYLN, WDC, ADUR | $14.64 | $10,465.73 | DVN×46, DNN×657, OCC×116, ALM×131, NEWP×306 |
| 2026-08-18 | -6.20 | $14.64 | DVN×46, DNN×657, OCC×116, ALM×131, NEWP×306 | $10,204.35 | -261.38 | +0.00 | — | DVN, DNN, OCC, ALM, NEWP | $10,184.78 | $10,184.78 | — |
| 2026-08-19 | -7.20 | $10,184.78 | — | $10,184.78 | +0.00 | +0.00 | — | — | $10,184.78 | $10,184.78 | — |
| 2026-08-20 | +1.12 | $10,184.78 | — | $10,184.78 | +0.00 | +100.68 | AG, BHP, HDSN, IAG, KGC, NFGC, DNA, EXK | — | $142.75 | $10,259.90 | AG×61, BHP×13, HDSN×220, IAG×64, KGC×42, NFGC×727, DNA×170, EXK×118 |
| 2026-08-21 | +3.25 | $142.75 | AG×61, BHP×13, HDSN×220, IAG×64, KGC×42, NFGC×727, DNA×170, EXK×118 | $10,521.18 | +261.28 | -125.45 | CRSP, BTBT, ORBS, CF, EMBC, TXG, DXYZ, BEKE | AG, BHP, HDSN, IAG, KGC, NFGC, DNA, EXK | $105.91 | $10,328.44 | CRSP×21, BTBT×790, ORBS×1518, CF×10, EMBC×241, TXG×20, DXYZ×37, BEKE×73 |
| 2026-08-24 | -5.17 | $105.91 | CRSP×21, BTBT×790, ORBS×1518, CF×10, EMBC×241, TXG×20, DXYZ×37, BEKE×73 | $10,272.79 | -55.65 | +0.00 | — | CRSP, BTBT, ORBS, CF, EMBC, TXG, DXYZ, BEKE | $10,230.43 | $10,230.43 | — |
| 2026-08-25 | +1.80 | $10,230.43 | — | $10,230.43 | +0.00 | +39.41 | OCUL, RZLT, HCA, KURA, LIFE, AMTX, AVAH, ETON | — | $496.89 | $10,244.82 | OCUL×116, RZLT×258, HCA×2, KURA×94, LIFE×34, AMTX×673, AVAH×93, ETON×19 |
| 2026-08-26 | +2.02 | $496.89 | OCUL×116, RZLT×258, HCA×2, KURA×94, LIFE×34, AMTX×673, AVAH×93, ETON×19 | $10,240.77 | -4.05 | +2.40 | INSP, CRMD, SENS, BE, ACRS, TMCI, CRDL | OCUL, HCA, KURA, LIFE, AMTX, AVAH, ETON | $209.28 | $10,198.22 | RZLT×258, INSP×21, CRMD×148, SENS×134, BE×5, ACRS×195, TMCI×266, CRDL×628 |
| 2026-08-27 | — | $209.28 | RZLT×258, INSP×21, CRMD×148, SENS×134, BE×5, ACRS×195, TMCI×266, CRDL×628 | $10,230.97 | +32.75 | -63.71 | RRC, CRK, MOS, ANET, DLO, GEN, MRVL, NUE | RZLT, INSP, CRMD, SENS, BE, ACRS, TMCI, CRDL | $115.41 | $10,123.71 | RRC×30, CRK×88, MOS×53, ANET×6, DLO×83, GEN×42, MRVL×5, NUE×5 |
| 2026-08-28 | +0.75 | $115.41 | RRC×30, CRK×88, MOS×53, ANET×6, DLO×83, GEN×42, MRVL×5, NUE×5 | $10,056.27 | -67.44 | -334.39 | GRRR, TTMI, EQ, BTSG, CRDL, ZYME, ADBT | RRC, MOS, ANET, DLO, GEN, MRVL, NUE | $61.29 | $9,681.11 | CRK×88, GRRR×79, TTMI×10, EQ×508, BTSG×20, CRDL×607, ZYME×43, ADBT×250 |
| 2026-08-31 | -5.85 | $61.29 | CRK×88, GRRR×79, TTMI×10, EQ×508, BTSG×20, CRDL×607, ZYME×43, ADBT×250 | $9,666.21 | -14.90 | +0.00 | — | CRK, GRRR, TTMI, EQ, BTSG, CRDL, ZYME, ADBT | $9,637.57 | $9,637.57 | — |
| 2026-09-01 | -6.30 | $9,637.57 | — | $9,637.57 | +0.00 | +0.00 | — | — | $9,637.57 | $9,637.57 | — |
| 2026-09-02 | -3.83 | $9,637.57 | — | $9,637.57 | +0.00 | +0.00 | — | — | $9,637.57 | $9,637.57 | — |
| 2026-09-03 | -0.90 | $9,637.57 | — | $9,637.57 | +0.00 | -258.92 | ATRC, HRMY, CABA, RVTY, CRK, ARCT, CRDL, SDGR | — | $72.08 | $9,354.53 | ATRC×22, HRMY×28, CABA×331, RVTY×9, CRK×77, ARCT×71, CRDL×552, SDGR×57 |
| 2026-09-04 | +2.25 | $72.08 | ATRC×22, HRMY×28, CABA×331, RVTY×9, CRK×77, ARCT×71, CRDL×552, SDGR×57 | $9,322.96 | -31.57 | -159.45 | ALEC, BHC, OABI, VIR, CRM | RVTY, CRK, ARCT, CRDL, SDGR | $117.67 | $9,131.66 | ATRC×22, HRMY×28, CABA×331, ALEC×464, BHC×174, OABI×244, VIR×103, CRM×4 |
| 2026-09-08 | -11.47 | $117.67 | ATRC×22, HRMY×28, CABA×331, ALEC×464, BHC×174, OABI×244, VIR×103, CRM×4 | $9,096.66 | -35.00 | +0.00 | — | ATRC, HRMY, CABA, ALEC, BHC, OABI, VIR, CRM | $9,071.99 | $9,071.99 | — |
| 2026-09-09 | -13.95 | $9,071.99 | — | $9,071.99 | -0.00 | +0.00 | — | — | $9,071.99 | $9,071.99 | — |
| 2026-09-10 | -13.28 | $9,071.99 | — | $9,071.99 | -0.00 | +0.00 | — | — | $9,071.99 | $9,071.99 | — |
| 2026-09-11 | +0.50 | $9,071.99 | — | $9,071.99 | -0.00 | +0.00 | — | — | $9,071.99 | $9,071.99 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 98 | $50.62 | $2.28 | — | $5,036.64 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 227 | $22.01 | $2.93 | — | $37.44 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.44 | ▲ close $10,677.03 vs 09:30 $10,000.00 (session +682.25) | 16:00 close · cash $37.44 · equity $10,677.03 vs 09:30 $10,000.00 (+677.03; session marks +682.25) · 2 name(s) marked open→close (per-name table). TPG×98 09:30 $50.62 → close $54.62 +391.69; VOR×227 09:30 $22.01 → close $23.29 +290.56 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.44 | ▲ 09:30 equity $10,751.77 vs yday $10,677.03 (+74.74) | 09:30 open · cash $37.44 (unchanged overnight, no fees) · equity $10,751.77 vs prior close $10,677.03 (+74.74) · 2 name(s) re-marked at the open (per-name table). TPG×98 yday $54.62 → 09:30 $55.29 +65.66; VOR×227 yday $23.29 → 09:30 $23.33 +9.08 | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 98 | $55.29 | $2.34 | $+452.72 | $5,453.52 | ▲ +452.72 after sell → book $10,749.43; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 227 | $23.33 | $3.01 | $+293.70 | $10,746.42 | ▲ +293.70 after sell → book $10,746.42; vs 09:30 mark -3.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,664.93 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+5.9; leftover $1343.30 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 23 | $57.61 | $2.06 | — | $8,337.84 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1343.30 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1433 | $0.94 | $17.73 | — | $6,977.40 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1343.30 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 895 | $1.50 | $11.55 | — | $5,623.35 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1343.30 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 311 | $4.31 | $4.01 | — | $4,278.93 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1343.30 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 321 | $4.18 | $4.14 | — | $2,933.01 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1343.30 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $1,924.01 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable; 🔵; ⚪; ret5=+7.9; leftover $1343.30 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 81 | $16.50 | $2.23 | — | $585.28 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1343.30 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $585.28 | ▼ close $10,643.82 vs 09:30 $10,751.77 (session -56.89) | 16:00 close · cash $585.28 · equity $10,643.82 vs 09:30 $10,751.77 (-107.95; session marks -56.89) · 8 name(s) marked open→close (per-name table). TLN×3 09:30 $359.83 → close $362.74 +8.73; SLG×23 09:30 $57.61 → close $56.09 -34.96; LDI×1433 09:30 $0.94 → close $0.90 -57.32; BTBT×895 09:30 $1.50 → close $1.57 +62.65; ANGX×311 09:30 $4.31 → close $4.37 +18.66; HYLN×321 09:30 $4.18 → close $4.06 -38.52; WDC×2 09:30 $503.50 → close $508.80 +10.60; ADUR×81 09:30 $16.50 → close $16.17 -26.73 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $585.28 | ▲ 09:30 equity $10,694.45 vs yday $10,643.82 (+50.63) | 09:30 open · cash $585.28 (unchanged overnight, no fees) · equity $10,694.45 vs prior close $10,643.82 (+50.63) · 8 name(s) re-marked at the open (per-name table). TLN×3 yday $362.74 → 09:30 $367.88 +15.42; SLG×23 yday $56.09 → 09:30 $55.37 -16.56; LDI×1433 yday $0.90 → 09:30 $0.91 +14.33; BTBT×895 yday $1.57 → 09:30 $1.52 -44.75; ANGX×311 yday $4.37 → 09:30 $4.60 +71.53; HYLN×321 yday $4.06 → 09:30 $4.10 +12.84; WDC×2 yday $508.80 → 09:30 $525.53 +33.46; ADUR×81 yday $16.17 → 09:30 $15.73 -35.64 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,686.90 | ▲ +20.13 after sell → book $10,692.43; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 23 | $55.37 | $2.08 | $-55.66 | $2,958.33 | ▼ -55.66 after sell → book $10,690.35; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1433 | $0.91 | $17.54 | $-78.26 | $4,240.52 | ▼ -78.26 after sell → book $10,672.81; vs 09:30 mark -17.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 895 | $1.52 | $11.71 | $-5.35 | $5,589.21 | ▼ -5.35 after sell → book $10,661.10; vs 09:30 mark -11.71 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 311 | $4.60 | $4.08 | $+82.10 | $7,015.74 | ▲ +82.10 after sell → book $10,657.03; vs 09:30 mark -4.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 321 | $4.10 | $4.20 | $-34.03 | $8,327.63 | ▼ -34.03 after sell → book $10,652.82; vs 09:30 mark -4.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $9,376.68 | ▲ +40.05 after sell → book $10,650.81; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 81 | $15.73 | $2.26 | $-66.86 | $10,648.55 | ▼ -66.86 after sell → book $10,648.55; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 46 | $46.18 | $2.13 | — | $8,522.14 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+6.7; leftover $2129.71 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 657 | $3.24 | $8.48 | — | $6,384.99 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $2129.71 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 116 | $18.24 | $2.34 | — | $4,266.81 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $2129.71 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 131 | $16.20 | $2.38 | — | $2,142.23 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $2129.71 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NEWP` | 306 | $6.94 | $3.95 | — | $14.64 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.1; leftover $2129.71 | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.64 | ▼ close $10,465.73 vs 09:30 $10,694.45 (session -163.55) | 16:00 close · cash $14.64 · equity $10,465.73 vs 09:30 $10,694.45 (-228.72; session marks -163.55) · 5 name(s) marked open→close (per-name table). DVN×46 09:30 $46.18 → close $47.57 +63.94; DNN×657 09:30 $3.24 → close $3.19 -32.85; OCC×116 09:30 $18.24 → close $17.12 -129.92; ALM×131 09:30 $16.20 → close $16.36 +20.96; NEWP×306 09:30 $6.94 → close $6.66 -85.68 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.64 | ▼ 09:30 equity $10,204.35 vs yday $10,465.73 (-261.38) | 09:30 open · cash $14.64 (unchanged overnight, no fees) · equity $10,204.35 vs prior close $10,465.73 (-261.38) · 5 name(s) re-marked at the open (per-name table). DVN×46 yday $47.57 → 09:30 $48.00 +19.78; DNN×657 yday $3.19 → 09:30 $3.11 -52.56; OCC×116 yday $17.12 → 09:30 $16.20 -106.72; ALM×131 yday $16.36 → 09:30 $15.78 -75.98; NEWP×306 yday $6.66 → 09:30 $6.51 -45.90 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 46 | $48.00 | $2.16 | $+79.44 | $2,220.48 | ▲ +79.44 after sell → book $10,202.19; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 657 | $3.11 | $8.60 | $-102.49 | $4,255.15 | ▼ -102.49 after sell → book $10,193.59; vs 09:30 mark -8.60 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 116 | $16.20 | $2.37 | $-241.35 | $6,131.98 | ▼ -241.35 after sell → book $10,191.22; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 131 | $15.78 | $2.42 | $-59.82 | $8,196.74 | ▼ -59.82 after sell → book $10,188.80; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NEWP` | 306 | $6.51 | $4.01 | $-139.54 | $10,184.78 | ▼ -139.54 after sell → book $10,184.78; vs 09:30 mark -4.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,184.78 | ▲ close $10,184.78 vs 09:30 $10,204.35 (session +0.00) | 16:00 close · cash $10,184.78 · no lots left · equity $10,184.78. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,184.78 | ▲ 09:30 equity $10,184.78 vs yday $10,184.78 (+0.00) | 09:30 open · cash $10,184.78 · no holdings · equity $10,184.78 vs prior close $10,184.78 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,184.78 | ▲ close $10,184.78 vs 09:30 $10,184.78 (session +0.00) | 16:00 close · cash $10,184.78 · no lots left · equity $10,184.78. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,184.78 | ▲ 09:30 equity $10,184.78 vs yday $10,184.78 (+0.00) | 09:30 open · cash $10,184.78 · no holdings · equity $10,184.78 vs prior close $10,184.78 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,929.06 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1273.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,743.90 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1273.10 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 220 | $5.77 | $2.84 | — | $6,471.66 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1273.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $5,213.16 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1273.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $3,966.59 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1273.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 727 | $1.75 | $9.38 | — | $2,684.96 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1273.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 170 | $7.45 | $2.50 | — | $1,415.96 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1273.10 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 118 | $10.77 | $2.34 | — | $142.75 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1273.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.75 | ▲ close $10,259.90 vs 09:30 $10,184.78 (session +100.68) | 16:00 close · cash $142.75 · equity $10,259.90 vs 09:30 $10,184.78 (+75.12; session marks +100.68) · 8 name(s) marked open→close (per-name table). AG×61 09:30 $20.55 → close $21.19 +39.04; BHP×13 09:30 $91.01 → close $93.63 +34.06; HDSN×220 09:30 $5.77 → close $5.57 -44.00; IAG×64 09:30 $19.63 → close $20.50 +55.68; KGC×42 09:30 $29.63 → close $31.43 +75.60; NFGC×727 09:30 $1.75 → close $1.75 +0.00; DNA×170 09:30 $7.45 → close $6.96 -83.30; EXK×118 09:30 $10.77 → close $10.97 +23.60 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.75 | ▲ 09:30 equity $10,521.18 vs yday $10,259.90 (+261.28) | 09:30 open · cash $142.75 (unchanged overnight, no fees) · equity $10,521.18 vs prior close $10,259.90 (+261.28) · 8 name(s) re-marked at the open (per-name table). AG×61 yday $21.19 → 09:30 $21.90 +43.31; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; HDSN×220 yday $5.57 → 09:30 $5.67 +22.00; IAG×64 yday $20.50 → 09:30 $21.17 +42.88; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×727 yday $1.75 → 09:30 $1.79 +29.08; DNA×170 yday $6.96 → 09:30 $7.09 +22.10; EXK×118 yday $10.97 → 09:30 $11.34 +43.66 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,476.46 | ▲ +77.98 after sell → book $10,518.99; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,718.77 | ▲ +57.15 after sell → book $10,516.94; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 220 | $5.67 | $2.88 | $-27.72 | $3,963.29 | ▼ -27.72 after sell → book $10,514.06; vs 09:30 mark -2.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 64 | $21.17 | $2.20 | $+94.17 | $5,315.96 | ▲ +94.17 after sell → book $10,511.85; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $6,664.97 | ▲ +102.43 after sell → book $10,509.72; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 727 | $1.79 | $9.51 | $+10.19 | $7,956.79 | ▲ +10.19 after sell → book $10,500.21; vs 09:30 mark -9.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 170 | $7.09 | $2.54 | $-66.24 | $9,159.55 | ▼ -66.24 after sell → book $10,497.67; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `EXK` | 118 | $11.34 | $2.37 | $+62.54 | $10,495.29 | ▲ +62.54 after sell → book $10,495.29; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $9,239.12 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1311.91 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 790 | $1.66 | $10.19 | — | $7,917.53 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1311.91 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1518 | $0.86 | $17.67 | — | $6,588.31 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1311.91 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 10 | $127.43 | $2.02 | — | $5,311.99 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable; 🔵; ⚪; ret5=+7.9; leftover $1311.91 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 241 | $5.43 | $3.11 | — | $4,000.25 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $1311.91 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TXG` | 20 | $64.39 | $2.05 | — | $2,710.40 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1311.91 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DXYZ` | 37 | $34.89 | $2.10 | — | $1,417.37 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.6; leftover $1311.91 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 73 | $17.93 | $2.21 | — | $105.91 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $1311.91 | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $105.91 | ▼ close $10,328.44 vs 09:30 $10,521.18 (session -125.45) | 16:00 close · cash $105.91 · equity $10,328.44 vs 09:30 $10,521.18 (-192.74; session marks -125.45) · 8 name(s) marked open→close (per-name table). CRSP×21 09:30 $59.72 → close $59.50 -4.62; BTBT×790 09:30 $1.66 → close $1.53 -102.70; ORBS×1518 09:30 $0.86 → close $0.88 +24.29; CF×10 09:30 $127.43 → close $129.60 +21.70; EMBC×241 09:30 $5.43 → close $5.23 -48.20; TXG×20 09:30 $64.39 → close $65.12 +14.60; DXYZ×37 09:30 $34.89 → close $34.43 -17.02; BEKE×73 09:30 $17.93 → close $17.75 -13.50 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $105.91 | ▼ 09:30 equity $10,272.79 vs yday $10,328.44 (-55.65) | 09:30 open · cash $105.91 (unchanged overnight, no fees) · equity $10,272.79 vs prior close $10,328.44 (-55.65) · 8 name(s) re-marked at the open (per-name table). CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; BTBT×790 yday $1.53 → 09:30 $1.55 +15.80; ORBS×1518 yday $0.88 → 09:30 $0.89 +15.18; CF×10 yday $129.60 → 09:30 $129.99 +3.90; EMBC×241 yday $5.23 → 09:30 $5.20 -8.44; TXG×20 yday $65.12 → 09:30 $63.15 -39.40; DXYZ×37 yday $34.43 → 09:30 $33.10 -49.21; BEKE×73 yday $17.75 → 09:30 $18.05 +22.26 | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $1,337.58 | ▼ -24.50 after sell → book $10,270.71; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 790 | $1.55 | $10.33 | $-107.42 | $2,551.75 | ▼ -107.42 after sell → book $10,260.38; vs 09:30 mark -10.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1518 | $0.89 | $18.33 | $+3.47 | $3,884.44 | ▲ +3.47 after sell → book $10,242.05; vs 09:30 mark -18.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 10 | $129.99 | $2.04 | $+21.54 | $5,182.30 | ▲ +21.54 after sell → book $10,240.01; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `EMBC` | 241 | $5.20 | $3.16 | $-62.90 | $6,431.14 | ▼ -62.90 after sell → book $10,236.85; vs 09:30 mark -3.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TXG` | 20 | $63.15 | $2.07 | $-28.92 | $7,692.07 | ▼ -28.92 after sell → book $10,234.78; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DXYZ` | 37 | $33.10 | $2.12 | $-70.45 | $8,914.65 | ▼ -70.45 after sell → book $10,232.66; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 73 | $18.05 | $2.23 | $+4.32 | $10,230.43 | ▲ +4.32 after sell → book $10,230.43; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,230.43 | ▲ close $10,230.43 vs 09:30 $10,272.79 (session +0.00) | 16:00 close · cash $10,230.43 · no lots left · equity $10,230.43. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,230.43 | ▲ 09:30 equity $10,230.43 vs yday $10,230.43 (+0.00) | 09:30 open · cash $10,230.43 · no holdings · equity $10,230.43 vs prior close $10,230.43 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 116 | $10.98 | $2.34 | — | $8,954.41 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+1.2; leftover $1278.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 258 | $4.94 | $3.33 | — | $7,676.57 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+7.1; leftover $1278.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $426.97 | $2.00 | — | $6,820.63 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+6.0; leftover $1278.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 94 | $13.59 | $2.27 | — | $5,540.90 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1278.80 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 34 | $36.96 | $2.09 | — | $4,282.17 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1278.80 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 673 | $1.90 | $8.68 | — | $2,994.78 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ⚪; ret5=+5.0; leftover $1278.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AVAH` | 93 | $13.62 | $2.27 | — | $1,725.39 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $1278.80 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 19 | $64.55 | $2.05 | — | $496.89 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+4.4; leftover $1278.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $496.89 | ▲ close $10,244.82 vs 09:30 $10,230.43 (session +39.41) | 16:00 close · cash $496.89 · equity $10,244.82 vs 09:30 $10,230.43 (+14.39; session marks +39.41) · 8 name(s) marked open→close (per-name table). OCUL×116 09:30 $10.98 → close $10.88 -11.60; RZLT×258 09:30 $4.94 → close $5.01 +18.06; HCA×2 09:30 $426.97 → close $428.76 +3.58; KURA×94 09:30 $13.59 → close $13.59 +0.00; LIFE×34 09:30 $36.96 → close $38.56 +54.40; AMTX×673 09:30 $1.90 → close $1.91 +6.73; AVAH×93 09:30 $13.62 → close $13.59 -3.26; ETON×19 09:30 $64.55 → close $63.05 -28.50 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $496.89 | ▼ 09:30 equity $10,240.77 vs yday $10,244.82 (-4.05) | 09:30 open · cash $496.89 (unchanged overnight, no fees) · equity $10,240.77 vs prior close $10,244.82 (-4.05) · 8 name(s) re-marked at the open (per-name table). OCUL×116 yday $10.88 → 09:30 $10.79 -10.44; RZLT×258 yday $5.01 → 09:30 $5.01 +0.00; HCA×2 yday $428.76 → 09:30 $427.50 -2.52; KURA×94 yday $13.59 → 09:30 $13.63 +3.76; LIFE×34 yday $38.56 → 09:30 $38.24 -10.88; AMTX×673 yday $1.91 → 09:30 $1.91 +0.00; AVAH×93 yday $13.59 → 09:30 $13.65 +5.58; ETON×19 yday $63.05 → 09:30 $63.60 +10.45 | — |
| 2026-08-26 09:30 ET | **SELL** | `OCUL` | 116 | $10.79 | $2.37 | $-26.75 | $1,746.17 | ▼ -26.75 after sell → book $10,238.41; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `HCA` | 2 | $427.50 | $2.02 | $-2.95 | $2,599.15 | ▼ -2.95 after sell → book $10,236.39; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 94 | $13.63 | $2.30 | $-0.81 | $3,878.07 | ▼ -0.81 after sell → book $10,234.09; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 34 | $38.24 | $2.11 | $+39.32 | $5,176.12 | ▲ +39.32 after sell → book $10,231.98; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AMTX` | 673 | $1.91 | $8.80 | $-10.76 | $6,452.75 | ▼ -10.76 after sell → book $10,223.18; vs 09:30 mark -8.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AVAH` | 93 | $13.65 | $2.29 | $-2.24 | $7,719.90 | ▼ -2.24 after sell → book $10,220.88; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 19 | $63.60 | $2.07 | $-22.16 | $8,926.23 | ▼ -22.16 after sell → book $10,218.81; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `INSP` | 21 | $60.07 | $2.05 | — | $7,662.71 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+6.4; leftover $1275.18 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `CRMD` | 148 | $8.60 | $2.43 | — | $6,387.48 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+4.8; leftover $1275.18 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SENS` | 134 | $9.48 | $2.39 | — | $5,114.76 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $1275.18 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 5 | $213.94 | $2.00 | — | $4,043.06 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1275.18 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ACRS` | 195 | $6.53 | $2.58 | — | $2,767.13 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.6; leftover $1275.18 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TMCI` | 266 | $4.78 | $3.43 | — | $1,492.22 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+8.1; leftover $1275.18 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CRDL` | 628 | $2.03 | $8.10 | — | $209.28 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+5.5; leftover $1275.18 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟡 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.28 | ▲ close $10,198.22 vs 09:30 $10,240.77 (session +2.40) | 16:00 close · cash $209.28 · equity $10,198.22 vs 09:30 $10,240.77 (-42.55; session marks +2.40) · 8 name(s) marked open→close (per-name table). RZLT×258 09:30 $5.01 → close $5.04 +7.74; INSP×21 09:30 $60.07 → close $61.80 +36.33; CRMD×148 09:30 $8.60 → close $8.39 -31.08; SENS×134 09:30 $9.48 → close $9.34 -18.76; BE×5 09:30 $213.94 → close $218.21 +21.35; ACRS×195 09:30 $6.53 → close $6.19 -66.30; TMCI×266 09:30 $4.78 → close $4.72 -15.96; CRDL×628 09:30 $2.03 → close $2.14 +69.08 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.28 | ▲ 09:30 equity $10,230.97 vs yday $10,198.22 (+32.75) | 09:30 open · cash $209.28 (unchanged overnight, no fees) · equity $10,230.97 vs prior close $10,198.22 (+32.75) · 8 name(s) re-marked at the open (per-name table). RZLT×258 yday $5.04 → 09:30 $5.07 +7.74; INSP×21 yday $61.80 → 09:30 $62.10 +6.30; CRMD×148 yday $8.39 → 09:30 $8.49 +14.80; SENS×134 yday $9.34 → 09:30 $9.33 -1.34; BE×5 yday $218.21 → 09:30 $227.10 +44.45; ACRS×195 yday $6.19 → 09:30 $6.15 -7.80; TMCI×266 yday $4.72 → 09:30 $4.72 +0.00; CRDL×628 yday $2.14 → 09:30 $2.09 -31.40 | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 258 | $5.07 | $3.38 | $+26.83 | $1,513.96 | ▲ +26.83 after sell → book $10,227.59; vs 09:30 mark -3.38 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 21 | $62.10 | $2.07 | $+38.50 | $2,815.99 | ▲ +38.50 after sell → book $10,225.52; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 148 | $8.49 | $2.47 | $-21.18 | $4,070.04 | ▼ -21.18 after sell → book $10,223.05; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SENS` | 134 | $9.33 | $2.42 | $-24.92 | $5,317.83 | ▼ -24.92 after sell → book $10,220.62; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BE` | 5 | $227.10 | $2.02 | $+61.77 | $6,451.31 | ▲ +61.77 after sell → book $10,218.60; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ACRS` | 195 | $6.15 | $2.62 | $-79.29 | $7,647.94 | ▼ -79.29 after sell → book $10,215.98; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TMCI` | 266 | $4.72 | $3.49 | $-22.88 | $8,899.98 | ▼ -22.88 after sell → book $10,212.50; vs 09:30 mark -3.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 628 | $2.09 | $8.22 | $+21.36 | $10,204.28 | ▲ +21.36 after sell → book $10,204.28; vs 09:30 mark -8.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 30 | $41.44 | $2.08 | — | $8,959.00 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+3.1; leftover $1275.54 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 88 | $14.42 | $2.25 | — | $7,687.79 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+7.1; leftover $1275.54 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 53 | $24.00 | $2.15 | — | $6,413.64 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+8.7; leftover $1275.54 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $5,176.23 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+8.5; leftover $1275.54 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 83 | $15.33 | $2.24 | — | $3,901.60 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+7.4; leftover $1275.54 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 42 | $29.83 | $2.12 | — | $2,646.62 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+7.6; leftover $1275.54 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $253.44 | $2.00 | — | $1,377.42 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+3.3; leftover $1275.54 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NUE` | 5 | $252.00 | $2.00 | — | $115.41 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+1.6; leftover $1275.54 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $115.41 | ▼ close $10,123.71 vs 09:30 $10,230.97 (session -63.71) | 16:00 close · cash $115.41 · equity $10,123.71 vs 09:30 $10,230.97 (-107.26; session marks -63.71) · 8 name(s) marked open→close (per-name table). RRC×30 09:30 $41.44 → close $41.64 +6.00; CRK×88 09:30 $14.42 → close $14.62 +17.60; MOS×53 09:30 $24.00 → close $23.76 -12.72; ANET×6 09:30 $205.90 → close $201.09 -28.86; DLO×83 09:30 $15.33 → close $15.14 -15.77; GEN×42 09:30 $29.83 → close $30.50 +28.14; MRVL×5 09:30 $253.44 → close $241.45 -59.95; NUE×5 09:30 $252.00 → close $252.37 +1.85 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $115.41 | ▼ 09:30 equity $10,056.27 vs yday $10,123.71 (-67.44) | 09:30 open · cash $115.41 (unchanged overnight, no fees) · equity $10,056.27 vs prior close $10,123.71 (-67.44) · 8 name(s) re-marked at the open (per-name table). RRC×30 yday $41.64 → 09:30 $41.74 +3.00; CRK×88 yday $14.62 → 09:30 $14.63 +0.88; MOS×53 yday $23.76 → 09:30 $23.95 +10.07; ANET×6 yday $201.09 → 09:30 $200.00 -6.54; DLO×83 yday $15.14 → 09:30 $15.19 +4.15; GEN×42 yday $30.50 → 09:30 $30.50 +0.00; MRVL×5 yday $241.45 → 09:30 $225.26 -80.95; NUE×5 yday $252.37 → 09:30 $252.76 +1.95 | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 30 | $41.74 | $2.10 | $+4.82 | $1,365.51 | ▲ +4.82 after sell → book $10,054.17; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 53 | $23.95 | $2.17 | $-6.97 | $2,632.70 | ▼ -6.97 after sell → book $10,052.01; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $200.00 | $2.03 | $-39.44 | $3,830.67 | ▼ -39.44 after sell → book $10,049.98; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 83 | $15.19 | $2.26 | $-16.12 | $5,089.17 | ▼ -16.12 after sell → book $10,047.71; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 42 | $30.50 | $2.14 | $+23.89 | $6,368.04 | ▲ +23.89 after sell → book $10,045.58; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $225.26 | $2.02 | $-144.93 | $7,492.31 | ▼ -144.93 after sell → book $10,043.55; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `NUE` | 5 | $252.76 | $2.03 | $-0.23 | $8,754.09 | ▼ -0.23 after sell → book $10,041.53; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 79 | $15.66 | $2.23 | — | $7,514.72 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1250.58 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $122.81 | $2.02 | — | $6,284.60 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1250.58 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `EQ` | 508 | $2.46 | $6.55 | — | $5,028.37 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+7.9; leftover $1250.58 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BTSG` | 20 | $60.54 | $2.05 | — | $3,815.52 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+2.3; leftover $1250.58 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CRDL` | 607 | $2.06 | $7.83 | — | $2,557.27 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+9.3; leftover $1250.58 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 43 | $28.91 | $2.12 | — | $1,312.02 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+9.2; leftover $1250.58 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADBT` | 250 | $4.99 | $3.23 | — | $61.29 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+0.0; leftover $1250.58 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.29 | ▼ close $9,681.11 vs 09:30 $10,056.27 (session -334.39) | 16:00 close · cash $61.29 · equity $9,681.11 vs 09:30 $10,056.27 (-375.16; session marks -334.39) · 8 name(s) marked open→close (per-name table). CRK×88 09:30 $14.63 → close $14.29 -29.92; GRRR×79 09:30 $15.66 → close $14.41 -98.75; TTMI×10 09:30 $122.81 → close $118.65 -41.60; EQ×508 09:30 $2.46 → close $2.39 -35.56; BTSG×20 09:30 $60.54 → close $59.13 -28.20; CRDL×607 09:30 $2.06 → close $1.94 -72.84; ZYME×43 09:30 $28.91 → close $28.27 -27.52; ADBT×250 09:30 $4.99 → close $4.99 +0.00 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.29 | ▼ 09:30 equity $9,666.21 vs yday $9,681.11 (-14.90) | 09:30 open · cash $61.29 (unchanged overnight, no fees) · equity $9,666.21 vs prior close $9,681.11 (-14.90) · 8 name(s) re-marked at the open (per-name table). CRK×88 yday $14.29 → 09:30 $14.54 +22.00; GRRR×79 yday $14.41 → 09:30 $14.44 +2.37; TTMI×10 yday $118.65 → 09:30 $118.83 +1.80; EQ×508 yday $2.39 → 09:30 $2.39 +0.00; BTSG×20 yday $59.13 → 09:30 $58.76 -7.40; CRDL×607 yday $1.94 → 09:30 $1.92 -12.14; ZYME×43 yday $28.27 → 09:30 $28.06 -9.03; ADBT×250 yday $4.99 → 09:30 $4.94 -12.50 | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 88 | $14.54 | $2.28 | $+6.03 | $1,338.53 | ▲ +6.03 after sell → book $9,663.93; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 79 | $14.44 | $2.25 | $-100.86 | $2,477.04 | ▼ -100.86 after sell → book $9,661.68; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 10 | $118.83 | $2.04 | $-43.86 | $3,663.30 | ▼ -43.86 after sell → book $9,659.64; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `EQ` | 508 | $2.39 | $6.65 | $-48.76 | $4,870.78 | ▼ -48.76 after sell → book $9,653.00; vs 09:30 mark -6.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BTSG` | 20 | $58.76 | $2.07 | $-39.72 | $6,043.91 | ▼ -39.72 after sell → book $9,650.93; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRDL` | 607 | $1.92 | $7.94 | $-100.75 | $7,201.41 | ▼ -100.75 after sell → book $9,642.99; vs 09:30 mark -7.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 43 | $28.06 | $2.14 | $-40.81 | $8,405.85 | ▼ -40.81 after sell → book $9,640.85; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADBT` | 250 | $4.94 | $3.28 | $-19.00 | $9,637.57 | ▼ -19.00 after sell → book $9,637.57; vs 09:30 mark -3.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,637.57 | ▲ close $9,637.57 vs 09:30 $9,666.21 (session +0.00) | 16:00 close · cash $9,637.57 · no lots left · equity $9,637.57. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,637.57 | ▲ 09:30 equity $9,637.57 vs yday $9,637.57 (+0.00) | 09:30 open · cash $9,637.57 · no holdings · equity $9,637.57 vs prior close $9,637.57 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,637.57 | ▲ close $9,637.57 vs 09:30 $9,637.57 (session +0.00) | 16:00 close · cash $9,637.57 · no lots left · equity $9,637.57. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,637.57 | ▲ 09:30 equity $9,637.57 vs yday $9,637.57 (+0.00) | 09:30 open · cash $9,637.57 · no holdings · equity $9,637.57 vs prior close $9,637.57 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,637.57 | ▲ close $9,637.57 vs 09:30 $9,637.57 (session +0.00) | 16:00 close · cash $9,637.57 · no lots left · equity $9,637.57. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,637.57 | ▲ 09:30 equity $9,637.57 vs yday $9,637.57 (+0.00) | 09:30 open · cash $9,637.57 · no holdings · equity $9,637.57 vs prior close $9,637.57 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 22 | $52.88 | $2.06 | — | $8,472.15 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1204.70 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 28 | $42.93 | $2.07 | — | $7,268.04 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1204.70 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 331 | $3.63 | $4.27 | — | $6,062.24 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1204.70 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $4,868.17 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1204.70 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 77 | $15.45 | $2.22 | — | $3,676.30 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1204.70 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 71 | $16.77 | $2.20 | — | $2,483.43 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1204.70 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 552 | $2.18 | $7.12 | — | $1,272.95 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1204.70 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SDGR` | 57 | $21.03 | $2.16 | — | $72.08 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $1204.70 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.08 | ▼ close $9,354.53 vs 09:30 $9,637.57 (session -258.92) | 16:00 close · cash $72.08 · equity $9,354.53 vs 09:30 $9,637.57 (-283.04; session marks -258.92) · 8 name(s) marked open→close (per-name table). ATRC×22 09:30 $52.88 → close $52.46 -9.24; HRMY×28 09:30 $42.93 → close $41.86 -29.96; CABA×331 09:30 $3.63 → close $3.48 -49.65; RVTY×9 09:30 $132.45 → close $130.63 -16.38; CRK×77 09:30 $15.45 → close $14.95 -38.50; ARCT×71 09:30 $16.77 → close $15.56 -85.91; CRDL×552 09:30 $2.18 → close $2.16 -11.04; SDGR×57 09:30 $21.03 → close $20.71 -18.24 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.08 | ▼ 09:30 equity $9,322.96 vs yday $9,354.53 (-31.57) | 09:30 open · cash $72.08 (unchanged overnight, no fees) · equity $9,322.96 vs prior close $9,354.53 (-31.57) · 8 name(s) re-marked at the open (per-name table). ATRC×22 yday $52.46 → 09:30 $52.03 -9.46; HRMY×28 yday $41.86 → 09:30 $41.50 -10.08; CABA×331 yday $3.48 → 09:30 $3.46 -6.62; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; CRK×77 yday $14.95 → 09:30 $15.00 +3.85; ARCT×71 yday $15.56 → 09:30 $15.61 +3.55; CRDL×552 yday $2.16 → 09:30 $2.16 +0.00; SDGR×57 yday $20.71 → 09:30 $20.58 -7.41 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $1,240.31 | ▼ -25.83 after sell → book $9,320.92; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 77 | $15.00 | $2.24 | $-39.11 | $2,393.07 | ▼ -39.11 after sell → book $9,318.68; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 71 | $15.61 | $2.22 | $-86.79 | $3,499.15 | ▼ -86.79 after sell → book $9,316.45; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 552 | $2.16 | $7.22 | $-25.38 | $4,684.25 | ▼ -25.38 after sell → book $9,309.23; vs 09:30 mark -7.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SDGR` | 57 | $20.58 | $2.18 | $-29.99 | $5,855.13 | ▼ -29.99 after sell → book $9,307.05; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 464 | $2.52 | $5.99 | — | $4,679.86 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1171.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 174 | $6.71 | $2.51 | — | $3,509.81 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1171.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 244 | $4.78 | $3.15 | — | $2,340.34 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1171.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 103 | $11.31 | $2.30 | — | $1,173.11 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1171.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $117.67 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1171.03 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $117.67 | ▼ close $9,131.66 vs 09:30 $9,322.96 (session -159.45) | 16:00 close · cash $117.67 · equity $9,131.66 vs 09:30 $9,322.96 (-191.30; session marks -159.45) · 8 name(s) marked open→close (per-name table). ATRC×22 09:30 $52.03 → close $51.52 -11.22; HRMY×28 09:30 $41.50 → close $42.25 +21.00; CABA×331 09:30 $3.46 → close $3.47 +3.31; ALEC×464 09:30 $2.52 → close $2.46 -27.84; BHC×174 09:30 $6.71 → close $6.56 -26.10; OABI×244 09:30 $4.78 → close $4.33 -109.80; VIR×103 09:30 $11.31 → close $11.38 +7.72; CRM×4 09:30 $263.36 → close $259.23 -16.52 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $117.67 | ▼ 09:30 equity $9,096.66 vs yday $9,131.66 (-35.00) | 09:30 open · cash $117.67 (unchanged overnight, no fees) · equity $9,096.66 vs prior close $9,131.66 (-35.00) · 8 name(s) re-marked at the open (per-name table). ATRC×22 yday $51.52 → 09:30 $54.31 +61.38; HRMY×28 yday $42.25 → 09:30 $42.20 -1.40; CABA×331 yday $3.47 → 09:30 $3.43 -13.24; ALEC×464 yday $2.46 → 09:30 $2.38 -37.12; BHC×174 yday $6.56 → 09:30 $6.57 +1.74; OABI×244 yday $4.33 → 09:30 $4.30 -7.32; VIR×103 yday $11.38 → 09:30 $11.22 -16.99; CRM×4 yday $259.23 → 09:30 $253.72 -22.04 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 22 | $54.31 | $2.08 | $+27.33 | $1,310.42 | ▲ +27.33 after sell → book $9,094.59; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 28 | $42.20 | $2.09 | $-24.61 | $2,489.92 | ▼ -24.61 after sell → book $9,092.49; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 331 | $3.43 | $4.33 | $-74.80 | $3,620.92 | ▼ -74.80 after sell → book $9,088.16; vs 09:30 mark -4.33 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 464 | $2.38 | $6.07 | $-77.02 | $4,719.17 | ▼ -77.02 after sell → book $9,082.09; vs 09:30 mark -6.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 174 | $6.57 | $2.55 | $-29.42 | $5,859.79 | ▼ -29.42 after sell → book $9,079.53; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 244 | $4.30 | $3.20 | $-123.47 | $6,905.80 | ▼ -123.47 after sell → book $9,076.34; vs 09:30 mark -3.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 103 | $11.22 | $2.33 | $-13.90 | $8,059.13 | ▼ -13.90 after sell → book $9,074.01; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $9,071.99 | ▼ -42.58 after sell → book $9,071.99; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,071.99 | ▲ close $9,071.99 vs 09:30 $9,096.66 (session +0.00) | 16:00 close · cash $9,071.99 · no lots left · equity $9,071.99. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,071.99 | ▲ 09:30 equity $9,071.99 vs yday $9,071.99 (-0.00) | 09:30 open · cash $9,071.99 · no holdings · equity $9,071.99 vs prior close $9,071.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,071.99 | ▲ close $9,071.99 vs 09:30 $9,071.99 (session +0.00) | 16:00 close · cash $9,071.99 · no lots left · equity $9,071.99. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,071.99 | ▲ 09:30 equity $9,071.99 vs yday $9,071.99 (-0.00) | 09:30 open · cash $9,071.99 · no holdings · equity $9,071.99 vs prior close $9,071.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,071.99 | ▲ close $9,071.99 vs 09:30 $9,071.99 (session +0.00) | 16:00 close · cash $9,071.99 · no lots left · equity $9,071.99. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,071.99 | ▲ 09:30 equity $9,071.99 vs yday $9,071.99 (-0.00) | 09:30 open · cash $9,071.99 · no holdings · equity $9,071.99 vs prior close $9,071.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,071.99 | ▲ close $9,071.99 vs 09:30 $9,071.99 (session +0.00) | 16:00 close · cash $9,071.99 · no lots left · equity $9,071.99. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MXL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABAT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `PURR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `KEP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OBE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HELP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `OVID` | no_price | no 09:30 open |
| 2026-09-11 | `SANM` | no_price | no 09:30 open |
| 2026-09-11 | `ORCL` | no_price | no 09:30 open |
| 2026-09-11 | `NVT` | no_price | no 09:30 open |
| 2026-09-11 | `CLOV` | no_price | no 09:30 open |
| 2026-09-11 | `TYRA` | no_price | no 09:30 open |
| 2026-09-11 | `QRVO` | no_price | no 09:30 open |
| 2026-09-11 | `APPS` | no_price | no 09:30 open |
