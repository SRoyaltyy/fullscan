# Factor mine action — `union_macd_up_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ macd_up, no 🚨

Cash book **-2.04%** ($9,796) · signal-only (no cash/fees) was +9.81%. Starts YES **3/26**. Fills 162 · skips 87 · realized $-203.86.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior MACD histogram is above zero (momentum still up).
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
- **Gate** `macd_up=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,796.16.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `IREN` | 43 | — | $45.98 | +0.00 | $44.76 | -52.46 | -52.46 | +0.00 | -52.46 |
| 2026-08-13 | `TPG` | 39 | — | $50.62 | +0.00 | $54.62 | +155.88 | +155.88 | +0.00 | +155.88 |
| 2026-08-13 | `HIMS` | 67 | — | $29.74 | +0.00 | $28.77 | -64.99 | -64.99 | +0.00 | -64.99 |
| 2026-08-13 | `TNDM` | 85 | — | $23.33 | +0.00 | $23.13 | -17.00 | -17.00 | +0.00 | -17.00 |
| 2026-08-13 | `VOR` | 90 | — | $22.01 | +0.00 | $23.29 | +115.20 | +115.20 | +0.00 | +115.20 |
| 2026-08-14 | `IREN` | 43 | $44.76 | $44.09 | -28.81 | — | +0.00 | -28.81 | -81.27 | — |
| 2026-08-14 | `TPG` | 39 | $54.62 | $55.29 | +26.13 | — | +0.00 | +26.13 | +182.01 | — |
| 2026-08-14 | `HIMS` | 67 | $28.77 | $29.15 | +25.46 | — | +0.00 | +25.46 | -39.53 | — |
| 2026-08-14 | `TNDM` | 85 | $23.13 | $22.92 | -17.85 | — | +0.00 | -17.85 | -34.85 | — |
| 2026-08-14 | `VOR` | 90 | $23.29 | $23.33 | +3.60 | — | +0.00 | +3.60 | +118.80 | — |
| 2026-08-14 | `TLN` | 3 | — | $359.83 | +0.00 | $362.74 | +8.73 | +8.73 | +0.00 | +8.73 |
| 2026-08-14 | `SLG` | 21 | — | $57.61 | +0.00 | $56.09 | -31.92 | -31.92 | +0.00 | -31.92 |
| 2026-08-14 | `BTBT` | 843 | — | $1.50 | +0.00 | $1.57 | +59.01 | +59.01 | +0.00 | +59.01 |
| 2026-08-14 | `HYLN` | 302 | — | $4.18 | +0.00 | $4.06 | -36.24 | -36.24 | +0.00 | -36.24 |
| 2026-08-14 | `ADUR` | 76 | — | $16.50 | +0.00 | $16.17 | -25.08 | -25.08 | +0.00 | -25.08 |
| 2026-08-14 | `ALGM` | 28 | — | $44.06 | +0.00 | $44.39 | +9.24 | +9.24 | +0.00 | +9.24 |
| 2026-08-14 | `ARX` | 64 | — | $19.57 | +0.00 | $19.58 | +0.64 | +0.64 | +0.00 | +0.64 |
| 2026-08-14 | `AIRO` | 113 | — | $11.12 | +0.00 | $9.57 | -175.15 | -175.15 | +0.00 | -175.15 |
| 2026-08-17 | `TLN` | 3 | $362.74 | $367.88 | +15.42 | — | +0.00 | +15.42 | +24.15 | — |
| 2026-08-17 | `SLG` | 21 | $56.09 | $55.37 | -15.12 | — | +0.00 | -15.12 | -47.04 | — |
| 2026-08-17 | `BTBT` | 843 | $1.57 | $1.52 | -42.15 | — | +0.00 | -42.15 | +16.86 | — |
| 2026-08-17 | `HYLN` | 302 | $4.06 | $4.10 | +12.08 | — | +0.00 | +12.08 | -24.16 | — |
| 2026-08-17 | `ADUR` | 76 | $16.17 | $15.73 | -33.44 | — | +0.00 | -33.44 | -58.52 | — |
| 2026-08-17 | `ALGM` | 28 | $44.39 | $45.32 | +26.04 | — | +0.00 | +26.04 | +35.28 | — |
| 2026-08-17 | `ARX` | 64 | $19.58 | $19.57 | -0.64 | — | +0.00 | -0.64 | +0.00 | — |
| 2026-08-17 | `AIRO` | 113 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -175.15 | — |
| 2026-08-17 | `DVN` | 26 | — | $46.18 | +0.00 | $47.57 | +36.14 | +36.14 | +0.00 | +36.14 |
| 2026-08-17 | `FANG` | 6 | — | $202.70 | +0.00 | $206.29 | +21.54 | +21.54 | +0.00 | +21.54 |
| 2026-08-17 | `TMC` | 303 | — | $4.05 | +0.00 | $3.77 | -84.84 | -84.84 | +0.00 | -84.84 |
| 2026-08-17 | `TGB` | 145 | — | $8.46 | +0.00 | $8.77 | +44.95 | +44.95 | +0.00 | +44.95 |
| 2026-08-17 | `ELF` | 13 | — | $90.54 | +0.00 | $93.66 | +40.56 | +40.56 | +0.00 | +40.56 |
| 2026-08-17 | `DNN` | 379 | — | $3.24 | +0.00 | $3.19 | -18.95 | -18.95 | +0.00 | -18.95 |
| 2026-08-17 | `NB` | 242 | — | $5.07 | +0.00 | $4.81 | -62.92 | -62.92 | +0.00 | -62.92 |
| 2026-08-17 | `CELC` | 13 | — | $92.99 | +0.00 | $92.44 | -7.15 | -7.15 | +0.00 | -7.15 |
| 2026-08-18 | `DVN` | 26 | $47.57 | $48.00 | +11.18 | — | +0.00 | +11.18 | +47.32 | — |
| 2026-08-18 | `FANG` | 6 | $206.29 | $208.93 | +15.84 | — | +0.00 | +15.84 | +37.38 | — |
| 2026-08-18 | `TMC` | 303 | $3.77 | $3.72 | -15.15 | — | +0.00 | -15.15 | -99.99 | — |
| 2026-08-18 | `TGB` | 145 | $8.77 | $8.55 | -31.90 | — | +0.00 | -31.90 | +13.05 | — |
| 2026-08-18 | `ELF` | 13 | $93.66 | $93.44 | -2.86 | — | +0.00 | -2.86 | +37.70 | — |
| 2026-08-18 | `DNN` | 379 | $3.19 | $3.11 | -30.32 | — | +0.00 | -30.32 | -49.27 | — |
| 2026-08-18 | `NB` | 242 | $4.81 | $4.66 | -36.30 | — | +0.00 | -36.30 | -99.22 | — |
| 2026-08-18 | `CELC` | 13 | $92.44 | $92.38 | -0.78 | — | +0.00 | -0.78 | -7.93 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 58 | — | $20.55 | +0.00 | $21.19 | +37.12 | +37.12 | +0.00 | +37.12 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 58 | — | $20.65 | +0.00 | $21.11 | +26.68 | +26.68 | +0.00 | +26.68 |
| 2026-08-20 | `IAG` | 61 | — | $19.63 | +0.00 | $20.50 | +53.07 | +53.07 | +0.00 | +53.07 |
| 2026-08-20 | `KGC` | 40 | — | $29.63 | +0.00 | $31.43 | +72.00 | +72.00 | +0.00 | +72.00 |
| 2026-08-20 | `NFGC` | 690 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-20 | `ABUS` | 245 | — | $4.92 | +0.00 | $4.77 | -36.75 | -36.75 | +0.00 | -36.75 |
| 2026-08-21 | `AG` | 58 | $21.19 | $21.90 | +41.18 | — | +0.00 | +41.18 | +78.30 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `CDE` | 58 | $21.11 | $21.75 | +37.12 | — | +0.00 | +37.12 | +63.80 | — |
| 2026-08-21 | `IAG` | 61 | $20.50 | $21.17 | +40.87 | — | +0.00 | +40.87 | +93.94 | — |
| 2026-08-21 | `KGC` | 40 | $31.43 | $32.17 | +29.60 | — | +0.00 | +29.60 | +101.60 | — |
| 2026-08-21 | `NFGC` | 690 | $1.75 | $1.79 | +27.60 | — | +0.00 | +27.60 | +27.60 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `ABUS` | 245 | $4.77 | $5.20 | +105.35 | — | +0.00 | +105.35 | +68.60 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 74 | — | $17.20 | +0.00 | $16.65 | -40.70 | -40.70 | +0.00 | -40.70 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 114 | — | $11.13 | +0.00 | $13.45 | +264.48 | +264.48 | +0.00 | +264.48 |
| 2026-08-21 | `AUTL` | 516 | — | $2.47 | +0.00 | $2.41 | -30.96 | -30.96 | +0.00 | -30.96 |
| 2026-08-21 | `CRDL` | 660 | — | $1.93 | +0.00 | $1.86 | -46.20 | -46.20 | +0.00 | -46.20 |
| 2026-08-21 | `CRSP` | 21 | — | $59.72 | +0.00 | $59.50 | -4.62 | -4.62 | +0.00 | -4.62 |
| 2026-08-21 | `CYPH` | 965 | — | $1.32 | +0.00 | $1.42 | +96.50 | +96.50 | +0.00 | +96.50 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 74 | $16.65 | $16.57 | -5.92 | — | +0.00 | -5.92 | -46.62 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 114 | $13.45 | $13.33 | -13.68 | — | +0.00 | -13.68 | +250.80 | — |
| 2026-08-24 | `AUTL` | 516 | $2.41 | $2.40 | -5.16 | — | +0.00 | -5.16 | -36.12 | — |
| 2026-08-24 | `CRDL` | 660 | $1.86 | $1.88 | +13.20 | — | +0.00 | +13.20 | -33.00 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | — | +0.00 | -15.75 | -20.37 | — |
| 2026-08-24 | `CYPH` | 965 | $1.42 | $1.83 | +395.65 | — | +0.00 | +395.65 | +492.15 | — |
| 2026-08-25 | `MOS` | 56 | — | $23.77 | +0.00 | $24.27 | +28.00 | +28.00 | +0.00 | +28.00 |
| 2026-08-25 | `OCUL` | 122 | — | $10.98 | +0.00 | $10.88 | -12.20 | -12.20 | +0.00 | -12.20 |
| 2026-08-25 | `INSP` | 21 | — | $61.19 | +0.00 | $61.07 | -2.52 | -2.52 | +0.00 | -2.52 |
| 2026-08-25 | `CRMD` | 160 | — | $8.35 | +0.00 | $8.56 | +33.60 | +33.60 | +0.00 | +33.60 |
| 2026-08-25 | `RZLT` | 271 | — | $4.94 | +0.00 | $5.01 | +18.97 | +18.97 | +0.00 | +18.97 |
| 2026-08-25 | `HCA` | 3 | — | $426.97 | +0.00 | $428.76 | +5.37 | +5.37 | +0.00 | +5.37 |
| 2026-08-25 | `CAPR` | 185 | — | $7.25 | +0.00 | $8.29 | +192.40 | +192.40 | +0.00 | +192.40 |
| 2026-08-25 | `KURA` | 98 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `MOS` | 56 | $24.27 | $24.84 | +31.92 | $24.16 | -38.08 | -6.16 | +59.92 | +21.84 |
| 2026-08-26 | `OCUL` | 122 | $10.88 | $10.79 | -10.98 | $10.77 | -2.44 | -13.42 | -23.18 | -25.62 |
| 2026-08-26 | `INSP` | 21 | $61.07 | $60.07 | -21.00 | — | +0.00 | -21.00 | -23.52 | — |
| 2026-08-26 | `CRMD` | 160 | $8.56 | $8.60 | +6.40 | $8.39 | -33.60 | -27.20 | +40.00 | +6.40 |
| 2026-08-26 | `RZLT` | 271 | $5.01 | $5.01 | +0.00 | $5.04 | +8.13 | +8.13 | +18.97 | +27.10 |
| 2026-08-26 | `HCA` | 3 | $428.76 | $427.50 | -3.78 | $427.16 | -1.02 | -4.80 | +1.59 | +0.57 |
| 2026-08-26 | `CAPR` | 185 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | +192.40 | — |
| 2026-08-26 | `KURA` | 98 | $13.59 | $13.63 | +3.92 | — | +0.00 | +3.92 | +3.92 | — |
| 2026-08-26 | `AVBP` | 45 | — | $31.21 | +0.00 | $31.14 | -3.15 | -3.15 | +0.00 | -3.15 |
| 2026-08-26 | `FLNC` | 127 | — | $11.12 | +0.00 | $11.08 | -5.08 | -5.08 | +0.00 | -5.08 |
| 2026-08-26 | `ABX` | 144 | — | $9.83 | +0.00 | $9.78 | -7.20 | -7.20 | +0.00 | -7.20 |
| 2026-08-27 | `MOS` | 56 | $24.16 | $24.00 | -8.96 | $23.76 | -13.44 | -22.40 | +12.88 | -0.56 |
| 2026-08-27 | `OCUL` | 122 | $10.77 | $10.63 | -17.08 | — | +0.00 | -17.08 | -42.70 | — |
| 2026-08-27 | `CRMD` | 160 | $8.39 | $8.49 | +16.00 | — | +0.00 | +16.00 | +22.40 | — |
| 2026-08-27 | `RZLT` | 271 | $5.04 | $5.07 | +8.13 | — | +0.00 | +8.13 | +35.23 | — |
| 2026-08-27 | `HCA` | 3 | $427.16 | $424.61 | -7.65 | — | +0.00 | -7.65 | -7.08 | — |
| 2026-08-27 | `AVBP` | 45 | $31.14 | $30.79 | -15.75 | — | +0.00 | -15.75 | -18.90 | — |
| 2026-08-27 | `FLNC` | 127 | $11.08 | $11.52 | +55.88 | — | +0.00 | +55.88 | +50.80 | — |
| 2026-08-27 | `ABX` | 144 | $9.78 | $9.68 | -14.40 | — | +0.00 | -14.40 | -21.60 | — |
| 2026-08-27 | `RRC` | 32 | — | $41.44 | +0.00 | $41.64 | +6.40 | +6.40 | +0.00 | +6.40 |
| 2026-08-27 | `CRK` | 94 | — | $14.42 | +0.00 | $14.62 | +18.80 | +18.80 | +0.00 | +18.80 |
| 2026-08-27 | `SLI` | 524 | — | $2.60 | +0.00 | $2.64 | +20.96 | +20.96 | +0.00 | +20.96 |
| 2026-08-27 | `ACMR` | 16 | — | $81.65 | +0.00 | $80.49 | -18.56 | -18.56 | +0.00 | -18.56 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `DLO` | 89 | — | $15.33 | +0.00 | $15.14 | -16.91 | -16.91 | +0.00 | -16.91 |
| 2026-08-27 | `GEN` | 45 | — | $29.83 | +0.00 | $30.50 | +30.15 | +30.15 | +0.00 | +30.15 |
| 2026-08-28 | `MOS` | 56 | $23.76 | $23.95 | +10.64 | $23.60 | -19.60 | -8.96 | +10.08 | -9.52 |
| 2026-08-28 | `RRC` | 32 | $41.64 | $41.74 | +3.20 | $41.46 | -8.96 | -5.76 | +9.60 | +0.64 |
| 2026-08-28 | `CRK` | 94 | $14.62 | $14.63 | +0.94 | $14.29 | -31.96 | -31.02 | +19.74 | -12.22 |
| 2026-08-28 | `SLI` | 524 | $2.64 | $2.68 | +20.96 | $2.55 | -68.12 | -47.16 | +41.92 | -26.20 |
| 2026-08-28 | `ACMR` | 16 | $80.49 | $79.27 | -19.52 | — | +0.00 | -19.52 | -38.08 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `DLO` | 89 | $15.14 | $15.19 | +4.45 | — | +0.00 | +4.45 | -12.46 | — |
| 2026-08-28 | `GEN` | 45 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +30.15 | — |
| 2026-08-28 | `SEDG` | 41 | — | $32.90 | +0.00 | $31.41 | -61.09 | -61.09 | +0.00 | -61.09 |
| 2026-08-28 | `GRRR` | 86 | — | $15.66 | +0.00 | $14.41 | -107.50 | -107.50 | +0.00 | -107.50 |
| 2026-08-28 | `URBN` | 17 | — | $79.42 | +0.00 | $81.09 | +28.39 | +28.39 | +0.00 | +28.39 |
| 2026-08-28 | `SIMO` | 5 | — | $252.24 | +0.00 | $245.81 | -32.15 | -32.15 | +0.00 | -32.15 |
| 2026-08-31 | `MOS` | 56 | $23.60 | $23.68 | +4.48 | — | +0.00 | +4.48 | -5.04 | — |
| 2026-08-31 | `RRC` | 32 | $41.46 | $42.00 | +17.28 | — | +0.00 | +17.28 | +17.92 | — |
| 2026-08-31 | `CRK` | 94 | $14.29 | $14.54 | +23.50 | — | +0.00 | +23.50 | +11.28 | — |
| 2026-08-31 | `SLI` | 524 | $2.55 | $2.58 | +15.72 | — | +0.00 | +15.72 | -10.48 | — |
| 2026-08-31 | `SEDG` | 41 | $31.41 | $31.15 | -10.66 | — | +0.00 | -10.66 | -71.75 | — |
| 2026-08-31 | `GRRR` | 86 | $14.41 | $14.44 | +2.58 | — | +0.00 | +2.58 | -104.92 | — |
| 2026-08-31 | `URBN` | 17 | $81.09 | $80.44 | -11.05 | — | +0.00 | -11.05 | +17.34 | — |
| 2026-08-31 | `SIMO` | 5 | $245.81 | $247.05 | +6.20 | — | +0.00 | +6.20 | -25.95 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 25 | — | $52.88 | +0.00 | $52.46 | -10.50 | -10.50 | +0.00 | -10.50 |
| 2026-09-03 | `HRMY` | 30 | — | $42.93 | +0.00 | $41.86 | -32.10 | -32.10 | +0.00 | -32.10 |
| 2026-09-03 | `CABA` | 364 | — | $3.63 | +0.00 | $3.48 | -54.60 | -54.60 | +0.00 | -54.60 |
| 2026-09-03 | `VSTM` | 164 | — | $8.03 | +0.00 | $7.98 | -8.20 | -8.20 | +0.00 | -8.20 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `CRK` | 85 | — | $15.45 | +0.00 | $14.95 | -42.50 | -42.50 | +0.00 | -42.50 |
| 2026-09-03 | `MRNA` | 9 | — | $145.94 | +0.00 | $148.87 | +26.33 | +26.33 | +0.00 | +26.33 |
| 2026-09-03 | `ARCT` | 78 | — | $16.77 | +0.00 | $15.56 | -94.38 | -94.38 | +0.00 | -94.38 |
| 2026-09-04 | `ATRC` | 25 | $52.46 | $52.03 | -10.75 | $51.52 | -12.75 | -23.50 | -21.25 | -34.00 |
| 2026-09-04 | `HRMY` | 30 | $41.86 | $41.50 | -10.80 | — | +0.00 | -10.80 | -42.90 | — |
| 2026-09-04 | `CABA` | 364 | $3.48 | $3.46 | -7.28 | $3.47 | +3.64 | -3.64 | -61.88 | -58.24 |
| 2026-09-04 | `VSTM` | 164 | $7.98 | $7.91 | -11.48 | — | +0.00 | -11.48 | -19.68 | — |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `CRK` | 85 | $14.95 | $15.00 | +4.25 | — | +0.00 | +4.25 | -38.25 | — |
| 2026-09-04 | `MRNA` | 9 | $148.87 | $153.62 | +42.75 | — | +0.00 | +42.75 | +69.08 | — |
| 2026-09-04 | `ARCT` | 78 | $15.56 | $15.61 | +3.90 | — | +0.00 | +3.90 | -90.48 | — |
| 2026-09-04 | `ALEC` | 513 | — | $2.52 | +0.00 | $2.46 | -30.78 | -30.78 | +0.00 | -30.78 |
| 2026-09-04 | `BMEA` | 681 | — | $1.90 | +0.00 | $2.03 | +88.53 | +88.53 | +0.00 | +88.53 |
| 2026-09-04 | `OABI` | 270 | — | $4.78 | +0.00 | $4.33 | -121.50 | -121.50 | +0.00 | -121.50 |
| 2026-09-04 | `OPK` | 813 | — | $1.59 | +0.00 | $1.64 | +40.65 | +40.65 | +0.00 | +40.65 |
| 2026-09-04 | `VIR` | 114 | — | $11.31 | +0.00 | $11.38 | +8.55 | +8.55 | +0.00 | +8.55 |
| 2026-09-04 | `CRM` | 4 | — | $263.36 | +0.00 | $259.23 | -16.52 | -16.52 | +0.00 | -16.52 |
| 2026-09-08 | `ATRC` | 25 | $51.52 | $54.31 | +69.75 | — | +0.00 | +69.75 | +35.75 | — |
| 2026-09-08 | `CABA` | 364 | $3.47 | $3.43 | -14.56 | — | +0.00 | -14.56 | -72.80 | — |
| 2026-09-08 | `ALEC` | 513 | $2.46 | $2.38 | -41.04 | — | +0.00 | -41.04 | -71.82 | — |
| 2026-09-08 | `BMEA` | 681 | $2.03 | $2.00 | -20.43 | — | +0.00 | -20.43 | +68.10 | — |
| 2026-09-08 | `OABI` | 270 | $4.33 | $4.30 | -8.10 | — | +0.00 | -8.10 | -129.60 | — |
| 2026-09-08 | `OPK` | 813 | $1.64 | $1.63 | -8.13 | — | +0.00 | -8.13 | +32.52 | — |
| 2026-09-08 | `VIR` | 114 | $11.38 | $11.22 | -18.81 | — | +0.00 | -18.81 | -10.26 | — |
| 2026-09-08 | `CRM` | 4 | $259.23 | $253.72 | -22.04 | — | +0.00 | -22.04 | -38.56 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `SANM` | 6 | — | $206.84 | +0.00 | $216.00 | +54.96 | +54.96 | +0.00 | +54.96 |
| 2026-09-11 | `ORCL` | 7 | — | $164.43 | +0.00 | $150.28 | -99.05 | -99.05 | +0.00 | -99.05 |
| 2026-09-11 | `NVT` | 8 | — | $157.78 | +0.00 | $162.38 | +36.80 | +36.80 | +0.00 | +36.80 |
| 2026-09-11 | `COHU` | 22 | — | $56.09 | +0.00 | $57.08 | +21.78 | +21.78 | +0.00 | +21.78 |
| 2026-09-11 | `CLOV` | 267 | — | $4.75 | +0.00 | $4.82 | +18.69 | +18.69 | +0.00 | +18.69 |
| 2026-09-11 | `BAK` | 598 | — | $2.12 | +0.00 | $2.08 | -23.92 | -23.92 | +0.00 | -23.92 |
| 2026-09-11 | `FUBO` | 109 | — | $11.55 | +0.00 | $11.53 | -2.18 | -2.18 | +0.00 | -2.18 |
| 2026-09-11 | `RDDT` | 8 | — | $157.55 | +0.00 | $157.77 | +1.76 | +1.76 | +0.00 | +1.76 |
| 2026-09-14 | `SANM` | 6 | $216.00 | $206.50 | -57.00 | — | +0.00 | -57.00 | -2.04 | — |
| 2026-09-14 | `ORCL` | 7 | $150.28 | $141.42 | -62.02 | — | +0.00 | -62.02 | -161.07 | — |
| 2026-09-14 | `NVT` | 8 | $162.38 | $150.00 | -99.04 | $146.64 | -26.88 | -125.92 | -62.24 | -89.12 |
| 2026-09-14 | `COHU` | 22 | $57.08 | $52.23 | -106.70 | — | +0.00 | -106.70 | -84.92 | — |
| 2026-09-14 | `CLOV` | 267 | $4.82 | $4.82 | +0.00 | — | +0.00 | +0.00 | +18.69 | — |
| 2026-09-14 | `BAK` | 598 | $2.08 | $2.05 | -17.94 | — | +0.00 | -17.94 | -41.86 | — |
| 2026-09-14 | `FUBO` | 109 | $11.53 | $11.56 | +3.27 | — | +0.00 | +3.27 | +1.09 | — |
| 2026-09-14 | `RDDT` | 8 | $157.77 | $160.00 | +17.84 | — | +0.00 | +17.84 | +19.60 | — |
| 2026-09-15 | `NVT` | 8 | $146.64 | $151.12 | +35.84 | — | +0.00 | +35.84 | -53.28 | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +136.63 | IREN, TPG, HIMS, TNDM, VOR | — | $81.10 | $10,125.70 | IREN×43, TPG×39, HIMS×67, TNDM×85, VOR×90 |
| 2026-08-14 | +5.50 | $81.10 | IREN×43, TPG×39, HIMS×67, TNDM×85, VOR×90 | $10,134.23 | +8.53 | -190.77 | TLN, SLG, BTBT, HYLN, ADUR, ALGM, ARX, AIRO | IREN, TPG, HIMS, TNDM, VOR | $282.67 | $9,904.78 | TLN×3, SLG×21, BTBT×843, HYLN×302, ADUR×76, ALGM×28, ARX×64, AIRO×113 |
| 2026-08-17 | +2.25 | $282.67 | TLN×3, SLG×21, BTBT×843, HYLN×302, ADUR×76, ALGM×28, ARX×64, AIRO×113 | $9,866.97 | -37.81 | -30.67 | DVN, FANG, TMC, TGB, ELF, DNN, NB, CELC | TLN, SLG, BTBT, HYLN, ADUR, ALGM, ARX, AIRO | $105.00 | $9,785.85 | DVN×26, FANG×6, TMC×303, TGB×145, ELF×13, DNN×379, NB×242, CELC×13 |
| 2026-08-18 | -6.20 | $105.00 | DVN×26, FANG×6, TMC×303, TGB×145, ELF×13, DNN×379, NB×242, CELC×13 | $9,695.56 | -90.29 | +0.00 | — | DVN, FANG, TMC, TGB, ELF, DNN, NB, CELC | $9,672.78 | $9,672.78 | — |
| 2026-08-19 | -7.20 | $9,672.78 | — | $9,672.78 | +0.00 | +0.00 | — | — | $9,672.78 | $9,672.78 | — |
| 2026-08-20 | +1.12 | $9,672.78 | — | $9,672.78 | +0.00 | +231.86 | AG, BHP, CDE, IAG, KGC, NFGC, WPM, ABUS | — | $123.49 | $9,879.93 | AG×58, BHP×13, CDE×58, IAG×61, KGC×40, NFGC×690, WPM×8, ABUS×245 |
| 2026-08-21 | +3.25 | $123.49 | AG×58, BHP×13, CDE×58, IAG×61, KGC×40, NFGC×690, WPM×8, ABUS×245 | $10,224.42 | +344.49 | +255.20 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, IAG, KGC, NFGC, WPM, ABUS | $267.50 | $10,416.36 | AU×10, AUPH×74, AEM×5, ARCT×114, AUTL×516, CRDL×660, CRSP×21, CYPH×965 |
| 2026-08-24 | -5.17 | $267.50 | AU×10, AUPH×74, AEM×5, ARCT×114, AUTL×516, CRDL×660, CRSP×21, CYPH×965 | $10,782.45 | +366.09 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,743.71 | $10,743.71 | — |
| 2026-08-25 | +1.80 | $10,743.71 | — | $10,743.71 | +0.00 | +263.62 | MOS, OCUL, INSP, CRMD, RZLT, HCA, CAPR, KURA | — | $139.96 | $10,987.97 | MOS×56, OCUL×122, INSP×21, CRMD×160, RZLT×271, HCA×3, CAPR×185, KURA×98 |
| 2026-08-26 | +2.02 | $139.96 | MOS×56, OCUL×122, INSP×21, CRMD×160, RZLT×271, HCA×3, CAPR×185, KURA×98 | $10,994.45 | +6.48 | -82.44 | AVBP, FLNC, ABX | INSP, CAPR, KURA | $24.72 | $10,898.12 | MOS×56, OCUL×122, CRMD×160, RZLT×271, HCA×3, AVBP×45, FLNC×127, ABX×144 |
| 2026-08-27 | — | $24.72 | MOS×56, OCUL×122, CRMD×160, RZLT×271, HCA×3, AVBP×45, FLNC×127, ABX×144 | $10,914.29 | +16.17 | -4.22 | RRC, CRK, SLI, ACMR, MU, DLO, GEN | OCUL, CRMD, RZLT, HCA, AVBP, FLNC, ABX | $509.20 | $10,873.07 | MOS×56, RRC×32, CRK×94, SLI×524, ACMR×16, MU×1, DLO×89, GEN×45 |
| 2026-08-28 | +0.75 | $509.20 | MOS×56, RRC×32, CRK×94, SLI×524, ACMR×16, MU×1, DLO×89, GEN×45 | $10,877.64 | +4.57 | -300.99 | SEDG, GRRR, URBN, SIMO | ACMR, MU, DLO, GEN | $97.31 | $10,559.74 | MOS×56, RRC×32, CRK×94, SLI×524, SEDG×41, GRRR×86, URBN×17, SIMO×5 |
| 2026-08-31 | -5.85 | $97.31 | MOS×56, RRC×32, CRK×94, SLI×524, SEDG×41, GRRR×86, URBN×17, SIMO×5 | $10,607.79 | +48.05 | +0.00 | — | MOS, RRC, CRK, SLI, SEDG, GRRR, URBN, SIMO | $10,585.86 | $10,585.86 | — |
| 2026-09-01 | -6.30 | $10,585.86 | — | $10,585.86 | -0.00 | +0.00 | — | — | $10,585.86 | $10,585.86 | — |
| 2026-09-02 | -3.83 | $10,585.86 | — | $10,585.86 | -0.00 | +0.00 | — | — | $10,585.86 | $10,585.86 | — |
| 2026-09-03 | -0.90 | $10,585.86 | — | $10,585.86 | -0.00 | -232.33 | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MRNA, ARCT | — | $191.03 | $10,333.70 | ATRC×25, HRMY×30, CABA×364, VSTM×164, RVTY×9, CRK×85, MRNA×9, ARCT×78 |
| 2026-09-04 | +2.25 | $191.03 | ATRC×25, HRMY×30, CABA×364, VSTM×164, RVTY×9, CRK×85, MRNA×9, ARCT×78 | $10,338.89 | +5.19 | -40.18 | ALEC, BMEA, OABI, OPK, VIR, CRM | HRMY, VSTM, RVTY, CRK, MRNA, ARCT | $219.07 | $10,251.79 | ATRC×25, CABA×364, ALEC×513, BMEA×681, OABI×270, OPK×813, VIR×114, CRM×4 |
| 2026-09-08 | -11.47 | $219.07 | ATRC×25, CABA×364, ALEC×513, BMEA×681, OABI×270, OPK×813, VIR×114, CRM×4 | $10,188.43 | -63.36 | +0.00 | — | ATRC, CABA, ALEC, BMEA, OABI, OPK, VIR, CRM | $10,147.40 | $10,147.40 | — |
| 2026-09-09 | -13.95 | $10,147.40 | — | $10,147.40 | -0.00 | +0.00 | — | — | $10,147.40 | $10,147.40 | — |
| 2026-09-10 | -13.28 | $10,147.40 | — | $10,147.40 | -0.00 | +0.00 | — | — | $10,147.40 | $10,147.40 | — |
| 2026-09-11 | +0.50 | $10,147.40 | — | $10,147.40 | -0.00 | +8.84 | SANM, ORCL, NVT, COHU, CLOV, BAK, FUBO, RDDT | — | $180.19 | $10,132.66 | SANM×6, ORCL×7, NVT×8, COHU×22, CLOV×267, BAK×598, FUBO×109, RDDT×8 |
| 2026-09-14 | -11.00 | $180.19 | SANM×6, ORCL×7, NVT×8, COHU×22, CLOV×267, BAK×598, FUBO×109, RDDT×8 | $9,811.07 | -321.59 | -26.88 | — | SANM, ORCL, COHU, CLOV, BAK, FUBO, RDDT | $8,589.23 | $9,762.35 | NVT×8 |
| 2026-09-15 | -3.84 | $8,589.23 | NVT×8 | $9,798.19 | +35.84 | +0.00 | — | NVT | $9,796.16 | $9,796.16 | — |
| 2026-09-16 | +5.30 | $9,796.16 | — | $9,796.16 | +0.00 | +0.00 | — | — | $9,796.16 | $9,796.16 | — |
| 2026-09-17 | +7.38 | $9,796.16 | — | $9,796.16 | +0.00 | +0.00 | — | — | $9,796.16 | $9,796.16 | — |
| 2026-09-18 | +4.86 | $9,796.16 | — | $9,796.16 | +0.00 | +0.00 | — | — | $9,796.16 | $9,796.16 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 43 | $45.98 | $2.12 | — | $8,020.74 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+12.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🔴 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 39 | $50.62 | $2.11 | — | $6,044.33 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+6.2; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🔴 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 67 | $29.74 | $2.19 | — | $4,049.56 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=-5.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 85 | $23.33 | $2.25 | — | $2,064.26 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=+19.7; leftover $2000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 90 | $22.01 | $2.26 | — | $81.10 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=+0.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.10 | ▲ close $10,125.70 vs 09:30 $10,000.00 (session +136.63) | 16:00 close · cash $81.10 · equity $10,125.70 vs 09:30 $10,000.00 (+125.70; session marks +136.63) · 5 name(s) marked open→close (per-name table). IREN×43 09:30 $45.98 → close $44.76 -52.46; TPG×39 09:30 $50.62 → close $54.62 +155.88; HIMS×67 09:30 $29.74 → close $28.77 -64.99; TNDM×85 09:30 $23.33 → close $23.13 -17.00; VOR×90 09:30 $22.01 → close $23.29 +115.20 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.10 | ▲ 09:30 equity $10,134.23 vs yday $10,125.70 (+8.53) | 09:30 open · cash $81.10 (unchanged overnight, no fees) · equity $10,134.23 vs prior close $10,125.70 (+8.53) · 5 name(s) re-marked at the open (per-name table). IREN×43 yday $44.76 → 09:30 $44.09 -28.81; TPG×39 yday $54.62 → 09:30 $55.29 +26.13; HIMS×67 yday $28.77 → 09:30 $29.15 +25.46; TNDM×85 yday $23.13 → 09:30 $22.92 -17.85; VOR×90 yday $23.29 → 09:30 $23.33 +3.60 | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 43 | $44.09 | $2.14 | $-85.53 | $1,974.83 | ▼ -85.53 after sell → book $10,132.09; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 39 | $55.29 | $2.13 | $+177.76 | $4,129.00 | ▲ +177.76 after sell → book $10,129.95; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 67 | $29.15 | $2.22 | $-43.94 | $6,079.84 | ▼ -43.94 after sell → book $10,127.74; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 85 | $22.92 | $2.27 | $-39.37 | $8,025.76 | ▼ -39.37 after sell → book $10,125.46; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 90 | $23.33 | $2.29 | $+114.25 | $10,123.17 | ▲ +114.25 after sell → book $10,123.17; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,041.68 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+5.9; leftover $1265.40 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $7,829.82 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1265.40 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 843 | $1.50 | $10.87 | — | $6,554.44 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+9.2; leftover $1265.40 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 302 | $4.18 | $3.90 | — | $5,288.19 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1265.40 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 76 | $16.50 | $2.22 | — | $4,031.97 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1265.40 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 28 | $44.06 | $2.07 | — | $2,796.22 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable; 🔵; ret5=+3.9; leftover $1265.40 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 64 | $19.57 | $2.18 | — | $1,541.55 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; 🔵; ret5=+58.7; leftover $1265.40 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 113 | $11.12 | $2.33 | — | $282.67 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1265.40 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $282.67 | ▼ close $9,904.78 vs 09:30 $10,134.23 (session -190.77) | 16:00 close · cash $282.67 · equity $9,904.78 vs 09:30 $10,134.23 (-229.45; session marks -190.77) · 8 name(s) marked open→close (per-name table). TLN×3 09:30 $359.83 → close $362.74 +8.73; SLG×21 09:30 $57.61 → close $56.09 -31.92; BTBT×843 09:30 $1.50 → close $1.57 +59.01; HYLN×302 09:30 $4.18 → close $4.06 -36.24; ADUR×76 09:30 $16.50 → close $16.17 -25.08; ALGM×28 09:30 $44.06 → close $44.39 +9.24; ARX×64 09:30 $19.57 → close $19.58 +0.64; AIRO×113 09:30 $11.12 → close $9.57 -175.15 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $282.67 | ▼ 09:30 equity $9,866.97 vs yday $9,904.78 (-37.81) | 09:30 open · cash $282.67 (unchanged overnight, no fees) · equity $9,866.97 vs prior close $9,904.78 (-37.81) · 8 name(s) re-marked at the open (per-name table). TLN×3 yday $362.74 → 09:30 $367.88 +15.42; SLG×21 yday $56.09 → 09:30 $55.37 -15.12; BTBT×843 yday $1.57 → 09:30 $1.52 -42.15; HYLN×302 yday $4.06 → 09:30 $4.10 +12.08; ADUR×76 yday $16.17 → 09:30 $15.73 -33.44; ALGM×28 yday $44.39 → 09:30 $45.32 +26.04; ARX×64 yday $19.58 → 09:30 $19.57 -0.64; AIRO×113 yday $9.57 → 09:30 $9.57 +0.00 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,384.29 | ▲ +20.13 after sell → book $9,864.95; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 21 | $55.37 | $2.07 | $-51.17 | $2,544.98 | ▼ -51.17 after sell → book $9,862.87; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 843 | $1.52 | $11.02 | $-5.04 | $3,815.32 | ▼ -5.04 after sell → book $9,851.85; vs 09:30 mark -11.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 302 | $4.10 | $3.96 | $-32.01 | $5,049.56 | ▼ -32.01 after sell → book $9,847.89; vs 09:30 mark -3.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 76 | $15.73 | $2.24 | $-62.98 | $6,242.80 | ▼ -62.98 after sell → book $9,845.65; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ALGM` | 28 | $45.32 | $2.09 | $+31.11 | $7,509.67 | ▲ +31.11 after sell → book $9,843.56; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 64 | $19.57 | $2.20 | $-4.38 | $8,759.94 | ▼ -4.38 after sell → book $9,841.35; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 113 | $9.57 | $2.36 | $-179.84 | $9,839.00 | ▼ -179.84 after sell → book $9,839.00; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 26 | $46.18 | $2.07 | — | $8,636.25 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+6.7; leftover $1229.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $7,418.04 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+8.3; leftover $1229.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 303 | $4.05 | $3.91 | — | $6,186.98 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=-12.3; leftover $1229.87 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 145 | $8.46 | $2.42 | — | $4,957.86 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+0.4; leftover $1229.87 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 13 | $90.54 | $2.03 | — | $3,778.81 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=-7.2; leftover $1229.87 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 379 | $3.24 | $4.89 | — | $2,545.96 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=+0.3; leftover $1229.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 242 | $5.07 | $3.12 | — | $1,315.90 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=-4.7; leftover $1229.87 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 13 | $92.99 | $2.03 | — | $105.00 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; ret5=-0.8; leftover $1229.87 | join🟡 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $105.00 | ▼ close $9,785.85 vs 09:30 $9,866.97 (session -30.67) | 16:00 close · cash $105.00 · equity $9,785.85 vs 09:30 $9,866.97 (-81.12; session marks -30.67) · 8 name(s) marked open→close (per-name table). DVN×26 09:30 $46.18 → close $47.57 +36.14; FANG×6 09:30 $202.70 → close $206.29 +21.54; TMC×303 09:30 $4.05 → close $3.77 -84.84; TGB×145 09:30 $8.46 → close $8.77 +44.95; ELF×13 09:30 $90.54 → close $93.66 +40.56; DNN×379 09:30 $3.24 → close $3.19 -18.95; NB×242 09:30 $5.07 → close $4.81 -62.92; CELC×13 09:30 $92.99 → close $92.44 -7.15 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $105.00 | ▼ 09:30 equity $9,695.56 vs yday $9,785.85 (-90.29) | 09:30 open · cash $105.00 (unchanged overnight, no fees) · equity $9,695.56 vs prior close $9,785.85 (-90.29) · 8 name(s) re-marked at the open (per-name table). DVN×26 yday $47.57 → 09:30 $48.00 +11.18; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; TMC×303 yday $3.77 → 09:30 $3.72 -15.15; TGB×145 yday $8.77 → 09:30 $8.55 -31.90; ELF×13 yday $93.66 → 09:30 $93.44 -2.86; DNN×379 yday $3.19 → 09:30 $3.11 -30.32; NB×242 yday $4.81 → 09:30 $4.66 -36.30; CELC×13 yday $92.44 → 09:30 $92.38 -0.78 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 26 | $48.00 | $2.09 | $+43.16 | $1,350.91 | ▲ +43.16 after sell → book $9,693.47; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $2,602.46 | ▲ +33.34 after sell → book $9,691.44; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 303 | $3.72 | $3.97 | $-107.87 | $3,725.65 | ▼ -107.87 after sell → book $9,687.47; vs 09:30 mark -3.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 145 | $8.55 | $2.46 | $+8.17 | $4,962.94 | ▲ +8.17 after sell → book $9,685.01; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 13 | $93.44 | $2.05 | $+33.62 | $6,175.62 | ▲ +33.62 after sell → book $9,682.97; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 379 | $3.11 | $4.96 | $-59.12 | $7,349.34 | ▼ -59.12 after sell → book $9,678.00; vs 09:30 mark -4.97 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `NB` | 242 | $4.66 | $3.17 | $-105.51 | $8,473.89 | ▼ -105.51 after sell → book $9,674.83; vs 09:30 mark -3.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 13 | $92.38 | $2.05 | $-12.01 | $9,672.78 | ▼ -12.01 after sell → book $9,672.78; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,672.78 | ▲ close $9,672.78 vs 09:30 $9,695.56 (session +0.00) | 16:00 close · cash $9,672.78 · no lots left · equity $9,672.78. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,672.78 | ▲ 09:30 equity $9,672.78 vs yday $9,672.78 (+0.00) | 09:30 open · cash $9,672.78 · no holdings · equity $9,672.78 vs prior close $9,672.78 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,672.78 | ▲ close $9,672.78 vs 09:30 $9,672.78 (session +0.00) | 16:00 close · cash $9,672.78 · no lots left · equity $9,672.78. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,672.78 | ▲ 09:30 equity $9,672.78 vs yday $9,672.78 (+0.00) | 09:30 open · cash $9,672.78 · no holdings · equity $9,672.78 vs prior close $9,672.78 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,478.72 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1209.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,293.56 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1209.10 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 58 | $20.65 | $2.16 | — | $6,093.70 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1209.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $4,894.09 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1209.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $3,706.78 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1209.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 690 | $1.75 | $8.90 | — | $2,490.38 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1209.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,332.05 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1209.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 245 | $4.92 | $3.16 | — | $123.49 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1209.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.49 | ▲ close $9,879.93 vs 09:30 $9,672.78 (session +231.86) | 16:00 close · cash $123.49 · equity $9,879.93 vs 09:30 $9,672.78 (+207.15; session marks +231.86) · 8 name(s) marked open→close (per-name table). AG×58 09:30 $20.55 → close $21.19 +37.12; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×58 09:30 $20.65 → close $21.11 +26.68; IAG×61 09:30 $19.63 → close $20.50 +53.07; KGC×40 09:30 $29.63 → close $31.43 +72.00; NFGC×690 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68; ABUS×245 09:30 $4.92 → close $4.77 -36.75 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.49 | ▲ 09:30 equity $10,224.42 vs yday $9,879.93 (+344.49) | 09:30 open · cash $123.49 (unchanged overnight, no fees) · equity $10,224.42 vs prior close $9,879.93 (+344.49) · 8 name(s) re-marked at the open (per-name table). AG×58 yday $21.19 → 09:30 $21.90 +41.18; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×58 yday $21.11 → 09:30 $21.75 +37.12; IAG×61 yday $20.50 → 09:30 $21.17 +40.87; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×690 yday $1.75 → 09:30 $1.79 +27.60; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×245 yday $4.77 → 09:30 $5.20 +105.35 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 58 | $21.90 | $2.18 | $+73.95 | $1,391.50 | ▲ +73.95 after sell → book $10,222.23; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,633.81 | ▲ +57.15 after sell → book $10,220.18; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 58 | $21.75 | $2.18 | $+59.45 | $3,893.13 | ▲ +59.45 after sell → book $10,218.00; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 61 | $21.17 | $2.19 | $+89.57 | $5,182.31 | ▲ +89.57 after sell → book $10,215.81; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $6,466.98 | ▲ +97.36 after sell → book $10,213.68; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 690 | $1.79 | $9.03 | $+9.67 | $7,693.05 | ▲ +9.67 after sell → book $10,204.65; vs 09:30 mark -9.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $8,928.62 | ▲ +77.23 after sell → book $10,202.62; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 245 | $5.20 | $3.21 | $+62.23 | $10,199.40 | ▲ +62.23 after sell → book $10,199.40; vs 09:30 mark -3.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,003.08 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1274.93 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 74 | $17.20 | $2.21 | — | $7,728.07 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1274.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,644.57 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1274.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 114 | $11.13 | $2.33 | — | $5,373.42 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1274.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 516 | $2.47 | $6.66 | — | $4,092.24 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1274.93 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 660 | $1.93 | $8.51 | — | $2,809.93 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1274.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,553.75 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1274.93 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 965 | $1.32 | $12.45 | — | $267.50 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1274.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $267.50 | ▲ close $10,416.36 vs 09:30 $10,224.42 (session +255.20) | 16:00 close · cash $267.50 · equity $10,416.36 vs 09:30 $10,224.42 (+191.94; session marks +255.20) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×74 09:30 $17.20 → close $16.65 -40.70; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×114 09:30 $11.13 → close $13.45 +264.48; AUTL×516 09:30 $2.47 → close $2.41 -30.96; CRDL×660 09:30 $1.93 → close $1.86 -46.20; CRSP×21 09:30 $59.72 → close $59.50 -4.62; CYPH×965 09:30 $1.32 → close $1.42 +96.50 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $267.50 | ▲ 09:30 equity $10,782.45 vs yday $10,416.36 (+366.09) | 09:30 open · cash $267.50 (unchanged overnight, no fees) · equity $10,782.45 vs prior close $10,416.36 (+366.09) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×74 yday $16.65 → 09:30 $16.57 -5.92; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×114 yday $13.45 → 09:30 $13.33 -13.68; AUTL×516 yday $2.41 → 09:30 $2.40 -5.16; CRDL×660 yday $1.86 → 09:30 $1.88 +13.20; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; CYPH×965 yday $1.42 → 09:30 $1.83 +395.65 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,470.56 | ▲ +6.74 after sell → book $10,780.41; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 74 | $16.57 | $2.23 | $-51.07 | $2,694.51 | ▼ -51.07 after sell → book $10,778.18; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,777.63 | ▼ -0.38 after sell → book $10,776.15; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 114 | $13.33 | $2.36 | $+246.10 | $5,294.89 | ▲ +246.10 after sell → book $10,773.79; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 516 | $2.40 | $6.75 | $-49.53 | $6,526.54 | ▼ -49.53 after sell → book $10,767.04; vs 09:30 mark -6.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 660 | $1.88 | $8.63 | $-50.15 | $7,758.71 | ▼ -50.15 after sell → book $10,758.41; vs 09:30 mark -8.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $8,990.38 | ▼ -24.50 after sell → book $10,756.33; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 965 | $1.83 | $12.62 | $+467.08 | $10,743.71 | ▲ +467.08 after sell → book $10,743.71; vs 09:30 mark -12.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,743.71 | ▲ close $10,743.71 vs 09:30 $10,782.45 (session +0.00) | 16:00 close · cash $10,743.71 · no lots left · equity $10,743.71. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,743.71 | ▲ 09:30 equity $10,743.71 vs yday $10,743.71 (+0.00) | 09:30 open · cash $10,743.71 · no holdings · equity $10,743.71 vs prior close $10,743.71 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 56 | $23.77 | $2.16 | — | $9,410.43 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=+13.0; leftover $1342.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 122 | $10.98 | $2.36 | — | $8,068.52 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+1.2; leftover $1342.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 21 | $61.19 | $2.05 | — | $6,781.47 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+7.4; leftover $1342.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 160 | $8.35 | $2.47 | — | $5,443.00 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1342.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 271 | $4.94 | $3.50 | — | $4,100.77 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+7.1; leftover $1342.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $2,817.86 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+6.0; leftover $1342.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 185 | $7.25 | $2.54 | — | $1,474.06 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1342.96 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 98 | $13.59 | $2.28 | — | $139.96 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1342.96 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $139.96 | ▲ close $10,987.97 vs 09:30 $10,743.71 (session +263.62) | 16:00 close · cash $139.96 · equity $10,987.97 vs 09:30 $10,743.71 (+244.26; session marks +263.62) · 8 name(s) marked open→close (per-name table). MOS×56 09:30 $23.77 → close $24.27 +28.00; OCUL×122 09:30 $10.98 → close $10.88 -12.20; INSP×21 09:30 $61.19 → close $61.07 -2.52; CRMD×160 09:30 $8.35 → close $8.56 +33.60; RZLT×271 09:30 $4.94 → close $5.01 +18.97; HCA×3 09:30 $426.97 → close $428.76 +5.37; CAPR×185 09:30 $7.25 → close $8.29 +192.40; KURA×98 09:30 $13.59 → close $13.59 +0.00 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.96 | ▲ 09:30 equity $10,994.45 vs yday $10,987.97 (+6.48) | 09:30 open · cash $139.96 (unchanged overnight, no fees) · equity $10,994.45 vs prior close $10,987.97 (+6.48) · 8 name(s) re-marked at the open (per-name table). MOS×56 yday $24.27 → 09:30 $24.84 +31.92; OCUL×122 yday $10.88 → 09:30 $10.79 -10.98; INSP×21 yday $61.07 → 09:30 $60.07 -21.00; CRMD×160 yday $8.56 → 09:30 $8.60 +6.40; RZLT×271 yday $5.01 → 09:30 $5.01 +0.00; HCA×3 yday $428.76 → 09:30 $427.50 -3.78; CAPR×185 yday $8.29 → 09:30 $8.29 +0.00; KURA×98 yday $13.59 → 09:30 $13.63 +3.92 | — |
| 2026-08-26 09:30 ET | **SELL** | `INSP` | 21 | $60.07 | $2.07 | $-27.65 | $1,399.36 | ▼ -27.65 after sell → book $10,992.38; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 185 | $8.29 | $2.59 | $+187.27 | $2,930.42 | ▲ +187.27 after sell → book $10,989.79; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 98 | $13.63 | $2.31 | $-0.67 | $4,263.85 | ▼ -0.67 after sell → book $10,987.48; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 45 | $31.21 | $2.12 | — | $2,857.27 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1421.28 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 127 | $11.12 | $2.37 | — | $1,442.66 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1421.28 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 144 | $9.83 | $2.42 | — | $24.72 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1421.28 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.72 | ▼ close $10,898.12 vs 09:30 $10,994.45 (session -82.44) | 16:00 close · cash $24.72 · equity $10,898.12 vs 09:30 $10,994.45 (-96.33; session marks -82.44) · 8 name(s) marked open→close (per-name table). MOS×56 09:30 $24.84 → close $24.16 -38.08; OCUL×122 09:30 $10.79 → close $10.77 -2.44; CRMD×160 09:30 $8.60 → close $8.39 -33.60; RZLT×271 09:30 $5.01 → close $5.04 +8.13; HCA×3 09:30 $427.50 → close $427.16 -1.02; AVBP×45 09:30 $31.21 → close $31.14 -3.15; FLNC×127 09:30 $11.12 → close $11.08 -5.08; ABX×144 09:30 $9.83 → close $9.78 -7.20 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.72 | ▲ 09:30 equity $10,914.29 vs yday $10,898.12 (+16.17) | 09:30 open · cash $24.72 (unchanged overnight, no fees) · equity $10,914.29 vs prior close $10,898.12 (+16.17) · 8 name(s) re-marked at the open (per-name table). MOS×56 yday $24.16 → 09:30 $24.00 -8.96; OCUL×122 yday $10.77 → 09:30 $10.63 -17.08; CRMD×160 yday $8.39 → 09:30 $8.49 +16.00; RZLT×271 yday $5.04 → 09:30 $5.07 +8.13; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; AVBP×45 yday $31.14 → 09:30 $30.79 -15.75; FLNC×127 yday $11.08 → 09:30 $11.52 +55.88; ABX×144 yday $9.78 → 09:30 $9.68 -14.40 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 122 | $10.63 | $2.39 | $-47.44 | $1,319.19 | ▼ -47.44 after sell → book $10,911.90; vs 09:30 mark -2.39 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 160 | $8.49 | $2.51 | $+17.42 | $2,675.09 | ▲ +17.42 after sell → book $10,909.40; vs 09:30 mark -2.50 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 271 | $5.07 | $3.55 | $+28.18 | $4,045.50 | ▲ +28.18 after sell → book $10,905.84; vs 09:30 mark -3.56 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-11.10 | $5,317.31 | ▼ -11.10 after sell → book $10,903.82; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 45 | $30.79 | $2.15 | $-23.17 | $6,700.72 | ▼ -23.17 after sell → book $10,901.68; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 127 | $11.52 | $2.40 | $+46.03 | $8,161.35 | ▲ +46.03 after sell → book $10,899.27; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ABX` | 144 | $9.68 | $2.46 | $-26.48 | $9,552.82 | ▼ -26.48 after sell → book $10,896.82; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 32 | $41.44 | $2.09 | — | $8,224.65 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+3.1; leftover $1364.69 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 94 | $14.42 | $2.27 | — | $6,866.90 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+7.1; leftover $1364.69 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 524 | $2.60 | $6.76 | — | $5,497.74 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+13.0; leftover $1364.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 16 | $81.65 | $2.04 | — | $4,189.30 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list mover_buy; 🔵; ret5=+2.0; leftover $1364.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $3,220.30 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list mover_buy; 🔵; ret5=+0.1; leftover $1364.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 89 | $15.33 | $2.26 | — | $1,853.67 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list mover_buy; 🔵; ret5=+7.4; leftover $1364.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 45 | $29.83 | $2.12 | — | $509.20 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list mover_buy; 🔵; ret5=+7.6; leftover $1364.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $509.20 | ▼ close $10,873.07 vs 09:30 $10,914.29 (session -4.22) | 16:00 close · cash $509.20 · equity $10,873.07 vs 09:30 $10,914.29 (-41.22; session marks -4.22) · 8 name(s) marked open→close (per-name table). MOS×56 09:30 $24.00 → close $23.76 -13.44; RRC×32 09:30 $41.44 → close $41.64 +6.40; CRK×94 09:30 $14.42 → close $14.62 +18.80; SLI×524 09:30 $2.60 → close $2.64 +20.96; ACMR×16 09:30 $81.65 → close $80.49 -18.56; MU×1 09:30 $967.01 → close $935.39 -31.62; DLO×89 09:30 $15.33 → close $15.14 -16.91; GEN×45 09:30 $29.83 → close $30.50 +30.15 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $509.20 | ▲ 09:30 equity $10,877.64 vs yday $10,873.07 (+4.57) | 09:30 open · cash $509.20 (unchanged overnight, no fees) · equity $10,877.64 vs prior close $10,873.07 (+4.57) · 8 name(s) re-marked at the open (per-name table). MOS×56 yday $23.76 → 09:30 $23.95 +10.64; RRC×32 yday $41.64 → 09:30 $41.74 +3.20; CRK×94 yday $14.62 → 09:30 $14.63 +0.94; SLI×524 yday $2.64 → 09:30 $2.68 +20.96; ACMR×16 yday $80.49 → 09:30 $79.27 -19.52; MU×1 yday $935.39 → 09:30 $919.29 -16.10; DLO×89 yday $15.14 → 09:30 $15.19 +4.45; GEN×45 yday $30.50 → 09:30 $30.50 +0.00 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 16 | $79.27 | $2.06 | $-42.18 | $1,775.46 | ▼ -42.18 after sell → book $10,875.58; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $2,692.74 | ▼ -51.73 after sell → book $10,873.57; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 89 | $15.19 | $2.28 | $-17.00 | $4,042.36 | ▼ -17.00 after sell → book $10,871.28; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 45 | $30.50 | $2.15 | $+25.88 | $5,412.72 | ▲ +25.88 after sell → book $10,869.14; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 41 | $32.90 | $2.11 | — | $4,061.70 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1353.18 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 86 | $15.66 | $2.25 | — | $2,712.70 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1353.18 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 17 | $79.42 | $2.04 | — | $1,360.51 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1353.18 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $97.31 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1353.18 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.31 | ▼ close $10,559.74 vs 09:30 $10,877.64 (session -300.99) | 16:00 close · cash $97.31 · equity $10,559.74 vs 09:30 $10,877.64 (-317.90; session marks -300.99) · 8 name(s) marked open→close (per-name table). MOS×56 09:30 $23.95 → close $23.60 -19.60; RRC×32 09:30 $41.74 → close $41.46 -8.96; CRK×94 09:30 $14.63 → close $14.29 -31.96; SLI×524 09:30 $2.68 → close $2.55 -68.12; SEDG×41 09:30 $32.90 → close $31.41 -61.09; GRRR×86 09:30 $15.66 → close $14.41 -107.50; URBN×17 09:30 $79.42 → close $81.09 +28.39; SIMO×5 09:30 $252.24 → close $245.81 -32.15 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.31 | ▲ 09:30 equity $10,607.79 vs yday $10,559.74 (+48.05) | 09:30 open · cash $97.31 (unchanged overnight, no fees) · equity $10,607.79 vs prior close $10,559.74 (+48.05) · 8 name(s) re-marked at the open (per-name table). MOS×56 yday $23.60 → 09:30 $23.68 +4.48; RRC×32 yday $41.46 → 09:30 $42.00 +17.28; CRK×94 yday $14.29 → 09:30 $14.54 +23.50; SLI×524 yday $2.55 → 09:30 $2.58 +15.72; SEDG×41 yday $31.41 → 09:30 $31.15 -10.66; GRRR×86 yday $14.41 → 09:30 $14.44 +2.58; URBN×17 yday $81.09 → 09:30 $80.44 -11.05; SIMO×5 yday $245.81 → 09:30 $247.05 +6.20 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 56 | $23.68 | $2.18 | $-9.38 | $1,421.21 | ▼ -9.38 after sell → book $10,605.61; vs 09:30 mark -2.18 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 32 | $42.00 | $2.11 | $+13.73 | $2,763.10 | ▲ +13.73 after sell → book $10,603.50; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 94 | $14.54 | $2.30 | $+6.71 | $4,127.57 | ▲ +6.71 after sell → book $10,601.21; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 524 | $2.58 | $6.86 | $-24.10 | $5,472.63 | ▼ -24.10 after sell → book $10,594.35; vs 09:30 mark -6.86 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 41 | $31.15 | $2.13 | $-76.00 | $6,747.65 | ▼ -76.00 after sell → book $10,592.22; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 86 | $14.44 | $2.27 | $-109.44 | $7,987.21 | ▼ -109.44 after sell → book $10,589.94; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 17 | $80.44 | $2.06 | $+13.24 | $9,352.63 | ▲ +13.24 after sell → book $10,587.88; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $10,585.86 | ▼ -29.98 after sell → book $10,585.86; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,585.86 | ▲ close $10,585.86 vs 09:30 $10,607.79 (session +0.00) | 16:00 close · cash $10,585.86 · no lots left · equity $10,585.86. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,585.86 | ▲ 09:30 equity $10,585.86 vs yday $10,585.86 (-0.00) | 09:30 open · cash $10,585.86 · no holdings · equity $10,585.86 vs prior close $10,585.86 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,585.86 | ▲ close $10,585.86 vs 09:30 $10,585.86 (session +0.00) | 16:00 close · cash $10,585.86 · no lots left · equity $10,585.86. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,585.86 | ▲ 09:30 equity $10,585.86 vs yday $10,585.86 (-0.00) | 09:30 open · cash $10,585.86 · no holdings · equity $10,585.86 vs prior close $10,585.86 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,585.86 | ▲ close $10,585.86 vs 09:30 $10,585.86 (session +0.00) | 16:00 close · cash $10,585.86 · no lots left · equity $10,585.86. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,585.86 | ▲ 09:30 equity $10,585.86 vs yday $10,585.86 (-0.00) | 09:30 open · cash $10,585.86 · no holdings · equity $10,585.86 vs prior close $10,585.86 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,261.79 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1323.23 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $42.93 | $2.08 | — | $7,971.81 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1323.23 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 364 | $3.63 | $4.70 | — | $6,645.80 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1323.23 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 164 | $8.03 | $2.48 | — | $5,326.39 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1323.23 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $4,132.33 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1323.23 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 85 | $15.45 | $2.25 | — | $2,816.83 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1323.23 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,501.31 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1323.23 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 78 | $16.77 | $2.22 | — | $191.03 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1323.23 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $191.03 | ▼ close $10,333.70 vs 09:30 $10,585.86 (session -232.33) | 16:00 close · cash $191.03 · equity $10,333.70 vs 09:30 $10,585.86 (-252.16; session marks -232.33) · 8 name(s) marked open→close (per-name table). ATRC×25 09:30 $52.88 → close $52.46 -10.50; HRMY×30 09:30 $42.93 → close $41.86 -32.10; CABA×364 09:30 $3.63 → close $3.48 -54.60; VSTM×164 09:30 $8.03 → close $7.98 -8.20; RVTY×9 09:30 $132.45 → close $130.63 -16.38; CRK×85 09:30 $15.45 → close $14.95 -42.50; MRNA×9 09:30 $145.94 → close $148.87 +26.33; ARCT×78 09:30 $16.77 → close $15.56 -94.38 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $191.03 | ▲ 09:30 equity $10,338.89 vs yday $10,333.70 (+5.19) | 09:30 open · cash $191.03 (unchanged overnight, no fees) · equity $10,338.89 vs prior close $10,333.70 (+5.19) · 8 name(s) re-marked at the open (per-name table). ATRC×25 yday $52.46 → 09:30 $52.03 -10.75; HRMY×30 yday $41.86 → 09:30 $41.50 -10.80; CABA×364 yday $3.48 → 09:30 $3.46 -7.28; VSTM×164 yday $7.98 → 09:30 $7.91 -11.48; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; CRK×85 yday $14.95 → 09:30 $15.00 +4.25; MRNA×9 yday $148.87 → 09:30 $153.62 +42.75; ARCT×78 yday $15.56 → 09:30 $15.61 +3.90 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 30 | $41.50 | $2.10 | $-47.08 | $1,433.93 | ▼ -47.08 after sell → book $10,336.79; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 164 | $7.91 | $2.52 | $-24.68 | $2,728.65 | ▼ -24.68 after sell → book $10,334.27; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $3,896.88 | ▼ -25.83 after sell → book $10,332.23; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 85 | $15.00 | $2.27 | $-42.76 | $5,169.61 | ▼ -42.76 after sell → book $10,329.96; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $6,550.15 | ▲ +65.02 after sell → book $10,327.92; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 78 | $15.61 | $2.25 | $-94.95 | $7,765.48 | ▼ -94.95 after sell → book $10,325.67; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 513 | $2.52 | $6.62 | — | $6,466.11 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1294.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 681 | $1.90 | $8.78 | — | $5,163.42 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1294.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 270 | $4.78 | $3.48 | — | $3,869.34 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1294.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 813 | $1.59 | $10.49 | — | $2,566.18 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1294.25 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 114 | $11.31 | $2.33 | — | $1,274.51 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1294.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $219.07 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1294.25 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $219.07 | ▼ close $10,251.79 vs 09:30 $10,338.89 (session -40.18) | 16:00 close · cash $219.07 · equity $10,251.79 vs 09:30 $10,338.89 (-87.10; session marks -40.18) · 8 name(s) marked open→close (per-name table). ATRC×25 09:30 $52.03 → close $51.52 -12.75; CABA×364 09:30 $3.46 → close $3.47 +3.64; ALEC×513 09:30 $2.52 → close $2.46 -30.78; BMEA×681 09:30 $1.90 → close $2.03 +88.53; OABI×270 09:30 $4.78 → close $4.33 -121.50; OPK×813 09:30 $1.59 → close $1.64 +40.65; VIR×114 09:30 $11.31 → close $11.38 +8.55; CRM×4 09:30 $263.36 → close $259.23 -16.52 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $219.07 | ▼ 09:30 equity $10,188.43 vs yday $10,251.79 (-63.36) | 09:30 open · cash $219.07 (unchanged overnight, no fees) · equity $10,188.43 vs prior close $10,251.79 (-63.36) · 8 name(s) re-marked at the open (per-name table). ATRC×25 yday $51.52 → 09:30 $54.31 +69.75; CABA×364 yday $3.47 → 09:30 $3.43 -14.56; ALEC×513 yday $2.46 → 09:30 $2.38 -41.04; BMEA×681 yday $2.03 → 09:30 $2.00 -20.43; OABI×270 yday $4.33 → 09:30 $4.30 -8.10; OPK×813 yday $1.64 → 09:30 $1.63 -8.13; VIR×114 yday $11.38 → 09:30 $11.22 -18.81; CRM×4 yday $259.23 → 09:30 $253.72 -22.04 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 25 | $54.31 | $2.09 | $+31.60 | $1,574.73 | ▲ +31.60 after sell → book $10,186.34; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 364 | $3.43 | $4.77 | $-82.26 | $2,818.49 | ▼ -82.26 after sell → book $10,181.58; vs 09:30 mark -4.76 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 513 | $2.38 | $6.71 | $-85.15 | $4,032.71 | ▼ -85.15 after sell → book $10,174.86; vs 09:30 mark -6.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 681 | $2.00 | $8.91 | $+50.41 | $5,385.80 | ▲ +50.41 after sell → book $10,165.95; vs 09:30 mark -8.91 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 270 | $4.30 | $3.54 | $-136.62 | $6,543.27 | ▼ -136.62 after sell → book $10,162.42; vs 09:30 mark -3.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 813 | $1.63 | $10.63 | $+11.40 | $7,857.82 | ▲ +11.40 after sell → book $10,151.78; vs 09:30 mark -10.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 114 | $11.22 | $2.36 | $-14.95 | $9,134.54 | ▼ -14.95 after sell → book $10,149.42; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $10,147.40 | ▼ -42.58 after sell → book $10,147.40; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,147.40 | ▲ close $10,147.40 vs 09:30 $10,188.43 (session +0.00) | 16:00 close · cash $10,147.40 · no lots left · equity $10,147.40. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,147.40 | ▲ 09:30 equity $10,147.40 vs yday $10,147.40 (-0.00) | 09:30 open · cash $10,147.40 · no holdings · equity $10,147.40 vs prior close $10,147.40 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,147.40 | ▲ close $10,147.40 vs 09:30 $10,147.40 (session +0.00) | 16:00 close · cash $10,147.40 · no lots left · equity $10,147.40. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,147.40 | ▲ 09:30 equity $10,147.40 vs yday $10,147.40 (-0.00) | 09:30 open · cash $10,147.40 · no holdings · equity $10,147.40 vs prior close $10,147.40 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,147.40 | ▲ close $10,147.40 vs 09:30 $10,147.40 (session +0.00) | 16:00 close · cash $10,147.40 · no lots left · equity $10,147.40. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,147.40 | ▲ 09:30 equity $10,147.40 vs yday $10,147.40 (-0.00) | 09:30 open · cash $10,147.40 · no holdings · equity $10,147.40 vs prior close $10,147.40 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $8,904.35 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+8.3; leftover $1268.42 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $7,751.33 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1268.42 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $6,487.08 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+4.7; leftover $1268.42 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 22 | $56.09 | $2.06 | — | $5,251.04 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+19.6; leftover $1268.42 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 267 | $4.75 | $3.44 | — | $3,979.35 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1268.42 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 598 | $2.12 | $7.71 | — | $2,703.87 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1268.42 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 109 | $11.55 | $2.32 | — | $1,442.60 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1268.42 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 8 | $157.55 | $2.01 | — | $180.19 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1268.42 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟡 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $180.19 | ▲ close $10,132.66 vs 09:30 $10,147.40 (session +8.84) | 16:00 close · cash $180.19 · equity $10,132.66 vs 09:30 $10,147.40 (-14.74; session marks +8.84) · 8 name(s) marked open→close (per-name table). SANM×6 09:30 $206.84 → close $216.00 +54.96; ORCL×7 09:30 $164.43 → close $150.28 -99.05; NVT×8 09:30 $157.78 → close $162.38 +36.80; COHU×22 09:30 $56.09 → close $57.08 +21.78; CLOV×267 09:30 $4.75 → close $4.82 +18.69; BAK×598 09:30 $2.12 → close $2.08 -23.92; FUBO×109 09:30 $11.55 → close $11.53 -2.18; RDDT×8 09:30 $157.55 → close $157.77 +1.76 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $180.19 | ▼ 09:30 equity $9,811.07 vs yday $10,132.66 (-321.59) | 09:30 open · cash $180.19 (unchanged overnight, no fees) · equity $9,811.07 vs prior close $10,132.66 (-321.59) · 8 name(s) re-marked at the open (per-name table). SANM×6 yday $216.00 → 09:30 $206.50 -57.00; ORCL×7 yday $150.28 → 09:30 $141.42 -62.02; NVT×8 yday $162.38 → 09:30 $150.00 -99.04; COHU×22 yday $57.08 → 09:30 $52.23 -106.70; CLOV×267 yday $4.82 → 09:30 $4.82 +0.00; BAK×598 yday $2.08 → 09:30 $2.05 -17.94; FUBO×109 yday $11.53 → 09:30 $11.56 +3.27; RDDT×8 yday $157.77 → 09:30 $160.00 +17.84 | — |
| 2026-09-14 09:30 ET | **SELL** | `SANM` | 6 | $206.50 | $2.03 | $-6.08 | $1,417.16 | ▼ -6.08 after sell → book $9,809.04; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 7 | $141.42 | $2.03 | $-165.11 | $2,405.07 | ▼ -165.11 after sell → book $9,807.01; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COHU` | 22 | $52.23 | $2.08 | $-89.05 | $3,552.06 | ▼ -89.05 after sell → book $9,804.94; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 267 | $4.82 | $3.50 | $+11.75 | $4,835.50 | ▲ +11.75 after sell → book $9,801.44; vs 09:30 mark -3.50 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 598 | $2.05 | $7.82 | $-57.40 | $6,053.57 | ▼ -57.40 after sell → book $9,793.61; vs 09:30 mark -7.83 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `FUBO` | 109 | $11.56 | $2.35 | $-3.57 | $7,311.27 | ▼ -3.57 after sell → book $9,791.27; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RDDT` | 8 | $160.00 | $2.03 | $+15.55 | $8,589.23 | ▲ +15.55 after sell → book $9,789.23; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,589.23 | ▼ close $9,762.35 vs 09:30 $9,811.07 (session -26.88) | 16:00 close · cash $8,589.23 · equity $9,762.35 vs 09:30 $9,811.07 (-48.72; session marks -26.88) · 1 name(s) marked open→close (per-name table). NVT×8 09:30 $150.00 → close $146.64 -26.88 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,589.23 | ▲ 09:30 equity $9,798.19 vs yday $9,762.35 (+35.84) | 09:30 open · cash $8,589.23 (unchanged overnight, no fees) · equity $9,798.19 vs prior close $9,762.35 (+35.84) · 1 name(s) re-marked at the open (per-name table). NVT×8 yday $146.64 → 09:30 $151.12 +35.84 | — |
| 2026-09-15 09:30 ET | **SELL** | `NVT` | 8 | $151.12 | $2.03 | $-57.33 | $9,796.16 | ▼ -57.33 after sell → book $9,796.16; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,796.16 | ▲ close $9,796.16 vs 09:30 $9,798.19 (session +0.00) | 16:00 close · cash $9,796.16 · no lots left · equity $9,796.16. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,796.16 | ▲ 09:30 equity $9,796.16 vs yday $9,796.16 (+0.00) | 09:30 open · cash $9,796.16 · no holdings · equity $9,796.16 vs prior close $9,796.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,796.16 | ▲ close $9,796.16 vs 09:30 $9,796.16 (session +0.00) | 16:00 close · cash $9,796.16 · no lots left · equity $9,796.16. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,796.16 | ▲ 09:30 equity $9,796.16 vs yday $9,796.16 (+0.00) | 09:30 open · cash $9,796.16 · no holdings · equity $9,796.16 vs prior close $9,796.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,796.16 | ▲ close $9,796.16 vs 09:30 $9,796.16 (session +0.00) | 16:00 close · cash $9,796.16 · no lots left · equity $9,796.16. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,796.16 | ▲ 09:30 equity $9,796.16 vs yday $9,796.16 (+0.00) | 09:30 open · cash $9,796.16 · no holdings · equity $9,796.16 vs prior close $9,796.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,796.16 | ▲ close $9,796.16 vs 09:30 $9,796.16 (session +0.00) | 16:00 close · cash $9,796.16 · no lots left · equity $9,796.16. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SSL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `KEP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VICR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VERI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CMRC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INDP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBLX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
