# Factor mine action — `union_clk_fresh_cat_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · Clock-B #2 fresh catalyst + limited extension (research; not KEEP)

Cash book **-2.20%** ($9,780) · signal-only (no cash/fees) was -0.34%. Starts YES **14/26**. Fills 198 · skips 87 · realized $-282.29.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: Clock-B #2: a fresh good catalyst (catal / EPS beat / packet or headline green) and the prior tape is not already exploded.
- Must-not: Clock-B #5 long veto: extreme prior extension plus diminishing progress or a failed breakout.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 8.
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
- **Gate** `clk_fresh_cat_coil=True` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $213.29.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 167 | — | $59.80 | +0.00 | $60.23 | +71.81 | +71.81 | +0.00 | +71.81 |
| 2026-08-14 | `BTSG` | 167 | $60.23 | $59.65 | -96.86 | — | +0.00 | -96.86 | -25.05 | — |
| 2026-08-14 | `ADUR` | 75 | — | $16.50 | +0.00 | $16.17 | -24.75 | -24.75 | +0.00 | -24.75 |
| 2026-08-14 | `ANGX` | 289 | — | $4.31 | +0.00 | $4.37 | +17.34 | +17.34 | +0.00 | +17.34 |
| 2026-08-14 | `BETR` | 84 | — | $14.80 | +0.00 | $13.73 | -89.88 | -89.88 | +0.00 | -89.88 |
| 2026-08-14 | `KULR` | 498 | — | $2.50 | +0.00 | $2.64 | +69.72 | +69.72 | +0.00 | +69.72 |
| 2026-08-14 | `WDC` | 2 | — | $503.50 | +0.00 | $508.80 | +10.60 | +10.60 | +0.00 | +10.60 |
| 2026-08-14 | `ALGM` | 28 | — | $44.06 | +0.00 | $44.39 | +9.24 | +9.24 | +0.00 | +9.24 |
| 2026-08-14 | `AMAT` | 2 | — | $499.40 | +0.00 | $507.18 | +15.56 | +15.56 | +0.00 | +15.56 |
| 2026-08-14 | `NRG` | 10 | — | $120.00 | +0.00 | $126.24 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-08-17 | `ADUR` | 75 | $16.17 | $15.73 | -33.00 | — | +0.00 | -33.00 | -57.75 | — |
| 2026-08-17 | `ANGX` | 289 | $4.37 | $4.60 | +66.47 | — | +0.00 | +66.47 | +83.81 | — |
| 2026-08-17 | `BETR` | 84 | $13.73 | $13.67 | -5.04 | — | +0.00 | -5.04 | -94.92 | — |
| 2026-08-17 | `KULR` | 498 | $2.64 | $2.63 | -4.98 | — | +0.00 | -4.98 | +64.74 | — |
| 2026-08-17 | `WDC` | 2 | $508.80 | $525.53 | +33.46 | — | +0.00 | +33.46 | +44.06 | — |
| 2026-08-17 | `ALGM` | 28 | $44.39 | $45.32 | +26.04 | — | +0.00 | +26.04 | +35.28 | — |
| 2026-08-17 | `AMAT` | 2 | $507.18 | $517.45 | +20.53 | — | +0.00 | +20.53 | +36.09 | — |
| 2026-08-17 | `NRG` | 10 | $126.24 | $127.40 | +11.60 | — | +0.00 | +11.60 | +74.00 | — |
| 2026-08-17 | `DVN` | 27 | — | $46.18 | +0.00 | $47.57 | +37.53 | +37.53 | +0.00 | +37.53 |
| 2026-08-17 | `EOG` | 8 | — | $142.77 | +0.00 | $146.15 | +27.04 | +27.04 | +0.00 | +27.04 |
| 2026-08-17 | `FANG` | 6 | — | $202.70 | +0.00 | $206.29 | +21.54 | +21.54 | +0.00 | +21.54 |
| 2026-08-17 | `ABX` | 138 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `ALM` | 78 | — | $16.20 | +0.00 | $16.36 | +12.48 | +12.48 | +0.00 | +12.48 |
| 2026-08-17 | `DNN` | 390 | — | $3.24 | +0.00 | $3.19 | -19.50 | -19.50 | +0.00 | -19.50 |
| 2026-08-17 | `ELF` | 13 | — | $90.54 | +0.00 | $93.66 | +40.56 | +40.56 | +0.00 | +40.56 |
| 2026-08-17 | `NB` | 249 | — | $5.07 | +0.00 | $4.81 | -64.74 | -64.74 | +0.00 | -64.74 |
| 2026-08-18 | `DVN` | 27 | $47.57 | $48.00 | +11.61 | — | +0.00 | +11.61 | +49.14 | — |
| 2026-08-18 | `EOG` | 8 | $146.15 | $148.04 | +15.12 | — | +0.00 | +15.12 | +42.16 | — |
| 2026-08-18 | `FANG` | 6 | $206.29 | $208.93 | +15.84 | — | +0.00 | +15.84 | +37.38 | — |
| 2026-08-18 | `ABX` | 138 | $9.12 | $9.03 | -12.42 | — | +0.00 | -12.42 | -12.42 | — |
| 2026-08-18 | `ALM` | 78 | $16.36 | $15.78 | -45.24 | — | +0.00 | -45.24 | -32.76 | — |
| 2026-08-18 | `DNN` | 390 | $3.19 | $3.11 | -31.20 | $3.15 | +15.60 | -15.60 | -50.70 | -35.10 |
| 2026-08-18 | `ELF` | 13 | $93.66 | $93.44 | -2.86 | — | +0.00 | -2.86 | +37.70 | — |
| 2026-08-18 | `NB` | 249 | $4.81 | $4.66 | -37.35 | — | +0.00 | -37.35 | -102.09 | — |
| 2026-08-19 | `DNN` | 390 | $3.15 | $3.19 | +15.60 | — | +0.00 | +15.60 | -19.50 | — |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `KGC` | 42 | — | $29.63 | +0.00 | $31.43 | +75.60 | +75.60 | +0.00 | +75.60 |
| 2026-08-20 | `MRVI` | 169 | — | $7.44 | +0.00 | $8.29 | +143.65 | +143.65 | +0.00 | +143.65 |
| 2026-08-20 | `CRCL` | 15 | — | $82.99 | +0.00 | $83.66 | +10.05 | +10.05 | +0.00 | +10.05 |
| 2026-08-20 | `FUTU` | 10 | — | $117.65 | +0.00 | $112.73 | -49.20 | -49.20 | +0.00 | -49.20 |
| 2026-08-20 | `IOND` | 19 | — | $65.60 | +0.00 | $68.77 | +60.23 | +60.23 | +0.00 | +60.23 |
| 2026-08-20 | `RERE` | 299 | — | $4.20 | +0.00 | $4.08 | -35.88 | -35.88 | +0.00 | -35.88 |
| 2026-08-20 | `DNA` | 168 | — | $7.45 | +0.00 | $6.96 | -82.32 | -82.32 | +0.00 | -82.32 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `KGC` | 42 | $31.43 | $32.17 | +31.08 | — | +0.00 | +31.08 | +106.68 | — |
| 2026-08-21 | `MRVI` | 169 | $8.29 | $8.28 | -1.69 | — | +0.00 | -1.69 | +141.96 | — |
| 2026-08-21 | `CRCL` | 15 | $83.66 | $87.98 | +64.80 | — | +0.00 | +64.80 | +74.85 | — |
| 2026-08-21 | `FUTU` | 10 | $112.73 | $115.18 | +24.50 | — | +0.00 | +24.50 | -24.70 | — |
| 2026-08-21 | `IOND` | 19 | $68.77 | $68.41 | -6.84 | — | +0.00 | -6.84 | +53.39 | — |
| 2026-08-21 | `RERE` | 299 | $4.08 | $4.17 | +26.91 | — | +0.00 | +26.91 | -8.97 | — |
| 2026-08-21 | `DNA` | 168 | $6.96 | $7.09 | +21.84 | — | +0.00 | +21.84 | -60.48 | — |
| 2026-08-21 | `CRSP` | 24 | — | $59.72 | +0.00 | $59.50 | -5.28 | -5.28 | +0.00 | -5.28 |
| 2026-08-21 | `HITI` | 609 | — | $2.43 | +0.00 | $2.45 | +12.18 | +12.18 | +0.00 | +12.18 |
| 2026-08-21 | `BEKE` | 82 | — | $17.93 | +0.00 | $17.75 | -15.17 | -15.17 | +0.00 | -15.17 |
| 2026-08-21 | `QDEL` | 99 | — | $14.96 | +0.00 | $14.74 | -21.78 | -21.78 | +0.00 | -21.78 |
| 2026-08-21 | `PSEC` | 644 | — | $2.30 | +0.00 | $2.33 | +19.32 | +19.32 | +0.00 | +19.32 |
| 2026-08-21 | `WOLF` | 55 | — | $26.86 | +0.00 | $25.76 | -60.50 | -60.50 | +0.00 | -60.50 |
| 2026-08-21 | `BKE` | 34 | — | $43.08 | +0.00 | $43.81 | +24.82 | +24.82 | +0.00 | +24.82 |
| 2026-08-24 | `CRSP` | 24 | $59.50 | $58.75 | -18.00 | — | +0.00 | -18.00 | -23.28 | — |
| 2026-08-24 | `HITI` | 609 | $2.45 | $2.45 | +0.00 | — | +0.00 | +0.00 | +12.18 | — |
| 2026-08-24 | `BEKE` | 82 | $17.75 | $18.05 | +25.01 | — | +0.00 | +25.01 | +9.84 | — |
| 2026-08-24 | `QDEL` | 99 | $14.74 | $14.74 | +0.00 | — | +0.00 | +0.00 | -21.78 | — |
| 2026-08-24 | `PSEC` | 644 | $2.33 | $2.34 | +6.44 | — | +0.00 | +6.44 | +25.76 | — |
| 2026-08-24 | `WOLF` | 55 | $25.76 | $25.00 | -41.80 | — | +0.00 | -41.80 | -102.30 | — |
| 2026-08-24 | `BKE` | 34 | $43.81 | $44.22 | +13.94 | — | +0.00 | +13.94 | +38.76 | — |
| 2026-08-25 | `RHI` | 29 | — | $43.76 | +0.00 | $44.90 | +33.06 | +33.06 | +0.00 | +33.06 |
| 2026-08-25 | `AMX` | 53 | — | $23.80 | +0.00 | $23.75 | -2.65 | -2.65 | +0.00 | -2.65 |
| 2026-08-25 | `INSP` | 20 | — | $61.19 | +0.00 | $61.07 | -2.40 | -2.40 | +0.00 | -2.40 |
| 2026-08-25 | `OCUL` | 116 | — | $10.98 | +0.00 | $10.88 | -11.60 | -11.60 | +0.00 | -11.60 |
| 2026-08-25 | `AMTX` | 674 | — | $1.90 | +0.00 | $1.91 | +6.74 | +6.74 | +0.00 | +6.74 |
| 2026-08-25 | `BZ` | 83 | — | $15.28 | +0.00 | $16.29 | +83.83 | +83.83 | +0.00 | +83.83 |
| 2026-08-25 | `CRMD` | 153 | — | $8.35 | +0.00 | $8.56 | +32.13 | +32.13 | +0.00 | +32.13 |
| 2026-08-25 | `ELMT` | 71 | — | $17.89 | +0.00 | $17.75 | -9.94 | -9.94 | +0.00 | -9.94 |
| 2026-08-26 | `RHI` | 29 | $44.90 | $44.33 | -16.53 | — | +0.00 | -16.53 | +16.53 | — |
| 2026-08-26 | `AMX` | 53 | $23.75 | $23.75 | +0.00 | $23.62 | -6.89 | -6.89 | -2.65 | -9.54 |
| 2026-08-26 | `INSP` | 20 | $61.07 | $60.07 | -20.00 | — | +0.00 | -20.00 | -22.40 | — |
| 2026-08-26 | `OCUL` | 116 | $10.88 | $10.79 | -10.44 | $10.77 | -2.32 | -12.76 | -22.04 | -24.36 |
| 2026-08-26 | `AMTX` | 674 | $1.91 | $1.91 | +0.00 | — | +0.00 | +0.00 | +6.74 | — |
| 2026-08-26 | `BZ` | 83 | $16.29 | $16.77 | +39.84 | — | +0.00 | +39.84 | +123.67 | — |
| 2026-08-26 | `CRMD` | 153 | $8.56 | $8.60 | +6.12 | $8.39 | -32.13 | -26.01 | +38.25 | +6.12 |
| 2026-08-26 | `ELMT` | 71 | $17.75 | $17.82 | +4.97 | — | +0.00 | +4.97 | -4.97 | — |
| 2026-08-26 | `GSM` | 326 | — | $4.00 | +0.00 | $4.07 | +22.82 | +22.82 | +0.00 | +22.82 |
| 2026-08-26 | `HEI` | 3 | — | $370.00 | +0.00 | $346.15 | -71.55 | -71.55 | +0.00 | -71.55 |
| 2026-08-26 | `TME` | 149 | — | $8.71 | +0.00 | $8.80 | +13.41 | +13.41 | +0.00 | +13.41 |
| 2026-08-26 | `SJM` | 9 | — | $134.80 | +0.00 | $130.90 | -35.10 | -35.10 | +0.00 | -35.10 |
| 2026-08-26 | `TAL` | 111 | — | $11.68 | +0.00 | $11.69 | +1.11 | +1.11 | +0.00 | +1.11 |
| 2026-08-27 | `AMX` | 53 | $23.62 | $23.77 | +7.95 | — | +0.00 | +7.95 | -1.59 | — |
| 2026-08-27 | `OCUL` | 116 | $10.77 | $10.63 | -16.24 | — | +0.00 | -16.24 | -40.60 | — |
| 2026-08-27 | `CRMD` | 153 | $8.39 | $8.49 | +15.30 | — | +0.00 | +15.30 | +21.42 | — |
| 2026-08-27 | `GSM` | 326 | $4.07 | $4.02 | -16.30 | — | +0.00 | -16.30 | +6.52 | — |
| 2026-08-27 | `HEI` | 3 | $346.15 | $346.19 | +0.12 | — | +0.00 | +0.12 | -71.43 | — |
| 2026-08-27 | `TME` | 149 | $8.80 | $8.80 | +0.00 | — | +0.00 | +0.00 | +13.41 | — |
| 2026-08-27 | `SJM` | 9 | $130.90 | $130.29 | -5.49 | — | +0.00 | -5.49 | -40.59 | — |
| 2026-08-27 | `TAL` | 111 | $11.69 | $11.62 | -7.77 | — | +0.00 | -7.77 | -6.66 | — |
| 2026-08-27 | `ACMR` | 15 | — | $81.65 | +0.00 | $80.49 | -17.40 | -17.40 | +0.00 | -17.40 |
| 2026-08-27 | `MT` | 17 | — | $74.54 | +0.00 | $74.63 | +1.53 | +1.53 | +0.00 | +1.53 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `TX` | 23 | — | $55.25 | +0.00 | $55.83 | +13.34 | +13.34 | +0.00 | +13.34 |
| 2026-08-27 | `MOS` | 53 | — | $24.00 | +0.00 | $23.76 | -12.72 | -12.72 | +0.00 | -12.72 |
| 2026-08-27 | `ANET` | 6 | — | $205.90 | +0.00 | $201.09 | -28.86 | -28.86 | +0.00 | -28.86 |
| 2026-08-27 | `CM` | 10 | — | $118.77 | +0.00 | $114.84 | -39.30 | -39.30 | +0.00 | -39.30 |
| 2026-08-28 | `ACMR` | 15 | $80.49 | $79.27 | -18.30 | — | +0.00 | -18.30 | -35.70 | — |
| 2026-08-28 | `MT` | 17 | $74.63 | $75.39 | +12.92 | — | +0.00 | +12.92 | +14.45 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `TX` | 23 | $55.83 | $55.97 | +3.22 | — | +0.00 | +3.22 | +16.56 | — |
| 2026-08-28 | `MOS` | 53 | $23.76 | $23.95 | +10.07 | — | +0.00 | +10.07 | -2.65 | — |
| 2026-08-28 | `ANET` | 6 | $201.09 | $200.00 | -6.54 | — | +0.00 | -6.54 | -35.40 | — |
| 2026-08-28 | `CM` | 10 | $114.84 | $115.66 | +8.20 | — | +0.00 | +8.20 | -31.10 | — |
| 2026-08-28 | `KEYS` | 3 | — | $324.41 | +0.00 | $319.97 | -13.32 | -13.32 | +0.00 | -13.32 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `AVT` | 13 | — | $91.49 | +0.00 | $88.63 | -37.18 | -37.18 | +0.00 | -37.18 |
| 2026-08-28 | `CGNX` | 19 | — | $62.82 | +0.00 | $60.46 | -44.84 | -44.84 | +0.00 | -44.84 |
| 2026-08-28 | `COHR` | 4 | — | $289.44 | +0.00 | $279.20 | -40.96 | -40.96 | +0.00 | -40.96 |
| 2026-08-28 | `LSCC` | 10 | — | $119.76 | +0.00 | $114.40 | -53.60 | -53.60 | +0.00 | -53.60 |
| 2026-08-28 | `MTSI` | 4 | — | $275.20 | +0.00 | $265.27 | -39.72 | -39.72 | +0.00 | -39.72 |
| 2026-08-31 | `KEYS` | 3 | $319.97 | $322.49 | +7.56 | — | +0.00 | +7.56 | -5.76 | — |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -65.94 | — |
| 2026-08-31 | `AVT` | 13 | $88.63 | $89.39 | +9.88 | — | +0.00 | +9.88 | -27.30 | — |
| 2026-08-31 | `CGNX` | 19 | $60.46 | $60.46 | +0.00 | — | +0.00 | +0.00 | -44.84 | — |
| 2026-08-31 | `COHR` | 4 | $279.20 | $280.25 | +4.20 | — | +0.00 | +4.20 | -36.76 | — |
| 2026-08-31 | `LSCC` | 10 | $114.40 | $115.56 | +11.60 | — | +0.00 | +11.60 | -42.00 | — |
| 2026-08-31 | `MTSI` | 4 | $265.27 | $266.96 | +6.76 | — | +0.00 | +6.76 | -32.96 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `HRMY` | 28 | — | $42.93 | +0.00 | $41.86 | -29.96 | -29.96 | +0.00 | -29.96 |
| 2026-09-03 | `PBH` | 22 | — | $53.45 | +0.00 | $52.56 | -19.58 | -19.58 | +0.00 | -19.58 |
| 2026-09-03 | `PCRX` | 45 | — | $26.74 | +0.00 | $26.60 | -6.30 | -6.30 | +0.00 | -6.30 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `AVGO` | 3 | — | $351.74 | +0.00 | $357.16 | +16.26 | +16.26 | +0.00 | +16.26 |
| 2026-09-03 | `PBF` | 16 | — | $74.75 | +0.00 | $75.33 | +9.28 | +9.28 | +0.00 | +9.28 |
| 2026-09-03 | `ATRC` | 23 | — | $52.88 | +0.00 | $52.46 | -9.66 | -9.66 | +0.00 | -9.66 |
| 2026-09-03 | `CABA` | 335 | — | $3.63 | +0.00 | $3.48 | -50.25 | -50.25 | +0.00 | -50.25 |
| 2026-09-04 | `HRMY` | 28 | $41.86 | $41.50 | -10.08 | $42.25 | +21.00 | +10.92 | -40.04 | -19.04 |
| 2026-09-04 | `PBH` | 22 | $52.56 | $51.80 | -16.72 | — | +0.00 | -16.72 | -36.30 | — |
| 2026-09-04 | `PCRX` | 45 | $26.60 | $26.38 | -9.90 | — | +0.00 | -9.90 | -16.20 | — |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | $130.22 | +1.71 | -3.69 | -21.78 | -20.07 |
| 2026-09-04 | `AVGO` | 3 | $357.16 | $359.70 | +7.62 | — | +0.00 | +7.62 | +23.88 | — |
| 2026-09-04 | `PBF` | 16 | $75.33 | $74.50 | -13.28 | — | +0.00 | -13.28 | -4.00 | — |
| 2026-09-04 | `ATRC` | 23 | $52.46 | $52.03 | -9.89 | $51.52 | -11.73 | -21.62 | -19.55 | -31.28 |
| 2026-09-04 | `CABA` | 335 | $3.48 | $3.46 | -6.70 | $3.47 | +3.35 | -3.35 | -56.95 | -53.60 |
| 2026-09-04 | `ALEC` | 482 | — | $2.52 | +0.00 | $2.46 | -28.92 | -28.92 | +0.00 | -28.92 |
| 2026-09-04 | `BHC` | 181 | — | $6.71 | +0.00 | $6.56 | -27.15 | -27.15 | +0.00 | -27.15 |
| 2026-09-04 | `VIR` | 107 | — | $11.31 | +0.00 | $11.38 | +8.02 | +8.02 | +0.00 | +8.02 |
| 2026-09-04 | `PIPR` | 15 | — | $76.55 | +0.00 | $77.04 | +7.35 | +7.35 | +0.00 | +7.35 |
| 2026-09-08 | `HRMY` | 28 | $42.25 | $42.20 | -1.40 | — | +0.00 | -1.40 | -20.44 | — |
| 2026-09-08 | `RVTY` | 9 | $130.22 | $128.50 | -15.48 | — | +0.00 | -15.48 | -35.55 | — |
| 2026-09-08 | `ATRC` | 23 | $51.52 | $54.31 | +64.17 | — | +0.00 | +64.17 | +32.89 | — |
| 2026-09-08 | `CABA` | 335 | $3.47 | $3.43 | -13.40 | — | +0.00 | -13.40 | -67.00 | — |
| 2026-09-08 | `ALEC` | 482 | $2.46 | $2.38 | -38.56 | — | +0.00 | -38.56 | -67.48 | — |
| 2026-09-08 | `BHC` | 181 | $6.56 | $6.57 | +1.81 | — | +0.00 | +1.81 | -25.34 | — |
| 2026-09-08 | `VIR` | 107 | $11.38 | $11.22 | -17.65 | — | +0.00 | -17.65 | -9.63 | — |
| 2026-09-08 | `PIPR` | 15 | $77.04 | $76.64 | -6.00 | — | +0.00 | -6.00 | +1.35 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `BAND` | 22 | — | $52.55 | +0.00 | $56.87 | +95.04 | +95.04 | +0.00 | +95.04 |
| 2026-09-11 | `BTI` | 21 | — | $56.03 | +0.00 | $55.24 | -16.59 | -16.59 | +0.00 | -16.59 |
| 2026-09-11 | `ASX` | 29 | — | $39.57 | +0.00 | $39.47 | -2.90 | -2.90 | +0.00 | -2.90 |
| 2026-09-11 | `SANM` | 5 | — | $206.84 | +0.00 | $216.00 | +45.80 | +45.80 | +0.00 | +45.80 |
| 2026-09-11 | `CLOV` | 248 | — | $4.75 | +0.00 | $4.82 | +17.36 | +17.36 | +0.00 | +17.36 |
| 2026-09-11 | `INGM` | 44 | — | $26.62 | +0.00 | $27.55 | +40.92 | +40.92 | +0.00 | +40.92 |
| 2026-09-11 | `PGNY` | 43 | — | $27.45 | +0.00 | $27.35 | -4.30 | -4.30 | +0.00 | -4.30 |
| 2026-09-11 | `DSGX` | 16 | — | $71.71 | +0.00 | $76.04 | +69.28 | +69.28 | +0.00 | +69.28 |
| 2026-09-14 | `BAND` | 22 | $56.87 | $56.90 | +0.66 | — | +0.00 | +0.66 | +95.70 | — |
| 2026-09-14 | `BTI` | 21 | $55.24 | $57.12 | +39.48 | — | +0.00 | +39.48 | +22.89 | — |
| 2026-09-14 | `ASX` | 29 | $39.47 | $37.41 | -59.74 | — | +0.00 | -59.74 | -62.64 | — |
| 2026-09-14 | `SANM` | 5 | $216.00 | $206.50 | -47.50 | — | +0.00 | -47.50 | -1.70 | — |
| 2026-09-14 | `CLOV` | 248 | $4.82 | $4.82 | +0.00 | — | +0.00 | +0.00 | +17.36 | — |
| 2026-09-14 | `INGM` | 44 | $27.55 | $26.89 | -29.04 | — | +0.00 | -29.04 | +11.88 | — |
| 2026-09-14 | `PGNY` | 43 | $27.35 | $27.69 | +14.62 | — | +0.00 | +14.62 | +10.32 | — |
| 2026-09-14 | `DSGX` | 16 | $76.04 | $77.68 | +26.24 | — | +0.00 | +26.24 | +95.52 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `BWIN` | 37 | — | $32.25 | +0.00 | $32.04 | -7.77 | -7.77 | +0.00 | -7.77 |
| 2026-09-16 | `AVAH` | 83 | — | $14.31 | +0.00 | $14.26 | -4.15 | -4.15 | +0.00 | -4.15 |
| 2026-09-16 | `ILMN` | 5 | — | $224.49 | +0.00 | $228.93 | +22.20 | +22.20 | +0.00 | +22.20 |
| 2026-09-16 | `SM` | 30 | — | $39.99 | +0.00 | $38.16 | -54.90 | -54.90 | +0.00 | -54.90 |
| 2026-09-16 | `AMX` | 51 | — | $23.18 | +0.00 | $22.98 | -10.20 | -10.20 | +0.00 | -10.20 |
| 2026-09-16 | `BLFS` | 32 | — | $36.46 | +0.00 | $36.11 | -11.20 | -11.20 | +0.00 | -11.20 |
| 2026-09-16 | `IQV` | 4 | — | $270.89 | +0.00 | $268.82 | -8.28 | -8.28 | +0.00 | -8.28 |
| 2026-09-16 | `NEO` | 63 | — | $18.84 | +0.00 | $18.94 | +6.30 | +6.30 | +0.00 | +6.30 |
| 2026-09-17 | `BWIN` | 37 | $32.04 | $32.06 | +0.74 | — | +0.00 | +0.74 | -7.03 | — |
| 2026-09-17 | `AVAH` | 83 | $14.26 | $14.33 | +5.81 | — | +0.00 | +5.81 | +1.66 | — |
| 2026-09-17 | `ILMN` | 5 | $228.93 | $233.85 | +24.60 | — | +0.00 | +24.60 | +46.80 | — |
| 2026-09-17 | `SM` | 30 | $38.16 | $37.57 | -17.70 | — | +0.00 | -17.70 | -72.60 | — |
| 2026-09-17 | `AMX` | 51 | $22.98 | $23.09 | +5.61 | — | +0.00 | +5.61 | -4.59 | — |
| 2026-09-17 | `BLFS` | 32 | $36.11 | $36.67 | +17.92 | — | +0.00 | +17.92 | +6.72 | — |
| 2026-09-17 | `IQV` | 4 | $268.82 | $273.15 | +17.32 | — | +0.00 | +17.32 | +9.04 | — |
| 2026-09-17 | `NEO` | 63 | $18.94 | $19.11 | +10.71 | — | +0.00 | +10.71 | +17.01 | — |
| 2026-09-17 | `BULL` | 150 | — | $7.95 | +0.00 | $7.71 | -36.00 | -36.00 | +0.00 | -36.00 |
| 2026-09-17 | `SMTC` | 7 | — | $170.85 | +0.00 | $178.19 | +51.38 | +51.38 | +0.00 | +51.38 |
| 2026-09-17 | `AXTI` | 17 | — | $67.91 | +0.00 | $67.75 | -2.72 | -2.72 | +0.00 | -2.72 |
| 2026-09-17 | `CYPH` | 447 | — | $2.67 | +0.00 | $3.07 | +176.56 | +176.56 | +0.00 | +176.56 |
| 2026-09-17 | `PGEN` | 157 | — | $7.59 | +0.00 | $7.87 | +43.96 | +43.96 | +0.00 | +43.96 |
| 2026-09-17 | `VOD` | 68 | — | $17.56 | +0.00 | $17.52 | -2.72 | -2.72 | +0.00 | -2.72 |
| 2026-09-17 | `AVTR` | 75 | — | $15.81 | +0.00 | $15.86 | +3.75 | +3.75 | +0.00 | +3.75 |
| 2026-09-17 | `LIFE` | 30 | — | $39.67 | +0.00 | $36.40 | -98.10 | -98.10 | +0.00 | -98.10 |
| 2026-09-18 | `BULL` | 150 | $7.71 | $7.85 | +21.00 | — | +0.00 | +21.00 | -15.00 | — |
| 2026-09-18 | `SMTC` | 7 | $178.19 | $182.33 | +28.98 | — | +0.00 | +28.98 | +80.36 | — |
| 2026-09-18 | `AXTI` | 17 | $67.75 | $69.72 | +33.49 | — | +0.00 | +33.49 | +30.77 | — |
| 2026-09-18 | `CYPH` | 447 | $3.07 | $3.04 | -15.64 | — | +0.00 | -15.64 | +160.92 | — |
| 2026-09-18 | `PGEN` | 157 | $7.87 | $7.98 | +17.27 | — | +0.00 | +17.27 | +61.23 | — |
| 2026-09-18 | `VOD` | 68 | $17.52 | $16.73 | -53.72 | — | +0.00 | -53.72 | -56.44 | — |
| 2026-09-18 | `AVTR` | 75 | $15.86 | $15.87 | +0.75 | — | +0.00 | +0.75 | +4.50 | — |
| 2026-09-18 | `LIFE` | 30 | $36.40 | $36.89 | +14.70 | — | +0.00 | +14.70 | -83.40 | — |
| 2026-09-18 | `CRWV` | 15 | — | $79.83 | +0.00 | $81.36 | +22.95 | +22.95 | +0.00 | +22.95 |
| 2026-09-18 | `TH` | 58 | — | $20.91 | +0.00 | $21.19 | +16.24 | +16.24 | +0.00 | +16.24 |
| 2026-09-18 | `TTAN` | 22 | — | $53.53 | +0.00 | $55.04 | +33.11 | +33.11 | +0.00 | +33.11 |
| 2026-09-18 | `LTRX` | 206 | — | $5.89 | +0.00 | $5.85 | -8.24 | -8.24 | +0.00 | -8.24 |
| 2026-09-18 | `DRVN` | 99 | — | $12.26 | +0.00 | $12.28 | +1.98 | +1.98 | +0.00 | +1.98 |
| 2026-09-18 | `QSR` | 16 | — | $73.00 | +0.00 | $72.88 | -1.92 | -1.92 | +0.00 | -1.92 |
| 2026-09-18 | `SFL` | 88 | — | $13.74 | +0.00 | $13.63 | -9.68 | -9.68 | +0.00 | -9.68 |
| 2026-09-18 | `AMD` | 2 | — | $547.37 | +0.00 | $559.82 | +24.90 | +24.90 | +0.00 | +24.90 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +71.81 | BTSG | — | $10.91 | $10,069.32 | BTSG×167 |
| 2026-08-14 | +5.50 | $10.91 | BTSG×167 | $9,972.46 | -96.86 | +70.23 | ADUR, ANGX, BETR, KULR, WDC, ALGM, AMAT, NRG | BTSG | $536.40 | $10,017.40 | ADUR×75, ANGX×289, BETR×84, KULR×498, WDC×2, ALGM×28, AMAT×2, NRG×10 |
| 2026-08-17 | +2.25 | $536.40 | ADUR×75, ANGX×289, BETR×84, KULR×498, WDC×2, ALGM×28, AMAT×2, NRG×10 | $10,132.48 | +115.08 | +54.91 | DVN, EOG, FANG, ABX, ALM, DNN, ELF, NB | ADUR, ANGX, BETR, KULR, WDC, ALGM, AMAT, NRG | $258.08 | $10,143.42 | DVN×27, EOG×8, FANG×6, ABX×138, ALM×78, DNN×390, ELF×13, NB×249 |
| 2026-08-18 | -6.20 | $258.08 | DVN×27, EOG×8, FANG×6, ABX×138, ALM×78, DNN×390, ELF×13, NB×249 | $10,056.92 | -86.50 | +15.60 | — | DVN, EOG, FANG, ABX, ALM, ELF, NB | $8,827.87 | $10,056.37 | DNN×390 |
| 2026-08-19 | -7.20 | $8,827.87 | DNN×390 | $10,071.97 | +15.60 | +0.00 | — | DNN | $10,066.87 | $10,066.87 | — |
| 2026-08-20 | +1.12 | $10,066.87 | — | $10,066.87 | -0.00 | +156.19 | BHP, KGC, MRVI, CRCL, FUTU, IOND, RERE, DNA | — | $187.67 | $10,203.96 | BHP×13, KGC×42, MRVI×169, CRCL×15, FUTU×10, IOND×19, RERE×299, DNA×168 |
| 2026-08-21 | +3.25 | $187.67 | BHP×13, KGC×42, MRVI×169, CRCL×15, FUTU×10, IOND×19, RERE×299, DNA×168 | $10,391.73 | +187.77 | -46.41 | CRSP, HITI, BEKE, QDEL, PSEC, WOLF, BKE | BHP, KGC, MRVI, CRCL, FUTU, IOND, RERE, DNA | $57.32 | $10,298.99 | CRSP×24, HITI×609, BEKE×82, QDEL×99, PSEC×644, WOLF×55, BKE×34 |
| 2026-08-24 | -5.17 | $57.32 | CRSP×24, HITI×609, BEKE×82, QDEL×99, PSEC×644, WOLF×55, BKE×34 | $10,284.58 | -14.41 | +0.00 | — | CRSP, HITI, BEKE, QDEL, PSEC, WOLF, BKE | $10,257.24 | $10,257.24 | — |
| 2026-08-25 | +1.80 | $10,257.24 | — | $10,257.24 | -0.00 | +129.17 | RHI, AMX, INSP, OCUL, AMTX, BZ, CRMD, ELMT | — | $108.54 | $10,362.21 | RHI×29, AMX×53, INSP×20, OCUL×116, AMTX×674, BZ×83, CRMD×153, ELMT×71 |
| 2026-08-26 | +2.02 | $108.54 | RHI×29, AMX×53, INSP×20, OCUL×116, AMTX×674, BZ×83, CRMD×153, ELMT×71 | $10,366.17 | +3.96 | -110.65 | GSM, HEI, TME, SJM, TAL | RHI, INSP, AMTX, BZ, ELMT | $288.05 | $10,225.06 | AMX×53, OCUL×116, CRMD×153, GSM×326, HEI×3, TME×149, SJM×9, TAL×111 |
| 2026-08-27 | — | $288.05 | AMX×53, OCUL×116, CRMD×153, GSM×326, HEI×3, TME×149, SJM×9, TAL×111 | $10,202.63 | -22.43 | -115.03 | ACMR, MT, MU, TX, MOS, ANET, CM | AMX, OCUL, CRMD, GSM, HEI, TME, SJM, TAL | $1,743.37 | $10,053.13 | ACMR×15, MT×17, MU×1, TX×23, MOS×53, ANET×6, CM×10 |
| 2026-08-28 | +0.75 | $1,743.37 | ACMR×15, MT×17, MU×1, TX×23, MOS×53, ANET×6, CM×10 | $10,046.60 | -6.53 | -295.56 | KEYS, CIEN, AVT, CGNX, COHR, LSCC, MTSI | ACMR, MT, MU, TX, MOS, ANET, CM | $2,004.45 | $9,722.49 | KEYS×3, CIEN×3, AVT×13, CGNX×19, COHR×4, LSCC×10, MTSI×4 |
| 2026-08-31 | -5.85 | $2,004.45 | KEYS×3, CIEN×3, AVT×13, CGNX×19, COHR×4, LSCC×10, MTSI×4 | $9,762.49 | +40.00 | +0.00 | — | KEYS, CIEN, AVT, CGNX, COHR, LSCC, MTSI | $9,748.25 | $9,748.25 | — |
| 2026-09-01 | -6.30 | $9,748.25 | — | $9,748.25 | +0.00 | +0.00 | — | — | $9,748.25 | $9,748.25 | — |
| 2026-09-02 | -3.83 | $9,748.25 | — | $9,748.25 | +0.00 | +0.00 | — | — | $9,748.25 | $9,748.25 | — |
| 2026-09-03 | -0.90 | $9,748.25 | — | $9,748.25 | +0.00 | -106.59 | HRMY, PBH, PCRX, RVTY, AVGO, PBF, ATRC, CABA | — | $272.77 | $9,622.98 | HRMY×28, PBH×22, PCRX×45, RVTY×9, AVGO×3, PBF×16, ATRC×23, CABA×335 |
| 2026-09-04 | +2.25 | $272.77 | HRMY×28, PBH×22, PCRX×45, RVTY×9, AVGO×3, PBF×16, ATRC×23, CABA×335 | $9,558.63 | -64.35 | -26.37 | ALEC, BHC, VIR, PIPR | PBH, PCRX, AVGO, PBF | $61.60 | $9,510.87 | HRMY×28, RVTY×9, ATRC×23, CABA×335, ALEC×482, BHC×181, VIR×107, PIPR×15 |
| 2026-09-08 | -11.47 | $61.60 | HRMY×28, RVTY×9, ATRC×23, CABA×335, ALEC×482, BHC×181, VIR×107, PIPR×15 | $9,484.35 | -26.52 | +0.00 | — | HRMY, RVTY, ATRC, CABA, ALEC, BHC, VIR, PIPR | $9,460.48 | $9,460.48 | — |
| 2026-09-09 | -13.95 | $9,460.48 | — | $9,460.48 | -0.00 | +0.00 | — | — | $9,460.48 | $9,460.48 | — |
| 2026-09-10 | -13.28 | $9,460.48 | — | $9,460.48 | -0.00 | +0.00 | — | — | $9,460.48 | $9,460.48 | — |
| 2026-09-11 | +0.50 | $9,460.48 | — | $9,460.48 | -0.00 | +244.61 | BAND, BTI, ASX, SANM, CLOV, INGM, PGNY, DSGX | — | $251.36 | $9,687.42 | BAND×22, BTI×21, ASX×29, SANM×5, CLOV×248, INGM×44, PGNY×43, DSGX×16 |
| 2026-09-14 | -11.00 | $251.36 | BAND×22, BTI×21, ASX×29, SANM×5, CLOV×248, INGM×44, PGNY×43, DSGX×16 | $9,632.14 | -55.28 | +0.00 | — | BAND, BTI, ASX, SANM, CLOV, INGM, PGNY, DSGX | $9,614.28 | $9,614.28 | — |
| 2026-09-15 | -3.84 | $9,614.28 | — | $9,614.28 | -0.00 | +0.00 | — | — | $9,614.28 | $9,614.28 | — |
| 2026-09-16 | +5.30 | $9,614.28 | — | $9,614.28 | -0.00 | -68.00 | BWIN, AVAH, ILMN, SM, AMX, BLFS, IQV, NEO | — | $274.93 | $9,529.44 | BWIN×37, AVAH×83, ILMN×5, SM×30, AMX×51, BLFS×32, IQV×4, NEO×63 |
| 2026-09-17 | +7.38 | $274.93 | BWIN×37, AVAH×83, ILMN×5, SM×30, AMX×51, BLFS×32, IQV×4, NEO×63 | $9,594.45 | +65.01 | +136.11 | BULL, SMTC, AXTI, CYPH, PGEN, VOD, AVTR, LIFE | BWIN, AVAH, ILMN, SM, AMX, BLFS, IQV, NEO | $56.04 | $9,692.36 | BULL×150, SMTC×7, AXTI×17, CYPH×447, PGEN×157, VOD×68, AVTR×75, LIFE×30 |
| 2026-09-18 | +4.86 | $56.04 | BULL×150, SMTC×7, AXTI×17, CYPH×447, PGEN×157, VOD×68, AVTR×75, LIFE×30 | $9,739.19 | +46.83 | +79.34 | CRWV, TH, TTAN, LTRX, DRVN, QSR, SFL, AMD | BULL, SMTC, AXTI, CYPH, PGEN, VOD, AVTR, LIFE | $213.29 | $9,779.57 | CRWV×15, TH×58, TTAN×22, LTRX×206, DRVN×99, QSR×16, SFL×88, AMD×2 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 167 | $59.80 | $2.49 | — | $10.91 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; ⚪; ret5=-5.3; leftover $10000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.91 | ▲ close $10,069.32 vs 09:30 $10,000.00 (session +71.81) | 16:00 close · cash $10.91 · equity $10,069.32 vs 09:30 $10,000.00 (+69.32; session marks +71.81) · 1 name(s) marked open→close (per-name table). BTSG×167 09:30 $59.80 → close $60.23 +71.81 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.91 | ▼ 09:30 equity $9,972.46 vs yday $10,069.32 (-96.86) | 09:30 open · cash $10.91 (unchanged overnight, no fees) · equity $9,972.46 vs prior close $10,069.32 (-96.86) · 1 name(s) re-marked at the open (per-name table). BTSG×167 yday $60.23 → 09:30 $59.65 -96.86 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 167 | $59.65 | $2.60 | $-30.14 | $9,969.86 | ▼ -30.14 after sell → book $9,969.86; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $8,730.15 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1246.23 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 289 | $4.31 | $3.73 | — | $7,480.83 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1246.23 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $6,235.39 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1246.23 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `KULR` | 498 | $2.50 | $6.42 | — | $4,983.96 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ⚪; ret5=+7.6; leftover $1246.23 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $3,974.97 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable; 🔵; ⚪; ret5=+7.9; leftover $1246.23 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 28 | $44.06 | $2.07 | — | $2,739.21 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable; 🔵; ret5=+3.9; leftover $1246.23 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AMAT` | 2 | $499.40 | $2.00 | — | $1,738.42 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ret5=+1.3; leftover $1246.23 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $536.40 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ret5=+0.6; leftover $1246.23 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $536.40 | ▲ close $10,017.40 vs 09:30 $9,972.46 (session +70.23) | 16:00 close · cash $536.40 · equity $10,017.40 vs 09:30 $9,972.46 (+44.94; session marks +70.23) · 8 name(s) marked open→close (per-name table). ADUR×75 09:30 $16.50 → close $16.17 -24.75; ANGX×289 09:30 $4.31 → close $4.37 +17.34; BETR×84 09:30 $14.80 → close $13.73 -89.88; KULR×498 09:30 $2.50 → close $2.64 +69.72; WDC×2 09:30 $503.50 → close $508.80 +10.60; ALGM×28 09:30 $44.06 → close $44.39 +9.24; AMAT×2 09:30 $499.40 → close $507.18 +15.56; NRG×10 09:30 $120.00 → close $126.24 +62.40 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $536.40 | ▲ 09:30 equity $10,132.48 vs yday $10,017.40 (+115.08) | 09:30 open · cash $536.40 (unchanged overnight, no fees) · equity $10,132.48 vs prior close $10,017.40 (+115.08) · 8 name(s) re-marked at the open (per-name table). ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; ANGX×289 yday $4.37 → 09:30 $4.60 +66.47; BETR×84 yday $13.73 → 09:30 $13.67 -5.04; KULR×498 yday $2.64 → 09:30 $2.63 -4.98; WDC×2 yday $508.80 → 09:30 $525.53 +33.46; ALGM×28 yday $44.39 → 09:30 $45.32 +26.04; AMAT×2 yday $507.18 → 09:30 $517.45 +20.53; NRG×10 yday $126.24 → 09:30 $127.40 +11.60 | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $1,713.91 | ▼ -62.20 after sell → book $10,130.24; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 289 | $4.60 | $3.79 | $+76.30 | $3,039.52 | ▲ +76.30 after sell → book $10,126.45; vs 09:30 mark -3.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 84 | $13.67 | $2.27 | $-99.43 | $4,185.54 | ▼ -99.43 after sell → book $10,124.19; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `KULR` | 498 | $2.63 | $6.52 | $+51.80 | $5,488.76 | ▲ +51.80 after sell → book $10,117.67; vs 09:30 mark -6.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $6,537.80 | ▲ +40.05 after sell → book $10,115.66; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ALGM` | 28 | $45.32 | $2.09 | $+31.11 | $7,804.67 | ▲ +31.11 after sell → book $10,113.56; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMAT` | 2 | $517.45 | $2.02 | $+32.08 | $8,837.55 | ▲ +32.08 after sell → book $10,111.55; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $10,109.50 | ▲ +69.94 after sell → book $10,109.50; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,860.57 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ret5=+6.7; leftover $1263.69 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,716.40 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ret5=+5.8; leftover $1263.69 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,498.19 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ret5=+8.3; leftover $1263.69 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 138 | $9.12 | $2.40 | — | $5,237.23 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1263.69 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 78 | $16.20 | $2.22 | — | $3,971.40 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1263.69 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 390 | $3.24 | $5.03 | — | $2,702.77 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; ⚪; ret5=+0.3; leftover $1263.69 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 13 | $90.54 | $2.03 | — | $1,523.72 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; ret5=-7.2; leftover $1263.69 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 249 | $5.07 | $3.21 | — | $258.08 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; ret5=-4.7; leftover $1263.69 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $258.08 | ▲ close $10,143.42 vs 09:30 $10,132.48 (session +54.91) | 16:00 close · cash $258.08 · equity $10,143.42 vs 09:30 $10,132.48 (+10.94; session marks +54.91) · 8 name(s) marked open→close (per-name table). DVN×27 09:30 $46.18 → close $47.57 +37.53; EOG×8 09:30 $142.77 → close $146.15 +27.04; FANG×6 09:30 $202.70 → close $206.29 +21.54; ABX×138 09:30 $9.12 → close $9.12 +0.00; ALM×78 09:30 $16.20 → close $16.36 +12.48; DNN×390 09:30 $3.24 → close $3.19 -19.50; ELF×13 09:30 $90.54 → close $93.66 +40.56; NB×249 09:30 $5.07 → close $4.81 -64.74 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $258.08 | ▼ 09:30 equity $10,056.92 vs yday $10,143.42 (-86.50) | 09:30 open · cash $258.08 (unchanged overnight, no fees) · equity $10,056.92 vs prior close $10,143.42 (-86.50) · 8 name(s) re-marked at the open (per-name table). DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; ABX×138 yday $9.12 → 09:30 $9.03 -12.42; ALM×78 yday $16.36 → 09:30 $15.78 -45.24; DNN×390 yday $3.19 → 09:30 $3.11 -31.20; ELF×13 yday $93.66 → 09:30 $93.44 -2.86; NB×249 yday $4.81 → 09:30 $4.66 -37.35 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,551.99 | ▲ +44.98 after sell → book $10,054.83; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,734.28 | ▲ +38.11 after sell → book $10,052.80; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,985.83 | ▲ +33.34 after sell → book $10,050.77; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 138 | $9.03 | $2.44 | $-17.26 | $5,229.53 | ▼ -17.26 after sell → book $10,048.33; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 78 | $15.78 | $2.25 | $-37.23 | $6,458.12 | ▼ -37.23 after sell → book $10,046.08; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 13 | $93.44 | $2.05 | $+33.62 | $7,670.80 | ▲ +33.62 after sell → book $10,044.04; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NB` | 249 | $4.66 | $3.26 | $-108.57 | $8,827.87 | ▼ -108.57 after sell → book $10,040.77; vs 09:30 mark -3.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,827.87 | ▲ close $10,056.37 vs 09:30 $10,056.92 (session +15.60) | 16:00 close · cash $8,827.87 · equity $10,056.37 vs 09:30 $10,056.92 (-0.55; session marks +15.60) · 1 name(s) marked open→close (per-name table). DNN×390 09:30 $3.11 → close $3.15 +15.60 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,827.87 | ▲ 09:30 equity $10,071.97 vs yday $10,056.37 (+15.60) | 09:30 open · cash $8,827.87 (unchanged overnight, no fees) · equity $10,071.97 vs prior close $10,056.37 (+15.60) · 1 name(s) re-marked at the open (per-name table). DNN×390 yday $3.15 → 09:30 $3.19 +15.60 | — |
| 2026-08-19 09:30 ET | **SELL** | `DNN` | 390 | $3.19 | $5.11 | $-29.64 | $10,066.87 | ▼ -29.64 after sell → book $10,066.87; vs 09:30 mark -5.10 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,066.87 | ▲ close $10,066.87 vs 09:30 $10,071.97 (session +0.00) | 16:00 close · cash $10,066.87 · no lots left · equity $10,066.87. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,066.87 | ▲ 09:30 equity $10,066.87 vs yday $10,066.87 (-0.00) | 09:30 open · cash $10,066.87 · no holdings · equity $10,066.87 vs prior close $10,066.87 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,881.71 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1258.36 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $7,635.13 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1258.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 169 | $7.44 | $2.50 | — | $6,375.27 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=-8.5; leftover $1258.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRCL` | 15 | $82.99 | $2.04 | — | $5,128.39 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable; 🔵; ⚪; ret5=+7.4; leftover $1258.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 10 | $117.65 | $2.02 | — | $3,949.87 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ret5=+4.1; leftover $1258.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IOND` | 19 | $65.60 | $2.05 | — | $2,701.42 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ⚪; ret5=+3.7; leftover $1258.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `RERE` | 299 | $4.20 | $3.86 | — | $1,441.77 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ret5=+2.9; leftover $1258.36 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 168 | $7.45 | $2.49 | — | $187.67 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1258.36 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $187.67 | ▲ close $10,203.96 vs 09:30 $10,066.87 (session +156.19) | 16:00 close · cash $187.67 · equity $10,203.96 vs 09:30 $10,066.87 (+137.09; session marks +156.19) · 8 name(s) marked open→close (per-name table). BHP×13 09:30 $91.01 → close $93.63 +34.06; KGC×42 09:30 $29.63 → close $31.43 +75.60; MRVI×169 09:30 $7.44 → close $8.29 +143.65; CRCL×15 09:30 $82.99 → close $83.66 +10.05; FUTU×10 09:30 $117.65 → close $112.73 -49.20; IOND×19 09:30 $65.60 → close $68.77 +60.23; RERE×299 09:30 $4.20 → close $4.08 -35.88; DNA×168 09:30 $7.45 → close $6.96 -82.32 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $187.67 | ▲ 09:30 equity $10,391.73 vs yday $10,203.96 (+187.77) | 09:30 open · cash $187.67 (unchanged overnight, no fees) · equity $10,391.73 vs prior close $10,203.96 (+187.77) · 8 name(s) re-marked at the open (per-name table). BHP×13 yday $93.63 → 09:30 $95.72 +27.17; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; MRVI×169 yday $8.29 → 09:30 $8.28 -1.69; CRCL×15 yday $83.66 → 09:30 $87.98 +64.80; FUTU×10 yday $112.73 → 09:30 $115.18 +24.50; IOND×19 yday $68.77 → 09:30 $68.41 -6.84; RERE×299 yday $4.08 → 09:30 $4.17 +26.91; DNA×168 yday $6.96 → 09:30 $7.09 +21.84 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,429.98 | ▲ +57.15 after sell → book $10,389.68; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $2,778.99 | ▲ +102.43 after sell → book $10,387.55; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `MRVI` | 169 | $8.28 | $2.54 | $+136.93 | $4,175.77 | ▲ +136.93 after sell → book $10,385.01; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `CRCL` | 15 | $87.98 | $2.06 | $+70.76 | $5,493.41 | ▲ +70.76 after sell → book $10,382.95; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `FUTU` | 10 | $115.18 | $2.04 | $-28.76 | $6,643.17 | ▼ -28.76 after sell → book $10,380.91; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `IOND` | 19 | $68.41 | $2.07 | $+49.28 | $7,940.90 | ▲ +49.28 after sell → book $10,378.85; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `RERE` | 299 | $4.17 | $3.92 | $-16.74 | $9,183.81 | ▼ -16.74 after sell → book $10,374.93; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 168 | $7.09 | $2.53 | $-65.51 | $10,372.40 | ▼ -65.51 after sell → book $10,372.40; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 24 | $59.72 | $2.06 | — | $8,937.06 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1481.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 609 | $2.43 | $7.86 | — | $7,449.33 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $1481.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 82 | $17.93 | $2.24 | — | $5,976.42 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $1481.77 | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 99 | $14.96 | $2.29 | — | $4,493.10 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer; ret5=-1.6; leftover $1481.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 644 | $2.30 | $8.31 | — | $3,003.59 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; ret5=-3.0; leftover $1481.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `WOLF` | 55 | $26.86 | $2.15 | — | $1,524.13 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list yday_mover; 🔵; ret5=-16.4; leftover $1481.77 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 34 | $43.08 | $2.09 | — | $57.32 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; ret5=-4.9; leftover $1481.77 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.32 | ▼ close $10,298.99 vs 09:30 $10,391.73 (session -46.41) | 16:00 close · cash $57.32 · equity $10,298.99 vs 09:30 $10,391.73 (-92.74; session marks -46.41) · 7 name(s) marked open→close (per-name table). CRSP×24 09:30 $59.72 → close $59.50 -5.28; HITI×609 09:30 $2.43 → close $2.45 +12.18; BEKE×82 09:30 $17.93 → close $17.75 -15.17; QDEL×99 09:30 $14.96 → close $14.74 -21.78; PSEC×644 09:30 $2.30 → close $2.33 +19.32; WOLF×55 09:30 $26.86 → close $25.76 -60.50; BKE×34 09:30 $43.08 → close $43.81 +24.82 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.32 | ▼ 09:30 equity $10,284.58 vs yday $10,298.99 (-14.41) | 09:30 open · cash $57.32 (unchanged overnight, no fees) · equity $10,284.58 vs prior close $10,298.99 (-14.41) · 7 name(s) re-marked at the open (per-name table). CRSP×24 yday $59.50 → 09:30 $58.75 -18.00; HITI×609 yday $2.45 → 09:30 $2.45 +0.00; BEKE×82 yday $17.75 → 09:30 $18.05 +25.01; QDEL×99 yday $14.74 → 09:30 $14.74 +0.00; PSEC×644 yday $2.33 → 09:30 $2.34 +6.44; WOLF×55 yday $25.76 → 09:30 $25.00 -41.80; BKE×34 yday $43.81 → 09:30 $44.22 +13.94 | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 24 | $58.75 | $2.08 | $-27.43 | $1,465.24 | ▼ -27.43 after sell → book $10,282.50; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `HITI` | 609 | $2.45 | $7.97 | $-3.65 | $2,949.32 | ▼ -3.65 after sell → book $10,274.53; vs 09:30 mark -7.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 82 | $18.05 | $2.26 | $+5.34 | $4,427.57 | ▲ +5.34 after sell → book $10,272.27; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 99 | $14.74 | $2.32 | $-26.38 | $5,884.51 | ▼ -26.38 after sell → book $10,269.95; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 644 | $2.34 | $8.43 | $+9.03 | $7,383.05 | ▲ +9.03 after sell → book $10,261.53; vs 09:30 mark -8.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WOLF` | 55 | $25.00 | $2.18 | $-106.63 | $8,755.87 | ▼ -106.63 after sell → book $10,259.35; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 34 | $44.22 | $2.11 | $+34.55 | $10,257.24 | ▲ +34.55 after sell → book $10,257.24; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,257.24 | ▲ close $10,257.24 vs 09:30 $10,284.58 (session +0.00) | 16:00 close · cash $10,257.24 · no lots left · equity $10,257.24. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,257.24 | ▲ 09:30 equity $10,257.24 vs yday $10,257.24 (-0.00) | 09:30 open · cash $10,257.24 · no holdings · equity $10,257.24 vs prior close $10,257.24 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 29 | $43.76 | $2.08 | — | $8,986.12 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $1282.15 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMX` | 53 | $23.80 | $2.15 | — | $7,722.57 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list oppset; 🔵; ret5=+0.5; leftover $1282.15 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.19 | $2.05 | — | $6,496.72 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ret5=+7.4; leftover $1282.15 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 116 | $10.98 | $2.34 | — | $5,220.70 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ret5=+1.2; leftover $1282.15 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 674 | $1.90 | $8.69 | — | $3,931.41 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list yday_mover; ⚪; ret5=+5.0; leftover $1282.15 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 83 | $15.28 | $2.24 | — | $2,660.93 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ⚪; ret5=-0.7; leftover $1282.15 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 153 | $8.35 | $2.45 | — | $1,380.93 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1282.15 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ELMT` | 71 | $17.89 | $2.20 | — | $108.54 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list yday_mover; ⚪; ret5=-7.5; leftover $1282.15 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $108.54 | ▲ close $10,362.21 vs 09:30 $10,257.24 (session +129.17) | 16:00 close · cash $108.54 · equity $10,362.21 vs 09:30 $10,257.24 (+104.97; session marks +129.17) · 8 name(s) marked open→close (per-name table). RHI×29 09:30 $43.76 → close $44.90 +33.06; AMX×53 09:30 $23.80 → close $23.75 -2.65; INSP×20 09:30 $61.19 → close $61.07 -2.40; OCUL×116 09:30 $10.98 → close $10.88 -11.60; AMTX×674 09:30 $1.90 → close $1.91 +6.74; BZ×83 09:30 $15.28 → close $16.29 +83.83; CRMD×153 09:30 $8.35 → close $8.56 +32.13; ELMT×71 09:30 $17.89 → close $17.75 -9.94 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $108.54 | ▲ 09:30 equity $10,366.17 vs yday $10,362.21 (+3.96) | 09:30 open · cash $108.54 (unchanged overnight, no fees) · equity $10,366.17 vs prior close $10,362.21 (+3.96) · 8 name(s) re-marked at the open (per-name table). RHI×29 yday $44.90 → 09:30 $44.33 -16.53; AMX×53 yday $23.75 → 09:30 $23.75 +0.00; INSP×20 yday $61.07 → 09:30 $60.07 -20.00; OCUL×116 yday $10.88 → 09:30 $10.79 -10.44; AMTX×674 yday $1.91 → 09:30 $1.91 +0.00; BZ×83 yday $16.29 → 09:30 $16.77 +39.84; CRMD×153 yday $8.56 → 09:30 $8.60 +6.12; ELMT×71 yday $17.75 → 09:30 $17.82 +4.97 | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 29 | $44.33 | $2.10 | $+12.36 | $1,392.01 | ▲ +12.36 after sell → book $10,364.07; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `INSP` | 20 | $60.07 | $2.07 | $-26.52 | $2,591.34 | ▼ -26.52 after sell → book $10,362.00; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `AMTX` | 674 | $1.91 | $8.82 | $-10.77 | $3,869.86 | ▼ -10.77 after sell → book $10,353.18; vs 09:30 mark -8.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BZ` | 83 | $16.77 | $2.26 | $+119.17 | $5,259.51 | ▲ +119.17 after sell → book $10,350.92; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `ELMT` | 71 | $17.82 | $2.22 | $-9.40 | $6,522.50 | ▼ -9.40 after sell → book $10,348.69; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `GSM` | 326 | $4.00 | $4.21 | — | $5,214.30 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list yday_mover; 🔵; ret5=-3.6; leftover $1304.50 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `HEI` | 3 | $370.00 | $2.00 | — | $4,102.30 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; ret5=-4.6; leftover $1304.50 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TME` | 149 | $8.71 | $2.44 | — | $2,802.07 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list oppset; 🔵; ret5=-0.6; leftover $1304.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SJM` | 9 | $134.80 | $2.02 | — | $1,586.86 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ret5=+5.9; leftover $1304.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TAL` | 111 | $11.68 | $2.32 | — | $288.05 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list oppset; 🔵; ret5=-3.7; leftover $1304.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $288.05 | ▼ close $10,225.06 vs 09:30 $10,366.17 (session -110.65) | 16:00 close · cash $288.05 · equity $10,225.06 vs 09:30 $10,366.17 (-141.11; session marks -110.65) · 8 name(s) marked open→close (per-name table). AMX×53 09:30 $23.75 → close $23.62 -6.89; OCUL×116 09:30 $10.79 → close $10.77 -2.32; CRMD×153 09:30 $8.60 → close $8.39 -32.13; GSM×326 09:30 $4.00 → close $4.07 +22.82; HEI×3 09:30 $370.00 → close $346.15 -71.55; TME×149 09:30 $8.71 → close $8.80 +13.41; SJM×9 09:30 $134.80 → close $130.90 -35.10; TAL×111 09:30 $11.68 → close $11.69 +1.11 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $288.05 | ▼ 09:30 equity $10,202.63 vs yday $10,225.06 (-22.43) | 09:30 open · cash $288.05 (unchanged overnight, no fees) · equity $10,202.63 vs prior close $10,225.06 (-22.43) · 8 name(s) re-marked at the open (per-name table). AMX×53 yday $23.62 → 09:30 $23.77 +7.95; OCUL×116 yday $10.77 → 09:30 $10.63 -16.24; CRMD×153 yday $8.39 → 09:30 $8.49 +15.30; GSM×326 yday $4.07 → 09:30 $4.02 -16.30; HEI×3 yday $346.15 → 09:30 $346.19 +0.12; TME×149 yday $8.80 → 09:30 $8.80 +0.00; SJM×9 yday $130.90 → 09:30 $130.29 -5.49; TAL×111 yday $11.69 → 09:30 $11.62 -7.77 | — |
| 2026-08-27 09:30 ET | **SELL** | `AMX` | 53 | $23.77 | $2.17 | $-5.91 | $1,545.69 | ▼ -5.91 after sell → book $10,200.46; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 116 | $10.63 | $2.37 | $-45.31 | $2,776.41 | ▼ -45.31 after sell → book $10,198.10; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 153 | $8.49 | $2.48 | $+16.49 | $4,072.89 | ▲ +16.49 after sell → book $10,195.61; vs 09:30 mark -2.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GSM` | 326 | $4.02 | $4.27 | $-1.96 | $5,379.14 | ▼ -1.96 after sell → book $10,191.34; vs 09:30 mark -4.27 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `HEI` | 3 | $346.19 | $2.02 | $-75.45 | $6,415.69 | ▼ -75.45 after sell → book $10,189.32; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `TME` | 149 | $8.80 | $2.47 | $+8.50 | $7,724.42 | ▲ +8.50 after sell → book $10,186.85; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SJM` | 9 | $130.29 | $2.04 | $-44.64 | $8,894.99 | ▼ -44.64 after sell → book $10,184.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `TAL` | 111 | $11.62 | $2.35 | $-11.33 | $10,182.46 | ▼ -11.33 after sell → book $10,182.46; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 15 | $81.65 | $2.04 | — | $8,955.68 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1272.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 17 | $74.54 | $2.04 | — | $7,686.46 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ret5=-0.1; leftover $1272.81 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $6,717.45 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1272.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 23 | $55.25 | $2.06 | — | $5,444.64 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ret5=+2.1; leftover $1272.81 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 53 | $24.00 | $2.15 | — | $4,170.49 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; ret5=+8.7; leftover $1272.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $2,933.09 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ret5=+8.5; leftover $1272.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 10 | $118.77 | $2.02 | — | $1,743.37 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; ret5=+0.3; leftover $1272.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,743.37 | ▼ close $10,053.13 vs 09:30 $10,202.63 (session -115.03) | 16:00 close · cash $1,743.37 · equity $10,053.13 vs 09:30 $10,202.63 (-149.50; session marks -115.03) · 7 name(s) marked open→close (per-name table). ACMR×15 09:30 $81.65 → close $80.49 -17.40; MT×17 09:30 $74.54 → close $74.63 +1.53; MU×1 09:30 $967.01 → close $935.39 -31.62; TX×23 09:30 $55.25 → close $55.83 +13.34; MOS×53 09:30 $24.00 → close $23.76 -12.72; ANET×6 09:30 $205.90 → close $201.09 -28.86; CM×10 09:30 $118.77 → close $114.84 -39.30 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,743.37 | ▼ 09:30 equity $10,046.60 vs yday $10,053.13 (-6.53) | 09:30 open · cash $1,743.37 (unchanged overnight, no fees) · equity $10,046.60 vs prior close $10,053.13 (-6.53) · 7 name(s) re-marked at the open (per-name table). ACMR×15 yday $80.49 → 09:30 $79.27 -18.30; MT×17 yday $74.63 → 09:30 $75.39 +12.92; MU×1 yday $935.39 → 09:30 $919.29 -16.10; TX×23 yday $55.83 → 09:30 $55.97 +3.22; MOS×53 yday $23.76 → 09:30 $23.95 +10.07; ANET×6 yday $201.09 → 09:30 $200.00 -6.54; CM×10 yday $114.84 → 09:30 $115.66 +8.20 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 15 | $79.27 | $2.06 | $-39.79 | $2,930.36 | ▼ -39.79 after sell → book $10,044.54; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 17 | $75.39 | $2.06 | $+10.35 | $4,209.93 | ▲ +10.35 after sell → book $10,042.48; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $5,127.21 | ▼ -51.73 after sell → book $10,040.47; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 23 | $55.97 | $2.08 | $+12.42 | $6,412.44 | ▲ +12.42 after sell → book $10,038.39; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 53 | $23.95 | $2.17 | $-6.97 | $7,679.62 | ▼ -6.97 after sell → book $10,036.22; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $200.00 | $2.03 | $-39.44 | $8,877.59 | ▼ -39.44 after sell → book $10,034.19; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 10 | $115.66 | $2.04 | $-35.16 | $10,032.15 | ▼ -35.16 after sell → book $10,032.15; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $9,056.92 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1254.02 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $7,853.66 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1254.02 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 13 | $91.49 | $2.03 | — | $6,662.26 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1254.02 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 19 | $62.82 | $2.05 | — | $5,466.64 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1254.02 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $4,306.87 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1254.02 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 10 | $119.76 | $2.02 | — | $3,107.25 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1254.02 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MTSI` | 4 | $275.20 | $2.00 | — | $2,004.45 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+4.1; leftover $1254.02 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,004.45 | ▼ close $9,722.49 vs 09:30 $10,046.60 (session -295.56) | 16:00 close · cash $2,004.45 · equity $9,722.49 vs 09:30 $10,046.60 (-324.11; session marks -295.56) · 7 name(s) marked open→close (per-name table). KEYS×3 09:30 $324.41 → close $319.97 -13.32; CIEN×3 09:30 $400.42 → close $378.44 -65.94; AVT×13 09:30 $91.49 → close $88.63 -37.18; CGNX×19 09:30 $62.82 → close $60.46 -44.84; COHR×4 09:30 $289.44 → close $279.20 -40.96; LSCC×10 09:30 $119.76 → close $114.40 -53.60; MTSI×4 09:30 $275.20 → close $265.27 -39.72 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,004.45 | ▲ 09:30 equity $9,762.49 vs yday $9,722.49 (+40.00) | 09:30 open · cash $2,004.45 (unchanged overnight, no fees) · equity $9,762.49 vs prior close $9,722.49 (+40.00) · 7 name(s) re-marked at the open (per-name table). KEYS×3 yday $319.97 → 09:30 $322.49 +7.56; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; AVT×13 yday $88.63 → 09:30 $89.39 +9.88; CGNX×19 yday $60.46 → 09:30 $60.46 +0.00; COHR×4 yday $279.20 → 09:30 $280.25 +4.20; LSCC×10 yday $114.40 → 09:30 $115.56 +11.60; MTSI×4 yday $265.27 → 09:30 $266.96 +6.76 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 3 | $322.49 | $2.02 | $-9.78 | $2,969.90 | ▼ -9.78 after sell → book $9,760.47; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $4,103.20 | ▼ -69.96 after sell → book $9,758.45; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 13 | $89.39 | $2.05 | $-31.38 | $5,263.23 | ▼ -31.38 after sell → book $9,756.41; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 19 | $60.46 | $2.07 | $-48.95 | $6,409.90 | ▼ -48.95 after sell → book $9,754.34; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $280.25 | $2.02 | $-40.78 | $7,528.88 | ▼ -40.78 after sell → book $9,752.32; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 10 | $115.56 | $2.04 | $-46.06 | $8,682.44 | ▼ -46.06 after sell → book $9,750.28; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MTSI` | 4 | $266.96 | $2.02 | $-36.98 | $9,748.25 | ▼ -36.98 after sell → book $9,748.25; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,748.25 | ▲ close $9,748.25 vs 09:30 $9,762.49 (session +0.00) | 16:00 close · cash $9,748.25 · no lots left · equity $9,748.25. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,748.25 | ▲ 09:30 equity $9,748.25 vs yday $9,748.25 (+0.00) | 09:30 open · cash $9,748.25 · no holdings · equity $9,748.25 vs prior close $9,748.25 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,748.25 | ▲ close $9,748.25 vs 09:30 $9,748.25 (session +0.00) | 16:00 close · cash $9,748.25 · no lots left · equity $9,748.25. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,748.25 | ▲ 09:30 equity $9,748.25 vs yday $9,748.25 (+0.00) | 09:30 open · cash $9,748.25 · no holdings · equity $9,748.25 vs prior close $9,748.25 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,748.25 | ▲ close $9,748.25 vs 09:30 $9,748.25 (session +0.00) | 16:00 close · cash $9,748.25 · no lots left · equity $9,748.25. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,748.25 | ▲ 09:30 equity $9,748.25 vs yday $9,748.25 (+0.00) | 09:30 open · cash $9,748.25 · no holdings · equity $9,748.25 vs prior close $9,748.25 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 28 | $42.93 | $2.07 | — | $8,544.14 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1218.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `PBH` | 22 | $53.45 | $2.06 | — | $7,366.18 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.3; leftover $1218.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `PCRX` | 45 | $26.74 | $2.12 | — | $6,160.76 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.6; leftover $1218.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $4,966.69 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1218.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $3,909.47 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1218.53 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PBF` | 16 | $74.75 | $2.04 | — | $2,711.44 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ret5=+8.2; leftover $1218.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $1,493.14 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1218.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 335 | $3.63 | $4.32 | — | $272.77 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1218.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $272.77 | ▼ close $9,622.98 vs 09:30 $9,748.25 (session -106.59) | 16:00 close · cash $272.77 · equity $9,622.98 vs 09:30 $9,748.25 (-125.27; session marks -106.59) · 8 name(s) marked open→close (per-name table). HRMY×28 09:30 $42.93 → close $41.86 -29.96; PBH×22 09:30 $53.45 → close $52.56 -19.58; PCRX×45 09:30 $26.74 → close $26.60 -6.30; RVTY×9 09:30 $132.45 → close $130.63 -16.38; AVGO×3 09:30 $351.74 → close $357.16 +16.26; PBF×16 09:30 $74.75 → close $75.33 +9.28; ATRC×23 09:30 $52.88 → close $52.46 -9.66; CABA×335 09:30 $3.63 → close $3.48 -50.25 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $272.77 | ▼ 09:30 equity $9,558.63 vs yday $9,622.98 (-64.35) | 09:30 open · cash $272.77 (unchanged overnight, no fees) · equity $9,558.63 vs prior close $9,622.98 (-64.35) · 8 name(s) re-marked at the open (per-name table). HRMY×28 yday $41.86 → 09:30 $41.50 -10.08; PBH×22 yday $52.56 → 09:30 $51.80 -16.72; PCRX×45 yday $26.60 → 09:30 $26.38 -9.90; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; AVGO×3 yday $357.16 → 09:30 $359.70 +7.62; PBF×16 yday $75.33 → 09:30 $74.50 -13.28; ATRC×23 yday $52.46 → 09:30 $52.03 -9.89; CABA×335 yday $3.48 → 09:30 $3.46 -6.70 | — |
| 2026-09-04 09:30 ET | **SELL** | `PBH` | 22 | $51.80 | $2.08 | $-40.43 | $1,410.29 | ▼ -40.43 after sell → book $9,556.55; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PCRX` | 45 | $26.38 | $2.15 | $-20.47 | $2,595.24 | ▼ -20.47 after sell → book $9,554.40; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $3,672.33 | ▲ +19.86 after sell → book $9,552.39; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBF` | 16 | $74.50 | $2.06 | $-8.10 | $4,862.27 | ▼ -8.10 after sell → book $9,550.33; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 482 | $2.52 | $6.22 | — | $3,641.41 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1215.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 181 | $6.71 | $2.53 | — | $2,424.37 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1215.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 107 | $11.31 | $2.31 | — | $1,211.89 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1215.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `PIPR` | 15 | $76.55 | $2.04 | — | $61.60 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $1215.57 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.60 | ▼ close $9,510.87 vs 09:30 $9,558.63 (session -26.37) | 16:00 close · cash $61.60 · equity $9,510.87 vs 09:30 $9,558.63 (-47.76; session marks -26.37) · 8 name(s) marked open→close (per-name table). HRMY×28 09:30 $41.50 → close $42.25 +21.00; RVTY×9 09:30 $130.03 → close $130.22 +1.71; ATRC×23 09:30 $52.03 → close $51.52 -11.73; CABA×335 09:30 $3.46 → close $3.47 +3.35; ALEC×482 09:30 $2.52 → close $2.46 -28.92; BHC×181 09:30 $6.71 → close $6.56 -27.15; VIR×107 09:30 $11.31 → close $11.38 +8.02; PIPR×15 09:30 $76.55 → close $77.04 +7.35 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.60 | ▼ 09:30 equity $9,484.35 vs yday $9,510.87 (-26.52) | 09:30 open · cash $61.60 (unchanged overnight, no fees) · equity $9,484.35 vs prior close $9,510.87 (-26.52) · 8 name(s) re-marked at the open (per-name table). HRMY×28 yday $42.25 → 09:30 $42.20 -1.40; RVTY×9 yday $130.22 → 09:30 $128.50 -15.48; ATRC×23 yday $51.52 → 09:30 $54.31 +64.17; CABA×335 yday $3.47 → 09:30 $3.43 -13.40; ALEC×482 yday $2.46 → 09:30 $2.38 -38.56; BHC×181 yday $6.56 → 09:30 $6.57 +1.81; VIR×107 yday $11.38 → 09:30 $11.22 -17.65; PIPR×15 yday $77.04 → 09:30 $76.64 -6.00 | — |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 28 | $42.20 | $2.09 | $-24.61 | $1,241.11 | ▼ -24.61 after sell → book $9,482.26; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `RVTY` | 9 | $128.50 | $2.04 | $-39.60 | $2,395.57 | ▼ -39.60 after sell → book $9,480.22; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 23 | $54.31 | $2.08 | $+28.75 | $3,642.62 | ▲ +28.75 after sell → book $9,478.14; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 335 | $3.43 | $4.39 | $-75.71 | $4,787.28 | ▼ -75.71 after sell → book $9,473.75; vs 09:30 mark -4.39 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 482 | $2.38 | $6.31 | $-80.01 | $5,928.14 | ▼ -80.01 after sell → book $9,467.45; vs 09:30 mark -6.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 181 | $6.57 | $2.57 | $-30.45 | $7,114.73 | ▼ -30.45 after sell → book $9,464.87; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 107 | $11.22 | $2.34 | $-14.28 | $8,312.93 | ▼ -14.28 after sell → book $9,462.53; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `PIPR` | 15 | $76.64 | $2.06 | $-2.74 | $9,460.48 | ▼ -2.74 after sell → book $9,460.48; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,460.48 | ▲ close $9,460.48 vs 09:30 $9,484.35 (session +0.00) | 16:00 close · cash $9,460.48 · no lots left · equity $9,460.48. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,460.48 | ▲ 09:30 equity $9,460.48 vs yday $9,460.48 (-0.00) | 09:30 open · cash $9,460.48 · no holdings · equity $9,460.48 vs prior close $9,460.48 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,460.48 | ▲ close $9,460.48 vs 09:30 $9,460.48 (session +0.00) | 16:00 close · cash $9,460.48 · no lots left · equity $9,460.48. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,460.48 | ▲ 09:30 equity $9,460.48 vs yday $9,460.48 (-0.00) | 09:30 open · cash $9,460.48 · no holdings · equity $9,460.48 vs prior close $9,460.48 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,460.48 | ▲ close $9,460.48 vs 09:30 $9,460.48 (session +0.00) | 16:00 close · cash $9,460.48 · no lots left · equity $9,460.48. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,460.48 | ▲ 09:30 equity $9,460.48 vs yday $9,460.48 (-0.00) | 09:30 open · cash $9,460.48 · no holdings · equity $9,460.48 vs prior close $9,460.48 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 22 | $52.55 | $2.06 | — | $8,302.32 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1182.56 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BTI` | 21 | $56.03 | $2.05 | — | $7,123.64 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list oppset; 🔵; ret5=-0.8; leftover $1182.56 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ASX` | 29 | $39.57 | $2.08 | — | $5,974.03 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; ret5=+7.7; leftover $1182.56 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 5 | $206.84 | $2.00 | — | $4,937.83 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; ret5=+8.3; leftover $1182.56 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 248 | $4.75 | $3.20 | — | $3,756.63 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1182.56 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INGM` | 44 | $26.62 | $2.12 | — | $2,583.23 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list oppset; ret5=+0.7; leftover $1182.56 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PGNY` | 43 | $27.45 | $2.12 | — | $1,400.76 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; 🔵; ret5=+6.1; leftover $1182.56 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 16 | $71.71 | $2.04 | — | $251.36 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; ret5=-9.1; leftover $1182.56 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $251.36 | ▲ close $9,687.42 vs 09:30 $9,460.48 (session +244.61) | 16:00 close · cash $251.36 · equity $9,687.42 vs 09:30 $9,460.48 (+226.94; session marks +244.61) · 8 name(s) marked open→close (per-name table). BAND×22 09:30 $52.55 → close $56.87 +95.04; BTI×21 09:30 $56.03 → close $55.24 -16.59; ASX×29 09:30 $39.57 → close $39.47 -2.90; SANM×5 09:30 $206.84 → close $216.00 +45.80; CLOV×248 09:30 $4.75 → close $4.82 +17.36; INGM×44 09:30 $26.62 → close $27.55 +40.92; PGNY×43 09:30 $27.45 → close $27.35 -4.30; DSGX×16 09:30 $71.71 → close $76.04 +69.28 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $251.36 | ▼ 09:30 equity $9,632.14 vs yday $9,687.42 (-55.28) | 09:30 open · cash $251.36 (unchanged overnight, no fees) · equity $9,632.14 vs prior close $9,687.42 (-55.28) · 8 name(s) re-marked at the open (per-name table). BAND×22 yday $56.87 → 09:30 $56.90 +0.66; BTI×21 yday $55.24 → 09:30 $57.12 +39.48; ASX×29 yday $39.47 → 09:30 $37.41 -59.74; SANM×5 yday $216.00 → 09:30 $206.50 -47.50; CLOV×248 yday $4.82 → 09:30 $4.82 +0.00; INGM×44 yday $27.55 → 09:30 $26.89 -29.04; PGNY×43 yday $27.35 → 09:30 $27.69 +14.62; DSGX×16 yday $76.04 → 09:30 $77.68 +26.24 | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 22 | $56.90 | $2.08 | $+91.57 | $1,501.08 | ▲ +91.57 after sell → book $9,630.06; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `BTI` | 21 | $57.12 | $2.07 | $+18.76 | $2,698.53 | ▲ +18.76 after sell → book $9,627.99; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASX` | 29 | $37.41 | $2.10 | $-66.81 | $3,781.32 | ▼ -66.81 after sell → book $9,625.89; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `SANM` | 5 | $206.50 | $2.02 | $-5.73 | $4,811.80 | ▼ -5.73 after sell → book $9,623.87; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 248 | $4.82 | $3.25 | $+10.91 | $6,003.91 | ▲ +10.91 after sell → book $9,620.62; vs 09:30 mark -3.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `INGM` | 44 | $26.89 | $2.14 | $+7.62 | $7,184.93 | ▲ +7.62 after sell → book $9,618.48; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `PGNY` | 43 | $27.69 | $2.14 | $+6.06 | $8,373.46 | ▲ +6.06 after sell → book $9,616.34; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `DSGX` | 16 | $77.68 | $2.06 | $+91.42 | $9,614.28 | ▲ +91.42 after sell → book $9,614.28; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,614.28 | ▲ close $9,614.28 vs 09:30 $9,632.14 (session +0.00) | 16:00 close · cash $9,614.28 · no lots left · equity $9,614.28. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,614.28 | ▲ 09:30 equity $9,614.28 vs yday $9,614.28 (-0.00) | 09:30 open · cash $9,614.28 · no holdings · equity $9,614.28 vs prior close $9,614.28 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,614.28 | ▲ close $9,614.28 vs 09:30 $9,614.28 (session +0.00) | 16:00 close · cash $9,614.28 · no lots left · equity $9,614.28. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,614.28 | ▲ 09:30 equity $9,614.28 vs yday $9,614.28 (-0.00) | 09:30 open · cash $9,614.28 · no holdings · equity $9,614.28 vs prior close $9,614.28 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `BWIN` | 37 | $32.25 | $2.10 | — | $8,418.93 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list oppset; 🔵; ret5=-3.7; leftover $1201.78 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 83 | $14.31 | $2.24 | — | $7,228.96 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; ret5=+4.8; leftover $1201.78 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `ILMN` | 5 | $224.49 | $2.00 | — | $6,104.50 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list yday_gainer; 🔵; ret5=+5.3; leftover $1201.78 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 30 | $39.99 | $2.08 | — | $4,902.72 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1201.78 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `AMX` | 51 | $23.18 | $2.14 | — | $3,718.40 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list oppset; 🔵; ret5=-0.2; leftover $1201.78 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 32 | $36.46 | $2.09 | — | $2,549.60 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ret5=+2.9; leftover $1201.78 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $1,464.03 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; ret5=+4.0; leftover $1201.78 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `NEO` | 63 | $18.84 | $2.18 | — | $274.93 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; ret5=+7.6; leftover $1201.78 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $274.93 | ▼ close $9,529.44 vs 09:30 $9,614.28 (session -68.00) | 16:00 close · cash $274.93 · equity $9,529.44 vs 09:30 $9,614.28 (-84.84; session marks -68.00) · 8 name(s) marked open→close (per-name table). BWIN×37 09:30 $32.25 → close $32.04 -7.77; AVAH×83 09:30 $14.31 → close $14.26 -4.15; ILMN×5 09:30 $224.49 → close $228.93 +22.20; SM×30 09:30 $39.99 → close $38.16 -54.90; AMX×51 09:30 $23.18 → close $22.98 -10.20; BLFS×32 09:30 $36.46 → close $36.11 -11.20; IQV×4 09:30 $270.89 → close $268.82 -8.28; NEO×63 09:30 $18.84 → close $18.94 +6.30 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $274.93 | ▲ 09:30 equity $9,594.45 vs yday $9,529.44 (+65.01) | 09:30 open · cash $274.93 (unchanged overnight, no fees) · equity $9,594.45 vs prior close $9,529.44 (+65.01) · 8 name(s) re-marked at the open (per-name table). BWIN×37 yday $32.04 → 09:30 $32.06 +0.74; AVAH×83 yday $14.26 → 09:30 $14.33 +5.81; ILMN×5 yday $228.93 → 09:30 $233.85 +24.60; SM×30 yday $38.16 → 09:30 $37.57 -17.70; AMX×51 yday $22.98 → 09:30 $23.09 +5.61; BLFS×32 yday $36.11 → 09:30 $36.67 +17.92; IQV×4 yday $268.82 → 09:30 $273.15 +17.32; NEO×63 yday $18.94 → 09:30 $19.11 +10.71 | — |
| 2026-09-17 09:30 ET | **SELL** | `BWIN` | 37 | $32.06 | $2.12 | $-11.25 | $1,459.03 | ▼ -11.25 after sell → book $9,592.33; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `AVAH` | 83 | $14.33 | $2.26 | $-2.84 | $2,646.16 | ▼ -2.84 after sell → book $9,590.07; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ILMN` | 5 | $233.85 | $2.02 | $+42.77 | $3,813.39 | ▲ +42.77 after sell → book $9,588.05; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 30 | $37.57 | $2.10 | $-76.78 | $4,938.39 | ▼ -76.78 after sell → book $9,585.95; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AMX` | 51 | $23.09 | $2.16 | $-8.90 | $6,113.81 | ▼ -8.90 after sell → book $9,583.78; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BLFS` | 32 | $36.67 | $2.11 | $+2.53 | $7,285.15 | ▲ +2.53 after sell → book $9,581.68; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `IQV` | 4 | $273.15 | $2.02 | $+5.02 | $8,375.72 | ▲ +5.02 after sell → book $9,579.65; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `NEO` | 63 | $19.11 | $2.20 | $+12.63 | $9,577.45 | ▲ +12.63 after sell → book $9,577.45; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BULL` | 150 | $7.95 | $2.44 | — | $8,382.51 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $1197.18 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $7,184.55 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1197.18 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AXTI` | 17 | $67.91 | $2.04 | — | $6,028.04 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.7; leftover $1197.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `CYPH` | 447 | $2.67 | $5.77 | — | $4,826.55 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list yday_gainer; 🔵; ret5=-0.4; leftover $1197.18 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 157 | $7.59 | $2.46 | — | $3,632.46 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1197.18 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `VOD` | 68 | $17.56 | $2.19 | — | $2,436.19 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+7.9; leftover $1197.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 75 | $15.81 | $2.21 | — | $1,248.22 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $1197.18 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `LIFE` | 30 | $39.67 | $2.08 | — | $56.04 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list oppset; 🔵; ret5=-2.9; leftover $1197.18 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.04 | ▲ close $9,692.36 vs 09:30 $9,594.45 (session +136.11) | 16:00 close · cash $56.04 · equity $9,692.36 vs 09:30 $9,594.45 (+97.91; session marks +136.11) · 8 name(s) marked open→close (per-name table). BULL×150 09:30 $7.95 → close $7.71 -36.00; SMTC×7 09:30 $170.85 → close $178.19 +51.38; AXTI×17 09:30 $67.91 → close $67.75 -2.72; CYPH×447 09:30 $2.67 → close $3.07 +176.56; PGEN×157 09:30 $7.59 → close $7.87 +43.96; VOD×68 09:30 $17.56 → close $17.52 -2.72; AVTR×75 09:30 $15.81 → close $15.86 +3.75; LIFE×30 09:30 $39.67 → close $36.40 -98.10 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.04 | ▲ 09:30 equity $9,739.19 vs yday $9,692.36 (+46.83) | 09:30 open · cash $56.04 (unchanged overnight, no fees) · equity $9,739.19 vs prior close $9,692.36 (+46.83) · 8 name(s) re-marked at the open (per-name table). BULL×150 yday $7.71 → 09:30 $7.85 +21.00; SMTC×7 yday $178.19 → 09:30 $182.33 +28.98; AXTI×17 yday $67.75 → 09:30 $69.72 +33.49; CYPH×447 yday $3.07 → 09:30 $3.04 -15.64; PGEN×157 yday $7.87 → 09:30 $7.98 +17.27; VOD×68 yday $17.52 → 09:30 $16.73 -53.72; AVTR×75 yday $15.86 → 09:30 $15.87 +0.75; LIFE×30 yday $36.40 → 09:30 $36.89 +14.70 | — |
| 2026-09-18 09:30 ET | **SELL** | `BULL` | 150 | $7.85 | $2.47 | $-19.91 | $1,231.07 | ▼ -19.91 after sell → book $9,736.71; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $2,505.35 | ▲ +76.32 after sell → book $9,734.68; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `AXTI` | 17 | $69.72 | $2.06 | $+26.67 | $3,688.52 | ▲ +26.67 after sell → book $9,732.62; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CYPH` | 447 | $3.04 | $5.85 | $+149.30 | $5,039.32 | ▲ +149.30 after sell → book $9,726.77; vs 09:30 mark -5.85 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 157 | $7.98 | $2.50 | $+56.27 | $6,289.68 | ▲ +56.27 after sell → book $9,724.27; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **SELL** | `VOD` | 68 | $16.73 | $2.22 | $-60.85 | $7,425.11 | ▼ -60.85 after sell → book $9,722.06; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 75 | $15.87 | $2.24 | $+0.05 | $8,613.12 | ▲ +0.05 after sell → book $9,719.82; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LIFE` | 30 | $36.89 | $2.10 | $-87.58 | $9,717.72 | ▼ -87.58 after sell → book $9,717.72; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `CRWV` | 15 | $79.83 | $2.04 | — | $8,518.23 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list oppset; 🔵; ⚪; ret5=+5.2; leftover $1214.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 58 | $20.91 | $2.16 | — | $7,303.29 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1214.71 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TTAN` | 22 | $53.53 | $2.06 | — | $6,123.46 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list oppset; ⚪; ret5=-41.3; leftover $1214.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `LTRX` | 206 | $5.89 | $2.66 | — | $4,907.47 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list oppset; 🔵; ret5=+0.4; leftover $1214.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `DRVN` | 99 | $12.26 | $2.29 | — | $3,691.44 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list oppset; 🔵; ⚪; ret5=+6.4; leftover $1214.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `QSR` | 16 | $73.00 | $2.04 | — | $2,521.40 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list oppset; 🔵; ⚪; ret5=+0.5; leftover $1214.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `SFL` | 88 | $13.74 | $2.25 | — | $1,310.03 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.4; leftover $1214.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `AMD` | 2 | $547.37 | $2.00 | — | $213.29 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; ret5=+8.2; leftover $1214.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $213.29 | ▲ close $9,779.57 vs 09:30 $9,739.19 (session +79.34) | 16:00 close · cash $213.29 · equity $9,779.57 vs 09:30 $9,739.19 (+40.38; session marks +79.34) · 8 name(s) marked open→close (per-name table). CRWV×15 09:30 $79.83 → close $81.36 +22.95; TH×58 09:30 $20.91 → close $21.19 +16.24; TTAN×22 09:30 $53.53 → close $55.04 +33.11; LTRX×206 09:30 $5.89 → close $5.85 -8.24; DRVN×99 09:30 $12.26 → close $12.28 +1.98; QSR×16 09:30 $73.00 → close $72.88 -1.92; SFL×88 09:30 $13.74 → close $13.63 -9.68; AMD×2 09:30 $547.37 → close $559.82 +24.90 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `EIX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PLX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MAIR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BHP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `EBAY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TME` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EROC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRCT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1272.81 < 1 share @ 1746.53 |
| 2026-08-28 | `MPWR` | cash | leftover split 1254.02 < 1 share @ 1306.03 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACIW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `AVPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CHKP` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DINO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALAB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ANET` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CRK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DHT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `ADM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASND` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASTH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CHEF` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `COUR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CRDL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `VNT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `STX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HAS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OBE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `WDS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `QRVO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `VLO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DHT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INGM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VLO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CLX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `KHC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PANW` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PBF` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `VLO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CVE` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `BWIN` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CRWV` | 15 | 2026-09-18 @ $79.83 | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list oppset; 🔵; ⚪; ret5=+5.2; leftover $1214.71 |
| `TH` | 58 | 2026-09-18 @ $20.91 | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1214.71 |
| `TTAN` | 22 | 2026-09-18 @ $53.53 | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list oppset; ⚪; ret5=-41.3; leftover $1214.71 |
| `LTRX` | 206 | 2026-09-18 @ $5.89 | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list oppset; 🔵; ret5=+0.4; leftover $1214.71 |
| `DRVN` | 99 | 2026-09-18 @ $12.26 | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list oppset; 🔵; ⚪; ret5=+6.4; leftover $1214.71 |
| `QSR` | 16 | 2026-09-18 @ $73.00 | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list oppset; 🔵; ⚪; ret5=+0.5; leftover $1214.71 |
| `SFL` | 88 | 2026-09-18 @ $13.74 | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.4; leftover $1214.71 |
| `AMD` | 2 | 2026-09-18 @ $547.37 | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; ret5=+8.2; leftover $1214.71 |
