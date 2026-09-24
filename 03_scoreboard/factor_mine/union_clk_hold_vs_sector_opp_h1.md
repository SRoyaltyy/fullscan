# Factor mine action — `union_clk_hold_vs_sector_opp_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `opp_rvol` · size `leftover` · sell `list` · S-boost `none` · Clock-B #6 ∩ Theme Radar T−1 oppset

Cash book **+11.93%** ($11,193) · signal-only (no cash/fees) was +2.00%. Starts YES **26/30**. Fills 122 · skips 54 · realized $+1193.25.

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
- Must-have: Clock-B #6: the stock held up (yesterday up or last bar green) while the sector camera is red.
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
- **Gate** `clk_hold_vs_sector=True,oppset=True` · **rank** `opp_rvol` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,193.26.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-17 | `HTFL` | 80 | — | $41.23 | +0.00 | $41.94 | +56.80 | +56.80 | +0.00 | +56.80 |
| 2026-08-17 | `VERA` | 106 | — | $31.30 | +0.00 | $31.63 | +34.98 | +34.98 | +0.00 | +34.98 |
| 2026-08-17 | `CELC` | 35 | — | $92.99 | +0.00 | $92.44 | -19.25 | -19.25 | +0.00 | -19.25 |
| 2026-08-18 | `HTFL` | 80 | $41.94 | $41.50 | -35.20 | — | +0.00 | -35.20 | +21.60 | — |
| 2026-08-18 | `VERA` | 106 | $31.63 | $31.31 | -33.92 | — | +0.00 | -33.92 | +1.06 | — |
| 2026-08-18 | `CELC` | 35 | $92.44 | $92.38 | -2.10 | — | +0.00 | -2.10 | -21.35 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `LZB` | 74 | — | $33.61 | +0.00 | $33.65 | +2.96 | +2.96 | +0.00 | +2.96 |
| 2026-08-20 | `ATAT` | 73 | — | $34.05 | +0.00 | $34.25 | +14.60 | +14.60 | +0.00 | +14.60 |
| 2026-08-20 | `SG` | 388 | — | $6.43 | +0.00 | $6.58 | +58.20 | +58.20 | +0.00 | +58.20 |
| 2026-08-20 | `HTHT` | 51 | — | $48.39 | +0.00 | $49.54 | +58.65 | +58.65 | +0.00 | +58.65 |
| 2026-08-21 | `LZB` | 74 | $33.65 | $33.63 | -1.48 | — | +0.00 | -1.48 | +1.48 | — |
| 2026-08-21 | `ATAT` | 73 | $34.25 | $34.31 | +4.38 | — | +0.00 | +4.38 | +18.98 | — |
| 2026-08-21 | `SG` | 388 | $6.58 | $6.61 | +11.64 | — | +0.00 | +11.64 | +69.84 | — |
| 2026-08-21 | `HTHT` | 51 | $49.54 | $49.58 | +2.04 | — | +0.00 | +2.04 | +60.69 | — |
| 2026-08-21 | `BJ` | 26 | — | $93.98 | +0.00 | $96.42 | +63.44 | +63.44 | +0.00 | +63.44 |
| 2026-08-21 | `DE` | 4 | — | $623.26 | +0.00 | $647.47 | +96.84 | +96.84 | +0.00 | +96.84 |
| 2026-08-21 | `XXI` | 393 | — | $6.42 | +0.00 | $6.49 | +27.51 | +27.51 | +0.00 | +27.51 |
| 2026-08-21 | `SM` | 66 | — | $37.81 | +0.00 | $37.20 | -40.26 | -40.26 | +0.00 | -40.26 |
| 2026-08-24 | `BJ` | 26 | $96.42 | $97.02 | +15.60 | — | +0.00 | +15.60 | +79.04 | — |
| 2026-08-24 | `DE` | 4 | $647.47 | $653.04 | +22.28 | — | +0.00 | +22.28 | +119.12 | — |
| 2026-08-24 | `XXI` | 393 | $6.49 | $6.64 | +60.91 | — | +0.00 | +60.91 | +88.42 | — |
| 2026-08-24 | `SM` | 66 | $37.20 | $36.61 | -38.94 | — | +0.00 | -38.94 | -79.20 | — |
| 2026-08-25 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-26 | `MAIR` | 46 | — | $27.59 | +0.00 | $28.51 | +42.32 | +42.32 | +0.00 | +42.32 |
| 2026-08-26 | `SMTC` | 9 | — | $130.90 | +0.00 | $140.80 | +89.10 | +89.10 | +0.00 | +89.10 |
| 2026-08-26 | `MNRO` | 91 | — | $14.00 | +0.00 | $12.61 | -126.49 | -126.49 | +0.00 | -126.49 |
| 2026-08-26 | `GRRR` | 91 | — | $14.03 | +0.00 | $15.45 | +129.22 | +129.22 | +0.00 | +129.22 |
| 2026-08-26 | `NCNO` | 66 | — | $19.33 | +0.00 | $21.51 | +143.88 | +143.88 | +0.00 | +143.88 |
| 2026-08-26 | `BE` | 6 | — | $213.94 | +0.00 | $218.21 | +25.62 | +25.62 | +0.00 | +25.62 |
| 2026-08-26 | `QMLS` | 198 | — | $6.47 | +0.00 | $6.10 | -73.26 | -73.26 | +0.00 | -73.26 |
| 2026-08-26 | `NVTS` | 102 | — | $12.60 | +0.00 | $12.67 | +7.14 | +7.14 | +0.00 | +7.14 |
| 2026-08-27 | `MAIR` | 46 | $28.51 | $28.76 | +11.50 | — | +0.00 | +11.50 | +53.82 | — |
| 2026-08-27 | `SMTC` | 9 | $140.80 | $149.40 | +77.40 | — | +0.00 | +77.40 | +166.50 | — |
| 2026-08-27 | `MNRO` | 91 | $12.61 | $12.56 | -4.55 | — | +0.00 | -4.55 | -131.04 | — |
| 2026-08-27 | `GRRR` | 91 | $15.45 | $15.94 | +44.59 | — | +0.00 | +44.59 | +173.81 | — |
| 2026-08-27 | `NCNO` | 66 | $21.51 | $22.03 | +34.32 | — | +0.00 | +34.32 | +178.20 | — |
| 2026-08-27 | `BE` | 6 | $218.21 | $227.10 | +53.34 | — | +0.00 | +53.34 | +78.96 | — |
| 2026-08-27 | `QMLS` | 198 | $6.10 | $6.33 | +45.54 | — | +0.00 | +45.54 | -27.72 | — |
| 2026-08-27 | `NVTS` | 102 | $12.67 | $13.18 | +52.02 | — | +0.00 | +52.02 | +59.16 | — |
| 2026-08-28 | `ANF` | 9 | — | $146.07 | +0.00 | $148.42 | +21.15 | +21.15 | +0.00 | +21.15 |
| 2026-08-28 | `SYRE` | 14 | — | $91.75 | +0.00 | $90.36 | -19.46 | -19.46 | +0.00 | -19.46 |
| 2026-08-28 | `ERAS` | 70 | — | $19.25 | +0.00 | $18.03 | -85.40 | -85.40 | +0.00 | -85.40 |
| 2026-08-28 | `TH` | 71 | — | $19.00 | +0.00 | $18.55 | -31.95 | -31.95 | +0.00 | -31.95 |
| 2026-08-28 | `FIGR` | 36 | — | $37.49 | +0.00 | $36.05 | -51.84 | -51.84 | +0.00 | -51.84 |
| 2026-08-28 | `FRO` | 30 | — | $44.40 | +0.00 | $44.19 | -6.30 | -6.30 | +0.00 | -6.30 |
| 2026-08-28 | `HAFN` | 161 | — | $8.35 | +0.00 | $8.47 | +19.32 | +19.32 | +0.00 | +19.32 |
| 2026-08-28 | `IREN` | 35 | — | $37.65 | +0.00 | $35.45 | -76.83 | -76.83 | +0.00 | -76.83 |
| 2026-08-31 | `ANF` | 9 | $148.42 | $148.03 | -3.51 | — | +0.00 | -3.51 | +17.64 | — |
| 2026-08-31 | `SYRE` | 14 | $90.36 | $89.15 | -16.94 | — | +0.00 | -16.94 | -36.40 | — |
| 2026-08-31 | `ERAS` | 70 | $18.03 | $17.87 | -11.20 | — | +0.00 | -11.20 | -96.60 | — |
| 2026-08-31 | `TH` | 71 | $18.55 | $18.12 | -30.18 | — | +0.00 | -30.18 | -62.12 | — |
| 2026-08-31 | `FIGR` | 36 | $36.05 | $35.77 | -10.08 | — | +0.00 | -10.08 | -61.92 | — |
| 2026-08-31 | `FRO` | 30 | $44.19 | $44.85 | +19.80 | — | +0.00 | +19.80 | +13.50 | — |
| 2026-08-31 | `HAFN` | 161 | $8.47 | $8.53 | +9.66 | — | +0.00 | +9.66 | +28.98 | — |
| 2026-08-31 | `IREN` | 35 | $35.45 | $35.81 | +12.60 | — | +0.00 | +12.60 | -64.23 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `XP` | 101 | — | $20.74 | +0.00 | $20.00 | -74.74 | -74.74 | +0.00 | -74.74 |
| 2026-09-03 | `HP` | 44 | — | $47.74 | +0.00 | $45.02 | -119.68 | -119.68 | +0.00 | -119.68 |
| 2026-09-03 | `PBR` | 99 | — | $21.18 | +0.00 | $20.51 | -66.33 | -66.33 | +0.00 | -66.33 |
| 2026-09-03 | `VSXY` | 27 | — | $76.86 | +0.00 | $73.64 | -86.94 | -86.94 | +0.00 | -86.94 |
| 2026-09-03 | `PBR-A` | 109 | — | $19.16 | +0.00 | $18.58 | -63.22 | -63.22 | +0.00 | -63.22 |
| 2026-09-04 | `XP` | 101 | $20.00 | $19.67 | -33.33 | — | +0.00 | -33.33 | -108.07 | — |
| 2026-09-04 | `HP` | 44 | $45.02 | $44.59 | -18.92 | — | +0.00 | -18.92 | -138.60 | — |
| 2026-09-04 | `PBR` | 99 | $20.51 | $20.25 | -25.74 | — | +0.00 | -25.74 | -92.07 | — |
| 2026-09-04 | `VSXY` | 27 | $73.64 | $73.63 | -0.27 | — | +0.00 | -0.27 | -87.21 | — |
| 2026-09-04 | `PBR-A` | 109 | $18.58 | $18.36 | -23.98 | — | +0.00 | -23.98 | -87.20 | — |
| 2026-09-04 | `LULU` | 20 | — | $98.15 | +0.00 | $100.61 | +49.20 | +49.20 | +0.00 | +49.20 |
| 2026-09-04 | `AUR` | 318 | — | $6.26 | +0.00 | $6.34 | +23.85 | +23.85 | +0.00 | +23.85 |
| 2026-09-04 | `MIR` | 120 | — | $16.60 | +0.00 | $16.93 | +39.60 | +39.60 | +0.00 | +39.60 |
| 2026-09-04 | `BE` | 8 | — | $236.82 | +0.00 | $252.87 | +128.40 | +128.40 | +0.00 | +128.40 |
| 2026-09-04 | `SCZM` | 199 | — | $10.03 | +0.00 | $9.94 | -17.91 | -17.91 | +0.00 | -17.91 |
| 2026-09-08 | `LULU` | 20 | $100.61 | $100.58 | -0.60 | — | +0.00 | -0.60 | +48.60 | — |
| 2026-09-08 | `AUR` | 318 | $6.34 | $6.34 | +0.00 | — | +0.00 | +0.00 | +23.85 | — |
| 2026-09-08 | `MIR` | 120 | $16.93 | $17.07 | +16.80 | — | +0.00 | +16.80 | +56.40 | — |
| 2026-09-08 | `BE` | 8 | $252.87 | $267.76 | +119.12 | — | +0.00 | +119.12 | +247.52 | — |
| 2026-09-08 | `SCZM` | 199 | $9.94 | $9.90 | -7.96 | — | +0.00 | -7.96 | -25.87 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `BKV` | 51 | — | $24.97 | +0.00 | $24.23 | -37.74 | -37.74 | +0.00 | -37.74 |
| 2026-09-11 | `DBI` | 218 | — | $5.91 | +0.00 | $5.88 | -6.54 | -6.54 | +0.00 | -6.54 |
| 2026-09-11 | `VIST` | 16 | — | $77.33 | +0.00 | $76.27 | -16.96 | -16.96 | +0.00 | -16.96 |
| 2026-09-11 | `ASO` | 23 | — | $54.91 | +0.00 | $55.36 | +10.35 | +10.35 | +0.00 | +10.35 |
| 2026-09-11 | `PBR` | 60 | — | $21.21 | +0.00 | $21.20 | -0.60 | -0.60 | +0.00 | -0.60 |
| 2026-09-11 | `GME` | 61 | — | $21.04 | +0.00 | $21.15 | +6.71 | +6.71 | +0.00 | +6.71 |
| 2026-09-11 | `SSL` | 89 | — | $14.35 | +0.00 | $14.59 | +21.36 | +21.36 | +0.00 | +21.36 |
| 2026-09-11 | `INTR` | 222 | — | $5.78 | +0.00 | $5.71 | -15.54 | -15.54 | +0.00 | -15.54 |
| 2026-09-14 | `BKV` | 51 | $24.23 | $24.26 | +1.53 | — | +0.00 | +1.53 | -36.21 | — |
| 2026-09-14 | `DBI` | 218 | $5.88 | $5.86 | -4.36 | — | +0.00 | -4.36 | -10.90 | — |
| 2026-09-14 | `VIST` | 16 | $76.27 | $77.10 | +13.28 | — | +0.00 | +13.28 | -3.68 | — |
| 2026-09-14 | `ASO` | 23 | $55.36 | $54.75 | -14.03 | — | +0.00 | -14.03 | -3.68 | — |
| 2026-09-14 | `PBR` | 60 | $21.20 | $21.23 | +1.80 | — | +0.00 | +1.80 | +1.20 | — |
| 2026-09-14 | `GME` | 61 | $21.15 | $21.00 | -9.15 | $21.62 | +37.82 | +28.67 | -2.44 | +35.38 |
| 2026-09-14 | `SSL` | 89 | $14.59 | $14.69 | +8.90 | — | +0.00 | +8.90 | +30.26 | — |
| 2026-09-14 | `INTR` | 222 | $5.71 | $5.49 | -48.84 | — | +0.00 | -48.84 | -64.38 | — |
| 2026-09-15 | `GME` | 61 | $21.62 | $21.51 | -6.71 | — | +0.00 | -6.71 | +28.67 | — |
| 2026-09-16 | `FPS` | 38 | — | $33.14 | +0.00 | $34.84 | +64.60 | +64.60 | +0.00 | +64.60 |
| 2026-09-16 | `GFR` | 186 | — | $6.83 | +0.00 | $6.49 | -63.24 | -63.24 | +0.00 | -63.24 |
| 2026-09-16 | `FRO` | 24 | — | $52.52 | +0.00 | $53.67 | +27.60 | +27.60 | +0.00 | +27.60 |
| 2026-09-16 | `TALO` | 71 | — | $17.87 | +0.00 | $17.42 | -31.95 | -31.95 | +0.00 | -31.95 |
| 2026-09-16 | `VAL` | 14 | — | $87.40 | +0.00 | $82.52 | -68.32 | -68.32 | +0.00 | -68.32 |
| 2026-09-16 | `RIG` | 217 | — | $5.87 | +0.00 | $5.54 | -71.61 | -71.61 | +0.00 | -71.61 |
| 2026-09-16 | `MRCY` | 14 | — | $87.52 | +0.00 | $87.25 | -3.78 | -3.78 | +0.00 | -3.78 |
| 2026-09-16 | `KRMN` | 33 | — | $38.01 | +0.00 | $36.94 | -35.31 | -35.31 | +0.00 | -35.31 |
| 2026-09-17 | `FPS` | 38 | $34.84 | $36.76 | +72.96 | $38.06 | +49.40 | +122.36 | +137.56 | +186.96 |
| 2026-09-17 | `GFR` | 186 | $6.49 | $6.48 | -1.86 | — | +0.00 | -1.86 | -65.10 | — |
| 2026-09-17 | `FRO` | 24 | $53.67 | $54.31 | +15.36 | — | +0.00 | +15.36 | +42.96 | — |
| 2026-09-17 | `TALO` | 71 | $17.42 | $17.19 | -16.33 | — | +0.00 | -16.33 | -48.28 | — |
| 2026-09-17 | `VAL` | 14 | $82.52 | $83.20 | +9.52 | — | +0.00 | +9.52 | -58.80 | — |
| 2026-09-17 | `RIG` | 217 | $5.54 | $5.58 | +8.68 | — | +0.00 | +8.68 | -62.93 | — |
| 2026-09-17 | `MRCY` | 14 | $87.25 | $89.27 | +28.28 | — | +0.00 | +28.28 | +24.50 | — |
| 2026-09-17 | `KRMN` | 33 | $36.94 | $37.89 | +31.35 | — | +0.00 | +31.35 | -3.96 | — |
| 2026-09-17 | `BKV` | 96 | — | $22.75 | +0.00 | $22.79 | +3.84 | +3.84 | +0.00 | +3.84 |
| 2026-09-17 | `FTAI` | 11 | — | $196.50 | +0.00 | $195.07 | -15.73 | -15.73 | +0.00 | -15.73 |
| 2026-09-17 | `TK` | 151 | — | $14.41 | +0.00 | $14.67 | +39.26 | +39.26 | +0.00 | +39.26 |
| 2026-09-17 | `EROC` | 173 | — | $12.64 | +0.00 | $12.90 | +44.98 | +44.98 | +0.00 | +44.98 |
| 2026-09-18 | `FPS` | 38 | $38.06 | $39.50 | +54.72 | — | +0.00 | +54.72 | +241.68 | — |
| 2026-09-18 | `BKV` | 96 | $22.79 | $22.92 | +12.48 | — | +0.00 | +12.48 | +16.32 | — |
| 2026-09-18 | `FTAI` | 11 | $195.07 | $195.55 | +5.28 | — | +0.00 | +5.28 | -10.45 | — |
| 2026-09-18 | `TK` | 151 | $14.67 | $14.60 | -10.57 | — | +0.00 | -10.57 | +28.69 | — |
| 2026-09-18 | `EROC` | 173 | $12.90 | $13.00 | +17.30 | — | +0.00 | +17.30 | +62.28 | — |
| 2026-09-18 | `FLNC` | 342 | — | $7.54 | +0.00 | $7.32 | -73.53 | -73.53 | +0.00 | -73.53 |
| 2026-09-18 | `USDE` | 270 | — | $9.54 | +0.00 | $10.19 | +175.50 | +175.50 | +0.00 | +175.50 |
| 2026-09-18 | `PURR` | 186 | — | $13.82 | +0.00 | $14.09 | +50.22 | +50.22 | +0.00 | +50.22 |
| 2026-09-18 | `ARE` | 45 | — | $56.70 | +0.00 | $53.30 | -153.00 | -153.00 | +0.00 | -153.00 |
| 2026-09-21 | `FLNC` | 342 | $7.32 | $7.36 | +13.68 | — | +0.00 | +13.68 | -59.85 | — |
| 2026-09-21 | `USDE` | 270 | $10.19 | $13.05 | +772.20 | — | +0.00 | +772.20 | +947.70 | — |
| 2026-09-21 | `PURR` | 186 | $14.09 | $14.65 | +104.16 | — | +0.00 | +104.16 | +154.38 | — |
| 2026-09-21 | `ARE` | 45 | $53.30 | $53.39 | +4.05 | — | +0.00 | +4.05 | -148.95 | — |
| 2026-09-22 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-23 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-24 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +72.53 | HTFL, VERA, CELC | — | $122.52 | $10,065.90 | HTFL×80, VERA×106, CELC×35 |
| 2026-08-18 | -6.20 | $122.52 | HTFL×80, VERA×106, CELC×35 | $9,994.68 | -71.22 | +0.00 | — | HTFL, VERA, CELC | $9,987.92 | $9,987.92 | — |
| 2026-08-19 | -7.20 | $9,987.92 | — | $9,987.92 | +0.00 | +0.00 | — | — | $9,987.92 | $9,987.92 | — |
| 2026-08-20 | +1.12 | $9,987.92 | — | $9,987.92 | +0.00 | +134.41 | LZB, ATAT, SG, HTHT | — | $40.84 | $10,110.77 | LZB×74, ATAT×73, SG×388, HTHT×51 |
| 2026-08-21 | +3.25 | $40.84 | LZB×74, ATAT×73, SG×388, HTHT×51 | $10,127.35 | +16.58 | +147.53 | BJ, DE, XXI, SM | LZB, ATAT, SG, HTHT | $149.23 | $10,251.80 | BJ×26, DE×4, XXI×393, SM×66 |
| 2026-08-24 | -5.17 | $149.23 | BJ×26, DE×4, XXI×393, SM×66 | $10,311.65 | +59.85 | +0.00 | — | BJ, DE, XXI, SM | $10,300.15 | $10,300.15 | — |
| 2026-08-25 | +1.80 | $10,300.15 | — | $10,300.15 | -0.00 | +0.00 | — | — | $10,300.15 | $10,300.15 | — |
| 2026-08-26 | +2.02 | $10,300.15 | — | $10,300.15 | -0.00 | +237.53 | MAIR, SMTC, MNRO, GRRR, NCNO, BE, QMLS, NVTS | — | $158.75 | $10,519.93 | MAIR×46, SMTC×9, MNRO×91, GRRR×91, NCNO×66, BE×6, QMLS×198, NVTS×102 |
| 2026-08-27 | — | $158.75 | MAIR×46, SMTC×9, MNRO×91, GRRR×91, NCNO×66, BE×6, QMLS×198, NVTS×102 | $10,834.09 | +314.16 | +0.00 | — | MAIR, SMTC, MNRO, GRRR, NCNO, BE, QMLS, NVTS | $10,816.14 | $10,816.14 | — |
| 2026-08-28 | +0.75 | $10,816.14 | — | $10,816.14 | -0.00 | -231.31 | ANF, SYRE, ERAS, TH, FIGR, FRO, HAFN, IREN | — | $159.74 | $10,567.63 | ANF×9, SYRE×14, ERAS×70, TH×71, FIGR×36, FRO×30, HAFN×161, IREN×35 |
| 2026-08-31 | -5.85 | $159.74 | ANF×9, SYRE×14, ERAS×70, TH×71, FIGR×36, FRO×30, HAFN×161, IREN×35 | $10,537.79 | -29.84 | +0.00 | — | ANF, SYRE, ERAS, TH, FIGR, FRO, HAFN, IREN | $10,520.41 | $10,520.41 | — |
| 2026-09-01 | -6.30 | $10,520.41 | — | $10,520.41 | -0.00 | +0.00 | — | — | $10,520.41 | $10,520.41 | — |
| 2026-09-02 | -3.83 | $10,520.41 | — | $10,520.41 | -0.00 | +0.00 | — | — | $10,520.41 | $10,520.41 | — |
| 2026-09-03 | -0.90 | $10,520.41 | — | $10,520.41 | -0.00 | -410.91 | XP, HP, PBR, VSXY, PBR-A | — | $53.54 | $10,098.41 | XP×101, HP×44, PBR×99, VSXY×27, PBR-A×109 |
| 2026-09-04 | +2.25 | $53.54 | XP×101, HP×44, PBR×99, VSXY×27, PBR-A×109 | $9,996.17 | -102.24 | +223.14 | LULU, AUR, MIR, BE, SCZM | XP, HP, PBR, VSXY, PBR-A | $134.02 | $10,194.96 | LULU×20, AUR×318, MIR×120, BE×8, SCZM×199 |
| 2026-09-08 | -11.47 | $134.02 | LULU×20, AUR×318, MIR×120, BE×8, SCZM×199 | $10,322.32 | +127.36 | +0.00 | — | LULU, AUR, MIR, BE, SCZM | $10,309.01 | $10,309.01 | — |
| 2026-09-09 | -13.95 | $10,309.01 | — | $10,309.01 | +0.00 | +0.00 | — | — | $10,309.01 | $10,309.01 | — |
| 2026-09-10 | -13.28 | $10,309.01 | — | $10,309.01 | +0.00 | +0.00 | — | — | $10,309.01 | $10,309.01 | — |
| 2026-09-11 | +0.50 | $10,309.01 | — | $10,309.01 | +0.00 | -38.96 | BKV, DBI, VIST, ASO, PBR, GME, SSL, INTR | — | $112.09 | $10,251.54 | BKV×51, DBI×218, VIST×16, ASO×23, PBR×60, GME×61, SSL×89, INTR×222 |
| 2026-09-14 | -11.00 | $112.09 | BKV×51, DBI×218, VIST×16, ASO×23, PBR×60, GME×61, SSL×89, INTR×222 | $10,200.67 | -50.87 | +37.82 | — | BKV, DBI, VIST, ASO, PBR, SSL, INTR | $8,903.13 | $10,221.95 | GME×61 |
| 2026-09-15 | -3.84 | $8,903.13 | GME×61 | $10,215.24 | -6.71 | +0.00 | — | GME | $10,213.04 | $10,213.04 | — |
| 2026-09-16 | +5.30 | $10,213.04 | — | $10,213.04 | +0.00 | -182.01 | FPS, GFR, FRO, TALO, VAL, RIG, MRCY, KRMN | — | $159.22 | $10,013.16 | FPS×38, GFR×186, FRO×24, TALO×71, VAL×14, RIG×217, MRCY×14, KRMN×33 |
| 2026-09-17 | +7.38 | $159.22 | FPS×38, GFR×186, FRO×24, TALO×71, VAL×14, RIG×217, MRCY×14, KRMN×33 | $10,161.12 | +147.96 | +121.75 | BKV, FTAI, TK, EROC | GFR, FRO, TALO, VAL, RIG, MRCY, KRMN | $30.91 | $10,257.67 | FPS×38, BKV×96, FTAI×11, TK×151, EROC×173 |
| 2026-09-18 | +4.86 | $30.91 | FPS×38, BKV×96, FTAI×11, TK×151, EROC×173 | $10,336.88 | +79.21 | -0.81 | FLNC, USDE, PURR, ARE | FPS, BKV, FTAI, TK, EROC | $37.99 | $10,311.97 | FLNC×342, USDE×270, PURR×186, ARE×45 |
| 2026-09-21 | +12.87 | $37.99 | FLNC×342, USDE×270, PURR×186, ARE×45 | $11,206.06 | +894.09 | +0.00 | — | FLNC, USDE, PURR, ARE | $11,193.26 | $11,193.26 | — |
| 2026-09-22 | -0.50 | $11,193.26 | — | $11,193.26 | -0.00 | +0.00 | — | — | $11,193.26 | $11,193.26 | — |
| 2026-09-23 | +2.29 | $11,193.26 | — | $11,193.26 | -0.00 | +0.00 | — | — | $11,193.26 | $11,193.26 | — |
| 2026-09-24 | -7.66 | $11,193.26 | — | $11,193.26 | -0.00 | +0.00 | — | — | $11,193.26 | $11,193.26 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 80 | $41.23 | $2.23 | — | $6,699.37 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+46.0; leftover $3333.33 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 106 | $31.30 | $2.31 | — | $3,379.26 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-3.8; leftover $3333.33 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 35 | $92.99 | $2.10 | — | $122.52 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-0.8; leftover $3333.33 | join🟡 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.52 | ▲ close $10,065.90 vs 09:30 $10,000.00 (session +72.53) | 16:00 close · cash $122.52 · equity $10,065.90 vs 09:30 $10,000.00 (+65.90; session marks +72.53) · 3 name(s) marked open→close (per-name table). HTFL×80 09:30 $41.23 → close $41.94 +56.80; VERA×106 09:30 $31.30 → close $31.63 +34.98; CELC×35 09:30 $92.99 → close $92.44 -19.25 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.52 | ▼ 09:30 equity $9,994.68 vs yday $10,065.90 (-71.22) | 09:30 open · cash $122.52 (unchanged overnight, no fees) · equity $9,994.68 vs prior close $10,065.90 (-71.22) · 3 name(s) re-marked at the open (per-name table). HTFL×80 yday $41.94 → 09:30 $41.50 -35.20; VERA×106 yday $31.63 → 09:30 $31.31 -33.92; CELC×35 yday $92.44 → 09:30 $92.38 -2.10 | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 80 | $41.50 | $2.27 | $+17.10 | $3,440.25 | ▲ +17.10 after sell → book $9,992.41; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 106 | $31.31 | $2.35 | $-3.60 | $6,756.76 | ▼ -3.60 after sell → book $9,990.06; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 35 | $92.38 | $2.13 | $-25.58 | $9,987.92 | ▼ -25.58 after sell → book $9,987.92; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,987.92 | ▲ close $9,987.92 vs 09:30 $9,994.68 (session +0.00) | 16:00 close · cash $9,987.92 · no lots left · equity $9,987.92. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,987.92 | ▲ 09:30 equity $9,987.92 vs yday $9,987.92 (+0.00) | 09:30 open · cash $9,987.92 · no holdings · equity $9,987.92 vs prior close $9,987.92 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,987.92 | ▲ close $9,987.92 vs 09:30 $9,987.92 (session +0.00) | 16:00 close · cash $9,987.92 · no lots left · equity $9,987.92. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,987.92 | ▲ 09:30 equity $9,987.92 vs yday $9,987.92 (+0.00) | 09:30 open · cash $9,987.92 · no holdings · equity $9,987.92 vs prior close $9,987.92 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `LZB` | 74 | $33.61 | $2.21 | — | $7,498.57 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-17.4; leftover $2496.98 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 73 | $34.05 | $2.21 | — | $5,010.71 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+9.3; leftover $2496.98 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SG` | 388 | $6.43 | $5.01 | — | $2,510.87 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+10.3; leftover $2496.98 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HTHT` | 51 | $48.39 | $2.14 | — | $40.84 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+17.8; leftover $2496.98 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.84 | ▲ close $10,110.77 vs 09:30 $9,987.92 (session +134.41) | 16:00 close · cash $40.84 · equity $10,110.77 vs 09:30 $9,987.92 (+122.85; session marks +134.41) · 4 name(s) marked open→close (per-name table). LZB×74 09:30 $33.61 → close $33.65 +2.96; ATAT×73 09:30 $34.05 → close $34.25 +14.60; SG×388 09:30 $6.43 → close $6.58 +58.20; HTHT×51 09:30 $48.39 → close $49.54 +58.65 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.84 | ▲ 09:30 equity $10,127.35 vs yday $10,110.77 (+16.58) | 09:30 open · cash $40.84 (unchanged overnight, no fees) · equity $10,127.35 vs prior close $10,110.77 (+16.58) · 4 name(s) re-marked at the open (per-name table). LZB×74 yday $33.65 → 09:30 $33.63 -1.48; ATAT×73 yday $34.25 → 09:30 $34.31 +4.38; SG×388 yday $6.58 → 09:30 $6.61 +11.64; HTHT×51 yday $49.54 → 09:30 $49.58 +2.04 | — |
| 2026-08-21 09:30 ET | **SELL** | `LZB` | 74 | $33.63 | $2.24 | $-2.98 | $2,527.21 | ▼ -2.98 after sell → book $10,125.10; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 73 | $34.31 | $2.24 | $+14.53 | $5,029.60 | ▲ +14.53 after sell → book $10,122.86; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SG` | 388 | $6.61 | $5.09 | $+59.74 | $7,589.19 | ▲ +59.74 after sell → book $10,117.77; vs 09:30 mark -5.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HTHT` | 51 | $49.58 | $2.17 | $+56.37 | $10,115.60 | ▲ +56.37 after sell → book $10,115.60; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 26 | $93.98 | $2.07 | — | $7,670.05 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list earn_react; ret5=-2.4; leftover $2528.90 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 4 | $623.26 | $2.00 | — | $5,175.01 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $2528.90 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XXI` | 393 | $6.42 | $5.07 | — | $2,646.88 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=+23.8; leftover $2528.90 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `SM` | 66 | $37.81 | $2.19 | — | $149.23 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+16.1; leftover $2528.90 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.23 | ▲ close $10,251.80 vs 09:30 $10,127.35 (session +147.53) | 16:00 close · cash $149.23 · equity $10,251.80 vs 09:30 $10,127.35 (+124.45; session marks +147.53) · 4 name(s) marked open→close (per-name table). BJ×26 09:30 $93.98 → close $96.42 +63.44; DE×4 09:30 $623.26 → close $647.47 +96.84; XXI×393 09:30 $6.42 → close $6.49 +27.51; SM×66 09:30 $37.81 → close $37.20 -40.26 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.23 | ▲ 09:30 equity $10,311.65 vs yday $10,251.80 (+59.85) | 09:30 open · cash $149.23 (unchanged overnight, no fees) · equity $10,311.65 vs prior close $10,251.80 (+59.85) · 4 name(s) re-marked at the open (per-name table). BJ×26 yday $96.42 → 09:30 $97.02 +15.60; DE×4 yday $647.47 → 09:30 $653.04 +22.28; XXI×393 yday $6.49 → 09:30 $6.64 +60.91; SM×66 yday $37.20 → 09:30 $36.61 -38.94 | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 26 | $97.02 | $2.10 | $+74.87 | $2,669.65 | ▲ +74.87 after sell → book $10,309.56; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 4 | $653.04 | $2.03 | $+115.09 | $5,279.78 | ▲ +115.09 after sell → book $10,307.52; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XXI` | 393 | $6.64 | $5.16 | $+78.20 | $7,886.11 | ▲ +78.20 after sell → book $10,302.37; vs 09:30 mark -5.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `SM` | 66 | $36.61 | $2.22 | $-83.61 | $10,300.15 | ▼ -83.61 after sell → book $10,300.15; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,300.15 | ▲ close $10,300.15 vs 09:30 $10,311.65 (session +0.00) | 16:00 close · cash $10,300.15 · no lots left · equity $10,300.15. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,300.15 | ▲ 09:30 equity $10,300.15 vs yday $10,300.15 (-0.00) | 09:30 open · cash $10,300.15 · no holdings · equity $10,300.15 vs prior close $10,300.15 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,300.15 | ▲ close $10,300.15 vs 09:30 $10,300.15 (session +0.00) | 16:00 close · cash $10,300.15 · no lots left · equity $10,300.15. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,300.15 | ▲ 09:30 equity $10,300.15 vs yday $10,300.15 (-0.00) | 09:30 open · cash $10,300.15 · no holdings · equity $10,300.15 vs prior close $10,300.15 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 09:30 ET | **BUY** | `MAIR` | 46 | $27.59 | $2.13 | — | $9,028.88 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=+2.0; leftover $1287.52 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SMTC` | 9 | $130.90 | $2.02 | — | $7,848.76 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-5.7; leftover $1287.52 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 91 | $14.00 | $2.26 | — | $6,572.50 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+17.8; leftover $1287.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `GRRR` | 91 | $14.03 | $2.26 | — | $5,293.51 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-7.6; leftover $1287.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NCNO` | 66 | $19.33 | $2.19 | — | $4,015.54 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list earn_react; ret5=+3.0; leftover $1287.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 6 | $213.94 | $2.01 | — | $2,729.89 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1287.52 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `QMLS` | 198 | $6.47 | $2.58 | — | $1,446.25 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-7.0; leftover $1287.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NVTS` | 102 | $12.60 | $2.30 | — | $158.75 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=-5.5; leftover $1287.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.75 | ▲ close $10,519.93 vs 09:30 $10,300.15 (session +237.53) | 16:00 close · cash $158.75 · equity $10,519.93 vs 09:30 $10,300.15 (+219.78; session marks +237.53) · 8 name(s) marked open→close (per-name table). MAIR×46 09:30 $27.59 → close $28.51 +42.32; SMTC×9 09:30 $130.90 → close $140.80 +89.10; MNRO×91 09:30 $14.00 → close $12.61 -126.49; GRRR×91 09:30 $14.03 → close $15.45 +129.22; NCNO×66 09:30 $19.33 → close $21.51 +143.88; BE×6 09:30 $213.94 → close $218.21 +25.62; QMLS×198 09:30 $6.47 → close $6.10 -73.26; NVTS×102 09:30 $12.60 → close $12.67 +7.14 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.75 | ▲ 09:30 equity $10,834.09 vs yday $10,519.93 (+314.16) | 09:30 open · cash $158.75 (unchanged overnight, no fees) · equity $10,834.09 vs prior close $10,519.93 (+314.16) · 8 name(s) re-marked at the open (per-name table). MAIR×46 yday $28.51 → 09:30 $28.76 +11.50; SMTC×9 yday $140.80 → 09:30 $149.40 +77.40; MNRO×91 yday $12.61 → 09:30 $12.56 -4.55; GRRR×91 yday $15.45 → 09:30 $15.94 +44.59; NCNO×66 yday $21.51 → 09:30 $22.03 +34.32; BE×6 yday $218.21 → 09:30 $227.10 +53.34; QMLS×198 yday $6.10 → 09:30 $6.33 +45.54; NVTS×102 yday $12.67 → 09:30 $13.18 +52.02 | — |
| 2026-08-27 09:30 ET | **SELL** | `MAIR` | 46 | $28.76 | $2.15 | $+49.54 | $1,479.56 | ▲ +49.54 after sell → book $10,831.94; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `SMTC` | 9 | $149.40 | $2.04 | $+162.45 | $2,822.13 | ▲ +162.45 after sell → book $10,829.91; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `MNRO` | 91 | $12.56 | $2.29 | $-135.59 | $3,962.80 | ▼ -135.59 after sell → book $10,827.62; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GRRR` | 91 | $15.94 | $2.29 | $+169.26 | $5,411.05 | ▲ +169.26 after sell → book $10,825.33; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `NCNO` | 66 | $22.03 | $2.21 | $+173.80 | $6,862.82 | ▲ +173.80 after sell → book $10,823.12; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `BE` | 6 | $227.10 | $2.03 | $+74.92 | $8,223.39 | ▲ +74.92 after sell → book $10,821.09; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `QMLS` | 198 | $6.33 | $2.63 | $-32.93 | $9,474.10 | ▼ -32.93 after sell → book $10,818.46; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `NVTS` | 102 | $13.18 | $2.32 | $+54.54 | $10,816.14 | ▲ +54.54 after sell → book $10,816.14; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,816.14 | ▲ close $10,816.14 vs 09:30 $10,834.09 (session +0.00) | 16:00 close · cash $10,816.14 · no lots left · equity $10,816.14. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,816.14 | ▲ 09:30 equity $10,816.14 vs yday $10,816.14 (-0.00) | 09:30 open · cash $10,816.14 · no holdings · equity $10,816.14 vs prior close $10,816.14 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $9,499.49 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1352.02 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SYRE` | 14 | $91.75 | $2.03 | — | $8,212.96 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-13.2; leftover $1352.02 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 70 | $19.25 | $2.20 | — | $6,863.26 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=+14.1; leftover $1352.02 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 71 | $19.00 | $2.20 | — | $5,512.06 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+7.5; leftover $1352.02 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 36 | $37.49 | $2.10 | — | $4,160.32 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_mover; ret5=+5.4; leftover $1352.02 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 30 | $44.40 | $2.08 | — | $2,826.24 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list earn_react; ret5=+0.4; leftover $1352.02 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 161 | $8.35 | $2.47 | — | $1,479.41 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list earn_react; ret5=+5.1; leftover $1352.02 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 35 | $37.65 | $2.10 | — | $159.74 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list earn_react; ret5=-4.9; leftover $1352.02 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $159.74 | ▼ close $10,567.63 vs 09:30 $10,816.14 (session -231.31) | 16:00 close · cash $159.74 · equity $10,567.63 vs 09:30 $10,816.14 (-248.51; session marks -231.31) · 8 name(s) marked open→close (per-name table). ANF×9 09:30 $146.07 → close $148.42 +21.15; SYRE×14 09:30 $91.75 → close $90.36 -19.46; ERAS×70 09:30 $19.25 → close $18.03 -85.40; TH×71 09:30 $19.00 → close $18.55 -31.95; FIGR×36 09:30 $37.49 → close $36.05 -51.84; FRO×30 09:30 $44.40 → close $44.19 -6.30; HAFN×161 09:30 $8.35 → close $8.47 +19.32; IREN×35 09:30 $37.65 → close $35.45 -76.83 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $159.74 | ▼ 09:30 equity $10,537.79 vs yday $10,567.63 (-29.84) | 09:30 open · cash $159.74 (unchanged overnight, no fees) · equity $10,537.79 vs prior close $10,567.63 (-29.84) · 8 name(s) re-marked at the open (per-name table). ANF×9 yday $148.42 → 09:30 $148.03 -3.51; SYRE×14 yday $90.36 → 09:30 $89.15 -16.94; ERAS×70 yday $18.03 → 09:30 $17.87 -11.20; TH×71 yday $18.55 → 09:30 $18.12 -30.18; FIGR×36 yday $36.05 → 09:30 $35.77 -10.08; FRO×30 yday $44.19 → 09:30 $44.85 +19.80; HAFN×161 yday $8.47 → 09:30 $8.53 +9.66; IREN×35 yday $35.45 → 09:30 $35.81 +12.60 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $1,489.98 | ▲ +13.59 after sell → book $10,535.75; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SYRE` | 14 | $89.15 | $2.05 | $-40.48 | $2,736.02 | ▼ -40.48 after sell → book $10,533.70; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 70 | $17.87 | $2.22 | $-101.02 | $3,984.70 | ▼ -101.02 after sell → book $10,531.48; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 71 | $18.12 | $2.23 | $-66.55 | $5,269.35 | ▼ -66.55 after sell → book $10,529.25; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 36 | $35.77 | $2.12 | $-66.14 | $6,554.95 | ▼ -66.14 after sell → book $10,527.13; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 30 | $44.85 | $2.10 | $+9.32 | $7,898.35 | ▲ +9.32 after sell → book $10,525.03; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 161 | $8.53 | $2.51 | $+24.00 | $9,269.17 | ▲ +24.00 after sell → book $10,522.52; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 35 | $35.81 | $2.12 | $-68.43 | $10,520.41 | ▼ -68.43 after sell → book $10,520.41; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,520.41 | ▲ close $10,520.41 vs 09:30 $10,537.79 (session +0.00) | 16:00 close · cash $10,520.41 · no lots left · equity $10,520.41. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,520.41 | ▲ 09:30 equity $10,520.41 vs yday $10,520.41 (-0.00) | 09:30 open · cash $10,520.41 · no holdings · equity $10,520.41 vs prior close $10,520.41 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,520.41 | ▲ close $10,520.41 vs 09:30 $10,520.41 (session +0.00) | 16:00 close · cash $10,520.41 · no lots left · equity $10,520.41. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,520.41 | ▲ 09:30 equity $10,520.41 vs yday $10,520.41 (-0.00) | 09:30 open · cash $10,520.41 · no holdings · equity $10,520.41 vs prior close $10,520.41 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,520.41 | ▲ close $10,520.41 vs 09:30 $10,520.41 (session +0.00) | 16:00 close · cash $10,520.41 · no lots left · equity $10,520.41. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,520.41 | ▲ 09:30 equity $10,520.41 vs yday $10,520.41 (-0.00) | 09:30 open · cash $10,520.41 · no holdings · equity $10,520.41 vs prior close $10,520.41 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `XP` | 101 | $20.74 | $2.29 | — | $8,423.37 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+12.2; leftover $2104.08 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HP` | 44 | $47.74 | $2.12 | — | $6,320.69 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+15.1; leftover $2104.08 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PBR` | 99 | $21.18 | $2.29 | — | $4,221.59 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+17.5; leftover $2104.08 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 27 | $76.86 | $2.07 | — | $2,144.29 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-6.6; leftover $2104.08 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PBR-A` | 109 | $19.16 | $2.32 | — | $53.54 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+17.4; leftover $2104.08 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.54 | ▼ close $10,098.41 vs 09:30 $10,520.41 (session -410.91) | 16:00 close · cash $53.54 · equity $10,098.41 vs 09:30 $10,520.41 (-422.00; session marks -410.91) · 5 name(s) marked open→close (per-name table). XP×101 09:30 $20.74 → close $20.00 -74.74; HP×44 09:30 $47.74 → close $45.02 -119.68; PBR×99 09:30 $21.18 → close $20.51 -66.33; VSXY×27 09:30 $76.86 → close $73.64 -86.94; PBR-A×109 09:30 $19.16 → close $18.58 -63.22 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.54 | ▼ 09:30 equity $9,996.17 vs yday $10,098.41 (-102.24) | 09:30 open · cash $53.54 (unchanged overnight, no fees) · equity $9,996.17 vs prior close $10,098.41 (-102.24) · 5 name(s) re-marked at the open (per-name table). XP×101 yday $20.00 → 09:30 $19.67 -33.33; HP×44 yday $45.02 → 09:30 $44.59 -18.92; PBR×99 yday $20.51 → 09:30 $20.25 -25.74; VSXY×27 yday $73.64 → 09:30 $73.63 -0.27; PBR-A×109 yday $18.58 → 09:30 $18.36 -23.98 | — |
| 2026-09-04 09:30 ET | **SELL** | `XP` | 101 | $19.67 | $2.33 | $-112.69 | $2,037.88 | ▼ -112.69 after sell → book $9,993.84; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `HP` | 44 | $44.59 | $2.15 | $-142.87 | $3,997.69 | ▼ -142.87 after sell → book $9,991.69; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBR` | 99 | $20.25 | $2.32 | $-96.68 | $6,000.13 | ▼ -96.68 after sell → book $9,989.38; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSXY` | 27 | $73.63 | $2.10 | $-91.38 | $7,986.04 | ▼ -91.38 after sell → book $9,987.28; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBR-A` | 109 | $18.36 | $2.35 | $-91.87 | $9,984.93 | ▼ -91.87 after sell → book $9,984.93; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 20 | $98.15 | $2.05 | — | $8,019.88 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list earn_react; ret5=+5.9; leftover $1996.99 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AUR` | 318 | $6.26 | $4.10 | — | $6,023.50 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+11.1; leftover $1996.99 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MIR` | 120 | $16.60 | $2.35 | — | $4,029.15 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+12.9; leftover $1996.99 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 8 | $236.82 | $2.01 | — | $2,132.58 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+8.1; leftover $1996.99 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SCZM` | 199 | $10.03 | $2.59 | — | $134.02 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=+4.0; leftover $1996.99 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $134.02 | ▲ close $10,194.96 vs 09:30 $9,996.17 (session +223.14) | 16:00 close · cash $134.02 · equity $10,194.96 vs 09:30 $9,996.17 (+198.79; session marks +223.14) · 5 name(s) marked open→close (per-name table). LULU×20 09:30 $98.15 → close $100.61 +49.20; AUR×318 09:30 $6.26 → close $6.34 +23.85; MIR×120 09:30 $16.60 → close $16.93 +39.60; BE×8 09:30 $236.82 → close $252.87 +128.40; SCZM×199 09:30 $10.03 → close $9.94 -17.91 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $134.02 | ▲ 09:30 equity $10,322.32 vs yday $10,194.96 (+127.36) | 09:30 open · cash $134.02 (unchanged overnight, no fees) · equity $10,322.32 vs prior close $10,194.96 (+127.36) · 5 name(s) re-marked at the open (per-name table). LULU×20 yday $100.61 → 09:30 $100.58 -0.60; AUR×318 yday $6.34 → 09:30 $6.34 +0.00; MIR×120 yday $16.93 → 09:30 $17.07 +16.80; BE×8 yday $252.87 → 09:30 $267.76 +119.12; SCZM×199 yday $9.94 → 09:30 $9.90 -7.96 | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 20 | $100.58 | $2.08 | $+44.47 | $2,143.55 | ▲ +44.47 after sell → book $10,320.25; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AUR` | 318 | $6.34 | $4.17 | $+15.58 | $4,155.50 | ▲ +15.58 after sell → book $10,316.08; vs 09:30 mark -4.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MIR` | 120 | $17.07 | $2.39 | $+51.66 | $6,201.51 | ▲ +51.66 after sell → book $10,313.69; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 8 | $267.76 | $2.04 | $+243.46 | $8,341.55 | ▲ +243.46 after sell → book $10,311.65; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `SCZM` | 199 | $9.90 | $2.64 | $-31.09 | $10,309.01 | ▼ -31.09 after sell → book $10,309.01; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,309.01 | ▲ close $10,309.01 vs 09:30 $10,322.32 (session +0.00) | 16:00 close · cash $10,309.01 · no lots left · equity $10,309.01. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,309.01 | ▲ 09:30 equity $10,309.01 vs yday $10,309.01 (+0.00) | 09:30 open · cash $10,309.01 · no holdings · equity $10,309.01 vs prior close $10,309.01 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,309.01 | ▲ close $10,309.01 vs 09:30 $10,309.01 (session +0.00) | 16:00 close · cash $10,309.01 · no lots left · equity $10,309.01. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,309.01 | ▲ 09:30 equity $10,309.01 vs yday $10,309.01 (+0.00) | 09:30 open · cash $10,309.01 · no holdings · equity $10,309.01 vs prior close $10,309.01 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,309.01 | ▲ close $10,309.01 vs 09:30 $10,309.01 (session +0.00) | 16:00 close · cash $10,309.01 · no lots left · equity $10,309.01. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,309.01 | ▲ 09:30 equity $10,309.01 vs yday $10,309.01 (+0.00) | 09:30 open · cash $10,309.01 · no holdings · equity $10,309.01 vs prior close $10,309.01 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `BKV` | 51 | $24.97 | $2.14 | — | $9,033.40 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+10.8; leftover $1288.63 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 218 | $5.91 | $2.81 | — | $7,742.21 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; leftover $1288.63 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 16 | $77.33 | $2.04 | — | $6,502.89 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=+2.5; leftover $1288.63 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 23 | $54.91 | $2.06 | — | $5,237.90 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+24.3; leftover $1288.63 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PBR` | 60 | $21.21 | $2.17 | — | $3,963.13 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+2.5; leftover $1288.63 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `GME` | 61 | $21.04 | $2.17 | — | $2,677.52 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+7.5; leftover $1288.63 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SSL` | 89 | $14.35 | $2.26 | — | $1,398.11 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+15.5; leftover $1288.63 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INTR` | 222 | $5.78 | $2.86 | — | $112.09 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=-1.7; leftover $1288.63 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $112.09 | ▼ close $10,251.54 vs 09:30 $10,309.01 (session -38.96) | 16:00 close · cash $112.09 · equity $10,251.54 vs 09:30 $10,309.01 (-57.47; session marks -38.96) · 8 name(s) marked open→close (per-name table). BKV×51 09:30 $24.97 → close $24.23 -37.74; DBI×218 09:30 $5.91 → close $5.88 -6.54; VIST×16 09:30 $77.33 → close $76.27 -16.96; ASO×23 09:30 $54.91 → close $55.36 +10.35; PBR×60 09:30 $21.21 → close $21.20 -0.60; GME×61 09:30 $21.04 → close $21.15 +6.71; SSL×89 09:30 $14.35 → close $14.59 +21.36; INTR×222 09:30 $5.78 → close $5.71 -15.54 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $112.09 | ▼ 09:30 equity $10,200.67 vs yday $10,251.54 (-50.87) | 09:30 open · cash $112.09 (unchanged overnight, no fees) · equity $10,200.67 vs prior close $10,251.54 (-50.87) · 8 name(s) re-marked at the open (per-name table). BKV×51 yday $24.23 → 09:30 $24.26 +1.53; DBI×218 yday $5.88 → 09:30 $5.86 -4.36; VIST×16 yday $76.27 → 09:30 $77.10 +13.28; ASO×23 yday $55.36 → 09:30 $54.75 -14.03; PBR×60 yday $21.20 → 09:30 $21.23 +1.80; GME×61 yday $21.15 → 09:30 $21.00 -9.15; SSL×89 yday $14.59 → 09:30 $14.69 +8.90; INTR×222 yday $5.71 → 09:30 $5.49 -48.84 | — |
| 2026-09-14 09:30 ET | **SELL** | `BKV` | 51 | $24.26 | $2.16 | $-40.52 | $1,347.18 | ▼ -40.52 after sell → book $10,198.50; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🔴 digest🟢 judge🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `DBI` | 218 | $5.86 | $2.86 | $-16.57 | $2,621.81 | ▼ -16.57 after sell → book $10,195.65; vs 09:30 mark -2.85 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIST` | 16 | $77.10 | $2.06 | $-7.78 | $3,853.35 | ▼ -7.78 after sell → book $10,193.59; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 23 | $54.75 | $2.08 | $-7.82 | $5,110.52 | ▼ -7.82 after sell → book $10,191.51; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PBR` | 60 | $21.23 | $2.19 | $-3.16 | $6,382.13 | ▼ -3.16 after sell → book $10,189.32; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SSL` | 89 | $14.69 | $2.28 | $+25.72 | $7,687.26 | ▲ +25.72 after sell → book $10,187.04; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INTR` | 222 | $5.49 | $2.91 | $-70.15 | $8,903.13 | ▼ -70.15 after sell → book $10,184.13; vs 09:30 mark -2.91 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,903.13 | ▲ close $10,221.95 vs 09:30 $10,200.67 (session +37.82) | 16:00 close · cash $8,903.13 · equity $10,221.95 vs 09:30 $10,200.67 (+21.28; session marks +37.82) · 1 name(s) marked open→close (per-name table). GME×61 09:30 $21.00 → close $21.62 +37.82 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,903.13 | ▼ 09:30 equity $10,215.24 vs yday $10,221.95 (-6.71) | 09:30 open · cash $8,903.13 (unchanged overnight, no fees) · equity $10,215.24 vs prior close $10,221.95 (-6.71) · 1 name(s) re-marked at the open (per-name table). GME×61 yday $21.62 → 09:30 $21.51 -6.71 | — |
| 2026-09-15 09:30 ET | **SELL** | `GME` | 61 | $21.51 | $2.19 | $+24.30 | $10,213.04 | ▲ +24.30 after sell → book $10,213.04; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,213.04 | ▲ close $10,213.04 vs 09:30 $10,215.24 (session +0.00) | 16:00 close · cash $10,213.04 · no lots left · equity $10,213.04. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,213.04 | ▲ 09:30 equity $10,213.04 vs yday $10,213.04 (+0.00) | 09:30 open · cash $10,213.04 · no holdings · equity $10,213.04 vs prior close $10,213.04 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 38 | $33.14 | $2.10 | — | $8,951.62 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=-2.9; leftover $1276.63 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `GFR` | 186 | $6.83 | $2.55 | — | $7,678.69 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+11.2; leftover $1276.63 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `FRO` | 24 | $52.52 | $2.06 | — | $6,416.15 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+10.7; leftover $1276.63 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `TALO` | 71 | $17.87 | $2.20 | — | $5,145.18 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+6.8; leftover $1276.63 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 14 | $87.40 | $2.03 | — | $3,919.54 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1276.63 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 217 | $5.87 | $2.80 | — | $2,642.95 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1276.63 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 catal🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `MRCY` | 14 | $87.52 | $2.03 | — | $1,415.64 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+4.3; leftover $1276.63 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 33 | $38.01 | $2.09 | — | $159.22 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $1276.63 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $159.22 | ▼ close $10,013.16 vs 09:30 $10,213.04 (session -182.01) | 16:00 close · cash $159.22 · equity $10,013.16 vs 09:30 $10,213.04 (-199.88; session marks -182.01) · 8 name(s) marked open→close (per-name table). FPS×38 09:30 $33.14 → close $34.84 +64.60; GFR×186 09:30 $6.83 → close $6.49 -63.24; FRO×24 09:30 $52.52 → close $53.67 +27.60; TALO×71 09:30 $17.87 → close $17.42 -31.95; VAL×14 09:30 $87.40 → close $82.52 -68.32; RIG×217 09:30 $5.87 → close $5.54 -71.61; MRCY×14 09:30 $87.52 → close $87.25 -3.78; KRMN×33 09:30 $38.01 → close $36.94 -35.31 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $159.22 | ▲ 09:30 equity $10,161.12 vs yday $10,013.16 (+147.96) | 09:30 open · cash $159.22 (unchanged overnight, no fees) · equity $10,161.12 vs prior close $10,013.16 (+147.96) · 8 name(s) re-marked at the open (per-name table). FPS×38 yday $34.84 → 09:30 $36.76 +72.96; GFR×186 yday $6.49 → 09:30 $6.48 -1.86; FRO×24 yday $53.67 → 09:30 $54.31 +15.36; TALO×71 yday $17.42 → 09:30 $17.19 -16.33; VAL×14 yday $82.52 → 09:30 $83.20 +9.52; RIG×217 yday $5.54 → 09:30 $5.58 +8.68; MRCY×14 yday $87.25 → 09:30 $89.27 +28.28; KRMN×33 yday $36.94 → 09:30 $37.89 +31.35 | — |
| 2026-09-17 09:30 ET | **SELL** | `GFR` | 186 | $6.48 | $2.59 | $-70.24 | $1,361.91 | ▼ -70.24 after sell → book $10,158.53; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FRO` | 24 | $54.31 | $2.08 | $+38.82 | $2,663.27 | ▲ +38.82 after sell → book $10,156.45; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `TALO` | 71 | $17.19 | $2.22 | $-52.71 | $3,881.54 | ▼ -52.71 after sell → book $10,154.23; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 14 | $83.20 | $2.05 | $-62.88 | $5,044.28 | ▼ -62.88 after sell → book $10,152.17; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 217 | $5.58 | $2.85 | $-68.57 | $6,252.30 | ▼ -68.57 after sell → book $10,149.33; vs 09:30 mark -2.84 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `MRCY` | 14 | $89.27 | $2.05 | $+20.42 | $7,500.03 | ▲ +20.42 after sell → book $10,147.28; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `KRMN` | 33 | $37.89 | $2.11 | $-8.16 | $8,748.29 | ▼ -8.16 after sell → book $10,145.17; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BKV` | 96 | $22.75 | $2.28 | — | $6,562.01 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+10.8; leftover $2187.07 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `FTAI` | 11 | $196.50 | $2.02 | — | $4,398.49 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+2.5; leftover $2187.07 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `TK` | 151 | $14.41 | $2.44 | — | $2,220.13 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+6.8; leftover $2187.07 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `EROC` | 173 | $12.64 | $2.51 | — | $30.91 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-3.6; leftover $2187.07 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.91 | ▲ close $10,257.67 vs 09:30 $10,161.12 (session +121.75) | 16:00 close · cash $30.91 · equity $10,257.67 vs 09:30 $10,161.12 (+96.55; session marks +121.75) · 5 name(s) marked open→close (per-name table). FPS×38 09:30 $36.76 → close $38.06 +49.40; BKV×96 09:30 $22.75 → close $22.79 +3.84; FTAI×11 09:30 $196.50 → close $195.07 -15.73; TK×151 09:30 $14.41 → close $14.67 +39.26; EROC×173 09:30 $12.64 → close $12.90 +44.98 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.91 | ▲ 09:30 equity $10,336.88 vs yday $10,257.67 (+79.21) | 09:30 open · cash $30.91 (unchanged overnight, no fees) · equity $10,336.88 vs prior close $10,257.67 (+79.21) · 5 name(s) re-marked at the open (per-name table). FPS×38 yday $38.06 → 09:30 $39.50 +54.72; BKV×96 yday $22.79 → 09:30 $22.92 +12.48; FTAI×11 yday $195.07 → 09:30 $195.55 +5.28; TK×151 yday $14.67 → 09:30 $14.60 -10.57; EROC×173 yday $12.90 → 09:30 $13.00 +17.30 | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 38 | $39.50 | $2.13 | $+237.45 | $1,529.78 | ▲ +237.45 after sell → book $10,334.75; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BKV` | 96 | $22.92 | $2.31 | $+11.73 | $3,727.79 | ▲ +11.73 after sell → book $10,332.44; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `FTAI` | 11 | $195.55 | $2.05 | $-14.52 | $5,876.79 | ▼ -14.52 after sell → book $10,330.39; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TK` | 151 | $14.60 | $2.49 | $+23.76 | $8,078.90 | ▲ +23.76 after sell → book $10,327.90; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `EROC` | 173 | $13.00 | $2.56 | $+57.22 | $10,325.35 | ▲ +57.22 after sell → book $10,325.35; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 342 | $7.54 | $4.41 | — | $7,743.96 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-20.9; leftover $2581.34 | join🟡 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `USDE` | 270 | $9.54 | $3.48 | — | $5,164.68 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+15.8; leftover $2581.34 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `PURR` | 186 | $13.82 | $2.55 | — | $2,591.61 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=+16.5; leftover $2581.34 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `ARE` | 45 | $56.70 | $2.12 | — | $37.99 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+13.7; leftover $2581.34 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.99 | ▼ close $10,311.97 vs 09:30 $10,336.88 (session -0.81) | 16:00 close · cash $37.99 · equity $10,311.97 vs 09:30 $10,336.88 (-24.91; session marks -0.81) · 4 name(s) marked open→close (per-name table). FLNC×342 09:30 $7.54 → close $7.32 -73.53; USDE×270 09:30 $9.54 → close $10.19 +175.50; PURR×186 09:30 $13.82 → close $14.09 +50.22; ARE×45 09:30 $56.70 → close $53.30 -153.00 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.99 | ▲ 09:30 equity $11,206.06 vs yday $10,311.97 (+894.09) | 09:30 open · cash $37.99 (unchanged overnight, no fees) · equity $11,206.06 vs prior close $10,311.97 (+894.09) · 4 name(s) re-marked at the open (per-name table). FLNC×342 yday $7.32 → 09:30 $7.36 +13.68; USDE×270 yday $10.19 → 09:30 $13.05 +772.20; PURR×186 yday $14.09 → 09:30 $14.65 +104.16; ARE×45 yday $53.30 → 09:30 $53.39 +4.05 | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 342 | $7.36 | $4.49 | $-68.75 | $2,550.62 | ▼ -68.75 after sell → book $11,201.57; vs 09:30 mark -4.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `USDE` | 270 | $13.05 | $3.56 | $+940.66 | $6,070.56 | ▲ +940.66 after sell → book $11,198.01; vs 09:30 mark -3.56 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `PURR` | 186 | $14.65 | $2.60 | $+149.23 | $8,792.86 | ▲ +149.23 after sell → book $11,195.41; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARE` | 45 | $53.39 | $2.15 | $-153.23 | $11,193.26 | ▼ -153.23 after sell → book $11,193.26; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,193.26 | ▲ close $11,193.26 vs 09:30 $11,206.06 (session +0.00) | 16:00 close · cash $11,193.26 · no lots left · equity $11,193.26. | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,193.26 | ▲ 09:30 equity $11,193.26 vs yday $11,193.26 (-0.00) | 09:30 open · cash $11,193.26 · no holdings · equity $11,193.26 vs prior close $11,193.26 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,193.26 | ▲ close $11,193.26 vs 09:30 $11,193.26 (session +0.00) | 16:00 close · cash $11,193.26 · no lots left · equity $11,193.26. | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,193.26 | ▲ 09:30 equity $11,193.26 vs yday $11,193.26 (-0.00) | 09:30 open · cash $11,193.26 · no holdings · equity $11,193.26 vs prior close $11,193.26 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,193.26 | ▲ close $11,193.26 vs 09:30 $11,193.26 (session +0.00) | 16:00 close · cash $11,193.26 · no lots left · equity $11,193.26. | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,193.26 | ▲ 09:30 equity $11,193.26 vs yday $11,193.26 (-0.00) | 09:30 open · cash $11,193.26 · no holdings · equity $11,193.26 vs prior close $11,193.26 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,193.26 | ▲ close $11,193.26 vs 09:30 $11,193.26 (session +0.00) | 16:00 close · cash $11,193.26 · no lots left · equity $11,193.26. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-24 | `DK` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `UEC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FNKO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MMED` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WEN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `AGCO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `METC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TSLA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `SCZM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `USDE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `DYN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CHA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `IONS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SRPT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TYRA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `SMR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `QRVO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `KGS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INIO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PUMP` | hard_red | hard-red S=-3.84 sit; no new buys |
