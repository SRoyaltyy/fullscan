# Factor mine action — `union_clk_hold_vs_sector_opp_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `opp_rvol` · size `leftover` · sell `list` · S-boost `none` · Clock-B #6 ∩ Theme Radar T−1 oppset

Cash book **+8.41%** ($10,841) · signal-only (no cash/fees) was +6.29%. Starts YES **26/26**. Fills 141 · skips 77 · realized $+821.35.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $21.34.

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
| 2026-08-20 | `SUI` | 10 | — | $121.21 | +0.00 | $122.29 | +10.80 | +10.80 | +0.00 | +10.80 |
| 2026-08-20 | `ADC` | 16 | — | $74.37 | +0.00 | $74.45 | +1.28 | +1.28 | +0.00 | +1.28 |
| 2026-08-20 | `LZB` | 37 | — | $33.61 | +0.00 | $33.65 | +1.48 | +1.48 | +0.00 | +1.48 |
| 2026-08-20 | `ZIM` | 45 | — | $27.45 | +0.00 | $27.16 | -13.05 | -13.05 | +0.00 | -13.05 |
| 2026-08-20 | `CTRE` | 31 | — | $39.79 | +0.00 | $39.76 | -0.93 | -0.93 | +0.00 | -0.93 |
| 2026-08-20 | `ATAT` | 36 | — | $34.05 | +0.00 | $34.25 | +7.20 | +7.20 | +0.00 | +7.20 |
| 2026-08-20 | `SG` | 194 | — | $6.43 | +0.00 | $6.58 | +29.10 | +29.10 | +0.00 | +29.10 |
| 2026-08-20 | `HTHT` | 25 | — | $48.39 | +0.00 | $49.54 | +28.75 | +28.75 | +0.00 | +28.75 |
| 2026-08-21 | `SUI` | 10 | $122.29 | $122.41 | +1.20 | — | +0.00 | +1.20 | +12.00 | — |
| 2026-08-21 | `ADC` | 16 | $74.45 | $74.60 | +2.40 | — | +0.00 | +2.40 | +3.68 | — |
| 2026-08-21 | `LZB` | 37 | $33.65 | $33.63 | -0.74 | — | +0.00 | -0.74 | +0.74 | — |
| 2026-08-21 | `ZIM` | 45 | $27.16 | $27.50 | +15.30 | — | +0.00 | +15.30 | +2.25 | — |
| 2026-08-21 | `CTRE` | 31 | $39.76 | $40.00 | +7.44 | — | +0.00 | +7.44 | +6.51 | — |
| 2026-08-21 | `ATAT` | 36 | $34.25 | $34.31 | +2.16 | — | +0.00 | +2.16 | +9.36 | — |
| 2026-08-21 | `SG` | 194 | $6.58 | $6.61 | +5.82 | — | +0.00 | +5.82 | +34.92 | — |
| 2026-08-21 | `HTHT` | 25 | $49.54 | $49.58 | +1.00 | — | +0.00 | +1.00 | +29.75 | — |
| 2026-08-21 | `VIK` | 22 | — | $91.00 | +0.00 | $92.79 | +39.38 | +39.38 | +0.00 | +39.38 |
| 2026-08-21 | `BJ` | 21 | — | $93.98 | +0.00 | $96.42 | +51.24 | +51.24 | +0.00 | +51.24 |
| 2026-08-21 | `DE` | 3 | — | $623.26 | +0.00 | $647.47 | +72.63 | +72.63 | +0.00 | +72.63 |
| 2026-08-21 | `XXI` | 313 | — | $6.42 | +0.00 | $6.49 | +21.91 | +21.91 | +0.00 | +21.91 |
| 2026-08-21 | `SM` | 53 | — | $37.81 | +0.00 | $37.20 | -32.33 | -32.33 | +0.00 | -32.33 |
| 2026-08-24 | `VIK` | 22 | $92.79 | $93.06 | +5.94 | — | +0.00 | +5.94 | +45.32 | — |
| 2026-08-24 | `BJ` | 21 | $96.42 | $97.02 | +12.60 | — | +0.00 | +12.60 | +63.84 | — |
| 2026-08-24 | `DE` | 3 | $647.47 | $653.04 | +16.71 | — | +0.00 | +16.71 | +89.34 | — |
| 2026-08-24 | `XXI` | 313 | $6.49 | $6.64 | +48.51 | — | +0.00 | +48.51 | +70.42 | — |
| 2026-08-24 | `SM` | 53 | $37.20 | $36.61 | -31.27 | — | +0.00 | -31.27 | -63.60 | — |
| 2026-08-25 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-26 | `VIPS` | 91 | — | $14.00 | +0.00 | $14.08 | +7.28 | +7.28 | +0.00 | +7.28 |
| 2026-08-26 | `MAIR` | 46 | — | $27.59 | +0.00 | $28.51 | +42.32 | +42.32 | +0.00 | +42.32 |
| 2026-08-26 | `VNET` | 188 | — | $6.80 | +0.00 | $6.59 | -39.48 | -39.48 | +0.00 | -39.48 |
| 2026-08-26 | `SMTC` | 9 | — | $130.90 | +0.00 | $140.80 | +89.10 | +89.10 | +0.00 | +89.10 |
| 2026-08-26 | `MNRO` | 91 | — | $14.00 | +0.00 | $12.61 | -126.49 | -126.49 | +0.00 | -126.49 |
| 2026-08-26 | `GRRR` | 91 | — | $14.03 | +0.00 | $15.45 | +129.22 | +129.22 | +0.00 | +129.22 |
| 2026-08-26 | `NCNO` | 66 | — | $19.33 | +0.00 | $21.51 | +143.88 | +143.88 | +0.00 | +143.88 |
| 2026-08-26 | `BE` | 5 | — | $213.94 | +0.00 | $218.21 | +21.35 | +21.35 | +0.00 | +21.35 |
| 2026-08-27 | `VIPS` | 91 | $14.08 | $14.00 | -7.28 | — | +0.00 | -7.28 | +0.00 | — |
| 2026-08-27 | `MAIR` | 46 | $28.51 | $28.76 | +11.50 | — | +0.00 | +11.50 | +53.82 | — |
| 2026-08-27 | `VNET` | 188 | $6.59 | $6.73 | +26.32 | — | +0.00 | +26.32 | -13.16 | — |
| 2026-08-27 | `SMTC` | 9 | $140.80 | $149.40 | +77.40 | — | +0.00 | +77.40 | +166.50 | — |
| 2026-08-27 | `MNRO` | 91 | $12.61 | $12.56 | -4.55 | — | +0.00 | -4.55 | -131.04 | — |
| 2026-08-27 | `GRRR` | 91 | $15.45 | $15.94 | +44.59 | — | +0.00 | +44.59 | +173.81 | — |
| 2026-08-27 | `NCNO` | 66 | $21.51 | $22.03 | +34.32 | — | +0.00 | +34.32 | +178.20 | — |
| 2026-08-27 | `BE` | 5 | $218.21 | $227.10 | +44.45 | — | +0.00 | +44.45 | +65.80 | — |
| 2026-08-28 | `ANF` | 9 | — | $146.07 | +0.00 | $148.42 | +21.15 | +21.15 | +0.00 | +21.15 |
| 2026-08-28 | `LEG` | 145 | — | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-28 | `SLF` | 16 | — | $78.95 | +0.00 | $78.76 | -3.04 | -3.04 | +0.00 | -3.04 |
| 2026-08-28 | `EDU` | 23 | — | $57.63 | +0.00 | $58.83 | +27.60 | +27.60 | +0.00 | +27.60 |
| 2026-08-28 | `TRMD` | 41 | — | $32.23 | +0.00 | $32.62 | +15.99 | +15.99 | +0.00 | +15.99 |
| 2026-08-28 | `JAZZ` | 5 | — | $249.48 | +0.00 | $244.54 | -24.70 | -24.70 | +0.00 | -24.70 |
| 2026-08-28 | `KSS` | 73 | — | $18.25 | +0.00 | $17.50 | -54.75 | -54.75 | +0.00 | -54.75 |
| 2026-08-28 | `SYRE` | 14 | — | $91.75 | +0.00 | $90.36 | -19.46 | -19.46 | +0.00 | -19.46 |
| 2026-08-31 | `ANF` | 9 | $148.42 | $148.03 | -3.51 | — | +0.00 | -3.51 | +17.64 | — |
| 2026-08-31 | `LEG` | 145 | $9.20 | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-31 | `SLF` | 16 | $78.76 | $78.70 | -0.96 | — | +0.00 | -0.96 | -4.00 | — |
| 2026-08-31 | `EDU` | 23 | $58.83 | $58.23 | -13.80 | — | +0.00 | -13.80 | +13.80 | — |
| 2026-08-31 | `TRMD` | 41 | $32.62 | $33.09 | +19.27 | — | +0.00 | +19.27 | +35.26 | — |
| 2026-08-31 | `JAZZ` | 5 | $244.54 | $241.39 | -15.75 | — | +0.00 | -15.75 | -40.45 | — |
| 2026-08-31 | `KSS` | 73 | $17.50 | $17.26 | -17.52 | — | +0.00 | -17.52 | -72.27 | — |
| 2026-08-31 | `SYRE` | 14 | $90.36 | $89.15 | -16.94 | — | +0.00 | -16.94 | -36.40 | — |
| 2026-09-01 | `LEG` | 145 | $9.20 | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-02 | `LEG` | 145 | $9.20 | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `LEG` | 145 | $9.20 | $9.20 | +0.00 | $9.20 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `GBTG` | 121 | — | $9.49 | +0.00 | $9.49 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `GRNT` | 224 | — | $5.15 | +0.00 | $5.08 | -15.68 | -15.68 | +0.00 | -15.68 |
| 2026-09-03 | `ETD` | 52 | — | $21.82 | +0.00 | $21.87 | +2.60 | +2.60 | +0.00 | +2.60 |
| 2026-09-03 | `XP` | 55 | — | $20.74 | +0.00 | $20.00 | -40.70 | -40.70 | +0.00 | -40.70 |
| 2026-09-03 | `HP` | 24 | — | $47.74 | +0.00 | $45.02 | -65.28 | -65.28 | +0.00 | -65.28 |
| 2026-09-03 | `PBR` | 54 | — | $21.18 | +0.00 | $20.51 | -36.18 | -36.18 | +0.00 | -36.18 |
| 2026-09-03 | `VSXY` | 15 | — | $76.86 | +0.00 | $73.64 | -48.30 | -48.30 | +0.00 | -48.30 |
| 2026-09-03 | `PBR-A` | 60 | — | $19.16 | +0.00 | $18.58 | -34.80 | -34.80 | +0.00 | -34.80 |
| 2026-09-04 | `LEG` | 145 | $9.20 | $9.20 | +0.00 | — | +0.00 | +0.00 | +0.00 | — |
| 2026-09-04 | `GBTG` | 121 | $9.49 | $9.49 | +0.00 | — | +0.00 | +0.00 | +0.00 | — |
| 2026-09-04 | `GRNT` | 224 | $5.08 | $5.03 | -11.20 | — | +0.00 | -11.20 | -26.88 | — |
| 2026-09-04 | `ETD` | 52 | $21.87 | $21.84 | -1.56 | — | +0.00 | -1.56 | +1.04 | — |
| 2026-09-04 | `XP` | 55 | $20.00 | $19.67 | -18.15 | — | +0.00 | -18.15 | -58.85 | — |
| 2026-09-04 | `HP` | 24 | $45.02 | $44.59 | -10.32 | — | +0.00 | -10.32 | -75.60 | — |
| 2026-09-04 | `PBR` | 54 | $20.51 | $20.25 | -14.04 | — | +0.00 | -14.04 | -50.22 | — |
| 2026-09-04 | `VSXY` | 15 | $73.64 | $73.63 | -0.15 | — | +0.00 | -0.15 | -48.45 | — |
| 2026-09-04 | `PBR-A` | 60 | $18.58 | $18.36 | -13.20 | — | +0.00 | -13.20 | -48.00 | — |
| 2026-09-04 | `CHPT` | 137 | — | $9.28 | +0.00 | $9.89 | +83.57 | +83.57 | +0.00 | +83.57 |
| 2026-09-04 | `LULU` | 13 | — | $98.15 | +0.00 | $100.61 | +31.98 | +31.98 | +0.00 | +31.98 |
| 2026-09-04 | `SLGN` | 31 | — | $41.16 | +0.00 | $41.18 | +0.62 | +0.62 | +0.00 | +0.62 |
| 2026-09-04 | `PVH` | 17 | — | $72.79 | +0.00 | $74.33 | +26.18 | +26.18 | +0.00 | +26.18 |
| 2026-09-04 | `AUR` | 204 | — | $6.26 | +0.00 | $6.34 | +15.30 | +15.30 | +0.00 | +15.30 |
| 2026-09-04 | `MIR` | 77 | — | $16.60 | +0.00 | $16.93 | +25.41 | +25.41 | +0.00 | +25.41 |
| 2026-09-04 | `BE` | 5 | — | $236.82 | +0.00 | $252.87 | +80.25 | +80.25 | +0.00 | +80.25 |
| 2026-09-04 | `SCZM` | 127 | — | $10.03 | +0.00 | $9.94 | -11.43 | -11.43 | +0.00 | -11.43 |
| 2026-09-08 | `CHPT` | 137 | $9.89 | $9.91 | +2.74 | $9.37 | -73.98 | -71.24 | +86.31 | +12.33 |
| 2026-09-08 | `LULU` | 13 | $100.61 | $100.58 | -0.39 | $103.19 | +33.93 | +33.54 | +31.59 | +65.52 |
| 2026-09-08 | `SLGN` | 31 | $41.18 | $40.60 | -17.98 | — | +0.00 | -17.98 | -17.36 | — |
| 2026-09-08 | `PVH` | 17 | $74.33 | $74.50 | +2.89 | — | +0.00 | +2.89 | +29.07 | — |
| 2026-09-08 | `AUR` | 204 | $6.34 | $6.34 | +0.00 | — | +0.00 | +0.00 | +15.30 | — |
| 2026-09-08 | `MIR` | 77 | $16.93 | $17.07 | +10.78 | — | +0.00 | +10.78 | +36.19 | — |
| 2026-09-08 | `BE` | 5 | $252.87 | $267.76 | +74.45 | — | +0.00 | +74.45 | +154.70 | — |
| 2026-09-08 | `SCZM` | 127 | $9.94 | $9.90 | -5.08 | — | +0.00 | -5.08 | -16.51 | — |
| 2026-09-09 | `CHPT` | 137 | $9.37 | $9.39 | +2.74 | — | +0.00 | +2.74 | +15.07 | — |
| 2026-09-09 | `LULU` | 13 | $103.19 | $101.90 | -16.77 | — | +0.00 | -16.77 | +48.75 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `BKV` | 52 | — | $24.97 | +0.00 | $24.23 | -38.48 | -38.48 | +0.00 | -38.48 |
| 2026-09-11 | `GFR` | 211 | — | $6.19 | +0.00 | $6.52 | +69.63 | +69.63 | +0.00 | +69.63 |
| 2026-09-11 | `SHOE` | 104 | — | $12.51 | +0.00 | $12.71 | +20.80 | +20.80 | +0.00 | +20.80 |
| 2026-09-11 | `ACVA` | 125 | — | $10.46 | +0.00 | $10.41 | -5.62 | -5.62 | +0.00 | -5.62 |
| 2026-09-11 | `AVAV` | 8 | — | $145.91 | +0.00 | $146.71 | +6.40 | +6.40 | +0.00 | +6.40 |
| 2026-09-11 | `ENB` | 27 | — | $48.37 | +0.00 | $47.76 | -16.47 | -16.47 | +0.00 | -16.47 |
| 2026-09-11 | `M` | 63 | — | $20.71 | +0.00 | $22.08 | +86.31 | +86.31 | +0.00 | +86.31 |
| 2026-09-11 | `DBI` | 221 | — | $5.91 | +0.00 | $5.88 | -6.63 | -6.63 | +0.00 | -6.63 |
| 2026-09-14 | `BKV` | 52 | $24.23 | $24.26 | +1.56 | — | +0.00 | +1.56 | -36.92 | — |
| 2026-09-14 | `GFR` | 211 | $6.52 | $6.60 | +16.88 | — | +0.00 | +16.88 | +86.51 | — |
| 2026-09-14 | `SHOE` | 104 | $12.71 | $12.55 | -16.64 | — | +0.00 | -16.64 | +4.16 | — |
| 2026-09-14 | `ACVA` | 125 | $10.41 | $10.42 | +1.25 | — | +0.00 | +1.25 | -4.38 | — |
| 2026-09-14 | `AVAV` | 8 | $146.71 | $145.80 | -7.28 | $153.40 | +60.80 | +53.52 | -0.88 | +59.92 |
| 2026-09-14 | `ENB` | 27 | $47.76 | $47.85 | +2.43 | — | +0.00 | +2.43 | -14.04 | — |
| 2026-09-14 | `M` | 63 | $22.08 | $21.77 | -19.53 | — | +0.00 | -19.53 | +66.78 | — |
| 2026-09-14 | `DBI` | 221 | $5.88 | $5.86 | -4.42 | — | +0.00 | -4.42 | -11.05 | — |
| 2026-09-15 | `AVAV` | 8 | $153.40 | $152.27 | -9.04 | — | +0.00 | -9.04 | +50.88 | — |
| 2026-09-16 | `TRMD` | 36 | — | $35.90 | +0.00 | $36.60 | +25.20 | +25.20 | +0.00 | +25.20 |
| 2026-09-16 | `FPS` | 39 | — | $33.14 | +0.00 | $34.84 | +66.30 | +66.30 | +0.00 | +66.30 |
| 2026-09-16 | `SYY` | 16 | — | $79.73 | +0.00 | $78.71 | -16.32 | -16.32 | +0.00 | -16.32 |
| 2026-09-16 | `GFR` | 193 | — | $6.83 | +0.00 | $6.49 | -65.62 | -65.62 | +0.00 | -65.62 |
| 2026-09-16 | `MEOH` | 20 | — | $63.34 | +0.00 | $61.51 | -36.60 | -36.60 | +0.00 | -36.60 |
| 2026-09-16 | `GNW` | 132 | — | $10.00 | +0.00 | $10.09 | +11.88 | +11.88 | +0.00 | +11.88 |
| 2026-09-16 | `PBF` | 18 | — | $73.02 | +0.00 | $75.93 | +52.38 | +52.38 | +0.00 | +52.38 |
| 2026-09-16 | `FRO` | 25 | — | $52.52 | +0.00 | $53.67 | +28.75 | +28.75 | +0.00 | +28.75 |
| 2026-09-17 | `TRMD` | 36 | $36.60 | $36.52 | -2.88 | — | +0.00 | -2.88 | +22.32 | — |
| 2026-09-17 | `FPS` | 39 | $34.84 | $36.76 | +74.88 | $38.06 | +50.70 | +125.58 | +141.18 | +191.88 |
| 2026-09-17 | `SYY` | 16 | $78.71 | $79.03 | +5.12 | — | +0.00 | +5.12 | -11.20 | — |
| 2026-09-17 | `GFR` | 193 | $6.49 | $6.48 | -1.93 | — | +0.00 | -1.93 | -67.55 | — |
| 2026-09-17 | `MEOH` | 20 | $61.51 | $60.83 | -13.60 | — | +0.00 | -13.60 | -50.20 | — |
| 2026-09-17 | `GNW` | 132 | $10.09 | $10.12 | +3.96 | — | +0.00 | +3.96 | +15.84 | — |
| 2026-09-17 | `PBF` | 18 | $75.93 | $74.28 | -29.70 | — | +0.00 | -29.70 | +22.68 | — |
| 2026-09-17 | `FRO` | 25 | $53.67 | $54.31 | +16.00 | — | +0.00 | +16.00 | +44.75 | — |
| 2026-09-17 | `CBC` | 72 | — | $31.60 | +0.00 | $31.67 | +5.04 | +5.04 | +0.00 | +5.04 |
| 2026-09-17 | `FTAI` | 11 | — | $196.50 | +0.00 | $195.07 | -15.73 | -15.73 | +0.00 | -15.73 |
| 2026-09-17 | `TK` | 159 | — | $14.41 | +0.00 | $14.67 | +41.34 | +41.34 | +0.00 | +41.34 |
| 2026-09-17 | `EROC` | 182 | — | $12.64 | +0.00 | $12.90 | +47.32 | +47.32 | +0.00 | +47.32 |
| 2026-09-18 | `FPS` | 39 | $38.06 | $39.50 | +56.16 | — | +0.00 | +56.16 | +248.04 | — |
| 2026-09-18 | `CBC` | 72 | $31.67 | $31.64 | -2.16 | — | +0.00 | -2.16 | +2.88 | — |
| 2026-09-18 | `FTAI` | 11 | $195.07 | $195.55 | +5.28 | — | +0.00 | +5.28 | -10.45 | — |
| 2026-09-18 | `TK` | 159 | $14.67 | $14.60 | -11.13 | — | +0.00 | -11.13 | +30.21 | — |
| 2026-09-18 | `EROC` | 182 | $12.90 | $13.00 | +18.20 | — | +0.00 | +18.20 | +65.52 | — |
| 2026-09-18 | `FLNC` | 287 | — | $7.54 | +0.00 | $7.32 | -61.70 | -61.70 | +0.00 | -61.70 |
| 2026-09-18 | `MNR` | 197 | — | $10.95 | +0.00 | $11.13 | +35.46 | +35.46 | +0.00 | +35.46 |
| 2026-09-18 | `USDE` | 226 | — | $9.54 | +0.00 | $10.19 | +146.90 | +146.90 | +0.00 | +146.90 |
| 2026-09-18 | `PURR` | 156 | — | $13.82 | +0.00 | $14.09 | +42.12 | +42.12 | +0.00 | +42.12 |
| 2026-09-18 | `ARE` | 38 | — | $56.70 | +0.00 | $53.30 | -129.20 | -129.20 | +0.00 | -129.20 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +72.53 | HTFL, VERA, CELC | — | $122.52 | $10,065.90 | HTFL×80, VERA×106, CELC×35 |
| 2026-08-18 | -6.20 | $122.52 | HTFL×80, VERA×106, CELC×35 | $9,994.68 | -71.22 | +0.00 | — | HTFL, VERA, CELC | $9,987.92 | $9,987.92 | — |
| 2026-08-19 | -7.20 | $9,987.92 | — | $9,987.92 | +0.00 | +0.00 | — | — | $9,987.92 | $9,987.92 | — |
| 2026-08-20 | +1.12 | $9,987.92 | — | $9,987.92 | +0.00 | +64.63 | SUI, ADC, LZB, ZIM, CTRE, ATAT, SG, HTHT | — | $173.52 | $10,035.45 | SUI×10, ADC×16, LZB×37, ZIM×45, CTRE×31, ATAT×36, SG×194, HTHT×25 |
| 2026-08-21 | +3.25 | $173.52 | SUI×10, ADC×16, LZB×37, ZIM×45, CTRE×31, ATAT×36, SG×194, HTHT×25 | $10,070.03 | +34.58 | +152.83 | VIK, BJ, DE, XXI, SM | SUI, ADC, LZB, ZIM, CTRE, ATAT, SG, HTHT | $181.70 | $10,193.28 | VIK×22, BJ×21, DE×3, XXI×313, SM×53 |
| 2026-08-24 | -5.17 | $181.70 | VIK×22, BJ×21, DE×3, XXI×313, SM×53 | $10,245.78 | +52.50 | +0.00 | — | VIK, BJ, DE, XXI, SM | $10,233.31 | $10,233.31 | — |
| 2026-08-25 | +1.80 | $10,233.31 | — | $10,233.31 | +0.00 | +0.00 | — | — | $10,233.31 | $10,233.31 | — |
| 2026-08-26 | +2.02 | $10,233.31 | — | $10,233.31 | +0.00 | +267.18 | VIPS, MAIR, VNET, SMTC, MNRO, GRRR, NCNO, BE | — | $319.78 | $10,482.81 | VIPS×91, MAIR×46, VNET×188, SMTC×9, MNRO×91, GRRR×91, NCNO×66, BE×5 |
| 2026-08-27 | — | $319.78 | VIPS×91, MAIR×46, VNET×188, SMTC×9, MNRO×91, GRRR×91, NCNO×66, BE×5 | $10,709.56 | +226.75 | +0.00 | — | VIPS, MAIR, VNET, SMTC, MNRO, GRRR, NCNO, BE | $10,691.68 | $10,691.68 | — |
| 2026-08-28 | +0.75 | $10,691.68 | — | $10,691.68 | -0.00 | -37.21 | ANF, LEG, SLF, EDU, TRMD, JAZZ, KSS, SYRE | — | $251.88 | $10,637.57 | ANF×9, LEG×145, SLF×16, EDU×23, TRMD×41, JAZZ×5, KSS×73, SYRE×14 |
| 2026-08-31 | -5.85 | $251.88 | ANF×9, LEG×145, SLF×16, EDU×23, TRMD×41, JAZZ×5, KSS×73, SYRE×14 | $10,588.36 | -49.21 | +0.00 | — | ANF, SLF, EDU, TRMD, JAZZ, KSS, SYRE | $9,239.74 | $10,573.74 | LEG×145 |
| 2026-09-01 | -6.30 | $9,239.74 | LEG×145 | $10,573.74 | +0.00 | +0.00 | — | — | $9,239.74 | $10,573.74 | LEG×145 |
| 2026-09-02 | -3.83 | $9,239.74 | LEG×145 | $10,573.74 | +0.00 | +0.00 | — | — | $9,239.74 | $10,573.74 | LEG×145 |
| 2026-09-03 | -0.90 | $9,239.74 | LEG×145 | $10,573.74 | +0.00 | -238.34 | GBTG, GRNT, ETD, XP, HP, PBR, VSXY, PBR-A | — | $52.57 | $10,317.44 | LEG×145, GBTG×121, GRNT×224, ETD×52, XP×55, HP×24, PBR×54, VSXY×15, PBR-A×60 |
| 2026-09-04 | +2.25 | $52.57 | LEG×145, GBTG×121, GRNT×224, ETD×52, XP×55, HP×24, PBR×54, VSXY×15, PBR-A×60 | $10,248.82 | -68.62 | +251.88 | CHPT, LULU, SLGN, PVH, AUR, MIR, BE, SCZM | LEG, GBTG, GRNT, ETD, XP, HP, PBR, VSXY, PBR-A | $135.55 | $10,462.30 | CHPT×137, LULU×13, SLGN×31, PVH×17, AUR×204, MIR×77, BE×5, SCZM×127 |
| 2026-09-08 | -11.47 | $135.55 | CHPT×137, LULU×13, SLGN×31, PVH×17, AUR×204, MIR×77, BE×5, SCZM×127 | $10,529.71 | +67.41 | -40.05 | — | SLGN, PVH, AUR, MIR, BE, SCZM | $7,850.98 | $10,476.14 | CHPT×137, LULU×13 |
| 2026-09-09 | -13.95 | $7,850.98 | CHPT×137, LULU×13 | $10,462.11 | -14.03 | +0.00 | — | CHPT, LULU | $10,457.63 | $10,457.63 | — |
| 2026-09-10 | -13.28 | $10,457.63 | — | $10,457.63 | +0.00 | +0.00 | — | — | $10,457.63 | $10,457.63 | — |
| 2026-09-11 | +0.50 | $10,457.63 | — | $10,457.63 | +0.00 | +115.94 | BKV, GFR, SHOE, ACVA, AVAV, ENB, M, DBI | — | $142.43 | $10,554.92 | BKV×52, GFR×211, SHOE×104, ACVA×125, AVAV×8, ENB×27, M×63, DBI×221 |
| 2026-09-14 | -11.00 | $142.43 | BKV×52, GFR×211, SHOE×104, ACVA×125, AVAV×8, ENB×27, M×63, DBI×221 | $10,529.17 | -25.75 | +60.80 | — | BKV, GFR, SHOE, ACVA, ENB, M, DBI | $9,345.92 | $10,573.12 | AVAV×8 |
| 2026-09-15 | -3.84 | $9,345.92 | AVAV×8 | $10,564.08 | -9.04 | +0.00 | — | AVAV | $10,562.04 | $10,562.04 | — |
| 2026-09-16 | +5.30 | $10,562.04 | — | $10,562.04 | +0.00 | +65.97 | TRMD, FPS, SYY, GFR, MEOH, GNW, PBF, FRO | — | $151.79 | $10,610.65 | TRMD×36, FPS×39, SYY×16, GFR×193, MEOH×20, GNW×132, PBF×18, FRO×25 |
| 2026-09-17 | +7.38 | $151.79 | TRMD×36, FPS×39, SYY×16, GFR×193, MEOH×20, GNW×132, PBF×18, FRO×25 | $10,662.50 | +51.85 | +128.67 | CBC, FTAI, TK, EROC | TRMD, SYY, GFR, MEOH, GNW, PBF, FRO | $175.84 | $10,766.52 | FPS×39, CBC×72, FTAI×11, TK×159, EROC×182 |
| 2026-09-18 | +4.86 | $175.84 | FPS×39, CBC×72, FTAI×11, TK×159, EROC×182 | $10,832.87 | +66.35 | +33.58 | FLNC, MNR, USDE, PURR, ARE | FPS, CBC, FTAI, TK, EROC | $21.34 | $10,841.17 | FLNC×287, MNR×197, USDE×226, PURR×156, ARE×38 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 80 | $41.23 | $2.23 | — | $6,699.37 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; ret5=+46.0; leftover $3333.33 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 106 | $31.30 | $2.31 | — | $3,379.26 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list probable,yday_gainer,oppset; ret5=-3.8; leftover $3333.33 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 35 | $92.99 | $2.10 | — | $122.52 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-0.8; leftover $3333.33 | join🟡 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.52 | ▲ close $10,065.90 vs 09:30 $10,000.00 (session +72.53) | 16:00 close · cash $122.52 · equity $10,065.90 vs 09:30 $10,000.00 (+65.90; session marks +72.53) · 3 name(s) marked open→close (per-name table). HTFL×80 09:30 $41.23 → close $41.94 +56.80; VERA×106 09:30 $31.30 → close $31.63 +34.98; CELC×35 09:30 $92.99 → close $92.44 -19.25 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.52 | ▼ 09:30 equity $9,994.68 vs yday $10,065.90 (-71.22) | 09:30 open · cash $122.52 (unchanged overnight, no fees) · equity $9,994.68 vs prior close $10,065.90 (-71.22) · 3 name(s) re-marked at the open (per-name table). HTFL×80 yday $41.94 → 09:30 $41.50 -35.20; VERA×106 yday $31.63 → 09:30 $31.31 -33.92; CELC×35 yday $92.44 → 09:30 $92.38 -2.10 | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 80 | $41.50 | $2.27 | $+17.10 | $3,440.25 | ▲ +17.10 after sell → book $9,992.41; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 106 | $31.31 | $2.35 | $-3.60 | $6,756.76 | ▼ -3.60 after sell → book $9,990.06; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 35 | $92.38 | $2.13 | $-25.58 | $9,987.92 | ▼ -25.58 after sell → book $9,987.92; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,987.92 | ▲ close $9,987.92 vs 09:30 $9,994.68 (session +0.00) | 16:00 close · cash $9,987.92 · no lots left · equity $9,987.92. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,987.92 | ▲ 09:30 equity $9,987.92 vs yday $9,987.92 (+0.00) | 09:30 open · cash $9,987.92 · no holdings · equity $9,987.92 vs prior close $9,987.92 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,987.92 | ▲ close $9,987.92 vs 09:30 $9,987.92 (session +0.00) | 16:00 close · cash $9,987.92 · no lots left · equity $9,987.92. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,987.92 | ▲ 09:30 equity $9,987.92 vs yday $9,987.92 (+0.00) | 09:30 open · cash $9,987.92 · no holdings · equity $9,987.92 vs prior close $9,987.92 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `SUI` | 10 | $121.21 | $2.02 | — | $8,773.80 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+3.1; leftover $1248.49 | join🔴 sector🔴 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ADC` | 16 | $74.37 | $2.04 | — | $7,581.85 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.0; leftover $1248.49 | join🟡 sector🔴 gen🟢 news🟡 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `LZB` | 37 | $33.61 | $2.10 | — | $6,336.18 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_mover,oppset; 🔵; ret5=-17.4; leftover $1248.49 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZIM` | 45 | $27.45 | $2.12 | — | $5,098.80 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+8.5; leftover $1248.49 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CTRE` | 31 | $39.79 | $2.08 | — | $3,863.23 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+1.7; leftover $1248.49 | join🟢 sector🔴 gen🟢 news🟡 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 36 | $34.05 | $2.10 | — | $2,635.33 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+9.3; leftover $1248.49 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SG` | 194 | $6.43 | $2.57 | — | $1,385.34 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+10.3; leftover $1248.49 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HTHT` | 25 | $48.39 | $2.06 | — | $173.52 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+17.8; leftover $1248.49 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $173.52 | ▲ close $10,035.45 vs 09:30 $9,987.92 (session +64.63) | 16:00 close · cash $173.52 · equity $10,035.45 vs 09:30 $9,987.92 (+47.53; session marks +64.63) · 8 name(s) marked open→close (per-name table). SUI×10 09:30 $121.21 → close $122.29 +10.80; ADC×16 09:30 $74.37 → close $74.45 +1.28; LZB×37 09:30 $33.61 → close $33.65 +1.48; ZIM×45 09:30 $27.45 → close $27.16 -13.05; CTRE×31 09:30 $39.79 → close $39.76 -0.93; ATAT×36 09:30 $34.05 → close $34.25 +7.20; SG×194 09:30 $6.43 → close $6.58 +29.10; HTHT×25 09:30 $48.39 → close $49.54 +28.75 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.52 | ▲ 09:30 equity $10,070.03 vs yday $10,035.45 (+34.58) | 09:30 open · cash $173.52 (unchanged overnight, no fees) · equity $10,070.03 vs prior close $10,035.45 (+34.58) · 8 name(s) re-marked at the open (per-name table). SUI×10 yday $122.29 → 09:30 $122.41 +1.20; ADC×16 yday $74.45 → 09:30 $74.60 +2.40; LZB×37 yday $33.65 → 09:30 $33.63 -0.74; ZIM×45 yday $27.16 → 09:30 $27.50 +15.30; CTRE×31 yday $39.76 → 09:30 $40.00 +7.44; ATAT×36 yday $34.25 → 09:30 $34.31 +2.16; SG×194 yday $6.58 → 09:30 $6.61 +5.82; HTHT×25 yday $49.54 → 09:30 $49.58 +1.00 | — |
| 2026-08-21 09:30 ET | **SELL** | `SUI` | 10 | $122.41 | $2.04 | $+7.94 | $1,395.58 | ▲ +7.94 after sell → book $10,067.99; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ADC` | 16 | $74.60 | $2.06 | $-0.42 | $2,587.12 | ▼ -0.42 after sell → book $10,065.93; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `LZB` | 37 | $33.63 | $2.12 | $-3.48 | $3,829.31 | ▼ -3.48 after sell → book $10,063.81; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZIM` | 45 | $27.50 | $2.15 | $-2.02 | $5,064.67 | ▼ -2.02 after sell → book $10,061.67; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CTRE` | 31 | $40.00 | $2.10 | $+2.32 | $6,302.57 | ▲ +2.32 after sell → book $10,059.57; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 36 | $34.31 | $2.12 | $+5.14 | $7,535.61 | ▲ +5.14 after sell → book $10,057.45; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SG` | 194 | $6.61 | $2.61 | $+29.73 | $8,815.33 | ▲ +29.73 after sell → book $10,054.83; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HTHT` | 25 | $49.58 | $2.08 | $+25.60 | $10,052.75 | ▲ +25.60 after sell → book $10,052.75; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `VIK` | 22 | $91.00 | $2.06 | — | $8,048.69 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=-14.7; leftover $2010.55 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 21 | $93.98 | $2.05 | — | $6,073.06 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list earn_react; ret5=-2.4; leftover $2010.55 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 3 | $623.26 | $2.00 | — | $4,201.28 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $2010.55 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XXI` | 313 | $6.42 | $4.04 | — | $2,187.78 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=+23.8; leftover $2010.55 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `SM` | 53 | $37.81 | $2.15 | — | $181.70 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+16.1; leftover $2010.55 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $181.70 | ▲ close $10,193.28 vs 09:30 $10,070.03 (session +152.83) | 16:00 close · cash $181.70 · equity $10,193.28 vs 09:30 $10,070.03 (+123.25; session marks +152.83) · 5 name(s) marked open→close (per-name table). VIK×22 09:30 $91.00 → close $92.79 +39.38; BJ×21 09:30 $93.98 → close $96.42 +51.24; DE×3 09:30 $623.26 → close $647.47 +72.63; XXI×313 09:30 $6.42 → close $6.49 +21.91; SM×53 09:30 $37.81 → close $37.20 -32.33 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $181.70 | ▲ 09:30 equity $10,245.78 vs yday $10,193.28 (+52.50) | 09:30 open · cash $181.70 (unchanged overnight, no fees) · equity $10,245.78 vs prior close $10,193.28 (+52.50) · 5 name(s) re-marked at the open (per-name table). VIK×22 yday $92.79 → 09:30 $93.06 +5.94; BJ×21 yday $96.42 → 09:30 $97.02 +12.60; DE×3 yday $647.47 → 09:30 $653.04 +16.71; XXI×313 yday $6.49 → 09:30 $6.64 +48.51; SM×53 yday $37.20 → 09:30 $36.61 -31.27 | — |
| 2026-08-24 09:30 ET | **SELL** | `VIK` | 22 | $93.06 | $2.08 | $+41.18 | $2,226.94 | ▲ +41.18 after sell → book $10,243.70; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 21 | $97.02 | $2.08 | $+59.71 | $4,262.28 | ▲ +59.71 after sell → book $10,241.62; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 3 | $653.04 | $2.02 | $+85.32 | $6,219.38 | ▲ +85.32 after sell → book $10,239.59; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XXI` | 313 | $6.64 | $4.11 | $+62.28 | $8,295.16 | ▲ +62.28 after sell → book $10,235.49; vs 09:30 mark -4.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `SM` | 53 | $36.61 | $2.17 | $-67.92 | $10,233.31 | ▼ -67.92 after sell → book $10,233.31; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,233.31 | ▲ close $10,233.31 vs 09:30 $10,245.78 (session +0.00) | 16:00 close · cash $10,233.31 · no lots left · equity $10,233.31. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,233.31 | ▲ 09:30 equity $10,233.31 vs yday $10,233.31 (+0.00) | 09:30 open · cash $10,233.31 · no holdings · equity $10,233.31 vs prior close $10,233.31 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,233.31 | ▲ close $10,233.31 vs 09:30 $10,233.31 (session +0.00) | 16:00 close · cash $10,233.31 · no lots left · equity $10,233.31. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,233.31 | ▲ 09:30 equity $10,233.31 vs yday $10,233.31 (+0.00) | 09:30 open · cash $10,233.31 · no holdings · equity $10,233.31 vs prior close $10,233.31 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 09:30 ET | **BUY** | `VIPS` | 91 | $14.00 | $2.26 | — | $8,957.05 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=-0.4; leftover $1279.16 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `MAIR` | 46 | $27.59 | $2.13 | — | $7,685.78 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer,oppset; ret5=+2.0; leftover $1279.16 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `VNET` | 188 | $6.80 | $2.55 | — | $6,404.83 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=+4.7; leftover $1279.16 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SMTC` | 9 | $130.90 | $2.02 | — | $5,224.71 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list earn_react,oppset; 🔵; ret5=-5.7; leftover $1279.16 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 91 | $14.00 | $2.26 | — | $3,948.45 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+17.8; leftover $1279.16 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `GRRR` | 91 | $14.03 | $2.26 | — | $2,669.45 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-7.6; leftover $1279.16 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NCNO` | 66 | $19.33 | $2.19 | — | $1,391.48 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list earn_react; ret5=+3.0; leftover $1279.16 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 5 | $213.94 | $2.00 | — | $319.78 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1279.16 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $319.78 | ▲ close $10,482.81 vs 09:30 $10,233.31 (session +267.18) | 16:00 close · cash $319.78 · equity $10,482.81 vs 09:30 $10,233.31 (+249.50; session marks +267.18) · 8 name(s) marked open→close (per-name table). VIPS×91 09:30 $14.00 → close $14.08 +7.28; MAIR×46 09:30 $27.59 → close $28.51 +42.32; VNET×188 09:30 $6.80 → close $6.59 -39.48; SMTC×9 09:30 $130.90 → close $140.80 +89.10; MNRO×91 09:30 $14.00 → close $12.61 -126.49; GRRR×91 09:30 $14.03 → close $15.45 +129.22; NCNO×66 09:30 $19.33 → close $21.51 +143.88; BE×5 09:30 $213.94 → close $218.21 +21.35 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $319.78 | ▲ 09:30 equity $10,709.56 vs yday $10,482.81 (+226.75) | 09:30 open · cash $319.78 (unchanged overnight, no fees) · equity $10,709.56 vs prior close $10,482.81 (+226.75) · 8 name(s) re-marked at the open (per-name table). VIPS×91 yday $14.08 → 09:30 $14.00 -7.28; MAIR×46 yday $28.51 → 09:30 $28.76 +11.50; VNET×188 yday $6.59 → 09:30 $6.73 +26.32; SMTC×9 yday $140.80 → 09:30 $149.40 +77.40; MNRO×91 yday $12.61 → 09:30 $12.56 -4.55; GRRR×91 yday $15.45 → 09:30 $15.94 +44.59; NCNO×66 yday $21.51 → 09:30 $22.03 +34.32; BE×5 yday $218.21 → 09:30 $227.10 +44.45 | — |
| 2026-08-27 09:30 ET | **SELL** | `VIPS` | 91 | $14.00 | $2.29 | $-4.55 | $1,591.49 | ▼ -4.55 after sell → book $10,707.27; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MAIR` | 46 | $28.76 | $2.15 | $+49.54 | $2,912.30 | ▲ +49.54 after sell → book $10,705.12; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `VNET` | 188 | $6.73 | $2.60 | $-18.31 | $4,174.95 | ▼ -18.31 after sell → book $10,702.53; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SMTC` | 9 | $149.40 | $2.04 | $+162.45 | $5,517.51 | ▲ +162.45 after sell → book $10,700.49; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `MNRO` | 91 | $12.56 | $2.29 | $-135.59 | $6,658.18 | ▼ -135.59 after sell → book $10,698.20; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GRRR` | 91 | $15.94 | $2.29 | $+169.26 | $8,106.43 | ▲ +169.26 after sell → book $10,695.91; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `NCNO` | 66 | $22.03 | $2.21 | $+173.80 | $9,558.20 | ▲ +173.80 after sell → book $10,693.70; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `BE` | 5 | $227.10 | $2.02 | $+61.77 | $10,691.68 | ▲ +61.77 after sell → book $10,691.68; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,691.68 | ▲ close $10,691.68 vs 09:30 $10,709.56 (session +0.00) | 16:00 close · cash $10,691.68 · no lots left · equity $10,691.68. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,691.68 | ▲ 09:30 equity $10,691.68 vs yday $10,691.68 (-0.00) | 09:30 open · cash $10,691.68 · no holdings · equity $10,691.68 vs prior close $10,691.68 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $9,375.03 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; 🔵; ret5=+38.8; leftover $1336.46 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LEG` | 145 | $9.20 | $2.42 | — | $8,038.60 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=-2.6; leftover $1336.46 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SLF` | 16 | $78.95 | $2.04 | — | $6,773.37 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=+0.4; leftover $1336.46 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `EDU` | 23 | $57.63 | $2.06 | — | $5,445.82 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=+5.5; leftover $1336.46 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TRMD` | 41 | $32.23 | $2.11 | — | $4,122.27 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+2.4; leftover $1336.46 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `JAZZ` | 5 | $249.48 | $2.00 | — | $2,872.87 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=+1.0; leftover $1336.46 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KSS` | 73 | $18.25 | $2.21 | — | $1,538.41 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=+4.7; leftover $1336.46 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟡 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SYRE` | 14 | $91.75 | $2.03 | — | $251.88 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_mover,oppset; ret5=-13.2; leftover $1336.46 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $251.88 | ▼ close $10,637.57 vs 09:30 $10,691.68 (session -37.21) | 16:00 close · cash $251.88 · equity $10,637.57 vs 09:30 $10,691.68 (-54.11; session marks -37.21) · 8 name(s) marked open→close (per-name table). ANF×9 09:30 $146.07 → close $148.42 +21.15; LEG×145 09:30 $9.20 → close $9.20 +0.00; SLF×16 09:30 $78.95 → close $78.76 -3.04; EDU×23 09:30 $57.63 → close $58.83 +27.60; TRMD×41 09:30 $32.23 → close $32.62 +15.99; JAZZ×5 09:30 $249.48 → close $244.54 -24.70; KSS×73 09:30 $18.25 → close $17.50 -54.75; SYRE×14 09:30 $91.75 → close $90.36 -19.46 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $251.88 | ▼ 09:30 equity $10,588.36 vs yday $10,637.57 (-49.21) | 09:30 open · cash $251.88 (unchanged overnight, no fees) · equity $10,588.36 vs prior close $10,637.57 (-49.21) · 8 name(s) re-marked at the open (per-name table). ANF×9 yday $148.42 → 09:30 $148.03 -3.51; LEG×145 yday $9.20 → 09:30 $9.20 +0.00; SLF×16 yday $78.76 → 09:30 $78.70 -0.96; EDU×23 yday $58.83 → 09:30 $58.23 -13.80; TRMD×41 yday $32.62 → 09:30 $33.09 +19.27; JAZZ×5 yday $244.54 → 09:30 $241.39 -15.75; KSS×73 yday $17.50 → 09:30 $17.26 -17.52; SYRE×14 yday $90.36 → 09:30 $89.15 -16.94 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $1,582.11 | ▲ +13.59 after sell → book $10,586.32; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLF` | 16 | $78.70 | $2.06 | $-8.10 | $2,839.25 | ▼ -8.10 after sell → book $10,584.26; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `EDU` | 23 | $58.23 | $2.08 | $+9.66 | $4,176.46 | ▲ +9.66 after sell → book $10,582.18; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TRMD` | 41 | $33.09 | $2.13 | $+31.01 | $5,531.02 | ▲ +31.01 after sell → book $10,580.05; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `JAZZ` | 5 | $241.39 | $2.02 | $-44.48 | $6,735.94 | ▼ -44.48 after sell → book $10,578.02; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KSS` | 73 | $17.26 | $2.23 | $-76.71 | $7,993.69 | ▼ -76.71 after sell → book $10,575.79; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SYRE` | 14 | $89.15 | $2.05 | $-40.48 | $9,239.74 | ▼ -40.48 after sell → book $10,573.74; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,239.74 | ▲ close $10,573.74 vs 09:30 $10,588.36 (session +0.00) | 16:00 close · cash $9,239.74 · equity $10,573.74 vs 09:30 $10,588.36 (-14.62; session marks +0.00) · 1 name(s) marked open→close (per-name table). LEG×145 09:30 $9.20 → close $9.20 +0.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,239.74 | ▲ 09:30 equity $10,573.74 vs yday $10,573.74 (+0.00) | 09:30 open · cash $9,239.74 (unchanged overnight, no fees) · equity $10,573.74 vs prior close $10,573.74 (+0.00) · 1 name(s) re-marked at the open (per-name table). LEG×145 yday $9.20 → 09:30 $9.20 +0.00 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,239.74 | ▲ close $10,573.74 vs 09:30 $10,573.74 (session +0.00) | 16:00 close · cash $9,239.74 · equity $10,573.74 vs 09:30 $10,573.74 (+0.00; session marks +0.00) · 1 name(s) marked open→close (per-name table). LEG×145 09:30 $9.20 → close $9.20 +0.00 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,239.74 | ▲ 09:30 equity $10,573.74 vs yday $10,573.74 (+0.00) | 09:30 open · cash $9,239.74 (unchanged overnight, no fees) · equity $10,573.74 vs prior close $10,573.74 (+0.00) · 1 name(s) re-marked at the open (per-name table). LEG×145 yday $9.20 → 09:30 $9.20 +0.00 | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,239.74 | ▲ close $10,573.74 vs 09:30 $10,573.74 (session +0.00) | 16:00 close · cash $9,239.74 · equity $10,573.74 vs 09:30 $10,573.74 (+0.00; session marks +0.00) · 1 name(s) marked open→close (per-name table). LEG×145 09:30 $9.20 → close $9.20 +0.00 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,239.74 | ▲ 09:30 equity $10,573.74 vs yday $10,573.74 (+0.00) | 09:30 open · cash $9,239.74 (unchanged overnight, no fees) · equity $10,573.74 vs prior close $10,573.74 (+0.00) · 1 name(s) re-marked at the open (per-name table). LEG×145 yday $9.20 → 09:30 $9.20 +0.00 | — |
| 2026-09-03 09:30 ET | **BUY** | `GBTG` | 121 | $9.49 | $2.35 | — | $8,089.10 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.2; leftover $1154.97 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GRNT` | 224 | $5.15 | $2.89 | — | $6,932.61 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=+3.2; leftover $1154.97 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ETD` | 52 | $21.82 | $2.15 | — | $5,795.82 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+4.7; leftover $1154.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `XP` | 55 | $20.74 | $2.15 | — | $4,652.97 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+12.2; leftover $1154.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HP` | 24 | $47.74 | $2.06 | — | $3,505.15 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+15.1; leftover $1154.97 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PBR` | 54 | $21.18 | $2.15 | — | $2,359.27 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+17.5; leftover $1154.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 15 | $76.86 | $2.04 | — | $1,204.34 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-6.6; leftover $1154.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PBR-A` | 60 | $19.16 | $2.17 | — | $52.57 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+17.4; leftover $1154.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.57 | ▼ close $10,317.44 vs 09:30 $10,573.74 (session -238.34) | 16:00 close · cash $52.57 · equity $10,317.44 vs 09:30 $10,573.74 (-256.30; session marks -238.34) · 9 name(s) marked open→close (per-name table). LEG×145 09:30 $9.20 → close $9.20 +0.00; GBTG×121 09:30 $9.49 → close $9.49 +0.00; GRNT×224 09:30 $5.15 → close $5.08 -15.68; ETD×52 09:30 $21.82 → close $21.87 +2.60; XP×55 09:30 $20.74 → close $20.00 -40.70; HP×24 09:30 $47.74 → close $45.02 -65.28; PBR×54 09:30 $21.18 → close $20.51 -36.18; VSXY×15 09:30 $76.86 → close $73.64 -48.30; PBR-A×60 09:30 $19.16 → close $18.58 -34.80 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.57 | ▼ 09:30 equity $10,248.82 vs yday $10,317.44 (-68.62) | 09:30 open · cash $52.57 (unchanged overnight, no fees) · equity $10,248.82 vs prior close $10,317.44 (-68.62) · 9 name(s) re-marked at the open (per-name table). LEG×145 yday $9.20 → 09:30 $9.20 +0.00; GBTG×121 yday $9.49 → 09:30 $9.49 +0.00; GRNT×224 yday $5.08 → 09:30 $5.03 -11.20; ETD×52 yday $21.87 → 09:30 $21.84 -1.56; XP×55 yday $20.00 → 09:30 $19.67 -18.15; HP×24 yday $45.02 → 09:30 $44.59 -10.32; PBR×54 yday $20.51 → 09:30 $20.25 -14.04; VSXY×15 yday $73.64 → 09:30 $73.63 -0.15; PBR-A×60 yday $18.58 → 09:30 $18.36 -13.20 | — |
| 2026-09-04 09:30 ET | **SELL** | `LEG` | 145 | $9.20 | $2.46 | $-4.88 | $1,384.11 | ▼ -4.88 after sell → book $10,246.36; vs 09:30 mark -2.46 | dropped from list after 5 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `GBTG` | 121 | $9.49 | $2.38 | $-4.74 | $2,530.02 | ▼ -4.74 after sell → book $10,243.98; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `GRNT` | 224 | $5.03 | $2.94 | $-32.71 | $3,653.80 | ▼ -32.71 after sell → book $10,241.04; vs 09:30 mark -2.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ETD` | 52 | $21.84 | $2.17 | $-3.27 | $4,787.31 | ▼ -3.27 after sell → book $10,238.87; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `XP` | 55 | $19.67 | $2.17 | $-63.18 | $5,866.99 | ▼ -63.18 after sell → book $10,236.70; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `HP` | 24 | $44.59 | $2.08 | $-79.74 | $6,935.07 | ▼ -79.74 after sell → book $10,234.62; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBR` | 54 | $20.25 | $2.17 | $-54.54 | $8,026.39 | ▼ -54.54 after sell → book $10,232.44; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSXY` | 15 | $73.63 | $2.06 | $-52.54 | $9,128.79 | ▼ -52.54 after sell → book $10,230.39; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `PBR-A` | 60 | $18.36 | $2.19 | $-52.36 | $10,228.20 | ▼ -52.36 after sell → book $10,228.20; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CHPT` | 137 | $9.28 | $2.40 | — | $8,954.44 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+55.7; leftover $1278.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 13 | $98.15 | $2.03 | — | $7,676.46 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list earn_react,oppset; ret5=+5.9; leftover $1278.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLGN` | 31 | $41.16 | $2.08 | — | $6,398.42 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=-0.8; leftover $1278.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `PVH` | 17 | $72.79 | $2.04 | — | $5,158.94 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=-4.9; leftover $1278.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AUR` | 204 | $6.26 | $2.63 | — | $3,878.25 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+11.1; leftover $1278.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MIR` | 77 | $16.60 | $2.22 | — | $2,597.83 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+12.9; leftover $1278.52 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 5 | $236.82 | $2.00 | — | $1,411.73 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+8.1; leftover $1278.52 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SCZM` | 127 | $10.03 | $2.37 | — | $135.55 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=+4.0; leftover $1278.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $135.55 | ▲ close $10,462.30 vs 09:30 $10,248.82 (session +251.88) | 16:00 close · cash $135.55 · equity $10,462.30 vs 09:30 $10,248.82 (+213.48; session marks +251.88) · 8 name(s) marked open→close (per-name table). CHPT×137 09:30 $9.28 → close $9.89 +83.57; LULU×13 09:30 $98.15 → close $100.61 +31.98; SLGN×31 09:30 $41.16 → close $41.18 +0.62; PVH×17 09:30 $72.79 → close $74.33 +26.18; AUR×204 09:30 $6.26 → close $6.34 +15.30; MIR×77 09:30 $16.60 → close $16.93 +25.41; BE×5 09:30 $236.82 → close $252.87 +80.25; SCZM×127 09:30 $10.03 → close $9.94 -11.43 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $135.55 | ▲ 09:30 equity $10,529.71 vs yday $10,462.30 (+67.41) | 09:30 open · cash $135.55 (unchanged overnight, no fees) · equity $10,529.71 vs prior close $10,462.30 (+67.41) · 8 name(s) re-marked at the open (per-name table). CHPT×137 yday $9.89 → 09:30 $9.91 +2.74; LULU×13 yday $100.61 → 09:30 $100.58 -0.39; SLGN×31 yday $41.18 → 09:30 $40.60 -17.98; PVH×17 yday $74.33 → 09:30 $74.50 +2.89; AUR×204 yday $6.34 → 09:30 $6.34 +0.00; MIR×77 yday $16.93 → 09:30 $17.07 +10.78; BE×5 yday $252.87 → 09:30 $267.76 +74.45; SCZM×127 yday $9.94 → 09:30 $9.90 -5.08 | — |
| 2026-09-08 09:30 ET | **SELL** | `SLGN` | 31 | $40.60 | $2.10 | $-21.55 | $1,392.04 | ▼ -21.55 after sell → book $10,527.60; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `PVH` | 17 | $74.50 | $2.06 | $+24.97 | $2,656.48 | ▲ +24.97 after sell → book $10,525.54; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AUR` | 204 | $6.34 | $2.68 | $+9.99 | $3,947.17 | ▲ +9.99 after sell → book $10,522.87; vs 09:30 mark -2.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MIR` | 77 | $17.07 | $2.24 | $+31.72 | $5,259.31 | ▲ +31.72 after sell → book $10,520.62; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 5 | $267.76 | $2.03 | $+150.67 | $6,596.09 | ▲ +150.67 after sell → book $10,518.60; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `SCZM` | 127 | $9.90 | $2.40 | $-21.28 | $7,850.98 | ▼ -21.28 after sell → book $10,516.19; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,850.98 | ▼ close $10,476.14 vs 09:30 $10,529.71 (session -40.05) | 16:00 close · cash $7,850.98 · equity $10,476.14 vs 09:30 $10,529.71 (-53.57; session marks -40.05) · 2 name(s) marked open→close (per-name table). CHPT×137 09:30 $9.91 → close $9.37 -73.98; LULU×13 09:30 $100.58 → close $103.19 +33.93 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,850.98 | ▼ 09:30 equity $10,462.11 vs yday $10,476.14 (-14.03) | 09:30 open · cash $7,850.98 (unchanged overnight, no fees) · equity $10,462.11 vs prior close $10,476.14 (-14.03) · 2 name(s) re-marked at the open (per-name table). CHPT×137 yday $9.37 → 09:30 $9.39 +2.74; LULU×13 yday $103.19 → 09:30 $101.90 -16.77 | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 137 | $9.39 | $2.43 | $+10.24 | $9,134.98 | ▲ +10.24 after sell → book $10,459.68; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-09 09:30 ET | **SELL** | `LULU` | 13 | $101.90 | $2.05 | $+44.67 | $10,457.63 | ▲ +44.67 after sell → book $10,457.63; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,457.63 | ▲ close $10,457.63 vs 09:30 $10,462.11 (session +0.00) | 16:00 close · cash $10,457.63 · no lots left · equity $10,457.63. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,457.63 | ▲ 09:30 equity $10,457.63 vs yday $10,457.63 (+0.00) | 09:30 open · cash $10,457.63 · no holdings · equity $10,457.63 vs prior close $10,457.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,457.63 | ▲ close $10,457.63 vs 09:30 $10,457.63 (session +0.00) | 16:00 close · cash $10,457.63 · no lots left · equity $10,457.63. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,457.63 | ▲ 09:30 equity $10,457.63 vs yday $10,457.63 (+0.00) | 09:30 open · cash $10,457.63 · no holdings · equity $10,457.63 vs prior close $10,457.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `BKV` | 52 | $24.97 | $2.15 | — | $9,157.04 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=-0.6; leftover $1307.20 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `GFR` | 211 | $6.19 | $2.72 | — | $7,848.23 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=+1.1; leftover $1307.20 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SHOE` | 104 | $12.51 | $2.30 | — | $6,544.89 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=-9.0; leftover $1307.20 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ACVA` | 125 | $10.46 | $2.37 | — | $5,235.65 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.0; leftover $1307.20 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AVAV` | 8 | $145.91 | $2.01 | — | $4,066.36 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+1.2; leftover $1307.20 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ENB` | 27 | $48.37 | $2.07 | — | $2,758.30 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=-0.2; leftover $1307.20 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `M` | 63 | $20.71 | $2.18 | — | $1,451.39 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=-8.6; leftover $1307.20 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 221 | $5.91 | $2.85 | — | $142.43 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover,ohlc_hot,oppset; ret5=+14.1; leftover $1307.20 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.43 | ▲ close $10,554.92 vs 09:30 $10,457.63 (session +115.94) | 16:00 close · cash $142.43 · equity $10,554.92 vs 09:30 $10,457.63 (+97.29; session marks +115.94) · 8 name(s) marked open→close (per-name table). BKV×52 09:30 $24.97 → close $24.23 -38.48; GFR×211 09:30 $6.19 → close $6.52 +69.63; SHOE×104 09:30 $12.51 → close $12.71 +20.80; ACVA×125 09:30 $10.46 → close $10.41 -5.62; AVAV×8 09:30 $145.91 → close $146.71 +6.40; ENB×27 09:30 $48.37 → close $47.76 -16.47; M×63 09:30 $20.71 → close $22.08 +86.31; DBI×221 09:30 $5.91 → close $5.88 -6.63 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.43 | ▼ 09:30 equity $10,529.17 vs yday $10,554.92 (-25.75) | 09:30 open · cash $142.43 (unchanged overnight, no fees) · equity $10,529.17 vs prior close $10,554.92 (-25.75) · 8 name(s) re-marked at the open (per-name table). BKV×52 yday $24.23 → 09:30 $24.26 +1.56; GFR×211 yday $6.52 → 09:30 $6.60 +16.88; SHOE×104 yday $12.71 → 09:30 $12.55 -16.64; ACVA×125 yday $10.41 → 09:30 $10.42 +1.25; AVAV×8 yday $146.71 → 09:30 $145.80 -7.28; ENB×27 yday $47.76 → 09:30 $47.85 +2.43; M×63 yday $22.08 → 09:30 $21.77 -19.53; DBI×221 yday $5.88 → 09:30 $5.86 -4.42 | — |
| 2026-09-14 09:30 ET | **SELL** | `BKV` | 52 | $24.26 | $2.17 | $-41.23 | $1,401.78 | ▼ -41.23 after sell → book $10,527.00; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `GFR` | 211 | $6.60 | $2.77 | $+81.02 | $2,791.61 | ▲ +81.02 after sell → book $10,524.23; vs 09:30 mark -2.77 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `SHOE` | 104 | $12.55 | $2.33 | $-0.47 | $4,094.48 | ▼ -0.47 after sell → book $10,521.90; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ACVA` | 125 | $10.42 | $2.40 | $-9.14 | $5,394.59 | ▼ -9.14 after sell → book $10,519.51; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `ENB` | 27 | $47.85 | $2.09 | $-18.20 | $6,684.44 | ▼ -18.20 after sell → book $10,517.41; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `M` | 63 | $21.77 | $2.20 | $+62.40 | $8,053.75 | ▲ +62.40 after sell → book $10,515.21; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DBI` | 221 | $5.86 | $2.90 | $-16.80 | $9,345.92 | ▼ -16.80 after sell → book $10,512.32; vs 09:30 mark -2.89 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,345.92 | ▲ close $10,573.12 vs 09:30 $10,529.17 (session +60.80) | 16:00 close · cash $9,345.92 · equity $10,573.12 vs 09:30 $10,529.17 (+43.95; session marks +60.80) · 1 name(s) marked open→close (per-name table). AVAV×8 09:30 $145.80 → close $153.40 +60.80 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,345.92 | ▼ 09:30 equity $10,564.08 vs yday $10,573.12 (-9.04) | 09:30 open · cash $9,345.92 (unchanged overnight, no fees) · equity $10,564.08 vs prior close $10,573.12 (-9.04) · 1 name(s) re-marked at the open (per-name table). AVAV×8 yday $153.40 → 09:30 $152.27 -9.04 | — |
| 2026-09-15 09:30 ET | **SELL** | `AVAV` | 8 | $152.27 | $2.03 | $+46.83 | $10,562.04 | ▲ +46.83 after sell → book $10,562.04; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,562.04 | ▲ close $10,562.04 vs 09:30 $10,564.08 (session +0.00) | 16:00 close · cash $10,562.04 · no lots left · equity $10,562.04. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,562.04 | ▲ 09:30 equity $10,562.04 vs yday $10,562.04 (+0.00) | 09:30 open · cash $10,562.04 · no holdings · equity $10,562.04 vs prior close $10,562.04 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `TRMD` | 36 | $35.90 | $2.10 | — | $9,267.54 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+2.6; leftover $1320.26 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 39 | $33.14 | $2.11 | — | $7,972.98 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer,oppset; 🔵; ret5=-2.9; leftover $1320.26 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SYY` | 16 | $79.73 | $2.04 | — | $6,695.26 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=-1.5; leftover $1320.26 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `GFR` | 193 | $6.83 | $2.57 | — | $5,374.50 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=+1.1; leftover $1320.26 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `MEOH` | 20 | $63.34 | $2.05 | — | $4,105.65 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+2.4; leftover $1320.26 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `GNW` | 132 | $10.00 | $2.39 | — | $2,783.26 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+4.8; leftover $1320.26 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `PBF` | 18 | $73.02 | $2.04 | — | $1,466.86 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=+3.9; leftover $1320.26 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `FRO` | 25 | $52.52 | $2.06 | — | $151.79 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+10.7; leftover $1320.26 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.79 | ▲ close $10,610.65 vs 09:30 $10,562.04 (session +65.97) | 16:00 close · cash $151.79 · equity $10,610.65 vs 09:30 $10,562.04 (+48.61; session marks +65.97) · 8 name(s) marked open→close (per-name table). TRMD×36 09:30 $35.90 → close $36.60 +25.20; FPS×39 09:30 $33.14 → close $34.84 +66.30; SYY×16 09:30 $79.73 → close $78.71 -16.32; GFR×193 09:30 $6.83 → close $6.49 -65.62; MEOH×20 09:30 $63.34 → close $61.51 -36.60; GNW×132 09:30 $10.00 → close $10.09 +11.88; PBF×18 09:30 $73.02 → close $75.93 +52.38; FRO×25 09:30 $52.52 → close $53.67 +28.75 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.79 | ▲ 09:30 equity $10,662.50 vs yday $10,610.65 (+51.85) | 09:30 open · cash $151.79 (unchanged overnight, no fees) · equity $10,662.50 vs prior close $10,610.65 (+51.85) · 8 name(s) re-marked at the open (per-name table). TRMD×36 yday $36.60 → 09:30 $36.52 -2.88; FPS×39 yday $34.84 → 09:30 $36.76 +74.88; SYY×16 yday $78.71 → 09:30 $79.03 +5.12; GFR×193 yday $6.49 → 09:30 $6.48 -1.93; MEOH×20 yday $61.51 → 09:30 $60.83 -13.60; GNW×132 yday $10.09 → 09:30 $10.12 +3.96; PBF×18 yday $75.93 → 09:30 $74.28 -29.70; FRO×25 yday $53.67 → 09:30 $54.31 +16.00 | — |
| 2026-09-17 09:30 ET | **SELL** | `TRMD` | 36 | $36.52 | $2.12 | $+18.10 | $1,464.40 | ▲ +18.10 after sell → book $10,660.39; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `SYY` | 16 | $79.03 | $2.06 | $-15.30 | $2,726.82 | ▼ -15.30 after sell → book $10,658.33; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `GFR` | 193 | $6.48 | $2.61 | $-72.73 | $3,974.85 | ▼ -72.73 after sell → book $10,655.72; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `MEOH` | 20 | $60.83 | $2.07 | $-54.32 | $5,189.38 | ▼ -54.32 after sell → book $10,653.65; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `GNW` | 132 | $10.12 | $2.42 | $+11.04 | $6,522.80 | ▲ +11.04 after sell → book $10,651.23; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `PBF` | 18 | $74.28 | $2.06 | $+18.57 | $7,857.77 | ▲ +18.57 after sell → book $10,649.16; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FRO` | 25 | $54.31 | $2.09 | $+40.60 | $9,213.44 | ▲ +40.60 after sell → book $10,647.08; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `CBC` | 72 | $31.60 | $2.21 | — | $6,936.03 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=+1.3; leftover $2303.36 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `FTAI` | 11 | $196.50 | $2.02 | — | $4,772.51 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+2.5; leftover $2303.36 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `TK` | 159 | $14.41 | $2.47 | — | $2,478.85 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+6.8; leftover $2303.36 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `EROC` | 182 | $12.64 | $2.54 | — | $175.84 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-3.6; leftover $2303.36 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $175.84 | ▲ close $10,766.52 vs 09:30 $10,662.50 (session +128.67) | 16:00 close · cash $175.84 · equity $10,766.52 vs 09:30 $10,662.50 (+104.02; session marks +128.67) · 5 name(s) marked open→close (per-name table). FPS×39 09:30 $36.76 → close $38.06 +50.70; CBC×72 09:30 $31.60 → close $31.67 +5.04; FTAI×11 09:30 $196.50 → close $195.07 -15.73; TK×159 09:30 $14.41 → close $14.67 +41.34; EROC×182 09:30 $12.64 → close $12.90 +47.32 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $175.84 | ▲ 09:30 equity $10,832.87 vs yday $10,766.52 (+66.35) | 09:30 open · cash $175.84 (unchanged overnight, no fees) · equity $10,832.87 vs prior close $10,766.52 (+66.35) · 5 name(s) re-marked at the open (per-name table). FPS×39 yday $38.06 → 09:30 $39.50 +56.16; CBC×72 yday $31.67 → 09:30 $31.64 -2.16; FTAI×11 yday $195.07 → 09:30 $195.55 +5.28; TK×159 yday $14.67 → 09:30 $14.60 -11.13; EROC×182 yday $12.90 → 09:30 $13.00 +18.20 | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 39 | $39.50 | $2.13 | $+243.80 | $1,714.21 | ▲ +243.80 after sell → book $10,830.74; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CBC` | 72 | $31.64 | $2.24 | $-1.56 | $3,990.05 | ▼ -1.56 after sell → book $10,828.50; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `FTAI` | 11 | $195.55 | $2.05 | $-14.52 | $6,139.05 | ▼ -14.52 after sell → book $10,826.45; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TK` | 159 | $14.60 | $2.51 | $+25.23 | $8,457.94 | ▲ +25.23 after sell → book $10,823.94; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `EROC` | 182 | $13.00 | $2.59 | $+60.40 | $10,821.35 | ▲ +60.40 after sell → book $10,821.35; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 287 | $7.54 | $3.70 | — | $8,655.11 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_mover,oppset; 🔵; ret5=-20.9; leftover $2164.27 | join🟡 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `MNR` | 197 | $10.95 | $2.58 | — | $6,495.37 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=+1.7; leftover $2164.27 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `USDE` | 226 | $9.54 | $2.92 | — | $4,336.42 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; ret5=+15.8; leftover $2164.27 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `PURR` | 156 | $13.82 | $2.46 | — | $2,178.04 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=-9.1; leftover $2164.27 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `ARE` | 38 | $56.70 | $2.10 | — | $21.34 | — | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+13.7; leftover $2164.27 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.34 | ▲ close $10,841.17 vs 09:30 $10,832.87 (session +33.58) | 16:00 close · cash $21.34 · equity $10,841.17 vs 09:30 $10,832.87 (+8.30; session marks +33.58) · 5 name(s) marked open→close (per-name table). FLNC×287 09:30 $7.54 → close $7.32 -61.70; MNR×197 09:30 $10.95 → close $11.13 +35.46; USDE×226 09:30 $9.54 → close $10.19 +146.90; PURR×156 09:30 $13.82 → close $14.09 +42.12; ARE×38 09:30 $56.70 → close $53.30 -129.20 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `KRNY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDAY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-24 | `AMX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `IQMX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRCT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DK` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `UEC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `LEG` | no_price | no 09:30 open — carry |
| 2026-08-31 | `SOLS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BEKE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SGI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CHA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HQY` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GPRE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FNKO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `LEG` | no_price | no 09:30 open — carry |
| 2026-09-01 | `CNH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CHA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CSGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PODD` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `LULU` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AGCO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `METC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `LEG` | no_price | no 09:30 open — carry |
| 2026-09-02 | `HASI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `IHS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `STDN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SCZM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `LEG` | no_price | no 09:30 open — carry |
| 2026-09-08 | `CHA` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BSBR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SU` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `USDE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `PHVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DYN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ROIV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CHA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `IONS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BEAM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TYRA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XYL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `DSGX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SMR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `AXGN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RH` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `KIM` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `OKLO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `PHVS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `BNC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ACVA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `AQN` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TAC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CVE` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GRNT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PBF` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FLNC` | 287 | 2026-09-18 @ $7.54 | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_mover,oppset; 🔵; ret5=-20.9; leftover $2164.27 |
| `MNR` | 197 | 2026-09-18 @ $10.95 | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list oppset; ret5=+1.7; leftover $2164.27 |
| `USDE` | 226 | 2026-09-18 @ $9.54 | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover,oppset; ret5=+15.8; leftover $2164.27 |
| `PURR` | 156 | 2026-09-18 @ $13.82 | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=-9.1; leftover $2164.27 |
| `ARE` | 38 | 2026-09-18 @ $56.70 | Clock-B #6 ∩ Theme Radar T−1 oppset; gate clk_hold_vs_sector=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+13.7; leftover $2164.27 |
