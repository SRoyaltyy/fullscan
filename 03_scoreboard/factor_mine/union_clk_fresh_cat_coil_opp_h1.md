# Factor mine action — `union_clk_fresh_cat_coil_opp_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `opp_rvol` · size `leftover` · sell `list` · S-boost `none` · Clock-B #2 ∩ Theme Radar T−1 oppset

Cash book **+2.24%** ($10,224) · signal-only (no cash/fees) was +1.82%. Starts YES **25/29**. Fills 160 · skips 62 · realized $+223.91.

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
- Must-have: Clock-B #2: a fresh good catalyst (catal / EPS beat / packet or headline green) and the prior tape is not already exploded.
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
- **Gate** `clk_fresh_cat_coil=True,oppset=True` · **rank** `opp_rvol` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,223.90.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ADUR` | 151 | — | $16.50 | +0.00 | $16.17 | -49.83 | -49.83 | +0.00 | -49.83 |
| 2026-08-14 | `AMAT` | 5 | — | $499.40 | +0.00 | $507.18 | +38.90 | +38.90 | +0.00 | +38.90 |
| 2026-08-14 | `WDC` | 4 | — | $503.50 | +0.00 | $508.80 | +21.20 | +21.20 | +0.00 | +21.20 |
| 2026-08-14 | `NU` | 158 | — | $15.74 | +0.00 | $15.23 | -80.58 | -80.58 | +0.00 | -80.58 |
| 2026-08-17 | `ADUR` | 151 | $16.17 | $15.73 | -66.44 | — | +0.00 | -66.44 | -116.27 | — |
| 2026-08-17 | `AMAT` | 5 | $507.18 | $517.45 | +51.33 | — | +0.00 | +51.33 | +90.23 | — |
| 2026-08-17 | `WDC` | 4 | $508.80 | $525.53 | +66.92 | — | +0.00 | +66.92 | +88.12 | — |
| 2026-08-17 | `NU` | 158 | $15.23 | $15.40 | +26.86 | — | +0.00 | +26.86 | -53.72 | — |
| 2026-08-17 | `ABX` | 365 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `ALM` | 205 | — | $16.20 | +0.00 | $16.36 | +32.80 | +32.80 | +0.00 | +32.80 |
| 2026-08-17 | `CELC` | 35 | — | $92.99 | +0.00 | $92.44 | -19.25 | -19.25 | +0.00 | -19.25 |
| 2026-08-18 | `ABX` | 365 | $9.12 | $9.03 | -32.85 | — | +0.00 | -32.85 | -32.85 | — |
| 2026-08-18 | `ALM` | 205 | $16.36 | $15.78 | -118.90 | — | +0.00 | -118.90 | -86.10 | — |
| 2026-08-18 | `CELC` | 35 | $92.44 | $92.38 | -2.10 | — | +0.00 | -2.10 | -21.35 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `MRVI` | 165 | — | $7.44 | +0.00 | $8.29 | +140.25 | +140.25 | +0.00 | +140.25 |
| 2026-08-20 | `DNA` | 164 | — | $7.45 | +0.00 | $6.96 | -80.36 | -80.36 | +0.00 | -80.36 |
| 2026-08-20 | `BILL` | 25 | — | $49.00 | +0.00 | $47.40 | -40.00 | -40.00 | +0.00 | -40.00 |
| 2026-08-20 | `CRCL` | 14 | — | $82.99 | +0.00 | $83.66 | +9.38 | +9.38 | +0.00 | +9.38 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `KGC` | 41 | — | $29.63 | +0.00 | $31.43 | +73.80 | +73.80 | +0.00 | +73.80 |
| 2026-08-20 | `WOLF` | 46 | — | $26.50 | +0.00 | $26.35 | -6.90 | -6.90 | +0.00 | -6.90 |
| 2026-08-20 | `BLSH` | 42 | — | $29.20 | +0.00 | $28.44 | -31.92 | -31.92 | +0.00 | -31.92 |
| 2026-08-21 | `MRVI` | 165 | $8.29 | $8.28 | -1.65 | — | +0.00 | -1.65 | +138.60 | — |
| 2026-08-21 | `DNA` | 164 | $6.96 | $7.09 | +21.32 | — | +0.00 | +21.32 | -59.04 | — |
| 2026-08-21 | `BILL` | 25 | $47.40 | $47.50 | +2.50 | — | +0.00 | +2.50 | -37.50 | — |
| 2026-08-21 | `CRCL` | 14 | $83.66 | $87.98 | +60.48 | — | +0.00 | +60.48 | +69.86 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `KGC` | 41 | $31.43 | $32.17 | +30.34 | — | +0.00 | +30.34 | +104.14 | — |
| 2026-08-21 | `WOLF` | 46 | $26.35 | $26.86 | +23.46 | $25.76 | -50.60 | -27.14 | +16.56 | -34.04 |
| 2026-08-21 | `BLSH` | 42 | $28.44 | $29.75 | +55.02 | — | +0.00 | +55.02 | +23.10 | — |
| 2026-08-21 | `QDEL` | 593 | — | $14.96 | +0.00 | $14.74 | -130.46 | -130.46 | +0.00 | -130.46 |
| 2026-08-24 | `WOLF` | 46 | $25.76 | $25.00 | -34.96 | — | +0.00 | -34.96 | -69.00 | — |
| 2026-08-24 | `QDEL` | 593 | $14.74 | $14.74 | +0.00 | — | +0.00 | +0.00 | -130.46 | — |
| 2026-08-25 | `AAOI` | 14 | — | $111.78 | +0.00 | $113.15 | +19.25 | +19.25 | +0.00 | +19.25 |
| 2026-08-25 | `ELMT` | 92 | — | $17.89 | +0.00 | $17.75 | -12.88 | -12.88 | +0.00 | -12.88 |
| 2026-08-25 | `GRRR` | 118 | — | $13.92 | +0.00 | $14.04 | +14.16 | +14.16 | +0.00 | +14.16 |
| 2026-08-25 | `AXTI` | 24 | — | $68.20 | +0.00 | $67.45 | -18.00 | -18.00 | +0.00 | -18.00 |
| 2026-08-25 | `EH` | 322 | — | $5.10 | +0.00 | $4.83 | -86.94 | -86.94 | +0.00 | -86.94 |
| 2026-08-25 | `QMCO` | 75 | — | $21.90 | +0.00 | $22.39 | +36.75 | +36.75 | +0.00 | +36.75 |
| 2026-08-26 | `AAOI` | 14 | $113.15 | $110.59 | -35.84 | — | +0.00 | -35.84 | -16.59 | — |
| 2026-08-26 | `ELMT` | 92 | $17.75 | $17.82 | +6.44 | — | +0.00 | +6.44 | -6.44 | — |
| 2026-08-26 | `GRRR` | 118 | $14.04 | $14.03 | -1.18 | — | +0.00 | -1.18 | +12.98 | — |
| 2026-08-26 | `AXTI` | 24 | $67.45 | $65.34 | -50.64 | — | +0.00 | -50.64 | -68.64 | — |
| 2026-08-26 | `EH` | 322 | $4.83 | $4.77 | -19.32 | — | +0.00 | -19.32 | -106.26 | — |
| 2026-08-26 | `QMCO` | 75 | $22.39 | $22.31 | -6.00 | — | +0.00 | -6.00 | +30.75 | — |
| 2026-08-26 | `BBWI` | 66 | — | $18.26 | +0.00 | $18.90 | +42.24 | +42.24 | +0.00 | +42.24 |
| 2026-08-26 | `INTU` | 3 | — | $323.47 | +0.00 | $345.88 | +67.23 | +67.23 | +0.00 | +67.23 |
| 2026-08-26 | `NCNO` | 62 | — | $19.33 | +0.00 | $21.51 | +135.16 | +135.16 | +0.00 | +135.16 |
| 2026-08-26 | `HEI` | 3 | — | $370.00 | +0.00 | $346.15 | -71.55 | -71.55 | +0.00 | -71.55 |
| 2026-08-26 | `BE` | 5 | — | $213.94 | +0.00 | $218.21 | +21.35 | +21.35 | +0.00 | +21.35 |
| 2026-08-26 | `QMLS` | 187 | — | $6.47 | +0.00 | $6.10 | -69.19 | -69.19 | +0.00 | -69.19 |
| 2026-08-26 | `NVTS` | 96 | — | $12.60 | +0.00 | $12.67 | +6.72 | +6.72 | +0.00 | +6.72 |
| 2026-08-26 | `FLNC` | 109 | — | $11.12 | +0.00 | $11.08 | -4.36 | -4.36 | +0.00 | -4.36 |
| 2026-08-27 | `BBWI` | 66 | $18.90 | $18.69 | -13.86 | — | +0.00 | -13.86 | +28.38 | — |
| 2026-08-27 | `INTU` | 3 | $345.88 | $353.54 | +22.98 | — | +0.00 | +22.98 | +90.21 | — |
| 2026-08-27 | `NCNO` | 62 | $21.51 | $22.03 | +32.24 | — | +0.00 | +32.24 | +167.40 | — |
| 2026-08-27 | `HEI` | 3 | $346.15 | $346.19 | +0.12 | — | +0.00 | +0.12 | -71.43 | — |
| 2026-08-27 | `BE` | 5 | $218.21 | $227.10 | +44.45 | — | +0.00 | +44.45 | +65.80 | — |
| 2026-08-27 | `QMLS` | 187 | $6.10 | $6.33 | +43.01 | — | +0.00 | +43.01 | -26.18 | — |
| 2026-08-27 | `NVTS` | 96 | $12.67 | $13.18 | +48.96 | — | +0.00 | +48.96 | +55.68 | — |
| 2026-08-27 | `FLNC` | 109 | $11.08 | $11.52 | +47.96 | — | +0.00 | +47.96 | +43.60 | — |
| 2026-08-28 | `DY` | 4 | — | $306.34 | +0.00 | $294.34 | -48.00 | -48.00 | +0.00 | -48.00 |
| 2026-08-28 | `BBWI` | 66 | — | $18.75 | +0.00 | $19.22 | +31.02 | +31.02 | +0.00 | +31.02 |
| 2026-08-28 | `SYRE` | 13 | — | $91.75 | +0.00 | $90.36 | -18.07 | -18.07 | +0.00 | -18.07 |
| 2026-08-28 | `SEDG` | 38 | — | $32.90 | +0.00 | $31.41 | -56.62 | -56.62 | +0.00 | -56.62 |
| 2026-08-28 | `ZYME` | 43 | — | $28.91 | +0.00 | $28.27 | -27.52 | -27.52 | +0.00 | -27.52 |
| 2026-08-28 | `MRNA` | 9 | — | $137.19 | +0.00 | $137.99 | +7.20 | +7.20 | +0.00 | +7.20 |
| 2026-08-28 | `TH` | 65 | — | $19.00 | +0.00 | $18.55 | -29.25 | -29.25 | +0.00 | -29.25 |
| 2026-08-28 | `FIGR` | 33 | — | $37.49 | +0.00 | $36.05 | -47.52 | -47.52 | +0.00 | -47.52 |
| 2026-08-31 | `DY` | 4 | $294.34 | $298.01 | +14.68 | — | +0.00 | +14.68 | -33.32 | — |
| 2026-08-31 | `BBWI` | 66 | $19.22 | $19.25 | +1.98 | — | +0.00 | +1.98 | +33.00 | — |
| 2026-08-31 | `SYRE` | 13 | $90.36 | $89.15 | -15.73 | — | +0.00 | -15.73 | -33.80 | — |
| 2026-08-31 | `SEDG` | 38 | $31.41 | $31.15 | -9.88 | — | +0.00 | -9.88 | -66.50 | — |
| 2026-08-31 | `ZYME` | 43 | $28.27 | $28.06 | -9.03 | — | +0.00 | -9.03 | -36.55 | — |
| 2026-08-31 | `MRNA` | 9 | $137.99 | $134.10 | -35.01 | $140.34 | +56.16 | +21.15 | -27.81 | +28.35 |
| 2026-08-31 | `TH` | 65 | $18.55 | $18.12 | -27.63 | — | +0.00 | -27.63 | -56.88 | — |
| 2026-08-31 | `FIGR` | 33 | $36.05 | $35.77 | -9.24 | — | +0.00 | -9.24 | -56.76 | — |
| 2026-09-01 | `MRNA` | 9 | $140.34 | $140.25 | -0.81 | — | +0.00 | -0.81 | +27.54 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `EIX` | 22 | — | $55.42 | +0.00 | $56.30 | +19.36 | +19.36 | +0.00 | +19.36 |
| 2026-09-03 | `ENOV` | 60 | — | $20.28 | +0.00 | $19.40 | -52.80 | -52.80 | +0.00 | -52.80 |
| 2026-09-03 | `FIVE` | 4 | — | $257.00 | +0.00 | $239.96 | -68.16 | -68.16 | +0.00 | -68.16 |
| 2026-09-03 | `NTSK` | 78 | — | $15.51 | +0.00 | $14.34 | -91.26 | -91.26 | +0.00 | -91.26 |
| 2026-09-03 | `NTAP` | 7 | — | $161.95 | +0.00 | $185.38 | +164.01 | +164.01 | +0.00 | +164.01 |
| 2026-09-03 | `ATRC` | 23 | — | $52.88 | +0.00 | $52.46 | -9.66 | -9.66 | +0.00 | -9.66 |
| 2026-09-03 | `VSXY` | 15 | — | $76.86 | +0.00 | $73.64 | -48.30 | -48.30 | +0.00 | -48.30 |
| 2026-09-03 | `RPD` | 107 | — | $11.39 | +0.00 | $11.47 | +8.56 | +8.56 | +0.00 | +8.56 |
| 2026-09-04 | `EIX` | 22 | $56.30 | $55.79 | -11.22 | — | +0.00 | -11.22 | +8.14 | — |
| 2026-09-04 | `ENOV` | 60 | $19.40 | $19.01 | -23.40 | — | +0.00 | -23.40 | -76.20 | — |
| 2026-09-04 | `FIVE` | 4 | $239.96 | $238.88 | -4.32 | — | +0.00 | -4.32 | -72.48 | — |
| 2026-09-04 | `NTSK` | 78 | $14.34 | $14.15 | -14.82 | — | +0.00 | -14.82 | -106.08 | — |
| 2026-09-04 | `NTAP` | 7 | $185.38 | $182.59 | -19.53 | — | +0.00 | -19.53 | +144.48 | — |
| 2026-09-04 | `ATRC` | 23 | $52.46 | $52.03 | -9.89 | — | +0.00 | -9.89 | -19.55 | — |
| 2026-09-04 | `VSXY` | 15 | $73.64 | $73.63 | -0.15 | — | +0.00 | -0.15 | -48.45 | — |
| 2026-09-04 | `RPD` | 107 | $11.47 | $11.23 | -25.68 | — | +0.00 | -25.68 | -17.12 | — |
| 2026-09-04 | `ASAN` | 136 | — | $8.74 | +0.00 | $8.81 | +9.52 | +9.52 | +0.00 | +9.52 |
| 2026-09-04 | `MSTR` | 8 | — | $137.35 | +0.00 | $142.80 | +43.60 | +43.60 | +0.00 | +43.60 |
| 2026-09-04 | `CRCL` | 12 | — | $97.98 | +0.00 | $102.05 | +48.84 | +48.84 | +0.00 | +48.84 |
| 2026-09-04 | `DOCU` | 17 | — | $68.52 | +0.00 | $68.41 | -1.87 | -1.87 | +0.00 | -1.87 |
| 2026-09-04 | `GWRE` | 7 | — | $167.55 | +0.00 | $162.42 | -35.91 | -35.91 | +0.00 | -35.91 |
| 2026-09-04 | `BLSH` | 34 | — | $34.69 | +0.00 | $36.00 | +44.54 | +44.54 | +0.00 | +44.54 |
| 2026-09-04 | `ZETA` | 36 | — | $32.65 | +0.00 | $31.35 | -46.80 | -46.80 | +0.00 | -46.80 |
| 2026-09-04 | `BE` | 5 | — | $236.82 | +0.00 | $252.87 | +80.25 | +80.25 | +0.00 | +80.25 |
| 2026-09-08 | `ASAN` | 136 | $8.81 | $8.73 | -10.88 | — | +0.00 | -10.88 | -1.36 | — |
| 2026-09-08 | `MSTR` | 8 | $142.80 | $137.62 | -41.44 | — | +0.00 | -41.44 | +2.16 | — |
| 2026-09-08 | `CRCL` | 12 | $102.05 | $100.65 | -16.80 | — | +0.00 | -16.80 | +32.04 | — |
| 2026-09-08 | `DOCU` | 17 | $68.41 | $67.05 | -23.12 | — | +0.00 | -23.12 | -24.99 | — |
| 2026-09-08 | `GWRE` | 7 | $162.42 | $160.52 | -13.30 | — | +0.00 | -13.30 | -49.21 | — |
| 2026-09-08 | `BLSH` | 34 | $36.00 | $35.90 | -3.40 | — | +0.00 | -3.40 | +41.14 | — |
| 2026-09-08 | `ZETA` | 36 | $31.35 | $31.08 | -9.72 | — | +0.00 | -9.72 | -56.52 | — |
| 2026-09-08 | `BE` | 5 | $252.87 | $267.76 | +74.45 | — | +0.00 | +74.45 | +154.70 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `PBR` | 75 | — | $21.21 | +0.00 | $21.20 | -0.75 | -0.75 | +0.00 | -0.75 |
| 2026-09-11 | `INTR` | 276 | — | $5.78 | +0.00 | $5.71 | -19.32 | -19.32 | +0.00 | -19.32 |
| 2026-09-11 | `INSP` | 22 | — | $69.88 | +0.00 | $73.00 | +68.64 | +68.64 | +0.00 | +68.64 |
| 2026-09-11 | `BAND` | 30 | — | $52.55 | +0.00 | $56.87 | +129.60 | +129.60 | +0.00 | +129.60 |
| 2026-09-11 | `RDDT` | 10 | — | $157.55 | +0.00 | $157.77 | +2.20 | +2.20 | +0.00 | +2.20 |
| 2026-09-11 | `FUBO` | 138 | — | $11.55 | +0.00 | $11.53 | -2.76 | -2.76 | +0.00 | -2.76 |
| 2026-09-14 | `PBR` | 75 | $21.20 | $21.23 | +2.25 | — | +0.00 | +2.25 | +1.50 | — |
| 2026-09-14 | `INTR` | 276 | $5.71 | $5.49 | -60.72 | — | +0.00 | -60.72 | -80.04 | — |
| 2026-09-14 | `INSP` | 22 | $73.00 | $72.14 | -18.92 | — | +0.00 | -18.92 | +49.72 | — |
| 2026-09-14 | `BAND` | 30 | $56.87 | $56.90 | +0.90 | — | +0.00 | +0.90 | +130.50 | — |
| 2026-09-14 | `RDDT` | 10 | $157.77 | $160.00 | +22.30 | — | +0.00 | +22.30 | +24.50 | — |
| 2026-09-14 | `FUBO` | 138 | $11.53 | $11.56 | +4.14 | — | +0.00 | +4.14 | +1.38 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `VAL` | 13 | — | $87.40 | +0.00 | $82.52 | -63.44 | -63.44 | +0.00 | -63.44 |
| 2026-09-16 | `RIG` | 206 | — | $5.87 | +0.00 | $5.54 | -67.98 | -67.98 | +0.00 | -67.98 |
| 2026-09-16 | `ILMN` | 5 | — | $224.49 | +0.00 | $228.93 | +22.20 | +22.20 | +0.00 | +22.20 |
| 2026-09-16 | `QLYS` | 6 | — | $179.60 | +0.00 | $183.06 | +20.76 | +20.76 | +0.00 | +20.76 |
| 2026-09-16 | `BBNX` | 65 | — | $18.61 | +0.00 | $22.18 | +232.05 | +232.05 | +0.00 | +232.05 |
| 2026-09-16 | `TEM` | 17 | — | $68.79 | +0.00 | $69.97 | +20.06 | +20.06 | +0.00 | +20.06 |
| 2026-09-16 | `KRMN` | 31 | — | $38.01 | +0.00 | $36.94 | -33.17 | -33.17 | +0.00 | -33.17 |
| 2026-09-16 | `SION` | 174 | — | $6.95 | +0.00 | $7.04 | +15.66 | +15.66 | +0.00 | +15.66 |
| 2026-09-17 | `VAL` | 13 | $82.52 | $83.20 | +8.84 | — | +0.00 | +8.84 | -54.60 | — |
| 2026-09-17 | `RIG` | 206 | $5.54 | $5.58 | +8.24 | — | +0.00 | +8.24 | -59.74 | — |
| 2026-09-17 | `ILMN` | 5 | $228.93 | $233.85 | +24.60 | — | +0.00 | +24.60 | +46.80 | — |
| 2026-09-17 | `QLYS` | 6 | $183.06 | $180.82 | -13.44 | — | +0.00 | -13.44 | +7.32 | — |
| 2026-09-17 | `BBNX` | 65 | $22.18 | $22.46 | +18.20 | — | +0.00 | +18.20 | +250.25 | — |
| 2026-09-17 | `TEM` | 17 | $69.97 | $72.70 | +46.41 | — | +0.00 | +46.41 | +66.47 | — |
| 2026-09-17 | `KRMN` | 31 | $36.94 | $37.89 | +29.45 | — | +0.00 | +29.45 | -3.72 | — |
| 2026-09-17 | `SION` | 174 | $7.04 | $7.27 | +40.02 | — | +0.00 | +40.02 | +55.68 | — |
| 2026-09-17 | `BULL` | 156 | — | $7.95 | +0.00 | $7.71 | -37.44 | -37.44 | +0.00 | -37.44 |
| 2026-09-17 | `MAMA` | 87 | — | $14.21 | +0.00 | $13.11 | -95.70 | -95.70 | +0.00 | -95.70 |
| 2026-09-17 | `PGEN` | 164 | — | $7.59 | +0.00 | $7.87 | +45.92 | +45.92 | +0.00 | +45.92 |
| 2026-09-17 | `SMTC` | 7 | — | $170.85 | +0.00 | $178.19 | +51.38 | +51.38 | +0.00 | +51.38 |
| 2026-09-17 | `EROC` | 98 | — | $12.64 | +0.00 | $12.90 | +25.48 | +25.48 | +0.00 | +25.48 |
| 2026-09-17 | `LITE` | 1 | — | $934.88 | +0.00 | $893.61 | -41.27 | -41.27 | +0.00 | -41.27 |
| 2026-09-17 | `CRDO` | 7 | — | $168.65 | +0.00 | $168.25 | -2.80 | -2.80 | +0.00 | -2.80 |
| 2026-09-17 | `AXTI` | 18 | — | $67.91 | +0.00 | $67.75 | -2.88 | -2.88 | +0.00 | -2.88 |
| 2026-09-18 | `BULL` | 156 | $7.71 | $7.85 | +21.84 | — | +0.00 | +21.84 | -15.60 | — |
| 2026-09-18 | `MAMA` | 87 | $13.11 | $12.91 | -17.40 | — | +0.00 | -17.40 | -113.10 | — |
| 2026-09-18 | `PGEN` | 164 | $7.87 | $7.98 | +18.04 | — | +0.00 | +18.04 | +63.96 | — |
| 2026-09-18 | `SMTC` | 7 | $178.19 | $182.33 | +28.98 | — | +0.00 | +28.98 | +80.36 | — |
| 2026-09-18 | `EROC` | 98 | $12.90 | $13.00 | +9.80 | — | +0.00 | +9.80 | +35.28 | — |
| 2026-09-18 | `LITE` | 1 | $893.61 | $915.66 | +22.05 | — | +0.00 | +22.05 | -19.22 | — |
| 2026-09-18 | `CRDO` | 7 | $168.25 | $171.11 | +20.02 | — | +0.00 | +20.02 | +17.22 | — |
| 2026-09-18 | `AXTI` | 18 | $67.75 | $69.72 | +35.46 | — | +0.00 | +35.46 | +32.58 | — |
| 2026-09-18 | `BHVN` | 178 | — | $14.07 | +0.00 | $13.62 | -80.10 | -80.10 | +0.00 | -80.10 |
| 2026-09-18 | `TH` | 119 | — | $20.91 | +0.00 | $21.19 | +33.32 | +33.32 | +0.00 | +33.32 |
| 2026-09-18 | `AMD` | 4 | — | $547.37 | +0.00 | $559.82 | +49.80 | +49.80 | +0.00 | +49.80 |
| 2026-09-18 | `SHLS` | 327 | — | $7.64 | +0.00 | $7.60 | -13.08 | -13.08 | +0.00 | -13.08 |
| 2026-09-21 | `BHVN` | 178 | $13.62 | $13.90 | +49.84 | — | +0.00 | +49.84 | -30.26 | — |
| 2026-09-21 | `TH` | 119 | $21.19 | $21.65 | +54.74 | — | +0.00 | +54.74 | +88.06 | — |
| 2026-09-21 | `AMD` | 4 | $559.82 | $583.88 | +96.24 | — | +0.00 | +96.24 | +146.04 | — |
| 2026-09-21 | `SHLS` | 327 | $7.60 | $7.71 | +35.97 | — | +0.00 | +35.97 | +22.89 | — |
| 2026-09-22 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-23 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -70.31 | ADUR, AMAT, WDC, NU | — | $501.67 | $9,920.78 | ADUR×151, AMAT×5, WDC×4, NU×158 |
| 2026-08-17 | +2.25 | $501.67 | ADUR×151, AMAT×5, WDC×4, NU×158 | $9,999.45 | +78.67 | +13.55 | ABX, ALM, CELC | ADUR, AMAT, WDC, NU | $76.49 | $9,994.49 | ABX×365, ALM×205, CELC×35 |
| 2026-08-18 | -6.20 | $76.49 | ABX×365, ALM×205, CELC×35 | $9,840.64 | -153.85 | +0.00 | — | ABX, ALM, CELC | $9,831.01 | $9,831.01 | — |
| 2026-08-19 | -7.20 | $9,831.01 | — | $9,831.01 | -0.00 | +0.00 | — | — | $9,831.01 | $9,831.01 | — |
| 2026-08-20 | +1.12 | $9,831.01 | — | $9,831.01 | -0.00 | +98.31 | MRVI, DNA, BILL, CRCL, BHP, KGC, WOLF, BLSH | — | $133.94 | $9,911.87 | MRVI×165, DNA×164, BILL×25, CRCL×14, BHP×13, KGC×41, WOLF×46, BLSH×42 |
| 2026-08-21 | +3.25 | $133.94 | MRVI×165, DNA×164, BILL×25, CRCL×14, BHP×13, KGC×41, WOLF×46, BLSH×42 | $10,130.51 | +218.64 | -181.06 | QDEL | MRVI, DNA, BILL, CRCL, BHP, KGC, BLSH | $0.52 | $9,926.30 | WOLF×46, QDEL×593 |
| 2026-08-24 | -5.17 | $0.52 | WOLF×46, QDEL×593 | $9,891.34 | -34.96 | +0.00 | — | WOLF, QDEL | $9,881.38 | $9,881.38 | — |
| 2026-08-25 | +1.80 | $9,881.38 | — | $9,881.38 | -0.00 | -47.66 | AAOI, ELMT, GRRR, AXTI, EH, QMCO | — | $91.51 | $9,818.64 | AAOI×14, ELMT×92, GRRR×118, AXTI×24, EH×322, QMCO×75 |
| 2026-08-26 | +2.02 | $91.51 | AAOI×14, ELMT×92, GRRR×118, AXTI×24, EH×322, QMCO×75 | $9,712.10 | -106.54 | +127.60 | BBWI, INTU, NCNO, HEI, BE, QMLS, NVTS, FLNC | AAOI, ELMT, GRRR, AXTI, EH, QMCO | $494.02 | $9,806.92 | BBWI×66, INTU×3, NCNO×62, HEI×3, BE×5, QMLS×187, NVTS×96, FLNC×109 |
| 2026-08-27 | — | $494.02 | BBWI×66, INTU×3, NCNO×62, HEI×3, BE×5, QMLS×187, NVTS×96, FLNC×109 | $10,032.78 | +225.86 | +0.00 | — | BBWI, INTU, NCNO, HEI, BE, QMLS, NVTS, FLNC | $10,015.07 | $10,015.07 | — |
| 2026-08-28 | +0.75 | $10,015.07 | — | $10,015.07 | -0.00 | -188.76 | DY, BBWI, SYRE, SEDG, ZYME, MRNA, TH, FIGR | — | $142.52 | $9,809.58 | DY×4, BBWI×66, SYRE×13, SEDG×38, ZYME×43, MRNA×9, TH×65, FIGR×33 |
| 2026-08-31 | -5.85 | $142.52 | DY×4, BBWI×66, SYRE×13, SEDG×38, ZYME×43, MRNA×9, TH×65, FIGR×33 | $9,719.72 | -89.86 | +56.16 | — | DY, BBWI, SYRE, SEDG, ZYME, TH, FIGR | $8,497.96 | $9,761.02 | MRNA×9 |
| 2026-09-01 | -6.30 | $8,497.96 | MRNA×9 | $9,760.21 | -0.81 | +0.00 | — | MRNA | $9,758.18 | $9,758.18 | — |
| 2026-09-02 | -3.83 | $9,758.18 | — | $9,758.18 | -0.00 | +0.00 | — | — | $9,758.18 | $9,758.18 | — |
| 2026-09-03 | -0.90 | $9,758.18 | — | $9,758.18 | -0.00 | -78.25 | EIX, ENOV, FIVE, NTSK, NTAP, ATRC, VSXY, RPD | — | $345.97 | $9,663.06 | EIX×22, ENOV×60, FIVE×4, NTSK×78, NTAP×7, ATRC×23, VSXY×15, RPD×107 |
| 2026-09-04 | +2.25 | $345.97 | EIX×22, ENOV×60, FIVE×4, NTSK×78, NTAP×7, ATRC×23, VSXY×15, RPD×107 | $9,554.05 | -109.01 | +142.17 | ASAN, MSTR, CRCL, DOCU, GWRE, BLSH, ZETA, BE | EIX, ENOV, FIVE, NTSK, NTAP, ATRC, VSXY, RPD | $180.47 | $9,662.49 | ASAN×136, MSTR×8, CRCL×12, DOCU×17, GWRE×7, BLSH×34, ZETA×36, BE×5 |
| 2026-09-08 | -11.47 | $180.47 | ASAN×136, MSTR×8, CRCL×12, DOCU×17, GWRE×7, BLSH×34, ZETA×36, BE×5 | $9,618.28 | -44.21 | +0.00 | — | ASAN, MSTR, CRCL, DOCU, GWRE, BLSH, ZETA, BE | $9,601.43 | $9,601.43 | — |
| 2026-09-09 | -13.95 | $9,601.43 | — | $9,601.43 | -0.00 | +0.00 | — | — | $9,601.43 | $9,601.43 | — |
| 2026-09-10 | -13.28 | $9,601.43 | — | $9,601.43 | -0.00 | +0.00 | — | — | $9,601.43 | $9,601.43 | — |
| 2026-09-11 | +0.50 | $9,601.43 | — | $9,601.43 | -0.00 | +177.61 | PBR, INTR, INSP, BAND, RDDT, FUBO | — | $117.80 | $9,764.70 | PBR×75, INTR×276, INSP×22, BAND×30, RDDT×10, FUBO×138 |
| 2026-09-14 | -11.00 | $117.80 | PBR×75, INTR×276, INSP×22, BAND×30, RDDT×10, FUBO×138 | $9,714.65 | -50.05 | +0.00 | — | PBR, INTR, INSP, BAND, RDDT, FUBO | $9,700.13 | $9,700.13 | — |
| 2026-09-15 | -3.84 | $9,700.13 | — | $9,700.13 | -0.00 | +0.00 | — | — | $9,700.13 | $9,700.13 | — |
| 2026-09-16 | +5.30 | $9,700.13 | — | $9,700.13 | -0.00 | +146.14 | VAL, RIG, ILMN, QLYS, BBNX, TEM, KRMN, SION | — | $370.45 | $9,828.75 | VAL×13, RIG×206, ILMN×5, QLYS×6, BBNX×65, TEM×17, KRMN×31, SION×174 |
| 2026-09-17 | +7.38 | $370.45 | VAL×13, RIG×206, ILMN×5, QLYS×6, BBNX×65, TEM×17, KRMN×31, SION×174 | $9,991.07 | +162.32 | -57.31 | BULL, MAMA, PGEN, SMTC, EROC, LITE, CRDO, AXTI | VAL, RIG, ILMN, QLYS, BBNX, TEM, KRMN, SION | $462.10 | $9,898.50 | BULL×156, MAMA×87, PGEN×164, SMTC×7, EROC×98, LITE×1, CRDO×7, AXTI×18 |
| 2026-09-18 | +4.86 | $462.10 | BULL×156, MAMA×87, PGEN×164, SMTC×7, EROC×98, LITE×1, CRDO×7, AXTI×18 | $10,037.29 | +138.79 | -10.06 | BHVN, TH, AMD, SHLS | BULL, MAMA, PGEN, SMTC, EROC, LITE, CRDO, AXTI | $327.95 | $9,998.40 | BHVN×178, TH×119, AMD×4, SHLS×327 |
| 2026-09-21 | +12.87 | $327.95 | BHVN×178, TH×119, AMD×4, SHLS×327 | $10,235.19 | +236.79 | +0.00 | — | BHVN, TH, AMD, SHLS | $10,223.90 | $10,223.90 | — |
| 2026-09-22 | -0.50 | $10,223.90 | — | $10,223.90 | +0.00 | +0.00 | — | — | $10,223.90 | $10,223.90 | — |
| 2026-09-23 | +2.29 | $10,223.90 | — | $10,223.90 | +0.00 | +0.00 | — | — | $10,223.90 | $10,223.90 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 151 | $16.50 | $2.44 | — | $7,506.06 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AMAT` | 5 | $499.40 | $2.00 | — | $5,007.05 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+1.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 4 | $503.50 | $2.00 | — | $2,991.05 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable; 🔵; ⚪; ret5=+7.9; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NU` | 158 | $15.74 | $2.46 | — | $501.67 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-1.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $501.67 | ▼ close $9,920.78 vs 09:30 $10,000.00 (session -70.31) | 16:00 close · cash $501.67 · equity $9,920.78 vs 09:30 $10,000.00 (-79.22; session marks -70.31) · 4 name(s) marked open→close (per-name table). ADUR×151 09:30 $16.50 → close $16.17 -49.83; AMAT×5 09:30 $499.40 → close $507.18 +38.90; WDC×4 09:30 $503.50 → close $508.80 +21.20; NU×158 09:30 $15.74 → close $15.23 -80.58 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $501.67 | ▲ 09:30 equity $9,999.45 vs yday $9,920.78 (+78.67) | 09:30 open · cash $501.67 (unchanged overnight, no fees) · equity $9,999.45 vs prior close $9,920.78 (+78.67) · 4 name(s) re-marked at the open (per-name table). ADUR×151 yday $16.17 → 09:30 $15.73 -66.44; AMAT×5 yday $507.18 → 09:30 $517.45 +51.33; WDC×4 yday $508.80 → 09:30 $525.53 +66.92; NU×158 yday $15.23 → 09:30 $15.40 +26.86 | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 151 | $15.73 | $2.49 | $-121.20 | $2,874.41 | ▼ -121.20 after sell → book $9,996.96; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMAT` | 5 | $517.45 | $2.04 | $+86.19 | $5,459.61 | ▲ +86.19 after sell → book $9,994.93; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 4 | $525.53 | $2.03 | $+84.09 | $7,559.70 | ▲ +84.09 after sell → book $9,992.90; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NU` | 158 | $15.40 | $2.51 | $-58.69 | $9,990.39 | ▼ -58.69 after sell → book $9,990.39; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 365 | $9.12 | $4.71 | — | $6,656.88 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $3330.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 205 | $16.20 | $2.64 | — | $3,333.24 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $3330.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 35 | $92.99 | $2.10 | — | $76.49 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-0.8; leftover $3330.13 | join🟡 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.49 | ▲ close $9,994.49 vs 09:30 $9,999.45 (session +13.55) | 16:00 close · cash $76.49 · equity $9,994.49 vs 09:30 $9,999.45 (-4.96; session marks +13.55) · 3 name(s) marked open→close (per-name table). ABX×365 09:30 $9.12 → close $9.12 +0.00; ALM×205 09:30 $16.20 → close $16.36 +32.80; CELC×35 09:30 $92.99 → close $92.44 -19.25 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.49 | ▼ 09:30 equity $9,840.64 vs yday $9,994.49 (-153.85) | 09:30 open · cash $76.49 (unchanged overnight, no fees) · equity $9,840.64 vs prior close $9,994.49 (-153.85) · 3 name(s) re-marked at the open (per-name table). ABX×365 yday $9.12 → 09:30 $9.03 -32.85; ALM×205 yday $16.36 → 09:30 $15.78 -118.90; CELC×35 yday $92.44 → 09:30 $92.38 -2.10 | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 365 | $9.03 | $4.80 | $-42.35 | $3,367.64 | ▼ -42.35 after sell → book $9,835.84; vs 09:30 mark -4.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 205 | $15.78 | $2.70 | $-91.45 | $6,599.84 | ▼ -91.45 after sell → book $9,833.14; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 35 | $92.38 | $2.13 | $-25.58 | $9,831.01 | ▼ -25.58 after sell → book $9,831.01; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,831.01 | ▲ close $9,831.01 vs 09:30 $9,840.64 (session +0.00) | 16:00 close · cash $9,831.01 · no lots left · equity $9,831.01. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,831.01 | ▲ 09:30 equity $9,831.01 vs yday $9,831.01 (-0.00) | 09:30 open · cash $9,831.01 · no holdings · equity $9,831.01 vs prior close $9,831.01 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,831.01 | ▲ close $9,831.01 vs 09:30 $9,831.01 (session +0.00) | 16:00 close · cash $9,831.01 · no lots left · equity $9,831.01. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,831.01 | ▲ 09:30 equity $9,831.01 vs yday $9,831.01 (-0.00) | 09:30 open · cash $9,831.01 · no holdings · equity $9,831.01 vs prior close $9,831.01 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 165 | $7.44 | $2.48 | — | $8,600.92 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1228.88 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 164 | $7.45 | $2.48 | — | $7,376.64 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1228.88 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BILL` | 25 | $49.00 | $2.06 | — | $6,149.58 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-2.0; leftover $1228.88 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRCL` | 14 | $82.99 | $2.03 | — | $4,985.69 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable; 🔵; ⚪; ret5=+7.4; leftover $1228.88 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $3,800.53 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1228.88 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $2,583.58 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1228.88 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WOLF` | 46 | $26.50 | $2.13 | — | $1,362.46 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-8.3; leftover $1228.88 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 42 | $29.20 | $2.12 | — | $133.94 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1228.88 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.94 | ▲ close $9,911.87 vs 09:30 $9,831.01 (session +98.31) | 16:00 close · cash $133.94 · equity $9,911.87 vs 09:30 $9,831.01 (+80.86; session marks +98.31) · 8 name(s) marked open→close (per-name table). MRVI×165 09:30 $7.44 → close $8.29 +140.25; DNA×164 09:30 $7.45 → close $6.96 -80.36; BILL×25 09:30 $49.00 → close $47.40 -40.00; CRCL×14 09:30 $82.99 → close $83.66 +9.38; BHP×13 09:30 $91.01 → close $93.63 +34.06; KGC×41 09:30 $29.63 → close $31.43 +73.80; WOLF×46 09:30 $26.50 → close $26.35 -6.90; BLSH×42 09:30 $29.20 → close $28.44 -31.92 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.94 | ▲ 09:30 equity $10,130.51 vs yday $9,911.87 (+218.64) | 09:30 open · cash $133.94 (unchanged overnight, no fees) · equity $10,130.51 vs prior close $9,911.87 (+218.64) · 8 name(s) re-marked at the open (per-name table). MRVI×165 yday $8.29 → 09:30 $8.28 -1.65; DNA×164 yday $6.96 → 09:30 $7.09 +21.32; BILL×25 yday $47.40 → 09:30 $47.50 +2.50; CRCL×14 yday $83.66 → 09:30 $87.98 +60.48; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; KGC×41 yday $31.43 → 09:30 $32.17 +30.34; WOLF×46 yday $26.35 → 09:30 $26.86 +23.46; BLSH×42 yday $28.44 → 09:30 $29.75 +55.02 | — |
| 2026-08-21 09:30 ET | **SELL** | `MRVI` | 165 | $8.28 | $2.52 | $+133.59 | $1,497.62 | ▲ +133.59 after sell → book $10,127.99; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 164 | $7.09 | $2.52 | $-64.04 | $2,657.86 | ▼ -64.04 after sell → book $10,125.47; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BILL` | 25 | $47.50 | $2.08 | $-41.65 | $3,843.27 | ▼ -41.65 after sell → book $10,123.38; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CRCL` | 14 | $87.98 | $2.05 | $+65.78 | $5,072.94 | ▲ +65.78 after sell → book $10,121.33; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $6,315.25 | ▲ +57.15 after sell → book $10,119.28; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $7,632.09 | ▲ +99.89 after sell → book $10,117.15; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BLSH` | 42 | $29.75 | $2.14 | $+18.85 | $8,879.45 | ▲ +18.85 after sell → book $10,115.01; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 593 | $14.96 | $7.65 | — | $0.52 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-1.6; leftover $8879.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.52 | ▼ close $9,926.30 vs 09:30 $10,130.51 (session -181.06) | 16:00 close · cash $0.52 · equity $9,926.30 vs 09:30 $10,130.51 (-204.21; session marks -181.06) · 2 name(s) marked open→close (per-name table). WOLF×46 09:30 $26.86 → close $25.76 -50.60; QDEL×593 09:30 $14.96 → close $14.74 -130.46 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.52 | ▼ 09:30 equity $9,891.34 vs yday $9,926.30 (-34.96) | 09:30 open · cash $0.52 (unchanged overnight, no fees) · equity $9,891.34 vs prior close $9,926.30 (-34.96) · 2 name(s) re-marked at the open (per-name table). WOLF×46 yday $25.76 → 09:30 $25.00 -34.96; QDEL×593 yday $14.74 → 09:30 $14.74 +0.00 | — |
| 2026-08-24 09:30 ET | **SELL** | `WOLF` | 46 | $25.00 | $2.15 | $-73.28 | $1,148.37 | ▼ -73.28 after sell → book $9,889.19; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 593 | $14.74 | $7.82 | $-145.93 | $9,881.38 | ▼ -145.93 after sell → book $9,881.38; vs 09:30 mark -7.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,881.38 | ▲ close $9,881.38 vs 09:30 $9,891.34 (session +0.00) | 16:00 close · cash $9,881.38 · no lots left · equity $9,881.38. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,881.38 | ▲ 09:30 equity $9,881.38 vs yday $9,881.38 (-0.00) | 09:30 open · cash $9,881.38 · no holdings · equity $9,881.38 vs prior close $9,881.38 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `AAOI` | 14 | $111.78 | $2.03 | — | $8,314.49 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-30.5; leftover $1646.90 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ELMT` | 92 | $17.89 | $2.27 | — | $6,666.35 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ⚪; ret5=-7.5; leftover $1646.90 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 118 | $13.92 | $2.34 | — | $5,021.44 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+5.9; leftover $1646.90 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AXTI` | 24 | $68.20 | $2.06 | — | $3,382.58 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-31.9; leftover $1646.90 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 322 | $5.10 | $4.15 | — | $1,736.23 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-8.9; leftover $1646.90 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `QMCO` | 75 | $21.90 | $2.21 | — | $91.51 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=-13.5; leftover $1646.90 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $91.51 | ▼ close $9,818.64 vs 09:30 $9,881.38 (session -47.66) | 16:00 close · cash $91.51 · equity $9,818.64 vs 09:30 $9,881.38 (-62.74; session marks -47.66) · 6 name(s) marked open→close (per-name table). AAOI×14 09:30 $111.78 → close $113.15 +19.25; ELMT×92 09:30 $17.89 → close $17.75 -12.88; GRRR×118 09:30 $13.92 → close $14.04 +14.16; AXTI×24 09:30 $68.20 → close $67.45 -18.00; EH×322 09:30 $5.10 → close $4.83 -86.94; QMCO×75 09:30 $21.90 → close $22.39 +36.75 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $91.51 | ▼ 09:30 equity $9,712.10 vs yday $9,818.64 (-106.54) | 09:30 open · cash $91.51 (unchanged overnight, no fees) · equity $9,712.10 vs prior close $9,818.64 (-106.54) · 6 name(s) re-marked at the open (per-name table). AAOI×14 yday $113.15 → 09:30 $110.59 -35.84; ELMT×92 yday $17.75 → 09:30 $17.82 +6.44; GRRR×118 yday $14.04 → 09:30 $14.03 -1.18; AXTI×24 yday $67.45 → 09:30 $65.34 -50.64; EH×322 yday $4.83 → 09:30 $4.77 -19.32; QMCO×75 yday $22.39 → 09:30 $22.31 -6.00 | — |
| 2026-08-26 09:30 ET | **SELL** | `AAOI` | 14 | $110.59 | $2.05 | $-20.68 | $1,637.72 | ▼ -20.68 after sell → book $9,710.05; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `ELMT` | 92 | $17.82 | $2.29 | $-11.00 | $3,274.86 | ▼ -11.00 after sell → book $9,707.75; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 118 | $14.03 | $2.38 | $+8.26 | $4,928.03 | ▲ +8.26 after sell → book $9,705.38; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `AXTI` | 24 | $65.34 | $2.08 | $-72.79 | $6,494.10 | ▼ -72.79 after sell → book $9,703.29; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `EH` | 322 | $4.77 | $4.22 | $-114.63 | $8,025.82 | ▼ -114.63 after sell → book $9,699.07; vs 09:30 mark -4.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `QMCO` | 75 | $22.31 | $2.24 | $+26.29 | $9,696.83 | ▲ +26.29 after sell → book $9,696.83; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 66 | $18.26 | $2.19 | — | $8,489.48 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; ret5=-11.4; leftover $1212.10 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `INTU` | 3 | $323.47 | $2.00 | — | $7,517.08 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; ret5=+2.0; leftover $1212.10 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NCNO` | 62 | $19.33 | $2.18 | — | $6,316.44 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; ret5=+3.0; leftover $1212.10 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `HEI` | 3 | $370.00 | $2.00 | — | $5,204.44 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; ret5=-4.6; leftover $1212.10 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 5 | $213.94 | $2.00 | — | $4,132.74 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1212.10 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `QMLS` | 187 | $6.47 | $2.55 | — | $2,920.29 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-7.0; leftover $1212.10 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NVTS` | 96 | $12.60 | $2.28 | — | $1,708.42 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=-5.5; leftover $1212.10 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 109 | $11.12 | $2.32 | — | $494.02 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1212.10 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $494.02 | ▲ close $9,806.92 vs 09:30 $9,712.10 (session +127.60) | 16:00 close · cash $494.02 · equity $9,806.92 vs 09:30 $9,712.10 (+94.82; session marks +127.60) · 8 name(s) marked open→close (per-name table). BBWI×66 09:30 $18.26 → close $18.90 +42.24; INTU×3 09:30 $323.47 → close $345.88 +67.23; NCNO×62 09:30 $19.33 → close $21.51 +135.16; HEI×3 09:30 $370.00 → close $346.15 -71.55; BE×5 09:30 $213.94 → close $218.21 +21.35; QMLS×187 09:30 $6.47 → close $6.10 -69.19; NVTS×96 09:30 $12.60 → close $12.67 +6.72; FLNC×109 09:30 $11.12 → close $11.08 -4.36 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $494.02 | ▲ 09:30 equity $10,032.78 vs yday $9,806.92 (+225.86) | 09:30 open · cash $494.02 (unchanged overnight, no fees) · equity $10,032.78 vs prior close $9,806.92 (+225.86) · 8 name(s) re-marked at the open (per-name table). BBWI×66 yday $18.90 → 09:30 $18.69 -13.86; INTU×3 yday $345.88 → 09:30 $353.54 +22.98; NCNO×62 yday $21.51 → 09:30 $22.03 +32.24; HEI×3 yday $346.15 → 09:30 $346.19 +0.12; BE×5 yday $218.21 → 09:30 $227.10 +44.45; QMLS×187 yday $6.10 → 09:30 $6.33 +43.01; NVTS×96 yday $12.67 → 09:30 $13.18 +48.96; FLNC×109 yday $11.08 → 09:30 $11.52 +47.96 | — |
| 2026-08-27 09:30 ET | **SELL** | `BBWI` | 66 | $18.69 | $2.21 | $+23.98 | $1,725.35 | ▲ +23.98 after sell → book $10,030.57; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INTU` | 3 | $353.54 | $2.02 | $+86.19 | $2,783.95 | ▲ +86.19 after sell → book $10,028.55; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `NCNO` | 62 | $22.03 | $2.20 | $+163.03 | $4,147.61 | ▲ +163.03 after sell → book $10,026.35; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `HEI` | 3 | $346.19 | $2.02 | $-75.45 | $5,184.17 | ▼ -75.45 after sell → book $10,024.34; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `BE` | 5 | $227.10 | $2.02 | $+61.77 | $6,317.64 | ▲ +61.77 after sell → book $10,022.31; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `QMLS` | 187 | $6.33 | $2.59 | $-31.32 | $7,498.76 | ▼ -31.32 after sell → book $10,019.72; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `NVTS` | 96 | $13.18 | $2.30 | $+51.10 | $8,761.73 | ▲ +51.10 after sell → book $10,017.41; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 109 | $11.52 | $2.35 | $+38.94 | $10,015.07 | ▲ +38.94 after sell → book $10,015.07; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,015.07 | ▲ close $10,015.07 vs 09:30 $10,032.78 (session +0.00) | 16:00 close · cash $10,015.07 · no lots left · equity $10,015.07. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,015.07 | ▲ 09:30 equity $10,015.07 vs yday $10,015.07 (-0.00) | 09:30 open · cash $10,015.07 · no holdings · equity $10,015.07 vs prior close $10,015.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 4 | $306.34 | $2.00 | — | $8,787.71 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-23.0; leftover $1251.88 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 66 | $18.75 | $2.19 | — | $7,548.02 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=-5.0; leftover $1251.88 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SYRE` | 13 | $91.75 | $2.03 | — | $6,353.24 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-13.2; leftover $1251.88 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 38 | $32.90 | $2.10 | — | $5,100.94 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1251.88 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 43 | $28.91 | $2.12 | — | $3,855.69 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=+9.2; leftover $1251.88 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 9 | $137.19 | $2.02 | — | $2,618.96 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+7.1; leftover $1251.88 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 65 | $19.00 | $2.19 | — | $1,381.77 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+7.5; leftover $1251.88 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 33 | $37.49 | $2.09 | — | $142.52 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ret5=+5.4; leftover $1251.88 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.52 | ▼ close $9,809.58 vs 09:30 $10,015.07 (session -188.76) | 16:00 close · cash $142.52 · equity $9,809.58 vs 09:30 $10,015.07 (-205.49; session marks -188.76) · 8 name(s) marked open→close (per-name table). DY×4 09:30 $306.34 → close $294.34 -48.00; BBWI×66 09:30 $18.75 → close $19.22 +31.02; SYRE×13 09:30 $91.75 → close $90.36 -18.07; SEDG×38 09:30 $32.90 → close $31.41 -56.62; ZYME×43 09:30 $28.91 → close $28.27 -27.52; MRNA×9 09:30 $137.19 → close $137.99 +7.20; TH×65 09:30 $19.00 → close $18.55 -29.25; FIGR×33 09:30 $37.49 → close $36.05 -47.52 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.52 | ▼ 09:30 equity $9,719.72 vs yday $9,809.58 (-89.86) | 09:30 open · cash $142.52 (unchanged overnight, no fees) · equity $9,719.72 vs prior close $9,809.58 (-89.86) · 8 name(s) re-marked at the open (per-name table). DY×4 yday $294.34 → 09:30 $298.01 +14.68; BBWI×66 yday $19.22 → 09:30 $19.25 +1.98; SYRE×13 yday $90.36 → 09:30 $89.15 -15.73; SEDG×38 yday $31.41 → 09:30 $31.15 -9.88; ZYME×43 yday $28.27 → 09:30 $28.06 -9.03; MRNA×9 yday $137.99 → 09:30 $134.10 -35.01; TH×65 yday $18.55 → 09:30 $18.12 -27.63; FIGR×33 yday $36.05 → 09:30 $35.77 -9.24 | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 4 | $298.01 | $2.02 | $-37.34 | $1,332.53 | ▼ -37.34 after sell → book $9,717.70; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 66 | $19.25 | $2.21 | $+28.60 | $2,600.82 | ▲ +28.60 after sell → book $9,715.49; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SYRE` | 13 | $89.15 | $2.05 | $-37.88 | $3,757.73 | ▼ -37.88 after sell → book $9,713.44; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 38 | $31.15 | $2.12 | $-70.73 | $4,939.30 | ▼ -70.73 after sell → book $9,711.32; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 43 | $28.06 | $2.14 | $-40.81 | $6,143.74 | ▼ -40.81 after sell → book $9,709.18; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 65 | $18.12 | $2.21 | $-61.27 | $7,319.66 | ▼ -61.27 after sell → book $9,706.97; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 33 | $35.77 | $2.11 | $-60.96 | $8,497.96 | ▼ -60.96 after sell → book $9,704.86; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,497.96 | ▲ close $9,761.02 vs 09:30 $9,719.72 (session +56.16) | 16:00 close · cash $8,497.96 · equity $9,761.02 vs 09:30 $9,719.72 (+41.30; session marks +56.16) · 1 name(s) marked open→close (per-name table). MRNA×9 09:30 $134.10 → close $140.34 +56.16 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,497.96 | ▼ 09:30 equity $9,760.21 vs yday $9,761.02 (-0.81) | 09:30 open · cash $8,497.96 (unchanged overnight, no fees) · equity $9,760.21 vs prior close $9,761.02 (-0.81) · 1 name(s) re-marked at the open (per-name table). MRNA×9 yday $140.34 → 09:30 $140.25 -0.81 | — |
| 2026-09-01 09:30 ET | **SELL** | `MRNA` | 9 | $140.25 | $2.04 | $+23.49 | $9,758.18 | ▲ +23.49 after sell → book $9,758.18; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟡 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,758.18 | ▲ close $9,758.18 vs 09:30 $9,760.21 (session +0.00) | 16:00 close · cash $9,758.18 · no lots left · equity $9,758.18. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,758.18 | ▲ 09:30 equity $9,758.18 vs yday $9,758.18 (-0.00) | 09:30 open · cash $9,758.18 · no holdings · equity $9,758.18 vs prior close $9,758.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,758.18 | ▲ close $9,758.18 vs 09:30 $9,758.18 (session +0.00) | 16:00 close · cash $9,758.18 · no lots left · equity $9,758.18. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,758.18 | ▲ 09:30 equity $9,758.18 vs yday $9,758.18 (-0.00) | 09:30 open · cash $9,758.18 · no holdings · equity $9,758.18 vs prior close $9,758.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $55.42 | $2.06 | — | $8,536.88 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-25.9; leftover $1219.77 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ENOV` | 60 | $20.28 | $2.17 | — | $7,317.91 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-20.0; leftover $1219.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 4 | $257.00 | $2.00 | — | $6,287.91 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-5.5; leftover $1219.77 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NTSK` | 78 | $15.51 | $2.22 | — | $5,075.90 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-1.2; leftover $1219.77 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NTAP` | 7 | $161.95 | $2.01 | — | $3,940.24 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-6.7; leftover $1219.77 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $2,721.94 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1219.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 15 | $76.86 | $2.04 | — | $1,567.01 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-6.6; leftover $1219.77 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RPD` | 107 | $11.39 | $2.31 | — | $345.97 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-6.1; leftover $1219.77 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $345.97 | ▼ close $9,663.06 vs 09:30 $9,758.18 (session -78.25) | 16:00 close · cash $345.97 · equity $9,663.06 vs 09:30 $9,758.18 (-95.12; session marks -78.25) · 8 name(s) marked open→close (per-name table). EIX×22 09:30 $55.42 → close $56.30 +19.36; ENOV×60 09:30 $20.28 → close $19.40 -52.80; FIVE×4 09:30 $257.00 → close $239.96 -68.16; NTSK×78 09:30 $15.51 → close $14.34 -91.26; NTAP×7 09:30 $161.95 → close $185.38 +164.01; ATRC×23 09:30 $52.88 → close $52.46 -9.66; VSXY×15 09:30 $76.86 → close $73.64 -48.30; RPD×107 09:30 $11.39 → close $11.47 +8.56 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $345.97 | ▼ 09:30 equity $9,554.05 vs yday $9,663.06 (-109.01) | 09:30 open · cash $345.97 (unchanged overnight, no fees) · equity $9,554.05 vs prior close $9,663.06 (-109.01) · 8 name(s) re-marked at the open (per-name table). EIX×22 yday $56.30 → 09:30 $55.79 -11.22; ENOV×60 yday $19.40 → 09:30 $19.01 -23.40; FIVE×4 yday $239.96 → 09:30 $238.88 -4.32; NTSK×78 yday $14.34 → 09:30 $14.15 -14.82; NTAP×7 yday $185.38 → 09:30 $182.59 -19.53; ATRC×23 yday $52.46 → 09:30 $52.03 -9.89; VSXY×15 yday $73.64 → 09:30 $73.63 -0.15; RPD×107 yday $11.47 → 09:30 $11.23 -25.68 | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 22 | $55.79 | $2.08 | $+4.01 | $1,571.27 | ▲ +4.01 after sell → book $9,551.97; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ENOV` | 60 | $19.01 | $2.19 | $-80.56 | $2,709.68 | ▼ -80.56 after sell → book $9,549.78; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 4 | $238.88 | $2.02 | $-76.50 | $3,663.18 | ▼ -76.50 after sell → book $9,547.76; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NTSK` | 78 | $14.15 | $2.25 | $-110.55 | $4,764.63 | ▼ -110.55 after sell → book $9,545.51; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NTAP` | 7 | $182.59 | $2.03 | $+140.44 | $6,040.73 | ▲ +140.44 after sell → book $9,543.48; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 23 | $52.03 | $2.08 | $-23.69 | $7,235.34 | ▼ -23.69 after sell → book $9,541.40; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSXY` | 15 | $73.63 | $2.06 | $-52.54 | $8,337.74 | ▼ -52.54 after sell → book $9,539.35; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RPD` | 107 | $11.23 | $2.34 | $-21.77 | $9,537.01 | ▼ -21.77 after sell → book $9,537.01; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 136 | $8.74 | $2.40 | — | $8,345.97 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-0.8; leftover $1192.13 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 8 | $137.35 | $2.01 | — | $7,245.16 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+5.4; leftover $1192.13 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRCL` | 12 | $97.98 | $2.03 | — | $6,067.37 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+9.5; leftover $1192.13 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 17 | $68.52 | $2.04 | — | $4,900.49 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+3.4; leftover $1192.13 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 7 | $167.55 | $2.01 | — | $3,725.63 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; ret5=+0.9; leftover $1192.13 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BLSH` | 34 | $34.69 | $2.09 | — | $2,544.08 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+7.9; leftover $1192.13 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ZETA` | 36 | $32.65 | $2.10 | — | $1,366.58 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+8.1; leftover $1192.13 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 5 | $236.82 | $2.00 | — | $180.47 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+8.1; leftover $1192.13 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $180.47 | ▲ close $9,662.49 vs 09:30 $9,554.05 (session +142.17) | 16:00 close · cash $180.47 · equity $9,662.49 vs 09:30 $9,554.05 (+108.44; session marks +142.17) · 8 name(s) marked open→close (per-name table). ASAN×136 09:30 $8.74 → close $8.81 +9.52; MSTR×8 09:30 $137.35 → close $142.80 +43.60; CRCL×12 09:30 $97.98 → close $102.05 +48.84; DOCU×17 09:30 $68.52 → close $68.41 -1.87; GWRE×7 09:30 $167.55 → close $162.42 -35.91; BLSH×34 09:30 $34.69 → close $36.00 +44.54; ZETA×36 09:30 $32.65 → close $31.35 -46.80; BE×5 09:30 $236.82 → close $252.87 +80.25 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $180.47 | ▼ 09:30 equity $9,618.28 vs yday $9,662.49 (-44.21) | 09:30 open · cash $180.47 (unchanged overnight, no fees) · equity $9,618.28 vs prior close $9,662.49 (-44.21) · 8 name(s) re-marked at the open (per-name table). ASAN×136 yday $8.81 → 09:30 $8.73 -10.88; MSTR×8 yday $142.80 → 09:30 $137.62 -41.44; CRCL×12 yday $102.05 → 09:30 $100.65 -16.80; DOCU×17 yday $68.41 → 09:30 $67.05 -23.12; GWRE×7 yday $162.42 → 09:30 $160.52 -13.30; BLSH×34 yday $36.00 → 09:30 $35.90 -3.40; ZETA×36 yday $31.35 → 09:30 $31.08 -9.72; BE×5 yday $252.87 → 09:30 $267.76 +74.45 | — |
| 2026-09-08 09:30 ET | **SELL** | `ASAN` | 136 | $8.73 | $2.43 | $-6.19 | $1,365.32 | ▼ -6.19 after sell → book $9,615.85; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MSTR` | 8 | $137.62 | $2.03 | $-1.89 | $2,464.25 | ▼ -1.89 after sell → book $9,613.82; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `CRCL` | 12 | $100.65 | $2.05 | $+27.97 | $3,670.00 | ▲ +27.97 after sell → book $9,611.77; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 17 | $67.05 | $2.06 | $-29.09 | $4,807.79 | ▼ -29.09 after sell → book $9,609.71; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 7 | $160.52 | $2.03 | $-53.25 | $5,929.40 | ▼ -53.25 after sell → book $9,607.68; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BLSH` | 34 | $35.90 | $2.11 | $+36.94 | $7,147.89 | ▲ +36.94 after sell → book $9,605.57; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ZETA` | 36 | $31.08 | $2.12 | $-60.74 | $8,264.65 | ▼ -60.74 after sell → book $9,603.45; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 5 | $267.76 | $2.03 | $+150.67 | $9,601.43 | ▲ +150.67 after sell → book $9,601.43; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,601.43 | ▲ close $9,601.43 vs 09:30 $9,618.28 (session +0.00) | 16:00 close · cash $9,601.43 · no lots left · equity $9,601.43. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,601.43 | ▲ 09:30 equity $9,601.43 vs yday $9,601.43 (-0.00) | 09:30 open · cash $9,601.43 · no holdings · equity $9,601.43 vs prior close $9,601.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,601.43 | ▲ close $9,601.43 vs 09:30 $9,601.43 (session +0.00) | 16:00 close · cash $9,601.43 · no lots left · equity $9,601.43. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,601.43 | ▲ 09:30 equity $9,601.43 vs yday $9,601.43 (-0.00) | 09:30 open · cash $9,601.43 · no holdings · equity $9,601.43 vs prior close $9,601.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,601.43 | ▲ close $9,601.43 vs 09:30 $9,601.43 (session +0.00) | 16:00 close · cash $9,601.43 · no lots left · equity $9,601.43. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,601.43 | ▲ 09:30 equity $9,601.43 vs yday $9,601.43 (-0.00) | 09:30 open · cash $9,601.43 · no holdings · equity $9,601.43 vs prior close $9,601.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `PBR` | 75 | $21.21 | $2.21 | — | $8,008.46 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+2.5; leftover $1600.24 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INTR` | 276 | $5.78 | $3.56 | — | $6,409.62 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=-1.7; leftover $1600.24 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INSP` | 22 | $69.88 | $2.06 | — | $4,870.20 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+8.0; leftover $1600.24 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 30 | $52.55 | $2.08 | — | $3,291.62 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1600.24 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 10 | $157.55 | $2.02 | — | $1,714.10 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1600.24 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟡 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 138 | $11.55 | $2.40 | — | $117.80 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1600.24 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $117.80 | ▲ close $9,764.70 vs 09:30 $9,601.43 (session +177.61) | 16:00 close · cash $117.80 · equity $9,764.70 vs 09:30 $9,601.43 (+163.27; session marks +177.61) · 6 name(s) marked open→close (per-name table). PBR×75 09:30 $21.21 → close $21.20 -0.75; INTR×276 09:30 $5.78 → close $5.71 -19.32; INSP×22 09:30 $69.88 → close $73.00 +68.64; BAND×30 09:30 $52.55 → close $56.87 +129.60; RDDT×10 09:30 $157.55 → close $157.77 +2.20; FUBO×138 09:30 $11.55 → close $11.53 -2.76 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $117.80 | ▼ 09:30 equity $9,714.65 vs yday $9,764.70 (-50.05) | 09:30 open · cash $117.80 (unchanged overnight, no fees) · equity $9,714.65 vs prior close $9,764.70 (-50.05) · 6 name(s) re-marked at the open (per-name table). PBR×75 yday $21.20 → 09:30 $21.23 +2.25; INTR×276 yday $5.71 → 09:30 $5.49 -60.72; INSP×22 yday $73.00 → 09:30 $72.14 -18.92; BAND×30 yday $56.87 → 09:30 $56.90 +0.90; RDDT×10 yday $157.77 → 09:30 $160.00 +22.30; FUBO×138 yday $11.53 → 09:30 $11.56 +4.14 | — |
| 2026-09-14 09:30 ET | **SELL** | `PBR` | 75 | $21.23 | $2.24 | $-2.96 | $1,707.81 | ▼ -2.96 after sell → book $9,712.41; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INTR` | 276 | $5.49 | $3.62 | $-87.22 | $3,219.43 | ▼ -87.22 after sell → book $9,708.79; vs 09:30 mark -3.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INSP` | 22 | $72.14 | $2.08 | $+45.59 | $4,804.43 | ▲ +45.59 after sell → book $9,706.71; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 30 | $56.90 | $2.10 | $+126.32 | $6,509.33 | ▲ +126.32 after sell → book $9,704.61; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `RDDT` | 10 | $160.00 | $2.04 | $+20.44 | $8,107.29 | ▲ +20.44 after sell → book $9,702.57; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `FUBO` | 138 | $11.56 | $2.44 | $-3.46 | $9,700.13 | ▼ -3.46 after sell → book $9,700.13; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,700.13 | ▲ close $9,700.13 vs 09:30 $9,714.65 (session +0.00) | 16:00 close · cash $9,700.13 · no lots left · equity $9,700.13. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,700.13 | ▲ 09:30 equity $9,700.13 vs yday $9,700.13 (-0.00) | 09:30 open · cash $9,700.13 · no holdings · equity $9,700.13 vs prior close $9,700.13 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,700.13 | ▲ close $9,700.13 vs 09:30 $9,700.13 (session +0.00) | 16:00 close · cash $9,700.13 · no lots left · equity $9,700.13. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,700.13 | ▲ 09:30 equity $9,700.13 vs yday $9,700.13 (-0.00) | 09:30 open · cash $9,700.13 · no holdings · equity $9,700.13 vs prior close $9,700.13 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 13 | $87.40 | $2.03 | — | $8,561.90 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1212.52 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 206 | $5.87 | $2.66 | — | $7,350.02 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1212.52 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 catal🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ILMN` | 5 | $224.49 | $2.00 | — | $6,225.57 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+5.3; leftover $1212.52 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `QLYS` | 6 | $179.60 | $2.01 | — | $5,145.96 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=+8.8; leftover $1212.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 65 | $18.61 | $2.19 | — | $3,934.12 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1212.52 | join🟢 sector🟢 gen🟢 news🔴 digest🔴 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 17 | $68.79 | $2.04 | — | $2,762.65 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1212.52 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 31 | $38.01 | $2.08 | — | $1,582.26 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $1212.52 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 174 | $6.95 | $2.51 | — | $370.45 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-5.8; leftover $1212.52 | join🔴 sector🟢 gen🟢 news🟢 digest🔴 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $370.45 | ▲ close $9,828.75 vs 09:30 $9,700.13 (session +146.14) | 16:00 close · cash $370.45 · equity $9,828.75 vs 09:30 $9,700.13 (+128.62; session marks +146.14) · 8 name(s) marked open→close (per-name table). VAL×13 09:30 $87.40 → close $82.52 -63.44; RIG×206 09:30 $5.87 → close $5.54 -67.98; ILMN×5 09:30 $224.49 → close $228.93 +22.20; QLYS×6 09:30 $179.60 → close $183.06 +20.76; BBNX×65 09:30 $18.61 → close $22.18 +232.05; TEM×17 09:30 $68.79 → close $69.97 +20.06; KRMN×31 09:30 $38.01 → close $36.94 -33.17; SION×174 09:30 $6.95 → close $7.04 +15.66 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $370.45 | ▲ 09:30 equity $9,991.07 vs yday $9,828.75 (+162.32) | 09:30 open · cash $370.45 (unchanged overnight, no fees) · equity $9,991.07 vs prior close $9,828.75 (+162.32) · 8 name(s) re-marked at the open (per-name table). VAL×13 yday $82.52 → 09:30 $83.20 +8.84; RIG×206 yday $5.54 → 09:30 $5.58 +8.24; ILMN×5 yday $228.93 → 09:30 $233.85 +24.60; QLYS×6 yday $183.06 → 09:30 $180.82 -13.44; BBNX×65 yday $22.18 → 09:30 $22.46 +18.20; TEM×17 yday $69.97 → 09:30 $72.70 +46.41; KRMN×31 yday $36.94 → 09:30 $37.89 +29.45; SION×174 yday $7.04 → 09:30 $7.27 +40.02 | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 13 | $83.20 | $2.05 | $-58.68 | $1,450.00 | ▼ -58.68 after sell → book $9,989.02; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 206 | $5.58 | $2.70 | $-65.10 | $2,596.78 | ▼ -65.10 after sell → book $9,986.32; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ILMN` | 5 | $233.85 | $2.02 | $+42.77 | $3,764.00 | ▲ +42.77 after sell → book $9,984.29; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `QLYS` | 6 | $180.82 | $2.03 | $+3.28 | $4,846.89 | ▲ +3.28 after sell → book $9,982.26; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BBNX` | 65 | $22.46 | $2.21 | $+245.86 | $6,304.59 | ▲ +245.86 after sell → book $9,980.06; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🔴 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 17 | $72.70 | $2.06 | $+62.37 | $7,538.42 | ▲ +62.37 after sell → book $9,977.99; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `KRMN` | 31 | $37.89 | $2.10 | $-7.91 | $8,710.91 | ▼ -7.91 after sell → book $9,975.89; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 174 | $7.27 | $2.55 | $+50.62 | $9,973.34 | ▲ +50.62 after sell → book $9,973.34; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BULL` | 156 | $7.95 | $2.46 | — | $8,730.68 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $1246.67 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `MAMA` | 87 | $14.21 | $2.25 | — | $7,492.16 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=-6.5; leftover $1246.67 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🟡 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 164 | $7.59 | $2.48 | — | $6,244.92 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1246.67 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $5,046.96 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1246.67 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `EROC` | 98 | $12.64 | $2.28 | — | $3,805.95 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-3.6; leftover $1246.67 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 1 | $934.88 | $1.99 | — | $2,869.08 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=-7.0; leftover $1246.67 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `CRDO` | 7 | $168.65 | $2.01 | — | $1,686.52 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=-3.8; leftover $1246.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AXTI` | 18 | $67.91 | $2.04 | — | $462.10 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.7; leftover $1246.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $462.10 | ▼ close $9,898.50 vs 09:30 $9,991.07 (session -57.31) | 16:00 close · cash $462.10 · equity $9,898.50 vs 09:30 $9,991.07 (-92.57; session marks -57.31) · 8 name(s) marked open→close (per-name table). BULL×156 09:30 $7.95 → close $7.71 -37.44; MAMA×87 09:30 $14.21 → close $13.11 -95.70; PGEN×164 09:30 $7.59 → close $7.87 +45.92; SMTC×7 09:30 $170.85 → close $178.19 +51.38; EROC×98 09:30 $12.64 → close $12.90 +25.48; LITE×1 09:30 $934.88 → close $893.61 -41.27; CRDO×7 09:30 $168.65 → close $168.25 -2.80; AXTI×18 09:30 $67.91 → close $67.75 -2.88 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $462.10 | ▲ 09:30 equity $10,037.29 vs yday $9,898.50 (+138.79) | 09:30 open · cash $462.10 (unchanged overnight, no fees) · equity $10,037.29 vs prior close $9,898.50 (+138.79) · 8 name(s) re-marked at the open (per-name table). BULL×156 yday $7.71 → 09:30 $7.85 +21.84; MAMA×87 yday $13.11 → 09:30 $12.91 -17.40; PGEN×164 yday $7.87 → 09:30 $7.98 +18.04; SMTC×7 yday $178.19 → 09:30 $182.33 +28.98; EROC×98 yday $12.90 → 09:30 $13.00 +9.80; LITE×1 yday $893.61 → 09:30 $915.66 +22.05; CRDO×7 yday $168.25 → 09:30 $171.11 +20.02; AXTI×18 yday $67.75 → 09:30 $69.72 +35.46 | — |
| 2026-09-18 09:30 ET | **SELL** | `BULL` | 156 | $7.85 | $2.49 | $-20.55 | $1,684.20 | ▼ -20.55 after sell → book $10,034.79; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `MAMA` | 87 | $12.91 | $2.28 | $-117.63 | $2,805.10 | ▼ -117.63 after sell → book $10,032.52; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 164 | $7.98 | $2.52 | $+58.96 | $4,111.30 | ▲ +58.96 after sell → book $10,030.00; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $5,385.58 | ▲ +76.32 after sell → book $10,027.97; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `EROC` | 98 | $13.00 | $2.31 | $+30.69 | $6,657.27 | ▲ +30.69 after sell → book $10,025.66; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 1 | $915.66 | $2.01 | $-23.23 | $7,570.91 | ▼ -23.23 after sell → book $10,023.64; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CRDO` | 7 | $171.11 | $2.03 | $+13.18 | $8,766.65 | ▲ +13.18 after sell → book $10,021.61; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AXTI` | 18 | $69.72 | $2.06 | $+28.47 | $10,019.55 | ▲ +28.47 after sell → book $10,019.55; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 178 | $14.07 | $2.52 | — | $7,512.56 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2504.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 119 | $20.91 | $2.35 | — | $5,021.93 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2504.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `AMD` | 4 | $547.37 | $2.00 | — | $2,830.44 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+8.2; leftover $2504.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `SHLS` | 327 | $7.64 | $4.22 | — | $327.95 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+7.6; leftover $2504.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $327.95 | ▼ close $9,998.40 vs 09:30 $10,037.29 (session -10.06) | 16:00 close · cash $327.95 · equity $9,998.40 vs 09:30 $10,037.29 (-38.89; session marks -10.06) · 4 name(s) marked open→close (per-name table). BHVN×178 09:30 $14.07 → close $13.62 -80.10; TH×119 09:30 $20.91 → close $21.19 +33.32; AMD×4 09:30 $547.37 → close $559.82 +49.80; SHLS×327 09:30 $7.64 → close $7.60 -13.08 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $327.95 | ▲ 09:30 equity $10,235.19 vs yday $9,998.40 (+236.79) | 09:30 open · cash $327.95 (unchanged overnight, no fees) · equity $10,235.19 vs prior close $9,998.40 (+236.79) · 4 name(s) re-marked at the open (per-name table). BHVN×178 yday $13.62 → 09:30 $13.90 +49.84; TH×119 yday $21.19 → 09:30 $21.65 +54.74; AMD×4 yday $559.82 → 09:30 $583.88 +96.24; SHLS×327 yday $7.60 → 09:30 $7.71 +35.97 | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 178 | $13.90 | $2.57 | $-35.36 | $2,799.57 | ▼ -35.36 after sell → book $10,232.61; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 119 | $21.65 | $2.39 | $+83.33 | $5,373.54 | ▲ +83.33 after sell → book $10,230.23; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `AMD` | 4 | $583.88 | $2.03 | $+142.01 | $7,707.03 | ▲ +142.01 after sell → book $10,228.20; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `SHLS` | 327 | $7.71 | $4.29 | $+14.38 | $10,223.90 | ▲ +14.38 after sell → book $10,223.90; vs 09:30 mark -4.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,223.90 | ▲ close $10,223.90 vs 09:30 $10,235.19 (session +0.00) | 16:00 close · cash $10,223.90 · no lots left · equity $10,223.90. | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,223.90 | ▲ 09:30 equity $10,223.90 vs yday $10,223.90 (+0.00) | 09:30 open · cash $10,223.90 · no holdings · equity $10,223.90 vs prior close $10,223.90 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,223.90 | ▲ close $10,223.90 vs 09:30 $10,223.90 (session +0.00) | 16:00 close · cash $10,223.90 · no lots left · equity $10,223.90. | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,223.90 | ▲ 09:30 equity $10,223.90 vs yday $10,223.90 (+0.00) | 09:30 open · cash $10,223.90 · no holdings · equity $10,223.90 vs prior close $10,223.90 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,223.90 | ▲ close $10,223.90 vs 09:30 $10,223.90 (session +0.00) | 16:00 close · cash $10,223.90 · no lots left · equity $10,223.90. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `CBRS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBNX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `COHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AAOI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEVA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EROC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `FNKO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WEN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `HAL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KRMN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OBE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VIR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SDGR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SIBN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `STX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ARLO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NMAX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ON` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NTAP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PANW` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IOT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SIMO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
