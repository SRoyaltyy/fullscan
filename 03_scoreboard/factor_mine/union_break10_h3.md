# Factor mine action — `union_break10_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ break10, no 🚨

Cash book **-5.00%** ($9,500) · signal-only (no cash/fees) was +13.69%. Starts YES **4/20**. Fills 83 · skips 182 · realized $-503.71.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the name broke its prior 10-session range.
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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `break_10=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,482.54.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `IREN` | 217 | — | $45.98 | +0.00 | $44.76 | -264.74 | -264.74 | +0.00 | -264.74 |
| 2026-08-14 | `IREN` | 217 | $44.76 | $44.09 | -145.39 | $44.06 | -6.51 | -151.90 | -410.13 | -416.64 |
| 2026-08-17 | `IREN` | 217 | $44.06 | $45.23 | +253.89 | $44.90 | -71.61 | +182.28 | -162.75 | -234.36 |
| 2026-08-17 | `NPWR` | 1 | — | $1.92 | +0.00 | $1.73 | -0.19 | -0.19 | +0.00 | -0.19 |
| 2026-08-18 | `IREN` | 217 | $44.90 | $43.56 | -290.78 | — | +0.00 | -290.78 | -525.14 | — |
| 2026-08-18 | `NPWR` | 1 | $1.73 | $1.70 | -0.03 | $1.65 | -0.05 | -0.08 | -0.22 | -0.27 |
| 2026-08-19 | `NPWR` | 1 | $1.65 | $1.70 | +0.05 | $1.67 | -0.03 | +0.02 | -0.22 | -0.25 |
| 2026-08-20 | `NPWR` | 1 | $1.67 | $1.64 | -0.03 | — | +0.00 | -0.03 | -0.28 | — |
| 2026-08-20 | `AG` | 57 | — | $20.55 | +0.00 | $21.19 | +36.48 | +36.48 | +0.00 | +36.48 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 57 | — | $20.65 | +0.00 | $21.11 | +26.22 | +26.22 | +0.00 | +26.22 |
| 2026-08-20 | `IAG` | 60 | — | $19.63 | +0.00 | $20.50 | +52.20 | +52.20 | +0.00 | +52.20 |
| 2026-08-20 | `KGC` | 39 | — | $29.63 | +0.00 | $31.43 | +70.20 | +70.20 | +0.00 | +70.20 |
| 2026-08-20 | `NFGC` | 676 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-20 | `ABUS` | 240 | — | $4.92 | +0.00 | $4.77 | -36.00 | -36.00 | +0.00 | -36.00 |
| 2026-08-21 | `AG` | 57 | $21.19 | $21.90 | +40.47 | $21.09 | -46.17 | -5.70 | +76.95 | +30.78 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | $97.03 | +17.03 | +44.20 | +61.23 | +78.26 |
| 2026-08-21 | `CDE` | 57 | $21.11 | $21.75 | +36.48 | $20.97 | -44.46 | -7.98 | +62.70 | +18.24 |
| 2026-08-21 | `IAG` | 60 | $20.50 | $21.17 | +40.20 | $21.14 | -1.80 | +38.40 | +92.40 | +90.60 |
| 2026-08-21 | `KGC` | 39 | $31.43 | $32.17 | +28.86 | $32.76 | +23.01 | +51.87 | +99.06 | +122.07 |
| 2026-08-21 | `NFGC` | 676 | $1.75 | $1.79 | +27.04 | $1.84 | +33.80 | +60.84 | +27.04 | +60.84 |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | $157.78 | +24.64 | +60.24 | +81.28 | +105.92 |
| 2026-08-21 | `ABUS` | 240 | $4.77 | $5.20 | +103.20 | $5.21 | +2.40 | +105.60 | +67.20 | +69.60 |
| 2026-08-21 | `CYPH` | 5 | — | $1.32 | +0.00 | $1.42 | +0.50 | +0.50 | +0.00 | +0.50 |
| 2026-08-21 | `ORBS` | 8 | — | $0.86 | +0.00 | $0.88 | +0.13 | +0.13 | +0.00 | +0.13 |
| 2026-08-21 | `CAN` | 25 | — | $0.29 | +0.00 | $0.35 | +1.52 | +1.52 | +0.00 | +1.52 |
| 2026-08-24 | `AG` | 57 | $21.09 | $21.30 | +11.97 | $20.83 | -26.79 | -14.82 | +42.75 | +15.96 |
| 2026-08-24 | `BHP` | 13 | $97.03 | $97.31 | +3.64 | $97.13 | -2.34 | +1.30 | +81.90 | +79.56 |
| 2026-08-24 | `CDE` | 57 | $20.97 | $21.26 | +16.53 | $20.88 | -21.66 | -5.13 | +34.77 | +13.11 |
| 2026-08-24 | `IAG` | 60 | $21.14 | $21.38 | +14.40 | $21.80 | +25.20 | +39.60 | +105.00 | +130.20 |
| 2026-08-24 | `KGC` | 39 | $32.76 | $33.03 | +10.53 | $32.98 | -1.95 | +8.58 | +132.60 | +130.65 |
| 2026-08-24 | `NFGC` | 676 | $1.84 | $1.86 | +13.52 | $1.90 | +27.04 | +40.56 | +74.36 | +101.40 |
| 2026-08-24 | `WPM` | 8 | $157.78 | $159.50 | +13.76 | $160.19 | +5.52 | +19.28 | +119.68 | +125.20 |
| 2026-08-24 | `ABUS` | 240 | $5.21 | $5.18 | -7.20 | $5.20 | +4.80 | -2.40 | +62.40 | +67.20 |
| 2026-08-24 | `CYPH` | 5 | $1.42 | $1.83 | +2.05 | $1.68 | -0.75 | +1.30 | +2.55 | +1.80 |
| 2026-08-24 | `ORBS` | 8 | $0.88 | $0.89 | +0.08 | $0.84 | -0.40 | -0.32 | +0.21 | -0.19 |
| 2026-08-24 | `CAN` | 25 | $0.35 | $0.38 | +0.70 | $0.36 | -0.58 | +0.12 | +2.23 | +1.65 |
| 2026-08-25 | `AG` | 57 | $20.83 | $20.32 | -29.07 | — | +0.00 | -29.07 | -13.11 | — |
| 2026-08-25 | `BHP` | 13 | $97.13 | $95.86 | -16.51 | — | +0.00 | -16.51 | +63.05 | — |
| 2026-08-25 | `CDE` | 57 | $20.88 | $20.47 | -23.37 | — | +0.00 | -23.37 | -10.26 | — |
| 2026-08-25 | `IAG` | 60 | $21.80 | $21.21 | -35.40 | — | +0.00 | -35.40 | +94.80 | — |
| 2026-08-25 | `KGC` | 39 | $32.98 | $32.32 | -25.74 | — | +0.00 | -25.74 | +104.91 | — |
| 2026-08-25 | `NFGC` | 676 | $1.90 | $1.90 | +0.00 | — | +0.00 | +0.00 | +101.40 | — |
| 2026-08-25 | `WPM` | 8 | $160.19 | $156.51 | -29.44 | — | +0.00 | -29.44 | +95.76 | — |
| 2026-08-25 | `ABUS` | 240 | $5.20 | $5.25 | +12.00 | — | +0.00 | +12.00 | +79.20 | — |
| 2026-08-25 | `CYPH` | 5 | $1.68 | $1.56 | -0.60 | $1.64 | +0.40 | -0.20 | +1.20 | +1.60 |
| 2026-08-25 | `ORBS` | 8 | $0.84 | $0.83 | -0.08 | $0.80 | -0.24 | -0.32 | -0.27 | -0.51 |
| 2026-08-25 | `CAN` | 25 | $0.36 | $0.36 | +0.00 | $0.42 | +1.42 | +1.42 | +1.65 | +3.08 |
| 2026-08-25 | `LIFE` | 38 | — | $36.96 | +0.00 | $38.56 | +60.80 | +60.80 | +0.00 | +60.80 |
| 2026-08-25 | `BMEA` | 868 | — | $1.63 | +0.00 | $1.73 | +86.80 | +86.80 | +0.00 | +86.80 |
| 2026-08-25 | `ALVO` | 270 | — | $5.24 | +0.00 | $5.05 | -51.30 | -51.30 | +0.00 | -51.30 |
| 2026-08-25 | `SUJA` | 161 | — | $8.79 | +0.00 | $9.33 | +86.94 | +86.94 | +0.00 | +86.94 |
| 2026-08-25 | `FWDI` | 248 | — | $5.71 | +0.00 | $6.05 | +84.32 | +84.32 | +0.00 | +84.32 |
| 2026-08-25 | `GORO` | 398 | — | $3.55 | +0.00 | $3.87 | +127.36 | +127.36 | +0.00 | +127.36 |
| 2026-08-25 | `ASST` | 73 | — | $19.04 | +0.00 | $21.39 | +171.55 | +171.55 | +0.00 | +171.55 |
| 2026-08-26 | `CYPH` | 5 | $1.64 | $1.60 | -0.20 | — | +0.00 | -0.20 | +1.40 | — |
| 2026-08-26 | `ORBS` | 8 | $0.80 | $0.80 | -0.03 | — | +0.00 | -0.03 | -0.54 | — |
| 2026-08-26 | `CAN` | 25 | $0.42 | $0.40 | -0.50 | — | +0.00 | -0.50 | +2.58 | — |
| 2026-08-26 | `LIFE` | 38 | $38.56 | $38.24 | -12.16 | $39.11 | +33.06 | +20.90 | +48.64 | +81.70 |
| 2026-08-26 | `BMEA` | 868 | $1.73 | $1.75 | +21.70 | $1.71 | -39.06 | -17.36 | +108.50 | +69.44 |
| 2026-08-26 | `ALVO` | 270 | $5.05 | $4.98 | -18.90 | $4.91 | -18.90 | -37.80 | -70.20 | -89.10 |
| 2026-08-26 | `SUJA` | 161 | $9.33 | $9.39 | +9.66 | $9.44 | +8.05 | +17.71 | +96.60 | +104.65 |
| 2026-08-26 | `FWDI` | 248 | $6.05 | $5.97 | -19.84 | $5.93 | -9.92 | -29.76 | +64.48 | +54.56 |
| 2026-08-26 | `GORO` | 398 | $3.87 | $3.77 | -39.80 | $3.56 | -83.58 | -123.38 | +87.56 | +3.98 |
| 2026-08-26 | `ASST` | 73 | $21.39 | $20.72 | -48.91 | $21.50 | +56.94 | +8.03 | +122.64 | +179.58 |
| 2026-08-26 | `CNTN` | 2 | — | $2.29 | +0.00 | $2.23 | -0.12 | -0.12 | +0.00 | -0.12 |
| 2026-08-27 | `LIFE` | 38 | $39.11 | $39.40 | +11.02 | $39.44 | +1.52 | +12.54 | +92.72 | +94.24 |
| 2026-08-27 | `BMEA` | 868 | $1.71 | $1.74 | +26.04 | $1.68 | -52.08 | -26.04 | +95.48 | +43.40 |
| 2026-08-27 | `ALVO` | 270 | $4.91 | $4.88 | -8.10 | $4.88 | +0.00 | -8.10 | -97.20 | -97.20 |
| 2026-08-27 | `SUJA` | 161 | $9.44 | $9.41 | -4.83 | $9.00 | -66.01 | -70.84 | +99.82 | +33.81 |
| 2026-08-27 | `FWDI` | 248 | $5.93 | $6.39 | +114.08 | $6.66 | +66.96 | +181.04 | +168.64 | +235.60 |
| 2026-08-27 | `GORO` | 398 | $3.56 | $3.59 | +11.94 | $3.79 | +79.60 | +91.54 | +15.92 | +95.52 |
| 2026-08-27 | `ASST` | 73 | $21.50 | $22.45 | +69.35 | $23.12 | +48.91 | +118.26 | +248.93 | +297.84 |
| 2026-08-27 | `CNTN` | 2 | $2.23 | $2.21 | -0.04 | $2.35 | +0.28 | +0.24 | -0.16 | +0.12 |
| 2026-08-27 | `SLI` | 6 | — | $2.60 | +0.00 | $2.64 | +0.24 | +0.24 | +0.00 | +0.24 |
| 2026-08-28 | `LIFE` | 38 | $39.44 | $39.60 | +6.08 | — | +0.00 | +6.08 | +100.32 | — |
| 2026-08-28 | `BMEA` | 868 | $1.68 | $1.69 | +8.68 | — | +0.00 | +8.68 | +52.08 | — |
| 2026-08-28 | `ALVO` | 270 | $4.88 | $4.84 | -10.80 | — | +0.00 | -10.80 | -108.00 | — |
| 2026-08-28 | `SUJA` | 161 | $9.00 | $9.08 | +12.88 | — | +0.00 | +12.88 | +46.69 | — |
| 2026-08-28 | `FWDI` | 248 | $6.66 | $6.73 | +17.36 | — | +0.00 | +17.36 | +252.96 | — |
| 2026-08-28 | `GORO` | 398 | $3.79 | $3.80 | +3.98 | — | +0.00 | +3.98 | +99.50 | — |
| 2026-08-28 | `ASST` | 73 | $23.12 | $22.50 | -45.26 | — | +0.00 | -45.26 | +252.58 | — |
| 2026-08-28 | `CNTN` | 2 | $2.35 | $2.34 | -0.02 | $2.22 | -0.24 | -0.26 | +0.10 | -0.14 |
| 2026-08-28 | `SLI` | 6 | $2.64 | $2.68 | +0.24 | $2.55 | -0.78 | -0.54 | +0.48 | -0.30 |
| 2026-08-28 | `CAPR` | 154 | — | $9.73 | +0.00 | $9.59 | -21.56 | -21.56 | +0.00 | -21.56 |
| 2026-08-28 | `VYX` | 165 | — | $9.13 | +0.00 | $8.78 | -57.75 | -57.75 | +0.00 | -57.75 |
| 2026-08-28 | `SNPS` | 3 | — | $461.85 | +0.00 | $442.61 | -57.72 | -57.72 | +0.00 | -57.72 |
| 2026-08-28 | `SRPT` | 70 | — | $21.49 | +0.00 | $20.86 | -44.10 | -44.10 | +0.00 | -44.10 |
| 2026-08-28 | `NEO` | 82 | — | $18.36 | +0.00 | $18.05 | -25.42 | -25.42 | +0.00 | -25.42 |
| 2026-08-28 | `NCNO` | 64 | — | $23.30 | +0.00 | $22.99 | -19.84 | -19.84 | +0.00 | -19.84 |
| 2026-08-28 | `DJT` | 155 | — | $9.72 | +0.00 | $9.63 | -13.18 | -13.18 | +0.00 | -13.18 |
| 2026-08-31 | `CNTN` | 2 | $2.22 | $2.23 | +0.02 | — | +0.00 | +0.02 | -0.12 | — |
| 2026-08-31 | `SLI` | 6 | $2.55 | $2.58 | +0.18 | $2.67 | +0.54 | +0.72 | -0.12 | +0.42 |
| 2026-08-31 | `CAPR` | 154 | $9.59 | $9.50 | -13.86 | $9.91 | +63.14 | +49.28 | -35.42 | +27.72 |
| 2026-08-31 | `VYX` | 165 | $8.78 | $8.66 | -19.80 | $8.38 | -46.20 | -66.00 | -77.55 | -123.75 |
| 2026-08-31 | `SNPS` | 3 | $442.61 | $437.95 | -13.98 | $439.59 | +4.92 | -9.06 | -71.70 | -66.78 |
| 2026-08-31 | `SRPT` | 70 | $20.86 | $20.56 | -21.00 | $21.03 | +32.90 | +11.90 | -65.10 | -32.20 |
| 2026-08-31 | `NEO` | 82 | $18.05 | $17.77 | -22.96 | $17.91 | +11.48 | -11.48 | -48.38 | -36.90 |
| 2026-08-31 | `NCNO` | 64 | $22.99 | $22.66 | -21.12 | $22.60 | -3.84 | -24.96 | -40.96 | -44.80 |
| 2026-08-31 | `DJT` | 155 | $9.63 | $9.55 | -13.17 | $9.77 | +34.10 | +20.93 | -26.35 | +7.75 |
| 2026-09-01 | `SLI` | 6 | $2.67 | $2.67 | +0.00 | — | +0.00 | +0.00 | +0.42 | — |
| 2026-09-01 | `CAPR` | 154 | $9.91 | $10.77 | +132.44 | $10.01 | -117.04 | +15.40 | +160.16 | +43.12 |
| 2026-09-01 | `VYX` | 165 | $8.38 | $8.30 | -13.20 | $8.74 | +72.60 | +59.40 | -136.95 | -64.35 |
| 2026-09-01 | `SNPS` | 3 | $439.59 | $428.21 | -34.14 | $414.82 | -40.17 | -74.31 | -100.92 | -141.09 |
| 2026-09-01 | `SRPT` | 70 | $21.03 | $20.88 | -10.50 | $21.33 | +31.50 | +21.00 | -42.70 | -11.20 |
| 2026-09-01 | `NEO` | 82 | $17.91 | $17.67 | -19.68 | $17.45 | -18.04 | -37.72 | -56.58 | -74.62 |
| 2026-09-01 | `NCNO` | 64 | $22.60 | $22.15 | -28.80 | $22.30 | +9.60 | -19.20 | -73.60 | -64.00 |
| 2026-09-01 | `DJT` | 155 | $9.77 | $9.57 | -31.00 | $9.06 | -79.05 | -110.05 | -23.25 | -102.30 |
| 2026-09-02 | `CAPR` | 154 | $10.01 | $10.07 | +9.24 | — | +0.00 | +9.24 | +52.36 | — |
| 2026-09-02 | `VYX` | 165 | $8.74 | $8.73 | -1.65 | — | +0.00 | -1.65 | -66.00 | — |
| 2026-09-02 | `SNPS` | 3 | $414.82 | $413.78 | -3.12 | — | +0.00 | -3.12 | -144.21 | — |
| 2026-09-02 | `SRPT` | 70 | $21.33 | $21.33 | +0.00 | — | +0.00 | +0.00 | -11.20 | — |
| 2026-09-02 | `NEO` | 82 | $17.45 | $17.40 | -4.10 | — | +0.00 | -4.10 | -78.72 | — |
| 2026-09-02 | `NCNO` | 64 | $22.30 | $22.20 | -6.40 | — | +0.00 | -6.40 | -70.40 | — |
| 2026-09-02 | `DJT` | 155 | $9.06 | $9.04 | -3.10 | — | +0.00 | -3.10 | -105.40 | — |
| 2026-09-03 | `ATRC` | 23 | — | $52.88 | +0.00 | $52.46 | -9.66 | -9.66 | +0.00 | -9.66 |
| 2026-09-03 | `HRMY` | 29 | — | $42.93 | +0.00 | $41.86 | -31.03 | -31.03 | +0.00 | -31.03 |
| 2026-09-03 | `CABA` | 348 | — | $3.63 | +0.00 | $3.48 | -52.20 | -52.20 | +0.00 | -52.20 |
| 2026-09-03 | `VSTM` | 157 | — | $8.03 | +0.00 | $7.98 | -7.85 | -7.85 | +0.00 | -7.85 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `GPRO` | 710 | — | $1.78 | +0.00 | $1.39 | -276.90 | -276.90 | +0.00 | -276.90 |
| 2026-09-03 | `MMED` | 52 | — | $23.88 | +0.00 | $23.84 | -2.08 | -2.08 | +0.00 | -2.08 |
| 2026-09-03 | `SID` | 929 | — | $1.36 | +0.00 | $1.26 | -92.90 | -92.90 | +0.00 | -92.90 |
| 2026-09-04 | `ATRC` | 23 | $52.46 | $52.03 | -9.89 | $51.52 | -11.73 | -21.62 | -19.55 | -31.28 |
| 2026-09-04 | `HRMY` | 29 | $41.86 | $41.50 | -10.44 | $42.25 | +21.75 | +11.31 | -41.47 | -19.72 |
| 2026-09-04 | `CABA` | 348 | $3.48 | $3.46 | -6.96 | $3.47 | +3.48 | -3.48 | -59.16 | -55.68 |
| 2026-09-04 | `VSTM` | 157 | $7.98 | $7.91 | -10.99 | $8.20 | +45.53 | +34.54 | -18.84 | +26.69 |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | $130.22 | +1.71 | -3.69 | -21.78 | -20.07 |
| 2026-09-04 | `GPRO` | 710 | $1.39 | $1.48 | +63.90 | $1.70 | +156.20 | +220.10 | -213.00 | -56.80 |
| 2026-09-04 | `MMED` | 52 | $23.84 | $23.84 | +0.00 | $23.29 | -28.60 | -28.60 | -2.08 | -30.68 |
| 2026-09-04 | `SID` | 929 | $1.26 | $1.23 | -27.87 | $1.22 | -9.29 | -37.16 | -120.77 | -130.06 |
| 2026-09-04 | `IRD` | 3 | — | $4.53 | +0.00 | $4.67 | +0.42 | +0.42 | +0.00 | +0.42 |
| 2026-09-04 | `BRR` | 6 | — | $2.51 | +0.00 | $2.66 | +0.90 | +0.90 | +0.00 | +0.90 |
| 2026-09-04 | `LENZ` | 2 | — | $5.75 | +0.00 | $5.96 | +0.42 | +0.42 | +0.00 | +0.42 |
| 2026-09-04 | `SCZM` | 1 | — | $10.03 | +0.00 | $9.94 | -0.09 | -0.09 | +0.00 | -0.09 |
| 2026-09-04 | `DFDV` | 2 | — | $5.79 | +0.00 | $5.87 | +0.16 | +0.16 | +0.00 | +0.16 |
| 2026-09-08 | `ATRC` | 23 | $51.52 | $54.31 | +64.17 | $53.73 | -13.34 | +50.83 | +32.89 | +19.55 |
| 2026-09-08 | `HRMY` | 29 | $42.25 | $42.20 | -1.45 | $42.07 | -3.77 | -5.22 | -21.17 | -24.94 |
| 2026-09-08 | `CABA` | 348 | $3.47 | $3.43 | -13.92 | $3.27 | -55.68 | -69.60 | -69.60 | -125.28 |
| 2026-09-08 | `VSTM` | 157 | $8.20 | $8.20 | +0.00 | $8.08 | -18.84 | -18.84 | +26.69 | +7.85 |
| 2026-09-08 | `RVTY` | 9 | $130.22 | $128.50 | -15.48 | $127.08 | -12.78 | -28.26 | -35.55 | -48.33 |
| 2026-09-08 | `GPRO` | 710 | $1.70 | $1.56 | -95.85 | $1.46 | -78.10 | -173.95 | -152.65 | -230.75 |
| 2026-09-08 | `MMED` | 52 | $23.29 | $23.16 | -6.76 | $23.32 | +8.32 | +1.56 | -37.44 | -29.12 |
| 2026-09-08 | `SID` | 929 | $1.22 | $1.28 | +55.74 | $1.24 | -37.16 | +18.58 | -74.32 | -111.48 |
| 2026-09-08 | `IRD` | 3 | $4.67 | $4.53 | -0.42 | $4.34 | -0.57 | -0.99 | +0.00 | -0.57 |
| 2026-09-08 | `BRR` | 6 | $2.66 | $2.66 | +0.00 | $2.73 | +0.42 | +0.42 | +0.90 | +1.32 |
| 2026-09-08 | `LENZ` | 2 | $5.96 | $5.95 | -0.02 | $5.33 | -1.24 | -1.26 | +0.40 | -0.84 |
| 2026-09-08 | `SCZM` | 1 | $9.94 | $9.90 | -0.04 | $9.97 | +0.07 | +0.03 | -0.13 | -0.06 |
| 2026-09-08 | `DFDV` | 2 | $5.87 | $5.81 | -0.12 | $5.99 | +0.36 | +0.24 | +0.04 | +0.40 |
| 2026-09-09 | `ATRC` | 23 | $53.73 | $53.16 | -13.11 | — | +0.00 | -13.11 | +6.44 | — |
| 2026-09-09 | `HRMY` | 29 | $42.07 | $42.01 | -1.74 | — | +0.00 | -1.74 | -26.68 | — |
| 2026-09-09 | `CABA` | 348 | $3.27 | $3.28 | +3.48 | — | +0.00 | +3.48 | -121.80 | — |
| 2026-09-09 | `VSTM` | 157 | $8.08 | $8.01 | -10.99 | — | +0.00 | -10.99 | -3.14 | — |
| 2026-09-09 | `RVTY` | 9 | $127.08 | $125.77 | -11.79 | — | +0.00 | -11.79 | -60.12 | — |
| 2026-09-09 | `GPRO` | 710 | $1.46 | $1.45 | -3.55 | — | +0.00 | -3.55 | -234.30 | — |
| 2026-09-09 | `MMED` | 52 | $23.32 | $23.22 | -5.20 | — | +0.00 | -5.20 | -34.32 | — |
| 2026-09-09 | `SID` | 929 | $1.24 | $1.28 | +37.16 | — | +0.00 | +37.16 | -74.32 | — |
| 2026-09-09 | `IRD` | 3 | $4.34 | $5.31 | +2.91 | $5.73 | +1.26 | +4.17 | +2.34 | +3.60 |
| 2026-09-09 | `BRR` | 6 | $2.73 | $2.75 | +0.12 | $2.86 | +0.66 | +0.78 | +1.44 | +2.10 |
| 2026-09-09 | `LENZ` | 2 | $5.33 | $5.31 | -0.04 | $4.91 | -0.80 | -0.84 | -0.88 | -1.68 |
| 2026-09-09 | `SCZM` | 1 | $9.97 | $10.12 | +0.15 | $10.43 | +0.31 | +0.46 | +0.09 | +0.40 |
| 2026-09-09 | `DFDV` | 2 | $5.99 | $6.02 | +0.06 | $5.44 | -1.16 | -1.10 | +0.46 | -0.70 |
| 2026-09-10 | `IRD` | 3 | $5.73 | $5.87 | +0.42 | $5.88 | +0.03 | +0.45 | +4.02 | +4.05 |
| 2026-09-10 | `BRR` | 6 | $2.86 | $2.86 | +0.00 | — | +0.00 | +0.00 | +2.10 | — |
| 2026-09-10 | `LENZ` | 2 | $4.91 | $4.98 | +0.14 | — | +0.00 | +0.14 | -1.54 | — |
| 2026-09-10 | `SCZM` | 1 | $10.43 | $10.77 | +0.34 | — | +0.00 | +0.34 | +0.74 | — |
| 2026-09-10 | `DFDV` | 2 | $5.44 | $5.51 | +0.14 | — | +0.00 | +0.14 | -0.56 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | -264.74 | IREN | — | $19.54 | $9,732.46 | IREN×217 |
| 2026-08-14 | +5.50 | $19.54 | IREN×217 | $9,587.07 | -145.39 | -6.51 | — | — | $19.54 | $9,580.56 | IREN×217 |
| 2026-08-17 | +2.25 | $19.54 | IREN×217 | $9,834.45 | +253.89 | -71.80 | NPWR | — | $17.60 | $9,762.63 | IREN×217, NPWR×1 |
| 2026-08-18 | -6.20 | $17.60 | IREN×217, NPWR×1 | $9,471.82 | -290.81 | -0.05 | — | IREN | $9,467.21 | $9,468.86 | NPWR×1 |
| 2026-08-19 | -7.20 | $9,467.21 | NPWR×1 | $9,468.91 | +0.05 | -0.03 | — | — | $9,467.21 | $9,468.88 | NPWR×1 |
| 2026-08-20 | +1.12 | $9,467.21 | NPWR×1 | $9,468.85 | -0.03 | +228.84 | AG, BHP, CDE, IAG, KGC, NFGC, WPM, ABUS | NPWR | $59.33 | $9,673.19 | AG×57, BHP×13, CDE×57, IAG×60, KGC×39, NFGC×676, WPM×8, ABUS×240 |
| 2026-08-21 | +3.25 | $59.33 | AG×57, BHP×13, CDE×57, IAG×60, KGC×39, NFGC×676, WPM×8, ABUS×240 | $10,012.21 | +339.02 | +10.60 | CYPH, ORBS, CAN | — | $38.15 | $10,022.49 | AG×57, BHP×13, CDE×57, IAG×60, KGC×39, NFGC×676, WPM×8, ABUS×240, CYPH×5, ORBS×8, CAN×25 |
| 2026-08-24 | -5.17 | $38.15 | AG×57, BHP×13, CDE×57, IAG×60, KGC×39, NFGC×676, WPM×8, ABUS×240, CYPH×5, ORBS×8, CAN×25 | $10,102.47 | +79.98 | +8.09 | — | — | $38.15 | $10,110.57 | AG×57, BHP×13, CDE×57, IAG×60, KGC×39, NFGC×676, WPM×8, ABUS×240, CYPH×5, ORBS×8, CAN×25 |
| 2026-08-25 | +1.80 | $38.15 | AG×57, BHP×13, CDE×57, IAG×60, KGC×39, NFGC×676, WPM×8, ABUS×240, CYPH×5, ORBS×8, CAN×25 | $9,962.36 | -148.21 | +568.05 | LIFE, BMEA, ALVO, SUJA, FWDI, GORO, ASST | AG, BHP, CDE, IAG, KGC, NFGC, WPM, ABUS | $16.15 | $10,475.86 | CYPH×5, ORBS×8, CAN×25, LIFE×38, BMEA×868, ALVO×270, SUJA×161, FWDI×248, GORO×398, ASST×73 |
| 2026-08-26 | +2.02 | $16.15 | CYPH×5, ORBS×8, CAN×25, LIFE×38, BMEA×868, ALVO×270, SUJA×161, FWDI×248, GORO×398, ASST×73 | $10,366.88 | -108.98 | -53.53 | CNTN | CYPH, ORBS, CAN | $35.40 | $10,312.88 | LIFE×38, BMEA×868, ALVO×270, SUJA×161, FWDI×248, GORO×398, ASST×73, CNTN×2 |
| 2026-08-27 | — | $35.40 | LIFE×38, BMEA×868, ALVO×270, SUJA×161, FWDI×248, GORO×398, ASST×73, CNTN×2 | $10,532.34 | +219.46 | +79.42 | SLI | — | $19.62 | $10,611.58 | LIFE×38, BMEA×868, ALVO×270, SUJA×161, FWDI×248, GORO×398, ASST×73, CNTN×2, SLI×6 |
| 2026-08-28 | +0.75 | $19.62 | LIFE×38, BMEA×868, ALVO×270, SUJA×161, FWDI×248, GORO×398, ASST×73, CNTN×2, SLI×6 | $10,604.72 | -6.86 | -240.59 | CAPR, VYX, SNPS, SRPT, NEO, NCNO, DJT | LIFE, BMEA, ALVO, SUJA, FWDI, GORO, ASST | $139.69 | $10,317.90 | CNTN×2, SLI×6, CAPR×154, VYX×165, SNPS×3, SRPT×70, NEO×82, NCNO×64, DJT×155 |
| 2026-08-31 | -5.85 | $139.69 | CNTN×2, SLI×6, CAPR×154, VYX×165, SNPS×3, SRPT×70, NEO×82, NCNO×64, DJT×155 | $10,192.21 | -125.69 | +97.04 | — | CNTN | $144.08 | $10,289.18 | SLI×6, CAPR×154, VYX×165, SNPS×3, SRPT×70, NEO×82, NCNO×64, DJT×155 |
| 2026-09-01 | -6.30 | $144.08 | SLI×6, CAPR×154, VYX×165, SNPS×3, SRPT×70, NEO×82, NCNO×64, DJT×155 | $10,284.30 | -4.88 | -140.60 | — | SLI | $159.90 | $10,143.50 | CAPR×154, VYX×165, SNPS×3, SRPT×70, NEO×82, NCNO×64, DJT×155 |
| 2026-09-02 | -3.83 | $159.90 | CAPR×154, VYX×165, SNPS×3, SRPT×70, NEO×82, NCNO×64, DJT×155 | $10,134.37 | -9.13 | +0.00 | — | CAPR, VYX, SNPS, SRPT, NEO, NCNO, DJT | $10,118.15 | $10,118.15 | — |
| 2026-09-03 | -0.90 | $10,118.15 | — | $10,118.15 | +0.00 | -489.00 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, MMED, SID | — | $135.55 | $9,592.76 | ATRC×23, HRMY×29, CABA×348, VSTM×157, RVTY×9, GPRO×710, MMED×52, SID×929 |
| 2026-09-04 | +2.25 | $135.55 | ATRC×23, HRMY×29, CABA×348, VSTM×157, RVTY×9, GPRO×710, MMED×52, SID×929 | $9,585.11 | -7.65 | +180.86 | IRD, BRR, LENZ, SCZM, DFDV | — | $73.13 | $9,765.31 | ATRC×23, HRMY×29, CABA×348, VSTM×157, RVTY×9, GPRO×710, MMED×52, SID×929, IRD×3, BRR×6, LENZ×2, SCZM×1, DFDV×2 |
| 2026-09-08 | -11.47 | $73.13 | ATRC×23, HRMY×29, CABA×348, VSTM×157, RVTY×9, GPRO×710, MMED×52, SID×929, IRD×3, BRR×6, LENZ×2, SCZM×1, DFDV×2 | $9,751.16 | -14.15 | -212.31 | — | — | $73.13 | $9,538.85 | ATRC×23, HRMY×29, CABA×348, VSTM×157, RVTY×9, GPRO×710, MMED×52, SID×929, IRD×3, BRR×6, LENZ×2, SCZM×1, DFDV×2 |
| 2026-09-09 | -13.95 | $73.13 | ATRC×23, HRMY×29, CABA×348, VSTM×157, RVTY×9, GPRO×710, MMED×52, SID×929, IRD×3, BRR×6, LENZ×2, SCZM×1, DFDV×2 | $9,536.31 | -2.54 | +0.27 | — | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, MMED, SID | $9,434.23 | $9,499.71 | IRD×3, BRR×6, LENZ×2, SCZM×1, DFDV×2 |
| 2026-09-10 | -13.28 | $9,434.23 | IRD×3, BRR×6, LENZ×2, SCZM×1, DFDV×2 | $9,500.75 | +1.04 | +0.03 | — | BRR, LENZ, SCZM, DFDV | $9,482.54 | $9,500.18 | IRD×3 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 217 | $45.98 | $2.80 | — | $19.54 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ⚪; ret5=+12.3; leftover $10000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.54 | ▼ close $9,732.46 vs 09:30 $10,000.00 (session -264.74) | 16:00 close · cash $19.54 · equity $9,732.46 vs 09:30 $10,000.00 (-267.54; session marks -264.74) · 1 name(s) marked open→close (per-name table). IREN×217 09:30 $45.98 → close $44.76 -264.74 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.54 | ▼ 09:30 equity $9,587.07 vs yday $9,732.46 (-145.39) | 09:30 open · cash $19.54 (unchanged overnight, no fees) · equity $9,587.07 vs prior close $9,732.46 (-145.39) · 1 name(s) re-marked at the open (per-name table). IREN×217 yday $44.76 → 09:30 $44.09 -145.39 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.54 | ▼ close $9,580.56 vs 09:30 $9,587.07 (session -6.51) | 16:00 close · cash $19.54 · equity $9,580.56 vs 09:30 $9,587.07 (-6.51; session marks -6.51) · 1 name(s) marked open→close (per-name table). IREN×217 09:30 $44.09 → close $44.06 -6.51 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.54 | ▲ 09:30 equity $9,834.45 vs yday $9,580.56 (+253.89) | 09:30 open · cash $19.54 (unchanged overnight, no fees) · equity $9,834.45 vs prior close $9,580.56 (+253.89) · 1 name(s) re-marked at the open (per-name table). IREN×217 yday $44.06 → 09:30 $45.23 +253.89 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 1 | $1.92 | $0.02 | — | $17.60 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $2.44 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.60 | ▼ close $9,762.63 vs 09:30 $9,834.45 (session -71.80) | 16:00 close · cash $17.60 · equity $9,762.63 vs 09:30 $9,834.45 (-71.82; session marks -71.80) · 2 name(s) marked open→close (per-name table). IREN×217 09:30 $45.23 → close $44.90 -71.61; NPWR×1 09:30 $1.92 → close $1.73 -0.19 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.60 | ▼ 09:30 equity $9,471.82 vs yday $9,762.63 (-290.81) | 09:30 open · cash $17.60 (unchanged overnight, no fees) · equity $9,471.82 vs prior close $9,762.63 (-290.81) · 2 name(s) re-marked at the open (per-name table). IREN×217 yday $44.90 → 09:30 $43.56 -290.78; NPWR×1 yday $1.73 → 09:30 $1.70 -0.03 | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 217 | $43.56 | $2.91 | $-530.85 | $9,467.21 | ▼ -530.85 after sell → book $9,468.91; vs 09:30 mark -2.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,467.21 | ▼ close $9,468.86 vs 09:30 $9,471.82 (session -0.05) | 16:00 close · cash $9,467.21 · equity $9,468.86 vs 09:30 $9,471.82 (-2.96; session marks -0.05) · 1 name(s) marked open→close (per-name table). NPWR×1 09:30 $1.70 → close $1.65 -0.05 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,467.21 | ▲ 09:30 equity $9,468.91 vs yday $9,468.86 (+0.05) | 09:30 open · cash $9,467.21 (unchanged overnight, no fees) · equity $9,468.91 vs prior close $9,468.86 (+0.05) · 1 name(s) re-marked at the open (per-name table). NPWR×1 yday $1.65 → 09:30 $1.70 +0.05 | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,467.21 | ▼ close $9,468.88 vs 09:30 $9,468.91 (session -0.03) | 16:00 close · cash $9,467.21 · equity $9,468.88 vs 09:30 $9,468.91 (-0.03; session marks -0.03) · 1 name(s) marked open→close (per-name table). NPWR×1 09:30 $1.70 → close $1.67 -0.03 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,467.21 | ▼ 09:30 equity $9,468.85 vs yday $9,468.88 (-0.03) | 09:30 open · cash $9,467.21 (unchanged overnight, no fees) · equity $9,468.85 vs prior close $9,468.88 (-0.03) · 1 name(s) re-marked at the open (per-name table). NPWR×1 yday $1.67 → 09:30 $1.64 -0.03 | — |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 1 | $1.64 | $0.04 | $-0.34 | $9,468.81 | ▼ -0.34 after sell → book $9,468.81; vs 09:30 mark -0.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 57 | $20.55 | $2.16 | — | $8,295.30 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,110.14 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $5,930.93 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $4,750.96 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $3,593.28 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 676 | $1.75 | $8.72 | — | $2,401.56 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,243.23 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 240 | $4.92 | $3.10 | — | $59.33 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.33 | ▲ close $9,673.19 vs 09:30 $9,468.85 (session +228.84) | 16:00 close · cash $59.33 · equity $9,673.19 vs 09:30 $9,468.85 (+204.34; session marks +228.84) · 8 name(s) marked open→close (per-name table). AG×57 09:30 $20.55 → close $21.19 +36.48; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×57 09:30 $20.65 → close $21.11 +26.22; IAG×60 09:30 $19.63 → close $20.50 +52.20; KGC×39 09:30 $29.63 → close $31.43 +70.20; NFGC×676 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68; ABUS×240 09:30 $4.92 → close $4.77 -36.00 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.33 | ▲ 09:30 equity $10,012.21 vs yday $9,673.19 (+339.02) | 09:30 open · cash $59.33 (unchanged overnight, no fees) · equity $10,012.21 vs prior close $9,673.19 (+339.02) · 8 name(s) re-marked at the open (per-name table). AG×57 yday $21.19 → 09:30 $21.90 +40.47; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×57 yday $21.11 → 09:30 $21.75 +36.48; IAG×60 yday $20.50 → 09:30 $21.17 +40.20; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×676 yday $1.75 → 09:30 $1.79 +27.04; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×240 yday $4.77 → 09:30 $5.20 +103.20 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 5 | $1.32 | $0.08 | — | $52.65 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $7.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 8 | $0.86 | $0.09 | — | $45.64 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $7.42 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 25 | $0.29 | $0.15 | — | $38.15 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $7.42 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.15 | ▲ close $10,022.49 vs 09:30 $10,012.21 (session +10.60) | 16:00 close · cash $38.15 · equity $10,022.49 vs 09:30 $10,012.21 (+10.28; session marks +10.60) · 11 name(s) marked open→close (per-name table). AG×57 09:30 $21.90 → close $21.09 -46.17; BHP×13 09:30 $95.72 → close $97.03 +17.03; CDE×57 09:30 $21.75 → close $20.97 -44.46; IAG×60 09:30 $21.17 → close $21.14 -1.80; KGC×39 09:30 $32.17 → close $32.76 +23.01; NFGC×676 09:30 $1.79 → close $1.84 +33.80; WPM×8 09:30 $154.70 → close $157.78 +24.64; ABUS×240 09:30 $5.20 → close $5.21 +2.40; CYPH×5 09:30 $1.32 → close $1.42 +0.50; ORBS×8 09:30 $0.86 → close $0.88 +0.13; CAN×25 09:30 $0.29 → close $0.35 +1.52 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.15 | ▲ 09:30 equity $10,102.47 vs yday $10,022.49 (+79.98) | 09:30 open · cash $38.15 (unchanged overnight, no fees) · equity $10,102.47 vs prior close $10,022.49 (+79.98) · 11 name(s) re-marked at the open (per-name table). AG×57 yday $21.09 → 09:30 $21.30 +11.97; BHP×13 yday $97.03 → 09:30 $97.31 +3.64; CDE×57 yday $20.97 → 09:30 $21.26 +16.53; IAG×60 yday $21.14 → 09:30 $21.38 +14.40; KGC×39 yday $32.76 → 09:30 $33.03 +10.53; NFGC×676 yday $1.84 → 09:30 $1.86 +13.52; WPM×8 yday $157.78 → 09:30 $159.50 +13.76; ABUS×240 yday $5.21 → 09:30 $5.18 -7.20; CYPH×5 yday $1.42 → 09:30 $1.83 +2.05; ORBS×8 yday $0.88 → 09:30 $0.89 +0.08; CAN×25 yday $0.35 → 09:30 $0.38 +0.70 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.15 | ▲ close $10,110.57 vs 09:30 $10,102.47 (session +8.09) | 16:00 close · cash $38.15 · equity $10,110.57 vs 09:30 $10,102.47 (+8.10; session marks +8.09) · 11 name(s) marked open→close (per-name table). AG×57 09:30 $21.30 → close $20.83 -26.79; BHP×13 09:30 $97.31 → close $97.13 -2.34; CDE×57 09:30 $21.26 → close $20.88 -21.66; IAG×60 09:30 $21.38 → close $21.80 +25.20; KGC×39 09:30 $33.03 → close $32.98 -1.95; NFGC×676 09:30 $1.86 → close $1.90 +27.04; WPM×8 09:30 $159.50 → close $160.19 +5.52; ABUS×240 09:30 $5.18 → close $5.20 +4.80; CYPH×5 09:30 $1.83 → close $1.68 -0.75; ORBS×8 09:30 $0.89 → close $0.84 -0.40; CAN×25 09:30 $0.38 → close $0.36 -0.58 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.15 | ▼ 09:30 equity $9,962.36 vs yday $10,110.57 (-148.21) | 09:30 open · cash $38.15 (unchanged overnight, no fees) · equity $9,962.36 vs prior close $10,110.57 (-148.21) · 11 name(s) re-marked at the open (per-name table). AG×57 yday $20.83 → 09:30 $20.32 -29.07; BHP×13 yday $97.13 → 09:30 $95.86 -16.51; CDE×57 yday $20.88 → 09:30 $20.47 -23.37; IAG×60 yday $21.80 → 09:30 $21.21 -35.40; KGC×39 yday $32.98 → 09:30 $32.32 -25.74; NFGC×676 yday $1.90 → 09:30 $1.90 +0.00; WPM×8 yday $160.19 → 09:30 $156.51 -29.44; ABUS×240 yday $5.20 → 09:30 $5.25 +12.00; CYPH×5 yday $1.68 → 09:30 $1.56 -0.60; ORBS×8 yday $0.84 → 09:30 $0.83 -0.08; CAN×25 yday $0.36 → 09:30 $0.36 +0.00 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 57 | $20.32 | $2.18 | $-17.45 | $1,194.20 | ▼ -17.45 after sell → book $9,960.17; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $2,438.34 | ▲ +58.97 after sell → book $9,958.13; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 57 | $20.47 | $2.18 | $-14.60 | $3,602.94 | ▼ -14.60 after sell → book $9,955.94; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 60 | $21.21 | $2.19 | $+90.44 | $4,873.35 | ▲ +90.44 after sell → book $9,953.75; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 39 | $32.32 | $2.13 | $+100.68 | $6,131.71 | ▲ +100.68 after sell → book $9,951.63; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 676 | $1.90 | $8.84 | $+83.84 | $7,407.26 | ▲ +83.84 after sell → book $9,942.78; vs 09:30 mark -8.85 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $8,657.31 | ▲ +91.71 after sell → book $9,940.75; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 240 | $5.25 | $3.15 | $+72.96 | $9,914.16 | ▲ +72.96 after sell → book $9,937.60; vs 09:30 mark -3.15 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 38 | $36.96 | $2.10 | — | $8,507.58 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1416.31 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 868 | $1.63 | $11.20 | — | $7,081.54 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1416.31 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 270 | $5.24 | $3.48 | — | $5,663.26 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1416.31 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 161 | $8.79 | $2.47 | — | $4,245.60 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1416.31 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 248 | $5.71 | $3.20 | — | $2,826.32 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $1416.31 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 398 | $3.55 | $5.13 | — | $1,408.28 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+27.9; leftover $1416.31 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 73 | $19.04 | $2.21 | — | $16.15 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+49.5; leftover $1416.31 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.15 | ▲ close $10,475.86 vs 09:30 $9,962.36 (session +568.05) | 16:00 close · cash $16.15 · equity $10,475.86 vs 09:30 $9,962.36 (+513.50; session marks +568.05) · 10 name(s) marked open→close (per-name table). CYPH×5 09:30 $1.56 → close $1.64 +0.40; ORBS×8 09:30 $0.83 → close $0.80 -0.24; CAN×25 09:30 $0.36 → close $0.42 +1.42; LIFE×38 09:30 $36.96 → close $38.56 +60.80; BMEA×868 09:30 $1.63 → close $1.73 +86.80; ALVO×270 09:30 $5.24 → close $5.05 -51.30; SUJA×161 09:30 $8.79 → close $9.33 +86.94; FWDI×248 09:30 $5.71 → close $6.05 +84.32; GORO×398 09:30 $3.55 → close $3.87 +127.36; ASST×73 09:30 $19.04 → close $21.39 +171.55 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.15 | ▼ 09:30 equity $10,366.88 vs yday $10,475.86 (-108.98) | 09:30 open · cash $16.15 (unchanged overnight, no fees) · equity $10,366.88 vs prior close $10,475.86 (-108.98) · 10 name(s) re-marked at the open (per-name table). CYPH×5 yday $1.64 → 09:30 $1.60 -0.20; ORBS×8 yday $0.80 → 09:30 $0.80 -0.03; CAN×25 yday $0.42 → 09:30 $0.40 -0.50; LIFE×38 yday $38.56 → 09:30 $38.24 -12.16; BMEA×868 yday $1.73 → 09:30 $1.75 +21.70; ALVO×270 yday $5.05 → 09:30 $4.98 -18.90; SUJA×161 yday $9.33 → 09:30 $9.39 +9.66; FWDI×248 yday $6.05 → 09:30 $5.97 -19.84; GORO×398 yday $3.87 → 09:30 $3.77 -39.80; ASST×73 yday $21.39 → 09:30 $20.72 -48.91 | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 5 | $1.60 | $0.12 | $+1.20 | $24.04 | ▲ +1.20 after sell → book $10,366.76; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ORBS` | 8 | $0.80 | $0.11 | $-0.74 | $30.30 | ▼ -0.74 after sell → book $10,366.65; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CAN` | 25 | $0.40 | $0.19 | $+2.23 | $40.03 | ▲ +2.23 after sell → book $10,366.46; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `CNTN` | 2 | $2.29 | $0.05 | — | $35.40 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot; 🔵; ret5=+14.9; leftover $5.72 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.40 | ▼ close $10,312.88 vs 09:30 $10,366.88 (session -53.53) | 16:00 close · cash $35.40 · equity $10,312.88 vs 09:30 $10,366.88 (-54.00; session marks -53.53) · 8 name(s) marked open→close (per-name table). LIFE×38 09:30 $38.24 → close $39.11 +33.06; BMEA×868 09:30 $1.75 → close $1.71 -39.06; ALVO×270 09:30 $4.98 → close $4.91 -18.90; SUJA×161 09:30 $9.39 → close $9.44 +8.05; FWDI×248 09:30 $5.97 → close $5.93 -9.92; GORO×398 09:30 $3.77 → close $3.56 -83.58; ASST×73 09:30 $20.72 → close $21.50 +56.94; CNTN×2 09:30 $2.29 → close $2.23 -0.12 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.40 | ▲ 09:30 equity $10,532.34 vs yday $10,312.88 (+219.46) | 09:30 open · cash $35.40 (unchanged overnight, no fees) · equity $10,532.34 vs prior close $10,312.88 (+219.46) · 8 name(s) re-marked at the open (per-name table). LIFE×38 yday $39.11 → 09:30 $39.40 +11.02; BMEA×868 yday $1.71 → 09:30 $1.74 +26.04; ALVO×270 yday $4.91 → 09:30 $4.88 -8.10; SUJA×161 yday $9.44 → 09:30 $9.41 -4.83; FWDI×248 yday $5.93 → 09:30 $6.39 +114.08; GORO×398 yday $3.56 → 09:30 $3.59 +11.94; ASST×73 yday $21.50 → 09:30 $22.45 +69.35; CNTN×2 yday $2.23 → 09:30 $2.21 -0.04 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 6 | $2.60 | $0.17 | — | $19.62 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ret5=+13.0; leftover $17.70 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.62 | ▲ close $10,611.58 vs 09:30 $10,532.34 (session +79.42) | 16:00 close · cash $19.62 · equity $10,611.58 vs 09:30 $10,532.34 (+79.24; session marks +79.42) · 9 name(s) marked open→close (per-name table). LIFE×38 09:30 $39.40 → close $39.44 +1.52; BMEA×868 09:30 $1.74 → close $1.68 -52.08; ALVO×270 09:30 $4.88 → close $4.88 +0.00; SUJA×161 09:30 $9.41 → close $9.00 -66.01; FWDI×248 09:30 $6.39 → close $6.66 +66.96; GORO×398 09:30 $3.59 → close $3.79 +79.60; ASST×73 09:30 $22.45 → close $23.12 +48.91; CNTN×2 09:30 $2.21 → close $2.35 +0.28; SLI×6 09:30 $2.60 → close $2.64 +0.24 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.62 | ▼ 09:30 equity $10,604.72 vs yday $10,611.58 (-6.86) | 09:30 open · cash $19.62 (unchanged overnight, no fees) · equity $10,604.72 vs prior close $10,611.58 (-6.86) · 9 name(s) re-marked at the open (per-name table). LIFE×38 yday $39.44 → 09:30 $39.60 +6.08; BMEA×868 yday $1.68 → 09:30 $1.69 +8.68; ALVO×270 yday $4.88 → 09:30 $4.84 -10.80; SUJA×161 yday $9.00 → 09:30 $9.08 +12.88; FWDI×248 yday $6.66 → 09:30 $6.73 +17.36; GORO×398 yday $3.79 → 09:30 $3.80 +3.98; ASST×73 yday $23.12 → 09:30 $22.50 -45.26; CNTN×2 yday $2.35 → 09:30 $2.34 -0.02; SLI×6 yday $2.64 → 09:30 $2.68 +0.24 | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 38 | $39.60 | $2.13 | $+96.09 | $1,522.30 | ▲ +96.09 after sell → book $10,602.60; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 868 | $1.69 | $11.35 | $+29.53 | $2,977.87 | ▲ +29.53 after sell → book $10,591.25; vs 09:30 mark -11.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 270 | $4.84 | $3.54 | $-115.02 | $4,281.13 | ▼ -115.02 after sell → book $10,587.71; vs 09:30 mark -3.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUJA` | 161 | $9.08 | $2.51 | $+41.71 | $5,740.50 | ▲ +41.71 after sell → book $10,585.20; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWDI` | 248 | $6.73 | $3.25 | $+246.51 | $7,406.28 | ▲ +246.51 after sell → book $10,581.94; vs 09:30 mark -3.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GORO` | 398 | $3.80 | $5.21 | $+89.15 | $8,913.47 | ▲ +89.15 after sell → book $10,576.73; vs 09:30 mark -5.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASST` | 73 | $22.50 | $2.23 | $+248.14 | $10,553.74 | ▲ +248.14 after sell → book $10,574.50; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 154 | $9.73 | $2.45 | — | $9,052.86 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+47.1; leftover $1507.68 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 165 | $9.13 | $2.48 | — | $7,543.93 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+20.0; leftover $1507.68 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 3 | $461.85 | $2.00 | — | $6,156.38 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+16.8; leftover $1507.68 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SRPT` | 70 | $21.49 | $2.20 | — | $4,649.88 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+12.3; leftover $1507.68 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 82 | $18.36 | $2.24 | — | $3,142.12 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+12.8; leftover $1507.68 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 64 | $23.30 | $2.18 | — | $1,648.74 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ret5=+14.5; leftover $1507.68 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DJT` | 155 | $9.72 | $2.46 | — | $139.69 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+14.8; leftover $1507.68 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $139.69 | ▼ close $10,317.90 vs 09:30 $10,604.72 (session -240.59) | 16:00 close · cash $139.69 · equity $10,317.90 vs 09:30 $10,604.72 (-286.82; session marks -240.59) · 9 name(s) marked open→close (per-name table). CNTN×2 09:30 $2.34 → close $2.22 -0.24; SLI×6 09:30 $2.68 → close $2.55 -0.78; CAPR×154 09:30 $9.73 → close $9.59 -21.56; VYX×165 09:30 $9.13 → close $8.78 -57.75; SNPS×3 09:30 $461.85 → close $442.61 -57.72; SRPT×70 09:30 $21.49 → close $20.86 -44.10; NEO×82 09:30 $18.36 → close $18.05 -25.42; NCNO×64 09:30 $23.30 → close $22.99 -19.84; DJT×155 09:30 $9.72 → close $9.63 -13.18 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.69 | ▼ 09:30 equity $10,192.21 vs yday $10,317.90 (-125.69) | 09:30 open · cash $139.69 (unchanged overnight, no fees) · equity $10,192.21 vs prior close $10,317.90 (-125.69) · 9 name(s) re-marked at the open (per-name table). CNTN×2 yday $2.22 → 09:30 $2.23 +0.02; SLI×6 yday $2.55 → 09:30 $2.58 +0.18; CAPR×154 yday $9.59 → 09:30 $9.50 -13.86; VYX×165 yday $8.78 → 09:30 $8.66 -19.80; SNPS×3 yday $442.61 → 09:30 $437.95 -13.98; SRPT×70 yday $20.86 → 09:30 $20.56 -21.00; NEO×82 yday $18.05 → 09:30 $17.77 -22.96; NCNO×64 yday $22.99 → 09:30 $22.66 -21.12; DJT×155 yday $9.63 → 09:30 $9.55 -13.17 | — |
| 2026-08-31 09:30 ET | **SELL** | `CNTN` | 2 | $2.23 | $0.07 | $-0.24 | $144.08 | ▼ -0.24 after sell → book $10,192.14; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $144.08 | ▲ close $10,289.18 vs 09:30 $10,192.21 (session +97.04) | 16:00 close · cash $144.08 · equity $10,289.18 vs 09:30 $10,192.21 (+96.97; session marks +97.04) · 8 name(s) marked open→close (per-name table). SLI×6 09:30 $2.58 → close $2.67 +0.54; CAPR×154 09:30 $9.50 → close $9.91 +63.14; VYX×165 09:30 $8.66 → close $8.38 -46.20; SNPS×3 09:30 $437.95 → close $439.59 +4.92; SRPT×70 09:30 $20.56 → close $21.03 +32.90; NEO×82 09:30 $17.77 → close $17.91 +11.48; NCNO×64 09:30 $22.66 → close $22.60 -3.84; DJT×155 09:30 $9.55 → close $9.77 +34.10 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $144.08 | ▼ 09:30 equity $10,284.30 vs yday $10,289.18 (-4.88) | 09:30 open · cash $144.08 (unchanged overnight, no fees) · equity $10,284.30 vs prior close $10,289.18 (-4.88) · 8 name(s) re-marked at the open (per-name table). SLI×6 yday $2.67 → 09:30 $2.67 +0.00; CAPR×154 yday $9.91 → 09:30 $10.77 +132.44; VYX×165 yday $8.38 → 09:30 $8.30 -13.20; SNPS×3 yday $439.59 → 09:30 $428.21 -34.14; SRPT×70 yday $21.03 → 09:30 $20.88 -10.50; NEO×82 yday $17.91 → 09:30 $17.67 -19.68; NCNO×64 yday $22.60 → 09:30 $22.15 -28.80; DJT×155 yday $9.77 → 09:30 $9.57 -31.00 | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 6 | $2.67 | $0.20 | $+0.05 | $159.90 | ▲ +0.05 after sell → book $10,284.10; vs 09:30 mark -0.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $159.90 | ▼ close $10,143.50 vs 09:30 $10,284.30 (session -140.60) | 16:00 close · cash $159.90 · equity $10,143.50 vs 09:30 $10,284.30 (-140.80; session marks -140.60) · 7 name(s) marked open→close (per-name table). CAPR×154 09:30 $10.77 → close $10.01 -117.04; VYX×165 09:30 $8.30 → close $8.74 +72.60; SNPS×3 09:30 $428.21 → close $414.82 -40.17; SRPT×70 09:30 $20.88 → close $21.33 +31.50; NEO×82 09:30 $17.67 → close $17.45 -18.04; NCNO×64 09:30 $22.15 → close $22.30 +9.60; DJT×155 09:30 $9.57 → close $9.06 -79.05 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $159.90 | ▼ 09:30 equity $10,134.37 vs yday $10,143.50 (-9.13) | 09:30 open · cash $159.90 (unchanged overnight, no fees) · equity $10,134.37 vs prior close $10,143.50 (-9.13) · 7 name(s) re-marked at the open (per-name table). CAPR×154 yday $10.01 → 09:30 $10.07 +9.24; VYX×165 yday $8.74 → 09:30 $8.73 -1.65; SNPS×3 yday $414.82 → 09:30 $413.78 -3.12; SRPT×70 yday $21.33 → 09:30 $21.33 +0.00; NEO×82 yday $17.45 → 09:30 $17.40 -4.10; NCNO×64 yday $22.30 → 09:30 $22.20 -6.40; DJT×155 yday $9.06 → 09:30 $9.04 -3.10 | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 154 | $10.07 | $2.49 | $+47.42 | $1,708.19 | ▲ +47.42 after sell → book $10,131.88; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `VYX` | 165 | $8.73 | $2.52 | $-71.01 | $3,146.11 | ▼ -71.01 after sell → book $10,129.35; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SNPS` | 3 | $413.78 | $2.02 | $-148.23 | $4,385.43 | ▼ -148.23 after sell → book $10,127.33; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SRPT` | 70 | $21.33 | $2.22 | $-15.62 | $5,876.31 | ▼ -15.62 after sell → book $10,125.11; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NEO` | 82 | $17.40 | $2.26 | $-83.22 | $7,300.85 | ▼ -83.22 after sell → book $10,122.85; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NCNO` | 64 | $22.20 | $2.20 | $-74.79 | $8,719.45 | ▼ -74.79 after sell → book $10,120.65; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DJT` | 155 | $9.04 | $2.49 | $-110.35 | $10,118.15 | ▼ -110.35 after sell → book $10,118.15; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,118.15 | ▲ close $10,118.15 vs 09:30 $10,134.37 (session +0.00) | 16:00 close · cash $10,118.15 · no lots left · equity $10,118.15. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,118.15 | ▲ 09:30 equity $10,118.15 vs yday $10,118.15 (+0.00) | 09:30 open · cash $10,118.15 · no holdings · equity $10,118.15 vs prior close $10,118.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $8,899.86 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1264.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $7,652.81 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1264.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 348 | $3.63 | $4.49 | — | $6,385.08 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1264.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 157 | $8.03 | $2.46 | — | $5,121.91 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1264.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $3,927.84 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1264.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 710 | $1.78 | $9.16 | — | $2,654.88 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+183.1; leftover $1264.77 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 52 | $23.88 | $2.15 | — | $1,410.98 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1264.77 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 929 | $1.36 | $11.98 | — | $135.55 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1264.77 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $135.55 | ▼ close $9,592.76 vs 09:30 $10,118.15 (session -489.00) | 16:00 close · cash $135.55 · equity $9,592.76 vs 09:30 $10,118.15 (-525.39; session marks -489.00) · 8 name(s) marked open→close (per-name table). ATRC×23 09:30 $52.88 → close $52.46 -9.66; HRMY×29 09:30 $42.93 → close $41.86 -31.03; CABA×348 09:30 $3.63 → close $3.48 -52.20; VSTM×157 09:30 $8.03 → close $7.98 -7.85; RVTY×9 09:30 $132.45 → close $130.63 -16.38; GPRO×710 09:30 $1.78 → close $1.39 -276.90; MMED×52 09:30 $23.88 → close $23.84 -2.08; SID×929 09:30 $1.36 → close $1.26 -92.90 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $135.55 | ▼ 09:30 equity $9,585.11 vs yday $9,592.76 (-7.65) | 09:30 open · cash $135.55 (unchanged overnight, no fees) · equity $9,585.11 vs prior close $9,592.76 (-7.65) · 8 name(s) re-marked at the open (per-name table). ATRC×23 yday $52.46 → 09:30 $52.03 -9.89; HRMY×29 yday $41.86 → 09:30 $41.50 -10.44; CABA×348 yday $3.48 → 09:30 $3.46 -6.96; VSTM×157 yday $7.98 → 09:30 $7.91 -10.99; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; GPRO×710 yday $1.39 → 09:30 $1.48 +63.90; MMED×52 yday $23.84 → 09:30 $23.84 +0.00; SID×929 yday $1.26 → 09:30 $1.23 -27.87 | — |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 3 | $4.53 | $0.14 | — | $121.82 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $16.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 6 | $2.51 | $0.17 | — | $106.59 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $16.94 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 2 | $5.75 | $0.12 | — | $94.97 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $16.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SCZM` | 1 | $10.03 | $0.10 | — | $84.83 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; ret5=+4.0; leftover $16.94 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 2 | $5.79 | $0.12 | — | $73.13 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $16.94 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.13 | ▲ close $9,765.31 vs 09:30 $9,585.11 (session +180.86) | 16:00 close · cash $73.13 · equity $9,765.31 vs 09:30 $9,585.11 (+180.20; session marks +180.86) · 13 name(s) marked open→close (per-name table). ATRC×23 09:30 $52.03 → close $51.52 -11.73; HRMY×29 09:30 $41.50 → close $42.25 +21.75; CABA×348 09:30 $3.46 → close $3.47 +3.48; VSTM×157 09:30 $7.91 → close $8.20 +45.53; RVTY×9 09:30 $130.03 → close $130.22 +1.71; GPRO×710 09:30 $1.48 → close $1.70 +156.20; MMED×52 09:30 $23.84 → close $23.29 -28.60; SID×929 09:30 $1.23 → close $1.22 -9.29; IRD×3 09:30 $4.53 → close $4.67 +0.42; BRR×6 09:30 $2.51 → close $2.66 +0.90; LENZ×2 09:30 $5.75 → close $5.96 +0.42; SCZM×1 09:30 $10.03 → close $9.94 -0.09; DFDV×2 09:30 $5.79 → close $5.87 +0.16 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.13 | ▼ 09:30 equity $9,751.16 vs yday $9,765.31 (-14.15) | 09:30 open · cash $73.13 (unchanged overnight, no fees) · equity $9,751.16 vs prior close $9,765.31 (-14.15) · 13 name(s) re-marked at the open (per-name table). ATRC×23 yday $51.52 → 09:30 $54.31 +64.17; HRMY×29 yday $42.25 → 09:30 $42.20 -1.45; CABA×348 yday $3.47 → 09:30 $3.43 -13.92; VSTM×157 yday $8.20 → 09:30 $8.20 +0.00; RVTY×9 yday $130.22 → 09:30 $128.50 -15.48; GPRO×710 yday $1.70 → 09:30 $1.56 -95.85; MMED×52 yday $23.29 → 09:30 $23.16 -6.76; SID×929 yday $1.22 → 09:30 $1.28 +55.74; IRD×3 yday $4.67 → 09:30 $4.53 -0.42; BRR×6 yday $2.66 → 09:30 $2.66 +0.00; LENZ×2 yday $5.96 → 09:30 $5.95 -0.02; SCZM×1 yday $9.94 → 09:30 $9.90 -0.04; DFDV×2 yday $5.87 → 09:30 $5.81 -0.12 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.13 | ▼ close $9,538.85 vs 09:30 $9,751.16 (session -212.31) | 16:00 close · cash $73.13 · equity $9,538.85 vs 09:30 $9,751.16 (-212.31; session marks -212.31) · 13 name(s) marked open→close (per-name table). ATRC×23 09:30 $54.31 → close $53.73 -13.34; HRMY×29 09:30 $42.20 → close $42.07 -3.77; CABA×348 09:30 $3.43 → close $3.27 -55.68; VSTM×157 09:30 $8.20 → close $8.08 -18.84; RVTY×9 09:30 $128.50 → close $127.08 -12.78; GPRO×710 09:30 $1.56 → close $1.46 -78.10; MMED×52 09:30 $23.16 → close $23.32 +8.32; SID×929 09:30 $1.28 → close $1.24 -37.16; IRD×3 09:30 $4.53 → close $4.34 -0.57; BRR×6 09:30 $2.66 → close $2.73 +0.42; LENZ×2 09:30 $5.95 → close $5.33 -1.24; SCZM×1 09:30 $9.90 → close $9.97 +0.07; DFDV×2 09:30 $5.81 → close $5.99 +0.36 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.13 | ▼ 09:30 equity $9,536.31 vs yday $9,538.85 (-2.54) | 09:30 open · cash $73.13 (unchanged overnight, no fees) · equity $9,536.31 vs prior close $9,538.85 (-2.54) · 13 name(s) re-marked at the open (per-name table). ATRC×23 yday $53.73 → 09:30 $53.16 -13.11; HRMY×29 yday $42.07 → 09:30 $42.01 -1.74; CABA×348 yday $3.27 → 09:30 $3.28 +3.48; VSTM×157 yday $8.08 → 09:30 $8.01 -10.99; RVTY×9 yday $127.08 → 09:30 $125.77 -11.79; GPRO×710 yday $1.46 → 09:30 $1.45 -3.55; MMED×52 yday $23.32 → 09:30 $23.22 -5.20; SID×929 yday $1.24 → 09:30 $1.28 +37.16; IRD×3 yday $4.34 → 09:30 $5.31 +2.91; BRR×6 yday $2.73 → 09:30 $2.75 +0.12; LENZ×2 yday $5.33 → 09:30 $5.31 -0.04; SCZM×1 yday $9.97 → 09:30 $10.12 +0.15; DFDV×2 yday $5.99 → 09:30 $6.02 +0.06 | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 23 | $53.16 | $2.08 | $+2.30 | $1,293.73 | ▲ +2.30 after sell → book $9,534.23; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 29 | $42.01 | $2.10 | $-30.85 | $2,509.93 | ▼ -30.85 after sell → book $9,532.14; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 348 | $3.28 | $4.56 | $-130.85 | $3,646.81 | ▼ -130.85 after sell → book $9,527.58; vs 09:30 mark -4.56 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 157 | $8.01 | $2.50 | $-8.10 | $4,901.88 | ▼ -8.10 after sell → book $9,525.08; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $6,031.78 | ▼ -64.17 after sell → book $9,523.05; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `GPRO` | 710 | $1.45 | $9.29 | $-252.75 | $7,051.99 | ▼ -252.75 after sell → book $9,513.76; vs 09:30 mark -9.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 52 | $23.22 | $2.17 | $-38.63 | $8,257.26 | ▼ -38.63 after sell → book $9,511.59; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SID` | 929 | $1.28 | $12.15 | $-98.45 | $9,434.23 | ▼ -98.45 after sell → book $9,499.44; vs 09:30 mark -12.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,434.23 | ▲ close $9,499.71 vs 09:30 $9,536.31 (session +0.27) | 16:00 close · cash $9,434.23 · equity $9,499.71 vs 09:30 $9,536.31 (-36.60; session marks +0.27) · 5 name(s) marked open→close (per-name table). IRD×3 09:30 $5.31 → close $5.73 +1.26; BRR×6 09:30 $2.75 → close $2.86 +0.66; LENZ×2 09:30 $5.31 → close $4.91 -0.80; SCZM×1 09:30 $10.12 → close $10.43 +0.31; DFDV×2 09:30 $6.02 → close $5.44 -1.16 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,434.23 | ▲ 09:30 equity $9,500.75 vs yday $9,499.71 (+1.04) | 09:30 open · cash $9,434.23 (unchanged overnight, no fees) · equity $9,500.75 vs prior close $9,499.71 (+1.04) · 5 name(s) re-marked at the open (per-name table). IRD×3 yday $5.73 → 09:30 $5.87 +0.42; BRR×6 yday $2.86 → 09:30 $2.86 +0.00; LENZ×2 yday $4.91 → 09:30 $4.98 +0.14; SCZM×1 yday $10.43 → 09:30 $10.77 +0.34; DFDV×2 yday $5.44 → 09:30 $5.51 +0.14 | — |
| 2026-09-10 09:30 ET | **SELL** | `BRR` | 6 | $2.86 | $0.21 | $+1.72 | $9,451.18 | ▲ +1.72 after sell → book $9,500.54; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LENZ` | 2 | $4.98 | $0.13 | $-1.79 | $9,461.02 | ▼ -1.79 after sell → book $9,500.42; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `SCZM` | 1 | $10.77 | $0.13 | $+0.51 | $9,471.66 | ▲ +0.51 after sell → book $9,500.29; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DFDV` | 2 | $5.51 | $0.14 | $-0.82 | $9,482.54 | ▼ -0.82 after sell → book $9,500.15; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,482.54 | ▲ close $9,500.18 vs 09:30 $9,500.75 (session +0.03) | 16:00 close · cash $9,482.54 · equity $9,500.18 vs 09:30 $9,500.75 (-0.57; session marks +0.03) · 1 name(s) marked open→close (per-name table). IRD×3 09:30 $5.87 → close $5.88 +0.03 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLG` | cash | leftover split 2.44 < 1 share @ 57.61 |
| 2026-08-14 | `ADUR` | cash | leftover split 2.44 < 1 share @ 16.50 |
| 2026-08-14 | `ARX` | cash | leftover split 2.44 < 1 share @ 19.57 |
| 2026-08-14 | `AIRO` | cash | leftover split 2.44 < 1 share @ 11.12 |
| 2026-08-14 | `TBBB` | cash | leftover split 2.44 < 1 share @ 48.82 |
| 2026-08-14 | `AMPY` | cash | leftover split 2.44 < 1 share @ 4.94 |
| 2026-08-14 | `SNDK` | cash | leftover split 2.44 < 1 share @ 1646.93 |
| 2026-08-14 | `MH` | cash | leftover split 2.44 < 1 share @ 13.55 |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 2.44 < 1 share @ 46.18 |
| 2026-08-17 | `OCC` | cash | leftover split 2.44 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 2.44 < 1 share @ 16.20 |
| 2026-08-17 | `CAPR` | cash | leftover split 2.44 < 1 share @ 6.87 |
| 2026-08-17 | `HTFL` | cash | leftover split 2.44 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 2.44 < 1 share @ 32.55 |
| 2026-08-17 | `LPTH` | cash | leftover split 2.44 < 1 share @ 14.94 |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ALEC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 7.42 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 7.42 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 7.42 < 1 share @ 216.30 |
| 2026-08-21 | `CF` | cash | leftover split 7.42 < 1 share @ 127.43 |
| 2026-08-21 | `MRVI` | cash | leftover split 7.42 < 1 share @ 8.28 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ABX` | cash | leftover split 5.72 < 1 share @ 9.83 |
| 2026-08-26 | `KURA` | cash | leftover split 5.72 < 1 share @ 13.63 |
| 2026-08-26 | `ACRS` | cash | leftover split 5.72 < 1 share @ 6.53 |
| 2026-08-26 | `FIGR` | cash | leftover split 5.72 < 1 share @ 40.50 |
| 2026-08-26 | `MNRO` | cash | leftover split 5.72 < 1 share @ 14.00 |
| 2026-08-26 | `FUTU` | cash | leftover split 5.72 < 1 share @ 124.67 |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FWDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CNTN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 17.70 < 1 share @ 41.44 |
| 2026-08-28 | `CNTN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `VYX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SNPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SRPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NEO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NCNO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DJT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `APPN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CXM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CMRC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RCKT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GWRE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `VYX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SNPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SRPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NEO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NCNO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DJT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SKYX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `METC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SID` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `GPRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 16.94 < 1 share @ 513.78 |
| 2026-09-04 | `TARS` | cash | leftover split 16.94 < 1 share @ 82.70 |
| 2026-09-04 | `ASST` | cash | leftover split 16.94 < 1 share @ 25.18 |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `GPRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SID` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `IRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `LENZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SECZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SKYX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GSM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `IRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LENZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SSL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `KEP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SLDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SKHY` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ARBE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `PAYP` | hard_red | hard-red S=-13.28 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `IRD` | 3 | 2026-09-04 @ $4.53 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $16.94 |
