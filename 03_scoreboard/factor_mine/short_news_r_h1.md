# Factor mine action — `short_news_r_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · news🔴

Cash book **-2.77%** ($9,723) · signal-only (no cash/fees) was -5.63%. Starts YES **2/26**. Fills 122 · skips 33 · realized $-446.03.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the news camera (does the morning packet like the headline?) is red.

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
- **Gate** `news=bad` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $15,892.73.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `EU` | 847 | — | $1.18 | +0.00 | $1.21 | -25.41 | -25.41 | -0.00 | -25.41 |
| 2026-08-14 | `LUNR` | 52 | — | $19.17 | +0.00 | $19.01 | +8.32 | +8.32 | -0.00 | +8.32 |
| 2026-08-14 | `OWL` | 78 | — | $12.70 | +0.00 | $12.22 | +37.05 | +37.05 | -0.00 | +37.05 |
| 2026-08-14 | `FA` | 45 | — | $22.15 | +0.00 | $21.58 | +25.65 | +25.65 | -0.00 | +25.65 |
| 2026-08-14 | `SVV` | 91 | — | $10.89 | +0.00 | $10.66 | +20.93 | +20.93 | -0.00 | +20.93 |
| 2026-08-17 | `EU` | 847 | $1.21 | $1.21 | +0.00 | — | +0.00 | +0.00 | -25.41 | — |
| 2026-08-17 | `LUNR` | 52 | $19.01 | $20.25 | -64.48 | — | +0.00 | -64.48 | -56.16 | — |
| 2026-08-17 | `OWL` | 78 | $12.22 | $12.12 | +7.80 | — | +0.00 | +7.80 | +44.85 | — |
| 2026-08-17 | `FA` | 45 | $21.58 | $21.35 | +10.35 | — | +0.00 | +10.35 | +36.00 | — |
| 2026-08-17 | `SVV` | 91 | $10.66 | $10.49 | +15.47 | — | +0.00 | +15.47 | +36.40 | — |
| 2026-08-17 | `VERI` | 724 | — | $1.15 | +0.00 | $1.08 | +47.06 | +47.06 | -0.00 | +47.06 |
| 2026-08-17 | `ZNTL` | 233 | — | $3.56 | +0.00 | $3.71 | -33.79 | -33.79 | -0.00 | -33.79 |
| 2026-08-17 | `APMD` | 26 | — | $31.70 | +0.00 | $32.55 | -22.10 | -22.10 | -0.00 | -22.10 |
| 2026-08-17 | `HIVE` | 276 | — | $3.01 | +0.00 | $3.07 | -16.56 | -16.56 | -0.00 | -16.56 |
| 2026-08-17 | `BIRK` | 21 | — | $39.48 | +0.00 | $37.86 | +34.02 | +34.02 | -0.00 | +34.02 |
| 2026-08-17 | `OPLN` | 23 | — | $35.36 | +0.00 | $35.09 | +6.21 | +6.21 | -0.00 | +6.21 |
| 2026-08-18 | `VERI` | 724 | $1.08 | $1.05 | +25.34 | — | +0.00 | +25.34 | +72.40 | — |
| 2026-08-18 | `ZNTL` | 233 | $3.71 | $3.75 | -10.48 | — | +0.00 | -10.48 | -44.27 | — |
| 2026-08-18 | `APMD` | 26 | $32.55 | $32.85 | -7.80 | — | +0.00 | -7.80 | -29.90 | — |
| 2026-08-18 | `HIVE` | 276 | $3.07 | $2.96 | +30.36 | — | +0.00 | +30.36 | +13.80 | — |
| 2026-08-18 | `BIRK` | 21 | $37.86 | $38.07 | -4.41 | — | +0.00 | -4.41 | +29.61 | — |
| 2026-08-18 | `OPLN` | 23 | $35.09 | $35.03 | +1.38 | — | +0.00 | +1.38 | +7.59 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AEM` | 3 | — | $204.45 | +0.00 | $212.04 | -22.77 | -22.77 | -0.00 | -22.77 |
| 2026-08-20 | `WYFI` | 29 | — | $21.40 | +0.00 | $21.16 | +6.96 | +6.96 | -0.00 | +6.96 |
| 2026-08-20 | `TOYO` | 141 | — | $4.43 | +0.00 | $4.51 | -11.98 | -11.98 | -0.00 | -11.98 |
| 2026-08-20 | `ABCL` | 52 | — | $11.81 | +0.00 | $11.57 | +12.74 | +12.74 | -0.00 | +12.74 |
| 2026-08-20 | `TEAM` | 3 | — | $173.90 | +0.00 | $174.91 | -3.03 | -3.03 | -0.00 | -3.03 |
| 2026-08-20 | `AAP` | 13 | — | $46.85 | +0.00 | $42.39 | +57.98 | +57.98 | -0.00 | +57.98 |
| 2026-08-20 | `WMT` | 5 | — | $106.38 | +0.00 | $103.84 | +12.70 | +12.70 | -0.00 | +12.70 |
| 2026-08-20 | `AQST` | 135 | — | $4.61 | +0.00 | $4.50 | +15.53 | +15.53 | -0.00 | +15.53 |
| 2026-08-21 | `AEM` | 3 | $212.04 | $216.30 | -12.78 | — | +0.00 | -12.78 | -35.55 | — |
| 2026-08-21 | `WYFI` | 29 | $21.16 | $21.54 | -11.02 | — | +0.00 | -11.02 | -4.06 | — |
| 2026-08-21 | `TOYO` | 141 | $4.51 | $4.68 | -23.27 | — | +0.00 | -23.27 | -35.25 | — |
| 2026-08-21 | `ABCL` | 52 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | +12.74 | — |
| 2026-08-21 | `TEAM` | 3 | $174.91 | $174.22 | +2.07 | — | +0.00 | +2.07 | -0.96 | — |
| 2026-08-21 | `AAP` | 13 | $42.39 | $42.41 | -0.26 | — | +0.00 | -0.26 | +57.72 | — |
| 2026-08-21 | `WMT` | 5 | $103.84 | $103.69 | +0.75 | — | +0.00 | +0.75 | +13.45 | — |
| 2026-08-21 | `AQST` | 135 | $4.50 | $4.54 | -6.07 | — | +0.00 | -6.07 | +9.45 | — |
| 2026-08-21 | `QTRX` | 200 | — | $3.11 | +0.00 | $2.99 | +24.00 | +24.00 | -0.00 | +24.00 |
| 2026-08-21 | `MRNA` | 4 | — | $133.11 | +0.00 | $145.13 | -48.08 | -48.08 | -0.00 | -48.08 |
| 2026-08-21 | `AUGO` | 7 | — | $89.10 | +0.00 | $87.26 | +12.88 | +12.88 | -0.00 | +12.88 |
| 2026-08-21 | `SSRM` | 16 | — | $38.40 | +0.00 | $37.77 | +10.08 | +10.08 | -0.00 | +10.08 |
| 2026-08-21 | `ARIS` | 29 | — | $20.90 | +0.00 | $20.86 | +1.16 | +1.16 | -0.00 | +1.16 |
| 2026-08-21 | `NOG` | 23 | — | $27.00 | +0.00 | $27.34 | -7.82 | -7.82 | -0.00 | -7.82 |
| 2026-08-21 | `ALH` | 26 | — | $23.33 | +0.00 | $23.47 | -3.64 | -3.64 | -0.00 | -3.64 |
| 2026-08-21 | `AMLX` | 15 | — | $39.80 | +0.00 | $38.66 | +17.03 | +17.03 | -0.00 | +17.03 |
| 2026-08-24 | `QTRX` | 200 | $2.99 | $2.99 | +0.00 | — | +0.00 | +0.00 | +24.00 | — |
| 2026-08-24 | `MRNA` | 4 | $145.13 | $142.70 | +9.72 | — | +0.00 | +9.72 | -38.36 | — |
| 2026-08-24 | `AUGO` | 7 | $87.26 | $88.60 | -9.38 | — | +0.00 | -9.38 | +3.50 | — |
| 2026-08-24 | `SSRM` | 16 | $37.77 | $38.32 | -8.80 | — | +0.00 | -8.80 | +1.28 | — |
| 2026-08-24 | `ARIS` | 29 | $20.86 | $20.98 | -3.48 | — | +0.00 | -3.48 | -2.32 | — |
| 2026-08-24 | `NOG` | 23 | $27.34 | $27.12 | +5.06 | $26.84 | +6.44 | +11.50 | -2.76 | +3.68 |
| 2026-08-24 | `ALH` | 26 | $23.47 | $23.66 | -4.94 | $23.84 | -4.68 | -9.62 | -8.58 | -13.26 |
| 2026-08-24 | `AMLX` | 15 | $38.66 | $38.64 | +0.30 | — | +0.00 | +0.30 | +17.33 | — |
| 2026-08-25 | `NOG` | 23 | $26.84 | $26.06 | +17.94 | — | +0.00 | +17.94 | +21.62 | — |
| 2026-08-25 | `ALH` | 26 | $23.84 | $24.16 | -8.32 | $23.85 | +8.06 | -0.26 | -21.58 | -13.52 |
| 2026-08-25 | `AVAH` | 91 | — | $13.62 | +0.00 | $13.59 | +3.19 | +3.19 | -0.00 | +3.19 |
| 2026-08-25 | `SSRM` | 32 | — | $37.75 | +0.00 | $39.21 | -46.72 | -46.72 | -0.00 | -46.72 |
| 2026-08-25 | `ARE` | 22 | — | $54.51 | +0.00 | $52.90 | +35.42 | +35.42 | -0.00 | +35.42 |
| 2026-08-25 | `BMO` | 7 | — | $175.01 | +0.00 | $173.46 | +10.85 | +10.85 | -0.00 | +10.85 |
| 2026-08-26 | `ALH` | 26 | $23.85 | $24.00 | -3.90 | — | +0.00 | -3.90 | -17.42 | — |
| 2026-08-26 | `AVAH` | 91 | $13.59 | $13.65 | -5.46 | — | +0.00 | -5.46 | -2.28 | — |
| 2026-08-26 | `SSRM` | 32 | $39.21 | $38.41 | +25.60 | — | +0.00 | +25.60 | -21.12 | — |
| 2026-08-26 | `ARE` | 22 | $52.90 | $52.77 | +2.86 | — | +0.00 | +2.86 | +38.28 | — |
| 2026-08-26 | `BMO` | 7 | $173.46 | $173.22 | +1.68 | — | +0.00 | +1.68 | +12.53 | — |
| 2026-08-26 | `BE` | 4 | — | $213.94 | +0.00 | $218.21 | -17.08 | -17.08 | -0.00 | -17.08 |
| 2026-08-26 | `ABCL` | 81 | — | $12.22 | +0.00 | $12.24 | -1.62 | -1.62 | -0.00 | -1.62 |
| 2026-08-26 | `AQST` | 196 | — | $5.08 | +0.00 | $5.39 | -60.76 | -60.76 | -0.00 | -60.76 |
| 2026-08-26 | `NEM` | 7 | — | $132.64 | +0.00 | $131.60 | +7.28 | +7.28 | -0.00 | +7.28 |
| 2026-08-26 | `WB` | 140 | — | $7.10 | +0.00 | $7.04 | +8.40 | +8.40 | -0.00 | +8.40 |
| 2026-08-27 | `BE` | 4 | $218.21 | $227.10 | -35.56 | — | +0.00 | -35.56 | -52.64 | — |
| 2026-08-27 | `ABCL` | 81 | $12.24 | $12.25 | -0.81 | — | +0.00 | -0.81 | -2.43 | — |
| 2026-08-27 | `AQST` | 196 | $5.39 | $5.39 | +0.00 | $5.16 | +45.08 | +45.08 | -60.76 | -15.68 |
| 2026-08-27 | `NEM` | 7 | $131.60 | $131.02 | +4.06 | — | +0.00 | +4.06 | +11.34 | — |
| 2026-08-27 | `WB` | 140 | $7.04 | $7.04 | +0.00 | — | +0.00 | +0.00 | +8.40 | — |
| 2026-08-27 | `INTU` | 4 | — | $353.54 | +0.00 | $348.00 | +22.16 | +22.16 | -0.00 | +22.16 |
| 2026-08-27 | `MT` | 22 | — | $74.54 | +0.00 | $74.63 | -1.98 | -1.98 | -0.00 | -1.98 |
| 2026-08-27 | `TX` | 29 | — | $55.25 | +0.00 | $55.83 | -16.82 | -16.82 | -0.00 | -16.82 |
| 2026-08-28 | `AQST` | 196 | $5.16 | $5.11 | +9.80 | — | +0.00 | +9.80 | -5.88 | — |
| 2026-08-28 | `INTU` | 4 | $348.00 | $347.82 | +0.72 | $358.06 | -40.96 | -40.24 | +22.88 | -18.08 |
| 2026-08-28 | `MT` | 22 | $74.63 | $75.39 | -16.72 | — | +0.00 | -16.72 | -18.70 | — |
| 2026-08-28 | `TX` | 29 | $55.83 | $55.97 | -4.06 | — | +0.00 | -4.06 | -20.88 | — |
| 2026-08-28 | `SIMO` | 4 | — | $252.24 | +0.00 | $245.81 | +25.72 | +25.72 | -0.00 | +25.72 |
| 2026-08-28 | `FIG` | 40 | — | $30.18 | +0.00 | $28.82 | +54.40 | +54.40 | -0.00 | +54.40 |
| 2026-08-28 | `JAZZ` | 4 | — | $249.48 | +0.00 | $244.54 | +19.76 | +19.76 | -0.00 | +19.76 |
| 2026-08-28 | `ALH` | 53 | — | $23.27 | +0.00 | $23.23 | +2.12 | +2.12 | -0.00 | +2.12 |
| 2026-08-31 | `INTU` | 4 | $358.06 | $356.05 | +8.04 | — | +0.00 | +8.04 | -10.04 | — |
| 2026-08-31 | `SIMO` | 4 | $245.81 | $247.05 | -4.96 | — | +0.00 | -4.96 | +20.76 | — |
| 2026-08-31 | `FIG` | 40 | $28.82 | $27.60 | +48.80 | — | +0.00 | +48.80 | +103.20 | — |
| 2026-08-31 | `JAZZ` | 4 | $244.54 | $241.39 | +12.60 | — | +0.00 | +12.60 | +32.36 | — |
| 2026-08-31 | `ALH` | 53 | $23.23 | $23.06 | +9.01 | — | +0.00 | +9.01 | +11.13 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `SLN` | 112 | — | $14.85 | +0.00 | $14.79 | +6.72 | +6.72 | -0.00 | +6.72 |
| 2026-09-03 | `OPK` | 974 | — | $1.71 | +0.00 | $1.61 | +97.40 | +97.40 | -0.00 | +97.40 |
| 2026-09-03 | `PCG` | 124 | — | $13.35 | +0.00 | $13.96 | -75.64 | -75.64 | -0.00 | -75.64 |
| 2026-09-04 | `SLN` | 112 | $14.79 | $14.63 | +17.92 | — | +0.00 | +17.92 | +24.64 | — |
| 2026-09-04 | `OPK` | 974 | $1.61 | $1.59 | +19.48 | $1.64 | -48.70 | -29.22 | +116.88 | +68.18 |
| 2026-09-04 | `PCG` | 124 | $13.96 | $13.80 | +19.84 | — | +0.00 | +19.84 | -55.80 | — |
| 2026-09-04 | `GSM` | 538 | — | $4.67 | +0.00 | $4.67 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-09-04 | `PIPR` | 32 | — | $76.55 | +0.00 | $77.04 | -15.68 | -15.68 | -0.00 | -15.68 |
| 2026-09-08 | `OPK` | 974 | $1.64 | $1.63 | +9.74 | — | +0.00 | +9.74 | +77.92 | — |
| 2026-09-08 | `GSM` | 538 | $4.67 | $4.75 | -43.04 | $4.52 | +123.74 | +80.70 | -43.04 | +80.70 |
| 2026-09-08 | `PIPR` | 32 | $77.04 | $76.64 | +12.80 | — | +0.00 | +12.80 | -2.88 | — |
| 2026-09-09 | `GSM` | 538 | $4.52 | $4.52 | +0.00 | — | +0.00 | +0.00 | +80.70 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `QRVO` | 6 | — | $112.83 | +0.00 | $116.65 | -22.89 | -22.89 | -0.00 | -22.89 |
| 2026-09-11 | `RWT` | 204 | — | $3.52 | +0.00 | $3.55 | -6.12 | -6.12 | -0.00 | -6.12 |
| 2026-09-11 | `CRDL` | 354 | — | $2.03 | +0.00 | $2.00 | +12.39 | +12.39 | -0.00 | +12.39 |
| 2026-09-11 | `MYGN` | 213 | — | $3.37 | +0.00 | $3.42 | -10.65 | -10.65 | -0.00 | -10.65 |
| 2026-09-11 | `BKV` | 28 | — | $24.97 | +0.00 | $24.23 | +20.72 | +20.72 | -0.00 | +20.72 |
| 2026-09-11 | `GFR` | 116 | — | $6.19 | +0.00 | $6.52 | -38.28 | -38.28 | -0.00 | -38.28 |
| 2026-09-11 | `INGM` | 27 | — | $26.62 | +0.00 | $27.55 | -25.11 | -25.11 | -0.00 | -25.11 |
| 2026-09-14 | `QRVO` | 6 | $116.65 | $114.11 | +15.24 | — | +0.00 | +15.24 | -7.65 | — |
| 2026-09-14 | `RWT` | 204 | $3.55 | $3.53 | +4.08 | — | +0.00 | +4.08 | -2.04 | — |
| 2026-09-14 | `CRDL` | 354 | $2.00 | $1.98 | +5.31 | — | +0.00 | +5.31 | +17.70 | — |
| 2026-09-14 | `MYGN` | 213 | $3.42 | $3.43 | -2.13 | $3.79 | -76.68 | -78.81 | -12.78 | -89.46 |
| 2026-09-14 | `BKV` | 28 | $24.23 | $24.26 | -0.84 | — | +0.00 | -0.84 | +19.88 | — |
| 2026-09-14 | `GFR` | 116 | $6.52 | $6.60 | -9.28 | $6.62 | -2.32 | -11.60 | -47.56 | -49.88 |
| 2026-09-14 | `INGM` | 27 | $27.55 | $26.89 | +17.82 | $26.85 | +1.08 | +18.90 | -7.29 | -6.21 |
| 2026-09-15 | `MYGN` | 213 | $3.79 | $3.80 | -2.13 | — | +0.00 | -2.13 | -91.59 | — |
| 2026-09-15 | `GFR` | 116 | $6.62 | $6.61 | +1.16 | $6.93 | -37.12 | -35.96 | -48.72 | -85.84 |
| 2026-09-15 | `INGM` | 27 | $26.85 | $26.91 | -1.62 | — | +0.00 | -1.62 | -7.83 | — |
| 2026-09-16 | `GFR` | 116 | $6.93 | $6.83 | +11.60 | $6.49 | +39.44 | +51.04 | -74.24 | -34.80 |
| 2026-09-16 | `BBNX` | 88 | — | $18.61 | +0.00 | $22.18 | -314.16 | -314.16 | -0.00 | -314.16 |
| 2026-09-16 | `TRMD` | 45 | — | $35.90 | +0.00 | $36.60 | -31.50 | -31.50 | -0.00 | -31.50 |
| 2026-09-16 | `AXON` | 3 | — | $441.33 | +0.00 | $468.42 | -81.27 | -81.27 | -0.00 | -81.27 |
| 2026-09-17 | `GFR` | 116 | $6.49 | $6.48 | +1.16 | $6.66 | -20.88 | -19.72 | -33.64 | -54.52 |
| 2026-09-17 | `BBNX` | 88 | $22.18 | $22.46 | -24.64 | $21.43 | +90.64 | +66.00 | -338.80 | -248.16 |
| 2026-09-17 | `TRMD` | 45 | $36.60 | $36.52 | +3.60 | $36.63 | -4.95 | -1.35 | -27.90 | -32.85 |
| 2026-09-17 | `AXON` | 3 | $468.42 | $468.73 | -0.93 | — | +0.00 | -0.93 | -82.20 | — |
| 2026-09-17 | `BULL` | 198 | — | $7.95 | +0.00 | $7.71 | +47.52 | +47.52 | -0.00 | +47.52 |
| 2026-09-17 | `LEN` | 19 | — | $81.00 | +0.00 | $79.70 | +24.70 | +24.70 | -0.00 | +24.70 |
| 2026-09-17 | `BWIN` | 49 | — | $32.06 | +0.00 | $31.95 | +5.39 | +5.39 | -0.00 | +5.39 |
| 2026-09-18 | `GFR` | 116 | $6.66 | $6.64 | +2.32 | — | +0.00 | +2.32 | -52.20 | — |
| 2026-09-18 | `BBNX` | 88 | $21.43 | $21.30 | +11.44 | — | +0.00 | +11.44 | -236.72 | — |
| 2026-09-18 | `TRMD` | 45 | $36.63 | $37.71 | -48.60 | — | +0.00 | -48.60 | -81.45 | — |
| 2026-09-18 | `BULL` | 198 | $7.71 | $7.85 | -27.72 | — | +0.00 | -27.72 | +19.80 | — |
| 2026-09-18 | `LEN` | 19 | $79.70 | $78.25 | +27.55 | — | +0.00 | +27.55 | +52.25 | — |
| 2026-09-18 | `BWIN` | 49 | $31.95 | $31.98 | -1.47 | $31.93 | +2.45 | +0.98 | +3.92 | +6.37 |
| 2026-09-18 | `PSKY` | 451 | — | $10.59 | +0.00 | $10.21 | +171.38 | +171.38 | -0.00 | +171.38 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +66.54 | EU, LUNR, OWL, FA, SVV | — | $14,954.19 | $10,046.48 | EU×847, LUNR×52, OWL×78, FA×45, SVV×91 |
| 2026-08-17 | +2.25 | $14,954.19 | EU×847, LUNR×52, OWL×78, FA×45, SVV×91 | $10,015.62 | -30.86 | +14.84 | VERI, ZNTL, APMD, HIVE, BIRK, OPLN | EU, LUNR, OWL, FA, SVV | $14,932.82 | $9,988.27 | VERI×724, ZNTL×233, APMD×26, HIVE×276, BIRK×21, OPLN×23 |
| 2026-08-18 | -6.20 | $14,932.82 | VERI×724, ZNTL×233, APMD×26, HIVE×276, BIRK×21, OPLN×23 | $10,022.65 | +34.38 | +0.00 | — | VERI, ZNTL, APMD, HIVE, BIRK, OPLN | $10,000.56 | $10,000.56 | — |
| 2026-08-19 | -7.20 | $10,000.56 | — | $10,000.56 | +0.00 | +0.00 | — | — | $10,000.56 | $10,000.56 | — |
| 2026-08-20 | +1.12 | $10,000.56 | — | $10,000.56 | +0.00 | +68.13 | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | — | $14,741.15 | $10,051.31 | AEM×3, WYFI×29, TOYO×141, ABCL×52, TEAM×3, AAP×13, WMT×5, AQST×135 |
| 2026-08-21 | +3.25 | $14,741.15 | AEM×3, WYFI×29, TOYO×141, ABCL×52, TEAM×3, AAP×13, WMT×5, AQST×135 | $10,000.73 | -50.58 | +5.61 | QTRX, MRNA, AUGO, SSRM, ARIS, NOG, ALH, AMLX | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | $14,789.61 | $9,972.07 | QTRX×200, MRNA×4, AUGO×7, SSRM×16, ARIS×29, NOG×23, ALH×26, AMLX×15 |
| 2026-08-24 | -5.17 | $14,789.61 | QTRX×200, MRNA×4, AUGO×7, SSRM×16, ARIS×29, NOG×23, ALH×26, AMLX×15 | $9,960.55 | -11.52 | +1.76 | — | QTRX, MRNA, AUGO, SSRM, ARIS, AMLX | $11,186.72 | $9,949.56 | NOG×23, ALH×26 |
| 2026-08-25 | +1.80 | $11,186.72 | NOG×23, ALH×26 | $9,959.18 | +9.62 | +10.80 | AVAH, SSRM, ARE, BMO | NOG | $15,448.81 | $9,959.28 | ALH×26, AVAH×91, SSRM×32, ARE×22, BMO×7 |
| 2026-08-26 | +2.02 | $15,448.81 | ALH×26, AVAH×91, SSRM×32, ARE×22, BMO×7 | $9,980.06 | +20.78 | -63.78 | BE, ABCL, AQST, NEM, WB | ALH, AVAH, SSRM, ARE, BMO | $14,721.81 | $9,894.29 | BE×4, ABCL×81, AQST×196, NEM×7, WB×140 |
| 2026-08-27 | — | $14,721.81 | BE×4, ABCL×81, AQST×196, NEM×7, WB×140 | $9,861.98 | -32.31 | +48.44 | INTU, MT, TX | BE, ABCL, NEM, WB | $15,559.73 | $9,895.44 | AQST×196, INTU×4, MT×22, TX×29 |
| 2026-08-28 | +0.75 | $15,559.73 | AQST×196, INTU×4, MT×22, TX×29 | $9,885.18 | -10.26 | +61.04 | SIMO, FIG, JAZZ, ALH | AQST, MT, TX | $15,708.67 | $9,931.04 | INTU×4, SIMO×4, FIG×40, JAZZ×4, ALH×53 |
| 2026-08-31 | -5.85 | $15,708.67 | INTU×4, SIMO×4, FIG×40, JAZZ×4, ALH×53 | $10,004.53 | +73.49 | +0.00 | — | INTU, SIMO, FIG, JAZZ, ALH | $9,994.27 | $9,994.27 | — |
| 2026-09-01 | -6.30 | $9,994.27 | — | $9,994.27 | -0.00 | +0.00 | — | — | $9,994.27 | $9,994.27 | — |
| 2026-09-02 | -3.83 | $9,994.27 | — | $9,994.27 | -0.00 | +0.00 | — | — | $9,994.27 | $9,994.27 | — |
| 2026-09-03 | -0.90 | $9,994.27 | — | $9,994.27 | -0.00 | +28.48 | SLN, OPK, PCG | — | $14,960.78 | $10,005.12 | SLN×112, OPK×974, PCG×124 |
| 2026-09-04 | +2.25 | $14,960.78 | SLN×112, OPK×974, PCG×124 | $10,062.36 | +57.24 | -64.38 | GSM, PIPR | SLN, PCG | $16,559.09 | $9,983.99 | OPK×974, GSM×538, PIPR×32 |
| 2026-09-08 | -11.47 | $16,559.09 | OPK×974, GSM×538, PIPR×32 | $9,963.49 | -20.50 | +123.74 | — | OPK, PIPR | $12,504.34 | $10,072.58 | GSM×538 |
| 2026-09-09 | -13.95 | $12,504.34 | GSM×538 | $10,072.58 | -0.00 | +0.00 | — | GSM | $10,065.64 | $10,065.64 | — |
| 2026-09-10 | -13.28 | $10,065.64 | — | $10,065.64 | -0.00 | +0.00 | — | — | $10,065.64 | $10,065.64 | — |
| 2026-09-11 | +0.50 | $10,065.64 | — | $10,065.64 | -0.00 | -69.94 | QRVO, RWT, CRDL, MYGN, BKV, GFR, INGM | — | $15,014.28 | $9,976.88 | QRVO×6, RWT×204, CRDL×354, MYGN×213, BKV×28, GFR×116, INGM×27 |
| 2026-09-14 | -11.00 | $15,014.28 | QRVO×6, RWT×204, CRDL×354, MYGN×213, BKV×28, GFR×116, INGM×27 | $10,007.08 | +30.20 | -77.92 | — | QRVO, RWT, CRDL, BKV | $12,218.02 | $9,917.88 | MYGN×213, GFR×116, INGM×27 |
| 2026-09-15 | -3.84 | $12,218.02 | MYGN×213, GFR×116, INGM×27 | $9,915.29 | -2.59 | -37.12 | — | MYGN, INGM | $10,677.23 | $9,873.35 | GFR×116 |
| 2026-09-16 | +5.30 | $10,677.23 | GFR×116 | $9,884.95 | +11.60 | -387.49 | BBNX, TRMD, AXON | — | $15,247.82 | $9,490.88 | GFR×116, BBNX×88, TRMD×45, AXON×3 |
| 2026-09-17 | +7.38 | $15,247.82 | GFR×116, BBNX×88, TRMD×45, AXON×3 | $9,470.07 | -20.81 | +142.42 | BULL, LEN, BWIN | AXON | $18,516.69 | $9,603.51 | GFR×116, BBNX×88, TRMD×45, BULL×198, LEN×19, BWIN×49 |
| 2026-09-18 | +4.86 | $18,516.69 | GFR×116, BBNX×88, TRMD×45, BULL×198, LEN×19, BWIN×49 | $9,567.03 | -36.48 | +173.83 | PSKY | GFR, BBNX, TRMD, BULL, LEN | $15,892.73 | $9,723.45 | BWIN×49, PSKY×451 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 847 | $1.18 | $11.10 | — | $10,988.36 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 52 | $19.17 | $2.19 | — | $11,983.00 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 78 | $12.70 | $2.27 | — | $12,970.94 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🔴 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `FA` | 45 | $22.15 | $2.17 | — | $13,965.52 | — | news🔴; gate news=bad; list oppset; 🔵; ⚪; ret5=-8.0; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `SVV` | 91 | $10.89 | $2.32 | — | $14,954.19 | — | news🔴; gate news=bad; list oppset; 🔵; ret5=-0.7; leftover $1000.00 | join🟢 sector🔴 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,954.19 | ▲ close $10,046.48 vs 09:30 $10,000.00 (session +66.54) | 16:00 close · cash $14,954.19 · equity $10,046.48 vs 09:30 $10,000.00 (+46.48; session marks +66.54) · 5 name(s) marked open→close (per-name table). EU×847 09:30 $1.18 → close $1.21 -25.41; LUNR×52 09:30 $19.17 → close $19.01 +8.32; OWL×78 09:30 $12.70 → close $12.22 +37.05; FA×45 09:30 $22.15 → close $21.58 +25.65; SVV×91 09:30 $10.89 → close $10.66 +20.93 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,954.19 | ▼ 09:30 equity $10,015.62 vs yday $10,046.48 (-30.86) | 09:30 open · cash $14,954.19 (unchanged overnight, no fees) · equity $10,015.62 vs prior close $10,046.48 (-30.86) · 5 name(s) re-marked at the open (per-name table). EU×847 yday $1.21 → 09:30 $1.21 -0.00; LUNR×52 yday $19.01 → 09:30 $20.25 -64.48; OWL×78 yday $12.22 → 09:30 $12.12 +7.80; FA×45 yday $21.58 → 09:30 $21.35 +10.35; SVV×91 yday $10.66 → 09:30 $10.49 +15.47 | — |
| 2026-08-17 09:30 ET | **COVER** | `EU` | 847 | $1.21 | $10.93 | $-47.44 | $13,918.39 | ▼ -47.44 after sell → book $10,004.69; vs 09:30 mark -10.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `LUNR` | 52 | $20.25 | $2.15 | $-60.50 | $12,863.25 | ▼ -60.50 after sell → book $10,002.55; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **COVER** | `OWL` | 78 | $12.12 | $2.22 | $+40.35 | $11,915.66 | ▲ +40.35 after sell → book $10,000.32; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `FA` | 45 | $21.35 | $2.12 | $+31.70 | $10,952.79 | ▲ +31.70 after sell → book $9,998.20; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `SVV` | 91 | $10.49 | $2.26 | $+31.82 | $9,995.94 | ▲ +31.82 after sell → book $9,995.94; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 724 | $1.15 | $9.49 | — | $10,819.04 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; leftover $832.99 | join🟡 sector🟢 gen🟢 news🔴 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 233 | $3.56 | $3.08 | — | $11,645.45 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; leftover $832.99 | join🟡 sector🔴 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 26 | $31.70 | $2.11 | — | $12,467.54 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; leftover $832.99 | join🟡 sector🔴 gen🟢 news🔴 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 276 | $3.01 | $3.64 | — | $13,294.66 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; leftover $832.99 | join🟢 sector🟢 gen🟢 news🔴 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `BIRK` | 21 | $39.48 | $2.10 | — | $14,121.64 | — | news🔴; gate news=bad; list oppset; ret5=+2.3; leftover $832.99 | join🟡 sector🔴 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `OPLN` | 23 | $35.36 | $2.10 | — | $14,932.82 | — | news🔴; gate news=bad; list oppset; ret5=-4.0; leftover $832.99 | join🟡 sector🔴 gen🟢 news🔴 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,932.82 | ▲ close $9,988.27 vs 09:30 $10,015.62 (session +14.84) | 16:00 close · cash $14,932.82 · equity $9,988.27 vs 09:30 $10,015.62 (-27.35; session marks +14.84) · 6 name(s) marked open→close (per-name table). VERI×724 09:30 $1.15 → close $1.08 +47.06; ZNTL×233 09:30 $3.56 → close $3.71 -33.79; APMD×26 09:30 $31.70 → close $32.55 -22.10; HIVE×276 09:30 $3.01 → close $3.07 -16.56; BIRK×21 09:30 $39.48 → close $37.86 +34.02; OPLN×23 09:30 $35.36 → close $35.09 +6.21 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,932.82 | ▲ 09:30 equity $10,022.65 vs yday $9,988.27 (+34.38) | 09:30 open · cash $14,932.82 (unchanged overnight, no fees) · equity $10,022.65 vs prior close $9,988.27 (+34.38) · 6 name(s) re-marked at the open (per-name table). VERI×724 yday $1.08 → 09:30 $1.05 +25.34; ZNTL×233 yday $3.71 → 09:30 $3.75 -10.48; APMD×26 yday $32.55 → 09:30 $32.85 -7.80; HIVE×276 yday $3.07 → 09:30 $2.96 +30.36; BIRK×21 yday $37.86 → 09:30 $38.07 -4.41; OPLN×23 yday $35.09 → 09:30 $35.03 +1.38 | — |
| 2026-08-18 09:30 ET | **COVER** | `VERI` | 724 | $1.05 | $9.34 | $+53.57 | $14,163.28 | ▲ +53.57 after sell → book $10,013.31; vs 09:30 mark -9.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `ZNTL` | 233 | $3.75 | $3.01 | $-50.35 | $13,286.52 | ▼ -50.35 after sell → book $10,010.30; vs 09:30 mark -3.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `APMD` | 26 | $32.85 | $2.07 | $-34.08 | $12,430.36 | ▼ -34.08 after sell → book $10,008.24; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `HIVE` | 276 | $2.96 | $3.56 | $+6.60 | $11,609.84 | ▲ +6.60 after sell → book $10,004.68; vs 09:30 mark -3.56 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `BIRK` | 21 | $38.07 | $2.05 | $+25.46 | $10,808.31 | ▲ +25.46 after sell → book $10,002.62; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `OPLN` | 23 | $35.03 | $2.06 | $+3.43 | $10,000.56 | ▲ +3.43 after sell → book $10,000.56; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.56 | ▲ close $10,000.56 vs 09:30 $10,022.65 (session +0.00) | 16:00 close · cash $10,000.56 · no lots left · equity $10,000.56. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.56 | ▲ 09:30 equity $10,000.56 vs yday $10,000.56 (+0.00) | 09:30 open · cash $10,000.56 · no holdings · equity $10,000.56 vs prior close $10,000.56 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.56 | ▲ close $10,000.56 vs 09:30 $10,000.56 (session +0.00) | 16:00 close · cash $10,000.56 · no lots left · equity $10,000.56. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.56 | ▲ 09:30 equity $10,000.56 vs yday $10,000.56 (+0.00) | 09:30 open · cash $10,000.56 · no holdings · equity $10,000.56 vs prior close $10,000.56 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $10,611.88 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $625.04 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 29 | $21.40 | $2.11 | — | $11,230.36 | — | news🔴; gate news=bad; list yday_mover,oppset; 🔵; ret5=-25.2; leftover $625.04 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 141 | $4.43 | $2.46 | — | $11,852.53 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; leftover $625.04 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 52 | $11.81 | $2.18 | — | $12,464.73 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $625.04 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $12,984.40 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; leftover $625.04 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $13,591.38 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; leftover $625.04 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 5 | $106.38 | $2.04 | — | $14,121.24 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; leftover $625.04 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 135 | $4.61 | $2.44 | — | $14,741.15 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $625.04 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,741.15 | ▲ close $10,051.31 vs 09:30 $10,000.56 (session +68.13) | 16:00 close · cash $14,741.15 · equity $10,051.31 vs 09:30 $10,000.56 (+50.75; session marks +68.13) · 8 name(s) marked open→close (per-name table). AEM×3 09:30 $204.45 → close $212.04 -22.77; WYFI×29 09:30 $21.40 → close $21.16 +6.96; TOYO×141 09:30 $4.43 → close $4.51 -11.98; ABCL×52 09:30 $11.81 → close $11.57 +12.74; TEAM×3 09:30 $173.90 → close $174.91 -3.03; AAP×13 09:30 $46.85 → close $42.39 +57.98; WMT×5 09:30 $106.38 → close $103.84 +12.70; AQST×135 09:30 $4.61 → close $4.50 +15.53 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,741.15 | ▼ 09:30 equity $10,000.73 vs yday $10,051.31 (-50.58) | 09:30 open · cash $14,741.15 (unchanged overnight, no fees) · equity $10,000.73 vs prior close $10,051.31 (-50.58) · 8 name(s) re-marked at the open (per-name table). AEM×3 yday $212.04 → 09:30 $216.30 -12.78; WYFI×29 yday $21.16 → 09:30 $21.54 -11.02; TOYO×141 yday $4.51 → 09:30 $4.68 -23.27; ABCL×52 yday $11.57 → 09:30 $11.57 -0.00; TEAM×3 yday $174.91 → 09:30 $174.22 +2.07; AAP×13 yday $42.39 → 09:30 $42.41 -0.26; WMT×5 yday $103.84 → 09:30 $103.69 +0.75; AQST×135 yday $4.50 → 09:30 $4.54 -6.07 | — |
| 2026-08-21 09:30 ET | **COVER** | `AEM` | 3 | $216.30 | $2.00 | $-39.58 | $14,090.25 | ▼ -39.58 after sell → book $9,998.73; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `WYFI` | 29 | $21.54 | $2.08 | $-8.25 | $13,463.51 | ▼ -8.25 after sell → book $9,996.65; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `TOYO` | 141 | $4.68 | $2.41 | $-40.13 | $12,801.22 | ▼ -40.13 after sell → book $9,994.24; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `ABCL` | 52 | $11.57 | $2.15 | $+8.41 | $12,197.43 | ▲ +8.41 after sell → book $9,992.09; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `TEAM` | 3 | $174.22 | $2.00 | $-4.99 | $11,672.77 | ▼ -4.99 after sell → book $9,990.09; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `AAP` | 13 | $42.41 | $2.03 | $+53.63 | $11,119.41 | ▲ +53.63 after sell → book $9,988.06; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `WMT` | 5 | $103.69 | $2.00 | $+9.41 | $10,598.96 | ▲ +9.41 after sell → book $9,986.06; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `AQST` | 135 | $4.54 | $2.40 | $+4.61 | $9,983.66 | ▲ +4.61 after sell → book $9,983.66; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 200 | $3.11 | $2.65 | — | $10,603.01 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $623.98 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 4 | $133.11 | $2.04 | — | $11,133.42 | — | news🔴; gate news=bad; list yday_mover,oppset; 🔵; ⚪; ret5=+109.5; leftover $623.98 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 7 | $89.10 | $2.05 | — | $11,755.07 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $623.98 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 16 | $38.40 | $2.07 | — | $12,367.39 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $623.98 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 29 | $20.90 | $2.11 | — | $12,971.38 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $623.98 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 23 | $27.00 | $2.10 | — | $13,590.28 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; leftover $623.98 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ALH` | 26 | $23.33 | $2.10 | — | $14,194.76 | — | news🔴; gate news=bad; list oppset; 🔵; ret5=-10.0; leftover $623.98 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AMLX` | 15 | $39.80 | $2.07 | — | $14,789.61 | — | news🔴; gate news=bad; list oppset; ⚪; ret5=+76.4; leftover $623.98 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,789.61 | ▲ close $9,972.07 vs 09:30 $10,000.73 (session +5.61) | 16:00 close · cash $14,789.61 · equity $9,972.07 vs 09:30 $10,000.73 (-28.66; session marks +5.61) · 8 name(s) marked open→close (per-name table). QTRX×200 09:30 $3.11 → close $2.99 +24.00; MRNA×4 09:30 $133.11 → close $145.13 -48.08; AUGO×7 09:30 $89.10 → close $87.26 +12.88; SSRM×16 09:30 $38.40 → close $37.77 +10.08; ARIS×29 09:30 $20.90 → close $20.86 +1.16; NOG×23 09:30 $27.00 → close $27.34 -7.82; ALH×26 09:30 $23.33 → close $23.47 -3.64; AMLX×15 09:30 $39.80 → close $38.66 +17.03 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,789.61 | ▼ 09:30 equity $9,960.55 vs yday $9,972.07 (-11.52) | 09:30 open · cash $14,789.61 (unchanged overnight, no fees) · equity $9,960.55 vs prior close $9,972.07 (-11.52) · 8 name(s) re-marked at the open (per-name table). QTRX×200 yday $2.99 → 09:30 $2.99 -0.00; MRNA×4 yday $145.13 → 09:30 $142.70 +9.72; AUGO×7 yday $87.26 → 09:30 $88.60 -9.38; SSRM×16 yday $37.77 → 09:30 $38.32 -8.80; ARIS×29 yday $20.86 → 09:30 $20.98 -3.48; NOG×23 yday $27.34 → 09:30 $27.12 +5.06; ALH×26 yday $23.47 → 09:30 $23.66 -4.94; AMLX×15 yday $38.66 → 09:30 $38.64 +0.30 | — |
| 2026-08-24 09:30 ET | **COVER** | `QTRX` | 200 | $2.99 | $2.59 | $+18.76 | $14,189.02 | ▲ +18.76 after sell → book $9,957.96; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `MRNA` | 4 | $142.70 | $2.00 | $-42.40 | $13,616.22 | ▼ -42.40 after sell → book $9,955.96; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **COVER** | `AUGO` | 7 | $88.60 | $2.01 | $-0.56 | $12,994.01 | ▼ -0.56 after sell → book $9,953.95; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `SSRM` | 16 | $38.32 | $2.04 | $-2.83 | $12,378.85 | ▼ -2.83 after sell → book $9,951.91; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `ARIS` | 29 | $20.98 | $2.08 | $-6.51 | $11,768.35 | ▼ -6.51 after sell → book $9,949.83; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AMLX` | 15 | $38.64 | $2.04 | $+13.22 | $11,186.72 | ▲ +13.22 after sell → book $9,947.80; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,186.72 | ▲ close $9,949.56 vs 09:30 $9,960.55 (session +1.76) | 16:00 close · cash $11,186.72 · equity $9,949.56 vs 09:30 $9,960.55 (-10.99; session marks +1.76) · 2 name(s) marked open→close (per-name table). NOG×23 09:30 $27.12 → close $26.84 +6.44; ALH×26 09:30 $23.66 → close $23.84 -4.68 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,186.72 | ▲ 09:30 equity $9,959.18 vs yday $9,949.56 (+9.62) | 09:30 open · cash $11,186.72 (unchanged overnight, no fees) · equity $9,959.18 vs prior close $9,949.56 (+9.62) · 2 name(s) re-marked at the open (per-name table). NOG×23 yday $26.84 → 09:30 $26.06 +17.94; ALH×26 yday $23.84 → 09:30 $24.16 -8.32 | — |
| 2026-08-25 09:30 ET | **COVER** | `NOG` | 23 | $26.06 | $2.06 | $+17.46 | $10,585.28 | ▲ +17.46 after sell → book $9,957.12; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 91 | $13.62 | $2.32 | — | $11,822.83 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $1244.64 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `SSRM` | 32 | $37.75 | $2.14 | — | $13,028.69 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.8; leftover $1244.64 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 22 | $54.51 | $2.11 | — | $14,225.81 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; leftover $1244.64 | join🔴 sector🟡 gen🟡 news🔴 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 7 | $175.01 | $2.06 | — | $15,448.81 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; leftover $1244.64 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,448.81 | ▲ close $9,959.28 vs 09:30 $9,959.18 (session +10.80) | 16:00 close · cash $15,448.81 · equity $9,959.28 vs 09:30 $9,959.18 (+0.10; session marks +10.80) · 5 name(s) marked open→close (per-name table). ALH×26 09:30 $24.16 → close $23.85 +8.06; AVAH×91 09:30 $13.62 → close $13.59 +3.19; SSRM×32 09:30 $37.75 → close $39.21 -46.72; ARE×22 09:30 $54.51 → close $52.90 +35.42; BMO×7 09:30 $175.01 → close $173.46 +10.85 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,448.81 | ▲ 09:30 equity $9,980.06 vs yday $9,959.28 (+20.78) | 09:30 open · cash $15,448.81 (unchanged overnight, no fees) · equity $9,980.06 vs prior close $9,959.28 (+20.78) · 5 name(s) re-marked at the open (per-name table). ALH×26 yday $23.85 → 09:30 $24.00 -3.90; AVAH×91 yday $13.59 → 09:30 $13.65 -5.46; SSRM×32 yday $39.21 → 09:30 $38.41 +25.60; ARE×22 yday $52.90 → 09:30 $52.77 +2.86; BMO×7 yday $173.46 → 09:30 $173.22 +1.68 | — |
| 2026-08-26 09:30 ET | **COVER** | `ALH` | 26 | $24.00 | $2.07 | $-21.59 | $14,822.74 | ▼ -21.59 after sell → book $9,977.99; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `AVAH` | 91 | $13.65 | $2.26 | $-6.86 | $13,578.33 | ▼ -6.86 after sell → book $9,975.73; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 32 | $38.41 | $2.09 | $-25.35 | $12,347.12 | ▼ -25.35 after sell → book $9,973.64; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARE` | 22 | $52.77 | $2.06 | $+34.12 | $11,184.13 | ▲ +34.12 after sell → book $9,971.59; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `BMO` | 7 | $173.22 | $2.01 | $+8.45 | $9,969.58 | ▲ +8.45 after sell → book $9,969.58; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 4 | $213.94 | $2.05 | — | $10,823.29 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $996.96 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 81 | $12.22 | $2.28 | — | $11,810.83 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; leftover $996.96 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 196 | $5.08 | $2.65 | — | $12,803.86 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; leftover $996.96 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 7 | $132.64 | $2.06 | — | $13,730.28 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; leftover $996.96 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `WB` | 140 | $7.10 | $2.47 | — | $14,721.81 | — | news🔴; gate news=bad; list oppset; 🔵; ret5=-6.2; leftover $996.96 | join🟡 sector🟡 gen🟢 news🔴 digest🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,721.81 | ▼ close $9,894.29 vs 09:30 $9,980.06 (session -63.78) | 16:00 close · cash $14,721.81 · equity $9,894.29 vs 09:30 $9,980.06 (-85.77; session marks -63.78) · 5 name(s) marked open→close (per-name table). BE×4 09:30 $213.94 → close $218.21 -17.08; ABCL×81 09:30 $12.22 → close $12.24 -1.62; AQST×196 09:30 $5.08 → close $5.39 -60.76; NEM×7 09:30 $132.64 → close $131.60 +7.28; WB×140 09:30 $7.10 → close $7.04 +8.40 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,721.81 | ▼ 09:30 equity $9,861.98 vs yday $9,894.29 (-32.31) | 09:30 open · cash $14,721.81 (unchanged overnight, no fees) · equity $9,861.98 vs prior close $9,894.29 (-32.31) · 5 name(s) re-marked at the open (per-name table). BE×4 yday $218.21 → 09:30 $227.10 -35.56; ABCL×81 yday $12.24 → 09:30 $12.25 -0.81; AQST×196 yday $5.39 → 09:30 $5.39 -0.00; NEM×7 yday $131.60 → 09:30 $131.02 +4.06; WB×140 yday $7.04 → 09:30 $7.04 -0.00 | — |
| 2026-08-27 09:30 ET | **COVER** | `BE` | 4 | $227.10 | $2.00 | $-56.69 | $13,811.41 | ▼ -56.69 after sell → book $9,859.98; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **COVER** | `ABCL` | 81 | $12.25 | $2.23 | $-6.95 | $12,816.93 | ▼ -6.95 after sell → book $9,857.75; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `NEM` | 7 | $131.02 | $2.01 | $+7.27 | $11,897.78 | ▲ +7.27 after sell → book $9,855.74; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `WB` | 140 | $7.04 | $2.41 | $+3.52 | $10,909.77 | ▲ +3.52 after sell → book $9,853.33; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SHORT** | `INTU` | 4 | $353.54 | $2.06 | — | $12,321.86 | — | news🔴; gate news=bad; list earn_react; ret5=-4.6; leftover $1642.22 | join🔴 sector🟢 gen🟢 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 22 | $74.54 | $2.12 | — | $13,959.62 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; leftover $1642.22 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 29 | $55.25 | $2.14 | — | $15,559.73 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; leftover $1642.22 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,559.73 | ▲ close $9,895.44 vs 09:30 $9,861.98 (session +48.44) | 16:00 close · cash $15,559.73 · equity $9,895.44 vs 09:30 $9,861.98 (+33.46; session marks +48.44) · 4 name(s) marked open→close (per-name table). AQST×196 09:30 $5.39 → close $5.16 +45.08; INTU×4 09:30 $353.54 → close $348.00 +22.16; MT×22 09:30 $74.54 → close $74.63 -1.98; TX×29 09:30 $55.25 → close $55.83 -16.82 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,559.73 | ▼ 09:30 equity $9,885.18 vs yday $9,895.44 (-10.26) | 09:30 open · cash $15,559.73 (unchanged overnight, no fees) · equity $9,885.18 vs prior close $9,895.44 (-10.26) · 4 name(s) re-marked at the open (per-name table). AQST×196 yday $5.16 → 09:30 $5.11 +9.80; INTU×4 yday $348.00 → 09:30 $347.82 +0.72; MT×22 yday $74.63 → 09:30 $75.39 -16.72; TX×29 yday $55.83 → 09:30 $55.97 -4.06 | — |
| 2026-08-28 09:30 ET | **COVER** | `AQST` | 196 | $5.11 | $2.58 | $-11.11 | $14,555.59 | ▼ -11.11 after sell → book $9,882.60; vs 09:30 mark -2.58 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `MT` | 22 | $75.39 | $2.06 | $-22.88 | $12,894.95 | ▼ -22.88 after sell → book $9,880.54; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `TX` | 29 | $55.97 | $2.08 | $-25.10 | $11,269.75 | ▼ -25.10 after sell → book $9,878.47; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 4 | $252.24 | $2.05 | — | $12,276.66 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1234.81 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 40 | $30.18 | $2.16 | — | $13,481.69 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; leftover $1234.81 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `JAZZ` | 4 | $249.48 | $2.05 | — | $14,477.56 | — | news🔴; gate news=bad; list oppset; ret5=+1.0; leftover $1234.81 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `ALH` | 53 | $23.27 | $2.20 | — | $15,708.67 | — | news🔴; gate news=bad; list oppset; ret5=+0.3; leftover $1234.81 | join🟡 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,708.67 | ▲ close $9,931.04 vs 09:30 $9,885.18 (session +61.04) | 16:00 close · cash $15,708.67 · equity $9,931.04 vs 09:30 $9,885.18 (+45.86; session marks +61.04) · 5 name(s) marked open→close (per-name table). INTU×4 09:30 $347.82 → close $358.06 -40.96; SIMO×4 09:30 $252.24 → close $245.81 +25.72; FIG×40 09:30 $30.18 → close $28.82 +54.40; JAZZ×4 09:30 $249.48 → close $244.54 +19.76; ALH×53 09:30 $23.27 → close $23.23 +2.12 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,708.67 | ▲ 09:30 equity $10,004.53 vs yday $9,931.04 (+73.49) | 09:30 open · cash $15,708.67 (unchanged overnight, no fees) · equity $10,004.53 vs prior close $9,931.04 (+73.49) · 5 name(s) re-marked at the open (per-name table). INTU×4 yday $358.06 → 09:30 $356.05 +8.04; SIMO×4 yday $245.81 → 09:30 $247.05 -4.96; FIG×40 yday $28.82 → 09:30 $27.60 +48.80; JAZZ×4 yday $244.54 → 09:30 $241.39 +12.60; ALH×53 yday $23.23 → 09:30 $23.06 +9.01 | — |
| 2026-08-31 09:30 ET | **COVER** | `INTU` | 4 | $356.05 | $2.00 | $-14.10 | $14,282.47 | ▼ -14.10 after sell → book $10,002.53; vs 09:30 mark -2.00 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SIMO` | 4 | $247.05 | $2.00 | $+16.71 | $13,292.27 | ▲ +16.71 after sell → book $10,000.53; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `FIG` | 40 | $27.60 | $2.11 | $+98.93 | $12,186.16 | ▲ +98.93 after sell → book $9,998.42; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **COVER** | `JAZZ` | 4 | $241.39 | $2.00 | $+28.31 | $11,218.60 | ▲ +28.31 after sell → book $9,996.42; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `ALH` | 53 | $23.06 | $2.15 | $+6.78 | $9,994.27 | ▲ +6.78 after sell → book $9,994.27; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,994.27 | ▲ close $9,994.27 vs 09:30 $10,004.53 (session +0.00) | 16:00 close · cash $9,994.27 · no lots left · equity $9,994.27. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,994.27 | ▲ 09:30 equity $9,994.27 vs yday $9,994.27 (-0.00) | 09:30 open · cash $9,994.27 · no holdings · equity $9,994.27 vs prior close $9,994.27 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,994.27 | ▲ close $9,994.27 vs 09:30 $9,994.27 (session +0.00) | 16:00 close · cash $9,994.27 · no lots left · equity $9,994.27. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,994.27 | ▲ 09:30 equity $9,994.27 vs yday $9,994.27 (-0.00) | 09:30 open · cash $9,994.27 · no holdings · equity $9,994.27 vs prior close $9,994.27 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,994.27 | ▲ close $9,994.27 vs 09:30 $9,994.27 (session +0.00) | 16:00 close · cash $9,994.27 · no lots left · equity $9,994.27. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,994.27 | ▲ 09:30 equity $9,994.27 vs yday $9,994.27 (-0.00) | 09:30 open · cash $9,994.27 · no holdings · equity $9,994.27 vs prior close $9,994.27 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 112 | $14.85 | $2.40 | — | $11,655.06 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1665.71 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 974 | $1.71 | $12.79 | — | $13,307.82 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; leftover $1665.71 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `PCG` | 124 | $13.35 | $2.44 | — | $14,960.78 | — | news🔴; gate news=bad; list oppset; ret5=-26.8; leftover $1665.71 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,960.78 | ▲ close $10,005.12 vs 09:30 $9,994.27 (session +28.48) | 16:00 close · cash $14,960.78 · equity $10,005.12 vs 09:30 $9,994.27 (+10.85; session marks +28.48) · 3 name(s) marked open→close (per-name table). SLN×112 09:30 $14.85 → close $14.79 +6.72; OPK×974 09:30 $1.71 → close $1.61 +97.40; PCG×124 09:30 $13.35 → close $13.96 -75.64 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,960.78 | ▲ 09:30 equity $10,062.36 vs yday $10,005.12 (+57.24) | 09:30 open · cash $14,960.78 (unchanged overnight, no fees) · equity $10,062.36 vs prior close $10,005.12 (+57.24) · 3 name(s) re-marked at the open (per-name table). SLN×112 yday $14.79 → 09:30 $14.63 +17.92; OPK×974 yday $1.61 → 09:30 $1.59 +19.48; PCG×124 yday $13.96 → 09:30 $13.80 +19.84 | — |
| 2026-09-04 09:30 ET | **COVER** | `SLN` | 112 | $14.63 | $2.33 | $+19.91 | $13,319.89 | ▲ +19.91 after sell → book $10,060.03; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `PCG` | 124 | $13.80 | $2.36 | $-60.60 | $11,606.33 | ▼ -60.60 after sell → book $10,057.67; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 538 | $4.67 | $7.12 | — | $14,111.67 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; leftover $2514.42 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 32 | $76.55 | $2.18 | — | $16,559.09 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $2514.42 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,559.09 | ▼ close $9,983.99 vs 09:30 $10,062.36 (session -64.38) | 16:00 close · cash $16,559.09 · equity $9,983.99 vs 09:30 $10,062.36 (-78.37; session marks -64.38) · 3 name(s) marked open→close (per-name table). OPK×974 09:30 $1.59 → close $1.64 -48.70; GSM×538 09:30 $4.67 → close $4.67 -0.00; PIPR×32 09:30 $76.55 → close $77.04 -15.68 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,559.09 | ▼ 09:30 equity $9,963.49 vs yday $9,983.99 (-20.50) | 09:30 open · cash $16,559.09 (unchanged overnight, no fees) · equity $9,963.49 vs prior close $9,983.99 (-20.50) · 3 name(s) re-marked at the open (per-name table). OPK×974 yday $1.64 → 09:30 $1.63 +9.74; GSM×538 yday $4.67 → 09:30 $4.75 -43.04; PIPR×32 yday $77.04 → 09:30 $76.64 +12.80 | — |
| 2026-09-08 09:30 ET | **COVER** | `OPK` | 974 | $1.63 | $12.56 | $+52.57 | $14,958.90 | ▲ +52.57 after sell → book $9,950.92; vs 09:30 mark -12.57 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `PIPR` | 32 | $76.64 | $2.09 | $-7.15 | $12,504.34 | ▼ -7.15 after sell → book $9,948.84; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,504.34 | ▲ close $10,072.58 vs 09:30 $9,963.49 (session +123.74) | 16:00 close · cash $12,504.34 · equity $10,072.58 vs 09:30 $9,963.49 (+109.09; session marks +123.74) · 1 name(s) marked open→close (per-name table). GSM×538 09:30 $4.75 → close $4.52 +123.74 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,504.34 | ▲ 09:30 equity $10,072.58 vs yday $10,072.58 (-0.00) | 09:30 open · cash $12,504.34 (unchanged overnight, no fees) · equity $10,072.58 vs prior close $10,072.58 (-0.00) · 1 name(s) re-marked at the open (per-name table). GSM×538 yday $4.52 → 09:30 $4.52 -0.00 | — |
| 2026-09-09 09:30 ET | **COVER** | `GSM` | 538 | $4.52 | $6.94 | $+66.64 | $10,065.64 | ▲ +66.64 after sell → book $10,065.64; vs 09:30 mark -6.94 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,065.64 | ▲ close $10,065.64 vs 09:30 $10,072.58 (session +0.00) | 16:00 close · cash $10,065.64 · no lots left · equity $10,065.64. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,065.64 | ▲ 09:30 equity $10,065.64 vs yday $10,065.64 (-0.00) | 09:30 open · cash $10,065.64 · no holdings · equity $10,065.64 vs prior close $10,065.64 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,065.64 | ▲ close $10,065.64 vs 09:30 $10,065.64 (session +0.00) | 16:00 close · cash $10,065.64 · no lots left · equity $10,065.64. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,065.64 | ▲ 09:30 equity $10,065.64 vs yday $10,065.64 (-0.00) | 09:30 open · cash $10,065.64 · no holdings · equity $10,065.64 vs prior close $10,065.64 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 6 | $112.83 | $2.05 | — | $10,740.60 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $718.97 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 204 | $3.52 | $2.70 | — | $11,455.98 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; leftover $718.97 | join🔴 sector🔴 gen🟡 news🔴 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 354 | $2.03 | $4.66 | — | $12,169.95 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; leftover $718.97 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 judge🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 213 | $3.37 | $2.81 | — | $12,884.95 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; leftover $718.97 | join🔴 sector🟡 gen🟡 news🔴 digest🔴 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 28 | $24.97 | $2.11 | — | $13,581.99 | — | news🔴; gate news=bad; list oppset; ret5=-0.6; leftover $718.97 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `GFR` | 116 | $6.19 | $2.39 | — | $14,297.65 | — | news🔴; gate news=bad; list oppset; ret5=+1.1; leftover $718.97 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `INGM` | 27 | $26.62 | $2.11 | — | $15,014.28 | — | news🔴; gate news=bad; list oppset; ret5=+0.7; leftover $718.97 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,014.28 | ▼ close $9,976.88 vs 09:30 $10,065.64 (session -69.94) | 16:00 close · cash $15,014.28 · equity $9,976.88 vs 09:30 $10,065.64 (-88.76; session marks -69.94) · 7 name(s) marked open→close (per-name table). QRVO×6 09:30 $112.83 → close $116.65 -22.89; RWT×204 09:30 $3.52 → close $3.55 -6.12; CRDL×354 09:30 $2.03 → close $2.00 +12.39; MYGN×213 09:30 $3.37 → close $3.42 -10.65; BKV×28 09:30 $24.97 → close $24.23 +20.72; GFR×116 09:30 $6.19 → close $6.52 -38.28; INGM×27 09:30 $26.62 → close $27.55 -25.11 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,014.28 | ▲ 09:30 equity $10,007.08 vs yday $9,976.88 (+30.20) | 09:30 open · cash $15,014.28 (unchanged overnight, no fees) · equity $10,007.08 vs prior close $9,976.88 (+30.20) · 7 name(s) re-marked at the open (per-name table). QRVO×6 yday $116.65 → 09:30 $114.11 +15.24; RWT×204 yday $3.55 → 09:30 $3.53 +4.08; CRDL×354 yday $2.00 → 09:30 $1.98 +5.31; MYGN×213 yday $3.42 → 09:30 $3.43 -2.13; BKV×28 yday $24.23 → 09:30 $24.26 -0.84; GFR×116 yday $6.52 → 09:30 $6.60 -9.28; INGM×27 yday $27.55 → 09:30 $26.89 +17.82 | — |
| 2026-09-14 09:30 ET | **COVER** | `QRVO` | 6 | $114.11 | $2.01 | $-11.70 | $14,327.61 | ▼ -11.70 after sell → book $10,005.07; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **COVER** | `RWT` | 204 | $3.53 | $2.63 | $-7.37 | $13,604.86 | ▼ -7.37 after sell → book $10,002.44; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `CRDL` | 354 | $1.98 | $4.57 | $+8.48 | $12,899.37 | ▲ +8.48 after sell → book $9,997.87; vs 09:30 mark -4.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `BKV` | 28 | $24.26 | $2.07 | $+15.69 | $12,218.02 | ▲ +15.69 after sell → book $9,995.80; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,218.02 | ▼ close $9,917.88 vs 09:30 $10,007.08 (session -77.92) | 16:00 close · cash $12,218.02 · equity $9,917.88 vs 09:30 $10,007.08 (-89.20; session marks -77.92) · 3 name(s) marked open→close (per-name table). MYGN×213 09:30 $3.43 → close $3.79 -76.68; GFR×116 09:30 $6.60 → close $6.62 -2.32; INGM×27 09:30 $26.89 → close $26.85 +1.08 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,218.02 | ▼ 09:30 equity $9,915.29 vs yday $9,917.88 (-2.59) | 09:30 open · cash $12,218.02 (unchanged overnight, no fees) · equity $9,915.29 vs prior close $9,917.88 (-2.59) · 3 name(s) re-marked at the open (per-name table). MYGN×213 yday $3.79 → 09:30 $3.80 -2.13; GFR×116 yday $6.62 → 09:30 $6.61 +1.16; INGM×27 yday $26.85 → 09:30 $26.91 -1.62 | — |
| 2026-09-15 09:30 ET | **COVER** | `MYGN` | 213 | $3.80 | $2.75 | $-97.15 | $11,405.87 | ▼ -97.15 after sell → book $9,912.54; vs 09:30 mark -2.75 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **COVER** | `INGM` | 27 | $26.91 | $2.07 | $-12.01 | $10,677.23 | ▼ -12.01 after sell → book $9,910.47; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,677.23 | ▼ close $9,873.35 vs 09:30 $9,915.29 (session -37.12) | 16:00 close · cash $10,677.23 · equity $9,873.35 vs 09:30 $9,915.29 (-41.94; session marks -37.12) · 1 name(s) marked open→close (per-name table). GFR×116 09:30 $6.61 → close $6.93 -37.12 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,677.23 | ▲ 09:30 equity $9,884.95 vs yday $9,873.35 (+11.60) | 09:30 open · cash $10,677.23 (unchanged overnight, no fees) · equity $9,884.95 vs prior close $9,873.35 (+11.60) · 1 name(s) re-marked at the open (per-name table). GFR×116 yday $6.93 → 09:30 $6.83 +11.60 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 88 | $18.61 | $2.33 | — | $12,312.58 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1647.49 | join🟢 sector🟢 gen🟢 news🔴 digest🔴 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **SHORT** | `TRMD` | 45 | $35.90 | $2.19 | — | $13,925.89 | — | news🔴; gate news=bad; list oppset; 🔵; ret5=+2.6; leftover $1647.49 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **SHORT** | `AXON` | 3 | $441.33 | $2.06 | — | $15,247.82 | — | news🔴; gate news=bad; list oppset; 🔵; ret5=-10.8; leftover $1647.49 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,247.82 | ▼ close $9,490.88 vs 09:30 $9,884.95 (session -387.49) | 16:00 close · cash $15,247.82 · equity $9,490.88 vs 09:30 $9,884.95 (-394.07; session marks -387.49) · 4 name(s) marked open→close (per-name table). GFR×116 09:30 $6.83 → close $6.49 +39.44; BBNX×88 09:30 $18.61 → close $22.18 -314.16; TRMD×45 09:30 $35.90 → close $36.60 -31.50; AXON×3 09:30 $441.33 → close $468.42 -81.27 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,247.82 | ▼ 09:30 equity $9,470.07 vs yday $9,490.88 (-20.81) | 09:30 open · cash $15,247.82 (unchanged overnight, no fees) · equity $9,470.07 vs prior close $9,490.88 (-20.81) · 4 name(s) re-marked at the open (per-name table). GFR×116 yday $6.49 → 09:30 $6.48 +1.16; BBNX×88 yday $22.18 → 09:30 $22.46 -24.64; TRMD×45 yday $36.60 → 09:30 $36.52 +3.60; AXON×3 yday $468.42 → 09:30 $468.73 -0.93 | — |
| 2026-09-17 09:30 ET | **COVER** | `AXON` | 3 | $468.73 | $2.00 | $-86.25 | $13,839.63 | ▼ -86.25 after sell → book $9,468.07; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 198 | $7.95 | $2.67 | — | $15,411.06 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $1578.01 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 19 | $81.00 | $2.11 | — | $16,947.95 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-3.0; leftover $1578.01 | join🔴 sector🟢 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `BWIN` | 49 | $32.06 | $2.20 | — | $18,516.69 | — | news🔴; gate news=bad; list oppset; ret5=-3.7; leftover $1578.01 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,516.69 | ▲ close $9,603.51 vs 09:30 $9,470.07 (session +142.42) | 16:00 close · cash $18,516.69 · equity $9,603.51 vs 09:30 $9,470.07 (+133.44; session marks +142.42) · 6 name(s) marked open→close (per-name table). GFR×116 09:30 $6.48 → close $6.66 -20.88; BBNX×88 09:30 $22.46 → close $21.43 +90.64; TRMD×45 09:30 $36.52 → close $36.63 -4.95; BULL×198 09:30 $7.95 → close $7.71 +47.52; LEN×19 09:30 $81.00 → close $79.70 +24.70; BWIN×49 09:30 $32.06 → close $31.95 +5.39 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,516.69 | ▼ 09:30 equity $9,567.03 vs yday $9,603.51 (-36.48) | 09:30 open · cash $18,516.69 (unchanged overnight, no fees) · equity $9,567.03 vs prior close $9,603.51 (-36.48) · 6 name(s) re-marked at the open (per-name table). GFR×116 yday $6.66 → 09:30 $6.64 +2.32; BBNX×88 yday $21.43 → 09:30 $21.30 +11.44; TRMD×45 yday $36.63 → 09:30 $37.71 -48.60; BULL×198 yday $7.71 → 09:30 $7.85 -27.72; LEN×19 yday $79.70 → 09:30 $78.25 +27.55; BWIN×49 yday $31.95 → 09:30 $31.98 -1.47 | — |
| 2026-09-18 09:30 ET | **COVER** | `GFR` | 116 | $6.64 | $2.34 | $-56.92 | $17,744.11 | ▼ -56.92 after sell → book $9,564.69; vs 09:30 mark -2.34 | dropped from list after 5 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `BBNX` | 88 | $21.30 | $2.25 | $-241.30 | $15,867.45 | ▼ -241.30 after sell → book $9,562.43; vs 09:30 mark -2.26 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `TRMD` | 45 | $37.71 | $2.12 | $-85.77 | $14,168.38 | ▼ -85.77 after sell → book $9,560.31; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `BULL` | 198 | $7.85 | $2.58 | $+14.54 | $12,611.50 | ▲ +14.54 after sell → book $9,557.73; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `LEN` | 19 | $78.25 | $2.05 | $+48.09 | $11,122.70 | ▲ +48.09 after sell → book $9,555.68; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SHORT** | `PSKY` | 451 | $10.59 | $6.06 | — | $15,892.73 | — | news🔴; gate news=bad; list oppset; ret5=-4.0; leftover $4777.84 | join🟢 sector🟢 gen🟢 news🔴 digest🔴 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,892.73 | ▲ close $9,723.45 vs 09:30 $9,567.03 (session +173.83) | 16:00 close · cash $15,892.73 · equity $9,723.45 vs 09:30 $9,567.03 (+156.42; session marks +173.83) · 2 name(s) marked open→close (per-name table). BWIN×49 09:30 $31.98 → close $31.93 +2.45; PSKY×451 09:30 $10.59 → close $10.21 +171.38 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `RNW` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SVV` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `LUNR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ALH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RSKD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OPLN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TXNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BILI` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LNG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INGM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LFST` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `BBD` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `BWIN` | 49 | 2026-09-17 @ $32.06 | news🔴; gate news=bad; list oppset; ret5=-3.7; leftover $1578.01 |
| `PSKY` | 451 | 2026-09-18 @ $10.59 | news🔴; gate news=bad; list oppset; ret5=-4.0; leftover $4777.84 |
