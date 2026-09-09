# Factor mine action — `short_news_r_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · news🔴

Cash book **-0.42%** ($9,958) · signal-only (no cash/fees) was +5.63%. Starts YES **15/19**. Fills 63 · skips 22 · realized $-50.93.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,591.12.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `EU` | 1412 | — | $1.18 | +0.00 | $1.21 | -42.36 | -42.36 | -0.00 | -42.36 |
| 2026-08-14 | `LUNR` | 86 | — | $19.17 | +0.00 | $19.01 | +13.76 | +13.76 | -0.00 | +13.76 |
| 2026-08-14 | `OWL` | 131 | — | $12.70 | +0.00 | $12.22 | +62.22 | +62.22 | -0.00 | +62.22 |
| 2026-08-17 | `EU` | 1412 | $1.21 | $1.21 | +0.00 | — | +0.00 | +0.00 | -42.36 | — |
| 2026-08-17 | `LUNR` | 86 | $19.01 | $20.25 | -106.64 | — | +0.00 | -106.64 | -92.88 | — |
| 2026-08-17 | `OWL` | 131 | $12.22 | $12.12 | +13.10 | — | +0.00 | +13.10 | +75.33 | — |
| 2026-08-17 | `VERI` | 1075 | — | $1.15 | +0.00 | $1.08 | +69.87 | +69.87 | -0.00 | +69.87 |
| 2026-08-17 | `ZNTL` | 347 | — | $3.56 | +0.00 | $3.71 | -50.32 | -50.32 | -0.00 | -50.32 |
| 2026-08-17 | `APMD` | 39 | — | $31.70 | +0.00 | $32.55 | -33.15 | -33.15 | -0.00 | -33.15 |
| 2026-08-17 | `HIVE` | 410 | — | $3.01 | +0.00 | $3.07 | -24.60 | -24.60 | -0.00 | -24.60 |
| 2026-08-18 | `VERI` | 1075 | $1.08 | $1.05 | +37.62 | — | +0.00 | +37.62 | +107.50 | — |
| 2026-08-18 | `ZNTL` | 347 | $3.71 | $3.75 | -15.61 | — | +0.00 | -15.61 | -65.93 | — |
| 2026-08-18 | `APMD` | 39 | $32.55 | $32.85 | -11.70 | — | +0.00 | -11.70 | -44.85 | — |
| 2026-08-18 | `HIVE` | 410 | $3.07 | $2.96 | +45.10 | — | +0.00 | +45.10 | +20.50 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AEM` | 3 | — | $204.45 | +0.00 | $212.04 | -22.77 | -22.77 | -0.00 | -22.77 |
| 2026-08-20 | `WYFI` | 28 | — | $21.40 | +0.00 | $21.16 | +6.72 | +6.72 | -0.00 | +6.72 |
| 2026-08-20 | `TOYO` | 139 | — | $4.43 | +0.00 | $4.51 | -11.81 | -11.81 | -0.00 | -11.81 |
| 2026-08-20 | `ABCL` | 52 | — | $11.81 | +0.00 | $11.57 | +12.74 | +12.74 | -0.00 | +12.74 |
| 2026-08-20 | `TEAM` | 3 | — | $173.90 | +0.00 | $174.91 | -3.03 | -3.03 | -0.00 | -3.03 |
| 2026-08-20 | `AAP` | 13 | — | $46.85 | +0.00 | $42.39 | +57.98 | +57.98 | -0.00 | +57.98 |
| 2026-08-20 | `WMT` | 5 | — | $106.13 | +0.00 | $103.59 | +12.67 | +12.67 | -0.00 | +12.67 |
| 2026-08-20 | `AQST` | 133 | — | $4.61 | +0.00 | $4.50 | +15.30 | +15.30 | -0.00 | +15.30 |
| 2026-08-21 | `AEM` | 3 | $212.04 | $216.30 | -12.78 | — | +0.00 | -12.78 | -35.55 | — |
| 2026-08-21 | `WYFI` | 28 | $21.16 | $21.54 | -10.64 | — | +0.00 | -10.64 | -3.92 | — |
| 2026-08-21 | `TOYO` | 139 | $4.51 | $4.68 | -22.94 | — | +0.00 | -22.94 | -34.75 | — |
| 2026-08-21 | `ABCL` | 52 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | +12.74 | — |
| 2026-08-21 | `TEAM` | 3 | $174.91 | $174.22 | +2.07 | — | +0.00 | +2.07 | -0.96 | — |
| 2026-08-21 | `AAP` | 13 | $42.39 | $42.41 | -0.26 | — | +0.00 | -0.26 | +57.72 | — |
| 2026-08-21 | `WMT` | 5 | $103.59 | $103.69 | -0.49 | — | +0.00 | -0.49 | +12.18 | — |
| 2026-08-21 | `AQST` | 133 | $4.50 | $4.54 | -5.98 | — | +0.00 | -5.98 | +9.31 | — |
| 2026-08-21 | `QTRX` | 263 | — | $3.11 | +0.00 | $2.99 | +31.56 | +31.56 | -0.00 | +31.56 |
| 2026-08-21 | `MRNA` | 6 | — | $133.11 | +0.00 | $145.13 | -72.12 | -72.12 | -0.00 | -72.12 |
| 2026-08-21 | `AUGO` | 9 | — | $89.10 | +0.00 | $87.26 | +16.56 | +16.56 | -0.00 | +16.56 |
| 2026-08-21 | `SSRM` | 21 | — | $38.40 | +0.00 | $37.77 | +13.23 | +13.23 | -0.00 | +13.23 |
| 2026-08-21 | `ARIS` | 39 | — | $20.90 | +0.00 | $20.86 | +1.56 | +1.56 | -0.00 | +1.56 |
| 2026-08-21 | `NOG` | 30 | — | $27.00 | +0.00 | $27.34 | -10.20 | -10.20 | -0.00 | -10.20 |
| 2026-08-24 | `QTRX` | 263 | $2.99 | $2.98 | +2.63 | — | +0.00 | +2.63 | +34.19 | — |
| 2026-08-24 | `MRNA` | 6 | $145.13 | $142.70 | +14.58 | — | +0.00 | +14.58 | -57.54 | — |
| 2026-08-24 | `AUGO` | 9 | $87.26 | $89.87 | -23.49 | — | +0.00 | -23.49 | -6.93 | — |
| 2026-08-24 | `SSRM` | 21 | $37.77 | $38.48 | -14.91 | — | +0.00 | -14.91 | -1.68 | — |
| 2026-08-24 | `ARIS` | 39 | $20.86 | $20.98 | -4.68 | — | +0.00 | -4.68 | -3.12 | — |
| 2026-08-24 | `NOG` | 30 | $27.34 | $27.09 | +7.50 | $26.49 | +18.00 | +25.50 | -2.70 | +15.30 |
| 2026-08-25 | `NOG` | 30 | $26.49 | $26.10 | +11.70 | $26.50 | -12.00 | -0.30 | +27.00 | +15.00 |
| 2026-08-25 | `BMO` | 14 | — | $172.40 | +0.00 | $175.00 | -36.40 | -36.40 | -0.00 | -36.40 |
| 2026-08-25 | `AVAH` | 178 | — | $13.70 | +0.00 | $13.70 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-08-26 | `NOG` | 30 | $26.50 | $26.50 | +0.00 | $26.50 | +0.00 | +0.00 | +15.00 | +15.00 |
| 2026-08-26 | `BMO` | 14 | $175.00 | $175.00 | +0.00 | $175.00 | +0.00 | +0.00 | -36.40 | -36.40 |
| 2026-08-26 | `AVAH` | 178 | $13.70 | $13.70 | +0.00 | $13.70 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-08-27 | `NOG` | 30 | $26.50 | $26.00 | +15.00 | — | +0.00 | +15.00 | +30.00 | — |
| 2026-08-27 | `BMO` | 14 | $175.00 | $173.22 | +24.92 | — | +0.00 | +24.92 | -11.48 | — |
| 2026-08-27 | `AVAH` | 178 | $13.70 | $13.65 | +8.90 | — | +0.00 | +8.90 | +8.90 | — |
| 2026-08-28 | `SIMO` | 9 | — | $272.00 | +0.00 | $255.08 | +152.28 | +152.28 | -0.00 | +152.28 |
| 2026-08-28 | `NOG` | 95 | — | $25.73 | +0.00 | $26.08 | -33.25 | -33.25 | -0.00 | -33.25 |
| 2026-08-31 | `SIMO` | 9 | $255.08 | $246.79 | +74.61 | — | +0.00 | +74.61 | +226.89 | — |
| 2026-08-31 | `NOG` | 95 | $26.08 | $25.73 | +33.25 | $25.73 | +0.00 | +33.25 | -0.00 | -0.00 |
| 2026-09-01 | `NOG` | 95 | $25.73 | $26.36 | -59.85 | — | +0.00 | -59.85 | -59.85 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `SLN` | 112 | — | $14.70 | +0.00 | $14.79 | -10.08 | -10.08 | -0.00 | -10.08 |
| 2026-09-03 | `NIQ` | 89 | — | $18.60 | +0.00 | $18.35 | +22.25 | +22.25 | -0.00 | +22.25 |
| 2026-09-03 | `NOG` | 63 | — | $26.10 | +0.00 | $26.60 | -31.50 | -31.50 | -0.00 | -31.50 |
| 2026-09-04 | `SLN` | 112 | $14.79 | $14.85 | -6.72 | — | +0.00 | -6.72 | -16.80 | — |
| 2026-09-04 | `NIQ` | 89 | $18.35 | $18.66 | -27.59 | — | +0.00 | -27.59 | -5.34 | — |
| 2026-09-04 | `NOG` | 63 | $26.60 | $26.59 | +0.63 | $25.89 | +44.10 | +44.73 | -30.87 | +13.23 |
| 2026-09-04 | `OPK` | 964 | — | $1.71 | +0.00 | $1.61 | +96.40 | +96.40 | -0.00 | +96.40 |
| 2026-09-04 | `GSM` | 362 | — | $4.55 | +0.00 | $4.53 | +7.24 | +7.24 | -0.00 | +7.24 |
| 2026-09-04 | `PIPR` | 21 | — | $76.47 | +0.00 | $77.75 | -26.88 | -26.88 | -0.00 | -26.88 |
| 2026-09-07 | `NOG` | 63 | $25.89 | $25.73 | +10.08 | $25.92 | -11.97 | -1.89 | +23.31 | +11.34 |
| 2026-09-07 | `OPK` | 964 | $1.61 | $1.59 | +19.28 | — | +0.00 | +19.28 | +115.68 | — |
| 2026-09-07 | `GSM` | 362 | $4.53 | $4.67 | -50.68 | — | +0.00 | -50.68 | -43.44 | — |
| 2026-09-07 | `PIPR` | 21 | $77.75 | $76.55 | +25.20 | — | +0.00 | +25.20 | -1.68 | — |
| 2026-09-07 | `GEMI` | 1068 | — | $4.67 | +0.00 | $4.68 | -10.68 | -10.68 | -0.00 | -10.68 |
| 2026-09-08 | `NOG` | 63 | $25.92 | $25.92 | +0.00 | $25.92 | +0.00 | +0.00 | +11.34 | +11.34 |
| 2026-09-08 | `GEMI` | 1068 | $4.68 | $4.65 | +32.04 | — | +0.00 | +32.04 | +21.36 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +33.62 | EU, LUNR, OWL | — | $14,954.53 | $10,010.33 | EU×1412, LUNR×86, OWL×131 |
| 2026-08-17 | +2.25 | $14,954.53 | EU×1412, LUNR×86, OWL×131 | $9,916.79 | -93.54 | -38.20 | VERI, ZNTL, APMD, HIVE | EU, LUNR, OWL | $14,809.69 | $9,829.53 | VERI×1075, ZNTL×347, APMD×39, HIVE×410 |
| 2026-08-18 | -6.20 | $14,809.69 | VERI×1075, ZNTL×347, APMD×39, HIVE×410 | $9,884.94 | +55.41 | +0.00 | — | VERI, ZNTL, APMD, HIVE | $9,859.20 | $9,859.20 | — |
| 2026-08-19 | -7.20 | $9,859.20 | — | $9,859.20 | -0.00 | +0.00 | — | — | $9,859.20 | $9,859.20 | — |
| 2026-08-20 | +1.12 | $9,859.20 | — | $9,859.20 | -0.00 | +67.80 | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | — | $14,559.04 | $9,909.62 | AEM×3, WYFI×28, TOYO×139, ABCL×52, TEAM×3, AAP×13, WMT×5, AQST×133 |
| 2026-08-21 | +3.25 | $14,559.04 | AEM×3, WYFI×28, TOYO×139, ABCL×52, TEAM×3, AAP×13, WMT×5, AQST×133 | $9,858.60 | -51.02 | -19.41 | QTRX, MRNA, AUGO, SSRM, ARIS, NOG | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | $14,677.60 | $9,808.20 | QTRX×263, MRNA×6, AUGO×9, SSRM×21, ARIS×39, NOG×30 |
| 2026-08-24 | -5.17 | $14,677.60 | QTRX×263, MRNA×6, AUGO×9, SSRM×21, ARIS×39, NOG×30 | $9,789.83 | -18.37 | +18.00 | — | QTRX, MRNA, AUGO, SSRM, ARIS | $10,590.95 | $9,796.25 | NOG×30 |
| 2026-08-25 | +1.80 | $10,590.95 | NOG×30 | $9,807.95 | +11.70 | -48.40 | BMO, AVAH | — | $15,438.39 | $9,754.79 | NOG×30, BMO×14, AVAH×178 |
| 2026-08-26 | +2.02 | $15,438.39 | NOG×30, BMO×14, AVAH×178 | $9,754.79 | -0.00 | +0.00 | — | — | $15,438.39 | $9,754.79 | NOG×30, BMO×14, AVAH×178 |
| 2026-08-27 | — | $15,438.39 | NOG×30, BMO×14, AVAH×178 | $9,803.61 | +48.82 | +0.00 | — | NOG, BMO, AVAH | $9,796.97 | $9,796.97 | — |
| 2026-08-28 | +0.75 | $9,796.97 | — | $9,796.97 | +0.00 | +119.03 | SIMO, NOG | — | $14,684.83 | $9,911.51 | SIMO×9, NOG×95 |
| 2026-08-31 | -5.85 | $14,684.83 | SIMO×9, NOG×95 | $10,019.37 | +107.86 | +0.00 | — | SIMO | $12,461.70 | $10,017.35 | NOG×95 |
| 2026-09-01 | -6.30 | $12,461.70 | NOG×95 | $9,957.50 | -59.85 | +0.00 | — | NOG | $9,955.23 | $9,955.23 | — |
| 2026-09-02 | -3.83 | $9,955.23 | — | $9,955.23 | -0.00 | +0.00 | — | — | $9,955.23 | $9,955.23 | — |
| 2026-09-03 | -0.90 | $9,955.23 | — | $9,955.23 | -0.00 | -19.33 | SLN, NIQ, NOG | — | $14,894.35 | $9,928.92 | SLN×112, NIQ×89, NOG×63 |
| 2026-09-04 | +2.25 | $14,894.35 | SLN×112, NIQ×89, NOG×63 | $9,895.24 | -33.68 | +120.86 | OPK, GSM, PIPR | SLN, NIQ | $16,447.67 | $9,991.95 | NOG×63, OPK×964, GSM×362, PIPR×21 |
| 2026-09-07 | — | $16,447.67 | NOG×63, OPK×964, GSM×362, PIPR×21 | $9,995.83 | +3.88 | -22.65 | GEMI | OPK, GSM, PIPR | $16,571.09 | $9,939.89 | NOG×63, GEMI×1068 |
| 2026-09-08 | -11.47 | $16,571.09 | NOG×63, GEMI×1068 | $9,971.93 | +32.04 | +0.00 | — | GEMI | $11,591.12 | $9,958.16 | NOG×63 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1412 | $1.18 | $18.51 | — | $11,647.65 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 86 | $19.17 | $2.32 | — | $13,293.95 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 131 | $12.70 | $2.46 | — | $14,954.53 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,954.53 | ▲ close $10,010.33 vs 09:30 $10,000.00 (session +33.62) | 16:00 close · cash $14,954.53 · equity $10,010.33 vs 09:30 $10,000.00 (+10.33; session marks +33.62) · 3 name(s) marked open→close (per-name table). EU×1412 09:30 $1.18 → close $1.21 -42.36; LUNR×86 09:30 $19.17 → close $19.01 +13.76; OWL×131 09:30 $12.70 → close $12.22 +62.22 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,954.53 | ▼ 09:30 equity $9,916.79 vs yday $10,010.33 (-93.54) | 09:30 open · cash $14,954.53 (unchanged overnight, no fees) · equity $9,916.79 vs prior close $10,010.33 (-93.54) · 3 name(s) re-marked at the open (per-name table). EU×1412 yday $1.21 → 09:30 $1.21 -0.00; LUNR×86 yday $19.01 → 09:30 $20.25 -106.64; OWL×131 yday $12.22 → 09:30 $12.12 +13.10 | — |
| 2026-08-17 09:30 ET | **COVER** | `EU` | 1412 | $1.21 | $18.21 | $-79.08 | $13,227.80 | ▼ -79.08 after sell → book $9,898.58; vs 09:30 mark -18.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `LUNR` | 86 | $20.25 | $2.25 | $-97.45 | $11,484.05 | ▼ -97.45 after sell → book $9,896.33; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **COVER** | `OWL` | 131 | $12.12 | $2.38 | $+70.48 | $9,893.95 | ▲ +70.48 after sell → book $9,893.95; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 1075 | $1.15 | $14.09 | — | $11,116.11 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; leftover $1236.74 | join🟡 sector🟢 gen🟢 news🔴 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 347 | $3.56 | $4.58 | — | $12,346.85 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; leftover $1236.74 | join🟡 sector🔴 gen🟢 news🔴 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 39 | $31.70 | $2.16 | — | $13,580.99 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; leftover $1236.74 | join🟡 sector🔴 gen🟢 news🔴 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 410 | $3.01 | $5.40 | — | $14,809.69 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; leftover $1236.74 | join🟢 sector🟢 gen🟢 news🔴 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,809.69 | ▼ close $9,829.53 vs 09:30 $9,916.79 (session -38.20) | 16:00 close · cash $14,809.69 · equity $9,829.53 vs 09:30 $9,916.79 (-87.26; session marks -38.20) · 4 name(s) marked open→close (per-name table). VERI×1075 09:30 $1.15 → close $1.08 +69.87; ZNTL×347 09:30 $3.56 → close $3.71 -50.32; APMD×39 09:30 $31.70 → close $32.55 -33.15; HIVE×410 09:30 $3.01 → close $3.07 -24.60 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,809.69 | ▲ 09:30 equity $9,884.94 vs yday $9,829.53 (+55.41) | 09:30 open · cash $14,809.69 (unchanged overnight, no fees) · equity $9,884.94 vs prior close $9,829.53 (+55.41) · 4 name(s) re-marked at the open (per-name table). VERI×1075 yday $1.08 → 09:30 $1.05 +37.62; ZNTL×347 yday $3.71 → 09:30 $3.75 -15.61; APMD×39 yday $32.55 → 09:30 $32.85 -11.70; HIVE×410 yday $3.07 → 09:30 $2.96 +45.10 | — |
| 2026-08-18 09:30 ET | **COVER** | `VERI` | 1075 | $1.05 | $13.87 | $+79.54 | $13,667.07 | ▲ +79.54 after sell → book $9,871.07; vs 09:30 mark -13.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `ZNTL` | 347 | $3.75 | $4.48 | $-74.98 | $12,361.34 | ▼ -74.98 after sell → book $9,866.59; vs 09:30 mark -4.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `APMD` | 39 | $32.85 | $2.11 | $-49.12 | $11,078.09 | ▼ -49.12 after sell → book $9,864.49; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `HIVE` | 410 | $2.96 | $5.29 | $+9.81 | $9,859.20 | ▲ +9.81 after sell → book $9,859.20; vs 09:30 mark -5.29 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,859.20 | ▲ close $9,859.20 vs 09:30 $9,884.94 (session +0.00) | 16:00 close · cash $9,859.20 · no lots left · equity $9,859.20. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,859.20 | ▲ 09:30 equity $9,859.20 vs yday $9,859.20 (-0.00) | 09:30 open · cash $9,859.20 · no holdings · equity $9,859.20 vs prior close $9,859.20 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,859.20 | ▲ close $9,859.20 vs 09:30 $9,859.20 (session +0.00) | 16:00 close · cash $9,859.20 · no lots left · equity $9,859.20. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,859.20 | ▲ 09:30 equity $9,859.20 vs yday $9,859.20 (-0.00) | 09:30 open · cash $9,859.20 · no holdings · equity $9,859.20 vs prior close $9,859.20 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $10,470.51 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $616.20 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 28 | $21.40 | $2.11 | — | $11,067.60 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; leftover $616.20 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 139 | $4.43 | $2.46 | — | $11,680.91 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; leftover $616.20 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 52 | $11.81 | $2.18 | — | $12,293.11 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $616.20 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $12,812.78 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; leftover $616.20 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $13,419.76 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; leftover $616.20 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 5 | $106.13 | $2.04 | — | $13,948.35 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; leftover $616.20 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 133 | $4.61 | $2.44 | — | $14,559.04 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $616.20 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,559.04 | ▲ close $9,909.62 vs 09:30 $9,859.20 (session +67.80) | 16:00 close · cash $14,559.04 · equity $9,909.62 vs 09:30 $9,859.20 (+50.42; session marks +67.80) · 8 name(s) marked open→close (per-name table). AEM×3 09:30 $204.45 → close $212.04 -22.77; WYFI×28 09:30 $21.40 → close $21.16 +6.72; TOYO×139 09:30 $4.43 → close $4.51 -11.81; ABCL×52 09:30 $11.81 → close $11.57 +12.74; TEAM×3 09:30 $173.90 → close $174.91 -3.03; AAP×13 09:30 $46.85 → close $42.39 +57.98; WMT×5 09:30 $106.13 → close $103.59 +12.67; AQST×133 09:30 $4.61 → close $4.50 +15.30 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,559.04 | ▼ 09:30 equity $9,858.60 vs yday $9,909.62 (-51.02) | 09:30 open · cash $14,559.04 (unchanged overnight, no fees) · equity $9,858.60 vs prior close $9,909.62 (-51.02) · 8 name(s) re-marked at the open (per-name table). AEM×3 yday $212.04 → 09:30 $216.30 -12.78; WYFI×28 yday $21.16 → 09:30 $21.54 -10.64; TOYO×139 yday $4.51 → 09:30 $4.68 -22.94; ABCL×52 yday $11.57 → 09:30 $11.57 -0.00; TEAM×3 yday $174.91 → 09:30 $174.22 +2.07; AAP×13 yday $42.39 → 09:30 $42.41 -0.26; WMT×5 yday $103.59 → 09:30 $103.69 -0.49; AQST×133 yday $4.50 → 09:30 $4.54 -5.98 | — |
| 2026-08-21 09:30 ET | **COVER** | `AEM` | 3 | $216.30 | $2.00 | $-39.58 | $13,908.15 | ▼ -39.58 after sell → book $9,856.61; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `WYFI` | 28 | $21.54 | $2.07 | $-8.10 | $13,302.95 | ▼ -8.10 after sell → book $9,854.53; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `TOYO` | 139 | $4.68 | $2.41 | $-39.61 | $12,650.02 | ▼ -39.61 after sell → book $9,852.12; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `ABCL` | 52 | $11.57 | $2.15 | $+8.41 | $12,046.24 | ▲ +8.41 after sell → book $9,849.98; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `TEAM` | 3 | $174.22 | $2.00 | $-4.99 | $11,521.58 | ▼ -4.99 after sell → book $9,847.98; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `AAP` | 13 | $42.41 | $2.03 | $+53.63 | $10,968.22 | ▲ +53.63 after sell → book $9,845.95; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `WMT` | 5 | $103.69 | $2.00 | $+8.13 | $10,447.77 | ▲ +8.13 after sell → book $9,843.95; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `AQST` | 133 | $4.54 | $2.39 | $+4.48 | $9,841.56 | ▲ +4.48 after sell → book $9,841.56; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 263 | $3.11 | $3.47 | — | $10,656.02 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $820.13 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 6 | $133.11 | $2.05 | — | $11,452.63 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; leftover $820.13 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 9 | $89.10 | $2.06 | — | $12,252.47 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $820.13 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 21 | $38.40 | $2.10 | — | $13,056.77 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $820.13 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 39 | $20.90 | $2.15 | — | $13,869.72 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $820.13 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 30 | $27.00 | $2.12 | — | $14,677.60 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; leftover $820.13 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,677.60 | ▼ close $9,808.20 vs 09:30 $9,858.60 (session -19.41) | 16:00 close · cash $14,677.60 · equity $9,808.20 vs 09:30 $9,858.60 (-50.40; session marks -19.41) · 6 name(s) marked open→close (per-name table). QTRX×263 09:30 $3.11 → close $2.99 +31.56; MRNA×6 09:30 $133.11 → close $145.13 -72.12; AUGO×9 09:30 $89.10 → close $87.26 +16.56; SSRM×21 09:30 $38.40 → close $37.77 +13.23; ARIS×39 09:30 $20.90 → close $20.86 +1.56; NOG×30 09:30 $27.00 → close $27.34 -10.20 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,677.60 | ▼ 09:30 equity $9,789.83 vs yday $9,808.20 (-18.37) | 09:30 open · cash $14,677.60 (unchanged overnight, no fees) · equity $9,789.83 vs prior close $9,808.20 (-18.37) · 6 name(s) re-marked at the open (per-name table). QTRX×263 yday $2.99 → 09:30 $2.98 +2.63; MRNA×6 yday $145.13 → 09:30 $142.70 +14.58; AUGO×9 yday $87.26 → 09:30 $89.87 -23.49; SSRM×21 yday $37.77 → 09:30 $38.48 -14.91; ARIS×39 yday $20.86 → 09:30 $20.98 -4.68; NOG×30 yday $27.34 → 09:30 $27.09 +7.50 | — |
| 2026-08-24 09:30 ET | **COVER** | `QTRX` | 263 | $2.98 | $3.39 | $+27.33 | $13,890.47 | ▲ +27.33 after sell → book $9,786.44; vs 09:30 mark -3.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `MRNA` | 6 | $142.70 | $2.01 | $-61.60 | $13,032.26 | ▼ -61.60 after sell → book $9,784.43; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AUGO` | 9 | $89.87 | $2.02 | $-11.01 | $12,221.41 | ▼ -11.01 after sell → book $9,782.41; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `SSRM` | 21 | $38.48 | $2.05 | $-5.83 | $11,411.28 | ▼ -5.83 after sell → book $9,780.36; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `ARIS` | 39 | $20.98 | $2.11 | $-7.38 | $10,590.95 | ▼ -7.38 after sell → book $9,778.25; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,590.95 | ▲ close $9,796.25 vs 09:30 $9,789.83 (session +18.00) | 16:00 close · cash $10,590.95 · equity $9,796.25 vs 09:30 $9,789.83 (+6.42; session marks +18.00) · 1 name(s) marked open→close (per-name table). NOG×30 09:30 $27.09 → close $26.49 +18.00 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,590.95 | ▲ 09:30 equity $9,807.95 vs yday $9,796.25 (+11.70) | 09:30 open · cash $10,590.95 (unchanged overnight, no fees) · equity $9,807.95 vs prior close $9,796.25 (+11.70) · 1 name(s) re-marked at the open (per-name table). NOG×30 yday $26.49 → 09:30 $26.10 +11.70 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 14 | $172.40 | $2.13 | — | $13,002.43 | — | news🔴; gate news=bad; list earn_react; ret5=-6.1; leftover $2451.99 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 178 | $13.70 | $2.64 | — | $15,438.39 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+6.8; leftover $2451.99 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,438.39 | ▼ close $9,754.79 vs 09:30 $9,807.95 (session -48.40) | 16:00 close · cash $15,438.39 · equity $9,754.79 vs 09:30 $9,807.95 (-53.16; session marks -48.40) · 3 name(s) marked open→close (per-name table). NOG×30 09:30 $26.10 → close $26.50 -12.00; BMO×14 09:30 $172.40 → close $175.00 -36.40; AVAH×178 09:30 $13.70 → close $13.70 -0.00 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,438.39 | ▲ 09:30 equity $9,754.79 vs yday $9,754.79 (-0.00) | 09:30 open · cash $15,438.39 (unchanged overnight, no fees) · equity $9,754.79 vs prior close $9,754.79 (-0.00) · 3 name(s) re-marked at the open (per-name table). NOG×30 yday $26.50 → 09:30 $26.50 -0.00; BMO×14 yday $175.00 → 09:30 $175.00 -0.00; AVAH×178 yday $13.70 → 09:30 $13.70 -0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,438.39 | ▲ close $9,754.79 vs 09:30 $9,754.79 (session +0.00) | 16:00 close · cash $15,438.39 · equity $9,754.79 vs 09:30 $9,754.79 (-0.00; session marks +0.00) · 3 name(s) marked open→close (per-name table). NOG×30 09:30 $26.50 → close $26.50 -0.00; BMO×14 09:30 $175.00 → close $175.00 -0.00; AVAH×178 09:30 $13.70 → close $13.70 -0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,438.39 | ▲ 09:30 equity $9,803.61 vs yday $9,754.79 (+48.82) | 09:30 open · cash $15,438.39 (unchanged overnight, no fees) · equity $9,803.61 vs prior close $9,754.79 (+48.82) · 3 name(s) re-marked at the open (per-name table). NOG×30 yday $26.50 → 09:30 $26.00 +15.00; BMO×14 yday $175.00 → 09:30 $173.22 +24.92; AVAH×178 yday $13.70 → 09:30 $13.65 +8.90 | — |
| 2026-08-27 09:30 ET | **COVER** | `NOG` | 30 | $26.00 | $2.08 | $+25.80 | $14,656.31 | ▲ +25.80 after sell → book $9,801.53; vs 09:30 mark -2.08 | dropped from list after 4 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `BMO` | 14 | $173.22 | $2.03 | $-15.64 | $12,229.20 | ▼ -15.64 after sell → book $9,799.50; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `AVAH` | 178 | $13.65 | $2.52 | $+3.74 | $9,796.97 | ▲ +3.74 after sell → book $9,796.97; vs 09:30 mark -2.53 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,796.97 | ▲ close $9,796.97 vs 09:30 $9,803.61 (session +0.00) | 16:00 close · cash $9,796.97 · no lots left · equity $9,796.97. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,796.97 | ▲ 09:30 equity $9,796.97 vs yday $9,796.97 (+0.00) | 09:30 open · cash $9,796.97 · no holdings · equity $9,796.97 vs prior close $9,796.97 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 9 | $272.00 | $2.11 | — | $12,242.86 | — | news🔴; gate news=bad; list yday_gainer; ⚪; ret5=-3.9; leftover $2449.24 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `NOG` | 95 | $25.73 | $2.38 | — | $14,684.83 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.6; leftover $2449.24 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,684.83 | ▲ close $9,911.51 vs 09:30 $9,796.97 (session +119.03) | 16:00 close · cash $14,684.83 · equity $9,911.51 vs 09:30 $9,796.97 (+114.54; session marks +119.03) · 2 name(s) marked open→close (per-name table). SIMO×9 09:30 $272.00 → close $255.08 +152.28; NOG×95 09:30 $25.73 → close $26.08 -33.25 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,684.83 | ▲ 09:30 equity $10,019.37 vs yday $9,911.51 (+107.86) | 09:30 open · cash $14,684.83 (unchanged overnight, no fees) · equity $10,019.37 vs prior close $9,911.51 (+107.86) · 2 name(s) re-marked at the open (per-name table). SIMO×9 yday $255.08 → 09:30 $246.79 +74.61; NOG×95 yday $26.08 → 09:30 $25.73 +33.25 | — |
| 2026-08-31 09:30 ET | **COVER** | `SIMO` | 9 | $246.79 | $2.02 | $+222.76 | $12,461.70 | ▲ +222.76 after sell → book $10,017.35; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,461.70 | ▲ close $10,017.35 vs 09:30 $10,019.37 (session +0.00) | 16:00 close · cash $12,461.70 · equity $10,017.35 vs 09:30 $10,019.37 (-2.02; session marks +0.00) · 1 name(s) marked open→close (per-name table). NOG×95 09:30 $25.73 → close $25.73 -0.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,461.70 | ▼ 09:30 equity $9,957.50 vs yday $10,017.35 (-59.85) | 09:30 open · cash $12,461.70 (unchanged overnight, no fees) · equity $9,957.50 vs prior close $10,017.35 (-59.85) · 1 name(s) re-marked at the open (per-name table). NOG×95 yday $25.73 → 09:30 $26.36 -59.85 | — |
| 2026-09-01 09:30 ET | **COVER** | `NOG` | 95 | $26.36 | $2.27 | $-64.50 | $9,955.23 | ▼ -64.50 after sell → book $9,955.23; vs 09:30 mark -2.27 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,955.23 | ▲ close $9,955.23 vs 09:30 $9,957.50 (session +0.00) | 16:00 close · cash $9,955.23 · no lots left · equity $9,955.23. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,955.23 | ▲ 09:30 equity $9,955.23 vs yday $9,955.23 (-0.00) | 09:30 open · cash $9,955.23 · no holdings · equity $9,955.23 vs prior close $9,955.23 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,955.23 | ▲ close $9,955.23 vs 09:30 $9,955.23 (session +0.00) | 16:00 close · cash $9,955.23 · no lots left · equity $9,955.23. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,955.23 | ▲ 09:30 equity $9,955.23 vs yday $9,955.23 (-0.00) | 09:30 open · cash $9,955.23 · no holdings · equity $9,955.23 vs prior close $9,955.23 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 112 | $14.70 | $2.40 | — | $11,599.23 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1659.20 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `NIQ` | 89 | $18.60 | $2.33 | — | $13,252.30 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+7.6; leftover $1659.20 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `NOG` | 63 | $26.10 | $2.25 | — | $14,894.35 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.6; leftover $1659.20 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,894.35 | ▼ close $9,928.92 vs 09:30 $9,955.23 (session -19.33) | 16:00 close · cash $14,894.35 · equity $9,928.92 vs 09:30 $9,955.23 (-26.31; session marks -19.33) · 3 name(s) marked open→close (per-name table). SLN×112 09:30 $14.70 → close $14.79 -10.08; NIQ×89 09:30 $18.60 → close $18.35 +22.25; NOG×63 09:30 $26.10 → close $26.60 -31.50 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,894.35 | ▼ 09:30 equity $9,895.24 vs yday $9,928.92 (-33.68) | 09:30 open · cash $14,894.35 (unchanged overnight, no fees) · equity $9,895.24 vs prior close $9,928.92 (-33.68) · 3 name(s) re-marked at the open (per-name table). SLN×112 yday $14.79 → 09:30 $14.85 -6.72; NIQ×89 yday $18.35 → 09:30 $18.66 -27.59; NOG×63 yday $26.60 → 09:30 $26.59 +0.63 | — |
| 2026-09-04 09:30 ET | **COVER** | `SLN` | 112 | $14.85 | $2.33 | $-21.53 | $13,228.82 | ▼ -21.53 after sell → book $9,892.91; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `NIQ` | 89 | $18.66 | $2.26 | $-9.93 | $11,565.82 | ▼ -9.93 after sell → book $9,890.65; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `OPK` | 964 | $1.71 | $12.65 | — | $13,201.61 | — | news🔴; gate news=bad; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.2; leftover $1648.44 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 362 | $4.55 | $4.79 | — | $14,843.92 | — | news🔴; gate news=bad; list yday_gainer; ret5=-7.1; leftover $1648.44 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 21 | $76.47 | $2.12 | — | $16,447.67 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=-1.9; leftover $1648.44 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,447.67 | ▲ close $9,991.95 vs 09:30 $9,895.24 (session +120.86) | 16:00 close · cash $16,447.67 · equity $9,991.95 vs 09:30 $9,895.24 (+96.71; session marks +120.86) · 4 name(s) marked open→close (per-name table). NOG×63 09:30 $26.59 → close $25.89 +44.10; OPK×964 09:30 $1.71 → close $1.61 +96.40; GSM×362 09:30 $4.55 → close $4.53 +7.24; PIPR×21 09:30 $76.47 → close $77.75 -26.88 | — |
| 2026-09-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,447.67 | ▲ 09:30 equity $9,995.83 vs yday $9,991.95 (+3.88) | 09:30 open · cash $16,447.67 (unchanged overnight, no fees) · equity $9,995.83 vs prior close $9,991.95 (+3.88) · 4 name(s) re-marked at the open (per-name table). NOG×63 yday $25.89 → 09:30 $25.73 +10.08; OPK×964 yday $1.61 → 09:30 $1.59 +19.28; GSM×362 yday $4.53 → 09:30 $4.67 -50.68; PIPR×21 yday $77.75 → 09:30 $76.55 +25.20 | — |
| 2026-09-07 09:30 ET | **COVER** | `OPK` | 964 | $1.59 | $12.44 | $+90.59 | $14,902.48 | ▲ +90.59 after sell → book $9,983.40; vs 09:30 mark -12.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **COVER** | `GSM` | 362 | $4.67 | $4.67 | $-52.90 | $13,207.27 | ▼ -52.90 after sell → book $9,978.73; vs 09:30 mark -4.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **COVER** | `PIPR` | 21 | $76.55 | $2.05 | $-5.85 | $11,597.66 | ▼ -5.85 after sell → book $9,976.67; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SHORT** | `GEMI` | 1068 | $4.67 | $14.13 | — | $16,571.09 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+9.2; leftover $4988.34 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,571.09 | ▼ close $9,939.89 vs 09:30 $9,995.83 (session -22.65) | 16:00 close · cash $16,571.09 · equity $9,939.89 vs 09:30 $9,995.83 (-55.94; session marks -22.65) · 2 name(s) marked open→close (per-name table). NOG×63 09:30 $25.73 → close $25.92 -11.97; GEMI×1068 09:30 $4.67 → close $4.68 -10.68 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,571.09 | ▲ 09:30 equity $9,971.93 vs yday $9,939.89 (+32.04) | 09:30 open · cash $16,571.09 (unchanged overnight, no fees) · equity $9,971.93 vs prior close $9,939.89 (+32.04) · 2 name(s) re-marked at the open (per-name table). NOG×63 yday $25.92 → 09:30 $25.92 -0.00; GEMI×1068 yday $4.68 → 09:30 $4.65 +32.04 | — |
| 2026-09-08 09:30 ET | **COVER** | `GEMI` | 1068 | $4.65 | $13.78 | $-6.55 | $11,591.12 | ▼ -6.55 after sell → book $9,958.16; vs 09:30 mark -13.77 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,591.12 | ▲ close $9,958.16 vs 09:30 $9,971.93 (session +0.00) | 16:00 close · cash $11,591.12 · equity $9,958.16 vs 09:30 $9,971.93 (-13.77; session marks +0.00) · 1 name(s) marked open→close (per-name table). NOG×63 09:30 $25.92 → close $25.92 -0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `RNW` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `LUNR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `BMO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AVAH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BE` | no_price | no 09:30 open |
| 2026-08-26 | `SSRM` | no_price | no 09:30 open |
| 2026-08-31 | `NIQ` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ARIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CELH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NOG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARIS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `NOG` | 63 | 2026-09-03 @ $26.10 | news🔴; gate news=bad; list ohlc_hot; ret5=+11.6; leftover $1659.20 |
