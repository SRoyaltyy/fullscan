# Factor mine action — `short_news_r_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · news🔴

Cash book **+1.04%** ($10,104) · signal-only (no cash/fees) was +4.17%. Starts YES **16/18**. Fills 69 · skips 15 · realized $+30.03.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $12,549.33.

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
| 2026-08-20 | `WMT` | 5 | — | $106.38 | +0.00 | $103.84 | +12.70 | +12.70 | -0.00 | +12.70 |
| 2026-08-20 | `AQST` | 133 | — | $4.61 | +0.00 | $4.50 | +15.30 | +15.30 | -0.00 | +15.30 |
| 2026-08-21 | `AEM` | 3 | $212.04 | $216.30 | -12.78 | — | +0.00 | -12.78 | -35.55 | — |
| 2026-08-21 | `WYFI` | 28 | $21.16 | $21.54 | -10.64 | — | +0.00 | -10.64 | -3.92 | — |
| 2026-08-21 | `TOYO` | 139 | $4.51 | $4.68 | -22.94 | — | +0.00 | -22.94 | -34.75 | — |
| 2026-08-21 | `ABCL` | 52 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | +12.74 | — |
| 2026-08-21 | `TEAM` | 3 | $174.91 | $174.22 | +2.07 | — | +0.00 | +2.07 | -0.96 | — |
| 2026-08-21 | `AAP` | 13 | $42.39 | $42.41 | -0.26 | — | +0.00 | -0.26 | +57.72 | — |
| 2026-08-21 | `WMT` | 5 | $103.84 | $103.69 | +0.75 | — | +0.00 | +0.75 | +13.45 | — |
| 2026-08-21 | `AQST` | 133 | $4.50 | $4.54 | -5.98 | — | +0.00 | -5.98 | +9.31 | — |
| 2026-08-21 | `QTRX` | 263 | — | $3.11 | +0.00 | $2.99 | +31.56 | +31.56 | -0.00 | +31.56 |
| 2026-08-21 | `MRNA` | 6 | — | $133.11 | +0.00 | $145.13 | -72.12 | -72.12 | -0.00 | -72.12 |
| 2026-08-21 | `AUGO` | 9 | — | $89.10 | +0.00 | $87.26 | +16.56 | +16.56 | -0.00 | +16.56 |
| 2026-08-21 | `SSRM` | 21 | — | $38.40 | +0.00 | $37.77 | +13.23 | +13.23 | -0.00 | +13.23 |
| 2026-08-21 | `ARIS` | 39 | — | $20.90 | +0.00 | $20.86 | +1.56 | +1.56 | -0.00 | +1.56 |
| 2026-08-21 | `NOG` | 30 | — | $27.00 | +0.00 | $27.34 | -10.20 | -10.20 | -0.00 | -10.20 |
| 2026-08-24 | `QTRX` | 263 | $2.99 | $2.99 | +0.00 | — | +0.00 | +0.00 | +31.56 | — |
| 2026-08-24 | `MRNA` | 6 | $145.13 | $142.70 | +14.58 | — | +0.00 | +14.58 | -57.54 | — |
| 2026-08-24 | `AUGO` | 9 | $87.26 | $88.60 | -12.06 | — | +0.00 | -12.06 | +4.50 | — |
| 2026-08-24 | `SSRM` | 21 | $37.77 | $38.32 | -11.55 | — | +0.00 | -11.55 | +1.68 | — |
| 2026-08-24 | `ARIS` | 39 | $20.86 | $20.98 | -4.68 | — | +0.00 | -4.68 | -3.12 | — |
| 2026-08-24 | `NOG` | 30 | $27.34 | $27.12 | +6.60 | $26.84 | +8.40 | +15.00 | -3.60 | +4.80 |
| 2026-08-25 | `NOG` | 30 | $26.84 | $26.06 | +23.40 | — | +0.00 | +23.40 | +28.20 | — |
| 2026-08-25 | `AVAH` | 90 | — | $13.62 | +0.00 | $13.59 | +3.15 | +3.15 | -0.00 | +3.15 |
| 2026-08-25 | `SSRM` | 32 | — | $37.75 | +0.00 | $39.21 | -46.72 | -46.72 | -0.00 | -46.72 |
| 2026-08-25 | `ARE` | 22 | — | $54.51 | +0.00 | $52.90 | +35.42 | +35.42 | -0.00 | +35.42 |
| 2026-08-25 | `BMO` | 7 | — | $175.01 | +0.00 | $173.46 | +10.85 | +10.85 | -0.00 | +10.85 |
| 2026-08-26 | `AVAH` | 90 | $13.59 | $13.65 | -5.40 | — | +0.00 | -5.40 | -2.25 | — |
| 2026-08-26 | `SSRM` | 32 | $39.21 | $38.41 | +25.60 | — | +0.00 | +25.60 | -21.12 | — |
| 2026-08-26 | `ARE` | 22 | $52.90 | $52.77 | +2.86 | — | +0.00 | +2.86 | +38.28 | — |
| 2026-08-26 | `BMO` | 7 | $173.46 | $173.22 | +1.68 | — | +0.00 | +1.68 | +12.53 | — |
| 2026-08-26 | `BE` | 5 | — | $213.94 | +0.00 | $218.21 | -21.35 | -21.35 | -0.00 | -21.35 |
| 2026-08-26 | `ABCL` | 100 | — | $12.22 | +0.00 | $12.24 | -2.00 | -2.00 | -0.00 | -2.00 |
| 2026-08-26 | `AQST` | 241 | — | $5.08 | +0.00 | $5.39 | -74.71 | -74.71 | -0.00 | -74.71 |
| 2026-08-26 | `NEM` | 9 | — | $132.64 | +0.00 | $131.60 | +9.36 | +9.36 | -0.00 | +9.36 |
| 2026-08-27 | `BE` | 5 | $218.21 | $227.10 | -44.45 | — | +0.00 | -44.45 | -65.80 | — |
| 2026-08-27 | `ABCL` | 100 | $12.24 | $12.25 | -1.00 | — | +0.00 | -1.00 | -3.00 | — |
| 2026-08-27 | `AQST` | 241 | $5.39 | $5.39 | +0.00 | — | +0.00 | +0.00 | -74.71 | — |
| 2026-08-27 | `NEM` | 9 | $131.60 | $131.02 | +5.22 | — | +0.00 | +5.22 | +14.58 | — |
| 2026-08-28 | `SIMO` | 9 | — | $252.24 | +0.00 | $245.81 | +57.87 | +57.87 | -0.00 | +57.87 |
| 2026-08-28 | `FIG` | 80 | — | $30.18 | +0.00 | $28.82 | +108.80 | +108.80 | -0.00 | +108.80 |
| 2026-08-31 | `SIMO` | 9 | $245.81 | $247.05 | -11.16 | — | +0.00 | -11.16 | +46.71 | — |
| 2026-08-31 | `FIG` | 80 | $28.82 | $27.60 | +97.60 | — | +0.00 | +97.60 | +206.40 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `SLN` | 167 | — | $14.85 | +0.00 | $14.79 | +10.02 | +10.02 | -0.00 | +10.02 |
| 2026-09-03 | `OPK` | 1451 | — | $1.71 | +0.00 | $1.61 | +145.10 | +145.10 | -0.00 | +145.10 |
| 2026-09-04 | `SLN` | 167 | $14.79 | $14.63 | +26.72 | — | +0.00 | +26.72 | +36.74 | — |
| 2026-09-04 | `OPK` | 1451 | $1.61 | $1.59 | +29.02 | $1.64 | -72.55 | -43.53 | +174.12 | +101.57 |
| 2026-09-04 | `GSM` | 541 | — | $4.67 | +0.00 | $4.67 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-09-04 | `PIPR` | 33 | — | $76.55 | +0.00 | $77.04 | -16.17 | -16.17 | -0.00 | -16.17 |
| 2026-09-08 | `OPK` | 1451 | $1.64 | $1.63 | +14.51 | — | +0.00 | +14.51 | +116.08 | — |
| 2026-09-08 | `GSM` | 541 | $4.67 | $4.75 | -43.28 | $4.52 | +124.43 | +81.15 | -43.28 | +81.15 |
| 2026-09-08 | `PIPR` | 33 | $77.04 | $76.64 | +13.20 | — | +0.00 | +13.20 | -2.97 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +33.62 | EU, LUNR, OWL | — | $14,954.53 | $10,010.33 | EU×1412, LUNR×86, OWL×131 |
| 2026-08-17 | +2.25 | $14,954.53 | EU×1412, LUNR×86, OWL×131 | $9,916.79 | -93.54 | -38.20 | VERI, ZNTL, APMD, HIVE | EU, LUNR, OWL | $14,809.69 | $9,829.53 | VERI×1075, ZNTL×347, APMD×39, HIVE×410 |
| 2026-08-18 | -6.20 | $14,809.69 | VERI×1075, ZNTL×347, APMD×39, HIVE×410 | $9,884.94 | +55.41 | +0.00 | — | VERI, ZNTL, APMD, HIVE | $9,859.20 | $9,859.20 | — |
| 2026-08-19 | -7.20 | $9,859.20 | — | $9,859.20 | -0.00 | +0.00 | — | — | $9,859.20 | $9,859.20 | — |
| 2026-08-20 | +1.12 | $9,859.20 | — | $9,859.20 | -0.00 | +67.83 | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | — | $14,560.32 | $9,909.66 | AEM×3, WYFI×28, TOYO×139, ABCL×52, TEAM×3, AAP×13, WMT×5, AQST×133 |
| 2026-08-21 | +3.25 | $14,560.32 | AEM×3, WYFI×28, TOYO×139, ABCL×52, TEAM×3, AAP×13, WMT×5, AQST×133 | $9,859.88 | -49.78 | -19.41 | QTRX, MRNA, AUGO, SSRM, ARIS, NOG | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | $14,678.87 | $9,809.47 | QTRX×263, MRNA×6, AUGO×9, SSRM×21, ARIS×39, NOG×30 |
| 2026-08-24 | -5.17 | $14,678.87 | QTRX×263, MRNA×6, AUGO×9, SSRM×21, ARIS×39, NOG×30 | $9,802.36 | -7.11 | +8.40 | — | QTRX, MRNA, AUGO, SSRM, ARIS | $10,604.39 | $9,799.19 | NOG×30 |
| 2026-08-25 | +1.80 | $10,604.39 | NOG×30 | $9,822.59 | +23.40 | +2.70 | AVAH, SSRM, ARE, BMO | NOG | $14,670.41 | $9,814.57 | AVAH×90, SSRM×32, ARE×22, BMO×7 |
| 2026-08-26 | +2.02 | $14,670.41 | AVAH×90, SSRM×32, ARE×22, BMO×7 | $9,839.31 | +24.74 | -88.70 | BE, ABCL, AQST, NEM | AVAH, SSRM, ARE, BMO | $14,530.97 | $9,732.53 | BE×5, ABCL×100, AQST×241, NEM×9 |
| 2026-08-27 | — | $14,530.97 | BE×5, ABCL×100, AQST×241, NEM×9 | $9,692.30 | -40.23 | +0.00 | — | BE, ABCL, AQST, NEM | $9,682.88 | $9,682.88 | — |
| 2026-08-28 | +0.75 | $9,682.88 | — | $9,682.88 | +0.00 | +166.67 | SIMO, FIG | — | $14,363.01 | $9,845.12 | SIMO×9, FIG×80 |
| 2026-08-31 | -5.85 | $14,363.01 | SIMO×9, FIG×80 | $9,931.56 | +86.44 | +0.00 | — | SIMO, FIG | $9,927.31 | $9,927.31 | — |
| 2026-09-01 | -6.30 | $9,927.31 | — | $9,927.31 | +0.00 | +0.00 | — | — | $9,927.31 | $9,927.31 | — |
| 2026-09-02 | -3.83 | $9,927.31 | — | $9,927.31 | +0.00 | +0.00 | — | — | $9,927.31 | $9,927.31 | — |
| 2026-09-03 | -0.90 | $9,927.31 | — | $9,927.31 | +0.00 | +155.12 | SLN, OPK | — | $14,866.82 | $10,060.78 | SLN×167, OPK×1451 |
| 2026-09-04 | +2.25 | $14,866.82 | SLN×167, OPK×1451 | $10,116.52 | +55.74 | -88.72 | GSM, PIPR | SLN | $17,464.39 | $10,015.96 | OPK×1451, GSM×541, PIPR×33 |
| 2026-09-08 | -11.47 | $17,464.39 | OPK×1451, GSM×541, PIPR×33 | $10,000.39 | -15.57 | +124.43 | — | OPK, PIPR | $12,549.33 | $10,104.01 | GSM×541 |

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
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 5 | $106.38 | $2.04 | — | $13,949.62 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; leftover $616.20 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 133 | $4.61 | $2.44 | — | $14,560.32 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $616.20 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,560.32 | ▲ close $9,909.66 vs 09:30 $9,859.20 (session +67.83) | 16:00 close · cash $14,560.32 · equity $9,909.66 vs 09:30 $9,859.20 (+50.46; session marks +67.83) · 8 name(s) marked open→close (per-name table). AEM×3 09:30 $204.45 → close $212.04 -22.77; WYFI×28 09:30 $21.40 → close $21.16 +6.72; TOYO×139 09:30 $4.43 → close $4.51 -11.81; ABCL×52 09:30 $11.81 → close $11.57 +12.74; TEAM×3 09:30 $173.90 → close $174.91 -3.03; AAP×13 09:30 $46.85 → close $42.39 +57.98; WMT×5 09:30 $106.38 → close $103.84 +12.70; AQST×133 09:30 $4.61 → close $4.50 +15.30 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,560.32 | ▼ 09:30 equity $9,859.88 vs yday $9,909.66 (-49.78) | 09:30 open · cash $14,560.32 (unchanged overnight, no fees) · equity $9,859.88 vs prior close $9,909.66 (-49.78) · 8 name(s) re-marked at the open (per-name table). AEM×3 yday $212.04 → 09:30 $216.30 -12.78; WYFI×28 yday $21.16 → 09:30 $21.54 -10.64; TOYO×139 yday $4.51 → 09:30 $4.68 -22.94; ABCL×52 yday $11.57 → 09:30 $11.57 -0.00; TEAM×3 yday $174.91 → 09:30 $174.22 +2.07; AAP×13 yday $42.39 → 09:30 $42.41 -0.26; WMT×5 yday $103.84 → 09:30 $103.69 +0.75; AQST×133 yday $4.50 → 09:30 $4.54 -5.98 | — |
| 2026-08-21 09:30 ET | **COVER** | `AEM` | 3 | $216.30 | $2.00 | $-39.58 | $13,909.42 | ▼ -39.58 after sell → book $9,857.88; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `WYFI` | 28 | $21.54 | $2.07 | $-8.10 | $13,304.22 | ▼ -8.10 after sell → book $9,855.80; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `TOYO` | 139 | $4.68 | $2.41 | $-39.61 | $12,651.30 | ▼ -39.61 after sell → book $9,853.40; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `ABCL` | 52 | $11.57 | $2.15 | $+8.41 | $12,047.51 | ▲ +8.41 after sell → book $9,851.25; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `TEAM` | 3 | $174.22 | $2.00 | $-4.99 | $11,522.85 | ▼ -4.99 after sell → book $9,849.25; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `AAP` | 13 | $42.41 | $2.03 | $+53.63 | $10,969.49 | ▲ +53.63 after sell → book $9,847.22; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `WMT` | 5 | $103.69 | $2.00 | $+9.41 | $10,449.04 | ▲ +9.41 after sell → book $9,845.22; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `AQST` | 133 | $4.54 | $2.39 | $+4.48 | $9,842.83 | ▲ +4.48 after sell → book $9,842.83; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 263 | $3.11 | $3.47 | — | $10,657.29 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $820.24 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 6 | $133.11 | $2.05 | — | $11,453.90 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; leftover $820.24 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 9 | $89.10 | $2.06 | — | $12,253.74 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $820.24 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 21 | $38.40 | $2.10 | — | $13,058.04 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $820.24 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 39 | $20.90 | $2.15 | — | $13,871.00 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $820.24 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 30 | $27.00 | $2.12 | — | $14,678.87 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; leftover $820.24 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,678.87 | ▼ close $9,809.47 vs 09:30 $9,859.88 (session -19.41) | 16:00 close · cash $14,678.87 · equity $9,809.47 vs 09:30 $9,859.88 (-50.41; session marks -19.41) · 6 name(s) marked open→close (per-name table). QTRX×263 09:30 $3.11 → close $2.99 +31.56; MRNA×6 09:30 $133.11 → close $145.13 -72.12; AUGO×9 09:30 $89.10 → close $87.26 +16.56; SSRM×21 09:30 $38.40 → close $37.77 +13.23; ARIS×39 09:30 $20.90 → close $20.86 +1.56; NOG×30 09:30 $27.00 → close $27.34 -10.20 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,678.87 | ▼ 09:30 equity $9,802.36 vs yday $9,809.47 (-7.11) | 09:30 open · cash $14,678.87 (unchanged overnight, no fees) · equity $9,802.36 vs prior close $9,809.47 (-7.11) · 6 name(s) re-marked at the open (per-name table). QTRX×263 yday $2.99 → 09:30 $2.99 -0.00; MRNA×6 yday $145.13 → 09:30 $142.70 +14.58; AUGO×9 yday $87.26 → 09:30 $88.60 -12.06; SSRM×21 yday $37.77 → 09:30 $38.32 -11.55; ARIS×39 yday $20.86 → 09:30 $20.98 -4.68; NOG×30 yday $27.34 → 09:30 $27.12 +6.60 | — |
| 2026-08-24 09:30 ET | **COVER** | `QTRX` | 263 | $2.99 | $3.39 | $+24.70 | $13,889.11 | ▲ +24.70 after sell → book $9,798.97; vs 09:30 mark -3.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `MRNA` | 6 | $142.70 | $2.01 | $-61.60 | $13,030.90 | ▼ -61.60 after sell → book $9,796.96; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AUGO` | 9 | $88.60 | $2.02 | $+0.42 | $12,231.49 | ▲ +0.42 after sell → book $9,794.95; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `SSRM` | 21 | $38.32 | $2.05 | $-2.47 | $11,424.71 | ▼ -2.47 after sell → book $9,792.89; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `ARIS` | 39 | $20.98 | $2.11 | $-7.38 | $10,604.39 | ▼ -7.38 after sell → book $9,790.79; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,604.39 | ▲ close $9,799.19 vs 09:30 $9,802.36 (session +8.40) | 16:00 close · cash $10,604.39 · equity $9,799.19 vs 09:30 $9,802.36 (-3.17; session marks +8.40) · 1 name(s) marked open→close (per-name table). NOG×30 09:30 $27.12 → close $26.84 +8.40 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,604.39 | ▲ 09:30 equity $9,822.59 vs yday $9,799.19 (+23.40) | 09:30 open · cash $10,604.39 (unchanged overnight, no fees) · equity $9,822.59 vs prior close $9,799.19 (+23.40) · 1 name(s) re-marked at the open (per-name table). NOG×30 yday $26.84 → 09:30 $26.06 +23.40 | — |
| 2026-08-25 09:30 ET | **COVER** | `NOG` | 30 | $26.06 | $2.08 | $+24.00 | $9,820.51 | ▲ +24.00 after sell → book $9,820.51; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 90 | $13.62 | $2.32 | — | $11,044.44 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $1227.56 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `SSRM` | 32 | $37.75 | $2.14 | — | $12,250.30 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.8; leftover $1227.56 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 22 | $54.51 | $2.11 | — | $13,447.41 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; leftover $1227.56 | join🔴 sector🟡 gen🟡 news🔴 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 7 | $175.01 | $2.06 | — | $14,670.41 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; leftover $1227.56 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,670.41 | ▲ close $9,814.57 vs 09:30 $9,822.59 (session +2.70) | 16:00 close · cash $14,670.41 · equity $9,814.57 vs 09:30 $9,822.59 (-8.02; session marks +2.70) · 4 name(s) marked open→close (per-name table). AVAH×90 09:30 $13.62 → close $13.59 +3.15; SSRM×32 09:30 $37.75 → close $39.21 -46.72; ARE×22 09:30 $54.51 → close $52.90 +35.42; BMO×7 09:30 $175.01 → close $173.46 +10.85 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,670.41 | ▲ 09:30 equity $9,839.31 vs yday $9,814.57 (+24.74) | 09:30 open · cash $14,670.41 (unchanged overnight, no fees) · equity $9,839.31 vs prior close $9,814.57 (+24.74) · 4 name(s) re-marked at the open (per-name table). AVAH×90 yday $13.59 → 09:30 $13.65 -5.40; SSRM×32 yday $39.21 → 09:30 $38.41 +25.60; ARE×22 yday $52.90 → 09:30 $52.77 +2.86; BMO×7 yday $173.46 → 09:30 $173.22 +1.68 | — |
| 2026-08-26 09:30 ET | **COVER** | `AVAH` | 90 | $13.65 | $2.26 | $-6.83 | $13,439.65 | ▼ -6.83 after sell → book $9,837.05; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 32 | $38.41 | $2.09 | $-25.35 | $12,208.45 | ▼ -25.35 after sell → book $9,834.97; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARE` | 22 | $52.77 | $2.06 | $+34.12 | $11,045.45 | ▲ +34.12 after sell → book $9,832.91; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `BMO` | 7 | $173.22 | $2.01 | $+8.45 | $9,830.90 | ▲ +8.45 after sell → book $9,830.90; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 5 | $213.94 | $2.05 | — | $10,898.55 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1228.86 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 100 | $12.22 | $2.35 | — | $12,118.20 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; leftover $1228.86 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 241 | $5.08 | $3.19 | — | $13,339.28 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; leftover $1228.86 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 9 | $132.64 | $2.07 | — | $14,530.97 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; leftover $1228.86 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,530.97 | ▼ close $9,732.53 vs 09:30 $9,839.31 (session -88.70) | 16:00 close · cash $14,530.97 · equity $9,732.53 vs 09:30 $9,839.31 (-106.78; session marks -88.70) · 4 name(s) marked open→close (per-name table). BE×5 09:30 $213.94 → close $218.21 -21.35; ABCL×100 09:30 $12.22 → close $12.24 -2.00; AQST×241 09:30 $5.08 → close $5.39 -74.71; NEM×9 09:30 $132.64 → close $131.60 +9.36 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,530.97 | ▼ 09:30 equity $9,692.30 vs yday $9,732.53 (-40.23) | 09:30 open · cash $14,530.97 (unchanged overnight, no fees) · equity $9,692.30 vs prior close $9,732.53 (-40.23) · 4 name(s) re-marked at the open (per-name table). BE×5 yday $218.21 → 09:30 $227.10 -44.45; ABCL×100 yday $12.24 → 09:30 $12.25 -1.00; AQST×241 yday $5.39 → 09:30 $5.39 -0.00; NEM×9 yday $131.60 → 09:30 $131.02 +5.22 | — |
| 2026-08-27 09:30 ET | **COVER** | `BE` | 5 | $227.10 | $2.00 | $-69.86 | $13,393.47 | ▼ -69.86 after sell → book $9,690.30; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `ABCL` | 100 | $12.25 | $2.29 | $-7.64 | $12,166.18 | ▼ -7.64 after sell → book $9,688.01; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `AQST` | 241 | $5.39 | $3.11 | $-81.01 | $10,864.08 | ▼ -81.01 after sell → book $9,684.90; vs 09:30 mark -3.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `NEM` | 9 | $131.02 | $2.02 | $+10.49 | $9,682.88 | ▲ +10.49 after sell → book $9,682.88; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,682.88 | ▲ close $9,682.88 vs 09:30 $9,692.30 (session +0.00) | 16:00 close · cash $9,682.88 · no lots left · equity $9,682.88. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,682.88 | ▲ 09:30 equity $9,682.88 vs yday $9,682.88 (+0.00) | 09:30 open · cash $9,682.88 · no holdings · equity $9,682.88 vs prior close $9,682.88 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 9 | $252.24 | $2.11 | — | $11,950.94 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $2420.72 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 80 | $30.18 | $2.33 | — | $14,363.01 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; leftover $2420.72 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,363.01 | ▲ close $9,845.12 vs 09:30 $9,682.88 (session +166.67) | 16:00 close · cash $14,363.01 · equity $9,845.12 vs 09:30 $9,682.88 (+162.24; session marks +166.67) · 2 name(s) marked open→close (per-name table). SIMO×9 09:30 $252.24 → close $245.81 +57.87; FIG×80 09:30 $30.18 → close $28.82 +108.80 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,363.01 | ▲ 09:30 equity $9,931.56 vs yday $9,845.12 (+86.44) | 09:30 open · cash $14,363.01 (unchanged overnight, no fees) · equity $9,931.56 vs prior close $9,845.12 (+86.44) · 2 name(s) re-marked at the open (per-name table). SIMO×9 yday $245.81 → 09:30 $247.05 -11.16; FIG×80 yday $28.82 → 09:30 $27.60 +97.60 | — |
| 2026-08-31 09:30 ET | **COVER** | `SIMO` | 9 | $247.05 | $2.02 | $+42.59 | $12,137.54 | ▲ +42.59 after sell → book $9,929.54; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `FIG` | 80 | $27.60 | $2.23 | $+201.84 | $9,927.31 | ▲ +201.84 after sell → book $9,927.31; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,927.31 | ▲ close $9,927.31 vs 09:30 $9,931.56 (session +0.00) | 16:00 close · cash $9,927.31 · no lots left · equity $9,927.31. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,927.31 | ▲ 09:30 equity $9,927.31 vs yday $9,927.31 (+0.00) | 09:30 open · cash $9,927.31 · no holdings · equity $9,927.31 vs prior close $9,927.31 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,927.31 | ▲ close $9,927.31 vs 09:30 $9,927.31 (session +0.00) | 16:00 close · cash $9,927.31 · no lots left · equity $9,927.31. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,927.31 | ▲ 09:30 equity $9,927.31 vs yday $9,927.31 (+0.00) | 09:30 open · cash $9,927.31 · no holdings · equity $9,927.31 vs prior close $9,927.31 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,927.31 | ▲ close $9,927.31 vs 09:30 $9,927.31 (session +0.00) | 16:00 close · cash $9,927.31 · no lots left · equity $9,927.31. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,927.31 | ▲ 09:30 equity $9,927.31 vs yday $9,927.31 (+0.00) | 09:30 open · cash $9,927.31 · no holdings · equity $9,927.31 vs prior close $9,927.31 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 167 | $14.85 | $2.61 | — | $12,404.65 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $2481.83 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1451 | $1.71 | $19.05 | — | $14,866.82 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; leftover $2481.83 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,866.82 | ▲ close $10,060.78 vs 09:30 $9,927.31 (session +155.12) | 16:00 close · cash $14,866.82 · equity $10,060.78 vs 09:30 $9,927.31 (+133.47; session marks +155.12) · 2 name(s) marked open→close (per-name table). SLN×167 09:30 $14.85 → close $14.79 +10.02; OPK×1451 09:30 $1.71 → close $1.61 +145.10 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,866.82 | ▲ 09:30 equity $10,116.52 vs yday $10,060.78 (+55.74) | 09:30 open · cash $14,866.82 (unchanged overnight, no fees) · equity $10,116.52 vs prior close $10,060.78 (+55.74) · 2 name(s) re-marked at the open (per-name table). SLN×167 yday $14.79 → 09:30 $14.63 +26.72; OPK×1451 yday $1.61 → 09:30 $1.59 +29.02 | — |
| 2026-09-04 09:30 ET | **COVER** | `SLN` | 167 | $14.63 | $2.49 | $+31.64 | $12,421.12 | ▲ +31.64 after sell → book $10,114.03; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 541 | $4.67 | $7.16 | — | $14,940.43 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; leftover $2528.51 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 33 | $76.55 | $2.19 | — | $17,464.39 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $2528.51 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,464.39 | ▼ close $10,015.96 vs 09:30 $10,116.52 (session -88.72) | 16:00 close · cash $17,464.39 · equity $10,015.96 vs 09:30 $10,116.52 (-100.56; session marks -88.72) · 3 name(s) marked open→close (per-name table). OPK×1451 09:30 $1.59 → close $1.64 -72.55; GSM×541 09:30 $4.67 → close $4.67 -0.00; PIPR×33 09:30 $76.55 → close $77.04 -16.17 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,464.39 | ▼ 09:30 equity $10,000.39 vs yday $10,015.96 (-15.57) | 09:30 open · cash $17,464.39 (unchanged overnight, no fees) · equity $10,000.39 vs prior close $10,015.96 (-15.57) · 3 name(s) re-marked at the open (per-name table). OPK×1451 yday $1.64 → 09:30 $1.63 +14.51; GSM×541 yday $4.67 → 09:30 $4.75 -43.28; PIPR×33 yday $77.04 → 09:30 $76.64 +13.20 | — |
| 2026-09-08 09:30 ET | **COVER** | `OPK` | 1451 | $1.63 | $18.72 | $+78.32 | $15,080.54 | ▲ +78.32 after sell → book $9,981.67; vs 09:30 mark -18.72 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `PIPR` | 33 | $76.64 | $2.09 | $-7.25 | $12,549.33 | ▼ -7.25 after sell → book $9,979.58; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,549.33 | ▲ close $10,104.01 vs 09:30 $10,000.39 (session +124.43) | 16:00 close · cash $12,549.33 · equity $10,104.01 vs 09:30 $10,000.39 (+103.62; session marks +124.43) · 1 name(s) marked open→close (per-name table). GSM×541 09:30 $4.75 → close $4.52 +124.43 | — |

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
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GSM` | 541 | 2026-09-04 @ $4.67 | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; leftover $2528.51 |
