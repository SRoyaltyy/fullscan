# Factor mine action — `short_alarm_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · alarm

Cash book **+0.55%** ($10,055) · signal-only (no cash/fees) was +3.57%. Starts YES **5/30**. Fills 124 · skips 96 · realized $+54.59.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the 🚨 alarm is on (cameras got worse overnight).

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
- **Gate** `alarm=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,054.61.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `WWW` | 30 | — | $20.60 | +0.00 | $21.03 | -12.90 | -12.90 | -0.00 | -12.90 |
| 2026-08-14 | `FOSL` | 110 | — | $5.64 | +0.00 | $5.57 | +7.70 | +7.70 | -0.00 | +7.70 |
| 2026-08-14 | `AIRS` | 185 | — | $3.37 | +0.00 | $3.43 | -11.10 | -11.10 | -0.00 | -11.10 |
| 2026-08-14 | `OMER` | 36 | — | $17.35 | +0.00 | $17.19 | +5.76 | +5.76 | -0.00 | +5.76 |
| 2026-08-14 | `MXCT` | 449 | — | $1.39 | +0.00 | $1.32 | +31.43 | +31.43 | -0.00 | +31.43 |
| 2026-08-14 | `AVAH` | 52 | — | $11.91 | +0.00 | $12.32 | -21.32 | -21.32 | -0.00 | -21.32 |
| 2026-08-14 | `CRMD` | 77 | — | $8.05 | +0.00 | $7.54 | +39.27 | +39.27 | -0.00 | +39.27 |
| 2026-08-14 | `LVWR` | 500 | — | $1.25 | +0.00 | $1.20 | +25.00 | +25.00 | -0.00 | +25.00 |
| 2026-08-17 | `WWW` | 30 | $21.03 | $20.98 | +1.50 | — | +0.00 | +1.50 | -11.40 | — |
| 2026-08-17 | `FOSL` | 110 | $5.57 | $5.50 | +7.70 | — | +0.00 | +7.70 | +15.40 | — |
| 2026-08-17 | `AIRS` | 185 | $3.43 | $3.40 | +6.48 | — | +0.00 | +6.48 | -4.62 | — |
| 2026-08-17 | `OMER` | 36 | $17.19 | $17.17 | +0.72 | — | +0.00 | +0.72 | +6.48 | — |
| 2026-08-17 | `MXCT` | 449 | $1.32 | $1.32 | +0.00 | — | +0.00 | +0.00 | +31.43 | — |
| 2026-08-17 | `AVAH` | 52 | $12.32 | $12.21 | +5.72 | — | +0.00 | +5.72 | -15.60 | — |
| 2026-08-17 | `CRMD` | 77 | $7.54 | $7.55 | -0.77 | — | +0.00 | -0.77 | +38.50 | — |
| 2026-08-17 | `LVWR` | 500 | $1.20 | $1.18 | +10.00 | — | +0.00 | +10.00 | +35.00 | — |
| 2026-08-17 | `HNST` | 130 | — | $4.81 | +0.00 | $4.70 | +14.30 | +14.30 | -0.00 | +14.30 |
| 2026-08-17 | `FCEL` | 28 | — | $22.37 | +0.00 | $22.36 | +0.28 | +0.28 | -0.00 | +0.28 |
| 2026-08-17 | `BW` | 60 | — | $10.35 | +0.00 | $9.92 | +25.80 | +25.80 | -0.00 | +25.80 |
| 2026-08-17 | `INO` | 586 | — | $1.07 | +0.00 | $1.15 | -46.88 | -46.88 | -0.00 | -46.88 |
| 2026-08-17 | `BYND` | 48 | — | $12.83 | +0.00 | $11.63 | +57.60 | +57.60 | -0.00 | +57.60 |
| 2026-08-17 | `AEHR` | 4 | — | $132.79 | +0.00 | $145.61 | -51.28 | -51.28 | -0.00 | -51.28 |
| 2026-08-17 | `LUNR` | 30 | — | $20.25 | +0.00 | $20.38 | -3.90 | -3.90 | -0.00 | -3.90 |
| 2026-08-17 | `IOVA` | 91 | — | $6.84 | +0.00 | $7.10 | -23.66 | -23.66 | -0.00 | -23.66 |
| 2026-08-18 | `HNST` | 130 | $4.70 | $4.67 | +3.90 | — | +0.00 | +3.90 | +18.20 | — |
| 2026-08-18 | `FCEL` | 28 | $22.36 | $21.18 | +33.04 | — | +0.00 | +33.04 | +33.32 | — |
| 2026-08-18 | `BW` | 60 | $9.92 | $9.60 | +19.20 | — | +0.00 | +19.20 | +45.00 | — |
| 2026-08-18 | `INO` | 586 | $1.15 | $1.14 | +5.86 | — | +0.00 | +5.86 | -41.02 | — |
| 2026-08-18 | `BYND` | 48 | $11.63 | $11.12 | +24.48 | — | +0.00 | +24.48 | +82.08 | — |
| 2026-08-18 | `AEHR` | 4 | $145.61 | $135.58 | +40.12 | — | +0.00 | +40.12 | -11.16 | — |
| 2026-08-18 | `LUNR` | 30 | $20.38 | $19.31 | +32.10 | — | +0.00 | +32.10 | +28.20 | — |
| 2026-08-18 | `IOVA` | 91 | $7.10 | $7.00 | +9.10 | — | +0.00 | +9.10 | -14.56 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-21 | `YSS` | 109 | — | $9.26 | +0.00 | $9.32 | -6.54 | -6.54 | -0.00 | -6.54 |
| 2026-08-21 | `SMJF` | 89 | — | $11.35 | +0.00 | $11.41 | -5.34 | -5.34 | -0.00 | -5.34 |
| 2026-08-21 | `NOG` | 37 | — | $27.00 | +0.00 | $27.34 | -12.58 | -12.58 | -0.00 | -12.58 |
| 2026-08-21 | `CPRT` | 29 | — | $34.48 | +0.00 | $33.80 | +19.72 | +19.72 | -0.00 | +19.72 |
| 2026-08-21 | `FLO` | 146 | — | $6.90 | +0.00 | $6.95 | -7.30 | -7.30 | -0.00 | -7.30 |
| 2026-08-24 | `YSS` | 109 | $9.32 | $9.22 | +10.90 | — | +0.00 | +10.90 | +4.36 | — |
| 2026-08-24 | `SMJF` | 89 | $11.41 | $11.25 | +14.24 | — | +0.00 | +14.24 | +8.90 | — |
| 2026-08-24 | `NOG` | 37 | $27.34 | $27.12 | +8.14 | — | +0.00 | +8.14 | -4.44 | — |
| 2026-08-24 | `CPRT` | 29 | $33.80 | $34.04 | -6.96 | — | +0.00 | -6.96 | +12.76 | — |
| 2026-08-24 | `FLO` | 146 | $6.95 | $6.96 | -1.46 | — | +0.00 | -1.46 | -8.76 | — |
| 2026-08-25 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-26 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-27 | `AVEX` | 34 | — | $18.43 | +0.00 | $18.76 | -11.22 | -11.22 | -0.00 | -11.22 |
| 2026-08-27 | `BKSY` | 25 | — | $25.29 | +0.00 | $24.76 | +13.25 | +13.25 | -0.00 | +13.25 |
| 2026-08-27 | `BRR` | 289 | — | $2.19 | +0.00 | $2.16 | +8.67 | +8.67 | -0.00 | +8.67 |
| 2026-08-27 | `USDE` | 97 | — | $6.50 | +0.00 | $8.07 | -152.29 | -152.29 | -0.00 | -152.29 |
| 2026-08-27 | `SUJA` | 67 | — | $9.41 | +0.00 | $9.00 | +27.47 | +27.47 | -0.00 | +27.47 |
| 2026-08-27 | `BYND` | 44 | — | $14.20 | +0.00 | $14.00 | +8.80 | +8.80 | -0.00 | +8.80 |
| 2026-08-27 | `FUTU` | 4 | — | $128.00 | +0.00 | $124.57 | +13.72 | +13.72 | -0.00 | +13.72 |
| 2026-08-27 | `HNST` | 111 | — | $5.67 | +0.00 | $5.76 | -10.54 | -10.54 | -0.00 | -10.54 |
| 2026-08-28 | `AVEX` | 34 | $18.76 | $18.75 | +0.34 | — | +0.00 | +0.34 | -10.88 | — |
| 2026-08-28 | `BKSY` | 25 | $24.76 | $24.44 | +8.00 | — | +0.00 | +8.00 | +21.25 | — |
| 2026-08-28 | `BRR` | 289 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | +8.67 | — |
| 2026-08-28 | `USDE` | 97 | $8.07 | $7.24 | +80.51 | — | +0.00 | +80.51 | -71.78 | — |
| 2026-08-28 | `SUJA` | 67 | $9.00 | $9.08 | -5.36 | — | +0.00 | -5.36 | +22.11 | — |
| 2026-08-28 | `BYND` | 44 | $14.00 | $14.00 | +0.00 | — | +0.00 | +0.00 | +8.80 | — |
| 2026-08-28 | `FUTU` | 4 | $124.57 | $124.27 | +1.20 | — | +0.00 | +1.20 | +14.92 | — |
| 2026-08-28 | `HNST` | 111 | $5.76 | $5.77 | -1.11 | — | +0.00 | -1.11 | -11.65 | — |
| 2026-08-28 | `PYXS` | 189 | — | $3.32 | +0.00 | $3.23 | +17.01 | +17.01 | -0.00 | +17.01 |
| 2026-08-28 | `SAFX` | 1724 | — | $0.36 | +0.00 | $0.36 | +10.34 | +10.34 | -0.00 | +10.34 |
| 2026-08-28 | `XPOF` | 117 | — | $5.38 | +0.00 | $5.43 | -5.85 | -5.85 | -0.00 | -5.85 |
| 2026-08-28 | `APMD` | 21 | — | $29.01 | +0.00 | $29.71 | -14.70 | -14.70 | -0.00 | -14.70 |
| 2026-08-28 | `OPTU` | 629 | — | $1.00 | +0.00 | $1.02 | -12.58 | -12.58 | -0.00 | -12.58 |
| 2026-08-28 | `ABTC` | 73 | — | $8.61 | +0.00 | $7.69 | +67.16 | +67.16 | -0.00 | +67.16 |
| 2026-08-28 | `SBET` | 72 | — | $8.65 | +0.00 | $8.20 | +32.40 | +32.40 | -0.00 | +32.40 |
| 2026-08-28 | `CRCL` | 6 | — | $92.61 | +0.00 | $87.14 | +32.82 | +32.82 | -0.00 | +32.82 |
| 2026-08-31 | `PYXS` | 189 | $3.23 | $3.20 | +5.67 | — | +0.00 | +5.67 | +22.68 | — |
| 2026-08-31 | `SAFX` | 1724 | $0.36 | $0.36 | -5.17 | — | +0.00 | -5.17 | +5.17 | — |
| 2026-08-31 | `XPOF` | 117 | $5.43 | $5.37 | +7.02 | — | +0.00 | +7.02 | +1.17 | — |
| 2026-08-31 | `APMD` | 21 | $29.71 | $29.71 | +0.00 | — | +0.00 | +0.00 | -14.70 | — |
| 2026-08-31 | `OPTU` | 629 | $1.02 | $1.06 | -25.16 | — | +0.00 | -25.16 | -37.74 | — |
| 2026-08-31 | `ABTC` | 73 | $7.69 | $7.66 | +2.56 | — | +0.00 | +2.56 | +69.71 | — |
| 2026-08-31 | `SBET` | 72 | $8.20 | $8.24 | -2.88 | — | +0.00 | -2.88 | +29.52 | — |
| 2026-08-31 | `CRCL` | 6 | $87.14 | $87.04 | +0.60 | — | +0.00 | +0.60 | +33.42 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-08 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | `FRO` | 15 | — | $54.31 | +0.00 | $54.03 | +4.20 | +4.20 | -0.00 | +4.20 |
| 2026-09-17 | `CVI` | 16 | — | $51.88 | +0.00 | $54.14 | -36.16 | -36.16 | -0.00 | -36.16 |
| 2026-09-17 | `DHT` | 36 | — | $22.97 | +0.00 | $22.82 | +5.40 | +5.40 | -0.00 | +5.40 |
| 2026-09-17 | `ATRC` | 14 | — | $57.96 | +0.00 | $59.12 | -16.24 | -16.24 | -0.00 | -16.24 |
| 2026-09-17 | `HAFN` | 86 | — | $9.75 | +0.00 | $9.72 | +2.58 | +2.58 | -0.00 | +2.58 |
| 2026-09-17 | `VLO` | 2 | — | $398.45 | +0.00 | $412.53 | -28.16 | -28.16 | -0.00 | -28.16 |
| 2026-09-18 | `FRO` | 15 | $54.03 | $51.19 | +42.60 | — | +0.00 | +42.60 | +46.80 | — |
| 2026-09-18 | `CVI` | 16 | $54.14 | $54.10 | +0.64 | — | +0.00 | +0.64 | -35.52 | — |
| 2026-09-18 | `DHT` | 36 | $22.82 | $23.16 | -12.24 | — | +0.00 | -12.24 | -6.84 | — |
| 2026-09-18 | `ATRC` | 14 | $59.12 | $58.51 | +8.54 | — | +0.00 | +8.54 | -7.70 | — |
| 2026-09-18 | `HAFN` | 86 | $9.72 | $9.85 | -11.18 | — | +0.00 | -11.18 | -8.60 | — |
| 2026-09-18 | `VLO` | 2 | $412.53 | $412.00 | +1.06 | — | +0.00 | +1.06 | -27.10 | — |
| 2026-09-18 | `DELL` | 1 | — | $593.15 | +0.00 | $568.06 | +25.09 | +25.09 | -0.00 | +25.09 |
| 2026-09-18 | `SWRD` | 483 | — | $2.08 | +0.00 | $2.15 | -33.81 | -33.81 | -0.00 | -33.81 |
| 2026-09-18 | `SMTC` | 5 | — | $182.33 | +0.00 | $185.00 | -13.35 | -13.35 | -0.00 | -13.35 |
| 2026-09-18 | `IQ` | 897 | — | $1.12 | +0.00 | $1.01 | +98.67 | +98.67 | -0.00 | +98.67 |
| 2026-09-18 | `BKV` | 43 | — | $22.92 | +0.00 | $22.97 | -2.15 | -2.15 | -0.00 | -2.15 |
| 2026-09-21 | `DELL` | 1 | $568.06 | $586.77 | -18.71 | — | +0.00 | -18.71 | +6.38 | — |
| 2026-09-21 | `SWRD` | 483 | $2.15 | $2.15 | +0.00 | — | +0.00 | +0.00 | -33.81 | — |
| 2026-09-21 | `SMTC` | 5 | $185.00 | $190.30 | -26.50 | — | +0.00 | -26.50 | -39.85 | — |
| 2026-09-21 | `IQ` | 897 | $1.01 | $1.01 | +0.00 | — | +0.00 | +0.00 | +98.67 | — |
| 2026-09-21 | `BKV` | 43 | $22.97 | $22.68 | +12.47 | — | +0.00 | +12.47 | +10.32 | — |
| 2026-09-21 | `PGEN` | 80 | — | $7.84 | +0.00 | $7.70 | +11.20 | +11.20 | -0.00 | +11.20 |
| 2026-09-21 | `IOVA` | 60 | — | $10.43 | +0.00 | $10.19 | +14.40 | +14.40 | -0.00 | +14.40 |
| 2026-09-21 | `MGTX` | 46 | — | $13.47 | +0.00 | $13.08 | +17.94 | +17.94 | -0.00 | +17.94 |
| 2026-09-21 | `CYPH` | 156 | — | $4.00 | +0.00 | $3.40 | +93.60 | +93.60 | -0.00 | +93.60 |
| 2026-09-21 | `CTKB` | 117 | — | $5.32 | +0.00 | $5.28 | +4.68 | +4.68 | -0.00 | +4.68 |
| 2026-09-21 | `ALVO` | 106 | — | $5.92 | +0.00 | $5.88 | +4.24 | +4.24 | -0.00 | +4.24 |
| 2026-09-21 | `TH` | 28 | — | $21.65 | +0.00 | $21.28 | +10.36 | +10.36 | -0.00 | +10.36 |
| 2026-09-21 | `AKBA` | 677 | — | $0.93 | +0.00 | $0.94 | -8.12 | -8.12 | -0.00 | -8.12 |
| 2026-09-22 | `PGEN` | 80 | $7.70 | $7.70 | +0.00 | $7.70 | +0.00 | +0.00 | +11.20 | +11.20 |
| 2026-09-22 | `IOVA` | 60 | $10.19 | $10.18 | +0.60 | — | +0.00 | +0.60 | +15.00 | — |
| 2026-09-22 | `MGTX` | 46 | $13.08 | $13.08 | +0.00 | $13.08 | +0.00 | +0.00 | +17.94 | +17.94 |
| 2026-09-22 | `CYPH` | 156 | $3.40 | $3.51 | -17.16 | — | +0.00 | -17.16 | +76.44 | — |
| 2026-09-22 | `CTKB` | 117 | $5.28 | $5.28 | +0.00 | $5.28 | +0.00 | +0.00 | +4.68 | +4.68 |
| 2026-09-22 | `ALVO` | 106 | $5.88 | $5.88 | +0.00 | $5.88 | +0.00 | +0.00 | +4.24 | +4.24 |
| 2026-09-22 | `TH` | 28 | $21.28 | $21.28 | +0.00 | $21.28 | +0.00 | +0.00 | +10.36 | +10.36 |
| 2026-09-22 | `AKBA` | 677 | $0.94 | $0.95 | -7.45 | — | +0.00 | -7.45 | -15.57 | — |
| 2026-09-22 | `GLND` | 215 | — | $2.94 | +0.00 | $2.51 | +92.45 | +92.45 | -0.00 | +92.45 |
| 2026-09-22 | `USDE` | 48 | — | $12.99 | +0.00 | $13.54 | -26.40 | -26.40 | -0.00 | -26.40 |
| 2026-09-22 | `VGZ` | 238 | — | $2.65 | +0.00 | $2.82 | -40.46 | -40.46 | -0.00 | -40.46 |
| 2026-09-22 | `AXTI` | 8 | — | $76.64 | +0.00 | $77.79 | -9.20 | -9.20 | -0.00 | -9.20 |
| 2026-09-22 | `UMC` | 25 | — | $25.26 | +0.00 | $25.77 | -12.75 | -12.75 | -0.00 | -12.75 |
| 2026-09-22 | `MRVL` | 2 | — | $255.46 | +0.00 | $262.36 | -13.80 | -13.80 | -0.00 | -13.80 |
| 2026-09-23 | `PGEN` | 80 | $7.70 | $7.95 | -20.00 | — | +0.00 | -20.00 | -8.80 | — |
| 2026-09-23 | `MGTX` | 46 | $13.08 | $12.26 | +37.72 | — | +0.00 | +37.72 | +55.66 | — |
| 2026-09-23 | `CTKB` | 117 | $5.28 | $5.73 | -52.65 | — | +0.00 | -52.65 | -47.97 | — |
| 2026-09-23 | `ALVO` | 106 | $5.88 | $5.86 | +2.12 | — | +0.00 | +2.12 | +6.36 | — |
| 2026-09-23 | `TH` | 28 | $21.28 | $21.15 | +3.64 | — | +0.00 | +3.64 | +14.00 | — |
| 2026-09-23 | `GLND` | 215 | $2.51 | $2.70 | -40.85 | — | +0.00 | -40.85 | +51.60 | — |
| 2026-09-23 | `USDE` | 48 | $13.54 | $13.22 | +15.36 | — | +0.00 | +15.36 | -11.04 | — |
| 2026-09-23 | `VGZ` | 238 | $2.82 | $2.73 | +21.42 | — | +0.00 | +21.42 | -19.04 | — |
| 2026-09-23 | `AXTI` | 8 | $77.79 | $78.41 | -4.96 | — | +0.00 | -4.96 | -14.16 | — |
| 2026-09-23 | `UMC` | 25 | $25.77 | $25.28 | +12.25 | — | +0.00 | +12.25 | -0.50 | — |
| 2026-09-23 | `MRVL` | 2 | $262.36 | $262.36 | +0.00 | — | +0.00 | +0.00 | -13.80 | — |
| 2026-09-24 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +63.84 | WWW, FOSL, AIRS, OMER, MXCT, AVAH, CRMD, LVWR | — | $14,948.61 | $10,037.72 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500 |
| 2026-08-17 | +2.25 | $14,948.61 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500 | $10,069.07 | +31.35 | -27.74 | HNST, FCEL, BW, INO, BYND, AEHR, LUNR, IOVA | WWW, FOSL, AIRS, OMER, MXCT, AVAH, CRMD, LVWR | $14,896.98 | $9,992.62 | HNST×130, FCEL×28, BW×60, INO×586, BYND×48, AEHR×4, LUNR×30, IOVA×91 |
| 2026-08-18 | -6.20 | $14,896.98 | HNST×130, FCEL×28, BW×60, INO×586, BYND×48, AEHR×4, LUNR×30, IOVA×91 | $10,160.42 | +167.80 | +0.00 | — | HNST, FCEL, BW, INO, BYND, AEHR, LUNR, IOVA | $10,137.75 | $10,137.75 | — |
| 2026-08-19 | -7.20 | $10,137.75 | — | $10,137.75 | +0.00 | +0.00 | — | — | $10,137.75 | $10,137.75 | — |
| 2026-08-20 | +1.12 | $10,137.75 | — | $10,137.75 | +0.00 | +0.00 | — | — | $10,137.75 | $10,137.75 | — |
| 2026-08-21 | +3.25 | $10,137.75 | — | $10,137.75 | +0.00 | -12.04 | YSS, SMJF, NOG, CPRT, FLO | — | $15,152.12 | $10,114.27 | YSS×109, SMJF×89, NOG×37, CPRT×29, FLO×146 |
| 2026-08-24 | -5.17 | $15,152.12 | YSS×109, SMJF×89, NOG×37, CPRT×29, FLO×146 | $10,139.13 | +24.86 | +0.00 | — | YSS, SMJF, NOG, CPRT, FLO | $10,127.95 | $10,127.95 | — |
| 2026-08-25 | +1.80 | $10,127.95 | — | $10,127.95 | -0.00 | +0.00 | — | — | $10,127.95 | $10,127.95 | — |
| 2026-08-26 | +2.02 | $10,127.95 | — | $10,127.95 | -0.00 | +0.00 | — | — | $10,127.95 | $10,127.95 | — |
| 2026-08-27 | — | $10,127.95 | — | $10,127.95 | -0.00 | -102.14 | AVEX, BKSY, BRR, USDE, SUJA, BYND, FUTU, HNST | — | $15,027.16 | $10,006.65 | AVEX×34, BKSY×25, BRR×289, USDE×97, SUJA×67, BYND×44, FUTU×4, HNST×111 |
| 2026-08-28 | +0.75 | $15,027.16 | AVEX×34, BKSY×25, BRR×289, USDE×97, SUJA×67, BYND×44, FUTU×4, HNST×111 | $10,090.23 | +83.58 | +126.60 | PYXS, SAFX, XPOF, APMD, OPTU, ABTC, SBET, CRCL | AVEX, BKSY, BRR, USDE, SUJA, BYND, FUTU, HNST | $14,969.17 | $10,164.38 | PYXS×189, SAFX×1724, XPOF×117, APMD×21, OPTU×629, ABTC×73, SBET×72, CRCL×6 |
| 2026-08-31 | -5.85 | $14,969.17 | PYXS×189, SAFX×1724, XPOF×117, APMD×21, OPTU×629, ABTC×73, SBET×72, CRCL×6 | $10,147.01 | -17.37 | +0.00 | — | PYXS, SAFX, XPOF, APMD, OPTU, ABTC, SBET, CRCL | $10,114.11 | $10,114.11 | — |
| 2026-09-01 | -6.30 | $10,114.11 | — | $10,114.11 | -0.00 | +0.00 | — | — | $10,114.11 | $10,114.11 | — |
| 2026-09-02 | -3.83 | $10,114.11 | — | $10,114.11 | -0.00 | +0.00 | — | — | $10,114.11 | $10,114.11 | — |
| 2026-09-03 | -0.90 | $10,114.11 | — | $10,114.11 | -0.00 | +0.00 | — | — | $10,114.11 | $10,114.11 | — |
| 2026-09-04 | +2.25 | $10,114.11 | — | $10,114.11 | -0.00 | +0.00 | — | — | $10,114.11 | $10,114.11 | — |
| 2026-09-08 | -11.47 | $10,114.11 | — | $10,114.11 | -0.00 | +0.00 | — | — | $10,114.11 | $10,114.11 | — |
| 2026-09-09 | -13.95 | $10,114.11 | — | $10,114.11 | -0.00 | +0.00 | — | — | $10,114.11 | $10,114.11 | — |
| 2026-09-10 | -13.28 | $10,114.11 | — | $10,114.11 | -0.00 | +0.00 | — | — | $10,114.11 | $10,114.11 | — |
| 2026-09-11 | +0.50 | $10,114.11 | — | $10,114.11 | -0.00 | +0.00 | — | — | $10,114.11 | $10,114.11 | — |
| 2026-09-14 | -11.00 | $10,114.11 | — | $10,114.11 | -0.00 | +0.00 | — | — | $10,114.11 | $10,114.11 | — |
| 2026-09-15 | -3.84 | $10,114.11 | — | $10,114.11 | -0.00 | +0.00 | — | — | $10,114.11 | $10,114.11 | — |
| 2026-09-16 | +5.30 | $10,114.11 | — | $10,114.11 | -0.00 | +0.00 | — | — | $10,114.11 | $10,114.11 | — |
| 2026-09-17 | +7.38 | $10,114.11 | — | $10,114.11 | -0.00 | -68.38 | FRO, CVI, DHT, ATRC, HAFN, VLO | — | $15,019.89 | $10,033.02 | FRO×15, CVI×16, DHT×36, ATRC×14, HAFN×86, VLO×2 |
| 2026-09-18 | +4.86 | $15,019.89 | FRO×15, CVI×16, DHT×36, ATRC×14, HAFN×86, VLO×2 | $10,062.44 | +29.42 | +74.45 | DELL, SWRD, SMTC, IQ, BKV | FRO, CVI, DHT, ATRC, HAFN, VLO | $14,525.28 | $10,100.09 | DELL×1, SWRD×483, SMTC×5, IQ×897, BKV×43 |
| 2026-09-21 | +12.87 | $14,525.28 | DELL×1, SWRD×483, SMTC×5, IQ×897, BKV×43 | $10,067.35 | -32.74 | +148.30 | PGEN, IOVA, MGTX, CYPH, CTKB, ALVO, TH, AKBA | DELL, SWRD, SMTC, IQ, BKV | $14,999.34 | $10,167.28 | PGEN×80, IOVA×60, MGTX×46, CYPH×156, CTKB×117, ALVO×106, TH×28, AKBA×677 |
| 2026-09-22 | -0.50 | $14,999.34 | PGEN×80, IOVA×60, MGTX×46, CYPH×156, CTKB×117, ALVO×106, TH×28, AKBA×677 | $10,143.27 | -24.01 | -10.16 | GLND, USDE, VGZ, AXTI, UMC, MRVL | IOVA, CYPH, AKBA | $16,812.28 | $10,105.70 | PGEN×80, MGTX×46, CTKB×117, ALVO×106, TH×28, GLND×215, USDE×48, VGZ×238, AXTI×8, UMC×25, MRVL×2 |
| 2026-09-23 | +2.29 | $16,812.28 | PGEN×80, MGTX×46, CTKB×117, ALVO×106, TH×28, GLND×215, USDE×48, VGZ×238, AXTI×8, UMC×25, MRVL×2 | $10,079.75 | -25.95 | +0.00 | — | PGEN, MGTX, CTKB, ALVO, TH, GLND, USDE, VGZ, AXTI, UMC, MRVL | $10,054.61 | $10,054.61 | — |
| 2026-09-24 | -7.66 | $10,054.61 | — | $10,054.61 | +0.00 | +0.00 | — | — | $10,054.61 | $10,054.61 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **SHORT** | `WWW` | 30 | $20.60 | $2.12 | — | $10,615.88 | — | alarm; gate alarm=True; list probable,yday_gainer; ret5=+4.4; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `FOSL` | 110 | $5.64 | $2.37 | — | $11,233.92 | — | alarm; gate alarm=True; list probable; 🔵; ret5=-4.1; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AIRS` | 185 | $3.37 | $2.60 | — | $11,854.76 | — | alarm; gate alarm=True; list probable; ret5=-29.1; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OMER` | 36 | $17.35 | $2.14 | — | $12,477.23 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `MXCT` | 449 | $1.39 | $5.89 | — | $13,095.45 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AVAH` | 52 | $11.91 | $2.18 | — | $13,712.58 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+21.3; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `CRMD` | 77 | $8.05 | $2.26 | — | $14,330.17 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `LVWR` | 500 | $1.25 | $6.56 | — | $14,948.61 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+12.6; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,948.61 | ▲ close $10,037.72 vs 09:30 $10,000.00 (session +63.84) | 16:00 close · cash $14,948.61 · equity $10,037.72 vs 09:30 $10,000.00 (+37.72; session marks +63.84) · 8 name(s) marked open→close (per-name table). WWW×30 09:30 $20.60 → close $21.03 -12.90; FOSL×110 09:30 $5.64 → close $5.57 +7.70; AIRS×185 09:30 $3.37 → close $3.43 -11.10; OMER×36 09:30 $17.35 → close $17.19 +5.76; MXCT×449 09:30 $1.39 → close $1.32 +31.43; AVAH×52 09:30 $11.91 → close $12.32 -21.32; CRMD×77 09:30 $8.05 → close $7.54 +39.27; LVWR×500 09:30 $1.25 → close $1.20 +25.00 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,948.61 | ▲ 09:30 equity $10,069.07 vs yday $10,037.72 (+31.35) | 09:30 open · cash $14,948.61 (unchanged overnight, no fees) · equity $10,069.07 vs prior close $10,037.72 (+31.35) · 8 name(s) re-marked at the open (per-name table). WWW×30 yday $21.03 → 09:30 $20.98 +1.50; FOSL×110 yday $5.57 → 09:30 $5.50 +7.70; AIRS×185 yday $3.43 → 09:30 $3.40 +6.48; OMER×36 yday $17.19 → 09:30 $17.17 +0.72; MXCT×449 yday $1.32 → 09:30 $1.32 -0.00; AVAH×52 yday $12.32 → 09:30 $12.21 +5.72; CRMD×77 yday $7.54 → 09:30 $7.55 -0.77; LVWR×500 yday $1.20 → 09:30 $1.18 +10.00 | — |
| 2026-08-17 09:30 ET | **COVER** | `WWW` | 30 | $20.98 | $2.08 | $-15.60 | $14,317.13 | ▼ -15.60 after sell → book $10,066.99; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `FOSL` | 110 | $5.50 | $2.32 | $+10.71 | $13,709.81 | ▲ +10.71 after sell → book $10,064.67; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AIRS` | 185 | $3.40 | $2.54 | $-9.77 | $13,079.19 | ▼ -9.77 after sell → book $10,062.12; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `OMER` | 36 | $17.17 | $2.10 | $+2.25 | $12,458.97 | ▲ +2.25 after sell → book $10,060.02; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `MXCT` | 449 | $1.32 | $5.79 | $+19.74 | $11,860.50 | ▲ +19.74 after sell → book $10,054.23; vs 09:30 mark -5.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AVAH` | 52 | $12.21 | $2.15 | $-19.93 | $11,223.44 | ▼ -19.93 after sell → book $10,052.09; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `CRMD` | 77 | $7.55 | $2.22 | $+34.02 | $10,639.87 | ▲ +34.02 after sell → book $10,049.87; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `LVWR` | 500 | $1.18 | $6.45 | $+21.99 | $10,043.42 | ▲ +21.99 after sell → book $10,043.42; vs 09:30 mark -6.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `HNST` | 130 | $4.81 | $2.43 | — | $10,666.29 | — | alarm; gate alarm=True; list flatten; ⚪; ret5=-11.4; leftover $627.71 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `FCEL` | 28 | $22.37 | $2.11 | — | $11,290.54 | — | alarm; gate alarm=True; list probable,yday_gainer; ⚪; ret5=+9.5; leftover $627.71 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `BW` | 60 | $10.35 | $2.21 | — | $11,909.33 | — | alarm; gate alarm=True; list probable; ⚪; ret5=+9.8; leftover $627.71 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `INO` | 586 | $1.07 | $7.68 | — | $12,528.66 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+62.7; leftover $627.71 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `BYND` | 48 | $12.83 | $2.17 | — | $13,142.33 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ⚪; ret5=-34.1; leftover $627.71 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `AEHR` | 4 | $132.79 | $2.04 | — | $13,671.46 | — | alarm; gate alarm=True; list yday_gainer; ⚪; ret5=+30.1; leftover $627.71 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `LUNR` | 30 | $20.25 | $2.12 | — | $14,276.84 | — | alarm; gate alarm=True; list yday_gainer,ohlc_hot; ⚪; ret5=+15.9; leftover $627.71 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `IOVA` | 91 | $6.84 | $2.31 | — | $14,896.98 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $627.71 | join🟡 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,896.98 | ▼ close $9,992.62 vs 09:30 $10,069.07 (session -27.74) | 16:00 close · cash $14,896.98 · equity $9,992.62 vs 09:30 $10,069.07 (-76.45; session marks -27.74) · 8 name(s) marked open→close (per-name table). HNST×130 09:30 $4.81 → close $4.70 +14.30; FCEL×28 09:30 $22.37 → close $22.36 +0.28; BW×60 09:30 $10.35 → close $9.92 +25.80; INO×586 09:30 $1.07 → close $1.15 -46.88; BYND×48 09:30 $12.83 → close $11.63 +57.60; AEHR×4 09:30 $132.79 → close $145.61 -51.28; LUNR×30 09:30 $20.25 → close $20.38 -3.90; IOVA×91 09:30 $6.84 → close $7.10 -23.66 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,896.98 | ▲ 09:30 equity $10,160.42 vs yday $9,992.62 (+167.80) | 09:30 open · cash $14,896.98 (unchanged overnight, no fees) · equity $10,160.42 vs prior close $9,992.62 (+167.80) · 8 name(s) re-marked at the open (per-name table). HNST×130 yday $4.70 → 09:30 $4.67 +3.90; FCEL×28 yday $22.36 → 09:30 $21.18 +33.04; BW×60 yday $9.92 → 09:30 $9.60 +19.20; INO×586 yday $1.15 → 09:30 $1.14 +5.86; BYND×48 yday $11.63 → 09:30 $11.12 +24.48; AEHR×4 yday $145.61 → 09:30 $135.58 +40.12; LUNR×30 yday $20.38 → 09:30 $19.31 +32.10; IOVA×91 yday $7.10 → 09:30 $7.00 +9.10 | — |
| 2026-08-18 09:30 ET | **COVER** | `HNST` | 130 | $4.67 | $2.38 | $+13.39 | $14,287.50 | ▲ +13.39 after sell → book $10,158.04; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `FCEL` | 28 | $21.18 | $2.07 | $+29.13 | $13,692.38 | ▲ +29.13 after sell → book $10,155.96; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `BW` | 60 | $9.60 | $2.17 | $+40.62 | $13,114.21 | ▲ +40.62 after sell → book $10,153.79; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `INO` | 586 | $1.14 | $7.56 | $-56.26 | $12,438.61 | ▼ -56.26 after sell → book $10,146.23; vs 09:30 mark -7.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `BYND` | 48 | $11.12 | $2.13 | $+77.78 | $11,902.72 | ▲ +77.78 after sell → book $10,144.10; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `AEHR` | 4 | $135.58 | $2.00 | $-15.20 | $11,358.40 | ▼ -15.20 after sell → book $10,142.10; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `LUNR` | 30 | $19.31 | $2.08 | $+24.00 | $10,777.02 | ▲ +24.00 after sell → book $10,140.02; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `IOVA` | 91 | $7.00 | $2.26 | $-19.13 | $10,137.75 | ▼ -19.13 after sell → book $10,137.75; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 vol🟡 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,137.75 | ▲ close $10,137.75 vs 09:30 $10,160.42 (session +0.00) | 16:00 close · cash $10,137.75 · no lots left · equity $10,137.75. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,137.75 | ▲ 09:30 equity $10,137.75 vs yday $10,137.75 (+0.00) | 09:30 open · cash $10,137.75 · no holdings · equity $10,137.75 vs prior close $10,137.75 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,137.75 | ▲ close $10,137.75 vs 09:30 $10,137.75 (session +0.00) | 16:00 close · cash $10,137.75 · no lots left · equity $10,137.75. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,137.75 | ▲ 09:30 equity $10,137.75 vs yday $10,137.75 (+0.00) | 09:30 open · cash $10,137.75 · no holdings · equity $10,137.75 vs prior close $10,137.75 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,137.75 | ▲ close $10,137.75 vs 09:30 $10,137.75 (session +0.00) | 16:00 close · cash $10,137.75 · no lots left · equity $10,137.75. | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,137.75 | ▲ 09:30 equity $10,137.75 vs yday $10,137.75 (+0.00) | 09:30 open · cash $10,137.75 · no holdings · equity $10,137.75 vs prior close $10,137.75 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 09:30 ET | **SHORT** | `YSS` | 109 | $9.26 | $2.37 | — | $11,144.72 | — | alarm; gate alarm=True; list yday_mover; ret5=-20.1; leftover $1013.78 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SMJF` | 89 | $11.35 | $2.31 | — | $12,152.56 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+13.4; leftover $1013.78 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 37 | $27.00 | $2.15 | — | $13,149.41 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $1013.78 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CPRT` | 29 | $34.48 | $2.12 | — | $14,147.21 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+16.8; leftover $1013.78 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `FLO` | 146 | $6.90 | $2.49 | — | $15,152.12 | — | alarm; gate alarm=True; list earn_react; ret5=-5.7; leftover $1013.78 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,152.12 | ▼ close $10,114.27 vs 09:30 $10,137.75 (session -12.04) | 16:00 close · cash $15,152.12 · equity $10,114.27 vs 09:30 $10,137.75 (-23.48; session marks -12.04) · 5 name(s) marked open→close (per-name table). YSS×109 09:30 $9.26 → close $9.32 -6.54; SMJF×89 09:30 $11.35 → close $11.41 -5.34; NOG×37 09:30 $27.00 → close $27.34 -12.58; CPRT×29 09:30 $34.48 → close $33.80 +19.72; FLO×146 09:30 $6.90 → close $6.95 -7.30 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,152.12 | ▲ 09:30 equity $10,139.13 vs yday $10,114.27 (+24.86) | 09:30 open · cash $15,152.12 (unchanged overnight, no fees) · equity $10,139.13 vs prior close $10,114.27 (+24.86) · 5 name(s) re-marked at the open (per-name table). YSS×109 yday $9.32 → 09:30 $9.22 +10.90; SMJF×89 yday $11.41 → 09:30 $11.25 +14.24; NOG×37 yday $27.34 → 09:30 $27.12 +8.14; CPRT×29 yday $33.80 → 09:30 $34.04 -6.96; FLO×146 yday $6.95 → 09:30 $6.96 -1.46 | — |
| 2026-08-24 09:30 ET | **COVER** | `YSS` | 109 | $9.22 | $2.32 | $-0.33 | $14,144.82 | ▼ -0.33 after sell → book $10,136.81; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `SMJF` | 89 | $11.25 | $2.26 | $+4.33 | $13,141.31 | ▲ +4.33 after sell → book $10,134.55; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `NOG` | 37 | $27.12 | $2.10 | $-8.69 | $12,135.77 | ▼ -8.69 after sell → book $10,132.45; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **COVER** | `CPRT` | 29 | $34.04 | $2.08 | $+8.56 | $11,146.54 | ▲ +8.56 after sell → book $10,130.38; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `FLO` | 146 | $6.96 | $2.43 | $-13.68 | $10,127.95 | ▼ -13.68 after sell → book $10,127.95; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,127.95 | ▲ close $10,127.95 vs 09:30 $10,139.13 (session +0.00) | 16:00 close · cash $10,127.95 · no lots left · equity $10,127.95. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,127.95 | ▲ 09:30 equity $10,127.95 vs yday $10,127.95 (-0.00) | 09:30 open · cash $10,127.95 · no holdings · equity $10,127.95 vs prior close $10,127.95 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,127.95 | ▲ close $10,127.95 vs 09:30 $10,127.95 (session +0.00) | 16:00 close · cash $10,127.95 · no lots left · equity $10,127.95. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,127.95 | ▲ 09:30 equity $10,127.95 vs yday $10,127.95 (-0.00) | 09:30 open · cash $10,127.95 · no holdings · equity $10,127.95 vs prior close $10,127.95 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,127.95 | ▲ close $10,127.95 vs 09:30 $10,127.95 (session +0.00) | 16:00 close · cash $10,127.95 · no lots left · equity $10,127.95. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,127.95 | ▲ 09:30 equity $10,127.95 vs yday $10,127.95 (-0.00) | 09:30 open · cash $10,127.95 · no holdings · equity $10,127.95 vs prior close $10,127.95 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 09:30 ET | **SHORT** | `AVEX` | 34 | $18.43 | $2.13 | — | $10,752.44 | — | alarm; gate alarm=True; list probable,yday_gainer,yday_mover; ret5=-3.0; leftover $633.00 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `BKSY` | 25 | $25.29 | $2.10 | — | $11,382.59 | — | alarm; gate alarm=True; list yday_gainer; ret5=-11.5; leftover $633.00 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `BRR` | 289 | $2.19 | $3.80 | — | $12,011.69 | — | alarm; gate alarm=True; list yday_gainer; ret5=+3.3; leftover $633.00 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `USDE` | 97 | $6.50 | $2.32 | — | $12,639.87 | — | alarm; gate alarm=True; list yday_mover; ⚪; ret5=+93.5; leftover $633.00 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟡 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `SUJA` | 67 | $9.41 | $2.23 | — | $13,268.11 | — | alarm; gate alarm=True; list yday_mover; ret5=+27.7; leftover $633.00 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `BYND` | 44 | $14.20 | $2.16 | — | $13,890.75 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+1.2; leftover $633.00 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `FUTU` | 4 | $128.00 | $2.04 | — | $14,400.71 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+16.4; leftover $633.00 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `HNST` | 111 | $5.67 | $2.37 | — | $15,027.16 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+13.9; leftover $633.00 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,027.16 | ▼ close $10,006.65 vs 09:30 $10,127.95 (session -102.14) | 16:00 close · cash $15,027.16 · equity $10,006.65 vs 09:30 $10,127.95 (-121.30; session marks -102.14) · 8 name(s) marked open→close (per-name table). AVEX×34 09:30 $18.43 → close $18.76 -11.22; BKSY×25 09:30 $25.29 → close $24.76 +13.25; BRR×289 09:30 $2.19 → close $2.16 +8.67; USDE×97 09:30 $6.50 → close $8.07 -152.29; SUJA×67 09:30 $9.41 → close $9.00 +27.47; BYND×44 09:30 $14.20 → close $14.00 +8.80; FUTU×4 09:30 $128.00 → close $124.57 +13.72; HNST×111 09:30 $5.67 → close $5.76 -10.54 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,027.16 | ▲ 09:30 equity $10,090.23 vs yday $10,006.65 (+83.58) | 09:30 open · cash $15,027.16 (unchanged overnight, no fees) · equity $10,090.23 vs prior close $10,006.65 (+83.58) · 8 name(s) re-marked at the open (per-name table). AVEX×34 yday $18.76 → 09:30 $18.75 +0.34; BKSY×25 yday $24.76 → 09:30 $24.44 +8.00; BRR×289 yday $2.16 → 09:30 $2.16 -0.00; USDE×97 yday $8.07 → 09:30 $7.24 +80.51; SUJA×67 yday $9.00 → 09:30 $9.08 -5.36; BYND×44 yday $14.00 → 09:30 $14.00 -0.00; FUTU×4 yday $124.57 → 09:30 $124.27 +1.20; HNST×111 yday $5.76 → 09:30 $5.77 -1.11 | — |
| 2026-08-28 09:30 ET | **COVER** | `AVEX` | 34 | $18.75 | $2.09 | $-15.10 | $14,387.57 | ▼ -15.10 after sell → book $10,088.14; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `BKSY` | 25 | $24.44 | $2.06 | $+17.08 | $13,774.50 | ▲ +17.08 after sell → book $10,086.07; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `BRR` | 289 | $2.16 | $3.73 | $+1.14 | $13,146.54 | ▲ +1.14 after sell → book $10,082.35; vs 09:30 mark -3.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `USDE` | 97 | $7.24 | $2.28 | $-76.39 | $12,441.97 | ▼ -76.39 after sell → book $10,080.06; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `SUJA` | 67 | $9.08 | $2.19 | $+17.69 | $11,831.42 | ▲ +17.69 after sell → book $10,077.87; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `BYND` | 44 | $14.00 | $2.12 | $+4.52 | $11,213.30 | ▲ +4.52 after sell → book $10,075.75; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **COVER** | `FUTU` | 4 | $124.27 | $2.00 | $+10.88 | $10,714.22 | ▲ +10.88 after sell → book $10,073.75; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `HNST` | 111 | $5.77 | $2.32 | $-16.35 | $10,071.43 | ▼ -16.35 after sell → book $10,071.43; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SHORT** | `PYXS` | 189 | $3.32 | $2.62 | — | $10,696.29 | — | alarm; gate alarm=True; list probable,yday_gainer; ret5=+6.4; leftover $629.46 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SAFX` | 1724 | $0.36 | $11.78 | — | $11,313.77 | — | alarm; gate alarm=True; list probable,yday_gainer; ret5=+7.6; leftover $629.46 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `XPOF` | 117 | $5.38 | $2.39 | — | $11,940.85 | — | alarm; gate alarm=True; list probable,yday_gainer; ret5=+6.5; leftover $629.46 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `APMD` | 21 | $29.01 | $2.09 | — | $12,547.97 | — | alarm; gate alarm=True; list yday_gainer; ret5=+0.6; leftover $629.46 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `OPTU` | 629 | $1.00 | $8.25 | — | $13,168.72 | — | alarm; gate alarm=True; list yday_gainer; ret5=+16.8; leftover $629.46 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `ABTC` | 73 | $8.61 | $2.25 | — | $13,795.00 | — | alarm; gate alarm=True; list yday_mover; ret5=+3.4; leftover $629.46 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SBET` | 72 | $8.65 | $2.25 | — | $14,415.56 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+17.0; leftover $629.46 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `CRCL` | 6 | $92.61 | $2.04 | — | $14,969.17 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+12.6; leftover $629.46 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,969.17 | ▲ close $10,164.38 vs 09:30 $10,090.23 (session +126.60) | 16:00 close · cash $14,969.17 · equity $10,164.38 vs 09:30 $10,090.23 (+74.15; session marks +126.60) · 8 name(s) marked open→close (per-name table). PYXS×189 09:30 $3.32 → close $3.23 +17.01; SAFX×1724 09:30 $0.36 → close $0.36 +10.34; XPOF×117 09:30 $5.38 → close $5.43 -5.85; APMD×21 09:30 $29.01 → close $29.71 -14.70; OPTU×629 09:30 $1.00 → close $1.02 -12.58; ABTC×73 09:30 $8.61 → close $7.69 +67.16; SBET×72 09:30 $8.65 → close $8.20 +32.40; CRCL×6 09:30 $92.61 → close $87.14 +32.82 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,969.17 | ▼ 09:30 equity $10,147.01 vs yday $10,164.38 (-17.37) | 09:30 open · cash $14,969.17 (unchanged overnight, no fees) · equity $10,147.01 vs prior close $10,164.38 (-17.37) · 8 name(s) re-marked at the open (per-name table). PYXS×189 yday $3.23 → 09:30 $3.20 +5.67; SAFX×1724 yday $0.36 → 09:30 $0.36 -5.17; XPOF×117 yday $5.43 → 09:30 $5.37 +7.02; APMD×21 yday $29.71 → 09:30 $29.71 -0.00; OPTU×629 yday $1.02 → 09:30 $1.06 -25.16; ABTC×73 yday $7.69 → 09:30 $7.66 +2.56; SBET×72 yday $8.20 → 09:30 $8.24 -2.88; CRCL×6 yday $87.14 → 09:30 $87.04 +0.60 | — |
| 2026-08-31 09:30 ET | **COVER** | `PYXS` | 189 | $3.20 | $2.56 | $+17.51 | $14,361.82 | ▲ +17.51 after sell → book $10,144.45; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SAFX` | 1724 | $0.36 | $11.41 | $-18.02 | $13,726.32 | ▼ -18.02 after sell → book $10,133.04; vs 09:30 mark -11.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `XPOF` | 117 | $5.37 | $2.34 | $-3.56 | $13,095.68 | ▼ -3.56 after sell → book $10,130.70; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `APMD` | 21 | $29.71 | $2.05 | $-18.84 | $12,469.72 | ▼ -18.84 after sell → book $10,128.65; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `OPTU` | 629 | $1.06 | $8.11 | $-54.10 | $11,794.87 | ▼ -54.10 after sell → book $10,120.53; vs 09:30 mark -8.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABTC` | 73 | $7.66 | $2.21 | $+65.26 | $11,233.84 | ▲ +65.26 after sell → book $10,118.32; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SBET` | 72 | $8.24 | $2.21 | $+25.07 | $10,638.36 | ▲ +25.07 after sell → book $10,116.12; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRCL` | 6 | $87.04 | $2.01 | $+29.37 | $10,114.11 | ▲ +29.37 after sell → book $10,114.11; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,147.01 (session +0.00) | 16:00 close · cash $10,114.11 · no lots left · equity $10,114.11. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | 09:30 open · cash $10,114.11 · no holdings · equity $10,114.11 vs prior close $10,114.11 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | 16:00 close · cash $10,114.11 · no lots left · equity $10,114.11. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | 09:30 open · cash $10,114.11 · no holdings · equity $10,114.11 vs prior close $10,114.11 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | 16:00 close · cash $10,114.11 · no lots left · equity $10,114.11. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | 09:30 open · cash $10,114.11 · no holdings · equity $10,114.11 vs prior close $10,114.11 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | 16:00 close · cash $10,114.11 · no lots left · equity $10,114.11. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | 09:30 open · cash $10,114.11 · no holdings · equity $10,114.11 vs prior close $10,114.11 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | 16:00 close · cash $10,114.11 · no lots left · equity $10,114.11. | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | 09:30 open · cash $10,114.11 · no holdings · equity $10,114.11 vs prior close $10,114.11 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | 16:00 close · cash $10,114.11 · no lots left · equity $10,114.11. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | 09:30 open · cash $10,114.11 · no holdings · equity $10,114.11 vs prior close $10,114.11 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | 16:00 close · cash $10,114.11 · no lots left · equity $10,114.11. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | 09:30 open · cash $10,114.11 · no holdings · equity $10,114.11 vs prior close $10,114.11 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | 16:00 close · cash $10,114.11 · no lots left · equity $10,114.11. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | 09:30 open · cash $10,114.11 · no holdings · equity $10,114.11 vs prior close $10,114.11 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | 16:00 close · cash $10,114.11 · no lots left · equity $10,114.11. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | 09:30 open · cash $10,114.11 · no holdings · equity $10,114.11 vs prior close $10,114.11 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | 16:00 close · cash $10,114.11 · no lots left · equity $10,114.11. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | 09:30 open · cash $10,114.11 · no holdings · equity $10,114.11 vs prior close $10,114.11 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | 16:00 close · cash $10,114.11 · no lots left · equity $10,114.11. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | 09:30 open · cash $10,114.11 · no holdings · equity $10,114.11 vs prior close $10,114.11 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | 16:00 close · cash $10,114.11 · no lots left · equity $10,114.11. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | 09:30 open · cash $10,114.11 · no holdings · equity $10,114.11 vs prior close $10,114.11 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 09:30 ET | **SHORT** | `FRO` | 15 | $54.31 | $2.08 | — | $10,926.68 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+13.7; leftover $842.84 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `CVI` | 16 | $51.88 | $2.08 | — | $11,754.68 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+11.1; leftover $842.84 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `DHT` | 36 | $22.97 | $2.14 | — | $12,579.46 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+8.7; leftover $842.84 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `ATRC` | 14 | $57.96 | $2.07 | — | $13,388.83 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+7.8; leftover $842.84 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `HAFN` | 86 | $9.75 | $2.30 | — | $14,225.03 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+6.0; leftover $842.84 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `VLO` | 2 | $398.45 | $2.04 | — | $15,019.89 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+6.7; leftover $842.84 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,019.89 | ▼ close $10,033.02 vs 09:30 $10,114.11 (session -68.38) | 16:00 close · cash $15,019.89 · equity $10,033.02 vs 09:30 $10,114.11 (-81.09; session marks -68.38) · 6 name(s) marked open→close (per-name table). FRO×15 09:30 $54.31 → close $54.03 +4.20; CVI×16 09:30 $51.88 → close $54.14 -36.16; DHT×36 09:30 $22.97 → close $22.82 +5.40; ATRC×14 09:30 $57.96 → close $59.12 -16.24; HAFN×86 09:30 $9.75 → close $9.72 +2.58; VLO×2 09:30 $398.45 → close $412.53 -28.16 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,019.89 | ▲ 09:30 equity $10,062.44 vs yday $10,033.02 (+29.42) | 09:30 open · cash $15,019.89 (unchanged overnight, no fees) · equity $10,062.44 vs prior close $10,033.02 (+29.42) · 6 name(s) re-marked at the open (per-name table). FRO×15 yday $54.03 → 09:30 $51.19 +42.60; CVI×16 yday $54.14 → 09:30 $54.10 +0.64; DHT×36 yday $22.82 → 09:30 $23.16 -12.24; ATRC×14 yday $59.12 → 09:30 $58.51 +8.54; HAFN×86 yday $9.72 → 09:30 $9.85 -11.18; VLO×2 yday $412.53 → 09:30 $412.00 +1.06 | — |
| 2026-09-18 09:30 ET | **COVER** | `FRO` | 15 | $51.19 | $2.04 | $+42.69 | $14,250.01 | ▲ +42.69 after sell → book $10,060.41; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `CVI` | 16 | $54.10 | $2.04 | $-39.64 | $13,382.37 | ▼ -39.64 after sell → book $10,058.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **COVER** | `DHT` | 36 | $23.16 | $2.10 | $-11.08 | $12,546.51 | ▼ -11.08 after sell → book $10,056.27; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `ATRC` | 14 | $58.51 | $2.03 | $-11.81 | $11,725.34 | ▼ -11.81 after sell → book $10,054.24; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **COVER** | `HAFN` | 86 | $9.85 | $2.25 | $-13.14 | $10,875.99 | ▼ -13.14 after sell → book $10,051.99; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `VLO` | 2 | $412.00 | $2.00 | $-31.13 | $10,050.00 | ▼ -31.13 after sell → book $10,050.00; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SHORT** | `DELL` | 1 | $593.15 | $2.03 | — | $10,641.12 | — | alarm; gate alarm=True; list flatten,ohlc_hot; ret5=+16.1; leftover $1005.00 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SHORT** | `SWRD` | 483 | $2.08 | $6.35 | — | $11,639.41 | — | alarm; gate alarm=True; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $1005.00 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SHORT** | `SMTC` | 5 | $182.33 | $2.05 | — | $12,549.01 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+12.0; leftover $1005.00 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SHORT** | `IQ` | 897 | $1.12 | $11.76 | — | $13,541.89 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+17.4; leftover $1005.00 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SHORT** | `BKV` | 43 | $22.92 | $2.17 | — | $14,525.28 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.8; leftover $1005.00 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,525.28 | ▲ close $10,100.09 vs 09:30 $10,062.44 (session +74.45) | 16:00 close · cash $14,525.28 · equity $10,100.09 vs 09:30 $10,062.44 (+37.65; session marks +74.45) · 5 name(s) marked open→close (per-name table). DELL×1 09:30 $593.15 → close $568.06 +25.09; SWRD×483 09:30 $2.08 → close $2.15 -33.81; SMTC×5 09:30 $182.33 → close $185.00 -13.35; IQ×897 09:30 $1.12 → close $1.01 +98.67; BKV×43 09:30 $22.92 → close $22.97 -2.15 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,525.28 | ▼ 09:30 equity $10,067.35 vs yday $10,100.09 (-32.74) | 09:30 open · cash $14,525.28 (unchanged overnight, no fees) · equity $10,067.35 vs prior close $10,100.09 (-32.74) · 5 name(s) re-marked at the open (per-name table). DELL×1 yday $568.06 → 09:30 $586.77 -18.71; SWRD×483 yday $2.15 → 09:30 $2.15 -0.00; SMTC×5 yday $185.00 → 09:30 $190.30 -26.50; IQ×897 yday $1.01 → 09:30 $1.01 -0.00; BKV×43 yday $22.97 → 09:30 $22.68 +12.47 | — |
| 2026-09-21 09:30 ET | **COVER** | `DELL` | 1 | $586.77 | $1.99 | $+2.36 | $13,936.52 | ▲ +2.36 after sell → book $10,065.36; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `SWRD` | 483 | $2.15 | $6.23 | $-46.39 | $12,891.84 | ▼ -46.39 after sell → book $10,059.13; vs 09:30 mark -6.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `SMTC` | 5 | $190.30 | $2.00 | $-43.90 | $11,938.34 | ▼ -43.90 after sell → book $10,057.13; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **COVER** | `IQ` | 897 | $1.01 | $11.57 | $+75.34 | $11,020.79 | ▲ +75.34 after sell → book $10,045.55; vs 09:30 mark -11.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `BKV` | 43 | $22.68 | $2.12 | $+6.03 | $10,043.44 | ▲ +6.03 after sell → book $10,043.44; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SHORT** | `PGEN` | 80 | $7.84 | $2.27 | — | $10,668.37 | — | alarm; gate alarm=True; list flatten; ret5=+13.6; leftover $627.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SHORT** | `IOVA` | 60 | $10.43 | $2.21 | — | $11,291.96 | — | alarm; gate alarm=True; list flatten; ret5=+19.2; leftover $627.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **SHORT** | `MGTX` | 46 | $13.47 | $2.16 | — | $11,909.41 | — | alarm; gate alarm=True; list flatten; ret5=+3.6; leftover $627.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **SHORT** | `CYPH` | 156 | $4.00 | $2.51 | — | $12,530.90 | — | alarm; gate alarm=True; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $627.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SHORT** | `CTKB` | 117 | $5.32 | $2.39 | — | $13,150.95 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+15.0; leftover $627.71 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SHORT** | `ALVO` | 106 | $5.92 | $2.35 | — | $13,776.12 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+11.0; leftover $627.71 | join🔴 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SHORT** | `TH` | 28 | $21.65 | $2.11 | — | $14,380.21 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $627.71 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SHORT** | `AKBA` | 677 | $0.93 | $8.45 | — | $14,999.34 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+12.1; leftover $627.71 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,999.34 | ▲ close $10,167.28 vs 09:30 $10,067.35 (session +148.30) | 16:00 close · cash $14,999.34 · equity $10,167.28 vs 09:30 $10,067.35 (+99.93; session marks +148.30) · 8 name(s) marked open→close (per-name table). PGEN×80 09:30 $7.84 → close $7.70 +11.20; IOVA×60 09:30 $10.43 → close $10.19 +14.40; MGTX×46 09:30 $13.47 → close $13.08 +17.94; CYPH×156 09:30 $4.00 → close $3.40 +93.60; CTKB×117 09:30 $5.32 → close $5.28 +4.68; ALVO×106 09:30 $5.92 → close $5.88 +4.24; TH×28 09:30 $21.65 → close $21.28 +10.36; AKBA×677 09:30 $0.93 → close $0.94 -8.12 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,999.34 | ▼ 09:30 equity $10,143.27 vs yday $10,167.28 (-24.01) | 09:30 open · cash $14,999.34 (unchanged overnight, no fees) · equity $10,143.27 vs prior close $10,167.28 (-24.01) · 8 name(s) re-marked at the open (per-name table). PGEN×80 yday $7.70 → 09:30 $7.70 -0.00; IOVA×60 yday $10.19 → 09:30 $10.18 +0.60; MGTX×46 yday $13.08 → 09:30 $13.08 -0.00; CYPH×156 yday $3.40 → 09:30 $3.51 -17.16; CTKB×117 yday $5.28 → 09:30 $5.28 -0.00; ALVO×106 yday $5.88 → 09:30 $5.88 -0.00; TH×28 yday $21.28 → 09:30 $21.28 -0.00; AKBA×677 yday $0.94 → 09:30 $0.95 -7.45 | — |
| 2026-09-22 09:30 ET | **COVER** | `IOVA` | 60 | $10.18 | $2.17 | $+10.62 | $14,386.37 | ▲ +10.62 after sell → book $10,141.10; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **COVER** | `CYPH` | 156 | $3.51 | $2.46 | $+71.47 | $13,836.36 | ▲ +71.47 after sell → book $10,138.65; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **COVER** | `AKBA` | 677 | $0.95 | $8.46 | $-32.48 | $13,184.74 | ▼ -32.48 after sell → book $10,130.18; vs 09:30 mark -8.47 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟡 judge🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-22 09:30 ET | **SHORT** | `GLND` | 215 | $2.94 | $2.84 | — | $13,814.01 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+136.1; leftover $633.14 | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🔴 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-22 09:30 ET | **SHORT** | `USDE` | 48 | $12.99 | $2.17 | — | $14,435.36 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+69.4; leftover $633.14 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-22 09:30 ET | **SHORT** | `VGZ` | 238 | $2.65 | $3.14 | — | $15,062.92 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+26.3; leftover $633.14 | join🔴 sector🔴 gen🔴 news🟡 digest🟡 judge🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-22 09:30 ET | **SHORT** | `AXTI` | 8 | $76.64 | $2.05 | — | $15,673.99 | — | alarm; gate alarm=True; list yday_gainer; ret5=+40.1; leftover $633.14 | join🟢 sector🔴 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-22 09:30 ET | **SHORT** | `UMC` | 25 | $25.26 | $2.10 | — | $16,303.39 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+16.1; leftover $633.14 | join🟢 sector🔴 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-22 09:30 ET | **SHORT** | `MRVL` | 2 | $255.46 | $2.03 | — | $16,812.28 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+17.6; leftover $633.14 | join🟢 sector🔴 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,812.28 | ▼ close $10,105.70 vs 09:30 $10,143.27 (session -10.16) | 16:00 close · cash $16,812.28 · equity $10,105.70 vs 09:30 $10,143.27 (-37.57; session marks -10.16) · 11 name(s) marked open→close (per-name table). PGEN×80 09:30 $7.70 → close $7.70 -0.00; MGTX×46 09:30 $13.08 → close $13.08 -0.00; CTKB×117 09:30 $5.28 → close $5.28 -0.00; ALVO×106 09:30 $5.88 → close $5.88 -0.00; TH×28 09:30 $21.28 → close $21.28 -0.00; GLND×215 09:30 $2.94 → close $2.51 +92.45; USDE×48 09:30 $12.99 → close $13.54 -26.40; VGZ×238 09:30 $2.65 → close $2.82 -40.46; AXTI×8 09:30 $76.64 → close $77.79 -9.20; UMC×25 09:30 $25.26 → close $25.77 -12.75; MRVL×2 09:30 $255.46 → close $262.36 -13.80 | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,812.28 | ▼ 09:30 equity $10,079.75 vs yday $10,105.70 (-25.95) | 09:30 open · cash $16,812.28 (unchanged overnight, no fees) · equity $10,079.75 vs prior close $10,105.70 (-25.95) · 11 name(s) re-marked at the open (per-name table). PGEN×80 yday $7.70 → 09:30 $7.95 -20.00; MGTX×46 yday $13.08 → 09:30 $12.26 +37.72; CTKB×117 yday $5.28 → 09:30 $5.73 -52.65; ALVO×106 yday $5.88 → 09:30 $5.86 +2.12; TH×28 yday $21.28 → 09:30 $21.15 +3.64; GLND×215 yday $2.51 → 09:30 $2.70 -40.85; USDE×48 yday $13.54 → 09:30 $13.22 +15.36; VGZ×238 yday $2.82 → 09:30 $2.73 +21.42; AXTI×8 yday $77.79 → 09:30 $78.41 -4.96; UMC×25 yday $25.77 → 09:30 $25.28 +12.25; MRVL×2 yday $262.36 → 09:30 $262.36 -0.00 | — |
| 2026-09-23 09:30 ET | **COVER** | `PGEN` | 80 | $7.95 | $2.23 | $-13.30 | $16,174.05 | ▼ -13.30 after sell → book $10,077.52; vs 09:30 mark -2.23 | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-23 09:30 ET | **COVER** | `MGTX` | 46 | $12.26 | $2.13 | $+51.37 | $15,607.96 | ▲ +51.37 after sell → book $10,075.39; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `CTKB` | 117 | $5.73 | $2.34 | $-52.70 | $14,935.21 | ▼ -52.70 after sell → book $10,073.05; vs 09:30 mark -2.34 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `ALVO` | 106 | $5.86 | $2.31 | $+1.70 | $14,311.74 | ▲ +1.70 after sell → book $10,070.74; vs 09:30 mark -2.31 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `TH` | 28 | $21.15 | $2.07 | $+9.82 | $13,717.46 | ▲ +9.82 after sell → book $10,068.66; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `GLND` | 215 | $2.70 | $2.77 | $+45.99 | $13,134.19 | ▲ +45.99 after sell → book $10,065.89; vs 09:30 mark -2.77 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-23 09:30 ET | **COVER** | `USDE` | 48 | $13.22 | $2.13 | $-15.35 | $12,497.50 | ▼ -15.35 after sell → book $10,063.76; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `VGZ` | 238 | $2.73 | $3.07 | $-25.25 | $11,844.69 | ▼ -25.25 after sell → book $10,060.69; vs 09:30 mark -3.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `AXTI` | 8 | $78.41 | $2.01 | $-18.22 | $11,215.39 | ▼ -18.22 after sell → book $10,058.67; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `UMC` | 25 | $25.28 | $2.06 | $-4.67 | $10,581.33 | ▼ -4.67 after sell → book $10,056.61; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `MRVL` | 2 | $262.36 | $2.00 | $-17.83 | $10,054.61 | ▼ -17.83 after sell → book $10,054.61; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,054.61 | ▲ close $10,054.61 vs 09:30 $10,079.75 (session +0.00) | 16:00 close · cash $10,054.61 · no lots left · equity $10,054.61. | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,054.61 | ▲ 09:30 equity $10,054.61 vs yday $10,054.61 (+0.00) | 09:30 open · cash $10,054.61 · no holdings · equity $10,054.61 vs prior close $10,054.61 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,054.61 | ▲ close $10,054.61 vs 09:30 $10,054.61 (session +0.00) | 16:00 close · cash $10,054.61 · no lots left · equity $10,054.61. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `CBRS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COHR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDTH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SNDK` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `LITE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WDC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENHA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INV` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `INMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WFF` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `EYPT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OABI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USDE` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CAN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ARCT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ASST` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SLS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `ARCT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CNXC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NABL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `AREC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SNAP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `STT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PTRN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PCG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MNSO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ED` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DUOL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DEFT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CNH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VIST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DINO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `XLAB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `DFDV` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CIFR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BRR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `DPRO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ADBT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `RARE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CABA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SARO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ORBS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SWIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TGB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DRTS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TRBG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `XHG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TRBG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BNC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `WYHG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AIAI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GPRO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ADBE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CPRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `XHLD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BAND` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CLOV` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ECO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `PGNY` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ECHO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `HLP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `DBI` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TJGC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `BBY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `VOD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MGTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `CTKB` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TH` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `HYLN` | no_price | no 09:30 open |
| 2026-09-24 | `AEHL` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AIRS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SVIA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CAI` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ILMN` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BETA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SG` | hard_red | hard-red S=-7.66 sit; no new buys |
