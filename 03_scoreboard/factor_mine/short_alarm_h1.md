# Factor mine action — `short_alarm_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · alarm

Cash book **+0.30%** ($10,030) · signal-only (no cash/fees) was +4.54%. Starts YES **3/27**. Fills 100 · skips 80 · realized $-5.06.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $14,545.47.

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
| 2026-09-17 | `FRO` | 23 | — | $54.31 | +0.00 | $54.03 | +6.44 | +6.44 | -0.00 | +6.44 |
| 2026-09-17 | `CVI` | 24 | — | $51.88 | +0.00 | $54.14 | -54.24 | -54.24 | -0.00 | -54.24 |
| 2026-09-17 | `DHT` | 55 | — | $22.97 | +0.00 | $22.82 | +8.25 | +8.25 | -0.00 | +8.25 |
| 2026-09-17 | `ATRC` | 21 | — | $57.96 | +0.00 | $59.12 | -24.36 | -24.36 | -0.00 | -24.36 |
| 2026-09-18 | `FRO` | 23 | $54.03 | $51.19 | +65.32 | — | +0.00 | +65.32 | +71.76 | — |
| 2026-09-18 | `CVI` | 24 | $54.14 | $54.10 | +0.96 | — | +0.00 | +0.96 | -53.28 | — |
| 2026-09-18 | `DHT` | 55 | $22.82 | $23.16 | -18.70 | — | +0.00 | -18.70 | -10.45 | — |
| 2026-09-18 | `ATRC` | 21 | $59.12 | $58.51 | +12.81 | — | +0.00 | +12.81 | -11.55 | — |
| 2026-09-18 | `DELL` | 1 | — | $593.15 | +0.00 | $568.06 | +25.09 | +25.09 | -0.00 | +25.09 |
| 2026-09-18 | `SWRD` | 485 | — | $2.08 | +0.00 | $2.15 | -33.95 | -33.95 | -0.00 | -33.95 |
| 2026-09-18 | `BRR` | 282 | — | $3.57 | +0.00 | $3.76 | -53.58 | -53.58 | -0.00 | -53.58 |
| 2026-09-18 | `SMTC` | 5 | — | $182.33 | +0.00 | $185.00 | -13.35 | -13.35 | -0.00 | -13.35 |
| 2026-09-18 | `IQ` | 901 | — | $1.12 | +0.00 | $1.01 | +99.11 | +99.11 | -0.00 | +99.11 |
| 2026-09-21 | `DELL` | 1 | $568.06 | $586.77 | -18.71 | — | +0.00 | -18.71 | +6.38 | — |
| 2026-09-21 | `SWRD` | 485 | $2.15 | $2.15 | +0.00 | — | +0.00 | +0.00 | -33.95 | — |
| 2026-09-21 | `BRR` | 282 | $3.76 | $3.85 | -25.38 | — | +0.00 | -25.38 | -78.96 | — |
| 2026-09-21 | `SMTC` | 5 | $185.00 | $190.30 | -26.50 | — | +0.00 | -26.50 | -39.85 | — |
| 2026-09-21 | `IQ` | 901 | $1.01 | $1.01 | +0.00 | — | +0.00 | +0.00 | +99.11 | — |
| 2026-09-21 | `PGEN` | 79 | — | $7.84 | +0.00 | $7.70 | +11.06 | +11.06 | -0.00 | +11.06 |
| 2026-09-21 | `IOVA` | 59 | — | $10.43 | +0.00 | $10.19 | +14.16 | +14.16 | -0.00 | +14.16 |
| 2026-09-21 | `AEHL` | 75 | — | $8.26 | +0.00 | $6.92 | +100.50 | +100.50 | -0.00 | +100.50 |
| 2026-09-21 | `TH` | 28 | — | $21.55 | +0.00 | $21.28 | +7.56 | +7.56 | -0.00 | +7.56 |
| 2026-09-21 | `AMD` | 1 | — | $583.88 | +0.00 | $615.52 | -31.64 | -31.64 | -0.00 | -31.64 |
| 2026-09-21 | `CLS` | 1 | — | $341.45 | +0.00 | $344.40 | -2.95 | -2.95 | -0.00 | -2.95 |
| 2026-09-21 | `SES` | 1156 | — | $0.54 | +0.00 | $0.57 | -34.56 | -34.56 | -0.00 | -34.56 |
| 2026-09-21 | `CECO` | 8 | — | $71.01 | +0.00 | $71.53 | -4.16 | -4.16 | -0.00 | -4.16 |

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
| 2026-09-17 | +7.38 | $10,114.11 | — | $10,114.11 | -0.00 | -63.91 | FRO, CVI, DHT, ATRC | — | $15,080.32 | $10,041.65 | FRO×23, CVI×24, DHT×55, ATRC×21 |
| 2026-09-18 | +4.86 | $15,080.32 | FRO×23, CVI×24, DHT×55, ATRC×21 | $10,102.04 | +60.39 | +23.32 | DELL, SWRD, BRR, SMTC, IQ | FRO, CVI, DHT, ATRC | $14,597.19 | $10,091.05 | DELL×1, SWRD×485, BRR×282, SMTC×5, IQ×901 |
| 2026-09-21 | +12.87 | $14,597.19 | DELL×1, SWRD×485, BRR×282, SMTC×5, IQ×901 | $10,020.46 | -70.59 | +59.97 | PGEN, IOVA, AEHL, TH, AMD, CLS, SES, CECO | DELL, SWRD, BRR, SMTC, IQ | $14,545.47 | $10,030.04 | PGEN×79, IOVA×59, AEHL×75, TH×28, AMD×1, CLS×1, SES×1156, CECO×8 |

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
| 2026-09-17 09:30 ET | **SHORT** | `FRO` | 23 | $54.31 | $2.11 | — | $11,361.13 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+13.7; leftover $1264.26 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `CVI` | 24 | $51.88 | $2.12 | — | $12,604.13 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+11.1; leftover $1264.26 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `DHT` | 55 | $22.97 | $2.21 | — | $13,865.27 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+8.7; leftover $1264.26 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `ATRC` | 21 | $57.96 | $2.11 | — | $15,080.32 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+7.8; leftover $1264.26 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,080.32 | ▼ close $10,041.65 vs 09:30 $10,114.11 (session -63.91) | 16:00 close · cash $15,080.32 · equity $10,041.65 vs 09:30 $10,114.11 (-72.46; session marks -63.91) · 4 name(s) marked open→close (per-name table). FRO×23 09:30 $54.31 → close $54.03 +6.44; CVI×24 09:30 $51.88 → close $54.14 -54.24; DHT×55 09:30 $22.97 → close $22.82 +8.25; ATRC×21 09:30 $57.96 → close $59.12 -24.36 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,080.32 | ▲ 09:30 equity $10,102.04 vs yday $10,041.65 (+60.39) | 09:30 open · cash $15,080.32 (unchanged overnight, no fees) · equity $10,102.04 vs prior close $10,041.65 (+60.39) · 4 name(s) re-marked at the open (per-name table). FRO×23 yday $54.03 → 09:30 $51.19 +65.32; CVI×24 yday $54.14 → 09:30 $54.10 +0.96; DHT×55 yday $22.82 → 09:30 $23.16 -18.70; ATRC×21 yday $59.12 → 09:30 $58.51 +12.81 | — |
| 2026-09-18 09:30 ET | **COVER** | `FRO` | 23 | $51.19 | $2.06 | $+67.59 | $13,900.89 | ▲ +67.59 after sell → book $10,099.98; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `CVI` | 24 | $54.10 | $2.06 | $-57.46 | $12,600.43 | ▼ -57.46 after sell → book $10,097.92; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **COVER** | `DHT` | 55 | $23.16 | $2.15 | $-14.81 | $11,324.48 | ▼ -14.81 after sell → book $10,095.77; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `ATRC` | 21 | $58.51 | $2.05 | $-15.71 | $10,093.71 | ▼ -15.71 after sell → book $10,093.71; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **SHORT** | `DELL` | 1 | $593.15 | $2.03 | — | $10,684.84 | — | alarm; gate alarm=True; list flatten,ohlc_hot; ret5=+16.1; leftover $1009.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SHORT** | `SWRD` | 485 | $2.08 | $6.37 | — | $11,687.26 | — | alarm; gate alarm=True; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $1009.37 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SHORT** | `BRR` | 282 | $3.57 | $3.72 | — | $12,690.28 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+15.4; leftover $1009.37 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SHORT** | `SMTC` | 5 | $182.33 | $2.05 | — | $13,599.88 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+12.0; leftover $1009.37 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SHORT** | `IQ` | 901 | $1.12 | $11.81 | — | $14,597.19 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+17.4; leftover $1009.37 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,597.19 | ▲ close $10,091.05 vs 09:30 $10,102.04 (session +23.32) | 16:00 close · cash $14,597.19 · equity $10,091.05 vs 09:30 $10,102.04 (-10.99; session marks +23.32) · 5 name(s) marked open→close (per-name table). DELL×1 09:30 $593.15 → close $568.06 +25.09; SWRD×485 09:30 $2.08 → close $2.15 -33.95; BRR×282 09:30 $3.57 → close $3.76 -53.58; SMTC×5 09:30 $182.33 → close $185.00 -13.35; IQ×901 09:30 $1.12 → close $1.01 +99.11 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,597.19 | ▼ 09:30 equity $10,020.46 vs yday $10,091.05 (-70.59) | 09:30 open · cash $14,597.19 (unchanged overnight, no fees) · equity $10,020.46 vs prior close $10,091.05 (-70.59) · 5 name(s) re-marked at the open (per-name table). DELL×1 yday $568.06 → 09:30 $586.77 -18.71; SWRD×485 yday $2.15 → 09:30 $2.15 -0.00; BRR×282 yday $3.76 → 09:30 $3.85 -25.38; SMTC×5 yday $185.00 → 09:30 $190.30 -26.50; IQ×901 yday $1.01 → 09:30 $1.01 -0.00 | — |
| 2026-09-21 09:30 ET | **COVER** | `DELL` | 1 | $586.77 | $1.99 | $+2.36 | $14,008.43 | ▲ +2.36 after sell → book $10,018.47; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `SWRD` | 485 | $2.15 | $6.26 | $-46.58 | $12,959.42 | ▼ -46.58 after sell → book $10,012.21; vs 09:30 mark -6.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `BRR` | 282 | $3.85 | $3.64 | $-86.32 | $11,870.08 | ▼ -86.32 after sell → book $10,008.57; vs 09:30 mark -3.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `SMTC` | 5 | $190.30 | $2.00 | $-43.90 | $10,916.58 | ▼ -43.90 after sell → book $10,006.57; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **COVER** | `IQ` | 901 | $1.01 | $11.62 | $+75.68 | $9,994.94 | ▲ +75.68 after sell → book $9,994.94; vs 09:30 mark -11.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SHORT** | `PGEN` | 79 | $7.84 | $2.27 | — | $10,612.04 | — | alarm; gate alarm=True; list flatten; ⚪; ret5=+13.6; leftover $624.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SHORT** | `IOVA` | 59 | $10.43 | $2.20 | — | $11,225.20 | — | alarm; gate alarm=True; list flatten; ⚪; ret5=+19.2; leftover $624.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 75 | $8.26 | $2.25 | — | $11,842.45 | — | alarm; gate alarm=True; list yday_mover; ret5=+23.9; leftover $624.68 | join🟡 sector🔴 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-21 09:30 ET | **SHORT** | `TH` | 28 | $21.55 | $2.11 | — | $12,443.74 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $624.68 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 1 | $583.88 | $2.03 | — | $13,025.59 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+8.5; leftover $624.68 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **SHORT** | `CLS` | 1 | $341.45 | $2.02 | — | $13,365.02 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.2; leftover $624.68 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **SHORT** | `SES` | 1156 | $0.54 | $9.93 | — | $13,979.44 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+9.6; leftover $624.68 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟡 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **SHORT** | `CECO` | 8 | $71.01 | $2.05 | — | $14,545.47 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+11.1; leftover $624.68 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,545.47 | ▲ close $10,030.04 vs 09:30 $10,020.46 (session +59.97) | 16:00 close · cash $14,545.47 · equity $10,030.04 vs 09:30 $10,020.46 (+9.58; session marks +59.97) · 8 name(s) marked open→close (per-name table). PGEN×79 09:30 $7.84 → close $7.70 +11.06; IOVA×59 09:30 $10.43 → close $10.19 +14.16; AEHL×75 09:30 $8.26 → close $6.92 +100.50; TH×28 09:30 $21.55 → close $21.28 +7.56; AMD×1 09:30 $583.88 → close $615.52 -31.64; CLS×1 09:30 $341.45 → close $344.40 -2.95; SES×1156 09:30 $0.54 → close $0.57 -34.56; CECO×8 09:30 $71.01 → close $71.53 -4.16 | — |

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
| 2026-09-10 | `ZVRA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ADBE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `XHLD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BAND` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CLOV` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ARLO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HITI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `HLP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `DBI` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TJGC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SPCX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `BBY` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `PGEN` | 79 | 2026-09-21 @ $7.84 | alarm; gate alarm=True; list flatten; ⚪; ret5=+13.6; leftover $624.68 |
| `IOVA` | 59 | 2026-09-21 @ $10.43 | alarm; gate alarm=True; list flatten; ⚪; ret5=+19.2; leftover $624.68 |
| `AEHL` | 75 | 2026-09-21 @ $8.26 | alarm; gate alarm=True; list yday_mover; ret5=+23.9; leftover $624.68 |
| `TH` | 28 | 2026-09-21 @ $21.55 | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $624.68 |
| `AMD` | 1 | 2026-09-21 @ $583.88 | alarm; gate alarm=True; list ohlc_hot; ret5=+8.5; leftover $624.68 |
| `CLS` | 1 | 2026-09-21 @ $341.45 | alarm; gate alarm=True; list ohlc_hot; ret5=+10.2; leftover $624.68 |
| `SES` | 1156 | 2026-09-21 @ $0.54 | alarm; gate alarm=True; list ohlc_hot; ret5=+9.6; leftover $624.68 |
| `CECO` | 8 | 2026-09-21 @ $71.01 | alarm; gate alarm=True; list ohlc_hot; ret5=+11.1; leftover $624.68 |
