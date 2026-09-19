# Factor mine action — `short_alarm_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · alarm

Cash book **+0.78%** ($10,078) · signal-only (no cash/fees) was +3.12%. Starts YES **5/26**. Fills 98 · skips 85 · realized $+63.64.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $15,337.09.

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
| 2026-08-21 | `YSS` | 91 | — | $9.26 | +0.00 | $9.32 | -5.46 | -5.46 | -0.00 | -5.46 |
| 2026-08-21 | `SMJF` | 74 | — | $11.35 | +0.00 | $11.41 | -4.44 | -4.44 | -0.00 | -4.44 |
| 2026-08-21 | `NOG` | 31 | — | $27.00 | +0.00 | $27.34 | -10.54 | -10.54 | -0.00 | -10.54 |
| 2026-08-21 | `CPRT` | 24 | — | $34.48 | +0.00 | $33.80 | +16.32 | +16.32 | -0.00 | +16.32 |
| 2026-08-21 | `FLO` | 122 | — | $6.90 | +0.00 | $6.95 | -6.10 | -6.10 | -0.00 | -6.10 |
| 2026-08-21 | `SSYS` | 106 | — | $7.94 | +0.00 | $7.90 | +4.24 | +4.24 | -0.00 | +4.24 |
| 2026-08-24 | `YSS` | 91 | $9.32 | $9.22 | +9.10 | — | +0.00 | +9.10 | +3.64 | — |
| 2026-08-24 | `SMJF` | 74 | $11.41 | $11.25 | +11.84 | — | +0.00 | +11.84 | +7.40 | — |
| 2026-08-24 | `NOG` | 31 | $27.34 | $27.12 | +6.82 | — | +0.00 | +6.82 | -3.72 | — |
| 2026-08-24 | `CPRT` | 24 | $33.80 | $34.04 | -5.76 | — | +0.00 | -5.76 | +10.56 | — |
| 2026-08-24 | `FLO` | 122 | $6.95 | $6.96 | -1.22 | — | +0.00 | -1.22 | -7.32 | — |
| 2026-08-24 | `SSYS` | 106 | $7.90 | $7.89 | +1.06 | — | +0.00 | +1.06 | +5.30 | — |
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
| 2026-08-28 | `XPOF` | 116 | — | $5.38 | +0.00 | $5.43 | -5.80 | -5.80 | -0.00 | -5.80 |
| 2026-08-28 | `APMD` | 21 | — | $29.01 | +0.00 | $29.71 | -14.70 | -14.70 | -0.00 | -14.70 |
| 2026-08-28 | `OPTU` | 629 | — | $1.00 | +0.00 | $1.02 | -12.58 | -12.58 | -0.00 | -12.58 |
| 2026-08-28 | `ABTC` | 73 | — | $8.61 | +0.00 | $7.69 | +67.16 | +67.16 | -0.00 | +67.16 |
| 2026-08-28 | `SBET` | 72 | — | $8.65 | +0.00 | $8.20 | +32.40 | +32.40 | -0.00 | +32.40 |
| 2026-08-28 | `CRCL` | 6 | — | $92.61 | +0.00 | $87.14 | +32.82 | +32.82 | -0.00 | +32.82 |
| 2026-08-31 | `PYXS` | 189 | $3.23 | $3.20 | +5.67 | — | +0.00 | +5.67 | +22.68 | — |
| 2026-08-31 | `SAFX` | 1724 | $0.36 | $0.36 | -5.17 | — | +0.00 | -5.17 | +5.17 | — |
| 2026-08-31 | `XPOF` | 116 | $5.43 | $5.37 | +6.96 | — | +0.00 | +6.96 | +1.16 | — |
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
| 2026-09-17 | `FRO` | 11 | — | $54.31 | +0.00 | $54.03 | +3.08 | +3.08 | -0.00 | +3.08 |
| 2026-09-17 | `CVI` | 12 | — | $51.88 | +0.00 | $54.14 | -27.12 | -27.12 | -0.00 | -27.12 |
| 2026-09-17 | `DHT` | 27 | — | $22.97 | +0.00 | $22.82 | +4.05 | +4.05 | -0.00 | +4.05 |
| 2026-09-17 | `ATRC` | 10 | — | $57.96 | +0.00 | $59.12 | -11.60 | -11.60 | -0.00 | -11.60 |
| 2026-09-17 | `HAFN` | 64 | — | $9.75 | +0.00 | $9.72 | +1.92 | +1.92 | -0.00 | +1.92 |
| 2026-09-17 | `VLO` | 1 | — | $398.45 | +0.00 | $412.53 | -14.08 | -14.08 | -0.00 | -14.08 |
| 2026-09-17 | `BWIN` | 19 | — | $32.06 | +0.00 | $31.95 | +2.09 | +2.09 | -0.00 | +2.09 |
| 2026-09-17 | `MNR` | 57 | — | $11.00 | +0.00 | $10.97 | +1.71 | +1.71 | -0.00 | +1.71 |
| 2026-09-18 | `FRO` | 11 | $54.03 | $51.19 | +31.24 | — | +0.00 | +31.24 | +34.32 | — |
| 2026-09-18 | `CVI` | 12 | $54.14 | $54.10 | +0.48 | — | +0.00 | +0.48 | -26.64 | — |
| 2026-09-18 | `DHT` | 27 | $22.82 | $23.16 | -9.18 | — | +0.00 | -9.18 | -5.13 | — |
| 2026-09-18 | `ATRC` | 10 | $59.12 | $58.51 | +6.10 | — | +0.00 | +6.10 | -5.50 | — |
| 2026-09-18 | `HAFN` | 64 | $9.72 | $9.85 | -8.32 | — | +0.00 | -8.32 | -6.40 | — |
| 2026-09-18 | `VLO` | 1 | $412.53 | $412.00 | +0.53 | — | +0.00 | +0.53 | -13.55 | — |
| 2026-09-18 | `BWIN` | 19 | $31.95 | $31.98 | -0.57 | $31.93 | +0.95 | +0.38 | +1.52 | +2.47 |
| 2026-09-18 | `MNR` | 57 | $10.97 | $10.95 | +1.14 | — | +0.00 | +1.14 | +2.85 | — |
| 2026-09-18 | `SWRD` | 345 | — | $2.08 | +0.00 | $2.15 | -24.15 | -24.15 | -0.00 | -24.15 |
| 2026-09-18 | `BRR` | 201 | — | $3.57 | +0.00 | $3.76 | -38.19 | -38.19 | -0.00 | -38.19 |
| 2026-09-18 | `SMTC` | 3 | — | $182.33 | +0.00 | $185.00 | -8.01 | -8.01 | -0.00 | -8.01 |
| 2026-09-18 | `DELL` | 1 | — | $593.15 | +0.00 | $568.06 | +25.09 | +25.09 | -0.00 | +25.09 |
| 2026-09-18 | `IQ` | 641 | — | $1.12 | +0.00 | $1.01 | +70.51 | +70.51 | -0.00 | +70.51 |
| 2026-09-18 | `CBC` | 22 | — | $31.64 | +0.00 | $31.85 | -4.62 | -4.62 | -0.00 | -4.62 |
| 2026-09-18 | `LIFE` | 19 | — | $36.89 | +0.00 | $36.00 | +16.91 | +16.91 | -0.00 | +16.91 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +63.84 | WWW, FOSL, AIRS, OMER, MXCT, AVAH, CRMD, LVWR | — | $14,948.61 | $10,037.72 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500 |
| 2026-08-17 | +2.25 | $14,948.61 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500 | $10,069.07 | +31.35 | -27.74 | HNST, FCEL, BW, INO, BYND, AEHR, LUNR, IOVA | WWW, FOSL, AIRS, OMER, MXCT, AVAH, CRMD, LVWR | $14,896.98 | $9,992.62 | HNST×130, FCEL×28, BW×60, INO×586, BYND×48, AEHR×4, LUNR×30, IOVA×91 |
| 2026-08-18 | -6.20 | $14,896.98 | HNST×130, FCEL×28, BW×60, INO×586, BYND×48, AEHR×4, LUNR×30, IOVA×91 | $10,160.42 | +167.80 | +0.00 | — | HNST, FCEL, BW, INO, BYND, AEHR, LUNR, IOVA | $10,137.75 | $10,137.75 | — |
| 2026-08-19 | -7.20 | $10,137.75 | — | $10,137.75 | +0.00 | +0.00 | — | — | $10,137.75 | $10,137.75 | — |
| 2026-08-20 | +1.12 | $10,137.75 | — | $10,137.75 | +0.00 | +0.00 | — | — | $10,137.75 | $10,137.75 | — |
| 2026-08-21 | +3.25 | $10,137.75 | — | $10,137.75 | +0.00 | -5.98 | YSS, SMJF, NOG, CPRT, FLO, SSYS | — | $15,154.71 | $10,118.21 | YSS×91, SMJF×74, NOG×31, CPRT×24, FLO×122, SSYS×106 |
| 2026-08-24 | -5.17 | $15,154.71 | YSS×91, SMJF×74, NOG×31, CPRT×24, FLO×122, SSYS×106 | $10,140.05 | +21.84 | +0.00 | — | YSS, SMJF, NOG, CPRT, FLO, SSYS | $10,126.76 | $10,126.76 | — |
| 2026-08-25 | +1.80 | $10,126.76 | — | $10,126.76 | +0.00 | +0.00 | — | — | $10,126.76 | $10,126.76 | — |
| 2026-08-26 | +2.02 | $10,126.76 | — | $10,126.76 | +0.00 | +0.00 | — | — | $10,126.76 | $10,126.76 | — |
| 2026-08-27 | — | $10,126.76 | — | $10,126.76 | +0.00 | -102.14 | AVEX, BKSY, BRR, USDE, SUJA, BYND, FUTU, HNST | — | $15,025.97 | $10,005.46 | AVEX×34, BKSY×25, BRR×289, USDE×97, SUJA×67, BYND×44, FUTU×4, HNST×111 |
| 2026-08-28 | +0.75 | $15,025.97 | AVEX×34, BKSY×25, BRR×289, USDE×97, SUJA×67, BYND×44, FUTU×4, HNST×111 | $10,089.04 | +83.58 | +126.65 | PYXS, SAFX, XPOF, APMD, OPTU, ABTC, SBET, CRCL | AVEX, BKSY, BRR, USDE, SUJA, BYND, FUTU, HNST | $14,962.61 | $10,163.24 | PYXS×189, SAFX×1724, XPOF×116, APMD×21, OPTU×629, ABTC×73, SBET×72, CRCL×6 |
| 2026-08-31 | -5.85 | $14,962.61 | PYXS×189, SAFX×1724, XPOF×116, APMD×21, OPTU×629, ABTC×73, SBET×72, CRCL×6 | $10,145.82 | -17.42 | +0.00 | — | PYXS, SAFX, XPOF, APMD, OPTU, ABTC, SBET, CRCL | $10,112.92 | $10,112.92 | — |
| 2026-09-01 | -6.30 | $10,112.92 | — | $10,112.92 | -0.00 | +0.00 | — | — | $10,112.92 | $10,112.92 | — |
| 2026-09-02 | -3.83 | $10,112.92 | — | $10,112.92 | -0.00 | +0.00 | — | — | $10,112.92 | $10,112.92 | — |
| 2026-09-03 | -0.90 | $10,112.92 | — | $10,112.92 | -0.00 | +0.00 | — | — | $10,112.92 | $10,112.92 | — |
| 2026-09-04 | +2.25 | $10,112.92 | — | $10,112.92 | -0.00 | +0.00 | — | — | $10,112.92 | $10,112.92 | — |
| 2026-09-08 | -11.47 | $10,112.92 | — | $10,112.92 | -0.00 | +0.00 | — | — | $10,112.92 | $10,112.92 | — |
| 2026-09-09 | -13.95 | $10,112.92 | — | $10,112.92 | -0.00 | +0.00 | — | — | $10,112.92 | $10,112.92 | — |
| 2026-09-10 | -13.28 | $10,112.92 | — | $10,112.92 | -0.00 | +0.00 | — | — | $10,112.92 | $10,112.92 | — |
| 2026-09-11 | +0.50 | $10,112.92 | — | $10,112.92 | -0.00 | +0.00 | — | — | $10,112.92 | $10,112.92 | — |
| 2026-09-14 | -11.00 | $10,112.92 | — | $10,112.92 | -0.00 | +0.00 | — | — | $10,112.92 | $10,112.92 | — |
| 2026-09-15 | -3.84 | $10,112.92 | — | $10,112.92 | -0.00 | +0.00 | — | — | $10,112.92 | $10,112.92 | — |
| 2026-09-16 | +5.30 | $10,112.92 | — | $10,112.92 | -0.00 | +0.00 | — | — | $10,112.92 | $10,112.92 | — |
| 2026-09-17 | +7.38 | $10,112.92 | — | $10,112.92 | -0.00 | -39.95 | FRO, CVI, DHT, ATRC, HAFN, VLO, BWIN, MNR | — | $14,774.46 | $10,056.16 | FRO×11, CVI×12, DHT×27, ATRC×10, HAFN×64, VLO×1, BWIN×19, MNR×57 |
| 2026-09-18 | +4.86 | $14,774.46 | FRO×11, CVI×12, DHT×27, ATRC×10, HAFN×64, VLO×1, BWIN×19, MNR×57 | $10,077.58 | +21.42 | +38.49 | SWRD, BRR, SMTC, DELL, IQ, CBC, LIFE | FRO, CVI, DHT, ATRC, HAFN, VLO, MNR | $15,337.09 | $10,077.74 | BWIN×19, SWRD×345, BRR×201, SMTC×3, DELL×1, IQ×641, CBC×22, LIFE×19 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **SHORT** | `WWW` | 30 | $20.60 | $2.12 | — | $10,615.88 | — | alarm; gate alarm=True; list probable,yday_gainer; ret5=+4.4; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `FOSL` | 110 | $5.64 | $2.37 | — | $11,233.92 | — | alarm; gate alarm=True; list probable; 🔵; ret5=-4.1; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AIRS` | 185 | $3.37 | $2.60 | — | $11,854.76 | — | alarm; gate alarm=True; list probable; ret5=-29.1; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OMER` | 36 | $17.35 | $2.14 | — | $12,477.23 | — | alarm; gate alarm=True; list yday_gainer,yday_mover,oppset; 🔵; ret5=+31.9; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `MXCT` | 449 | $1.39 | $5.89 | — | $13,095.45 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AVAH` | 52 | $11.91 | $2.18 | — | $13,712.58 | — | alarm; gate alarm=True; list yday_gainer,yday_mover,oppset; 🔵; ret5=+21.3; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `CRMD` | 77 | $8.05 | $2.26 | — | $14,330.17 | — | alarm; gate alarm=True; list yday_gainer,yday_mover,oppset; 🔵; ret5=+8.4; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `LVWR` | 500 | $1.25 | $6.56 | — | $14,948.61 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+12.6; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,948.61 | ▲ close $10,037.72 vs 09:30 $10,000.00 (session +63.84) | 16:00 close · cash $14,948.61 · equity $10,037.72 vs 09:30 $10,000.00 (+37.72; session marks +63.84) · 8 name(s) marked open→close (per-name table). WWW×30 09:30 $20.60 → close $21.03 -12.90; FOSL×110 09:30 $5.64 → close $5.57 +7.70; AIRS×185 09:30 $3.37 → close $3.43 -11.10; OMER×36 09:30 $17.35 → close $17.19 +5.76; MXCT×449 09:30 $1.39 → close $1.32 +31.43; AVAH×52 09:30 $11.91 → close $12.32 -21.32; CRMD×77 09:30 $8.05 → close $7.54 +39.27; LVWR×500 09:30 $1.25 → close $1.20 +25.00 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,948.61 | ▲ 09:30 equity $10,069.07 vs yday $10,037.72 (+31.35) | 09:30 open · cash $14,948.61 (unchanged overnight, no fees) · equity $10,069.07 vs prior close $10,037.72 (+31.35) · 8 name(s) re-marked at the open (per-name table). WWW×30 yday $21.03 → 09:30 $20.98 +1.50; FOSL×110 yday $5.57 → 09:30 $5.50 +7.70; AIRS×185 yday $3.43 → 09:30 $3.40 +6.48; OMER×36 yday $17.19 → 09:30 $17.17 +0.72; MXCT×449 yday $1.32 → 09:30 $1.32 -0.00; AVAH×52 yday $12.32 → 09:30 $12.21 +5.72; CRMD×77 yday $7.54 → 09:30 $7.55 -0.77; LVWR×500 yday $1.20 → 09:30 $1.18 +10.00 | — |
| 2026-08-17 09:30 ET | **COVER** | `WWW` | 30 | $20.98 | $2.08 | $-15.60 | $14,317.13 | ▼ -15.60 after sell → book $10,066.99; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `FOSL` | 110 | $5.50 | $2.32 | $+10.71 | $13,709.81 | ▲ +10.71 after sell → book $10,064.67; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AIRS` | 185 | $3.40 | $2.54 | $-9.77 | $13,079.19 | ▼ -9.77 after sell → book $10,062.12; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `OMER` | 36 | $17.17 | $2.10 | $+2.25 | $12,458.97 | ▲ +2.25 after sell → book $10,060.02; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `MXCT` | 449 | $1.32 | $5.79 | $+19.74 | $11,860.50 | ▲ +19.74 after sell → book $10,054.23; vs 09:30 mark -5.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AVAH` | 52 | $12.21 | $2.15 | $-19.93 | $11,223.44 | ▼ -19.93 after sell → book $10,052.09; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **COVER** | `CRMD` | 77 | $7.55 | $2.22 | $+34.02 | $10,639.87 | ▲ +34.02 after sell → book $10,049.87; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `LVWR` | 500 | $1.18 | $6.45 | $+21.99 | $10,043.42 | ▲ +21.99 after sell → book $10,043.42; vs 09:30 mark -6.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `HNST` | 130 | $4.81 | $2.43 | — | $10,666.29 | — | alarm; gate alarm=True; list flatten; ⚪; ret5=-11.4; leftover $627.71 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `FCEL` | 28 | $22.37 | $2.11 | — | $11,290.54 | — | alarm; gate alarm=True; list probable,yday_gainer; ⚪; ret5=+9.5; leftover $627.71 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `BW` | 60 | $10.35 | $2.21 | — | $11,909.33 | — | alarm; gate alarm=True; list probable; ⚪; ret5=+9.8; leftover $627.71 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `INO` | 586 | $1.07 | $7.68 | — | $12,528.66 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+62.7; leftover $627.71 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `BYND` | 48 | $12.83 | $2.17 | — | $13,142.33 | — | alarm; gate alarm=True; list yday_gainer,yday_mover,oppset; ⚪; ret5=-34.1; leftover $627.71 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
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
| 2026-08-21 09:30 ET | **SHORT** | `YSS` | 91 | $9.26 | $2.31 | — | $10,978.10 | — | alarm; gate alarm=True; list yday_mover; ret5=-20.1; leftover $844.81 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SMJF` | 74 | $11.35 | $2.26 | — | $11,815.74 | — | alarm; gate alarm=True; list ohlc_hot,oppset; ret5=+13.4; leftover $844.81 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 31 | $27.00 | $2.13 | — | $12,650.62 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $844.81 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CPRT` | 24 | $34.48 | $2.10 | — | $13,476.03 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+16.8; leftover $844.81 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `FLO` | 122 | $6.90 | $2.41 | — | $14,315.42 | — | alarm; gate alarm=True; list earn_react; ret5=-5.7; leftover $844.81 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SSYS` | 106 | $7.94 | $2.36 | — | $15,154.71 | — | alarm; gate alarm=True; list oppset; ret5=-12.4; leftover $844.81 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,154.71 | ▼ close $10,118.21 vs 09:30 $10,137.75 (session -5.98) | 16:00 close · cash $15,154.71 · equity $10,118.21 vs 09:30 $10,137.75 (-19.54; session marks -5.98) · 6 name(s) marked open→close (per-name table). YSS×91 09:30 $9.26 → close $9.32 -5.46; SMJF×74 09:30 $11.35 → close $11.41 -4.44; NOG×31 09:30 $27.00 → close $27.34 -10.54; CPRT×24 09:30 $34.48 → close $33.80 +16.32; FLO×122 09:30 $6.90 → close $6.95 -6.10; SSYS×106 09:30 $7.94 → close $7.90 +4.24 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,154.71 | ▲ 09:30 equity $10,140.05 vs yday $10,118.21 (+21.84) | 09:30 open · cash $15,154.71 (unchanged overnight, no fees) · equity $10,140.05 vs prior close $10,118.21 (+21.84) · 6 name(s) re-marked at the open (per-name table). YSS×91 yday $9.32 → 09:30 $9.22 +9.10; SMJF×74 yday $11.41 → 09:30 $11.25 +11.84; NOG×31 yday $27.34 → 09:30 $27.12 +6.82; CPRT×24 yday $33.80 → 09:30 $34.04 -5.76; FLO×122 yday $6.95 → 09:30 $6.96 -1.22; SSYS×106 yday $7.90 → 09:30 $7.89 +1.06 | — |
| 2026-08-24 09:30 ET | **COVER** | `YSS` | 91 | $9.22 | $2.26 | $-0.93 | $14,313.42 | ▼ -0.93 after sell → book $10,137.78; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `SMJF` | 74 | $11.25 | $2.21 | $+2.93 | $13,478.71 | ▲ +2.93 after sell → book $10,135.57; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `NOG` | 31 | $27.12 | $2.08 | $-7.93 | $12,635.91 | ▼ -7.93 after sell → book $10,133.49; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **COVER** | `CPRT` | 24 | $34.04 | $2.06 | $+6.39 | $11,816.89 | ▲ +6.39 after sell → book $10,131.43; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `FLO` | 122 | $6.96 | $2.36 | $-12.09 | $10,965.41 | ▼ -12.09 after sell → book $10,129.07; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `SSYS` | 106 | $7.89 | $2.31 | $+0.63 | $10,126.76 | ▲ +0.63 after sell → book $10,126.76; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,126.76 | ▲ close $10,126.76 vs 09:30 $10,140.05 (session +0.00) | 16:00 close · cash $10,126.76 · no lots left · equity $10,126.76. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,126.76 | ▲ 09:30 equity $10,126.76 vs yday $10,126.76 (+0.00) | 09:30 open · cash $10,126.76 · no holdings · equity $10,126.76 vs prior close $10,126.76 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,126.76 | ▲ close $10,126.76 vs 09:30 $10,126.76 (session +0.00) | 16:00 close · cash $10,126.76 · no lots left · equity $10,126.76. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,126.76 | ▲ 09:30 equity $10,126.76 vs yday $10,126.76 (+0.00) | 09:30 open · cash $10,126.76 · no holdings · equity $10,126.76 vs prior close $10,126.76 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,126.76 | ▲ close $10,126.76 vs 09:30 $10,126.76 (session +0.00) | 16:00 close · cash $10,126.76 · no lots left · equity $10,126.76. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,126.76 | ▲ 09:30 equity $10,126.76 vs yday $10,126.76 (+0.00) | 09:30 open · cash $10,126.76 · no holdings · equity $10,126.76 vs prior close $10,126.76 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 09:30 ET | **SHORT** | `AVEX` | 34 | $18.43 | $2.13 | — | $10,751.25 | — | alarm; gate alarm=True; list probable,yday_gainer,yday_mover; ret5=-3.0; leftover $632.92 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `BKSY` | 25 | $25.29 | $2.10 | — | $11,381.40 | — | alarm; gate alarm=True; list yday_gainer; ret5=-11.5; leftover $632.92 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `BRR` | 289 | $2.19 | $3.80 | — | $12,010.51 | — | alarm; gate alarm=True; list yday_gainer; ret5=+3.3; leftover $632.92 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `USDE` | 97 | $6.50 | $2.32 | — | $12,638.68 | — | alarm; gate alarm=True; list yday_mover; ⚪; ret5=+93.5; leftover $632.92 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟡 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `SUJA` | 67 | $9.41 | $2.23 | — | $13,266.92 | — | alarm; gate alarm=True; list yday_mover; ret5=+27.7; leftover $632.92 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `BYND` | 44 | $14.20 | $2.16 | — | $13,889.56 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+1.2; leftover $632.92 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `FUTU` | 4 | $128.00 | $2.04 | — | $14,399.53 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+16.4; leftover $632.92 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `HNST` | 111 | $5.67 | $2.37 | — | $15,025.97 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+13.9; leftover $632.92 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,025.97 | ▼ close $10,005.46 vs 09:30 $10,126.76 (session -102.14) | 16:00 close · cash $15,025.97 · equity $10,005.46 vs 09:30 $10,126.76 (-121.30; session marks -102.14) · 8 name(s) marked open→close (per-name table). AVEX×34 09:30 $18.43 → close $18.76 -11.22; BKSY×25 09:30 $25.29 → close $24.76 +13.25; BRR×289 09:30 $2.19 → close $2.16 +8.67; USDE×97 09:30 $6.50 → close $8.07 -152.29; SUJA×67 09:30 $9.41 → close $9.00 +27.47; BYND×44 09:30 $14.20 → close $14.00 +8.80; FUTU×4 09:30 $128.00 → close $124.57 +13.72; HNST×111 09:30 $5.67 → close $5.76 -10.54 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,025.97 | ▲ 09:30 equity $10,089.04 vs yday $10,005.46 (+83.58) | 09:30 open · cash $15,025.97 (unchanged overnight, no fees) · equity $10,089.04 vs prior close $10,005.46 (+83.58) · 8 name(s) re-marked at the open (per-name table). AVEX×34 yday $18.76 → 09:30 $18.75 +0.34; BKSY×25 yday $24.76 → 09:30 $24.44 +8.00; BRR×289 yday $2.16 → 09:30 $2.16 -0.00; USDE×97 yday $8.07 → 09:30 $7.24 +80.51; SUJA×67 yday $9.00 → 09:30 $9.08 -5.36; BYND×44 yday $14.00 → 09:30 $14.00 -0.00; FUTU×4 yday $124.57 → 09:30 $124.27 +1.20; HNST×111 yday $5.76 → 09:30 $5.77 -1.11 | — |
| 2026-08-28 09:30 ET | **COVER** | `AVEX` | 34 | $18.75 | $2.09 | $-15.10 | $14,386.38 | ▼ -15.10 after sell → book $10,086.95; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `BKSY` | 25 | $24.44 | $2.06 | $+17.08 | $13,773.32 | ▲ +17.08 after sell → book $10,084.89; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `BRR` | 289 | $2.16 | $3.73 | $+1.14 | $13,145.35 | ▲ +1.14 after sell → book $10,081.16; vs 09:30 mark -3.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `USDE` | 97 | $7.24 | $2.28 | $-76.39 | $12,440.79 | ▼ -76.39 after sell → book $10,078.88; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `SUJA` | 67 | $9.08 | $2.19 | $+17.69 | $11,830.24 | ▲ +17.69 after sell → book $10,076.69; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `BYND` | 44 | $14.00 | $2.12 | $+4.52 | $11,212.12 | ▲ +4.52 after sell → book $10,074.57; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **COVER** | `FUTU` | 4 | $124.27 | $2.00 | $+10.88 | $10,713.03 | ▲ +10.88 after sell → book $10,072.56; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `HNST` | 111 | $5.77 | $2.32 | $-16.35 | $10,070.24 | ▼ -16.35 after sell → book $10,070.24; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SHORT** | `PYXS` | 189 | $3.32 | $2.62 | — | $10,695.10 | — | alarm; gate alarm=True; list probable,yday_gainer; ret5=+6.4; leftover $629.39 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SAFX` | 1724 | $0.36 | $11.78 | — | $11,312.59 | — | alarm; gate alarm=True; list probable,yday_gainer; ret5=+7.6; leftover $629.39 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `XPOF` | 116 | $5.38 | $2.38 | — | $11,934.28 | — | alarm; gate alarm=True; list probable,yday_gainer; ret5=+6.5; leftover $629.39 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `APMD` | 21 | $29.01 | $2.09 | — | $12,541.40 | — | alarm; gate alarm=True; list yday_gainer; ret5=+0.6; leftover $629.39 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `OPTU` | 629 | $1.00 | $8.25 | — | $13,162.16 | — | alarm; gate alarm=True; list yday_gainer; ret5=+16.8; leftover $629.39 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `ABTC` | 73 | $8.61 | $2.25 | — | $13,788.44 | — | alarm; gate alarm=True; list yday_mover; ret5=+3.4; leftover $629.39 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SBET` | 72 | $8.65 | $2.25 | — | $14,408.99 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+17.0; leftover $629.39 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `CRCL` | 6 | $92.61 | $2.04 | — | $14,962.61 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+12.6; leftover $629.39 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,962.61 | ▲ close $10,163.24 vs 09:30 $10,089.04 (session +126.65) | 16:00 close · cash $14,962.61 · equity $10,163.24 vs 09:30 $10,089.04 (+74.20; session marks +126.65) · 8 name(s) marked open→close (per-name table). PYXS×189 09:30 $3.32 → close $3.23 +17.01; SAFX×1724 09:30 $0.36 → close $0.36 +10.34; XPOF×116 09:30 $5.38 → close $5.43 -5.80; APMD×21 09:30 $29.01 → close $29.71 -14.70; OPTU×629 09:30 $1.00 → close $1.02 -12.58; ABTC×73 09:30 $8.61 → close $7.69 +67.16; SBET×72 09:30 $8.65 → close $8.20 +32.40; CRCL×6 09:30 $92.61 → close $87.14 +32.82 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,962.61 | ▼ 09:30 equity $10,145.82 vs yday $10,163.24 (-17.42) | 09:30 open · cash $14,962.61 (unchanged overnight, no fees) · equity $10,145.82 vs prior close $10,163.24 (-17.42) · 8 name(s) re-marked at the open (per-name table). PYXS×189 yday $3.23 → 09:30 $3.20 +5.67; SAFX×1724 yday $0.36 → 09:30 $0.36 -5.17; XPOF×116 yday $5.43 → 09:30 $5.37 +6.96; APMD×21 yday $29.71 → 09:30 $29.71 -0.00; OPTU×629 yday $1.02 → 09:30 $1.06 -25.16; ABTC×73 yday $7.69 → 09:30 $7.66 +2.56; SBET×72 yday $8.20 → 09:30 $8.24 -2.88; CRCL×6 yday $87.14 → 09:30 $87.04 +0.60 | — |
| 2026-08-31 09:30 ET | **COVER** | `PYXS` | 189 | $3.20 | $2.56 | $+17.51 | $14,355.25 | ▲ +17.51 after sell → book $10,143.26; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SAFX` | 1724 | $0.36 | $11.41 | $-18.02 | $13,719.75 | ▼ -18.02 after sell → book $10,131.85; vs 09:30 mark -11.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `XPOF` | 116 | $5.37 | $2.34 | $-3.56 | $13,094.49 | ▼ -3.56 after sell → book $10,129.51; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `APMD` | 21 | $29.71 | $2.05 | $-18.84 | $12,468.53 | ▼ -18.84 after sell → book $10,127.46; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `OPTU` | 629 | $1.06 | $8.11 | $-54.10 | $11,793.68 | ▼ -54.10 after sell → book $10,119.34; vs 09:30 mark -8.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABTC` | 73 | $7.66 | $2.21 | $+65.26 | $11,232.65 | ▲ +65.26 after sell → book $10,117.13; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SBET` | 72 | $8.24 | $2.21 | $+25.07 | $10,637.17 | ▲ +25.07 after sell → book $10,114.93; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRCL` | 6 | $87.04 | $2.01 | $+29.37 | $10,112.92 | ▲ +29.37 after sell → book $10,112.92; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,112.92 | ▲ close $10,112.92 vs 09:30 $10,145.82 (session +0.00) | 16:00 close · cash $10,112.92 · no lots left · equity $10,112.92. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,112.92 | ▲ 09:30 equity $10,112.92 vs yday $10,112.92 (-0.00) | 09:30 open · cash $10,112.92 · no holdings · equity $10,112.92 vs prior close $10,112.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,112.92 | ▲ close $10,112.92 vs 09:30 $10,112.92 (session +0.00) | 16:00 close · cash $10,112.92 · no lots left · equity $10,112.92. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,112.92 | ▲ 09:30 equity $10,112.92 vs yday $10,112.92 (-0.00) | 09:30 open · cash $10,112.92 · no holdings · equity $10,112.92 vs prior close $10,112.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,112.92 | ▲ close $10,112.92 vs 09:30 $10,112.92 (session +0.00) | 16:00 close · cash $10,112.92 · no lots left · equity $10,112.92. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,112.92 | ▲ 09:30 equity $10,112.92 vs yday $10,112.92 (-0.00) | 09:30 open · cash $10,112.92 · no holdings · equity $10,112.92 vs prior close $10,112.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,112.92 | ▲ close $10,112.92 vs 09:30 $10,112.92 (session +0.00) | 16:00 close · cash $10,112.92 · no lots left · equity $10,112.92. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,112.92 | ▲ 09:30 equity $10,112.92 vs yday $10,112.92 (-0.00) | 09:30 open · cash $10,112.92 · no holdings · equity $10,112.92 vs prior close $10,112.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,112.92 | ▲ close $10,112.92 vs 09:30 $10,112.92 (session +0.00) | 16:00 close · cash $10,112.92 · no lots left · equity $10,112.92. | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,112.92 | ▲ 09:30 equity $10,112.92 vs yday $10,112.92 (-0.00) | 09:30 open · cash $10,112.92 · no holdings · equity $10,112.92 vs prior close $10,112.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,112.92 | ▲ close $10,112.92 vs 09:30 $10,112.92 (session +0.00) | 16:00 close · cash $10,112.92 · no lots left · equity $10,112.92. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,112.92 | ▲ 09:30 equity $10,112.92 vs yday $10,112.92 (-0.00) | 09:30 open · cash $10,112.92 · no holdings · equity $10,112.92 vs prior close $10,112.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,112.92 | ▲ close $10,112.92 vs 09:30 $10,112.92 (session +0.00) | 16:00 close · cash $10,112.92 · no lots left · equity $10,112.92. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,112.92 | ▲ 09:30 equity $10,112.92 vs yday $10,112.92 (-0.00) | 09:30 open · cash $10,112.92 · no holdings · equity $10,112.92 vs prior close $10,112.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,112.92 | ▲ close $10,112.92 vs 09:30 $10,112.92 (session +0.00) | 16:00 close · cash $10,112.92 · no lots left · equity $10,112.92. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,112.92 | ▲ 09:30 equity $10,112.92 vs yday $10,112.92 (-0.00) | 09:30 open · cash $10,112.92 · no holdings · equity $10,112.92 vs prior close $10,112.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,112.92 | ▲ close $10,112.92 vs 09:30 $10,112.92 (session +0.00) | 16:00 close · cash $10,112.92 · no lots left · equity $10,112.92. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,112.92 | ▲ 09:30 equity $10,112.92 vs yday $10,112.92 (-0.00) | 09:30 open · cash $10,112.92 · no holdings · equity $10,112.92 vs prior close $10,112.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,112.92 | ▲ close $10,112.92 vs 09:30 $10,112.92 (session +0.00) | 16:00 close · cash $10,112.92 · no lots left · equity $10,112.92. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,112.92 | ▲ 09:30 equity $10,112.92 vs yday $10,112.92 (-0.00) | 09:30 open · cash $10,112.92 · no holdings · equity $10,112.92 vs prior close $10,112.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,112.92 | ▲ close $10,112.92 vs 09:30 $10,112.92 (session +0.00) | 16:00 close · cash $10,112.92 · no lots left · equity $10,112.92. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,112.92 | ▲ 09:30 equity $10,112.92 vs yday $10,112.92 (-0.00) | 09:30 open · cash $10,112.92 · no holdings · equity $10,112.92 vs prior close $10,112.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,112.92 | ▲ close $10,112.92 vs 09:30 $10,112.92 (session +0.00) | 16:00 close · cash $10,112.92 · no lots left · equity $10,112.92. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,112.92 | ▲ 09:30 equity $10,112.92 vs yday $10,112.92 (-0.00) | 09:30 open · cash $10,112.92 · no holdings · equity $10,112.92 vs prior close $10,112.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 09:30 ET | **SHORT** | `FRO` | 11 | $54.31 | $2.06 | — | $10,708.27 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+13.7; leftover $632.06 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `CVI` | 12 | $51.88 | $2.06 | — | $11,328.77 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+11.1; leftover $632.06 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `DHT` | 27 | $22.97 | $2.11 | — | $11,946.85 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+8.7; leftover $632.06 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `ATRC` | 10 | $57.96 | $2.06 | — | $12,524.39 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+7.8; leftover $632.06 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `HAFN` | 64 | $9.75 | $2.22 | — | $13,146.17 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+6.0; leftover $632.06 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `VLO` | 1 | $398.45 | $2.02 | — | $13,542.60 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+6.7; leftover $632.06 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `BWIN` | 19 | $32.06 | $2.08 | — | $14,149.66 | — | alarm; gate alarm=True; list oppset; ret5=-3.7; leftover $632.06 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `MNR` | 57 | $11.00 | $2.20 | — | $14,774.46 | — | alarm; gate alarm=True; list oppset; ret5=+1.7; leftover $632.06 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,774.46 | ▼ close $10,056.16 vs 09:30 $10,112.92 (session -39.95) | 16:00 close · cash $14,774.46 · equity $10,056.16 vs 09:30 $10,112.92 (-56.76; session marks -39.95) · 8 name(s) marked open→close (per-name table). FRO×11 09:30 $54.31 → close $54.03 +3.08; CVI×12 09:30 $51.88 → close $54.14 -27.12; DHT×27 09:30 $22.97 → close $22.82 +4.05; ATRC×10 09:30 $57.96 → close $59.12 -11.60; HAFN×64 09:30 $9.75 → close $9.72 +1.92; VLO×1 09:30 $398.45 → close $412.53 -14.08; BWIN×19 09:30 $32.06 → close $31.95 +2.09; MNR×57 09:30 $11.00 → close $10.97 +1.71 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,774.46 | ▲ 09:30 equity $10,077.58 vs yday $10,056.16 (+21.42) | 09:30 open · cash $14,774.46 (unchanged overnight, no fees) · equity $10,077.58 vs prior close $10,056.16 (+21.42) · 8 name(s) re-marked at the open (per-name table). FRO×11 yday $54.03 → 09:30 $51.19 +31.24; CVI×12 yday $54.14 → 09:30 $54.10 +0.48; DHT×27 yday $22.82 → 09:30 $23.16 -9.18; ATRC×10 yday $59.12 → 09:30 $58.51 +6.10; HAFN×64 yday $9.72 → 09:30 $9.85 -8.32; VLO×1 yday $412.53 → 09:30 $412.00 +0.53; BWIN×19 yday $31.95 → 09:30 $31.98 -0.57; MNR×57 yday $10.97 → 09:30 $10.95 +1.14 | — |
| 2026-09-18 09:30 ET | **COVER** | `FRO` | 11 | $51.19 | $2.02 | $+30.24 | $14,209.34 | ▲ +30.24 after sell → book $10,075.55; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `CVI` | 12 | $54.10 | $2.03 | $-30.73 | $13,558.12 | ▼ -30.73 after sell → book $10,073.53; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **COVER** | `DHT` | 27 | $23.16 | $2.07 | $-9.31 | $12,930.73 | ▼ -9.31 after sell → book $10,071.46; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `ATRC` | 10 | $58.51 | $2.02 | $-9.58 | $12,343.61 | ▼ -9.58 after sell → book $10,069.44; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **COVER** | `HAFN` | 64 | $9.85 | $2.18 | $-10.80 | $11,711.03 | ▼ -10.80 after sell → book $10,067.26; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `VLO` | 1 | $412.00 | $1.99 | $-17.57 | $11,297.03 | ▼ -17.57 after sell → book $10,065.26; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `MNR` | 57 | $10.95 | $2.16 | $-1.51 | $10,670.72 | ▼ -1.51 after sell → book $10,063.10; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SHORT** | `SWRD` | 345 | $2.08 | $4.54 | — | $11,383.78 | — | alarm; gate alarm=True; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $718.79 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SHORT** | `BRR` | 201 | $3.57 | $2.66 | — | $12,098.69 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+15.4; leftover $718.79 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SHORT** | `SMTC` | 3 | $182.33 | $2.03 | — | $12,643.65 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+12.0; leftover $718.79 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SHORT** | `DELL` | 1 | $593.15 | $2.03 | — | $13,234.77 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+16.1; leftover $718.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SHORT** | `IQ` | 641 | $1.12 | $8.40 | — | $13,944.28 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+17.4; leftover $718.79 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SHORT** | `CBC` | 22 | $31.64 | $2.10 | — | $14,638.27 | — | alarm; gate alarm=True; list oppset; ret5=+1.3; leftover $718.79 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SHORT** | `LIFE` | 19 | $36.89 | $2.09 | — | $15,337.09 | — | alarm; gate alarm=True; list oppset; ret5=-6.3; leftover $718.79 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,337.09 | ▲ close $10,077.74 vs 09:30 $10,077.58 (session +38.49) | 16:00 close · cash $15,337.09 · equity $10,077.74 vs 09:30 $10,077.58 (+0.16; session marks +38.49) · 8 name(s) marked open→close (per-name table). BWIN×19 09:30 $31.98 → close $31.93 +0.95; SWRD×345 09:30 $2.08 → close $2.15 -24.15; BRR×201 09:30 $3.57 → close $3.76 -38.19; SMTC×3 09:30 $182.33 → close $185.00 -8.01; DELL×1 09:30 $593.15 → close $568.06 +25.09; IQ×641 09:30 $1.12 → close $1.01 +70.51; CBC×22 09:30 $31.64 → close $31.85 -4.62; LIFE×19 09:30 $36.89 → close $36.00 +16.91 | — |

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
| 2026-08-31 | `PCG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SUNB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GAP` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACM` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `XHLD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BAND` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CLOV` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `PGNY` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ECHO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `WLTH` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `HLP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `DBI` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TJGC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `VOD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WCC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `BBD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `AYA` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `BWIN` | 19 | 2026-09-17 @ $32.06 | alarm; gate alarm=True; list oppset; ret5=-3.7; leftover $632.06 |
| `SWRD` | 345 | 2026-09-18 @ $2.08 | alarm; gate alarm=True; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $718.79 |
| `BRR` | 201 | 2026-09-18 @ $3.57 | alarm; gate alarm=True; list ohlc_hot; ret5=+15.4; leftover $718.79 |
| `SMTC` | 3 | 2026-09-18 @ $182.33 | alarm; gate alarm=True; list ohlc_hot; ret5=+12.0; leftover $718.79 |
| `DELL` | 1 | 2026-09-18 @ $593.15 | alarm; gate alarm=True; list ohlc_hot; ret5=+16.1; leftover $718.79 |
| `IQ` | 641 | 2026-09-18 @ $1.12 | alarm; gate alarm=True; list ohlc_hot; ret5=+17.4; leftover $718.79 |
| `CBC` | 22 | 2026-09-18 @ $31.64 | alarm; gate alarm=True; list oppset; ret5=+1.3; leftover $718.79 |
| `LIFE` | 19 | 2026-09-18 @ $36.89 | alarm; gate alarm=True; list oppset; ret5=-6.3; leftover $718.79 |
