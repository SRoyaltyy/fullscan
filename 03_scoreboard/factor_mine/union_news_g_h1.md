# Factor mine action — `union_news_g_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_g, no 🚨

Cash book **+5.49%** ($10,549) · signal-only (no cash/fees) was +10.93%. Starts YES **15/21**. Fills 137 · skips 58 · realized $+650.79.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the news camera (does the morning packet like the headline?) is green.
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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $692.93.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `TLN` | 3 | — | $359.83 | +0.00 | $362.74 | +8.73 | +8.73 | +0.00 | +8.73 |
| 2026-08-14 | `VST` | 8 | — | $146.90 | +0.00 | $148.13 | +9.84 | +9.84 | +0.00 | +9.84 |
| 2026-08-14 | `NRG` | 10 | — | $120.00 | +0.00 | $126.24 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-08-14 | `ANGX` | 290 | — | $4.31 | +0.00 | $4.37 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-14 | `ARX` | 63 | — | $19.57 | +0.00 | $19.58 | +0.63 | +0.63 | +0.00 | +0.63 |
| 2026-08-14 | `MH` | 92 | — | $13.55 | +0.00 | $13.10 | -41.40 | -41.40 | +0.00 | -41.40 |
| 2026-08-14 | `HLIT` | 94 | — | $13.18 | +0.00 | $13.92 | +69.56 | +69.56 | +0.00 | +69.56 |
| 2026-08-17 | `TLN` | 3 | $362.74 | $367.88 | +15.42 | — | +0.00 | +15.42 | +24.15 | — |
| 2026-08-17 | `VST` | 8 | $148.13 | $149.37 | +9.92 | — | +0.00 | +9.92 | +19.76 | — |
| 2026-08-17 | `NRG` | 10 | $126.24 | $127.40 | +11.60 | — | +0.00 | +11.60 | +74.00 | — |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | — | +0.00 | -0.63 | +0.00 | — |
| 2026-08-17 | `MH` | 92 | $13.10 | $13.16 | +5.52 | — | +0.00 | +5.52 | -35.88 | — |
| 2026-08-17 | `HLIT` | 94 | $13.92 | $13.84 | -7.52 | — | +0.00 | -7.52 | +62.04 | — |
| 2026-08-17 | `DVN` | 44 | — | $46.18 | +0.00 | $47.57 | +61.16 | +61.16 | +0.00 | +61.16 |
| 2026-08-17 | `EOG` | 14 | — | $142.77 | +0.00 | $146.15 | +47.32 | +47.32 | +0.00 | +47.32 |
| 2026-08-17 | `FANG` | 10 | — | $202.70 | +0.00 | $206.29 | +35.90 | +35.90 | +0.00 | +35.90 |
| 2026-08-17 | `CELC` | 21 | — | $92.99 | +0.00 | $92.44 | -11.55 | -11.55 | +0.00 | -11.55 |
| 2026-08-17 | `OUST` | 41 | — | $49.00 | +0.00 | $48.13 | -35.67 | -35.67 | +0.00 | -35.67 |
| 2026-08-18 | `DVN` | 44 | $47.57 | $48.00 | +18.92 | — | +0.00 | +18.92 | +80.08 | — |
| 2026-08-18 | `EOG` | 14 | $146.15 | $148.04 | +26.46 | — | +0.00 | +26.46 | +73.78 | — |
| 2026-08-18 | `FANG` | 10 | $206.29 | $208.93 | +26.40 | — | +0.00 | +26.40 | +62.30 | — |
| 2026-08-18 | `CELC` | 21 | $92.44 | $92.38 | -1.26 | — | +0.00 | -1.26 | -12.81 | — |
| 2026-08-18 | `OUST` | 41 | $48.13 | $45.09 | -124.64 | — | +0.00 | -124.64 | -160.31 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 14 | — | $91.01 | +0.00 | $93.63 | +36.68 | +36.68 | +0.00 | +36.68 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `HUMA` | 1806 | — | $0.71 | +0.00 | $0.68 | -46.96 | -46.96 | +0.00 | -46.96 |
| 2026-08-20 | `BTGO` | 193 | — | $6.61 | +0.00 | $6.60 | -0.97 | -0.97 | +0.00 | -0.97 |
| 2026-08-20 | `ASST` | 79 | — | $16.00 | +0.00 | $16.13 | +10.27 | +10.27 | +0.00 | +10.27 |
| 2026-08-20 | `ZLAB` | 48 | — | $26.57 | +0.00 | $26.02 | -26.40 | -26.40 | +0.00 | -26.40 |
| 2026-08-20 | `CRSP` | 21 | — | $58.73 | +0.00 | $58.12 | -12.81 | -12.81 | +0.00 | -12.81 |
| 2026-08-20 | `APA` | 28 | — | $44.76 | +0.00 | $44.39 | -10.36 | -10.36 | +0.00 | -10.36 |
| 2026-08-21 | `BHP` | 14 | $93.63 | $95.72 | +29.26 | — | +0.00 | +29.26 | +65.94 | — |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | — | +0.00 | -1.68 | -136.24 | — |
| 2026-08-21 | `HUMA` | 1806 | $0.68 | $0.67 | -12.64 | — | +0.00 | -12.64 | -59.60 | — |
| 2026-08-21 | `BTGO` | 193 | $6.60 | $6.95 | +67.55 | — | +0.00 | +67.55 | +66.58 | — |
| 2026-08-21 | `ASST` | 79 | $16.13 | $17.66 | +120.87 | — | +0.00 | +120.87 | +131.14 | — |
| 2026-08-21 | `ZLAB` | 48 | $26.02 | $26.25 | +11.04 | — | +0.00 | +11.04 | -15.36 | — |
| 2026-08-21 | `CRSP` | 21 | $58.12 | $59.72 | +33.60 | $59.50 | -4.62 | +28.98 | +20.79 | +16.17 |
| 2026-08-21 | `APA` | 28 | $44.39 | $44.52 | +3.64 | — | +0.00 | +3.64 | -6.72 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUTL` | 518 | — | $2.47 | +0.00 | $2.41 | -31.08 | -31.08 | +0.00 | -31.08 |
| 2026-08-21 | `FUTU` | 11 | — | $115.18 | +0.00 | $123.64 | +93.06 | +93.06 | +0.00 | +93.06 |
| 2026-08-21 | `DE` | 2 | — | $623.26 | +0.00 | $647.47 | +48.42 | +48.42 | +0.00 | +48.42 |
| 2026-08-21 | `MARA` | 109 | — | $11.70 | +0.00 | $11.26 | -47.96 | -47.96 | +0.00 | -47.96 |
| 2026-08-21 | `BTDR` | 115 | — | $11.10 | +0.00 | $11.37 | +31.62 | +31.62 | +0.00 | +31.62 |
| 2026-08-21 | `HIVE` | 395 | — | $3.24 | +0.00 | $3.03 | -82.95 | -82.95 | +0.00 | -82.95 |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | $57.08 | -35.17 | -50.92 | +0.42 | -34.75 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUTL` | 518 | $2.41 | $2.40 | -5.18 | — | +0.00 | -5.18 | -36.26 | — |
| 2026-08-24 | `FUTU` | 11 | $123.64 | $121.00 | -29.04 | — | +0.00 | -29.04 | +64.02 | — |
| 2026-08-24 | `DE` | 2 | $647.47 | $653.04 | +11.14 | — | +0.00 | +11.14 | +59.56 | — |
| 2026-08-24 | `MARA` | 109 | $11.26 | $11.17 | -9.81 | — | +0.00 | -9.81 | -57.77 | — |
| 2026-08-24 | `BTDR` | 115 | $11.37 | $11.48 | +12.65 | — | +0.00 | +12.65 | +44.27 | — |
| 2026-08-24 | `HIVE` | 395 | $3.03 | $2.99 | -15.80 | — | +0.00 | -15.80 | -98.75 | — |
| 2026-08-25 | `CRSP` | 21 | $57.08 | $57.93 | +17.95 | — | +0.00 | +17.95 | -16.80 | — |
| 2026-08-25 | `RUM` | 153 | — | $9.42 | +0.00 | $10.23 | +123.93 | +123.93 | +0.00 | +123.93 |
| 2026-08-25 | `EZPW` | 41 | — | $35.05 | +0.00 | $35.23 | +7.38 | +7.38 | +0.00 | +7.38 |
| 2026-08-25 | `REAX` | 59 | — | $24.11 | +0.00 | $28.43 | +254.88 | +254.88 | +0.00 | +254.88 |
| 2026-08-25 | `ZYME` | 50 | — | $28.86 | +0.00 | $27.47 | -69.50 | -69.50 | +0.00 | -69.50 |
| 2026-08-25 | `EOLS` | 165 | — | $8.72 | +0.00 | $8.97 | +42.07 | +42.07 | +0.00 | +42.07 |
| 2026-08-25 | `AU` | 12 | — | $118.52 | +0.00 | $123.39 | +58.44 | +58.44 | +0.00 | +58.44 |
| 2026-08-25 | `FCX` | 18 | — | $77.13 | +0.00 | $79.91 | +50.04 | +50.04 | +0.00 | +50.04 |
| 2026-08-26 | `RUM` | 153 | $10.23 | $10.07 | -24.48 | — | +0.00 | -24.48 | +99.45 | — |
| 2026-08-26 | `EZPW` | 41 | $35.23 | $35.70 | +19.27 | — | +0.00 | +19.27 | +26.65 | — |
| 2026-08-26 | `REAX` | 59 | $28.43 | $26.61 | -107.38 | — | +0.00 | -107.38 | +147.50 | — |
| 2026-08-26 | `ZYME` | 50 | $27.47 | $27.56 | +4.50 | — | +0.00 | +4.50 | -65.00 | — |
| 2026-08-26 | `EOLS` | 165 | $8.97 | $8.86 | -18.98 | — | +0.00 | -18.98 | +23.10 | — |
| 2026-08-26 | `AU` | 12 | $123.39 | $119.80 | -43.08 | — | +0.00 | -43.08 | +15.36 | — |
| 2026-08-26 | `FCX` | 18 | $79.91 | $79.34 | -10.26 | — | +0.00 | -10.26 | +39.78 | — |
| 2026-08-26 | `FLNC` | 186 | — | $11.12 | +0.00 | $11.08 | -7.44 | -7.44 | +0.00 | -7.44 |
| 2026-08-26 | `CAPR` | 250 | — | $8.29 | +0.00 | $9.36 | +267.50 | +267.50 | +0.00 | +267.50 |
| 2026-08-26 | `FWRD` | 119 | — | $17.41 | +0.00 | $17.63 | +26.18 | +26.18 | +0.00 | +26.18 |
| 2026-08-26 | `TRLV` | 184 | — | $11.22 | +0.00 | $11.43 | +38.64 | +38.64 | +0.00 | +38.64 |
| 2026-08-26 | `FNV` | 7 | — | $267.02 | +0.00 | $267.37 | +2.45 | +2.45 | +0.00 | +2.45 |
| 2026-08-27 | `FLNC` | 186 | $11.08 | $11.52 | +81.84 | — | +0.00 | +81.84 | +74.40 | — |
| 2026-08-27 | `CAPR` | 250 | $9.36 | $9.19 | -42.50 | — | +0.00 | -42.50 | +225.00 | — |
| 2026-08-27 | `FWRD` | 119 | $17.63 | $17.60 | -3.57 | — | +0.00 | -3.57 | +22.61 | — |
| 2026-08-27 | `TRLV` | 184 | $11.43 | $11.38 | -9.20 | — | +0.00 | -9.20 | +29.44 | — |
| 2026-08-27 | `FNV` | 7 | $267.37 | $267.23 | -0.98 | — | +0.00 | -0.98 | +1.47 | — |
| 2026-08-27 | `RRC` | 43 | — | $41.44 | +0.00 | $41.64 | +8.60 | +8.60 | +0.00 | +8.60 |
| 2026-08-27 | `ACMR` | 21 | — | $81.65 | +0.00 | $80.49 | -24.36 | -24.36 | +0.00 | -24.36 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `ASML` | 1 | — | $1746.53 | +0.00 | $1735.01 | -11.52 | -11.52 | +0.00 | -11.52 |
| 2026-08-27 | `LRCX` | 5 | — | $318.88 | +0.00 | $318.58 | -1.50 | -1.50 | +0.00 | -1.50 |
| 2026-08-27 | `NVDA` | 8 | — | $222.86 | +0.00 | $227.98 | +40.96 | +40.96 | +0.00 | +40.96 |
| 2026-08-28 | `RRC` | 43 | $41.64 | $41.74 | +4.30 | $41.46 | -12.04 | -7.74 | +12.90 | +0.86 |
| 2026-08-28 | `ACMR` | 21 | $80.49 | $79.27 | -25.62 | — | +0.00 | -25.62 | -49.98 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `ASML` | 1 | $1735.01 | $1734.75 | -0.26 | — | +0.00 | -0.26 | -11.78 | — |
| 2026-08-28 | `LRCX` | 5 | $318.58 | $318.03 | -2.75 | — | +0.00 | -2.75 | -4.25 | — |
| 2026-08-28 | `NVDA` | 8 | $227.98 | $227.36 | -4.96 | — | +0.00 | -4.96 | +36.00 | — |
| 2026-08-28 | `SEDG` | 38 | — | $32.90 | +0.00 | $31.41 | -56.62 | -56.62 | +0.00 | -56.62 |
| 2026-08-28 | `OPTX` | 146 | — | $8.61 | +0.00 | $8.52 | -13.14 | -13.14 | +0.00 | -13.14 |
| 2026-08-28 | `CAPR` | 129 | — | $9.73 | +0.00 | $9.59 | -18.06 | -18.06 | +0.00 | -18.06 |
| 2026-08-28 | `SMTC` | 8 | — | $141.76 | +0.00 | $131.17 | -84.72 | -84.72 | +0.00 | -84.72 |
| 2026-08-28 | `ERAS` | 65 | — | $19.25 | +0.00 | $18.03 | -79.30 | -79.30 | +0.00 | -79.30 |
| 2026-08-28 | `BBWI` | 67 | — | $18.75 | +0.00 | $19.22 | +31.49 | +31.49 | +0.00 | +31.49 |
| 2026-08-28 | `ZYME` | 43 | — | $28.91 | +0.00 | $28.27 | -27.52 | -27.52 | +0.00 | -27.52 |
| 2026-08-31 | `RRC` | 43 | $41.46 | $42.00 | +23.22 | — | +0.00 | +23.22 | +24.08 | — |
| 2026-08-31 | `SEDG` | 38 | $31.41 | $31.15 | -9.88 | — | +0.00 | -9.88 | -66.50 | — |
| 2026-08-31 | `OPTX` | 146 | $8.52 | $8.52 | +0.00 | — | +0.00 | +0.00 | -13.14 | — |
| 2026-08-31 | `CAPR` | 129 | $9.59 | $9.50 | -11.61 | — | +0.00 | -11.61 | -29.67 | — |
| 2026-08-31 | `SMTC` | 8 | $131.17 | $132.30 | +9.04 | — | +0.00 | +9.04 | -75.68 | — |
| 2026-08-31 | `ERAS` | 65 | $18.03 | $17.87 | -10.40 | — | +0.00 | -10.40 | -89.70 | — |
| 2026-08-31 | `BBWI` | 67 | $19.22 | $19.25 | +2.01 | — | +0.00 | +2.01 | +33.50 | — |
| 2026-08-31 | `ZYME` | 43 | $28.27 | $28.06 | -9.03 | — | +0.00 | -9.03 | -36.55 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `MMED` | 54 | — | $23.88 | +0.00 | $23.84 | -2.16 | -2.16 | +0.00 | -2.16 |
| 2026-09-03 | `CNXC` | 39 | — | $32.88 | +0.00 | $32.85 | -1.17 | -1.17 | +0.00 | -1.17 |
| 2026-09-03 | `OPTX` | 169 | — | $7.59 | +0.00 | $7.76 | +28.73 | +28.73 | +0.00 | +28.73 |
| 2026-09-03 | `DE` | 1 | — | $703.25 | +0.00 | $694.41 | -8.84 | -8.84 | +0.00 | -8.84 |
| 2026-09-03 | `FRNM` | 81 | — | $15.87 | +0.00 | $16.90 | +83.43 | +83.43 | +0.00 | +83.43 |
| 2026-09-03 | `AVGO` | 3 | — | $351.74 | +0.00 | $357.16 | +16.26 | +16.26 | +0.00 | +16.26 |
| 2026-09-03 | `CIEN` | 3 | — | $354.49 | +0.00 | $317.46 | -111.09 | -111.09 | +0.00 | -111.09 |
| 2026-09-03 | `HPE` | 27 | — | $47.60 | +0.00 | $54.44 | +184.68 | +184.68 | +0.00 | +184.68 |
| 2026-09-04 | `MMED` | 54 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.16 | — |
| 2026-09-04 | `CNXC` | 39 | $32.85 | $32.48 | -14.43 | — | +0.00 | -14.43 | -15.60 | — |
| 2026-09-04 | `OPTX` | 169 | $7.76 | $7.79 | +5.07 | — | +0.00 | +5.07 | +33.80 | — |
| 2026-09-04 | `DE` | 1 | $694.41 | $692.03 | -2.38 | — | +0.00 | -2.38 | -11.22 | — |
| 2026-09-04 | `FRNM` | 81 | $16.90 | $16.40 | -40.50 | $16.31 | -7.29 | -47.79 | +42.93 | +35.64 |
| 2026-09-04 | `AVGO` | 3 | $357.16 | $359.70 | +7.62 | — | +0.00 | +7.62 | +23.88 | — |
| 2026-09-04 | `CIEN` | 3 | $317.46 | $321.67 | +12.63 | — | +0.00 | +12.63 | -98.46 | — |
| 2026-09-04 | `HPE` | 27 | $54.44 | $53.85 | -15.93 | — | +0.00 | -15.93 | +168.75 | — |
| 2026-09-04 | `CRM` | 6 | — | $263.36 | +0.00 | $259.23 | -24.78 | -24.78 | +0.00 | -24.78 |
| 2026-09-04 | `BAK` | 937 | — | $1.94 | +0.00 | $1.89 | -46.85 | -46.85 | +0.00 | -46.85 |
| 2026-09-04 | `MSTR` | 13 | — | $137.35 | +0.00 | $142.80 | +70.85 | +70.85 | +0.00 | +70.85 |
| 2026-09-04 | `BE` | 7 | — | $236.82 | +0.00 | $252.87 | +112.35 | +112.35 | +0.00 | +112.35 |
| 2026-09-04 | `MRX` | 24 | — | $75.65 | +0.00 | $78.27 | +62.88 | +62.88 | +0.00 | +62.88 |
| 2026-09-08 | `FRNM` | 81 | $16.31 | $16.74 | +34.83 | — | +0.00 | +34.83 | +70.47 | — |
| 2026-09-08 | `CRM` | 6 | $259.23 | $253.72 | -33.06 | — | +0.00 | -33.06 | -57.84 | — |
| 2026-09-08 | `BAK` | 937 | $1.89 | $1.94 | +46.85 | — | +0.00 | +46.85 | +0.00 | — |
| 2026-09-08 | `MSTR` | 13 | $142.80 | $137.62 | -67.34 | $136.52 | -14.30 | -81.64 | +3.51 | -10.79 |
| 2026-09-08 | `BE` | 7 | $252.87 | $267.76 | +104.23 | — | +0.00 | +104.23 | +216.58 | — |
| 2026-09-08 | `MRX` | 24 | $78.27 | $78.84 | +13.68 | $76.71 | -51.12 | -37.44 | +76.56 | +25.44 |
| 2026-09-09 | `MSTR` | 13 | $136.52 | $141.82 | +68.90 | — | +0.00 | +68.90 | +58.11 | — |
| 2026-09-09 | `MRX` | 24 | $76.71 | $76.60 | -2.64 | — | +0.00 | -2.64 | +22.80 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 9 | — | $164.43 | +0.00 | $150.28 | -127.35 | -127.35 | +0.00 | -127.35 |
| 2026-09-11 | `AMTX` | 745 | — | $2.04 | +0.00 | $2.01 | -22.35 | -22.35 | +0.00 | -22.35 |
| 2026-09-11 | `BAK` | 717 | — | $2.12 | +0.00 | $2.08 | -28.68 | -28.68 | +0.00 | -28.68 |
| 2026-09-11 | `CLSK` | 117 | — | $12.97 | +0.00 | $13.67 | +81.90 | +81.90 | +0.00 | +81.90 |
| 2026-09-11 | `LITE` | 1 | — | $945.60 | +0.00 | $927.03 | -18.57 | -18.57 | +0.00 | -18.57 |
| 2026-09-11 | `ADBE` | 6 | — | $242.17 | +0.00 | $252.23 | +60.36 | +60.36 | +0.00 | +60.36 |
| 2026-09-11 | `RH` | 11 | — | $135.71 | +0.00 | $134.07 | -18.04 | -18.04 | +0.00 | -18.04 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +127.16 | TLN, VST, NRG, ANGX, ARX, MH, HLIT | — | $1,560.49 | $10,110.67 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94 |
| 2026-08-17 | +2.25 | $1,560.49 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94 | $10,211.68 | +101.01 | +97.16 | DVN, EOG, FANG, CELC, OUST | TLN, VST, NRG, ANGX, ARX, MH, HLIT | $165.17 | $10,281.82 | DVN×44, EOG×14, FANG×10, CELC×21, OUST×41 |
| 2026-08-18 | -6.20 | $165.17 | DVN×44, EOG×14, FANG×10, CELC×21, OUST×41 | $10,227.70 | -54.12 | +0.00 | — | DVN, EOG, FANG, CELC, OUST | $10,217.23 | $10,217.23 | — |
| 2026-08-19 | -7.20 | $10,217.23 | — | $10,217.23 | -0.00 | +0.00 | — | — | $10,217.23 | $10,217.23 | — |
| 2026-08-20 | +1.12 | $10,217.23 | — | $10,217.23 | -0.00 | -185.11 | BHP, MRNA, HUMA, BTGO, ASST, ZLAB, CRSP, APA | — | $131.10 | $9,998.84 | BHP×14, MRNA×8, HUMA×1806, BTGO×193, ASST×79, ZLAB×48, CRSP×21, APA×28 |
| 2026-08-21 | +3.25 | $131.10 | BHP×14, MRNA×8, HUMA×1806, BTGO×193, ASST×79, ZLAB×48, CRSP×21, APA×28 | $10,250.47 | +251.63 | +24.39 | AU, AUTL, FUTU, DE, MARA, BTDR, HIVE | BHP, MRNA, HUMA, BTGO, ASST, ZLAB, APA | $124.50 | $10,221.30 | CRSP×21, AU×10, AUTL×518, FUTU×11, DE×2, MARA×109, BTDR×115, HIVE×395 |
| 2026-08-24 | -5.17 | $124.50 | CRSP×21, AU×10, AUTL×518, FUTU×11, DE×2, MARA×109, BTDR×115, HIVE×395 | $10,162.41 | -58.89 | -35.17 | — | AU, AUTL, FUTU, DE, MARA, BTDR, HIVE | $8,905.90 | $10,104.48 | CRSP×21 |
| 2026-08-25 | +1.80 | $8,905.90 | CRSP×21 | $10,122.43 | +17.95 | +467.24 | RUM, EZPW, REAX, ZYME, EOLS, AU, FCX | CRSP | $111.76 | $10,572.18 | RUM×153, EZPW×41, REAX×59, ZYME×50, EOLS×165, AU×12, FCX×18 |
| 2026-08-26 | +2.02 | $111.76 | RUM×153, EZPW×41, REAX×59, ZYME×50, EOLS×165, AU×12, FCX×18 | $10,391.78 | -180.40 | +327.33 | FLNC, CAPR, FWRD, TRLV, FNV | RUM, EZPW, REAX, ZYME, EOLS, AU, FCX | $217.26 | $10,690.82 | FLNC×186, CAPR×250, FWRD×119, TRLV×184, FNV×7 |
| 2026-08-27 | — | $217.26 | FLNC×186, CAPR×250, FWRD×119, TRLV×184, FNV×7 | $10,716.41 | +25.59 | -19.44 | RRC, ACMR, MU, ASML, LRCX, NVDA | FLNC, CAPR, FWRD, TRLV, FNV | $1,103.96 | $10,671.91 | RRC×43, ACMR×21, MU×1, ASML×1, LRCX×5, NVDA×8 |
| 2026-08-28 | +0.75 | $1,103.96 | RRC×43, ACMR×21, MU×1, ASML×1, LRCX×5, NVDA×8 | $10,626.52 | -45.39 | -259.91 | SEDG, OPTX, CAPR, SMTC, ERAS, BBWI, ZYME | ACMR, MU, ASML, LRCX, NVDA | $158.97 | $10,341.02 | RRC×43, SEDG×38, OPTX×146, CAPR×129, SMTC×8, ERAS×65, BBWI×67, ZYME×43 |
| 2026-08-31 | -5.85 | $158.97 | RRC×43, SEDG×38, OPTX×146, CAPR×129, SMTC×8, ERAS×65, BBWI×67, ZYME×43 | $10,334.37 | -6.65 | +0.00 | — | RRC, SEDG, OPTX, CAPR, SMTC, ERAS, BBWI, ZYME | $10,316.64 | $10,316.64 | — |
| 2026-09-01 | -6.30 | $10,316.64 | — | $10,316.64 | -0.00 | +0.00 | — | — | $10,316.64 | $10,316.64 | — |
| 2026-09-02 | -3.83 | $10,316.64 | — | $10,316.64 | -0.00 | +0.00 | — | — | $10,316.64 | $10,316.64 | — |
| 2026-09-03 | -0.90 | $10,316.64 | — | $10,316.64 | -0.00 | +189.84 | MMED, CNXC, OPTX, DE, FRNM, AVGO, CIEN, HPE | — | $1,052.43 | $10,489.43 | MMED×54, CNXC×39, OPTX×169, DE×1, FRNM×81, AVGO×3, CIEN×3, HPE×27 |
| 2026-09-04 | +2.25 | $1,052.43 | MMED×54, CNXC×39, OPTX×169, DE×1, FRNM×81, AVGO×3, CIEN×3, HPE×27 | $10,441.51 | -47.92 | +167.16 | CRM, BAK, MSTR, BE, MRX | MMED, CNXC, OPTX, DE, AVGO, CIEN, HPE | $421.10 | $10,573.49 | FRNM×81, CRM×6, BAK×937, MSTR×13, BE×7, MRX×24 |
| 2026-09-08 | -11.47 | $421.10 | FRNM×81, CRM×6, BAK×937, MSTR×13, BE×7, MRX×24 | $10,672.68 | +99.19 | -65.42 | — | FRNM, CRM, BAK, BE | $6,972.88 | $10,588.68 | MSTR×13, MRX×24 |
| 2026-09-09 | -13.95 | $6,972.88 | MSTR×13, MRX×24 | $10,654.94 | +66.26 | +0.00 | — | MSTR, MRX | $10,650.80 | $10,650.80 | — |
| 2026-09-10 | -13.28 | $10,650.80 | — | $10,650.80 | -0.00 | +0.00 | — | — | $10,650.80 | $10,650.80 | — |
| 2026-09-11 | +0.50 | $10,650.80 | — | $10,650.80 | -0.00 | -72.73 | ORCL, AMTX, BAK, CLSK, LITE, ADBE, RH | — | $692.93 | $10,548.83 | ORCL×9, AMTX×745, BAK×717, CLSK×117, LITE×1, ADBE×6, RH×11 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $5,285.64 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $4,050.55 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $2,801.68 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $1,560.49 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,560.49 | ▲ close $10,110.67 vs 09:30 $10,000.00 (session +127.16) | 16:00 close · cash $1,560.49 · equity $10,110.67 vs 09:30 $10,000.00 (+110.67; session marks +127.16) · 7 name(s) marked open→close (per-name table). TLN×3 09:30 $359.83 → close $362.74 +8.73; VST×8 09:30 $146.90 → close $148.13 +9.84; NRG×10 09:30 $120.00 → close $126.24 +62.40; ANGX×290 09:30 $4.31 → close $4.37 +17.40; ARX×63 09:30 $19.57 → close $19.58 +0.63; MH×92 09:30 $13.55 → close $13.10 -41.40; HLIT×94 09:30 $13.18 → close $13.92 +69.56 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,560.49 | ▲ 09:30 equity $10,211.68 vs yday $10,110.67 (+101.01) | 09:30 open · cash $1,560.49 (unchanged overnight, no fees) · equity $10,211.68 vs prior close $10,110.67 (+101.01) · 7 name(s) re-marked at the open (per-name table). TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; MH×92 yday $13.10 → 09:30 $13.16 +5.52; HLIT×94 yday $13.92 → 09:30 $13.84 -7.52 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $2,662.11 | ▲ +20.13 after sell → book $10,209.66; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $3,855.04 | ▲ +15.71 after sell → book $10,207.63; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $5,127.00 | ▲ +69.94 after sell → book $10,205.59; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $6,457.20 | ▲ +76.56 after sell → book $10,201.79; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $7,687.91 | ▼ -4.38 after sell → book $10,199.59; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $8,896.34 | ▼ -40.44 after sell → book $10,197.30; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $10,195.00 | ▲ +57.47 after sell → book $10,195.00; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 44 | $46.18 | $2.12 | — | $8,160.96 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; leftover $2039.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 14 | $142.77 | $2.03 | — | $6,160.14 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; leftover $2039.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 10 | $202.70 | $2.02 | — | $4,131.12 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; leftover $2039.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 21 | $92.99 | $2.05 | — | $2,176.28 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; leftover $2039.00 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 41 | $49.00 | $2.11 | — | $165.17 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; leftover $2039.00 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $165.17 | ▲ close $10,281.82 vs 09:30 $10,211.68 (session +97.16) | 16:00 close · cash $165.17 · equity $10,281.82 vs 09:30 $10,211.68 (+70.14; session marks +97.16) · 5 name(s) marked open→close (per-name table). DVN×44 09:30 $46.18 → close $47.57 +61.16; EOG×14 09:30 $142.77 → close $146.15 +47.32; FANG×10 09:30 $202.70 → close $206.29 +35.90; CELC×21 09:30 $92.99 → close $92.44 -11.55; OUST×41 09:30 $49.00 → close $48.13 -35.67 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $165.17 | ▼ 09:30 equity $10,227.70 vs yday $10,281.82 (-54.12) | 09:30 open · cash $165.17 (unchanged overnight, no fees) · equity $10,227.70 vs prior close $10,281.82 (-54.12) · 5 name(s) re-marked at the open (per-name table). DVN×44 yday $47.57 → 09:30 $48.00 +18.92; EOG×14 yday $146.15 → 09:30 $148.04 +26.46; FANG×10 yday $206.29 → 09:30 $208.93 +26.40; CELC×21 yday $92.44 → 09:30 $92.38 -1.26; OUST×41 yday $48.13 → 09:30 $45.09 -124.64 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 44 | $48.00 | $2.15 | $+75.81 | $2,275.02 | ▲ +75.81 after sell → book $10,225.55; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 14 | $148.04 | $2.06 | $+69.69 | $4,345.52 | ▲ +69.69 after sell → book $10,223.49; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 10 | $208.93 | $2.05 | $+58.23 | $6,432.77 | ▲ +58.23 after sell → book $10,221.44; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 21 | $92.38 | $2.08 | $-16.94 | $8,370.67 | ▼ -16.94 after sell → book $10,219.36; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 41 | $45.09 | $2.14 | $-164.56 | $10,217.23 | ▼ -164.56 after sell → book $10,217.23; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,217.23 | ▲ close $10,217.23 vs 09:30 $10,227.70 (session +0.00) | 16:00 close · cash $10,217.23 · no lots left · equity $10,217.23. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,217.23 | ▲ 09:30 equity $10,217.23 vs yday $10,217.23 (-0.00) | 09:30 open · cash $10,217.23 · no holdings · equity $10,217.23 vs prior close $10,217.23 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,217.23 | ▲ close $10,217.23 vs 09:30 $10,217.23 (session +0.00) | 16:00 close · cash $10,217.23 · no lots left · equity $10,217.23. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,217.23 | ▲ 09:30 equity $10,217.23 vs yday $10,217.23 (-0.00) | 09:30 open · cash $10,217.23 · no holdings · equity $10,217.23 vs prior close $10,217.23 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,941.05 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $7,737.92 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1277.15 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1806 | $0.71 | $18.19 | — | $6,442.89 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1277.15 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 193 | $6.61 | $2.57 | — | $5,165.56 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1277.15 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 79 | $16.00 | $2.23 | — | $3,899.33 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 48 | $26.57 | $2.13 | — | $2,621.84 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $1,386.45 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $131.10 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; leftover $1277.15 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.10 | ▼ close $9,998.84 vs 09:30 $10,217.23 (session -185.11) | 16:00 close · cash $131.10 · equity $9,998.84 vs 09:30 $10,217.23 (-218.39; session marks -185.11) · 8 name(s) marked open→close (per-name table). BHP×14 09:30 $91.01 → close $93.63 +36.68; MRNA×8 09:30 $150.14 → close $133.32 -134.56; HUMA×1806 09:30 $0.71 → close $0.68 -46.96; BTGO×193 09:30 $6.61 → close $6.60 -0.97; ASST×79 09:30 $16.00 → close $16.13 +10.27; ZLAB×48 09:30 $26.57 → close $26.02 -26.40; CRSP×21 09:30 $58.73 → close $58.12 -12.81; APA×28 09:30 $44.76 → close $44.39 -10.36 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.10 | ▲ 09:30 equity $10,250.47 vs yday $9,998.84 (+251.63) | 09:30 open · cash $131.10 (unchanged overnight, no fees) · equity $10,250.47 vs prior close $9,998.84 (+251.63) · 8 name(s) re-marked at the open (per-name table). BHP×14 yday $93.63 → 09:30 $95.72 +29.26; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; HUMA×1806 yday $0.68 → 09:30 $0.67 -12.64; BTGO×193 yday $6.60 → 09:30 $6.95 +67.55; ASST×79 yday $16.13 → 09:30 $17.66 +120.87; ZLAB×48 yday $26.02 → 09:30 $26.25 +11.04; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; APA×28 yday $44.39 → 09:30 $44.52 +3.64 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 14 | $95.72 | $2.05 | $+61.86 | $1,469.13 | ▲ +61.86 after sell → book $10,248.42; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $2,531.97 | ▼ -140.29 after sell → book $10,246.39; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1806 | $0.67 | $17.90 | $-95.68 | $3,731.32 | ▼ -95.68 after sell → book $10,228.49; vs 09:30 mark -17.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 193 | $6.95 | $2.61 | $+61.40 | $5,070.06 | ▲ +61.40 after sell → book $10,225.88; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 79 | $17.66 | $2.25 | $+126.66 | $6,462.94 | ▲ +126.66 after sell → book $10,223.62; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 48 | $26.25 | $2.15 | $-19.65 | $7,720.79 | ▼ -19.65 after sell → book $10,221.47; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $8,965.26 | ▼ -10.89 after sell → book $10,219.38; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $7,768.94 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1280.75 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 518 | $2.47 | $6.68 | — | $6,482.79 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1280.75 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $5,213.79 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1280.75 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $3,965.28 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1280.75 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 109 | $11.70 | $2.32 | — | $2,687.66 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1280.75 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 115 | $11.10 | $2.33 | — | $1,409.40 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; leftover $1280.75 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 395 | $3.24 | $5.10 | — | $124.50 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1280.75 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.50 | ▲ close $10,221.30 vs 09:30 $10,250.47 (session +24.39) | 16:00 close · cash $124.50 · equity $10,221.30 vs 09:30 $10,250.47 (-29.17; session marks +24.39) · 8 name(s) marked open→close (per-name table). CRSP×21 09:30 $59.72 → close $59.50 -4.62; AU×10 09:30 $119.43 → close $121.22 +17.90; AUTL×518 09:30 $2.47 → close $2.41 -31.08; FUTU×11 09:30 $115.18 → close $123.64 +93.06; DE×2 09:30 $623.26 → close $647.47 +48.42; MARA×109 09:30 $11.70 → close $11.26 -47.96; BTDR×115 09:30 $11.10 → close $11.37 +31.62; HIVE×395 09:30 $3.24 → close $3.03 -82.95 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.50 | ▼ 09:30 equity $10,162.41 vs yday $10,221.30 (-58.89) | 09:30 open · cash $124.50 (unchanged overnight, no fees) · equity $10,162.41 vs prior close $10,221.30 (-58.89) · 8 name(s) re-marked at the open (per-name table). CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUTL×518 yday $2.41 → 09:30 $2.40 -5.18; FUTU×11 yday $123.64 → 09:30 $121.00 -29.04; DE×2 yday $647.47 → 09:30 $653.04 +11.14; MARA×109 yday $11.26 → 09:30 $11.17 -9.81; BTDR×115 yday $11.37 → 09:30 $11.48 +12.65; HIVE×395 yday $3.03 → 09:30 $2.99 -15.80 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,327.56 | ▲ +6.74 after sell → book $10,160.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 518 | $2.40 | $6.78 | $-49.72 | $2,563.98 | ▼ -49.72 after sell → book $10,153.59; vs 09:30 mark -6.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $121.00 | $2.04 | $+59.95 | $3,892.94 | ▲ +59.95 after sell → book $10,151.55; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $5,197.00 | ▲ +55.55 after sell → book $10,149.53; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 109 | $11.17 | $2.35 | $-62.43 | $6,412.19 | ▼ -62.43 after sell → book $10,147.19; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 115 | $11.48 | $2.36 | $+39.58 | $7,730.02 | ▲ +39.58 after sell → book $10,144.82; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 395 | $2.99 | $5.17 | $-109.02 | $8,905.90 | ▼ -109.02 after sell → book $10,139.65; vs 09:30 mark -5.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,905.90 | ▼ close $10,104.48 vs 09:30 $10,162.41 (session -35.17) | 16:00 close · cash $8,905.90 · equity $10,104.48 vs 09:30 $10,162.41 (-57.93; session marks -35.17) · 1 name(s) marked open→close (per-name table). CRSP×21 09:30 $58.75 → close $57.08 -35.17 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,905.90 | ▲ 09:30 equity $10,122.43 vs yday $10,104.48 (+17.95) | 09:30 open · cash $8,905.90 (unchanged overnight, no fees) · equity $10,122.43 vs prior close $10,104.48 (+17.95) · 1 name(s) re-marked at the open (per-name table). CRSP×21 yday $57.08 → 09:30 $57.93 +17.95 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $10,120.36 | ▼ -20.93 after sell → book $10,120.36; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 153 | $9.42 | $2.45 | — | $8,676.65 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1445.77 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 41 | $35.05 | $2.11 | — | $7,237.49 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1445.77 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 59 | $24.11 | $2.17 | — | $5,812.83 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=+891.7; leftover $1445.77 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 50 | $28.86 | $2.14 | — | $4,367.69 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.7; leftover $1445.77 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 165 | $8.72 | $2.48 | — | $2,926.41 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.0; leftover $1445.77 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 12 | $118.52 | $2.03 | — | $1,502.14 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1445.77 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 18 | $77.13 | $2.04 | — | $111.76 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; ⚪; ret5=+13.8; leftover $1445.77 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.76 | ▲ close $10,572.18 vs 09:30 $10,122.43 (session +467.24) | 16:00 close · cash $111.76 · equity $10,572.18 vs 09:30 $10,122.43 (+449.75; session marks +467.24) · 7 name(s) marked open→close (per-name table). RUM×153 09:30 $9.42 → close $10.23 +123.93; EZPW×41 09:30 $35.05 → close $35.23 +7.38; REAX×59 09:30 $24.11 → close $28.43 +254.88; ZYME×50 09:30 $28.86 → close $27.47 -69.50; EOLS×165 09:30 $8.72 → close $8.97 +42.07; AU×12 09:30 $118.52 → close $123.39 +58.44; FCX×18 09:30 $77.13 → close $79.91 +50.04 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.76 | ▼ 09:30 equity $10,391.78 vs yday $10,572.18 (-180.40) | 09:30 open · cash $111.76 (unchanged overnight, no fees) · equity $10,391.78 vs prior close $10,572.18 (-180.40) · 7 name(s) re-marked at the open (per-name table). RUM×153 yday $10.23 → 09:30 $10.07 -24.48; EZPW×41 yday $35.23 → 09:30 $35.70 +19.27; REAX×59 yday $28.43 → 09:30 $26.61 -107.38; ZYME×50 yday $27.47 → 09:30 $27.56 +4.50; EOLS×165 yday $8.97 → 09:30 $8.86 -18.98; AU×12 yday $123.39 → 09:30 $119.80 -43.08; FCX×18 yday $79.91 → 09:30 $79.34 -10.26 | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 153 | $10.07 | $2.49 | $+94.51 | $1,649.98 | ▲ +94.51 after sell → book $10,389.29; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 41 | $35.70 | $2.13 | $+22.40 | $3,111.55 | ▲ +22.40 after sell → book $10,387.16; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 59 | $26.61 | $2.19 | $+143.14 | $4,679.35 | ▲ +143.14 after sell → book $10,384.97; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 50 | $27.56 | $2.16 | $-69.30 | $6,055.18 | ▼ -69.30 after sell → book $10,382.80; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 165 | $8.86 | $2.52 | $+18.09 | $7,514.56 | ▲ +18.09 after sell → book $10,380.28; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 12 | $119.80 | $2.05 | $+11.29 | $8,950.11 | ▲ +11.29 after sell → book $10,378.23; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 18 | $79.34 | $2.07 | $+35.67 | $10,376.17 | ▲ +35.67 after sell → book $10,376.17; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 186 | $11.12 | $2.55 | — | $8,305.30 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $2075.23 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 250 | $8.29 | $3.23 | — | $6,229.57 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $2075.23 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 119 | $17.41 | $2.35 | — | $4,155.44 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-9.2; leftover $2075.23 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 184 | $11.22 | $2.54 | — | $2,088.42 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.8; leftover $2075.23 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 7 | $267.02 | $2.01 | — | $217.26 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.7; leftover $2075.23 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $217.26 | ▲ close $10,690.82 vs 09:30 $10,391.78 (session +327.33) | 16:00 close · cash $217.26 · equity $10,690.82 vs 09:30 $10,391.78 (+299.04; session marks +327.33) · 5 name(s) marked open→close (per-name table). FLNC×186 09:30 $11.12 → close $11.08 -7.44; CAPR×250 09:30 $8.29 → close $9.36 +267.50; FWRD×119 09:30 $17.41 → close $17.63 +26.18; TRLV×184 09:30 $11.22 → close $11.43 +38.64; FNV×7 09:30 $267.02 → close $267.37 +2.45 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $217.26 | ▲ 09:30 equity $10,716.41 vs yday $10,690.82 (+25.59) | 09:30 open · cash $217.26 (unchanged overnight, no fees) · equity $10,716.41 vs prior close $10,690.82 (+25.59) · 5 name(s) re-marked at the open (per-name table). FLNC×186 yday $11.08 → 09:30 $11.52 +81.84; CAPR×250 yday $9.36 → 09:30 $9.19 -42.50; FWRD×119 yday $17.63 → 09:30 $17.60 -3.57; TRLV×184 yday $11.43 → 09:30 $11.38 -9.20; FNV×7 yday $267.37 → 09:30 $267.23 -0.98 | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 186 | $11.52 | $2.60 | $+69.26 | $2,357.39 | ▲ +69.26 after sell → book $10,713.82; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 250 | $9.19 | $3.28 | $+218.49 | $4,651.60 | ▲ +218.49 after sell → book $10,710.53; vs 09:30 mark -3.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FWRD` | 119 | $17.60 | $2.38 | $+17.88 | $6,743.62 | ▲ +17.88 after sell → book $10,708.15; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 184 | $11.38 | $2.59 | $+24.31 | $8,834.95 | ▲ +24.31 after sell → book $10,705.56; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 7 | $267.23 | $2.04 | $-2.58 | $10,703.52 | ▼ -2.58 after sell → book $10,703.52; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 43 | $41.44 | $2.12 | — | $8,919.49 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+3.1; leftover $1783.92 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 21 | $81.65 | $2.05 | — | $7,202.78 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+2.0; leftover $1783.92 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $6,233.78 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+0.1; leftover $1783.92 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ASML` | 1 | $1746.53 | $1.99 | — | $4,485.26 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=-0.3; leftover $1783.92 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 5 | $318.88 | $2.00 | — | $2,888.85 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+1.9; leftover $1783.92 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 8 | $222.86 | $2.01 | — | $1,103.96 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=-3.6; leftover $1783.92 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,103.96 | ▼ close $10,671.91 vs 09:30 $10,716.41 (session -19.44) | 16:00 close · cash $1,103.96 · equity $10,671.91 vs 09:30 $10,716.41 (-44.50; session marks -19.44) · 6 name(s) marked open→close (per-name table). RRC×43 09:30 $41.44 → close $41.64 +8.60; ACMR×21 09:30 $81.65 → close $80.49 -24.36; MU×1 09:30 $967.01 → close $935.39 -31.62; ASML×1 09:30 $1746.53 → close $1735.01 -11.52; LRCX×5 09:30 $318.88 → close $318.58 -1.50; NVDA×8 09:30 $222.86 → close $227.98 +40.96 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,103.96 | ▼ 09:30 equity $10,626.52 vs yday $10,671.91 (-45.39) | 09:30 open · cash $1,103.96 (unchanged overnight, no fees) · equity $10,626.52 vs prior close $10,671.91 (-45.39) · 6 name(s) re-marked at the open (per-name table). RRC×43 yday $41.64 → 09:30 $41.74 +4.30; ACMR×21 yday $80.49 → 09:30 $79.27 -25.62; MU×1 yday $935.39 → 09:30 $919.29 -16.10; ASML×1 yday $1735.01 → 09:30 $1734.75 -0.26; LRCX×5 yday $318.58 → 09:30 $318.03 -2.75; NVDA×8 yday $227.98 → 09:30 $227.36 -4.96 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 21 | $79.27 | $2.08 | $-54.11 | $2,766.55 | ▼ -54.11 after sell → book $10,624.44; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $3,683.83 | ▼ -51.73 after sell → book $10,622.43; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASML` | 1 | $1734.75 | $2.02 | $-15.79 | $5,416.56 | ▼ -15.79 after sell → book $10,620.41; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 5 | $318.03 | $2.03 | $-8.28 | $7,004.68 | ▼ -8.28 after sell → book $10,618.38; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 8 | $227.36 | $2.04 | $+31.95 | $8,821.53 | ▲ +31.95 after sell → book $10,616.35; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 38 | $32.90 | $2.10 | — | $7,569.22 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1260.22 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 146 | $8.61 | $2.43 | — | $6,309.73 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.7; leftover $1260.22 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 129 | $9.73 | $2.38 | — | $5,052.19 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+47.1; leftover $1260.22 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $3,916.09 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1260.22 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 65 | $19.25 | $2.19 | — | $2,662.66 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+14.1; leftover $1260.22 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 67 | $18.75 | $2.19 | — | $1,404.22 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-5.0; leftover $1260.22 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 43 | $28.91 | $2.12 | — | $158.97 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+9.2; leftover $1260.22 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.97 | ▼ close $10,341.02 vs 09:30 $10,626.52 (session -259.91) | 16:00 close · cash $158.97 · equity $10,341.02 vs 09:30 $10,626.52 (-285.50; session marks -259.91) · 8 name(s) marked open→close (per-name table). RRC×43 09:30 $41.74 → close $41.46 -12.04; SEDG×38 09:30 $32.90 → close $31.41 -56.62; OPTX×146 09:30 $8.61 → close $8.52 -13.14; CAPR×129 09:30 $9.73 → close $9.59 -18.06; SMTC×8 09:30 $141.76 → close $131.17 -84.72; ERAS×65 09:30 $19.25 → close $18.03 -79.30; BBWI×67 09:30 $18.75 → close $19.22 +31.49; ZYME×43 09:30 $28.91 → close $28.27 -27.52 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.97 | ▼ 09:30 equity $10,334.37 vs yday $10,341.02 (-6.65) | 09:30 open · cash $158.97 (unchanged overnight, no fees) · equity $10,334.37 vs prior close $10,341.02 (-6.65) · 8 name(s) re-marked at the open (per-name table). RRC×43 yday $41.46 → 09:30 $42.00 +23.22; SEDG×38 yday $31.41 → 09:30 $31.15 -9.88; OPTX×146 yday $8.52 → 09:30 $8.52 +0.00; CAPR×129 yday $9.59 → 09:30 $9.50 -11.61; SMTC×8 yday $131.17 → 09:30 $132.30 +9.04; ERAS×65 yday $18.03 → 09:30 $17.87 -10.40; BBWI×67 yday $19.22 → 09:30 $19.25 +2.01; ZYME×43 yday $28.27 → 09:30 $28.06 -9.03 | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 43 | $42.00 | $2.14 | $+19.82 | $1,962.82 | ▲ +19.82 after sell → book $10,332.22; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 38 | $31.15 | $2.12 | $-70.73 | $3,144.40 | ▼ -70.73 after sell → book $10,330.10; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 146 | $8.52 | $2.46 | $-18.03 | $4,385.86 | ▼ -18.03 after sell → book $10,327.64; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 129 | $9.50 | $2.41 | $-34.46 | $5,608.95 | ▼ -34.46 after sell → book $10,325.23; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $132.30 | $2.03 | $-79.73 | $6,665.32 | ▼ -79.73 after sell → book $10,323.20; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 65 | $17.87 | $2.21 | $-94.09 | $7,824.66 | ▼ -94.09 after sell → book $10,320.99; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 67 | $19.25 | $2.21 | $+29.10 | $9,112.20 | ▲ +29.10 after sell → book $10,318.78; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 43 | $28.06 | $2.14 | $-40.81 | $10,316.64 | ▼ -40.81 after sell → book $10,316.64; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,316.64 | ▲ close $10,316.64 vs 09:30 $10,334.37 (session +0.00) | 16:00 close · cash $10,316.64 · no lots left · equity $10,316.64. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,316.64 | ▲ 09:30 equity $10,316.64 vs yday $10,316.64 (-0.00) | 09:30 open · cash $10,316.64 · no holdings · equity $10,316.64 vs prior close $10,316.64 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,316.64 | ▲ close $10,316.64 vs 09:30 $10,316.64 (session +0.00) | 16:00 close · cash $10,316.64 · no lots left · equity $10,316.64. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,316.64 | ▲ 09:30 equity $10,316.64 vs yday $10,316.64 (-0.00) | 09:30 open · cash $10,316.64 · no holdings · equity $10,316.64 vs prior close $10,316.64 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,316.64 | ▲ close $10,316.64 vs 09:30 $10,316.64 (session +0.00) | 16:00 close · cash $10,316.64 · no lots left · equity $10,316.64. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,316.64 | ▲ 09:30 equity $10,316.64 vs yday $10,316.64 (-0.00) | 09:30 open · cash $10,316.64 · no holdings · equity $10,316.64 vs prior close $10,316.64 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 54 | $23.88 | $2.15 | — | $9,024.97 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1289.58 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 39 | $32.88 | $2.11 | — | $7,740.54 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+16.2; leftover $1289.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 169 | $7.59 | $2.50 | — | $6,455.33 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.5; leftover $1289.58 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $5,750.09 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1289.58 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 81 | $15.87 | $2.23 | — | $4,462.39 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1289.58 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $3,405.17 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+3.3; leftover $1289.58 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 3 | $354.49 | $2.00 | — | $2,339.70 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-12.3; leftover $1289.58 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 27 | $47.60 | $2.07 | — | $1,052.43 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; leftover $1289.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,052.43 | ▲ close $10,489.43 vs 09:30 $10,316.64 (session +189.84) | 16:00 close · cash $1,052.43 · equity $10,489.43 vs 09:30 $10,316.64 (+172.79; session marks +189.84) · 8 name(s) marked open→close (per-name table). MMED×54 09:30 $23.88 → close $23.84 -2.16; CNXC×39 09:30 $32.88 → close $32.85 -1.17; OPTX×169 09:30 $7.59 → close $7.76 +28.73; DE×1 09:30 $703.25 → close $694.41 -8.84; FRNM×81 09:30 $15.87 → close $16.90 +83.43; AVGO×3 09:30 $351.74 → close $357.16 +16.26; CIEN×3 09:30 $354.49 → close $317.46 -111.09; HPE×27 09:30 $47.60 → close $54.44 +184.68 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,052.43 | ▼ 09:30 equity $10,441.51 vs yday $10,489.43 (-47.92) | 09:30 open · cash $1,052.43 (unchanged overnight, no fees) · equity $10,441.51 vs prior close $10,489.43 (-47.92) · 8 name(s) re-marked at the open (per-name table). MMED×54 yday $23.84 → 09:30 $23.84 +0.00; CNXC×39 yday $32.85 → 09:30 $32.48 -14.43; OPTX×169 yday $7.76 → 09:30 $7.79 +5.07; DE×1 yday $694.41 → 09:30 $692.03 -2.38; FRNM×81 yday $16.90 → 09:30 $16.40 -40.50; AVGO×3 yday $357.16 → 09:30 $359.70 +7.62; CIEN×3 yday $317.46 → 09:30 $321.67 +12.63; HPE×27 yday $54.44 → 09:30 $53.85 -15.93 | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 54 | $23.84 | $2.17 | $-6.48 | $2,337.61 | ▼ -6.48 after sell → book $10,439.33; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 39 | $32.48 | $2.13 | $-19.83 | $3,602.21 | ▼ -19.83 after sell → book $10,437.21; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 169 | $7.79 | $2.54 | $+28.77 | $4,916.18 | ▲ +28.77 after sell → book $10,434.67; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $5,606.20 | ▼ -15.23 after sell → book $10,432.66; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $6,683.28 | ▲ +19.86 after sell → book $10,430.64; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 3 | $321.67 | $2.02 | $-102.48 | $7,646.27 | ▼ -102.48 after sell → book $10,428.62; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 27 | $53.85 | $2.09 | $+164.59 | $9,098.13 | ▲ +164.59 after sell → book $10,426.53; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 6 | $263.36 | $2.01 | — | $7,515.96 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1819.63 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 937 | $1.94 | $12.09 | — | $5,686.09 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+18.3; leftover $1819.63 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 13 | $137.35 | $2.03 | — | $3,898.51 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+5.4; leftover $1819.63 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 7 | $236.82 | $2.01 | — | $2,238.76 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.1; leftover $1819.63 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 24 | $75.65 | $2.06 | — | $421.10 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1819.63 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $421.10 | ▲ close $10,573.49 vs 09:30 $10,441.51 (session +167.16) | 16:00 close · cash $421.10 · equity $10,573.49 vs 09:30 $10,441.51 (+131.98; session marks +167.16) · 6 name(s) marked open→close (per-name table). FRNM×81 09:30 $16.40 → close $16.31 -7.29; CRM×6 09:30 $263.36 → close $259.23 -24.78; BAK×937 09:30 $1.94 → close $1.89 -46.85; MSTR×13 09:30 $137.35 → close $142.80 +70.85; BE×7 09:30 $236.82 → close $252.87 +112.35; MRX×24 09:30 $75.65 → close $78.27 +62.88 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $421.10 | ▲ 09:30 equity $10,672.68 vs yday $10,573.49 (+99.19) | 09:30 open · cash $421.10 (unchanged overnight, no fees) · equity $10,672.68 vs prior close $10,573.49 (+99.19) · 6 name(s) re-marked at the open (per-name table). FRNM×81 yday $16.31 → 09:30 $16.74 +34.83; CRM×6 yday $259.23 → 09:30 $253.72 -33.06; BAK×937 yday $1.89 → 09:30 $1.94 +46.85; MSTR×13 yday $142.80 → 09:30 $137.62 -67.34; BE×7 yday $252.87 → 09:30 $267.76 +104.23; MRX×24 yday $78.27 → 09:30 $78.84 +13.68 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 81 | $16.74 | $2.26 | $+65.98 | $1,774.78 | ▲ +65.98 after sell → book $10,670.42; vs 09:30 mark -2.26 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 6 | $253.72 | $2.03 | $-61.88 | $3,295.07 | ▼ -61.88 after sell → book $10,668.39; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 937 | $1.94 | $12.26 | $-24.34 | $5,100.60 | ▼ -24.34 after sell → book $10,656.14; vs 09:30 mark -12.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 7 | $267.76 | $2.04 | $+212.53 | $6,972.88 | ▲ +212.53 after sell → book $10,654.10; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,972.88 | ▼ close $10,588.68 vs 09:30 $10,672.68 (session -65.42) | 16:00 close · cash $6,972.88 · equity $10,588.68 vs 09:30 $10,672.68 (-84.00; session marks -65.42) · 2 name(s) marked open→close (per-name table). MSTR×13 09:30 $137.62 → close $136.52 -14.30; MRX×24 09:30 $78.84 → close $76.71 -51.12 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,972.88 | ▲ 09:30 equity $10,654.94 vs yday $10,588.68 (+66.26) | 09:30 open · cash $6,972.88 (unchanged overnight, no fees) · equity $10,654.94 vs prior close $10,588.68 (+66.26) · 2 name(s) re-marked at the open (per-name table). MSTR×13 yday $136.52 → 09:30 $141.82 +68.90; MRX×24 yday $76.71 → 09:30 $76.60 -2.64 | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 13 | $141.82 | $2.05 | $+54.03 | $8,814.49 | ▲ +54.03 after sell → book $10,652.89; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 24 | $76.60 | $2.09 | $+18.65 | $10,650.80 | ▲ +18.65 after sell → book $10,650.80; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,650.80 | ▲ close $10,650.80 vs 09:30 $10,654.94 (session +0.00) | 16:00 close · cash $10,650.80 · no lots left · equity $10,650.80. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,650.80 | ▲ 09:30 equity $10,650.80 vs yday $10,650.80 (-0.00) | 09:30 open · cash $10,650.80 · no holdings · equity $10,650.80 vs prior close $10,650.80 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,650.80 | ▲ close $10,650.80 vs 09:30 $10,650.80 (session +0.00) | 16:00 close · cash $10,650.80 · no lots left · equity $10,650.80. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,650.80 | ▲ 09:30 equity $10,650.80 vs yday $10,650.80 (-0.00) | 09:30 open · cash $10,650.80 · no holdings · equity $10,650.80 vs prior close $10,650.80 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 9 | $164.43 | $2.02 | — | $9,168.91 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,earn_react; 🔵; ⚪; ret5=+9.0; leftover $1521.54 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 745 | $2.04 | $9.61 | — | $7,639.50 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.8; leftover $1521.54 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 717 | $2.12 | $9.25 | — | $6,110.21 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1521.54 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CLSK` | 117 | $12.97 | $2.34 | — | $4,590.38 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.0; leftover $1521.54 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `LITE` | 1 | $945.60 | $1.99 | — | $3,642.79 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $1521.54 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 6 | $242.17 | $2.01 | — | $2,187.77 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-12.1; leftover $1521.54 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 11 | $135.71 | $2.02 | — | $692.93 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-4.5; leftover $1521.54 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $692.93 | ▼ close $10,548.83 vs 09:30 $10,650.80 (session -72.73) | 16:00 close · cash $692.93 · equity $10,548.83 vs 09:30 $10,650.80 (-101.97; session marks -72.73) · 7 name(s) marked open→close (per-name table). ORCL×9 09:30 $164.43 → close $150.28 -127.35; AMTX×745 09:30 $2.04 → close $2.01 -22.35; BAK×717 09:30 $2.12 → close $2.08 -28.68; CLSK×117 09:30 $12.97 → close $13.67 +81.90; LITE×1 09:30 $945.60 → close $927.03 -18.57; ADBE×6 09:30 $242.17 → close $252.23 +60.36; RH×11 09:30 $135.71 → close $134.07 -18.04 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLSK` | hard_red | hard-red S=-13.28 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ORCL` | 9 | 2026-09-11 @ $164.43 | union ∩ news_g, no 🚨; gate news=good; list flatten,earn_react; 🔵; ⚪; ret5=+9.0; leftover $1521.54 |
| `AMTX` | 745 | 2026-09-11 @ $2.04 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.8; leftover $1521.54 |
| `BAK` | 717 | 2026-09-11 @ $2.12 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1521.54 |
| `CLSK` | 117 | 2026-09-11 @ $12.97 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.0; leftover $1521.54 |
| `LITE` | 1 | 2026-09-11 @ $945.60 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $1521.54 |
| `ADBE` | 6 | 2026-09-11 @ $242.17 | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-12.1; leftover $1521.54 |
| `RH` | 11 | 2026-09-11 @ $135.71 | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-4.5; leftover $1521.54 |
