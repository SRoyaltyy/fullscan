# Factor mine action — `union_news_g_cond_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · merged news🟢, rank +G−R

Cash book **+1.63%** ($10,163) · signal-only (no cash/fees) was +19.61%. Starts YES **14/24**. Fills 136 · skips 66 · realized $+163.00.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: the news camera (does the morning packet like the headline?) is green.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 8.
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
- **Gate** `news=good` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,163.01.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `HLIT` | 94 | — | $13.18 | +0.00 | $13.92 | +69.56 | +69.56 | +0.00 | +69.56 |
| 2026-08-14 | `ANGX` | 290 | — | $4.31 | +0.00 | $4.37 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-14 | `MH` | 92 | — | $13.55 | +0.00 | $13.10 | -41.40 | -41.40 | +0.00 | -41.40 |
| 2026-08-14 | `VELO` | 81 | — | $15.38 | +0.00 | $16.16 | +63.18 | +63.18 | +0.00 | +63.18 |
| 2026-08-14 | `ARX` | 63 | — | $19.57 | +0.00 | $19.58 | +0.63 | +0.63 | +0.00 | +0.63 |
| 2026-08-14 | `NRG` | 10 | — | $120.00 | +0.00 | $126.24 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-08-14 | `S` | 52 | — | $23.77 | +0.00 | $23.11 | -34.58 | -34.58 | +0.00 | -34.58 |
| 2026-08-17 | `HLIT` | 94 | $13.92 | $13.84 | -7.52 | — | +0.00 | -7.52 | +62.04 | — |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `MH` | 92 | $13.10 | $13.16 | +5.52 | — | +0.00 | +5.52 | -35.88 | — |
| 2026-08-17 | `VELO` | 81 | $16.16 | $16.05 | -8.91 | — | +0.00 | -8.91 | +54.27 | — |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | — | +0.00 | -0.63 | +0.00 | — |
| 2026-08-17 | `NRG` | 10 | $126.24 | $127.40 | +11.60 | — | +0.00 | +11.60 | +74.00 | — |
| 2026-08-17 | `S` | 52 | $23.11 | $22.50 | -31.72 | — | +0.00 | -31.72 | -66.30 | — |
| 2026-08-17 | `DVN` | 43 | — | $46.18 | +0.00 | $47.57 | +59.77 | +59.77 | +0.00 | +59.77 |
| 2026-08-17 | `EOG` | 14 | — | $142.77 | +0.00 | $146.15 | +47.32 | +47.32 | +0.00 | +47.32 |
| 2026-08-17 | `FANG` | 10 | — | $202.70 | +0.00 | $206.29 | +35.90 | +35.90 | +0.00 | +35.90 |
| 2026-08-17 | `OUST` | 41 | — | $49.00 | +0.00 | $48.13 | -35.67 | -35.67 | +0.00 | -35.67 |
| 2026-08-17 | `CELC` | 21 | — | $92.99 | +0.00 | $92.44 | -11.55 | -11.55 | +0.00 | -11.55 |
| 2026-08-18 | `DVN` | 43 | $47.57 | $48.00 | +18.49 | — | +0.00 | +18.49 | +78.26 | — |
| 2026-08-18 | `EOG` | 14 | $146.15 | $148.04 | +26.46 | — | +0.00 | +26.46 | +73.78 | — |
| 2026-08-18 | `FANG` | 10 | $206.29 | $208.93 | +26.40 | — | +0.00 | +26.40 | +62.30 | — |
| 2026-08-18 | `OUST` | 41 | $48.13 | $45.09 | -124.64 | — | +0.00 | -124.64 | -160.31 | — |
| 2026-08-18 | `CELC` | 21 | $92.44 | $92.38 | -1.26 | — | +0.00 | -1.26 | -12.81 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `APA` | 28 | — | $44.76 | +0.00 | $44.39 | -10.36 | -10.36 | +0.00 | -10.36 |
| 2026-08-20 | `AUTL` | 514 | — | $2.47 | +0.00 | $2.46 | -5.14 | -5.14 | +0.00 | -5.14 |
| 2026-08-20 | `CRSP` | 21 | — | $58.73 | +0.00 | $58.12 | -12.81 | -12.81 | +0.00 | -12.81 |
| 2026-08-20 | `ASST` | 79 | — | $16.00 | +0.00 | $16.13 | +10.27 | +10.27 | +0.00 | +10.27 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `ZLAB` | 47 | — | $26.57 | +0.00 | $26.02 | -25.85 | -25.85 | +0.00 | -25.85 |
| 2026-08-20 | `HUMA` | 1796 | — | $0.71 | +0.00 | $0.68 | -46.70 | -46.70 | +0.00 | -46.70 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `APA` | 28 | $44.39 | $44.52 | +3.64 | — | +0.00 | +3.64 | -6.72 | — |
| 2026-08-21 | `AUTL` | 514 | $2.46 | $2.47 | +5.14 | $2.41 | -30.84 | -25.70 | +0.00 | -30.84 |
| 2026-08-21 | `CRSP` | 21 | $58.12 | $59.72 | +33.60 | $59.50 | -4.62 | +28.98 | +20.79 | +16.17 |
| 2026-08-21 | `ASST` | 79 | $16.13 | $17.66 | +120.87 | — | +0.00 | +120.87 | +131.14 | — |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | — | +0.00 | -1.68 | -136.24 | — |
| 2026-08-21 | `ZLAB` | 47 | $26.02 | $26.25 | +10.81 | — | +0.00 | +10.81 | -15.04 | — |
| 2026-08-21 | `HUMA` | 1796 | $0.68 | $0.67 | -12.57 | — | +0.00 | -12.57 | -59.27 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `FUTU` | 10 | — | $115.18 | +0.00 | $123.64 | +84.60 | +84.60 | +0.00 | +84.60 |
| 2026-08-21 | `GRAL` | 15 | — | $78.88 | +0.00 | $79.54 | +9.90 | +9.90 | +0.00 | +9.90 |
| 2026-08-21 | `ABTC` | 145 | — | $8.66 | +0.00 | $7.93 | -105.85 | -105.85 | +0.00 | -105.85 |
| 2026-08-21 | `HIVE` | 389 | — | $3.24 | +0.00 | $3.03 | -81.69 | -81.69 | +0.00 | -81.69 |
| 2026-08-21 | `MARA` | 107 | — | $11.70 | +0.00 | $11.26 | -47.08 | -47.08 | +0.00 | -47.08 |
| 2026-08-24 | `AUTL` | 514 | $2.41 | $2.40 | -5.14 | — | +0.00 | -5.14 | -35.98 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | $57.08 | -35.17 | -50.92 | +0.42 | -34.75 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `FUTU` | 10 | $123.64 | $121.00 | -26.40 | — | +0.00 | -26.40 | +58.20 | — |
| 2026-08-24 | `GRAL` | 15 | $79.54 | $81.87 | +34.95 | — | +0.00 | +34.95 | +44.85 | — |
| 2026-08-24 | `ABTC` | 145 | $7.93 | $8.00 | +10.15 | — | +0.00 | +10.15 | -95.70 | — |
| 2026-08-24 | `HIVE` | 389 | $3.03 | $2.99 | -15.56 | — | +0.00 | -15.56 | -97.25 | — |
| 2026-08-24 | `MARA` | 107 | $11.26 | $11.17 | -9.63 | — | +0.00 | -9.63 | -56.71 | — |
| 2026-08-25 | `CRSP` | 21 | $57.08 | $57.93 | +17.95 | — | +0.00 | +17.95 | -16.80 | — |
| 2026-08-25 | `AU` | 11 | — | $118.52 | +0.00 | $123.39 | +53.57 | +53.57 | +0.00 | +53.57 |
| 2026-08-25 | `FCX` | 18 | — | $77.13 | +0.00 | $79.91 | +50.04 | +50.04 | +0.00 | +50.04 |
| 2026-08-25 | `EZPW` | 40 | — | $35.05 | +0.00 | $35.23 | +7.20 | +7.20 | +0.00 | +7.20 |
| 2026-08-25 | `RUM` | 149 | — | $9.42 | +0.00 | $10.23 | +120.69 | +120.69 | +0.00 | +120.69 |
| 2026-08-25 | `ZYME` | 48 | — | $28.86 | +0.00 | $27.47 | -66.72 | -66.72 | +0.00 | -66.72 |
| 2026-08-25 | `REAX` | 58 | — | $24.11 | +0.00 | $28.43 | +250.56 | +250.56 | +0.00 | +250.56 |
| 2026-08-25 | `EOLS` | 161 | — | $8.72 | +0.00 | $8.97 | +41.05 | +41.05 | +0.00 | +41.05 |
| 2026-08-26 | `AU` | 11 | $123.39 | $119.80 | -39.49 | — | +0.00 | -39.49 | +14.08 | — |
| 2026-08-26 | `FCX` | 18 | $79.91 | $79.34 | -10.26 | — | +0.00 | -10.26 | +39.78 | — |
| 2026-08-26 | `EZPW` | 40 | $35.23 | $35.70 | +18.80 | — | +0.00 | +18.80 | +26.00 | — |
| 2026-08-26 | `RUM` | 149 | $10.23 | $10.07 | -23.84 | — | +0.00 | -23.84 | +96.85 | — |
| 2026-08-26 | `ZYME` | 48 | $27.47 | $27.56 | +4.32 | — | +0.00 | +4.32 | -62.40 | — |
| 2026-08-26 | `REAX` | 58 | $28.43 | $26.61 | -105.56 | — | +0.00 | -105.56 | +145.00 | — |
| 2026-08-26 | `EOLS` | 161 | $8.97 | $8.86 | -18.52 | — | +0.00 | -18.52 | +22.54 | — |
| 2026-08-26 | `FNV` | 7 | — | $267.02 | +0.00 | $267.37 | +2.45 | +2.45 | +0.00 | +2.45 |
| 2026-08-26 | `TRLV` | 179 | — | $11.22 | +0.00 | $11.43 | +37.59 | +37.59 | +0.00 | +37.59 |
| 2026-08-26 | `CAPR` | 243 | — | $8.29 | +0.00 | $9.36 | +260.01 | +260.01 | +0.00 | +260.01 |
| 2026-08-26 | `FWRD` | 115 | — | $17.41 | +0.00 | $17.63 | +25.30 | +25.30 | +0.00 | +25.30 |
| 2026-08-26 | `FLNC` | 181 | — | $11.12 | +0.00 | $11.08 | -7.24 | -7.24 | +0.00 | -7.24 |
| 2026-08-27 | `FNV` | 7 | $267.37 | $267.23 | -0.98 | — | +0.00 | -0.98 | +1.47 | — |
| 2026-08-27 | `TRLV` | 179 | $11.43 | $11.38 | -8.95 | — | +0.00 | -8.95 | +28.64 | — |
| 2026-08-27 | `CAPR` | 243 | $9.36 | $9.19 | -41.31 | — | +0.00 | -41.31 | +218.70 | — |
| 2026-08-27 | `FWRD` | 115 | $17.63 | $17.60 | -3.45 | — | +0.00 | -3.45 | +21.85 | — |
| 2026-08-27 | `FLNC` | 181 | $11.08 | $11.52 | +79.64 | — | +0.00 | +79.64 | +72.40 | — |
| 2026-08-27 | `ACMR` | 21 | — | $81.65 | +0.00 | $80.49 | -24.36 | -24.36 | +0.00 | -24.36 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `LRCX` | 5 | — | $318.88 | +0.00 | $318.58 | -1.50 | -1.50 | +0.00 | -1.50 |
| 2026-08-27 | `NVDA` | 7 | — | $222.86 | +0.00 | $227.98 | +35.84 | +35.84 | +0.00 | +35.84 |
| 2026-08-27 | `RRC` | 41 | — | $41.44 | +0.00 | $41.64 | +8.20 | +8.20 | +0.00 | +8.20 |
| 2026-08-28 | `ACMR` | 21 | $80.49 | $79.27 | -25.62 | — | +0.00 | -25.62 | -49.98 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `LRCX` | 5 | $318.58 | $318.03 | -2.75 | — | +0.00 | -2.75 | -4.25 | — |
| 2026-08-28 | `NVDA` | 7 | $227.98 | $227.36 | -4.34 | — | +0.00 | -4.34 | +31.50 | — |
| 2026-08-28 | `RRC` | 41 | $41.64 | $41.74 | +4.10 | — | +0.00 | +4.10 | +12.30 | — |
| 2026-08-28 | `KEYS` | 3 | — | $324.41 | +0.00 | $319.97 | -13.32 | -13.32 | +0.00 | -13.32 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `DDOG` | 5 | — | $240.22 | +0.00 | $236.98 | -16.20 | -16.20 | +0.00 | -16.20 |
| 2026-08-28 | `ADSK` | 4 | — | $261.16 | +0.00 | $260.66 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-08-28 | `SEDG` | 39 | — | $32.90 | +0.00 | $31.41 | -58.11 | -58.11 | +0.00 | -58.11 |
| 2026-08-28 | `TLS` | 267 | — | $4.82 | +0.00 | $4.79 | -8.01 | -8.01 | +0.00 | -8.01 |
| 2026-08-31 | `KEYS` | 3 | $319.97 | $322.49 | +7.56 | — | +0.00 | +7.56 | -5.76 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -65.94 | — |
| 2026-08-31 | `DDOG` | 5 | $236.98 | $233.97 | -15.07 | — | +0.00 | -15.07 | -31.27 | — |
| 2026-08-31 | `ADSK` | 4 | $260.66 | $257.71 | -11.80 | — | +0.00 | -11.80 | -13.80 | — |
| 2026-08-31 | `SEDG` | 39 | $31.41 | $31.15 | -10.14 | — | +0.00 | -10.14 | -68.25 | — |
| 2026-08-31 | `TLS` | 267 | $4.79 | $4.81 | +5.34 | — | +0.00 | +5.34 | -2.67 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 3 | — | $351.74 | +0.00 | $357.16 | +16.26 | +16.26 | +0.00 | +16.26 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `CXW` | 38 | — | $32.31 | +0.00 | $33.66 | +51.30 | +51.30 | +0.00 | +51.30 |
| 2026-09-03 | `FRNM` | 78 | — | $15.87 | +0.00 | $16.90 | +80.34 | +80.34 | +0.00 | +80.34 |
| 2026-09-03 | `MMED` | 52 | — | $23.88 | +0.00 | $23.84 | -2.08 | -2.08 | +0.00 | -2.08 |
| 2026-09-03 | `DE` | 1 | — | $703.25 | +0.00 | $694.41 | -8.84 | -8.84 | +0.00 | -8.84 |
| 2026-09-03 | `HPE` | 26 | — | $47.60 | +0.00 | $54.44 | +177.84 | +177.84 | +0.00 | +177.84 |
| 2026-09-03 | `CNXC` | 38 | — | $32.88 | +0.00 | $32.85 | -1.14 | -1.14 | +0.00 | -1.14 |
| 2026-09-04 | `AVGO` | 3 | $357.16 | $359.70 | +7.62 | — | +0.00 | +7.62 | +23.88 | — |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | — | +0.00 | -5.22 | +54.94 | — |
| 2026-09-04 | `CXW` | 38 | $33.66 | $33.46 | -7.60 | — | +0.00 | -7.60 | +43.70 | — |
| 2026-09-04 | `FRNM` | 78 | $16.90 | $16.40 | -39.00 | $16.31 | -7.02 | -46.02 | +41.34 | +34.32 |
| 2026-09-04 | `MMED` | 52 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.08 | — |
| 2026-09-04 | `DE` | 1 | $694.41 | $692.03 | -2.38 | — | +0.00 | -2.38 | -11.22 | — |
| 2026-09-04 | `HPE` | 26 | $54.44 | $53.85 | -15.34 | — | +0.00 | -15.34 | +162.50 | — |
| 2026-09-04 | `CNXC` | 38 | $32.85 | $32.48 | -14.06 | — | +0.00 | -14.06 | -15.20 | — |
| 2026-09-04 | `CRM` | 6 | — | $263.36 | +0.00 | $259.23 | -24.78 | -24.78 | +0.00 | -24.78 |
| 2026-09-04 | `MRX` | 23 | — | $75.65 | +0.00 | $78.27 | +60.26 | +60.26 | +0.00 | +60.26 |
| 2026-09-04 | `BE` | 7 | — | $236.82 | +0.00 | $252.87 | +112.35 | +112.35 | +0.00 | +112.35 |
| 2026-09-04 | `BAK` | 929 | — | $1.94 | +0.00 | $1.89 | -46.45 | -46.45 | +0.00 | -46.45 |
| 2026-09-04 | `MSTR` | 13 | — | $137.35 | +0.00 | $142.80 | +70.85 | +70.85 | +0.00 | +70.85 |
| 2026-09-08 | `FRNM` | 78 | $16.31 | $16.74 | +33.54 | — | +0.00 | +33.54 | +67.86 | — |
| 2026-09-08 | `CRM` | 6 | $259.23 | $253.72 | -33.06 | — | +0.00 | -33.06 | -57.84 | — |
| 2026-09-08 | `MRX` | 23 | $78.27 | $78.84 | +13.11 | $76.71 | -48.99 | -35.88 | +73.37 | +24.38 |
| 2026-09-08 | `BE` | 7 | $252.87 | $267.76 | +104.23 | — | +0.00 | +104.23 | +216.58 | — |
| 2026-09-08 | `BAK` | 929 | $1.89 | $1.94 | +46.45 | — | +0.00 | +46.45 | +0.00 | — |
| 2026-09-08 | `MSTR` | 13 | $142.80 | $137.62 | -67.34 | $136.52 | -14.30 | -81.64 | +3.51 | -10.79 |
| 2026-09-09 | `MRX` | 23 | $76.71 | $76.60 | -2.53 | — | +0.00 | -2.53 | +21.85 | — |
| 2026-09-09 | `MSTR` | 13 | $136.52 | $141.82 | +68.90 | — | +0.00 | +68.90 | +58.11 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 12 | — | $164.43 | +0.00 | $150.28 | -169.80 | -169.80 | +0.00 | -169.80 |
| 2026-09-11 | `ADBE` | 8 | — | $242.17 | +0.00 | $252.23 | +80.48 | +80.48 | +0.00 | +80.48 |
| 2026-09-11 | `BAK` | 991 | — | $2.12 | +0.00 | $2.08 | -39.64 | -39.64 | +0.00 | -39.64 |
| 2026-09-11 | `AMTX` | 1030 | — | $2.04 | +0.00 | $2.01 | -30.90 | -30.90 | +0.00 | -30.90 |
| 2026-09-11 | `RH` | 15 | — | $135.71 | +0.00 | $134.07 | -24.60 | -24.60 | +0.00 | -24.60 |
| 2026-09-14 | `ORCL` | 12 | $150.28 | $141.42 | -106.32 | — | +0.00 | -106.32 | -276.12 | — |
| 2026-09-14 | `ADBE` | 8 | $252.23 | $261.51 | +74.24 | — | +0.00 | +74.24 | +154.72 | — |
| 2026-09-14 | `BAK` | 991 | $2.08 | $2.05 | -29.73 | — | +0.00 | -29.73 | -69.37 | — |
| 2026-09-14 | `AMTX` | 1030 | $2.01 | $2.01 | +0.00 | — | +0.00 | +0.00 | -30.90 | — |
| 2026-09-14 | `RH` | 15 | $134.07 | $131.40 | -40.05 | — | +0.00 | -40.05 | -64.65 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +137.19 | HLIT, ANGX, MH, VELO, ARX, NRG, S | — | $1,332.73 | $10,120.33 | HLIT×94, ANGX×290, MH×92, VELO×81, ARX×63, NRG×10, S×52 |
| 2026-08-17 | +2.25 | $1,332.73 | HLIT×94, ANGX×290, MH×92, VELO×81, ARX×63, NRG×10, S×52 | $10,155.37 | +35.04 | +95.77 | DVN, EOG, FANG, OUST, CELC | HLIT, ANGX, MH, VELO, ARX, NRG, S | $154.67 | $10,223.75 | DVN×43, EOG×14, FANG×10, OUST×41, CELC×21 |
| 2026-08-18 | -6.20 | $154.67 | DVN×43, EOG×14, FANG×10, OUST×41, CELC×21 | $10,169.20 | -54.55 | +0.00 | — | DVN, EOG, FANG, OUST, CELC | $10,158.74 | $10,158.74 | — |
| 2026-08-19 | -7.20 | $10,158.74 | — | $10,158.74 | -0.00 | +0.00 | — | — | $10,158.74 | $10,158.74 | — |
| 2026-08-20 | +1.12 | $10,158.74 | — | $10,158.74 | -0.00 | -191.09 | BHP, APA, AUTL, CRSP, ASST, MRNA, ZLAB, HUMA | — | $198.49 | $9,930.41 | BHP×13, APA×28, AUTL×514, CRSP×21, ASST×79, MRNA×8, ZLAB×47, HUMA×1796 |
| 2026-08-21 | +3.25 | $198.49 | BHP×13, APA×28, AUTL×514, CRSP×21, ASST×79, MRNA×8, ZLAB×47, HUMA×1796 | $10,117.39 | +186.98 | -157.68 | AU, FUTU, GRAL, ABTC, HIVE, MARA | BHP, APA, ASST, MRNA, ZLAB, HUMA | $252.22 | $9,915.50 | AUTL×514, CRSP×21, AU×10, FUTU×10, GRAL×15, ABTC×145, HIVE×389, MARA×107 |
| 2026-08-24 | -5.17 | $252.22 | AUTL×514, CRSP×21, AU×10, FUTU×10, GRAL×15, ABTC×145, HIVE×389, MARA×107 | $9,881.02 | -34.48 | -35.17 | — | AUTL, AU, FUTU, GRAL, ABTC, HIVE, MARA | $8,624.51 | $9,823.09 | CRSP×21 |
| 2026-08-25 | +1.80 | $8,624.51 | CRSP×21 | $9,841.04 | +17.95 | +456.39 | AU, FCX, EZPW, RUM, ZYME, REAX, EOLS | CRSP | $138.37 | $10,279.98 | AU×11, FCX×18, EZPW×40, RUM×149, ZYME×48, REAX×58, EOLS×161 |
| 2026-08-26 | +2.02 | $138.37 | AU×11, FCX×18, EZPW×40, RUM×149, ZYME×48, REAX×58, EOLS×161 | $10,105.44 | -174.54 | +318.11 | FNV, TRLV, CAPR, FWRD, FLNC | AU, FCX, EZPW, RUM, ZYME, REAX, EOLS | $170.47 | $10,395.44 | FNV×7, TRLV×179, CAPR×243, FWRD×115, FLNC×181 |
| 2026-08-27 | — | $170.47 | FNV×7, TRLV×179, CAPR×243, FWRD×115, FLNC×181 | $10,420.39 | +24.95 | -13.44 | ACMR, MU, LRCX, NVDA, RRC | FNV, TRLV, CAPR, FWRD, FLNC | $2,862.34 | $10,384.02 | ACMR×21, MU×1, LRCX×5, NVDA×7, RRC×41 |
| 2026-08-28 | +0.75 | $2,862.34 | ACMR×21, MU×1, LRCX×5, NVDA×7, RRC×41 | $10,339.31 | -44.71 | -258.89 | KEYS, SMTC, CIEN, DDOG, ADSK, SEDG, TLS | ACMR, MU, LRCX, NVDA, RRC | $2,047.34 | $10,054.56 | KEYS×3, SMTC×9, CIEN×3, DDOG×5, ADSK×4, SEDG×39, TLS×267 |
| 2026-08-31 | -5.85 | $2,047.34 | KEYS×3, SMTC×9, CIEN×3, DDOG×5, ADSK×4, SEDG×39, TLS×267 | $10,040.62 | -13.94 | +0.00 | — | KEYS, SMTC, CIEN, DDOG, ADSK, SEDG, TLS | $10,024.87 | $10,024.87 | — |
| 2026-09-01 | -6.30 | $10,024.87 | — | $10,024.87 | -0.00 | +0.00 | — | — | $10,024.87 | $10,024.87 | — |
| 2026-09-02 | -3.83 | $10,024.87 | — | $10,024.87 | -0.00 | +0.00 | — | — | $10,024.87 | $10,024.87 | — |
| 2026-09-03 | -0.90 | $10,024.87 | — | $10,024.87 | -0.00 | +373.84 | AVGO, DELL, CXW, FRNM, MMED, DE, HPE, CNXC | — | $1,082.70 | $10,382.07 | AVGO×3, DELL×2, CXW×38, FRNM×78, MMED×52, DE×1, HPE×26, CNXC×38 |
| 2026-09-04 | +2.25 | $1,082.70 | AVGO×3, DELL×2, CXW×38, FRNM×78, MMED×52, DE×1, HPE×26, CNXC×38 | $10,306.09 | -75.98 | +165.21 | CRM, MRX, BE, BAK, MSTR | AVGO, DELL, CXW, MMED, DE, HPE, CNXC | $426.59 | $10,436.66 | FRNM×78, CRM×6, MRX×23, BE×7, BAK×929, MSTR×13 |
| 2026-09-08 | -11.47 | $426.59 | FRNM×78, CRM×6, MRX×23, BE×7, BAK×929, MSTR×13 | $10,533.59 | +96.93 | -63.29 | — | FRNM, CRM, BE, BAK | $6,912.75 | $10,451.84 | MRX×23, MSTR×13 |
| 2026-09-09 | -13.95 | $6,912.75 | MRX×23, MSTR×13 | $10,518.21 | +66.37 | +0.00 | — | MRX, MSTR | $10,514.07 | $10,514.07 | — |
| 2026-09-10 | -13.28 | $10,514.07 | — | $10,514.07 | -0.00 | +0.00 | — | — | $10,514.07 | $10,514.07 | — |
| 2026-09-11 | +0.50 | $10,514.07 | — | $10,514.07 | -0.00 | -184.46 | ORCL, ADBE, BAK, AMTX, RH | — | $333.63 | $10,297.46 | ORCL×12, ADBE×8, BAK×991, AMTX×1030, RH×15 |
| 2026-09-14 | -11.00 | $333.63 | ORCL×12, ADBE×8, BAK×991, AMTX×1030, RH×15 | $10,195.60 | -101.86 | +0.00 | — | ORCL, ADBE, BAK, AMTX, RH | $10,163.01 | $10,163.01 | — |
| 2026-09-15 | -3.84 | $10,163.01 | — | $10,163.01 | +0.00 | +0.00 | — | — | $10,163.01 | $10,163.01 | — |
| 2026-09-16 | +5.30 | $10,163.01 | — | $10,163.01 | +0.00 | +0.00 | — | — | $10,163.01 | $10,163.01 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $8,758.81 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $7,505.17 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $6,256.30 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 81 | $15.38 | $2.23 | — | $5,008.29 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $3,773.20 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $2,571.18 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `S` | 52 | $23.77 | $2.15 | — | $1,332.73 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,332.73 | ▲ close $10,120.33 vs 09:30 $10,000.00 (session +137.19) | 16:00 close · cash $1,332.73 · equity $10,120.33 vs 09:30 $10,000.00 (+120.33; session marks +137.19) · 7 name(s) marked open→close (per-name table). HLIT×94 09:30 $13.18 → close $13.92 +69.56; ANGX×290 09:30 $4.31 → close $4.37 +17.40; MH×92 09:30 $13.55 → close $13.10 -41.40; VELO×81 09:30 $15.38 → close $16.16 +63.18; ARX×63 09:30 $19.57 → close $19.58 +0.63; NRG×10 09:30 $120.00 → close $126.24 +62.40; S×52 09:30 $23.77 → close $23.11 -34.58 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,332.73 | ▲ 09:30 equity $10,155.37 vs yday $10,120.33 (+35.04) | 09:30 open · cash $1,332.73 (unchanged overnight, no fees) · equity $10,155.37 vs prior close $10,120.33 (+35.04) · 7 name(s) re-marked at the open (per-name table). HLIT×94 yday $13.92 → 09:30 $13.84 -7.52; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; MH×92 yday $13.10 → 09:30 $13.16 +5.52; VELO×81 yday $16.16 → 09:30 $16.05 -8.91; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; S×52 yday $23.11 → 09:30 $22.50 -31.72 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $2,631.40 | ▲ +57.47 after sell → book $10,153.08; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,961.60 | ▲ +76.56 after sell → book $10,149.28; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $5,170.02 | ▼ -40.44 after sell → book $10,146.98; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VELO` | 81 | $16.05 | $2.26 | $+49.78 | $6,467.82 | ▲ +49.78 after sell → book $10,144.73; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $7,698.53 | ▼ -4.38 after sell → book $10,142.53; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $8,970.49 | ▲ +69.94 after sell → book $10,140.49; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `S` | 52 | $22.50 | $2.17 | $-70.61 | $10,138.32 | ▼ -70.61 after sell → book $10,138.32; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 43 | $46.18 | $2.12 | — | $8,150.46 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ret5=+6.7; leftover $2027.66 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 14 | $142.77 | $2.03 | — | $6,149.65 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ret5=+5.8; leftover $2027.66 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 10 | $202.70 | $2.02 | — | $4,120.63 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ret5=+8.3; leftover $2027.66 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 41 | $49.00 | $2.11 | — | $2,109.52 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $2027.66 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 21 | $92.99 | $2.05 | — | $154.67 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer; ret5=-0.8; leftover $2027.66 | join🟡 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.67 | ▲ close $10,223.75 vs 09:30 $10,155.37 (session +95.77) | 16:00 close · cash $154.67 · equity $10,223.75 vs 09:30 $10,155.37 (+68.38; session marks +95.77) · 5 name(s) marked open→close (per-name table). DVN×43 09:30 $46.18 → close $47.57 +59.77; EOG×14 09:30 $142.77 → close $146.15 +47.32; FANG×10 09:30 $202.70 → close $206.29 +35.90; OUST×41 09:30 $49.00 → close $48.13 -35.67; CELC×21 09:30 $92.99 → close $92.44 -11.55 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.67 | ▼ 09:30 equity $10,169.20 vs yday $10,223.75 (-54.55) | 09:30 open · cash $154.67 (unchanged overnight, no fees) · equity $10,169.20 vs prior close $10,223.75 (-54.55) · 5 name(s) re-marked at the open (per-name table). DVN×43 yday $47.57 → 09:30 $48.00 +18.49; EOG×14 yday $146.15 → 09:30 $148.04 +26.46; FANG×10 yday $206.29 → 09:30 $208.93 +26.40; OUST×41 yday $48.13 → 09:30 $45.09 -124.64; CELC×21 yday $92.44 → 09:30 $92.38 -1.26 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 43 | $48.00 | $2.15 | $+74.00 | $2,216.53 | ▲ +74.00 after sell → book $10,167.06; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 14 | $148.04 | $2.06 | $+69.69 | $4,287.03 | ▲ +69.69 after sell → book $10,165.00; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 10 | $208.93 | $2.05 | $+58.23 | $6,374.28 | ▲ +58.23 after sell → book $10,162.95; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 41 | $45.09 | $2.14 | $-164.56 | $8,220.84 | ▼ -164.56 after sell → book $10,160.82; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 21 | $92.38 | $2.08 | $-16.94 | $10,158.74 | ▼ -16.94 after sell → book $10,158.74; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,158.74 | ▲ close $10,158.74 vs 09:30 $10,169.20 (session +0.00) | 16:00 close · cash $10,158.74 · no lots left · equity $10,158.74. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,158.74 | ▲ 09:30 equity $10,158.74 vs yday $10,158.74 (-0.00) | 09:30 open · cash $10,158.74 · no holdings · equity $10,158.74 vs prior close $10,158.74 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,158.74 | ▲ close $10,158.74 vs 09:30 $10,158.74 (session +0.00) | 16:00 close · cash $10,158.74 · no lots left · equity $10,158.74. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,158.74 | ▲ 09:30 equity $10,158.74 vs yday $10,158.74 (-0.00) | 09:30 open · cash $10,158.74 · no holdings · equity $10,158.74 vs prior close $10,158.74 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,973.58 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1269.84 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $7,718.22 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1269.84 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 514 | $2.47 | $6.63 | — | $6,442.01 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1269.84 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $5,206.63 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1269.84 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 79 | $16.00 | $2.23 | — | $3,940.40 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1269.84 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $2,737.27 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1269.84 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $1,486.35 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1269.84 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1796 | $0.71 | $18.09 | — | $198.49 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1269.84 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $198.49 | ▼ close $9,930.41 vs 09:30 $10,158.74 (session -191.09) | 16:00 close · cash $198.49 · equity $9,930.41 vs 09:30 $10,158.74 (-228.33; session marks -191.09) · 8 name(s) marked open→close (per-name table). BHP×13 09:30 $91.01 → close $93.63 +34.06; APA×28 09:30 $44.76 → close $44.39 -10.36; AUTL×514 09:30 $2.47 → close $2.46 -5.14; CRSP×21 09:30 $58.73 → close $58.12 -12.81; ASST×79 09:30 $16.00 → close $16.13 +10.27; MRNA×8 09:30 $150.14 → close $133.32 -134.56; ZLAB×47 09:30 $26.57 → close $26.02 -25.85; HUMA×1796 09:30 $0.71 → close $0.68 -46.70 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $198.49 | ▲ 09:30 equity $10,117.39 vs yday $9,930.41 (+186.98) | 09:30 open · cash $198.49 (unchanged overnight, no fees) · equity $10,117.39 vs prior close $9,930.41 (+186.98) · 8 name(s) re-marked at the open (per-name table). BHP×13 yday $93.63 → 09:30 $95.72 +27.17; APA×28 yday $44.39 → 09:30 $44.52 +3.64; AUTL×514 yday $2.46 → 09:30 $2.47 +5.14; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; ASST×79 yday $16.13 → 09:30 $17.66 +120.87; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; ZLAB×47 yday $26.02 → 09:30 $26.25 +10.81; HUMA×1796 yday $0.68 → 09:30 $0.67 -12.57 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,440.80 | ▲ +57.15 after sell → book $10,115.34; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $2,685.27 | ▼ -10.89 after sell → book $10,113.24; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 79 | $17.66 | $2.25 | $+126.66 | $4,078.16 | ▲ +126.66 after sell → book $10,110.99; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $5,141.00 | ▼ -140.29 after sell → book $10,108.96; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 47 | $26.25 | $2.15 | $-19.32 | $6,372.60 | ▼ -19.32 after sell → book $10,106.81; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1796 | $0.67 | $17.80 | $-95.15 | $7,565.30 | ▼ -95.15 after sell → book $10,089.00; vs 09:30 mark -17.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,368.98 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1260.88 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $5,215.16 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1260.88 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 15 | $78.88 | $2.04 | — | $4,029.93 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1260.88 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 145 | $8.66 | $2.42 | — | $2,771.80 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1260.88 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 389 | $3.24 | $5.02 | — | $1,506.43 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $1260.88 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 107 | $11.70 | $2.31 | — | $252.22 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1260.88 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $252.22 | ▼ close $9,915.50 vs 09:30 $10,117.39 (session -157.68) | 16:00 close · cash $252.22 · equity $9,915.50 vs 09:30 $10,117.39 (-201.89; session marks -157.68) · 8 name(s) marked open→close (per-name table). AUTL×514 09:30 $2.47 → close $2.41 -30.84; CRSP×21 09:30 $59.72 → close $59.50 -4.62; AU×10 09:30 $119.43 → close $121.22 +17.90; FUTU×10 09:30 $115.18 → close $123.64 +84.60; GRAL×15 09:30 $78.88 → close $79.54 +9.90; ABTC×145 09:30 $8.66 → close $7.93 -105.85; HIVE×389 09:30 $3.24 → close $3.03 -81.69; MARA×107 09:30 $11.70 → close $11.26 -47.08 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $252.22 | ▼ 09:30 equity $9,881.02 vs yday $9,915.50 (-34.48) | 09:30 open · cash $252.22 (unchanged overnight, no fees) · equity $9,881.02 vs prior close $9,915.50 (-34.48) · 8 name(s) re-marked at the open (per-name table). AUTL×514 yday $2.41 → 09:30 $2.40 -5.14; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; AU×10 yday $121.22 → 09:30 $120.51 -7.10; FUTU×10 yday $123.64 → 09:30 $121.00 -26.40; GRAL×15 yday $79.54 → 09:30 $81.87 +34.95; ABTC×145 yday $7.93 → 09:30 $8.00 +10.15; HIVE×389 yday $3.03 → 09:30 $2.99 -15.56; MARA×107 yday $11.26 → 09:30 $11.17 -9.63 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 514 | $2.40 | $6.73 | $-49.34 | $1,479.09 | ▼ -49.34 after sell → book $9,874.29; vs 09:30 mark -6.73 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $2,682.15 | ▲ +6.74 after sell → book $9,872.25; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $121.00 | $2.04 | $+54.14 | $3,890.11 | ▲ +54.14 after sell → book $9,870.21; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 15 | $81.87 | $2.06 | $+40.76 | $5,116.10 | ▲ +40.76 after sell → book $9,868.15; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 145 | $8.00 | $2.46 | $-100.58 | $6,273.65 | ▼ -100.58 after sell → book $9,865.70; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 389 | $2.99 | $5.09 | $-107.36 | $7,431.66 | ▼ -107.36 after sell → book $9,860.60; vs 09:30 mark -5.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 107 | $11.17 | $2.34 | $-61.36 | $8,624.51 | ▼ -61.36 after sell → book $9,858.26; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,624.51 | ▼ close $9,823.09 vs 09:30 $9,881.02 (session -35.17) | 16:00 close · cash $8,624.51 · equity $9,823.09 vs 09:30 $9,881.02 (-57.93; session marks -35.17) · 1 name(s) marked open→close (per-name table). CRSP×21 09:30 $58.75 → close $57.08 -35.17 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,624.51 | ▲ 09:30 equity $9,841.04 vs yday $9,823.09 (+17.95) | 09:30 open · cash $8,624.51 (unchanged overnight, no fees) · equity $9,841.04 vs prior close $9,823.09 (+17.95) · 1 name(s) re-marked at the open (per-name table). CRSP×21 yday $57.08 → 09:30 $57.93 +17.95 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $9,838.97 | ▼ -20.93 after sell → book $9,838.97; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $118.52 | $2.02 | — | $8,533.23 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1405.57 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 18 | $77.13 | $2.04 | — | $7,142.84 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1405.57 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 40 | $35.05 | $2.11 | — | $5,738.73 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1405.57 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 149 | $9.42 | $2.44 | — | $4,332.72 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1405.57 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 48 | $28.86 | $2.13 | — | $2,945.30 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+13.7; leftover $1405.57 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 58 | $24.11 | $2.16 | — | $1,544.76 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_mover; ret5=+891.7; leftover $1405.57 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 161 | $8.72 | $2.47 | — | $138.37 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+13.0; leftover $1405.57 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $138.37 | ▲ close $10,279.98 vs 09:30 $9,841.04 (session +456.39) | 16:00 close · cash $138.37 · equity $10,279.98 vs 09:30 $9,841.04 (+438.94; session marks +456.39) · 7 name(s) marked open→close (per-name table). AU×11 09:30 $118.52 → close $123.39 +53.57; FCX×18 09:30 $77.13 → close $79.91 +50.04; EZPW×40 09:30 $35.05 → close $35.23 +7.20; RUM×149 09:30 $9.42 → close $10.23 +120.69; ZYME×48 09:30 $28.86 → close $27.47 -66.72; REAX×58 09:30 $24.11 → close $28.43 +250.56; EOLS×161 09:30 $8.72 → close $8.97 +41.05 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $138.37 | ▼ 09:30 equity $10,105.44 vs yday $10,279.98 (-174.54) | 09:30 open · cash $138.37 (unchanged overnight, no fees) · equity $10,105.44 vs prior close $10,279.98 (-174.54) · 7 name(s) re-marked at the open (per-name table). AU×11 yday $123.39 → 09:30 $119.80 -39.49; FCX×18 yday $79.91 → 09:30 $79.34 -10.26; EZPW×40 yday $35.23 → 09:30 $35.70 +18.80; RUM×149 yday $10.23 → 09:30 $10.07 -23.84; ZYME×48 yday $27.47 → 09:30 $27.56 +4.32; REAX×58 yday $28.43 → 09:30 $26.61 -105.56; EOLS×161 yday $8.97 → 09:30 $8.86 -18.52 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 11 | $119.80 | $2.04 | $+10.01 | $1,454.12 | ▲ +10.01 after sell → book $10,103.39; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 18 | $79.34 | $2.07 | $+35.67 | $2,880.18 | ▲ +35.67 after sell → book $10,101.33; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 40 | $35.70 | $2.13 | $+21.76 | $4,306.05 | ▲ +21.76 after sell → book $10,099.20; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 149 | $10.07 | $2.47 | $+91.94 | $5,804.00 | ▲ +91.94 after sell → book $10,096.72; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 48 | $27.56 | $2.15 | $-66.69 | $7,124.73 | ▼ -66.69 after sell → book $10,094.57; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 58 | $26.61 | $2.19 | $+140.65 | $8,665.92 | ▲ +140.65 after sell → book $10,092.38; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 161 | $8.86 | $2.51 | $+17.56 | $10,089.87 | ▲ +17.56 after sell → book $10,089.87; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 7 | $267.02 | $2.01 | — | $8,218.72 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $2017.97 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 179 | $11.22 | $2.53 | — | $6,207.81 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $2017.97 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 243 | $8.29 | $3.13 | — | $4,190.21 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $2017.97 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 115 | $17.41 | $2.33 | — | $2,185.72 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_mover; ret5=-9.2; leftover $2017.97 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 181 | $11.12 | $2.53 | — | $170.47 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $2017.97 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $170.47 | ▲ close $10,395.44 vs 09:30 $10,105.44 (session +318.11) | 16:00 close · cash $170.47 · equity $10,395.44 vs 09:30 $10,105.44 (+290.00; session marks +318.11) · 5 name(s) marked open→close (per-name table). FNV×7 09:30 $267.02 → close $267.37 +2.45; TRLV×179 09:30 $11.22 → close $11.43 +37.59; CAPR×243 09:30 $8.29 → close $9.36 +260.01; FWRD×115 09:30 $17.41 → close $17.63 +25.30; FLNC×181 09:30 $11.12 → close $11.08 -7.24 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $170.47 | ▲ 09:30 equity $10,420.39 vs yday $10,395.44 (+24.95) | 09:30 open · cash $170.47 (unchanged overnight, no fees) · equity $10,420.39 vs prior close $10,395.44 (+24.95) · 5 name(s) re-marked at the open (per-name table). FNV×7 yday $267.37 → 09:30 $267.23 -0.98; TRLV×179 yday $11.43 → 09:30 $11.38 -8.95; CAPR×243 yday $9.36 → 09:30 $9.19 -41.31; FWRD×115 yday $17.63 → 09:30 $17.60 -3.45; FLNC×181 yday $11.08 → 09:30 $11.52 +79.64 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 7 | $267.23 | $2.04 | $-2.58 | $2,039.04 | ▼ -2.58 after sell → book $10,418.35; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 179 | $11.38 | $2.57 | $+23.54 | $4,073.49 | ▲ +23.54 after sell → book $10,415.78; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 243 | $9.19 | $3.19 | $+212.37 | $6,303.47 | ▲ +212.37 after sell → book $10,412.59; vs 09:30 mark -3.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FWRD` | 115 | $17.60 | $2.37 | $+17.14 | $8,325.10 | ▲ +17.14 after sell → book $10,410.22; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 181 | $11.52 | $2.58 | $+67.29 | $10,407.64 | ▲ +67.29 after sell → book $10,407.64; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 21 | $81.65 | $2.05 | — | $8,690.93 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1734.61 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $7,721.93 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1734.61 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 5 | $318.88 | $2.00 | — | $6,125.53 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1734.61 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 7 | $222.86 | $2.01 | — | $4,563.50 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $1734.61 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 41 | $41.44 | $2.11 | — | $2,862.34 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; ret5=+3.1; leftover $1734.61 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,862.34 | ▼ close $10,384.02 vs 09:30 $10,420.39 (session -13.44) | 16:00 close · cash $2,862.34 · equity $10,384.02 vs 09:30 $10,420.39 (-36.37; session marks -13.44) · 5 name(s) marked open→close (per-name table). ACMR×21 09:30 $81.65 → close $80.49 -24.36; MU×1 09:30 $967.01 → close $935.39 -31.62; LRCX×5 09:30 $318.88 → close $318.58 -1.50; NVDA×7 09:30 $222.86 → close $227.98 +35.84; RRC×41 09:30 $41.44 → close $41.64 +8.20 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,862.34 | ▼ 09:30 equity $10,339.31 vs yday $10,384.02 (-44.71) | 09:30 open · cash $2,862.34 (unchanged overnight, no fees) · equity $10,339.31 vs prior close $10,384.02 (-44.71) · 5 name(s) re-marked at the open (per-name table). ACMR×21 yday $80.49 → 09:30 $79.27 -25.62; MU×1 yday $935.39 → 09:30 $919.29 -16.10; LRCX×5 yday $318.58 → 09:30 $318.03 -2.75; NVDA×7 yday $227.98 → 09:30 $227.36 -4.34; RRC×41 yday $41.64 → 09:30 $41.74 +4.10 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 21 | $79.27 | $2.08 | $-54.11 | $4,524.94 | ▼ -54.11 after sell → book $10,337.24; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $5,442.21 | ▼ -51.73 after sell → book $10,335.22; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 5 | $318.03 | $2.03 | $-8.28 | $7,030.34 | ▼ -8.28 after sell → book $10,333.20; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 7 | $227.36 | $2.03 | $+27.46 | $8,619.82 | ▲ +27.46 after sell → book $10,331.16; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 41 | $41.74 | $2.14 | $+8.05 | $10,329.02 | ▲ +8.05 after sell → book $10,329.02; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $9,353.80 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1291.13 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $8,075.94 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1291.13 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,872.68 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1291.13 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $5,669.57 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1291.13 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $4,622.93 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; ret5=+7.8; leftover $1291.13 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $32.90 | $2.11 | — | $3,337.73 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1291.13 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 267 | $4.82 | $3.44 | — | $2,047.34 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1291.13 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,047.34 | ▼ close $10,054.56 vs 09:30 $10,339.31 (session -258.89) | 16:00 close · cash $2,047.34 · equity $10,054.56 vs 09:30 $10,339.31 (-284.75; session marks -258.89) · 7 name(s) marked open→close (per-name table). KEYS×3 09:30 $324.41 → close $319.97 -13.32; SMTC×9 09:30 $141.76 → close $131.17 -95.31; CIEN×3 09:30 $400.42 → close $378.44 -65.94; DDOG×5 09:30 $240.22 → close $236.98 -16.20; ADSK×4 09:30 $261.16 → close $260.66 -2.00; SEDG×39 09:30 $32.90 → close $31.41 -58.11; TLS×267 09:30 $4.82 → close $4.79 -8.01 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,047.34 | ▼ 09:30 equity $10,040.62 vs yday $10,054.56 (-13.94) | 09:30 open · cash $2,047.34 (unchanged overnight, no fees) · equity $10,040.62 vs prior close $10,054.56 (-13.94) · 7 name(s) re-marked at the open (per-name table). KEYS×3 yday $319.97 → 09:30 $322.49 +7.56; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; DDOG×5 yday $236.98 → 09:30 $233.97 -15.07; ADSK×4 yday $260.66 → 09:30 $257.71 -11.80; SEDG×39 yday $31.41 → 09:30 $31.15 -10.14; TLS×267 yday $4.79 → 09:30 $4.81 +5.34 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 3 | $322.49 | $2.02 | $-9.78 | $3,012.79 | ▼ -9.78 after sell → book $10,038.60; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $4,201.46 | ▼ -89.19 after sell → book $10,036.56; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $5,334.76 | ▼ -69.96 after sell → book $10,034.54; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 5 | $233.97 | $2.02 | $-35.31 | $6,502.56 | ▼ -35.31 after sell → book $10,032.52; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-17.82 | $7,531.37 | ▼ -17.82 after sell → book $10,030.49; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 39 | $31.15 | $2.13 | $-72.48 | $8,744.10 | ▼ -72.48 after sell → book $10,028.37; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 267 | $4.81 | $3.50 | $-9.61 | $10,024.87 | ▼ -9.61 after sell → book $10,024.87; vs 09:30 mark -3.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,024.87 | ▲ close $10,024.87 vs 09:30 $10,040.62 (session +0.00) | 16:00 close · cash $10,024.87 · no lots left · equity $10,024.87. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,024.87 | ▲ 09:30 equity $10,024.87 vs yday $10,024.87 (-0.00) | 09:30 open · cash $10,024.87 · no holdings · equity $10,024.87 vs prior close $10,024.87 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,024.87 | ▲ close $10,024.87 vs 09:30 $10,024.87 (session +0.00) | 16:00 close · cash $10,024.87 · no lots left · equity $10,024.87. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,024.87 | ▲ 09:30 equity $10,024.87 vs yday $10,024.87 (-0.00) | 09:30 open · cash $10,024.87 · no holdings · equity $10,024.87 vs prior close $10,024.87 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,024.87 | ▲ close $10,024.87 vs 09:30 $10,024.87 (session +0.00) | 16:00 close · cash $10,024.87 · no lots left · equity $10,024.87. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,024.87 | ▲ 09:30 equity $10,024.87 vs yday $10,024.87 (-0.00) | 09:30 open · cash $10,024.87 · no holdings · equity $10,024.87 vs prior close $10,024.87 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,967.65 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1253.11 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,993.03 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1253.11 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 38 | $32.31 | $2.10 | — | $6,763.15 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1253.11 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 78 | $15.87 | $2.22 | — | $5,523.07 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1253.11 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 52 | $23.88 | $2.15 | — | $4,279.16 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1253.11 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $3,573.92 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1253.11 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 26 | $47.60 | $2.07 | — | $2,334.25 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1253.11 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 38 | $32.88 | $2.10 | — | $1,082.70 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1253.11 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,082.70 | ▲ close $10,382.07 vs 09:30 $10,024.87 (session +373.84) | 16:00 close · cash $1,082.70 · equity $10,382.07 vs 09:30 $10,024.87 (+357.20; session marks +373.84) · 8 name(s) marked open→close (per-name table). AVGO×3 09:30 $351.74 → close $357.16 +16.26; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×38 09:30 $32.31 → close $33.66 +51.30; FRNM×78 09:30 $15.87 → close $16.90 +80.34; MMED×52 09:30 $23.88 → close $23.84 -2.08; DE×1 09:30 $703.25 → close $694.41 -8.84; HPE×26 09:30 $47.60 → close $54.44 +177.84; CNXC×38 09:30 $32.88 → close $32.85 -1.14 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,082.70 | ▼ 09:30 equity $10,306.09 vs yday $10,382.07 (-75.98) | 09:30 open · cash $1,082.70 (unchanged overnight, no fees) · equity $10,306.09 vs prior close $10,382.07 (-75.98) · 8 name(s) re-marked at the open (per-name table). AVGO×3 yday $357.16 → 09:30 $359.70 +7.62; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×38 yday $33.66 → 09:30 $33.46 -7.60; FRNM×78 yday $16.90 → 09:30 $16.40 -39.00; MMED×52 yday $23.84 → 09:30 $23.84 +0.00; DE×1 yday $694.41 → 09:30 $692.03 -2.38; HPE×26 yday $54.44 → 09:30 $53.85 -15.34; CNXC×38 yday $32.85 → 09:30 $32.48 -14.06 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $2,159.79 | ▲ +19.86 after sell → book $10,304.08; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $3,185.33 | ▲ +50.93 after sell → book $10,302.06; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 38 | $33.46 | $2.12 | $+39.47 | $4,454.69 | ▲ +39.47 after sell → book $10,299.94; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 52 | $23.84 | $2.17 | $-6.39 | $5,692.20 | ▼ -6.39 after sell → book $10,297.77; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $6,382.22 | ▼ -15.23 after sell → book $10,295.76; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 26 | $53.85 | $2.09 | $+158.34 | $7,780.23 | ▲ +158.34 after sell → book $10,293.67; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 38 | $32.48 | $2.12 | $-19.43 | $9,012.34 | ▼ -19.43 after sell → book $10,291.54; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 6 | $263.36 | $2.01 | — | $7,430.18 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1802.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 23 | $75.65 | $2.06 | — | $5,688.17 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1802.47 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 7 | $236.82 | $2.01 | — | $4,028.42 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; ret5=+8.1; leftover $1802.47 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 929 | $1.94 | $11.98 | — | $2,214.17 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $1802.47 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 13 | $137.35 | $2.03 | — | $426.59 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; ret5=+5.4; leftover $1802.47 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $426.59 | ▲ close $10,436.66 vs 09:30 $10,306.09 (session +165.21) | 16:00 close · cash $426.59 · equity $10,436.66 vs 09:30 $10,306.09 (+130.57; session marks +165.21) · 6 name(s) marked open→close (per-name table). FRNM×78 09:30 $16.40 → close $16.31 -7.02; CRM×6 09:30 $263.36 → close $259.23 -24.78; MRX×23 09:30 $75.65 → close $78.27 +60.26; BE×7 09:30 $236.82 → close $252.87 +112.35; BAK×929 09:30 $1.94 → close $1.89 -46.45; MSTR×13 09:30 $137.35 → close $142.80 +70.85 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $426.59 | ▲ 09:30 equity $10,533.59 vs yday $10,436.66 (+96.93) | 09:30 open · cash $426.59 (unchanged overnight, no fees) · equity $10,533.59 vs prior close $10,436.66 (+96.93) · 6 name(s) re-marked at the open (per-name table). FRNM×78 yday $16.31 → 09:30 $16.74 +33.54; CRM×6 yday $259.23 → 09:30 $253.72 -33.06; MRX×23 yday $78.27 → 09:30 $78.84 +13.11; BE×7 yday $252.87 → 09:30 $267.76 +104.23; BAK×929 yday $1.89 → 09:30 $1.94 +46.45; MSTR×13 yday $142.80 → 09:30 $137.62 -67.34 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 78 | $16.74 | $2.25 | $+63.39 | $1,730.06 | ▲ +63.39 after sell → book $10,531.34; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 6 | $253.72 | $2.03 | $-61.88 | $3,250.35 | ▼ -61.88 after sell → book $10,529.31; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 7 | $267.76 | $2.04 | $+212.53 | $5,122.64 | ▲ +212.53 after sell → book $10,527.28; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 929 | $1.94 | $12.15 | $-24.14 | $6,912.75 | ▼ -24.14 after sell → book $10,515.13; vs 09:30 mark -12.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,912.75 | ▼ close $10,451.84 vs 09:30 $10,533.59 (session -63.29) | 16:00 close · cash $6,912.75 · equity $10,451.84 vs 09:30 $10,533.59 (-81.75; session marks -63.29) · 2 name(s) marked open→close (per-name table). MRX×23 09:30 $78.84 → close $76.71 -48.99; MSTR×13 09:30 $137.62 → close $136.52 -14.30 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,912.75 | ▲ 09:30 equity $10,518.21 vs yday $10,451.84 (+66.37) | 09:30 open · cash $6,912.75 (unchanged overnight, no fees) · equity $10,518.21 vs prior close $10,451.84 (+66.37) · 2 name(s) re-marked at the open (per-name table). MRX×23 yday $76.71 → 09:30 $76.60 -2.53; MSTR×13 yday $136.52 → 09:30 $141.82 +68.90 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 23 | $76.60 | $2.08 | $+17.71 | $8,672.46 | ▲ +17.71 after sell → book $10,516.12; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 13 | $141.82 | $2.05 | $+54.03 | $10,514.07 | ▲ +54.03 after sell → book $10,514.07; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,514.07 | ▲ close $10,514.07 vs 09:30 $10,518.21 (session +0.00) | 16:00 close · cash $10,514.07 · no lots left · equity $10,514.07. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,514.07 | ▲ 09:30 equity $10,514.07 vs yday $10,514.07 (-0.00) | 09:30 open · cash $10,514.07 · no holdings · equity $10,514.07 vs prior close $10,514.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,514.07 | ▲ close $10,514.07 vs 09:30 $10,514.07 (session +0.00) | 16:00 close · cash $10,514.07 · no lots left · equity $10,514.07. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,514.07 | ▲ 09:30 equity $10,514.07 vs yday $10,514.07 (-0.00) | 09:30 open · cash $10,514.07 · no holdings · equity $10,514.07 vs prior close $10,514.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 12 | $164.43 | $2.03 | — | $8,538.88 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $2102.81 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 8 | $242.17 | $2.01 | — | $6,599.51 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; ret5=-11.1; leftover $2102.81 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 991 | $2.12 | $12.78 | — | $4,485.81 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $2102.81 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 1030 | $2.04 | $13.29 | — | $2,371.32 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $2102.81 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 15 | $135.71 | $2.04 | — | $333.63 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; ret5=-9.2; leftover $2102.81 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $333.63 | ▼ close $10,297.46 vs 09:30 $10,514.07 (session -184.46) | 16:00 close · cash $333.63 · equity $10,297.46 vs 09:30 $10,514.07 (-216.61; session marks -184.46) · 5 name(s) marked open→close (per-name table). ORCL×12 09:30 $164.43 → close $150.28 -169.80; ADBE×8 09:30 $242.17 → close $252.23 +80.48; BAK×991 09:30 $2.12 → close $2.08 -39.64; AMTX×1030 09:30 $2.04 → close $2.01 -30.90; RH×15 09:30 $135.71 → close $134.07 -24.60 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $333.63 | ▼ 09:30 equity $10,195.60 vs yday $10,297.46 (-101.86) | 09:30 open · cash $333.63 (unchanged overnight, no fees) · equity $10,195.60 vs prior close $10,297.46 (-101.86) · 5 name(s) re-marked at the open (per-name table). ORCL×12 yday $150.28 → 09:30 $141.42 -106.32; ADBE×8 yday $252.23 → 09:30 $261.51 +74.24; BAK×991 yday $2.08 → 09:30 $2.05 -29.73; AMTX×1030 yday $2.01 → 09:30 $2.01 +0.00; RH×15 yday $134.07 → 09:30 $131.40 -40.05 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 12 | $141.42 | $2.05 | $-280.20 | $2,028.62 | ▼ -280.20 after sell → book $10,193.55; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 8 | $261.51 | $2.04 | $+150.67 | $4,118.66 | ▲ +150.67 after sell → book $10,191.51; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 991 | $2.05 | $12.96 | $-95.12 | $6,137.25 | ▼ -95.12 after sell → book $10,178.55; vs 09:30 mark -12.96 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 1030 | $2.01 | $13.47 | $-57.66 | $8,194.07 | ▼ -57.66 after sell → book $10,165.07; vs 09:30 mark -13.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 15 | $131.40 | $2.06 | $-68.75 | $10,163.01 | ▼ -68.75 after sell → book $10,163.01; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,163.01 | ▲ close $10,163.01 vs 09:30 $10,195.60 (session +0.00) | 16:00 close · cash $10,163.01 · no lots left · equity $10,163.01. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,163.01 | ▲ 09:30 equity $10,163.01 vs yday $10,163.01 (+0.00) | 09:30 open · cash $10,163.01 · no holdings · equity $10,163.01 vs prior close $10,163.01 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,163.01 | ▲ close $10,163.01 vs 09:30 $10,163.01 (session +0.00) | 16:00 close · cash $10,163.01 · no lots left · equity $10,163.01. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,163.01 | ▲ 09:30 equity $10,163.01 vs yday $10,163.01 (+0.00) | 09:30 open · cash $10,163.01 · no holdings · equity $10,163.01 vs prior close $10,163.01 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,163.01 | ▲ close $10,163.01 vs 09:30 $10,163.01 (session +0.00) | 16:00 close · cash $10,163.01 · no lots left · equity $10,163.01. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AUTL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1734.61 < 1 share @ 1746.53 |
| 2026-08-28 | `MPWR` | cash | leftover split 1291.13 < 1 share @ 1306.03 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `WLTH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `AVTR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
