# Factor mine action — `probable_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-3.49%** ($9,651) · signal-only (no cash/fees) was +0.91%. Starts YES **0/18**. Fills 132 · skips 62 · realized $-232.55.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at yesterday's 'likely to keep moving' list and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: yesterday's 'likely to keep moving' list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).

### When it buys

- At 09:30, take names on yesterday's 'likely to keep moving' list that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
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

- **Universe** `probable` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $175.52.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ANGX` | 290 | — | $4.31 | +0.00 | $4.37 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-14 | `WWW` | 60 | — | $20.60 | +0.00 | $21.03 | +25.80 | +25.80 | +0.00 | +25.80 |
| 2026-08-14 | `HYLN` | 299 | — | $4.18 | +0.00 | $4.06 | -35.88 | -35.88 | +0.00 | -35.88 |
| 2026-08-14 | `WDC` | 2 | — | $503.50 | +0.00 | $508.80 | +10.60 | +10.60 | +0.00 | +10.60 |
| 2026-08-14 | `FOSL` | 221 | — | $5.64 | +0.00 | $5.57 | -15.47 | -15.47 | +0.00 | -15.47 |
| 2026-08-14 | `ADUR` | 75 | — | $16.50 | +0.00 | $16.17 | -24.75 | -24.75 | +0.00 | -24.75 |
| 2026-08-14 | `AIRS` | 370 | — | $3.37 | +0.00 | $3.43 | +22.20 | +22.20 | +0.00 | +22.20 |
| 2026-08-14 | `ALGM` | 28 | — | $44.06 | +0.00 | $44.39 | +9.24 | +9.24 | +0.00 | +9.24 |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `WWW` | 60 | $21.03 | $20.98 | -3.00 | — | +0.00 | -3.00 | +22.80 | — |
| 2026-08-17 | `HYLN` | 299 | $4.06 | $4.10 | +11.96 | — | +0.00 | +11.96 | -23.92 | — |
| 2026-08-17 | `WDC` | 2 | $508.80 | $525.53 | +33.46 | — | +0.00 | +33.46 | +44.06 | — |
| 2026-08-17 | `FOSL` | 221 | $5.57 | $5.50 | -15.47 | — | +0.00 | -15.47 | -30.94 | — |
| 2026-08-17 | `ADUR` | 75 | $16.17 | $15.73 | -33.00 | — | +0.00 | -33.00 | -57.75 | — |
| 2026-08-17 | `AIRS` | 370 | $3.43 | $3.40 | -12.95 | — | +0.00 | -12.95 | +9.25 | — |
| 2026-08-17 | `ALGM` | 28 | $44.39 | $45.32 | +26.04 | — | +0.00 | +26.04 | +35.28 | — |
| 2026-08-17 | `CDNL` | 31 | — | $39.85 | +0.00 | $39.23 | -19.22 | -19.22 | +0.00 | -19.22 |
| 2026-08-17 | `ABX` | 137 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `FCEL` | 56 | — | $22.37 | +0.00 | $22.36 | -0.56 | -0.56 | +0.00 | -0.56 |
| 2026-08-17 | `VERA` | 40 | — | $31.30 | +0.00 | $31.63 | +13.20 | +13.20 | +0.00 | +13.20 |
| 2026-08-17 | `CELC` | 13 | — | $92.99 | +0.00 | $92.44 | -7.15 | -7.15 | +0.00 | -7.15 |
| 2026-08-17 | `BW` | 121 | — | $10.35 | +0.00 | $9.92 | -52.03 | -52.03 | +0.00 | -52.03 |
| 2026-08-17 | `OCC` | 68 | — | $18.24 | +0.00 | $17.12 | -76.16 | -76.16 | +0.00 | -76.16 |
| 2026-08-17 | `ALM` | 77 | — | $16.20 | +0.00 | $16.36 | +12.32 | +12.32 | +0.00 | +12.32 |
| 2026-08-18 | `CDNL` | 31 | $39.23 | $41.57 | +72.54 | — | +0.00 | +72.54 | +53.32 | — |
| 2026-08-18 | `ABX` | 137 | $9.12 | $9.03 | -12.33 | — | +0.00 | -12.33 | -12.33 | — |
| 2026-08-18 | `FCEL` | 56 | $22.36 | $21.18 | -66.08 | — | +0.00 | -66.08 | -66.64 | — |
| 2026-08-18 | `VERA` | 40 | $31.63 | $31.31 | -12.80 | — | +0.00 | -12.80 | +0.40 | — |
| 2026-08-18 | `CELC` | 13 | $92.44 | $92.38 | -0.78 | — | +0.00 | -0.78 | -7.93 | — |
| 2026-08-18 | `BW` | 121 | $9.92 | $9.60 | -38.72 | — | +0.00 | -38.72 | -90.75 | — |
| 2026-08-18 | `OCC` | 68 | $17.12 | $16.20 | -62.56 | — | +0.00 | -62.56 | -138.72 | — |
| 2026-08-18 | `ALM` | 77 | $16.36 | $15.78 | -44.66 | — | +0.00 | -44.66 | -32.34 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `MRVI` | 164 | — | $7.38 | +0.00 | $8.26 | +144.32 | +144.32 | +0.00 | +144.32 |
| 2026-08-20 | `DNA` | 162 | — | $7.45 | +0.00 | $6.96 | -79.38 | -79.38 | +0.00 | -79.38 |
| 2026-08-20 | `MSTR` | 10 | — | $113.23 | +0.00 | $112.39 | -8.40 | -8.40 | +0.00 | -8.40 |
| 2026-08-20 | `EXK` | 112 | — | $10.77 | +0.00 | $10.97 | +22.40 | +22.40 | +0.00 | +22.40 |
| 2026-08-20 | `SCZM` | 128 | — | $9.46 | +0.00 | $9.76 | +38.40 | +38.40 | +0.00 | +38.40 |
| 2026-08-20 | `NG` | 144 | — | $8.38 | +0.00 | $8.66 | +40.32 | +40.32 | +0.00 | +40.32 |
| 2026-08-20 | `BLSH` | 41 | — | $29.20 | +0.00 | $28.44 | -31.16 | -31.16 | +0.00 | -31.16 |
| 2026-08-20 | `CRCL` | 14 | — | $83.29 | +0.00 | $83.99 | +9.80 | +9.80 | +0.00 | +9.80 |
| 2026-08-21 | `MRVI` | 164 | $8.26 | $8.20 | -9.84 | $8.70 | +82.00 | +72.16 | +134.48 | +216.48 |
| 2026-08-21 | `DNA` | 162 | $6.96 | $7.09 | +21.06 | — | +0.00 | +21.06 | -58.32 | — |
| 2026-08-21 | `MSTR` | 10 | $112.39 | $119.69 | +73.00 | — | +0.00 | +73.00 | +64.60 | — |
| 2026-08-21 | `EXK` | 112 | $10.97 | $11.34 | +41.44 | — | +0.00 | +41.44 | +63.84 | — |
| 2026-08-21 | `SCZM` | 128 | $9.76 | $10.26 | +64.00 | — | +0.00 | +64.00 | +102.40 | — |
| 2026-08-21 | `NG` | 144 | $8.66 | $9.02 | +51.84 | — | +0.00 | +51.84 | +92.16 | — |
| 2026-08-21 | `BLSH` | 41 | $28.44 | $29.75 | +53.71 | — | +0.00 | +53.71 | +22.55 | — |
| 2026-08-21 | `CRCL` | 14 | $83.99 | $87.65 | +51.24 | — | +0.00 | +51.24 | +61.04 | — |
| 2026-08-21 | `BTBT` | 758 | — | $1.66 | +0.00 | $1.53 | -98.54 | -98.54 | +0.00 | -98.54 |
| 2026-08-21 | `ENHA` | 735 | — | $1.71 | +0.00 | $1.72 | +7.35 | +7.35 | +0.00 | +7.35 |
| 2026-08-21 | `DE` | 2 | — | $623.26 | +0.00 | $647.47 | +48.42 | +48.42 | +0.00 | +48.42 |
| 2026-08-21 | `QDEL` | 84 | — | $14.96 | +0.00 | $14.74 | -18.48 | -18.48 | +0.00 | -18.48 |
| 2026-08-21 | `ORBS` | 1456 | — | $0.86 | +0.00 | $0.88 | +23.30 | +23.30 | +0.00 | +23.30 |
| 2026-08-21 | `GORO` | 404 | — | $3.11 | +0.00 | $3.19 | +32.32 | +32.32 | +0.00 | +32.32 |
| 2026-08-21 | `QTRX` | 393 | — | $3.11 | +0.00 | $2.99 | -47.16 | -47.16 | +0.00 | -47.16 |
| 2026-08-24 | `MRVI` | 164 | $8.70 | $8.59 | -18.04 | — | +0.00 | -18.04 | +198.44 | — |
| 2026-08-24 | `BTBT` | 758 | $1.53 | $1.55 | +15.16 | — | +0.00 | +15.16 | -83.38 | — |
| 2026-08-24 | `ENHA` | 735 | $1.72 | $1.74 | +14.70 | — | +0.00 | +14.70 | +22.05 | — |
| 2026-08-24 | `DE` | 2 | $647.47 | $653.62 | +12.30 | — | +0.00 | +12.30 | +60.72 | — |
| 2026-08-24 | `QDEL` | 84 | $14.74 | $14.71 | -2.52 | — | +0.00 | -2.52 | -21.00 | — |
| 2026-08-24 | `ORBS` | 1456 | $0.88 | $0.89 | +14.56 | — | +0.00 | +14.56 | +37.86 | — |
| 2026-08-24 | `GORO` | 404 | $3.19 | $3.20 | +4.04 | — | +0.00 | +4.04 | +36.36 | — |
| 2026-08-24 | `QTRX` | 393 | $2.99 | $2.98 | -3.93 | — | +0.00 | -3.93 | -51.09 | — |
| 2026-08-25 | `BMEA` | 780 | — | $1.62 | +0.00 | $1.61 | -7.80 | -7.80 | +0.00 | -7.80 |
| 2026-08-25 | `NPWR` | 632 | — | $2.00 | +0.00 | $2.02 | +12.64 | +12.64 | +0.00 | +12.64 |
| 2026-08-25 | `PUSA` | 341 | — | $3.70 | +0.00 | $3.91 | +71.61 | +71.61 | +0.00 | +71.61 |
| 2026-08-25 | `ALVO` | 242 | — | $5.22 | +0.00 | $5.25 | +7.26 | +7.26 | +0.00 | +7.26 |
| 2026-08-25 | `CAPR` | 186 | — | $6.79 | +0.00 | $7.19 | +74.40 | +74.40 | +0.00 | +74.40 |
| 2026-08-25 | `ALIT` | 85 | — | $14.86 | +0.00 | $14.87 | +0.85 | +0.85 | +0.00 | +0.85 |
| 2026-08-25 | `ZURA` | 198 | — | $6.38 | +0.00 | $6.50 | +23.76 | +23.76 | +0.00 | +23.76 |
| 2026-08-25 | `SAFX` | 3286 | — | $0.37 | +0.00 | $0.37 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `BMEA` | 780 | $1.61 | $1.61 | +0.00 | $1.61 | +0.00 | +0.00 | -7.80 | -7.80 |
| 2026-08-26 | `NPWR` | 632 | $2.02 | $2.02 | +0.00 | $2.02 | +0.00 | +0.00 | +12.64 | +12.64 |
| 2026-08-26 | `PUSA` | 341 | $3.91 | $3.91 | +0.00 | $3.91 | +0.00 | +0.00 | +71.61 | +71.61 |
| 2026-08-26 | `ALVO` | 242 | $5.25 | $5.25 | +0.00 | $5.25 | +0.00 | +0.00 | +7.26 | +7.26 |
| 2026-08-26 | `CAPR` | 186 | $7.19 | $7.19 | +0.00 | $7.19 | +0.00 | +0.00 | +74.40 | +74.40 |
| 2026-08-26 | `ALIT` | 85 | $14.87 | $14.87 | +0.00 | $14.87 | +0.00 | +0.00 | +0.85 | +0.85 |
| 2026-08-26 | `ZURA` | 198 | $6.50 | $6.50 | +0.00 | $6.50 | +0.00 | +0.00 | +23.76 | +23.76 |
| 2026-08-26 | `SAFX` | 3286 | $0.37 | $0.37 | +0.00 | $0.37 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-27 | `BMEA` | 780 | $1.61 | $1.75 | +109.20 | — | +0.00 | +109.20 | +101.40 | — |
| 2026-08-27 | `NPWR` | 632 | $2.02 | $1.93 | -56.88 | — | +0.00 | -56.88 | -44.24 | — |
| 2026-08-27 | `PUSA` | 341 | $3.91 | $3.84 | -23.87 | — | +0.00 | -23.87 | +47.74 | — |
| 2026-08-27 | `ALVO` | 242 | $5.25 | $4.98 | -65.34 | — | +0.00 | -65.34 | -58.08 | — |
| 2026-08-27 | `CAPR` | 186 | $7.19 | $8.29 | +204.60 | — | +0.00 | +204.60 | +279.00 | — |
| 2026-08-27 | `ALIT` | 85 | $14.87 | $14.85 | -1.70 | — | +0.00 | -1.70 | -0.85 | — |
| 2026-08-27 | `ZURA` | 198 | $6.50 | $6.13 | -73.26 | — | +0.00 | -73.26 | -49.50 | — |
| 2026-08-27 | `SAFX` | 3286 | $0.37 | $0.35 | -65.72 | — | +0.00 | -65.72 | -65.72 | — |
| 2026-08-28 | `ANF` | 8 | — | $144.70 | +0.00 | $145.75 | +8.40 | +8.40 | +0.00 | +8.40 |
| 2026-08-28 | `BHVN` | 75 | — | $16.95 | +0.00 | $16.12 | -62.25 | -62.25 | +0.00 | -62.25 |
| 2026-08-28 | `BZ` | 69 | — | $18.50 | +0.00 | $18.00 | -34.50 | -34.50 | +0.00 | -34.50 |
| 2026-08-28 | `CAPR` | 138 | — | $9.19 | +0.00 | $10.06 | +120.06 | +120.06 | +0.00 | +120.06 |
| 2026-08-28 | `LVWR` | 925 | — | $1.38 | +0.00 | $1.36 | -18.50 | -18.50 | +0.00 | -18.50 |
| 2026-08-28 | `SEDG` | 37 | — | $33.78 | +0.00 | $33.51 | -9.99 | -9.99 | +0.00 | -9.99 |
| 2026-08-28 | `SMTC` | 8 | — | $149.40 | +0.00 | $142.43 | -55.76 | -55.76 | +0.00 | -55.76 |
| 2026-08-28 | `GRRR` | 80 | — | $15.94 | +0.00 | $15.66 | -22.40 | -22.40 | +0.00 | -22.40 |
| 2026-08-31 | `ANF` | 8 | $145.75 | $148.67 | +23.36 | — | +0.00 | +23.36 | +31.76 | — |
| 2026-08-31 | `BHVN` | 75 | $16.12 | $15.44 | -51.00 | — | +0.00 | -51.00 | -113.25 | — |
| 2026-08-31 | `BZ` | 69 | $18.00 | $17.89 | -7.59 | — | +0.00 | -7.59 | -42.09 | — |
| 2026-08-31 | `CAPR` | 138 | $10.06 | $9.44 | -85.56 | — | +0.00 | -85.56 | +34.50 | — |
| 2026-08-31 | `LVWR` | 925 | $1.36 | $1.37 | +9.25 | — | +0.00 | +9.25 | -9.25 | — |
| 2026-08-31 | `SEDG` | 37 | $33.51 | $31.50 | -74.37 | — | +0.00 | -74.37 | -84.36 | — |
| 2026-08-31 | `SMTC` | 8 | $142.43 | $133.04 | -75.12 | — | +0.00 | -75.12 | -130.88 | — |
| 2026-08-31 | `GRRR` | 80 | $15.66 | $14.32 | -107.20 | — | +0.00 | -107.20 | -129.60 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GPRO` | 995 | — | $1.22 | +0.00 | $1.69 | +467.65 | +467.65 | +0.00 | +467.65 |
| 2026-09-03 | `FRVO` | 66 | — | $18.40 | +0.00 | $17.98 | -27.72 | -27.72 | +0.00 | -27.72 |
| 2026-09-03 | `CRK` | 77 | — | $15.70 | +0.00 | $15.54 | -12.32 | -12.32 | +0.00 | -12.32 |
| 2026-09-03 | `MMED` | 53 | — | $22.78 | +0.00 | $23.76 | +51.94 | +51.94 | +0.00 | +51.94 |
| 2026-09-03 | `CTMX` | 326 | — | $3.72 | +0.00 | $3.72 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `SLN` | 82 | — | $14.70 | +0.00 | $14.79 | +7.38 | +7.38 | +0.00 | +7.38 |
| 2026-09-03 | `EIX` | 21 | — | $56.78 | +0.00 | $55.19 | -33.39 | -33.39 | +0.00 | -33.39 |
| 2026-09-03 | `CRDL` | 562 | — | $2.16 | +0.00 | $2.17 | +5.62 | +5.62 | +0.00 | +5.62 |
| 2026-09-04 | `GPRO` | 995 | $1.69 | $1.78 | +89.55 | $1.39 | -388.05 | -298.50 | +557.20 | +169.15 |
| 2026-09-04 | `FRVO` | 66 | $17.98 | $18.27 | +19.14 | — | +0.00 | +19.14 | -8.58 | — |
| 2026-09-04 | `CRK` | 77 | $15.54 | $15.45 | -6.93 | — | +0.00 | -6.93 | -19.25 | — |
| 2026-09-04 | `MMED` | 53 | $23.76 | $23.88 | +6.36 | — | +0.00 | +6.36 | +58.30 | — |
| 2026-09-04 | `CTMX` | 326 | $3.72 | $3.73 | +3.26 | — | +0.00 | +3.26 | +3.26 | — |
| 2026-09-04 | `SLN` | 82 | $14.79 | $14.85 | +4.92 | — | +0.00 | +4.92 | +12.30 | — |
| 2026-09-04 | `EIX` | 21 | $55.19 | $55.42 | +4.83 | — | +0.00 | +4.83 | -28.56 | — |
| 2026-09-04 | `CRDL` | 562 | $2.17 | $2.18 | +5.62 | — | +0.00 | +5.62 | +11.24 | — |
| 2026-09-04 | `BAK` | 620 | — | $1.95 | +0.00 | $1.94 | -6.20 | -6.20 | +0.00 | -6.20 |
| 2026-09-04 | `EOSE` | 339 | — | $3.57 | +0.00 | $3.50 | -23.73 | -23.73 | +0.00 | -23.73 |
| 2026-09-04 | `SLBT` | 394 | — | $3.07 | +0.00 | $3.15 | +31.52 | +31.52 | +0.00 | +31.52 |
| 2026-09-04 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-04 | `MLYS` | 41 | — | $29.15 | +0.00 | $28.27 | -36.08 | -36.08 | +0.00 | -36.08 |
| 2026-09-04 | `CCOI` | 118 | — | $10.22 | +0.00 | $9.98 | -28.32 | -28.32 | +0.00 | -28.32 |
| 2026-09-04 | `SION` | 165 | — | $7.31 | +0.00 | $6.75 | -92.40 | -92.40 | +0.00 | -92.40 |
| 2026-09-07 | `GPRO` | 995 | $1.39 | $1.48 | +89.55 | — | +0.00 | +89.55 | +258.70 | — |
| 2026-09-07 | `BAK` | 620 | $1.94 | $1.94 | +0.00 | — | +0.00 | +0.00 | -6.20 | — |
| 2026-09-07 | `EOSE` | 339 | $3.50 | $3.52 | +6.78 | — | +0.00 | +6.78 | -16.95 | — |
| 2026-09-07 | `SLBT` | 394 | $3.15 | $3.15 | +0.00 | — | +0.00 | +0.00 | +31.52 | — |
| 2026-09-07 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | — | +0.00 | -5.22 | +54.94 | — |
| 2026-09-07 | `MLYS` | 41 | $28.27 | $28.00 | -11.07 | — | +0.00 | -11.07 | -47.15 | — |
| 2026-09-07 | `CCOI` | 118 | $9.98 | $10.02 | +4.72 | — | +0.00 | +4.72 | -23.60 | — |
| 2026-09-07 | `SION` | 165 | $6.75 | $6.68 | -11.55 | — | +0.00 | -11.55 | -103.95 | — |
| 2026-09-07 | `CHPT` | 131 | — | $9.28 | +0.00 | $9.89 | +79.91 | +79.91 | +0.00 | +79.91 |
| 2026-09-07 | `ABTC` | 136 | — | $8.95 | +0.00 | $7.99 | -130.56 | -130.56 | +0.00 | -130.56 |
| 2026-09-07 | `CHGG` | 1285 | — | $0.95 | +0.00 | $0.85 | -128.50 | -128.50 | +0.00 | -128.50 |
| 2026-09-07 | `MRLN` | 372 | — | $3.28 | +0.00 | $3.35 | +26.04 | +26.04 | +0.00 | +26.04 |
| 2026-09-07 | `SMMT` | 72 | — | $16.93 | +0.00 | $17.60 | +48.24 | +48.24 | +0.00 | +48.24 |
| 2026-09-07 | `BTBT` | 763 | — | $1.60 | +0.00 | $1.64 | +30.52 | +30.52 | +0.00 | +30.52 |
| 2026-09-07 | `SNOW` | 3 | — | $353.63 | +0.00 | $337.18 | -49.35 | -49.35 | +0.00 | -49.35 |
| 2026-09-07 | `CRCL` | 12 | — | $97.98 | +0.00 | $102.05 | +48.84 | +48.84 | +0.00 | +48.84 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +9.14 | ANGX, WWW, HYLN, WDC, FOSL, ADUR, AIRS, ALGM | — | $269.08 | $9,985.46 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28 |
| 2026-08-17 | +2.25 | $269.08 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28 | $10,059.20 | +73.74 | -129.60 | CDNL, ABX, FCEL, VERA, CELC, BW, OCC, ALM | ANGX, WWW, HYLN, WDC, FOSL, ADUR, AIRS, ALGM | $79.21 | $9,888.06 | CDNL×31, ABX×137, FCEL×56, VERA×40, CELC×13, BW×121, OCC×68, ALM×77 |
| 2026-08-18 | -6.20 | $79.21 | CDNL×31, ABX×137, FCEL×56, VERA×40, CELC×13, BW×121, OCC×68, ALM×77 | $9,722.67 | -165.39 | +0.00 | — | CDNL, ABX, FCEL, VERA, CELC, BW, OCC, ALM | $9,704.93 | $9,704.93 | — |
| 2026-08-19 | -7.20 | $9,704.93 | — | $9,704.93 | +0.00 | +0.00 | — | — | $9,704.93 | $9,704.93 | — |
| 2026-08-20 | +1.12 | $9,704.93 | — | $9,704.93 | +0.00 | +136.30 | MRVI, DNA, MSTR, EXK, SCZM, NG, BLSH, CRCL | — | $150.07 | $9,822.99 | MRVI×164, DNA×162, MSTR×10, EXK×112, SCZM×128, NG×144, BLSH×41, CRCL×14 |
| 2026-08-21 | +3.25 | $150.07 | MRVI×164, DNA×162, MSTR×10, EXK×112, SCZM×128, NG×144, BLSH×41, CRCL×14 | $10,169.44 | +346.45 | +29.21 | BTBT, ENHA, DE, QDEL, ORBS, GORO, QTRX | DNA, MSTR, EXK, SCZM, NG, BLSH, CRCL | $3.01 | $10,131.96 | MRVI×164, BTBT×758, ENHA×735, DE×2, QDEL×84, ORBS×1456, GORO×404, QTRX×393 |
| 2026-08-24 | -5.17 | $3.01 | MRVI×164, BTBT×758, ENHA×735, DE×2, QDEL×84, ORBS×1456, GORO×404, QTRX×393 | $10,168.23 | +36.27 | +0.00 | — | MRVI, BTBT, ENHA, DE, QDEL, ORBS, GORO, QTRX | $10,113.89 | $10,113.89 | — |
| 2026-08-25 | +1.80 | $10,113.89 | — | $10,113.89 | -0.00 | +182.72 | BMEA, NPWR, PUSA, ALVO, CAPR, ALIT, ZURA, SAFX | — | $1.12 | $10,241.48 | BMEA×780, NPWR×632, PUSA×341, ALVO×242, CAPR×186, ALIT×85, ZURA×198, SAFX×3286 |
| 2026-08-26 | +2.02 | $1.12 | BMEA×780, NPWR×632, PUSA×341, ALVO×242, CAPR×186, ALIT×85, ZURA×198, SAFX×3286 | $10,241.48 | -0.00 | +0.00 | — | — | $1.12 | $10,241.48 | BMEA×780, NPWR×632, PUSA×341, ALVO×242, CAPR×186, ALIT×85, ZURA×198, SAFX×3286 |
| 2026-08-27 | — | $1.12 | BMEA×780, NPWR×632, PUSA×341, ALVO×242, CAPR×186, ALIT×85, ZURA×198, SAFX×3286 | $10,268.51 | +27.03 | +0.00 | — | BMEA, NPWR, PUSA, ALVO, CAPR, ALIT, ZURA, SAFX | $10,213.00 | $10,213.00 | — |
| 2026-08-28 | +0.75 | $10,213.00 | — | $10,213.00 | -0.00 | -74.94 | ANF, BHVN, BZ, CAPR, LVWR, SEDG, SMTC, GRRR | — | $215.56 | $10,110.95 | ANF×8, BHVN×75, BZ×69, CAPR×138, LVWR×925, SEDG×37, SMTC×8, GRRR×80 |
| 2026-08-31 | -5.85 | $215.56 | ANF×8, BHVN×75, BZ×69, CAPR×138, LVWR×925, SEDG×37, SMTC×8, GRRR×80 | $9,742.72 | -368.23 | +0.00 | — | ANF, BHVN, BZ, CAPR, LVWR, SEDG, SMTC, GRRR | $9,715.29 | $9,715.29 | — |
| 2026-09-01 | -6.30 | $9,715.29 | — | $9,715.29 | +0.00 | +0.00 | — | — | $9,715.29 | $9,715.29 | — |
| 2026-09-02 | -3.83 | $9,715.29 | — | $9,715.29 | +0.00 | +0.00 | — | — | $9,715.29 | $9,715.29 | — |
| 2026-09-03 | -0.90 | $9,715.29 | — | $9,715.29 | +0.00 | +459.16 | GPRO, FRVO, CRK, MMED, CTMX, SLN, EIX, CRDL | — | $11.19 | $10,139.31 | GPRO×995, FRVO×66, CRK×77, MMED×53, CTMX×326, SLN×82, EIX×21, CRDL×562 |
| 2026-09-04 | — | $11.19 | GPRO×995, FRVO×66, CRK×77, MMED×53, CTMX×326, SLN×82, EIX×21, CRDL×562 | $10,266.06 | +126.75 | -483.10 | BAK, EOSE, SLBT, DELL, MLYS, CCOI, SION | FRVO, CRK, MMED, CTMX, SLN, EIX, CRDL | $237.30 | $9,733.99 | GPRO×995, BAK×620, EOSE×339, SLBT×394, DELL×2, MLYS×41, CCOI×118, SION×165 |
| 2026-09-07 | — | $237.30 | GPRO×995, BAK×620, EOSE×339, SLBT×394, DELL×2, MLYS×41, CCOI×118, SION×165 | $9,807.20 | +73.21 | -74.86 | CHPT, ABTC, CHGG, MRLN, SMMT, BTBT, SNOW, CRCL | GPRO, BAK, EOSE, SLBT, DELL, MLYS, CCOI, SION | $175.52 | $9,650.86 | CHPT×131, ABTC×136, CHGG×1285, MRLN×372, SMMT×72, BTBT×763, SNOW×3, CRCL×12 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $8,746.36 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WWW` | 60 | $20.60 | $2.17 | — | $7,508.19 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+4.4; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $6,254.51 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $5,245.52 | — | baseline list, no extra gate; list probable; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FOSL` | 221 | $5.64 | $2.85 | — | $3,996.23 | — | baseline list, no extra gate; list probable; 🔵; ret5=-4.1; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $2,756.51 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRS` | 370 | $3.37 | $4.77 | — | $1,504.84 | — | baseline list, no extra gate; list probable; ret5=-29.1; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 28 | $44.06 | $2.07 | — | $269.08 | — | baseline list, no extra gate; list probable; 🔵; ret5=+3.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $269.08 | ▲ close $9,985.46 vs 09:30 $10,000.00 (session +9.14) | 16:00 close · cash $269.08 · equity $9,985.46 vs 09:30 $10,000.00 (-14.54; session marks +9.14) · 8 name(s) marked open→close (per-name table). ANGX×290 09:30 $4.31 → close $4.37 +17.40; WWW×60 09:30 $20.60 → close $21.03 +25.80; HYLN×299 09:30 $4.18 → close $4.06 -35.88; WDC×2 09:30 $503.50 → close $508.80 +10.60; FOSL×221 09:30 $5.64 → close $5.57 -15.47; ADUR×75 09:30 $16.50 → close $16.17 -24.75; AIRS×370 09:30 $3.37 → close $3.43 +22.20; ALGM×28 09:30 $44.06 → close $44.39 +9.24 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $269.08 | ▲ 09:30 equity $10,059.20 vs yday $9,985.46 (+73.74) | 09:30 open · cash $269.08 (unchanged overnight, no fees) · equity $10,059.20 vs prior close $9,985.46 (+73.74) · 8 name(s) re-marked at the open (per-name table). ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; WWW×60 yday $21.03 → 09:30 $20.98 -3.00; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; WDC×2 yday $508.80 → 09:30 $525.53 +33.46; FOSL×221 yday $5.57 → 09:30 $5.50 -15.47; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; AIRS×370 yday $3.43 → 09:30 $3.40 -12.95; ALGM×28 yday $44.39 → 09:30 $45.32 +26.04 | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $1,599.28 | ▲ +76.56 after sell → book $10,055.40; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WWW` | 60 | $20.98 | $2.19 | $+18.44 | $2,855.89 | ▲ +18.44 after sell → book $10,053.21; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $4,077.88 | ▼ -31.69 after sell → book $10,049.30; vs 09:30 mark -3.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $5,126.92 | ▲ +40.05 after sell → book $10,047.28; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `FOSL` | 221 | $5.50 | $2.90 | $-36.69 | $6,339.52 | ▼ -36.69 after sell → book $10,044.38; vs 09:30 mark -2.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $7,517.04 | ▼ -62.20 after sell → book $10,042.15; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRS` | 370 | $3.40 | $4.84 | $-0.37 | $8,768.34 | ▼ -0.37 after sell → book $10,037.30; vs 09:30 mark -4.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ALGM` | 28 | $45.32 | $2.09 | $+31.11 | $10,035.21 | ▲ +31.11 after sell → book $10,035.21; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $8,797.77 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1254.40 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 137 | $9.12 | $2.40 | — | $7,545.93 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1254.40 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `FCEL` | 56 | $22.37 | $2.16 | — | $6,291.05 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+9.5; leftover $1254.40 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 40 | $31.30 | $2.11 | — | $5,036.94 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-3.8; leftover $1254.40 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 13 | $92.99 | $2.03 | — | $3,826.05 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.8; leftover $1254.40 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BW` | 121 | $10.35 | $2.35 | — | $2,571.34 | — | baseline list, no extra gate; list probable; ⚪; ret5=+9.8; leftover $1254.40 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 68 | $18.24 | $2.19 | — | $1,328.83 | — | baseline list, no extra gate; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1254.40 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 77 | $16.20 | $2.22 | — | $79.21 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1254.40 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.21 | ▼ close $9,888.06 vs 09:30 $10,059.20 (session -129.60) | 16:00 close · cash $79.21 · equity $9,888.06 vs 09:30 $10,059.20 (-171.14; session marks -129.60) · 8 name(s) marked open→close (per-name table). CDNL×31 09:30 $39.85 → close $39.23 -19.22; ABX×137 09:30 $9.12 → close $9.12 +0.00; FCEL×56 09:30 $22.37 → close $22.36 -0.56; VERA×40 09:30 $31.30 → close $31.63 +13.20; CELC×13 09:30 $92.99 → close $92.44 -7.15; BW×121 09:30 $10.35 → close $9.92 -52.03; OCC×68 09:30 $18.24 → close $17.12 -76.16; ALM×77 09:30 $16.20 → close $16.36 +12.32 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.21 | ▼ 09:30 equity $9,722.67 vs yday $9,888.06 (-165.39) | 09:30 open · cash $79.21 (unchanged overnight, no fees) · equity $9,722.67 vs prior close $9,888.06 (-165.39) · 8 name(s) re-marked at the open (per-name table). CDNL×31 yday $39.23 → 09:30 $41.57 +72.54; ABX×137 yday $9.12 → 09:30 $9.03 -12.33; FCEL×56 yday $22.36 → 09:30 $21.18 -66.08; VERA×40 yday $31.63 → 09:30 $31.31 -12.80; CELC×13 yday $92.44 → 09:30 $92.38 -0.78; BW×121 yday $9.92 → 09:30 $9.60 -38.72; OCC×68 yday $17.12 → 09:30 $16.20 -62.56; ALM×77 yday $16.36 → 09:30 $15.78 -44.66 | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $1,365.77 | ▲ +49.13 after sell → book $9,720.56; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 137 | $9.03 | $2.43 | $-17.16 | $2,600.45 | ▼ -17.16 after sell → book $9,718.13; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FCEL` | 56 | $21.18 | $2.18 | $-70.98 | $3,784.35 | ▼ -70.98 after sell → book $9,715.95; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 40 | $31.31 | $2.13 | $-3.84 | $5,034.62 | ▼ -3.84 after sell → book $9,713.82; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 13 | $92.38 | $2.05 | $-12.01 | $6,233.51 | ▼ -12.01 after sell → book $9,711.77; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BW` | 121 | $9.60 | $2.38 | $-95.49 | $7,392.73 | ▼ -95.49 after sell → book $9,709.39; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 68 | $16.20 | $2.22 | $-143.13 | $8,492.12 | ▼ -143.13 after sell → book $9,707.18; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 77 | $15.78 | $2.24 | $-36.80 | $9,704.93 | ▼ -36.80 after sell → book $9,704.93; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,704.93 | ▲ close $9,704.93 vs 09:30 $9,722.67 (session +0.00) | 16:00 close · cash $9,704.93 · no lots left · equity $9,704.93. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,704.93 | ▲ 09:30 equity $9,704.93 vs yday $9,704.93 (+0.00) | 09:30 open · cash $9,704.93 · no holdings · equity $9,704.93 vs prior close $9,704.93 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,704.93 | ▲ close $9,704.93 vs 09:30 $9,704.93 (session +0.00) | 16:00 close · cash $9,704.93 · no lots left · equity $9,704.93. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,704.93 | ▲ 09:30 equity $9,704.93 vs yday $9,704.93 (+0.00) | 09:30 open · cash $9,704.93 · no holdings · equity $9,704.93 vs prior close $9,704.93 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 164 | $7.38 | $2.48 | — | $8,492.13 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1213.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 162 | $7.45 | $2.48 | — | $7,282.75 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1213.12 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 10 | $113.23 | $2.02 | — | $6,148.43 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1213.12 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 112 | $10.77 | $2.33 | — | $4,939.87 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1213.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 128 | $9.46 | $2.37 | — | $3,726.61 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1213.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 144 | $8.38 | $2.42 | — | $2,517.47 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1213.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 41 | $29.20 | $2.11 | — | $1,318.16 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1213.12 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRCL` | 14 | $83.29 | $2.03 | — | $150.07 | — | baseline list, no extra gate; list probable; 🔵; ⚪; ret5=+7.4; leftover $1213.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $150.07 | ▲ close $9,822.99 vs 09:30 $9,704.93 (session +136.30) | 16:00 close · cash $150.07 · equity $9,822.99 vs 09:30 $9,704.93 (+118.06; session marks +136.30) · 8 name(s) marked open→close (per-name table). MRVI×164 09:30 $7.38 → close $8.26 +144.32; DNA×162 09:30 $7.45 → close $6.96 -79.38; MSTR×10 09:30 $113.23 → close $112.39 -8.40; EXK×112 09:30 $10.77 → close $10.97 +22.40; SCZM×128 09:30 $9.46 → close $9.76 +38.40; NG×144 09:30 $8.38 → close $8.66 +40.32; BLSH×41 09:30 $29.20 → close $28.44 -31.16; CRCL×14 09:30 $83.29 → close $83.99 +9.80 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $150.07 | ▲ 09:30 equity $10,169.44 vs yday $9,822.99 (+346.45) | 09:30 open · cash $150.07 (unchanged overnight, no fees) · equity $10,169.44 vs prior close $9,822.99 (+346.45) · 8 name(s) re-marked at the open (per-name table). MRVI×164 yday $8.26 → 09:30 $8.20 -9.84; DNA×162 yday $6.96 → 09:30 $7.09 +21.06; MSTR×10 yday $112.39 → 09:30 $119.69 +73.00; EXK×112 yday $10.97 → 09:30 $11.34 +41.44; SCZM×128 yday $9.76 → 09:30 $10.26 +64.00; NG×144 yday $8.66 → 09:30 $9.02 +51.84; BLSH×41 yday $28.44 → 09:30 $29.75 +53.71; CRCL×14 yday $83.99 → 09:30 $87.65 +51.24 | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 162 | $7.09 | $2.51 | $-63.31 | $1,296.13 | ▼ -63.31 after sell → book $10,166.92; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MSTR` | 10 | $119.69 | $2.04 | $+60.54 | $2,490.99 | ▲ +60.54 after sell → book $10,164.88; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `EXK` | 112 | $11.34 | $2.35 | $+59.16 | $3,758.72 | ▲ +59.16 after sell → book $10,162.53; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 128 | $10.26 | $2.41 | $+97.62 | $5,069.59 | ▲ +97.62 after sell → book $10,160.12; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NG` | 144 | $9.02 | $2.46 | $+87.28 | $6,366.02 | ▲ +87.28 after sell → book $10,157.67; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BLSH` | 41 | $29.75 | $2.13 | $+18.30 | $7,583.63 | ▲ +18.30 after sell → book $10,155.53; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CRCL` | 14 | $87.65 | $2.05 | $+56.96 | $8,808.68 | ▲ +56.96 after sell → book $10,153.48; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 758 | $1.66 | $9.78 | — | $7,540.62 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1258.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 735 | $1.71 | $9.48 | — | $6,274.29 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1258.38 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $5,025.78 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1258.38 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 84 | $14.96 | $2.24 | — | $3,766.89 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-1.6; leftover $1258.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1456 | $0.86 | $16.95 | — | $2,491.96 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1258.38 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 404 | $3.11 | $5.21 | — | $1,230.31 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+7.1; leftover $1258.38 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QTRX` | 393 | $3.11 | $5.07 | — | $3.01 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $1258.38 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.01 | ▲ close $10,131.96 vs 09:30 $10,169.44 (session +29.21) | 16:00 close · cash $3.01 · equity $10,131.96 vs 09:30 $10,169.44 (-37.48; session marks +29.21) · 8 name(s) marked open→close (per-name table). MRVI×164 09:30 $8.20 → close $8.70 +82.00; BTBT×758 09:30 $1.66 → close $1.53 -98.54; ENHA×735 09:30 $1.71 → close $1.72 +7.35; DE×2 09:30 $623.26 → close $647.47 +48.42; QDEL×84 09:30 $14.96 → close $14.74 -18.48; ORBS×1456 09:30 $0.86 → close $0.88 +23.30; GORO×404 09:30 $3.11 → close $3.19 +32.32; QTRX×393 09:30 $3.11 → close $2.99 -47.16 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.01 | ▲ 09:30 equity $10,168.23 vs yday $10,131.96 (+36.27) | 09:30 open · cash $3.01 (unchanged overnight, no fees) · equity $10,168.23 vs prior close $10,131.96 (+36.27) · 8 name(s) re-marked at the open (per-name table). MRVI×164 yday $8.70 → 09:30 $8.59 -18.04; BTBT×758 yday $1.53 → 09:30 $1.55 +15.16; ENHA×735 yday $1.72 → 09:30 $1.74 +14.70; DE×2 yday $647.47 → 09:30 $653.62 +12.30; QDEL×84 yday $14.74 → 09:30 $14.71 -2.52; ORBS×1456 yday $0.88 → 09:30 $0.89 +14.56; GORO×404 yday $3.19 → 09:30 $3.20 +4.04; QTRX×393 yday $2.99 → 09:30 $2.98 -3.93 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 164 | $8.59 | $2.52 | $+193.44 | $1,409.25 | ▲ +193.44 after sell → book $10,165.71; vs 09:30 mark -2.52 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 758 | $1.55 | $9.91 | $-103.07 | $2,574.24 | ▼ -103.07 after sell → book $10,155.80; vs 09:30 mark -9.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ENHA` | 735 | $1.74 | $9.61 | $+2.95 | $3,843.52 | ▲ +2.95 after sell → book $10,146.18; vs 09:30 mark -9.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.62 | $2.02 | $+56.71 | $5,148.75 | ▲ +56.71 after sell → book $10,144.17; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 84 | $14.71 | $2.27 | $-25.51 | $6,382.12 | ▼ -25.51 after sell → book $10,141.90; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1456 | $0.89 | $17.58 | $+3.33 | $7,660.38 | ▲ +3.33 after sell → book $10,124.32; vs 09:30 mark -17.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GORO` | 404 | $3.20 | $5.29 | $+25.86 | $8,947.89 | ▲ +25.86 after sell → book $10,119.03; vs 09:30 mark -5.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QTRX` | 393 | $2.98 | $5.14 | $-61.30 | $10,113.89 | ▼ -61.30 after sell → book $10,113.89; vs 09:30 mark -5.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,113.89 | ▲ close $10,113.89 vs 09:30 $10,168.23 (session +0.00) | 16:00 close · cash $10,113.89 · no lots left · equity $10,113.89. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,113.89 | ▲ 09:30 equity $10,113.89 vs yday $10,113.89 (-0.00) | 09:30 open · cash $10,113.89 · no holdings · equity $10,113.89 vs prior close $10,113.89 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 780 | $1.62 | $10.06 | — | $8,840.23 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1264.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 632 | $2.00 | $8.15 | — | $7,568.07 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1264.24 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 341 | $3.70 | $4.40 | — | $6,301.97 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1264.24 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 242 | $5.22 | $3.12 | — | $5,035.61 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1264.24 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 186 | $6.79 | $2.55 | — | $3,770.12 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1264.24 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 85 | $14.86 | $2.25 | — | $2,504.78 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1264.24 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 198 | $6.38 | $2.58 | — | $1,238.96 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1264.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3286 | $0.37 | $22.02 | — | $1.12 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-26.5; leftover $1264.24 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.12 | ▲ close $10,241.48 vs 09:30 $10,113.89 (session +182.72) | 16:00 close · cash $1.12 · equity $10,241.48 vs 09:30 $10,113.89 (+127.59; session marks +182.72) · 8 name(s) marked open→close (per-name table). BMEA×780 09:30 $1.62 → close $1.61 -7.80; NPWR×632 09:30 $2.00 → close $2.02 +12.64; PUSA×341 09:30 $3.70 → close $3.91 +71.61; ALVO×242 09:30 $5.22 → close $5.25 +7.26; CAPR×186 09:30 $6.79 → close $7.19 +74.40; ALIT×85 09:30 $14.86 → close $14.87 +0.85; ZURA×198 09:30 $6.38 → close $6.50 +23.76; SAFX×3286 09:30 $0.37 → close $0.37 +0.00 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.12 | ▲ 09:30 equity $10,241.48 vs yday $10,241.48 (-0.00) | 09:30 open · cash $1.12 (unchanged overnight, no fees) · equity $10,241.48 vs prior close $10,241.48 (-0.00) · 8 name(s) re-marked at the open (per-name table). BMEA×780 yday $1.61 → 09:30 $1.61 +0.00; NPWR×632 yday $2.02 → 09:30 $2.02 +0.00; PUSA×341 yday $3.91 → 09:30 $3.91 +0.00; ALVO×242 yday $5.25 → 09:30 $5.25 +0.00; CAPR×186 yday $7.19 → 09:30 $7.19 +0.00; ALIT×85 yday $14.87 → 09:30 $14.87 +0.00; ZURA×198 yday $6.50 → 09:30 $6.50 +0.00; SAFX×3286 yday $0.37 → 09:30 $0.37 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.12 | ▲ close $10,241.48 vs 09:30 $10,241.48 (session +0.00) | 16:00 close · cash $1.12 · equity $10,241.48 vs 09:30 $10,241.48 (-0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). BMEA×780 09:30 $1.61 → close $1.61 +0.00; NPWR×632 09:30 $2.02 → close $2.02 +0.00; PUSA×341 09:30 $3.91 → close $3.91 +0.00; ALVO×242 09:30 $5.25 → close $5.25 +0.00; CAPR×186 09:30 $7.19 → close $7.19 +0.00; ALIT×85 09:30 $14.87 → close $14.87 +0.00; ZURA×198 09:30 $6.50 → close $6.50 +0.00; SAFX×3286 09:30 $0.37 → close $0.37 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.12 | ▲ 09:30 equity $10,268.51 vs yday $10,241.48 (+27.03) | 09:30 open · cash $1.12 (unchanged overnight, no fees) · equity $10,268.51 vs prior close $10,241.48 (+27.03) · 8 name(s) re-marked at the open (per-name table). BMEA×780 yday $1.61 → 09:30 $1.75 +109.20; NPWR×632 yday $2.02 → 09:30 $1.93 -56.88; PUSA×341 yday $3.91 → 09:30 $3.84 -23.87; ALVO×242 yday $5.25 → 09:30 $4.98 -65.34; CAPR×186 yday $7.19 → 09:30 $8.29 +204.60; ALIT×85 yday $14.87 → 09:30 $14.85 -1.70; ZURA×198 yday $6.50 → 09:30 $6.13 -73.26; SAFX×3286 yday $0.37 → 09:30 $0.35 -65.72 | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 780 | $1.75 | $10.20 | $+81.14 | $1,355.92 | ▲ +81.14 after sell → book $10,258.31; vs 09:30 mark -10.20 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 632 | $1.93 | $8.27 | $-60.66 | $2,567.41 | ▼ -60.66 after sell → book $10,250.04; vs 09:30 mark -8.27 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PUSA` | 341 | $3.84 | $4.47 | $+38.88 | $3,872.38 | ▲ +38.88 after sell → book $10,245.57; vs 09:30 mark -4.47 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 242 | $4.98 | $3.17 | $-64.37 | $5,074.37 | ▼ -64.37 after sell → book $10,242.40; vs 09:30 mark -3.17 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 186 | $8.29 | $2.59 | $+273.86 | $6,613.72 | ▲ +273.86 after sell → book $10,239.81; vs 09:30 mark -2.59 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALIT` | 85 | $14.85 | $2.27 | $-5.36 | $7,873.70 | ▼ -5.36 after sell → book $10,237.54; vs 09:30 mark -2.27 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 198 | $6.13 | $2.63 | $-54.71 | $9,084.81 | ▼ -54.71 after sell → book $10,234.91; vs 09:30 mark -2.63 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SAFX` | 3286 | $0.35 | $21.91 | $-109.65 | $10,213.00 | ▼ -109.65 after sell → book $10,213.00; vs 09:30 mark -21.91 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,213.00 | ▲ close $10,213.00 vs 09:30 $10,268.51 (session +0.00) | 16:00 close · cash $10,213.00 · no lots left · equity $10,213.00. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,213.00 | ▲ 09:30 equity $10,213.00 vs yday $10,213.00 (-0.00) | 09:30 open · cash $10,213.00 · no holdings · equity $10,213.00 vs prior close $10,213.00 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $144.70 | $2.01 | — | $9,053.39 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1276.62 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 75 | $16.95 | $2.21 | — | $7,779.92 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1276.62 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 69 | $18.50 | $2.20 | — | $6,501.22 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1276.62 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 138 | $9.19 | $2.40 | — | $5,230.60 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1276.62 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 925 | $1.38 | $11.93 | — | $3,942.17 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1276.62 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 37 | $33.78 | $2.10 | — | $2,690.21 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1276.62 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $1,492.99 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1276.62 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 80 | $15.94 | $2.23 | — | $215.56 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1276.62 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $215.56 | ▼ close $10,110.95 vs 09:30 $10,213.00 (session -74.94) | 16:00 close · cash $215.56 · equity $10,110.95 vs 09:30 $10,213.00 (-102.05; session marks -74.94) · 8 name(s) marked open→close (per-name table). ANF×8 09:30 $144.70 → close $145.75 +8.40; BHVN×75 09:30 $16.95 → close $16.12 -62.25; BZ×69 09:30 $18.50 → close $18.00 -34.50; CAPR×138 09:30 $9.19 → close $10.06 +120.06; LVWR×925 09:30 $1.38 → close $1.36 -18.50; SEDG×37 09:30 $33.78 → close $33.51 -9.99; SMTC×8 09:30 $149.40 → close $142.43 -55.76; GRRR×80 09:30 $15.94 → close $15.66 -22.40 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $215.56 | ▼ 09:30 equity $9,742.72 vs yday $10,110.95 (-368.23) | 09:30 open · cash $215.56 (unchanged overnight, no fees) · equity $9,742.72 vs prior close $10,110.95 (-368.23) · 8 name(s) re-marked at the open (per-name table). ANF×8 yday $145.75 → 09:30 $148.67 +23.36; BHVN×75 yday $16.12 → 09:30 $15.44 -51.00; BZ×69 yday $18.00 → 09:30 $17.89 -7.59; CAPR×138 yday $10.06 → 09:30 $9.44 -85.56; LVWR×925 yday $1.36 → 09:30 $1.37 +9.25; SEDG×37 yday $33.51 → 09:30 $31.50 -74.37; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; GRRR×80 yday $15.66 → 09:30 $14.32 -107.20 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.67 | $2.03 | $+27.71 | $1,402.89 | ▲ +27.71 after sell → book $9,740.69; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 75 | $15.44 | $2.24 | $-117.70 | $2,558.65 | ▼ -117.70 after sell → book $9,738.45; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 69 | $17.89 | $2.22 | $-46.51 | $3,790.84 | ▼ -46.51 after sell → book $9,736.23; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 138 | $9.44 | $2.44 | $+29.66 | $5,091.12 | ▲ +29.66 after sell → book $9,733.79; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 925 | $1.37 | $12.10 | $-33.28 | $6,346.28 | ▼ -33.28 after sell → book $9,721.70; vs 09:30 mark -12.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 37 | $31.50 | $2.12 | $-88.58 | $7,509.66 | ▼ -88.58 after sell → book $9,719.58; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $133.04 | $2.03 | $-134.93 | $8,571.94 | ▼ -134.93 after sell → book $9,717.54; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 80 | $14.32 | $2.25 | $-134.08 | $9,715.29 | ▼ -134.08 after sell → book $9,715.29; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,715.29 | ▲ close $9,715.29 vs 09:30 $9,742.72 (session +0.00) | 16:00 close · cash $9,715.29 · no lots left · equity $9,715.29. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,715.29 | ▲ 09:30 equity $9,715.29 vs yday $9,715.29 (+0.00) | 09:30 open · cash $9,715.29 · no holdings · equity $9,715.29 vs prior close $9,715.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,715.29 | ▲ close $9,715.29 vs 09:30 $9,715.29 (session +0.00) | 16:00 close · cash $9,715.29 · no lots left · equity $9,715.29. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,715.29 | ▲ 09:30 equity $9,715.29 vs yday $9,715.29 (+0.00) | 09:30 open · cash $9,715.29 · no holdings · equity $9,715.29 vs prior close $9,715.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,715.29 | ▲ close $9,715.29 vs 09:30 $9,715.29 (session +0.00) | 16:00 close · cash $9,715.29 · no lots left · equity $9,715.29. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,715.29 | ▲ 09:30 equity $9,715.29 vs yday $9,715.29 (+0.00) | 09:30 open · cash $9,715.29 · no holdings · equity $9,715.29 vs prior close $9,715.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 995 | $1.22 | $12.84 | — | $8,488.55 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1214.41 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 66 | $18.40 | $2.19 | — | $7,271.97 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1214.41 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 77 | $15.70 | $2.22 | — | $6,060.85 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1214.41 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 53 | $22.78 | $2.15 | — | $4,851.36 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1214.41 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 326 | $3.72 | $4.21 | — | $3,634.43 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1214.41 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 82 | $14.70 | $2.24 | — | $2,426.80 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1214.41 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 21 | $56.78 | $2.05 | — | $1,232.36 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+0.3; leftover $1214.41 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 562 | $2.16 | $7.25 | — | $11.19 | — | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1214.41 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.19 | ▲ close $10,139.31 vs 09:30 $9,715.29 (session +459.16) | 16:00 close · cash $11.19 · equity $10,139.31 vs 09:30 $9,715.29 (+424.02; session marks +459.16) · 8 name(s) marked open→close (per-name table). GPRO×995 09:30 $1.22 → close $1.69 +467.65; FRVO×66 09:30 $18.40 → close $17.98 -27.72; CRK×77 09:30 $15.70 → close $15.54 -12.32; MMED×53 09:30 $22.78 → close $23.76 +51.94; CTMX×326 09:30 $3.72 → close $3.72 +0.00; SLN×82 09:30 $14.70 → close $14.79 +7.38; EIX×21 09:30 $56.78 → close $55.19 -33.39; CRDL×562 09:30 $2.16 → close $2.17 +5.62 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.19 | ▲ 09:30 equity $10,266.06 vs yday $10,139.31 (+126.75) | 09:30 open · cash $11.19 (unchanged overnight, no fees) · equity $10,266.06 vs prior close $10,139.31 (+126.75) · 8 name(s) re-marked at the open (per-name table). GPRO×995 yday $1.69 → 09:30 $1.78 +89.55; FRVO×66 yday $17.98 → 09:30 $18.27 +19.14; CRK×77 yday $15.54 → 09:30 $15.45 -6.93; MMED×53 yday $23.76 → 09:30 $23.88 +6.36; CTMX×326 yday $3.72 → 09:30 $3.73 +3.26; SLN×82 yday $14.79 → 09:30 $14.85 +4.92; EIX×21 yday $55.19 → 09:30 $55.42 +4.83; CRDL×562 yday $2.17 → 09:30 $2.18 +5.62 | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 66 | $18.27 | $2.21 | $-12.98 | $1,214.80 | ▼ -12.98 after sell → book $10,263.85; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 77 | $15.45 | $2.24 | $-23.71 | $2,402.21 | ▼ -23.71 after sell → book $10,261.61; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 53 | $23.88 | $2.17 | $+53.98 | $3,665.68 | ▲ +53.98 after sell → book $10,259.44; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTMX` | 326 | $3.73 | $4.27 | $-5.21 | $4,877.39 | ▼ -5.21 after sell → book $10,255.17; vs 09:30 mark -4.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 82 | $14.85 | $2.26 | $+7.80 | $6,092.83 | ▲ +7.80 after sell → book $10,252.91; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 21 | $55.42 | $2.07 | $-32.69 | $7,254.58 | ▼ -32.69 after sell → book $10,250.84; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 562 | $2.18 | $7.35 | $-3.36 | $8,472.39 | ▼ -3.36 after sell → book $10,243.49; vs 09:30 mark -7.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 620 | $1.95 | $8.00 | — | $7,255.39 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1210.34 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 339 | $3.57 | $4.37 | — | $6,040.78 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1210.34 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 394 | $3.07 | $5.08 | — | $4,826.12 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1210.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $3,851.51 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-9.9; leftover $1210.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 41 | $29.15 | $2.11 | — | $2,654.24 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1210.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 118 | $10.22 | $2.34 | — | $1,445.94 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1210.34 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SION` | 165 | $7.31 | $2.48 | — | $237.30 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1210.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $237.30 | ▼ close $9,733.99 vs 09:30 $10,266.06 (session -483.10) | 16:00 close · cash $237.30 · equity $9,733.99 vs 09:30 $10,266.06 (-532.07; session marks -483.10) · 8 name(s) marked open→close (per-name table). GPRO×995 09:30 $1.78 → close $1.39 -388.05; BAK×620 09:30 $1.95 → close $1.94 -6.20; EOSE×339 09:30 $3.57 → close $3.50 -23.73; SLBT×394 09:30 $3.07 → close $3.15 +31.52; DELL×2 09:30 $486.31 → close $516.39 +60.16; MLYS×41 09:30 $29.15 → close $28.27 -36.08; CCOI×118 09:30 $10.22 → close $9.98 -28.32; SION×165 09:30 $7.31 → close $6.75 -92.40 | — |
| 2026-09-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $237.30 | ▲ 09:30 equity $9,807.20 vs yday $9,733.99 (+73.21) | 09:30 open · cash $237.30 (unchanged overnight, no fees) · equity $9,807.20 vs prior close $9,733.99 (+73.21) · 8 name(s) re-marked at the open (per-name table). GPRO×995 yday $1.39 → 09:30 $1.48 +89.55; BAK×620 yday $1.94 → 09:30 $1.94 +0.00; EOSE×339 yday $3.50 → 09:30 $3.52 +6.78; SLBT×394 yday $3.15 → 09:30 $3.15 +0.00; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; MLYS×41 yday $28.27 → 09:30 $28.00 -11.07; CCOI×118 yday $9.98 → 09:30 $10.02 +4.72; SION×165 yday $6.75 → 09:30 $6.68 -11.55 | — |
| 2026-09-07 09:30 ET | **SELL** | `GPRO` | 995 | $1.48 | $13.01 | $+232.85 | $1,696.89 | ▲ +232.85 after sell → book $9,794.19; vs 09:30 mark -13.01 | dropped from list after 2 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **SELL** | `BAK` | 620 | $1.94 | $8.11 | $-22.31 | $2,891.58 | ▼ -22.31 after sell → book $9,786.08; vs 09:30 mark -8.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `EOSE` | 339 | $3.52 | $4.44 | $-25.76 | $4,080.42 | ▼ -25.76 after sell → book $9,781.64; vs 09:30 mark -4.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `SLBT` | 394 | $3.15 | $5.16 | $+21.28 | $5,316.36 | ▲ +21.28 after sell → book $9,776.48; vs 09:30 mark -5.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $6,341.91 | ▲ +50.93 after sell → book $9,774.47; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `MLYS` | 41 | $28.00 | $2.13 | $-51.40 | $7,487.77 | ▼ -51.40 after sell → book $9,772.33; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `CCOI` | 118 | $10.02 | $2.37 | $-28.32 | $8,667.76 | ▼ -28.32 after sell → book $9,769.96; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `SION` | 165 | $6.68 | $2.52 | $-108.96 | $9,767.44 | ▼ -108.96 after sell → book $9,767.44; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **BUY** | `CHPT` | 131 | $9.28 | $2.38 | — | $8,549.37 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.1; leftover $1220.93 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `ABTC` | 136 | $8.95 | $2.40 | — | $7,329.78 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1220.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `CHGG` | 1285 | $0.95 | $16.06 | — | $6,092.96 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $1220.93 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `MRLN` | 372 | $3.28 | $4.80 | — | $4,868.01 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.6; leftover $1220.93 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `SMMT` | 72 | $16.93 | $2.21 | — | $3,646.84 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-1.4; leftover $1220.93 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `BTBT` | 763 | $1.60 | $9.84 | — | $2,416.20 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-2.5; leftover $1220.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `SNOW` | 3 | $353.63 | $2.00 | — | $1,353.31 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+1.2; leftover $1220.93 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `CRCL` | 12 | $97.98 | $2.03 | — | $175.52 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+7.4; leftover $1220.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $175.52 | ▼ close $9,650.86 vs 09:30 $9,807.20 (session -74.86) | 16:00 close · cash $175.52 · equity $9,650.86 vs 09:30 $9,807.20 (-156.34; session marks -74.86) · 8 name(s) marked open→close (per-name table). CHPT×131 09:30 $9.28 → close $9.89 +79.91; ABTC×136 09:30 $8.95 → close $7.99 -130.56; CHGG×1285 09:30 $0.95 → close $0.85 -128.50; MRLN×372 09:30 $3.28 → close $3.35 +26.04; SMMT×72 09:30 $16.93 → close $17.60 +48.24; BTBT×763 09:30 $1.60 → close $1.64 +30.52; SNOW×3 09:30 $353.63 → close $337.18 -49.35; CRCL×12 09:30 $97.98 → close $102.05 +48.84 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CBRS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COHR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDTH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `PUSA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALIT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SAFX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-26 | `BE` | no_price | no 09:30 open |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CHPT` | 131 | 2026-09-07 @ $9.28 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.1; leftover $1220.93 |
| `ABTC` | 136 | 2026-09-07 @ $8.95 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1220.93 |
| `CHGG` | 1285 | 2026-09-07 @ $0.95 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $1220.93 |
| `MRLN` | 372 | 2026-09-07 @ $3.28 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.6; leftover $1220.93 |
| `SMMT` | 72 | 2026-09-07 @ $16.93 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-1.4; leftover $1220.93 |
| `BTBT` | 763 | 2026-09-07 @ $1.60 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-2.5; leftover $1220.93 |
| `SNOW` | 3 | 2026-09-07 @ $353.63 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+1.2; leftover $1220.93 |
| `CRCL` | 12 | 2026-09-07 @ $97.98 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+7.4; leftover $1220.93 |
