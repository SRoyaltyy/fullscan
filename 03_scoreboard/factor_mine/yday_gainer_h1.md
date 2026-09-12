# Factor mine action — `yday_gainer_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `yday_gainer` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **+6.53%** ($10,653) · signal-only (no cash/fees) was +11.19%. Starts YES **11/21**. Fills 152 · skips 72 · realized $+616.11.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at yesterday's top liquid winners and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: yesterday's top liquid winners.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).

### When it buys

- At 09:30, take names on yesterday's top liquid winners that pass the must-haves.
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

- **Universe** `yday_gainer` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $73.95.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ANGX` | 290 | — | $4.31 | +0.00 | $4.37 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-14 | `WWW` | 60 | — | $20.60 | +0.00 | $21.03 | +25.80 | +25.80 | +0.00 | +25.80 |
| 2026-08-14 | `HYLN` | 299 | — | $4.18 | +0.00 | $4.06 | -35.88 | -35.88 | +0.00 | -35.88 |
| 2026-08-14 | `ARX` | 63 | — | $19.57 | +0.00 | $19.58 | +0.63 | +0.63 | +0.00 | +0.63 |
| 2026-08-14 | `OMER` | 72 | — | $17.35 | +0.00 | $17.19 | -11.52 | -11.52 | +0.00 | -11.52 |
| 2026-08-14 | `AIRO` | 112 | — | $11.12 | +0.00 | $9.57 | -173.60 | -173.60 | +0.00 | -173.60 |
| 2026-08-14 | `NCMI` | 464 | — | $2.69 | +0.00 | $2.86 | +78.88 | +78.88 | +0.00 | +78.88 |
| 2026-08-14 | `MXCT` | 899 | — | $1.39 | +0.00 | $1.32 | -62.93 | -62.93 | +0.00 | -62.93 |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `WWW` | 60 | $21.03 | $20.98 | -3.00 | — | +0.00 | -3.00 | +22.80 | — |
| 2026-08-17 | `HYLN` | 299 | $4.06 | $4.10 | +11.96 | — | +0.00 | +11.96 | -23.92 | — |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | — | +0.00 | -0.63 | +0.00 | — |
| 2026-08-17 | `OMER` | 72 | $17.19 | $17.17 | -1.44 | — | +0.00 | -1.44 | -12.96 | — |
| 2026-08-17 | `AIRO` | 112 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -173.60 | — |
| 2026-08-17 | `NCMI` | 464 | $2.86 | $2.80 | -27.84 | — | +0.00 | -27.84 | +51.04 | — |
| 2026-08-17 | `MXCT` | 899 | $1.32 | $1.32 | +0.00 | — | +0.00 | +0.00 | -62.93 | — |
| 2026-08-17 | `CDNL` | 30 | — | $39.85 | +0.00 | $39.23 | -18.60 | -18.60 | +0.00 | -18.60 |
| 2026-08-17 | `ABX` | 134 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `FCEL` | 54 | — | $22.37 | +0.00 | $22.36 | -0.54 | -0.54 | +0.00 | -0.54 |
| 2026-08-17 | `VERA` | 39 | — | $31.30 | +0.00 | $31.63 | +12.87 | +12.87 | +0.00 | +12.87 |
| 2026-08-17 | `CELC` | 13 | — | $92.99 | +0.00 | $92.44 | -7.15 | -7.15 | +0.00 | -7.15 |
| 2026-08-17 | `CAPR` | 178 | — | $6.87 | +0.00 | $7.45 | +103.24 | +103.24 | +0.00 | +103.24 |
| 2026-08-17 | `HTFL` | 29 | — | $41.23 | +0.00 | $41.94 | +20.59 | +20.59 | +0.00 | +20.59 |
| 2026-08-17 | `UMAC` | 37 | — | $32.55 | +0.00 | $30.15 | -88.80 | -88.80 | +0.00 | -88.80 |
| 2026-08-18 | `CDNL` | 30 | $39.23 | $41.57 | +70.20 | — | +0.00 | +70.20 | +51.60 | — |
| 2026-08-18 | `ABX` | 134 | $9.12 | $9.03 | -12.06 | — | +0.00 | -12.06 | -12.06 | — |
| 2026-08-18 | `FCEL` | 54 | $22.36 | $21.18 | -63.72 | — | +0.00 | -63.72 | -64.26 | — |
| 2026-08-18 | `VERA` | 39 | $31.63 | $31.31 | -12.48 | — | +0.00 | -12.48 | +0.39 | — |
| 2026-08-18 | `CELC` | 13 | $92.44 | $92.38 | -0.78 | — | +0.00 | -0.78 | -7.93 | — |
| 2026-08-18 | `CAPR` | 178 | $7.45 | $7.50 | +8.90 | — | +0.00 | +8.90 | +112.14 | — |
| 2026-08-18 | `HTFL` | 29 | $41.94 | $41.50 | -12.76 | — | +0.00 | -12.76 | +7.83 | — |
| 2026-08-18 | `UMAC` | 37 | $30.15 | $28.59 | -57.72 | — | +0.00 | -57.72 | -146.52 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `CDE` | 58 | — | $20.65 | +0.00 | $21.11 | +26.68 | +26.68 | +0.00 | +26.68 |
| 2026-08-20 | `MRVI` | 163 | — | $7.44 | +0.00 | $8.29 | +138.55 | +138.55 | +0.00 | +138.55 |
| 2026-08-20 | `DNA` | 163 | — | $7.45 | +0.00 | $6.96 | -79.87 | -79.87 | +0.00 | -79.87 |
| 2026-08-20 | `MSTR` | 10 | — | $113.23 | +0.00 | $112.39 | -8.40 | -8.40 | +0.00 | -8.40 |
| 2026-08-20 | `EXK` | 112 | — | $10.77 | +0.00 | $10.97 | +22.40 | +22.40 | +0.00 | +22.40 |
| 2026-08-20 | `SCZM` | 128 | — | $9.46 | +0.00 | $9.76 | +38.40 | +38.40 | +0.00 | +38.40 |
| 2026-08-20 | `NG` | 145 | — | $8.38 | +0.00 | $8.66 | +40.60 | +40.60 | +0.00 | +40.60 |
| 2026-08-20 | `BLSH` | 41 | — | $29.20 | +0.00 | $28.44 | -31.16 | -31.16 | +0.00 | -31.16 |
| 2026-08-21 | `CDE` | 58 | $21.11 | $21.75 | +37.12 | — | +0.00 | +37.12 | +63.80 | — |
| 2026-08-21 | `MRVI` | 163 | $8.29 | $8.28 | -1.63 | — | +0.00 | -1.63 | +136.92 | — |
| 2026-08-21 | `DNA` | 163 | $6.96 | $7.09 | +21.19 | — | +0.00 | +21.19 | -58.68 | — |
| 2026-08-21 | `MSTR` | 10 | $112.39 | $119.69 | +73.00 | — | +0.00 | +73.00 | +64.60 | — |
| 2026-08-21 | `EXK` | 112 | $10.97 | $11.34 | +41.44 | — | +0.00 | +41.44 | +63.84 | — |
| 2026-08-21 | `SCZM` | 128 | $9.76 | $10.26 | +64.00 | — | +0.00 | +64.00 | +102.40 | — |
| 2026-08-21 | `NG` | 145 | $8.66 | $9.02 | +52.20 | — | +0.00 | +52.20 | +92.80 | — |
| 2026-08-21 | `BLSH` | 41 | $28.44 | $29.75 | +53.71 | — | +0.00 | +53.71 | +22.55 | — |
| 2026-08-21 | `ARCT` | 114 | — | $11.13 | +0.00 | $13.45 | +264.48 | +264.48 | +0.00 | +264.48 |
| 2026-08-21 | `CYPH` | 963 | — | $1.32 | +0.00 | $1.42 | +96.30 | +96.30 | +0.00 | +96.30 |
| 2026-08-21 | `BTBT` | 766 | — | $1.66 | +0.00 | $1.53 | -99.58 | -99.58 | +0.00 | -99.58 |
| 2026-08-21 | `ENHA` | 743 | — | $1.71 | +0.00 | $1.72 | +7.43 | +7.43 | +0.00 | +7.43 |
| 2026-08-21 | `DE` | 2 | — | $623.26 | +0.00 | $647.47 | +48.42 | +48.42 | +0.00 | +48.42 |
| 2026-08-21 | `QDEL` | 85 | — | $14.96 | +0.00 | $14.74 | -18.70 | -18.70 | +0.00 | -18.70 |
| 2026-08-21 | `ORBS` | 1471 | — | $0.86 | +0.00 | $0.88 | +23.54 | +23.54 | +0.00 | +23.54 |
| 2026-08-21 | `GORO` | 399 | — | $3.11 | +0.00 | $3.19 | +31.92 | +31.92 | +0.00 | +31.92 |
| 2026-08-24 | `ARCT` | 114 | $13.45 | $13.33 | -13.68 | — | +0.00 | -13.68 | +250.80 | — |
| 2026-08-24 | `CYPH` | 963 | $1.42 | $1.83 | +394.83 | — | +0.00 | +394.83 | +491.13 | — |
| 2026-08-24 | `BTBT` | 766 | $1.53 | $1.55 | +15.32 | — | +0.00 | +15.32 | -84.26 | — |
| 2026-08-24 | `ENHA` | 743 | $1.72 | $1.74 | +14.86 | — | +0.00 | +14.86 | +22.29 | — |
| 2026-08-24 | `DE` | 2 | $647.47 | $653.04 | +11.14 | — | +0.00 | +11.14 | +59.56 | — |
| 2026-08-24 | `QDEL` | 85 | $14.74 | $14.74 | +0.00 | — | +0.00 | +0.00 | -18.70 | — |
| 2026-08-24 | `ORBS` | 1471 | $0.88 | $0.89 | +14.71 | — | +0.00 | +14.71 | +38.25 | — |
| 2026-08-24 | `GORO` | 399 | $3.19 | $3.20 | +3.99 | — | +0.00 | +3.99 | +35.91 | — |
| 2026-08-25 | `CAPR` | 186 | — | $7.25 | +0.00 | $8.29 | +193.44 | +193.44 | +0.00 | +193.44 |
| 2026-08-25 | `SAFX` | 3786 | — | $0.36 | +0.00 | $0.35 | -15.14 | -15.14 | +0.00 | -15.14 |
| 2026-08-25 | `VITL` | 121 | — | $11.12 | +0.00 | $11.11 | -1.21 | -1.21 | +0.00 | -1.21 |
| 2026-08-25 | `KURA` | 99 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CCOI` | 142 | — | $9.49 | +0.00 | $9.88 | +55.38 | +55.38 | +0.00 | +55.38 |
| 2026-08-25 | `LIFE` | 36 | — | $36.96 | +0.00 | $38.56 | +57.60 | +57.60 | +0.00 | +57.60 |
| 2026-08-25 | `ZIP` | 297 | — | $4.55 | +0.00 | $4.35 | -59.40 | -59.40 | +0.00 | -59.40 |
| 2026-08-25 | `BMEA` | 831 | — | $1.63 | +0.00 | $1.73 | +83.10 | +83.10 | +0.00 | +83.10 |
| 2026-08-26 | `CAPR` | 186 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | +193.44 | — |
| 2026-08-26 | `SAFX` | 3786 | $0.35 | $0.35 | -3.79 | — | +0.00 | -3.79 | -18.93 | — |
| 2026-08-26 | `VITL` | 121 | $11.11 | $11.03 | -9.68 | — | +0.00 | -9.68 | -10.89 | — |
| 2026-08-26 | `KURA` | 99 | $13.59 | $13.63 | +3.96 | — | +0.00 | +3.96 | +3.96 | — |
| 2026-08-26 | `CCOI` | 142 | $9.88 | $9.89 | +1.42 | — | +0.00 | +1.42 | +56.80 | — |
| 2026-08-26 | `LIFE` | 36 | $38.56 | $38.24 | -11.52 | — | +0.00 | -11.52 | +46.08 | — |
| 2026-08-26 | `ZIP` | 297 | $4.35 | $4.31 | -11.88 | — | +0.00 | -11.88 | -71.28 | — |
| 2026-08-26 | `BMEA` | 831 | $1.73 | $1.75 | +20.77 | — | +0.00 | +20.77 | +103.88 | — |
| 2026-08-26 | `RZLT` | 275 | — | $5.01 | +0.00 | $5.04 | +8.25 | +8.25 | +0.00 | +8.25 |
| 2026-08-26 | `AVBP` | 44 | — | $31.21 | +0.00 | $31.14 | -3.08 | -3.08 | +0.00 | -3.08 |
| 2026-08-26 | `FLNC` | 124 | — | $11.12 | +0.00 | $11.08 | -4.96 | -4.96 | +0.00 | -4.96 |
| 2026-08-26 | `ABX` | 140 | — | $9.83 | +0.00 | $9.78 | -7.00 | -7.00 | +0.00 | -7.00 |
| 2026-08-26 | `AVEX` | 78 | — | $17.51 | +0.00 | $18.34 | +64.74 | +64.74 | +0.00 | +64.74 |
| 2026-08-26 | `ITG` | 114 | — | $12.04 | +0.00 | $12.45 | +46.74 | +46.74 | +0.00 | +46.74 |
| 2026-08-26 | `SENS` | 145 | — | $9.48 | +0.00 | $9.34 | -20.30 | -20.30 | +0.00 | -20.30 |
| 2026-08-26 | `BE` | 6 | — | $213.94 | +0.00 | $218.21 | +25.62 | +25.62 | +0.00 | +25.62 |
| 2026-08-27 | `RZLT` | 275 | $5.04 | $5.07 | +8.25 | — | +0.00 | +8.25 | +16.50 | — |
| 2026-08-27 | `AVBP` | 44 | $31.14 | $30.79 | -15.40 | — | +0.00 | -15.40 | -18.48 | — |
| 2026-08-27 | `FLNC` | 124 | $11.08 | $11.52 | +54.56 | — | +0.00 | +54.56 | +49.60 | — |
| 2026-08-27 | `ABX` | 140 | $9.78 | $9.68 | -14.00 | — | +0.00 | -14.00 | -21.00 | — |
| 2026-08-27 | `AVEX` | 78 | $18.34 | $18.43 | +7.02 | — | +0.00 | +7.02 | +71.76 | — |
| 2026-08-27 | `ITG` | 114 | $12.45 | $12.36 | -10.26 | — | +0.00 | -10.26 | +36.48 | — |
| 2026-08-27 | `SENS` | 145 | $9.34 | $9.33 | -1.45 | — | +0.00 | -1.45 | -21.75 | — |
| 2026-08-27 | `BE` | 6 | $218.21 | $227.10 | +53.34 | — | +0.00 | +53.34 | +78.96 | — |
| 2026-08-28 | `SEDG` | 42 | — | $32.90 | +0.00 | $31.41 | -62.58 | -62.58 | +0.00 | -62.58 |
| 2026-08-28 | `GRRR` | 89 | — | $15.66 | +0.00 | $14.41 | -111.25 | -111.25 | +0.00 | -111.25 |
| 2026-08-28 | `URBN` | 17 | — | $79.42 | +0.00 | $81.09 | +28.39 | +28.39 | +0.00 | +28.39 |
| 2026-08-28 | `PYXS` | 421 | — | $3.32 | +0.00 | $3.23 | -37.89 | -37.89 | +0.00 | -37.89 |
| 2026-08-28 | `SAFX` | 3835 | — | $0.36 | +0.00 | $0.36 | -23.01 | -23.01 | +0.00 | -23.01 |
| 2026-08-28 | `SIMO` | 5 | — | $252.24 | +0.00 | $245.81 | -32.15 | -32.15 | +0.00 | -32.15 |
| 2026-08-28 | `OPTX` | 162 | — | $8.61 | +0.00 | $8.52 | -14.58 | -14.58 | +0.00 | -14.58 |
| 2026-08-28 | `XPOF` | 260 | — | $5.38 | +0.00 | $5.43 | +13.00 | +13.00 | +0.00 | +13.00 |
| 2026-08-31 | `SEDG` | 42 | $31.41 | $31.15 | -10.92 | — | +0.00 | -10.92 | -73.50 | — |
| 2026-08-31 | `GRRR` | 89 | $14.41 | $14.44 | +2.67 | — | +0.00 | +2.67 | -108.58 | — |
| 2026-08-31 | `URBN` | 17 | $81.09 | $80.44 | -11.05 | — | +0.00 | -11.05 | +17.34 | — |
| 2026-08-31 | `PYXS` | 421 | $3.23 | $3.20 | -12.63 | — | +0.00 | -12.63 | -50.52 | — |
| 2026-08-31 | `SAFX` | 3835 | $0.36 | $0.36 | +11.51 | — | +0.00 | +11.51 | -11.51 | — |
| 2026-08-31 | `SIMO` | 5 | $245.81 | $247.05 | +6.20 | — | +0.00 | +6.20 | -25.95 | — |
| 2026-08-31 | `OPTX` | 162 | $8.52 | $8.52 | +0.00 | — | +0.00 | +0.00 | -14.58 | — |
| 2026-08-31 | `XPOF` | 260 | $5.43 | $5.37 | -15.60 | — | +0.00 | -15.60 | -2.60 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `CRK` | 87 | — | $15.45 | +0.00 | $14.95 | -43.50 | -43.50 | +0.00 | -43.50 |
| 2026-09-03 | `MRNA` | 9 | — | $145.94 | +0.00 | $148.87 | +26.33 | +26.33 | +0.00 | +26.33 |
| 2026-09-03 | `ARCT` | 80 | — | $16.77 | +0.00 | $15.56 | -96.80 | -96.80 | +0.00 | -96.80 |
| 2026-09-03 | `SLN` | 91 | — | $14.85 | +0.00 | $14.79 | -5.46 | -5.46 | +0.00 | -5.46 |
| 2026-09-03 | `EIX` | 24 | — | $55.42 | +0.00 | $56.30 | +21.12 | +21.12 | +0.00 | +21.12 |
| 2026-09-03 | `CRDL` | 621 | — | $2.18 | +0.00 | $2.16 | -12.42 | -12.42 | +0.00 | -12.42 |
| 2026-09-03 | `CLYM` | 97 | — | $13.96 | +0.00 | $14.59 | +61.11 | +61.11 | +0.00 | +61.11 |
| 2026-09-03 | `SAFX` | 3593 | — | $0.38 | +0.00 | $0.38 | +7.19 | +7.19 | +0.00 | +7.19 |
| 2026-09-04 | `CRK` | 87 | $14.95 | $15.00 | +4.35 | — | +0.00 | +4.35 | -39.15 | — |
| 2026-09-04 | `MRNA` | 9 | $148.87 | $153.62 | +42.75 | — | +0.00 | +42.75 | +69.08 | — |
| 2026-09-04 | `ARCT` | 80 | $15.56 | $15.61 | +4.00 | — | +0.00 | +4.00 | -92.80 | — |
| 2026-09-04 | `SLN` | 91 | $14.79 | $14.63 | -14.56 | — | +0.00 | -14.56 | -20.02 | — |
| 2026-09-04 | `EIX` | 24 | $56.30 | $55.79 | -12.24 | — | +0.00 | -12.24 | +8.88 | — |
| 2026-09-04 | `CRDL` | 621 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -12.42 | — |
| 2026-09-04 | `CLYM` | 97 | $14.59 | $14.49 | -9.70 | — | +0.00 | -9.70 | +51.41 | — |
| 2026-09-04 | `SAFX` | 3593 | $0.38 | $0.38 | -3.59 | — | +0.00 | -3.59 | +3.59 | — |
| 2026-09-04 | `ALEC` | 531 | — | $2.52 | +0.00 | $2.46 | -31.86 | -31.86 | +0.00 | -31.86 |
| 2026-09-04 | `OABI` | 280 | — | $4.78 | +0.00 | $4.33 | -126.00 | -126.00 | +0.00 | -126.00 |
| 2026-09-04 | `OPK` | 842 | — | $1.59 | +0.00 | $1.64 | +42.10 | +42.10 | +0.00 | +42.10 |
| 2026-09-04 | `HQ` | 84 | — | $15.90 | +0.00 | $15.56 | -28.56 | -28.56 | +0.00 | -28.56 |
| 2026-09-04 | `EOSE` | 380 | — | $3.52 | +0.00 | $3.88 | +136.80 | +136.80 | +0.00 | +136.80 |
| 2026-09-04 | `DELL` | 2 | — | $513.78 | +0.00 | $524.14 | +20.72 | +20.72 | +0.00 | +20.72 |
| 2026-09-04 | `MLYS` | 47 | — | $28.00 | +0.00 | $28.21 | +9.87 | +9.87 | +0.00 | +9.87 |
| 2026-09-04 | `CCOI` | 133 | — | $10.02 | +0.00 | $10.05 | +3.99 | +3.99 | +0.00 | +3.99 |
| 2026-09-08 | `ALEC` | 531 | $2.46 | $2.38 | -42.48 | — | +0.00 | -42.48 | -74.34 | — |
| 2026-09-08 | `OABI` | 280 | $4.33 | $4.30 | -8.40 | — | +0.00 | -8.40 | -134.40 | — |
| 2026-09-08 | `OPK` | 842 | $1.64 | $1.63 | -8.42 | — | +0.00 | -8.42 | +33.68 | — |
| 2026-09-08 | `HQ` | 84 | $15.56 | $15.40 | -13.44 | — | +0.00 | -13.44 | -42.00 | — |
| 2026-09-08 | `EOSE` | 380 | $3.88 | $3.99 | +41.80 | — | +0.00 | +41.80 | +178.60 | — |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | — | +0.00 | -5.98 | +14.74 | — |
| 2026-09-08 | `MLYS` | 47 | $28.21 | $28.03 | -8.46 | — | +0.00 | -8.46 | +1.41 | — |
| 2026-09-08 | `CCOI` | 133 | $10.05 | $9.98 | -9.31 | — | +0.00 | -9.31 | -5.32 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `CMRC` | 423 | — | $3.13 | +0.00 | $3.50 | +158.62 | +158.62 | +0.00 | +158.62 |
| 2026-09-11 | `DBI` | 224 | — | $5.91 | +0.00 | $5.88 | -6.72 | -6.72 | +0.00 | -6.72 |
| 2026-09-11 | `AMTX` | 650 | — | $2.04 | +0.00 | $2.01 | -19.50 | -19.50 | +0.00 | -19.50 |
| 2026-09-11 | `CLOV` | 279 | — | $4.75 | +0.00 | $4.82 | +19.53 | +19.53 | +0.00 | +19.53 |
| 2026-09-11 | `BAK` | 625 | — | $2.12 | +0.00 | $2.08 | -25.00 | -25.00 | +0.00 | -25.00 |
| 2026-09-11 | `TYRA` | 56 | — | $23.63 | +0.00 | $22.03 | -89.60 | -89.60 | +0.00 | -89.60 |
| 2026-09-11 | `QRVO` | 11 | — | $112.83 | +0.00 | $116.65 | +41.97 | +41.97 | +0.00 | +41.97 |
| 2026-09-11 | `APPS` | 111 | — | $11.88 | +0.00 | $11.81 | -7.77 | -7.77 | +0.00 | -7.77 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -161.22 | ANGX, WWW, HYLN, ARX, OMER, AIRO, NCMI, MXCT | — | $4.90 | $9,804.72 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 |
| 2026-08-17 | +2.25 | $4.90 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | $9,850.47 | +45.75 | +21.61 | CDNL, ABX, FCEL, VERA, CELC, CAPR, HTFL, UMAC | ANGX, WWW, HYLN, ARX, OMER, AIRO, NCMI, MXCT | $120.48 | $9,820.10 | CDNL×30, ABX×134, FCEL×54, VERA×39, CELC×13, CAPR×178, HTFL×29, UMAC×37 |
| 2026-08-18 | -6.20 | $120.48 | CDNL×30, ABX×134, FCEL×54, VERA×39, CELC×13, CAPR×178, HTFL×29, UMAC×37 | $9,739.68 | -80.42 | +0.00 | — | CDNL, ABX, FCEL, VERA, CELC, CAPR, HTFL, UMAC | $9,722.02 | $9,722.02 | — |
| 2026-08-19 | -7.20 | $9,722.02 | — | $9,722.02 | +0.00 | +0.00 | — | — | $9,722.02 | $9,722.02 | — |
| 2026-08-20 | +1.12 | $9,722.02 | — | $9,722.02 | +0.00 | +147.20 | CDE, MRVI, DNA, MSTR, EXK, SCZM, NG, BLSH | — | $117.15 | $9,850.84 | CDE×58, MRVI×163, DNA×163, MSTR×10, EXK×112, SCZM×128, NG×145, BLSH×41 |
| 2026-08-21 | +3.25 | $117.15 | CDE×58, MRVI×163, DNA×163, MSTR×10, EXK×112, SCZM×128, NG×145, BLSH×41 | $10,191.87 | +341.03 | +353.81 | ARCT, CYPH, BTBT, ENHA, DE, QDEL, ORBS, GORO | CDE, MRVI, DNA, MSTR, EXK, SCZM, NG, BLSH | $0.51 | $10,466.34 | ARCT×114, CYPH×963, BTBT×766, ENHA×743, DE×2, QDEL×85, ORBS×1471, GORO×399 |
| 2026-08-24 | -5.17 | $0.51 | ARCT×114, CYPH×963, BTBT×766, ENHA×743, DE×2, QDEL×85, ORBS×1471, GORO×399 | $10,907.51 | +441.17 | +0.00 | — | ARCT, CYPH, BTBT, ENHA, DE, QDEL, ORBS, GORO | $10,845.54 | $10,845.54 | — |
| 2026-08-25 | +1.80 | $10,845.54 | — | $10,845.54 | +0.00 | +313.77 | CAPR, SAFX, VITL, KURA, CCOI, LIFE, ZIP, BMEA | — | $15.54 | $11,108.14 | CAPR×186, SAFX×3786, VITL×121, KURA×99, CCOI×142, LIFE×36, ZIP×297, BMEA×831 |
| 2026-08-26 | +2.02 | $15.54 | CAPR×186, SAFX×3786, VITL×121, KURA×99, CCOI×142, LIFE×36, ZIP×297, BMEA×831 | $11,097.43 | -10.71 | +110.01 | RZLT, AVBP, FLNC, ABX, AVEX, ITG, SENS, BE | CAPR, SAFX, VITL, KURA, CCOI, LIFE, ZIP, BMEA | $123.37 | $11,136.03 | RZLT×275, AVBP×44, FLNC×124, ABX×140, AVEX×78, ITG×114, SENS×145, BE×6 |
| 2026-08-27 | — | $123.37 | RZLT×275, AVBP×44, FLNC×124, ABX×140, AVEX×78, ITG×114, SENS×145, BE×6 | $11,218.09 | +82.06 | +0.00 | — | RZLT, AVBP, FLNC, ABX, AVEX, ITG, SENS, BE | $11,198.41 | $11,198.41 | — |
| 2026-08-28 | +0.75 | $11,198.41 | — | $11,198.41 | -0.00 | -240.07 | SEDG, GRRR, URBN, PYXS, SAFX, SIMO, OPTX, XPOF | — | $175.23 | $10,913.15 | SEDG×42, GRRR×89, URBN×17, PYXS×421, SAFX×3835, SIMO×5, OPTX×162, XPOF×260 |
| 2026-08-31 | -5.85 | $175.23 | SEDG×42, GRRR×89, URBN×17, PYXS×421, SAFX×3835, SIMO×5, OPTX×162, XPOF×260 | $10,883.33 | -29.82 | +0.00 | — | SEDG, GRRR, URBN, PYXS, SAFX, SIMO, OPTX, XPOF | $10,837.35 | $10,837.35 | — |
| 2026-09-01 | -6.30 | $10,837.35 | — | $10,837.35 | +0.00 | +0.00 | — | — | $10,837.35 | $10,837.35 | — |
| 2026-09-02 | -3.83 | $10,837.35 | — | $10,837.35 | +0.00 | +0.00 | — | — | $10,837.35 | $10,837.35 | — |
| 2026-09-03 | -0.90 | $10,837.35 | — | $10,837.35 | +0.00 | -42.43 | CRK, MRNA, ARCT, SLN, EIX, CRDL, CLYM, SAFX | — | $48.77 | $10,749.48 | CRK×87, MRNA×9, ARCT×80, SLN×91, EIX×24, CRDL×621, CLYM×97, SAFX×3593 |
| 2026-09-04 | +2.25 | $48.77 | CRK×87, MRNA×9, ARCT×80, SLN×91, EIX×24, CRDL×621, CLYM×97, SAFX×3593 | $10,760.48 | +11.00 | +27.06 | ALEC, OABI, OPK, HQ, EOSE, DELL, MLYS, CCOI | CRK, MRNA, ARCT, SLN, EIX, CRDL, CLYM, SAFX | $314.44 | $10,706.22 | ALEC×531, OABI×280, OPK×842, HQ×84, EOSE×380, DELL×2, MLYS×47, CCOI×133 |
| 2026-09-08 | -11.47 | $314.44 | ALEC×531, OABI×280, OPK×842, HQ×84, EOSE×380, DELL×2, MLYS×47, CCOI×133 | $10,651.53 | -54.69 | +0.00 | — | ALEC, OABI, OPK, HQ, EOSE, DELL, MLYS, CCOI | $10,616.07 | $10,616.07 | — |
| 2026-09-09 | -13.95 | $10,616.07 | — | $10,616.07 | -0.00 | +0.00 | — | — | $10,616.07 | $10,616.07 | — |
| 2026-09-10 | -13.28 | $10,616.07 | — | $10,616.07 | -0.00 | +0.00 | — | — | $10,616.07 | $10,616.07 | — |
| 2026-09-11 | +0.50 | $10,616.07 | — | $10,616.07 | -0.00 | +71.53 | CMRC, DBI, AMTX, CLOV, BAK, TYRA, QRVO, APPS | — | $73.95 | $10,652.70 | CMRC×423, DBI×224, AMTX×650, CLOV×279, BAK×625, TYRA×56, QRVO×11, APPS×111 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $8,746.36 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WWW` | 60 | $20.60 | $2.17 | — | $7,508.19 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+4.4; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $6,254.51 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $5,019.42 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `OMER` | 72 | $17.35 | $2.21 | — | $3,768.02 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $2,520.25 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $1,266.11 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MXCT` | 899 | $1.39 | $11.60 | — | $4.90 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.90 | ▼ close $9,804.72 vs 09:30 $10,000.00 (session -161.22) | 16:00 close · cash $4.90 · equity $9,804.72 vs 09:30 $10,000.00 (-195.28; session marks -161.22) · 8 name(s) marked open→close (per-name table). ANGX×290 09:30 $4.31 → close $4.37 +17.40; WWW×60 09:30 $20.60 → close $21.03 +25.80; HYLN×299 09:30 $4.18 → close $4.06 -35.88; ARX×63 09:30 $19.57 → close $19.58 +0.63; OMER×72 09:30 $17.35 → close $17.19 -11.52; AIRO×112 09:30 $11.12 → close $9.57 -173.60; NCMI×464 09:30 $2.69 → close $2.86 +78.88; MXCT×899 09:30 $1.39 → close $1.32 -62.93 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.90 | ▲ 09:30 equity $9,850.47 vs yday $9,804.72 (+45.75) | 09:30 open · cash $4.90 (unchanged overnight, no fees) · equity $9,850.47 vs prior close $9,804.72 (+45.75) · 8 name(s) re-marked at the open (per-name table). ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; WWW×60 yday $21.03 → 09:30 $20.98 -3.00; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; OMER×72 yday $17.19 → 09:30 $17.17 -1.44; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84; MXCT×899 yday $1.32 → 09:30 $1.32 +0.00 | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $1,335.10 | ▲ +76.56 after sell → book $9,846.67; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WWW` | 60 | $20.98 | $2.19 | $+18.44 | $2,591.71 | ▲ +18.44 after sell → book $9,844.48; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $3,813.69 | ▼ -31.69 after sell → book $9,840.56; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $5,044.40 | ▼ -4.38 after sell → book $9,838.36; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `OMER` | 72 | $17.17 | $2.23 | $-17.39 | $6,278.41 | ▼ -17.39 after sell → book $9,836.13; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $7,347.90 | ▼ -178.28 after sell → book $9,833.78; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 464 | $2.80 | $6.07 | $+38.98 | $8,641.03 | ▲ +38.98 after sell → book $9,827.71; vs 09:30 mark -6.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MXCT` | 899 | $1.32 | $11.76 | $-86.28 | $9,815.95 | ▼ -86.28 after sell → book $9,815.95; vs 09:30 mark -11.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 30 | $39.85 | $2.08 | — | $8,618.37 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1226.99 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 134 | $9.12 | $2.39 | — | $7,393.90 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1226.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `FCEL` | 54 | $22.37 | $2.15 | — | $6,183.77 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+9.5; leftover $1226.99 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 39 | $31.30 | $2.11 | — | $4,960.96 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-3.8; leftover $1226.99 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 13 | $92.99 | $2.03 | — | $3,750.06 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.8; leftover $1226.99 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 178 | $6.87 | $2.52 | — | $2,524.68 | — | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+62.6; leftover $1226.99 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $1,326.93 | — | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+46.0; leftover $1226.99 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $120.48 | — | baseline list, no extra gate; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1226.99 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $120.48 | ▲ close $9,820.10 vs 09:30 $9,850.47 (session +21.61) | 16:00 close · cash $120.48 · equity $9,820.10 vs 09:30 $9,850.47 (-30.37; session marks +21.61) · 8 name(s) marked open→close (per-name table). CDNL×30 09:30 $39.85 → close $39.23 -18.60; ABX×134 09:30 $9.12 → close $9.12 +0.00; FCEL×54 09:30 $22.37 → close $22.36 -0.54; VERA×39 09:30 $31.30 → close $31.63 +12.87; CELC×13 09:30 $92.99 → close $92.44 -7.15; CAPR×178 09:30 $6.87 → close $7.45 +103.24; HTFL×29 09:30 $41.23 → close $41.94 +20.59; UMAC×37 09:30 $32.55 → close $30.15 -88.80 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $120.48 | ▼ 09:30 equity $9,739.68 vs yday $9,820.10 (-80.42) | 09:30 open · cash $120.48 (unchanged overnight, no fees) · equity $9,739.68 vs prior close $9,820.10 (-80.42) · 8 name(s) re-marked at the open (per-name table). CDNL×30 yday $39.23 → 09:30 $41.57 +70.20; ABX×134 yday $9.12 → 09:30 $9.03 -12.06; FCEL×54 yday $22.36 → 09:30 $21.18 -63.72; VERA×39 yday $31.63 → 09:30 $31.31 -12.48; CELC×13 yday $92.44 → 09:30 $92.38 -0.78; CAPR×178 yday $7.45 → 09:30 $7.50 +8.90; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×37 yday $30.15 → 09:30 $28.59 -57.72 | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 30 | $41.57 | $2.10 | $+47.42 | $1,365.48 | ▲ +47.42 after sell → book $9,737.58; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 134 | $9.03 | $2.42 | $-16.88 | $2,573.07 | ▼ -16.88 after sell → book $9,735.15; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FCEL` | 54 | $21.18 | $2.17 | $-68.58 | $3,714.62 | ▼ -68.58 after sell → book $9,732.98; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 39 | $31.31 | $2.13 | $-3.84 | $4,933.59 | ▼ -3.84 after sell → book $9,730.86; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 13 | $92.38 | $2.05 | $-12.01 | $6,132.48 | ▼ -12.01 after sell → book $9,728.81; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CAPR` | 178 | $7.50 | $2.56 | $+107.05 | $7,464.91 | ▲ +107.05 after sell → book $9,726.24; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $8,666.31 | ▲ +3.66 after sell → book $9,724.14; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $9,722.02 | ▼ -150.74 after sell → book $9,722.02; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,722.02 | ▲ close $9,722.02 vs 09:30 $9,739.68 (session +0.00) | 16:00 close · cash $9,722.02 · no lots left · equity $9,722.02. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,722.02 | ▲ 09:30 equity $9,722.02 vs yday $9,722.02 (+0.00) | 09:30 open · cash $9,722.02 · no holdings · equity $9,722.02 vs prior close $9,722.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,722.02 | ▲ close $9,722.02 vs 09:30 $9,722.02 (session +0.00) | 16:00 close · cash $9,722.02 · no lots left · equity $9,722.02. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,722.02 | ▲ 09:30 equity $9,722.02 vs yday $9,722.02 (+0.00) | 09:30 open · cash $9,722.02 · no holdings · equity $9,722.02 vs prior close $9,722.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 58 | $20.65 | $2.16 | — | $8,522.16 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1215.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 163 | $7.44 | $2.48 | — | $7,306.96 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1215.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 163 | $7.45 | $2.48 | — | $6,090.13 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1215.25 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 10 | $113.23 | $2.02 | — | $4,955.81 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1215.25 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 112 | $10.77 | $2.33 | — | $3,747.25 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1215.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 128 | $9.46 | $2.37 | — | $2,533.99 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1215.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 145 | $8.38 | $2.42 | — | $1,316.47 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1215.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 41 | $29.20 | $2.11 | — | $117.15 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1215.25 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $117.15 | ▲ close $9,850.84 vs 09:30 $9,722.02 (session +147.20) | 16:00 close · cash $117.15 · equity $9,850.84 vs 09:30 $9,722.02 (+128.82; session marks +147.20) · 8 name(s) marked open→close (per-name table). CDE×58 09:30 $20.65 → close $21.11 +26.68; MRVI×163 09:30 $7.44 → close $8.29 +138.55; DNA×163 09:30 $7.45 → close $6.96 -79.87; MSTR×10 09:30 $113.23 → close $112.39 -8.40; EXK×112 09:30 $10.77 → close $10.97 +22.40; SCZM×128 09:30 $9.46 → close $9.76 +38.40; NG×145 09:30 $8.38 → close $8.66 +40.60; BLSH×41 09:30 $29.20 → close $28.44 -31.16 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $117.15 | ▲ 09:30 equity $10,191.87 vs yday $9,850.84 (+341.03) | 09:30 open · cash $117.15 (unchanged overnight, no fees) · equity $10,191.87 vs prior close $9,850.84 (+341.03) · 8 name(s) re-marked at the open (per-name table). CDE×58 yday $21.11 → 09:30 $21.75 +37.12; MRVI×163 yday $8.29 → 09:30 $8.28 -1.63; DNA×163 yday $6.96 → 09:30 $7.09 +21.19; MSTR×10 yday $112.39 → 09:30 $119.69 +73.00; EXK×112 yday $10.97 → 09:30 $11.34 +41.44; SCZM×128 yday $9.76 → 09:30 $10.26 +64.00; NG×145 yday $8.66 → 09:30 $9.02 +52.20; BLSH×41 yday $28.44 → 09:30 $29.75 +53.71 | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 58 | $21.75 | $2.18 | $+59.45 | $1,376.47 | ▲ +59.45 after sell → book $10,189.69; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRVI` | 163 | $8.28 | $2.52 | $+131.92 | $2,723.59 | ▲ +131.92 after sell → book $10,187.17; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 163 | $7.09 | $2.52 | $-63.68 | $3,876.75 | ▼ -63.68 after sell → book $10,184.66; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MSTR` | 10 | $119.69 | $2.04 | $+60.54 | $5,071.61 | ▲ +60.54 after sell → book $10,182.62; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `EXK` | 112 | $11.34 | $2.35 | $+59.16 | $6,339.33 | ▲ +59.16 after sell → book $10,180.26; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 128 | $10.26 | $2.41 | $+97.62 | $7,650.21 | ▲ +97.62 after sell → book $10,177.86; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NG` | 145 | $9.02 | $2.46 | $+87.92 | $8,955.65 | ▲ +87.92 after sell → book $10,175.40; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BLSH` | 41 | $29.75 | $2.13 | $+18.30 | $10,173.26 | ▲ +18.30 after sell → book $10,173.26; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 114 | $11.13 | $2.33 | — | $8,902.11 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1271.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 963 | $1.32 | $12.42 | — | $7,618.53 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1271.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 766 | $1.66 | $9.88 | — | $6,337.09 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1271.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 743 | $1.71 | $9.58 | — | $5,056.97 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1271.66 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $3,808.46 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1271.66 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 85 | $14.96 | $2.25 | — | $2,534.61 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-1.6; leftover $1271.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1471 | $0.86 | $17.12 | — | $1,246.55 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1271.66 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 399 | $3.11 | $5.15 | — | $0.51 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+7.1; leftover $1271.66 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.51 | ▲ close $10,466.34 vs 09:30 $10,191.87 (session +353.81) | 16:00 close · cash $0.51 · equity $10,466.34 vs 09:30 $10,191.87 (+274.47; session marks +353.81) · 8 name(s) marked open→close (per-name table). ARCT×114 09:30 $11.13 → close $13.45 +264.48; CYPH×963 09:30 $1.32 → close $1.42 +96.30; BTBT×766 09:30 $1.66 → close $1.53 -99.58; ENHA×743 09:30 $1.71 → close $1.72 +7.43; DE×2 09:30 $623.26 → close $647.47 +48.42; QDEL×85 09:30 $14.96 → close $14.74 -18.70; ORBS×1471 09:30 $0.86 → close $0.88 +23.54; GORO×399 09:30 $3.11 → close $3.19 +31.92 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.51 | ▲ 09:30 equity $10,907.51 vs yday $10,466.34 (+441.17) | 09:30 open · cash $0.51 (unchanged overnight, no fees) · equity $10,907.51 vs prior close $10,466.34 (+441.17) · 8 name(s) re-marked at the open (per-name table). ARCT×114 yday $13.45 → 09:30 $13.33 -13.68; CYPH×963 yday $1.42 → 09:30 $1.83 +394.83; BTBT×766 yday $1.53 → 09:30 $1.55 +15.32; ENHA×743 yday $1.72 → 09:30 $1.74 +14.86; DE×2 yday $647.47 → 09:30 $653.04 +11.14; QDEL×85 yday $14.74 → 09:30 $14.74 +0.00; ORBS×1471 yday $0.88 → 09:30 $0.89 +14.71; GORO×399 yday $3.19 → 09:30 $3.20 +3.99 | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 114 | $13.33 | $2.36 | $+246.10 | $1,517.77 | ▲ +246.10 after sell → book $10,905.15; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 963 | $1.83 | $12.60 | $+466.11 | $3,267.46 | ▲ +466.11 after sell → book $10,892.55; vs 09:30 mark -12.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 766 | $1.55 | $10.02 | $-104.16 | $4,444.74 | ▼ -104.16 after sell → book $10,882.53; vs 09:30 mark -10.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ENHA` | 743 | $1.74 | $9.72 | $+2.99 | $5,727.84 | ▲ +2.99 after sell → book $10,872.81; vs 09:30 mark -9.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $7,031.91 | ▲ +55.55 after sell → book $10,870.80; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 85 | $14.74 | $2.27 | $-23.21 | $8,282.54 | ▼ -23.21 after sell → book $10,868.53; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1471 | $0.89 | $17.76 | $+3.36 | $9,573.97 | ▲ +3.36 after sell → book $10,850.77; vs 09:30 mark -17.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GORO` | 399 | $3.20 | $5.22 | $+25.54 | $10,845.54 | ▲ +25.54 after sell → book $10,845.54; vs 09:30 mark -5.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,845.54 | ▲ close $10,845.54 vs 09:30 $10,907.51 (session +0.00) | 16:00 close · cash $10,845.54 · no lots left · equity $10,845.54. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,845.54 | ▲ 09:30 equity $10,845.54 vs yday $10,845.54 (+0.00) | 09:30 open · cash $10,845.54 · no holdings · equity $10,845.54 vs prior close $10,845.54 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 186 | $7.25 | $2.55 | — | $9,494.49 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1355.69 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3786 | $0.36 | $24.91 | — | $8,114.20 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-15.6; leftover $1355.69 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 121 | $11.12 | $2.35 | — | $6,766.32 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.7; leftover $1355.69 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 99 | $13.59 | $2.29 | — | $5,418.63 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1355.69 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 142 | $9.49 | $2.42 | — | $4,068.63 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1355.69 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 36 | $36.96 | $2.10 | — | $2,735.97 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1355.69 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 297 | $4.55 | $3.83 | — | $1,380.79 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1355.69 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 831 | $1.63 | $10.72 | — | $15.54 | — | baseline list, no extra gate; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1355.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.54 | ▲ close $11,108.14 vs 09:30 $10,845.54 (session +313.77) | 16:00 close · cash $15.54 · equity $11,108.14 vs 09:30 $10,845.54 (+262.60; session marks +313.77) · 8 name(s) marked open→close (per-name table). CAPR×186 09:30 $7.25 → close $8.29 +193.44; SAFX×3786 09:30 $0.36 → close $0.35 -15.14; VITL×121 09:30 $11.12 → close $11.11 -1.21; KURA×99 09:30 $13.59 → close $13.59 +0.00; CCOI×142 09:30 $9.49 → close $9.88 +55.38; LIFE×36 09:30 $36.96 → close $38.56 +57.60; ZIP×297 09:30 $4.55 → close $4.35 -59.40; BMEA×831 09:30 $1.63 → close $1.73 +83.10 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.54 | ▼ 09:30 equity $11,097.43 vs yday $11,108.14 (-10.71) | 09:30 open · cash $15.54 (unchanged overnight, no fees) · equity $11,097.43 vs prior close $11,108.14 (-10.71) · 8 name(s) re-marked at the open (per-name table). CAPR×186 yday $8.29 → 09:30 $8.29 +0.00; SAFX×3786 yday $0.35 → 09:30 $0.35 -3.79; VITL×121 yday $11.11 → 09:30 $11.03 -9.68; KURA×99 yday $13.59 → 09:30 $13.63 +3.96; CCOI×142 yday $9.88 → 09:30 $9.89 +1.42; LIFE×36 yday $38.56 → 09:30 $38.24 -11.52; ZIP×297 yday $4.35 → 09:30 $4.31 -11.88; BMEA×831 yday $1.73 → 09:30 $1.75 +20.77 | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 186 | $8.29 | $2.59 | $+188.30 | $1,554.89 | ▲ +188.30 after sell → book $11,094.84; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 3786 | $0.35 | $25.36 | $-69.20 | $2,865.98 | ▼ -69.20 after sell → book $11,069.48; vs 09:30 mark -25.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `VITL` | 121 | $11.03 | $2.38 | $-15.63 | $4,198.23 | ▼ -15.63 after sell → book $11,067.10; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 99 | $13.63 | $2.31 | $-0.64 | $5,545.29 | ▼ -0.64 after sell → book $11,064.78; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 142 | $9.89 | $2.45 | $+51.93 | $6,947.22 | ▲ +51.93 after sell → book $11,062.33; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 36 | $38.24 | $2.12 | $+41.86 | $8,321.74 | ▲ +41.86 after sell → book $11,060.21; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 297 | $4.31 | $3.89 | $-79.00 | $9,597.92 | ▼ -79.00 after sell → book $11,056.32; vs 09:30 mark -3.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 831 | $1.75 | $10.87 | $+82.29 | $11,045.45 | ▲ +82.29 after sell → book $11,045.45; vs 09:30 mark -10.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `RZLT` | 275 | $5.01 | $3.55 | — | $9,664.15 | — | baseline list, no extra gate; list flatten,yday_gainer; 🔵; ret5=+7.5; leftover $1380.68 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 44 | $31.21 | $2.12 | — | $8,288.79 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1380.68 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 124 | $11.12 | $2.36 | — | $6,907.55 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1380.68 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 140 | $9.83 | $2.41 | — | $5,528.94 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1380.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AVEX` | 78 | $17.51 | $2.22 | — | $4,160.94 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $1380.68 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ITG` | 114 | $12.04 | $2.33 | — | $2,786.04 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-5.1; leftover $1380.68 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SENS` | 145 | $9.48 | $2.42 | — | $1,409.02 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $1380.68 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 6 | $213.94 | $2.01 | — | $123.37 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1380.68 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.37 | ▲ close $11,136.03 vs 09:30 $11,097.43 (session +110.01) | 16:00 close · cash $123.37 · equity $11,136.03 vs 09:30 $11,097.43 (+38.60; session marks +110.01) · 8 name(s) marked open→close (per-name table). RZLT×275 09:30 $5.01 → close $5.04 +8.25; AVBP×44 09:30 $31.21 → close $31.14 -3.08; FLNC×124 09:30 $11.12 → close $11.08 -4.96; ABX×140 09:30 $9.83 → close $9.78 -7.00; AVEX×78 09:30 $17.51 → close $18.34 +64.74; ITG×114 09:30 $12.04 → close $12.45 +46.74; SENS×145 09:30 $9.48 → close $9.34 -20.30; BE×6 09:30 $213.94 → close $218.21 +25.62 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.37 | ▲ 09:30 equity $11,218.09 vs yday $11,136.03 (+82.06) | 09:30 open · cash $123.37 (unchanged overnight, no fees) · equity $11,218.09 vs prior close $11,136.03 (+82.06) · 8 name(s) re-marked at the open (per-name table). RZLT×275 yday $5.04 → 09:30 $5.07 +8.25; AVBP×44 yday $31.14 → 09:30 $30.79 -15.40; FLNC×124 yday $11.08 → 09:30 $11.52 +54.56; ABX×140 yday $9.78 → 09:30 $9.68 -14.00; AVEX×78 yday $18.34 → 09:30 $18.43 +7.02; ITG×114 yday $12.45 → 09:30 $12.36 -10.26; SENS×145 yday $9.34 → 09:30 $9.33 -1.45; BE×6 yday $218.21 → 09:30 $227.10 +53.34 | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 275 | $5.07 | $3.60 | $+9.35 | $1,514.02 | ▲ +9.35 after sell → book $11,214.49; vs 09:30 mark -3.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 44 | $30.79 | $2.14 | $-22.74 | $2,866.63 | ▼ -22.74 after sell → book $11,212.34; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 124 | $11.52 | $2.39 | $+44.84 | $4,292.72 | ▲ +44.84 after sell → book $11,209.95; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ABX` | 140 | $9.68 | $2.44 | $-25.85 | $5,645.48 | ▼ -25.85 after sell → book $11,207.51; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVEX` | 78 | $18.43 | $2.25 | $+67.29 | $7,080.77 | ▲ +67.29 after sell → book $11,205.26; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ITG` | 114 | $12.36 | $2.36 | $+31.79 | $8,487.45 | ▲ +31.79 after sell → book $11,202.90; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SENS` | 145 | $9.33 | $2.46 | $-26.63 | $9,837.84 | ▼ -26.63 after sell → book $11,200.44; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BE` | 6 | $227.10 | $2.03 | $+74.92 | $11,198.41 | ▲ +74.92 after sell → book $11,198.41; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,198.41 | ▲ close $11,198.41 vs 09:30 $11,218.09 (session +0.00) | 16:00 close · cash $11,198.41 · no lots left · equity $11,198.41. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,198.41 | ▲ 09:30 equity $11,198.41 vs yday $11,198.41 (-0.00) | 09:30 open · cash $11,198.41 · no holdings · equity $11,198.41 vs prior close $11,198.41 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 42 | $32.90 | $2.12 | — | $9,814.49 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1399.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 89 | $15.66 | $2.26 | — | $8,418.49 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1399.80 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 17 | $79.42 | $2.04 | — | $7,066.31 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1399.80 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 421 | $3.32 | $5.43 | — | $5,663.16 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.4; leftover $1399.80 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SAFX` | 3835 | $0.36 | $25.50 | — | $4,237.88 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+7.6; leftover $1399.80 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $2,974.68 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1399.80 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 162 | $8.61 | $2.48 | — | $1,577.38 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.7; leftover $1399.80 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `XPOF` | 260 | $5.38 | $3.35 | — | $175.23 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.5; leftover $1399.80 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $175.23 | ▼ close $10,913.15 vs 09:30 $11,198.41 (session -240.07) | 16:00 close · cash $175.23 · equity $10,913.15 vs 09:30 $11,198.41 (-285.26; session marks -240.07) · 8 name(s) marked open→close (per-name table). SEDG×42 09:30 $32.90 → close $31.41 -62.58; GRRR×89 09:30 $15.66 → close $14.41 -111.25; URBN×17 09:30 $79.42 → close $81.09 +28.39; PYXS×421 09:30 $3.32 → close $3.23 -37.89; SAFX×3835 09:30 $0.36 → close $0.36 -23.01; SIMO×5 09:30 $252.24 → close $245.81 -32.15; OPTX×162 09:30 $8.61 → close $8.52 -14.58; XPOF×260 09:30 $5.38 → close $5.43 +13.00 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $175.23 | ▼ 09:30 equity $10,883.33 vs yday $10,913.15 (-29.82) | 09:30 open · cash $175.23 (unchanged overnight, no fees) · equity $10,883.33 vs prior close $10,913.15 (-29.82) · 8 name(s) re-marked at the open (per-name table). SEDG×42 yday $31.41 → 09:30 $31.15 -10.92; GRRR×89 yday $14.41 → 09:30 $14.44 +2.67; URBN×17 yday $81.09 → 09:30 $80.44 -11.05; PYXS×421 yday $3.23 → 09:30 $3.20 -12.63; SAFX×3835 yday $0.36 → 09:30 $0.36 +11.51; SIMO×5 yday $245.81 → 09:30 $247.05 +6.20; OPTX×162 yday $8.52 → 09:30 $8.52 +0.00; XPOF×260 yday $5.43 → 09:30 $5.37 -15.60 | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 42 | $31.15 | $2.14 | $-77.75 | $1,481.39 | ▼ -77.75 after sell → book $10,881.19; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 89 | $14.44 | $2.28 | $-113.12 | $2,764.27 | ▼ -113.12 after sell → book $10,878.91; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 17 | $80.44 | $2.06 | $+13.24 | $4,129.69 | ▲ +13.24 after sell → book $10,876.85; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PYXS` | 421 | $3.20 | $5.51 | $-61.46 | $5,471.38 | ▼ -61.46 after sell → book $10,871.34; vs 09:30 mark -5.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SAFX` | 3835 | $0.36 | $26.04 | $-63.04 | $6,833.61 | ▼ -63.04 after sell → book $10,845.30; vs 09:30 mark -26.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $8,066.84 | ▼ -29.98 after sell → book $10,843.28; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 162 | $8.52 | $2.51 | $-19.57 | $9,444.56 | ▼ -19.57 after sell → book $10,840.76; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `XPOF` | 260 | $5.37 | $3.41 | $-9.36 | $10,837.35 | ▼ -9.36 after sell → book $10,837.35; vs 09:30 mark -3.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,837.35 | ▲ close $10,837.35 vs 09:30 $10,883.33 (session +0.00) | 16:00 close · cash $10,837.35 · no lots left · equity $10,837.35. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,837.35 | ▲ 09:30 equity $10,837.35 vs yday $10,837.35 (+0.00) | 09:30 open · cash $10,837.35 · no holdings · equity $10,837.35 vs prior close $10,837.35 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,837.35 | ▲ close $10,837.35 vs 09:30 $10,837.35 (session +0.00) | 16:00 close · cash $10,837.35 · no lots left · equity $10,837.35. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,837.35 | ▲ 09:30 equity $10,837.35 vs yday $10,837.35 (+0.00) | 09:30 open · cash $10,837.35 · no holdings · equity $10,837.35 vs prior close $10,837.35 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,837.35 | ▲ close $10,837.35 vs 09:30 $10,837.35 (session +0.00) | 16:00 close · cash $10,837.35 · no lots left · equity $10,837.35. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,837.35 | ▲ 09:30 equity $10,837.35 vs yday $10,837.35 (+0.00) | 09:30 open · cash $10,837.35 · no holdings · equity $10,837.35 vs prior close $10,837.35 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 87 | $15.45 | $2.25 | — | $9,490.95 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1354.67 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $8,175.43 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1354.67 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 80 | $16.77 | $2.23 | — | $6,831.60 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1354.67 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 91 | $14.85 | $2.26 | — | $5,477.99 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1354.67 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 24 | $55.42 | $2.06 | — | $4,145.85 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-25.9; leftover $1354.67 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 621 | $2.18 | $8.01 | — | $2,784.06 | — | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1354.67 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 97 | $13.96 | $2.28 | — | $1,427.65 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-6.4; leftover $1354.67 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SAFX` | 3593 | $0.38 | $24.32 | — | $48.77 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-2.3; leftover $1354.67 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.77 | ▼ close $10,749.48 vs 09:30 $10,837.35 (session -42.43) | 16:00 close · cash $48.77 · equity $10,749.48 vs 09:30 $10,837.35 (-87.87; session marks -42.43) · 8 name(s) marked open→close (per-name table). CRK×87 09:30 $15.45 → close $14.95 -43.50; MRNA×9 09:30 $145.94 → close $148.87 +26.33; ARCT×80 09:30 $16.77 → close $15.56 -96.80; SLN×91 09:30 $14.85 → close $14.79 -5.46; EIX×24 09:30 $55.42 → close $56.30 +21.12; CRDL×621 09:30 $2.18 → close $2.16 -12.42; CLYM×97 09:30 $13.96 → close $14.59 +61.11; SAFX×3593 09:30 $0.38 → close $0.38 +7.19 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.77 | ▲ 09:30 equity $10,760.48 vs yday $10,749.48 (+11.00) | 09:30 open · cash $48.77 (unchanged overnight, no fees) · equity $10,760.48 vs prior close $10,749.48 (+11.00) · 8 name(s) re-marked at the open (per-name table). CRK×87 yday $14.95 → 09:30 $15.00 +4.35; MRNA×9 yday $148.87 → 09:30 $153.62 +42.75; ARCT×80 yday $15.56 → 09:30 $15.61 +4.00; SLN×91 yday $14.79 → 09:30 $14.63 -14.56; EIX×24 yday $56.30 → 09:30 $55.79 -12.24; CRDL×621 yday $2.16 → 09:30 $2.16 +0.00; CLYM×97 yday $14.59 → 09:30 $14.49 -9.70; SAFX×3593 yday $0.38 → 09:30 $0.38 -3.59 | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 87 | $15.00 | $2.28 | $-43.68 | $1,351.49 | ▼ -43.68 after sell → book $10,758.21; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $2,732.03 | ▲ +65.02 after sell → book $10,756.17; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 80 | $15.61 | $2.25 | $-97.28 | $3,978.58 | ▼ -97.28 after sell → book $10,753.92; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 91 | $14.63 | $2.29 | $-24.57 | $5,307.62 | ▼ -24.57 after sell → book $10,751.63; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 24 | $55.79 | $2.08 | $+4.74 | $6,644.50 | ▲ +4.74 after sell → book $10,749.54; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 621 | $2.16 | $8.12 | $-28.56 | $7,977.74 | ▼ -28.56 after sell → book $10,741.42; vs 09:30 mark -8.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CLYM` | 97 | $14.49 | $2.31 | $+46.82 | $9,380.96 | ▲ +46.82 after sell → book $10,739.11; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SAFX` | 3593 | $0.38 | $24.97 | $-45.70 | $10,714.14 | ▼ -45.70 after sell → book $10,714.14; vs 09:30 mark -24.97 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 531 | $2.52 | $6.85 | — | $9,369.17 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1339.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 280 | $4.78 | $3.61 | — | $8,027.16 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1339.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 842 | $1.59 | $10.86 | — | $6,677.52 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1339.27 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 84 | $15.90 | $2.24 | — | $5,339.68 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.1; leftover $1339.27 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 380 | $3.52 | $4.90 | — | $3,997.18 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $1339.27 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $2,967.62 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1339.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 47 | $28.00 | $2.13 | — | $1,649.49 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+8.7; leftover $1339.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 133 | $10.02 | $2.39 | — | $314.44 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.2; leftover $1339.27 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $314.44 | ▲ close $10,706.22 vs 09:30 $10,760.48 (session +27.06) | 16:00 close · cash $314.44 · equity $10,706.22 vs 09:30 $10,760.48 (-54.26; session marks +27.06) · 8 name(s) marked open→close (per-name table). ALEC×531 09:30 $2.52 → close $2.46 -31.86; OABI×280 09:30 $4.78 → close $4.33 -126.00; OPK×842 09:30 $1.59 → close $1.64 +42.10; HQ×84 09:30 $15.90 → close $15.56 -28.56; EOSE×380 09:30 $3.52 → close $3.88 +136.80; DELL×2 09:30 $513.78 → close $524.14 +20.72; MLYS×47 09:30 $28.00 → close $28.21 +9.87; CCOI×133 09:30 $10.02 → close $10.05 +3.99 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $314.44 | ▼ 09:30 equity $10,651.53 vs yday $10,706.22 (-54.69) | 09:30 open · cash $314.44 (unchanged overnight, no fees) · equity $10,651.53 vs prior close $10,706.22 (-54.69) · 8 name(s) re-marked at the open (per-name table). ALEC×531 yday $2.46 → 09:30 $2.38 -42.48; OABI×280 yday $4.33 → 09:30 $4.30 -8.40; OPK×842 yday $1.64 → 09:30 $1.63 -8.42; HQ×84 yday $15.56 → 09:30 $15.40 -13.44; EOSE×380 yday $3.88 → 09:30 $3.99 +41.80; DELL×2 yday $524.14 → 09:30 $521.15 -5.98; MLYS×47 yday $28.21 → 09:30 $28.03 -8.46; CCOI×133 yday $10.05 → 09:30 $9.98 -9.31 | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 531 | $2.38 | $6.95 | $-88.14 | $1,571.27 | ▼ -88.14 after sell → book $10,644.58; vs 09:30 mark -6.95 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 280 | $4.30 | $3.67 | $-141.68 | $2,771.60 | ▼ -141.68 after sell → book $10,640.91; vs 09:30 mark -3.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 842 | $1.63 | $11.01 | $+11.81 | $4,133.05 | ▲ +11.81 after sell → book $10,629.90; vs 09:30 mark -11.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HQ` | 84 | $15.40 | $2.27 | $-46.51 | $5,424.38 | ▼ -46.51 after sell → book $10,627.63; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `EOSE` | 380 | $3.99 | $4.98 | $+168.72 | $6,935.61 | ▲ +168.72 after sell → book $10,622.66; vs 09:30 mark -4.97 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $7,975.89 | ▲ +10.73 after sell → book $10,620.64; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MLYS` | 47 | $28.03 | $2.15 | $-2.87 | $9,291.15 | ▼ -2.87 after sell → book $10,618.49; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CCOI` | 133 | $9.98 | $2.42 | $-10.13 | $10,616.07 | ▼ -10.13 after sell → book $10,616.07; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,616.07 | ▲ close $10,616.07 vs 09:30 $10,651.53 (session +0.00) | 16:00 close · cash $10,616.07 · no lots left · equity $10,616.07. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,616.07 | ▲ 09:30 equity $10,616.07 vs yday $10,616.07 (-0.00) | 09:30 open · cash $10,616.07 · no holdings · equity $10,616.07 vs prior close $10,616.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,616.07 | ▲ close $10,616.07 vs 09:30 $10,616.07 (session +0.00) | 16:00 close · cash $10,616.07 · no lots left · equity $10,616.07. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,616.07 | ▲ 09:30 equity $10,616.07 vs yday $10,616.07 (-0.00) | 09:30 open · cash $10,616.07 · no holdings · equity $10,616.07 vs prior close $10,616.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,616.07 | ▲ close $10,616.07 vs 09:30 $10,616.07 (session +0.00) | 16:00 close · cash $10,616.07 · no lots left · equity $10,616.07. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,616.07 | ▲ 09:30 equity $10,616.07 vs yday $10,616.07 (-0.00) | 09:30 open · cash $10,616.07 · no holdings · equity $10,616.07 vs prior close $10,616.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 423 | $3.13 | $5.46 | — | $9,286.62 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+6.2; leftover $1327.01 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 224 | $5.91 | $2.89 | — | $7,959.89 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.9; leftover $1327.01 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 650 | $2.04 | $8.38 | — | $6,625.51 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.8; leftover $1327.01 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 279 | $4.75 | $3.60 | — | $5,296.66 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.4; leftover $1327.01 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 625 | $2.12 | $8.06 | — | $3,963.59 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1327.01 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 56 | $23.63 | $2.16 | — | $2,638.16 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.8; leftover $1327.01 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `QRVO` | 11 | $112.83 | $2.02 | — | $1,394.95 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1327.01 | join🟢 sector🟢 gen🟡 news🔴 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `APPS` | 111 | $11.88 | $2.32 | — | $73.95 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+5.0; leftover $1327.01 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.95 | ▲ close $10,652.70 vs 09:30 $10,616.07 (session +71.53) | 16:00 close · cash $73.95 · equity $10,652.70 vs 09:30 $10,616.07 (+36.63; session marks +71.53) · 8 name(s) marked open→close (per-name table). CMRC×423 09:30 $3.13 → close $3.50 +158.62; DBI×224 09:30 $5.91 → close $5.88 -6.72; AMTX×650 09:30 $2.04 → close $2.01 -19.50; CLOV×279 09:30 $4.75 → close $4.82 +19.53; BAK×625 09:30 $2.12 → close $2.08 -25.00; TYRA×56 09:30 $23.63 → close $22.03 -89.60; QRVO×11 09:30 $112.83 → close $116.65 +41.97; APPS×111 09:30 $11.88 → close $11.81 -7.77 | — |

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
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ELMT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `IRD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `XLAB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CABA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CNTB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HAS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BHC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SARO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XHLD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HELP` | hard_red | hard-red S=-13.28 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CMRC` | 423 | 2026-09-11 @ $3.13 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+6.2; leftover $1327.01 |
| `DBI` | 224 | 2026-09-11 @ $5.91 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.9; leftover $1327.01 |
| `AMTX` | 650 | 2026-09-11 @ $2.04 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.8; leftover $1327.01 |
| `CLOV` | 279 | 2026-09-11 @ $4.75 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.4; leftover $1327.01 |
| `BAK` | 625 | 2026-09-11 @ $2.12 | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1327.01 |
| `TYRA` | 56 | 2026-09-11 @ $23.63 | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.8; leftover $1327.01 |
| `QRVO` | 11 | 2026-09-11 @ $112.83 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1327.01 |
| `APPS` | 111 | 2026-09-11 @ $11.88 | baseline list, no extra gate; list probable,yday_gainer; ret5=+5.0; leftover $1327.01 |
