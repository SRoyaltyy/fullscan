# Factor mine action — `probable_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-1.86%** ($9,814) · signal-only (no cash/fees) was +5.15%. Starts YES **7/21**. Fills 152 · skips 72 · realized $-220.15.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $100.00.

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
| 2026-08-20 | `MRVI` | 163 | — | $7.44 | +0.00 | $8.29 | +138.55 | +138.55 | +0.00 | +138.55 |
| 2026-08-20 | `DNA` | 162 | — | $7.45 | +0.00 | $6.96 | -79.38 | -79.38 | +0.00 | -79.38 |
| 2026-08-20 | `MSTR` | 10 | — | $113.23 | +0.00 | $112.39 | -8.40 | -8.40 | +0.00 | -8.40 |
| 2026-08-20 | `EXK` | 112 | — | $10.77 | +0.00 | $10.97 | +22.40 | +22.40 | +0.00 | +22.40 |
| 2026-08-20 | `SCZM` | 128 | — | $9.46 | +0.00 | $9.76 | +38.40 | +38.40 | +0.00 | +38.40 |
| 2026-08-20 | `NG` | 144 | — | $8.38 | +0.00 | $8.66 | +40.32 | +40.32 | +0.00 | +40.32 |
| 2026-08-20 | `BLSH` | 41 | — | $29.20 | +0.00 | $28.44 | -31.16 | -31.16 | +0.00 | -31.16 |
| 2026-08-20 | `HYMC` | 44 | — | $27.25 | +0.00 | $26.14 | -48.84 | -48.84 | +0.00 | -48.84 |
| 2026-08-21 | `MRVI` | 163 | $8.29 | $8.28 | -1.63 | — | +0.00 | -1.63 | +136.92 | — |
| 2026-08-21 | `DNA` | 162 | $6.96 | $7.09 | +21.06 | — | +0.00 | +21.06 | -58.32 | — |
| 2026-08-21 | `MSTR` | 10 | $112.39 | $119.69 | +73.00 | — | +0.00 | +73.00 | +64.60 | — |
| 2026-08-21 | `EXK` | 112 | $10.97 | $11.34 | +41.44 | — | +0.00 | +41.44 | +63.84 | — |
| 2026-08-21 | `SCZM` | 128 | $9.76 | $10.26 | +64.00 | — | +0.00 | +64.00 | +102.40 | — |
| 2026-08-21 | `NG` | 144 | $8.66 | $9.02 | +51.84 | — | +0.00 | +51.84 | +92.16 | — |
| 2026-08-21 | `BLSH` | 41 | $28.44 | $29.75 | +53.71 | — | +0.00 | +53.71 | +22.55 | — |
| 2026-08-21 | `HYMC` | 44 | $26.14 | $27.40 | +55.44 | — | +0.00 | +55.44 | +6.60 | — |
| 2026-08-21 | `BTBT` | 760 | — | $1.66 | +0.00 | $1.53 | -98.80 | -98.80 | +0.00 | -98.80 |
| 2026-08-21 | `ENHA` | 738 | — | $1.71 | +0.00 | $1.72 | +7.38 | +7.38 | +0.00 | +7.38 |
| 2026-08-21 | `DE` | 2 | — | $623.26 | +0.00 | $647.47 | +48.42 | +48.42 | +0.00 | +48.42 |
| 2026-08-21 | `QDEL` | 84 | — | $14.96 | +0.00 | $14.74 | -18.48 | -18.48 | +0.00 | -18.48 |
| 2026-08-21 | `ORBS` | 1461 | — | $0.86 | +0.00 | $0.88 | +23.38 | +23.38 | +0.00 | +23.38 |
| 2026-08-21 | `GORO` | 405 | — | $3.11 | +0.00 | $3.19 | +32.40 | +32.40 | +0.00 | +32.40 |
| 2026-08-21 | `QTRX` | 405 | — | $3.11 | +0.00 | $2.99 | -48.60 | -48.60 | +0.00 | -48.60 |
| 2026-08-21 | `CF` | 9 | — | $127.43 | +0.00 | $129.60 | +19.53 | +19.53 | +0.00 | +19.53 |
| 2026-08-24 | `BTBT` | 760 | $1.53 | $1.55 | +15.20 | — | +0.00 | +15.20 | -83.60 | — |
| 2026-08-24 | `ENHA` | 738 | $1.72 | $1.74 | +14.76 | — | +0.00 | +14.76 | +22.14 | — |
| 2026-08-24 | `DE` | 2 | $647.47 | $653.04 | +11.14 | — | +0.00 | +11.14 | +59.56 | — |
| 2026-08-24 | `QDEL` | 84 | $14.74 | $14.74 | +0.00 | — | +0.00 | +0.00 | -18.48 | — |
| 2026-08-24 | `ORBS` | 1461 | $0.88 | $0.89 | +14.61 | — | +0.00 | +14.61 | +37.99 | — |
| 2026-08-24 | `GORO` | 405 | $3.19 | $3.20 | +4.05 | — | +0.00 | +4.05 | +36.45 | — |
| 2026-08-24 | `QTRX` | 405 | $2.99 | $2.99 | +0.00 | — | +0.00 | +0.00 | -48.60 | — |
| 2026-08-24 | `CF` | 9 | $129.60 | $129.99 | +3.51 | — | +0.00 | +3.51 | +23.04 | — |
| 2026-08-25 | `CAPR` | 172 | — | $7.25 | +0.00 | $8.29 | +178.88 | +178.88 | +0.00 | +178.88 |
| 2026-08-25 | `SAFX` | 3498 | — | $0.36 | +0.00 | $0.35 | -13.99 | -13.99 | +0.00 | -13.99 |
| 2026-08-25 | `VITL` | 112 | — | $11.12 | +0.00 | $11.11 | -1.12 | -1.12 | +0.00 | -1.12 |
| 2026-08-25 | `KURA` | 92 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CCOI` | 131 | — | $9.49 | +0.00 | $9.88 | +51.09 | +51.09 | +0.00 | +51.09 |
| 2026-08-25 | `LIFE` | 33 | — | $36.96 | +0.00 | $38.56 | +52.80 | +52.80 | +0.00 | +52.80 |
| 2026-08-25 | `ZIP` | 275 | — | $4.55 | +0.00 | $4.35 | -55.00 | -55.00 | +0.00 | -55.00 |
| 2026-08-25 | `ADIG` | 57 | — | $21.79 | +0.00 | $22.27 | +27.36 | +27.36 | +0.00 | +27.36 |
| 2026-08-26 | `CAPR` | 172 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | +178.88 | — |
| 2026-08-26 | `SAFX` | 3498 | $0.35 | $0.35 | -3.50 | — | +0.00 | -3.50 | -17.49 | — |
| 2026-08-26 | `VITL` | 112 | $11.11 | $11.03 | -8.96 | — | +0.00 | -8.96 | -10.08 | — |
| 2026-08-26 | `KURA` | 92 | $13.59 | $13.63 | +3.68 | — | +0.00 | +3.68 | +3.68 | — |
| 2026-08-26 | `CCOI` | 131 | $9.88 | $9.89 | +1.31 | — | +0.00 | +1.31 | +52.40 | — |
| 2026-08-26 | `LIFE` | 33 | $38.56 | $38.24 | -10.56 | — | +0.00 | -10.56 | +42.24 | — |
| 2026-08-26 | `ZIP` | 275 | $4.35 | $4.31 | -11.00 | — | +0.00 | -11.00 | -66.00 | — |
| 2026-08-26 | `ADIG` | 57 | $22.27 | $21.78 | -27.93 | — | +0.00 | -27.93 | -0.57 | — |
| 2026-08-26 | `AVBP` | 40 | — | $31.21 | +0.00 | $31.14 | -2.80 | -2.80 | +0.00 | -2.80 |
| 2026-08-26 | `FLNC` | 113 | — | $11.12 | +0.00 | $11.08 | -4.52 | -4.52 | +0.00 | -4.52 |
| 2026-08-26 | `ABX` | 128 | — | $9.83 | +0.00 | $9.78 | -6.40 | -6.40 | +0.00 | -6.40 |
| 2026-08-26 | `AVEX` | 72 | — | $17.51 | +0.00 | $18.34 | +59.76 | +59.76 | +0.00 | +59.76 |
| 2026-08-26 | `ITG` | 105 | — | $12.04 | +0.00 | $12.45 | +43.05 | +43.05 | +0.00 | +43.05 |
| 2026-08-26 | `SENS` | 133 | — | $9.48 | +0.00 | $9.34 | -18.62 | -18.62 | +0.00 | -18.62 |
| 2026-08-26 | `BE` | 5 | — | $213.94 | +0.00 | $218.21 | +21.35 | +21.35 | +0.00 | +21.35 |
| 2026-08-26 | `AXTI` | 19 | — | $65.34 | +0.00 | $65.18 | -3.04 | -3.04 | +0.00 | -3.04 |
| 2026-08-27 | `AVBP` | 40 | $31.14 | $30.79 | -14.00 | — | +0.00 | -14.00 | -16.80 | — |
| 2026-08-27 | `FLNC` | 113 | $11.08 | $11.52 | +49.72 | — | +0.00 | +49.72 | +45.20 | — |
| 2026-08-27 | `ABX` | 128 | $9.78 | $9.68 | -12.80 | — | +0.00 | -12.80 | -19.20 | — |
| 2026-08-27 | `AVEX` | 72 | $18.34 | $18.43 | +6.48 | — | +0.00 | +6.48 | +66.24 | — |
| 2026-08-27 | `ITG` | 105 | $12.45 | $12.36 | -9.45 | — | +0.00 | -9.45 | +33.60 | — |
| 2026-08-27 | `SENS` | 133 | $9.34 | $9.33 | -1.33 | — | +0.00 | -1.33 | -19.95 | — |
| 2026-08-27 | `BE` | 5 | $218.21 | $227.10 | +44.45 | — | +0.00 | +44.45 | +65.80 | — |
| 2026-08-27 | `AXTI` | 19 | $65.18 | $70.30 | +97.28 | — | +0.00 | +97.28 | +94.24 | — |
| 2026-08-28 | `SEDG` | 39 | — | $32.90 | +0.00 | $31.41 | -58.11 | -58.11 | +0.00 | -58.11 |
| 2026-08-28 | `GRRR` | 82 | — | $15.66 | +0.00 | $14.41 | -102.50 | -102.50 | +0.00 | -102.50 |
| 2026-08-28 | `URBN` | 16 | — | $79.42 | +0.00 | $81.09 | +26.72 | +26.72 | +0.00 | +26.72 |
| 2026-08-28 | `PYXS` | 389 | — | $3.32 | +0.00 | $3.23 | -35.01 | -35.01 | +0.00 | -35.01 |
| 2026-08-28 | `SAFX` | 3539 | — | $0.36 | +0.00 | $0.36 | -21.23 | -21.23 | +0.00 | -21.23 |
| 2026-08-28 | `SIMO` | 5 | — | $252.24 | +0.00 | $245.81 | -32.15 | -32.15 | +0.00 | -32.15 |
| 2026-08-28 | `OPTX` | 150 | — | $8.61 | +0.00 | $8.52 | -13.50 | -13.50 | +0.00 | -13.50 |
| 2026-08-28 | `XPOF` | 240 | — | $5.38 | +0.00 | $5.43 | +12.00 | +12.00 | +0.00 | +12.00 |
| 2026-08-31 | `SEDG` | 39 | $31.41 | $31.15 | -10.14 | — | +0.00 | -10.14 | -68.25 | — |
| 2026-08-31 | `GRRR` | 82 | $14.41 | $14.44 | +2.46 | — | +0.00 | +2.46 | -100.04 | — |
| 2026-08-31 | `URBN` | 16 | $81.09 | $80.44 | -10.40 | — | +0.00 | -10.40 | +16.32 | — |
| 2026-08-31 | `PYXS` | 389 | $3.23 | $3.20 | -11.67 | — | +0.00 | -11.67 | -46.68 | — |
| 2026-08-31 | `SAFX` | 3539 | $0.36 | $0.36 | +10.62 | — | +0.00 | +10.62 | -10.62 | — |
| 2026-08-31 | `SIMO` | 5 | $245.81 | $247.05 | +6.20 | — | +0.00 | +6.20 | -25.95 | — |
| 2026-08-31 | `OPTX` | 150 | $8.52 | $8.52 | +0.00 | — | +0.00 | +0.00 | -13.50 | — |
| 2026-08-31 | `XPOF` | 240 | $5.43 | $5.37 | -14.40 | — | +0.00 | -14.40 | -2.40 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `CRK` | 80 | — | $15.45 | +0.00 | $14.95 | -40.00 | -40.00 | +0.00 | -40.00 |
| 2026-09-03 | `MRNA` | 8 | — | $145.94 | +0.00 | $148.87 | +23.40 | +23.40 | +0.00 | +23.40 |
| 2026-09-03 | `ARCT` | 74 | — | $16.77 | +0.00 | $15.56 | -89.54 | -89.54 | +0.00 | -89.54 |
| 2026-09-03 | `SLN` | 84 | — | $14.85 | +0.00 | $14.79 | -5.04 | -5.04 | +0.00 | -5.04 |
| 2026-09-03 | `EIX` | 22 | — | $55.42 | +0.00 | $56.30 | +19.36 | +19.36 | +0.00 | +19.36 |
| 2026-09-03 | `CRDL` | 573 | — | $2.18 | +0.00 | $2.16 | -11.46 | -11.46 | +0.00 | -11.46 |
| 2026-09-03 | `CLYM` | 89 | — | $13.96 | +0.00 | $14.59 | +56.07 | +56.07 | +0.00 | +56.07 |
| 2026-09-03 | `SAFX` | 3315 | — | $0.38 | +0.00 | $0.38 | +6.63 | +6.63 | +0.00 | +6.63 |
| 2026-09-04 | `CRK` | 80 | $14.95 | $15.00 | +4.00 | — | +0.00 | +4.00 | -36.00 | — |
| 2026-09-04 | `MRNA` | 8 | $148.87 | $153.62 | +38.00 | — | +0.00 | +38.00 | +61.40 | — |
| 2026-09-04 | `ARCT` | 74 | $15.56 | $15.61 | +3.70 | — | +0.00 | +3.70 | -85.84 | — |
| 2026-09-04 | `SLN` | 84 | $14.79 | $14.63 | -13.44 | — | +0.00 | -13.44 | -18.48 | — |
| 2026-09-04 | `EIX` | 22 | $56.30 | $55.79 | -11.22 | — | +0.00 | -11.22 | +8.14 | — |
| 2026-09-04 | `CRDL` | 573 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -11.46 | — |
| 2026-09-04 | `CLYM` | 89 | $14.59 | $14.49 | -8.90 | — | +0.00 | -8.90 | +47.17 | — |
| 2026-09-04 | `SAFX` | 3315 | $0.38 | $0.38 | -3.32 | — | +0.00 | -3.32 | +3.32 | — |
| 2026-09-04 | `ALEC` | 490 | — | $2.52 | +0.00 | $2.46 | -29.40 | -29.40 | +0.00 | -29.40 |
| 2026-09-04 | `OABI` | 258 | — | $4.78 | +0.00 | $4.33 | -116.10 | -116.10 | +0.00 | -116.10 |
| 2026-09-04 | `HQ` | 77 | — | $15.90 | +0.00 | $15.56 | -26.18 | -26.18 | +0.00 | -26.18 |
| 2026-09-04 | `EOSE` | 350 | — | $3.52 | +0.00 | $3.88 | +126.00 | +126.00 | +0.00 | +126.00 |
| 2026-09-04 | `DELL` | 2 | — | $513.78 | +0.00 | $524.14 | +20.72 | +20.72 | +0.00 | +20.72 |
| 2026-09-04 | `MLYS` | 44 | — | $28.00 | +0.00 | $28.21 | +9.24 | +9.24 | +0.00 | +9.24 |
| 2026-09-04 | `CCOI` | 123 | — | $10.02 | +0.00 | $10.05 | +3.69 | +3.69 | +0.00 | +3.69 |
| 2026-09-04 | `UAMY` | 235 | — | $5.25 | +0.00 | $5.20 | -11.75 | -11.75 | +0.00 | -11.75 |
| 2026-09-08 | `ALEC` | 490 | $2.46 | $2.38 | -39.20 | — | +0.00 | -39.20 | -68.60 | — |
| 2026-09-08 | `OABI` | 258 | $4.33 | $4.30 | -7.74 | — | +0.00 | -7.74 | -123.84 | — |
| 2026-09-08 | `HQ` | 77 | $15.56 | $15.40 | -12.32 | — | +0.00 | -12.32 | -38.50 | — |
| 2026-09-08 | `EOSE` | 350 | $3.88 | $3.99 | +38.50 | — | +0.00 | +38.50 | +164.50 | — |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | — | +0.00 | -5.98 | +14.74 | — |
| 2026-09-08 | `MLYS` | 44 | $28.21 | $28.03 | -7.92 | — | +0.00 | -7.92 | +1.32 | — |
| 2026-09-08 | `CCOI` | 123 | $10.05 | $9.98 | -8.61 | — | +0.00 | -8.61 | -4.92 | — |
| 2026-09-08 | `UAMY` | 235 | $5.20 | $5.28 | +18.80 | — | +0.00 | +18.80 | +7.05 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `CMRC` | 390 | — | $3.13 | +0.00 | $3.50 | +146.25 | +146.25 | +0.00 | +146.25 |
| 2026-09-11 | `DBI` | 206 | — | $5.91 | +0.00 | $5.88 | -6.18 | -6.18 | +0.00 | -6.18 |
| 2026-09-11 | `AMTX` | 599 | — | $2.04 | +0.00 | $2.01 | -17.97 | -17.97 | +0.00 | -17.97 |
| 2026-09-11 | `CLOV` | 257 | — | $4.75 | +0.00 | $4.82 | +17.99 | +17.99 | +0.00 | +17.99 |
| 2026-09-11 | `BAK` | 576 | — | $2.12 | +0.00 | $2.08 | -23.04 | -23.04 | +0.00 | -23.04 |
| 2026-09-11 | `TYRA` | 51 | — | $23.63 | +0.00 | $22.03 | -81.60 | -81.60 | +0.00 | -81.60 |
| 2026-09-11 | `QRVO` | 10 | — | $112.83 | +0.00 | $116.65 | +38.15 | +38.15 | +0.00 | +38.15 |
| 2026-09-11 | `APPS` | 102 | — | $11.88 | +0.00 | $11.81 | -7.14 | -7.14 | +0.00 | -7.14 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +9.14 | ANGX, WWW, HYLN, WDC, FOSL, ADUR, AIRS, ALGM | — | $269.08 | $9,985.46 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28 |
| 2026-08-17 | +2.25 | $269.08 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28 | $10,059.20 | +73.74 | -129.60 | CDNL, ABX, FCEL, VERA, CELC, BW, OCC, ALM | ANGX, WWW, HYLN, WDC, FOSL, ADUR, AIRS, ALGM | $79.21 | $9,888.06 | CDNL×31, ABX×137, FCEL×56, VERA×40, CELC×13, BW×121, OCC×68, ALM×77 |
| 2026-08-18 | -6.20 | $79.21 | CDNL×31, ABX×137, FCEL×56, VERA×40, CELC×13, BW×121, OCC×68, ALM×77 | $9,722.67 | -165.39 | +0.00 | — | CDNL, ABX, FCEL, VERA, CELC, BW, OCC, ALM | $9,704.93 | $9,704.93 | — |
| 2026-08-19 | -7.20 | $9,704.93 | — | $9,704.93 | +0.00 | +0.00 | — | — | $9,704.93 | $9,704.93 | — |
| 2026-08-20 | +1.12 | $9,704.93 | — | $9,704.93 | +0.00 | +71.89 | MRVI, DNA, MSTR, EXK, SCZM, NG, BLSH, HYMC | — | $114.64 | $9,758.49 | MRVI×163, DNA×162, MSTR×10, EXK×112, SCZM×128, NG×144, BLSH×41, HYMC×44 |
| 2026-08-21 | +3.25 | $114.64 | MRVI×163, DNA×162, MSTR×10, EXK×112, SCZM×128, NG×144, BLSH×41, HYMC×44 | $10,117.35 | +358.86 | -34.77 | BTBT, ENHA, DE, QDEL, ORBS, GORO, QTRX, CF | MRVI, DNA, MSTR, EXK, SCZM, NG, BLSH, HYMC | $90.74 | $10,010.98 | BTBT×760, ENHA×738, DE×2, QDEL×84, ORBS×1461, GORO×405, QTRX×405, CF×9 |
| 2026-08-24 | -5.17 | $90.74 | BTBT×760, ENHA×738, DE×2, QDEL×84, ORBS×1461, GORO×405, QTRX×405, CF×9 | $10,074.25 | +63.27 | +0.00 | — | BTBT, ENHA, DE, QDEL, ORBS, GORO, QTRX, CF | $10,020.09 | $10,020.09 | — |
| 2026-08-25 | +1.80 | $10,020.09 | — | $10,020.09 | +0.00 | +240.02 | CAPR, SAFX, VITL, KURA, CCOI, LIFE, ZIP, ADIG | — | $28.65 | $10,219.82 | CAPR×172, SAFX×3498, VITL×112, KURA×92, CCOI×131, LIFE×33, ZIP×275, ADIG×57 |
| 2026-08-26 | +2.02 | $28.65 | CAPR×172, SAFX×3498, VITL×112, KURA×92, CCOI×131, LIFE×33, ZIP×275, ADIG×57 | $10,162.86 | -56.96 | +88.78 | AVBP, FLNC, ABX, AVEX, ITG, SENS, BE, AXTI | CAPR, SAFX, VITL, KURA, CCOI, LIFE, ZIP, ADIG | $244.04 | $10,192.94 | AVBP×40, FLNC×113, ABX×128, AVEX×72, ITG×105, SENS×133, BE×5, AXTI×19 |
| 2026-08-27 | — | $244.04 | AVBP×40, FLNC×113, ABX×128, AVEX×72, ITG×105, SENS×133, BE×5, AXTI×19 | $10,353.29 | +160.35 | +0.00 | — | AVBP, FLNC, ABX, AVEX, ITG, SENS, BE, AXTI | $10,335.32 | $10,335.32 | — |
| 2026-08-28 | +0.75 | $10,335.32 | — | $10,335.32 | +0.00 | -223.78 | SEDG, GRRR, URBN, PYXS, SAFX, SIMO, OPTX, XPOF | — | $27.79 | $10,069.06 | SEDG×39, GRRR×82, URBN×16, PYXS×389, SAFX×3539, SIMO×5, OPTX×150, XPOF×240 |
| 2026-08-31 | -5.85 | $27.79 | SEDG×39, GRRR×82, URBN×16, PYXS×389, SAFX×3539, SIMO×5, OPTX×150, XPOF×240 | $10,041.73 | -27.33 | +0.00 | — | SEDG, GRRR, URBN, PYXS, SAFX, SIMO, OPTX, XPOF | $9,998.52 | $9,998.52 | — |
| 2026-09-01 | -6.30 | $9,998.52 | — | $9,998.52 | +0.00 | +0.00 | — | — | $9,998.52 | $9,998.52 | — |
| 2026-09-02 | -3.83 | $9,998.52 | — | $9,998.52 | +0.00 | +0.00 | — | — | $9,998.52 | $9,998.52 | — |
| 2026-09-03 | -0.90 | $9,998.52 | — | $9,998.52 | +0.00 | -40.58 | CRK, MRNA, ARCT, SLN, EIX, CRDL, CLYM, SAFX | — | $103.16 | $9,915.10 | CRK×80, MRNA×8, ARCT×74, SLN×84, EIX×22, CRDL×573, CLYM×89, SAFX×3315 |
| 2026-09-04 | +2.25 | $103.16 | CRK×80, MRNA×8, ARCT×74, SLN×84, EIX×22, CRDL×573, CLYM×89, SAFX×3315 | $9,923.92 | +8.82 | -23.78 | ALEC, OABI, HQ, EOSE, DELL, MLYS, CCOI, UAMY | CRK, MRNA, ARCT, SLN, EIX, CRDL, CLYM, SAFX | $204.24 | $9,830.57 | ALEC×490, OABI×258, HQ×77, EOSE×350, DELL×2, MLYS×44, CCOI×123, UAMY×235 |
| 2026-09-08 | -11.47 | $204.24 | ALEC×490, OABI×258, HQ×77, EOSE×350, DELL×2, MLYS×44, CCOI×123, UAMY×235 | $9,806.10 | -24.47 | +0.00 | — | ALEC, OABI, HQ, EOSE, DELL, MLYS, CCOI, UAMY | $9,779.85 | $9,779.85 | — |
| 2026-09-09 | -13.95 | $9,779.85 | — | $9,779.85 | -0.00 | +0.00 | — | — | $9,779.85 | $9,779.85 | — |
| 2026-09-10 | -13.28 | $9,779.85 | — | $9,779.85 | -0.00 | +0.00 | — | — | $9,779.85 | $9,779.85 | — |
| 2026-09-11 | +0.50 | $9,779.85 | — | $9,779.85 | -0.00 | +66.46 | CMRC, DBI, AMTX, CLOV, BAK, TYRA, QRVO, APPS | — | $100.00 | $9,813.69 | CMRC×390, DBI×206, AMTX×599, CLOV×257, BAK×576, TYRA×51, QRVO×10, APPS×102 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $8,746.36 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WWW` | 60 | $20.60 | $2.17 | — | $7,508.19 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+4.4; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $6,254.51 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $5,245.52 | — | baseline list, no extra gate; list probable; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FOSL` | 221 | $5.64 | $2.85 | — | $3,996.23 | — | baseline list, no extra gate; list probable; 🔵; ret5=-4.1; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $2,756.51 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRS` | 370 | $3.37 | $4.77 | — | $1,504.84 | — | baseline list, no extra gate; list probable; ret5=-29.1; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 28 | $44.06 | $2.07 | — | $269.08 | — | baseline list, no extra gate; list probable; 🔵; ret5=+3.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
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
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $8,797.77 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1254.40 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 137 | $9.12 | $2.40 | — | $7,545.93 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1254.40 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `FCEL` | 56 | $22.37 | $2.16 | — | $6,291.05 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+9.5; leftover $1254.40 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 40 | $31.30 | $2.11 | — | $5,036.94 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-3.8; leftover $1254.40 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 13 | $92.99 | $2.03 | — | $3,826.05 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.8; leftover $1254.40 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BW` | 121 | $10.35 | $2.35 | — | $2,571.34 | — | baseline list, no extra gate; list probable; ⚪; ret5=+9.8; leftover $1254.40 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 68 | $18.24 | $2.19 | — | $1,328.83 | — | baseline list, no extra gate; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1254.40 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
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
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 163 | $7.44 | $2.48 | — | $8,489.73 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1213.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 162 | $7.45 | $2.48 | — | $7,280.36 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1213.12 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 10 | $113.23 | $2.02 | — | $6,146.04 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1213.12 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 112 | $10.77 | $2.33 | — | $4,937.47 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1213.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 128 | $9.46 | $2.37 | — | $3,724.22 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1213.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 144 | $8.38 | $2.42 | — | $2,515.07 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1213.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 41 | $29.20 | $2.11 | — | $1,315.76 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1213.12 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HYMC` | 44 | $27.25 | $2.12 | — | $114.64 | — | baseline list, no extra gate; list probable; 🔵; ret5=+1.6; leftover $1213.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $114.64 | ▲ close $9,758.49 vs 09:30 $9,704.93 (session +71.89) | 16:00 close · cash $114.64 · equity $9,758.49 vs 09:30 $9,704.93 (+53.56; session marks +71.89) · 8 name(s) marked open→close (per-name table). MRVI×163 09:30 $7.44 → close $8.29 +138.55; DNA×162 09:30 $7.45 → close $6.96 -79.38; MSTR×10 09:30 $113.23 → close $112.39 -8.40; EXK×112 09:30 $10.77 → close $10.97 +22.40; SCZM×128 09:30 $9.46 → close $9.76 +38.40; NG×144 09:30 $8.38 → close $8.66 +40.32; BLSH×41 09:30 $29.20 → close $28.44 -31.16; HYMC×44 09:30 $27.25 → close $26.14 -48.84 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $114.64 | ▲ 09:30 equity $10,117.35 vs yday $9,758.49 (+358.86) | 09:30 open · cash $114.64 (unchanged overnight, no fees) · equity $10,117.35 vs prior close $9,758.49 (+358.86) · 8 name(s) re-marked at the open (per-name table). MRVI×163 yday $8.29 → 09:30 $8.28 -1.63; DNA×162 yday $6.96 → 09:30 $7.09 +21.06; MSTR×10 yday $112.39 → 09:30 $119.69 +73.00; EXK×112 yday $10.97 → 09:30 $11.34 +41.44; SCZM×128 yday $9.76 → 09:30 $10.26 +64.00; NG×144 yday $8.66 → 09:30 $9.02 +51.84; BLSH×41 yday $28.44 → 09:30 $29.75 +53.71; HYMC×44 yday $26.14 → 09:30 $27.40 +55.44 | — |
| 2026-08-21 09:30 ET | **SELL** | `MRVI` | 163 | $8.28 | $2.52 | $+131.92 | $1,461.76 | ▲ +131.92 after sell → book $10,114.83; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 162 | $7.09 | $2.51 | $-63.31 | $2,607.83 | ▼ -63.31 after sell → book $10,112.32; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MSTR` | 10 | $119.69 | $2.04 | $+60.54 | $3,802.69 | ▲ +60.54 after sell → book $10,110.28; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `EXK` | 112 | $11.34 | $2.35 | $+59.16 | $5,070.41 | ▲ +59.16 after sell → book $10,107.92; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 128 | $10.26 | $2.41 | $+97.62 | $6,381.29 | ▲ +97.62 after sell → book $10,105.52; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NG` | 144 | $9.02 | $2.46 | $+87.28 | $7,677.71 | ▲ +87.28 after sell → book $10,103.06; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BLSH` | 41 | $29.75 | $2.13 | $+18.30 | $8,895.33 | ▲ +18.30 after sell → book $10,100.93; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYMC` | 44 | $27.40 | $2.14 | $+2.34 | $10,098.79 | ▲ +2.34 after sell → book $10,098.79; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 760 | $1.66 | $9.80 | — | $8,827.38 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1262.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 738 | $1.71 | $9.52 | — | $7,555.88 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1262.35 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $6,307.37 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1262.35 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 84 | $14.96 | $2.24 | — | $5,048.49 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-1.6; leftover $1262.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1461 | $0.86 | $17.01 | — | $3,769.18 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1262.35 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 405 | $3.11 | $5.22 | — | $2,504.40 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+7.1; leftover $1262.35 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QTRX` | 405 | $3.11 | $5.22 | — | $1,239.63 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $1262.35 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 9 | $127.43 | $2.02 | — | $90.74 | — | baseline list, no extra gate; list probable; 🔵; ⚪; ret5=+7.9; leftover $1262.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.74 | ▼ close $10,010.98 vs 09:30 $10,117.35 (session -34.77) | 16:00 close · cash $90.74 · equity $10,010.98 vs 09:30 $10,117.35 (-106.37; session marks -34.77) · 8 name(s) marked open→close (per-name table). BTBT×760 09:30 $1.66 → close $1.53 -98.80; ENHA×738 09:30 $1.71 → close $1.72 +7.38; DE×2 09:30 $623.26 → close $647.47 +48.42; QDEL×84 09:30 $14.96 → close $14.74 -18.48; ORBS×1461 09:30 $0.86 → close $0.88 +23.38; GORO×405 09:30 $3.11 → close $3.19 +32.40; QTRX×405 09:30 $3.11 → close $2.99 -48.60; CF×9 09:30 $127.43 → close $129.60 +19.53 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.74 | ▲ 09:30 equity $10,074.25 vs yday $10,010.98 (+63.27) | 09:30 open · cash $90.74 (unchanged overnight, no fees) · equity $10,074.25 vs prior close $10,010.98 (+63.27) · 8 name(s) re-marked at the open (per-name table). BTBT×760 yday $1.53 → 09:30 $1.55 +15.20; ENHA×738 yday $1.72 → 09:30 $1.74 +14.76; DE×2 yday $647.47 → 09:30 $653.04 +11.14; QDEL×84 yday $14.74 → 09:30 $14.74 +0.00; ORBS×1461 yday $0.88 → 09:30 $0.89 +14.61; GORO×405 yday $3.19 → 09:30 $3.20 +4.05; QTRX×405 yday $2.99 → 09:30 $2.99 +0.00; CF×9 yday $129.60 → 09:30 $129.99 +3.51 | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 760 | $1.55 | $9.94 | $-103.34 | $1,258.80 | ▼ -103.34 after sell → book $10,064.31; vs 09:30 mark -9.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ENHA` | 738 | $1.74 | $9.65 | $+2.97 | $2,533.27 | ▲ +2.97 after sell → book $10,054.66; vs 09:30 mark -9.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $3,837.33 | ▲ +55.55 after sell → book $10,052.64; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 84 | $14.74 | $2.27 | $-22.99 | $5,073.22 | ▼ -22.99 after sell → book $10,050.37; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1461 | $0.89 | $17.64 | $+3.34 | $6,355.88 | ▲ +3.34 after sell → book $10,032.74; vs 09:30 mark -17.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GORO` | 405 | $3.20 | $5.30 | $+25.92 | $7,646.57 | ▲ +25.92 after sell → book $10,027.43; vs 09:30 mark -5.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QTRX` | 405 | $2.99 | $5.30 | $-59.13 | $8,852.22 | ▼ -59.13 after sell → book $10,022.13; vs 09:30 mark -5.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 9 | $129.99 | $2.04 | $+18.99 | $10,020.09 | ▲ +18.99 after sell → book $10,020.09; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,020.09 | ▲ close $10,020.09 vs 09:30 $10,074.25 (session +0.00) | 16:00 close · cash $10,020.09 · no lots left · equity $10,020.09. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,020.09 | ▲ 09:30 equity $10,020.09 vs yday $10,020.09 (+0.00) | 09:30 open · cash $10,020.09 · no holdings · equity $10,020.09 vs prior close $10,020.09 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 172 | $7.25 | $2.51 | — | $8,770.59 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1252.51 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3498 | $0.36 | $23.02 | — | $7,495.29 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-15.6; leftover $1252.51 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 112 | $11.12 | $2.33 | — | $6,247.52 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.7; leftover $1252.51 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 92 | $13.59 | $2.27 | — | $4,994.98 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1252.51 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 131 | $9.49 | $2.38 | — | $3,749.40 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1252.51 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 33 | $36.96 | $2.09 | — | $2,527.63 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1252.51 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 275 | $4.55 | $3.55 | — | $1,272.84 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1252.51 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ADIG` | 57 | $21.79 | $2.16 | — | $28.65 | — | baseline list, no extra gate; list probable; 🔵; ret5=+3.1; leftover $1252.51 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.65 | ▲ close $10,219.82 vs 09:30 $10,020.09 (session +240.02) | 16:00 close · cash $28.65 · equity $10,219.82 vs 09:30 $10,020.09 (+199.73; session marks +240.02) · 8 name(s) marked open→close (per-name table). CAPR×172 09:30 $7.25 → close $8.29 +178.88; SAFX×3498 09:30 $0.36 → close $0.35 -13.99; VITL×112 09:30 $11.12 → close $11.11 -1.12; KURA×92 09:30 $13.59 → close $13.59 +0.00; CCOI×131 09:30 $9.49 → close $9.88 +51.09; LIFE×33 09:30 $36.96 → close $38.56 +52.80; ZIP×275 09:30 $4.55 → close $4.35 -55.00; ADIG×57 09:30 $21.79 → close $22.27 +27.36 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.65 | ▼ 09:30 equity $10,162.86 vs yday $10,219.82 (-56.96) | 09:30 open · cash $28.65 (unchanged overnight, no fees) · equity $10,162.86 vs prior close $10,219.82 (-56.96) · 8 name(s) re-marked at the open (per-name table). CAPR×172 yday $8.29 → 09:30 $8.29 +0.00; SAFX×3498 yday $0.35 → 09:30 $0.35 -3.50; VITL×112 yday $11.11 → 09:30 $11.03 -8.96; KURA×92 yday $13.59 → 09:30 $13.63 +3.68; CCOI×131 yday $9.88 → 09:30 $9.89 +1.31; LIFE×33 yday $38.56 → 09:30 $38.24 -10.56; ZIP×275 yday $4.35 → 09:30 $4.31 -11.00; ADIG×57 yday $22.27 → 09:30 $21.78 -27.93 | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 172 | $8.29 | $2.55 | $+173.83 | $1,451.98 | ▲ +173.83 after sell → book $10,160.31; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 3498 | $0.35 | $23.43 | $-63.94 | $2,663.34 | ▼ -63.94 after sell → book $10,136.88; vs 09:30 mark -23.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `VITL` | 112 | $11.03 | $2.35 | $-14.76 | $3,896.35 | ▼ -14.76 after sell → book $10,134.53; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 92 | $13.63 | $2.29 | $-0.88 | $5,148.01 | ▼ -0.88 after sell → book $10,132.23; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 131 | $9.89 | $2.42 | $+47.60 | $6,441.19 | ▲ +47.60 after sell → book $10,129.82; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 33 | $38.24 | $2.11 | $+38.04 | $7,701.00 | ▲ +38.04 after sell → book $10,127.71; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 275 | $4.31 | $3.60 | $-73.15 | $8,882.65 | ▼ -73.15 after sell → book $10,124.11; vs 09:30 mark -3.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ADIG` | 57 | $21.78 | $2.18 | $-4.91 | $10,121.93 | ▼ -4.91 after sell → book $10,121.93; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 40 | $31.21 | $2.11 | — | $8,871.42 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1265.24 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 113 | $11.12 | $2.33 | — | $7,612.53 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1265.24 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 128 | $9.83 | $2.37 | — | $6,351.91 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1265.24 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AVEX` | 72 | $17.51 | $2.21 | — | $5,088.99 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $1265.24 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ITG` | 105 | $12.04 | $2.31 | — | $3,822.48 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-5.1; leftover $1265.24 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SENS` | 133 | $9.48 | $2.39 | — | $2,559.25 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $1265.24 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 5 | $213.94 | $2.00 | — | $1,487.55 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1265.24 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AXTI` | 19 | $65.34 | $2.05 | — | $244.04 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1265.24 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $244.04 | ▲ close $10,192.94 vs 09:30 $10,162.86 (session +88.78) | 16:00 close · cash $244.04 · equity $10,192.94 vs 09:30 $10,162.86 (+30.08; session marks +88.78) · 8 name(s) marked open→close (per-name table). AVBP×40 09:30 $31.21 → close $31.14 -2.80; FLNC×113 09:30 $11.12 → close $11.08 -4.52; ABX×128 09:30 $9.83 → close $9.78 -6.40; AVEX×72 09:30 $17.51 → close $18.34 +59.76; ITG×105 09:30 $12.04 → close $12.45 +43.05; SENS×133 09:30 $9.48 → close $9.34 -18.62; BE×5 09:30 $213.94 → close $218.21 +21.35; AXTI×19 09:30 $65.34 → close $65.18 -3.04 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $244.04 | ▲ 09:30 equity $10,353.29 vs yday $10,192.94 (+160.35) | 09:30 open · cash $244.04 (unchanged overnight, no fees) · equity $10,353.29 vs prior close $10,192.94 (+160.35) · 8 name(s) re-marked at the open (per-name table). AVBP×40 yday $31.14 → 09:30 $30.79 -14.00; FLNC×113 yday $11.08 → 09:30 $11.52 +49.72; ABX×128 yday $9.78 → 09:30 $9.68 -12.80; AVEX×72 yday $18.34 → 09:30 $18.43 +6.48; ITG×105 yday $12.45 → 09:30 $12.36 -9.45; SENS×133 yday $9.34 → 09:30 $9.33 -1.33; BE×5 yday $218.21 → 09:30 $227.10 +44.45; AXTI×19 yday $65.18 → 09:30 $70.30 +97.28 | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 40 | $30.79 | $2.13 | $-21.04 | $1,473.51 | ▼ -21.04 after sell → book $10,351.16; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 113 | $11.52 | $2.36 | $+40.51 | $2,772.91 | ▲ +40.51 after sell → book $10,348.80; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ABX` | 128 | $9.68 | $2.41 | $-23.98 | $4,009.55 | ▼ -23.98 after sell → book $10,346.40; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVEX` | 72 | $18.43 | $2.23 | $+61.81 | $5,334.28 | ▲ +61.81 after sell → book $10,344.17; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ITG` | 105 | $12.36 | $2.33 | $+28.96 | $6,629.75 | ▲ +28.96 after sell → book $10,341.84; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SENS` | 133 | $9.33 | $2.42 | $-24.76 | $7,868.22 | ▼ -24.76 after sell → book $10,339.42; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BE` | 5 | $227.10 | $2.02 | $+61.77 | $9,001.69 | ▲ +61.77 after sell → book $10,337.39; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AXTI` | 19 | $70.30 | $2.07 | $+90.13 | $10,335.32 | ▲ +90.13 after sell → book $10,335.32; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,335.32 | ▲ close $10,335.32 vs 09:30 $10,353.29 (session +0.00) | 16:00 close · cash $10,335.32 · no lots left · equity $10,335.32. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,335.32 | ▲ 09:30 equity $10,335.32 vs yday $10,335.32 (+0.00) | 09:30 open · cash $10,335.32 · no holdings · equity $10,335.32 vs prior close $10,335.32 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $32.90 | $2.11 | — | $9,050.12 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1291.92 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 82 | $15.66 | $2.24 | — | $7,763.76 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1291.92 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $79.42 | $2.04 | — | $6,491.00 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1291.92 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 389 | $3.32 | $5.02 | — | $5,194.50 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.4; leftover $1291.92 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SAFX` | 3539 | $0.36 | $23.53 | — | $3,879.23 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+7.6; leftover $1291.92 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $2,616.03 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1291.92 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 150 | $8.61 | $2.44 | — | $1,322.09 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.7; leftover $1291.92 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `XPOF` | 240 | $5.38 | $3.10 | — | $27.79 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.5; leftover $1291.92 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.79 | ▼ close $10,069.06 vs 09:30 $10,335.32 (session -223.78) | 16:00 close · cash $27.79 · equity $10,069.06 vs 09:30 $10,335.32 (-266.26; session marks -223.78) · 8 name(s) marked open→close (per-name table). SEDG×39 09:30 $32.90 → close $31.41 -58.11; GRRR×82 09:30 $15.66 → close $14.41 -102.50; URBN×16 09:30 $79.42 → close $81.09 +26.72; PYXS×389 09:30 $3.32 → close $3.23 -35.01; SAFX×3539 09:30 $0.36 → close $0.36 -21.23; SIMO×5 09:30 $252.24 → close $245.81 -32.15; OPTX×150 09:30 $8.61 → close $8.52 -13.50; XPOF×240 09:30 $5.38 → close $5.43 +12.00 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.79 | ▼ 09:30 equity $10,041.73 vs yday $10,069.06 (-27.33) | 09:30 open · cash $27.79 (unchanged overnight, no fees) · equity $10,041.73 vs prior close $10,069.06 (-27.33) · 8 name(s) re-marked at the open (per-name table). SEDG×39 yday $31.41 → 09:30 $31.15 -10.14; GRRR×82 yday $14.41 → 09:30 $14.44 +2.46; URBN×16 yday $81.09 → 09:30 $80.44 -10.40; PYXS×389 yday $3.23 → 09:30 $3.20 -11.67; SAFX×3539 yday $0.36 → 09:30 $0.36 +10.62; SIMO×5 yday $245.81 → 09:30 $247.05 +6.20; OPTX×150 yday $8.52 → 09:30 $8.52 +0.00; XPOF×240 yday $5.43 → 09:30 $5.37 -14.40 | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 39 | $31.15 | $2.13 | $-72.48 | $1,240.52 | ▼ -72.48 after sell → book $10,039.60; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 82 | $14.44 | $2.26 | $-104.54 | $2,422.34 | ▼ -104.54 after sell → book $10,037.34; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $80.44 | $2.06 | $+12.22 | $3,707.32 | ▲ +12.22 after sell → book $10,035.29; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PYXS` | 389 | $3.20 | $5.09 | $-56.79 | $4,947.03 | ▼ -56.79 after sell → book $10,030.19; vs 09:30 mark -5.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SAFX` | 3539 | $0.36 | $24.03 | $-58.18 | $6,204.12 | ▼ -58.18 after sell → book $10,006.17; vs 09:30 mark -24.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $7,437.34 | ▼ -29.98 after sell → book $10,004.14; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 150 | $8.52 | $2.48 | $-18.42 | $8,712.87 | ▼ -18.42 after sell → book $10,001.67; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `XPOF` | 240 | $5.37 | $3.15 | $-8.64 | $9,998.52 | ▼ -8.64 after sell → book $9,998.52; vs 09:30 mark -3.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,998.52 | ▲ close $9,998.52 vs 09:30 $10,041.73 (session +0.00) | 16:00 close · cash $9,998.52 · no lots left · equity $9,998.52. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,998.52 | ▲ 09:30 equity $9,998.52 vs yday $9,998.52 (+0.00) | 09:30 open · cash $9,998.52 · no holdings · equity $9,998.52 vs prior close $9,998.52 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,998.52 | ▲ close $9,998.52 vs 09:30 $9,998.52 (session +0.00) | 16:00 close · cash $9,998.52 · no lots left · equity $9,998.52. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,998.52 | ▲ 09:30 equity $9,998.52 vs yday $9,998.52 (+0.00) | 09:30 open · cash $9,998.52 · no holdings · equity $9,998.52 vs prior close $9,998.52 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,998.52 | ▲ close $9,998.52 vs 09:30 $9,998.52 (session +0.00) | 16:00 close · cash $9,998.52 · no lots left · equity $9,998.52. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,998.52 | ▲ 09:30 equity $9,998.52 vs yday $9,998.52 (+0.00) | 09:30 open · cash $9,998.52 · no holdings · equity $9,998.52 vs prior close $9,998.52 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 80 | $15.45 | $2.23 | — | $8,760.29 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1249.82 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $7,590.72 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1249.82 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.77 | $2.21 | — | $6,347.53 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1249.82 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 84 | $14.85 | $2.24 | — | $5,097.88 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1249.82 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $55.42 | $2.06 | — | $3,876.59 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-25.9; leftover $1249.82 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 573 | $2.18 | $7.39 | — | $2,620.06 | — | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1249.82 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 89 | $13.96 | $2.26 | — | $1,375.36 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-6.4; leftover $1249.82 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SAFX` | 3315 | $0.38 | $22.44 | — | $103.16 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-2.3; leftover $1249.82 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $103.16 | ▼ close $9,915.10 vs 09:30 $9,998.52 (session -40.58) | 16:00 close · cash $103.16 · equity $9,915.10 vs 09:30 $9,998.52 (-83.42; session marks -40.58) · 8 name(s) marked open→close (per-name table). CRK×80 09:30 $15.45 → close $14.95 -40.00; MRNA×8 09:30 $145.94 → close $148.87 +23.40; ARCT×74 09:30 $16.77 → close $15.56 -89.54; SLN×84 09:30 $14.85 → close $14.79 -5.04; EIX×22 09:30 $55.42 → close $56.30 +19.36; CRDL×573 09:30 $2.18 → close $2.16 -11.46; CLYM×89 09:30 $13.96 → close $14.59 +56.07; SAFX×3315 09:30 $0.38 → close $0.38 +6.63 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $103.16 | ▲ 09:30 equity $9,923.92 vs yday $9,915.10 (+8.82) | 09:30 open · cash $103.16 (unchanged overnight, no fees) · equity $9,923.92 vs prior close $9,915.10 (+8.82) · 8 name(s) re-marked at the open (per-name table). CRK×80 yday $14.95 → 09:30 $15.00 +4.00; MRNA×8 yday $148.87 → 09:30 $153.62 +38.00; ARCT×74 yday $15.56 → 09:30 $15.61 +3.70; SLN×84 yday $14.79 → 09:30 $14.63 -13.44; EIX×22 yday $56.30 → 09:30 $55.79 -11.22; CRDL×573 yday $2.16 → 09:30 $2.16 +0.00; CLYM×89 yday $14.59 → 09:30 $14.49 -8.90; SAFX×3315 yday $0.38 → 09:30 $0.38 -3.32 | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 80 | $15.00 | $2.25 | $-40.48 | $1,300.91 | ▼ -40.48 after sell → book $9,921.67; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $153.62 | $2.03 | $+57.35 | $2,527.83 | ▲ +57.35 after sell → book $9,919.63; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 74 | $15.61 | $2.23 | $-90.29 | $3,680.74 | ▼ -90.29 after sell → book $9,917.40; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 84 | $14.63 | $2.27 | $-22.99 | $4,907.39 | ▼ -22.99 after sell → book $9,915.13; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 22 | $55.79 | $2.08 | $+4.01 | $6,132.70 | ▲ +4.01 after sell → book $9,913.06; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 573 | $2.16 | $7.50 | $-26.35 | $7,362.88 | ▼ -26.35 after sell → book $9,905.56; vs 09:30 mark -7.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CLYM` | 89 | $14.49 | $2.28 | $+42.63 | $8,650.21 | ▲ +42.63 after sell → book $9,903.28; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SAFX` | 3315 | $0.38 | $23.04 | $-42.16 | $9,880.24 | ▼ -42.16 after sell → book $9,880.24; vs 09:30 mark -23.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 490 | $2.52 | $6.32 | — | $8,639.12 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1235.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 258 | $4.78 | $3.33 | — | $7,402.55 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1235.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 77 | $15.90 | $2.22 | — | $6,176.03 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.1; leftover $1235.03 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 350 | $3.52 | $4.51 | — | $4,939.52 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $1235.03 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $3,909.96 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1235.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 44 | $28.00 | $2.12 | — | $2,675.84 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+8.7; leftover $1235.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 123 | $10.02 | $2.36 | — | $1,441.02 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.2; leftover $1235.03 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `UAMY` | 235 | $5.25 | $3.03 | — | $204.24 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-0.4; leftover $1235.03 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $204.24 | ▼ close $9,830.57 vs 09:30 $9,923.92 (session -23.78) | 16:00 close · cash $204.24 · equity $9,830.57 vs 09:30 $9,923.92 (-93.35; session marks -23.78) · 8 name(s) marked open→close (per-name table). ALEC×490 09:30 $2.52 → close $2.46 -29.40; OABI×258 09:30 $4.78 → close $4.33 -116.10; HQ×77 09:30 $15.90 → close $15.56 -26.18; EOSE×350 09:30 $3.52 → close $3.88 +126.00; DELL×2 09:30 $513.78 → close $524.14 +20.72; MLYS×44 09:30 $28.00 → close $28.21 +9.24; CCOI×123 09:30 $10.02 → close $10.05 +3.69; UAMY×235 09:30 $5.25 → close $5.20 -11.75 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $204.24 | ▼ 09:30 equity $9,806.10 vs yday $9,830.57 (-24.47) | 09:30 open · cash $204.24 (unchanged overnight, no fees) · equity $9,806.10 vs prior close $9,830.57 (-24.47) · 8 name(s) re-marked at the open (per-name table). ALEC×490 yday $2.46 → 09:30 $2.38 -39.20; OABI×258 yday $4.33 → 09:30 $4.30 -7.74; HQ×77 yday $15.56 → 09:30 $15.40 -12.32; EOSE×350 yday $3.88 → 09:30 $3.99 +38.50; DELL×2 yday $524.14 → 09:30 $521.15 -5.98; MLYS×44 yday $28.21 → 09:30 $28.03 -7.92; CCOI×123 yday $10.05 → 09:30 $9.98 -8.61; UAMY×235 yday $5.20 → 09:30 $5.28 +18.80 | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 490 | $2.38 | $6.41 | $-81.33 | $1,364.03 | ▼ -81.33 after sell → book $9,799.69; vs 09:30 mark -6.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 258 | $4.30 | $3.38 | $-130.55 | $2,470.05 | ▼ -130.55 after sell → book $9,796.31; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HQ` | 77 | $15.40 | $2.24 | $-42.96 | $3,653.60 | ▼ -42.96 after sell → book $9,794.06; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `EOSE` | 350 | $3.99 | $4.58 | $+155.40 | $5,045.52 | ▲ +155.40 after sell → book $9,789.48; vs 09:30 mark -4.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $6,085.80 | ▲ +10.73 after sell → book $9,787.46; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MLYS` | 44 | $28.03 | $2.14 | $-2.94 | $7,316.98 | ▼ -2.94 after sell → book $9,785.32; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CCOI` | 123 | $9.98 | $2.39 | $-9.67 | $8,542.13 | ▼ -9.67 after sell → book $9,782.93; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `UAMY` | 235 | $5.28 | $3.08 | $+0.94 | $9,779.85 | ▲ +0.94 after sell → book $9,779.85; vs 09:30 mark -3.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,779.85 | ▲ close $9,779.85 vs 09:30 $9,806.10 (session +0.00) | 16:00 close · cash $9,779.85 · no lots left · equity $9,779.85. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,779.85 | ▲ 09:30 equity $9,779.85 vs yday $9,779.85 (-0.00) | 09:30 open · cash $9,779.85 · no holdings · equity $9,779.85 vs prior close $9,779.85 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,779.85 | ▲ close $9,779.85 vs 09:30 $9,779.85 (session +0.00) | 16:00 close · cash $9,779.85 · no lots left · equity $9,779.85. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,779.85 | ▲ 09:30 equity $9,779.85 vs yday $9,779.85 (-0.00) | 09:30 open · cash $9,779.85 · no holdings · equity $9,779.85 vs prior close $9,779.85 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,779.85 | ▲ close $9,779.85 vs 09:30 $9,779.85 (session +0.00) | 16:00 close · cash $9,779.85 · no lots left · equity $9,779.85. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,779.85 | ▲ 09:30 equity $9,779.85 vs yday $9,779.85 (-0.00) | 09:30 open · cash $9,779.85 · no holdings · equity $9,779.85 vs prior close $9,779.85 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 390 | $3.13 | $5.03 | — | $8,554.12 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+6.2; leftover $1222.48 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 206 | $5.91 | $2.66 | — | $7,334.00 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.9; leftover $1222.48 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 599 | $2.04 | $7.73 | — | $6,104.31 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.8; leftover $1222.48 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 257 | $4.75 | $3.32 | — | $4,880.25 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.4; leftover $1222.48 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 576 | $2.12 | $7.43 | — | $3,651.70 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1222.48 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 51 | $23.63 | $2.14 | — | $2,444.43 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.8; leftover $1222.48 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `QRVO` | 10 | $112.83 | $2.02 | — | $1,314.06 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1222.48 | join🟢 sector🟢 gen🟡 news🔴 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `APPS` | 102 | $11.88 | $2.30 | — | $100.00 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+5.0; leftover $1222.48 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $100.00 | ▲ close $9,813.69 vs 09:30 $9,779.85 (session +66.46) | 16:00 close · cash $100.00 · equity $9,813.69 vs 09:30 $9,779.85 (+33.84; session marks +66.46) · 8 name(s) marked open→close (per-name table). CMRC×390 09:30 $3.13 → close $3.50 +146.25; DBI×206 09:30 $5.91 → close $5.88 -6.18; AMTX×599 09:30 $2.04 → close $2.01 -17.97; CLOV×257 09:30 $4.75 → close $4.82 +17.99; BAK×576 09:30 $2.12 → close $2.08 -23.04; TYRA×51 09:30 $23.63 → close $22.03 -81.60; QRVO×10 09:30 $112.83 → close $116.65 +38.15; APPS×102 09:30 $11.88 → close $11.81 -7.14 | — |

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
| `CMRC` | 390 | 2026-09-11 @ $3.13 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+6.2; leftover $1222.48 |
| `DBI` | 206 | 2026-09-11 @ $5.91 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.9; leftover $1222.48 |
| `AMTX` | 599 | 2026-09-11 @ $2.04 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.8; leftover $1222.48 |
| `CLOV` | 257 | 2026-09-11 @ $4.75 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.4; leftover $1222.48 |
| `BAK` | 576 | 2026-09-11 @ $2.12 | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1222.48 |
| `TYRA` | 51 | 2026-09-11 @ $23.63 | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.8; leftover $1222.48 |
| `QRVO` | 10 | 2026-09-11 @ $112.83 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1222.48 |
| `APPS` | 102 | 2026-09-11 @ $11.88 | baseline list, no extra gate; list probable,yday_gainer; ret5=+5.0; leftover $1222.48 |
