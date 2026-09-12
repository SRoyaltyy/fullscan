# Factor mine action — `probable_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-2.23%** ($9,777) · signal-only (no cash/fees) was -0.32%. Starts YES **3/21**. Fills 146 · skips 78 · realized $-158.55.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,356.96.

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
| 2026-08-20 | `CRCL` | 14 | — | $82.99 | +0.00 | $83.66 | +9.38 | +9.38 | +0.00 | +9.38 |
| 2026-08-21 | `MRVI` | 163 | $8.29 | $8.28 | -1.63 | — | +0.00 | -1.63 | +136.92 | — |
| 2026-08-21 | `DNA` | 162 | $6.96 | $7.09 | +21.06 | — | +0.00 | +21.06 | -58.32 | — |
| 2026-08-21 | `MSTR` | 10 | $112.39 | $119.69 | +73.00 | — | +0.00 | +73.00 | +64.60 | — |
| 2026-08-21 | `EXK` | 112 | $10.97 | $11.34 | +41.44 | — | +0.00 | +41.44 | +63.84 | — |
| 2026-08-21 | `SCZM` | 128 | $9.76 | $10.26 | +64.00 | — | +0.00 | +64.00 | +102.40 | — |
| 2026-08-21 | `NG` | 144 | $8.66 | $9.02 | +51.84 | — | +0.00 | +51.84 | +92.16 | — |
| 2026-08-21 | `BLSH` | 41 | $28.44 | $29.75 | +53.71 | — | +0.00 | +53.71 | +22.55 | — |
| 2026-08-21 | `CRCL` | 14 | $83.66 | $87.98 | +60.48 | — | +0.00 | +60.48 | +69.86 | — |
| 2026-08-21 | `BTBT` | 765 | — | $1.66 | +0.00 | $1.53 | -99.45 | -99.45 | +0.00 | -99.45 |
| 2026-08-21 | `ENHA` | 742 | — | $1.71 | +0.00 | $1.72 | +7.42 | +7.42 | +0.00 | +7.42 |
| 2026-08-21 | `DE` | 2 | — | $623.26 | +0.00 | $647.47 | +48.42 | +48.42 | +0.00 | +48.42 |
| 2026-08-21 | `QDEL` | 84 | — | $14.96 | +0.00 | $14.74 | -18.48 | -18.48 | +0.00 | -18.48 |
| 2026-08-21 | `ORBS` | 1470 | — | $0.86 | +0.00 | $0.88 | +23.52 | +23.52 | +0.00 | +23.52 |
| 2026-08-21 | `GORO` | 408 | — | $3.11 | +0.00 | $3.19 | +32.64 | +32.64 | +0.00 | +32.64 |
| 2026-08-21 | `QTRX` | 408 | — | $3.11 | +0.00 | $2.99 | -48.96 | -48.96 | +0.00 | -48.96 |
| 2026-08-21 | `CF` | 9 | — | $127.43 | +0.00 | $129.60 | +19.53 | +19.53 | +0.00 | +19.53 |
| 2026-08-24 | `BTBT` | 765 | $1.53 | $1.55 | +15.30 | — | +0.00 | +15.30 | -84.15 | — |
| 2026-08-24 | `ENHA` | 742 | $1.72 | $1.74 | +14.84 | — | +0.00 | +14.84 | +22.26 | — |
| 2026-08-24 | `DE` | 2 | $647.47 | $653.04 | +11.14 | — | +0.00 | +11.14 | +59.56 | — |
| 2026-08-24 | `QDEL` | 84 | $14.74 | $14.74 | +0.00 | — | +0.00 | +0.00 | -18.48 | — |
| 2026-08-24 | `ORBS` | 1470 | $0.88 | $0.89 | +14.70 | — | +0.00 | +14.70 | +38.22 | — |
| 2026-08-24 | `GORO` | 408 | $3.19 | $3.20 | +4.08 | — | +0.00 | +4.08 | +36.72 | — |
| 2026-08-24 | `QTRX` | 408 | $2.99 | $2.99 | +0.00 | — | +0.00 | +0.00 | -48.96 | — |
| 2026-08-24 | `CF` | 9 | $129.60 | $129.99 | +3.51 | — | +0.00 | +3.51 | +23.04 | — |
| 2026-08-25 | `CAPR` | 173 | — | $7.25 | +0.00 | $8.29 | +179.92 | +179.92 | +0.00 | +179.92 |
| 2026-08-25 | `SAFX` | 3520 | — | $0.36 | +0.00 | $0.35 | -14.08 | -14.08 | +0.00 | -14.08 |
| 2026-08-25 | `VITL` | 113 | — | $11.12 | +0.00 | $11.11 | -1.13 | -1.13 | +0.00 | -1.13 |
| 2026-08-25 | `KURA` | 92 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CCOI` | 132 | — | $9.49 | +0.00 | $9.88 | +51.48 | +51.48 | +0.00 | +51.48 |
| 2026-08-25 | `LIFE` | 34 | — | $36.96 | +0.00 | $38.56 | +54.40 | +54.40 | +0.00 | +54.40 |
| 2026-08-25 | `ZIP` | 276 | — | $4.55 | +0.00 | $4.35 | -55.20 | -55.20 | +0.00 | -55.20 |
| 2026-08-25 | `ADIG` | 57 | — | $21.79 | +0.00 | $22.27 | +27.36 | +27.36 | +0.00 | +27.36 |
| 2026-08-26 | `CAPR` | 173 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | +179.92 | — |
| 2026-08-26 | `SAFX` | 3520 | $0.35 | $0.35 | -3.52 | — | +0.00 | -3.52 | -17.60 | — |
| 2026-08-26 | `VITL` | 113 | $11.11 | $11.03 | -9.04 | — | +0.00 | -9.04 | -10.17 | — |
| 2026-08-26 | `KURA` | 92 | $13.59 | $13.63 | +3.68 | — | +0.00 | +3.68 | +3.68 | — |
| 2026-08-26 | `CCOI` | 132 | $9.88 | $9.89 | +1.32 | — | +0.00 | +1.32 | +52.80 | — |
| 2026-08-26 | `LIFE` | 34 | $38.56 | $38.24 | -10.88 | — | +0.00 | -10.88 | +43.52 | — |
| 2026-08-26 | `ZIP` | 276 | $4.35 | $4.31 | -11.04 | — | +0.00 | -11.04 | -66.24 | — |
| 2026-08-26 | `ADIG` | 57 | $22.27 | $21.78 | -27.93 | — | +0.00 | -27.93 | -0.57 | — |
| 2026-08-26 | `AVBP` | 40 | — | $31.21 | +0.00 | $31.14 | -2.80 | -2.80 | +0.00 | -2.80 |
| 2026-08-26 | `FLNC` | 114 | — | $11.12 | +0.00 | $11.08 | -4.56 | -4.56 | +0.00 | -4.56 |
| 2026-08-26 | `ABX` | 129 | — | $9.83 | +0.00 | $9.78 | -6.45 | -6.45 | +0.00 | -6.45 |
| 2026-08-26 | `AVEX` | 72 | — | $17.51 | +0.00 | $18.34 | +59.76 | +59.76 | +0.00 | +59.76 |
| 2026-08-26 | `ITG` | 105 | — | $12.04 | +0.00 | $12.45 | +43.05 | +43.05 | +0.00 | +43.05 |
| 2026-08-26 | `SENS` | 134 | — | $9.48 | +0.00 | $9.34 | -18.76 | -18.76 | +0.00 | -18.76 |
| 2026-08-26 | `BE` | 5 | — | $213.94 | +0.00 | $218.21 | +21.35 | +21.35 | +0.00 | +21.35 |
| 2026-08-26 | `AXTI` | 19 | — | $65.34 | +0.00 | $65.18 | -3.04 | -3.04 | +0.00 | -3.04 |
| 2026-08-27 | `AVBP` | 40 | $31.14 | $30.79 | -14.00 | — | +0.00 | -14.00 | -16.80 | — |
| 2026-08-27 | `FLNC` | 114 | $11.08 | $11.52 | +50.16 | — | +0.00 | +50.16 | +45.60 | — |
| 2026-08-27 | `ABX` | 129 | $9.78 | $9.68 | -12.90 | — | +0.00 | -12.90 | -19.35 | — |
| 2026-08-27 | `AVEX` | 72 | $18.34 | $18.43 | +6.48 | — | +0.00 | +6.48 | +66.24 | — |
| 2026-08-27 | `ITG` | 105 | $12.45 | $12.36 | -9.45 | — | +0.00 | -9.45 | +33.60 | — |
| 2026-08-27 | `SENS` | 134 | $9.34 | $9.33 | -1.34 | — | +0.00 | -1.34 | -20.10 | — |
| 2026-08-27 | `BE` | 5 | $218.21 | $227.10 | +44.45 | — | +0.00 | +44.45 | +65.80 | — |
| 2026-08-27 | `AXTI` | 19 | $65.18 | $70.30 | +97.28 | — | +0.00 | +97.28 | +94.24 | — |
| 2026-08-28 | `SEDG` | 39 | — | $32.90 | +0.00 | $31.41 | -58.11 | -58.11 | +0.00 | -58.11 |
| 2026-08-28 | `GRRR` | 83 | — | $15.66 | +0.00 | $14.41 | -103.75 | -103.75 | +0.00 | -103.75 |
| 2026-08-28 | `URBN` | 16 | — | $79.42 | +0.00 | $81.09 | +26.72 | +26.72 | +0.00 | +26.72 |
| 2026-08-28 | `PYXS` | 391 | — | $3.32 | +0.00 | $3.23 | -35.19 | -35.19 | +0.00 | -35.19 |
| 2026-08-28 | `SAFX` | 3561 | — | $0.36 | +0.00 | $0.36 | -21.37 | -21.37 | +0.00 | -21.37 |
| 2026-08-28 | `SIMO` | 5 | — | $252.24 | +0.00 | $245.81 | -32.15 | -32.15 | +0.00 | -32.15 |
| 2026-08-28 | `OPTX` | 150 | — | $8.61 | +0.00 | $8.52 | -13.50 | -13.50 | +0.00 | -13.50 |
| 2026-08-28 | `XPOF` | 241 | — | $5.38 | +0.00 | $5.43 | +12.05 | +12.05 | +0.00 | +12.05 |
| 2026-08-31 | `SEDG` | 39 | $31.41 | $31.15 | -10.14 | — | +0.00 | -10.14 | -68.25 | — |
| 2026-08-31 | `GRRR` | 83 | $14.41 | $14.44 | +2.49 | — | +0.00 | +2.49 | -101.26 | — |
| 2026-08-31 | `URBN` | 16 | $81.09 | $80.44 | -10.40 | — | +0.00 | -10.40 | +16.32 | — |
| 2026-08-31 | `PYXS` | 391 | $3.23 | $3.20 | -11.73 | — | +0.00 | -11.73 | -46.92 | — |
| 2026-08-31 | `SAFX` | 3561 | $0.36 | $0.36 | +10.68 | — | +0.00 | +10.68 | -10.68 | — |
| 2026-08-31 | `SIMO` | 5 | $245.81 | $247.05 | +6.20 | — | +0.00 | +6.20 | -25.95 | — |
| 2026-08-31 | `OPTX` | 150 | $8.52 | $8.52 | +0.00 | — | +0.00 | +0.00 | -13.50 | — |
| 2026-08-31 | `XPOF` | 241 | $5.43 | $5.37 | -14.46 | — | +0.00 | -14.46 | -2.41 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `CRK` | 81 | — | $15.45 | +0.00 | $14.95 | -40.50 | -40.50 | +0.00 | -40.50 |
| 2026-09-03 | `MRNA` | 8 | — | $145.94 | +0.00 | $148.87 | +23.40 | +23.40 | +0.00 | +23.40 |
| 2026-09-03 | `ARCT` | 74 | — | $16.77 | +0.00 | $15.56 | -89.54 | -89.54 | +0.00 | -89.54 |
| 2026-09-03 | `SLN` | 84 | — | $14.85 | +0.00 | $14.79 | -5.04 | -5.04 | +0.00 | -5.04 |
| 2026-09-03 | `EIX` | 22 | — | $55.42 | +0.00 | $56.30 | +19.36 | +19.36 | +0.00 | +19.36 |
| 2026-09-03 | `CRDL` | 576 | — | $2.18 | +0.00 | $2.16 | -11.52 | -11.52 | +0.00 | -11.52 |
| 2026-09-03 | `CLYM` | 90 | — | $13.96 | +0.00 | $14.59 | +56.70 | +56.70 | +0.00 | +56.70 |
| 2026-09-03 | `SAFX` | 3335 | — | $0.38 | +0.00 | $0.38 | +6.67 | +6.67 | +0.00 | +6.67 |
| 2026-09-04 | `CRK` | 81 | $14.95 | $15.00 | +4.05 | — | +0.00 | +4.05 | -36.45 | — |
| 2026-09-04 | `MRNA` | 8 | $148.87 | $153.62 | +38.00 | — | +0.00 | +38.00 | +61.40 | — |
| 2026-09-04 | `ARCT` | 74 | $15.56 | $15.61 | +3.70 | — | +0.00 | +3.70 | -85.84 | — |
| 2026-09-04 | `SLN` | 84 | $14.79 | $14.63 | -13.44 | — | +0.00 | -13.44 | -18.48 | — |
| 2026-09-04 | `EIX` | 22 | $56.30 | $55.79 | -11.22 | — | +0.00 | -11.22 | +8.14 | — |
| 2026-09-04 | `CRDL` | 576 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -11.52 | — |
| 2026-09-04 | `CLYM` | 90 | $14.59 | $14.49 | -9.00 | — | +0.00 | -9.00 | +47.70 | — |
| 2026-09-04 | `SAFX` | 3335 | $0.38 | $0.38 | -3.34 | — | +0.00 | -3.34 | +3.34 | — |
| 2026-09-04 | `ALEC` | 493 | — | $2.52 | +0.00 | $2.46 | -29.58 | -29.58 | +0.00 | -29.58 |
| 2026-09-04 | `OABI` | 260 | — | $4.78 | +0.00 | $4.33 | -117.00 | -117.00 | +0.00 | -117.00 |
| 2026-09-04 | `HQ` | 78 | — | $15.90 | +0.00 | $15.56 | -26.52 | -26.52 | +0.00 | -26.52 |
| 2026-09-04 | `EOSE` | 353 | — | $3.52 | +0.00 | $3.88 | +127.08 | +127.08 | +0.00 | +127.08 |
| 2026-09-04 | `DELL` | 2 | — | $513.78 | +0.00 | $524.14 | +20.72 | +20.72 | +0.00 | +20.72 |
| 2026-09-04 | `MLYS` | 44 | — | $28.00 | +0.00 | $28.21 | +9.24 | +9.24 | +0.00 | +9.24 |
| 2026-09-04 | `CCOI` | 124 | — | $10.02 | +0.00 | $10.05 | +3.72 | +3.72 | +0.00 | +3.72 |
| 2026-09-04 | `UAMY` | 236 | — | $5.25 | +0.00 | $5.20 | -11.80 | -11.80 | +0.00 | -11.80 |
| 2026-09-08 | `ALEC` | 493 | $2.46 | $2.38 | -39.44 | — | +0.00 | -39.44 | -69.02 | — |
| 2026-09-08 | `OABI` | 260 | $4.33 | $4.30 | -7.80 | — | +0.00 | -7.80 | -124.80 | — |
| 2026-09-08 | `HQ` | 78 | $15.56 | $15.40 | -12.48 | — | +0.00 | -12.48 | -39.00 | — |
| 2026-09-08 | `EOSE` | 353 | $3.88 | $3.99 | +38.83 | — | +0.00 | +38.83 | +165.91 | — |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | — | +0.00 | -5.98 | +14.74 | — |
| 2026-09-08 | `MLYS` | 44 | $28.21 | $28.03 | -7.92 | — | +0.00 | -7.92 | +1.32 | — |
| 2026-09-08 | `CCOI` | 124 | $10.05 | $9.98 | -8.68 | — | +0.00 | -8.68 | -4.96 | — |
| 2026-09-08 | `UAMY` | 236 | $5.20 | $5.28 | +18.88 | — | +0.00 | +18.88 | +7.08 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `AMTX` | 603 | — | $2.04 | +0.00 | $2.01 | -18.09 | -18.09 | +0.00 | -18.09 |
| 2026-09-11 | `LDI` | 1447 | — | $0.85 | +0.00 | $0.83 | -21.71 | -21.71 | +0.00 | -21.71 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +9.14 | ANGX, WWW, HYLN, WDC, FOSL, ADUR, AIRS, ALGM | — | $269.08 | $9,985.46 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28 |
| 2026-08-17 | +2.25 | $269.08 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28 | $10,059.20 | +73.74 | -129.60 | CDNL, ABX, FCEL, VERA, CELC, BW, OCC, ALM | ANGX, WWW, HYLN, WDC, FOSL, ADUR, AIRS, ALGM | $79.21 | $9,888.06 | CDNL×31, ABX×137, FCEL×56, VERA×40, CELC×13, BW×121, OCC×68, ALM×77 |
| 2026-08-18 | -6.20 | $79.21 | CDNL×31, ABX×137, FCEL×56, VERA×40, CELC×13, BW×121, OCC×68, ALM×77 | $9,722.67 | -165.39 | +0.00 | — | CDNL, ABX, FCEL, VERA, CELC, BW, OCC, ALM | $9,704.93 | $9,704.93 | — |
| 2026-08-19 | -7.20 | $9,704.93 | — | $9,704.93 | +0.00 | +0.00 | — | — | $9,704.93 | $9,704.93 | — |
| 2026-08-20 | +1.12 | $9,704.93 | — | $9,704.93 | +0.00 | +130.11 | MRVI, DNA, MSTR, EXK, SCZM, NG, BLSH, CRCL | — | $151.87 | $9,816.80 | MRVI×163, DNA×162, MSTR×10, EXK×112, SCZM×128, NG×144, BLSH×41, CRCL×14 |
| 2026-08-21 | +3.25 | $151.87 | MRVI×163, DNA×162, MSTR×10, EXK×112, SCZM×128, NG×144, BLSH×41, CRCL×14 | $10,180.70 | +363.90 | -35.36 | BTBT, ENHA, DE, QDEL, ORBS, GORO, QTRX, CF | MRVI, DNA, MSTR, EXK, SCZM, NG, BLSH, CRCL | $112.31 | $10,073.54 | BTBT×765, ENHA×742, DE×2, QDEL×84, ORBS×1470, GORO×408, QTRX×408, CF×9 |
| 2026-08-24 | -5.17 | $112.31 | BTBT×765, ENHA×742, DE×2, QDEL×84, ORBS×1470, GORO×408, QTRX×408, CF×9 | $10,137.11 | +63.57 | +0.00 | — | BTBT, ENHA, DE, QDEL, ORBS, GORO, QTRX, CF | $10,082.65 | $10,082.65 | — |
| 2026-08-25 | +1.80 | $10,082.65 | — | $10,082.65 | -0.00 | +242.75 | CAPR, SAFX, VITL, KURA, CCOI, LIFE, ZIP, ADIG | — | $13.78 | $10,284.93 | CAPR×173, SAFX×3520, VITL×113, KURA×92, CCOI×132, LIFE×34, ZIP×276, ADIG×57 |
| 2026-08-26 | +2.02 | $13.78 | CAPR×173, SAFX×3520, VITL×113, KURA×92, CCOI×132, LIFE×34, ZIP×276, ADIG×57 | $10,227.52 | -57.41 | +88.55 | AVBP, FLNC, ABX, AVEX, ITG, SENS, BE, AXTI | CAPR, SAFX, VITL, KURA, CCOI, LIFE, ZIP, ADIG | $278.09 | $10,257.19 | AVBP×40, FLNC×114, ABX×129, AVEX×72, ITG×105, SENS×134, BE×5, AXTI×19 |
| 2026-08-27 | — | $278.09 | AVBP×40, FLNC×114, ABX×129, AVEX×72, ITG×105, SENS×134, BE×5, AXTI×19 | $10,417.87 | +160.68 | +0.00 | — | AVBP, FLNC, ABX, AVEX, ITG, SENS, BE, AXTI | $10,399.89 | $10,399.89 | — |
| 2026-08-28 | +0.75 | $10,399.89 | — | $10,399.89 | +0.00 | -225.30 | SEDG, GRRR, URBN, PYXS, SAFX, SIMO, OPTX, XPOF | — | $56.46 | $10,131.93 | SEDG×39, GRRR×83, URBN×16, PYXS×391, SAFX×3561, SIMO×5, OPTX×150, XPOF×241 |
| 2026-08-31 | -5.85 | $56.46 | SEDG×39, GRRR×83, URBN×16, PYXS×391, SAFX×3561, SIMO×5, OPTX×150, XPOF×241 | $10,104.58 | -27.35 | +0.00 | — | SEDG, GRRR, URBN, PYXS, SAFX, SIMO, OPTX, XPOF | $10,061.18 | $10,061.18 | — |
| 2026-09-01 | -6.30 | $10,061.18 | — | $10,061.18 | -0.00 | +0.00 | — | — | $10,061.18 | $10,061.18 | — |
| 2026-09-02 | -3.83 | $10,061.18 | — | $10,061.18 | -0.00 | +0.00 | — | — | $10,061.18 | $10,061.18 | — |
| 2026-09-03 | -0.90 | $10,061.18 | — | $10,061.18 | -0.00 | -40.47 | CRK, MRNA, ARCT, SLN, EIX, CRDL, CLYM, SAFX | — | $122.14 | $9,977.68 | CRK×81, MRNA×8, ARCT×74, SLN×84, EIX×22, CRDL×576, CLYM×90, SAFX×3335 |
| 2026-09-04 | +2.25 | $122.14 | CRK×81, MRNA×8, ARCT×74, SLN×84, EIX×22, CRDL×576, CLYM×90, SAFX×3335 | $9,986.43 | +8.75 | -24.14 | ALEC, OABI, HQ, EOSE, DELL, MLYS, CCOI, UAMY | CRK, MRNA, ARCT, SLN, EIX, CRDL, CLYM, SAFX | $207.60 | $9,892.42 | ALEC×493, OABI×260, HQ×78, EOSE×353, DELL×2, MLYS×44, CCOI×124, UAMY×236 |
| 2026-09-08 | -11.47 | $207.60 | ALEC×493, OABI×260, HQ×78, EOSE×353, DELL×2, MLYS×44, CCOI×124, UAMY×236 | $9,867.83 | -24.59 | +0.00 | — | ALEC, OABI, HQ, EOSE, DELL, MLYS, CCOI, UAMY | $9,841.45 | $9,841.45 | — |
| 2026-09-09 | -13.95 | $9,841.45 | — | $9,841.45 | +0.00 | +0.00 | — | — | $9,841.45 | $9,841.45 | — |
| 2026-09-10 | -13.28 | $9,841.45 | — | $9,841.45 | +0.00 | +0.00 | — | — | $9,841.45 | $9,841.45 | — |
| 2026-09-11 | +0.50 | $9,841.45 | — | $9,841.45 | +0.00 | -39.80 | AMTX, LDI | — | $7,356.96 | $9,777.24 | AMTX×603, LDI×1447 |

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
| 2026-08-20 09:30 ET | **BUY** | `CRCL` | 14 | $82.99 | $2.03 | — | $151.87 | — | baseline list, no extra gate; list probable; 🔵; ⚪; ret5=+7.4; leftover $1213.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.87 | ▲ close $9,816.80 vs 09:30 $9,704.93 (session +130.11) | 16:00 close · cash $151.87 · equity $9,816.80 vs 09:30 $9,704.93 (+111.87; session marks +130.11) · 8 name(s) marked open→close (per-name table). MRVI×163 09:30 $7.44 → close $8.29 +138.55; DNA×162 09:30 $7.45 → close $6.96 -79.38; MSTR×10 09:30 $113.23 → close $112.39 -8.40; EXK×112 09:30 $10.77 → close $10.97 +22.40; SCZM×128 09:30 $9.46 → close $9.76 +38.40; NG×144 09:30 $8.38 → close $8.66 +40.32; BLSH×41 09:30 $29.20 → close $28.44 -31.16; CRCL×14 09:30 $82.99 → close $83.66 +9.38 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.87 | ▲ 09:30 equity $10,180.70 vs yday $9,816.80 (+363.90) | 09:30 open · cash $151.87 (unchanged overnight, no fees) · equity $10,180.70 vs prior close $9,816.80 (+363.90) · 8 name(s) re-marked at the open (per-name table). MRVI×163 yday $8.29 → 09:30 $8.28 -1.63; DNA×162 yday $6.96 → 09:30 $7.09 +21.06; MSTR×10 yday $112.39 → 09:30 $119.69 +73.00; EXK×112 yday $10.97 → 09:30 $11.34 +41.44; SCZM×128 yday $9.76 → 09:30 $10.26 +64.00; NG×144 yday $8.66 → 09:30 $9.02 +51.84; BLSH×41 yday $28.44 → 09:30 $29.75 +53.71; CRCL×14 yday $83.66 → 09:30 $87.98 +60.48 | — |
| 2026-08-21 09:30 ET | **SELL** | `MRVI` | 163 | $8.28 | $2.52 | $+131.92 | $1,498.99 | ▲ +131.92 after sell → book $10,178.18; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 162 | $7.09 | $2.51 | $-63.31 | $2,645.06 | ▼ -63.31 after sell → book $10,175.67; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MSTR` | 10 | $119.69 | $2.04 | $+60.54 | $3,839.92 | ▲ +60.54 after sell → book $10,173.63; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `EXK` | 112 | $11.34 | $2.35 | $+59.16 | $5,107.64 | ▲ +59.16 after sell → book $10,171.27; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 128 | $10.26 | $2.41 | $+97.62 | $6,418.52 | ▲ +97.62 after sell → book $10,168.87; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NG` | 144 | $9.02 | $2.46 | $+87.28 | $7,714.94 | ▲ +87.28 after sell → book $10,166.41; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BLSH` | 41 | $29.75 | $2.13 | $+18.30 | $8,932.56 | ▲ +18.30 after sell → book $10,164.28; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CRCL` | 14 | $87.98 | $2.05 | $+65.78 | $10,162.23 | ▲ +65.78 after sell → book $10,162.23; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 765 | $1.66 | $9.87 | — | $8,882.46 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1270.28 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 742 | $1.71 | $9.57 | — | $7,604.07 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1270.28 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $6,355.55 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1270.28 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 84 | $14.96 | $2.24 | — | $5,096.67 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-1.6; leftover $1270.28 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1470 | $0.86 | $17.11 | — | $3,809.48 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1270.28 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 408 | $3.11 | $5.26 | — | $2,535.34 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+7.1; leftover $1270.28 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QTRX` | 408 | $3.11 | $5.26 | — | $1,261.19 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $1270.28 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 9 | $127.43 | $2.02 | — | $112.31 | — | baseline list, no extra gate; list probable; 🔵; ⚪; ret5=+7.9; leftover $1270.28 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $112.31 | ▼ close $10,073.54 vs 09:30 $10,180.70 (session -35.36) | 16:00 close · cash $112.31 · equity $10,073.54 vs 09:30 $10,180.70 (-107.16; session marks -35.36) · 8 name(s) marked open→close (per-name table). BTBT×765 09:30 $1.66 → close $1.53 -99.45; ENHA×742 09:30 $1.71 → close $1.72 +7.42; DE×2 09:30 $623.26 → close $647.47 +48.42; QDEL×84 09:30 $14.96 → close $14.74 -18.48; ORBS×1470 09:30 $0.86 → close $0.88 +23.52; GORO×408 09:30 $3.11 → close $3.19 +32.64; QTRX×408 09:30 $3.11 → close $2.99 -48.96; CF×9 09:30 $127.43 → close $129.60 +19.53 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $112.31 | ▲ 09:30 equity $10,137.11 vs yday $10,073.54 (+63.57) | 09:30 open · cash $112.31 (unchanged overnight, no fees) · equity $10,137.11 vs prior close $10,073.54 (+63.57) · 8 name(s) re-marked at the open (per-name table). BTBT×765 yday $1.53 → 09:30 $1.55 +15.30; ENHA×742 yday $1.72 → 09:30 $1.74 +14.84; DE×2 yday $647.47 → 09:30 $653.04 +11.14; QDEL×84 yday $14.74 → 09:30 $14.74 +0.00; ORBS×1470 yday $0.88 → 09:30 $0.89 +14.70; GORO×408 yday $3.19 → 09:30 $3.20 +4.08; QTRX×408 yday $2.99 → 09:30 $2.99 +0.00; CF×9 yday $129.60 → 09:30 $129.99 +3.51 | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 765 | $1.55 | $10.01 | $-104.02 | $1,288.05 | ▼ -104.02 after sell → book $10,127.10; vs 09:30 mark -10.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ENHA` | 742 | $1.74 | $9.71 | $+2.98 | $2,569.42 | ▲ +2.98 after sell → book $10,117.39; vs 09:30 mark -9.71 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $3,873.49 | ▲ +55.55 after sell → book $10,115.38; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 84 | $14.74 | $2.27 | $-22.99 | $5,109.38 | ▼ -22.99 after sell → book $10,113.11; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1470 | $0.89 | $17.75 | $+3.36 | $6,399.93 | ▲ +3.36 after sell → book $10,095.36; vs 09:30 mark -17.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GORO` | 408 | $3.20 | $5.34 | $+26.12 | $7,700.19 | ▲ +26.12 after sell → book $10,090.02; vs 09:30 mark -5.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QTRX` | 408 | $2.99 | $5.34 | $-59.56 | $8,914.77 | ▼ -59.56 after sell → book $10,084.68; vs 09:30 mark -5.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 9 | $129.99 | $2.04 | $+18.99 | $10,082.65 | ▲ +18.99 after sell → book $10,082.65; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,082.65 | ▲ close $10,082.65 vs 09:30 $10,137.11 (session +0.00) | 16:00 close · cash $10,082.65 · no lots left · equity $10,082.65. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,082.65 | ▲ 09:30 equity $10,082.65 vs yday $10,082.65 (-0.00) | 09:30 open · cash $10,082.65 · no holdings · equity $10,082.65 vs prior close $10,082.65 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 173 | $7.25 | $2.51 | — | $8,825.89 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1260.33 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3520 | $0.36 | $23.16 | — | $7,542.56 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-15.6; leftover $1260.33 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 113 | $11.12 | $2.33 | — | $6,283.68 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.7; leftover $1260.33 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 92 | $13.59 | $2.27 | — | $5,031.13 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1260.33 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 132 | $9.49 | $2.39 | — | $3,776.06 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1260.33 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 34 | $36.96 | $2.09 | — | $2,517.33 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1260.33 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 276 | $4.55 | $3.56 | — | $1,257.97 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1260.33 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ADIG` | 57 | $21.79 | $2.16 | — | $13.78 | — | baseline list, no extra gate; list probable; 🔵; ret5=+3.1; leftover $1260.33 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.78 | ▲ close $10,284.93 vs 09:30 $10,082.65 (session +242.75) | 16:00 close · cash $13.78 · equity $10,284.93 vs 09:30 $10,082.65 (+202.28; session marks +242.75) · 8 name(s) marked open→close (per-name table). CAPR×173 09:30 $7.25 → close $8.29 +179.92; SAFX×3520 09:30 $0.36 → close $0.35 -14.08; VITL×113 09:30 $11.12 → close $11.11 -1.13; KURA×92 09:30 $13.59 → close $13.59 +0.00; CCOI×132 09:30 $9.49 → close $9.88 +51.48; LIFE×34 09:30 $36.96 → close $38.56 +54.40; ZIP×276 09:30 $4.55 → close $4.35 -55.20; ADIG×57 09:30 $21.79 → close $22.27 +27.36 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.78 | ▼ 09:30 equity $10,227.52 vs yday $10,284.93 (-57.41) | 09:30 open · cash $13.78 (unchanged overnight, no fees) · equity $10,227.52 vs prior close $10,284.93 (-57.41) · 8 name(s) re-marked at the open (per-name table). CAPR×173 yday $8.29 → 09:30 $8.29 +0.00; SAFX×3520 yday $0.35 → 09:30 $0.35 -3.52; VITL×113 yday $11.11 → 09:30 $11.03 -9.04; KURA×92 yday $13.59 → 09:30 $13.63 +3.68; CCOI×132 yday $9.88 → 09:30 $9.89 +1.32; LIFE×34 yday $38.56 → 09:30 $38.24 -10.88; ZIP×276 yday $4.35 → 09:30 $4.31 -11.04; ADIG×57 yday $22.27 → 09:30 $21.78 -27.93 | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 173 | $8.29 | $2.55 | $+174.86 | $1,445.40 | ▲ +174.86 after sell → book $10,224.97; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 3520 | $0.35 | $23.58 | $-64.34 | $2,664.38 | ▼ -64.34 after sell → book $10,201.39; vs 09:30 mark -23.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `VITL` | 113 | $11.03 | $2.36 | $-14.86 | $3,908.41 | ▼ -14.86 after sell → book $10,199.03; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 92 | $13.63 | $2.29 | $-0.88 | $5,160.08 | ▼ -0.88 after sell → book $10,196.74; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 132 | $9.89 | $2.42 | $+48.00 | $6,463.14 | ▲ +48.00 after sell → book $10,194.32; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 34 | $38.24 | $2.11 | $+39.32 | $7,761.19 | ▲ +39.32 after sell → book $10,192.21; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 276 | $4.31 | $3.62 | $-73.42 | $8,947.14 | ▼ -73.42 after sell → book $10,188.60; vs 09:30 mark -3.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ADIG` | 57 | $21.78 | $2.18 | $-4.91 | $10,186.41 | ▼ -4.91 after sell → book $10,186.41; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 40 | $31.21 | $2.11 | — | $8,935.90 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1273.30 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 114 | $11.12 | $2.33 | — | $7,665.89 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1273.30 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 129 | $9.83 | $2.38 | — | $6,395.45 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1273.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AVEX` | 72 | $17.51 | $2.21 | — | $5,132.52 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $1273.30 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ITG` | 105 | $12.04 | $2.31 | — | $3,866.01 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-5.1; leftover $1273.30 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SENS` | 134 | $9.48 | $2.39 | — | $2,593.30 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $1273.30 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 5 | $213.94 | $2.00 | — | $1,521.60 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1273.30 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AXTI` | 19 | $65.34 | $2.05 | — | $278.09 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1273.30 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $278.09 | ▲ close $10,257.19 vs 09:30 $10,227.52 (session +88.55) | 16:00 close · cash $278.09 · equity $10,257.19 vs 09:30 $10,227.52 (+29.67; session marks +88.55) · 8 name(s) marked open→close (per-name table). AVBP×40 09:30 $31.21 → close $31.14 -2.80; FLNC×114 09:30 $11.12 → close $11.08 -4.56; ABX×129 09:30 $9.83 → close $9.78 -6.45; AVEX×72 09:30 $17.51 → close $18.34 +59.76; ITG×105 09:30 $12.04 → close $12.45 +43.05; SENS×134 09:30 $9.48 → close $9.34 -18.76; BE×5 09:30 $213.94 → close $218.21 +21.35; AXTI×19 09:30 $65.34 → close $65.18 -3.04 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $278.09 | ▲ 09:30 equity $10,417.87 vs yday $10,257.19 (+160.68) | 09:30 open · cash $278.09 (unchanged overnight, no fees) · equity $10,417.87 vs prior close $10,257.19 (+160.68) · 8 name(s) re-marked at the open (per-name table). AVBP×40 yday $31.14 → 09:30 $30.79 -14.00; FLNC×114 yday $11.08 → 09:30 $11.52 +50.16; ABX×129 yday $9.78 → 09:30 $9.68 -12.90; AVEX×72 yday $18.34 → 09:30 $18.43 +6.48; ITG×105 yday $12.45 → 09:30 $12.36 -9.45; SENS×134 yday $9.34 → 09:30 $9.33 -1.34; BE×5 yday $218.21 → 09:30 $227.10 +44.45; AXTI×19 yday $65.18 → 09:30 $70.30 +97.28 | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 40 | $30.79 | $2.13 | $-21.04 | $1,507.56 | ▼ -21.04 after sell → book $10,415.74; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 114 | $11.52 | $2.36 | $+40.91 | $2,818.48 | ▲ +40.91 after sell → book $10,413.38; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ABX` | 129 | $9.68 | $2.41 | $-24.14 | $4,064.79 | ▼ -24.14 after sell → book $10,410.97; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVEX` | 72 | $18.43 | $2.23 | $+61.81 | $5,389.52 | ▲ +61.81 after sell → book $10,408.74; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ITG` | 105 | $12.36 | $2.33 | $+28.96 | $6,684.99 | ▲ +28.96 after sell → book $10,406.41; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SENS` | 134 | $9.33 | $2.42 | $-24.92 | $7,932.78 | ▼ -24.92 after sell → book $10,403.98; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BE` | 5 | $227.10 | $2.02 | $+61.77 | $9,066.26 | ▲ +61.77 after sell → book $10,401.96; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AXTI` | 19 | $70.30 | $2.07 | $+90.13 | $10,399.89 | ▲ +90.13 after sell → book $10,399.89; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,399.89 | ▲ close $10,399.89 vs 09:30 $10,417.87 (session +0.00) | 16:00 close · cash $10,399.89 · no lots left · equity $10,399.89. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,399.89 | ▲ 09:30 equity $10,399.89 vs yday $10,399.89 (+0.00) | 09:30 open · cash $10,399.89 · no holdings · equity $10,399.89 vs prior close $10,399.89 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $32.90 | $2.11 | — | $9,114.69 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1299.99 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 83 | $15.66 | $2.24 | — | $7,812.67 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1299.99 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $79.42 | $2.04 | — | $6,539.91 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1299.99 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 391 | $3.32 | $5.04 | — | $5,236.74 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.4; leftover $1299.99 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SAFX` | 3561 | $0.36 | $23.68 | — | $3,913.30 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+7.6; leftover $1299.99 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $2,650.09 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1299.99 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 150 | $8.61 | $2.44 | — | $1,356.15 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.7; leftover $1299.99 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `XPOF` | 241 | $5.38 | $3.11 | — | $56.46 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.5; leftover $1299.99 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.46 | ▼ close $10,131.93 vs 09:30 $10,399.89 (session -225.30) | 16:00 close · cash $56.46 · equity $10,131.93 vs 09:30 $10,399.89 (-267.96; session marks -225.30) · 8 name(s) marked open→close (per-name table). SEDG×39 09:30 $32.90 → close $31.41 -58.11; GRRR×83 09:30 $15.66 → close $14.41 -103.75; URBN×16 09:30 $79.42 → close $81.09 +26.72; PYXS×391 09:30 $3.32 → close $3.23 -35.19; SAFX×3561 09:30 $0.36 → close $0.36 -21.37; SIMO×5 09:30 $252.24 → close $245.81 -32.15; OPTX×150 09:30 $8.61 → close $8.52 -13.50; XPOF×241 09:30 $5.38 → close $5.43 +12.05 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.46 | ▼ 09:30 equity $10,104.58 vs yday $10,131.93 (-27.35) | 09:30 open · cash $56.46 (unchanged overnight, no fees) · equity $10,104.58 vs prior close $10,131.93 (-27.35) · 8 name(s) re-marked at the open (per-name table). SEDG×39 yday $31.41 → 09:30 $31.15 -10.14; GRRR×83 yday $14.41 → 09:30 $14.44 +2.49; URBN×16 yday $81.09 → 09:30 $80.44 -10.40; PYXS×391 yday $3.23 → 09:30 $3.20 -11.73; SAFX×3561 yday $0.36 → 09:30 $0.36 +10.68; SIMO×5 yday $245.81 → 09:30 $247.05 +6.20; OPTX×150 yday $8.52 → 09:30 $8.52 +0.00; XPOF×241 yday $5.43 → 09:30 $5.37 -14.46 | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 39 | $31.15 | $2.13 | $-72.48 | $1,269.19 | ▼ -72.48 after sell → book $10,102.45; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 83 | $14.44 | $2.26 | $-105.76 | $2,465.44 | ▼ -105.76 after sell → book $10,100.19; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $80.44 | $2.06 | $+12.22 | $3,750.43 | ▲ +12.22 after sell → book $10,098.13; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PYXS` | 391 | $3.20 | $5.12 | $-57.08 | $4,996.51 | ▼ -57.08 after sell → book $10,093.01; vs 09:30 mark -5.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SAFX` | 3561 | $0.36 | $24.18 | $-58.54 | $6,261.41 | ▼ -58.54 after sell → book $10,068.83; vs 09:30 mark -24.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $7,494.64 | ▼ -29.98 after sell → book $10,066.81; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 150 | $8.52 | $2.48 | $-18.42 | $8,770.16 | ▼ -18.42 after sell → book $10,064.33; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `XPOF` | 241 | $5.37 | $3.16 | $-8.68 | $10,061.18 | ▼ -8.68 after sell → book $10,061.18; vs 09:30 mark -3.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,061.18 | ▲ close $10,061.18 vs 09:30 $10,104.58 (session +0.00) | 16:00 close · cash $10,061.18 · no lots left · equity $10,061.18. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,061.18 | ▲ 09:30 equity $10,061.18 vs yday $10,061.18 (-0.00) | 09:30 open · cash $10,061.18 · no holdings · equity $10,061.18 vs prior close $10,061.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,061.18 | ▲ close $10,061.18 vs 09:30 $10,061.18 (session +0.00) | 16:00 close · cash $10,061.18 · no lots left · equity $10,061.18. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,061.18 | ▲ 09:30 equity $10,061.18 vs yday $10,061.18 (-0.00) | 09:30 open · cash $10,061.18 · no holdings · equity $10,061.18 vs prior close $10,061.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,061.18 | ▲ close $10,061.18 vs 09:30 $10,061.18 (session +0.00) | 16:00 close · cash $10,061.18 · no lots left · equity $10,061.18. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,061.18 | ▲ 09:30 equity $10,061.18 vs yday $10,061.18 (-0.00) | 09:30 open · cash $10,061.18 · no holdings · equity $10,061.18 vs prior close $10,061.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 81 | $15.45 | $2.23 | — | $8,807.49 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1257.65 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $7,637.92 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1257.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.77 | $2.21 | — | $6,394.73 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1257.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 84 | $14.85 | $2.24 | — | $5,145.08 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1257.65 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $55.42 | $2.06 | — | $3,923.79 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-25.9; leftover $1257.65 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 576 | $2.18 | $7.43 | — | $2,660.68 | — | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1257.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 90 | $13.96 | $2.26 | — | $1,402.02 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-6.4; leftover $1257.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SAFX` | 3335 | $0.38 | $22.58 | — | $122.14 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-2.3; leftover $1257.65 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.14 | ▼ close $9,977.68 vs 09:30 $10,061.18 (session -40.47) | 16:00 close · cash $122.14 · equity $9,977.68 vs 09:30 $10,061.18 (-83.50; session marks -40.47) · 8 name(s) marked open→close (per-name table). CRK×81 09:30 $15.45 → close $14.95 -40.50; MRNA×8 09:30 $145.94 → close $148.87 +23.40; ARCT×74 09:30 $16.77 → close $15.56 -89.54; SLN×84 09:30 $14.85 → close $14.79 -5.04; EIX×22 09:30 $55.42 → close $56.30 +19.36; CRDL×576 09:30 $2.18 → close $2.16 -11.52; CLYM×90 09:30 $13.96 → close $14.59 +56.70; SAFX×3335 09:30 $0.38 → close $0.38 +6.67 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.14 | ▲ 09:30 equity $9,986.43 vs yday $9,977.68 (+8.75) | 09:30 open · cash $122.14 (unchanged overnight, no fees) · equity $9,986.43 vs prior close $9,977.68 (+8.75) · 8 name(s) re-marked at the open (per-name table). CRK×81 yday $14.95 → 09:30 $15.00 +4.05; MRNA×8 yday $148.87 → 09:30 $153.62 +38.00; ARCT×74 yday $15.56 → 09:30 $15.61 +3.70; SLN×84 yday $14.79 → 09:30 $14.63 -13.44; EIX×22 yday $56.30 → 09:30 $55.79 -11.22; CRDL×576 yday $2.16 → 09:30 $2.16 +0.00; CLYM×90 yday $14.59 → 09:30 $14.49 -9.00; SAFX×3335 yday $0.38 → 09:30 $0.38 -3.34 | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 81 | $15.00 | $2.26 | $-40.94 | $1,334.89 | ▼ -40.94 after sell → book $9,984.18; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $153.62 | $2.03 | $+57.35 | $2,561.81 | ▲ +57.35 after sell → book $9,982.14; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 74 | $15.61 | $2.23 | $-90.29 | $3,714.72 | ▼ -90.29 after sell → book $9,979.91; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 84 | $14.63 | $2.27 | $-22.99 | $4,941.37 | ▼ -22.99 after sell → book $9,977.64; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 22 | $55.79 | $2.08 | $+4.01 | $6,166.68 | ▲ +4.01 after sell → book $9,975.57; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 576 | $2.16 | $7.54 | $-26.49 | $7,403.30 | ▼ -26.49 after sell → book $9,968.03; vs 09:30 mark -7.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CLYM` | 90 | $14.49 | $2.29 | $+43.15 | $8,705.12 | ▲ +43.15 after sell → book $9,965.75; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SAFX` | 3335 | $0.38 | $23.18 | $-42.42 | $9,942.57 | ▼ -42.42 after sell → book $9,942.57; vs 09:30 mark -23.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 493 | $2.52 | $6.36 | — | $8,693.85 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1242.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 260 | $4.78 | $3.35 | — | $7,447.70 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1242.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 78 | $15.90 | $2.22 | — | $6,205.27 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.1; leftover $1242.82 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 353 | $3.52 | $4.55 | — | $4,958.16 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $1242.82 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $3,928.60 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1242.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 44 | $28.00 | $2.12 | — | $2,694.48 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+8.7; leftover $1242.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 124 | $10.02 | $2.36 | — | $1,449.64 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.2; leftover $1242.82 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `UAMY` | 236 | $5.25 | $3.04 | — | $207.60 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-0.4; leftover $1242.82 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $207.60 | ▼ close $9,892.42 vs 09:30 $9,986.43 (session -24.14) | 16:00 close · cash $207.60 · equity $9,892.42 vs 09:30 $9,986.43 (-94.01; session marks -24.14) · 8 name(s) marked open→close (per-name table). ALEC×493 09:30 $2.52 → close $2.46 -29.58; OABI×260 09:30 $4.78 → close $4.33 -117.00; HQ×78 09:30 $15.90 → close $15.56 -26.52; EOSE×353 09:30 $3.52 → close $3.88 +127.08; DELL×2 09:30 $513.78 → close $524.14 +20.72; MLYS×44 09:30 $28.00 → close $28.21 +9.24; CCOI×124 09:30 $10.02 → close $10.05 +3.72; UAMY×236 09:30 $5.25 → close $5.20 -11.80 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $207.60 | ▼ 09:30 equity $9,867.83 vs yday $9,892.42 (-24.59) | 09:30 open · cash $207.60 (unchanged overnight, no fees) · equity $9,867.83 vs prior close $9,892.42 (-24.59) · 8 name(s) re-marked at the open (per-name table). ALEC×493 yday $2.46 → 09:30 $2.38 -39.44; OABI×260 yday $4.33 → 09:30 $4.30 -7.80; HQ×78 yday $15.56 → 09:30 $15.40 -12.48; EOSE×353 yday $3.88 → 09:30 $3.99 +38.83; DELL×2 yday $524.14 → 09:30 $521.15 -5.98; MLYS×44 yday $28.21 → 09:30 $28.03 -7.92; CCOI×124 yday $10.05 → 09:30 $9.98 -8.68; UAMY×236 yday $5.20 → 09:30 $5.28 +18.88 | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 493 | $2.38 | $6.45 | $-81.83 | $1,374.48 | ▼ -81.83 after sell → book $9,861.37; vs 09:30 mark -6.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 260 | $4.30 | $3.41 | $-131.56 | $2,489.08 | ▼ -131.56 after sell → book $9,857.97; vs 09:30 mark -3.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HQ` | 78 | $15.40 | $2.25 | $-43.47 | $3,688.03 | ▼ -43.47 after sell → book $9,855.72; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `EOSE` | 353 | $3.99 | $4.62 | $+156.73 | $5,091.88 | ▲ +156.73 after sell → book $9,851.10; vs 09:30 mark -4.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $6,132.16 | ▲ +10.73 after sell → book $9,849.08; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MLYS` | 44 | $28.03 | $2.14 | $-2.94 | $7,363.34 | ▼ -2.94 after sell → book $9,846.94; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CCOI` | 124 | $9.98 | $2.39 | $-9.71 | $8,598.47 | ▼ -9.71 after sell → book $9,844.55; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `UAMY` | 236 | $5.28 | $3.09 | $+0.94 | $9,841.45 | ▲ +0.94 after sell → book $9,841.45; vs 09:30 mark -3.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,841.45 | ▲ close $9,841.45 vs 09:30 $9,867.83 (session +0.00) | 16:00 close · cash $9,841.45 · no lots left · equity $9,841.45. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,841.45 | ▲ 09:30 equity $9,841.45 vs yday $9,841.45 (+0.00) | 09:30 open · cash $9,841.45 · no holdings · equity $9,841.45 vs prior close $9,841.45 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,841.45 | ▲ close $9,841.45 vs 09:30 $9,841.45 (session +0.00) | 16:00 close · cash $9,841.45 · no lots left · equity $9,841.45. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,841.45 | ▲ 09:30 equity $9,841.45 vs yday $9,841.45 (+0.00) | 09:30 open · cash $9,841.45 · no holdings · equity $9,841.45 vs prior close $9,841.45 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,841.45 | ▲ close $9,841.45 vs 09:30 $9,841.45 (session +0.00) | 16:00 close · cash $9,841.45 · no lots left · equity $9,841.45. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,841.45 | ▲ 09:30 equity $9,841.45 vs yday $9,841.45 (+0.00) | 09:30 open · cash $9,841.45 · no holdings · equity $9,841.45 vs prior close $9,841.45 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 603 | $2.04 | $7.78 | — | $8,603.55 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.9; leftover $1230.18 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `LDI` | 1447 | $0.85 | $16.64 | — | $7,356.96 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-12.5; leftover $1230.18 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,356.96 | ▼ close $9,777.24 vs 09:30 $9,841.45 (session -39.80) | 16:00 close · cash $7,356.96 · equity $9,777.24 vs 09:30 $9,841.45 (-64.21; session marks -39.80) · 2 name(s) marked open→close (per-name table). AMTX×603 09:30 $2.04 → close $2.01 -18.09; LDI×1447 09:30 $0.85 → close $0.83 -21.71 | — |

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
| 2026-09-11 | `CMRC` | no_price | no 09:30 open |
| 2026-09-11 | `DBI` | no_price | no 09:30 open |
| 2026-09-11 | `CLOV` | no_price | no 09:30 open |
| 2026-09-11 | `TYRA` | no_price | no 09:30 open |
| 2026-09-11 | `QRVO` | no_price | no 09:30 open |
| 2026-09-11 | `APPS` | no_price | no 09:30 open |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `AMTX` | 603 | 2026-09-11 @ $2.04 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.9; leftover $1230.18 |
| `LDI` | 1447 | 2026-09-11 @ $0.85 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-12.5; leftover $1230.18 |
