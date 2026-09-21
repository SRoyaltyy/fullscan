# Factor mine action — `probable_probable_ok_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-4.33%** ($9,567) · signal-only (no cash/fees) was +2.98%. Starts YES **8/27**. Fills 147 · skips 41 · realized $-209.47.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at yesterday's 'likely to keep moving' list and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: yesterday's 'likely to keep moving' list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the last finished bar was green (closed up).
- Must-have: prior 5-session return is at most 10% (not already exploded).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

### When it buys

- At 09:30, take names on yesterday's 'likely to keep moving' list that pass the must-haves.
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

- **Universe** `probable` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True,ret_5_max=10.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $75.99.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ANGX` | 464 | — | $4.31 | +0.00 | $4.37 | +27.84 | +27.84 | +0.00 | +27.84 |
| 2026-08-14 | `HYLN` | 478 | — | $4.18 | +0.00 | $4.06 | -57.36 | -57.36 | +0.00 | -57.36 |
| 2026-08-14 | `WDC` | 3 | — | $503.50 | +0.00 | $508.80 | +15.90 | +15.90 | +0.00 | +15.90 |
| 2026-08-14 | `ADUR` | 121 | — | $16.50 | +0.00 | $16.17 | -39.93 | -39.93 | +0.00 | -39.93 |
| 2026-08-14 | `ALGM` | 45 | — | $44.06 | +0.00 | $44.39 | +14.85 | +14.85 | +0.00 | +14.85 |
| 2026-08-17 | `ANGX` | 464 | $4.37 | $4.60 | +106.72 | — | +0.00 | +106.72 | +134.56 | — |
| 2026-08-17 | `HYLN` | 478 | $4.06 | $4.10 | +19.12 | — | +0.00 | +19.12 | -38.24 | — |
| 2026-08-17 | `WDC` | 3 | $508.80 | $525.53 | +50.19 | — | +0.00 | +50.19 | +66.09 | — |
| 2026-08-17 | `ADUR` | 121 | $16.17 | $15.73 | -53.24 | — | +0.00 | -53.24 | -93.17 | — |
| 2026-08-17 | `ALGM` | 45 | $44.39 | $45.32 | +41.85 | — | +0.00 | +41.85 | +56.70 | — |
| 2026-08-17 | `CDNL` | 42 | — | $39.85 | +0.00 | $39.23 | -26.04 | -26.04 | +0.00 | -26.04 |
| 2026-08-17 | `ABX` | 184 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `VERA` | 53 | — | $31.30 | +0.00 | $31.63 | +17.49 | +17.49 | +0.00 | +17.49 |
| 2026-08-17 | `CELC` | 18 | — | $92.99 | +0.00 | $92.44 | -9.90 | -9.90 | +0.00 | -9.90 |
| 2026-08-17 | `OCC` | 92 | — | $18.24 | +0.00 | $17.12 | -103.04 | -103.04 | +0.00 | -103.04 |
| 2026-08-17 | `ALM` | 103 | — | $16.20 | +0.00 | $16.36 | +16.48 | +16.48 | +0.00 | +16.48 |
| 2026-08-18 | `CDNL` | 42 | $39.23 | $41.57 | +98.28 | — | +0.00 | +98.28 | +72.24 | — |
| 2026-08-18 | `ABX` | 184 | $9.12 | $9.03 | -16.56 | — | +0.00 | -16.56 | -16.56 | — |
| 2026-08-18 | `VERA` | 53 | $31.63 | $31.31 | -16.96 | — | +0.00 | -16.96 | +0.53 | — |
| 2026-08-18 | `CELC` | 18 | $92.44 | $92.38 | -1.08 | — | +0.00 | -1.08 | -10.98 | — |
| 2026-08-18 | `OCC` | 92 | $17.12 | $16.20 | -84.64 | — | +0.00 | -84.64 | -187.68 | — |
| 2026-08-18 | `ALM` | 103 | $16.36 | $15.78 | -59.74 | — | +0.00 | -59.74 | -43.26 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `DNA` | 189 | — | $7.45 | +0.00 | $6.96 | -92.61 | -92.61 | +0.00 | -92.61 |
| 2026-08-20 | `MSTR` | 12 | — | $113.23 | +0.00 | $112.39 | -10.08 | -10.08 | +0.00 | -10.08 |
| 2026-08-20 | `EXK` | 130 | — | $10.77 | +0.00 | $10.97 | +26.00 | +26.00 | +0.00 | +26.00 |
| 2026-08-20 | `SCZM` | 149 | — | $9.46 | +0.00 | $9.76 | +44.70 | +44.70 | +0.00 | +44.70 |
| 2026-08-20 | `NG` | 168 | — | $8.38 | +0.00 | $8.66 | +47.04 | +47.04 | +0.00 | +47.04 |
| 2026-08-20 | `BLSH` | 48 | — | $29.20 | +0.00 | $28.44 | -36.48 | -36.48 | +0.00 | -36.48 |
| 2026-08-20 | `HYMC` | 51 | — | $27.25 | +0.00 | $26.14 | -56.61 | -56.61 | +0.00 | -56.61 |
| 2026-08-21 | `DNA` | 189 | $6.96 | $7.09 | +24.57 | — | +0.00 | +24.57 | -68.04 | — |
| 2026-08-21 | `MSTR` | 12 | $112.39 | $119.69 | +87.60 | — | +0.00 | +87.60 | +77.52 | — |
| 2026-08-21 | `EXK` | 130 | $10.97 | $11.34 | +48.10 | — | +0.00 | +48.10 | +74.10 | — |
| 2026-08-21 | `SCZM` | 149 | $9.76 | $10.26 | +74.50 | — | +0.00 | +74.50 | +119.20 | — |
| 2026-08-21 | `NG` | 168 | $8.66 | $9.02 | +60.48 | — | +0.00 | +60.48 | +107.52 | — |
| 2026-08-21 | `BLSH` | 48 | $28.44 | $29.75 | +62.88 | — | +0.00 | +62.88 | +26.40 | — |
| 2026-08-21 | `HYMC` | 51 | $26.14 | $27.40 | +64.26 | — | +0.00 | +64.26 | +7.65 | — |
| 2026-08-21 | `BTBT` | 1022 | — | $1.66 | +0.00 | $1.53 | -132.86 | -132.86 | +0.00 | -132.86 |
| 2026-08-21 | `DE` | 2 | — | $623.26 | +0.00 | $647.47 | +48.42 | +48.42 | +0.00 | +48.42 |
| 2026-08-21 | `QDEL` | 113 | — | $14.96 | +0.00 | $14.74 | -24.86 | -24.86 | +0.00 | -24.86 |
| 2026-08-21 | `ORBS` | 1965 | — | $0.86 | +0.00 | $0.88 | +31.44 | +31.44 | +0.00 | +31.44 |
| 2026-08-21 | `GORO` | 545 | — | $3.11 | +0.00 | $3.19 | +43.60 | +43.60 | +0.00 | +43.60 |
| 2026-08-21 | `CF` | 13 | — | $127.43 | +0.00 | $129.60 | +28.21 | +28.21 | +0.00 | +28.21 |
| 2026-08-24 | `BTBT` | 1022 | $1.53 | $1.55 | +20.44 | — | +0.00 | +20.44 | -112.42 | — |
| 2026-08-24 | `DE` | 2 | $647.47 | $653.04 | +11.14 | — | +0.00 | +11.14 | +59.56 | — |
| 2026-08-24 | `QDEL` | 113 | $14.74 | $14.74 | +0.00 | — | +0.00 | +0.00 | -24.86 | — |
| 2026-08-24 | `ORBS` | 1965 | $0.88 | $0.89 | +19.65 | — | +0.00 | +19.65 | +51.09 | — |
| 2026-08-24 | `GORO` | 545 | $3.19 | $3.20 | +5.45 | — | +0.00 | +5.45 | +49.05 | — |
| 2026-08-24 | `CF` | 13 | $129.60 | $129.99 | +5.07 | — | +0.00 | +5.07 | +33.28 | — |
| 2026-08-25 | `SAFX` | 4047 | — | $0.36 | +0.00 | $0.35 | -16.19 | -16.19 | +0.00 | -16.19 |
| 2026-08-25 | `VITL` | 130 | — | $11.12 | +0.00 | $11.11 | -1.30 | -1.30 | +0.00 | -1.30 |
| 2026-08-25 | `KURA` | 106 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CCOI` | 152 | — | $9.49 | +0.00 | $9.88 | +59.28 | +59.28 | +0.00 | +59.28 |
| 2026-08-25 | `LIFE` | 39 | — | $36.96 | +0.00 | $38.56 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-08-25 | `ZIP` | 318 | — | $4.55 | +0.00 | $4.35 | -63.60 | -63.60 | +0.00 | -63.60 |
| 2026-08-25 | `ADIG` | 65 | — | $21.79 | +0.00 | $22.27 | +31.20 | +31.20 | +0.00 | +31.20 |
| 2026-08-26 | `SAFX` | 4047 | $0.35 | $0.35 | -4.05 | — | +0.00 | -4.05 | -20.24 | — |
| 2026-08-26 | `VITL` | 130 | $11.11 | $11.03 | -10.40 | — | +0.00 | -10.40 | -11.70 | — |
| 2026-08-26 | `KURA` | 106 | $13.59 | $13.63 | +4.24 | — | +0.00 | +4.24 | +4.24 | — |
| 2026-08-26 | `CCOI` | 152 | $9.88 | $9.89 | +1.52 | — | +0.00 | +1.52 | +60.80 | — |
| 2026-08-26 | `LIFE` | 39 | $38.56 | $38.24 | -12.48 | — | +0.00 | -12.48 | +49.92 | — |
| 2026-08-26 | `ZIP` | 318 | $4.35 | $4.31 | -12.72 | — | +0.00 | -12.72 | -76.32 | — |
| 2026-08-26 | `ADIG` | 65 | $22.27 | $21.78 | -31.85 | — | +0.00 | -31.85 | -0.65 | — |
| 2026-08-26 | `AVBP` | 80 | — | $31.21 | +0.00 | $31.14 | -5.60 | -5.60 | +0.00 | -5.60 |
| 2026-08-26 | `ABX` | 255 | — | $9.83 | +0.00 | $9.78 | -12.75 | -12.75 | +0.00 | -12.75 |
| 2026-08-26 | `ITG` | 208 | — | $12.04 | +0.00 | $12.45 | +85.28 | +85.28 | +0.00 | +85.28 |
| 2026-08-26 | `SENS` | 265 | — | $9.48 | +0.00 | $9.34 | -37.10 | -37.10 | +0.00 | -37.10 |
| 2026-08-27 | `AVBP` | 80 | $31.14 | $30.79 | -28.00 | — | +0.00 | -28.00 | -33.60 | — |
| 2026-08-27 | `ABX` | 255 | $9.78 | $9.68 | -25.50 | — | +0.00 | -25.50 | -38.25 | — |
| 2026-08-27 | `ITG` | 208 | $12.45 | $12.36 | -18.72 | $12.87 | +106.08 | +87.36 | +66.56 | +172.64 |
| 2026-08-27 | `SENS` | 265 | $9.34 | $9.33 | -2.65 | — | +0.00 | -2.65 | -39.75 | — |
| 2026-08-27 | `BE` | 32 | — | $227.10 | +0.00 | $217.83 | -296.64 | -296.64 | +0.00 | -296.64 |
| 2026-08-28 | `ITG` | 208 | $12.87 | $12.79 | -16.64 | — | +0.00 | -16.64 | +156.00 | — |
| 2026-08-28 | `BE` | 32 | $217.83 | $215.71 | -68.00 | — | +0.00 | -68.00 | -364.64 | — |
| 2026-08-28 | `OPTX` | 1126 | — | $8.61 | +0.00 | $8.52 | -101.34 | -101.34 | +0.00 | -101.34 |
| 2026-08-31 | `OPTX` | 1126 | $8.52 | $8.52 | +0.00 | — | +0.00 | +0.00 | -101.34 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ARCT` | 190 | — | $16.77 | +0.00 | $15.56 | -229.90 | -229.90 | +0.00 | -229.90 |
| 2026-09-03 | `CRDL` | 1465 | — | $2.18 | +0.00 | $2.16 | -29.30 | -29.30 | +0.00 | -29.30 |
| 2026-09-03 | `CLYM` | 227 | — | $13.96 | +0.00 | $14.59 | +143.01 | +143.01 | +0.00 | +143.01 |
| 2026-09-04 | `ARCT` | 190 | $15.56 | $15.61 | +9.50 | — | +0.00 | +9.50 | -220.40 | — |
| 2026-09-04 | `CRDL` | 1465 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -29.30 | — |
| 2026-09-04 | `CLYM` | 227 | $14.59 | $14.49 | -22.70 | — | +0.00 | -22.70 | +120.31 | — |
| 2026-09-04 | `DELL` | 18 | — | $513.78 | +0.00 | $524.14 | +186.48 | +186.48 | +0.00 | +186.48 |
| 2026-09-08 | `DELL` | 18 | $524.14 | $521.15 | -53.82 | — | +0.00 | -53.82 | +132.66 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `AMTX` | 584 | — | $2.04 | +0.00 | $2.01 | -17.52 | -17.52 | +0.00 | -17.52 |
| 2026-09-11 | `CLOV` | 250 | — | $4.75 | +0.00 | $4.82 | +17.50 | +17.50 | +0.00 | +17.50 |
| 2026-09-11 | `BAK` | 562 | — | $2.12 | +0.00 | $2.08 | -22.48 | -22.48 | +0.00 | -22.48 |
| 2026-09-11 | `TYRA` | 50 | — | $23.63 | +0.00 | $22.03 | -80.00 | -80.00 | +0.00 | -80.00 |
| 2026-09-11 | `FUBO` | 103 | — | $11.55 | +0.00 | $11.53 | -2.06 | -2.06 | +0.00 | -2.06 |
| 2026-09-11 | `RDDT` | 7 | — | $157.55 | +0.00 | $157.77 | +1.54 | +1.54 | +0.00 | +1.54 |
| 2026-09-11 | `VIST` | 15 | — | $77.33 | +0.00 | $76.27 | -15.90 | -15.90 | +0.00 | -15.90 |
| 2026-09-11 | `BAND` | 22 | — | $52.55 | +0.00 | $56.87 | +95.04 | +95.04 | +0.00 | +95.04 |
| 2026-09-14 | `AMTX` | 584 | $2.01 | $2.01 | +0.00 | — | +0.00 | +0.00 | -17.52 | — |
| 2026-09-14 | `CLOV` | 250 | $4.82 | $4.82 | +0.00 | — | +0.00 | +0.00 | +17.50 | — |
| 2026-09-14 | `BAK` | 562 | $2.08 | $2.05 | -16.86 | — | +0.00 | -16.86 | -39.34 | — |
| 2026-09-14 | `TYRA` | 50 | $22.03 | $23.20 | +58.50 | — | +0.00 | +58.50 | -21.50 | — |
| 2026-09-14 | `FUBO` | 103 | $11.53 | $11.56 | +3.09 | — | +0.00 | +3.09 | +1.03 | — |
| 2026-09-14 | `RDDT` | 7 | $157.77 | $160.00 | +15.61 | — | +0.00 | +15.61 | +17.15 | — |
| 2026-09-14 | `VIST` | 15 | $76.27 | $77.10 | +12.45 | — | +0.00 | +12.45 | -3.45 | — |
| 2026-09-14 | `BAND` | 22 | $56.87 | $56.90 | +0.66 | — | +0.00 | +0.66 | +95.70 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `ARQQ` | 74 | — | $18.21 | +0.00 | $19.01 | +59.20 | +59.20 | +0.00 | +59.20 |
| 2026-09-16 | `TEM` | 19 | — | $68.79 | +0.00 | $69.97 | +22.42 | +22.42 | +0.00 | +22.42 |
| 2026-09-16 | `RIG` | 231 | — | $5.87 | +0.00 | $5.54 | -76.23 | -76.23 | +0.00 | -76.23 |
| 2026-09-16 | `QTRX` | 500 | — | $2.72 | +0.00 | $2.91 | +95.00 | +95.00 | +0.00 | +95.00 |
| 2026-09-16 | `VAL` | 15 | — | $87.40 | +0.00 | $82.52 | -73.20 | -73.20 | +0.00 | -73.20 |
| 2026-09-16 | `KRMN` | 35 | — | $38.01 | +0.00 | $36.94 | -37.45 | -37.45 | +0.00 | -37.45 |
| 2026-09-16 | `ADPT` | 50 | — | $27.09 | +0.00 | $27.67 | +29.00 | +29.00 | +0.00 | +29.00 |
| 2026-09-17 | `ARQQ` | 74 | $19.01 | $19.59 | +42.92 | — | +0.00 | +42.92 | +102.12 | — |
| 2026-09-17 | `TEM` | 19 | $69.97 | $72.70 | +51.87 | — | +0.00 | +51.87 | +74.29 | — |
| 2026-09-17 | `RIG` | 231 | $5.54 | $5.58 | +9.24 | — | +0.00 | +9.24 | -66.99 | — |
| 2026-09-17 | `QTRX` | 500 | $2.91 | $2.94 | +15.00 | — | +0.00 | +15.00 | +110.00 | — |
| 2026-09-17 | `VAL` | 15 | $82.52 | $83.20 | +10.20 | — | +0.00 | +10.20 | -63.00 | — |
| 2026-09-17 | `KRMN` | 35 | $36.94 | $37.89 | +33.25 | — | +0.00 | +33.25 | -4.20 | — |
| 2026-09-17 | `ADPT` | 50 | $27.67 | $28.23 | +28.00 | — | +0.00 | +28.00 | +57.00 | — |
| 2026-09-17 | `DVLT` | 7129 | — | $0.17 | +0.00 | $0.16 | -71.29 | -71.29 | +0.00 | -71.29 |
| 2026-09-17 | `BRUN` | 76 | — | $15.87 | +0.00 | $16.68 | +61.56 | +61.56 | +0.00 | +61.56 |
| 2026-09-17 | `AXTI` | 17 | — | $67.91 | +0.00 | $67.75 | -2.72 | -2.72 | +0.00 | -2.72 |
| 2026-09-17 | `ARQT` | 46 | — | $25.95 | +0.00 | $26.46 | +23.46 | +23.46 | +0.00 | +23.46 |
| 2026-09-17 | `SMTC` | 7 | — | $170.85 | +0.00 | $178.19 | +51.38 | +51.38 | +0.00 | +51.38 |
| 2026-09-17 | `SABR` | 505 | — | $2.40 | +0.00 | $2.32 | -40.40 | -40.40 | +0.00 | -40.40 |
| 2026-09-17 | `CIFR` | 67 | — | $18.04 | +0.00 | $16.94 | -73.36 | -73.36 | +0.00 | -73.36 |
| 2026-09-17 | `EROC` | 95 | — | $12.64 | +0.00 | $12.90 | +24.70 | +24.70 | +0.00 | +24.70 |
| 2026-09-18 | `DVLT` | 7129 | $0.16 | $0.17 | +71.29 | — | +0.00 | +71.29 | +0.00 | — |
| 2026-09-18 | `BRUN` | 76 | $16.68 | $17.44 | +57.76 | — | +0.00 | +57.76 | +119.32 | — |
| 2026-09-18 | `AXTI` | 17 | $67.75 | $69.72 | +33.49 | — | +0.00 | +33.49 | +30.77 | — |
| 2026-09-18 | `ARQT` | 46 | $26.46 | $26.14 | -14.72 | — | +0.00 | -14.72 | +8.74 | — |
| 2026-09-18 | `SMTC` | 7 | $178.19 | $182.33 | +28.98 | — | +0.00 | +28.98 | +80.36 | — |
| 2026-09-18 | `SABR` | 505 | $2.32 | $2.29 | -15.15 | — | +0.00 | -15.15 | -55.55 | — |
| 2026-09-18 | `CIFR` | 67 | $16.94 | $17.80 | +57.62 | — | +0.00 | +57.62 | -15.74 | — |
| 2026-09-18 | `EROC` | 95 | $12.90 | $13.00 | +9.50 | — | +0.00 | +9.50 | +34.20 | — |
| 2026-09-18 | `TLSA` | 1442 | — | $0.97 | +0.00 | $0.91 | -86.52 | -86.52 | +0.00 | -86.52 |
| 2026-09-18 | `EYPT` | 354 | — | $3.95 | +0.00 | $3.85 | -35.40 | -35.40 | +0.00 | -35.40 |
| 2026-09-18 | `BHVN` | 99 | — | $14.07 | +0.00 | $13.62 | -44.55 | -44.55 | +0.00 | -44.55 |
| 2026-09-18 | `BNC` | 239 | — | $5.83 | +0.00 | $5.98 | +35.85 | +35.85 | +0.00 | +35.85 |
| 2026-09-18 | `DDD` | 390 | — | $3.58 | +0.00 | $3.63 | +19.50 | +19.50 | +0.00 | +19.50 |
| 2026-09-18 | `RANI` | 1645 | — | $0.85 | +0.00 | $0.86 | +19.74 | +19.74 | +0.00 | +19.74 |
| 2026-09-18 | `RARE` | 91 | — | $14.79 | +0.00 | $14.51 | -25.48 | -25.48 | +0.00 | -25.48 |
| 2026-09-21 | `TLSA` | 1442 | $0.91 | $0.94 | +43.26 | — | +0.00 | +43.26 | -43.26 | — |
| 2026-09-21 | `EYPT` | 354 | $3.85 | $3.87 | +7.08 | — | +0.00 | +7.08 | -28.32 | — |
| 2026-09-21 | `BHVN` | 99 | $13.62 | $13.90 | +27.72 | — | +0.00 | +27.72 | -16.83 | — |
| 2026-09-21 | `BNC` | 239 | $5.98 | $6.42 | +103.96 | — | +0.00 | +103.96 | +139.81 | — |
| 2026-09-21 | `DDD` | 390 | $3.63 | $3.71 | +31.20 | — | +0.00 | +31.20 | +50.70 | — |
| 2026-09-21 | `RANI` | 1645 | $0.86 | $0.86 | +3.95 | — | +0.00 | +3.95 | +23.69 | — |
| 2026-09-21 | `RARE` | 91 | $14.51 | $14.60 | +8.19 | — | +0.00 | +8.19 | -17.29 | — |
| 2026-09-21 | `BTDR` | 145 | — | $13.44 | +0.00 | $13.14 | -43.50 | -43.50 | +0.00 | -43.50 |
| 2026-09-21 | `ORBS` | 1780 | — | $1.10 | +0.00 | $1.05 | -89.00 | -89.00 | +0.00 | -89.00 |
| 2026-09-21 | `SBET` | 196 | — | $9.99 | +0.00 | $9.98 | -1.96 | -1.96 | +0.00 | -1.96 |
| 2026-09-21 | `BTBT` | 1072 | — | $1.82 | +0.00 | $1.82 | -5.36 | -5.36 | +0.00 | -5.36 |
| 2026-09-21 | `COIN` | 9 | — | $205.50 | +0.00 | $201.05 | -40.05 | -40.05 | +0.00 | -40.05 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -38.70 | ANGX, HYLN, WDC, ADUR, ALGM | — | $493.79 | $9,942.67 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 |
| 2026-08-17 | +2.25 | $493.79 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 | $10,107.31 | +164.64 | -105.01 | CDNL, ABX, VERA, CELC, OCC, ALM | ANGX, HYLN, WDC, ADUR, ALGM | $43.81 | $9,969.98 | CDNL×42, ABX×184, VERA×53, CELC×18, OCC×92, ALM×103 |
| 2026-08-18 | -6.20 | $43.81 | CDNL×42, ABX×184, VERA×53, CELC×18, OCC×92, ALM×103 | $9,889.28 | -80.70 | +0.00 | — | CDNL, ABX, VERA, CELC, OCC, ALM | $9,875.70 | $9,875.70 | — |
| 2026-08-19 | -7.20 | $9,875.70 | — | $9,875.70 | -0.00 | +0.00 | — | — | $9,875.70 | $9,875.70 | — |
| 2026-08-20 | +1.12 | $9,875.70 | — | $9,875.70 | -0.00 | -78.04 | DNA, MSTR, EXK, SCZM, NG, BLSH, HYMC | — | $83.88 | $9,781.48 | DNA×189, MSTR×12, EXK×130, SCZM×149, NG×168, BLSH×48, HYMC×51 |
| 2026-08-21 | +3.25 | $83.88 | DNA×189, MSTR×12, EXK×130, SCZM×149, NG×168, BLSH×48, HYMC×51 | $10,203.87 | +422.39 | -6.05 | BTBT, DE, QDEL, ORBS, GORO, CF | DNA, MSTR, EXK, SCZM, NG, BLSH, HYMC | $455.23 | $10,132.00 | BTBT×1022, DE×2, QDEL×113, ORBS×1965, GORO×545, CF×13 |
| 2026-08-24 | -5.17 | $455.23 | BTBT×1022, DE×2, QDEL×113, ORBS×1965, GORO×545, CF×13 | $10,193.75 | +61.75 | +0.00 | — | BTBT, DE, QDEL, ORBS, GORO, CF | $10,143.09 | $10,143.09 | — |
| 2026-08-25 | +1.80 | $10,143.09 | — | $10,143.09 | +0.00 | +71.79 | SAFX, VITL, KURA, CCOI, LIFE, ZIP, ADIG | — | $18.80 | $10,172.73 | SAFX×4047, VITL×130, KURA×106, CCOI×152, LIFE×39, ZIP×318, ADIG×65 |
| 2026-08-26 | +2.02 | $18.80 | SAFX×4047, VITL×130, KURA×106, CCOI×152, LIFE×39, ZIP×318, ADIG×65 | $10,106.99 | -65.74 | +29.83 | AVBP, ABX, ITG, SENS | SAFX, VITL, KURA, CCOI, LIFE, ZIP, ADIG | $32.55 | $10,082.35 | AVBP×80, ABX×255, ITG×208, SENS×265 |
| 2026-08-27 | — | $32.55 | AVBP×80, ABX×255, ITG×208, SENS×265 | $10,007.48 | -74.87 | -190.56 | BE | AVBP, ABX, SENS | $158.22 | $9,805.74 | ITG×208, BE×32 |
| 2026-08-28 | +0.75 | $158.22 | ITG×208, BE×32 | $9,721.10 | -84.64 | -101.34 | OPTX | ITG, BE | $6.82 | $9,600.34 | OPTX×1126 |
| 2026-08-31 | -5.85 | $6.82 | OPTX×1126 | $9,600.34 | +0.00 | +0.00 | — | OPTX | $9,585.55 | $9,585.55 | — |
| 2026-09-01 | -6.30 | $9,585.55 | — | $9,585.55 | +0.00 | +0.00 | — | — | $9,585.55 | $9,585.55 | — |
| 2026-09-02 | -3.83 | $9,585.55 | — | $9,585.55 | +0.00 | +0.00 | — | — | $9,585.55 | $9,585.55 | — |
| 2026-09-03 | -0.90 | $9,585.55 | — | $9,585.55 | +0.00 | -116.19 | ARCT, CRDL, CLYM | — | $12.25 | $9,444.98 | ARCT×190, CRDL×1465, CLYM×227 |
| 2026-09-04 | +2.25 | $12.25 | ARCT×190, CRDL×1465, CLYM×227 | $9,431.78 | -13.20 | +186.48 | DELL | ARCT, CRDL, CLYM | $156.92 | $9,591.44 | DELL×18 |
| 2026-09-08 | -11.47 | $156.92 | DELL×18 | $9,537.62 | -53.82 | +0.00 | — | DELL | $9,535.49 | $9,535.49 | — |
| 2026-09-09 | -13.95 | $9,535.49 | — | $9,535.49 | -0.00 | +0.00 | — | — | $9,535.49 | $9,535.49 | — |
| 2026-09-10 | -13.28 | $9,535.49 | — | $9,535.49 | -0.00 | +0.00 | — | — | $9,535.49 | $9,535.49 | — |
| 2026-09-11 | +0.50 | $9,535.49 | — | $9,535.49 | -0.00 | -23.88 | AMTX, CLOV, BAK, TYRA, FUBO, RDDT, VIST, BAND | — | $146.59 | $9,483.06 | AMTX×584, CLOV×250, BAK×562, TYRA×50, FUBO×103, RDDT×7, VIST×15, BAND×22 |
| 2026-09-14 | -11.00 | $146.59 | AMTX×584, CLOV×250, BAK×562, TYRA×50, FUBO×103, RDDT×7, VIST×15, BAND×22 | $9,556.51 | +73.45 | +0.00 | — | AMTX, CLOV, BAK, TYRA, FUBO, RDDT, VIST, BAND | $9,527.59 | $9,527.59 | — |
| 2026-09-15 | -3.84 | $9,527.59 | — | $9,527.59 | +0.00 | +0.00 | — | — | $9,527.59 | $9,527.59 | — |
| 2026-09-16 | +5.30 | $9,527.59 | — | $9,527.59 | +0.00 | +18.74 | ARQQ, TEM, RIG, QTRX, VAL, KRMN, ADPT | — | $141.26 | $9,526.37 | ARQQ×74, TEM×19, RIG×231, QTRX×500, VAL×15, KRMN×35, ADPT×50 |
| 2026-09-17 | +7.38 | $141.26 | ARQQ×74, TEM×19, RIG×231, QTRX×500, VAL×15, KRMN×35, ADPT×50 | $9,716.85 | +190.48 | -26.67 | DVLT, BRUN, AXTI, ARQT, SMTC, SABR, CIFR, EROC | ARQQ, TEM, RIG, QTRX, VAL, KRMN, ADPT | $60.44 | $9,617.08 | DVLT×7129, BRUN×76, AXTI×17, ARQT×46, SMTC×7, SABR×505, CIFR×67, EROC×95 |
| 2026-09-18 | +4.86 | $60.44 | DVLT×7129, BRUN×76, AXTI×17, ARQT×46, SMTC×7, SABR×505, CIFR×67, EROC×95 | $9,845.85 | +228.77 | -116.86 | TLSA, EYPT, BHVN, BNC, DDD, RANI, RARE | DVLT, BRUN, AXTI, ARQT, SMTC, SABR, CIFR, EROC | $13.41 | $9,620.23 | TLSA×1442, EYPT×354, BHVN×99, BNC×239, DDD×390, RANI×1645, RARE×91 |
| 2026-09-21 | +12.87 | $13.41 | TLSA×1442, EYPT×354, BHVN×99, BNC×239, DDD×390, RANI×1645, RARE×91 | $9,845.59 | +225.36 | -179.87 | BTDR, ORBS, SBET, BTBT, COIN | TLSA, EYPT, BHVN, BNC, DDD, RANI, RARE | $75.99 | $9,566.86 | BTDR×145, ORBS×1780, SBET×196, BTBT×1072, COIN×9 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $7,994.17 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 478 | $4.18 | $6.17 | — | $5,989.97 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 3 | $503.50 | $2.00 | — | $4,477.47 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ⚪; ret5=+7.9; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 121 | $16.50 | $2.35 | — | $2,478.62 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 45 | $44.06 | $2.12 | — | $493.79 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+3.9; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $493.79 | ▼ close $9,942.67 vs 09:30 $10,000.00 (session -38.70) | 16:00 close · cash $493.79 · equity $9,942.67 vs 09:30 $10,000.00 (-57.33; session marks -38.70) · 5 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.31 → close $4.37 +27.84; HYLN×478 09:30 $4.18 → close $4.06 -57.36; WDC×3 09:30 $503.50 → close $508.80 +15.90; ADUR×121 09:30 $16.50 → close $16.17 -39.93; ALGM×45 09:30 $44.06 → close $44.39 +14.85 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $493.79 | ▲ 09:30 equity $10,107.31 vs yday $9,942.67 (+164.64) | 09:30 open · cash $493.79 (unchanged overnight, no fees) · equity $10,107.31 vs prior close $9,942.67 (+164.64) · 5 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; HYLN×478 yday $4.06 → 09:30 $4.10 +19.12; WDC×3 yday $508.80 → 09:30 $525.53 +50.19; ADUR×121 yday $16.17 → 09:30 $15.73 -53.24; ALGM×45 yday $44.39 → 09:30 $45.32 +41.85 | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 464 | $4.60 | $6.08 | $+122.49 | $2,622.11 | ▲ +122.49 after sell → book $10,101.23; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 478 | $4.10 | $6.26 | $-50.67 | $4,575.65 | ▼ -50.67 after sell → book $10,094.97; vs 09:30 mark -6.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 3 | $525.53 | $2.02 | $+62.07 | $6,150.22 | ▲ +62.07 after sell → book $10,092.95; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 121 | $15.73 | $2.39 | $-97.91 | $8,051.16 | ▼ -97.91 after sell → book $10,090.56; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ALGM` | 45 | $45.32 | $2.15 | $+52.42 | $10,088.41 | ▲ +52.42 after sell → book $10,088.41; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 42 | $39.85 | $2.12 | — | $8,412.59 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1681.40 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 184 | $9.12 | $2.54 | — | $6,731.97 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1681.40 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 53 | $31.30 | $2.15 | — | $5,070.92 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-3.8; leftover $1681.40 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 18 | $92.99 | $2.04 | — | $3,395.06 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.8; leftover $1681.40 | join🟡 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 92 | $18.24 | $2.27 | — | $1,714.71 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1681.40 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 103 | $16.20 | $2.30 | — | $43.81 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1681.40 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.81 | ▼ close $9,969.98 vs 09:30 $10,107.31 (session -105.01) | 16:00 close · cash $43.81 · equity $9,969.98 vs 09:30 $10,107.31 (-137.33; session marks -105.01) · 6 name(s) marked open→close (per-name table). CDNL×42 09:30 $39.85 → close $39.23 -26.04; ABX×184 09:30 $9.12 → close $9.12 +0.00; VERA×53 09:30 $31.30 → close $31.63 +17.49; CELC×18 09:30 $92.99 → close $92.44 -9.90; OCC×92 09:30 $18.24 → close $17.12 -103.04; ALM×103 09:30 $16.20 → close $16.36 +16.48 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.81 | ▼ 09:30 equity $9,889.28 vs yday $9,969.98 (-80.70) | 09:30 open · cash $43.81 (unchanged overnight, no fees) · equity $9,889.28 vs prior close $9,969.98 (-80.70) · 6 name(s) re-marked at the open (per-name table). CDNL×42 yday $39.23 → 09:30 $41.57 +98.28; ABX×184 yday $9.12 → 09:30 $9.03 -16.56; VERA×53 yday $31.63 → 09:30 $31.31 -16.96; CELC×18 yday $92.44 → 09:30 $92.38 -1.08; OCC×92 yday $17.12 → 09:30 $16.20 -84.64; ALM×103 yday $16.36 → 09:30 $15.78 -59.74 | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 42 | $41.57 | $2.14 | $+67.98 | $1,787.61 | ▲ +67.98 after sell → book $9,887.14; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 184 | $9.03 | $2.59 | $-21.69 | $3,446.55 | ▼ -21.69 after sell → book $9,884.56; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 53 | $31.31 | $2.17 | $-3.79 | $5,103.81 | ▼ -3.79 after sell → book $9,882.39; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 18 | $92.38 | $2.07 | $-15.09 | $6,764.58 | ▼ -15.09 after sell → book $9,880.32; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 92 | $16.20 | $2.29 | $-192.24 | $8,252.68 | ▼ -192.24 after sell → book $9,878.02; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 103 | $15.78 | $2.33 | $-47.89 | $9,875.70 | ▼ -47.89 after sell → book $9,875.70; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,875.70 | ▲ close $9,875.70 vs 09:30 $9,889.28 (session +0.00) | 16:00 close · cash $9,875.70 · no lots left · equity $9,875.70. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,875.70 | ▲ 09:30 equity $9,875.70 vs yday $9,875.70 (-0.00) | 09:30 open · cash $9,875.70 · no holdings · equity $9,875.70 vs prior close $9,875.70 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,875.70 | ▲ close $9,875.70 vs 09:30 $9,875.70 (session +0.00) | 16:00 close · cash $9,875.70 · no lots left · equity $9,875.70. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,875.70 | ▲ 09:30 equity $9,875.70 vs yday $9,875.70 (-0.00) | 09:30 open · cash $9,875.70 · no holdings · equity $9,875.70 vs prior close $9,875.70 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 189 | $7.45 | $2.56 | — | $8,465.09 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1410.81 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 12 | $113.23 | $2.03 | — | $7,104.30 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1410.81 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 130 | $10.77 | $2.38 | — | $5,701.82 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1410.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 149 | $9.46 | $2.44 | — | $4,289.85 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1410.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 168 | $8.38 | $2.49 | — | $2,879.51 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1410.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 48 | $29.20 | $2.13 | — | $1,475.78 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1410.81 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HYMC` | 51 | $27.25 | $2.14 | — | $83.88 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+1.6; leftover $1410.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.88 | ▼ close $9,781.48 vs 09:30 $9,875.70 (session -78.04) | 16:00 close · cash $83.88 · equity $9,781.48 vs 09:30 $9,875.70 (-94.22; session marks -78.04) · 7 name(s) marked open→close (per-name table). DNA×189 09:30 $7.45 → close $6.96 -92.61; MSTR×12 09:30 $113.23 → close $112.39 -10.08; EXK×130 09:30 $10.77 → close $10.97 +26.00; SCZM×149 09:30 $9.46 → close $9.76 +44.70; NG×168 09:30 $8.38 → close $8.66 +47.04; BLSH×48 09:30 $29.20 → close $28.44 -36.48; HYMC×51 09:30 $27.25 → close $26.14 -56.61 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.88 | ▲ 09:30 equity $10,203.87 vs yday $9,781.48 (+422.39) | 09:30 open · cash $83.88 (unchanged overnight, no fees) · equity $10,203.87 vs prior close $9,781.48 (+422.39) · 7 name(s) re-marked at the open (per-name table). DNA×189 yday $6.96 → 09:30 $7.09 +24.57; MSTR×12 yday $112.39 → 09:30 $119.69 +87.60; EXK×130 yday $10.97 → 09:30 $11.34 +48.10; SCZM×149 yday $9.76 → 09:30 $10.26 +74.50; NG×168 yday $8.66 → 09:30 $9.02 +60.48; BLSH×48 yday $28.44 → 09:30 $29.75 +62.88; HYMC×51 yday $26.14 → 09:30 $27.40 +64.26 | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 189 | $7.09 | $2.60 | $-73.20 | $1,421.30 | ▼ -73.20 after sell → book $10,201.28; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MSTR` | 12 | $119.69 | $2.05 | $+73.45 | $2,855.53 | ▲ +73.45 after sell → book $10,199.23; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `EXK` | 130 | $11.34 | $2.41 | $+69.31 | $4,327.31 | ▲ +69.31 after sell → book $10,196.81; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 149 | $10.26 | $2.47 | $+114.29 | $5,853.58 | ▲ +114.29 after sell → book $10,194.34; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NG` | 168 | $9.02 | $2.53 | $+102.49 | $7,366.41 | ▲ +102.49 after sell → book $10,191.81; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BLSH` | 48 | $29.75 | $2.16 | $+22.11 | $8,792.25 | ▲ +22.11 after sell → book $10,189.65; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYMC` | 51 | $27.40 | $2.16 | $+3.34 | $10,187.49 | ▲ +3.34 after sell → book $10,187.49; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 1022 | $1.66 | $13.18 | — | $8,477.78 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1697.91 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $7,229.27 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1697.91 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 113 | $14.96 | $2.33 | — | $5,536.46 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-1.6; leftover $1697.91 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1965 | $0.86 | $22.87 | — | $3,815.83 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1697.91 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 545 | $3.11 | $7.03 | — | $2,113.84 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+7.1; leftover $1697.91 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 13 | $127.43 | $2.03 | — | $455.23 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ⚪; ret5=+7.9; leftover $1697.91 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $455.23 | ▼ close $10,132.00 vs 09:30 $10,203.87 (session -6.05) | 16:00 close · cash $455.23 · equity $10,132.00 vs 09:30 $10,203.87 (-71.87; session marks -6.05) · 6 name(s) marked open→close (per-name table). BTBT×1022 09:30 $1.66 → close $1.53 -132.86; DE×2 09:30 $623.26 → close $647.47 +48.42; QDEL×113 09:30 $14.96 → close $14.74 -24.86; ORBS×1965 09:30 $0.86 → close $0.88 +31.44; GORO×545 09:30 $3.11 → close $3.19 +43.60; CF×13 09:30 $127.43 → close $129.60 +28.21 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $455.23 | ▲ 09:30 equity $10,193.75 vs yday $10,132.00 (+61.75) | 09:30 open · cash $455.23 (unchanged overnight, no fees) · equity $10,193.75 vs prior close $10,132.00 (+61.75) · 6 name(s) re-marked at the open (per-name table). BTBT×1022 yday $1.53 → 09:30 $1.55 +20.44; DE×2 yday $647.47 → 09:30 $653.04 +11.14; QDEL×113 yday $14.74 → 09:30 $14.74 +0.00; ORBS×1965 yday $0.88 → 09:30 $0.89 +19.65; GORO×545 yday $3.19 → 09:30 $3.20 +5.45; CF×13 yday $129.60 → 09:30 $129.99 +5.07 | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 1022 | $1.55 | $13.37 | $-138.97 | $2,025.96 | ▼ -138.97 after sell → book $10,180.38; vs 09:30 mark -13.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $3,330.02 | ▲ +55.55 after sell → book $10,178.36; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 113 | $14.74 | $2.36 | $-29.55 | $4,993.28 | ▼ -29.55 after sell → book $10,176.00; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1965 | $0.89 | $23.72 | $+4.49 | $6,718.41 | ▲ +4.49 after sell → book $10,152.28; vs 09:30 mark -23.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GORO` | 545 | $3.20 | $7.13 | $+34.88 | $8,455.27 | ▲ +34.88 after sell → book $10,145.14; vs 09:30 mark -7.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 13 | $129.99 | $2.05 | $+29.20 | $10,143.09 | ▲ +29.20 after sell → book $10,143.09; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,143.09 | ▲ close $10,143.09 vs 09:30 $10,193.75 (session +0.00) | 16:00 close · cash $10,143.09 · no lots left · equity $10,143.09. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.09 | ▲ 09:30 equity $10,143.09 vs yday $10,143.09 (+0.00) | 09:30 open · cash $10,143.09 · no holdings · equity $10,143.09 vs prior close $10,143.09 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 4047 | $0.36 | $26.63 | — | $8,667.64 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-15.6; leftover $1449.01 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 130 | $11.12 | $2.38 | — | $7,219.66 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.7; leftover $1449.01 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 106 | $13.59 | $2.31 | — | $5,776.81 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1449.01 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 152 | $9.49 | $2.45 | — | $4,331.88 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1449.01 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 39 | $36.96 | $2.11 | — | $2,888.33 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1449.01 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 318 | $4.55 | $4.10 | — | $1,437.33 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1449.01 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ADIG` | 65 | $21.79 | $2.19 | — | $18.80 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+3.1; leftover $1449.01 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.80 | ▲ close $10,172.73 vs 09:30 $10,143.09 (session +71.79) | 16:00 close · cash $18.80 · equity $10,172.73 vs 09:30 $10,143.09 (+29.64; session marks +71.79) · 7 name(s) marked open→close (per-name table). SAFX×4047 09:30 $0.36 → close $0.35 -16.19; VITL×130 09:30 $11.12 → close $11.11 -1.30; KURA×106 09:30 $13.59 → close $13.59 +0.00; CCOI×152 09:30 $9.49 → close $9.88 +59.28; LIFE×39 09:30 $36.96 → close $38.56 +62.40; ZIP×318 09:30 $4.55 → close $4.35 -63.60; ADIG×65 09:30 $21.79 → close $22.27 +31.20 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.80 | ▼ 09:30 equity $10,106.99 vs yday $10,172.73 (-65.74) | 09:30 open · cash $18.80 (unchanged overnight, no fees) · equity $10,106.99 vs prior close $10,172.73 (-65.74) · 7 name(s) re-marked at the open (per-name table). SAFX×4047 yday $0.35 → 09:30 $0.35 -4.05; VITL×130 yday $11.11 → 09:30 $11.03 -10.40; KURA×106 yday $13.59 → 09:30 $13.63 +4.24; CCOI×152 yday $9.88 → 09:30 $9.89 +1.52; LIFE×39 yday $38.56 → 09:30 $38.24 -12.48; ZIP×318 yday $4.35 → 09:30 $4.31 -12.72; ADIG×65 yday $22.27 → 09:30 $21.78 -31.85 | — |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 4047 | $0.35 | $27.11 | $-73.97 | $1,420.28 | ▼ -73.97 after sell → book $10,079.88; vs 09:30 mark -27.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `VITL` | 130 | $11.03 | $2.41 | $-16.49 | $2,851.77 | ▼ -16.49 after sell → book $10,077.47; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 106 | $13.63 | $2.34 | $-0.41 | $4,294.21 | ▼ -0.41 after sell → book $10,075.13; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 152 | $9.89 | $2.48 | $+55.87 | $5,795.01 | ▲ +55.87 after sell → book $10,072.65; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 39 | $38.24 | $2.13 | $+45.68 | $7,284.24 | ▲ +45.68 after sell → book $10,070.52; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 318 | $4.31 | $4.17 | $-84.59 | $8,650.65 | ▼ -84.59 after sell → book $10,066.35; vs 09:30 mark -4.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ADIG` | 65 | $21.78 | $2.21 | $-5.04 | $10,064.14 | ▼ -5.04 after sell → book $10,064.14; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 80 | $31.21 | $2.23 | — | $7,565.11 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $2516.04 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 255 | $9.83 | $3.29 | — | $5,055.17 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $2516.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ITG` | 208 | $12.04 | $2.68 | — | $2,548.17 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-5.1; leftover $2516.04 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SENS` | 265 | $9.48 | $3.42 | — | $32.55 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $2516.04 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.55 | ▲ close $10,082.35 vs 09:30 $10,106.99 (session +29.83) | 16:00 close · cash $32.55 · equity $10,082.35 vs 09:30 $10,106.99 (-24.64; session marks +29.83) · 4 name(s) marked open→close (per-name table). AVBP×80 09:30 $31.21 → close $31.14 -5.60; ABX×255 09:30 $9.83 → close $9.78 -12.75; ITG×208 09:30 $12.04 → close $12.45 +85.28; SENS×265 09:30 $9.48 → close $9.34 -37.10 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.55 | ▼ 09:30 equity $10,007.48 vs yday $10,082.35 (-74.87) | 09:30 open · cash $32.55 (unchanged overnight, no fees) · equity $10,007.48 vs prior close $10,082.35 (-74.87) · 4 name(s) re-marked at the open (per-name table). AVBP×80 yday $31.14 → 09:30 $30.79 -28.00; ABX×255 yday $9.78 → 09:30 $9.68 -25.50; ITG×208 yday $12.45 → 09:30 $12.36 -18.72; SENS×265 yday $9.34 → 09:30 $9.33 -2.65 | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 80 | $30.79 | $2.26 | $-38.09 | $2,493.49 | ▼ -38.09 after sell → book $10,005.22; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `ABX` | 255 | $9.68 | $3.35 | $-44.89 | $4,958.54 | ▼ -44.89 after sell → book $10,001.87; vs 09:30 mark -3.35 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `SENS` | 265 | $9.33 | $3.48 | $-46.65 | $7,427.50 | ▼ -46.65 after sell → book $9,998.38; vs 09:30 mark -3.49 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `BE` | 32 | $227.10 | $2.09 | — | $158.22 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.6; leftover $7427.50 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.22 | ▼ close $9,805.74 vs 09:30 $10,007.48 (session -190.56) | 16:00 close · cash $158.22 · equity $9,805.74 vs 09:30 $10,007.48 (-201.74; session marks -190.56) · 2 name(s) marked open→close (per-name table). ITG×208 09:30 $12.36 → close $12.87 +106.08; BE×32 09:30 $227.10 → close $217.83 -296.64 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.22 | ▼ 09:30 equity $9,721.10 vs yday $9,805.74 (-84.64) | 09:30 open · cash $158.22 (unchanged overnight, no fees) · equity $9,721.10 vs prior close $9,805.74 (-84.64) · 2 name(s) re-marked at the open (per-name table). ITG×208 yday $12.87 → 09:30 $12.79 -16.64; BE×32 yday $217.83 → 09:30 $215.71 -68.00 | — |
| 2026-08-28 09:30 ET | **SELL** | `ITG` | 208 | $12.79 | $2.74 | $+150.58 | $2,815.80 | ▲ +150.58 after sell → book $9,718.36; vs 09:30 mark -2.74 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BE` | 32 | $215.71 | $2.15 | $-368.88 | $9,716.21 | ▼ -368.88 after sell → book $9,716.21; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 1126 | $8.61 | $14.53 | — | $6.82 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.7; leftover $9716.21 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.82 | ▼ close $9,600.34 vs 09:30 $9,721.10 (session -101.34) | 16:00 close · cash $6.82 · equity $9,600.34 vs 09:30 $9,721.10 (-120.76; session marks -101.34) · 1 name(s) marked open→close (per-name table). OPTX×1126 09:30 $8.61 → close $8.52 -101.34 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.82 | ▲ 09:30 equity $9,600.34 vs yday $9,600.34 (+0.00) | 09:30 open · cash $6.82 (unchanged overnight, no fees) · equity $9,600.34 vs prior close $9,600.34 (+0.00) · 1 name(s) re-marked at the open (per-name table). OPTX×1126 yday $8.52 → 09:30 $8.52 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 1126 | $8.52 | $14.79 | $-130.65 | $9,585.55 | ▼ -130.65 after sell → book $9,585.55; vs 09:30 mark -14.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,585.55 | ▲ close $9,585.55 vs 09:30 $9,600.34 (session +0.00) | 16:00 close · cash $9,585.55 · no lots left · equity $9,585.55. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,585.55 | ▲ 09:30 equity $9,585.55 vs yday $9,585.55 (+0.00) | 09:30 open · cash $9,585.55 · no holdings · equity $9,585.55 vs prior close $9,585.55 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,585.55 | ▲ close $9,585.55 vs 09:30 $9,585.55 (session +0.00) | 16:00 close · cash $9,585.55 · no lots left · equity $9,585.55. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,585.55 | ▲ 09:30 equity $9,585.55 vs yday $9,585.55 (+0.00) | 09:30 open · cash $9,585.55 · no holdings · equity $9,585.55 vs prior close $9,585.55 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,585.55 | ▲ close $9,585.55 vs 09:30 $9,585.55 (session +0.00) | 16:00 close · cash $9,585.55 · no lots left · equity $9,585.55. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,585.55 | ▲ 09:30 equity $9,585.55 vs yday $9,585.55 (+0.00) | 09:30 open · cash $9,585.55 · no holdings · equity $9,585.55 vs prior close $9,585.55 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 190 | $16.77 | $2.56 | — | $6,396.69 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $3195.18 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 1465 | $2.18 | $18.90 | — | $3,184.10 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $3195.18 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 227 | $13.96 | $2.93 | — | $12.25 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-6.4; leftover $3195.18 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.25 | ▼ close $9,444.98 vs 09:30 $9,585.55 (session -116.19) | 16:00 close · cash $12.25 · equity $9,444.98 vs 09:30 $9,585.55 (-140.57; session marks -116.19) · 3 name(s) marked open→close (per-name table). ARCT×190 09:30 $16.77 → close $15.56 -229.90; CRDL×1465 09:30 $2.18 → close $2.16 -29.30; CLYM×227 09:30 $13.96 → close $14.59 +143.01 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.25 | ▼ 09:30 equity $9,431.78 vs yday $9,444.98 (-13.20) | 09:30 open · cash $12.25 (unchanged overnight, no fees) · equity $9,431.78 vs prior close $9,444.98 (-13.20) · 3 name(s) re-marked at the open (per-name table). ARCT×190 yday $15.56 → 09:30 $15.61 +9.50; CRDL×1465 yday $2.16 → 09:30 $2.16 +0.00; CLYM×227 yday $14.59 → 09:30 $14.49 -22.70 | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 190 | $15.61 | $2.62 | $-225.58 | $2,975.53 | ▼ -225.58 after sell → book $9,429.16; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 1465 | $2.16 | $19.17 | $-67.37 | $6,120.77 | ▼ -67.37 after sell → book $9,410.00; vs 09:30 mark -19.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CLYM` | 227 | $14.49 | $2.99 | $+114.39 | $9,407.00 | ▲ +114.39 after sell → book $9,407.00; vs 09:30 mark -3.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 18 | $513.78 | $2.04 | — | $156.92 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $9407.00 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $156.92 | ▲ close $9,591.44 vs 09:30 $9,431.78 (session +186.48) | 16:00 close · cash $156.92 · equity $9,591.44 vs 09:30 $9,431.78 (+159.66; session marks +186.48) · 1 name(s) marked open→close (per-name table). DELL×18 09:30 $513.78 → close $524.14 +186.48 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $156.92 | ▼ 09:30 equity $9,537.62 vs yday $9,591.44 (-53.82) | 09:30 open · cash $156.92 (unchanged overnight, no fees) · equity $9,537.62 vs prior close $9,591.44 (-53.82) · 1 name(s) re-marked at the open (per-name table). DELL×18 yday $524.14 → 09:30 $521.15 -53.82 | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 18 | $521.15 | $2.13 | $+128.49 | $9,535.49 | ▲ +128.49 after sell → book $9,535.49; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,535.49 | ▲ close $9,535.49 vs 09:30 $9,537.62 (session +0.00) | 16:00 close · cash $9,535.49 · no lots left · equity $9,535.49. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,535.49 | ▲ 09:30 equity $9,535.49 vs yday $9,535.49 (-0.00) | 09:30 open · cash $9,535.49 · no holdings · equity $9,535.49 vs prior close $9,535.49 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,535.49 | ▲ close $9,535.49 vs 09:30 $9,535.49 (session +0.00) | 16:00 close · cash $9,535.49 · no lots left · equity $9,535.49. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,535.49 | ▲ 09:30 equity $9,535.49 vs yday $9,535.49 (-0.00) | 09:30 open · cash $9,535.49 · no holdings · equity $9,535.49 vs prior close $9,535.49 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,535.49 | ▲ close $9,535.49 vs 09:30 $9,535.49 (session +0.00) | 16:00 close · cash $9,535.49 · no lots left · equity $9,535.49. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,535.49 | ▲ 09:30 equity $9,535.49 vs yday $9,535.49 (-0.00) | 09:30 open · cash $9,535.49 · no holdings · equity $9,535.49 vs prior close $9,535.49 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 584 | $2.04 | $7.53 | — | $8,336.60 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1191.94 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 250 | $4.75 | $3.23 | — | $7,145.87 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1191.94 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 562 | $2.12 | $7.25 | — | $5,947.18 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1191.94 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 50 | $23.63 | $2.14 | — | $4,763.54 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-6.3; leftover $1191.94 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 103 | $11.55 | $2.30 | — | $3,571.59 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1191.94 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 7 | $157.55 | $2.01 | — | $2,466.73 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1191.94 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟡 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 15 | $77.33 | $2.04 | — | $1,304.75 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+2.5; leftover $1191.94 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 22 | $52.55 | $2.06 | — | $146.59 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1191.94 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $146.59 | ▼ close $9,483.06 vs 09:30 $9,535.49 (session -23.88) | 16:00 close · cash $146.59 · equity $9,483.06 vs 09:30 $9,535.49 (-52.43; session marks -23.88) · 8 name(s) marked open→close (per-name table). AMTX×584 09:30 $2.04 → close $2.01 -17.52; CLOV×250 09:30 $4.75 → close $4.82 +17.50; BAK×562 09:30 $2.12 → close $2.08 -22.48; TYRA×50 09:30 $23.63 → close $22.03 -80.00; FUBO×103 09:30 $11.55 → close $11.53 -2.06; RDDT×7 09:30 $157.55 → close $157.77 +1.54; VIST×15 09:30 $77.33 → close $76.27 -15.90; BAND×22 09:30 $52.55 → close $56.87 +95.04 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $146.59 | ▲ 09:30 equity $9,556.51 vs yday $9,483.06 (+73.45) | 09:30 open · cash $146.59 (unchanged overnight, no fees) · equity $9,556.51 vs prior close $9,483.06 (+73.45) · 8 name(s) re-marked at the open (per-name table). AMTX×584 yday $2.01 → 09:30 $2.01 +0.00; CLOV×250 yday $4.82 → 09:30 $4.82 +0.00; BAK×562 yday $2.08 → 09:30 $2.05 -16.86; TYRA×50 yday $22.03 → 09:30 $23.20 +58.50; FUBO×103 yday $11.53 → 09:30 $11.56 +3.09; RDDT×7 yday $157.77 → 09:30 $160.00 +15.61; VIST×15 yday $76.27 → 09:30 $77.10 +12.45; BAND×22 yday $56.87 → 09:30 $56.90 +0.66 | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 584 | $2.01 | $7.64 | $-32.69 | $1,312.79 | ▼ -32.69 after sell → book $9,548.87; vs 09:30 mark -7.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 250 | $4.82 | $3.28 | $+11.00 | $2,514.51 | ▲ +11.00 after sell → book $9,545.59; vs 09:30 mark -3.28 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 562 | $2.05 | $7.35 | $-53.94 | $3,659.26 | ▼ -53.94 after sell → book $9,538.24; vs 09:30 mark -7.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 50 | $23.20 | $2.16 | $-25.80 | $4,817.10 | ▼ -25.80 after sell → book $9,536.08; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `FUBO` | 103 | $11.56 | $2.33 | $-3.60 | $6,005.45 | ▼ -3.60 after sell → book $9,533.75; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RDDT` | 7 | $160.00 | $2.03 | $+13.11 | $7,123.42 | ▲ +13.11 after sell → book $9,531.72; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIST` | 15 | $77.10 | $2.06 | $-7.54 | $8,277.87 | ▼ -7.54 after sell → book $9,529.67; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 22 | $56.90 | $2.08 | $+91.57 | $9,527.59 | ▲ +91.57 after sell → book $9,527.59; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,527.59 | ▲ close $9,527.59 vs 09:30 $9,556.51 (session +0.00) | 16:00 close · cash $9,527.59 · no lots left · equity $9,527.59. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,527.59 | ▲ 09:30 equity $9,527.59 vs yday $9,527.59 (+0.00) | 09:30 open · cash $9,527.59 · no holdings · equity $9,527.59 vs prior close $9,527.59 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,527.59 | ▲ close $9,527.59 vs 09:30 $9,527.59 (session +0.00) | 16:00 close · cash $9,527.59 · no lots left · equity $9,527.59. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,527.59 | ▲ 09:30 equity $9,527.59 vs yday $9,527.59 (+0.00) | 09:30 open · cash $9,527.59 · no holdings · equity $9,527.59 vs prior close $9,527.59 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 74 | $18.21 | $2.21 | — | $8,177.84 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-19.1; leftover $1361.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 19 | $68.79 | $2.05 | — | $6,868.78 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1361.08 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 231 | $5.87 | $2.98 | — | $5,509.83 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1361.08 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 catal🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 500 | $2.72 | $6.45 | — | $4,143.38 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.4; leftover $1361.08 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 15 | $87.40 | $2.04 | — | $2,830.35 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1361.08 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 35 | $38.01 | $2.10 | — | $1,497.90 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $1361.08 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 50 | $27.09 | $2.14 | — | $141.26 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1361.08 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $141.26 | ▲ close $9,526.37 vs 09:30 $9,527.59 (session +18.74) | 16:00 close · cash $141.26 · equity $9,526.37 vs 09:30 $9,527.59 (-1.22; session marks +18.74) · 7 name(s) marked open→close (per-name table). ARQQ×74 09:30 $18.21 → close $19.01 +59.20; TEM×19 09:30 $68.79 → close $69.97 +22.42; RIG×231 09:30 $5.87 → close $5.54 -76.23; QTRX×500 09:30 $2.72 → close $2.91 +95.00; VAL×15 09:30 $87.40 → close $82.52 -73.20; KRMN×35 09:30 $38.01 → close $36.94 -37.45; ADPT×50 09:30 $27.09 → close $27.67 +29.00 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $141.26 | ▲ 09:30 equity $9,716.85 vs yday $9,526.37 (+190.48) | 09:30 open · cash $141.26 (unchanged overnight, no fees) · equity $9,716.85 vs prior close $9,526.37 (+190.48) · 7 name(s) re-marked at the open (per-name table). ARQQ×74 yday $19.01 → 09:30 $19.59 +42.92; TEM×19 yday $69.97 → 09:30 $72.70 +51.87; RIG×231 yday $5.54 → 09:30 $5.58 +9.24; QTRX×500 yday $2.91 → 09:30 $2.94 +15.00; VAL×15 yday $82.52 → 09:30 $83.20 +10.20; KRMN×35 yday $36.94 → 09:30 $37.89 +33.25; ADPT×50 yday $27.67 → 09:30 $28.23 +28.00 | — |
| 2026-09-17 09:30 ET | **SELL** | `ARQQ` | 74 | $19.59 | $2.24 | $+97.67 | $1,588.69 | ▲ +97.67 after sell → book $9,714.62; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 19 | $72.70 | $2.07 | $+70.17 | $2,967.92 | ▲ +70.17 after sell → book $9,712.55; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 231 | $5.58 | $3.03 | $-73.00 | $4,253.87 | ▼ -73.00 after sell → book $9,709.52; vs 09:30 mark -3.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QTRX` | 500 | $2.94 | $6.54 | $+97.01 | $5,717.33 | ▲ +97.01 after sell → book $9,702.98; vs 09:30 mark -6.54 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 15 | $83.20 | $2.06 | $-67.09 | $6,963.27 | ▼ -67.09 after sell → book $9,700.92; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `KRMN` | 35 | $37.89 | $2.12 | $-8.41 | $8,287.31 | ▼ -8.41 after sell → book $9,698.81; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 50 | $28.23 | $2.16 | $+52.70 | $9,696.64 | ▲ +52.70 after sell → book $9,696.64; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 7129 | $0.17 | $33.51 | — | $8,451.21 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $1212.08 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 76 | $15.87 | $2.22 | — | $7,242.87 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $1212.08 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AXTI` | 17 | $67.91 | $2.04 | — | $6,086.36 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.7; leftover $1212.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 46 | $25.95 | $2.13 | — | $4,890.53 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1212.08 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $3,692.57 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1212.08 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 505 | $2.40 | $6.51 | — | $2,474.06 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1212.08 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 67 | $18.04 | $2.19 | — | $1,263.52 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $1212.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `EROC` | 95 | $12.64 | $2.27 | — | $60.44 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-3.6; leftover $1212.08 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.44 | ▼ close $9,617.08 vs 09:30 $9,716.85 (session -26.67) | 16:00 close · cash $60.44 · equity $9,617.08 vs 09:30 $9,716.85 (-99.77; session marks -26.67) · 8 name(s) marked open→close (per-name table). DVLT×7129 09:30 $0.17 → close $0.16 -71.29; BRUN×76 09:30 $15.87 → close $16.68 +61.56; AXTI×17 09:30 $67.91 → close $67.75 -2.72; ARQT×46 09:30 $25.95 → close $26.46 +23.46; SMTC×7 09:30 $170.85 → close $178.19 +51.38; SABR×505 09:30 $2.40 → close $2.32 -40.40; CIFR×67 09:30 $18.04 → close $16.94 -73.36; EROC×95 09:30 $12.64 → close $12.90 +24.70 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.44 | ▲ 09:30 equity $9,845.85 vs yday $9,617.08 (+228.77) | 09:30 open · cash $60.44 (unchanged overnight, no fees) · equity $9,845.85 vs prior close $9,617.08 (+228.77) · 8 name(s) re-marked at the open (per-name table). DVLT×7129 yday $0.16 → 09:30 $0.17 +71.29; BRUN×76 yday $16.68 → 09:30 $17.44 +57.76; AXTI×17 yday $67.75 → 09:30 $69.72 +33.49; ARQT×46 yday $26.46 → 09:30 $26.14 -14.72; SMTC×7 yday $178.19 → 09:30 $182.33 +28.98; SABR×505 yday $2.32 → 09:30 $2.29 -15.15; CIFR×67 yday $16.94 → 09:30 $17.80 +57.62; EROC×95 yday $12.90 → 09:30 $13.00 +9.50 | — |
| 2026-09-18 09:30 ET | **SELL** | `DVLT` | 7129 | $0.17 | $34.70 | $-68.21 | $1,237.67 | ▼ -68.21 after sell → book $9,811.15; vs 09:30 mark -34.70 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRUN` | 76 | $17.44 | $2.24 | $+114.86 | $2,560.87 | ▲ +114.86 after sell → book $9,808.91; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AXTI` | 17 | $69.72 | $2.06 | $+26.67 | $3,744.05 | ▲ +26.67 after sell → book $9,806.85; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 46 | $26.14 | $2.15 | $+4.46 | $4,944.34 | ▲ +4.46 after sell → book $9,804.70; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $6,218.62 | ▲ +76.32 after sell → book $9,802.67; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 505 | $2.29 | $6.61 | $-68.67 | $7,368.46 | ▼ -68.67 after sell → book $9,796.06; vs 09:30 mark -6.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 67 | $17.80 | $2.21 | $-20.15 | $8,558.85 | ▼ -20.15 after sell → book $9,793.85; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `EROC` | 95 | $13.00 | $2.30 | $+29.62 | $9,791.55 | ▲ +29.62 after sell → book $9,791.55; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 1442 | $0.97 | $18.31 | — | $8,374.50 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1398.79 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 354 | $3.95 | $4.57 | — | $6,971.63 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1398.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 99 | $14.07 | $2.29 | — | $5,576.41 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1398.79 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 239 | $5.83 | $3.08 | — | $4,179.96 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1398.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 390 | $3.58 | $5.03 | — | $2,778.73 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1398.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RANI` | 1645 | $0.85 | $18.92 | — | $1,361.56 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+3.6; leftover $1398.79 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 91 | $14.79 | $2.26 | — | $13.41 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1398.79 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.41 | ▼ close $9,620.23 vs 09:30 $9,845.85 (session -116.86) | 16:00 close · cash $13.41 · equity $9,620.23 vs 09:30 $9,845.85 (-225.62; session marks -116.86) · 7 name(s) marked open→close (per-name table). TLSA×1442 09:30 $0.97 → close $0.91 -86.52; EYPT×354 09:30 $3.95 → close $3.85 -35.40; BHVN×99 09:30 $14.07 → close $13.62 -44.55; BNC×239 09:30 $5.83 → close $5.98 +35.85; DDD×390 09:30 $3.58 → close $3.63 +19.50; RANI×1645 09:30 $0.85 → close $0.86 +19.74; RARE×91 09:30 $14.79 → close $14.51 -25.48 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.41 | ▲ 09:30 equity $9,845.59 vs yday $9,620.23 (+225.36) | 09:30 open · cash $13.41 (unchanged overnight, no fees) · equity $9,845.59 vs prior close $9,620.23 (+225.36) · 7 name(s) re-marked at the open (per-name table). TLSA×1442 yday $0.91 → 09:30 $0.94 +43.26; EYPT×354 yday $3.85 → 09:30 $3.87 +7.08; BHVN×99 yday $13.62 → 09:30 $13.90 +27.72; BNC×239 yday $5.98 → 09:30 $6.42 +103.96; DDD×390 yday $3.63 → 09:30 $3.71 +31.20; RANI×1645 yday $0.86 → 09:30 $0.86 +3.95; RARE×91 yday $14.51 → 09:30 $14.60 +8.19 | — |
| 2026-09-21 09:30 ET | **SELL** | `TLSA` | 1442 | $0.94 | $18.13 | $-79.70 | $1,350.76 | ▼ -79.70 after sell → book $9,827.46; vs 09:30 mark -18.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 354 | $3.87 | $4.64 | $-37.52 | $2,716.10 | ▼ -37.52 after sell → book $9,822.83; vs 09:30 mark -4.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 99 | $13.90 | $2.31 | $-21.43 | $4,089.89 | ▼ -21.43 after sell → book $9,820.51; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BNC` | 239 | $6.42 | $3.13 | $+133.60 | $5,619.94 | ▲ +133.60 after sell → book $9,817.38; vs 09:30 mark -3.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DDD` | 390 | $3.71 | $5.11 | $+40.56 | $7,061.73 | ▲ +40.56 after sell → book $9,812.27; vs 09:30 mark -5.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RANI` | 1645 | $0.86 | $19.44 | $-14.67 | $8,464.23 | ▼ -14.67 after sell → book $9,792.83; vs 09:30 mark -19.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 91 | $14.60 | $2.29 | $-21.84 | $9,790.54 | ▼ -21.84 after sell → book $9,790.54; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 145 | $13.44 | $2.42 | — | $7,839.32 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1958.11 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1780 | $1.10 | $22.96 | — | $5,858.35 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1958.11 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 196 | $9.99 | $2.58 | — | $3,897.74 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1958.11 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 1072 | $1.82 | $13.83 | — | $1,927.51 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1958.11 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `COIN` | 9 | $205.50 | $2.02 | — | $75.99 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-9.1; leftover $1958.11 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.99 | ▼ close $9,566.86 vs 09:30 $9,845.59 (session -179.87) | 16:00 close · cash $75.99 · equity $9,566.86 vs 09:30 $9,845.59 (-278.73; session marks -179.87) · 5 name(s) marked open→close (per-name table). BTDR×145 09:30 $13.44 → close $13.14 -43.50; ORBS×1780 09:30 $1.10 → close $1.05 -89.00; SBET×196 09:30 $9.99 → close $9.98 -1.96; BTBT×1072 09:30 $1.82 → close $1.82 -5.36; COIN×9 09:30 $205.50 → close $201.05 -40.05 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `IRD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CAN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `USDE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NTAP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `BTDR` | 145 | 2026-09-21 @ $13.44 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1958.11 |
| `ORBS` | 1780 | 2026-09-21 @ $1.10 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1958.11 |
| `SBET` | 196 | 2026-09-21 @ $9.99 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1958.11 |
| `BTBT` | 1072 | 2026-09-21 @ $1.82 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1958.11 |
| `COIN` | 9 | 2026-09-21 @ $205.50 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-9.1; leftover $1958.11 |
