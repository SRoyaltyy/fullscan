# Factor mine action — `union_white_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-7.10%** ($9,290) · signal-only (no cash/fees) was -4.17%. Starts YES **0/24**. Fills 130 · skips 0 · realized $-614.16.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: no morning camera is red (the 'white' / all-clear row).
- Must-have: prior 5-session return is at most 10% (not already exploded).
- Must-have: prior relative volume is at most 2.2 (not a blow-off).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

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
- **Gate** `zero_red=True,ret_5_max=10.0,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $230.40.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 33 | — | $59.80 | +0.00 | $60.23 | +14.19 | +14.19 | +0.00 | +14.19 |
| 2026-08-13 | `TGTX` | 40 | — | $49.70 | +0.00 | $47.94 | -70.40 | -70.40 | +0.00 | -70.40 |
| 2026-08-13 | `SLS` | 170 | — | $11.70 | +0.00 | $12.36 | +112.20 | +112.20 | +0.00 | +112.20 |
| 2026-08-13 | `HIMS` | 67 | — | $29.74 | +0.00 | $28.77 | -64.99 | -64.99 | +0.00 | -64.99 |
| 2026-08-13 | `VOR` | 90 | — | $22.01 | +0.00 | $23.29 | +115.20 | +115.20 | +0.00 | +115.20 |
| 2026-08-14 | `BTSG` | 33 | $60.23 | $59.65 | -19.14 | — | +0.00 | -19.14 | -4.95 | — |
| 2026-08-14 | `TGTX` | 40 | $47.94 | $47.27 | -26.80 | — | +0.00 | -26.80 | -97.20 | — |
| 2026-08-14 | `SLS` | 170 | $12.36 | $12.40 | +6.80 | — | +0.00 | +6.80 | +119.00 | — |
| 2026-08-14 | `HIMS` | 67 | $28.77 | $29.15 | +25.46 | — | +0.00 | +25.46 | -39.53 | — |
| 2026-08-14 | `VOR` | 90 | $23.29 | $23.33 | +3.60 | — | +0.00 | +3.60 | +118.80 | — |
| 2026-08-14 | `DAVE` | 3 | — | $330.91 | +0.00 | $334.57 | +10.98 | +10.98 | +0.00 | +10.98 |
| 2026-08-14 | `SLG` | 21 | — | $57.61 | +0.00 | $56.09 | -31.92 | -31.92 | +0.00 | -31.92 |
| 2026-08-14 | `ANGX` | 292 | — | $4.31 | +0.00 | $4.37 | +17.52 | +17.52 | +0.00 | +17.52 |
| 2026-08-14 | `HYLN` | 301 | — | $4.18 | +0.00 | $4.06 | -36.12 | -36.12 | +0.00 | -36.12 |
| 2026-08-14 | `WDC` | 2 | — | $503.50 | +0.00 | $508.80 | +10.60 | +10.60 | +0.00 | +10.60 |
| 2026-08-14 | `ADUR` | 76 | — | $16.50 | +0.00 | $16.17 | -25.08 | -25.08 | +0.00 | -25.08 |
| 2026-08-14 | `DLO` | 82 | — | $15.28 | +0.00 | $14.17 | -91.02 | -91.02 | +0.00 | -91.02 |
| 2026-08-14 | `KULR` | 503 | — | $2.50 | +0.00 | $2.64 | +70.42 | +70.42 | +0.00 | +70.42 |
| 2026-08-17 | `DAVE` | 3 | $334.57 | $336.94 | +7.11 | — | +0.00 | +7.11 | +18.09 | — |
| 2026-08-17 | `SLG` | 21 | $56.09 | $55.37 | -15.12 | — | +0.00 | -15.12 | -47.04 | — |
| 2026-08-17 | `ANGX` | 292 | $4.37 | $4.60 | +67.16 | — | +0.00 | +67.16 | +84.68 | — |
| 2026-08-17 | `HYLN` | 301 | $4.06 | $4.10 | +12.04 | — | +0.00 | +12.04 | -24.08 | — |
| 2026-08-17 | `WDC` | 2 | $508.80 | $525.53 | +33.46 | — | +0.00 | +33.46 | +44.06 | — |
| 2026-08-17 | `ADUR` | 76 | $16.17 | $15.73 | -33.44 | — | +0.00 | -33.44 | -58.52 | — |
| 2026-08-17 | `DLO` | 82 | $14.17 | $14.23 | +4.92 | — | +0.00 | +4.92 | -86.10 | — |
| 2026-08-17 | `KULR` | 503 | $2.64 | $2.63 | -5.03 | — | +0.00 | -5.03 | +65.39 | — |
| 2026-08-17 | `DNN` | 618 | — | $3.24 | +0.00 | $3.19 | -30.90 | -30.90 | +0.00 | -30.90 |
| 2026-08-17 | `CDNL` | 50 | — | $39.85 | +0.00 | $39.23 | -31.00 | -31.00 | +0.00 | -31.00 |
| 2026-08-17 | `OCC` | 109 | — | $18.24 | +0.00 | $17.12 | -122.08 | -122.08 | +0.00 | -122.08 |
| 2026-08-17 | `MRLN` | 534 | — | $3.75 | +0.00 | $3.54 | -114.81 | -114.81 | +0.00 | -114.81 |
| 2026-08-17 | `CSAN` | 801 | — | $2.50 | +0.00 | $2.52 | +16.02 | +16.02 | +0.00 | +16.02 |
| 2026-08-18 | `DNN` | 618 | $3.19 | $3.11 | -49.44 | — | +0.00 | -49.44 | -80.34 | — |
| 2026-08-18 | `CDNL` | 50 | $39.23 | $41.57 | +117.00 | — | +0.00 | +117.00 | +86.00 | — |
| 2026-08-18 | `OCC` | 109 | $17.12 | $16.20 | -100.28 | — | +0.00 | -100.28 | -222.36 | — |
| 2026-08-18 | `MRLN` | 534 | $3.54 | $3.50 | -18.69 | — | +0.00 | -18.69 | -133.50 | — |
| 2026-08-18 | `CSAN` | 801 | $2.52 | $2.51 | -8.01 | — | +0.00 | -8.01 | +8.01 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 58 | — | $20.55 | +0.00 | $21.19 | +37.12 | +37.12 | +0.00 | +37.12 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `HDSN` | 208 | — | $5.77 | +0.00 | $5.57 | -41.60 | -41.60 | +0.00 | -41.60 |
| 2026-08-20 | `IAG` | 61 | — | $19.63 | +0.00 | $20.50 | +53.07 | +53.07 | +0.00 | +53.07 |
| 2026-08-20 | `KGC` | 40 | — | $29.63 | +0.00 | $31.43 | +72.00 | +72.00 | +0.00 | +72.00 |
| 2026-08-20 | `NFGC` | 687 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `MRVI` | 161 | — | $7.44 | +0.00 | $8.29 | +136.85 | +136.85 | +0.00 | +136.85 |
| 2026-08-20 | `SCZM` | 127 | — | $9.46 | +0.00 | $9.76 | +38.10 | +38.10 | +0.00 | +38.10 |
| 2026-08-21 | `AG` | 58 | $21.19 | $21.90 | +41.18 | — | +0.00 | +41.18 | +78.30 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `HDSN` | 208 | $5.57 | $5.67 | +20.80 | — | +0.00 | +20.80 | -20.80 | — |
| 2026-08-21 | `IAG` | 61 | $20.50 | $21.17 | +40.87 | — | +0.00 | +40.87 | +93.94 | — |
| 2026-08-21 | `KGC` | 40 | $31.43 | $32.17 | +29.60 | — | +0.00 | +29.60 | +101.60 | — |
| 2026-08-21 | `NFGC` | 687 | $1.75 | $1.79 | +27.48 | — | +0.00 | +27.48 | +27.48 | — |
| 2026-08-21 | `MRVI` | 161 | $8.29 | $8.28 | -1.61 | — | +0.00 | -1.61 | +135.24 | — |
| 2026-08-21 | `SCZM` | 127 | $9.76 | $10.26 | +63.50 | — | +0.00 | +63.50 | +101.60 | — |
| 2026-08-21 | `CRSP` | 28 | — | $59.72 | +0.00 | $59.50 | -6.16 | -6.16 | +0.00 | -6.16 |
| 2026-08-21 | `CF` | 13 | — | $127.43 | +0.00 | $129.60 | +28.21 | +28.21 | +0.00 | +28.21 |
| 2026-08-21 | `EMBC` | 311 | — | $5.43 | +0.00 | $5.23 | -62.20 | -62.20 | +0.00 | -62.20 |
| 2026-08-21 | `TXG` | 26 | — | $64.39 | +0.00 | $65.12 | +18.98 | +18.98 | +0.00 | +18.98 |
| 2026-08-21 | `BEKE` | 94 | — | $17.93 | +0.00 | $17.75 | -17.39 | -17.39 | +0.00 | -17.39 |
| 2026-08-21 | `HITI` | 695 | — | $2.43 | +0.00 | $2.45 | +13.90 | +13.90 | +0.00 | +13.90 |
| 2026-08-24 | `CRSP` | 28 | $59.50 | $58.75 | -21.00 | — | +0.00 | -21.00 | -27.16 | — |
| 2026-08-24 | `CF` | 13 | $129.60 | $129.99 | +5.07 | — | +0.00 | +5.07 | +33.28 | — |
| 2026-08-24 | `EMBC` | 311 | $5.23 | $5.20 | -10.89 | — | +0.00 | -10.89 | -73.08 | — |
| 2026-08-24 | `TXG` | 26 | $65.12 | $63.15 | -51.22 | — | +0.00 | -51.22 | -32.24 | — |
| 2026-08-24 | `BEKE` | 94 | $17.75 | $18.05 | +28.67 | — | +0.00 | +28.67 | +11.28 | — |
| 2026-08-24 | `HITI` | 695 | $2.45 | $2.45 | +0.00 | — | +0.00 | +0.00 | +13.90 | — |
| 2026-08-25 | `CRMD` | 150 | — | $8.35 | +0.00 | $8.56 | +31.50 | +31.50 | +0.00 | +31.50 |
| 2026-08-25 | `ELMT` | 70 | — | $17.89 | +0.00 | $17.75 | -9.80 | -9.80 | +0.00 | -9.80 |
| 2026-08-25 | `AMTX` | 659 | — | $1.90 | +0.00 | $1.91 | +6.59 | +6.59 | +0.00 | +6.59 |
| 2026-08-25 | `BZ` | 82 | — | $15.28 | +0.00 | $16.29 | +82.82 | +82.82 | +0.00 | +82.82 |
| 2026-08-25 | `VIPS` | 89 | — | $13.96 | +0.00 | $14.16 | +17.80 | +17.80 | +0.00 | +17.80 |
| 2026-08-25 | `RHI` | 28 | — | $43.76 | +0.00 | $44.90 | +31.92 | +31.92 | +0.00 | +31.92 |
| 2026-08-25 | `VALE` | 83 | — | $15.01 | +0.00 | $15.33 | +26.56 | +26.56 | +0.00 | +26.56 |
| 2026-08-25 | `AYA` | 47 | — | $26.21 | +0.00 | $27.71 | +70.50 | +70.50 | +0.00 | +70.50 |
| 2026-08-26 | `CRMD` | 150 | $8.56 | $8.60 | +6.00 | — | +0.00 | +6.00 | +37.50 | — |
| 2026-08-26 | `ELMT` | 70 | $17.75 | $17.82 | +4.90 | — | +0.00 | +4.90 | -4.90 | — |
| 2026-08-26 | `AMTX` | 659 | $1.91 | $1.91 | +0.00 | — | +0.00 | +0.00 | +6.59 | — |
| 2026-08-26 | `BZ` | 82 | $16.29 | $16.77 | +39.36 | — | +0.00 | +39.36 | +122.18 | — |
| 2026-08-26 | `VIPS` | 89 | $14.16 | $14.00 | -14.24 | — | +0.00 | -14.24 | +3.56 | — |
| 2026-08-26 | `RHI` | 28 | $44.90 | $44.33 | -15.96 | — | +0.00 | -15.96 | +15.96 | — |
| 2026-08-26 | `VALE` | 83 | $15.33 | $15.37 | +3.32 | — | +0.00 | +3.32 | +29.88 | — |
| 2026-08-26 | `AYA` | 47 | $27.71 | $27.24 | -22.09 | — | +0.00 | -22.09 | +48.41 | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | `TTMI` | 10 | — | $122.81 | +0.00 | $118.65 | -41.60 | -41.60 | +0.00 | -41.60 |
| 2026-08-28 | `KEYS` | 3 | — | $324.41 | +0.00 | $319.97 | -13.32 | -13.32 | +0.00 | -13.32 |
| 2026-08-28 | `AVT` | 13 | — | $91.49 | +0.00 | $88.63 | -37.18 | -37.18 | +0.00 | -37.18 |
| 2026-08-28 | `CGNX` | 20 | — | $62.82 | +0.00 | $60.46 | -47.20 | -47.20 | +0.00 | -47.20 |
| 2026-08-28 | `COHR` | 4 | — | $289.44 | +0.00 | $279.20 | -40.96 | -40.96 | +0.00 | -40.96 |
| 2026-08-28 | `LSCC` | 10 | — | $119.76 | +0.00 | $114.40 | -53.60 | -53.60 | +0.00 | -53.60 |
| 2026-08-28 | `MTSI` | 4 | — | $275.20 | +0.00 | $265.27 | -39.72 | -39.72 | +0.00 | -39.72 |
| 2026-08-28 | `OLED` | 15 | — | $85.02 | +0.00 | $82.98 | -30.60 | -30.60 | +0.00 | -30.60 |
| 2026-08-31 | `TTMI` | 10 | $118.65 | $118.83 | +1.80 | — | +0.00 | +1.80 | -39.80 | — |
| 2026-08-31 | `KEYS` | 3 | $319.97 | $322.49 | +7.56 | — | +0.00 | +7.56 | -5.76 | — |
| 2026-08-31 | `AVT` | 13 | $88.63 | $89.39 | +9.88 | — | +0.00 | +9.88 | -27.30 | — |
| 2026-08-31 | `CGNX` | 20 | $60.46 | $60.46 | +0.00 | — | +0.00 | +0.00 | -47.20 | — |
| 2026-08-31 | `COHR` | 4 | $279.20 | $280.25 | +4.20 | — | +0.00 | +4.20 | -36.76 | — |
| 2026-08-31 | `LSCC` | 10 | $114.40 | $115.56 | +11.60 | — | +0.00 | +11.60 | -42.00 | — |
| 2026-08-31 | `MTSI` | 4 | $265.27 | $266.96 | +6.76 | — | +0.00 | +6.76 | -32.96 | — |
| 2026-08-31 | `OLED` | 15 | $82.98 | $83.28 | +4.50 | — | +0.00 | +4.50 | -26.10 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 23 | — | $52.88 | +0.00 | $52.46 | -9.66 | -9.66 | +0.00 | -9.66 |
| 2026-09-03 | `HRMY` | 28 | — | $42.93 | +0.00 | $41.86 | -29.96 | -29.96 | +0.00 | -29.96 |
| 2026-09-03 | `CABA` | 342 | — | $3.63 | +0.00 | $3.48 | -51.30 | -51.30 | +0.00 | -51.30 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `ARCT` | 74 | — | $16.77 | +0.00 | $15.56 | -89.54 | -89.54 | +0.00 | -89.54 |
| 2026-09-03 | `CRDL` | 570 | — | $2.18 | +0.00 | $2.16 | -11.40 | -11.40 | +0.00 | -11.40 |
| 2026-09-03 | `SDGR` | 59 | — | $21.03 | +0.00 | $20.71 | -18.88 | -18.88 | +0.00 | -18.88 |
| 2026-09-03 | `VIR` | 107 | — | $11.54 | +0.00 | $11.45 | -9.63 | -9.63 | +0.00 | -9.63 |
| 2026-09-04 | `ATRC` | 23 | $52.46 | $52.03 | -9.89 | $51.52 | -11.73 | -21.62 | -19.55 | -31.28 |
| 2026-09-04 | `HRMY` | 28 | $41.86 | $41.50 | -10.08 | $42.25 | +21.00 | +10.92 | -40.04 | -19.04 |
| 2026-09-04 | `CABA` | 342 | $3.48 | $3.46 | -6.84 | $3.47 | +3.42 | -3.42 | -58.14 | -54.72 |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `ARCT` | 74 | $15.56 | $15.61 | +3.70 | — | +0.00 | +3.70 | -85.84 | — |
| 2026-09-04 | `CRDL` | 570 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -11.40 | — |
| 2026-09-04 | `SDGR` | 59 | $20.71 | $20.58 | -7.67 | — | +0.00 | -7.67 | -26.55 | — |
| 2026-09-04 | `VIR` | 107 | $11.45 | $11.31 | -14.98 | $11.38 | +8.02 | -6.96 | -24.61 | -16.58 |
| 2026-09-04 | `ALEC` | 483 | — | $2.52 | +0.00 | $2.46 | -28.98 | -28.98 | +0.00 | -28.98 |
| 2026-09-04 | `BHC` | 181 | — | $6.71 | +0.00 | $6.56 | -27.15 | -27.15 | +0.00 | -27.15 |
| 2026-09-04 | `OABI` | 254 | — | $4.78 | +0.00 | $4.33 | -114.30 | -114.30 | +0.00 | -114.30 |
| 2026-09-04 | `CRM` | 4 | — | $263.36 | +0.00 | $259.23 | -16.52 | -16.52 | +0.00 | -16.52 |
| 2026-09-08 | `ATRC` | 23 | $51.52 | $54.31 | +64.17 | — | +0.00 | +64.17 | +32.89 | — |
| 2026-09-08 | `HRMY` | 28 | $42.25 | $42.20 | -1.40 | — | +0.00 | -1.40 | -20.44 | — |
| 2026-09-08 | `CABA` | 342 | $3.47 | $3.43 | -13.68 | — | +0.00 | -13.68 | -68.40 | — |
| 2026-09-08 | `VIR` | 107 | $11.38 | $11.22 | -17.65 | — | +0.00 | -17.65 | -34.24 | — |
| 2026-09-08 | `ALEC` | 483 | $2.46 | $2.38 | -38.64 | — | +0.00 | -38.64 | -67.62 | — |
| 2026-09-08 | `BHC` | 181 | $6.56 | $6.57 | +1.81 | — | +0.00 | +1.81 | -25.34 | — |
| 2026-09-08 | `OABI` | 254 | $4.33 | $4.30 | -7.62 | — | +0.00 | -7.62 | -121.92 | — |
| 2026-09-08 | `CRM` | 4 | $259.23 | $253.72 | -22.04 | — | +0.00 | -22.04 | -38.56 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `BAND` | 59 | — | $52.55 | +0.00 | $56.87 | +254.88 | +254.88 | +0.00 | +254.88 |
| 2026-09-11 | `PAGS` | 309 | — | $10.11 | +0.00 | $10.12 | +3.09 | +3.09 | +0.00 | +3.09 |
| 2026-09-11 | `ZSQR` | 962 | — | $3.25 | +0.00 | $3.07 | -173.16 | -173.16 | +0.00 | -173.16 |
| 2026-09-14 | `BAND` | 59 | $56.87 | $56.90 | +1.77 | — | +0.00 | +1.77 | +256.65 | — |
| 2026-09-14 | `PAGS` | 309 | $10.12 | $10.00 | -37.08 | — | +0.00 | -37.08 | -33.99 | — |
| 2026-09-14 | `ZSQR` | 962 | $3.07 | $3.06 | -9.62 | — | +0.00 | -9.62 | -182.78 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `IQV` | 8 | — | $270.89 | +0.00 | $268.82 | -16.56 | -16.56 | +0.00 | -16.56 |
| 2026-09-16 | `RDNT` | 30 | — | $77.12 | +0.00 | $75.78 | -40.20 | -40.20 | +0.00 | -40.20 |
| 2026-09-16 | `AVAH` | 163 | — | $14.31 | +0.00 | $14.26 | -8.15 | -8.15 | +0.00 | -8.15 |
| 2026-09-16 | `BLFS` | 64 | — | $36.46 | +0.00 | $36.11 | -22.40 | -22.40 | +0.00 | -22.40 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +106.20 | BTSG, TGTX, SLS, HIMS, VOR | — | $64.97 | $10,095.05 | BTSG×33, TGTX×40, SLS×170, HIMS×67, VOR×90 |
| 2026-08-14 | +5.50 | $64.97 | BTSG×33, TGTX×40, SLS×170, HIMS×67, VOR×90 | $10,084.97 | -10.08 | -74.62 | DAVE, SLG, ANGX, HYLN, WDC, ADUR, DLO, KULR | BTSG, TGTX, SLS, HIMS, VOR | $558.33 | $9,974.41 | DAVE×3, SLG×21, ANGX×292, HYLN×301, WDC×2, ADUR×76, DLO×82, KULR×503 |
| 2026-08-17 | +2.25 | $558.33 | DAVE×3, SLG×21, ANGX×292, HYLN×301, WDC×2, ADUR×76, DLO×82, KULR×503 | $10,045.51 | +71.10 | -282.77 | DNN, CDNL, OCC, MRLN, CSAN | DAVE, SLG, ANGX, HYLN, WDC, ADUR, DLO, KULR | $2.91 | $9,708.12 | DNN×618, CDNL×50, OCC×109, MRLN×534, CSAN×801 |
| 2026-08-18 | -6.20 | $2.91 | DNN×618, CDNL×50, OCC×109, MRLN×534, CSAN×801 | $9,648.70 | -59.42 | +0.00 | — | DNN, CDNL, OCC, MRLN, CSAN | $9,618.62 | $9,618.62 | — |
| 2026-08-19 | -7.20 | $9,618.62 | — | $9,618.62 | +0.00 | +0.00 | — | — | $9,618.62 | $9,618.62 | — |
| 2026-08-20 | +1.12 | $9,618.62 | — | $9,618.62 | +0.00 | +329.60 | AG, BHP, HDSN, IAG, KGC, NFGC, MRVI, SCZM | — | $34.43 | $9,923.36 | AG×58, BHP×13, HDSN×208, IAG×61, KGC×40, NFGC×687, MRVI×161, SCZM×127 |
| 2026-08-21 | +3.25 | $34.43 | AG×58, BHP×13, HDSN×208, IAG×61, KGC×40, NFGC×687, MRVI×161, SCZM×127 | $10,172.35 | +248.99 | -24.66 | CRSP, CF, EMBC, TXG, BEKE, HITI | AG, BHP, HDSN, IAG, KGC, NFGC, MRVI, SCZM | $59.38 | $10,101.08 | CRSP×28, CF×13, EMBC×311, TXG×26, BEKE×94, HITI×695 |
| 2026-08-24 | -5.17 | $59.38 | CRSP×28, CF×13, EMBC×311, TXG×26, BEKE×94, HITI×695 | $10,051.72 | -49.36 | +0.00 | — | CRSP, CF, EMBC, TXG, BEKE, HITI | $10,030.01 | $10,030.01 | — |
| 2026-08-25 | +1.80 | $10,030.01 | — | $10,030.01 | -0.00 | +257.89 | CRMD, ELMT, AMTX, BZ, VIPS, RHI, VALE, AYA | — | $50.65 | $10,263.82 | CRMD×150, ELMT×70, AMTX×659, BZ×82, VIPS×89, RHI×28, VALE×83, AYA×47 |
| 2026-08-26 | +2.02 | $50.65 | CRMD×150, ELMT×70, AMTX×659, BZ×82, VIPS×89, RHI×28, VALE×83, AYA×47 | $10,265.11 | +1.29 | +0.00 | — | CRMD, ELMT, AMTX, BZ, VIPS, RHI, VALE, AYA | $10,240.74 | $10,240.74 | — |
| 2026-08-27 | — | $10,240.74 | — | $10,240.74 | +0.00 | +0.00 | — | — | $10,240.74 | $10,240.74 | — |
| 2026-08-28 | +0.75 | $10,240.74 | — | $10,240.74 | +0.00 | -304.18 | TTMI, KEYS, AVT, CGNX, COHR, LSCC, MTSI, OLED | — | $846.02 | $9,920.40 | TTMI×10, KEYS×3, AVT×13, CGNX×20, COHR×4, LSCC×10, MTSI×4, OLED×15 |
| 2026-08-31 | -5.85 | $846.02 | TTMI×10, KEYS×3, AVT×13, CGNX×20, COHR×4, LSCC×10, MTSI×4, OLED×15 | $9,966.70 | +46.30 | +0.00 | — | TTMI, KEYS, AVT, CGNX, COHR, LSCC, MTSI, OLED | $9,950.39 | $9,950.39 | — |
| 2026-09-01 | -6.30 | $9,950.39 | — | $9,950.39 | -0.00 | +0.00 | — | — | $9,950.39 | $9,950.39 | — |
| 2026-09-02 | -3.83 | $9,950.39 | — | $9,950.39 | -0.00 | +0.00 | — | — | $9,950.39 | $9,950.39 | — |
| 2026-09-03 | -0.90 | $9,950.39 | — | $9,950.39 | -0.00 | -236.75 | ATRC, HRMY, CABA, RVTY, ARCT, CRDL, SDGR, VIR | — | $114.86 | $9,689.03 | ATRC×23, HRMY×28, CABA×342, RVTY×9, ARCT×74, CRDL×570, SDGR×59, VIR×107 |
| 2026-09-04 | +2.25 | $114.86 | ATRC×23, HRMY×28, CABA×342, RVTY×9, ARCT×74, CRDL×570, SDGR×59, VIR×107 | $9,637.87 | -51.16 | -166.24 | ALEC, BHC, OABI, CRM | RVTY, ARCT, CRDL, SDGR | $158.50 | $9,443.68 | ATRC×23, HRMY×28, CABA×342, VIR×107, ALEC×483, BHC×181, OABI×254, CRM×4 |
| 2026-09-08 | -11.47 | $158.50 | ATRC×23, HRMY×28, CABA×342, VIR×107, ALEC×483, BHC×181, OABI×254, CRM×4 | $9,408.62 | -35.06 | +0.00 | — | ATRC, HRMY, CABA, VIR, ALEC, BHC, OABI, CRM | $9,383.39 | $9,383.39 | — |
| 2026-09-09 | -13.95 | $9,383.39 | — | $9,383.39 | -0.00 | +0.00 | — | — | $9,383.39 | $9,383.39 | — |
| 2026-09-10 | -13.28 | $9,383.39 | — | $9,383.39 | -0.00 | +0.00 | — | — | $9,383.39 | $9,383.39 | — |
| 2026-09-11 | +0.50 | $9,383.39 | — | $9,383.39 | -0.00 | +84.81 | BAND, PAGS, ZSQR | — | $13.89 | $9,449.64 | BAND×59, PAGS×309, ZSQR×962 |
| 2026-09-14 | -11.00 | $13.89 | BAND×59, PAGS×309, ZSQR×962 | $9,404.71 | -44.93 | +0.00 | — | BAND, PAGS, ZSQR | $9,385.85 | $9,385.85 | — |
| 2026-09-15 | -3.84 | $9,385.85 | — | $9,385.85 | -0.00 | +0.00 | — | — | $9,385.85 | $9,385.85 | — |
| 2026-09-16 | +5.30 | $9,385.85 | — | $9,385.85 | -0.00 | -87.31 | IQV, RDNT, AVAH, BLFS | — | $230.40 | $9,289.78 | IQV×8, RDNT×30, AVAH×163, BLFS×64 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 33 | $59.80 | $2.09 | — | $8,024.51 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 40 | $49.70 | $2.11 | — | $6,034.40 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $2000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 170 | $11.70 | $2.50 | — | $4,042.90 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $2000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 67 | $29.74 | $2.19 | — | $2,048.13 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 90 | $22.01 | $2.26 | — | $64.97 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $64.97 | ▲ close $10,095.05 vs 09:30 $10,000.00 (session +106.20) | 16:00 close · cash $64.97 · equity $10,095.05 vs 09:30 $10,000.00 (+95.05; session marks +106.20) · 5 name(s) marked open→close (per-name table). BTSG×33 09:30 $59.80 → close $60.23 +14.19; TGTX×40 09:30 $49.70 → close $47.94 -70.40; SLS×170 09:30 $11.70 → close $12.36 +112.20; HIMS×67 09:30 $29.74 → close $28.77 -64.99; VOR×90 09:30 $22.01 → close $23.29 +115.20 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.97 | ▼ 09:30 equity $10,084.97 vs yday $10,095.05 (-10.08) | 09:30 open · cash $64.97 (unchanged overnight, no fees) · equity $10,084.97 vs prior close $10,095.05 (-10.08) · 5 name(s) re-marked at the open (per-name table). BTSG×33 yday $60.23 → 09:30 $59.65 -19.14; TGTX×40 yday $47.94 → 09:30 $47.27 -26.80; SLS×170 yday $12.36 → 09:30 $12.40 +6.80; HIMS×67 yday $28.77 → 09:30 $29.15 +25.46; VOR×90 yday $23.29 → 09:30 $23.33 +3.60 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 33 | $59.65 | $2.11 | $-9.15 | $2,031.31 | ▼ -9.15 after sell → book $10,082.86; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 40 | $47.27 | $2.14 | $-101.45 | $3,919.97 | ▼ -101.45 after sell → book $10,080.72; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 170 | $12.40 | $2.55 | $+113.95 | $6,025.43 | ▲ +113.95 after sell → book $10,078.18; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 67 | $29.15 | $2.22 | $-43.94 | $7,976.26 | ▼ -43.94 after sell → book $10,075.96; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 90 | $23.33 | $2.29 | $+114.25 | $10,073.67 | ▲ +114.25 after sell → book $10,073.67; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $9,078.94 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1259.21 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $7,867.07 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1259.21 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 292 | $4.31 | $3.77 | — | $6,604.79 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1259.21 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 301 | $4.18 | $3.88 | — | $5,342.72 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1259.21 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $4,333.73 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable; 🔵; ⚪; ret5=+7.9; leftover $1259.21 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 76 | $16.50 | $2.22 | — | $3,077.51 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1259.21 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DLO` | 82 | $15.28 | $2.24 | — | $1,822.31 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=-0.1; leftover $1259.21 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `KULR` | 503 | $2.50 | $6.49 | — | $558.33 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+7.6; leftover $1259.21 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $558.33 | ▼ close $9,974.41 vs 09:30 $10,084.97 (session -74.62) | 16:00 close · cash $558.33 · equity $9,974.41 vs 09:30 $10,084.97 (-110.56; session marks -74.62) · 8 name(s) marked open→close (per-name table). DAVE×3 09:30 $330.91 → close $334.57 +10.98; SLG×21 09:30 $57.61 → close $56.09 -31.92; ANGX×292 09:30 $4.31 → close $4.37 +17.52; HYLN×301 09:30 $4.18 → close $4.06 -36.12; WDC×2 09:30 $503.50 → close $508.80 +10.60; ADUR×76 09:30 $16.50 → close $16.17 -25.08; DLO×82 09:30 $15.28 → close $14.17 -91.02; KULR×503 09:30 $2.50 → close $2.64 +70.42 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $558.33 | ▲ 09:30 equity $10,045.51 vs yday $9,974.41 (+71.10) | 09:30 open · cash $558.33 (unchanged overnight, no fees) · equity $10,045.51 vs prior close $9,974.41 (+71.10) · 8 name(s) re-marked at the open (per-name table). DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×21 yday $56.09 → 09:30 $55.37 -15.12; ANGX×292 yday $4.37 → 09:30 $4.60 +67.16; HYLN×301 yday $4.06 → 09:30 $4.10 +12.04; WDC×2 yday $508.80 → 09:30 $525.53 +33.46; ADUR×76 yday $16.17 → 09:30 $15.73 -33.44; DLO×82 yday $14.17 → 09:30 $14.23 +4.92; KULR×503 yday $2.64 → 09:30 $2.63 -5.03 | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $1,567.13 | ▲ +14.07 after sell → book $10,043.49; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 21 | $55.37 | $2.07 | $-51.17 | $2,727.82 | ▼ -51.17 after sell → book $10,041.41; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 292 | $4.60 | $3.83 | $+77.09 | $4,067.20 | ▲ +77.09 after sell → book $10,037.59; vs 09:30 mark -3.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 301 | $4.10 | $3.94 | $-31.91 | $5,297.35 | ▼ -31.91 after sell → book $10,033.64; vs 09:30 mark -3.95 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $6,346.40 | ▲ +40.05 after sell → book $10,031.63; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 76 | $15.73 | $2.24 | $-62.98 | $7,539.64 | ▼ -62.98 after sell → book $10,029.39; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DLO` | 82 | $14.23 | $2.26 | $-90.60 | $8,704.24 | ▼ -90.60 after sell → book $10,027.13; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `KULR` | 503 | $2.63 | $6.58 | $+52.32 | $10,020.55 | ▲ +52.32 after sell → book $10,020.55; vs 09:30 mark -6.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 618 | $3.24 | $7.97 | — | $8,010.25 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $2004.11 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 50 | $39.85 | $2.14 | — | $6,015.61 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $2004.11 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 109 | $18.24 | $2.32 | — | $4,025.14 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $2004.11 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `MRLN` | 534 | $3.75 | $6.89 | — | $2,015.75 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=-15.4; leftover $2004.11 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CSAN` | 801 | $2.50 | $10.33 | — | $2.91 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=-12.5; leftover $2004.11 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.91 | ▼ close $9,708.12 vs 09:30 $10,045.51 (session -282.77) | 16:00 close · cash $2.91 · equity $9,708.12 vs 09:30 $10,045.51 (-337.39; session marks -282.77) · 5 name(s) marked open→close (per-name table). DNN×618 09:30 $3.24 → close $3.19 -30.90; CDNL×50 09:30 $39.85 → close $39.23 -31.00; OCC×109 09:30 $18.24 → close $17.12 -122.08; MRLN×534 09:30 $3.75 → close $3.54 -114.81; CSAN×801 09:30 $2.50 → close $2.52 +16.02 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.91 | ▼ 09:30 equity $9,648.70 vs yday $9,708.12 (-59.42) | 09:30 open · cash $2.91 (unchanged overnight, no fees) · equity $9,648.70 vs prior close $9,708.12 (-59.42) · 5 name(s) re-marked at the open (per-name table). DNN×618 yday $3.19 → 09:30 $3.11 -49.44; CDNL×50 yday $39.23 → 09:30 $41.57 +117.00; OCC×109 yday $17.12 → 09:30 $16.20 -100.28; MRLN×534 yday $3.54 → 09:30 $3.50 -18.69; CSAN×801 yday $2.52 → 09:30 $2.51 -8.01 | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 618 | $3.11 | $8.09 | $-96.40 | $1,916.80 | ▼ -96.40 after sell → book $9,640.61; vs 09:30 mark -8.09 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 50 | $41.57 | $2.17 | $+81.69 | $3,993.14 | ▲ +81.69 after sell → book $9,638.45; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 109 | $16.20 | $2.35 | $-227.03 | $5,756.59 | ▼ -227.03 after sell → book $9,636.10; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `MRLN` | 534 | $3.50 | $6.99 | $-147.38 | $7,618.60 | ▼ -147.38 after sell → book $9,629.11; vs 09:30 mark -6.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CSAN` | 801 | $2.51 | $10.48 | $-12.80 | $9,618.62 | ▼ -12.80 after sell → book $9,618.62; vs 09:30 mark -10.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,618.62 | ▲ close $9,618.62 vs 09:30 $9,648.70 (session +0.00) | 16:00 close · cash $9,618.62 · no lots left · equity $9,618.62. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,618.62 | ▲ 09:30 equity $9,618.62 vs yday $9,618.62 (+0.00) | 09:30 open · cash $9,618.62 · no holdings · equity $9,618.62 vs prior close $9,618.62 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,618.62 | ▲ close $9,618.62 vs 09:30 $9,618.62 (session +0.00) | 16:00 close · cash $9,618.62 · no lots left · equity $9,618.62. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,618.62 | ▲ 09:30 equity $9,618.62 vs yday $9,618.62 (+0.00) | 09:30 open · cash $9,618.62 · no holdings · equity $9,618.62 vs prior close $9,618.62 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,424.56 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1202.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,239.40 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1202.33 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 208 | $5.77 | $2.68 | — | $6,036.56 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1202.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $4,836.96 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1202.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $3,649.65 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1202.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 687 | $1.75 | $8.86 | — | $2,438.53 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1202.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 161 | $7.44 | $2.47 | — | $1,238.22 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1202.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 127 | $9.46 | $2.37 | — | $34.43 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1202.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.43 | ▲ close $9,923.36 vs 09:30 $9,618.62 (session +329.60) | 16:00 close · cash $34.43 · equity $9,923.36 vs 09:30 $9,618.62 (+304.74; session marks +329.60) · 8 name(s) marked open→close (per-name table). AG×58 09:30 $20.55 → close $21.19 +37.12; BHP×13 09:30 $91.01 → close $93.63 +34.06; HDSN×208 09:30 $5.77 → close $5.57 -41.60; IAG×61 09:30 $19.63 → close $20.50 +53.07; KGC×40 09:30 $29.63 → close $31.43 +72.00; NFGC×687 09:30 $1.75 → close $1.75 +0.00; MRVI×161 09:30 $7.44 → close $8.29 +136.85; SCZM×127 09:30 $9.46 → close $9.76 +38.10 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.43 | ▲ 09:30 equity $10,172.35 vs yday $9,923.36 (+248.99) | 09:30 open · cash $34.43 (unchanged overnight, no fees) · equity $10,172.35 vs prior close $9,923.36 (+248.99) · 8 name(s) re-marked at the open (per-name table). AG×58 yday $21.19 → 09:30 $21.90 +41.18; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; HDSN×208 yday $5.57 → 09:30 $5.67 +20.80; IAG×61 yday $20.50 → 09:30 $21.17 +40.87; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×687 yday $1.75 → 09:30 $1.79 +27.48; MRVI×161 yday $8.29 → 09:30 $8.28 -1.61; SCZM×127 yday $9.76 → 09:30 $10.26 +63.50 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 58 | $21.90 | $2.18 | $+73.95 | $1,302.44 | ▲ +73.95 after sell → book $10,170.16; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,544.76 | ▲ +57.15 after sell → book $10,168.12; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 208 | $5.67 | $2.73 | $-26.21 | $3,721.39 | ▼ -26.21 after sell → book $10,165.39; vs 09:30 mark -2.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 61 | $21.17 | $2.19 | $+89.57 | $5,010.56 | ▲ +89.57 after sell → book $10,163.19; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $6,295.23 | ▲ +97.36 after sell → book $10,161.06; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 687 | $1.79 | $8.99 | $+9.63 | $7,515.98 | ▲ +9.63 after sell → book $10,152.08; vs 09:30 mark -8.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRVI` | 161 | $8.28 | $2.51 | $+130.26 | $8,846.55 | ▲ +130.26 after sell → book $10,149.57; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 127 | $10.26 | $2.40 | $+96.83 | $10,147.17 | ▲ +96.83 after sell → book $10,147.17; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 28 | $59.72 | $2.07 | — | $8,472.93 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1691.19 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 13 | $127.43 | $2.03 | — | $6,814.31 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable; 🔵; ⚪; ret5=+7.9; leftover $1691.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 311 | $5.43 | $4.01 | — | $5,121.57 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $1691.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TXG` | 26 | $64.39 | $2.07 | — | $3,445.36 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1691.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 94 | $17.93 | $2.27 | — | $1,757.20 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $1691.19 | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 695 | $2.43 | $8.97 | — | $59.38 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $1691.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.38 | ▼ close $10,101.08 vs 09:30 $10,172.35 (session -24.66) | 16:00 close · cash $59.38 · equity $10,101.08 vs 09:30 $10,172.35 (-71.27; session marks -24.66) · 6 name(s) marked open→close (per-name table). CRSP×28 09:30 $59.72 → close $59.50 -6.16; CF×13 09:30 $127.43 → close $129.60 +28.21; EMBC×311 09:30 $5.43 → close $5.23 -62.20; TXG×26 09:30 $64.39 → close $65.12 +18.98; BEKE×94 09:30 $17.93 → close $17.75 -17.39; HITI×695 09:30 $2.43 → close $2.45 +13.90 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.38 | ▼ 09:30 equity $10,051.72 vs yday $10,101.08 (-49.36) | 09:30 open · cash $59.38 (unchanged overnight, no fees) · equity $10,051.72 vs prior close $10,101.08 (-49.36) · 6 name(s) re-marked at the open (per-name table). CRSP×28 yday $59.50 → 09:30 $58.75 -21.00; CF×13 yday $129.60 → 09:30 $129.99 +5.07; EMBC×311 yday $5.23 → 09:30 $5.20 -10.89; TXG×26 yday $65.12 → 09:30 $63.15 -51.22; BEKE×94 yday $17.75 → 09:30 $18.05 +28.67; HITI×695 yday $2.45 → 09:30 $2.45 +0.00 | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 28 | $58.75 | $2.10 | $-31.33 | $1,702.29 | ▼ -31.33 after sell → book $10,049.62; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 13 | $129.99 | $2.05 | $+29.20 | $3,390.10 | ▲ +29.20 after sell → book $10,047.57; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `EMBC` | 311 | $5.20 | $4.08 | $-81.17 | $5,001.67 | ▼ -81.17 after sell → book $10,043.49; vs 09:30 mark -4.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TXG` | 26 | $63.15 | $2.09 | $-36.40 | $6,641.48 | ▼ -36.40 after sell → book $10,041.40; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 94 | $18.05 | $2.30 | $+6.71 | $8,336.35 | ▲ +6.71 after sell → book $10,039.10; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HITI` | 695 | $2.45 | $9.09 | $-4.16 | $10,030.01 | ▼ -4.16 after sell → book $10,030.01; vs 09:30 mark -9.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,030.01 | ▲ close $10,030.01 vs 09:30 $10,051.72 (session +0.00) | 16:00 close · cash $10,030.01 · no lots left · equity $10,030.01. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,030.01 | ▲ 09:30 equity $10,030.01 vs yday $10,030.01 (-0.00) | 09:30 open · cash $10,030.01 · no holdings · equity $10,030.01 vs prior close $10,030.01 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 150 | $8.35 | $2.44 | — | $8,775.07 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1253.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ELMT` | 70 | $17.89 | $2.20 | — | $7,520.57 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=-7.5; leftover $1253.75 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 659 | $1.90 | $8.50 | — | $6,259.97 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=+5.0; leftover $1253.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 82 | $15.28 | $2.24 | — | $5,004.77 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=-0.7; leftover $1253.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `VIPS` | 89 | $13.96 | $2.26 | — | $3,760.07 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+2.0; leftover $1253.75 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 28 | $43.76 | $2.07 | — | $2,532.72 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $1253.75 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VALE` | 83 | $15.01 | $2.24 | — | $1,284.65 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; ⚪; ret5=+9.4; leftover $1253.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AYA` | 47 | $26.21 | $2.13 | — | $50.65 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; ⚪; ret5=-0.3; leftover $1253.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.65 | ▲ close $10,263.82 vs 09:30 $10,030.01 (session +257.89) | 16:00 close · cash $50.65 · equity $10,263.82 vs 09:30 $10,030.01 (+233.81; session marks +257.89) · 8 name(s) marked open→close (per-name table). CRMD×150 09:30 $8.35 → close $8.56 +31.50; ELMT×70 09:30 $17.89 → close $17.75 -9.80; AMTX×659 09:30 $1.90 → close $1.91 +6.59; BZ×82 09:30 $15.28 → close $16.29 +82.82; VIPS×89 09:30 $13.96 → close $14.16 +17.80; RHI×28 09:30 $43.76 → close $44.90 +31.92; VALE×83 09:30 $15.01 → close $15.33 +26.56; AYA×47 09:30 $26.21 → close $27.71 +70.50 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.65 | ▲ 09:30 equity $10,265.11 vs yday $10,263.82 (+1.29) | 09:30 open · cash $50.65 (unchanged overnight, no fees) · equity $10,265.11 vs prior close $10,263.82 (+1.29) · 8 name(s) re-marked at the open (per-name table). CRMD×150 yday $8.56 → 09:30 $8.60 +6.00; ELMT×70 yday $17.75 → 09:30 $17.82 +4.90; AMTX×659 yday $1.91 → 09:30 $1.91 +0.00; BZ×82 yday $16.29 → 09:30 $16.77 +39.36; VIPS×89 yday $14.16 → 09:30 $14.00 -14.24; RHI×28 yday $44.90 → 09:30 $44.33 -15.96; VALE×83 yday $15.33 → 09:30 $15.37 +3.32; AYA×47 yday $27.71 → 09:30 $27.24 -22.09 | — |
| 2026-08-26 09:30 ET | **SELL** | `CRMD` | 150 | $8.60 | $2.48 | $+32.58 | $1,338.17 | ▲ +32.58 after sell → book $10,262.63; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `ELMT` | 70 | $17.82 | $2.22 | $-9.32 | $2,583.35 | ▼ -9.32 after sell → book $10,260.41; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AMTX` | 659 | $1.91 | $8.62 | $-10.53 | $3,833.42 | ▼ -10.53 after sell → book $10,251.79; vs 09:30 mark -8.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BZ` | 82 | $16.77 | $2.26 | $+117.68 | $5,206.30 | ▲ +117.68 after sell → book $10,249.53; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `VIPS` | 89 | $14.00 | $2.28 | $-0.98 | $6,450.02 | ▼ -0.98 after sell → book $10,247.25; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 28 | $44.33 | $2.09 | $+11.79 | $7,689.16 | ▲ +11.79 after sell → book $10,245.15; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `VALE` | 83 | $15.37 | $2.26 | $+25.38 | $8,962.61 | ▲ +25.38 after sell → book $10,242.89; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AYA` | 47 | $27.24 | $2.15 | $+44.13 | $10,240.74 | ▲ +44.13 after sell → book $10,240.74; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,240.74 | ▲ close $10,240.74 vs 09:30 $10,265.11 (session +0.00) | 16:00 close · cash $10,240.74 · no lots left · equity $10,240.74. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,240.74 | ▲ 09:30 equity $10,240.74 vs yday $10,240.74 (+0.00) | 09:30 open · cash $10,240.74 · no holdings · equity $10,240.74 vs prior close $10,240.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,240.74 | ▲ close $10,240.74 vs 09:30 $10,240.74 (session +0.00) | 16:00 close · cash $10,240.74 · no lots left · equity $10,240.74. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,240.74 | ▲ 09:30 equity $10,240.74 vs yday $10,240.74 (+0.00) | 09:30 open · cash $10,240.74 · no holdings · equity $10,240.74 vs prior close $10,240.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $122.81 | $2.02 | — | $9,010.62 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1280.09 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $8,035.39 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1280.09 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 13 | $91.49 | $2.03 | — | $6,843.99 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1280.09 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 20 | $62.82 | $2.05 | — | $5,585.54 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1280.09 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $4,425.78 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1280.09 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 10 | $119.76 | $2.02 | — | $3,226.16 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1280.09 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MTSI` | 4 | $275.20 | $2.00 | — | $2,123.36 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+4.1; leftover $1280.09 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OLED` | 15 | $85.02 | $2.04 | — | $846.02 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-1.9; leftover $1280.09 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $846.02 | ▼ close $9,920.40 vs 09:30 $10,240.74 (session -304.18) | 16:00 close · cash $846.02 · equity $9,920.40 vs 09:30 $10,240.74 (-320.34; session marks -304.18) · 8 name(s) marked open→close (per-name table). TTMI×10 09:30 $122.81 → close $118.65 -41.60; KEYS×3 09:30 $324.41 → close $319.97 -13.32; AVT×13 09:30 $91.49 → close $88.63 -37.18; CGNX×20 09:30 $62.82 → close $60.46 -47.20; COHR×4 09:30 $289.44 → close $279.20 -40.96; LSCC×10 09:30 $119.76 → close $114.40 -53.60; MTSI×4 09:30 $275.20 → close $265.27 -39.72; OLED×15 09:30 $85.02 → close $82.98 -30.60 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $846.02 | ▲ 09:30 equity $9,966.70 vs yday $9,920.40 (+46.30) | 09:30 open · cash $846.02 (unchanged overnight, no fees) · equity $9,966.70 vs prior close $9,920.40 (+46.30) · 8 name(s) re-marked at the open (per-name table). TTMI×10 yday $118.65 → 09:30 $118.83 +1.80; KEYS×3 yday $319.97 → 09:30 $322.49 +7.56; AVT×13 yday $88.63 → 09:30 $89.39 +9.88; CGNX×20 yday $60.46 → 09:30 $60.46 +0.00; COHR×4 yday $279.20 → 09:30 $280.25 +4.20; LSCC×10 yday $114.40 → 09:30 $115.56 +11.60; MTSI×4 yday $265.27 → 09:30 $266.96 +6.76; OLED×15 yday $82.98 → 09:30 $83.28 +4.50 | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 10 | $118.83 | $2.04 | $-43.86 | $2,032.28 | ▼ -43.86 after sell → book $9,964.66; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 3 | $322.49 | $2.02 | $-9.78 | $2,997.73 | ▼ -9.78 after sell → book $9,962.64; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 13 | $89.39 | $2.05 | $-31.38 | $4,157.76 | ▼ -31.38 after sell → book $9,960.60; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 20 | $60.46 | $2.07 | $-51.32 | $5,364.89 | ▼ -51.32 after sell → book $9,958.53; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $280.25 | $2.02 | $-40.78 | $6,483.86 | ▼ -40.78 after sell → book $9,956.50; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 10 | $115.56 | $2.04 | $-46.06 | $7,637.42 | ▼ -46.06 after sell → book $9,954.46; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MTSI` | 4 | $266.96 | $2.02 | $-36.98 | $8,703.24 | ▼ -36.98 after sell → book $9,952.44; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OLED` | 15 | $83.28 | $2.06 | $-30.19 | $9,950.39 | ▼ -30.19 after sell → book $9,950.39; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,950.39 | ▲ close $9,950.39 vs 09:30 $9,966.70 (session +0.00) | 16:00 close · cash $9,950.39 · no lots left · equity $9,950.39. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,950.39 | ▲ 09:30 equity $9,950.39 vs yday $9,950.39 (-0.00) | 09:30 open · cash $9,950.39 · no holdings · equity $9,950.39 vs prior close $9,950.39 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,950.39 | ▲ close $9,950.39 vs 09:30 $9,950.39 (session +0.00) | 16:00 close · cash $9,950.39 · no lots left · equity $9,950.39. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,950.39 | ▲ 09:30 equity $9,950.39 vs yday $9,950.39 (-0.00) | 09:30 open · cash $9,950.39 · no holdings · equity $9,950.39 vs prior close $9,950.39 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,950.39 | ▲ close $9,950.39 vs 09:30 $9,950.39 (session +0.00) | 16:00 close · cash $9,950.39 · no lots left · equity $9,950.39. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,950.39 | ▲ 09:30 equity $9,950.39 vs yday $9,950.39 (-0.00) | 09:30 open · cash $9,950.39 · no holdings · equity $9,950.39 vs prior close $9,950.39 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $8,732.09 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1243.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 28 | $42.93 | $2.07 | — | $7,527.97 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1243.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 342 | $3.63 | $4.41 | — | $6,282.10 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1243.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $5,088.03 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1243.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.77 | $2.21 | — | $3,844.84 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1243.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 570 | $2.18 | $7.35 | — | $2,594.89 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1243.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SDGR` | 59 | $21.03 | $2.17 | — | $1,351.95 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $1243.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 107 | $11.54 | $2.31 | — | $114.86 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $1243.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $114.86 | ▼ close $9,689.03 vs 09:30 $9,950.39 (session -236.75) | 16:00 close · cash $114.86 · equity $9,689.03 vs 09:30 $9,950.39 (-261.36; session marks -236.75) · 8 name(s) marked open→close (per-name table). ATRC×23 09:30 $52.88 → close $52.46 -9.66; HRMY×28 09:30 $42.93 → close $41.86 -29.96; CABA×342 09:30 $3.63 → close $3.48 -51.30; RVTY×9 09:30 $132.45 → close $130.63 -16.38; ARCT×74 09:30 $16.77 → close $15.56 -89.54; CRDL×570 09:30 $2.18 → close $2.16 -11.40; SDGR×59 09:30 $21.03 → close $20.71 -18.88; VIR×107 09:30 $11.54 → close $11.45 -9.63 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $114.86 | ▼ 09:30 equity $9,637.87 vs yday $9,689.03 (-51.16) | 09:30 open · cash $114.86 (unchanged overnight, no fees) · equity $9,637.87 vs prior close $9,689.03 (-51.16) · 8 name(s) re-marked at the open (per-name table). ATRC×23 yday $52.46 → 09:30 $52.03 -9.89; HRMY×28 yday $41.86 → 09:30 $41.50 -10.08; CABA×342 yday $3.48 → 09:30 $3.46 -6.84; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; ARCT×74 yday $15.56 → 09:30 $15.61 +3.70; CRDL×570 yday $2.16 → 09:30 $2.16 +0.00; SDGR×59 yday $20.71 → 09:30 $20.58 -7.67; VIR×107 yday $11.45 → 09:30 $11.31 -14.98 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $1,283.09 | ▼ -25.83 after sell → book $9,635.83; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 74 | $15.61 | $2.23 | $-90.29 | $2,436.00 | ▼ -90.29 after sell → book $9,633.60; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 570 | $2.16 | $7.46 | $-26.21 | $3,659.74 | ▼ -26.21 after sell → book $9,626.14; vs 09:30 mark -7.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SDGR` | 59 | $20.58 | $2.19 | $-30.90 | $4,871.78 | ▼ -30.90 after sell → book $9,623.96; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 483 | $2.52 | $6.23 | — | $3,648.39 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1217.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 181 | $6.71 | $2.53 | — | $2,431.34 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1217.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 254 | $4.78 | $3.28 | — | $1,213.95 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1217.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $158.50 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1217.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.50 | ▼ close $9,443.68 vs 09:30 $9,637.87 (session -166.24) | 16:00 close · cash $158.50 · equity $9,443.68 vs 09:30 $9,637.87 (-194.19; session marks -166.24) · 8 name(s) marked open→close (per-name table). ATRC×23 09:30 $52.03 → close $51.52 -11.73; HRMY×28 09:30 $41.50 → close $42.25 +21.00; CABA×342 09:30 $3.46 → close $3.47 +3.42; VIR×107 09:30 $11.31 → close $11.38 +8.02; ALEC×483 09:30 $2.52 → close $2.46 -28.98; BHC×181 09:30 $6.71 → close $6.56 -27.15; OABI×254 09:30 $4.78 → close $4.33 -114.30; CRM×4 09:30 $263.36 → close $259.23 -16.52 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.50 | ▼ 09:30 equity $9,408.62 vs yday $9,443.68 (-35.06) | 09:30 open · cash $158.50 (unchanged overnight, no fees) · equity $9,408.62 vs prior close $9,443.68 (-35.06) · 8 name(s) re-marked at the open (per-name table). ATRC×23 yday $51.52 → 09:30 $54.31 +64.17; HRMY×28 yday $42.25 → 09:30 $42.20 -1.40; CABA×342 yday $3.47 → 09:30 $3.43 -13.68; VIR×107 yday $11.38 → 09:30 $11.22 -17.65; ALEC×483 yday $2.46 → 09:30 $2.38 -38.64; BHC×181 yday $6.56 → 09:30 $6.57 +1.81; OABI×254 yday $4.33 → 09:30 $4.30 -7.62; CRM×4 yday $259.23 → 09:30 $253.72 -22.04 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 23 | $54.31 | $2.08 | $+28.75 | $1,405.55 | ▲ +28.75 after sell → book $9,406.54; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 28 | $42.20 | $2.09 | $-24.61 | $2,585.06 | ▼ -24.61 after sell → book $9,404.45; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 342 | $3.43 | $4.48 | $-77.29 | $3,753.64 | ▼ -77.29 after sell → book $9,399.97; vs 09:30 mark -4.48 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 107 | $11.22 | $2.34 | $-38.89 | $4,951.84 | ▼ -38.89 after sell → book $9,397.63; vs 09:30 mark -2.34 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 483 | $2.38 | $6.32 | $-80.17 | $6,095.06 | ▼ -80.17 after sell → book $9,391.31; vs 09:30 mark -6.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 181 | $6.57 | $2.57 | $-30.45 | $7,281.66 | ▼ -30.45 after sell → book $9,388.74; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 254 | $4.30 | $3.33 | $-128.53 | $8,370.53 | ▼ -128.53 after sell → book $9,385.41; vs 09:30 mark -3.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $9,383.39 | ▼ -42.58 after sell → book $9,383.39; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,383.39 | ▲ close $9,383.39 vs 09:30 $9,408.62 (session +0.00) | 16:00 close · cash $9,383.39 · no lots left · equity $9,383.39. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,383.39 | ▲ 09:30 equity $9,383.39 vs yday $9,383.39 (-0.00) | 09:30 open · cash $9,383.39 · no holdings · equity $9,383.39 vs prior close $9,383.39 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,383.39 | ▲ close $9,383.39 vs 09:30 $9,383.39 (session +0.00) | 16:00 close · cash $9,383.39 · no lots left · equity $9,383.39. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,383.39 | ▲ 09:30 equity $9,383.39 vs yday $9,383.39 (-0.00) | 09:30 open · cash $9,383.39 · no holdings · equity $9,383.39 vs prior close $9,383.39 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,383.39 | ▲ close $9,383.39 vs 09:30 $9,383.39 (session +0.00) | 16:00 close · cash $9,383.39 · no lots left · equity $9,383.39. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,383.39 | ▲ 09:30 equity $9,383.39 vs yday $9,383.39 (-0.00) | 09:30 open · cash $9,383.39 · no holdings · equity $9,383.39 vs prior close $9,383.39 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 59 | $52.55 | $2.17 | — | $6,280.77 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $3127.80 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 309 | $10.11 | $3.99 | — | $3,152.80 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $3127.80 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 962 | $3.25 | $12.41 | — | $13.89 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+3.6; leftover $3127.80 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.89 | ▲ close $9,449.64 vs 09:30 $9,383.39 (session +84.81) | 16:00 close · cash $13.89 · equity $9,449.64 vs 09:30 $9,383.39 (+66.25; session marks +84.81) · 3 name(s) marked open→close (per-name table). BAND×59 09:30 $52.55 → close $56.87 +254.88; PAGS×309 09:30 $10.11 → close $10.12 +3.09; ZSQR×962 09:30 $3.25 → close $3.07 -173.16 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.89 | ▼ 09:30 equity $9,404.71 vs yday $9,449.64 (-44.93) | 09:30 open · cash $13.89 (unchanged overnight, no fees) · equity $9,404.71 vs prior close $9,449.64 (-44.93) · 3 name(s) re-marked at the open (per-name table). BAND×59 yday $56.87 → 09:30 $56.90 +1.77; PAGS×309 yday $10.12 → 09:30 $10.00 -37.08; ZSQR×962 yday $3.07 → 09:30 $3.06 -9.62 | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 59 | $56.90 | $2.20 | $+252.28 | $3,368.78 | ▲ +252.28 after sell → book $9,402.50; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `PAGS` | 309 | $10.00 | $4.06 | $-42.04 | $6,454.72 | ▼ -42.04 after sell → book $9,398.44; vs 09:30 mark -4.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ZSQR` | 962 | $3.06 | $12.59 | $-207.78 | $9,385.85 | ▼ -207.78 after sell → book $9,385.85; vs 09:30 mark -12.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,385.85 | ▲ close $9,385.85 vs 09:30 $9,404.71 (session +0.00) | 16:00 close · cash $9,385.85 · no lots left · equity $9,385.85. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,385.85 | ▲ 09:30 equity $9,385.85 vs yday $9,385.85 (-0.00) | 09:30 open · cash $9,385.85 · no holdings · equity $9,385.85 vs prior close $9,385.85 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,385.85 | ▲ close $9,385.85 vs 09:30 $9,385.85 (session +0.00) | 16:00 close · cash $9,385.85 · no lots left · equity $9,385.85. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,385.85 | ▲ 09:30 equity $9,385.85 vs yday $9,385.85 (-0.00) | 09:30 open · cash $9,385.85 · no holdings · equity $9,385.85 vs prior close $9,385.85 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 8 | $270.89 | $2.01 | — | $7,216.71 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $2346.46 | join🟢 sector🟢 gen🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 30 | $77.12 | $2.08 | — | $4,901.03 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.1; leftover $2346.46 | join🟢 sector🟢 gen🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 163 | $14.31 | $2.48 | — | $2,566.02 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+3.2; leftover $2346.46 | join🟢 sector🟢 gen🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 64 | $36.46 | $2.18 | — | $230.40 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-4.6; leftover $2346.46 | join🟢 sector🟢 gen🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $230.40 | ▼ close $9,289.78 vs 09:30 $9,385.85 (session -87.31) | 16:00 close · cash $230.40 · equity $9,289.78 vs 09:30 $9,385.85 (-96.07; session marks -87.31) · 4 name(s) marked open→close (per-name table). IQV×8 09:30 $270.89 → close $268.82 -16.56; RDNT×30 09:30 $77.12 → close $75.78 -40.20; AVAH×163 09:30 $14.31 → close $14.26 -8.15; BLFS×64 09:30 $36.46 → close $36.11 -22.40 | — |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `IQV` | 8 | 2026-09-16 @ $270.89 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $2346.46 |
| `RDNT` | 30 | 2026-09-16 @ $77.12 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.1; leftover $2346.46 |
| `AVAH` | 163 | 2026-09-16 @ $14.31 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+3.2; leftover $2346.46 |
| `BLFS` | 64 | 2026-09-16 @ $36.46 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-4.6; leftover $2346.46 |
