# Factor mine action — `union_clk_mom_break_peer_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `hot_score` · size `leftover` · sell `list` · S-boost `none` · Clock-B #1 mom+breakout+peer/sector (research; not KEEP)

Cash book **+3.05%** ($10,305) · signal-only (no cash/fees) was +11.47%. Starts YES **26/26**. Fills 190 · skips 84 · realized $+221.77.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how hot the prior tape looked.
- Must-have: Clock-B #1: moderate prior momentum, a completed 10-session breakout (or candle capture), and peer or sector camera green.
- Must-not: Clock-B #5 long veto: extreme prior extension plus diminishing progress or a failed breakout.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how hot the prior tape looked and keep the top 8.
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
- **Gate** `clk_mom_break_peer=True` · **rank** `hot_score` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $225.14.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ADUR` | 75 | — | $16.50 | +0.00 | $16.17 | -24.75 | -24.75 | +0.00 | -24.75 |
| 2026-08-14 | `ENS` | 6 | — | $196.00 | +0.00 | $203.40 | +44.40 | +44.40 | +0.00 | +44.40 |
| 2026-08-14 | `GEMI` | 320 | — | $3.90 | +0.00 | $3.92 | +6.40 | +6.40 | +0.00 | +6.40 |
| 2026-08-14 | `SLG` | 21 | — | $57.61 | +0.00 | $56.09 | -31.92 | -31.92 | +0.00 | -31.92 |
| 2026-08-14 | `ZIM` | 45 | — | $27.25 | +0.00 | $28.14 | +40.05 | +40.05 | +0.00 | +40.05 |
| 2026-08-14 | `MRLN` | 301 | — | $4.15 | +0.00 | $3.75 | -118.90 | -118.90 | +0.00 | -118.90 |
| 2026-08-14 | `ANGX` | 290 | — | $4.31 | +0.00 | $4.37 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-14 | `YSS` | 124 | — | $10.06 | +0.00 | $10.93 | +107.88 | +107.88 | +0.00 | +107.88 |
| 2026-08-17 | `ADUR` | 75 | $16.17 | $15.73 | -33.00 | — | +0.00 | -33.00 | -57.75 | — |
| 2026-08-17 | `ENS` | 6 | $203.40 | $205.03 | +9.78 | — | +0.00 | +9.78 | +54.18 | — |
| 2026-08-17 | `GEMI` | 320 | $3.92 | $3.89 | -9.60 | — | +0.00 | -9.60 | -3.20 | — |
| 2026-08-17 | `SLG` | 21 | $56.09 | $55.37 | -15.12 | — | +0.00 | -15.12 | -47.04 | — |
| 2026-08-17 | `ZIM` | 45 | $28.14 | $28.83 | +31.05 | — | +0.00 | +31.05 | +71.10 | — |
| 2026-08-17 | `MRLN` | 301 | $3.75 | $3.75 | -1.50 | — | +0.00 | -1.50 | -120.40 | — |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `YSS` | 124 | $10.93 | $10.36 | -70.68 | — | +0.00 | -70.68 | +37.20 | — |
| 2026-08-17 | `OCC` | 109 | — | $18.24 | +0.00 | $17.12 | -122.08 | -122.08 | +0.00 | -122.08 |
| 2026-08-17 | `ALM` | 123 | — | $16.20 | +0.00 | $16.36 | +19.68 | +19.68 | +0.00 | +19.68 |
| 2026-08-17 | `DVN` | 43 | — | $46.18 | +0.00 | $47.57 | +59.77 | +59.77 | +0.00 | +59.77 |
| 2026-08-17 | `WBS` | 25 | — | $79.00 | +0.00 | $78.69 | -7.75 | -7.75 | +0.00 | -7.75 |
| 2026-08-17 | `FANG` | 9 | — | $202.70 | +0.00 | $206.29 | +32.31 | +32.31 | +0.00 | +32.31 |
| 2026-08-18 | `OCC` | 109 | $17.12 | $16.20 | -100.28 | — | +0.00 | -100.28 | -222.36 | — |
| 2026-08-18 | `ALM` | 123 | $16.36 | $15.78 | -71.34 | — | +0.00 | -71.34 | -51.66 | — |
| 2026-08-18 | `DVN` | 43 | $47.57 | $48.00 | +18.49 | — | +0.00 | +18.49 | +78.26 | — |
| 2026-08-18 | `WBS` | 25 | $78.69 | $78.52 | -4.25 | — | +0.00 | -4.25 | -12.00 | — |
| 2026-08-18 | `FANG` | 9 | $206.29 | $208.93 | +23.76 | — | +0.00 | +23.76 | +56.07 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `CABA` | 402 | — | $3.04 | +0.00 | $3.19 | +60.30 | +60.30 | +0.00 | +60.30 |
| 2026-08-20 | `IAG` | 62 | — | $19.63 | +0.00 | $20.50 | +53.94 | +53.94 | +0.00 | +53.94 |
| 2026-08-20 | `AG` | 59 | — | $20.55 | +0.00 | $21.19 | +37.76 | +37.76 | +0.00 | +37.76 |
| 2026-08-20 | `ABUS` | 248 | — | $4.92 | +0.00 | $4.77 | -37.20 | -37.20 | +0.00 | -37.20 |
| 2026-08-20 | `KGC` | 41 | — | $29.63 | +0.00 | $31.43 | +73.80 | +73.80 | +0.00 | +73.80 |
| 2026-08-20 | `COTY` | 480 | — | $2.55 | +0.00 | $2.75 | +96.00 | +96.00 | +0.00 | +96.00 |
| 2026-08-20 | `NFGC` | 699 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `ATAT` | 35 | — | $34.05 | +0.00 | $34.25 | +7.00 | +7.00 | +0.00 | +7.00 |
| 2026-08-21 | `CABA` | 402 | $3.19 | $3.20 | +4.02 | — | +0.00 | +4.02 | +64.32 | — |
| 2026-08-21 | `IAG` | 62 | $20.50 | $21.17 | +41.54 | — | +0.00 | +41.54 | +95.48 | — |
| 2026-08-21 | `AG` | 59 | $21.19 | $21.90 | +41.89 | — | +0.00 | +41.89 | +79.65 | — |
| 2026-08-21 | `ABUS` | 248 | $4.77 | $5.20 | +106.64 | — | +0.00 | +106.64 | +69.44 | — |
| 2026-08-21 | `KGC` | 41 | $31.43 | $32.17 | +30.34 | — | +0.00 | +30.34 | +104.14 | — |
| 2026-08-21 | `COTY` | 480 | $2.75 | $2.71 | -19.20 | — | +0.00 | -19.20 | +76.80 | — |
| 2026-08-21 | `NFGC` | 699 | $1.75 | $1.79 | +27.96 | — | +0.00 | +27.96 | +27.96 | — |
| 2026-08-21 | `ATAT` | 35 | $34.25 | $34.31 | +2.10 | — | +0.00 | +2.10 | +9.10 | — |
| 2026-08-21 | `DXYZ` | 36 | — | $34.89 | +0.00 | $34.43 | -16.56 | -16.56 | +0.00 | -16.56 |
| 2026-08-21 | `ORBS` | 1484 | — | $0.86 | +0.00 | $0.88 | +23.74 | +23.74 | +0.00 | +23.74 |
| 2026-08-21 | `GORO` | 412 | — | $3.11 | +0.00 | $3.19 | +32.96 | +32.96 | +0.00 | +32.96 |
| 2026-08-21 | `CF` | 10 | — | $127.43 | +0.00 | $129.60 | +21.70 | +21.70 | +0.00 | +21.70 |
| 2026-08-21 | `VIRT` | 21 | — | $60.66 | +0.00 | $67.93 | +152.67 | +152.67 | +0.00 | +152.67 |
| 2026-08-21 | `BTBT` | 772 | — | $1.66 | +0.00 | $1.53 | -100.36 | -100.36 | +0.00 | -100.36 |
| 2026-08-21 | `GMAB` | 38 | — | $33.36 | +0.00 | $33.45 | +3.42 | +3.42 | +0.00 | +3.42 |
| 2026-08-21 | `CRSP` | 21 | — | $59.72 | +0.00 | $59.50 | -4.62 | -4.62 | +0.00 | -4.62 |
| 2026-08-24 | `DXYZ` | 36 | $34.43 | $33.10 | -47.88 | — | +0.00 | -47.88 | -64.44 | — |
| 2026-08-24 | `ORBS` | 1484 | $0.88 | $0.89 | +14.84 | — | +0.00 | +14.84 | +38.58 | — |
| 2026-08-24 | `GORO` | 412 | $3.19 | $3.20 | +4.12 | — | +0.00 | +4.12 | +37.08 | — |
| 2026-08-24 | `CF` | 10 | $129.60 | $129.99 | +3.90 | — | +0.00 | +3.90 | +25.60 | — |
| 2026-08-24 | `VIRT` | 21 | $67.93 | $66.80 | -23.73 | — | +0.00 | -23.73 | +128.94 | — |
| 2026-08-24 | `BTBT` | 772 | $1.53 | $1.55 | +15.44 | — | +0.00 | +15.44 | -84.92 | — |
| 2026-08-24 | `GMAB` | 38 | $33.45 | $32.82 | -23.94 | — | +0.00 | -23.94 | -20.52 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | — | +0.00 | -15.75 | -20.37 | — |
| 2026-08-25 | `VALE` | 136 | — | $15.01 | +0.00 | $15.33 | +43.52 | +43.52 | +0.00 | +43.52 |
| 2026-08-25 | `KURA` | 150 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `DBRG` | 127 | — | $15.98 | +0.00 | $15.97 | -1.27 | -1.27 | +0.00 | -1.27 |
| 2026-08-25 | `HCA` | 4 | — | $426.97 | +0.00 | $428.76 | +7.16 | +7.16 | +0.00 | +7.16 |
| 2026-08-26 | `VALE` | 136 | $15.33 | $15.37 | +5.44 | — | +0.00 | +5.44 | +48.96 | — |
| 2026-08-26 | `KURA` | 150 | $13.59 | $13.63 | +6.00 | — | +0.00 | +6.00 | +6.00 | — |
| 2026-08-26 | `DBRG` | 127 | $15.97 | $15.97 | +0.00 | — | +0.00 | +0.00 | -1.27 | — |
| 2026-08-26 | `HCA` | 4 | $428.76 | $427.50 | -5.04 | — | +0.00 | -5.04 | +2.12 | — |
| 2026-08-26 | `ABX` | 173 | — | $9.83 | +0.00 | $9.78 | -8.65 | -8.65 | +0.00 | -8.65 |
| 2026-08-26 | `ACRS` | 261 | — | $6.53 | +0.00 | $6.19 | -88.74 | -88.74 | +0.00 | -88.74 |
| 2026-08-26 | `SJM` | 12 | — | $134.80 | +0.00 | $130.90 | -46.80 | -46.80 | +0.00 | -46.80 |
| 2026-08-26 | `BZ` | 101 | — | $16.77 | +0.00 | $18.84 | +209.07 | +209.07 | +0.00 | +209.07 |
| 2026-08-26 | `CRMD` | 198 | — | $8.60 | +0.00 | $8.39 | -41.58 | -41.58 | +0.00 | -41.58 |
| 2026-08-26 | `LI` | 140 | — | $12.14 | +0.00 | $12.14 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-27 | `ABX` | 173 | $9.78 | $9.68 | -17.30 | — | +0.00 | -17.30 | -25.95 | — |
| 2026-08-27 | `ACRS` | 261 | $6.19 | $6.15 | -10.44 | — | +0.00 | -10.44 | -99.18 | — |
| 2026-08-27 | `SJM` | 12 | $130.90 | $130.29 | -7.32 | — | +0.00 | -7.32 | -54.12 | — |
| 2026-08-27 | `BZ` | 101 | $18.84 | $18.50 | -34.34 | — | +0.00 | -34.34 | +174.73 | — |
| 2026-08-27 | `CRMD` | 198 | $8.39 | $8.49 | +19.80 | — | +0.00 | +19.80 | -21.78 | — |
| 2026-08-27 | `LI` | 140 | $12.14 | $12.35 | +29.40 | — | +0.00 | +29.40 | +29.40 | — |
| 2026-08-27 | `NCNO` | 58 | — | $22.03 | +0.00 | $23.32 | +74.82 | +74.82 | +0.00 | +74.82 |
| 2026-08-27 | `NABL` | 330 | — | $3.87 | +0.00 | $3.95 | +26.40 | +26.40 | +0.00 | +26.40 |
| 2026-08-27 | `INO` | 990 | — | $1.29 | +0.00 | $1.26 | -29.70 | -29.70 | +0.00 | -29.70 |
| 2026-08-27 | `SRRK` | 21 | — | $60.00 | +0.00 | $59.26 | -15.54 | -15.54 | +0.00 | -15.54 |
| 2026-08-27 | `PAGP` | 45 | — | $28.00 | +0.00 | $28.03 | +1.35 | +1.35 | +0.00 | +1.35 |
| 2026-08-27 | `DASH` | 5 | — | $235.94 | +0.00 | $231.89 | -20.25 | -20.25 | +0.00 | -20.25 |
| 2026-08-27 | `AEO` | 74 | — | $17.27 | +0.00 | $16.69 | -42.92 | -42.92 | +0.00 | -42.92 |
| 2026-08-27 | `PGY` | 55 | — | $22.93 | +0.00 | $23.26 | +18.15 | +18.15 | +0.00 | +18.15 |
| 2026-08-28 | `NCNO` | 58 | $23.32 | $23.30 | -1.16 | — | +0.00 | -1.16 | +73.66 | — |
| 2026-08-28 | `NABL` | 330 | $3.95 | $4.25 | +99.00 | — | +0.00 | +99.00 | +125.40 | — |
| 2026-08-28 | `INO` | 990 | $1.26 | $1.27 | +9.90 | — | +0.00 | +9.90 | -19.80 | — |
| 2026-08-28 | `SRRK` | 21 | $59.26 | $58.75 | -10.71 | — | +0.00 | -10.71 | -26.25 | — |
| 2026-08-28 | `PAGP` | 45 | $28.03 | $28.08 | +2.25 | — | +0.00 | +2.25 | +3.60 | — |
| 2026-08-28 | `DASH` | 5 | $231.89 | $233.37 | +7.40 | — | +0.00 | +7.40 | -12.85 | — |
| 2026-08-28 | `AEO` | 74 | $16.69 | $17.06 | +27.38 | — | +0.00 | +27.38 | -15.54 | — |
| 2026-08-28 | `PGY` | 55 | $23.26 | $23.21 | -2.75 | — | +0.00 | -2.75 | +15.40 | — |
| 2026-08-28 | `TH` | 67 | — | $19.00 | +0.00 | $18.55 | -30.15 | -30.15 | +0.00 | -30.15 |
| 2026-08-28 | `TLS` | 267 | — | $4.82 | +0.00 | $4.79 | -8.01 | -8.01 | +0.00 | -8.01 |
| 2026-08-28 | `RBRK` | 13 | — | $98.95 | +0.00 | $93.05 | -76.70 | -76.70 | +0.00 | -76.70 |
| 2026-08-28 | `ADSK` | 4 | — | $261.16 | +0.00 | $260.66 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-08-28 | `PD` | 98 | — | $13.09 | +0.00 | $13.83 | +72.52 | +72.52 | +0.00 | +72.52 |
| 2026-08-28 | `S` | 59 | — | $21.49 | +0.00 | $21.54 | +2.95 | +2.95 | +0.00 | +2.95 |
| 2026-08-28 | `ULTA` | 2 | — | $542.00 | +0.00 | $517.50 | -49.00 | -49.00 | +0.00 | -49.00 |
| 2026-08-28 | `HAFN` | 154 | — | $8.35 | +0.00 | $8.47 | +18.48 | +18.48 | +0.00 | +18.48 |
| 2026-08-31 | `TH` | 67 | $18.55 | $18.12 | -28.48 | — | +0.00 | -28.48 | -58.62 | — |
| 2026-08-31 | `TLS` | 267 | $4.79 | $4.81 | +5.34 | — | +0.00 | +5.34 | -2.67 | — |
| 2026-08-31 | `RBRK` | 13 | $93.05 | $92.83 | -2.86 | — | +0.00 | -2.86 | -79.56 | — |
| 2026-08-31 | `ADSK` | 4 | $260.66 | $257.71 | -11.80 | — | +0.00 | -11.80 | -13.80 | — |
| 2026-08-31 | `PD` | 98 | $13.83 | $13.58 | -24.50 | — | +0.00 | -24.50 | +48.02 | — |
| 2026-08-31 | `S` | 59 | $21.54 | $21.45 | -5.31 | — | +0.00 | -5.31 | -2.36 | — |
| 2026-08-31 | `ULTA` | 2 | $517.50 | $521.10 | +7.20 | — | +0.00 | +7.20 | -41.80 | — |
| 2026-08-31 | `HAFN` | 154 | $8.47 | $8.53 | +9.24 | — | +0.00 | +9.24 | +27.72 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `EBS` | 193 | — | $6.56 | +0.00 | $6.23 | -63.69 | -63.69 | +0.00 | -63.69 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `CABA` | 349 | — | $3.63 | +0.00 | $3.48 | -52.35 | -52.35 | +0.00 | -52.35 |
| 2026-09-03 | `VSTM` | 157 | — | $8.03 | +0.00 | $7.98 | -7.85 | -7.85 | +0.00 | -7.85 |
| 2026-09-03 | `GALT` | 281 | — | $4.50 | +0.00 | $4.35 | -42.15 | -42.15 | +0.00 | -42.15 |
| 2026-09-03 | `CTVA` | 14 | — | $90.24 | +0.00 | $88.62 | -22.68 | -22.68 | +0.00 | -22.68 |
| 2026-09-03 | `ATRC` | 23 | — | $52.88 | +0.00 | $52.46 | -9.66 | -9.66 | +0.00 | -9.66 |
| 2026-09-03 | `ETD` | 58 | — | $21.82 | +0.00 | $21.87 | +2.90 | +2.90 | +0.00 | +2.90 |
| 2026-09-04 | `EBS` | 193 | $6.23 | $6.26 | +5.79 | — | +0.00 | +5.79 | -57.90 | — |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | $524.14 | +20.72 | +15.50 | +54.94 | +75.66 |
| 2026-09-04 | `CABA` | 349 | $3.48 | $3.46 | -6.98 | — | +0.00 | -6.98 | -59.33 | — |
| 2026-09-04 | `VSTM` | 157 | $7.98 | $7.91 | -10.99 | — | +0.00 | -10.99 | -18.84 | — |
| 2026-09-04 | `GALT` | 281 | $4.35 | $4.33 | -5.62 | — | +0.00 | -5.62 | -47.77 | — |
| 2026-09-04 | `CTVA` | 14 | $88.62 | $87.64 | -13.72 | — | +0.00 | -13.72 | -36.40 | — |
| 2026-09-04 | `ATRC` | 23 | $52.46 | $52.03 | -9.89 | — | +0.00 | -9.89 | -19.55 | — |
| 2026-09-04 | `ETD` | 58 | $21.87 | $21.84 | -1.74 | — | +0.00 | -1.74 | +1.16 | — |
| 2026-09-04 | `GORO` | 321 | — | $3.95 | +0.00 | $4.15 | +64.20 | +64.20 | +0.00 | +64.20 |
| 2026-09-04 | `MSTR` | 9 | — | $137.35 | +0.00 | $142.80 | +49.05 | +49.05 | +0.00 | +49.05 |
| 2026-09-04 | `BLSH` | 36 | — | $34.69 | +0.00 | $36.00 | +47.16 | +47.16 | +0.00 | +47.16 |
| 2026-09-04 | `ZETA` | 38 | — | $32.65 | +0.00 | $31.35 | -49.40 | -49.40 | +0.00 | -49.40 |
| 2026-09-04 | `HAFN` | 142 | — | $8.94 | +0.00 | $9.22 | +39.76 | +39.76 | +0.00 | +39.76 |
| 2026-09-04 | `BE` | 5 | — | $236.82 | +0.00 | $252.87 | +80.25 | +80.25 | +0.00 | +80.25 |
| 2026-09-04 | `MRX` | 16 | — | $75.65 | +0.00 | $78.27 | +41.92 | +41.92 | +0.00 | +41.92 |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | — | +0.00 | -5.98 | +69.68 | — |
| 2026-09-08 | `GORO` | 321 | $4.15 | $4.13 | -6.42 | — | +0.00 | -6.42 | +57.78 | — |
| 2026-09-08 | `MSTR` | 9 | $142.80 | $137.62 | -46.62 | — | +0.00 | -46.62 | +2.43 | — |
| 2026-09-08 | `BLSH` | 36 | $36.00 | $35.90 | -3.60 | — | +0.00 | -3.60 | +43.56 | — |
| 2026-09-08 | `ZETA` | 38 | $31.35 | $31.08 | -10.26 | — | +0.00 | -10.26 | -59.66 | — |
| 2026-09-08 | `HAFN` | 142 | $9.22 | $8.81 | -58.22 | $8.96 | +21.30 | -36.92 | -18.46 | +2.84 |
| 2026-09-08 | `BE` | 5 | $252.87 | $267.76 | +74.45 | — | +0.00 | +74.45 | +154.70 | — |
| 2026-09-08 | `MRX` | 16 | $78.27 | $78.84 | +9.12 | $76.71 | -34.08 | -24.96 | +51.04 | +16.96 |
| 2026-09-09 | `HAFN` | 142 | $8.96 | $9.00 | +5.68 | — | +0.00 | +5.68 | +8.52 | — |
| 2026-09-09 | `MRX` | 16 | $76.71 | $76.60 | -1.76 | — | +0.00 | -1.76 | +15.20 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `BAK` | 596 | — | $2.12 | +0.00 | $2.08 | -23.84 | -23.84 | +0.00 | -23.84 |
| 2026-09-11 | `OBE` | 100 | — | $12.55 | +0.00 | $12.97 | +42.00 | +42.00 | +0.00 | +42.00 |
| 2026-09-11 | `CLOV` | 266 | — | $4.75 | +0.00 | $4.82 | +18.62 | +18.62 | +0.00 | +18.62 |
| 2026-09-11 | `INSP` | 18 | — | $69.88 | +0.00 | $73.00 | +56.16 | +56.16 | +0.00 | +56.16 |
| 2026-09-11 | `GME` | 60 | — | $21.04 | +0.00 | $21.15 | +6.60 | +6.60 | +0.00 | +6.60 |
| 2026-09-11 | `DHT` | 59 | — | $21.30 | +0.00 | $22.00 | +41.30 | +41.30 | +0.00 | +41.30 |
| 2026-09-11 | `PBR` | 59 | — | $21.21 | +0.00 | $21.20 | -0.59 | -0.59 | +0.00 | -0.59 |
| 2026-09-11 | `AMTX` | 620 | — | $2.04 | +0.00 | $2.01 | -18.60 | -18.60 | +0.00 | -18.60 |
| 2026-09-14 | `BAK` | 596 | $2.08 | $2.05 | -17.88 | — | +0.00 | -17.88 | -41.72 | — |
| 2026-09-14 | `OBE` | 100 | $12.97 | $13.57 | +60.00 | — | +0.00 | +60.00 | +102.00 | — |
| 2026-09-14 | `CLOV` | 266 | $4.82 | $4.82 | +0.00 | — | +0.00 | +0.00 | +18.62 | — |
| 2026-09-14 | `INSP` | 18 | $73.00 | $72.14 | -15.48 | — | +0.00 | -15.48 | +40.68 | — |
| 2026-09-14 | `GME` | 60 | $21.15 | $21.00 | -9.00 | $21.62 | +37.20 | +28.20 | -2.40 | +34.80 |
| 2026-09-14 | `DHT` | 59 | $22.00 | $22.14 | +8.26 | $22.15 | +0.59 | +8.85 | +49.56 | +50.15 |
| 2026-09-14 | `PBR` | 59 | $21.20 | $21.23 | +1.77 | — | +0.00 | +1.77 | +1.18 | — |
| 2026-09-14 | `AMTX` | 620 | $2.01 | $2.01 | +0.00 | — | +0.00 | +0.00 | -18.60 | — |
| 2026-09-15 | `GME` | 60 | $21.62 | $21.51 | -6.60 | — | +0.00 | -6.60 | +28.20 | — |
| 2026-09-15 | `DHT` | 59 | $22.15 | $22.40 | +14.75 | — | +0.00 | +14.75 | +64.90 | — |
| 2026-09-16 | `ADPT` | 47 | — | $27.09 | +0.00 | $27.67 | +27.26 | +27.26 | +0.00 | +27.26 |
| 2026-09-16 | `SM` | 32 | — | $39.99 | +0.00 | $38.16 | -58.56 | -58.56 | +0.00 | -58.56 |
| 2026-09-16 | `KR` | 20 | — | $61.93 | +0.00 | $61.14 | -15.80 | -15.80 | +0.00 | -15.80 |
| 2026-09-16 | `APA` | 27 | — | $46.44 | +0.00 | $44.79 | -44.55 | -44.55 | +0.00 | -44.55 |
| 2026-09-16 | `RDNT` | 16 | — | $77.12 | +0.00 | $75.78 | -21.44 | -21.44 | +0.00 | -21.44 |
| 2026-09-16 | `TK` | 89 | — | $14.26 | +0.00 | $14.39 | +11.57 | +11.57 | +0.00 | +11.57 |
| 2026-09-16 | `PGNY` | 46 | — | $27.63 | +0.00 | $27.38 | -11.50 | -11.50 | +0.00 | -11.50 |
| 2026-09-16 | `VLO` | 3 | — | $391.68 | +0.00 | $403.28 | +34.80 | +34.80 | +0.00 | +34.80 |
| 2026-09-17 | `ADPT` | 47 | $27.67 | $28.23 | +26.32 | — | +0.00 | +26.32 | +53.58 | — |
| 2026-09-17 | `SM` | 32 | $38.16 | $37.57 | -18.88 | — | +0.00 | -18.88 | -77.44 | — |
| 2026-09-17 | `KR` | 20 | $61.14 | $61.02 | -2.40 | — | +0.00 | -2.40 | -18.20 | — |
| 2026-09-17 | `APA` | 27 | $44.79 | $44.63 | -4.32 | — | +0.00 | -4.32 | -48.87 | — |
| 2026-09-17 | `RDNT` | 16 | $75.78 | $76.44 | +10.56 | — | +0.00 | +10.56 | -10.88 | — |
| 2026-09-17 | `TK` | 89 | $14.39 | $14.41 | +1.78 | — | +0.00 | +1.78 | +13.35 | — |
| 2026-09-17 | `PGNY` | 46 | $27.38 | $27.38 | +0.00 | $27.01 | -17.02 | -17.02 | -11.50 | -28.52 |
| 2026-09-17 | `VLO` | 3 | $403.28 | $398.45 | -14.49 | — | +0.00 | -14.49 | +20.31 | — |
| 2026-09-17 | `ARQT` | 48 | — | $25.95 | +0.00 | $26.46 | +24.48 | +24.48 | +0.00 | +24.48 |
| 2026-09-17 | `SABR` | 529 | — | $2.40 | +0.00 | $2.32 | -42.32 | -42.32 | +0.00 | -42.32 |
| 2026-09-17 | `PGEN` | 167 | — | $7.59 | +0.00 | $7.87 | +46.76 | +46.76 | +0.00 | +46.76 |
| 2026-09-17 | `QTRX` | 431 | — | $2.94 | +0.00 | $3.12 | +77.58 | +77.58 | +0.00 | +77.58 |
| 2026-09-17 | `ASX` | 32 | — | $38.95 | +0.00 | $39.99 | +33.28 | +33.28 | +0.00 | +33.28 |
| 2026-09-17 | `SFL` | 93 | — | $13.55 | +0.00 | $13.75 | +18.60 | +18.60 | +0.00 | +18.60 |
| 2026-09-17 | `FOSL` | 232 | — | $5.46 | +0.00 | $5.63 | +39.44 | +39.44 | +0.00 | +39.44 |
| 2026-09-18 | `PGNY` | 46 | $27.01 | $27.01 | +0.00 | — | +0.00 | +0.00 | -28.52 | — |
| 2026-09-18 | `ARQT` | 48 | $26.46 | $26.14 | -15.36 | — | +0.00 | -15.36 | +9.12 | — |
| 2026-09-18 | `SABR` | 529 | $2.32 | $2.29 | -15.87 | — | +0.00 | -15.87 | -58.19 | — |
| 2026-09-18 | `PGEN` | 167 | $7.87 | $7.98 | +18.37 | — | +0.00 | +18.37 | +65.13 | — |
| 2026-09-18 | `QTRX` | 431 | $3.12 | $3.12 | +0.00 | — | +0.00 | +0.00 | +77.58 | — |
| 2026-09-18 | `ASX` | 32 | $39.99 | $40.35 | +11.52 | $41.63 | +40.96 | +52.48 | +44.80 | +85.76 |
| 2026-09-18 | `SFL` | 93 | $13.75 | $13.74 | -0.93 | $13.63 | -10.23 | -11.16 | +17.67 | +7.44 |
| 2026-09-18 | `FOSL` | 232 | $5.63 | $5.63 | +0.00 | — | +0.00 | +0.00 | +39.44 | — |
| 2026-09-18 | `BNC` | 220 | — | $5.83 | +0.00 | $5.98 | +33.00 | +33.00 | +0.00 | +33.00 |
| 2026-09-18 | `AMD` | 2 | — | $547.37 | +0.00 | $559.82 | +24.90 | +24.90 | +0.00 | +24.90 |
| 2026-09-18 | `SYM` | 28 | — | $44.70 | +0.00 | $41.89 | -78.68 | -78.68 | +0.00 | -78.68 |
| 2026-09-18 | `TH` | 61 | — | $20.91 | +0.00 | $21.19 | +17.08 | +17.08 | +0.00 | +17.08 |
| 2026-09-18 | `KOPN` | 270 | — | $4.75 | +0.00 | $4.74 | -2.70 | -2.70 | +0.00 | -2.70 |
| 2026-09-18 | `DDD` | 358 | — | $3.58 | +0.00 | $3.63 | +17.90 | +17.90 | +0.00 | +17.90 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +40.56 | ADUR, ENS, GEMI, SLG, ZIM, MRLN, ANGX, YSS | — | $133.44 | $10,018.05 | ADUR×75, ENS×6, GEMI×320, SLG×21, ZIM×45, MRLN×301, ANGX×290, YSS×124 |
| 2026-08-17 | +2.25 | $133.44 | ADUR×75, ENS×6, GEMI×320, SLG×21, ZIM×45, MRLN×301, ANGX×290, YSS×124 | $9,995.68 | -22.37 | -18.07 | OCC, ALM, DVN, WBS, FANG | ADUR, ENS, GEMI, SLG, ZIM, MRLN, ANGX, YSS | $196.19 | $9,943.92 | OCC×109, ALM×123, DVN×43, WBS×25, FANG×9 |
| 2026-08-18 | -6.20 | $196.19 | OCC×109, ALM×123, DVN×43, WBS×25, FANG×9 | $9,810.30 | -133.62 | +0.00 | — | OCC, ALM, DVN, WBS, FANG | $9,799.28 | $9,799.28 | — |
| 2026-08-19 | -7.20 | $9,799.28 | — | $9,799.28 | -0.00 | +0.00 | — | — | $9,799.28 | $9,799.28 | — |
| 2026-08-20 | +1.12 | $9,799.28 | — | $9,799.28 | -0.00 | +291.60 | CABA, IAG, AG, ABUS, KGC, COTY, NFGC, ATAT | — | $41.55 | $10,058.73 | CABA×402, IAG×62, AG×59, ABUS×248, KGC×41, COTY×480, NFGC×699, ATAT×35 |
| 2026-08-21 | +3.25 | $41.55 | CABA×402, IAG×62, AG×59, ABUS×248, KGC×41, COTY×480, NFGC×699, ATAT×35 | $10,294.02 | +235.29 | +112.95 | DXYZ, ORBS, GORO, CF, VIRT, BTBT, GMAB, CRSP | CABA, IAG, AG, ABUS, KGC, COTY, NFGC, ATAT | $47.56 | $10,331.53 | DXYZ×36, ORBS×1484, GORO×412, CF×10, VIRT×21, BTBT×772, GMAB×38, CRSP×21 |
| 2026-08-24 | -5.17 | $47.56 | DXYZ×36, ORBS×1484, GORO×412, CF×10, VIRT×21, BTBT×772, GMAB×38, CRSP×21 | $10,258.53 | -73.00 | +0.00 | — | DXYZ, ORBS, GORO, CF, VIRT, BTBT, GMAB, CRSP | $10,214.69 | $10,214.69 | — |
| 2026-08-25 | +1.80 | $10,214.69 | — | $10,214.69 | +0.00 | +49.41 | VALE, KURA, DBRG, HCA | — | $2,388.28 | $10,254.89 | VALE×136, KURA×150, DBRG×127, HCA×4 |
| 2026-08-26 | +2.02 | $2,388.28 | VALE×136, KURA×150, DBRG×127, HCA×4 | $10,261.29 | +6.40 | +23.30 | ABX, ACRS, SJM, BZ, CRMD, LI | VALE, KURA, DBRG, HCA | $118.06 | $10,260.05 | ABX×173, ACRS×261, SJM×12, BZ×101, CRMD×198, LI×140 |
| 2026-08-27 | — | $118.06 | ABX×173, ACRS×261, SJM×12, BZ×101, CRMD×198, LI×140 | $10,239.85 | -20.20 | +12.31 | NCNO, NABL, INO, SRRK, PAGP, DASH, AEO, PGY | ABX, ACRS, SJM, BZ, CRMD, LI | $123.91 | $10,206.99 | NCNO×58, NABL×330, INO×990, SRRK×21, PAGP×45, DASH×5, AEO×74, PGY×55 |
| 2026-08-28 | +0.75 | $123.91 | NCNO×58, NABL×330, INO×990, SRRK×21, PAGP×45, DASH×5, AEO×74, PGY×55 | $10,338.30 | +131.31 | -71.91 | TH, TLS, RBRK, ADSK, PD, S, ULTA, HAFN | NCNO, NABL, INO, SRRK, PAGP, DASH, AEO, PGY | $478.07 | $10,217.72 | TH×67, TLS×267, RBRK×13, ADSK×4, PD×98, S×59, ULTA×2, HAFN×154 |
| 2026-08-31 | -5.85 | $478.07 | TH×67, TLS×267, RBRK×13, ADSK×4, PD×98, S×59, ULTA×2, HAFN×154 | $10,166.56 | -51.16 | +0.00 | — | TH, TLS, RBRK, ADSK, PD, S, ULTA, HAFN | $10,147.77 | $10,147.77 | — |
| 2026-09-01 | -6.30 | $10,147.77 | — | $10,147.77 | +0.00 | +0.00 | — | — | $10,147.77 | $10,147.77 | — |
| 2026-09-02 | -3.83 | $10,147.77 | — | $10,147.77 | +0.00 | +0.00 | — | — | $10,147.77 | $10,147.77 | — |
| 2026-09-03 | -0.90 | $10,147.77 | — | $10,147.77 | +0.00 | -135.32 | EBS, DELL, CABA, VSTM, GALT, CTVA, ATRC, ETD | — | $350.42 | $9,991.04 | EBS×193, DELL×2, CABA×349, VSTM×157, GALT×281, CTVA×14, ATRC×23, ETD×58 |
| 2026-09-04 | +2.25 | $350.42 | EBS×193, DELL×2, CABA×349, VSTM×157, GALT×281, CTVA×14, ATRC×23, ETD×58 | $9,942.67 | -48.37 | +293.66 | GORO, MSTR, BLSH, ZETA, HAFN, BE, MRX | EBS, CABA, VSTM, GALT, CTVA, ATRC, ETD | $221.00 | $10,199.84 | DELL×2, GORO×321, MSTR×9, BLSH×36, ZETA×38, HAFN×142, BE×5, MRX×16 |
| 2026-09-08 | -11.47 | $221.00 | DELL×2, GORO×321, MSTR×9, BLSH×36, ZETA×38, HAFN×142, BE×5, MRX×16 | $10,152.31 | -47.53 | -12.78 | — | DELL, GORO, MSTR, BLSH, ZETA, BE | $7,625.32 | $10,125.00 | HAFN×142, MRX×16 |
| 2026-09-09 | -13.95 | $7,625.32 | HAFN×142, MRX×16 | $10,128.92 | +3.92 | +0.00 | — | HAFN, MRX | $10,124.42 | $10,124.42 | — |
| 2026-09-10 | -13.28 | $10,124.42 | — | $10,124.42 | -0.00 | +0.00 | — | — | $10,124.42 | $10,124.42 | — |
| 2026-09-11 | +0.50 | $10,124.42 | — | $10,124.42 | -0.00 | +121.65 | BAK, OBE, CLOV, INSP, GME, DHT, PBR, AMTX | — | $19.31 | $10,216.11 | BAK×596, OBE×100, CLOV×266, INSP×18, GME×60, DHT×59, PBR×59, AMTX×620 |
| 2026-09-14 | -11.00 | $19.31 | BAK×596, OBE×100, CLOV×266, INSP×18, GME×60, DHT×59, PBR×59, AMTX×620 | $10,243.78 | +27.67 | +37.79 | — | BAK, OBE, CLOV, INSP, PBR, AMTX | $7,651.56 | $10,255.61 | GME×60, DHT×59 |
| 2026-09-15 | -3.84 | $7,651.56 | GME×60, DHT×59 | $10,263.76 | +8.15 | +0.00 | — | GME, DHT | $10,259.38 | $10,259.38 | — |
| 2026-09-16 | +5.30 | $10,259.38 | — | $10,259.38 | -0.00 | -78.22 | ADPT, SM, KR, APA, RDNT, TK, PGNY, VLO | — | $248.15 | $10,164.40 | ADPT×47, SM×32, KR×20, APA×27, RDNT×16, TK×89, PGNY×46, VLO×3 |
| 2026-09-17 | +7.38 | $248.15 | ADPT×47, SM×32, KR×20, APA×27, RDNT×16, TK×89, PGNY×46, VLO×3 | $10,162.97 | -1.43 | +180.80 | ARQT, SABR, PGEN, QTRX, ASX, SFL, FOSL | ADPT, SM, KR, APA, RDNT, TK, VLO | $41.21 | $10,304.63 | PGNY×46, ARQT×48, SABR×529, PGEN×167, QTRX×431, ASX×32, SFL×93, FOSL×232 |
| 2026-09-18 | +4.86 | $41.21 | PGNY×46, ARQT×48, SABR×529, PGEN×167, QTRX×431, ASX×32, SFL×93, FOSL×232 | $10,302.36 | -2.27 | +42.23 | BNC, AMD, SYM, TH, KOPN, DDD | PGNY, ARQT, SABR, PGEN, QTRX, FOSL | $225.14 | $10,304.98 | ASX×32, SFL×93, BNC×220, AMD×2, SYM×28, TH×61, KOPN×270, DDD×358 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $8,760.28 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ENS` | 6 | $196.00 | $2.01 | — | $7,582.28 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list oppset; 🔵; ⚪; ret5=+5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `GEMI` | 320 | $3.90 | $4.13 | — | $6,330.15 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; 🔵; ⚪; ret5=+8.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $5,118.29 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZIM` | 45 | $27.25 | $2.12 | — | $3,889.91 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list oppset; 🔵; ⚪; ret5=+1.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MRLN` | 301 | $4.15 | $3.88 | — | $2,636.88 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; 🔵; ret5=+4.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $1,383.24 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `YSS` | 124 | $10.06 | $2.36 | — | $133.44 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; 🔵; ⚪; ret5=+5.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.44 | ▲ close $10,018.05 vs 09:30 $10,000.00 (session +40.56) | 16:00 close · cash $133.44 · equity $10,018.05 vs 09:30 $10,000.00 (+18.05; session marks +40.56) · 8 name(s) marked open→close (per-name table). ADUR×75 09:30 $16.50 → close $16.17 -24.75; ENS×6 09:30 $196.00 → close $203.40 +44.40; GEMI×320 09:30 $3.90 → close $3.92 +6.40; SLG×21 09:30 $57.61 → close $56.09 -31.92; ZIM×45 09:30 $27.25 → close $28.14 +40.05; MRLN×301 09:30 $4.15 → close $3.75 -118.90; ANGX×290 09:30 $4.31 → close $4.37 +17.40; YSS×124 09:30 $10.06 → close $10.93 +107.88 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.44 | ▼ 09:30 equity $9,995.68 vs yday $10,018.05 (-22.37) | 09:30 open · cash $133.44 (unchanged overnight, no fees) · equity $9,995.68 vs prior close $10,018.05 (-22.37) · 8 name(s) re-marked at the open (per-name table). ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; ENS×6 yday $203.40 → 09:30 $205.03 +9.78; GEMI×320 yday $3.92 → 09:30 $3.89 -9.60; SLG×21 yday $56.09 → 09:30 $55.37 -15.12; ZIM×45 yday $28.14 → 09:30 $28.83 +31.05; MRLN×301 yday $3.75 → 09:30 $3.75 -1.50; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; YSS×124 yday $10.93 → 09:30 $10.36 -70.68 | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $1,310.95 | ▼ -62.20 after sell → book $9,993.44; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ENS` | 6 | $205.03 | $2.03 | $+50.14 | $2,539.10 | ▲ +50.14 after sell → book $9,991.41; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `GEMI` | 320 | $3.89 | $4.19 | $-11.52 | $3,779.71 | ▼ -11.52 after sell → book $9,987.22; vs 09:30 mark -4.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 21 | $55.37 | $2.07 | $-51.17 | $4,940.41 | ▼ -51.17 after sell → book $9,985.15; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZIM` | 45 | $28.83 | $2.15 | $+66.83 | $6,235.61 | ▲ +66.83 after sell → book $9,983.00; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `MRLN` | 301 | $3.75 | $3.94 | $-128.23 | $7,360.42 | ▼ -128.23 after sell → book $9,979.06; vs 09:30 mark -3.94 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $8,690.62 | ▲ +76.56 after sell → book $9,975.26; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `YSS` | 124 | $10.36 | $2.39 | $+32.45 | $9,972.86 | ▲ +32.45 after sell → book $9,972.86; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 109 | $18.24 | $2.32 | — | $7,982.39 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1994.57 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 123 | $16.20 | $2.36 | — | $5,987.43 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1994.57 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 43 | $46.18 | $2.12 | — | $3,999.57 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten; 🔵; ret5=+6.7; leftover $1994.57 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `WBS` | 25 | $79.00 | $2.06 | — | $2,022.50 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list oppset; ⚪; ret5=+0.5; leftover $1994.57 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 9 | $202.70 | $2.02 | — | $196.19 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten; 🔵; ret5=+8.3; leftover $1994.57 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $196.19 | ▼ close $9,943.92 vs 09:30 $9,995.68 (session -18.07) | 16:00 close · cash $196.19 · equity $9,943.92 vs 09:30 $9,995.68 (-51.76; session marks -18.07) · 5 name(s) marked open→close (per-name table). OCC×109 09:30 $18.24 → close $17.12 -122.08; ALM×123 09:30 $16.20 → close $16.36 +19.68; DVN×43 09:30 $46.18 → close $47.57 +59.77; WBS×25 09:30 $79.00 → close $78.69 -7.75; FANG×9 09:30 $202.70 → close $206.29 +32.31 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $196.19 | ▼ 09:30 equity $9,810.30 vs yday $9,943.92 (-133.62) | 09:30 open · cash $196.19 (unchanged overnight, no fees) · equity $9,810.30 vs prior close $9,943.92 (-133.62) · 5 name(s) re-marked at the open (per-name table). OCC×109 yday $17.12 → 09:30 $16.20 -100.28; ALM×123 yday $16.36 → 09:30 $15.78 -71.34; DVN×43 yday $47.57 → 09:30 $48.00 +18.49; WBS×25 yday $78.69 → 09:30 $78.52 -4.25; FANG×9 yday $206.29 → 09:30 $208.93 +23.76 | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 109 | $16.20 | $2.35 | $-227.03 | $1,959.64 | ▼ -227.03 after sell → book $9,807.95; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 123 | $15.78 | $2.39 | $-56.41 | $3,898.18 | ▼ -56.41 after sell → book $9,805.55; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 43 | $48.00 | $2.15 | $+74.00 | $5,960.04 | ▲ +74.00 after sell → book $9,803.41; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `WBS` | 25 | $78.52 | $2.09 | $-16.16 | $7,920.95 | ▼ -16.16 after sell → book $9,801.32; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 9 | $208.93 | $2.04 | $+52.01 | $9,799.28 | ▲ +52.01 after sell → book $9,799.28; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,799.28 | ▲ close $9,799.28 vs 09:30 $9,810.30 (session +0.00) | 16:00 close · cash $9,799.28 · no lots left · equity $9,799.28. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,799.28 | ▲ 09:30 equity $9,799.28 vs yday $9,799.28 (-0.00) | 09:30 open · cash $9,799.28 · no holdings · equity $9,799.28 vs prior close $9,799.28 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,799.28 | ▲ close $9,799.28 vs 09:30 $9,799.28 (session +0.00) | 16:00 close · cash $9,799.28 · no lots left · equity $9,799.28. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,799.28 | ▲ 09:30 equity $9,799.28 vs yday $9,799.28 (-0.00) | 09:30 open · cash $9,799.28 · no holdings · equity $9,799.28 vs prior close $9,799.28 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `CABA` | 402 | $3.04 | $5.19 | — | $8,572.01 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+8.8; leftover $1224.91 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 62 | $19.63 | $2.18 | — | $7,352.77 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1224.91 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 59 | $20.55 | $2.17 | — | $6,138.16 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1224.91 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 248 | $4.92 | $3.20 | — | $4,914.80 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1224.91 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $3,697.85 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1224.91 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `COTY` | 480 | $2.55 | $6.19 | — | $2,467.66 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; 🔵; ⚪; ret5=+9.8; leftover $1224.91 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 699 | $1.75 | $9.02 | — | $1,235.40 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1224.91 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 35 | $34.05 | $2.10 | — | $41.55 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; 🔵; ret5=+9.3; leftover $1224.91 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.55 | ▲ close $10,058.73 vs 09:30 $9,799.28 (session +291.60) | 16:00 close · cash $41.55 · equity $10,058.73 vs 09:30 $9,799.28 (+259.45; session marks +291.60) · 8 name(s) marked open→close (per-name table). CABA×402 09:30 $3.04 → close $3.19 +60.30; IAG×62 09:30 $19.63 → close $20.50 +53.94; AG×59 09:30 $20.55 → close $21.19 +37.76; ABUS×248 09:30 $4.92 → close $4.77 -37.20; KGC×41 09:30 $29.63 → close $31.43 +73.80; COTY×480 09:30 $2.55 → close $2.75 +96.00; NFGC×699 09:30 $1.75 → close $1.75 +0.00; ATAT×35 09:30 $34.05 → close $34.25 +7.00 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.55 | ▲ 09:30 equity $10,294.02 vs yday $10,058.73 (+235.29) | 09:30 open · cash $41.55 (unchanged overnight, no fees) · equity $10,294.02 vs prior close $10,058.73 (+235.29) · 8 name(s) re-marked at the open (per-name table). CABA×402 yday $3.19 → 09:30 $3.20 +4.02; IAG×62 yday $20.50 → 09:30 $21.17 +41.54; AG×59 yday $21.19 → 09:30 $21.90 +41.89; ABUS×248 yday $4.77 → 09:30 $5.20 +106.64; KGC×41 yday $31.43 → 09:30 $32.17 +30.34; COTY×480 yday $2.75 → 09:30 $2.71 -19.20; NFGC×699 yday $1.75 → 09:30 $1.79 +27.96; ATAT×35 yday $34.25 → 09:30 $34.31 +2.10 | — |
| 2026-08-21 09:30 ET | **SELL** | `CABA` | 402 | $3.20 | $5.26 | $+53.87 | $1,322.69 | ▲ +53.87 after sell → book $10,288.76; vs 09:30 mark -5.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 62 | $21.17 | $2.20 | $+91.11 | $2,633.03 | ▲ +91.11 after sell → book $10,286.56; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 59 | $21.90 | $2.19 | $+75.30 | $3,922.94 | ▲ +75.30 after sell → book $10,284.37; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 248 | $5.20 | $3.25 | $+62.99 | $5,209.29 | ▲ +62.99 after sell → book $10,281.12; vs 09:30 mark -3.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $6,526.13 | ▲ +99.89 after sell → book $10,278.99; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `COTY` | 480 | $2.71 | $6.28 | $+64.33 | $7,820.65 | ▲ +64.33 after sell → book $10,272.71; vs 09:30 mark -6.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 699 | $1.79 | $9.14 | $+9.80 | $9,062.71 | ▲ +9.80 after sell → book $10,263.56; vs 09:30 mark -9.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 35 | $34.31 | $2.12 | $+4.89 | $10,261.45 | ▲ +4.89 after sell → book $10,261.45; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `DXYZ` | 36 | $34.89 | $2.10 | — | $9,003.31 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.6; leftover $1282.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1484 | $0.86 | $17.27 | — | $7,703.86 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1282.68 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 412 | $3.11 | $5.31 | — | $6,417.23 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer; ret5=+7.1; leftover $1282.68 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 10 | $127.43 | $2.02 | — | $5,140.91 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable; 🔵; ⚪; ret5=+7.9; leftover $1282.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `VIRT` | 21 | $60.66 | $2.05 | — | $3,864.99 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list oppset; 🔵; ⚪; ret5=+7.0; leftover $1282.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 772 | $1.66 | $9.96 | — | $2,573.51 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1282.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 38 | $33.36 | $2.10 | — | $1,303.73 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,mover_buy,oppset; 🔵; ⚪; ret5=+6.6; leftover $1282.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $47.56 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1282.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.56 | ▲ close $10,331.53 vs 09:30 $10,294.02 (session +112.95) | 16:00 close · cash $47.56 · equity $10,331.53 vs 09:30 $10,294.02 (+37.51; session marks +112.95) · 8 name(s) marked open→close (per-name table). DXYZ×36 09:30 $34.89 → close $34.43 -16.56; ORBS×1484 09:30 $0.86 → close $0.88 +23.74; GORO×412 09:30 $3.11 → close $3.19 +32.96; CF×10 09:30 $127.43 → close $129.60 +21.70; VIRT×21 09:30 $60.66 → close $67.93 +152.67; BTBT×772 09:30 $1.66 → close $1.53 -100.36; GMAB×38 09:30 $33.36 → close $33.45 +3.42; CRSP×21 09:30 $59.72 → close $59.50 -4.62 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.56 | ▼ 09:30 equity $10,258.53 vs yday $10,331.53 (-73.00) | 09:30 open · cash $47.56 (unchanged overnight, no fees) · equity $10,258.53 vs prior close $10,331.53 (-73.00) · 8 name(s) re-marked at the open (per-name table). DXYZ×36 yday $34.43 → 09:30 $33.10 -47.88; ORBS×1484 yday $0.88 → 09:30 $0.89 +14.84; GORO×412 yday $3.19 → 09:30 $3.20 +4.12; CF×10 yday $129.60 → 09:30 $129.99 +3.90; VIRT×21 yday $67.93 → 09:30 $66.80 -23.73; BTBT×772 yday $1.53 → 09:30 $1.55 +15.44; GMAB×38 yday $33.45 → 09:30 $32.82 -23.94; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75 | — |
| 2026-08-24 09:30 ET | **SELL** | `DXYZ` | 36 | $33.10 | $2.12 | $-68.66 | $1,237.04 | ▼ -68.66 after sell → book $10,256.41; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1484 | $0.89 | $17.92 | $+3.39 | $2,539.88 | ▲ +3.39 after sell → book $10,238.49; vs 09:30 mark -17.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GORO` | 412 | $3.20 | $5.39 | $+26.37 | $3,852.89 | ▲ +26.37 after sell → book $10,233.10; vs 09:30 mark -5.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 10 | $129.99 | $2.04 | $+21.54 | $5,150.75 | ▲ +21.54 after sell → book $10,231.06; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `VIRT` | 21 | $66.80 | $2.07 | $+124.81 | $6,551.47 | ▲ +124.81 after sell → book $10,228.98; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 772 | $1.55 | $10.10 | $-104.98 | $7,737.98 | ▼ -104.98 after sell → book $10,218.89; vs 09:30 mark -10.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 38 | $32.82 | $2.12 | $-24.75 | $8,983.01 | ▼ -24.75 after sell → book $10,216.76; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $10,214.69 | ▼ -24.50 after sell → book $10,214.69; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,214.69 | ▲ close $10,214.69 vs 09:30 $10,258.53 (session +0.00) | 16:00 close · cash $10,214.69 · no lots left · equity $10,214.69. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,214.69 | ▲ 09:30 equity $10,214.69 vs yday $10,214.69 (+0.00) | 09:30 open · cash $10,214.69 · no holdings · equity $10,214.69 vs prior close $10,214.69 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `VALE` | 136 | $15.01 | $2.40 | — | $8,170.93 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list mover_buy; ⚪; ret5=+9.4; leftover $2042.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 150 | $13.59 | $2.44 | — | $6,129.99 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $2042.94 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DBRG` | 127 | $15.98 | $2.37 | — | $4,098.16 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list oppset; 🔵; ⚪; ret5=+0.4; leftover $2042.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 4 | $426.97 | $2.00 | — | $2,388.28 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten; ret5=+6.0; leftover $2042.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,388.28 | ▲ close $10,254.89 vs 09:30 $10,214.69 (session +49.41) | 16:00 close · cash $2,388.28 · equity $10,254.89 vs 09:30 $10,214.69 (+40.20; session marks +49.41) · 4 name(s) marked open→close (per-name table). VALE×136 09:30 $15.01 → close $15.33 +43.52; KURA×150 09:30 $13.59 → close $13.59 +0.00; DBRG×127 09:30 $15.98 → close $15.97 -1.27; HCA×4 09:30 $426.97 → close $428.76 +7.16 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,388.28 | ▲ 09:30 equity $10,261.29 vs yday $10,254.89 (+6.40) | 09:30 open · cash $2,388.28 (unchanged overnight, no fees) · equity $10,261.29 vs prior close $10,254.89 (+6.40) · 4 name(s) re-marked at the open (per-name table). VALE×136 yday $15.33 → 09:30 $15.37 +5.44; KURA×150 yday $13.59 → 09:30 $13.63 +6.00; DBRG×127 yday $15.97 → 09:30 $15.97 +0.00; HCA×4 yday $428.76 → 09:30 $427.50 -5.04 | — |
| 2026-08-26 09:30 ET | **SELL** | `VALE` | 136 | $15.37 | $2.44 | $+44.12 | $4,476.16 | ▲ +44.12 after sell → book $10,258.85; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 150 | $13.63 | $2.48 | $+1.08 | $6,518.18 | ▲ +1.08 after sell → book $10,256.37; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `DBRG` | 127 | $15.97 | $2.41 | $-6.05 | $8,543.96 | ▼ -6.05 after sell → book $10,253.96; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `HCA` | 4 | $427.50 | $2.03 | $-1.91 | $10,251.94 | ▼ -1.91 after sell → book $10,251.94; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 173 | $9.83 | $2.51 | — | $8,548.84 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1708.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ACRS` | 261 | $6.53 | $3.37 | — | $6,841.14 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list yday_gainer; 🔵; ret5=+3.6; leftover $1708.66 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SJM` | 12 | $134.80 | $2.03 | — | $5,221.52 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; 🔵; ret5=+5.9; leftover $1708.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BZ` | 101 | $16.77 | $2.29 | — | $3,525.45 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list yday_gainer,yday_mover,oppset; 🔵; ret5=+3.1; leftover $1708.66 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `CRMD` | 198 | $8.60 | $2.58 | — | $1,820.07 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten; 🔵; ret5=+4.8; leftover $1708.66 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `LI` | 140 | $12.14 | $2.41 | — | $118.06 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; ret5=+1.2; leftover $1708.66 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $118.06 | ▲ close $10,260.05 vs 09:30 $10,261.29 (session +23.30) | 16:00 close · cash $118.06 · equity $10,260.05 vs 09:30 $10,261.29 (-1.24; session marks +23.30) · 6 name(s) marked open→close (per-name table). ABX×173 09:30 $9.83 → close $9.78 -8.65; ACRS×261 09:30 $6.53 → close $6.19 -88.74; SJM×12 09:30 $134.80 → close $130.90 -46.80; BZ×101 09:30 $16.77 → close $18.84 +209.07; CRMD×198 09:30 $8.60 → close $8.39 -41.58; LI×140 09:30 $12.14 → close $12.14 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.06 | ▼ 09:30 equity $10,239.85 vs yday $10,260.05 (-20.20) | 09:30 open · cash $118.06 (unchanged overnight, no fees) · equity $10,239.85 vs prior close $10,260.05 (-20.20) · 6 name(s) re-marked at the open (per-name table). ABX×173 yday $9.78 → 09:30 $9.68 -17.30; ACRS×261 yday $6.19 → 09:30 $6.15 -10.44; SJM×12 yday $130.90 → 09:30 $130.29 -7.32; BZ×101 yday $18.84 → 09:30 $18.50 -34.34; CRMD×198 yday $8.39 → 09:30 $8.49 +19.80; LI×140 yday $12.14 → 09:30 $12.35 +29.40 | — |
| 2026-08-27 09:30 ET | **SELL** | `ABX` | 173 | $9.68 | $2.55 | $-31.01 | $1,790.15 | ▼ -31.01 after sell → book $10,237.30; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `ACRS` | 261 | $6.15 | $3.42 | $-105.97 | $3,391.87 | ▼ -105.97 after sell → book $10,233.87; vs 09:30 mark -3.43 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `SJM` | 12 | $130.29 | $2.05 | $-58.19 | $4,953.31 | ▼ -58.19 after sell → book $10,231.83; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 101 | $18.50 | $2.32 | $+170.11 | $6,819.48 | ▲ +170.11 after sell → book $10,229.50; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 198 | $8.49 | $2.63 | $-26.99 | $8,497.87 | ▼ -26.99 after sell → book $10,226.87; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `LI` | 140 | $12.35 | $2.45 | $+24.54 | $10,224.42 | ▲ +24.54 after sell → book $10,224.42; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `NCNO` | 58 | $22.03 | $2.16 | — | $8,944.52 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot,earn_react; 🔵; ret5=+4.0; leftover $1278.05 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NABL` | 330 | $3.87 | $4.26 | — | $7,663.16 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+9.8; leftover $1278.05 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `INO` | 990 | $1.29 | $12.77 | — | $6,373.29 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+0.0; leftover $1278.05 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 21 | $60.00 | $2.05 | — | $5,111.24 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+6.2; leftover $1278.05 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PAGP` | 45 | $28.00 | $2.12 | — | $3,849.11 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+7.8; leftover $1278.05 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DASH` | 5 | $235.94 | $2.00 | — | $2,667.41 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+7.6; leftover $1278.05 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `AEO` | 74 | $17.27 | $2.21 | — | $1,387.22 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+5.5; leftover $1278.05 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 55 | $22.93 | $2.15 | — | $123.91 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list mover_buy; 🔵; ret5=+9.5; leftover $1278.05 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.91 | ▲ close $10,206.99 vs 09:30 $10,239.85 (session +12.31) | 16:00 close · cash $123.91 · equity $10,206.99 vs 09:30 $10,239.85 (-32.86; session marks +12.31) · 8 name(s) marked open→close (per-name table). NCNO×58 09:30 $22.03 → close $23.32 +74.82; NABL×330 09:30 $3.87 → close $3.95 +26.40; INO×990 09:30 $1.29 → close $1.26 -29.70; SRRK×21 09:30 $60.00 → close $59.26 -15.54; PAGP×45 09:30 $28.00 → close $28.03 +1.35; DASH×5 09:30 $235.94 → close $231.89 -20.25; AEO×74 09:30 $17.27 → close $16.69 -42.92; PGY×55 09:30 $22.93 → close $23.26 +18.15 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.91 | ▲ 09:30 equity $10,338.30 vs yday $10,206.99 (+131.31) | 09:30 open · cash $123.91 (unchanged overnight, no fees) · equity $10,338.30 vs prior close $10,206.99 (+131.31) · 8 name(s) re-marked at the open (per-name table). NCNO×58 yday $23.32 → 09:30 $23.30 -1.16; NABL×330 yday $3.95 → 09:30 $4.25 +99.00; INO×990 yday $1.26 → 09:30 $1.27 +9.90; SRRK×21 yday $59.26 → 09:30 $58.75 -10.71; PAGP×45 yday $28.03 → 09:30 $28.08 +2.25; DASH×5 yday $231.89 → 09:30 $233.37 +7.40; AEO×74 yday $16.69 → 09:30 $17.06 +27.38; PGY×55 yday $23.26 → 09:30 $23.21 -2.75 | — |
| 2026-08-28 09:30 ET | **SELL** | `NCNO` | 58 | $23.30 | $2.18 | $+69.31 | $1,473.13 | ▲ +69.31 after sell → book $10,336.12; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `NABL` | 330 | $4.25 | $4.32 | $+116.82 | $2,871.30 | ▲ +116.82 after sell → book $10,331.79; vs 09:30 mark -4.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `INO` | 990 | $1.27 | $12.95 | $-45.52 | $4,115.66 | ▼ -45.52 after sell → book $10,318.85; vs 09:30 mark -12.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 21 | $58.75 | $2.07 | $-30.38 | $5,347.34 | ▼ -30.38 after sell → book $10,316.78; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PAGP` | 45 | $28.08 | $2.15 | $-0.67 | $6,608.79 | ▼ -0.67 after sell → book $10,314.63; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DASH` | 5 | $233.37 | $2.02 | $-16.88 | $7,773.62 | ▼ -16.88 after sell → book $10,312.61; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEO` | 74 | $17.06 | $2.23 | $-19.99 | $9,033.82 | ▼ -19.99 after sell → book $10,310.37; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 55 | $23.21 | $2.18 | $+11.07 | $10,308.20 | ▲ +11.07 after sell → book $10,308.20; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 67 | $19.00 | $2.19 | — | $9,033.00 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+7.5; leftover $1288.52 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 267 | $4.82 | $3.44 | — | $7,742.62 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.8; leftover $1288.52 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `RBRK` | 13 | $98.95 | $2.03 | — | $6,454.24 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; ret5=+9.7; leftover $1288.52 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $5,407.60 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; ret5=+7.8; leftover $1288.52 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PD` | 98 | $13.09 | $2.28 | — | $4,122.50 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; ret5=+4.2; leftover $1288.52 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `S` | 59 | $21.49 | $2.17 | — | $2,852.42 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; ret5=+8.5; leftover $1288.52 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 2 | $542.00 | $2.00 | — | $1,766.42 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; ret5=+4.8; leftover $1288.52 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 154 | $8.35 | $2.45 | — | $478.07 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; ret5=+5.1; leftover $1288.52 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $478.07 | ▼ close $10,217.72 vs 09:30 $10,338.30 (session -71.91) | 16:00 close · cash $478.07 · equity $10,217.72 vs 09:30 $10,338.30 (-120.58; session marks -71.91) · 8 name(s) marked open→close (per-name table). TH×67 09:30 $19.00 → close $18.55 -30.15; TLS×267 09:30 $4.82 → close $4.79 -8.01; RBRK×13 09:30 $98.95 → close $93.05 -76.70; ADSK×4 09:30 $261.16 → close $260.66 -2.00; PD×98 09:30 $13.09 → close $13.83 +72.52; S×59 09:30 $21.49 → close $21.54 +2.95; ULTA×2 09:30 $542.00 → close $517.50 -49.00; HAFN×154 09:30 $8.35 → close $8.47 +18.48 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $478.07 | ▼ 09:30 equity $10,166.56 vs yday $10,217.72 (-51.16) | 09:30 open · cash $478.07 (unchanged overnight, no fees) · equity $10,166.56 vs prior close $10,217.72 (-51.16) · 8 name(s) re-marked at the open (per-name table). TH×67 yday $18.55 → 09:30 $18.12 -28.48; TLS×267 yday $4.79 → 09:30 $4.81 +5.34; RBRK×13 yday $93.05 → 09:30 $92.83 -2.86; ADSK×4 yday $260.66 → 09:30 $257.71 -11.80; PD×98 yday $13.83 → 09:30 $13.58 -24.50; S×59 yday $21.54 → 09:30 $21.45 -5.31; ULTA×2 yday $517.50 → 09:30 $521.10 +7.20; HAFN×154 yday $8.47 → 09:30 $8.53 +9.24 | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 67 | $18.12 | $2.21 | $-63.03 | $1,690.23 | ▼ -63.03 after sell → book $10,164.34; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 267 | $4.81 | $3.50 | $-9.61 | $2,971.00 | ▼ -9.61 after sell → book $10,160.84; vs 09:30 mark -3.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RBRK` | 13 | $92.83 | $2.05 | $-83.64 | $4,175.75 | ▼ -83.64 after sell → book $10,158.80; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-17.82 | $5,204.56 | ▼ -17.82 after sell → book $10,156.77; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PD` | 98 | $13.58 | $2.31 | $+43.43 | $6,533.09 | ▲ +43.43 after sell → book $10,154.46; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `S` | 59 | $21.45 | $2.19 | $-6.71 | $7,796.46 | ▼ -6.71 after sell → book $10,152.28; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ULTA` | 2 | $521.10 | $2.02 | $-45.81 | $8,836.64 | ▼ -45.81 after sell → book $10,150.26; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 154 | $8.53 | $2.49 | $+22.78 | $10,147.77 | ▲ +22.78 after sell → book $10,147.77; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,147.77 | ▲ close $10,147.77 vs 09:30 $10,166.56 (session +0.00) | 16:00 close · cash $10,147.77 · no lots left · equity $10,147.77. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,147.77 | ▲ 09:30 equity $10,147.77 vs yday $10,147.77 (+0.00) | 09:30 open · cash $10,147.77 · no holdings · equity $10,147.77 vs prior close $10,147.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,147.77 | ▲ close $10,147.77 vs 09:30 $10,147.77 (session +0.00) | 16:00 close · cash $10,147.77 · no lots left · equity $10,147.77. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,147.77 | ▲ 09:30 equity $10,147.77 vs yday $10,147.77 (+0.00) | 09:30 open · cash $10,147.77 · no holdings · equity $10,147.77 vs prior close $10,147.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,147.77 | ▲ close $10,147.77 vs 09:30 $10,147.77 (session +0.00) | 16:00 close · cash $10,147.77 · no lots left · equity $10,147.77. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,147.77 | ▲ 09:30 equity $10,147.77 vs yday $10,147.77 (+0.00) | 09:30 open · cash $10,147.77 · no holdings · equity $10,147.77 vs prior close $10,147.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `EBS` | 193 | $6.56 | $2.57 | — | $8,879.12 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ⚪; ret5=+8.2; leftover $1268.47 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,904.51 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $1268.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 349 | $3.63 | $4.50 | — | $6,633.13 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1268.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 157 | $8.03 | $2.46 | — | $5,369.96 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1268.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `GALT` | 281 | $4.50 | $3.62 | — | $4,101.84 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.0; leftover $1268.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTVA` | 14 | $90.24 | $2.03 | — | $2,836.45 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.6; leftover $1268.47 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $1,618.15 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1268.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ETD` | 58 | $21.82 | $2.16 | — | $350.42 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list oppset; 🔵; ret5=+4.7; leftover $1268.47 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $350.42 | ▼ close $9,991.04 vs 09:30 $10,147.77 (session -135.32) | 16:00 close · cash $350.42 · equity $9,991.04 vs 09:30 $10,147.77 (-156.73; session marks -135.32) · 8 name(s) marked open→close (per-name table). EBS×193 09:30 $6.56 → close $6.23 -63.69; DELL×2 09:30 $486.31 → close $516.39 +60.16; CABA×349 09:30 $3.63 → close $3.48 -52.35; VSTM×157 09:30 $8.03 → close $7.98 -7.85; GALT×281 09:30 $4.50 → close $4.35 -42.15; CTVA×14 09:30 $90.24 → close $88.62 -22.68; ATRC×23 09:30 $52.88 → close $52.46 -9.66; ETD×58 09:30 $21.82 → close $21.87 +2.90 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $350.42 | ▼ 09:30 equity $9,942.67 vs yday $9,991.04 (-48.37) | 09:30 open · cash $350.42 (unchanged overnight, no fees) · equity $9,942.67 vs prior close $9,991.04 (-48.37) · 8 name(s) re-marked at the open (per-name table). EBS×193 yday $6.23 → 09:30 $6.26 +5.79; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CABA×349 yday $3.48 → 09:30 $3.46 -6.98; VSTM×157 yday $7.98 → 09:30 $7.91 -10.99; GALT×281 yday $4.35 → 09:30 $4.33 -5.62; CTVA×14 yday $88.62 → 09:30 $87.64 -13.72; ATRC×23 yday $52.46 → 09:30 $52.03 -9.89; ETD×58 yday $21.87 → 09:30 $21.84 -1.74 | — |
| 2026-09-04 09:30 ET | **SELL** | `EBS` | 193 | $6.26 | $2.61 | $-63.08 | $1,555.99 | ▼ -63.08 after sell → book $9,940.06; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 349 | $3.46 | $4.57 | $-68.40 | $2,758.96 | ▼ -68.40 after sell → book $9,935.49; vs 09:30 mark -4.57 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 157 | $7.91 | $2.50 | $-23.80 | $3,998.33 | ▼ -23.80 after sell → book $9,932.99; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `GALT` | 281 | $4.33 | $3.68 | $-55.08 | $5,211.38 | ▼ -55.08 after sell → book $9,929.31; vs 09:30 mark -3.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTVA` | 14 | $87.64 | $2.05 | $-40.48 | $6,436.29 | ▼ -40.48 after sell → book $9,927.26; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 23 | $52.03 | $2.08 | $-23.69 | $7,630.90 | ▼ -23.69 after sell → book $9,925.18; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `ETD` | 58 | $21.84 | $2.18 | $-3.19 | $8,895.44 | ▼ -3.19 after sell → book $9,923.00; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `GORO` | 321 | $3.95 | $4.14 | — | $7,623.35 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+6.9; leftover $1270.78 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 9 | $137.35 | $2.02 | — | $6,385.18 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+5.4; leftover $1270.78 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BLSH` | 36 | $34.69 | $2.10 | — | $5,134.24 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+7.9; leftover $1270.78 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ZETA` | 38 | $32.65 | $2.10 | — | $3,891.44 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+8.1; leftover $1270.78 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 142 | $8.94 | $2.42 | — | $2,619.54 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+7.7; leftover $1270.78 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 5 | $236.82 | $2.00 | — | $1,433.44 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+8.1; leftover $1270.78 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 16 | $75.65 | $2.04 | — | $221.00 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1270.78 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $221.00 | ▲ close $10,199.84 vs 09:30 $9,942.67 (session +293.66) | 16:00 close · cash $221.00 · equity $10,199.84 vs 09:30 $9,942.67 (+257.17; session marks +293.66) · 8 name(s) marked open→close (per-name table). DELL×2 09:30 $513.78 → close $524.14 +20.72; GORO×321 09:30 $3.95 → close $4.15 +64.20; MSTR×9 09:30 $137.35 → close $142.80 +49.05; BLSH×36 09:30 $34.69 → close $36.00 +47.16; ZETA×38 09:30 $32.65 → close $31.35 -49.40; HAFN×142 09:30 $8.94 → close $9.22 +39.76; BE×5 09:30 $236.82 → close $252.87 +80.25; MRX×16 09:30 $75.65 → close $78.27 +41.92 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $221.00 | ▼ 09:30 equity $10,152.31 vs yday $10,199.84 (-47.53) | 09:30 open · cash $221.00 (unchanged overnight, no fees) · equity $10,152.31 vs prior close $10,199.84 (-47.53) · 8 name(s) re-marked at the open (per-name table). DELL×2 yday $524.14 → 09:30 $521.15 -5.98; GORO×321 yday $4.15 → 09:30 $4.13 -6.42; MSTR×9 yday $142.80 → 09:30 $137.62 -46.62; BLSH×36 yday $36.00 → 09:30 $35.90 -3.60; ZETA×38 yday $31.35 → 09:30 $31.08 -10.26; HAFN×142 yday $9.22 → 09:30 $8.81 -58.22; BE×5 yday $252.87 → 09:30 $267.76 +74.45; MRX×16 yday $78.27 → 09:30 $78.84 +9.12 | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+65.67 | $1,261.28 | ▲ +65.67 after sell → book $10,150.29; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `GORO` | 321 | $4.13 | $4.20 | $+49.43 | $2,582.81 | ▲ +49.43 after sell → book $10,146.09; vs 09:30 mark -4.20 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MSTR` | 9 | $137.62 | $2.04 | $-1.62 | $3,819.35 | ▼ -1.62 after sell → book $10,144.05; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `BLSH` | 36 | $35.90 | $2.12 | $+39.34 | $5,109.63 | ▲ +39.34 after sell → book $10,141.93; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ZETA` | 38 | $31.08 | $2.12 | $-63.89 | $6,288.55 | ▼ -63.89 after sell → book $10,139.81; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 5 | $267.76 | $2.03 | $+150.67 | $7,625.32 | ▲ +150.67 after sell → book $10,137.78; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,625.32 | ▼ close $10,125.00 vs 09:30 $10,152.31 (session -12.78) | 16:00 close · cash $7,625.32 · equity $10,125.00 vs 09:30 $10,152.31 (-27.31; session marks -12.78) · 2 name(s) marked open→close (per-name table). HAFN×142 09:30 $8.81 → close $8.96 +21.30; MRX×16 09:30 $78.84 → close $76.71 -34.08 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,625.32 | ▲ 09:30 equity $10,128.92 vs yday $10,125.00 (+3.92) | 09:30 open · cash $7,625.32 (unchanged overnight, no fees) · equity $10,128.92 vs prior close $10,125.00 (+3.92) · 2 name(s) re-marked at the open (per-name table). HAFN×142 yday $8.96 → 09:30 $9.00 +5.68; MRX×16 yday $76.71 → 09:30 $76.60 -1.76 | — |
| 2026-09-09 09:30 ET | **SELL** | `HAFN` | 142 | $9.00 | $2.45 | $+3.65 | $8,900.87 | ▲ +3.65 after sell → book $10,126.47; vs 09:30 mark -2.45 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 16 | $76.60 | $2.06 | $+11.10 | $10,124.42 | ▲ +11.10 after sell → book $10,124.42; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,124.42 | ▲ close $10,124.42 vs 09:30 $10,128.92 (session +0.00) | 16:00 close · cash $10,124.42 · no lots left · equity $10,124.42. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,124.42 | ▲ 09:30 equity $10,124.42 vs yday $10,124.42 (-0.00) | 09:30 open · cash $10,124.42 · no holdings · equity $10,124.42 vs prior close $10,124.42 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,124.42 | ▲ close $10,124.42 vs 09:30 $10,124.42 (session +0.00) | 16:00 close · cash $10,124.42 · no lots left · equity $10,124.42. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,124.42 | ▲ 09:30 equity $10,124.42 vs yday $10,124.42 (-0.00) | 09:30 open · cash $10,124.42 · no holdings · equity $10,124.42 vs prior close $10,124.42 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 596 | $2.12 | $7.69 | — | $8,853.21 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1265.55 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `OBE` | 100 | $12.55 | $2.29 | — | $7,595.92 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list oppset; ret5=+5.5; leftover $1265.55 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 266 | $4.75 | $3.43 | — | $6,328.99 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1265.55 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INSP` | 18 | $69.88 | $2.04 | — | $5,069.10 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.0; leftover $1265.55 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `GME` | 60 | $21.04 | $2.17 | — | $3,804.53 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+7.5; leftover $1265.55 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `DHT` | 59 | $21.30 | $2.17 | — | $2,545.67 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+6.8; leftover $1265.55 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PBR` | 59 | $21.21 | $2.17 | — | $1,292.11 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+2.5; leftover $1265.55 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 620 | $2.04 | $8.00 | — | $19.31 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1265.55 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.31 | ▲ close $10,216.11 vs 09:30 $10,124.42 (session +121.65) | 16:00 close · cash $19.31 · equity $10,216.11 vs 09:30 $10,124.42 (+91.69; session marks +121.65) · 8 name(s) marked open→close (per-name table). BAK×596 09:30 $2.12 → close $2.08 -23.84; OBE×100 09:30 $12.55 → close $12.97 +42.00; CLOV×266 09:30 $4.75 → close $4.82 +18.62; INSP×18 09:30 $69.88 → close $73.00 +56.16; GME×60 09:30 $21.04 → close $21.15 +6.60; DHT×59 09:30 $21.30 → close $22.00 +41.30; PBR×59 09:30 $21.21 → close $21.20 -0.59; AMTX×620 09:30 $2.04 → close $2.01 -18.60 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.31 | ▲ 09:30 equity $10,243.78 vs yday $10,216.11 (+27.67) | 09:30 open · cash $19.31 (unchanged overnight, no fees) · equity $10,243.78 vs prior close $10,216.11 (+27.67) · 8 name(s) re-marked at the open (per-name table). BAK×596 yday $2.08 → 09:30 $2.05 -17.88; OBE×100 yday $12.97 → 09:30 $13.57 +60.00; CLOV×266 yday $4.82 → 09:30 $4.82 +0.00; INSP×18 yday $73.00 → 09:30 $72.14 -15.48; GME×60 yday $21.15 → 09:30 $21.00 -9.00; DHT×59 yday $22.00 → 09:30 $22.14 +8.26; PBR×59 yday $21.20 → 09:30 $21.23 +1.77; AMTX×620 yday $2.01 → 09:30 $2.01 +0.00 | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 596 | $2.05 | $7.80 | $-57.21 | $1,233.31 | ▼ -57.21 after sell → book $10,235.98; vs 09:30 mark -7.80 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `OBE` | 100 | $13.57 | $2.32 | $+97.39 | $2,588.00 | ▲ +97.39 after sell → book $10,233.67; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 266 | $4.82 | $3.49 | $+11.70 | $3,866.63 | ▲ +11.70 after sell → book $10,230.18; vs 09:30 mark -3.49 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `INSP` | 18 | $72.14 | $2.06 | $+36.57 | $5,163.09 | ▲ +36.57 after sell → book $10,228.12; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `PBR` | 59 | $21.23 | $2.19 | $-3.17 | $6,413.47 | ▼ -3.17 after sell → book $10,225.93; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 620 | $2.01 | $8.11 | $-34.71 | $7,651.56 | ▼ -34.71 after sell → book $10,217.82; vs 09:30 mark -8.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,651.56 | ▲ close $10,255.61 vs 09:30 $10,243.78 (session +37.79) | 16:00 close · cash $7,651.56 · equity $10,255.61 vs 09:30 $10,243.78 (+11.83; session marks +37.79) · 2 name(s) marked open→close (per-name table). GME×60 09:30 $21.00 → close $21.62 +37.20; DHT×59 09:30 $22.14 → close $22.15 +0.59 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,651.56 | ▲ 09:30 equity $10,263.76 vs yday $10,255.61 (+8.15) | 09:30 open · cash $7,651.56 (unchanged overnight, no fees) · equity $10,263.76 vs prior close $10,255.61 (+8.15) · 2 name(s) re-marked at the open (per-name table). GME×60 yday $21.62 → 09:30 $21.51 -6.60; DHT×59 yday $22.15 → 09:30 $22.40 +14.75 | — |
| 2026-09-15 09:30 ET | **SELL** | `GME` | 60 | $21.51 | $2.19 | $+23.84 | $8,939.97 | ▲ +23.84 after sell → book $10,261.57; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-15 09:30 ET | **SELL** | `DHT` | 59 | $22.40 | $2.19 | $+60.55 | $10,259.38 | ▲ +60.55 after sell → book $10,259.38; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,259.38 | ▲ close $10,259.38 vs 09:30 $10,263.76 (session +0.00) | 16:00 close · cash $10,259.38 · no lots left · equity $10,259.38. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,259.38 | ▲ 09:30 equity $10,259.38 vs yday $10,259.38 (-0.00) | 09:30 open · cash $10,259.38 · no holdings · equity $10,259.38 vs prior close $10,259.38 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 47 | $27.09 | $2.13 | — | $8,984.02 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1282.42 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 32 | $39.99 | $2.09 | — | $7,702.25 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+9.3; leftover $1282.42 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `KR` | 20 | $61.93 | $2.05 | — | $6,461.60 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.8; leftover $1282.42 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `APA` | 27 | $46.44 | $2.07 | — | $5,205.65 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.9; leftover $1282.42 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 16 | $77.12 | $2.04 | — | $3,969.69 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,ohlc_hot; ret5=+7.2; leftover $1282.42 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `TK` | 89 | $14.26 | $2.26 | — | $2,698.30 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+6.8; leftover $1282.42 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `PGNY` | 46 | $27.63 | $2.13 | — | $1,425.19 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+6.1; leftover $1282.42 | join🟡 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `VLO` | 3 | $391.68 | $2.00 | — | $248.15 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+6.7; leftover $1282.42 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $248.15 | ▼ close $10,164.40 vs 09:30 $10,259.38 (session -78.22) | 16:00 close · cash $248.15 · equity $10,164.40 vs 09:30 $10,259.38 (-94.98; session marks -78.22) · 8 name(s) marked open→close (per-name table). ADPT×47 09:30 $27.09 → close $27.67 +27.26; SM×32 09:30 $39.99 → close $38.16 -58.56; KR×20 09:30 $61.93 → close $61.14 -15.80; APA×27 09:30 $46.44 → close $44.79 -44.55; RDNT×16 09:30 $77.12 → close $75.78 -21.44; TK×89 09:30 $14.26 → close $14.39 +11.57; PGNY×46 09:30 $27.63 → close $27.38 -11.50; VLO×3 09:30 $391.68 → close $403.28 +34.80 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $248.15 | ▼ 09:30 equity $10,162.97 vs yday $10,164.40 (-1.43) | 09:30 open · cash $248.15 (unchanged overnight, no fees) · equity $10,162.97 vs prior close $10,164.40 (-1.43) · 8 name(s) re-marked at the open (per-name table). ADPT×47 yday $27.67 → 09:30 $28.23 +26.32; SM×32 yday $38.16 → 09:30 $37.57 -18.88; KR×20 yday $61.14 → 09:30 $61.02 -2.40; APA×27 yday $44.79 → 09:30 $44.63 -4.32; RDNT×16 yday $75.78 → 09:30 $76.44 +10.56; TK×89 yday $14.39 → 09:30 $14.41 +1.78; PGNY×46 yday $27.38 → 09:30 $27.38 +0.00; VLO×3 yday $403.28 → 09:30 $398.45 -14.49 | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 47 | $28.23 | $2.15 | $+49.30 | $1,572.81 | ▲ +49.30 after sell → book $10,160.82; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 32 | $37.57 | $2.11 | $-81.63 | $2,772.94 | ▼ -81.63 after sell → book $10,158.71; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `KR` | 20 | $61.02 | $2.07 | $-22.32 | $3,991.27 | ▼ -22.32 after sell → book $10,156.64; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `APA` | 27 | $44.63 | $2.09 | $-53.03 | $5,194.19 | ▼ -53.03 after sell → book $10,154.55; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 16 | $76.44 | $2.06 | $-14.98 | $6,415.17 | ▼ -14.98 after sell → book $10,152.49; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TK` | 89 | $14.41 | $2.28 | $+8.81 | $7,695.38 | ▲ +8.81 after sell → book $10,150.21; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `VLO` | 3 | $398.45 | $2.02 | $+16.29 | $8,888.71 | ▲ +16.29 after sell → book $10,148.19; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 48 | $25.95 | $2.13 | — | $7,640.98 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,yday_mover,ohlc_hot,oppset; 🔵; ret5=+9.6; leftover $1269.82 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 529 | $2.40 | $6.82 | — | $6,364.55 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1269.82 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 167 | $7.59 | $2.49 | — | $5,094.53 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1269.82 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `QTRX` | 431 | $2.94 | $5.56 | — | $3,821.83 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list yday_gainer,ohlc_hot; 🔵; ret5=+9.8; leftover $1269.82 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ASX` | 32 | $38.95 | $2.09 | — | $2,573.35 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+7.7; leftover $1269.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SFL` | 93 | $13.55 | $2.27 | — | $1,310.93 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+7.8; leftover $1269.82 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `FOSL` | 232 | $5.46 | $2.99 | — | $41.21 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+9.5; leftover $1269.82 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.21 | ▲ close $10,304.63 vs 09:30 $10,162.97 (session +180.80) | 16:00 close · cash $41.21 · equity $10,304.63 vs 09:30 $10,162.97 (+141.66; session marks +180.80) · 8 name(s) marked open→close (per-name table). PGNY×46 09:30 $27.38 → close $27.01 -17.02; ARQT×48 09:30 $25.95 → close $26.46 +24.48; SABR×529 09:30 $2.40 → close $2.32 -42.32; PGEN×167 09:30 $7.59 → close $7.87 +46.76; QTRX×431 09:30 $2.94 → close $3.12 +77.58; ASX×32 09:30 $38.95 → close $39.99 +33.28; SFL×93 09:30 $13.55 → close $13.75 +18.60; FOSL×232 09:30 $5.46 → close $5.63 +39.44 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.21 | ▼ 09:30 equity $10,302.36 vs yday $10,304.63 (-2.27) | 09:30 open · cash $41.21 (unchanged overnight, no fees) · equity $10,302.36 vs prior close $10,304.63 (-2.27) · 8 name(s) re-marked at the open (per-name table). PGNY×46 yday $27.01 → 09:30 $27.01 +0.00; ARQT×48 yday $26.46 → 09:30 $26.14 -15.36; SABR×529 yday $2.32 → 09:30 $2.29 -15.87; PGEN×167 yday $7.87 → 09:30 $7.98 +18.37; QTRX×431 yday $3.12 → 09:30 $3.12 +0.00; ASX×32 yday $39.99 → 09:30 $40.35 +11.52; SFL×93 yday $13.75 → 09:30 $13.74 -0.93; FOSL×232 yday $5.63 → 09:30 $5.63 +0.00 | — |
| 2026-09-18 09:30 ET | **SELL** | `PGNY` | 46 | $27.01 | $2.15 | $-32.80 | $1,281.53 | ▼ -32.80 after sell → book $10,300.22; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 48 | $26.14 | $2.15 | $+4.83 | $2,534.09 | ▲ +4.83 after sell → book $10,298.06; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 529 | $2.29 | $6.92 | $-71.94 | $3,738.58 | ▼ -71.94 after sell → book $10,291.14; vs 09:30 mark -6.92 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 167 | $7.98 | $2.53 | $+60.11 | $5,068.71 | ▲ +60.11 after sell → book $10,288.61; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **SELL** | `QTRX` | 431 | $3.12 | $5.64 | $+66.38 | $6,407.79 | ▲ +66.38 after sell → book $10,282.97; vs 09:30 mark -5.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FOSL` | 232 | $5.63 | $3.04 | $+33.41 | $7,710.91 | ▲ +33.41 after sell → book $10,279.93; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 220 | $5.83 | $2.84 | — | $6,425.47 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1285.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `AMD` | 2 | $547.37 | $2.00 | — | $5,328.73 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+8.2; leftover $1285.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `SYM` | 28 | $44.70 | $2.07 | — | $4,075.06 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.5; leftover $1285.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 61 | $20.91 | $2.17 | — | $2,797.38 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1285.15 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `KOPN` | 270 | $4.75 | $3.48 | — | $1,511.39 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+7.7; leftover $1285.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 358 | $3.58 | $4.62 | — | $225.14 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1285.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $225.14 | ▲ close $10,304.98 vs 09:30 $10,302.36 (session +42.23) | 16:00 close · cash $225.14 · equity $10,304.98 vs 09:30 $10,302.36 (+2.62; session marks +42.23) · 8 name(s) marked open→close (per-name table). ASX×32 09:30 $40.35 → close $41.63 +40.96; SFL×93 09:30 $13.74 → close $13.63 -10.23; BNC×220 09:30 $5.83 → close $5.98 +33.00; AMD×2 09:30 $547.37 → close $559.82 +24.90; SYM×28 09:30 $44.70 → close $41.89 -78.68; TH×61 09:30 $20.91 → close $21.19 +17.08; KOPN×270 09:30 $4.75 → close $4.74 -2.70; DDD×358 09:30 $3.58 → close $3.63 +17.90 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `EIX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `ALEC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMTX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MTDR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SQM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `DK` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BVN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AMX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `TWO` | no_price | no 09:30 open |
| 2026-08-31 | `RCKT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GWRE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `YEXT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FNKO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HUBS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SLB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `AGCO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ZETA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TSLA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SIGA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `HAL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PODD` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VIR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AGRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CSAN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `LAND` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FMC` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CF` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `LAND` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `XRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `KMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `STX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CDZI` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `KEP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BILI` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `QRVO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `PUMP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `PGNY` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `VLO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FRO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ASX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VLO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `AVTR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `FRO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IOVA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ASX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PGNY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `VLO` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ASX` | 32 | 2026-09-17 @ $38.95 | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+7.7; leftover $1269.82 |
| `SFL` | 93 | 2026-09-17 @ $13.55 | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+7.8; leftover $1269.82 |
| `BNC` | 220 | 2026-09-18 @ $5.83 | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1285.15 |
| `AMD` | 2 | 2026-09-18 @ $547.37 | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+8.2; leftover $1285.15 |
| `SYM` | 28 | 2026-09-18 @ $44.70 | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.5; leftover $1285.15 |
| `TH` | 61 | 2026-09-18 @ $20.91 | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1285.15 |
| `KOPN` | 270 | 2026-09-18 @ $4.75 | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+7.7; leftover $1285.15 |
| `DDD` | 358 | 2026-09-18 @ $3.58 | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1285.15 |
