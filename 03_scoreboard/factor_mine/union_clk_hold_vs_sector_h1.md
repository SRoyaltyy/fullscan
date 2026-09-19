# Factor mine action — `union_clk_hold_vs_sector_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · Clock-B #6 stock holds while sector camera is red

Cash book **+4.71%** ($10,472) · signal-only (no cash/fees) was +6.28%. Starts YES **18/26**. Fills 169 · skips 80 · realized $+449.64.

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
- Must-have: Clock-B #6: the stock held up (yesterday up or last bar green) while the sector camera is red.
- Must-not: Clock-B #5 long veto: extreme prior extension plus diminishing progress or a failed breakout.
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
- **Gate** `clk_hold_vs_sector=True` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $51.18.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-17 | `HTFL` | 48 | — | $41.23 | +0.00 | $41.94 | +34.08 | +34.08 | +0.00 | +34.08 |
| 2026-08-17 | `JBIO` | 81 | — | $24.60 | +0.00 | $23.45 | -93.15 | -93.15 | +0.00 | -93.15 |
| 2026-08-17 | `VERA` | 63 | — | $31.30 | +0.00 | $31.63 | +20.79 | +20.79 | +0.00 | +20.79 |
| 2026-08-17 | `ZNTL` | 561 | — | $3.56 | +0.00 | $3.71 | +81.35 | +81.35 | +0.00 | +81.35 |
| 2026-08-17 | `CELC` | 21 | — | $92.99 | +0.00 | $92.44 | -11.55 | -11.55 | +0.00 | -11.55 |
| 2026-08-18 | `HTFL` | 48 | $41.94 | $41.50 | -21.12 | — | +0.00 | -21.12 | +12.96 | — |
| 2026-08-18 | `JBIO` | 81 | $23.45 | $23.07 | -30.78 | — | +0.00 | -30.78 | -123.93 | — |
| 2026-08-18 | `VERA` | 63 | $31.63 | $31.31 | -20.16 | — | +0.00 | -20.16 | +0.63 | — |
| 2026-08-18 | `ZNTL` | 561 | $3.71 | $3.75 | +25.24 | — | +0.00 | +25.24 | +106.59 | — |
| 2026-08-18 | `CELC` | 21 | $92.44 | $92.38 | -1.26 | — | +0.00 | -1.26 | -12.81 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `HTHT` | 25 | — | $48.39 | +0.00 | $49.54 | +28.75 | +28.75 | +0.00 | +28.75 |
| 2026-08-20 | `RERE` | 296 | — | $4.20 | +0.00 | $4.08 | -35.52 | -35.52 | +0.00 | -35.52 |
| 2026-08-20 | `ATAT` | 36 | — | $34.05 | +0.00 | $34.25 | +7.20 | +7.20 | +0.00 | +7.20 |
| 2026-08-20 | `SG` | 193 | — | $6.43 | +0.00 | $6.58 | +28.95 | +28.95 | +0.00 | +28.95 |
| 2026-08-20 | `ZIM` | 45 | — | $27.45 | +0.00 | $27.16 | -13.05 | -13.05 | +0.00 | -13.05 |
| 2026-08-20 | `SUI` | 10 | — | $121.21 | +0.00 | $122.29 | +10.80 | +10.80 | +0.00 | +10.80 |
| 2026-08-20 | `BABA` | 10 | — | $123.47 | +0.00 | $130.53 | +70.60 | +70.60 | +0.00 | +70.60 |
| 2026-08-20 | `CTRE` | 31 | — | $39.79 | +0.00 | $39.76 | -0.93 | -0.93 | +0.00 | -0.93 |
| 2026-08-21 | `HTHT` | 25 | $49.54 | $49.58 | +1.00 | — | +0.00 | +1.00 | +29.75 | — |
| 2026-08-21 | `RERE` | 296 | $4.08 | $4.17 | +26.64 | — | +0.00 | +26.64 | -8.88 | — |
| 2026-08-21 | `ATAT` | 36 | $34.25 | $34.31 | +2.16 | — | +0.00 | +2.16 | +9.36 | — |
| 2026-08-21 | `SG` | 193 | $6.58 | $6.61 | +5.79 | — | +0.00 | +5.79 | +34.74 | — |
| 2026-08-21 | `ZIM` | 45 | $27.16 | $27.50 | +15.30 | — | +0.00 | +15.30 | +2.25 | — |
| 2026-08-21 | `SUI` | 10 | $122.29 | $122.41 | +1.20 | — | +0.00 | +1.20 | +12.00 | — |
| 2026-08-21 | `BABA` | 10 | $130.53 | $125.35 | -51.80 | — | +0.00 | -51.80 | +18.80 | — |
| 2026-08-21 | `CTRE` | 31 | $39.76 | $40.00 | +7.44 | — | +0.00 | +7.44 | +6.51 | — |
| 2026-08-21 | `SM` | 33 | — | $37.81 | +0.00 | $37.20 | -20.13 | -20.13 | +0.00 | -20.13 |
| 2026-08-21 | `TALO` | 70 | — | $17.88 | +0.00 | $17.47 | -28.70 | -28.70 | +0.00 | -28.70 |
| 2026-08-21 | `DE` | 2 | — | $623.26 | +0.00 | $647.47 | +48.42 | +48.42 | +0.00 | +48.42 |
| 2026-08-21 | `TRON` | 645 | — | $1.94 | +0.00 | $2.01 | +45.15 | +45.15 | +0.00 | +45.15 |
| 2026-08-21 | `VIK` | 13 | — | $91.00 | +0.00 | $92.79 | +23.27 | +23.27 | +0.00 | +23.27 |
| 2026-08-21 | `ORBS` | 1449 | — | $0.86 | +0.00 | $0.88 | +23.18 | +23.18 | +0.00 | +23.18 |
| 2026-08-21 | `BKE` | 29 | — | $43.08 | +0.00 | $43.81 | +21.17 | +21.17 | +0.00 | +21.17 |
| 2026-08-21 | `BJ` | 13 | — | $93.98 | +0.00 | $96.42 | +31.72 | +31.72 | +0.00 | +31.72 |
| 2026-08-24 | `SM` | 33 | $37.20 | $36.61 | -19.47 | — | +0.00 | -19.47 | -39.60 | — |
| 2026-08-24 | `TALO` | 70 | $17.47 | $17.24 | -16.10 | — | +0.00 | -16.10 | -44.80 | — |
| 2026-08-24 | `DE` | 2 | $647.47 | $653.04 | +11.14 | — | +0.00 | +11.14 | +59.56 | — |
| 2026-08-24 | `TRON` | 645 | $2.01 | $2.02 | +6.45 | — | +0.00 | +6.45 | +51.60 | — |
| 2026-08-24 | `VIK` | 13 | $92.79 | $93.06 | +3.51 | — | +0.00 | +3.51 | +26.78 | — |
| 2026-08-24 | `ORBS` | 1449 | $0.88 | $0.89 | +14.49 | — | +0.00 | +14.49 | +37.67 | — |
| 2026-08-24 | `BKE` | 29 | $43.81 | $44.22 | +11.89 | — | +0.00 | +11.89 | +33.06 | — |
| 2026-08-24 | `BJ` | 13 | $96.42 | $97.02 | +7.80 | — | +0.00 | +7.80 | +39.52 | — |
| 2026-08-25 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-26 | `LI` | 104 | — | $12.14 | +0.00 | $12.14 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `MNRO` | 90 | — | $14.00 | +0.00 | $12.61 | -125.10 | -125.10 | +0.00 | -125.10 |
| 2026-08-26 | `SFL` | 102 | — | $12.35 | +0.00 | $12.03 | -32.64 | -32.64 | +0.00 | -32.64 |
| 2026-08-26 | `VIPS` | 90 | — | $14.00 | +0.00 | $14.08 | +7.20 | +7.20 | +0.00 | +7.20 |
| 2026-08-26 | `AXTI` | 19 | — | $65.34 | +0.00 | $65.18 | -3.04 | -3.04 | +0.00 | -3.04 |
| 2026-08-26 | `BE` | 5 | — | $213.94 | +0.00 | $218.21 | +21.35 | +21.35 | +0.00 | +21.35 |
| 2026-08-26 | `GRRR` | 90 | — | $14.03 | +0.00 | $15.45 | +127.80 | +127.80 | +0.00 | +127.80 |
| 2026-08-26 | `SMTC` | 9 | — | $130.90 | +0.00 | $140.80 | +89.10 | +89.10 | +0.00 | +89.10 |
| 2026-08-27 | `LI` | 104 | $12.14 | $12.35 | +21.84 | — | +0.00 | +21.84 | +21.84 | — |
| 2026-08-27 | `MNRO` | 90 | $12.61 | $12.56 | -4.50 | — | +0.00 | -4.50 | -129.60 | — |
| 2026-08-27 | `SFL` | 102 | $12.03 | $12.03 | +0.00 | — | +0.00 | +0.00 | -32.64 | — |
| 2026-08-27 | `VIPS` | 90 | $14.08 | $14.00 | -7.20 | — | +0.00 | -7.20 | +0.00 | — |
| 2026-08-27 | `AXTI` | 19 | $65.18 | $70.30 | +97.28 | — | +0.00 | +97.28 | +94.24 | — |
| 2026-08-27 | `BE` | 5 | $218.21 | $227.10 | +44.45 | $217.83 | -46.35 | -1.90 | +65.80 | +19.45 |
| 2026-08-27 | `GRRR` | 90 | $15.45 | $15.94 | +44.10 | — | +0.00 | +44.10 | +171.90 | — |
| 2026-08-27 | `SMTC` | 9 | $140.80 | $149.40 | +77.40 | — | +0.00 | +77.40 | +166.50 | — |
| 2026-08-27 | `DASH` | 5 | — | $235.94 | +0.00 | $231.89 | -20.25 | -20.25 | +0.00 | -20.25 |
| 2026-08-27 | `AEO` | 76 | — | $17.27 | +0.00 | $16.69 | -44.08 | -44.08 | +0.00 | -44.08 |
| 2026-08-27 | `BBY` | 16 | — | $80.60 | +0.00 | $83.56 | +47.36 | +47.36 | +0.00 | +47.36 |
| 2026-08-27 | `DKS` | 10 | — | $128.73 | +0.00 | $131.77 | +30.40 | +30.40 | +0.00 | +30.40 |
| 2026-08-27 | `RRC` | 32 | — | $41.44 | +0.00 | $41.64 | +6.40 | +6.40 | +0.00 | +6.40 |
| 2026-08-27 | `GAP` | 63 | — | $20.75 | +0.00 | $20.79 | +2.52 | +2.52 | +0.00 | +2.52 |
| 2026-08-27 | `CRK` | 92 | — | $14.42 | +0.00 | $14.62 | +18.40 | +18.40 | +0.00 | +18.40 |
| 2026-08-28 | `BE` | 5 | $217.83 | $215.71 | -10.62 | — | +0.00 | -10.62 | +8.83 | — |
| 2026-08-28 | `DASH` | 5 | $231.89 | $233.37 | +7.40 | — | +0.00 | +7.40 | -12.85 | — |
| 2026-08-28 | `AEO` | 76 | $16.69 | $17.06 | +28.12 | — | +0.00 | +28.12 | -15.96 | — |
| 2026-08-28 | `BBY` | 16 | $83.56 | $83.85 | +4.64 | — | +0.00 | +4.64 | +52.00 | — |
| 2026-08-28 | `DKS` | 10 | $131.77 | $132.80 | +10.30 | — | +0.00 | +10.30 | +40.70 | — |
| 2026-08-28 | `RRC` | 32 | $41.64 | $41.74 | +3.20 | $41.46 | -8.96 | -5.76 | +9.60 | +0.64 |
| 2026-08-28 | `GAP` | 63 | $20.79 | $24.69 | +245.70 | — | +0.00 | +245.70 | +248.22 | — |
| 2026-08-28 | `CRK` | 92 | $14.62 | $14.63 | +0.92 | — | +0.00 | +0.92 | +19.32 | — |
| 2026-08-28 | `ANF` | 9 | — | $146.07 | +0.00 | $148.42 | +21.15 | +21.15 | +0.00 | +21.15 |
| 2026-08-28 | `TGB` | 136 | — | $9.75 | +0.00 | $9.18 | -77.52 | -77.52 | +0.00 | -77.52 |
| 2026-08-28 | `TH` | 70 | — | $19.00 | +0.00 | $18.55 | -31.50 | -31.50 | +0.00 | -31.50 |
| 2026-08-28 | `SLF` | 16 | — | $78.95 | +0.00 | $78.76 | -3.04 | -3.04 | +0.00 | -3.04 |
| 2026-08-28 | `TRMD` | 41 | — | $32.23 | +0.00 | $32.62 | +15.99 | +15.99 | +0.00 | +15.99 |
| 2026-08-28 | `WSM` | 5 | — | $235.67 | +0.00 | $235.09 | -2.90 | -2.90 | +0.00 | -2.90 |
| 2026-08-28 | `FIGR` | 35 | — | $37.49 | +0.00 | $36.05 | -50.40 | -50.40 | +0.00 | -50.40 |
| 2026-08-31 | `RRC` | 32 | $41.46 | $42.00 | +17.28 | — | +0.00 | +17.28 | +17.92 | — |
| 2026-08-31 | `ANF` | 9 | $148.42 | $148.03 | -3.51 | — | +0.00 | -3.51 | +17.64 | — |
| 2026-08-31 | `TGB` | 136 | $9.18 | $9.15 | -4.08 | — | +0.00 | -4.08 | -81.60 | — |
| 2026-08-31 | `TH` | 70 | $18.55 | $18.12 | -29.75 | — | +0.00 | -29.75 | -61.25 | — |
| 2026-08-31 | `SLF` | 16 | $78.76 | $78.70 | -0.96 | — | +0.00 | -0.96 | -4.00 | — |
| 2026-08-31 | `TRMD` | 41 | $32.62 | $33.09 | +19.27 | — | +0.00 | +19.27 | +35.26 | — |
| 2026-08-31 | `WSM` | 5 | $235.09 | $232.06 | -15.15 | — | +0.00 | -15.15 | -18.05 | — |
| 2026-08-31 | `FIGR` | 35 | $36.05 | $35.77 | -9.80 | — | +0.00 | -9.80 | -60.20 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `PBF` | 17 | — | $74.75 | +0.00 | $75.33 | +9.86 | +9.86 | +0.00 | +9.86 |
| 2026-09-03 | `ETD` | 60 | — | $21.82 | +0.00 | $21.87 | +3.00 | +3.00 | +0.00 | +3.00 |
| 2026-09-03 | `PBR` | 61 | — | $21.18 | +0.00 | $20.51 | -40.87 | -40.87 | +0.00 | -40.87 |
| 2026-09-03 | `XP` | 63 | — | $20.74 | +0.00 | $20.00 | -46.62 | -46.62 | +0.00 | -46.62 |
| 2026-09-03 | `HP` | 27 | — | $47.74 | +0.00 | $45.02 | -73.44 | -73.44 | +0.00 | -73.44 |
| 2026-09-03 | `PBR-A` | 68 | — | $19.16 | +0.00 | $18.58 | -39.44 | -39.44 | +0.00 | -39.44 |
| 2026-09-03 | `VIST` | 16 | — | $77.14 | +0.00 | $74.61 | -40.48 | -40.48 | +0.00 | -40.48 |
| 2026-09-03 | `GRNT` | 254 | — | $5.15 | +0.00 | $5.08 | -17.78 | -17.78 | +0.00 | -17.78 |
| 2026-09-04 | `PBF` | 17 | $75.33 | $74.50 | -14.11 | — | +0.00 | -14.11 | -4.25 | — |
| 2026-09-04 | `ETD` | 60 | $21.87 | $21.84 | -1.80 | — | +0.00 | -1.80 | +1.20 | — |
| 2026-09-04 | `PBR` | 61 | $20.51 | $20.25 | -15.86 | — | +0.00 | -15.86 | -56.73 | — |
| 2026-09-04 | `XP` | 63 | $20.00 | $19.67 | -20.79 | — | +0.00 | -20.79 | -67.41 | — |
| 2026-09-04 | `HP` | 27 | $45.02 | $44.59 | -11.61 | — | +0.00 | -11.61 | -85.05 | — |
| 2026-09-04 | `PBR-A` | 68 | $18.58 | $18.36 | -14.96 | — | +0.00 | -14.96 | -54.40 | — |
| 2026-09-04 | `VIST` | 16 | $74.61 | $73.97 | -10.24 | — | +0.00 | -10.24 | -50.72 | — |
| 2026-09-04 | `GRNT` | 254 | $5.08 | $5.03 | -12.70 | — | +0.00 | -12.70 | -30.48 | — |
| 2026-09-04 | `BE` | 5 | — | $236.82 | +0.00 | $252.87 | +80.25 | +80.25 | +0.00 | +80.25 |
| 2026-09-04 | `HAFN` | 141 | — | $8.94 | +0.00 | $9.22 | +39.48 | +39.48 | +0.00 | +39.48 |
| 2026-09-04 | `MIR` | 76 | — | $16.60 | +0.00 | $16.93 | +25.08 | +25.08 | +0.00 | +25.08 |
| 2026-09-04 | `GORO` | 319 | — | $3.95 | +0.00 | $4.15 | +63.80 | +63.80 | +0.00 | +63.80 |
| 2026-09-04 | `GSM` | 270 | — | $4.67 | +0.00 | $4.67 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-04 | `SLGN` | 30 | — | $41.16 | +0.00 | $41.18 | +0.60 | +0.60 | +0.00 | +0.60 |
| 2026-09-04 | `WNC` | 89 | — | $14.17 | +0.00 | $14.31 | +12.46 | +12.46 | +0.00 | +12.46 |
| 2026-09-04 | `XRX` | 381 | — | $3.31 | +0.00 | $3.32 | +3.81 | +3.81 | +0.00 | +3.81 |
| 2026-09-08 | `BE` | 5 | $252.87 | $267.76 | +74.45 | — | +0.00 | +74.45 | +154.70 | — |
| 2026-09-08 | `HAFN` | 141 | $9.22 | $8.81 | -57.81 | — | +0.00 | -57.81 | -18.33 | — |
| 2026-09-08 | `MIR` | 76 | $16.93 | $17.07 | +10.64 | — | +0.00 | +10.64 | +35.72 | — |
| 2026-09-08 | `GORO` | 319 | $4.15 | $4.13 | -6.38 | — | +0.00 | -6.38 | +57.42 | — |
| 2026-09-08 | `GSM` | 270 | $4.67 | $4.75 | +21.60 | — | +0.00 | +21.60 | +21.60 | — |
| 2026-09-08 | `SLGN` | 30 | $41.18 | $40.60 | -17.40 | — | +0.00 | -17.40 | -16.80 | — |
| 2026-09-08 | `WNC` | 89 | $14.31 | $14.22 | -8.01 | — | +0.00 | -8.01 | +4.45 | — |
| 2026-09-08 | `XRX` | 381 | $3.32 | $3.50 | +68.58 | — | +0.00 | +68.58 | +72.39 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ASO` | 23 | — | $54.91 | +0.00 | $55.36 | +10.35 | +10.35 | +0.00 | +10.35 |
| 2026-09-11 | `BNC` | 263 | — | $4.91 | +0.00 | $4.80 | -28.93 | -28.93 | +0.00 | -28.93 |
| 2026-09-11 | `GFR` | 209 | — | $6.19 | +0.00 | $6.52 | +68.97 | +68.97 | +0.00 | +68.97 |
| 2026-09-11 | `OBE` | 103 | — | $12.55 | +0.00 | $12.97 | +43.26 | +43.26 | +0.00 | +43.26 |
| 2026-09-11 | `PBR` | 61 | — | $21.21 | +0.00 | $21.20 | -0.61 | -0.61 | +0.00 | -0.61 |
| 2026-09-11 | `VIST` | 16 | — | $77.33 | +0.00 | $76.27 | -16.96 | -16.96 | +0.00 | -16.96 |
| 2026-09-11 | `CNQ` | 25 | — | $49.94 | +0.00 | $50.07 | +3.25 | +3.25 | +0.00 | +3.25 |
| 2026-09-11 | `AVAV` | 8 | — | $145.91 | +0.00 | $146.71 | +6.40 | +6.40 | +0.00 | +6.40 |
| 2026-09-14 | `ASO` | 23 | $55.36 | $54.75 | -14.03 | — | +0.00 | -14.03 | -3.68 | — |
| 2026-09-14 | `BNC` | 263 | $4.80 | $5.03 | +60.49 | — | +0.00 | +60.49 | +31.56 | — |
| 2026-09-14 | `GFR` | 209 | $6.52 | $6.60 | +16.72 | — | +0.00 | +16.72 | +85.69 | — |
| 2026-09-14 | `OBE` | 103 | $12.97 | $13.57 | +61.80 | — | +0.00 | +61.80 | +105.06 | — |
| 2026-09-14 | `PBR` | 61 | $21.20 | $21.23 | +1.83 | — | +0.00 | +1.83 | +1.22 | — |
| 2026-09-14 | `VIST` | 16 | $76.27 | $77.10 | +13.28 | — | +0.00 | +13.28 | -3.68 | — |
| 2026-09-14 | `CNQ` | 25 | $50.07 | $50.76 | +17.25 | — | +0.00 | +17.25 | +20.50 | — |
| 2026-09-14 | `AVAV` | 8 | $146.71 | $145.80 | -7.28 | — | +0.00 | -7.28 | -0.88 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `SM` | 33 | — | $39.99 | +0.00 | $38.16 | -60.39 | -60.39 | +0.00 | -60.39 |
| 2026-09-16 | `APA` | 28 | — | $46.44 | +0.00 | $44.79 | -46.20 | -46.20 | +0.00 | -46.20 |
| 2026-09-16 | `CVI` | 25 | — | $51.05 | +0.00 | $53.05 | +50.00 | +50.00 | +0.00 | +50.00 |
| 2026-09-16 | `MEOH` | 20 | — | $63.34 | +0.00 | $61.51 | -36.60 | -36.60 | +0.00 | -36.60 |
| 2026-09-16 | `RIG` | 225 | — | $5.87 | +0.00 | $5.54 | -74.25 | -74.25 | +0.00 | -74.25 |
| 2026-09-16 | `VAL` | 15 | — | $87.40 | +0.00 | $82.52 | -73.20 | -73.20 | +0.00 | -73.20 |
| 2026-09-16 | `VLO` | 3 | — | $391.68 | +0.00 | $403.28 | +34.80 | +34.80 | +0.00 | +34.80 |
| 2026-09-16 | `FRO` | 25 | — | $52.52 | +0.00 | $53.67 | +28.75 | +28.75 | +0.00 | +28.75 |
| 2026-09-17 | `SM` | 33 | $38.16 | $37.57 | -19.47 | — | +0.00 | -19.47 | -79.86 | — |
| 2026-09-17 | `APA` | 28 | $44.79 | $44.63 | -4.48 | — | +0.00 | -4.48 | -50.68 | — |
| 2026-09-17 | `CVI` | 25 | $53.05 | $51.88 | -29.25 | — | +0.00 | -29.25 | +20.75 | — |
| 2026-09-17 | `MEOH` | 20 | $61.51 | $60.83 | -13.60 | — | +0.00 | -13.60 | -50.20 | — |
| 2026-09-17 | `RIG` | 225 | $5.54 | $5.58 | +9.00 | — | +0.00 | +9.00 | -65.25 | — |
| 2026-09-17 | `VAL` | 15 | $82.52 | $83.20 | +10.20 | — | +0.00 | +10.20 | -63.00 | — |
| 2026-09-17 | `VLO` | 3 | $403.28 | $398.45 | -14.49 | — | +0.00 | -14.49 | +20.31 | — |
| 2026-09-17 | `FRO` | 25 | $53.67 | $54.31 | +16.00 | — | +0.00 | +16.00 | +44.75 | — |
| 2026-09-17 | `SFL` | 108 | — | $13.55 | +0.00 | $13.75 | +21.60 | +21.60 | +0.00 | +21.60 |
| 2026-09-17 | `FTAI` | 7 | — | $196.50 | +0.00 | $195.07 | -10.01 | -10.01 | +0.00 | -10.01 |
| 2026-09-17 | `CBC` | 46 | — | $31.60 | +0.00 | $31.67 | +3.22 | +3.22 | +0.00 | +3.22 |
| 2026-09-17 | `FPS` | 40 | — | $36.76 | +0.00 | $38.06 | +52.00 | +52.00 | +0.00 | +52.00 |
| 2026-09-17 | `EROC` | 116 | — | $12.64 | +0.00 | $12.90 | +30.16 | +30.16 | +0.00 | +30.16 |
| 2026-09-17 | `WCC` | 4 | — | $344.29 | +0.00 | $339.32 | -19.88 | -19.88 | +0.00 | -19.88 |
| 2026-09-17 | `TK` | 102 | — | $14.41 | +0.00 | $14.67 | +26.52 | +26.52 | +0.00 | +26.52 |
| 2026-09-18 | `SFL` | 108 | $13.75 | $13.74 | -1.08 | — | +0.00 | -1.08 | +20.52 | — |
| 2026-09-18 | `FTAI` | 7 | $195.07 | $195.55 | +3.36 | — | +0.00 | +3.36 | -6.65 | — |
| 2026-09-18 | `CBC` | 46 | $31.67 | $31.64 | -1.38 | — | +0.00 | -1.38 | +1.84 | — |
| 2026-09-18 | `FPS` | 40 | $38.06 | $39.50 | +57.60 | — | +0.00 | +57.60 | +109.60 | — |
| 2026-09-18 | `EROC` | 116 | $12.90 | $13.00 | +11.60 | — | +0.00 | +11.60 | +41.76 | — |
| 2026-09-18 | `WCC` | 4 | $339.32 | $340.45 | +4.52 | — | +0.00 | +4.52 | -15.36 | — |
| 2026-09-18 | `TK` | 102 | $14.67 | $14.60 | -7.14 | — | +0.00 | -7.14 | +19.38 | — |
| 2026-09-18 | `PURR` | 151 | — | $13.82 | +0.00 | $14.09 | +40.77 | +40.77 | +0.00 | +40.77 |
| 2026-09-18 | `ARE` | 36 | — | $56.70 | +0.00 | $53.30 | -122.40 | -122.40 | +0.00 | -122.40 |
| 2026-09-18 | `MNR` | 190 | — | $10.95 | +0.00 | $11.13 | +34.20 | +34.20 | +0.00 | +34.20 |
| 2026-09-18 | `USDE` | 219 | — | $9.54 | +0.00 | $10.19 | +142.35 | +142.35 | +0.00 | +142.35 |
| 2026-09-18 | `FLNC` | 277 | — | $7.54 | +0.00 | $7.32 | -59.55 | -59.55 | +0.00 | -59.55 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +31.52 | HTFL, JBIO, VERA, ZNTL, CELC | — | $90.67 | $10,015.68 | HTFL×48, JBIO×81, VERA×63, ZNTL×561, CELC×21 |
| 2026-08-18 | -6.20 | $90.67 | HTFL×48, JBIO×81, VERA×63, ZNTL×561, CELC×21 | $9,967.60 | -48.08 | +0.00 | — | HTFL, JBIO, VERA, ZNTL, CELC | $9,951.55 | $9,951.55 | — |
| 2026-08-19 | -7.20 | $9,951.55 | — | $9,951.55 | +0.00 | +0.00 | — | — | $9,951.55 | $9,951.55 | — |
| 2026-08-20 | +1.12 | $9,951.55 | — | $9,951.55 | +0.00 | +96.80 | HTHT, RERE, ATAT, SG, ZIM, SUI, BABA, CTRE | — | $97.47 | $10,029.55 | HTHT×25, RERE×296, ATAT×36, SG×193, ZIM×45, SUI×10, BABA×10, CTRE×31 |
| 2026-08-21 | +3.25 | $97.47 | HTHT×25, RERE×296, ATAT×36, SG×193, ZIM×45, SUI×10, BABA×10, CTRE×31 | $10,037.28 | +7.73 | +144.08 | SM, TALO, DE, TRON, VIK, ORBS, BKE, BJ | HTHT, RERE, ATAT, SG, ZIM, SUI, BABA, CTRE | $77.51 | $10,124.74 | SM×33, TALO×70, DE×2, TRON×645, VIK×13, ORBS×1449, BKE×29, BJ×13 |
| 2026-08-24 | -5.17 | $77.51 | SM×33, TALO×70, DE×2, TRON×645, VIK×13, ORBS×1449, BKE×29, BJ×13 | $10,144.45 | +19.71 | +0.00 | — | SM, TALO, DE, TRON, VIK, ORBS, BKE, BJ | $10,105.98 | $10,105.98 | — |
| 2026-08-25 | +1.80 | $10,105.98 | — | $10,105.98 | -0.00 | +0.00 | — | — | $10,105.98 | $10,105.98 | — |
| 2026-08-26 | +2.02 | $10,105.98 | — | $10,105.98 | -0.00 | +84.67 | LI, MNRO, SFL, VIPS, AXTI, BE, GRRR, SMTC | — | $294.31 | $10,173.20 | LI×104, MNRO×90, SFL×102, VIPS×90, AXTI×19, BE×5, GRRR×90, SMTC×9 |
| 2026-08-27 | — | $294.31 | LI×104, MNRO×90, SFL×102, VIPS×90, AXTI×19, BE×5, GRRR×90, SMTC×9 | $10,446.57 | +273.37 | -5.60 | DASH, AEO, BBY, DKS, RRC, GAP, CRK | LI, MNRO, SFL, VIPS, AXTI, GRRR, SMTC | $251.55 | $10,410.54 | BE×5, DASH×5, AEO×76, BBY×16, DKS×10, RRC×32, GAP×63, CRK×92 |
| 2026-08-28 | +0.75 | $251.55 | BE×5, DASH×5, AEO×76, BBY×16, DKS×10, RRC×32, GAP×63, CRK×92 | $10,700.20 | +289.66 | -137.18 | ANF, TGB, TH, SLF, TRMD, WSM, FIGR | BE, DASH, AEO, BBY, DKS, GAP, CRK | $289.01 | $10,533.27 | RRC×32, ANF×9, TGB×136, TH×70, SLF×16, TRMD×41, WSM×5, FIGR×35 |
| 2026-08-31 | -5.85 | $289.01 | RRC×32, ANF×9, TGB×136, TH×70, SLF×16, TRMD×41, WSM×5, FIGR×35 | $10,506.57 | -26.70 | +0.00 | — | RRC, ANF, TGB, TH, SLF, TRMD, WSM, FIGR | $10,489.44 | $10,489.44 | — |
| 2026-09-01 | -6.30 | $10,489.44 | — | $10,489.44 | -0.00 | +0.00 | — | — | $10,489.44 | $10,489.44 | — |
| 2026-09-02 | -3.83 | $10,489.44 | — | $10,489.44 | -0.00 | +0.00 | — | — | $10,489.44 | $10,489.44 | — |
| 2026-09-03 | -0.90 | $10,489.44 | — | $10,489.44 | -0.00 | -245.77 | PBF, ETD, PBR, XP, HP, PBR-A, VIST, GRNT | — | $158.55 | $10,225.53 | PBF×17, ETD×60, PBR×61, XP×63, HP×27, PBR-A×68, VIST×16, GRNT×254 |
| 2026-09-04 | +2.25 | $158.55 | PBF×17, ETD×60, PBR×61, XP×63, HP×27, PBR-A×68, VIST×16, GRNT×254 | $10,123.46 | -102.07 | +225.48 | BE, HAFN, MIR, GORO, GSM, SLGN, WNC, XRX | PBF, ETD, PBR, XP, HP, PBR-A, VIST, GRNT | $97.40 | $10,307.11 | BE×5, HAFN×141, MIR×76, GORO×319, GSM×270, SLGN×30, WNC×89, XRX×381 |
| 2026-09-08 | -11.47 | $97.40 | BE×5, HAFN×141, MIR×76, GORO×319, GSM×270, SLGN×30, WNC×89, XRX×381 | $10,392.78 | +85.67 | +0.00 | — | BE, HAFN, MIR, GORO, GSM, SLGN, WNC, XRX | $10,368.98 | $10,368.98 | — |
| 2026-09-09 | -13.95 | $10,368.98 | — | $10,368.98 | +0.00 | +0.00 | — | — | $10,368.98 | $10,368.98 | — |
| 2026-09-10 | -13.28 | $10,368.98 | — | $10,368.98 | +0.00 | +0.00 | — | — | $10,368.98 | $10,368.98 | — |
| 2026-09-11 | +0.50 | $10,368.98 | — | $10,368.98 | +0.00 | +85.73 | ASO, BNC, GFR, OBE, PBR, VIST, CNQ, AVAV | — | $262.76 | $10,435.98 | ASO×23, BNC×263, GFR×209, OBE×103, PBR×61, VIST×16, CNQ×25, AVAV×8 |
| 2026-09-14 | -11.00 | $262.76 | ASO×23, BNC×263, GFR×209, OBE×103, PBR×61, VIST×16, CNQ×25, AVAV×8 | $10,586.04 | +150.06 | +0.00 | — | ASO, BNC, GFR, OBE, PBR, VIST, CNQ, AVAV | $10,567.07 | $10,567.07 | — |
| 2026-09-15 | -3.84 | $10,567.07 | — | $10,567.07 | +0.00 | +0.00 | — | — | $10,567.07 | $10,567.07 | — |
| 2026-09-16 | +5.30 | $10,567.07 | — | $10,567.07 | +0.00 | -177.09 | SM, APA, CVI, MEOH, RIG, VAL, VLO, FRO | — | $266.96 | $10,372.70 | SM×33, APA×28, CVI×25, MEOH×20, RIG×225, VAL×15, VLO×3, FRO×25 |
| 2026-09-17 | +7.38 | $266.96 | SM×33, APA×28, CVI×25, MEOH×20, RIG×225, VAL×15, VLO×3, FRO×25 | $10,326.61 | -46.09 | +103.61 | SFL, FTAI, CBC, FPS, EROC, WCC, TK | SM, APA, CVI, MEOH, RIG, VAL, VLO, FRO | $217.82 | $10,397.55 | SFL×108, FTAI×7, CBC×46, FPS×40, EROC×116, WCC×4, TK×102 |
| 2026-09-18 | +4.86 | $217.82 | SFL×108, FTAI×7, CBC×46, FPS×40, EROC×116, WCC×4, TK×102 | $10,465.03 | +67.48 | +35.37 | PURR, ARE, MNR, USDE, FLNC | SFL, FTAI, CBC, FPS, EROC, WCC, TK | $51.18 | $10,471.52 | PURR×151, ARE×36, MNR×190, USDE×219, FLNC×277 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 48 | $41.23 | $2.13 | — | $8,018.83 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover,oppset; ret5=+46.0; leftover $2000.00 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `JBIO` | 81 | $24.60 | $2.23 | — | $6,023.99 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+12.5; leftover $2000.00 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 63 | $31.30 | $2.18 | — | $4,049.91 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer,oppset; ret5=-3.8; leftover $2000.00 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ZNTL` | 561 | $3.56 | $7.24 | — | $2,045.52 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_mover; ret5=-15.6; leftover $2000.00 | join🟡 sector🔴 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 21 | $92.99 | $2.05 | — | $90.67 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; ret5=-0.8; leftover $2000.00 | join🟡 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.67 | ▲ close $10,015.68 vs 09:30 $10,000.00 (session +31.52) | 16:00 close · cash $90.67 · equity $10,015.68 vs 09:30 $10,000.00 (+15.68; session marks +31.52) · 5 name(s) marked open→close (per-name table). HTFL×48 09:30 $41.23 → close $41.94 +34.08; JBIO×81 09:30 $24.60 → close $23.45 -93.15; VERA×63 09:30 $31.30 → close $31.63 +20.79; ZNTL×561 09:30 $3.56 → close $3.71 +81.35; CELC×21 09:30 $92.99 → close $92.44 -11.55 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.67 | ▼ 09:30 equity $9,967.60 vs yday $10,015.68 (-48.08) | 09:30 open · cash $90.67 (unchanged overnight, no fees) · equity $9,967.60 vs prior close $10,015.68 (-48.08) · 5 name(s) re-marked at the open (per-name table). HTFL×48 yday $41.94 → 09:30 $41.50 -21.12; JBIO×81 yday $23.45 → 09:30 $23.07 -30.78; VERA×63 yday $31.63 → 09:30 $31.31 -20.16; ZNTL×561 yday $3.71 → 09:30 $3.75 +25.24; CELC×21 yday $92.44 → 09:30 $92.38 -1.26 | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 48 | $41.50 | $2.16 | $+8.67 | $2,080.51 | ▲ +8.67 after sell → book $9,965.44; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `JBIO` | 81 | $23.07 | $2.26 | $-128.42 | $3,946.92 | ▼ -128.42 after sell → book $9,963.18; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 63 | $31.31 | $2.21 | $-3.75 | $5,917.25 | ▼ -3.75 after sell → book $9,960.98; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ZNTL` | 561 | $3.75 | $7.35 | $+92.01 | $8,013.65 | ▲ +92.01 after sell → book $9,953.63; vs 09:30 mark -7.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 21 | $92.38 | $2.08 | $-16.94 | $9,951.55 | ▼ -16.94 after sell → book $9,951.55; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,951.55 | ▲ close $9,951.55 vs 09:30 $9,967.60 (session +0.00) | 16:00 close · cash $9,951.55 · no lots left · equity $9,951.55. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,951.55 | ▲ 09:30 equity $9,951.55 vs yday $9,951.55 (+0.00) | 09:30 open · cash $9,951.55 · no holdings · equity $9,951.55 vs prior close $9,951.55 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,951.55 | ▲ close $9,951.55 vs 09:30 $9,951.55 (session +0.00) | 16:00 close · cash $9,951.55 · no lots left · equity $9,951.55. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,951.55 | ▲ 09:30 equity $9,951.55 vs yday $9,951.55 (+0.00) | 09:30 open · cash $9,951.55 · no holdings · equity $9,951.55 vs prior close $9,951.55 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `HTHT` | 25 | $48.39 | $2.06 | — | $8,739.74 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+17.8; leftover $1243.94 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `RERE` | 296 | $4.20 | $3.82 | — | $7,492.72 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; 🔵; ret5=+2.9; leftover $1243.94 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 36 | $34.05 | $2.10 | — | $6,264.82 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; 🔵; ret5=+9.3; leftover $1243.94 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SG` | 193 | $6.43 | $2.57 | — | $5,021.26 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; 🔵; ret5=+10.3; leftover $1243.94 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZIM` | 45 | $27.45 | $2.12 | — | $3,783.89 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list oppset; 🔵; ret5=+8.5; leftover $1243.94 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SUI` | 10 | $121.21 | $2.02 | — | $2,569.77 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list oppset; 🔵; ret5=+3.1; leftover $1243.94 | join🔴 sector🔴 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 10 | $123.47 | $2.02 | — | $1,333.05 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; 🔵; ret5=+2.9; leftover $1243.94 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CTRE` | 31 | $39.79 | $2.08 | — | $97.47 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list oppset; 🔵; ret5=+1.7; leftover $1243.94 | join🟢 sector🔴 gen🟢 news🟡 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.47 | ▲ close $10,029.55 vs 09:30 $9,951.55 (session +96.80) | 16:00 close · cash $97.47 · equity $10,029.55 vs 09:30 $9,951.55 (+78.00; session marks +96.80) · 8 name(s) marked open→close (per-name table). HTHT×25 09:30 $48.39 → close $49.54 +28.75; RERE×296 09:30 $4.20 → close $4.08 -35.52; ATAT×36 09:30 $34.05 → close $34.25 +7.20; SG×193 09:30 $6.43 → close $6.58 +28.95; ZIM×45 09:30 $27.45 → close $27.16 -13.05; SUI×10 09:30 $121.21 → close $122.29 +10.80; BABA×10 09:30 $123.47 → close $130.53 +70.60; CTRE×31 09:30 $39.79 → close $39.76 -0.93 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.47 | ▲ 09:30 equity $10,037.28 vs yday $10,029.55 (+7.73) | 09:30 open · cash $97.47 (unchanged overnight, no fees) · equity $10,037.28 vs prior close $10,029.55 (+7.73) · 8 name(s) re-marked at the open (per-name table). HTHT×25 yday $49.54 → 09:30 $49.58 +1.00; RERE×296 yday $4.08 → 09:30 $4.17 +26.64; ATAT×36 yday $34.25 → 09:30 $34.31 +2.16; SG×193 yday $6.58 → 09:30 $6.61 +5.79; ZIM×45 yday $27.16 → 09:30 $27.50 +15.30; SUI×10 yday $122.29 → 09:30 $122.41 +1.20; BABA×10 yday $130.53 → 09:30 $125.35 -51.80; CTRE×31 yday $39.76 → 09:30 $40.00 +7.44 | — |
| 2026-08-21 09:30 ET | **SELL** | `HTHT` | 25 | $49.58 | $2.08 | $+25.60 | $1,334.89 | ▲ +25.60 after sell → book $10,035.20; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `RERE` | 296 | $4.17 | $3.88 | $-16.58 | $2,565.33 | ▼ -16.58 after sell → book $10,031.32; vs 09:30 mark -3.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 36 | $34.31 | $2.12 | $+5.14 | $3,798.37 | ▲ +5.14 after sell → book $10,029.20; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SG` | 193 | $6.61 | $2.61 | $+29.56 | $5,071.49 | ▲ +29.56 after sell → book $10,026.59; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZIM` | 45 | $27.50 | $2.15 | $-2.02 | $6,306.85 | ▼ -2.02 after sell → book $10,024.45; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SUI` | 10 | $122.41 | $2.04 | $+7.94 | $7,528.91 | ▲ +7.94 after sell → book $10,022.41; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BABA` | 10 | $125.35 | $2.04 | $+14.74 | $8,780.37 | ▲ +14.74 after sell → book $10,020.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CTRE` | 31 | $40.00 | $2.10 | $+2.32 | $10,018.26 | ▲ +2.32 after sell → book $10,018.26; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `SM` | 33 | $37.81 | $2.09 | — | $8,768.45 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+16.1; leftover $1252.28 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TALO` | 70 | $17.88 | $2.20 | — | $7,514.65 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+14.9; leftover $1252.28 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $6,266.13 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1252.28 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TRON` | 645 | $1.94 | $8.32 | — | $5,006.51 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,ohlc_hot; ret5=+15.4; leftover $1252.28 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `VIK` | 13 | $91.00 | $2.03 | — | $3,821.48 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list oppset; ret5=-14.7; leftover $1252.28 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1449 | $0.86 | $16.87 | — | $2,552.68 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1252.28 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 29 | $43.08 | $2.08 | — | $1,301.28 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; ret5=-4.9; leftover $1252.28 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 13 | $93.98 | $2.03 | — | $77.51 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; ret5=-2.4; leftover $1252.28 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $77.51 | ▲ close $10,124.74 vs 09:30 $10,037.28 (session +144.08) | 16:00 close · cash $77.51 · equity $10,124.74 vs 09:30 $10,037.28 (+87.46; session marks +144.08) · 8 name(s) marked open→close (per-name table). SM×33 09:30 $37.81 → close $37.20 -20.13; TALO×70 09:30 $17.88 → close $17.47 -28.70; DE×2 09:30 $623.26 → close $647.47 +48.42; TRON×645 09:30 $1.94 → close $2.01 +45.15; VIK×13 09:30 $91.00 → close $92.79 +23.27; ORBS×1449 09:30 $0.86 → close $0.88 +23.18; BKE×29 09:30 $43.08 → close $43.81 +21.17; BJ×13 09:30 $93.98 → close $96.42 +31.72 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $77.51 | ▲ 09:30 equity $10,144.45 vs yday $10,124.74 (+19.71) | 09:30 open · cash $77.51 (unchanged overnight, no fees) · equity $10,144.45 vs prior close $10,124.74 (+19.71) · 8 name(s) re-marked at the open (per-name table). SM×33 yday $37.20 → 09:30 $36.61 -19.47; TALO×70 yday $17.47 → 09:30 $17.24 -16.10; DE×2 yday $647.47 → 09:30 $653.04 +11.14; TRON×645 yday $2.01 → 09:30 $2.02 +6.45; VIK×13 yday $92.79 → 09:30 $93.06 +3.51; ORBS×1449 yday $0.88 → 09:30 $0.89 +14.49; BKE×29 yday $43.81 → 09:30 $44.22 +11.89; BJ×13 yday $96.42 → 09:30 $97.02 +7.80 | — |
| 2026-08-24 09:30 ET | **SELL** | `SM` | 33 | $36.61 | $2.11 | $-43.80 | $1,283.53 | ▼ -43.80 after sell → book $10,142.34; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TALO` | 70 | $17.24 | $2.22 | $-49.22 | $2,488.11 | ▼ -49.22 after sell → book $10,140.12; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $3,792.17 | ▲ +55.55 after sell → book $10,138.10; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TRON` | 645 | $2.02 | $8.44 | $+34.84 | $5,086.64 | ▲ +34.84 after sell → book $10,129.67; vs 09:30 mark -8.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `VIK` | 13 | $93.06 | $2.05 | $+22.70 | $6,294.37 | ▲ +22.70 after sell → book $10,127.62; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1449 | $0.89 | $17.49 | $+3.31 | $7,566.48 | ▲ +3.31 after sell → book $10,110.12; vs 09:30 mark -17.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 29 | $44.22 | $2.10 | $+28.89 | $8,846.77 | ▲ +28.89 after sell → book $10,108.03; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 13 | $97.02 | $2.05 | $+35.44 | $10,105.98 | ▲ +35.44 after sell → book $10,105.98; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,105.98 | ▲ close $10,105.98 vs 09:30 $10,144.45 (session +0.00) | 16:00 close · cash $10,105.98 · no lots left · equity $10,105.98. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,105.98 | ▲ 09:30 equity $10,105.98 vs yday $10,105.98 (-0.00) | 09:30 open · cash $10,105.98 · no holdings · equity $10,105.98 vs prior close $10,105.98 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,105.98 | ▲ close $10,105.98 vs 09:30 $10,105.98 (session +0.00) | 16:00 close · cash $10,105.98 · no lots left · equity $10,105.98. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,105.98 | ▲ 09:30 equity $10,105.98 vs yday $10,105.98 (-0.00) | 09:30 open · cash $10,105.98 · no holdings · equity $10,105.98 vs prior close $10,105.98 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 09:30 ET | **BUY** | `LI` | 104 | $12.14 | $2.30 | — | $8,841.11 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; ret5=+1.2; leftover $1263.25 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 90 | $14.00 | $2.26 | — | $7,578.85 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+17.8; leftover $1263.25 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SFL` | 102 | $12.35 | $2.30 | — | $6,316.86 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; ret5=-1.7; leftover $1263.25 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `VIPS` | 90 | $14.00 | $2.26 | — | $5,054.60 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list oppset; ret5=-0.4; leftover $1263.25 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AXTI` | 19 | $65.34 | $2.05 | — | $3,811.09 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1263.25 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 5 | $213.94 | $2.00 | — | $2,739.39 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1263.25 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `GRRR` | 90 | $14.03 | $2.26 | — | $1,474.43 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_mover; ret5=-7.6; leftover $1263.25 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SMTC` | 9 | $130.90 | $2.02 | — | $294.31 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react,oppset; 🔵; ret5=-5.7; leftover $1263.25 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $294.31 | ▲ close $10,173.20 vs 09:30 $10,105.98 (session +84.67) | 16:00 close · cash $294.31 · equity $10,173.20 vs 09:30 $10,105.98 (+67.22; session marks +84.67) · 8 name(s) marked open→close (per-name table). LI×104 09:30 $12.14 → close $12.14 +0.00; MNRO×90 09:30 $14.00 → close $12.61 -125.10; SFL×102 09:30 $12.35 → close $12.03 -32.64; VIPS×90 09:30 $14.00 → close $14.08 +7.20; AXTI×19 09:30 $65.34 → close $65.18 -3.04; BE×5 09:30 $213.94 → close $218.21 +21.35; GRRR×90 09:30 $14.03 → close $15.45 +127.80; SMTC×9 09:30 $130.90 → close $140.80 +89.10 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $294.31 | ▲ 09:30 equity $10,446.57 vs yday $10,173.20 (+273.37) | 09:30 open · cash $294.31 (unchanged overnight, no fees) · equity $10,446.57 vs prior close $10,173.20 (+273.37) · 8 name(s) re-marked at the open (per-name table). LI×104 yday $12.14 → 09:30 $12.35 +21.84; MNRO×90 yday $12.61 → 09:30 $12.56 -4.50; SFL×102 yday $12.03 → 09:30 $12.03 +0.00; VIPS×90 yday $14.08 → 09:30 $14.00 -7.20; AXTI×19 yday $65.18 → 09:30 $70.30 +97.28; BE×5 yday $218.21 → 09:30 $227.10 +44.45; GRRR×90 yday $15.45 → 09:30 $15.94 +44.10; SMTC×9 yday $140.80 → 09:30 $149.40 +77.40 | — |
| 2026-08-27 09:30 ET | **SELL** | `LI` | 104 | $12.35 | $2.33 | $+17.21 | $1,576.38 | ▲ +17.21 after sell → book $10,444.24; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MNRO` | 90 | $12.56 | $2.28 | $-134.14 | $2,704.50 | ▼ -134.14 after sell → book $10,441.96; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SFL` | 102 | $12.03 | $2.32 | $-37.26 | $3,929.23 | ▼ -37.26 after sell → book $10,439.63; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `VIPS` | 90 | $14.00 | $2.29 | $-4.55 | $5,186.95 | ▼ -4.55 after sell → book $10,437.35; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AXTI` | 19 | $70.30 | $2.07 | $+90.13 | $6,520.58 | ▲ +90.13 after sell → book $10,435.28; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `GRRR` | 90 | $15.94 | $2.29 | $+167.35 | $7,952.89 | ▲ +167.35 after sell → book $10,432.99; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `SMTC` | 9 | $149.40 | $2.04 | $+162.45 | $9,295.46 | ▲ +162.45 after sell → book $10,430.96; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DASH` | 5 | $235.94 | $2.00 | — | $8,113.75 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+7.6; leftover $1327.92 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `AEO` | 76 | $17.27 | $2.22 | — | $6,799.01 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+5.5; leftover $1327.92 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 16 | $80.60 | $2.04 | — | $5,507.37 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; ret5=-2.0; leftover $1327.92 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DKS` | 10 | $128.73 | $2.02 | — | $4,218.05 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_mover; ret5=-32.2; leftover $1327.92 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 32 | $41.44 | $2.09 | — | $2,889.89 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list flatten; ret5=+3.1; leftover $1327.92 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GAP` | 63 | $20.75 | $2.18 | — | $1,580.46 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+5.2; leftover $1327.92 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 92 | $14.42 | $2.27 | — | $251.55 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list flatten; ret5=+7.1; leftover $1327.92 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $251.55 | ▼ close $10,410.54 vs 09:30 $10,446.57 (session -5.60) | 16:00 close · cash $251.55 · equity $10,410.54 vs 09:30 $10,446.57 (-36.03; session marks -5.60) · 8 name(s) marked open→close (per-name table). BE×5 09:30 $227.10 → close $217.83 -46.35; DASH×5 09:30 $235.94 → close $231.89 -20.25; AEO×76 09:30 $17.27 → close $16.69 -44.08; BBY×16 09:30 $80.60 → close $83.56 +47.36; DKS×10 09:30 $128.73 → close $131.77 +30.40; RRC×32 09:30 $41.44 → close $41.64 +6.40; GAP×63 09:30 $20.75 → close $20.79 +2.52; CRK×92 09:30 $14.42 → close $14.62 +18.40 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $251.55 | ▲ 09:30 equity $10,700.20 vs yday $10,410.54 (+289.66) | 09:30 open · cash $251.55 (unchanged overnight, no fees) · equity $10,700.20 vs prior close $10,410.54 (+289.66) · 8 name(s) re-marked at the open (per-name table). BE×5 yday $217.83 → 09:30 $215.71 -10.62; DASH×5 yday $231.89 → 09:30 $233.37 +7.40; AEO×76 yday $16.69 → 09:30 $17.06 +28.12; BBY×16 yday $83.56 → 09:30 $83.85 +4.64; DKS×10 yday $131.77 → 09:30 $132.80 +10.30; RRC×32 yday $41.64 → 09:30 $41.74 +3.20; GAP×63 yday $20.79 → 09:30 $24.69 +245.70; CRK×92 yday $14.62 → 09:30 $14.63 +0.92 | — |
| 2026-08-28 09:30 ET | **SELL** | `BE` | 5 | $215.71 | $2.02 | $+4.79 | $1,328.05 | ▲ +4.79 after sell → book $10,698.17; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DASH` | 5 | $233.37 | $2.02 | $-16.88 | $2,492.88 | ▼ -16.88 after sell → book $10,696.15; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEO` | 76 | $17.06 | $2.24 | $-20.42 | $3,787.20 | ▼ -20.42 after sell → book $10,693.91; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BBY` | 16 | $83.85 | $2.06 | $+47.90 | $5,126.74 | ▲ +47.90 after sell → book $10,691.85; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 10 | $132.80 | $2.04 | $+36.64 | $6,452.70 | ▲ +36.64 after sell → book $10,689.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `GAP` | 63 | $24.69 | $2.20 | $+243.84 | $8,005.97 | ▲ +243.84 after sell → book $10,687.61; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 92 | $14.63 | $2.29 | $+14.76 | $9,349.63 | ▲ +14.76 after sell → book $10,685.31; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $8,032.99 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ret5=+38.8; leftover $1335.66 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TGB` | 136 | $9.75 | $2.40 | — | $6,704.59 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+11.4; leftover $1335.66 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 70 | $19.00 | $2.20 | — | $5,372.39 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+7.5; leftover $1335.66 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SLF` | 16 | $78.95 | $2.04 | — | $4,107.15 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list oppset; ret5=+0.4; leftover $1335.66 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TRMD` | 41 | $32.23 | $2.11 | — | $2,783.61 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list oppset; 🔵; ret5=+2.4; leftover $1335.66 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `WSM` | 5 | $235.67 | $2.00 | — | $1,603.25 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list oppset; ret5=+1.1; leftover $1335.66 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 35 | $37.49 | $2.10 | — | $289.01 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_mover; ret5=+5.4; leftover $1335.66 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $289.01 | ▼ close $10,533.27 vs 09:30 $10,700.20 (session -137.18) | 16:00 close · cash $289.01 · equity $10,533.27 vs 09:30 $10,700.20 (-166.93; session marks -137.18) · 8 name(s) marked open→close (per-name table). RRC×32 09:30 $41.74 → close $41.46 -8.96; ANF×9 09:30 $146.07 → close $148.42 +21.15; TGB×136 09:30 $9.75 → close $9.18 -77.52; TH×70 09:30 $19.00 → close $18.55 -31.50; SLF×16 09:30 $78.95 → close $78.76 -3.04; TRMD×41 09:30 $32.23 → close $32.62 +15.99; WSM×5 09:30 $235.67 → close $235.09 -2.90; FIGR×35 09:30 $37.49 → close $36.05 -50.40 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $289.01 | ▼ 09:30 equity $10,506.57 vs yday $10,533.27 (-26.70) | 09:30 open · cash $289.01 (unchanged overnight, no fees) · equity $10,506.57 vs prior close $10,533.27 (-26.70) · 8 name(s) re-marked at the open (per-name table). RRC×32 yday $41.46 → 09:30 $42.00 +17.28; ANF×9 yday $148.42 → 09:30 $148.03 -3.51; TGB×136 yday $9.18 → 09:30 $9.15 -4.08; TH×70 yday $18.55 → 09:30 $18.12 -29.75; SLF×16 yday $78.76 → 09:30 $78.70 -0.96; TRMD×41 yday $32.62 → 09:30 $33.09 +19.27; WSM×5 yday $235.09 → 09:30 $232.06 -15.15; FIGR×35 yday $36.05 → 09:30 $35.77 -9.80 | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 32 | $42.00 | $2.11 | $+13.73 | $1,630.90 | ▲ +13.73 after sell → book $10,504.46; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $2,961.13 | ▲ +13.59 after sell → book $10,502.42; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TGB` | 136 | $9.15 | $2.43 | $-86.43 | $4,203.10 | ▼ -86.43 after sell → book $10,499.99; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 70 | $18.12 | $2.22 | $-65.67 | $5,469.63 | ▼ -65.67 after sell → book $10,497.77; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `SLF` | 16 | $78.70 | $2.06 | $-8.10 | $6,726.77 | ▼ -8.10 after sell → book $10,495.71; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TRMD` | 41 | $33.09 | $2.13 | $+31.01 | $8,081.33 | ▲ +31.01 after sell → book $10,493.58; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `WSM` | 5 | $232.06 | $2.02 | $-22.08 | $9,239.60 | ▼ -22.08 after sell → book $10,491.55; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 35 | $35.77 | $2.12 | $-64.41 | $10,489.44 | ▼ -64.41 after sell → book $10,489.44; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,489.44 | ▲ close $10,489.44 vs 09:30 $10,506.57 (session +0.00) | 16:00 close · cash $10,489.44 · no lots left · equity $10,489.44. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,489.44 | ▲ 09:30 equity $10,489.44 vs yday $10,489.44 (-0.00) | 09:30 open · cash $10,489.44 · no holdings · equity $10,489.44 vs prior close $10,489.44 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,489.44 | ▲ close $10,489.44 vs 09:30 $10,489.44 (session +0.00) | 16:00 close · cash $10,489.44 · no lots left · equity $10,489.44. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,489.44 | ▲ 09:30 equity $10,489.44 vs yday $10,489.44 (-0.00) | 09:30 open · cash $10,489.44 · no holdings · equity $10,489.44 vs prior close $10,489.44 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,489.44 | ▲ close $10,489.44 vs 09:30 $10,489.44 (session +0.00) | 16:00 close · cash $10,489.44 · no lots left · equity $10,489.44. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,489.44 | ▲ 09:30 equity $10,489.44 vs yday $10,489.44 (-0.00) | 09:30 open · cash $10,489.44 · no holdings · equity $10,489.44 vs prior close $10,489.44 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `PBF` | 17 | $74.75 | $2.04 | — | $9,216.65 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list mover_buy; 🔵; ret5=+8.2; leftover $1311.18 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `ETD` | 60 | $21.82 | $2.17 | — | $7,905.28 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list oppset; 🔵; ret5=+4.7; leftover $1311.18 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PBR` | 61 | $21.18 | $2.17 | — | $6,611.13 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+17.5; leftover $1311.18 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `XP` | 63 | $20.74 | $2.18 | — | $5,302.33 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+12.2; leftover $1311.18 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HP` | 27 | $47.74 | $2.07 | — | $4,011.28 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+15.1; leftover $1311.18 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PBR-A` | 68 | $19.16 | $2.19 | — | $2,706.20 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+17.4; leftover $1311.18 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VIST` | 16 | $77.14 | $2.04 | — | $1,469.92 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+12.2; leftover $1311.18 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GRNT` | 254 | $5.15 | $3.28 | — | $158.55 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list oppset; ret5=+3.2; leftover $1311.18 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.55 | ▼ close $10,225.53 vs 09:30 $10,489.44 (session -245.77) | 16:00 close · cash $158.55 · equity $10,225.53 vs 09:30 $10,489.44 (-263.91; session marks -245.77) · 8 name(s) marked open→close (per-name table). PBF×17 09:30 $74.75 → close $75.33 +9.86; ETD×60 09:30 $21.82 → close $21.87 +3.00; PBR×61 09:30 $21.18 → close $20.51 -40.87; XP×63 09:30 $20.74 → close $20.00 -46.62; HP×27 09:30 $47.74 → close $45.02 -73.44; PBR-A×68 09:30 $19.16 → close $18.58 -39.44; VIST×16 09:30 $77.14 → close $74.61 -40.48; GRNT×254 09:30 $5.15 → close $5.08 -17.78 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.55 | ▼ 09:30 equity $10,123.46 vs yday $10,225.53 (-102.07) | 09:30 open · cash $158.55 (unchanged overnight, no fees) · equity $10,123.46 vs prior close $10,225.53 (-102.07) · 8 name(s) re-marked at the open (per-name table). PBF×17 yday $75.33 → 09:30 $74.50 -14.11; ETD×60 yday $21.87 → 09:30 $21.84 -1.80; PBR×61 yday $20.51 → 09:30 $20.25 -15.86; XP×63 yday $20.00 → 09:30 $19.67 -20.79; HP×27 yday $45.02 → 09:30 $44.59 -11.61; PBR-A×68 yday $18.58 → 09:30 $18.36 -14.96; VIST×16 yday $74.61 → 09:30 $73.97 -10.24; GRNT×254 yday $5.08 → 09:30 $5.03 -12.70 | — |
| 2026-09-04 09:30 ET | **SELL** | `PBF` | 17 | $74.50 | $2.06 | $-8.35 | $1,422.99 | ▼ -8.35 after sell → book $10,121.40; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ETD` | 60 | $21.84 | $2.19 | $-3.16 | $2,731.20 | ▼ -3.16 after sell → book $10,119.21; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBR` | 61 | $20.25 | $2.19 | $-61.10 | $3,964.25 | ▼ -61.10 after sell → book $10,117.01; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `XP` | 63 | $19.67 | $2.20 | $-71.79 | $5,201.26 | ▼ -71.79 after sell → book $10,114.81; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `HP` | 27 | $44.59 | $2.09 | $-89.21 | $6,403.10 | ▼ -89.21 after sell → book $10,112.72; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBR-A` | 68 | $18.36 | $2.22 | $-58.81 | $7,649.37 | ▼ -58.81 after sell → book $10,110.51; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VIST` | 16 | $73.97 | $2.06 | $-54.82 | $8,830.83 | ▼ -54.82 after sell → book $10,108.45; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `GRNT` | 254 | $5.03 | $3.33 | $-37.09 | $10,105.12 | ▼ -37.09 after sell → book $10,105.12; vs 09:30 mark -3.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 5 | $236.82 | $2.00 | — | $8,919.01 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+8.1; leftover $1263.14 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 141 | $8.94 | $2.41 | — | $7,656.06 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+7.7; leftover $1263.14 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MIR` | 76 | $16.60 | $2.22 | — | $6,392.24 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+12.9; leftover $1263.14 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GORO` | 319 | $3.95 | $4.12 | — | $5,128.08 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+6.9; leftover $1263.14 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GSM` | 270 | $4.67 | $3.48 | — | $3,863.70 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; ret5=+11.9; leftover $1263.14 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLGN` | 30 | $41.16 | $2.08 | — | $2,626.82 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list oppset; 🔵; ret5=-0.8; leftover $1263.14 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `WNC` | 89 | $14.17 | $2.26 | — | $1,363.43 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; 🔵; ret5=+7.9; leftover $1263.14 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `XRX` | 381 | $3.31 | $4.91 | — | $97.40 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+11.1; leftover $1263.14 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.40 | ▲ close $10,307.11 vs 09:30 $10,123.46 (session +225.48) | 16:00 close · cash $97.40 · equity $10,307.11 vs 09:30 $10,123.46 (+183.65; session marks +225.48) · 8 name(s) marked open→close (per-name table). BE×5 09:30 $236.82 → close $252.87 +80.25; HAFN×141 09:30 $8.94 → close $9.22 +39.48; MIR×76 09:30 $16.60 → close $16.93 +25.08; GORO×319 09:30 $3.95 → close $4.15 +63.80; GSM×270 09:30 $4.67 → close $4.67 +0.00; SLGN×30 09:30 $41.16 → close $41.18 +0.60; WNC×89 09:30 $14.17 → close $14.31 +12.46; XRX×381 09:30 $3.31 → close $3.32 +3.81 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.40 | ▲ 09:30 equity $10,392.78 vs yday $10,307.11 (+85.67) | 09:30 open · cash $97.40 (unchanged overnight, no fees) · equity $10,392.78 vs prior close $10,307.11 (+85.67) · 8 name(s) re-marked at the open (per-name table). BE×5 yday $252.87 → 09:30 $267.76 +74.45; HAFN×141 yday $9.22 → 09:30 $8.81 -57.81; MIR×76 yday $16.93 → 09:30 $17.07 +10.64; GORO×319 yday $4.15 → 09:30 $4.13 -6.38; GSM×270 yday $4.67 → 09:30 $4.75 +21.60; SLGN×30 yday $41.18 → 09:30 $40.60 -17.40; WNC×89 yday $14.31 → 09:30 $14.22 -8.01; XRX×381 yday $3.32 → 09:30 $3.50 +68.58 | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 5 | $267.76 | $2.03 | $+150.67 | $1,434.18 | ▲ +150.67 after sell → book $10,390.76; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HAFN` | 141 | $8.81 | $2.45 | $-23.19 | $2,673.94 | ▼ -23.19 after sell → book $10,388.31; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MIR` | 76 | $17.07 | $2.24 | $+31.26 | $3,969.02 | ▲ +31.26 after sell → book $10,386.07; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GORO` | 319 | $4.13 | $4.18 | $+49.13 | $5,282.31 | ▲ +49.13 after sell → book $10,381.89; vs 09:30 mark -4.18 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `GSM` | 270 | $4.75 | $3.54 | $+14.58 | $6,561.27 | ▲ +14.58 after sell → book $10,378.35; vs 09:30 mark -3.54 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `SLGN` | 30 | $40.60 | $2.10 | $-20.98 | $7,777.17 | ▼ -20.98 after sell → book $10,376.25; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `WNC` | 89 | $14.22 | $2.28 | $-0.09 | $9,040.47 | ▼ -0.09 after sell → book $10,373.97; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `XRX` | 381 | $3.50 | $4.99 | $+62.49 | $10,368.98 | ▲ +62.49 after sell → book $10,368.98; vs 09:30 mark -4.99 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,368.98 | ▲ close $10,368.98 vs 09:30 $10,392.78 (session +0.00) | 16:00 close · cash $10,368.98 · no lots left · equity $10,368.98. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,368.98 | ▲ 09:30 equity $10,368.98 vs yday $10,368.98 (+0.00) | 09:30 open · cash $10,368.98 · no holdings · equity $10,368.98 vs prior close $10,368.98 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,368.98 | ▲ close $10,368.98 vs 09:30 $10,368.98 (session +0.00) | 16:00 close · cash $10,368.98 · no lots left · equity $10,368.98. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,368.98 | ▲ 09:30 equity $10,368.98 vs yday $10,368.98 (+0.00) | 09:30 open · cash $10,368.98 · no holdings · equity $10,368.98 vs prior close $10,368.98 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,368.98 | ▲ close $10,368.98 vs 09:30 $10,368.98 (session +0.00) | 16:00 close · cash $10,368.98 · no lots left · equity $10,368.98. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,368.98 | ▲ 09:30 equity $10,368.98 vs yday $10,368.98 (+0.00) | 09:30 open · cash $10,368.98 · no holdings · equity $10,368.98 vs prior close $10,368.98 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 23 | $54.91 | $2.06 | — | $9,103.99 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; 🔵; ret5=+24.3; leftover $1296.12 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 263 | $4.91 | $3.39 | — | $7,809.27 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1296.12 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `GFR` | 209 | $6.19 | $2.70 | — | $6,512.87 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list oppset; ret5=+1.1; leftover $1296.12 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `OBE` | 103 | $12.55 | $2.30 | — | $5,217.92 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list oppset; ret5=+5.5; leftover $1296.12 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PBR` | 61 | $21.21 | $2.17 | — | $3,921.93 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+2.5; leftover $1296.12 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 16 | $77.33 | $2.04 | — | $2,682.62 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; ret5=+2.5; leftover $1296.12 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CNQ` | 25 | $49.94 | $2.06 | — | $1,432.05 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list oppset; ret5=+1.7; leftover $1296.12 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AVAV` | 8 | $145.91 | $2.01 | — | $262.76 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list oppset; 🔵; ret5=+1.2; leftover $1296.12 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $262.76 | ▲ close $10,435.98 vs 09:30 $10,368.98 (session +85.73) | 16:00 close · cash $262.76 · equity $10,435.98 vs 09:30 $10,368.98 (+67.00; session marks +85.73) · 8 name(s) marked open→close (per-name table). ASO×23 09:30 $54.91 → close $55.36 +10.35; BNC×263 09:30 $4.91 → close $4.80 -28.93; GFR×209 09:30 $6.19 → close $6.52 +68.97; OBE×103 09:30 $12.55 → close $12.97 +43.26; PBR×61 09:30 $21.21 → close $21.20 -0.61; VIST×16 09:30 $77.33 → close $76.27 -16.96; CNQ×25 09:30 $49.94 → close $50.07 +3.25; AVAV×8 09:30 $145.91 → close $146.71 +6.40 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $262.76 | ▲ 09:30 equity $10,586.04 vs yday $10,435.98 (+150.06) | 09:30 open · cash $262.76 (unchanged overnight, no fees) · equity $10,586.04 vs prior close $10,435.98 (+150.06) · 8 name(s) re-marked at the open (per-name table). ASO×23 yday $55.36 → 09:30 $54.75 -14.03; BNC×263 yday $4.80 → 09:30 $5.03 +60.49; GFR×209 yday $6.52 → 09:30 $6.60 +16.72; OBE×103 yday $12.97 → 09:30 $13.57 +61.80; PBR×61 yday $21.20 → 09:30 $21.23 +1.83; VIST×16 yday $76.27 → 09:30 $77.10 +13.28; CNQ×25 yday $50.07 → 09:30 $50.76 +17.25; AVAV×8 yday $146.71 → 09:30 $145.80 -7.28 | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 23 | $54.75 | $2.08 | $-7.82 | $1,519.93 | ▼ -7.82 after sell → book $10,583.96; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 263 | $5.03 | $3.45 | $+24.72 | $2,839.37 | ▲ +24.72 after sell → book $10,580.51; vs 09:30 mark -3.45 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `GFR` | 209 | $6.60 | $2.74 | $+80.25 | $4,216.03 | ▲ +80.25 after sell → book $10,577.77; vs 09:30 mark -2.74 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `OBE` | 103 | $13.57 | $2.33 | $+100.43 | $5,611.41 | ▲ +100.43 after sell → book $10,575.44; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PBR` | 61 | $21.23 | $2.19 | $-3.15 | $6,904.25 | ▼ -3.15 after sell → book $10,573.25; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIST` | 16 | $77.10 | $2.06 | $-7.78 | $8,135.79 | ▼ -7.78 after sell → book $10,571.19; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CNQ` | 25 | $50.76 | $2.09 | $+16.35 | $9,402.70 | ▲ +16.35 after sell → book $10,569.10; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AVAV` | 8 | $145.80 | $2.03 | $-4.93 | $10,567.07 | ▼ -4.93 after sell → book $10,567.07; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,567.07 | ▲ close $10,567.07 vs 09:30 $10,586.04 (session +0.00) | 16:00 close · cash $10,567.07 · no lots left · equity $10,567.07. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,567.07 | ▲ 09:30 equity $10,567.07 vs yday $10,567.07 (+0.00) | 09:30 open · cash $10,567.07 · no holdings · equity $10,567.07 vs prior close $10,567.07 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,567.07 | ▲ close $10,567.07 vs 09:30 $10,567.07 (session +0.00) | 16:00 close · cash $10,567.07 · no lots left · equity $10,567.07. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,567.07 | ▲ 09:30 equity $10,567.07 vs yday $10,567.07 (+0.00) | 09:30 open · cash $10,567.07 · no holdings · equity $10,567.07 vs prior close $10,567.07 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 33 | $39.99 | $2.09 | — | $9,245.31 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1320.88 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `APA` | 28 | $46.44 | $2.07 | — | $7,942.92 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+8.9; leftover $1320.88 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `CVI` | 25 | $51.05 | $2.06 | — | $6,664.60 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; ret5=+13.1; leftover $1320.88 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `MEOH` | 20 | $63.34 | $2.05 | — | $5,395.75 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list oppset; 🔵; ret5=+2.4; leftover $1320.88 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 225 | $5.87 | $2.90 | — | $4,072.10 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1320.88 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 catal🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 15 | $87.40 | $2.04 | — | $2,759.06 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1320.88 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `VLO` | 3 | $391.68 | $2.00 | — | $1,582.03 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+6.7; leftover $1320.88 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `FRO` | 25 | $52.52 | $2.06 | — | $266.96 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+10.7; leftover $1320.88 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $266.96 | ▼ close $10,372.70 vs 09:30 $10,567.07 (session -177.09) | 16:00 close · cash $266.96 · equity $10,372.70 vs 09:30 $10,567.07 (-194.37; session marks -177.09) · 8 name(s) marked open→close (per-name table). SM×33 09:30 $39.99 → close $38.16 -60.39; APA×28 09:30 $46.44 → close $44.79 -46.20; CVI×25 09:30 $51.05 → close $53.05 +50.00; MEOH×20 09:30 $63.34 → close $61.51 -36.60; RIG×225 09:30 $5.87 → close $5.54 -74.25; VAL×15 09:30 $87.40 → close $82.52 -73.20; VLO×3 09:30 $391.68 → close $403.28 +34.80; FRO×25 09:30 $52.52 → close $53.67 +28.75 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $266.96 | ▼ 09:30 equity $10,326.61 vs yday $10,372.70 (-46.09) | 09:30 open · cash $266.96 (unchanged overnight, no fees) · equity $10,326.61 vs prior close $10,372.70 (-46.09) · 8 name(s) re-marked at the open (per-name table). SM×33 yday $38.16 → 09:30 $37.57 -19.47; APA×28 yday $44.79 → 09:30 $44.63 -4.48; CVI×25 yday $53.05 → 09:30 $51.88 -29.25; MEOH×20 yday $61.51 → 09:30 $60.83 -13.60; RIG×225 yday $5.54 → 09:30 $5.58 +9.00; VAL×15 yday $82.52 → 09:30 $83.20 +10.20; VLO×3 yday $403.28 → 09:30 $398.45 -14.49; FRO×25 yday $53.67 → 09:30 $54.31 +16.00 | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 33 | $37.57 | $2.11 | $-84.06 | $1,504.66 | ▼ -84.06 after sell → book $10,324.50; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `APA` | 28 | $44.63 | $2.09 | $-54.85 | $2,752.21 | ▼ -54.85 after sell → book $10,322.41; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CVI` | 25 | $51.88 | $2.09 | $+16.60 | $4,047.12 | ▲ +16.60 after sell → book $10,320.32; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `MEOH` | 20 | $60.83 | $2.07 | $-54.32 | $5,261.65 | ▼ -54.32 after sell → book $10,318.25; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 225 | $5.58 | $2.95 | $-71.10 | $6,514.20 | ▼ -71.10 after sell → book $10,315.30; vs 09:30 mark -2.95 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 15 | $83.20 | $2.06 | $-67.09 | $7,760.15 | ▼ -67.09 after sell → book $10,313.25; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VLO` | 3 | $398.45 | $2.02 | $+16.29 | $8,953.48 | ▲ +16.29 after sell → book $10,311.23; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `FRO` | 25 | $54.31 | $2.09 | $+40.60 | $10,309.14 | ▲ +40.60 after sell → book $10,309.14; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SFL` | 108 | $13.55 | $2.31 | — | $8,843.43 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+7.8; leftover $1472.73 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `FTAI` | 7 | $196.50 | $2.01 | — | $7,465.92 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; 🔵; ret5=+2.5; leftover $1472.73 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `CBC` | 46 | $31.60 | $2.13 | — | $6,010.19 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list oppset; ret5=+1.3; leftover $1472.73 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 40 | $36.76 | $2.11 | — | $4,537.68 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover,ohlc_hot,oppset; ret5=+12.4; leftover $1472.73 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `EROC` | 116 | $12.64 | $2.34 | — | $3,069.10 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; ret5=-3.6; leftover $1472.73 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `WCC` | 4 | $344.29 | $2.00 | — | $1,689.94 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+5.8; leftover $1472.73 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `TK` | 102 | $14.41 | $2.30 | — | $217.82 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+6.8; leftover $1472.73 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $217.82 | ▲ close $10,397.55 vs 09:30 $10,326.61 (session +103.61) | 16:00 close · cash $217.82 · equity $10,397.55 vs 09:30 $10,326.61 (+70.94; session marks +103.61) · 7 name(s) marked open→close (per-name table). SFL×108 09:30 $13.55 → close $13.75 +21.60; FTAI×7 09:30 $196.50 → close $195.07 -10.01; CBC×46 09:30 $31.60 → close $31.67 +3.22; FPS×40 09:30 $36.76 → close $38.06 +52.00; EROC×116 09:30 $12.64 → close $12.90 +30.16; WCC×4 09:30 $344.29 → close $339.32 -19.88; TK×102 09:30 $14.41 → close $14.67 +26.52 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $217.82 | ▲ 09:30 equity $10,465.03 vs yday $10,397.55 (+67.48) | 09:30 open · cash $217.82 (unchanged overnight, no fees) · equity $10,465.03 vs prior close $10,397.55 (+67.48) · 7 name(s) re-marked at the open (per-name table). SFL×108 yday $13.75 → 09:30 $13.74 -1.08; FTAI×7 yday $195.07 → 09:30 $195.55 +3.36; CBC×46 yday $31.67 → 09:30 $31.64 -1.38; FPS×40 yday $38.06 → 09:30 $39.50 +57.60; EROC×116 yday $12.90 → 09:30 $13.00 +11.60; WCC×4 yday $339.32 → 09:30 $340.45 +4.52; TK×102 yday $14.67 → 09:30 $14.60 -7.14 | — |
| 2026-09-18 09:30 ET | **SELL** | `SFL` | 108 | $13.74 | $2.34 | $+15.86 | $1,699.40 | ▲ +15.86 after sell → book $10,462.69; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `FTAI` | 7 | $195.55 | $2.03 | $-10.69 | $3,066.22 | ▼ -10.69 after sell → book $10,460.66; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CBC` | 46 | $31.64 | $2.15 | $-2.44 | $4,519.51 | ▼ -2.44 after sell → book $10,458.51; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 40 | $39.50 | $2.13 | $+105.36 | $6,097.38 | ▲ +105.36 after sell → book $10,456.38; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `EROC` | 116 | $13.00 | $2.37 | $+37.05 | $7,603.01 | ▲ +37.05 after sell → book $10,454.01; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `WCC` | 4 | $340.45 | $2.02 | $-19.38 | $8,962.78 | ▼ -19.38 after sell → book $10,451.98; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TK` | 102 | $14.60 | $2.32 | $+14.76 | $10,449.66 | ▲ +14.76 after sell → book $10,449.66; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `PURR` | 151 | $13.82 | $2.44 | — | $8,360.40 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; ret5=-9.1; leftover $2089.93 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `ARE` | 36 | $56.70 | $2.10 | — | $6,317.10 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+13.7; leftover $2089.93 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `MNR` | 190 | $10.95 | $2.56 | — | $4,234.04 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list oppset; ret5=+1.7; leftover $2089.93 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `USDE` | 219 | $9.54 | $2.83 | — | $2,141.95 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover,oppset; ret5=+15.8; leftover $2089.93 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 277 | $7.54 | $3.57 | — | $51.18 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_mover,oppset; 🔵; ret5=-20.9; leftover $2089.93 | join🟡 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.18 | ▲ close $10,471.52 vs 09:30 $10,465.03 (session +35.37) | 16:00 close · cash $51.18 · equity $10,471.52 vs 09:30 $10,465.03 (+6.49; session marks +35.37) · 5 name(s) marked open→close (per-name table). PURR×151 09:30 $13.82 → close $14.09 +40.77; ARE×36 09:30 $56.70 → close $53.30 -122.40; MNR×190 09:30 $10.95 → close $11.13 +34.20; USDE×219 09:30 $9.54 → close $10.19 +142.35; FLNC×277 09:30 $7.54 → close $7.32 -59.55 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `MU` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AMKR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-24 | `AMX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DK` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `UEC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRCT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `SOLS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HQY` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BEKE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FNKO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SGI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GPRE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TII` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `BMRN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `LENZ` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PODD` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RANI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `XRX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `CF` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FLR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FMC` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HASI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `XP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SID` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UGP` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BSBR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OBE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `WDS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HELP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ARQT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FNV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LFST` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SID` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `EQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NEOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SANM` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `QRVO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `WCC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `PBF` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `VLO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CVE` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `FRO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `BNC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TK` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `PURR` | 151 | 2026-09-18 @ $13.82 | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; ret5=-9.1; leftover $2089.93 |
| `ARE` | 36 | 2026-09-18 @ $56.70 | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+13.7; leftover $2089.93 |
| `MNR` | 190 | 2026-09-18 @ $10.95 | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list oppset; ret5=+1.7; leftover $2089.93 |
| `USDE` | 219 | 2026-09-18 @ $9.54 | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover,oppset; ret5=+15.8; leftover $2089.93 |
| `FLNC` | 277 | 2026-09-18 @ $7.54 | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_mover,oppset; 🔵; ret5=-20.9; leftover $2089.93 |
