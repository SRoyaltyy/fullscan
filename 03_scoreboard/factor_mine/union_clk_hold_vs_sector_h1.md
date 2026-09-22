# Factor mine action — `union_clk_hold_vs_sector_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · Clock-B #6 stock holds while sector camera is red

Cash book **+1.98%** ($10,198) · signal-only (no cash/fees) was -3.94%. Starts YES **25/27**. Fills 180 · skips 80 · realized $+162.00.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $567.95.

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
| 2026-08-20 | `BEKE` | 73 | — | $17.04 | +0.00 | $16.99 | -3.65 | -3.65 | +0.00 | -3.65 |
| 2026-08-20 | `BABA` | 10 | — | $123.47 | +0.00 | $130.53 | +70.60 | +70.60 | +0.00 | +70.60 |
| 2026-08-20 | `ROST` | 5 | — | $229.55 | +0.00 | $228.99 | -2.80 | -2.80 | +0.00 | -2.80 |
| 2026-08-20 | `BKE` | 29 | — | $42.60 | +0.00 | $42.64 | +1.16 | +1.16 | +0.00 | +1.16 |
| 2026-08-21 | `HTHT` | 25 | $49.54 | $49.58 | +1.00 | — | +0.00 | +1.00 | +29.75 | — |
| 2026-08-21 | `RERE` | 296 | $4.08 | $4.17 | +26.64 | — | +0.00 | +26.64 | -8.88 | — |
| 2026-08-21 | `ATAT` | 36 | $34.25 | $34.31 | +2.16 | — | +0.00 | +2.16 | +9.36 | — |
| 2026-08-21 | `SG` | 193 | $6.58 | $6.61 | +5.79 | — | +0.00 | +5.79 | +34.74 | — |
| 2026-08-21 | `BEKE` | 73 | $16.99 | $17.93 | +68.99 | — | +0.00 | +68.99 | +65.33 | — |
| 2026-08-21 | `BABA` | 10 | $130.53 | $125.35 | -51.80 | — | +0.00 | -51.80 | +18.80 | — |
| 2026-08-21 | `ROST` | 5 | $228.99 | $243.85 | +74.30 | — | +0.00 | +74.30 | +71.50 | — |
| 2026-08-21 | `BKE` | 29 | $42.64 | $43.08 | +12.76 | $43.81 | +21.17 | +33.93 | +13.92 | +35.09 |
| 2026-08-21 | `SM` | 33 | — | $37.81 | +0.00 | $37.20 | -20.13 | -20.13 | +0.00 | -20.13 |
| 2026-08-21 | `TALO` | 71 | — | $17.88 | +0.00 | $17.47 | -29.11 | -29.11 | +0.00 | -29.11 |
| 2026-08-21 | `DE` | 2 | — | $623.26 | +0.00 | $647.47 | +48.42 | +48.42 | +0.00 | +48.42 |
| 2026-08-21 | `TRON` | 655 | — | $1.94 | +0.00 | $2.01 | +45.85 | +45.85 | +0.00 | +45.85 |
| 2026-08-21 | `ORBS` | 1471 | — | $0.86 | +0.00 | $0.88 | +23.54 | +23.54 | +0.00 | +23.54 |
| 2026-08-21 | `PDD` | 14 | — | $90.03 | +0.00 | $88.38 | -23.10 | -23.10 | +0.00 | -23.10 |
| 2026-08-21 | `XPEV` | 103 | — | $12.29 | +0.00 | $12.19 | -10.30 | -10.30 | +0.00 | -10.30 |
| 2026-08-24 | `BKE` | 29 | $43.81 | $44.22 | +11.89 | — | +0.00 | +11.89 | +46.98 | — |
| 2026-08-24 | `SM` | 33 | $37.20 | $36.61 | -19.47 | — | +0.00 | -19.47 | -39.60 | — |
| 2026-08-24 | `TALO` | 71 | $17.47 | $17.24 | -16.33 | — | +0.00 | -16.33 | -45.44 | — |
| 2026-08-24 | `DE` | 2 | $647.47 | $653.04 | +11.14 | — | +0.00 | +11.14 | +59.56 | — |
| 2026-08-24 | `TRON` | 655 | $2.01 | $2.02 | +6.55 | — | +0.00 | +6.55 | +52.40 | — |
| 2026-08-24 | `ORBS` | 1471 | $0.88 | $0.89 | +14.71 | — | +0.00 | +14.71 | +38.25 | — |
| 2026-08-24 | `PDD` | 14 | $88.38 | $90.95 | +35.98 | — | +0.00 | +35.98 | +12.88 | — |
| 2026-08-24 | `XPEV` | 103 | $12.19 | $11.83 | -37.08 | — | +0.00 | -37.08 | -47.38 | — |
| 2026-08-25 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-26 | `NVDA` | 5 | — | $212.64 | +0.00 | $209.66 | -14.90 | -14.90 | +0.00 | -14.90 |
| 2026-08-26 | `P` | 12 | — | $103.16 | +0.00 | $108.90 | +68.88 | +68.88 | +0.00 | +68.88 |
| 2026-08-26 | `HPQ` | 43 | — | $29.42 | +0.00 | $30.52 | +47.30 | +47.30 | +0.00 | +47.30 |
| 2026-08-26 | `LI` | 104 | — | $12.14 | +0.00 | $12.14 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `MNRO` | 90 | — | $14.00 | +0.00 | $12.61 | -125.10 | -125.10 | +0.00 | -125.10 |
| 2026-08-26 | `NTNX` | 19 | — | $65.00 | +0.00 | $65.39 | +7.37 | +7.37 | +0.00 | +7.37 |
| 2026-08-26 | `SFL` | 102 | — | $12.35 | +0.00 | $12.03 | -32.64 | -32.64 | +0.00 | -32.64 |
| 2026-08-26 | `AXTI` | 19 | — | $65.34 | +0.00 | $65.18 | -3.04 | -3.04 | +0.00 | -3.04 |
| 2026-08-27 | `NVDA` | 5 | $209.66 | $222.86 | +66.00 | — | +0.00 | +66.00 | +51.10 | — |
| 2026-08-27 | `P` | 12 | $108.90 | $110.66 | +21.12 | — | +0.00 | +21.12 | +90.00 | — |
| 2026-08-27 | `HPQ` | 43 | $30.52 | $27.86 | -114.38 | — | +0.00 | -114.38 | -67.08 | — |
| 2026-08-27 | `LI` | 104 | $12.14 | $12.35 | +21.84 | — | +0.00 | +21.84 | +21.84 | — |
| 2026-08-27 | `MNRO` | 90 | $12.61 | $12.56 | -4.50 | — | +0.00 | -4.50 | -129.60 | — |
| 2026-08-27 | `NTNX` | 19 | $65.39 | $71.24 | +111.15 | — | +0.00 | +111.15 | +118.52 | — |
| 2026-08-27 | `SFL` | 102 | $12.03 | $12.03 | +0.00 | — | +0.00 | +0.00 | -32.64 | — |
| 2026-08-27 | `AXTI` | 19 | $65.18 | $70.30 | +97.28 | — | +0.00 | +97.28 | +94.24 | — |
| 2026-08-27 | `BE` | 5 | — | $227.10 | +0.00 | $217.83 | -46.35 | -46.35 | +0.00 | -46.35 |
| 2026-08-27 | `DASH` | 5 | — | $235.94 | +0.00 | $231.89 | -20.25 | -20.25 | +0.00 | -20.25 |
| 2026-08-27 | `AEO` | 74 | — | $17.27 | +0.00 | $16.69 | -42.92 | -42.92 | +0.00 | -42.92 |
| 2026-08-27 | `BBY` | 15 | — | $80.60 | +0.00 | $83.56 | +44.40 | +44.40 | +0.00 | +44.40 |
| 2026-08-27 | `DKS` | 9 | — | $128.73 | +0.00 | $131.77 | +27.36 | +27.36 | +0.00 | +27.36 |
| 2026-08-27 | `ULTA` | 2 | — | $536.07 | +0.00 | $540.10 | +8.06 | +8.06 | +0.00 | +8.06 |
| 2026-08-27 | `RRC` | 30 | — | $41.44 | +0.00 | $41.64 | +6.00 | +6.00 | +0.00 | +6.00 |
| 2026-08-27 | `GAP` | 61 | — | $20.75 | +0.00 | $20.79 | +2.44 | +2.44 | +0.00 | +2.44 |
| 2026-08-28 | `BE` | 5 | $217.83 | $215.71 | -10.62 | — | +0.00 | -10.62 | -56.97 | — |
| 2026-08-28 | `DASH` | 5 | $231.89 | $233.37 | +7.40 | — | +0.00 | +7.40 | -12.85 | — |
| 2026-08-28 | `AEO` | 74 | $16.69 | $17.06 | +27.38 | — | +0.00 | +27.38 | -15.54 | — |
| 2026-08-28 | `BBY` | 15 | $83.56 | $83.85 | +4.35 | — | +0.00 | +4.35 | +48.75 | — |
| 2026-08-28 | `DKS` | 9 | $131.77 | $132.80 | +9.27 | — | +0.00 | +9.27 | +36.63 | — |
| 2026-08-28 | `ULTA` | 2 | $540.10 | $542.00 | +3.80 | — | +0.00 | +3.80 | +11.86 | — |
| 2026-08-28 | `RRC` | 30 | $41.64 | $41.74 | +3.00 | $41.46 | -8.40 | -5.40 | +9.00 | +0.60 |
| 2026-08-28 | `GAP` | 61 | $20.79 | $24.69 | +237.90 | $23.48 | -73.81 | +164.09 | +240.34 | +166.53 |
| 2026-08-28 | `ANF` | 8 | — | $146.07 | +0.00 | $148.42 | +18.80 | +18.80 | +0.00 | +18.80 |
| 2026-08-28 | `TGB` | 132 | — | $9.75 | +0.00 | $9.18 | -75.24 | -75.24 | +0.00 | -75.24 |
| 2026-08-28 | `TH` | 67 | — | $19.00 | +0.00 | $18.55 | -30.15 | -30.15 | +0.00 | -30.15 |
| 2026-08-28 | `FIGR` | 34 | — | $37.49 | +0.00 | $36.05 | -48.96 | -48.96 | +0.00 | -48.96 |
| 2026-08-28 | `ABAT` | 483 | — | $2.66 | +0.00 | $2.57 | -43.47 | -43.47 | +0.00 | -43.47 |
| 2026-08-28 | `HAFN` | 154 | — | $8.35 | +0.00 | $8.47 | +18.48 | +18.48 | +0.00 | +18.48 |
| 2026-08-31 | `RRC` | 30 | $41.46 | $42.00 | +16.20 | — | +0.00 | +16.20 | +16.80 | — |
| 2026-08-31 | `GAP` | 61 | $23.48 | $22.98 | -30.50 | — | +0.00 | -30.50 | +136.03 | — |
| 2026-08-31 | `ANF` | 8 | $148.42 | $148.03 | -3.12 | — | +0.00 | -3.12 | +15.68 | — |
| 2026-08-31 | `TGB` | 132 | $9.18 | $9.15 | -3.96 | — | +0.00 | -3.96 | -79.20 | — |
| 2026-08-31 | `TH` | 67 | $18.55 | $18.12 | -28.48 | — | +0.00 | -28.48 | -58.62 | — |
| 2026-08-31 | `FIGR` | 34 | $36.05 | $35.77 | -9.52 | — | +0.00 | -9.52 | -58.48 | — |
| 2026-08-31 | `ABAT` | 483 | $2.57 | $2.56 | -4.83 | — | +0.00 | -4.83 | -48.30 | — |
| 2026-08-31 | `HAFN` | 154 | $8.47 | $8.53 | +9.24 | — | +0.00 | +9.24 | +27.72 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `PBF` | 16 | — | $74.75 | +0.00 | $75.33 | +9.28 | +9.28 | +0.00 | +9.28 |
| 2026-09-03 | `PBR` | 59 | — | $21.18 | +0.00 | $20.51 | -39.53 | -39.53 | +0.00 | -39.53 |
| 2026-09-03 | `XP` | 61 | — | $20.74 | +0.00 | $20.00 | -45.14 | -45.14 | +0.00 | -45.14 |
| 2026-09-03 | `HP` | 26 | — | $47.74 | +0.00 | $45.02 | -70.72 | -70.72 | +0.00 | -70.72 |
| 2026-09-03 | `PBR-A` | 66 | — | $19.16 | +0.00 | $18.58 | -38.28 | -38.28 | +0.00 | -38.28 |
| 2026-09-03 | `VIST` | 16 | — | $77.14 | +0.00 | $74.61 | -40.48 | -40.48 | +0.00 | -40.48 |
| 2026-09-03 | `LULU` | 10 | — | $121.15 | +0.00 | $121.77 | +6.20 | +6.20 | +0.00 | +6.20 |
| 2026-09-03 | `VSXY` | 16 | — | $76.86 | +0.00 | $73.64 | -51.52 | -51.52 | +0.00 | -51.52 |
| 2026-09-04 | `PBF` | 16 | $75.33 | $74.50 | -13.28 | — | +0.00 | -13.28 | -4.00 | — |
| 2026-09-04 | `PBR` | 59 | $20.51 | $20.25 | -15.34 | — | +0.00 | -15.34 | -54.87 | — |
| 2026-09-04 | `XP` | 61 | $20.00 | $19.67 | -20.13 | — | +0.00 | -20.13 | -65.27 | — |
| 2026-09-04 | `HP` | 26 | $45.02 | $44.59 | -11.18 | — | +0.00 | -11.18 | -81.90 | — |
| 2026-09-04 | `PBR-A` | 66 | $18.58 | $18.36 | -14.52 | — | +0.00 | -14.52 | -52.80 | — |
| 2026-09-04 | `VIST` | 16 | $74.61 | $73.97 | -10.24 | — | +0.00 | -10.24 | -50.72 | — |
| 2026-09-04 | `LULU` | 10 | $121.77 | $98.15 | -236.20 | — | +0.00 | -236.20 | -230.00 | — |
| 2026-09-04 | `VSXY` | 16 | $73.64 | $73.63 | -0.16 | — | +0.00 | -0.16 | -51.68 | — |
| 2026-09-04 | `BE` | 5 | — | $236.82 | +0.00 | $252.87 | +80.25 | +80.25 | +0.00 | +80.25 |
| 2026-09-04 | `HAFN` | 133 | — | $8.94 | +0.00 | $9.22 | +37.24 | +37.24 | +0.00 | +37.24 |
| 2026-09-04 | `MIR` | 71 | — | $16.60 | +0.00 | $16.93 | +23.43 | +23.43 | +0.00 | +23.43 |
| 2026-09-04 | `GORO` | 301 | — | $3.95 | +0.00 | $4.15 | +60.20 | +60.20 | +0.00 | +60.20 |
| 2026-09-04 | `GSM` | 254 | — | $4.67 | +0.00 | $4.67 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-04 | `WNC` | 83 | — | $14.17 | +0.00 | $14.31 | +11.62 | +11.62 | +0.00 | +11.62 |
| 2026-09-04 | `XRX` | 359 | — | $3.31 | +0.00 | $3.32 | +3.59 | +3.59 | +0.00 | +3.59 |
| 2026-09-04 | `ABM` | 25 | — | $46.79 | +0.00 | $47.05 | +6.50 | +6.50 | +0.00 | +6.50 |
| 2026-09-08 | `BE` | 5 | $252.87 | $267.76 | +74.45 | — | +0.00 | +74.45 | +154.70 | — |
| 2026-09-08 | `HAFN` | 133 | $9.22 | $8.81 | -54.53 | — | +0.00 | -54.53 | -17.29 | — |
| 2026-09-08 | `MIR` | 71 | $16.93 | $17.07 | +9.94 | — | +0.00 | +9.94 | +33.37 | — |
| 2026-09-08 | `GORO` | 301 | $4.15 | $4.13 | -6.02 | — | +0.00 | -6.02 | +54.18 | — |
| 2026-09-08 | `GSM` | 254 | $4.67 | $4.75 | +20.32 | — | +0.00 | +20.32 | +20.32 | — |
| 2026-09-08 | `WNC` | 83 | $14.31 | $14.22 | -7.47 | — | +0.00 | -7.47 | +4.15 | — |
| 2026-09-08 | `XRX` | 359 | $3.32 | $3.50 | +64.62 | — | +0.00 | +64.62 | +68.21 | — |
| 2026-09-08 | `ABM` | 25 | $47.05 | $45.81 | -31.00 | — | +0.00 | -31.00 | -24.50 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ASO` | 22 | — | $54.91 | +0.00 | $55.36 | +9.90 | +9.90 | +0.00 | +9.90 |
| 2026-09-11 | `BNC` | 248 | — | $4.91 | +0.00 | $4.80 | -27.28 | -27.28 | +0.00 | -27.28 |
| 2026-09-11 | `CECO` | 15 | — | $77.51 | +0.00 | $78.34 | +12.45 | +12.45 | +0.00 | +12.45 |
| 2026-09-11 | `PBR` | 57 | — | $21.21 | +0.00 | $21.20 | -0.57 | -0.57 | +0.00 | -0.57 |
| 2026-09-11 | `VIST` | 15 | — | $77.33 | +0.00 | $76.27 | -15.90 | -15.90 | +0.00 | -15.90 |
| 2026-09-11 | `ARLO` | 92 | — | $13.22 | +0.00 | $13.19 | -2.76 | -2.76 | +0.00 | -2.76 |
| 2026-09-11 | `BAK` | 575 | — | $2.12 | +0.00 | $2.08 | -23.00 | -23.00 | +0.00 | -23.00 |
| 2026-09-11 | `SSL` | 85 | — | $14.35 | +0.00 | $14.59 | +20.40 | +20.40 | +0.00 | +20.40 |
| 2026-09-14 | `ASO` | 22 | $55.36 | $54.75 | -13.42 | — | +0.00 | -13.42 | -3.52 | — |
| 2026-09-14 | `BNC` | 248 | $4.80 | $5.03 | +57.04 | — | +0.00 | +57.04 | +29.76 | — |
| 2026-09-14 | `CECO` | 15 | $78.34 | $74.34 | -60.00 | — | +0.00 | -60.00 | -47.55 | — |
| 2026-09-14 | `PBR` | 57 | $21.20 | $21.23 | +1.71 | — | +0.00 | +1.71 | +1.14 | — |
| 2026-09-14 | `VIST` | 15 | $76.27 | $77.10 | +12.45 | — | +0.00 | +12.45 | -3.45 | — |
| 2026-09-14 | `ARLO` | 92 | $13.19 | $13.07 | -11.04 | — | +0.00 | -11.04 | -13.80 | — |
| 2026-09-14 | `BAK` | 575 | $2.08 | $2.05 | -17.25 | — | +0.00 | -17.25 | -40.25 | — |
| 2026-09-14 | `SSL` | 85 | $14.59 | $14.69 | +8.50 | — | +0.00 | +8.50 | +28.90 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `SM` | 30 | — | $39.99 | +0.00 | $38.16 | -54.90 | -54.90 | +0.00 | -54.90 |
| 2026-09-16 | `TALO` | 67 | — | $17.87 | +0.00 | $17.42 | -30.15 | -30.15 | +0.00 | -30.15 |
| 2026-09-16 | `APA` | 26 | — | $46.44 | +0.00 | $44.79 | -42.90 | -42.90 | +0.00 | -42.90 |
| 2026-09-16 | `CVI` | 23 | — | $51.05 | +0.00 | $53.05 | +46.00 | +46.00 | +0.00 | +46.00 |
| 2026-09-16 | `RIG` | 205 | — | $5.87 | +0.00 | $5.54 | -67.65 | -67.65 | +0.00 | -67.65 |
| 2026-09-16 | `VAL` | 13 | — | $87.40 | +0.00 | $82.52 | -63.44 | -63.44 | +0.00 | -63.44 |
| 2026-09-16 | `FRO` | 23 | — | $52.52 | +0.00 | $53.67 | +26.45 | +26.45 | +0.00 | +26.45 |
| 2026-09-16 | `GFR` | 177 | — | $6.83 | +0.00 | $6.49 | -60.18 | -60.18 | +0.00 | -60.18 |
| 2026-09-17 | `SM` | 30 | $38.16 | $37.57 | -17.70 | — | +0.00 | -17.70 | -72.60 | — |
| 2026-09-17 | `TALO` | 67 | $17.42 | $17.19 | -15.41 | — | +0.00 | -15.41 | -45.56 | — |
| 2026-09-17 | `APA` | 26 | $44.79 | $44.63 | -4.16 | — | +0.00 | -4.16 | -47.06 | — |
| 2026-09-17 | `CVI` | 23 | $53.05 | $51.88 | -26.91 | — | +0.00 | -26.91 | +19.09 | — |
| 2026-09-17 | `RIG` | 205 | $5.54 | $5.58 | +8.20 | — | +0.00 | +8.20 | -59.45 | — |
| 2026-09-17 | `VAL` | 13 | $82.52 | $83.20 | +8.84 | — | +0.00 | +8.84 | -54.60 | — |
| 2026-09-17 | `FRO` | 23 | $53.67 | $54.31 | +14.72 | — | +0.00 | +14.72 | +41.17 | — |
| 2026-09-17 | `GFR` | 177 | $6.49 | $6.48 | -1.77 | — | +0.00 | -1.77 | -61.95 | — |
| 2026-09-17 | `SFL` | 86 | — | $13.55 | +0.00 | $13.75 | +17.20 | +17.20 | +0.00 | +17.20 |
| 2026-09-17 | `FTAI` | 5 | — | $196.50 | +0.00 | $195.07 | -7.15 | -7.15 | +0.00 | -7.15 |
| 2026-09-17 | `FPS` | 31 | — | $36.76 | +0.00 | $38.06 | +40.30 | +40.30 | +0.00 | +40.30 |
| 2026-09-17 | `VSTS` | 84 | — | $13.90 | +0.00 | $13.84 | -5.04 | -5.04 | +0.00 | -5.04 |
| 2026-09-17 | `ARLO` | 85 | — | $13.62 | +0.00 | $13.35 | -22.95 | -22.95 | +0.00 | -22.95 |
| 2026-09-17 | `CECO` | 16 | — | $72.95 | +0.00 | $70.83 | -33.92 | -33.92 | +0.00 | -33.92 |
| 2026-09-17 | `EROC` | 92 | — | $12.64 | +0.00 | $12.90 | +23.92 | +23.92 | +0.00 | +23.92 |
| 2026-09-17 | `AESI` | 85 | — | $13.65 | +0.00 | $13.42 | -19.55 | -19.55 | +0.00 | -19.55 |
| 2026-09-18 | `SFL` | 86 | $13.75 | $13.74 | -0.86 | — | +0.00 | -0.86 | +16.34 | — |
| 2026-09-18 | `FTAI` | 5 | $195.07 | $195.55 | +2.40 | — | +0.00 | +2.40 | -4.75 | — |
| 2026-09-18 | `FPS` | 31 | $38.06 | $39.50 | +44.64 | — | +0.00 | +44.64 | +84.94 | — |
| 2026-09-18 | `VSTS` | 84 | $13.84 | $13.71 | -10.92 | — | +0.00 | -10.92 | -15.96 | — |
| 2026-09-18 | `ARLO` | 85 | $13.35 | $13.42 | +5.95 | — | +0.00 | +5.95 | -17.00 | — |
| 2026-09-18 | `CECO` | 16 | $70.83 | $71.00 | +2.72 | — | +0.00 | +2.72 | -31.20 | — |
| 2026-09-18 | `EROC` | 92 | $12.90 | $13.00 | +9.20 | — | +0.00 | +9.20 | +33.12 | — |
| 2026-09-18 | `AESI` | 85 | $13.42 | $13.52 | +8.50 | — | +0.00 | +8.50 | -11.05 | — |
| 2026-09-18 | `PURR` | 169 | — | $13.82 | +0.00 | $14.09 | +45.63 | +45.63 | +0.00 | +45.63 |
| 2026-09-18 | `ARE` | 41 | — | $56.70 | +0.00 | $53.30 | -139.40 | -139.40 | +0.00 | -139.40 |
| 2026-09-18 | `USDE` | 245 | — | $9.54 | +0.00 | $10.19 | +159.25 | +159.25 | +0.00 | +159.25 |
| 2026-09-18 | `FLNC` | 311 | — | $7.54 | +0.00 | $7.32 | -66.86 | -66.86 | +0.00 | -66.86 |
| 2026-09-21 | `PURR` | 169 | $14.09 | $14.65 | +94.64 | — | +0.00 | +94.64 | +140.27 | — |
| 2026-09-21 | `ARE` | 41 | $53.30 | $53.39 | +3.69 | — | +0.00 | +3.69 | -135.71 | — |
| 2026-09-21 | `USDE` | 245 | $10.19 | $13.05 | +700.70 | — | +0.00 | +700.70 | +859.95 | — |
| 2026-09-21 | `FLNC` | 311 | $7.32 | $7.36 | +12.44 | — | +0.00 | +12.44 | -54.42 | — |
| 2026-09-21 | `MSTR` | 7 | — | $164.58 | +0.00 | $168.50 | +27.44 | +27.44 | +0.00 | +27.44 |
| 2026-09-21 | `VICR` | 5 | — | $230.25 | +0.00 | $223.90 | -31.75 | -31.75 | +0.00 | -31.75 |
| 2026-09-21 | `KEEL` | 304 | — | $4.17 | +0.00 | $4.07 | -31.92 | -31.92 | +0.00 | -31.92 |
| 2026-09-21 | `SECZ` | 108 | — | $11.67 | +0.00 | $13.50 | +197.64 | +197.64 | +0.00 | +197.64 |
| 2026-09-21 | `BKKT` | 136 | — | $9.31 | +0.00 | $9.14 | -23.12 | -23.12 | +0.00 | -23.12 |
| 2026-09-21 | `BTDR` | 94 | — | $13.47 | +0.00 | $13.14 | -31.49 | -31.49 | +0.00 | -31.49 |
| 2026-09-21 | `COHR` | 3 | — | $326.48 | +0.00 | $321.52 | -14.88 | -14.88 | +0.00 | -14.88 |
| 2026-09-21 | `FORM` | 10 | — | $123.00 | +0.00 | $119.31 | -36.90 | -36.90 | +0.00 | -36.90 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +31.52 | HTFL, JBIO, VERA, ZNTL, CELC | — | $90.67 | $10,015.68 | HTFL×48, JBIO×81, VERA×63, ZNTL×561, CELC×21 |
| 2026-08-18 | -6.20 | $90.67 | HTFL×48, JBIO×81, VERA×63, ZNTL×561, CELC×21 | $9,967.60 | -48.08 | +0.00 | — | HTFL, JBIO, VERA, ZNTL, CELC | $9,951.55 | $9,951.55 | — |
| 2026-08-19 | -7.20 | $9,951.55 | — | $9,951.55 | +0.00 | +0.00 | — | — | $9,951.55 | $9,951.55 | — |
| 2026-08-20 | +1.12 | $9,951.55 | — | $9,951.55 | +0.00 | +94.69 | HTHT, RERE, ATAT, SG, BEKE, BABA, ROST, BKE | — | $151.18 | $10,027.38 | HTHT×25, RERE×296, ATAT×36, SG×193, BEKE×73, BABA×10, ROST×5, BKE×29 |
| 2026-08-21 | +3.25 | $151.18 | HTHT×25, RERE×296, ATAT×36, SG×193, BEKE×73, BABA×10, ROST×5, BKE×29 | $10,167.22 | +139.84 | +56.34 | SM, TALO, DE, TRON, ORBS, PDD, XPEV | HTHT, RERE, ATAT, SG, BEKE, BABA, ROST | $33.05 | $10,170.37 | BKE×29, SM×33, TALO×71, DE×2, TRON×655, ORBS×1471, PDD×14, XPEV×103 |
| 2026-08-24 | -5.17 | $33.05 | BKE×29, SM×33, TALO×71, DE×2, TRON×655, ORBS×1471, PDD×14, XPEV×103 | $10,177.76 | +7.39 | +0.00 | — | BKE, SM, TALO, DE, TRON, ORBS, PDD, XPEV | $10,138.61 | $10,138.61 | — |
| 2026-08-25 | +1.80 | $10,138.61 | — | $10,138.61 | -0.00 | +0.00 | — | — | $10,138.61 | $10,138.61 | — |
| 2026-08-26 | +2.02 | $10,138.61 | — | $10,138.61 | -0.00 | -52.13 | NVDA, P, HPQ, LI, MNRO, NTNX, SFL, AXTI | — | $296.57 | $10,069.38 | NVDA×5, P×12, HPQ×43, LI×104, MNRO×90, NTNX×19, SFL×102, AXTI×19 |
| 2026-08-27 | — | $296.57 | NVDA×5, P×12, HPQ×43, LI×104, MNRO×90, NTNX×19, SFL×102, AXTI×19 | $10,267.89 | +198.51 | -21.26 | BE, DASH, AEO, BBY, DKS, ULTA, RRC, GAP | NVDA, P, HPQ, LI, MNRO, NTNX, SFL, AXTI | $692.24 | $10,212.82 | BE×5, DASH×5, AEO×74, BBY×15, DKS×9, ULTA×2, RRC×30, GAP×61 |
| 2026-08-28 | +0.75 | $692.24 | BE×5, DASH×5, AEO×74, BBY×15, DKS×9, ULTA×2, RRC×30, GAP×61 | $10,495.30 | +282.48 | -242.75 | ANF, TGB, TH, FIGR, ABAT, HAFN | BE, DASH, AEO, BBY, DKS, ULTA | $133.35 | $10,222.79 | RRC×30, GAP×61, ANF×8, TGB×132, TH×67, FIGR×34, ABAT×483, HAFN×154 |
| 2026-08-31 | -5.85 | $133.35 | RRC×30, GAP×61, ANF×8, TGB×132, TH×67, FIGR×34, ABAT×483, HAFN×154 | $10,167.82 | -54.97 | +0.00 | — | RRC, GAP, ANF, TGB, TH, FIGR, ABAT, HAFN | $10,145.94 | $10,145.94 | — |
| 2026-09-01 | -6.30 | $10,145.94 | — | $10,145.94 | +0.00 | +0.00 | — | — | $10,145.94 | $10,145.94 | — |
| 2026-09-02 | -3.83 | $10,145.94 | — | $10,145.94 | +0.00 | +0.00 | — | — | $10,145.94 | $10,145.94 | — |
| 2026-09-03 | -0.90 | $10,145.94 | — | $10,145.94 | +0.00 | -270.19 | PBF, PBR, XP, HP, PBR-A, VIST, LULU, VSXY | — | $237.15 | $9,859.02 | PBF×16, PBR×59, XP×61, HP×26, PBR-A×66, VIST×16, LULU×10, VSXY×16 |
| 2026-09-04 | +2.25 | $237.15 | PBF×16, PBR×59, XP×61, HP×26, PBR-A×66, VIST×16, LULU×10, VSXY×16 | $9,537.97 | -321.05 | +222.83 | BE, HAFN, MIR, GORO, GSM, WNC, XRX, ABM | PBF, PBR, XP, HP, PBR-A, VIST, LULU, VSXY | $37.39 | $9,721.22 | BE×5, HAFN×133, MIR×71, GORO×301, GSM×254, WNC×83, XRX×359, ABM×25 |
| 2026-09-08 | -11.47 | $37.39 | BE×5, HAFN×133, MIR×71, GORO×301, GSM×254, WNC×83, XRX×359, ABM×25 | $9,791.53 | +70.31 | +0.00 | — | BE, HAFN, MIR, GORO, GSM, WNC, XRX, ABM | $9,768.54 | $9,768.54 | — |
| 2026-09-09 | -13.95 | $9,768.54 | — | $9,768.54 | -0.00 | +0.00 | — | — | $9,768.54 | $9,768.54 | — |
| 2026-09-10 | -13.28 | $9,768.54 | — | $9,768.54 | -0.00 | +0.00 | — | — | $9,768.54 | $9,768.54 | — |
| 2026-09-11 | +0.50 | $9,768.54 | — | $9,768.54 | -0.00 | -26.76 | ASO, BNC, CECO, PBR, VIST, ARLO, BAK, SSL | — | $132.87 | $9,718.37 | ASO×22, BNC×248, CECO×15, PBR×57, VIST×15, ARLO×92, BAK×575, SSL×85 |
| 2026-09-14 | -11.00 | $132.87 | ASO×22, BNC×248, CECO×15, PBR×57, VIST×15, ARLO×92, BAK×575, SSL×85 | $9,696.36 | -22.01 | +0.00 | — | ASO, BNC, CECO, PBR, VIST, ARLO, BAK, SSL | $9,672.65 | $9,672.65 | — |
| 2026-09-15 | -3.84 | $9,672.65 | — | $9,672.65 | +0.00 | +0.00 | — | — | $9,672.65 | $9,672.65 | — |
| 2026-09-16 | +5.30 | $9,672.65 | — | $9,672.65 | +0.00 | -246.77 | SM, TALO, APA, CVI, RIG, VAL, FRO, GFR | — | $120.00 | $9,408.23 | SM×30, TALO×67, APA×26, CVI×23, RIG×205, VAL×13, FRO×23, GFR×177 |
| 2026-09-17 | +7.38 | $120.00 | SM×30, TALO×67, APA×26, CVI×23, RIG×205, VAL×13, FRO×23, GFR×177 | $9,374.04 | -34.19 | -7.19 | SFL, FTAI, FPS, VSTS, ARLO, CECO, EROC, AESI | SM, TALO, APA, CVI, RIG, VAL, FRO, GFR | $235.82 | $9,331.62 | SFL×86, FTAI×5, FPS×31, VSTS×84, ARLO×85, CECO×16, EROC×92, AESI×85 |
| 2026-09-18 | +4.86 | $235.82 | SFL×86, FTAI×5, FPS×31, VSTS×84, ARLO×85, CECO×16, EROC×92, AESI×85 | $9,393.25 | +61.63 | -1.38 | PURR, ARE, USDE, FLNC | SFL, FTAI, FPS, VSTS, ARLO, CECO, EROC, AESI | $22.95 | $9,362.53 | PURR×169, ARE×41, USDE×245, FLNC×311 |
| 2026-09-21 | +12.87 | $22.95 | PURR×169, ARE×41, USDE×245, FLNC×311 | $10,174.00 | +811.47 | +55.02 | MSTR, VICR, KEEL, SECZ, BKKT, BTDR, COHR, FORM | PURR, ARE, USDE, FLNC | $567.95 | $10,198.09 | MSTR×7, VICR×5, KEEL×304, SECZ×108, BKKT×136, BTDR×94, COHR×3, FORM×10 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 48 | $41.23 | $2.13 | — | $8,018.83 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover; ret5=+46.0; leftover $2000.00 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `JBIO` | 81 | $24.60 | $2.23 | — | $6,023.99 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+12.5; leftover $2000.00 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 63 | $31.30 | $2.18 | — | $4,049.91 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; ret5=-3.8; leftover $2000.00 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ZNTL` | 561 | $3.56 | $7.24 | — | $2,045.52 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_mover; ret5=-15.6; leftover $2000.00 | join🟡 sector🔴 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 21 | $92.99 | $2.05 | — | $90.67 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; ret5=-0.8; leftover $2000.00 | join🟡 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.67 | ▲ close $10,015.68 vs 09:30 $10,000.00 (session +31.52) | 16:00 close · cash $90.67 · equity $10,015.68 vs 09:30 $10,000.00 (+15.68; session marks +31.52) · 5 name(s) marked open→close (per-name table). HTFL×48 09:30 $41.23 → close $41.94 +34.08; JBIO×81 09:30 $24.60 → close $23.45 -93.15; VERA×63 09:30 $31.30 → close $31.63 +20.79; ZNTL×561 09:30 $3.56 → close $3.71 +81.35; CELC×21 09:30 $92.99 → close $92.44 -11.55 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.67 | ▼ 09:30 equity $9,967.60 vs yday $10,015.68 (-48.08) | 09:30 open · cash $90.67 (unchanged overnight, no fees) · equity $9,967.60 vs prior close $10,015.68 (-48.08) · 5 name(s) re-marked at the open (per-name table). HTFL×48 yday $41.94 → 09:30 $41.50 -21.12; JBIO×81 yday $23.45 → 09:30 $23.07 -30.78; VERA×63 yday $31.63 → 09:30 $31.31 -20.16; ZNTL×561 yday $3.71 → 09:30 $3.75 +25.24; CELC×21 yday $92.44 → 09:30 $92.38 -1.26 | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 48 | $41.50 | $2.16 | $+8.67 | $2,080.51 | ▲ +8.67 after sell → book $9,965.44; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
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
| 2026-08-20 09:30 ET | **BUY** | `BEKE` | 73 | $17.04 | $2.21 | — | $3,775.13 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight; 🔵; ret5=-0.2; leftover $1243.94 | join🟢 sector🔴 gen🟢 news🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 10 | $123.47 | $2.02 | — | $2,538.41 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; 🔵; ret5=+2.9; leftover $1243.94 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ROST` | 5 | $229.55 | $2.00 | — | $1,388.66 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight,overnight_mega; 🔵; ret5=-5.5; leftover $1243.94 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BKE` | 29 | $42.60 | $2.08 | — | $151.18 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight; 🔵; ret5=-4.6; leftover $1243.94 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.18 | ▲ close $10,027.38 vs 09:30 $9,951.55 (session +94.69) | 16:00 close · cash $151.18 · equity $10,027.38 vs 09:30 $9,951.55 (+75.83; session marks +94.69) · 8 name(s) marked open→close (per-name table). HTHT×25 09:30 $48.39 → close $49.54 +28.75; RERE×296 09:30 $4.20 → close $4.08 -35.52; ATAT×36 09:30 $34.05 → close $34.25 +7.20; SG×193 09:30 $6.43 → close $6.58 +28.95; BEKE×73 09:30 $17.04 → close $16.99 -3.65; BABA×10 09:30 $123.47 → close $130.53 +70.60; ROST×5 09:30 $229.55 → close $228.99 -2.80; BKE×29 09:30 $42.60 → close $42.64 +1.16 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.18 | ▲ 09:30 equity $10,167.22 vs yday $10,027.38 (+139.84) | 09:30 open · cash $151.18 (unchanged overnight, no fees) · equity $10,167.22 vs prior close $10,027.38 (+139.84) · 8 name(s) re-marked at the open (per-name table). HTHT×25 yday $49.54 → 09:30 $49.58 +1.00; RERE×296 yday $4.08 → 09:30 $4.17 +26.64; ATAT×36 yday $34.25 → 09:30 $34.31 +2.16; SG×193 yday $6.58 → 09:30 $6.61 +5.79; BEKE×73 yday $16.99 → 09:30 $17.93 +68.99; BABA×10 yday $130.53 → 09:30 $125.35 -51.80; ROST×5 yday $228.99 → 09:30 $243.85 +74.30; BKE×29 yday $42.64 → 09:30 $43.08 +12.76 | — |
| 2026-08-21 09:30 ET | **SELL** | `HTHT` | 25 | $49.58 | $2.08 | $+25.60 | $1,388.60 | ▲ +25.60 after sell → book $10,165.13; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `RERE` | 296 | $4.17 | $3.88 | $-16.58 | $2,619.04 | ▼ -16.58 after sell → book $10,161.25; vs 09:30 mark -3.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 36 | $34.31 | $2.12 | $+5.14 | $3,852.08 | ▲ +5.14 after sell → book $10,159.14; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SG` | 193 | $6.61 | $2.61 | $+29.56 | $5,125.20 | ▲ +29.56 after sell → book $10,156.52; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BEKE` | 73 | $17.93 | $2.23 | $+60.89 | $6,432.22 | ▲ +60.89 after sell → book $10,154.29; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BABA` | 10 | $125.35 | $2.04 | $+14.74 | $7,683.68 | ▲ +14.74 after sell → book $10,152.25; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ROST` | 5 | $243.85 | $2.02 | $+67.47 | $8,900.91 | ▲ +67.47 after sell → book $10,150.23; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `SM` | 33 | $37.81 | $2.09 | — | $7,651.09 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+16.1; leftover $1271.56 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TALO` | 71 | $17.88 | $2.20 | — | $6,379.41 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+14.9; leftover $1271.56 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $5,130.89 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1271.56 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TRON` | 655 | $1.94 | $8.45 | — | $3,851.74 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,ohlc_hot; ret5=+15.4; leftover $1271.56 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1471 | $0.86 | $17.12 | — | $2,563.67 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1271.56 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PDD` | 14 | $90.03 | $2.03 | — | $1,301.22 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight,overnight_mega; 🔵; ret5=+6.4; leftover $1271.56 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XPEV` | 103 | $12.29 | $2.30 | — | $33.05 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight; ret5=+1.9; leftover $1271.56 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.05 | ▲ close $10,170.37 vs 09:30 $10,167.22 (session +56.34) | 16:00 close · cash $33.05 · equity $10,170.37 vs 09:30 $10,167.22 (+3.15; session marks +56.34) · 8 name(s) marked open→close (per-name table). BKE×29 09:30 $43.08 → close $43.81 +21.17; SM×33 09:30 $37.81 → close $37.20 -20.13; TALO×71 09:30 $17.88 → close $17.47 -29.11; DE×2 09:30 $623.26 → close $647.47 +48.42; TRON×655 09:30 $1.94 → close $2.01 +45.85; ORBS×1471 09:30 $0.86 → close $0.88 +23.54; PDD×14 09:30 $90.03 → close $88.38 -23.10; XPEV×103 09:30 $12.29 → close $12.19 -10.30 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.05 | ▲ 09:30 equity $10,177.76 vs yday $10,170.37 (+7.39) | 09:30 open · cash $33.05 (unchanged overnight, no fees) · equity $10,177.76 vs prior close $10,170.37 (+7.39) · 8 name(s) re-marked at the open (per-name table). BKE×29 yday $43.81 → 09:30 $44.22 +11.89; SM×33 yday $37.20 → 09:30 $36.61 -19.47; TALO×71 yday $17.47 → 09:30 $17.24 -16.33; DE×2 yday $647.47 → 09:30 $653.04 +11.14; TRON×655 yday $2.01 → 09:30 $2.02 +6.55; ORBS×1471 yday $0.88 → 09:30 $0.89 +14.71; PDD×14 yday $88.38 → 09:30 $90.95 +35.98; XPEV×103 yday $12.19 → 09:30 $11.83 -37.08 | — |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 29 | $44.22 | $2.10 | $+42.81 | $1,313.34 | ▲ +42.81 after sell → book $10,175.67; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `SM` | 33 | $36.61 | $2.11 | $-43.80 | $2,519.36 | ▼ -43.80 after sell → book $10,173.56; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TALO` | 71 | $17.24 | $2.22 | $-49.87 | $3,741.17 | ▼ -49.87 after sell → book $10,171.33; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $5,045.24 | ▲ +55.55 after sell → book $10,169.32; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TRON` | 655 | $2.02 | $8.57 | $+35.38 | $6,359.77 | ▲ +35.38 after sell → book $10,160.75; vs 09:30 mark -8.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1471 | $0.89 | $17.76 | $+3.36 | $7,651.20 | ▲ +3.36 after sell → book $10,142.99; vs 09:30 mark -17.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PDD` | 14 | $90.95 | $2.05 | $+8.80 | $8,922.44 | ▲ +8.80 after sell → book $10,140.93; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `XPEV` | 103 | $11.83 | $2.33 | $-52.01 | $10,138.61 | ▼ -52.01 after sell → book $10,138.61; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,138.61 | ▲ close $10,138.61 vs 09:30 $10,177.76 (session +0.00) | 16:00 close · cash $10,138.61 · no lots left · equity $10,138.61. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,138.61 | ▲ 09:30 equity $10,138.61 vs yday $10,138.61 (-0.00) | 09:30 open · cash $10,138.61 · no holdings · equity $10,138.61 vs prior close $10,138.61 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,138.61 | ▲ close $10,138.61 vs 09:30 $10,138.61 (session +0.00) | 16:00 close · cash $10,138.61 · no lots left · equity $10,138.61. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,138.61 | ▲ 09:30 equity $10,138.61 vs yday $10,138.61 (-0.00) | 09:30 open · cash $10,138.61 · no holdings · equity $10,138.61 vs prior close $10,138.61 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 09:30 ET | **BUY** | `NVDA` | 5 | $212.64 | $2.00 | — | $9,073.40 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight,overnight_mega; 🔵; ret5=-3.0; leftover $1267.33 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 catal🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `P` | 12 | $103.16 | $2.03 | — | $7,833.46 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight; 🔵; ret5=-12.2; leftover $1267.33 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `HPQ` | 43 | $29.42 | $2.12 | — | $6,566.28 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight; ret5=-1.5; leftover $1267.33 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `LI` | 104 | $12.14 | $2.30 | — | $5,301.42 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; ret5=+1.2; leftover $1267.33 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 90 | $14.00 | $2.26 | — | $4,039.16 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+17.8; leftover $1267.33 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NTNX` | 19 | $65.00 | $2.05 | — | $2,802.07 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight; ret5=+0.9; leftover $1267.33 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SFL` | 102 | $12.35 | $2.30 | — | $1,540.08 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; ret5=-1.7; leftover $1267.33 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AXTI` | 19 | $65.34 | $2.05 | — | $296.57 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1267.33 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $296.57 | ▼ close $10,069.38 vs 09:30 $10,138.61 (session -52.13) | 16:00 close · cash $296.57 · equity $10,069.38 vs 09:30 $10,138.61 (-69.23; session marks -52.13) · 8 name(s) marked open→close (per-name table). NVDA×5 09:30 $212.64 → close $209.66 -14.90; P×12 09:30 $103.16 → close $108.90 +68.88; HPQ×43 09:30 $29.42 → close $30.52 +47.30; LI×104 09:30 $12.14 → close $12.14 +0.00; MNRO×90 09:30 $14.00 → close $12.61 -125.10; NTNX×19 09:30 $65.00 → close $65.39 +7.37; SFL×102 09:30 $12.35 → close $12.03 -32.64; AXTI×19 09:30 $65.34 → close $65.18 -3.04 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $296.57 | ▲ 09:30 equity $10,267.89 vs yday $10,069.38 (+198.51) | 09:30 open · cash $296.57 (unchanged overnight, no fees) · equity $10,267.89 vs prior close $10,069.38 (+198.51) · 8 name(s) re-marked at the open (per-name table). NVDA×5 yday $209.66 → 09:30 $222.86 +66.00; P×12 yday $108.90 → 09:30 $110.66 +21.12; HPQ×43 yday $30.52 → 09:30 $27.86 -114.38; LI×104 yday $12.14 → 09:30 $12.35 +21.84; MNRO×90 yday $12.61 → 09:30 $12.56 -4.50; NTNX×19 yday $65.39 → 09:30 $71.24 +111.15; SFL×102 yday $12.03 → 09:30 $12.03 +0.00; AXTI×19 yday $65.18 → 09:30 $70.30 +97.28 | — |
| 2026-08-27 09:30 ET | **SELL** | `NVDA` | 5 | $222.86 | $2.02 | $+47.07 | $1,408.84 | ▲ +47.07 after sell → book $10,265.86; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `P` | 12 | $110.66 | $2.05 | $+85.93 | $2,734.72 | ▲ +85.93 after sell → book $10,263.82; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HPQ` | 43 | $27.86 | $2.14 | $-71.34 | $3,930.56 | ▼ -71.34 after sell → book $10,261.68; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `LI` | 104 | $12.35 | $2.33 | $+17.21 | $5,212.63 | ▲ +17.21 after sell → book $10,259.35; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MNRO` | 90 | $12.56 | $2.28 | $-134.14 | $6,340.74 | ▼ -134.14 after sell → book $10,257.06; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NTNX` | 19 | $71.24 | $2.07 | $+114.41 | $7,692.24 | ▲ +114.41 after sell → book $10,255.00; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SFL` | 102 | $12.03 | $2.32 | $-37.26 | $8,916.97 | ▼ -37.26 after sell → book $10,252.67; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AXTI` | 19 | $70.30 | $2.07 | $+90.13 | $10,250.60 | ▲ +90.13 after sell → book $10,250.60; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `BE` | 5 | $227.10 | $2.00 | — | $9,113.10 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.6; leftover $1281.33 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DASH` | 5 | $235.94 | $2.00 | — | $7,931.39 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+7.6; leftover $1281.33 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `AEO` | 74 | $17.27 | $2.21 | — | $6,651.20 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+5.5; leftover $1281.33 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 15 | $80.60 | $2.04 | — | $5,440.17 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; ret5=-2.0; leftover $1281.33 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DKS` | 9 | $128.73 | $2.02 | — | $4,279.58 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_mover; ret5=-32.2; leftover $1281.33 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ULTA` | 2 | $536.07 | $2.00 | — | $3,205.44 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight; ret5=+2.9; leftover $1281.33 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 30 | $41.44 | $2.08 | — | $1,960.16 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list flatten; ret5=+3.1; leftover $1281.33 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GAP` | 61 | $20.75 | $2.17 | — | $692.24 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot,overnight; ret5=+5.2; leftover $1281.33 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $692.24 | ▼ close $10,212.82 vs 09:30 $10,267.89 (session -21.26) | 16:00 close · cash $692.24 · equity $10,212.82 vs 09:30 $10,267.89 (-55.07; session marks -21.26) · 8 name(s) marked open→close (per-name table). BE×5 09:30 $227.10 → close $217.83 -46.35; DASH×5 09:30 $235.94 → close $231.89 -20.25; AEO×74 09:30 $17.27 → close $16.69 -42.92; BBY×15 09:30 $80.60 → close $83.56 +44.40; DKS×9 09:30 $128.73 → close $131.77 +27.36; ULTA×2 09:30 $536.07 → close $540.10 +8.06; RRC×30 09:30 $41.44 → close $41.64 +6.00; GAP×61 09:30 $20.75 → close $20.79 +2.44 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $692.24 | ▲ 09:30 equity $10,495.30 vs yday $10,212.82 (+282.48) | 09:30 open · cash $692.24 (unchanged overnight, no fees) · equity $10,495.30 vs prior close $10,212.82 (+282.48) · 8 name(s) re-marked at the open (per-name table). BE×5 yday $217.83 → 09:30 $215.71 -10.62; DASH×5 yday $231.89 → 09:30 $233.37 +7.40; AEO×74 yday $16.69 → 09:30 $17.06 +27.38; BBY×15 yday $83.56 → 09:30 $83.85 +4.35; DKS×9 yday $131.77 → 09:30 $132.80 +9.27; ULTA×2 yday $540.10 → 09:30 $542.00 +3.80; RRC×30 yday $41.64 → 09:30 $41.74 +3.00; GAP×61 yday $20.79 → 09:30 $24.69 +237.90 | — |
| 2026-08-28 09:30 ET | **SELL** | `BE` | 5 | $215.71 | $2.02 | $-61.01 | $1,768.74 | ▼ -61.01 after sell → book $10,493.27; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DASH` | 5 | $233.37 | $2.02 | $-16.88 | $2,933.57 | ▼ -16.88 after sell → book $10,491.25; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEO` | 74 | $17.06 | $2.23 | $-19.99 | $4,193.77 | ▼ -19.99 after sell → book $10,489.01; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BBY` | 15 | $83.85 | $2.06 | $+44.66 | $5,449.47 | ▲ +44.66 after sell → book $10,486.96; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 9 | $132.80 | $2.04 | $+32.58 | $6,642.63 | ▲ +32.58 after sell → book $10,484.92; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ULTA` | 2 | $542.00 | $2.02 | $+7.85 | $7,724.61 | ▲ +7.85 after sell → book $10,482.90; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $6,554.04 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1287.44 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TGB` | 132 | $9.75 | $2.39 | — | $5,264.65 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+11.4; leftover $1287.44 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 67 | $19.00 | $2.19 | — | $3,989.46 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+7.5; leftover $1287.44 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 34 | $37.49 | $2.09 | — | $2,712.71 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_mover; ret5=+5.4; leftover $1287.44 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ABAT` | 483 | $2.66 | $6.23 | — | $1,421.70 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1287.44 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 154 | $8.35 | $2.45 | — | $133.35 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; ret5=+5.1; leftover $1287.44 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.35 | ▼ close $10,222.79 vs 09:30 $10,495.30 (session -242.75) | 16:00 close · cash $133.35 · equity $10,222.79 vs 09:30 $10,495.30 (-272.51; session marks -242.75) · 8 name(s) marked open→close (per-name table). RRC×30 09:30 $41.74 → close $41.46 -8.40; GAP×61 09:30 $24.69 → close $23.48 -73.81; ANF×8 09:30 $146.07 → close $148.42 +18.80; TGB×132 09:30 $9.75 → close $9.18 -75.24; TH×67 09:30 $19.00 → close $18.55 -30.15; FIGR×34 09:30 $37.49 → close $36.05 -48.96; ABAT×483 09:30 $2.66 → close $2.57 -43.47; HAFN×154 09:30 $8.35 → close $8.47 +18.48 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.35 | ▼ 09:30 equity $10,167.82 vs yday $10,222.79 (-54.97) | 09:30 open · cash $133.35 (unchanged overnight, no fees) · equity $10,167.82 vs prior close $10,222.79 (-54.97) · 8 name(s) re-marked at the open (per-name table). RRC×30 yday $41.46 → 09:30 $42.00 +16.20; GAP×61 yday $23.48 → 09:30 $22.98 -30.50; ANF×8 yday $148.42 → 09:30 $148.03 -3.12; TGB×132 yday $9.18 → 09:30 $9.15 -3.96; TH×67 yday $18.55 → 09:30 $18.12 -28.48; FIGR×34 yday $36.05 → 09:30 $35.77 -9.52; ABAT×483 yday $2.57 → 09:30 $2.56 -4.83; HAFN×154 yday $8.47 → 09:30 $8.53 +9.24 | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 30 | $42.00 | $2.10 | $+12.62 | $1,391.25 | ▲ +12.62 after sell → book $10,165.72; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 61 | $22.98 | $2.19 | $+131.66 | $2,790.83 | ▲ +131.66 after sell → book $10,163.53; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.03 | $2.03 | $+11.63 | $3,973.04 | ▲ +11.63 after sell → book $10,161.50; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TGB` | 132 | $9.15 | $2.42 | $-84.00 | $5,178.42 | ▼ -84.00 after sell → book $10,159.08; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 67 | $18.12 | $2.21 | $-63.03 | $6,390.59 | ▼ -63.03 after sell → book $10,156.87; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 34 | $35.77 | $2.11 | $-62.68 | $7,604.65 | ▼ -62.68 after sell → book $10,154.75; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ABAT` | 483 | $2.56 | $6.32 | $-60.85 | $8,834.81 | ▼ -60.85 after sell → book $10,148.43; vs 09:30 mark -6.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 154 | $8.53 | $2.49 | $+22.78 | $10,145.94 | ▲ +22.78 after sell → book $10,145.94; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,145.94 | ▲ close $10,145.94 vs 09:30 $10,167.82 (session +0.00) | 16:00 close · cash $10,145.94 · no lots left · equity $10,145.94. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,145.94 | ▲ 09:30 equity $10,145.94 vs yday $10,145.94 (+0.00) | 09:30 open · cash $10,145.94 · no holdings · equity $10,145.94 vs prior close $10,145.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,145.94 | ▲ close $10,145.94 vs 09:30 $10,145.94 (session +0.00) | 16:00 close · cash $10,145.94 · no lots left · equity $10,145.94. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,145.94 | ▲ 09:30 equity $10,145.94 vs yday $10,145.94 (+0.00) | 09:30 open · cash $10,145.94 · no holdings · equity $10,145.94 vs prior close $10,145.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,145.94 | ▲ close $10,145.94 vs 09:30 $10,145.94 (session +0.00) | 16:00 close · cash $10,145.94 · no lots left · equity $10,145.94. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,145.94 | ▲ 09:30 equity $10,145.94 vs yday $10,145.94 (+0.00) | 09:30 open · cash $10,145.94 · no holdings · equity $10,145.94 vs prior close $10,145.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `PBF` | 16 | $74.75 | $2.04 | — | $8,947.91 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list mover_buy; 🔵; ret5=+8.2; leftover $1268.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `PBR` | 59 | $21.18 | $2.17 | — | $7,696.12 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+17.5; leftover $1268.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `XP` | 61 | $20.74 | $2.17 | — | $6,428.81 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+12.2; leftover $1268.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HP` | 26 | $47.74 | $2.07 | — | $5,185.50 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+15.1; leftover $1268.24 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PBR-A` | 66 | $19.16 | $2.19 | — | $3,918.75 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+17.4; leftover $1268.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VIST` | 16 | $77.14 | $2.04 | — | $2,682.47 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+12.2; leftover $1268.24 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `LULU` | 10 | $121.15 | $2.02 | — | $1,468.95 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight; 🔵; ret5=+3.2; leftover $1268.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 16 | $76.86 | $2.04 | — | $237.15 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; 🔵; ret5=-6.6; leftover $1268.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $237.15 | ▼ close $9,859.02 vs 09:30 $10,145.94 (session -270.19) | 16:00 close · cash $237.15 · equity $9,859.02 vs 09:30 $10,145.94 (-286.92; session marks -270.19) · 8 name(s) marked open→close (per-name table). PBF×16 09:30 $74.75 → close $75.33 +9.28; PBR×59 09:30 $21.18 → close $20.51 -39.53; XP×61 09:30 $20.74 → close $20.00 -45.14; HP×26 09:30 $47.74 → close $45.02 -70.72; PBR-A×66 09:30 $19.16 → close $18.58 -38.28; VIST×16 09:30 $77.14 → close $74.61 -40.48; LULU×10 09:30 $121.15 → close $121.77 +6.20; VSXY×16 09:30 $76.86 → close $73.64 -51.52 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $237.15 | ▼ 09:30 equity $9,537.97 vs yday $9,859.02 (-321.05) | 09:30 open · cash $237.15 (unchanged overnight, no fees) · equity $9,537.97 vs prior close $9,859.02 (-321.05) · 8 name(s) re-marked at the open (per-name table). PBF×16 yday $75.33 → 09:30 $74.50 -13.28; PBR×59 yday $20.51 → 09:30 $20.25 -15.34; XP×61 yday $20.00 → 09:30 $19.67 -20.13; HP×26 yday $45.02 → 09:30 $44.59 -11.18; PBR-A×66 yday $18.58 → 09:30 $18.36 -14.52; VIST×16 yday $74.61 → 09:30 $73.97 -10.24; LULU×10 yday $121.77 → 09:30 $98.15 -236.20; VSXY×16 yday $73.64 → 09:30 $73.63 -0.16 | — |
| 2026-09-04 09:30 ET | **SELL** | `PBF` | 16 | $74.50 | $2.06 | $-8.10 | $1,427.10 | ▼ -8.10 after sell → book $9,535.92; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBR` | 59 | $20.25 | $2.19 | $-59.22 | $2,619.66 | ▼ -59.22 after sell → book $9,533.73; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `XP` | 61 | $19.67 | $2.19 | $-69.64 | $3,817.34 | ▼ -69.64 after sell → book $9,531.54; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `HP` | 26 | $44.59 | $2.09 | $-86.06 | $4,974.59 | ▼ -86.06 after sell → book $9,529.45; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBR-A` | 66 | $18.36 | $2.21 | $-57.20 | $6,184.14 | ▼ -57.20 after sell → book $9,527.24; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VIST` | 16 | $73.97 | $2.06 | $-54.82 | $7,365.60 | ▼ -54.82 after sell → book $9,525.18; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `LULU` | 10 | $98.15 | $2.04 | $-234.06 | $8,345.06 | ▼ -234.06 after sell → book $9,523.14; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `VSXY` | 16 | $73.63 | $2.06 | $-55.78 | $9,521.08 | ▼ -55.78 after sell → book $9,521.08; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 5 | $236.82 | $2.00 | — | $8,334.98 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+8.1; leftover $1190.14 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 133 | $8.94 | $2.39 | — | $7,143.57 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+7.7; leftover $1190.14 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MIR` | 71 | $16.60 | $2.20 | — | $5,962.77 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+12.9; leftover $1190.14 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GORO` | 301 | $3.95 | $3.88 | — | $4,769.93 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+6.9; leftover $1190.14 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GSM` | 254 | $4.67 | $3.28 | — | $3,580.48 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; ret5=+11.9; leftover $1190.14 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `WNC` | 83 | $14.17 | $2.24 | — | $2,402.13 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; 🔵; ret5=+7.9; leftover $1190.14 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `XRX` | 359 | $3.31 | $4.63 | — | $1,209.21 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+11.1; leftover $1190.14 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ABM` | 25 | $46.79 | $2.06 | — | $37.39 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight; ret5=+0.2; leftover $1190.14 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.39 | ▲ close $9,721.22 vs 09:30 $9,537.97 (session +222.83) | 16:00 close · cash $37.39 · equity $9,721.22 vs 09:30 $9,537.97 (+183.25; session marks +222.83) · 8 name(s) marked open→close (per-name table). BE×5 09:30 $236.82 → close $252.87 +80.25; HAFN×133 09:30 $8.94 → close $9.22 +37.24; MIR×71 09:30 $16.60 → close $16.93 +23.43; GORO×301 09:30 $3.95 → close $4.15 +60.20; GSM×254 09:30 $4.67 → close $4.67 +0.00; WNC×83 09:30 $14.17 → close $14.31 +11.62; XRX×359 09:30 $3.31 → close $3.32 +3.59; ABM×25 09:30 $46.79 → close $47.05 +6.50 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.39 | ▲ 09:30 equity $9,791.53 vs yday $9,721.22 (+70.31) | 09:30 open · cash $37.39 (unchanged overnight, no fees) · equity $9,791.53 vs prior close $9,721.22 (+70.31) · 8 name(s) re-marked at the open (per-name table). BE×5 yday $252.87 → 09:30 $267.76 +74.45; HAFN×133 yday $9.22 → 09:30 $8.81 -54.53; MIR×71 yday $16.93 → 09:30 $17.07 +9.94; GORO×301 yday $4.15 → 09:30 $4.13 -6.02; GSM×254 yday $4.67 → 09:30 $4.75 +20.32; WNC×83 yday $14.31 → 09:30 $14.22 -7.47; XRX×359 yday $3.32 → 09:30 $3.50 +64.62; ABM×25 yday $47.05 → 09:30 $45.81 -31.00 | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 5 | $267.76 | $2.03 | $+150.67 | $1,374.17 | ▲ +150.67 after sell → book $9,789.51; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HAFN` | 133 | $8.81 | $2.42 | $-22.10 | $2,543.47 | ▼ -22.10 after sell → book $9,787.08; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MIR` | 71 | $17.07 | $2.22 | $+28.94 | $3,753.22 | ▲ +28.94 after sell → book $9,784.86; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GORO` | 301 | $4.13 | $3.94 | $+46.35 | $4,992.41 | ▲ +46.35 after sell → book $9,780.92; vs 09:30 mark -3.94 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `GSM` | 254 | $4.75 | $3.33 | $+13.71 | $6,195.58 | ▲ +13.71 after sell → book $9,777.59; vs 09:30 mark -3.33 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `WNC` | 83 | $14.22 | $2.26 | $-0.35 | $7,373.58 | ▼ -0.35 after sell → book $9,775.33; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `XRX` | 359 | $3.50 | $4.70 | $+58.88 | $8,625.37 | ▲ +58.88 after sell → book $9,770.62; vs 09:30 mark -4.71 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `ABM` | 25 | $45.81 | $2.08 | $-28.65 | $9,768.54 | ▼ -28.65 after sell → book $9,768.54; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,768.54 | ▲ close $9,768.54 vs 09:30 $9,791.53 (session +0.00) | 16:00 close · cash $9,768.54 · no lots left · equity $9,768.54. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,768.54 | ▲ 09:30 equity $9,768.54 vs yday $9,768.54 (-0.00) | 09:30 open · cash $9,768.54 · no holdings · equity $9,768.54 vs prior close $9,768.54 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,768.54 | ▲ close $9,768.54 vs 09:30 $9,768.54 (session +0.00) | 16:00 close · cash $9,768.54 · no lots left · equity $9,768.54. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,768.54 | ▲ 09:30 equity $9,768.54 vs yday $9,768.54 (-0.00) | 09:30 open · cash $9,768.54 · no holdings · equity $9,768.54 vs prior close $9,768.54 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,768.54 | ▲ close $9,768.54 vs 09:30 $9,768.54 (session +0.00) | 16:00 close · cash $9,768.54 · no lots left · equity $9,768.54. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,768.54 | ▲ 09:30 equity $9,768.54 vs yday $9,768.54 (-0.00) | 09:30 open · cash $9,768.54 · no holdings · equity $9,768.54 vs prior close $9,768.54 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 22 | $54.91 | $2.06 | — | $8,558.46 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; 🔵; ret5=+24.3; leftover $1221.07 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 248 | $4.91 | $3.20 | — | $7,337.58 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1221.07 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CECO` | 15 | $77.51 | $2.04 | — | $6,172.90 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+11.1; leftover $1221.07 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PBR` | 57 | $21.21 | $2.16 | — | $4,961.77 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+2.5; leftover $1221.07 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 15 | $77.33 | $2.04 | — | $3,799.78 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; ret5=+2.5; leftover $1221.07 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ARLO` | 92 | $13.22 | $2.27 | — | $2,581.28 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+7.3; leftover $1221.07 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 575 | $2.12 | $7.42 | — | $1,354.86 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1221.07 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SSL` | 85 | $14.35 | $2.25 | — | $132.87 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+15.5; leftover $1221.07 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.87 | ▼ close $9,718.37 vs 09:30 $9,768.54 (session -26.76) | 16:00 close · cash $132.87 · equity $9,718.37 vs 09:30 $9,768.54 (-50.17; session marks -26.76) · 8 name(s) marked open→close (per-name table). ASO×22 09:30 $54.91 → close $55.36 +9.90; BNC×248 09:30 $4.91 → close $4.80 -27.28; CECO×15 09:30 $77.51 → close $78.34 +12.45; PBR×57 09:30 $21.21 → close $21.20 -0.57; VIST×15 09:30 $77.33 → close $76.27 -15.90; ARLO×92 09:30 $13.22 → close $13.19 -2.76; BAK×575 09:30 $2.12 → close $2.08 -23.00; SSL×85 09:30 $14.35 → close $14.59 +20.40 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.87 | ▼ 09:30 equity $9,696.36 vs yday $9,718.37 (-22.01) | 09:30 open · cash $132.87 (unchanged overnight, no fees) · equity $9,696.36 vs prior close $9,718.37 (-22.01) · 8 name(s) re-marked at the open (per-name table). ASO×22 yday $55.36 → 09:30 $54.75 -13.42; BNC×248 yday $4.80 → 09:30 $5.03 +57.04; CECO×15 yday $78.34 → 09:30 $74.34 -60.00; PBR×57 yday $21.20 → 09:30 $21.23 +1.71; VIST×15 yday $76.27 → 09:30 $77.10 +12.45; ARLO×92 yday $13.19 → 09:30 $13.07 -11.04; BAK×575 yday $2.08 → 09:30 $2.05 -17.25; SSL×85 yday $14.59 → 09:30 $14.69 +8.50 | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 22 | $54.75 | $2.08 | $-7.65 | $1,335.29 | ▼ -7.65 after sell → book $9,694.28; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 248 | $5.03 | $3.25 | $+23.31 | $2,579.48 | ▲ +23.31 after sell → book $9,691.03; vs 09:30 mark -3.25 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `CECO` | 15 | $74.34 | $2.06 | $-51.64 | $3,692.52 | ▼ -51.64 after sell → book $9,688.97; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `PBR` | 57 | $21.23 | $2.18 | $-3.20 | $4,900.45 | ▼ -3.20 after sell → book $9,686.79; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIST` | 15 | $77.10 | $2.06 | $-7.54 | $6,054.90 | ▼ -7.54 after sell → book $9,684.74; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ARLO` | 92 | $13.07 | $2.29 | $-18.36 | $7,255.05 | ▼ -18.36 after sell → book $9,682.45; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 575 | $2.05 | $7.52 | $-55.19 | $8,426.27 | ▼ -55.19 after sell → book $9,674.92; vs 09:30 mark -7.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SSL` | 85 | $14.69 | $2.27 | $+24.39 | $9,672.65 | ▲ +24.39 after sell → book $9,672.65; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,672.65 | ▲ close $9,672.65 vs 09:30 $9,696.36 (session +0.00) | 16:00 close · cash $9,672.65 · no lots left · equity $9,672.65. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,672.65 | ▲ 09:30 equity $9,672.65 vs yday $9,672.65 (+0.00) | 09:30 open · cash $9,672.65 · no holdings · equity $9,672.65 vs prior close $9,672.65 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,672.65 | ▲ close $9,672.65 vs 09:30 $9,672.65 (session +0.00) | 16:00 close · cash $9,672.65 · no lots left · equity $9,672.65. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,672.65 | ▲ 09:30 equity $9,672.65 vs yday $9,672.65 (+0.00) | 09:30 open · cash $9,672.65 · no holdings · equity $9,672.65 vs prior close $9,672.65 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 30 | $39.99 | $2.08 | — | $8,470.87 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1209.08 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `TALO` | 67 | $17.87 | $2.19 | — | $7,271.39 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+6.8; leftover $1209.08 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `APA` | 26 | $46.44 | $2.07 | — | $6,061.89 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+8.9; leftover $1209.08 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `CVI` | 23 | $51.05 | $2.06 | — | $4,885.68 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; ret5=+13.1; leftover $1209.08 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 205 | $5.87 | $2.64 | — | $3,679.68 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1209.08 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 catal🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 13 | $87.40 | $2.03 | — | $2,541.45 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1209.08 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `FRO` | 23 | $52.52 | $2.06 | — | $1,331.43 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+10.7; leftover $1209.08 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `GFR` | 177 | $6.83 | $2.52 | — | $120.00 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+11.2; leftover $1209.08 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $120.00 | ▼ close $9,408.23 vs 09:30 $9,672.65 (session -246.77) | 16:00 close · cash $120.00 · equity $9,408.23 vs 09:30 $9,672.65 (-264.42; session marks -246.77) · 8 name(s) marked open→close (per-name table). SM×30 09:30 $39.99 → close $38.16 -54.90; TALO×67 09:30 $17.87 → close $17.42 -30.15; APA×26 09:30 $46.44 → close $44.79 -42.90; CVI×23 09:30 $51.05 → close $53.05 +46.00; RIG×205 09:30 $5.87 → close $5.54 -67.65; VAL×13 09:30 $87.40 → close $82.52 -63.44; FRO×23 09:30 $52.52 → close $53.67 +26.45; GFR×177 09:30 $6.83 → close $6.49 -60.18 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $120.00 | ▼ 09:30 equity $9,374.04 vs yday $9,408.23 (-34.19) | 09:30 open · cash $120.00 (unchanged overnight, no fees) · equity $9,374.04 vs prior close $9,408.23 (-34.19) · 8 name(s) re-marked at the open (per-name table). SM×30 yday $38.16 → 09:30 $37.57 -17.70; TALO×67 yday $17.42 → 09:30 $17.19 -15.41; APA×26 yday $44.79 → 09:30 $44.63 -4.16; CVI×23 yday $53.05 → 09:30 $51.88 -26.91; RIG×205 yday $5.54 → 09:30 $5.58 +8.20; VAL×13 yday $82.52 → 09:30 $83.20 +8.84; FRO×23 yday $53.67 → 09:30 $54.31 +14.72; GFR×177 yday $6.49 → 09:30 $6.48 -1.77 | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 30 | $37.57 | $2.10 | $-76.78 | $1,245.00 | ▼ -76.78 after sell → book $9,371.94; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TALO` | 67 | $17.19 | $2.21 | $-49.96 | $2,394.52 | ▼ -49.96 after sell → book $9,369.73; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `APA` | 26 | $44.63 | $2.09 | $-51.22 | $3,552.81 | ▼ -51.22 after sell → book $9,367.64; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CVI` | 23 | $51.88 | $2.08 | $+14.95 | $4,743.97 | ▲ +14.95 after sell → book $9,365.56; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 205 | $5.58 | $2.69 | $-64.78 | $5,885.19 | ▼ -64.78 after sell → book $9,362.88; vs 09:30 mark -2.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 13 | $83.20 | $2.05 | $-58.68 | $6,964.74 | ▼ -58.68 after sell → book $9,360.83; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FRO` | 23 | $54.31 | $2.08 | $+37.03 | $8,211.79 | ▲ +37.03 after sell → book $9,358.75; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `GFR` | 177 | $6.48 | $2.56 | $-67.03 | $9,356.19 | ▼ -67.03 after sell → book $9,356.19; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SFL` | 86 | $13.55 | $2.25 | — | $8,188.64 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+7.8; leftover $1169.52 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `FTAI` | 5 | $196.50 | $2.00 | — | $7,204.13 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; 🔵; ret5=+2.5; leftover $1169.52 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 31 | $36.76 | $2.08 | — | $6,062.49 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; leftover $1169.52 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `VSTS` | 84 | $13.90 | $2.24 | — | $4,892.65 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+9.1; leftover $1169.52 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ARLO` | 85 | $13.62 | $2.25 | — | $3,732.70 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+7.3; leftover $1169.52 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `CECO` | 16 | $72.95 | $2.04 | — | $2,563.47 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+11.1; leftover $1169.52 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `EROC` | 92 | $12.64 | $2.27 | — | $1,398.32 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; ret5=-3.6; leftover $1169.52 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AESI` | 85 | $13.65 | $2.25 | — | $235.82 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+6.8; leftover $1169.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $235.82 | ▼ close $9,331.62 vs 09:30 $9,374.04 (session -7.19) | 16:00 close · cash $235.82 · equity $9,331.62 vs 09:30 $9,374.04 (-42.42; session marks -7.19) · 8 name(s) marked open→close (per-name table). SFL×86 09:30 $13.55 → close $13.75 +17.20; FTAI×5 09:30 $196.50 → close $195.07 -7.15; FPS×31 09:30 $36.76 → close $38.06 +40.30; VSTS×84 09:30 $13.90 → close $13.84 -5.04; ARLO×85 09:30 $13.62 → close $13.35 -22.95; CECO×16 09:30 $72.95 → close $70.83 -33.92; EROC×92 09:30 $12.64 → close $12.90 +23.92; AESI×85 09:30 $13.65 → close $13.42 -19.55 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $235.82 | ▲ 09:30 equity $9,393.25 vs yday $9,331.62 (+61.63) | 09:30 open · cash $235.82 (unchanged overnight, no fees) · equity $9,393.25 vs prior close $9,331.62 (+61.63) · 8 name(s) re-marked at the open (per-name table). SFL×86 yday $13.75 → 09:30 $13.74 -0.86; FTAI×5 yday $195.07 → 09:30 $195.55 +2.40; FPS×31 yday $38.06 → 09:30 $39.50 +44.64; VSTS×84 yday $13.84 → 09:30 $13.71 -10.92; ARLO×85 yday $13.35 → 09:30 $13.42 +5.95; CECO×16 yday $70.83 → 09:30 $71.00 +2.72; EROC×92 yday $12.90 → 09:30 $13.00 +9.20; AESI×85 yday $13.42 → 09:30 $13.52 +8.50 | — |
| 2026-09-18 09:30 ET | **SELL** | `SFL` | 86 | $13.74 | $2.27 | $+11.82 | $1,415.19 | ▲ +11.82 after sell → book $9,390.98; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FTAI` | 5 | $195.55 | $2.02 | $-8.78 | $2,390.92 | ▼ -8.78 after sell → book $9,388.96; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 31 | $39.50 | $2.10 | $+80.75 | $3,613.31 | ▲ +80.75 after sell → book $9,386.85; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `VSTS` | 84 | $13.71 | $2.27 | $-20.47 | $4,762.69 | ▼ -20.47 after sell → book $9,384.59; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `ARLO` | 85 | $13.42 | $2.27 | $-21.51 | $5,901.12 | ▼ -21.51 after sell → book $9,382.32; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CECO` | 16 | $71.00 | $2.06 | $-35.30 | $7,035.06 | ▼ -35.30 after sell → book $9,380.26; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `EROC` | 92 | $13.00 | $2.29 | $+28.56 | $8,228.77 | ▲ +28.56 after sell → book $9,377.97; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AESI` | 85 | $13.52 | $2.27 | $-15.56 | $9,375.70 | ▼ -15.56 after sell → book $9,375.70; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `PURR` | 169 | $13.82 | $2.50 | — | $7,037.62 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; ret5=+16.5; leftover $2343.93 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `ARE` | 41 | $56.70 | $2.11 | — | $4,710.81 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+13.7; leftover $2343.93 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `USDE` | 245 | $9.54 | $3.16 | — | $2,370.35 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover; ret5=+15.8; leftover $2343.93 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 311 | $7.54 | $4.01 | — | $22.95 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_mover; 🔵; ret5=-20.9; leftover $2343.93 | join🟡 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.95 | ▼ close $9,362.53 vs 09:30 $9,393.25 (session -1.38) | 16:00 close · cash $22.95 · equity $9,362.53 vs 09:30 $9,393.25 (-30.72; session marks -1.38) · 4 name(s) marked open→close (per-name table). PURR×169 09:30 $13.82 → close $14.09 +45.63; ARE×41 09:30 $56.70 → close $53.30 -139.40; USDE×245 09:30 $9.54 → close $10.19 +159.25; FLNC×311 09:30 $7.54 → close $7.32 -66.86 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.95 | ▲ 09:30 equity $10,174.00 vs yday $9,362.53 (+811.47) | 09:30 open · cash $22.95 (unchanged overnight, no fees) · equity $10,174.00 vs prior close $9,362.53 (+811.47) · 4 name(s) re-marked at the open (per-name table). PURR×169 yday $14.09 → 09:30 $14.65 +94.64; ARE×41 yday $53.30 → 09:30 $53.39 +3.69; USDE×245 yday $10.19 → 09:30 $13.05 +700.70; FLNC×311 yday $7.32 → 09:30 $7.36 +12.44 | — |
| 2026-09-21 09:30 ET | **SELL** | `PURR` | 169 | $14.65 | $2.54 | $+135.23 | $2,496.26 | ▲ +135.23 after sell → book $10,171.46; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARE` | 41 | $53.39 | $2.14 | $-139.96 | $4,683.11 | ▼ -139.96 after sell → book $10,169.32; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `USDE` | 245 | $13.05 | $3.23 | $+853.56 | $7,877.13 | ▲ +853.56 after sell → book $10,166.09; vs 09:30 mark -3.23 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 311 | $7.36 | $4.08 | $-62.52 | $10,162.01 | ▼ -62.52 after sell → book $10,162.01; vs 09:30 mark -4.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `MSTR` | 7 | $164.58 | $2.01 | — | $9,007.94 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.5; leftover $1270.25 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 5 | $230.25 | $2.00 | — | $7,854.68 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+12.5; leftover $1270.25 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-21 09:30 ET | **BUY** | `KEEL` | 304 | $4.17 | $3.92 | — | $6,581.56 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; ret5=+18.4; leftover $1270.25 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 108 | $11.67 | $2.31 | — | $5,318.89 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover; ret5=+31.3; leftover $1270.25 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 136 | $9.31 | $2.40 | — | $4,050.33 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer,yday_mover; ret5=-2.4; leftover $1270.25 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 94 | $13.47 | $2.27 | — | $2,781.41 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1270.25 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `COHR` | 3 | $326.48 | $2.00 | — | $1,799.97 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+3.9; leftover $1270.25 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `FORM` | 10 | $123.00 | $2.02 | — | $567.95 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+3.0; leftover $1270.25 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $567.95 | ▲ close $10,198.09 vs 09:30 $10,174.00 (session +55.02) | 16:00 close · cash $567.95 · equity $10,198.09 vs 09:30 $10,174.00 (+24.09; session marks +55.02) · 8 name(s) marked open→close (per-name table). MSTR×7 09:30 $164.58 → close $168.50 +27.44; VICR×5 09:30 $230.25 → close $223.90 -31.75; KEEL×304 09:30 $4.17 → close $4.07 -31.92; SECZ×108 09:30 $11.67 → close $13.50 +197.64; BKKT×136 09:30 $9.31 → close $9.14 -23.12; BTDR×94 09:30 $13.47 → close $13.14 -31.49; COHR×3 09:30 $326.48 → close $321.52 -14.88; FORM×10 09:30 $123.00 → close $119.31 -36.90 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `MU` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AMKR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-24 | `BZ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `VIPS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DK` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `UEC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TUYA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `MMED` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MDT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SSL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FNKO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TII` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RCKT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PROK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `BMRN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `LENZ` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RANI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `XRX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AGCO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `CF` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FLR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FMC` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `XP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FIVE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PVH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UGP` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OBE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `WDS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HELP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ARLO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CECO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SID` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `EQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SANM` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `QRVO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `FRO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `KGS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CECO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PUMP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ARLO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `AESI` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `MSTR` | 7 | 2026-09-21 @ $164.58 | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.5; leftover $1270.25 |
| `VICR` | 5 | 2026-09-21 @ $230.25 | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+12.5; leftover $1270.25 |
| `KEEL` | 304 | 2026-09-21 @ $4.17 | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; ret5=+18.4; leftover $1270.25 |
| `SECZ` | 108 | 2026-09-21 @ $11.67 | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover; ret5=+31.3; leftover $1270.25 |
| `BKKT` | 136 | 2026-09-21 @ $9.31 | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer,yday_mover; ret5=-2.4; leftover $1270.25 |
| `BTDR` | 94 | 2026-09-21 @ $13.47 | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1270.25 |
| `COHR` | 3 | 2026-09-21 @ $326.48 | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+3.9; leftover $1270.25 |
| `FORM` | 10 | 2026-09-21 @ $123.00 | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+3.0; leftover $1270.25 |
