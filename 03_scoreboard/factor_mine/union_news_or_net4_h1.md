# Factor mine action — `union_news_or_net4_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 4

Cash book **-1.06%** ($9,894) · signal-only (no cash/fees) was -0.41%. Starts YES **0/26**. Fills 140 · skips 25 · realized $-42.89.

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
- Must-have: the morning news packet OR the prior-export headline is green.
- Must-have: camera net (+G −R) is at least 4.
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
- **Gate** `news_or_headline=True,cam_net_min=4` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $16.52.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `HLIT` | 126 | — | $13.18 | +0.00 | $13.92 | +93.24 | +93.24 | +0.00 | +93.24 |
| 2026-08-14 | `SNDK` | 1 | — | $1646.93 | +0.00 | $1641.11 | -5.82 | -5.82 | +0.00 | -5.82 |
| 2026-08-14 | `ANGX` | 386 | — | $4.31 | +0.00 | $4.37 | +23.16 | +23.16 | +0.00 | +23.16 |
| 2026-08-14 | `ARX` | 85 | — | $19.57 | +0.00 | $19.58 | +0.85 | +0.85 | +0.00 | +0.85 |
| 2026-08-14 | `MH` | 123 | — | $13.55 | +0.00 | $13.10 | -55.35 | -55.35 | +0.00 | -55.35 |
| 2026-08-14 | `VELO` | 108 | — | $15.38 | +0.00 | $16.16 | +84.24 | +84.24 | +0.00 | +84.24 |
| 2026-08-17 | `HLIT` | 126 | $13.92 | $13.84 | -10.08 | — | +0.00 | -10.08 | +83.16 | — |
| 2026-08-17 | `SNDK` | 1 | $1641.11 | $1700.74 | +59.63 | — | +0.00 | +59.63 | +53.81 | — |
| 2026-08-17 | `ANGX` | 386 | $4.37 | $4.60 | +88.78 | — | +0.00 | +88.78 | +111.94 | — |
| 2026-08-17 | `ARX` | 85 | $19.58 | $19.57 | -0.85 | — | +0.00 | -0.85 | +0.00 | — |
| 2026-08-17 | `MH` | 123 | $13.10 | $13.16 | +7.38 | — | +0.00 | +7.38 | -47.97 | — |
| 2026-08-17 | `VELO` | 108 | $16.16 | $16.05 | -11.88 | — | +0.00 | -11.88 | +72.36 | — |
| 2026-08-17 | `DVN` | 44 | — | $46.18 | +0.00 | $47.57 | +61.16 | +61.16 | +0.00 | +61.16 |
| 2026-08-17 | `EOG` | 14 | — | $142.77 | +0.00 | $146.15 | +47.32 | +47.32 | +0.00 | +47.32 |
| 2026-08-17 | `FANG` | 10 | — | $202.70 | +0.00 | $206.29 | +35.90 | +35.90 | +0.00 | +35.90 |
| 2026-08-17 | `GLOB` | 55 | — | $37.18 | +0.00 | $36.26 | -50.60 | -50.60 | +0.00 | -50.60 |
| 2026-08-17 | `TPG` | 38 | — | $52.67 | +0.00 | $51.77 | -34.20 | -34.20 | +0.00 | -34.20 |
| 2026-08-18 | `DVN` | 44 | $47.57 | $48.00 | +18.92 | — | +0.00 | +18.92 | +80.08 | — |
| 2026-08-18 | `EOG` | 14 | $146.15 | $148.04 | +26.46 | — | +0.00 | +26.46 | +73.78 | — |
| 2026-08-18 | `FANG` | 10 | $206.29 | $208.93 | +26.40 | — | +0.00 | +26.40 | +62.30 | — |
| 2026-08-18 | `GLOB` | 55 | $36.26 | $36.98 | +39.60 | — | +0.00 | +39.60 | -11.00 | — |
| 2026-08-18 | `TPG` | 38 | $51.77 | $51.77 | +0.00 | — | +0.00 | +0.00 | -34.20 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 14 | — | $91.01 | +0.00 | $93.63 | +36.68 | +36.68 | +0.00 | +36.68 |
| 2026-08-20 | `APA` | 29 | — | $44.76 | +0.00 | $44.39 | -10.73 | -10.73 | +0.00 | -10.73 |
| 2026-08-20 | `AUTL` | 525 | — | $2.47 | +0.00 | $2.46 | -5.25 | -5.25 | +0.00 | -5.25 |
| 2026-08-20 | `CRSP` | 22 | — | $58.73 | +0.00 | $58.12 | -13.42 | -13.42 | +0.00 | -13.42 |
| 2026-08-20 | `MRK` | 8 | — | $150.78 | +0.00 | $148.99 | -14.32 | -14.32 | +0.00 | -14.32 |
| 2026-08-20 | `ASST` | 81 | — | $16.00 | +0.00 | $16.13 | +10.53 | +10.53 | +0.00 | +10.53 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `ZLAB` | 48 | — | $26.57 | +0.00 | $26.02 | -26.40 | -26.40 | +0.00 | -26.40 |
| 2026-08-21 | `BHP` | 14 | $93.63 | $95.72 | +29.26 | — | +0.00 | +29.26 | +65.94 | — |
| 2026-08-21 | `APA` | 29 | $44.39 | $44.52 | +3.77 | — | +0.00 | +3.77 | -6.96 | — |
| 2026-08-21 | `AUTL` | 525 | $2.46 | $2.47 | +5.25 | $2.41 | -31.50 | -26.25 | +0.00 | -31.50 |
| 2026-08-21 | `CRSP` | 22 | $58.12 | $59.72 | +35.20 | $59.50 | -4.84 | +30.36 | +21.78 | +16.94 |
| 2026-08-21 | `MRK` | 8 | $148.99 | $149.12 | +1.04 | — | +0.00 | +1.04 | -13.28 | — |
| 2026-08-21 | `ASST` | 81 | $16.13 | $17.66 | +123.93 | — | +0.00 | +123.93 | +134.46 | — |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | — | +0.00 | -1.68 | -136.24 | — |
| 2026-08-21 | `ZLAB` | 48 | $26.02 | $26.25 | +11.04 | — | +0.00 | +11.04 | -15.36 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `FUTU` | 11 | — | $115.18 | +0.00 | $123.64 | +93.06 | +93.06 | +0.00 | +93.06 |
| 2026-08-21 | `GRAL` | 16 | — | $78.88 | +0.00 | $79.54 | +10.56 | +10.56 | +0.00 | +10.56 |
| 2026-08-21 | `VIRT` | 21 | — | $60.66 | +0.00 | $67.93 | +152.67 | +152.67 | +0.00 | +152.67 |
| 2026-08-21 | `MFC` | 30 | — | $42.48 | +0.00 | $42.51 | +0.90 | +0.90 | +0.00 | +0.90 |
| 2026-08-21 | `ABTC` | 150 | — | $8.66 | +0.00 | $7.93 | -109.50 | -109.50 | +0.00 | -109.50 |
| 2026-08-24 | `AUTL` | 525 | $2.41 | $2.40 | -5.25 | — | +0.00 | -5.25 | -36.75 | — |
| 2026-08-24 | `CRSP` | 22 | $59.50 | $58.75 | -16.50 | $57.08 | -36.85 | -53.35 | +0.44 | -36.41 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `FUTU` | 11 | $123.64 | $121.00 | -29.04 | — | +0.00 | -29.04 | +64.02 | — |
| 2026-08-24 | `GRAL` | 16 | $79.54 | $81.87 | +37.28 | — | +0.00 | +37.28 | +47.84 | — |
| 2026-08-24 | `VIRT` | 21 | $67.93 | $66.80 | -23.73 | — | +0.00 | -23.73 | +128.94 | — |
| 2026-08-24 | `MFC` | 30 | $42.51 | $42.31 | -6.00 | — | +0.00 | -6.00 | -5.10 | — |
| 2026-08-24 | `ABTC` | 150 | $7.93 | $8.00 | +10.50 | — | +0.00 | +10.50 | -99.00 | — |
| 2026-08-25 | `CRSP` | 22 | $57.08 | $57.93 | +18.81 | — | +0.00 | +18.81 | -17.60 | — |
| 2026-08-25 | `AU` | 22 | — | $118.52 | +0.00 | $123.39 | +107.14 | +107.14 | +0.00 | +107.14 |
| 2026-08-25 | `FCX` | 33 | — | $77.13 | +0.00 | $79.91 | +91.74 | +91.74 | +0.00 | +91.74 |
| 2026-08-25 | `EZPW` | 74 | — | $35.05 | +0.00 | $35.23 | +13.32 | +13.32 | +0.00 | +13.32 |
| 2026-08-25 | `AMX` | 109 | — | $23.80 | +0.00 | $23.75 | -5.45 | -5.45 | +0.00 | -5.45 |
| 2026-08-26 | `AU` | 22 | $123.39 | $119.80 | -78.98 | — | +0.00 | -78.98 | +28.16 | — |
| 2026-08-26 | `FCX` | 33 | $79.91 | $79.34 | -18.81 | — | +0.00 | -18.81 | +72.93 | — |
| 2026-08-26 | `EZPW` | 74 | $35.23 | $35.70 | +34.78 | — | +0.00 | +34.78 | +48.10 | — |
| 2026-08-26 | `AMX` | 109 | $23.75 | $23.75 | +0.00 | $23.62 | -14.17 | -14.17 | -5.45 | -19.62 |
| 2026-08-26 | `FNV` | 14 | — | $267.02 | +0.00 | $267.37 | +4.90 | +4.90 | +0.00 | +4.90 |
| 2026-08-26 | `ASST` | 192 | — | $20.72 | +0.00 | $21.50 | +149.76 | +149.76 | +0.00 | +149.76 |
| 2026-08-27 | `AMX` | 109 | $23.62 | $23.77 | +16.35 | — | +0.00 | +16.35 | -3.27 | — |
| 2026-08-27 | `FNV` | 14 | $267.37 | $267.23 | -1.96 | — | +0.00 | -1.96 | +2.94 | — |
| 2026-08-27 | `ASST` | 192 | $21.50 | $22.45 | +182.40 | — | +0.00 | +182.40 | +332.16 | — |
| 2026-08-27 | `ACMR` | 16 | — | $81.65 | +0.00 | $80.49 | -18.56 | -18.56 | +0.00 | -18.56 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `CM` | 11 | — | $118.77 | +0.00 | $114.84 | -43.23 | -43.23 | +0.00 | -43.23 |
| 2026-08-27 | `GEN` | 45 | — | $29.83 | +0.00 | $30.50 | +30.15 | +30.15 | +0.00 | +30.15 |
| 2026-08-27 | `LRCX` | 4 | — | $318.88 | +0.00 | $318.58 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-27 | `NVDA` | 6 | — | $222.86 | +0.00 | $227.98 | +30.72 | +30.72 | +0.00 | +30.72 |
| 2026-08-27 | `AXTI` | 19 | — | $70.30 | +0.00 | $66.92 | -64.22 | -64.22 | +0.00 | -64.22 |
| 2026-08-28 | `ACMR` | 16 | $80.49 | $79.27 | -19.52 | — | +0.00 | -19.52 | -38.08 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `CM` | 11 | $114.84 | $115.66 | +9.02 | — | +0.00 | +9.02 | -34.21 | — |
| 2026-08-28 | `GEN` | 45 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +30.15 | — |
| 2026-08-28 | `LRCX` | 4 | $318.58 | $318.03 | -2.20 | — | +0.00 | -2.20 | -3.40 | — |
| 2026-08-28 | `NVDA` | 6 | $227.98 | $227.36 | -3.72 | — | +0.00 | -3.72 | +27.00 | — |
| 2026-08-28 | `AXTI` | 19 | $66.92 | $65.29 | -30.97 | — | +0.00 | -30.97 | -95.19 | — |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 5 | — | $240.22 | +0.00 | $236.98 | -16.20 | -16.20 | +0.00 | -16.20 |
| 2026-08-28 | `PLAB` | 44 | — | $30.01 | +0.00 | $27.73 | -100.32 | -100.32 | +0.00 | -100.32 |
| 2026-08-28 | `ADSK` | 5 | — | $261.16 | +0.00 | $260.66 | -2.50 | -2.50 | +0.00 | -2.50 |
| 2026-08-28 | `SEDG` | 40 | — | $32.90 | +0.00 | $31.41 | -59.60 | -59.60 | +0.00 | -59.60 |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | — | +0.00 | +10.08 | -7.68 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -65.94 | — |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | — | +0.00 | +5.64 | -44.13 | — |
| 2026-08-31 | `DDOG` | 5 | $236.98 | $233.97 | -15.07 | — | +0.00 | -15.07 | -31.27 | — |
| 2026-08-31 | `PLAB` | 44 | $27.73 | $28.04 | +13.64 | — | +0.00 | +13.64 | -86.68 | — |
| 2026-08-31 | `ADSK` | 5 | $260.66 | $257.71 | -14.75 | — | +0.00 | -14.75 | -17.25 | — |
| 2026-08-31 | `SEDG` | 40 | $31.41 | $31.15 | -10.40 | — | +0.00 | -10.40 | -70.00 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 4 | — | $351.74 | +0.00 | $357.16 | +21.68 | +21.68 | +0.00 | +21.68 |
| 2026-09-03 | `DELL` | 3 | — | $486.31 | +0.00 | $516.39 | +90.24 | +90.24 | +0.00 | +90.24 |
| 2026-09-03 | `CXW` | 45 | — | $32.31 | +0.00 | $33.66 | +60.75 | +60.75 | +0.00 | +60.75 |
| 2026-09-03 | `FRNM` | 92 | — | $15.87 | +0.00 | $16.90 | +94.76 | +94.76 | +0.00 | +94.76 |
| 2026-09-03 | `MMED` | 61 | — | $23.88 | +0.00 | $23.84 | -2.44 | -2.44 | +0.00 | -2.44 |
| 2026-09-03 | `DE` | 2 | — | $703.25 | +0.00 | $694.41 | -17.68 | -17.68 | +0.00 | -17.68 |
| 2026-09-03 | `HPE` | 30 | — | $47.60 | +0.00 | $54.44 | +205.20 | +205.20 | +0.00 | +205.20 |
| 2026-09-04 | `AVGO` | 4 | $357.16 | $359.70 | +10.16 | — | +0.00 | +10.16 | +31.84 | — |
| 2026-09-04 | `DELL` | 3 | $516.39 | $513.78 | -7.83 | — | +0.00 | -7.83 | +82.41 | — |
| 2026-09-04 | `CXW` | 45 | $33.66 | $33.46 | -9.00 | — | +0.00 | -9.00 | +51.75 | — |
| 2026-09-04 | `FRNM` | 92 | $16.90 | $16.40 | -46.00 | $16.31 | -8.28 | -54.28 | +48.76 | +40.48 |
| 2026-09-04 | `MMED` | 61 | $23.84 | $23.84 | +0.00 | $23.29 | -33.55 | -33.55 | -2.44 | -35.99 |
| 2026-09-04 | `DE` | 2 | $694.41 | $692.03 | -4.76 | — | +0.00 | -4.76 | -22.44 | — |
| 2026-09-04 | `HPE` | 30 | $54.44 | $53.85 | -17.70 | $52.00 | -55.50 | -73.20 | +187.50 | +132.00 |
| 2026-09-04 | `CRM` | 4 | — | $263.36 | +0.00 | $259.23 | -16.52 | -16.52 | +0.00 | -16.52 |
| 2026-09-04 | `MRX` | 15 | — | $75.65 | +0.00 | $78.27 | +39.30 | +39.30 | +0.00 | +39.30 |
| 2026-09-04 | `BE` | 5 | — | $236.82 | +0.00 | $252.87 | +80.25 | +80.25 | +0.00 | +80.25 |
| 2026-09-04 | `AMX` | 52 | — | $23.03 | +0.00 | $23.00 | -1.56 | -1.56 | +0.00 | -1.56 |
| 2026-09-04 | `BAK` | 623 | — | $1.94 | +0.00 | $1.89 | -31.15 | -31.15 | +0.00 | -31.15 |
| 2026-09-08 | `FRNM` | 92 | $16.31 | $16.74 | +39.56 | — | +0.00 | +39.56 | +80.04 | — |
| 2026-09-08 | `MMED` | 61 | $23.29 | $23.16 | -7.93 | — | +0.00 | -7.93 | -43.92 | — |
| 2026-09-08 | `HPE` | 30 | $52.00 | $52.29 | +8.70 | — | +0.00 | +8.70 | +140.70 | — |
| 2026-09-08 | `CRM` | 4 | $259.23 | $253.72 | -22.04 | — | +0.00 | -22.04 | -38.56 | — |
| 2026-09-08 | `MRX` | 15 | $78.27 | $78.84 | +8.55 | — | +0.00 | +8.55 | +47.85 | — |
| 2026-09-08 | `BE` | 5 | $252.87 | $267.76 | +74.45 | — | +0.00 | +74.45 | +154.70 | — |
| 2026-09-08 | `AMX` | 52 | $23.00 | $23.15 | +7.80 | — | +0.00 | +7.80 | +6.24 | — |
| 2026-09-08 | `BAK` | 623 | $1.89 | $1.94 | +31.15 | — | +0.00 | +31.15 | +0.00 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 32 | — | $164.43 | +0.00 | $150.28 | -452.80 | -452.80 | +0.00 | -452.80 |
| 2026-09-11 | `BTI` | 95 | — | $56.03 | +0.00 | $55.24 | -75.05 | -75.05 | +0.00 | -75.05 |
| 2026-09-14 | `ORCL` | 32 | $150.28 | $141.42 | -283.52 | — | +0.00 | -283.52 | -736.32 | — |
| 2026-09-14 | `BTI` | 95 | $55.24 | $57.12 | +178.60 | — | +0.00 | +178.60 | +103.55 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 95 | — | $26.27 | +0.00 | $26.59 | +30.40 | +30.40 | +0.00 | +30.40 |
| 2026-09-16 | `QCOM` | 13 | — | $189.17 | +0.00 | $184.84 | -56.29 | -56.29 | +0.00 | -56.29 |
| 2026-09-16 | `SM` | 62 | — | $39.99 | +0.00 | $38.16 | -113.46 | -113.46 | +0.00 | -113.46 |
| 2026-09-16 | `AMX` | 108 | — | $23.18 | +0.00 | $22.98 | -21.60 | -21.60 | +0.00 | -21.60 |
| 2026-09-17 | `WAY` | 95 | $26.59 | $26.51 | -7.60 | — | +0.00 | -7.60 | +22.80 | — |
| 2026-09-17 | `QCOM` | 13 | $184.84 | $190.35 | +71.63 | — | +0.00 | +71.63 | +15.34 | — |
| 2026-09-17 | `SM` | 62 | $38.16 | $37.57 | -36.58 | — | +0.00 | -36.58 | -150.04 | — |
| 2026-09-17 | `AMX` | 108 | $22.98 | $23.09 | +11.88 | — | +0.00 | +11.88 | -9.72 | — |
| 2026-09-17 | `SMTC` | 11 | — | $170.85 | +0.00 | $178.19 | +80.74 | +80.74 | +0.00 | +80.74 |
| 2026-09-17 | `AVTR` | 125 | — | $15.81 | +0.00 | $15.86 | +6.25 | +6.25 | +0.00 | +6.25 |
| 2026-09-17 | `GME` | 89 | — | $22.12 | +0.00 | $22.77 | +57.85 | +57.85 | +0.00 | +57.85 |
| 2026-09-17 | `JBHT` | 8 | — | $238.60 | +0.00 | $236.80 | -14.40 | -14.40 | +0.00 | -14.40 |
| 2026-09-17 | `SRRK` | 40 | — | $49.52 | +0.00 | $49.02 | -20.00 | -20.00 | +0.00 | -20.00 |
| 2026-09-18 | `SMTC` | 11 | $178.19 | $182.33 | +45.54 | — | +0.00 | +45.54 | +126.28 | — |
| 2026-09-18 | `AVTR` | 125 | $15.86 | $15.87 | +1.25 | — | +0.00 | +1.25 | +7.50 | — |
| 2026-09-18 | `GME` | 89 | $22.77 | $22.90 | +11.57 | $22.64 | -23.14 | -11.57 | +69.42 | +46.28 |
| 2026-09-18 | `JBHT` | 8 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -14.40 | — |
| 2026-09-18 | `SRRK` | 40 | $49.02 | $48.02 | -40.00 | — | +0.00 | -40.00 | -60.00 | — |
| 2026-09-18 | `TH` | 127 | — | $20.91 | +0.00 | $21.19 | +35.56 | +35.56 | +0.00 | +35.56 |
| 2026-09-18 | `RARE` | 179 | — | $14.79 | +0.00 | $14.51 | -50.12 | -50.12 | +0.00 | -50.12 |
| 2026-09-18 | `BHVN` | 189 | — | $14.07 | +0.00 | $13.62 | -85.05 | -85.05 | +0.00 | -85.05 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +140.32 | HLIT, SNDK, ANGX, ARX, MH, VELO | — | $21.33 | $10,124.06 | HLIT×126, SNDK×1, ANGX×386, ARX×85, MH×123, VELO×108 |
| 2026-08-17 | +2.25 | $21.33 | HLIT×126, SNDK×1, ANGX×386, ARX×85, MH×123, VELO×108 | $10,257.05 | +132.99 | +59.58 | DVN, EOG, FANG, GLOB, TPG | HLIT, SNDK, ANGX, ARX, MH, VELO | $126.07 | $10,289.71 | DVN×44, EOG×14, FANG×10, GLOB×55, TPG×38 |
| 2026-08-18 | -6.20 | $126.07 | DVN×44, EOG×14, FANG×10, GLOB×55, TPG×38 | $10,401.09 | +111.38 | +0.00 | — | DVN, EOG, FANG, GLOB, TPG | $10,390.52 | $10,390.52 | — |
| 2026-08-19 | -7.20 | $10,390.52 | — | $10,390.52 | +0.00 | +0.00 | — | — | $10,390.52 | $10,390.52 | — |
| 2026-08-20 | +1.12 | $10,390.52 | — | $10,390.52 | +0.00 | -157.47 | BHP, APA, AUTL, CRSP, MRK, ASST, MRNA, ZLAB | — | $229.48 | $10,211.72 | BHP×14, APA×29, AUTL×525, CRSP×22, MRK×8, ASST×81, MRNA×8, ZLAB×48 |
| 2026-08-21 | +3.25 | $229.48 | BHP×14, APA×29, AUTL×525, CRSP×22, MRK×8, ASST×81, MRNA×8, ZLAB×48 | $10,419.53 | +207.81 | +129.25 | AU, FUTU, GRAL, VIRT, MFC, ABTC | BHP, APA, MRK, ASST, MRNA, ZLAB | $213.03 | $10,523.49 | AUTL×525, CRSP×22, AU×10, FUTU×11, GRAL×16, VIRT×21, MFC×30, ABTC×150 |
| 2026-08-24 | -5.17 | $213.03 | AUTL×525, CRSP×22, AU×10, FUTU×11, GRAL×16, VIRT×21, MFC×30, ABTC×150 | $10,483.65 | -39.84 | -36.85 | — | AUTL, AU, FUTU, GRAL, VIRT, MFC, ABTC | $9,171.49 | $10,427.14 | CRSP×22 |
| 2026-08-25 | +1.80 | $9,171.49 | CRSP×22 | $10,445.95 | +18.81 | +206.75 | AU, FCX, EZPW, AMX | CRSP | $94.57 | $10,641.95 | AU×22, FCX×33, EZPW×74, AMX×109 |
| 2026-08-26 | +2.02 | $94.57 | AU×22, FCX×33, EZPW×74, AMX×109 | $10,578.94 | -63.01 | +140.49 | FNV, ASST | AU, FCX, EZPW | $262.62 | $10,708.38 | AMX×109, FNV×14, ASST×192 |
| 2026-08-27 | — | $262.62 | AMX×109, FNV×14, ASST×192 | $10,905.17 | +196.79 | -97.96 | ACMR, MU, CM, GEN, LRCX, NVDA, AXTI | AMX, FNV, ASST | $2,013.27 | $10,785.92 | ACMR×16, MU×1, CM×11, GEN×45, LRCX×4, NVDA×6, AXTI×19 |
| 2026-08-28 | +0.75 | $2,013.27 | ACMR×16, MU×1, CM×11, GEN×45, LRCX×4, NVDA×6, AXTI×19 | $10,722.43 | -63.49 | -407.40 | KEYS, SMTC, CIEN, MPWR, DDOG, PLAB, ADSK, SEDG | ACMR, MU, CM, GEN, LRCX, NVDA, AXTI | $467.68 | $10,284.39 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, PLAB×44, ADSK×5, SEDG×40 |
| 2026-08-31 | -5.85 | $467.68 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, PLAB×44, ADSK×5, SEDG×40 | $10,283.70 | -0.69 | +0.00 | — | KEYS, SMTC, CIEN, MPWR, DDOG, PLAB, ADSK, SEDG | $10,267.29 | $10,267.29 | — |
| 2026-09-01 | -6.30 | $10,267.29 | — | $10,267.29 | -0.00 | +0.00 | — | — | $10,267.29 | $10,267.29 | — |
| 2026-09-02 | -3.83 | $10,267.29 | — | $10,267.29 | -0.00 | +0.00 | — | — | $10,267.29 | $10,267.29 | — |
| 2026-09-03 | -0.90 | $10,267.29 | — | $10,267.29 | -0.00 | +452.51 | AVGO, DELL, CXW, FRNM, MMED, DE, HPE | — | $181.59 | $10,705.16 | AVGO×4, DELL×3, CXW×45, FRNM×92, MMED×61, DE×2, HPE×30 |
| 2026-09-04 | +2.25 | $181.59 | AVGO×4, DELL×3, CXW×45, FRNM×92, MMED×61, DE×2, HPE×30 | $10,630.03 | -75.13 | -27.01 | CRM, MRX, BE, AMX, BAK | AVGO, DELL, CXW, DE | $248.58 | $10,578.58 | FRNM×92, MMED×61, HPE×30, CRM×4, MRX×15, BE×5, AMX×52, BAK×623 |
| 2026-09-08 | -11.47 | $248.58 | FRNM×92, MMED×61, HPE×30, CRM×4, MRX×15, BE×5, AMX×52, BAK×623 | $10,718.82 | +140.24 | +0.00 | — | FRNM, MMED, HPE, CRM, MRX, BE, AMX, BAK | $10,695.81 | $10,695.81 | — |
| 2026-09-09 | -13.95 | $10,695.81 | — | $10,695.81 | +0.00 | +0.00 | — | — | $10,695.81 | $10,695.81 | — |
| 2026-09-10 | -13.28 | $10,695.81 | — | $10,695.81 | +0.00 | +0.00 | — | — | $10,695.81 | $10,695.81 | — |
| 2026-09-11 | +0.50 | $10,695.81 | — | $10,695.81 | +0.00 | -527.85 | ORCL, BTI | — | $106.84 | $10,163.60 | ORCL×32, BTI×95 |
| 2026-09-14 | -11.00 | $106.84 | ORCL×32, BTI×95 | $10,058.68 | -104.92 | +0.00 | — | ORCL, BTI | $10,054.21 | $10,054.21 | — |
| 2026-09-15 | -3.84 | $10,054.21 | — | $10,054.21 | +0.00 | +0.00 | — | — | $10,054.21 | $10,054.21 | — |
| 2026-09-16 | +5.30 | $10,054.21 | — | $10,054.21 | +0.00 | -160.95 | WAY, QCOM, SM, AMX | — | $107.74 | $9,884.47 | WAY×95, QCOM×13, SM×62, AMX×108 |
| 2026-09-17 | +7.38 | $107.74 | WAY×95, QCOM×13, SM×62, AMX×108 | $9,923.80 | +39.33 | +110.44 | SMTC, AVTR, GME, JBHT, SRRK | WAY, QCOM, SM, AMX | $190.23 | $10,014.55 | SMTC×11, AVTR×125, GME×89, JBHT×8, SRRK×40 |
| 2026-09-18 | +4.86 | $190.23 | SMTC×11, AVTR×125, GME×89, JBHT×8, SRRK×40 | $10,032.91 | +18.36 | -122.75 | TH, RARE, BHVN | SMTC, AVTR, JBHT, SRRK | $16.52 | $9,894.08 | GME×89, TH×127, RARE×179, BHVN×189 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 126 | $13.18 | $2.37 | — | $8,336.95 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 1 | $1646.93 | $1.99 | — | $6,688.03 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 386 | $4.31 | $4.98 | — | $5,019.39 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 85 | $19.57 | $2.25 | — | $3,353.69 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+58.7; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 123 | $13.55 | $2.36 | — | $1,684.69 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 108 | $15.38 | $2.31 | — | $21.33 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.33 | ▲ close $10,124.06 vs 09:30 $10,000.00 (session +140.32) | 16:00 close · cash $21.33 · equity $10,124.06 vs 09:30 $10,000.00 (+124.06; session marks +140.32) · 6 name(s) marked open→close (per-name table). HLIT×126 09:30 $13.18 → close $13.92 +93.24; SNDK×1 09:30 $1646.93 → close $1641.11 -5.82; ANGX×386 09:30 $4.31 → close $4.37 +23.16; ARX×85 09:30 $19.57 → close $19.58 +0.85; MH×123 09:30 $13.55 → close $13.10 -55.35; VELO×108 09:30 $15.38 → close $16.16 +84.24 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.33 | ▲ 09:30 equity $10,257.05 vs yday $10,124.06 (+132.99) | 09:30 open · cash $21.33 (unchanged overnight, no fees) · equity $10,257.05 vs prior close $10,124.06 (+132.99) · 6 name(s) re-marked at the open (per-name table). HLIT×126 yday $13.92 → 09:30 $13.84 -10.08; SNDK×1 yday $1641.11 → 09:30 $1700.74 +59.63; ANGX×386 yday $4.37 → 09:30 $4.60 +88.78; ARX×85 yday $19.58 → 09:30 $19.57 -0.85; MH×123 yday $13.10 → 09:30 $13.16 +7.38; VELO×108 yday $16.16 → 09:30 $16.05 -11.88 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 126 | $13.84 | $2.40 | $+78.39 | $1,762.77 | ▲ +78.39 after sell → book $10,254.64; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SNDK` | 1 | $1700.74 | $2.02 | $+49.81 | $3,461.50 | ▲ +49.81 after sell → book $10,252.63; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 386 | $4.60 | $5.06 | $+101.90 | $5,232.04 | ▲ +101.90 after sell → book $10,247.57; vs 09:30 mark -5.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 85 | $19.57 | $2.27 | $-4.52 | $6,893.22 | ▼ -4.52 after sell → book $10,245.30; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 123 | $13.16 | $2.39 | $-52.72 | $8,509.50 | ▼ -52.72 after sell → book $10,242.90; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VELO` | 108 | $16.05 | $2.35 | $+67.70 | $10,240.56 | ▲ +67.70 after sell → book $10,240.56; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 44 | $46.18 | $2.12 | — | $8,206.52 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+6.7; leftover $2048.11 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 14 | $142.77 | $2.03 | — | $6,205.70 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+5.8; leftover $2048.11 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 10 | $202.70 | $2.02 | — | $4,176.68 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+8.3; leftover $2048.11 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `GLOB` | 55 | $37.18 | $2.15 | — | $2,129.63 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; ⚪; ret5=-0.1; leftover $2048.11 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TPG` | 38 | $52.67 | $2.10 | — | $126.07 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ⚪; ret5=+9.4; leftover $2048.11 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.07 | ▲ close $10,289.71 vs 09:30 $10,257.05 (session +59.58) | 16:00 close · cash $126.07 · equity $10,289.71 vs 09:30 $10,257.05 (+32.66; session marks +59.58) · 5 name(s) marked open→close (per-name table). DVN×44 09:30 $46.18 → close $47.57 +61.16; EOG×14 09:30 $142.77 → close $146.15 +47.32; FANG×10 09:30 $202.70 → close $206.29 +35.90; GLOB×55 09:30 $37.18 → close $36.26 -50.60; TPG×38 09:30 $52.67 → close $51.77 -34.20 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.07 | ▲ 09:30 equity $10,401.09 vs yday $10,289.71 (+111.38) | 09:30 open · cash $126.07 (unchanged overnight, no fees) · equity $10,401.09 vs prior close $10,289.71 (+111.38) · 5 name(s) re-marked at the open (per-name table). DVN×44 yday $47.57 → 09:30 $48.00 +18.92; EOG×14 yday $146.15 → 09:30 $148.04 +26.46; FANG×10 yday $206.29 → 09:30 $208.93 +26.40; GLOB×55 yday $36.26 → 09:30 $36.98 +39.60; TPG×38 yday $51.77 → 09:30 $51.77 +0.00 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 44 | $48.00 | $2.15 | $+75.81 | $2,235.92 | ▲ +75.81 after sell → book $10,398.94; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 14 | $148.04 | $2.06 | $+69.69 | $4,306.42 | ▲ +69.69 after sell → book $10,396.88; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 10 | $208.93 | $2.05 | $+58.23 | $6,393.67 | ▲ +58.23 after sell → book $10,394.83; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `GLOB` | 55 | $36.98 | $2.18 | $-15.34 | $8,425.39 | ▼ -15.34 after sell → book $10,392.65; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 38 | $51.77 | $2.13 | $-38.43 | $10,390.52 | ▼ -38.43 after sell → book $10,390.52; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,390.52 | ▲ close $10,390.52 vs 09:30 $10,401.09 (session +0.00) | 16:00 close · cash $10,390.52 · no lots left · equity $10,390.52. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,390.52 | ▲ 09:30 equity $10,390.52 vs yday $10,390.52 (+0.00) | 09:30 open · cash $10,390.52 · no holdings · equity $10,390.52 vs prior close $10,390.52 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,390.52 | ▲ close $10,390.52 vs 09:30 $10,390.52 (session +0.00) | 16:00 close · cash $10,390.52 · no lots left · equity $10,390.52. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,390.52 | ▲ 09:30 equity $10,390.52 vs yday $10,390.52 (+0.00) | 09:30 open · cash $10,390.52 · no holdings · equity $10,390.52 vs prior close $10,390.52 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $9,114.35 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1298.82 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 29 | $44.76 | $2.08 | — | $7,814.23 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1298.82 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 525 | $2.47 | $6.77 | — | $6,510.71 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1298.82 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 22 | $58.73 | $2.06 | — | $5,216.59 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1298.82 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRK` | 8 | $150.78 | $2.01 | — | $4,008.34 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ⚪; ret5=+14.5; leftover $1298.82 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 81 | $16.00 | $2.23 | — | $2,710.11 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1298.82 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $1,506.97 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ret5=+173.9; leftover $1298.82 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 48 | $26.57 | $2.13 | — | $229.48 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1298.82 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $229.48 | ▼ close $10,211.72 vs 09:30 $10,390.52 (session -157.47) | 16:00 close · cash $229.48 · equity $10,211.72 vs 09:30 $10,390.52 (-178.80; session marks -157.47) · 8 name(s) marked open→close (per-name table). BHP×14 09:30 $91.01 → close $93.63 +36.68; APA×29 09:30 $44.76 → close $44.39 -10.73; AUTL×525 09:30 $2.47 → close $2.46 -5.25; CRSP×22 09:30 $58.73 → close $58.12 -13.42; MRK×8 09:30 $150.78 → close $148.99 -14.32; ASST×81 09:30 $16.00 → close $16.13 +10.53; MRNA×8 09:30 $150.14 → close $133.32 -134.56; ZLAB×48 09:30 $26.57 → close $26.02 -26.40 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $229.48 | ▲ 09:30 equity $10,419.53 vs yday $10,211.72 (+207.81) | 09:30 open · cash $229.48 (unchanged overnight, no fees) · equity $10,419.53 vs prior close $10,211.72 (+207.81) · 8 name(s) re-marked at the open (per-name table). BHP×14 yday $93.63 → 09:30 $95.72 +29.26; APA×29 yday $44.39 → 09:30 $44.52 +3.77; AUTL×525 yday $2.46 → 09:30 $2.47 +5.25; CRSP×22 yday $58.12 → 09:30 $59.72 +35.20; MRK×8 yday $148.99 → 09:30 $149.12 +1.04; ASST×81 yday $16.13 → 09:30 $17.66 +123.93; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; ZLAB×48 yday $26.02 → 09:30 $26.25 +11.04 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 14 | $95.72 | $2.05 | $+61.86 | $1,567.51 | ▲ +61.86 after sell → book $10,417.48; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 29 | $44.52 | $2.10 | $-11.13 | $2,856.49 | ▼ -11.13 after sell → book $10,415.38; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRK` | 8 | $149.12 | $2.03 | $-17.33 | $4,047.41 | ▼ -17.33 after sell → book $10,413.34; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 81 | $17.66 | $2.26 | $+129.97 | $5,475.62 | ▲ +129.97 after sell → book $10,411.09; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $6,538.46 | ▼ -140.29 after sell → book $10,409.05; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 48 | $26.25 | $2.15 | $-19.65 | $7,796.31 | ▼ -19.65 after sell → book $10,406.90; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,599.99 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1299.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $5,330.99 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1299.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 16 | $78.88 | $2.04 | — | $4,066.87 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1299.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `VIRT` | 21 | $60.66 | $2.05 | — | $2,790.95 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ⚪; ret5=+7.0; leftover $1299.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MFC` | 30 | $42.48 | $2.08 | — | $1,514.47 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ⚪; ret5=-3.3; leftover $1299.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 150 | $8.66 | $2.44 | — | $213.03 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1299.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $213.03 | ▲ close $10,523.49 vs 09:30 $10,419.53 (session +129.25) | 16:00 close · cash $213.03 · equity $10,523.49 vs 09:30 $10,419.53 (+103.96; session marks +129.25) · 8 name(s) marked open→close (per-name table). AUTL×525 09:30 $2.47 → close $2.41 -31.50; CRSP×22 09:30 $59.72 → close $59.50 -4.84; AU×10 09:30 $119.43 → close $121.22 +17.90; FUTU×11 09:30 $115.18 → close $123.64 +93.06; GRAL×16 09:30 $78.88 → close $79.54 +10.56; VIRT×21 09:30 $60.66 → close $67.93 +152.67; MFC×30 09:30 $42.48 → close $42.51 +0.90; ABTC×150 09:30 $8.66 → close $7.93 -109.50 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $213.03 | ▼ 09:30 equity $10,483.65 vs yday $10,523.49 (-39.84) | 09:30 open · cash $213.03 (unchanged overnight, no fees) · equity $10,483.65 vs prior close $10,523.49 (-39.84) · 8 name(s) re-marked at the open (per-name table). AUTL×525 yday $2.41 → 09:30 $2.40 -5.25; CRSP×22 yday $59.50 → 09:30 $58.75 -16.50; AU×10 yday $121.22 → 09:30 $120.51 -7.10; FUTU×11 yday $123.64 → 09:30 $121.00 -29.04; GRAL×16 yday $79.54 → 09:30 $81.87 +37.28; VIRT×21 yday $67.93 → 09:30 $66.80 -23.73; MFC×30 yday $42.51 → 09:30 $42.31 -6.00; ABTC×150 yday $7.93 → 09:30 $8.00 +10.50 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 525 | $2.40 | $6.87 | $-50.39 | $1,466.16 | ▼ -50.39 after sell → book $10,476.78; vs 09:30 mark -6.87 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $2,669.22 | ▲ +6.74 after sell → book $10,474.74; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $121.00 | $2.04 | $+59.95 | $3,998.18 | ▲ +59.95 after sell → book $10,472.70; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 16 | $81.87 | $2.06 | $+43.74 | $5,306.04 | ▲ +43.74 after sell → book $10,470.64; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `VIRT` | 21 | $66.80 | $2.07 | $+124.81 | $6,706.77 | ▲ +124.81 after sell → book $10,468.57; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `MFC` | 30 | $42.31 | $2.10 | $-9.28 | $7,973.97 | ▼ -9.28 after sell → book $10,466.47; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 150 | $8.00 | $2.47 | $-103.91 | $9,171.49 | ▼ -103.91 after sell → book $10,463.99; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,171.49 | ▼ close $10,427.14 vs 09:30 $10,483.65 (session -36.85) | 16:00 close · cash $9,171.49 · equity $10,427.14 vs 09:30 $10,483.65 (-56.51; session marks -36.85) · 1 name(s) marked open→close (per-name table). CRSP×22 09:30 $58.75 → close $57.08 -36.85 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,171.49 | ▲ 09:30 equity $10,445.95 vs yday $10,427.14 (+18.81) | 09:30 open · cash $9,171.49 (unchanged overnight, no fees) · equity $10,445.95 vs prior close $10,427.14 (+18.81) · 1 name(s) re-marked at the open (per-name table). CRSP×22 yday $57.08 → 09:30 $57.93 +18.81 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 22 | $57.93 | $2.08 | $-21.73 | $10,443.88 | ▼ -21.73 after sell → book $10,443.88; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 22 | $118.52 | $2.06 | — | $7,834.38 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $2610.97 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 33 | $77.13 | $2.09 | — | $5,287.00 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $2610.97 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 74 | $35.05 | $2.21 | — | $2,691.09 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,oppset; 🔵; ⚪; ret5=+19.7; leftover $2610.97 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMX` | 109 | $23.80 | $2.32 | — | $94.57 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ret5=+0.5; leftover $2610.97 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.57 | ▲ close $10,641.95 vs 09:30 $10,445.95 (session +206.75) | 16:00 close · cash $94.57 · equity $10,641.95 vs 09:30 $10,445.95 (+196.00; session marks +206.75) · 4 name(s) marked open→close (per-name table). AU×22 09:30 $118.52 → close $123.39 +107.14; FCX×33 09:30 $77.13 → close $79.91 +91.74; EZPW×74 09:30 $35.05 → close $35.23 +13.32; AMX×109 09:30 $23.80 → close $23.75 -5.45 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.57 | ▼ 09:30 equity $10,578.94 vs yday $10,641.95 (-63.01) | 09:30 open · cash $94.57 (unchanged overnight, no fees) · equity $10,578.94 vs prior close $10,641.95 (-63.01) · 4 name(s) re-marked at the open (per-name table). AU×22 yday $123.39 → 09:30 $119.80 -78.98; FCX×33 yday $79.91 → 09:30 $79.34 -18.81; EZPW×74 yday $35.23 → 09:30 $35.70 +34.78; AMX×109 yday $23.75 → 09:30 $23.75 +0.00 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 22 | $119.80 | $2.09 | $+24.02 | $2,728.09 | ▲ +24.02 after sell → book $10,576.86; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 33 | $79.34 | $2.12 | $+68.72 | $5,344.19 | ▲ +68.72 after sell → book $10,574.74; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 74 | $35.70 | $2.25 | $+43.64 | $7,983.74 | ▲ +43.64 after sell → book $10,572.49; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 14 | $267.02 | $2.03 | — | $4,243.43 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $3991.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ASST` | 192 | $20.72 | $2.57 | — | $262.62 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ret5=+67.1; leftover $3991.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $262.62 | ▲ close $10,708.38 vs 09:30 $10,578.94 (session +140.49) | 16:00 close · cash $262.62 · equity $10,708.38 vs 09:30 $10,578.94 (+129.44; session marks +140.49) · 3 name(s) marked open→close (per-name table). AMX×109 09:30 $23.75 → close $23.62 -14.17; FNV×14 09:30 $267.02 → close $267.37 +4.90; ASST×192 09:30 $20.72 → close $21.50 +149.76 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $262.62 | ▲ 09:30 equity $10,905.17 vs yday $10,708.38 (+196.79) | 09:30 open · cash $262.62 (unchanged overnight, no fees) · equity $10,905.17 vs prior close $10,708.38 (+196.79) · 3 name(s) re-marked at the open (per-name table). AMX×109 yday $23.62 → 09:30 $23.77 +16.35; FNV×14 yday $267.37 → 09:30 $267.23 -1.96; ASST×192 yday $21.50 → 09:30 $22.45 +182.40 | — |
| 2026-08-27 09:30 ET | **SELL** | `AMX` | 109 | $23.77 | $2.36 | $-7.94 | $2,851.20 | ▼ -7.94 after sell → book $10,902.82; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 14 | $267.23 | $2.07 | $-1.16 | $6,590.34 | ▼ -1.16 after sell → book $10,900.74; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 192 | $22.45 | $2.63 | $+326.96 | $10,898.11 | ▲ +326.96 after sell → book $10,898.11; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 16 | $81.65 | $2.04 | — | $9,589.67 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1362.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $8,620.67 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1362.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 11 | $118.77 | $2.02 | — | $7,312.18 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; ret5=+0.3; leftover $1362.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 45 | $29.83 | $2.12 | — | $5,967.70 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+7.6; leftover $1362.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 4 | $318.88 | $2.00 | — | $4,690.18 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1362.26 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 6 | $222.86 | $2.01 | — | $3,351.01 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $1362.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 19 | $70.30 | $2.05 | — | $2,013.27 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=-11.2; leftover $1362.26 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,013.27 | ▼ close $10,785.92 vs 09:30 $10,905.17 (session -97.96) | 16:00 close · cash $2,013.27 · equity $10,785.92 vs 09:30 $10,905.17 (-119.25; session marks -97.96) · 7 name(s) marked open→close (per-name table). ACMR×16 09:30 $81.65 → close $80.49 -18.56; MU×1 09:30 $967.01 → close $935.39 -31.62; CM×11 09:30 $118.77 → close $114.84 -43.23; GEN×45 09:30 $29.83 → close $30.50 +30.15; LRCX×4 09:30 $318.88 → close $318.58 -1.20; NVDA×6 09:30 $222.86 → close $227.98 +30.72; AXTI×19 09:30 $70.30 → close $66.92 -64.22 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,013.27 | ▼ 09:30 equity $10,722.43 vs yday $10,785.92 (-63.49) | 09:30 open · cash $2,013.27 (unchanged overnight, no fees) · equity $10,722.43 vs prior close $10,785.92 (-63.49) · 7 name(s) re-marked at the open (per-name table). ACMR×16 yday $80.49 → 09:30 $79.27 -19.52; MU×1 yday $935.39 → 09:30 $919.29 -16.10; CM×11 yday $114.84 → 09:30 $115.66 +9.02; GEN×45 yday $30.50 → 09:30 $30.50 +0.00; LRCX×4 yday $318.58 → 09:30 $318.03 -2.20; NVDA×6 yday $227.98 → 09:30 $227.36 -3.72; AXTI×19 yday $66.92 → 09:30 $65.29 -30.97 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 16 | $79.27 | $2.06 | $-42.18 | $3,279.53 | ▼ -42.18 after sell → book $10,720.37; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $4,196.81 | ▼ -51.73 after sell → book $10,718.36; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 11 | $115.66 | $2.04 | $-38.28 | $5,467.02 | ▼ -38.28 after sell → book $10,716.31; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 45 | $30.50 | $2.15 | $+25.88 | $6,837.38 | ▲ +25.88 after sell → book $10,714.17; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 4 | $318.03 | $2.02 | $-7.42 | $8,107.47 | ▼ -7.42 after sell → book $10,712.14; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 6 | $227.36 | $2.03 | $+22.96 | $9,469.60 | ▲ +22.96 after sell → book $10,710.11; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 19 | $65.29 | $2.07 | $-99.30 | $10,708.05 | ▼ -99.30 after sell → book $10,708.05; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $9,408.41 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1338.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $8,130.55 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $1338.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,927.29 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1338.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $5,619.27 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1338.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $4,416.16 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1338.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PLAB` | 44 | $30.01 | $2.12 | — | $3,093.60 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ret5=-0.9; leftover $1338.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $1,785.79 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; ret5=+7.8; leftover $1338.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 40 | $32.90 | $2.11 | — | $467.68 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1338.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $467.68 | ▼ close $10,284.39 vs 09:30 $10,722.43 (session -407.40) | 16:00 close · cash $467.68 · equity $10,284.39 vs 09:30 $10,722.43 (-438.04; session marks -407.40) · 8 name(s) marked open→close (per-name table). KEYS×4 09:30 $324.41 → close $319.97 -17.76; SMTC×9 09:30 $141.76 → close $131.17 -95.31; CIEN×3 09:30 $400.42 → close $378.44 -65.94; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×5 09:30 $240.22 → close $236.98 -16.20; PLAB×44 09:30 $30.01 → close $27.73 -100.32; ADSK×5 09:30 $261.16 → close $260.66 -2.50; SEDG×40 09:30 $32.90 → close $31.41 -59.60 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $467.68 | ▼ 09:30 equity $10,283.70 vs yday $10,284.39 (-0.69) | 09:30 open · cash $467.68 (unchanged overnight, no fees) · equity $10,283.70 vs prior close $10,284.39 (-0.69) · 8 name(s) re-marked at the open (per-name table). KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×5 yday $236.98 → 09:30 $233.97 -15.07; PLAB×44 yday $27.73 → 09:30 $28.04 +13.64; ADSK×5 yday $260.66 → 09:30 $257.71 -14.75; SEDG×40 yday $31.41 → 09:30 $31.15 -10.40 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $1,755.62 | ▼ -11.70 after sell → book $10,281.68; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $2,944.29 | ▼ -89.19 after sell → book $10,279.64; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $4,077.59 | ▼ -69.96 after sell → book $10,277.62; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $5,337.47 | ▼ -48.14 after sell → book $10,275.61; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 5 | $233.97 | $2.02 | $-35.31 | $6,505.27 | ▼ -35.31 after sell → book $10,273.58; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PLAB` | 44 | $28.04 | $2.14 | $-90.94 | $7,736.89 | ▼ -90.94 after sell → book $10,271.44; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 5 | $257.71 | $2.03 | $-21.28 | $9,023.42 | ▼ -21.28 after sell → book $10,269.42; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 40 | $31.15 | $2.13 | $-74.24 | $10,267.29 | ▼ -74.24 after sell → book $10,267.29; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,267.29 | ▲ close $10,267.29 vs 09:30 $10,283.70 (session +0.00) | 16:00 close · cash $10,267.29 · no lots left · equity $10,267.29. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,267.29 | ▲ 09:30 equity $10,267.29 vs yday $10,267.29 (-0.00) | 09:30 open · cash $10,267.29 · no holdings · equity $10,267.29 vs prior close $10,267.29 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,267.29 | ▲ close $10,267.29 vs 09:30 $10,267.29 (session +0.00) | 16:00 close · cash $10,267.29 · no lots left · equity $10,267.29. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,267.29 | ▲ 09:30 equity $10,267.29 vs yday $10,267.29 (-0.00) | 09:30 open · cash $10,267.29 · no holdings · equity $10,267.29 vs prior close $10,267.29 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,267.29 | ▲ close $10,267.29 vs 09:30 $10,267.29 (session +0.00) | 16:00 close · cash $10,267.29 · no lots left · equity $10,267.29. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,267.29 | ▲ 09:30 equity $10,267.29 vs yday $10,267.29 (-0.00) | 09:30 open · cash $10,267.29 · no holdings · equity $10,267.29 vs prior close $10,267.29 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 4 | $351.74 | $2.00 | — | $8,858.32 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1466.76 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $7,397.40 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $1466.76 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 45 | $32.31 | $2.12 | — | $5,941.32 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1466.76 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 92 | $15.87 | $2.27 | — | $4,479.01 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1466.76 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 61 | $23.88 | $2.17 | — | $3,020.16 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+21.9; leftover $1466.76 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $1,611.67 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1466.76 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 30 | $47.60 | $2.08 | — | $181.59 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react,oppset; 🔵; ret5=-6.2; leftover $1466.76 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $181.59 | ▲ close $10,705.16 vs 09:30 $10,267.29 (session +452.51) | 16:00 close · cash $181.59 · equity $10,705.16 vs 09:30 $10,267.29 (+437.87; session marks +452.51) · 7 name(s) marked open→close (per-name table). AVGO×4 09:30 $351.74 → close $357.16 +21.68; DELL×3 09:30 $486.31 → close $516.39 +90.24; CXW×45 09:30 $32.31 → close $33.66 +60.75; FRNM×92 09:30 $15.87 → close $16.90 +94.76; MMED×61 09:30 $23.88 → close $23.84 -2.44; DE×2 09:30 $703.25 → close $694.41 -17.68; HPE×30 09:30 $47.60 → close $54.44 +205.20 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $181.59 | ▼ 09:30 equity $10,630.03 vs yday $10,705.16 (-75.13) | 09:30 open · cash $181.59 (unchanged overnight, no fees) · equity $10,630.03 vs prior close $10,705.16 (-75.13) · 7 name(s) re-marked at the open (per-name table). AVGO×4 yday $357.16 → 09:30 $359.70 +10.16; DELL×3 yday $516.39 → 09:30 $513.78 -7.83; CXW×45 yday $33.66 → 09:30 $33.46 -9.00; FRNM×92 yday $16.90 → 09:30 $16.40 -46.00; MMED×61 yday $23.84 → 09:30 $23.84 +0.00; DE×2 yday $694.41 → 09:30 $692.03 -4.76; HPE×30 yday $54.44 → 09:30 $53.85 -17.70 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 4 | $359.70 | $2.02 | $+27.81 | $1,618.36 | ▲ +27.81 after sell → book $10,628.00; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 3 | $513.78 | $2.02 | $+78.39 | $3,157.68 | ▲ +78.39 after sell → book $10,625.98; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 45 | $33.46 | $2.15 | $+47.48 | $4,661.23 | ▲ +47.48 after sell → book $10,623.83; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 2 | $692.03 | $2.02 | $-26.45 | $6,043.28 | ▼ -26.45 after sell → book $10,621.82; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $4,987.83 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1208.66 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 15 | $75.65 | $2.04 | — | $3,851.05 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1208.66 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 5 | $236.82 | $2.00 | — | $2,664.94 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+8.1; leftover $1208.66 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AMX` | 52 | $23.03 | $2.15 | — | $1,465.24 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ret5=-1.4; leftover $1208.66 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 623 | $1.94 | $8.04 | — | $248.58 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $1208.66 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $248.58 | ▼ close $10,578.58 vs 09:30 $10,630.03 (session -27.01) | 16:00 close · cash $248.58 · equity $10,578.58 vs 09:30 $10,630.03 (-51.45; session marks -27.01) · 8 name(s) marked open→close (per-name table). FRNM×92 09:30 $16.40 → close $16.31 -8.28; MMED×61 09:30 $23.84 → close $23.29 -33.55; HPE×30 09:30 $53.85 → close $52.00 -55.50; CRM×4 09:30 $263.36 → close $259.23 -16.52; MRX×15 09:30 $75.65 → close $78.27 +39.30; BE×5 09:30 $236.82 → close $252.87 +80.25; AMX×52 09:30 $23.03 → close $23.00 -1.56; BAK×623 09:30 $1.94 → close $1.89 -31.15 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $248.58 | ▲ 09:30 equity $10,718.82 vs yday $10,578.58 (+140.24) | 09:30 open · cash $248.58 (unchanged overnight, no fees) · equity $10,718.82 vs prior close $10,578.58 (+140.24) · 8 name(s) re-marked at the open (per-name table). FRNM×92 yday $16.31 → 09:30 $16.74 +39.56; MMED×61 yday $23.29 → 09:30 $23.16 -7.93; HPE×30 yday $52.00 → 09:30 $52.29 +8.70; CRM×4 yday $259.23 → 09:30 $253.72 -22.04; MRX×15 yday $78.27 → 09:30 $78.84 +8.55; BE×5 yday $252.87 → 09:30 $267.76 +74.45; AMX×52 yday $23.00 → 09:30 $23.15 +7.80; BAK×623 yday $1.89 → 09:30 $1.94 +31.15 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 92 | $16.74 | $2.29 | $+75.48 | $1,786.37 | ▲ +75.48 after sell → book $10,716.53; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MMED` | 61 | $23.16 | $2.19 | $-48.29 | $3,196.93 | ▼ -48.29 after sell → book $10,714.33; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HPE` | 30 | $52.29 | $2.10 | $+136.52 | $4,763.53 | ▲ +136.52 after sell → book $10,712.23; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $5,776.39 | ▼ -42.58 after sell → book $10,710.21; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 15 | $78.84 | $2.06 | $+43.76 | $6,956.93 | ▲ +43.76 after sell → book $10,708.15; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 5 | $267.76 | $2.03 | $+150.67 | $8,293.71 | ▲ +150.67 after sell → book $10,706.13; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AMX` | 52 | $23.15 | $2.17 | $+1.93 | $9,495.34 | ▲ +1.93 after sell → book $10,703.96; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 623 | $1.94 | $8.15 | $-16.19 | $10,695.81 | ▼ -16.19 after sell → book $10,695.81; vs 09:30 mark -8.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,695.81 | ▲ close $10,695.81 vs 09:30 $10,718.82 (session +0.00) | 16:00 close · cash $10,695.81 · no lots left · equity $10,695.81. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,695.81 | ▲ 09:30 equity $10,695.81 vs yday $10,695.81 (+0.00) | 09:30 open · cash $10,695.81 · no holdings · equity $10,695.81 vs prior close $10,695.81 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,695.81 | ▲ close $10,695.81 vs 09:30 $10,695.81 (session +0.00) | 16:00 close · cash $10,695.81 · no lots left · equity $10,695.81. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,695.81 | ▲ 09:30 equity $10,695.81 vs yday $10,695.81 (+0.00) | 09:30 open · cash $10,695.81 · no holdings · equity $10,695.81 vs prior close $10,695.81 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,695.81 | ▲ close $10,695.81 vs 09:30 $10,695.81 (session +0.00) | 16:00 close · cash $10,695.81 · no lots left · equity $10,695.81. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,695.81 | ▲ 09:30 equity $10,695.81 vs yday $10,695.81 (+0.00) | 09:30 open · cash $10,695.81 · no holdings · equity $10,695.81 vs prior close $10,695.81 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 32 | $164.43 | $2.09 | — | $5,431.97 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $5347.91 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BTI` | 95 | $56.03 | $2.27 | — | $106.84 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ret5=-0.8; leftover $5347.91 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.84 | ▼ close $10,163.60 vs 09:30 $10,695.81 (session -527.85) | 16:00 close · cash $106.84 · equity $10,163.60 vs 09:30 $10,695.81 (-532.21; session marks -527.85) · 2 name(s) marked open→close (per-name table). ORCL×32 09:30 $164.43 → close $150.28 -452.80; BTI×95 09:30 $56.03 → close $55.24 -75.05 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.84 | ▼ 09:30 equity $10,058.68 vs yday $10,163.60 (-104.92) | 09:30 open · cash $106.84 (unchanged overnight, no fees) · equity $10,058.68 vs prior close $10,163.60 (-104.92) · 2 name(s) re-marked at the open (per-name table). ORCL×32 yday $150.28 → 09:30 $141.42 -283.52; BTI×95 yday $55.24 → 09:30 $57.12 +178.60 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 32 | $141.42 | $2.13 | $-740.54 | $4,630.15 | ▼ -740.54 after sell → book $10,056.55; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-14 09:30 ET | **SELL** | `BTI` | 95 | $57.12 | $2.33 | $+98.94 | $10,054.21 | ▲ +98.94 after sell → book $10,054.21; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,054.21 | ▲ close $10,054.21 vs 09:30 $10,058.68 (session +0.00) | 16:00 close · cash $10,054.21 · no lots left · equity $10,054.21. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,054.21 | ▲ 09:30 equity $10,054.21 vs yday $10,054.21 (+0.00) | 09:30 open · cash $10,054.21 · no holdings · equity $10,054.21 vs prior close $10,054.21 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,054.21 | ▲ close $10,054.21 vs 09:30 $10,054.21 (session +0.00) | 16:00 close · cash $10,054.21 · no lots left · equity $10,054.21. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,054.21 | ▲ 09:30 equity $10,054.21 vs yday $10,054.21 (+0.00) | 09:30 open · cash $10,054.21 · no holdings · equity $10,054.21 vs prior close $10,054.21 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 95 | $26.27 | $2.27 | — | $7,556.29 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2513.55 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 13 | $189.17 | $2.03 | — | $5,095.05 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $2513.55 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 62 | $39.99 | $2.18 | — | $2,613.49 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2513.55 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `AMX` | 108 | $23.18 | $2.31 | — | $107.74 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ret5=-0.2; leftover $2513.55 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.74 | ▼ close $9,884.47 vs 09:30 $10,054.21 (session -160.95) | 16:00 close · cash $107.74 · equity $9,884.47 vs 09:30 $10,054.21 (-169.74; session marks -160.95) · 4 name(s) marked open→close (per-name table). WAY×95 09:30 $26.27 → close $26.59 +30.40; QCOM×13 09:30 $189.17 → close $184.84 -56.29; SM×62 09:30 $39.99 → close $38.16 -113.46; AMX×108 09:30 $23.18 → close $22.98 -21.60 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.74 | ▲ 09:30 equity $9,923.80 vs yday $9,884.47 (+39.33) | 09:30 open · cash $107.74 (unchanged overnight, no fees) · equity $9,923.80 vs prior close $9,884.47 (+39.33) · 4 name(s) re-marked at the open (per-name table). WAY×95 yday $26.59 → 09:30 $26.51 -7.60; QCOM×13 yday $184.84 → 09:30 $190.35 +71.63; SM×62 yday $38.16 → 09:30 $37.57 -36.58; AMX×108 yday $22.98 → 09:30 $23.09 +11.88 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 95 | $26.51 | $2.31 | $+18.21 | $2,623.88 | ▲ +18.21 after sell → book $9,921.49; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 13 | $190.35 | $2.06 | $+11.25 | $5,096.37 | ▲ +11.25 after sell → book $9,919.43; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 62 | $37.57 | $2.20 | $-154.42 | $7,423.51 | ▼ -154.42 after sell → book $9,917.23; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AMX` | 108 | $23.09 | $2.35 | $-14.39 | $9,914.87 | ▼ -14.39 after sell → book $9,914.87; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 11 | $170.85 | $2.02 | — | $8,033.50 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1982.97 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 125 | $15.81 | $2.37 | — | $6,054.89 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $1982.97 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 89 | $22.12 | $2.26 | — | $4,083.95 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1982.97 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 8 | $238.60 | $2.01 | — | $2,173.14 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_mover,oppset; ret5=-11.6; leftover $1982.97 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SRRK` | 40 | $49.52 | $2.11 | — | $190.23 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ret5=+1.1; leftover $1982.97 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $190.23 | ▲ close $10,014.55 vs 09:30 $9,923.80 (session +110.44) | 16:00 close · cash $190.23 · equity $10,014.55 vs 09:30 $9,923.80 (+90.75; session marks +110.44) · 5 name(s) marked open→close (per-name table). SMTC×11 09:30 $170.85 → close $178.19 +80.74; AVTR×125 09:30 $15.81 → close $15.86 +6.25; GME×89 09:30 $22.12 → close $22.77 +57.85; JBHT×8 09:30 $238.60 → close $236.80 -14.40; SRRK×40 09:30 $49.52 → close $49.02 -20.00 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $190.23 | ▲ 09:30 equity $10,032.91 vs yday $10,014.55 (+18.36) | 09:30 open · cash $190.23 (unchanged overnight, no fees) · equity $10,032.91 vs prior close $10,014.55 (+18.36) · 5 name(s) re-marked at the open (per-name table). SMTC×11 yday $178.19 → 09:30 $182.33 +45.54; AVTR×125 yday $15.86 → 09:30 $15.87 +1.25; GME×89 yday $22.77 → 09:30 $22.90 +11.57; JBHT×8 yday $236.80 → 09:30 $236.80 +0.00; SRRK×40 yday $49.02 → 09:30 $48.02 -40.00 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 11 | $182.33 | $2.05 | $+122.21 | $2,193.81 | ▲ +122.21 after sell → book $10,030.86; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 125 | $15.87 | $2.40 | $+2.73 | $4,175.15 | ▲ +2.73 after sell → book $10,028.45; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 8 | $236.80 | $2.04 | $-18.45 | $6,067.52 | ▼ -18.45 after sell → book $10,026.42; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SRRK` | 40 | $48.02 | $2.14 | $-64.25 | $7,986.18 | ▼ -64.25 after sell → book $10,024.28; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 127 | $20.91 | $2.37 | — | $5,328.24 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2662.06 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 179 | $14.79 | $2.53 | — | $2,678.30 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $2662.06 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 189 | $14.07 | $2.56 | — | $16.52 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2662.06 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.52 | ▼ close $9,894.08 vs 09:30 $10,032.91 (session -122.75) | 16:00 close · cash $16.52 · equity $9,894.08 vs 09:30 $10,032.91 (-138.83; session marks -122.75) · 4 name(s) marked open→close (per-name table). GME×89 09:30 $22.90 → close $22.64 -23.14; TH×127 09:30 $20.91 → close $21.19 +35.56; RARE×179 09:30 $14.79 → close $14.51 -50.12; BHVN×189 09:30 $14.07 → close $13.62 -85.05 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1362.26 < 1 share @ 1746.53 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ESI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CVE` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GME` | 89 | 2026-09-17 @ $22.12 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1982.97 |
| `TH` | 127 | 2026-09-18 @ $20.91 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2662.06 |
| `RARE` | 179 | 2026-09-18 @ $14.79 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $2662.06 |
| `BHVN` | 189 | 2026-09-18 @ $14.07 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2662.06 |
