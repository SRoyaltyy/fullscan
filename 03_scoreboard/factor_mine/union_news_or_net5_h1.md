# Factor mine action — `union_news_or_net5_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 5

Cash book **+1.25%** ($10,126) · signal-only (no cash/fees) was +4.64%. Starts YES **6/26**. Fills 106 · skips 12 · realized $+143.21.

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
- Must-have: camera net (+G −R) is at least 5.
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
- **Gate** `news_or_headline=True,cam_net_min=5` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $1.48.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `HLIT` | 379 | — | $13.18 | +0.00 | $13.92 | +280.46 | +280.46 | +0.00 | +280.46 |
| 2026-08-14 | `SNDK` | 3 | — | $1646.93 | +0.00 | $1641.11 | -17.46 | -17.46 | +0.00 | -17.46 |
| 2026-08-17 | `HLIT` | 379 | $13.92 | $13.84 | -30.32 | — | +0.00 | -30.32 | +250.14 | — |
| 2026-08-17 | `SNDK` | 3 | $1641.11 | $1700.74 | +178.90 | — | +0.00 | +178.90 | +161.44 | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 22 | — | $91.01 | +0.00 | $93.63 | +57.64 | +57.64 | +0.00 | +57.64 |
| 2026-08-20 | `APA` | 46 | — | $44.76 | +0.00 | $44.39 | -17.02 | -17.02 | +0.00 | -17.02 |
| 2026-08-20 | `AUTL` | 841 | — | $2.47 | +0.00 | $2.46 | -8.41 | -8.41 | +0.00 | -8.41 |
| 2026-08-20 | `CRSP` | 35 | — | $58.73 | +0.00 | $58.12 | -21.35 | -21.35 | +0.00 | -21.35 |
| 2026-08-20 | `MRK` | 13 | — | $150.78 | +0.00 | $148.99 | -23.27 | -23.27 | +0.00 | -23.27 |
| 2026-08-21 | `BHP` | 22 | $93.63 | $95.72 | +45.98 | — | +0.00 | +45.98 | +103.62 | — |
| 2026-08-21 | `APA` | 46 | $44.39 | $44.52 | +5.98 | — | +0.00 | +5.98 | -11.04 | — |
| 2026-08-21 | `AUTL` | 841 | $2.46 | $2.47 | +8.41 | $2.41 | -50.46 | -42.05 | +0.00 | -50.46 |
| 2026-08-21 | `CRSP` | 35 | $58.12 | $59.72 | +56.00 | $59.50 | -7.70 | +48.30 | +34.65 | +26.95 |
| 2026-08-21 | `MRK` | 13 | $148.99 | $149.12 | +1.69 | — | +0.00 | +1.69 | -21.58 | — |
| 2026-08-21 | `AU` | 8 | — | $119.43 | +0.00 | $121.22 | +14.32 | +14.32 | +0.00 | +14.32 |
| 2026-08-21 | `FUTU` | 9 | — | $115.18 | +0.00 | $123.64 | +76.14 | +76.14 | +0.00 | +76.14 |
| 2026-08-21 | `GRAL` | 13 | — | $78.88 | +0.00 | $79.54 | +8.58 | +8.58 | +0.00 | +8.58 |
| 2026-08-21 | `VIRT` | 17 | — | $60.66 | +0.00 | $67.93 | +123.59 | +123.59 | +0.00 | +123.59 |
| 2026-08-21 | `MFC` | 24 | — | $42.48 | +0.00 | $42.51 | +0.72 | +0.72 | +0.00 | +0.72 |
| 2026-08-21 | `ABTC` | 121 | — | $8.66 | +0.00 | $7.93 | -88.33 | -88.33 | +0.00 | -88.33 |
| 2026-08-24 | `AUTL` | 841 | $2.41 | $2.40 | -8.41 | — | +0.00 | -8.41 | -58.87 | — |
| 2026-08-24 | `CRSP` | 35 | $59.50 | $58.75 | -26.25 | $57.08 | -58.62 | -84.87 | +0.70 | -57.92 |
| 2026-08-24 | `AU` | 8 | $121.22 | $120.51 | -5.68 | — | +0.00 | -5.68 | +8.64 | — |
| 2026-08-24 | `FUTU` | 9 | $123.64 | $121.00 | -23.76 | — | +0.00 | -23.76 | +52.38 | — |
| 2026-08-24 | `GRAL` | 13 | $79.54 | $81.87 | +30.29 | — | +0.00 | +30.29 | +38.87 | — |
| 2026-08-24 | `VIRT` | 17 | $67.93 | $66.80 | -19.21 | — | +0.00 | -19.21 | +104.38 | — |
| 2026-08-24 | `MFC` | 24 | $42.51 | $42.31 | -4.80 | — | +0.00 | -4.80 | -4.08 | — |
| 2026-08-24 | `ABTC` | 121 | $7.93 | $8.00 | +8.47 | — | +0.00 | +8.47 | -79.86 | — |
| 2026-08-25 | `CRSP` | 35 | $57.08 | $57.93 | +29.92 | — | +0.00 | +29.92 | -28.00 | — |
| 2026-08-25 | `AU` | 29 | — | $118.52 | +0.00 | $123.39 | +141.23 | +141.23 | +0.00 | +141.23 |
| 2026-08-25 | `FCX` | 45 | — | $77.13 | +0.00 | $79.91 | +125.10 | +125.10 | +0.00 | +125.10 |
| 2026-08-25 | `EZPW` | 99 | — | $35.05 | +0.00 | $35.23 | +17.82 | +17.82 | +0.00 | +17.82 |
| 2026-08-26 | `AU` | 29 | $123.39 | $119.80 | -104.11 | — | +0.00 | -104.11 | +37.12 | — |
| 2026-08-26 | `FCX` | 45 | $79.91 | $79.34 | -25.65 | — | +0.00 | -25.65 | +99.45 | — |
| 2026-08-26 | `EZPW` | 99 | $35.23 | $35.70 | +46.53 | — | +0.00 | +46.53 | +64.35 | — |
| 2026-08-26 | `FNV` | 13 | — | $267.02 | +0.00 | $267.37 | +4.55 | +4.55 | +0.00 | +4.55 |
| 2026-08-26 | `ASST` | 170 | — | $20.72 | +0.00 | $21.50 | +132.60 | +132.60 | +0.00 | +132.60 |
| 2026-08-26 | `AMX` | 149 | — | $23.75 | +0.00 | $23.62 | -19.37 | -19.37 | +0.00 | -19.37 |
| 2026-08-27 | `FNV` | 13 | $267.37 | $267.23 | -1.82 | — | +0.00 | -1.82 | +2.73 | — |
| 2026-08-27 | `ASST` | 170 | $21.50 | $22.45 | +161.50 | — | +0.00 | +161.50 | +294.10 | — |
| 2026-08-27 | `AMX` | 149 | $23.62 | $23.77 | +22.35 | — | +0.00 | +22.35 | +2.98 | — |
| 2026-08-27 | `ACMR` | 19 | — | $81.65 | +0.00 | $80.49 | -22.04 | -22.04 | +0.00 | -22.04 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `CM` | 13 | — | $118.77 | +0.00 | $114.84 | -51.09 | -51.09 | +0.00 | -51.09 |
| 2026-08-27 | `GEN` | 52 | — | $29.83 | +0.00 | $30.50 | +34.84 | +34.84 | +0.00 | +34.84 |
| 2026-08-27 | `LRCX` | 4 | — | $318.88 | +0.00 | $318.58 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-27 | `NVDA` | 6 | — | $222.86 | +0.00 | $227.98 | +30.72 | +30.72 | +0.00 | +30.72 |
| 2026-08-28 | `ACMR` | 19 | $80.49 | $79.27 | -23.18 | — | +0.00 | -23.18 | -45.22 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `CM` | 13 | $114.84 | $115.66 | +10.66 | — | +0.00 | +10.66 | -40.43 | — |
| 2026-08-28 | `GEN` | 52 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +34.84 | — |
| 2026-08-28 | `LRCX` | 4 | $318.58 | $318.03 | -2.20 | — | +0.00 | -2.20 | -3.40 | — |
| 2026-08-28 | `NVDA` | 6 | $227.98 | $227.36 | -3.72 | — | +0.00 | -3.72 | +27.00 | — |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 5 | — | $240.22 | +0.00 | $236.98 | -16.20 | -16.20 | +0.00 | -16.20 |
| 2026-08-28 | `PLAB` | 45 | — | $30.01 | +0.00 | $27.73 | -102.60 | -102.60 | +0.00 | -102.60 |
| 2026-08-28 | `ADSK` | 5 | — | $261.16 | +0.00 | $260.66 | -2.50 | -2.50 | +0.00 | -2.50 |
| 2026-08-28 | `SEDG` | 41 | — | $32.90 | +0.00 | $31.41 | -61.09 | -61.09 | +0.00 | -61.09 |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | — | +0.00 | +10.08 | -7.68 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -65.94 | — |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | — | +0.00 | +5.64 | -44.13 | — |
| 2026-08-31 | `DDOG` | 5 | $236.98 | $233.97 | -15.07 | — | +0.00 | -15.07 | -31.27 | — |
| 2026-08-31 | `PLAB` | 45 | $27.73 | $28.04 | +13.95 | — | +0.00 | +13.95 | -88.65 | — |
| 2026-08-31 | `ADSK` | 5 | $260.66 | $257.71 | -14.75 | — | +0.00 | -14.75 | -17.25 | — |
| 2026-08-31 | `SEDG` | 41 | $31.41 | $31.15 | -10.66 | — | +0.00 | -10.66 | -71.75 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 4 | — | $351.74 | +0.00 | $357.16 | +21.68 | +21.68 | +0.00 | +21.68 |
| 2026-09-03 | `DELL` | 3 | — | $486.31 | +0.00 | $516.39 | +90.24 | +90.24 | +0.00 | +90.24 |
| 2026-09-03 | `CXW` | 53 | — | $32.31 | +0.00 | $33.66 | +71.55 | +71.55 | +0.00 | +71.55 |
| 2026-09-03 | `FRNM` | 108 | — | $15.87 | +0.00 | $16.90 | +111.24 | +111.24 | +0.00 | +111.24 |
| 2026-09-03 | `MMED` | 72 | — | $23.88 | +0.00 | $23.84 | -2.88 | -2.88 | +0.00 | -2.88 |
| 2026-09-03 | `DE` | 2 | — | $703.25 | +0.00 | $694.41 | -17.68 | -17.68 | +0.00 | -17.68 |
| 2026-09-04 | `AVGO` | 4 | $357.16 | $359.70 | +10.16 | — | +0.00 | +10.16 | +31.84 | — |
| 2026-09-04 | `DELL` | 3 | $516.39 | $513.78 | -7.83 | — | +0.00 | -7.83 | +82.41 | — |
| 2026-09-04 | `CXW` | 53 | $33.66 | $33.46 | -10.60 | — | +0.00 | -10.60 | +60.95 | — |
| 2026-09-04 | `FRNM` | 108 | $16.90 | $16.40 | -54.00 | $16.31 | -9.72 | -63.72 | +57.24 | +47.52 |
| 2026-09-04 | `MMED` | 72 | $23.84 | $23.84 | +0.00 | $23.29 | -39.60 | -39.60 | -2.88 | -42.48 |
| 2026-09-04 | `DE` | 2 | $694.41 | $692.03 | -4.76 | — | +0.00 | -4.76 | -22.44 | — |
| 2026-09-04 | `CRM` | 6 | — | $263.36 | +0.00 | $259.23 | -24.78 | -24.78 | +0.00 | -24.78 |
| 2026-09-04 | `HPE` | 32 | — | $53.85 | +0.00 | $52.00 | -59.20 | -59.20 | +0.00 | -59.20 |
| 2026-09-04 | `MRX` | 23 | — | $75.65 | +0.00 | $78.27 | +60.26 | +60.26 | +0.00 | +60.26 |
| 2026-09-04 | `BE` | 7 | — | $236.82 | +0.00 | $252.87 | +112.35 | +112.35 | +0.00 | +112.35 |
| 2026-09-08 | `FRNM` | 108 | $16.31 | $16.74 | +46.44 | — | +0.00 | +46.44 | +93.96 | — |
| 2026-09-08 | `MMED` | 72 | $23.29 | $23.16 | -9.36 | — | +0.00 | -9.36 | -51.84 | — |
| 2026-09-08 | `CRM` | 6 | $259.23 | $253.72 | -33.06 | — | +0.00 | -33.06 | -57.84 | — |
| 2026-09-08 | `HPE` | 32 | $52.00 | $52.29 | +9.28 | — | +0.00 | +9.28 | -49.92 | — |
| 2026-09-08 | `MRX` | 23 | $78.27 | $78.84 | +13.11 | — | +0.00 | +13.11 | +73.37 | — |
| 2026-09-08 | `BE` | 7 | $252.87 | $267.76 | +104.23 | — | +0.00 | +104.23 | +216.58 | — |
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
| 2026-09-17 | `SMTC` | 19 | — | $170.85 | +0.00 | $178.19 | +139.46 | +139.46 | +0.00 | +139.46 |
| 2026-09-17 | `AVTR` | 209 | — | $15.81 | +0.00 | $15.86 | +10.45 | +10.45 | +0.00 | +10.45 |
| 2026-09-17 | `GME` | 149 | — | $22.12 | +0.00 | $22.77 | +96.85 | +96.85 | +0.00 | +96.85 |
| 2026-09-18 | `SMTC` | 19 | $178.19 | $182.33 | +78.66 | — | +0.00 | +78.66 | +218.12 | — |
| 2026-09-18 | `AVTR` | 209 | $15.86 | $15.87 | +2.09 | — | +0.00 | +2.09 | +12.54 | — |
| 2026-09-18 | `GME` | 149 | $22.77 | $22.90 | +19.37 | $22.64 | -38.74 | -19.37 | +116.22 | +77.48 |
| 2026-09-18 | `TH` | 109 | — | $20.91 | +0.00 | $21.19 | +30.52 | +30.52 | +0.00 | +30.52 |
| 2026-09-18 | `RARE` | 154 | — | $14.79 | +0.00 | $14.51 | -43.12 | -43.12 | +0.00 | -43.12 |
| 2026-09-18 | `BHVN` | 162 | — | $14.07 | +0.00 | $13.62 | -72.90 | -72.90 | +0.00 | -72.90 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +263.00 | HLIT, SNDK | — | $57.10 | $10,256.11 | HLIT×379, SNDK×3 |
| 2026-08-17 | +2.25 | $57.10 | HLIT×379, SNDK×3 | $10,404.70 | +148.59 | +0.00 | — | HLIT, SNDK | $10,397.65 | $10,397.65 | — |
| 2026-08-18 | -6.20 | $10,397.65 | — | $10,397.65 | +0.00 | +0.00 | — | — | $10,397.65 | $10,397.65 | — |
| 2026-08-19 | -7.20 | $10,397.65 | — | $10,397.65 | +0.00 | +0.00 | — | — | $10,397.65 | $10,397.65 | — |
| 2026-08-20 | +1.12 | $10,397.65 | — | $10,397.65 | +0.00 | -12.41 | BHP, APA, AUTL, CRSP, MRK | — | $224.36 | $10,366.09 | BHP×22, APA×46, AUTL×841, CRSP×35, MRK×13 |
| 2026-08-21 | +3.25 | $224.36 | BHP×22, APA×46, AUTL×841, CRSP×35, MRK×13 | $10,484.15 | +118.06 | +76.86 | AU, FUTU, GRAL, VIRT, MFC, ABTC | BHP, APA, MRK | $181.77 | $10,542.20 | AUTL×841, CRSP×35, AU×8, FUTU×9, GRAL×13, VIRT×17, MFC×24, ABTC×121 |
| 2026-08-24 | -5.17 | $181.77 | AUTL×841, CRSP×35, AU×8, FUTU×9, GRAL×13, VIRT×17, MFC×24, ABTC×121 | $10,492.85 | -49.35 | -58.62 | — | AUTL, AU, FUTU, GRAL, VIRT, MFC, ABTC | $8,412.95 | $10,410.57 | CRSP×35 |
| 2026-08-25 | +1.80 | $8,412.95 | CRSP×35 | $10,440.50 | +29.93 | +284.15 | AU, FCX, EZPW | CRSP | $54.01 | $10,716.04 | AU×29, FCX×45, EZPW×99 |
| 2026-08-26 | +2.02 | $54.01 | AU×29, FCX×45, EZPW×99 | $10,632.81 | -83.23 | +117.78 | FNV, ASST, AMX | AU, FCX, EZPW | $86.82 | $10,737.01 | FNV×13, ASST×170, AMX×149 |
| 2026-08-27 | — | $86.82 | FNV×13, ASST×170, AMX×149 | $10,919.04 | +182.03 | -40.39 | ACMR, MU, CM, GEN, LRCX, NVDA | FNV, ASST, AMX | $2,673.49 | $10,859.31 | ACMR×19, MU×1, CM×13, GEN×52, LRCX×4, NVDA×6 |
| 2026-08-28 | +0.75 | $2,673.49 | ACMR×19, MU×1, CM×13, GEN×52, LRCX×4, NVDA×6 | $10,824.77 | -34.54 | -411.17 | KEYS, SMTC, CIEN, MPWR, DDOG, PLAB, ADSK, SEDG | ACMR, MU, CM, GEN, LRCX, NVDA | $509.14 | $10,384.99 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, PLAB×45, ADSK×5, SEDG×41 |
| 2026-08-31 | -5.85 | $509.14 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, PLAB×45, ADSK×5, SEDG×41 | $10,384.34 | -0.65 | +0.00 | — | KEYS, SMTC, CIEN, MPWR, DDOG, PLAB, ADSK, SEDG | $10,367.92 | $10,367.92 | — |
| 2026-09-01 | -6.30 | $10,367.92 | — | $10,367.92 | +0.00 | +0.00 | — | — | $10,367.92 | $10,367.92 | — |
| 2026-09-02 | -3.83 | $10,367.92 | — | $10,367.92 | +0.00 | +0.00 | — | — | $10,367.92 | $10,367.92 | — |
| 2026-09-03 | -0.90 | $10,367.92 | — | $10,367.92 | +0.00 | +274.15 | AVGO, DELL, CXW, FRNM, MMED, DE | — | $937.12 | $10,629.41 | AVGO×4, DELL×3, CXW×53, FRNM×108, MMED×72, DE×2 |
| 2026-09-04 | +2.25 | $937.12 | AVGO×4, DELL×3, CXW×53, FRNM×108, MMED×72, DE×2 | $10,562.38 | -67.03 | +39.31 | CRM, HPE, MRX, BE | AVGO, DELL, CXW, DE | $357.25 | $10,585.29 | FRNM×108, MMED×72, CRM×6, HPE×32, MRX×23, BE×7 |
| 2026-09-08 | -11.47 | $357.25 | FRNM×108, MMED×72, CRM×6, HPE×32, MRX×23, BE×7 | $10,715.93 | +130.64 | +0.00 | — | FRNM, MMED, CRM, HPE, MRX, BE | $10,703.09 | $10,703.09 | — |
| 2026-09-09 | -13.95 | $10,703.09 | — | $10,703.09 | +0.00 | +0.00 | — | — | $10,703.09 | $10,703.09 | — |
| 2026-09-10 | -13.28 | $10,703.09 | — | $10,703.09 | +0.00 | +0.00 | — | — | $10,703.09 | $10,703.09 | — |
| 2026-09-11 | +0.50 | $10,703.09 | — | $10,703.09 | +0.00 | -527.85 | ORCL, BTI | — | $114.12 | $10,170.88 | ORCL×32, BTI×95 |
| 2026-09-14 | -11.00 | $114.12 | ORCL×32, BTI×95 | $10,065.96 | -104.92 | +0.00 | — | ORCL, BTI | $10,061.49 | $10,061.49 | — |
| 2026-09-15 | -3.84 | $10,061.49 | — | $10,061.49 | +0.00 | +0.00 | — | — | $10,061.49 | $10,061.49 | — |
| 2026-09-16 | +5.30 | $10,061.49 | — | $10,061.49 | +0.00 | -160.95 | WAY, QCOM, SM, AMX | — | $115.02 | $9,891.75 | WAY×95, QCOM×13, SM×62, AMX×108 |
| 2026-09-17 | +7.38 | $115.02 | WAY×95, QCOM×13, SM×62, AMX×108 | $9,931.08 | +39.33 | +246.76 | SMTC, AVTR, GME | WAY, QCOM, SM, AMX | $68.65 | $10,161.73 | SMTC×19, AVTR×209, GME×149 |
| 2026-09-18 | +4.86 | $68.65 | SMTC×19, AVTR×209, GME×149 | $10,261.85 | +100.12 | -124.24 | TH, RARE, BHVN | SMTC, AVTR | $1.48 | $10,125.53 | GME×149, TH×109, RARE×154, BHVN×162 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 379 | $13.18 | $4.89 | — | $4,999.89 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $5000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 3 | $1646.93 | $2.00 | — | $57.10 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $5000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.10 | ▲ close $10,256.11 vs 09:30 $10,000.00 (session +263.00) | 16:00 close · cash $57.10 · equity $10,256.11 vs 09:30 $10,000.00 (+256.11; session marks +263.00) · 2 name(s) marked open→close (per-name table). HLIT×379 09:30 $13.18 → close $13.92 +280.46; SNDK×3 09:30 $1646.93 → close $1641.11 -17.46 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.10 | ▲ 09:30 equity $10,404.70 vs yday $10,256.11 (+148.59) | 09:30 open · cash $57.10 (unchanged overnight, no fees) · equity $10,404.70 vs prior close $10,256.11 (+148.59) · 2 name(s) re-marked at the open (per-name table). HLIT×379 yday $13.92 → 09:30 $13.84 -30.32; SNDK×3 yday $1641.11 → 09:30 $1700.74 +178.90 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 379 | $13.84 | $4.99 | $+240.26 | $5,297.47 | ▲ +240.26 after sell → book $10,399.70; vs 09:30 mark -5.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SNDK` | 3 | $1700.74 | $2.05 | $+157.40 | $10,397.65 | ▲ +157.40 after sell → book $10,397.65; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,397.65 | ▲ close $10,397.65 vs 09:30 $10,404.70 (session +0.00) | 16:00 close · cash $10,397.65 · no lots left · equity $10,397.65. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,397.65 | ▲ 09:30 equity $10,397.65 vs yday $10,397.65 (+0.00) | 09:30 open · cash $10,397.65 · no holdings · equity $10,397.65 vs prior close $10,397.65 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,397.65 | ▲ close $10,397.65 vs 09:30 $10,397.65 (session +0.00) | 16:00 close · cash $10,397.65 · no lots left · equity $10,397.65. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,397.65 | ▲ 09:30 equity $10,397.65 vs yday $10,397.65 (+0.00) | 09:30 open · cash $10,397.65 · no holdings · equity $10,397.65 vs prior close $10,397.65 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,397.65 | ▲ close $10,397.65 vs 09:30 $10,397.65 (session +0.00) | 16:00 close · cash $10,397.65 · no lots left · equity $10,397.65. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,397.65 | ▲ 09:30 equity $10,397.65 vs yday $10,397.65 (+0.00) | 09:30 open · cash $10,397.65 · no holdings · equity $10,397.65 vs prior close $10,397.65 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 22 | $91.01 | $2.06 | — | $8,393.38 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2079.53 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 46 | $44.76 | $2.13 | — | $6,332.29 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $2079.53 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 841 | $2.47 | $10.85 | — | $4,244.17 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $2079.53 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 35 | $58.73 | $2.10 | — | $2,186.52 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $2079.53 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRK` | 13 | $150.78 | $2.03 | — | $224.36 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list oppset; 🔵; ⚪; ret5=+14.5; leftover $2079.53 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $224.36 | ▼ close $10,366.09 vs 09:30 $10,397.65 (session -12.41) | 16:00 close · cash $224.36 · equity $10,366.09 vs 09:30 $10,397.65 (-31.56; session marks -12.41) · 5 name(s) marked open→close (per-name table). BHP×22 09:30 $91.01 → close $93.63 +57.64; APA×46 09:30 $44.76 → close $44.39 -17.02; AUTL×841 09:30 $2.47 → close $2.46 -8.41; CRSP×35 09:30 $58.73 → close $58.12 -21.35; MRK×13 09:30 $150.78 → close $148.99 -23.27 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $224.36 | ▲ 09:30 equity $10,484.15 vs yday $10,366.09 (+118.06) | 09:30 open · cash $224.36 (unchanged overnight, no fees) · equity $10,484.15 vs prior close $10,366.09 (+118.06) · 5 name(s) re-marked at the open (per-name table). BHP×22 yday $93.63 → 09:30 $95.72 +45.98; APA×46 yday $44.39 → 09:30 $44.52 +5.98; AUTL×841 yday $2.46 → 09:30 $2.47 +8.41; CRSP×35 yday $58.12 → 09:30 $59.72 +56.00; MRK×13 yday $148.99 → 09:30 $149.12 +1.69 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 22 | $95.72 | $2.08 | $+99.48 | $2,328.11 | ▲ +99.48 after sell → book $10,482.06; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 46 | $44.52 | $2.15 | $-15.32 | $4,373.88 | ▼ -15.32 after sell → book $10,479.91; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRK` | 13 | $149.12 | $2.05 | $-25.66 | $6,310.38 | ▼ -25.66 after sell → book $10,477.85; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 8 | $119.43 | $2.01 | — | $5,352.93 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1051.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 9 | $115.18 | $2.02 | — | $4,314.29 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1051.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 13 | $78.88 | $2.03 | — | $3,286.82 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1051.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `VIRT` | 17 | $60.66 | $2.04 | — | $2,253.56 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list oppset; 🔵; ⚪; ret5=+7.0; leftover $1051.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MFC` | 24 | $42.48 | $2.06 | — | $1,231.98 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list oppset; 🔵; ⚪; ret5=-3.3; leftover $1051.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 121 | $8.66 | $2.35 | — | $181.77 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1051.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $181.77 | ▲ close $10,542.20 vs 09:30 $10,484.15 (session +76.86) | 16:00 close · cash $181.77 · equity $10,542.20 vs 09:30 $10,484.15 (+58.05; session marks +76.86) · 8 name(s) marked open→close (per-name table). AUTL×841 09:30 $2.47 → close $2.41 -50.46; CRSP×35 09:30 $59.72 → close $59.50 -7.70; AU×8 09:30 $119.43 → close $121.22 +14.32; FUTU×9 09:30 $115.18 → close $123.64 +76.14; GRAL×13 09:30 $78.88 → close $79.54 +8.58; VIRT×17 09:30 $60.66 → close $67.93 +123.59; MFC×24 09:30 $42.48 → close $42.51 +0.72; ABTC×121 09:30 $8.66 → close $7.93 -88.33 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $181.77 | ▼ 09:30 equity $10,492.85 vs yday $10,542.20 (-49.35) | 09:30 open · cash $181.77 (unchanged overnight, no fees) · equity $10,492.85 vs prior close $10,542.20 (-49.35) · 8 name(s) re-marked at the open (per-name table). AUTL×841 yday $2.41 → 09:30 $2.40 -8.41; CRSP×35 yday $59.50 → 09:30 $58.75 -26.25; AU×8 yday $121.22 → 09:30 $120.51 -5.68; FUTU×9 yday $123.64 → 09:30 $121.00 -23.76; GRAL×13 yday $79.54 → 09:30 $81.87 +30.29; VIRT×17 yday $67.93 → 09:30 $66.80 -19.21; MFC×24 yday $42.51 → 09:30 $42.31 -4.80; ABTC×121 yday $7.93 → 09:30 $8.00 +8.47 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 841 | $2.40 | $11.00 | $-80.72 | $2,189.16 | ▼ -80.72 after sell → book $10,481.84; vs 09:30 mark -11.01 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 8 | $120.51 | $2.03 | $+4.59 | $3,151.21 | ▲ +4.59 after sell → book $10,479.81; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 9 | $121.00 | $2.04 | $+48.33 | $4,238.17 | ▲ +48.33 after sell → book $10,477.77; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 13 | $81.87 | $2.05 | $+34.79 | $5,300.43 | ▲ +34.79 after sell → book $10,475.72; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `VIRT` | 17 | $66.80 | $2.06 | $+100.28 | $6,433.97 | ▲ +100.28 after sell → book $10,473.66; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `MFC` | 24 | $42.31 | $2.08 | $-8.22 | $7,447.33 | ▼ -8.22 after sell → book $10,471.58; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 121 | $8.00 | $2.38 | $-84.60 | $8,412.95 | ▼ -84.60 after sell → book $10,469.20; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,412.95 | ▼ close $10,410.57 vs 09:30 $10,492.85 (session -58.62) | 16:00 close · cash $8,412.95 · equity $10,410.57 vs 09:30 $10,492.85 (-82.28; session marks -58.62) · 1 name(s) marked open→close (per-name table). CRSP×35 09:30 $58.75 → close $57.08 -58.62 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,412.95 | ▲ 09:30 equity $10,440.50 vs yday $10,410.57 (+29.93) | 09:30 open · cash $8,412.95 (unchanged overnight, no fees) · equity $10,440.50 vs prior close $10,410.57 (+29.93) · 1 name(s) re-marked at the open (per-name table). CRSP×35 yday $57.08 → 09:30 $57.93 +29.92 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 35 | $57.93 | $2.12 | $-32.22 | $10,438.38 | ▼ -32.22 after sell → book $10,438.38; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 29 | $118.52 | $2.08 | — | $6,999.22 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $3479.46 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 45 | $77.13 | $2.12 | — | $3,526.24 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3479.46 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 99 | $35.05 | $2.29 | — | $54.01 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,oppset; 🔵; ⚪; ret5=+19.7; leftover $3479.46 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.01 | ▲ close $10,716.04 vs 09:30 $10,440.50 (session +284.15) | 16:00 close · cash $54.01 · equity $10,716.04 vs 09:30 $10,440.50 (+275.54; session marks +284.15) · 3 name(s) marked open→close (per-name table). AU×29 09:30 $118.52 → close $123.39 +141.23; FCX×45 09:30 $77.13 → close $79.91 +125.10; EZPW×99 09:30 $35.05 → close $35.23 +17.82 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.01 | ▼ 09:30 equity $10,632.81 vs yday $10,716.04 (-83.23) | 09:30 open · cash $54.01 (unchanged overnight, no fees) · equity $10,632.81 vs prior close $10,716.04 (-83.23) · 3 name(s) re-marked at the open (per-name table). AU×29 yday $123.39 → 09:30 $119.80 -104.11; FCX×45 yday $79.91 → 09:30 $79.34 -25.65; EZPW×99 yday $35.23 → 09:30 $35.70 +46.53 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 29 | $119.80 | $2.11 | $+32.93 | $3,526.09 | ▲ +32.93 after sell → book $10,630.69; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 45 | $79.34 | $2.16 | $+95.16 | $7,094.23 | ▲ +95.16 after sell → book $10,628.53; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 99 | $35.70 | $2.33 | $+59.73 | $10,626.20 | ▲ +59.73 after sell → book $10,626.20; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 13 | $267.02 | $2.03 | — | $7,152.91 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $3542.07 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ASST` | 170 | $20.72 | $2.50 | — | $3,628.01 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list oppset; 🔵; ret5=+67.1; leftover $3542.07 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AMX` | 149 | $23.75 | $2.44 | — | $86.82 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list oppset; 🔵; ret5=+0.5; leftover $3542.07 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.82 | ▲ close $10,737.01 vs 09:30 $10,632.81 (session +117.78) | 16:00 close · cash $86.82 · equity $10,737.01 vs 09:30 $10,632.81 (+104.20; session marks +117.78) · 3 name(s) marked open→close (per-name table). FNV×13 09:30 $267.02 → close $267.37 +4.55; ASST×170 09:30 $20.72 → close $21.50 +132.60; AMX×149 09:30 $23.75 → close $23.62 -19.37 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.82 | ▲ 09:30 equity $10,919.04 vs yday $10,737.01 (+182.03) | 09:30 open · cash $86.82 (unchanged overnight, no fees) · equity $10,919.04 vs prior close $10,737.01 (+182.03) · 3 name(s) re-marked at the open (per-name table). FNV×13 yday $267.37 → 09:30 $267.23 -1.82; ASST×170 yday $21.50 → 09:30 $22.45 +161.50; AMX×149 yday $23.62 → 09:30 $23.77 +22.35 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 13 | $267.23 | $2.07 | $-1.37 | $3,558.74 | ▼ -1.37 after sell → book $10,916.97; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 170 | $22.45 | $2.56 | $+289.04 | $7,372.69 | ▲ +289.04 after sell → book $10,914.42; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AMX` | 149 | $23.77 | $2.49 | $-1.95 | $10,911.93 | ▼ -1.95 after sell → book $10,911.93; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 19 | $81.65 | $2.05 | — | $9,358.53 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1558.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $8,389.53 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1558.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 13 | $118.77 | $2.03 | — | $6,843.49 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list earn_react; ret5=+0.3; leftover $1558.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 52 | $29.83 | $2.15 | — | $5,290.18 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+7.6; leftover $1558.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 4 | $318.88 | $2.00 | — | $4,012.66 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1558.85 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 6 | $222.86 | $2.01 | — | $2,673.49 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $1558.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,673.49 | ▼ close $10,859.31 vs 09:30 $10,919.04 (session -40.39) | 16:00 close · cash $2,673.49 · equity $10,859.31 vs 09:30 $10,919.04 (-59.73; session marks -40.39) · 6 name(s) marked open→close (per-name table). ACMR×19 09:30 $81.65 → close $80.49 -22.04; MU×1 09:30 $967.01 → close $935.39 -31.62; CM×13 09:30 $118.77 → close $114.84 -51.09; GEN×52 09:30 $29.83 → close $30.50 +34.84; LRCX×4 09:30 $318.88 → close $318.58 -1.20; NVDA×6 09:30 $222.86 → close $227.98 +30.72 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,673.49 | ▼ 09:30 equity $10,824.77 vs yday $10,859.31 (-34.54) | 09:30 open · cash $2,673.49 (unchanged overnight, no fees) · equity $10,824.77 vs prior close $10,859.31 (-34.54) · 6 name(s) re-marked at the open (per-name table). ACMR×19 yday $80.49 → 09:30 $79.27 -23.18; MU×1 yday $935.39 → 09:30 $919.29 -16.10; CM×13 yday $114.84 → 09:30 $115.66 +10.66; GEN×52 yday $30.50 → 09:30 $30.50 +0.00; LRCX×4 yday $318.58 → 09:30 $318.03 -2.20; NVDA×6 yday $227.98 → 09:30 $227.36 -3.72 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 19 | $79.27 | $2.07 | $-49.34 | $4,177.55 | ▼ -49.34 after sell → book $10,822.70; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $5,094.83 | ▼ -51.73 after sell → book $10,820.69; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 13 | $115.66 | $2.05 | $-44.51 | $6,596.36 | ▼ -44.51 after sell → book $10,818.64; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 52 | $30.50 | $2.17 | $+30.53 | $8,180.19 | ▲ +30.53 after sell → book $10,816.47; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 4 | $318.03 | $2.02 | $-7.42 | $9,450.29 | ▼ -7.42 after sell → book $10,814.45; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 6 | $227.36 | $2.03 | $+22.96 | $10,812.42 | ▲ +22.96 after sell → book $10,812.42; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $9,512.78 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1351.55 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $8,234.92 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $1351.55 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $7,031.66 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1351.55 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $5,723.64 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1351.55 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $4,520.53 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1351.55 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PLAB` | 45 | $30.01 | $2.12 | — | $3,167.96 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list oppset; 🔵; ret5=-0.9; leftover $1351.55 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $1,860.15 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list earn_react; ret5=+7.8; leftover $1351.55 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 41 | $32.90 | $2.11 | — | $509.14 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1351.55 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $509.14 | ▼ close $10,384.99 vs 09:30 $10,824.77 (session -411.17) | 16:00 close · cash $509.14 · equity $10,384.99 vs 09:30 $10,824.77 (-439.78; session marks -411.17) · 8 name(s) marked open→close (per-name table). KEYS×4 09:30 $324.41 → close $319.97 -17.76; SMTC×9 09:30 $141.76 → close $131.17 -95.31; CIEN×3 09:30 $400.42 → close $378.44 -65.94; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×5 09:30 $240.22 → close $236.98 -16.20; PLAB×45 09:30 $30.01 → close $27.73 -102.60; ADSK×5 09:30 $261.16 → close $260.66 -2.50; SEDG×41 09:30 $32.90 → close $31.41 -61.09 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $509.14 | ▼ 09:30 equity $10,384.34 vs yday $10,384.99 (-0.65) | 09:30 open · cash $509.14 (unchanged overnight, no fees) · equity $10,384.34 vs prior close $10,384.99 (-0.65) · 8 name(s) re-marked at the open (per-name table). KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×5 yday $236.98 → 09:30 $233.97 -15.07; PLAB×45 yday $27.73 → 09:30 $28.04 +13.95; ADSK×5 yday $260.66 → 09:30 $257.71 -14.75; SEDG×41 yday $31.41 → 09:30 $31.15 -10.66 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $1,797.08 | ▼ -11.70 after sell → book $10,382.32; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $2,985.74 | ▼ -89.19 after sell → book $10,380.28; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $4,119.04 | ▼ -69.96 after sell → book $10,378.27; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $5,378.93 | ▼ -48.14 after sell → book $10,376.25; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 5 | $233.97 | $2.02 | $-35.31 | $6,546.73 | ▼ -35.31 after sell → book $10,374.23; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PLAB` | 45 | $28.04 | $2.15 | $-92.92 | $7,806.38 | ▼ -92.92 after sell → book $10,372.08; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 5 | $257.71 | $2.03 | $-21.28 | $9,092.91 | ▼ -21.28 after sell → book $10,370.06; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 41 | $31.15 | $2.13 | $-76.00 | $10,367.92 | ▼ -76.00 after sell → book $10,367.92; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,367.92 | ▲ close $10,367.92 vs 09:30 $10,384.34 (session +0.00) | 16:00 close · cash $10,367.92 · no lots left · equity $10,367.92. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,367.92 | ▲ 09:30 equity $10,367.92 vs yday $10,367.92 (+0.00) | 09:30 open · cash $10,367.92 · no holdings · equity $10,367.92 vs prior close $10,367.92 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,367.92 | ▲ close $10,367.92 vs 09:30 $10,367.92 (session +0.00) | 16:00 close · cash $10,367.92 · no lots left · equity $10,367.92. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,367.92 | ▲ 09:30 equity $10,367.92 vs yday $10,367.92 (+0.00) | 09:30 open · cash $10,367.92 · no holdings · equity $10,367.92 vs prior close $10,367.92 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,367.92 | ▲ close $10,367.92 vs 09:30 $10,367.92 (session +0.00) | 16:00 close · cash $10,367.92 · no lots left · equity $10,367.92. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,367.92 | ▲ 09:30 equity $10,367.92 vs yday $10,367.92 (+0.00) | 09:30 open · cash $10,367.92 · no holdings · equity $10,367.92 vs prior close $10,367.92 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 4 | $351.74 | $2.00 | — | $8,958.96 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1727.99 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $7,498.03 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $1727.99 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 53 | $32.31 | $2.15 | — | $5,783.45 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1727.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 108 | $15.87 | $2.31 | — | $4,067.18 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1727.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 72 | $23.88 | $2.21 | — | $2,345.61 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+21.9; leftover $1727.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $937.12 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1727.99 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $937.12 | ▲ close $10,629.41 vs 09:30 $10,367.92 (session +274.15) | 16:00 close · cash $937.12 · equity $10,629.41 vs 09:30 $10,367.92 (+261.49; session marks +274.15) · 6 name(s) marked open→close (per-name table). AVGO×4 09:30 $351.74 → close $357.16 +21.68; DELL×3 09:30 $486.31 → close $516.39 +90.24; CXW×53 09:30 $32.31 → close $33.66 +71.55; FRNM×108 09:30 $15.87 → close $16.90 +111.24; MMED×72 09:30 $23.88 → close $23.84 -2.88; DE×2 09:30 $703.25 → close $694.41 -17.68 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $937.12 | ▼ 09:30 equity $10,562.38 vs yday $10,629.41 (-67.03) | 09:30 open · cash $937.12 (unchanged overnight, no fees) · equity $10,562.38 vs prior close $10,629.41 (-67.03) · 6 name(s) re-marked at the open (per-name table). AVGO×4 yday $357.16 → 09:30 $359.70 +10.16; DELL×3 yday $516.39 → 09:30 $513.78 -7.83; CXW×53 yday $33.66 → 09:30 $33.46 -10.60; FRNM×108 yday $16.90 → 09:30 $16.40 -54.00; MMED×72 yday $23.84 → 09:30 $23.84 +0.00; DE×2 yday $694.41 → 09:30 $692.03 -4.76 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 4 | $359.70 | $2.02 | $+27.81 | $2,373.89 | ▲ +27.81 after sell → book $10,560.35; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 3 | $513.78 | $2.02 | $+78.39 | $3,913.21 | ▲ +78.39 after sell → book $10,558.33; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 53 | $33.46 | $2.17 | $+56.63 | $5,684.42 | ▲ +56.63 after sell → book $10,556.16; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 2 | $692.03 | $2.02 | $-26.45 | $7,066.46 | ▼ -26.45 after sell → book $10,554.14; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 6 | $263.36 | $2.01 | — | $5,484.29 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1766.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `HPE` | 32 | $53.85 | $2.09 | — | $3,759.01 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list oppset; 🔵; ret5=+0.1; leftover $1766.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟡 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 23 | $75.65 | $2.06 | — | $2,017.00 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1766.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 7 | $236.82 | $2.01 | — | $357.25 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; ret5=+8.1; leftover $1766.62 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $357.25 | ▲ close $10,585.29 vs 09:30 $10,562.38 (session +39.31) | 16:00 close · cash $357.25 · equity $10,585.29 vs 09:30 $10,562.38 (+22.91; session marks +39.31) · 6 name(s) marked open→close (per-name table). FRNM×108 09:30 $16.40 → close $16.31 -9.72; MMED×72 09:30 $23.84 → close $23.29 -39.60; CRM×6 09:30 $263.36 → close $259.23 -24.78; HPE×32 09:30 $53.85 → close $52.00 -59.20; MRX×23 09:30 $75.65 → close $78.27 +60.26; BE×7 09:30 $236.82 → close $252.87 +112.35 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $357.25 | ▲ 09:30 equity $10,715.93 vs yday $10,585.29 (+130.64) | 09:30 open · cash $357.25 (unchanged overnight, no fees) · equity $10,715.93 vs prior close $10,585.29 (+130.64) · 6 name(s) re-marked at the open (per-name table). FRNM×108 yday $16.31 → 09:30 $16.74 +46.44; MMED×72 yday $23.29 → 09:30 $23.16 -9.36; CRM×6 yday $259.23 → 09:30 $253.72 -33.06; HPE×32 yday $52.00 → 09:30 $52.29 +9.28; MRX×23 yday $78.27 → 09:30 $78.84 +13.11; BE×7 yday $252.87 → 09:30 $267.76 +104.23 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 108 | $16.74 | $2.35 | $+89.30 | $2,162.82 | ▲ +89.30 after sell → book $10,713.58; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MMED` | 72 | $23.16 | $2.23 | $-56.28 | $3,828.11 | ▼ -56.28 after sell → book $10,711.35; vs 09:30 mark -2.23 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 6 | $253.72 | $2.03 | $-61.88 | $5,348.40 | ▼ -61.88 after sell → book $10,709.32; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HPE` | 32 | $52.29 | $2.11 | $-54.12 | $7,019.57 | ▼ -54.12 after sell → book $10,707.21; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 23 | $78.84 | $2.08 | $+69.23 | $8,830.81 | ▲ +69.23 after sell → book $10,705.13; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 7 | $267.76 | $2.04 | $+212.53 | $10,703.09 | ▲ +212.53 after sell → book $10,703.09; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,703.09 | ▲ close $10,703.09 vs 09:30 $10,715.93 (session +0.00) | 16:00 close · cash $10,703.09 · no lots left · equity $10,703.09. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,703.09 | ▲ 09:30 equity $10,703.09 vs yday $10,703.09 (+0.00) | 09:30 open · cash $10,703.09 · no holdings · equity $10,703.09 vs prior close $10,703.09 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,703.09 | ▲ close $10,703.09 vs 09:30 $10,703.09 (session +0.00) | 16:00 close · cash $10,703.09 · no lots left · equity $10,703.09. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,703.09 | ▲ 09:30 equity $10,703.09 vs yday $10,703.09 (+0.00) | 09:30 open · cash $10,703.09 · no holdings · equity $10,703.09 vs prior close $10,703.09 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,703.09 | ▲ close $10,703.09 vs 09:30 $10,703.09 (session +0.00) | 16:00 close · cash $10,703.09 · no lots left · equity $10,703.09. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,703.09 | ▲ 09:30 equity $10,703.09 vs yday $10,703.09 (+0.00) | 09:30 open · cash $10,703.09 · no holdings · equity $10,703.09 vs prior close $10,703.09 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 32 | $164.43 | $2.09 | — | $5,439.25 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $5351.55 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BTI` | 95 | $56.03 | $2.27 | — | $114.12 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list oppset; 🔵; ret5=-0.8; leftover $5351.55 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $114.12 | ▼ close $10,170.88 vs 09:30 $10,703.09 (session -527.85) | 16:00 close · cash $114.12 · equity $10,170.88 vs 09:30 $10,703.09 (-532.21; session marks -527.85) · 2 name(s) marked open→close (per-name table). ORCL×32 09:30 $164.43 → close $150.28 -452.80; BTI×95 09:30 $56.03 → close $55.24 -75.05 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $114.12 | ▼ 09:30 equity $10,065.96 vs yday $10,170.88 (-104.92) | 09:30 open · cash $114.12 (unchanged overnight, no fees) · equity $10,065.96 vs prior close $10,170.88 (-104.92) · 2 name(s) re-marked at the open (per-name table). ORCL×32 yday $150.28 → 09:30 $141.42 -283.52; BTI×95 yday $55.24 → 09:30 $57.12 +178.60 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 32 | $141.42 | $2.13 | $-740.54 | $4,637.43 | ▼ -740.54 after sell → book $10,063.83; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-14 09:30 ET | **SELL** | `BTI` | 95 | $57.12 | $2.33 | $+98.94 | $10,061.49 | ▲ +98.94 after sell → book $10,061.49; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,061.49 | ▲ close $10,061.49 vs 09:30 $10,065.96 (session +0.00) | 16:00 close · cash $10,061.49 · no lots left · equity $10,061.49. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,061.49 | ▲ 09:30 equity $10,061.49 vs yday $10,061.49 (+0.00) | 09:30 open · cash $10,061.49 · no holdings · equity $10,061.49 vs prior close $10,061.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,061.49 | ▲ close $10,061.49 vs 09:30 $10,061.49 (session +0.00) | 16:00 close · cash $10,061.49 · no lots left · equity $10,061.49. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,061.49 | ▲ 09:30 equity $10,061.49 vs yday $10,061.49 (+0.00) | 09:30 open · cash $10,061.49 · no holdings · equity $10,061.49 vs prior close $10,061.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 95 | $26.27 | $2.27 | — | $7,563.57 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2515.37 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 13 | $189.17 | $2.03 | — | $5,102.33 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $2515.37 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 62 | $39.99 | $2.18 | — | $2,620.77 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2515.37 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `AMX` | 108 | $23.18 | $2.31 | — | $115.02 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list oppset; 🔵; ret5=-0.2; leftover $2515.37 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $115.02 | ▼ close $9,891.75 vs 09:30 $10,061.49 (session -160.95) | 16:00 close · cash $115.02 · equity $9,891.75 vs 09:30 $10,061.49 (-169.74; session marks -160.95) · 4 name(s) marked open→close (per-name table). WAY×95 09:30 $26.27 → close $26.59 +30.40; QCOM×13 09:30 $189.17 → close $184.84 -56.29; SM×62 09:30 $39.99 → close $38.16 -113.46; AMX×108 09:30 $23.18 → close $22.98 -21.60 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $115.02 | ▲ 09:30 equity $9,931.08 vs yday $9,891.75 (+39.33) | 09:30 open · cash $115.02 (unchanged overnight, no fees) · equity $9,931.08 vs prior close $9,891.75 (+39.33) · 4 name(s) re-marked at the open (per-name table). WAY×95 yday $26.59 → 09:30 $26.51 -7.60; QCOM×13 yday $184.84 → 09:30 $190.35 +71.63; SM×62 yday $38.16 → 09:30 $37.57 -36.58; AMX×108 yday $22.98 → 09:30 $23.09 +11.88 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 95 | $26.51 | $2.31 | $+18.21 | $2,631.16 | ▲ +18.21 after sell → book $9,928.77; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 13 | $190.35 | $2.06 | $+11.25 | $5,103.65 | ▲ +11.25 after sell → book $9,926.71; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 62 | $37.57 | $2.20 | $-154.42 | $7,430.79 | ▼ -154.42 after sell → book $9,924.51; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AMX` | 108 | $23.09 | $2.35 | $-14.39 | $9,922.15 | ▼ -14.39 after sell → book $9,922.15; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 19 | $170.85 | $2.05 | — | $6,673.96 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $3307.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 209 | $15.81 | $2.70 | — | $3,366.97 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $3307.38 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 149 | $22.12 | $2.44 | — | $68.65 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $3307.38 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $68.65 | ▲ close $10,161.73 vs 09:30 $9,931.08 (session +246.76) | 16:00 close · cash $68.65 · equity $10,161.73 vs 09:30 $9,931.08 (+230.65; session marks +246.76) · 3 name(s) marked open→close (per-name table). SMTC×19 09:30 $170.85 → close $178.19 +139.46; AVTR×209 09:30 $15.81 → close $15.86 +10.45; GME×149 09:30 $22.12 → close $22.77 +96.85 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $68.65 | ▲ 09:30 equity $10,261.85 vs yday $10,161.73 (+100.12) | 09:30 open · cash $68.65 (unchanged overnight, no fees) · equity $10,261.85 vs prior close $10,161.73 (+100.12) · 3 name(s) re-marked at the open (per-name table). SMTC×19 yday $178.19 → 09:30 $182.33 +78.66; AVTR×209 yday $15.86 → 09:30 $15.87 +2.09; GME×149 yday $22.77 → 09:30 $22.90 +19.37 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 19 | $182.33 | $2.08 | $+213.99 | $3,530.84 | ▲ +213.99 after sell → book $10,259.77; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 209 | $15.87 | $2.76 | $+7.09 | $6,844.91 | ▲ +7.09 after sell → book $10,257.01; vs 09:30 mark -2.76 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 109 | $20.91 | $2.32 | — | $4,563.40 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2281.64 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 154 | $14.79 | $2.45 | — | $2,283.29 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $2281.64 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 162 | $14.07 | $2.48 | — | $1.48 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2281.64 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.48 | ▼ close $10,125.53 vs 09:30 $10,261.85 (session -124.24) | 16:00 close · cash $1.48 · equity $10,125.53 vs 09:30 $10,261.85 (-136.32; session marks -124.24) · 4 name(s) marked open→close (per-name table). GME×149 09:30 $22.90 → close $22.64 -38.74; TH×109 09:30 $20.91 → close $21.19 +30.52; RARE×154 09:30 $14.79 → close $14.51 -43.12; BHVN×162 09:30 $14.07 → close $13.62 -72.90 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1558.85 < 1 share @ 1746.53 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GME` | 149 | 2026-09-17 @ $22.12 | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $3307.38 |
| `TH` | 109 | 2026-09-18 @ $20.91 | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2281.64 |
| `RARE` | 154 | 2026-09-18 @ $14.79 | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $2281.64 |
| `BHVN` | 162 | 2026-09-18 @ $14.07 | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2281.64 |
