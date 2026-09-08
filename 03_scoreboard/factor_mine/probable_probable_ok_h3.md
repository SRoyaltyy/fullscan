# Factor mine action — `probable_probable_ok_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-3.22%** ($9,678) · signal-only (no cash/fees) was +6.24%. Starts YES **9/18**. Fills 62 · skips 101 · realized $-1396.42.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at yesterday's 'likely to keep moving' list and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `probable` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True,ret_5_max=10.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7.81.

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
| 2026-08-17 | `ANGX` | 464 | $4.37 | $4.60 | +106.72 | $4.71 | +51.04 | +157.76 | +134.56 | +185.60 |
| 2026-08-17 | `HYLN` | 478 | $4.06 | $4.10 | +19.12 | $4.09 | -4.78 | +14.34 | -38.24 | -43.02 |
| 2026-08-17 | `WDC` | 3 | $508.80 | $525.53 | +50.19 | $536.01 | +31.44 | +81.63 | +66.09 | +97.53 |
| 2026-08-17 | `ADUR` | 121 | $16.17 | $15.73 | -53.24 | $15.85 | +14.52 | -38.72 | -93.17 | -78.65 |
| 2026-08-17 | `ALGM` | 45 | $44.39 | $45.32 | +41.85 | $44.25 | -48.15 | -6.30 | +56.70 | +8.55 |
| 2026-08-17 | `CDNL` | 2 | — | $39.85 | +0.00 | $39.23 | -1.24 | -1.24 | +0.00 | -1.24 |
| 2026-08-17 | `ABX` | 9 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `VERA` | 2 | — | $31.30 | +0.00 | $31.63 | +0.66 | +0.66 | +0.00 | +0.66 |
| 2026-08-17 | `OCC` | 4 | — | $18.24 | +0.00 | $17.12 | -4.48 | -4.48 | +0.00 | -4.48 |
| 2026-08-17 | `ALM` | 5 | — | $16.20 | +0.00 | $16.36 | +0.80 | +0.80 | +0.00 | +0.80 |
| 2026-08-18 | `ANGX` | 464 | $4.71 | $4.79 | +37.12 | $4.85 | +27.84 | +64.96 | +222.72 | +250.56 |
| 2026-08-18 | `HYLN` | 478 | $4.09 | $3.95 | -66.92 | $3.86 | -43.02 | -109.94 | -109.94 | -152.96 |
| 2026-08-18 | `WDC` | 3 | $536.01 | $496.07 | -119.82 | $496.16 | +0.27 | -119.55 | -22.29 | -22.02 |
| 2026-08-18 | `ADUR` | 121 | $15.85 | $15.41 | -53.24 | $15.63 | +26.62 | -26.62 | -131.89 | -105.27 |
| 2026-08-18 | `ALGM` | 45 | $44.25 | $42.54 | -76.95 | $39.39 | -141.75 | -218.70 | -68.40 | -210.15 |
| 2026-08-18 | `CDNL` | 2 | $39.23 | $41.57 | +4.68 | $45.14 | +7.14 | +11.82 | +3.44 | +10.58 |
| 2026-08-18 | `ABX` | 9 | $9.12 | $9.03 | -0.81 | $9.01 | -0.18 | -0.99 | -0.81 | -0.99 |
| 2026-08-18 | `VERA` | 2 | $31.63 | $31.31 | -0.64 | $32.28 | +1.94 | +1.30 | +0.02 | +1.96 |
| 2026-08-18 | `OCC` | 4 | $17.12 | $16.20 | -3.68 | $16.20 | +0.00 | -3.68 | -8.16 | -8.16 |
| 2026-08-18 | `ALM` | 5 | $16.36 | $15.78 | -2.90 | $15.60 | -0.90 | -3.80 | -2.10 | -3.00 |
| 2026-08-19 | `ANGX` | 464 | $4.85 | $4.79 | -27.84 | — | +0.00 | -27.84 | +222.72 | — |
| 2026-08-19 | `HYLN` | 478 | $3.86 | $3.87 | +4.78 | — | +0.00 | +4.78 | -148.18 | — |
| 2026-08-19 | `WDC` | 3 | $496.16 | $494.28 | -5.64 | — | +0.00 | -5.64 | -27.66 | — |
| 2026-08-19 | `ADUR` | 121 | $15.63 | $15.65 | +2.42 | — | +0.00 | +2.42 | -102.85 | — |
| 2026-08-19 | `ALGM` | 45 | $39.39 | $40.00 | +27.45 | — | +0.00 | +27.45 | -182.70 | — |
| 2026-08-19 | `CDNL` | 2 | $45.14 | $44.83 | -0.62 | $43.33 | -3.00 | -3.62 | +9.96 | +6.96 |
| 2026-08-19 | `ABX` | 9 | $9.01 | $9.08 | +0.63 | $9.15 | +0.63 | +1.26 | -0.36 | +0.27 |
| 2026-08-19 | `VERA` | 2 | $32.28 | $32.88 | +1.20 | $32.27 | -1.21 | -0.01 | +3.16 | +1.95 |
| 2026-08-19 | `OCC` | 4 | $16.20 | $16.21 | +0.04 | $14.36 | -7.40 | -7.36 | -8.12 | -15.52 |
| 2026-08-19 | `ALM` | 5 | $15.60 | $16.05 | +2.25 | $16.18 | +0.65 | +2.90 | -0.75 | -0.10 |
| 2026-08-20 | `CDNL` | 2 | $43.33 | $43.13 | -0.40 | — | +0.00 | -0.40 | +6.56 | — |
| 2026-08-20 | `ABX` | 9 | $9.15 | $9.13 | -0.18 | — | +0.00 | -0.18 | +0.09 | — |
| 2026-08-20 | `VERA` | 2 | $32.27 | $32.30 | +0.04 | — | +0.00 | +0.04 | +1.99 | — |
| 2026-08-20 | `OCC` | 4 | $14.36 | $14.10 | -1.04 | — | +0.00 | -1.04 | -16.56 | — |
| 2026-08-20 | `ALM` | 5 | $16.18 | $15.81 | -1.85 | — | +0.00 | -1.85 | -1.95 | — |
| 2026-08-20 | `DNA` | 217 | — | $7.45 | +0.00 | $6.96 | -106.33 | -106.33 | +0.00 | -106.33 |
| 2026-08-20 | `MSTR` | 14 | — | $113.23 | +0.00 | $112.39 | -11.76 | -11.76 | +0.00 | -11.76 |
| 2026-08-20 | `EXK` | 150 | — | $10.77 | +0.00 | $10.97 | +30.00 | +30.00 | +0.00 | +30.00 |
| 2026-08-20 | `SCZM` | 171 | — | $9.46 | +0.00 | $9.76 | +51.30 | +51.30 | +0.00 | +51.30 |
| 2026-08-20 | `NG` | 193 | — | $8.38 | +0.00 | $8.66 | +54.04 | +54.04 | +0.00 | +54.04 |
| 2026-08-20 | `BLSH` | 55 | — | $29.20 | +0.00 | $28.44 | -41.80 | -41.80 | +0.00 | -41.80 |
| 2026-08-21 | `DNA` | 217 | $6.96 | $7.09 | +28.21 | $7.40 | +67.27 | +95.48 | -78.12 | -10.85 |
| 2026-08-21 | `MSTR` | 14 | $112.39 | $119.69 | +102.20 | $119.25 | -6.16 | +96.04 | +90.44 | +84.28 |
| 2026-08-21 | `EXK` | 150 | $10.97 | $11.34 | +55.50 | $10.62 | -108.00 | -52.50 | +85.50 | -22.50 |
| 2026-08-21 | `SCZM` | 171 | $9.76 | $10.26 | +85.50 | $9.68 | -100.03 | -14.53 | +136.80 | +36.76 |
| 2026-08-21 | `NG` | 193 | $8.66 | $9.02 | +69.48 | $8.72 | -57.90 | +11.58 | +123.52 | +65.62 |
| 2026-08-21 | `BLSH` | 55 | $28.44 | $29.75 | +72.05 | $30.41 | +36.30 | +108.35 | +30.25 | +66.55 |
| 2026-08-21 | `BTBT` | 4 | — | $1.66 | +0.00 | $1.53 | -0.52 | -0.52 | +0.00 | -0.52 |
| 2026-08-21 | `ORBS` | 7 | — | $0.86 | +0.00 | $0.88 | +0.11 | +0.11 | +0.00 | +0.11 |
| 2026-08-21 | `GORO` | 2 | — | $3.11 | +0.00 | $3.19 | +0.16 | +0.16 | +0.00 | +0.16 |
| 2026-08-24 | `DNA` | 217 | $7.40 | $7.26 | -30.38 | $6.98 | -60.76 | -91.14 | -41.23 | -101.99 |
| 2026-08-24 | `MSTR` | 14 | $119.25 | $121.76 | +35.14 | $124.59 | +39.62 | +74.76 | +119.42 | +159.04 |
| 2026-08-24 | `EXK` | 150 | $10.62 | $11.01 | +58.50 | $10.74 | -40.50 | +18.00 | +36.00 | -4.50 |
| 2026-08-24 | `SCZM` | 171 | $9.68 | $9.82 | +24.79 | $9.53 | -49.59 | -24.80 | +61.56 | +11.97 |
| 2026-08-24 | `NG` | 193 | $8.72 | $8.89 | +32.81 | $9.24 | +67.55 | +100.36 | +98.43 | +165.98 |
| 2026-08-24 | `BLSH` | 55 | $30.41 | $30.18 | -12.65 | $30.88 | +38.50 | +25.85 | +53.90 | +92.40 |
| 2026-08-24 | `BTBT` | 4 | $1.53 | $1.55 | +0.08 | $1.56 | +0.04 | +0.12 | -0.44 | -0.40 |
| 2026-08-24 | `ORBS` | 7 | $0.88 | $0.89 | +0.07 | $0.85 | -0.28 | -0.21 | +0.18 | -0.10 |
| 2026-08-24 | `GORO` | 2 | $3.19 | $3.20 | +0.02 | $3.57 | +0.74 | +0.76 | +0.18 | +0.92 |
| 2026-08-25 | `DNA` | 217 | $6.98 | $6.82 | -34.72 | — | +0.00 | -34.72 | -136.71 | — |
| 2026-08-25 | `MSTR` | 14 | $124.59 | $125.56 | +13.58 | — | +0.00 | +13.58 | +172.62 | — |
| 2026-08-25 | `EXK` | 150 | $10.74 | $10.72 | -3.00 | — | +0.00 | -3.00 | -7.50 | — |
| 2026-08-25 | `SCZM` | 171 | $9.53 | $9.57 | +6.84 | — | +0.00 | +6.84 | +18.81 | — |
| 2026-08-25 | `NG` | 193 | $9.24 | $9.34 | +19.30 | — | +0.00 | +19.30 | +185.28 | — |
| 2026-08-25 | `BLSH` | 55 | $30.88 | $31.00 | +6.60 | — | +0.00 | +6.60 | +99.00 | — |
| 2026-08-25 | `BTBT` | 4 | $1.56 | $1.55 | -0.04 | $1.53 | -0.08 | -0.12 | -0.44 | -0.52 |
| 2026-08-25 | `ORBS` | 7 | $0.85 | $0.85 | +0.00 | $0.84 | -0.07 | -0.07 | -0.10 | -0.17 |
| 2026-08-25 | `GORO` | 2 | $3.57 | $3.53 | -0.08 | $3.56 | +0.06 | -0.02 | +0.84 | +0.90 |
| 2026-08-25 | `NPWR` | 1248 | — | $2.00 | +0.00 | $2.02 | +24.96 | +24.96 | +0.00 | +24.96 |
| 2026-08-25 | `ALVO` | 478 | — | $5.22 | +0.00 | $5.25 | +14.34 | +14.34 | +0.00 | +14.34 |
| 2026-08-25 | `ALIT` | 168 | — | $14.86 | +0.00 | $14.87 | +1.68 | +1.68 | +0.00 | +1.68 |
| 2026-08-25 | `ZURA` | 387 | — | $6.38 | +0.00 | $6.50 | +46.44 | +46.44 | +0.00 | +46.44 |
| 2026-08-26 | `BTBT` | 4 | $1.53 | $1.53 | +0.00 | $1.53 | +0.00 | +0.00 | -0.52 | -0.52 |
| 2026-08-26 | `ORBS` | 7 | $0.84 | $0.84 | +0.00 | $0.84 | +0.00 | +0.00 | -0.17 | -0.17 |
| 2026-08-26 | `GORO` | 2 | $3.56 | $3.56 | +0.00 | $3.56 | +0.00 | +0.00 | +0.90 | +0.90 |
| 2026-08-26 | `NPWR` | 1248 | $2.02 | $2.02 | +0.00 | $2.02 | +0.00 | +0.00 | +24.96 | +24.96 |
| 2026-08-26 | `ALVO` | 478 | $5.25 | $5.25 | +0.00 | $5.25 | +0.00 | +0.00 | +14.34 | +14.34 |
| 2026-08-26 | `ALIT` | 168 | $14.87 | $14.87 | +0.00 | $14.87 | +0.00 | +0.00 | +1.68 | +1.68 |
| 2026-08-26 | `ZURA` | 387 | $6.50 | $6.50 | +0.00 | $6.50 | +0.00 | +0.00 | +46.44 | +46.44 |
| 2026-08-27 | `BTBT` | 4 | $1.53 | $1.53 | +0.00 | — | +0.00 | +0.00 | -0.52 | — |
| 2026-08-27 | `ORBS` | 7 | $0.84 | $0.80 | -0.28 | — | +0.00 | -0.28 | -0.45 | — |
| 2026-08-27 | `GORO` | 2 | $3.56 | $3.77 | +0.42 | — | +0.00 | +0.42 | +1.32 | — |
| 2026-08-27 | `NPWR` | 1248 | $2.02 | $1.93 | -112.32 | $1.81 | -149.76 | -262.08 | -87.36 | -237.12 |
| 2026-08-27 | `ALVO` | 478 | $5.25 | $4.98 | -129.06 | $4.91 | -33.46 | -162.52 | -114.72 | -148.18 |
| 2026-08-27 | `ALIT` | 168 | $14.87 | $14.85 | -3.36 | $14.33 | -87.36 | -90.72 | -1.68 | -89.04 |
| 2026-08-27 | `ZURA` | 387 | $6.50 | $6.13 | -143.19 | $5.99 | -54.18 | -197.37 | -96.75 | -150.93 |
| 2026-08-28 | `NPWR` | 1248 | $1.81 | $1.83 | +24.96 | — | +0.00 | +24.96 | -212.16 | — |
| 2026-08-28 | `ALVO` | 478 | $4.91 | $4.88 | -14.34 | — | +0.00 | -14.34 | -162.52 | — |
| 2026-08-28 | `ALIT` | 168 | $14.33 | $14.54 | +35.28 | — | +0.00 | +35.28 | -53.76 | — |
| 2026-08-28 | `ZURA` | 387 | $5.99 | $6.02 | +11.61 | — | +0.00 | +11.61 | -139.32 | — |
| 2026-08-28 | `ANF` | 12 | — | $144.70 | +0.00 | $145.75 | +12.60 | +12.60 | +0.00 | +12.60 |
| 2026-08-28 | `BHVN` | 110 | — | $16.95 | +0.00 | $16.12 | -91.30 | -91.30 | +0.00 | -91.30 |
| 2026-08-28 | `BZ` | 101 | — | $18.50 | +0.00 | $18.00 | -50.50 | -50.50 | +0.00 | -50.50 |
| 2026-08-28 | `LVWR` | 1359 | — | $1.38 | +0.00 | $1.36 | -27.18 | -27.18 | +0.00 | -27.18 |
| 2026-08-28 | `GRRR` | 117 | — | $15.94 | +0.00 | $15.66 | -32.76 | -32.76 | +0.00 | -32.76 |
| 2026-08-31 | `ANF` | 12 | $145.75 | $148.67 | +35.04 | $149.28 | +7.32 | +42.36 | +47.64 | +54.96 |
| 2026-08-31 | `BHVN` | 110 | $16.12 | $15.44 | -74.80 | $15.40 | -4.40 | -79.20 | -166.10 | -170.50 |
| 2026-08-31 | `BZ` | 101 | $18.00 | $17.89 | -11.11 | $17.90 | +1.01 | -10.10 | -61.61 | -60.60 |
| 2026-08-31 | `LVWR` | 1359 | $1.36 | $1.37 | +13.59 | $1.34 | -40.77 | -27.18 | -13.59 | -54.36 |
| 2026-08-31 | `GRRR` | 117 | $15.66 | $14.32 | -156.78 | $14.20 | -14.04 | -170.82 | -189.54 | -203.58 |
| 2026-09-01 | `ANF` | 12 | $149.28 | $142.47 | -81.72 | $143.00 | +6.36 | -75.36 | -26.76 | -20.40 |
| 2026-09-01 | `BHVN` | 110 | $15.40 | $15.45 | +5.50 | $15.45 | +0.00 | +5.50 | -165.00 | -165.00 |
| 2026-09-01 | `BZ` | 101 | $17.90 | $17.37 | -53.53 | $17.17 | -20.20 | -73.73 | -114.13 | -134.33 |
| 2026-09-01 | `LVWR` | 1359 | $1.34 | $1.22 | -163.08 | $1.18 | -54.36 | -217.44 | -217.44 | -271.80 |
| 2026-09-01 | `GRRR` | 117 | $14.20 | $15.05 | +99.45 | $14.80 | -29.25 | +70.20 | -104.13 | -133.38 |
| 2026-09-02 | `ANF` | 12 | $143.00 | $142.00 | -12.00 | — | +0.00 | -12.00 | -32.40 | — |
| 2026-09-02 | `BHVN` | 110 | $15.45 | $15.39 | -6.60 | — | +0.00 | -6.60 | -171.60 | — |
| 2026-09-02 | `BZ` | 101 | $17.17 | $17.29 | +12.12 | — | +0.00 | +12.12 | -122.21 | — |
| 2026-09-02 | `LVWR` | 1359 | $1.18 | $1.19 | +13.59 | — | +0.00 | +13.59 | -258.21 | — |
| 2026-09-02 | `GRRR` | 117 | $14.80 | $14.75 | -5.85 | — | +0.00 | -5.85 | -139.23 | — |
| 2026-09-03 | `GPRO` | 2350 | — | $1.22 | +0.00 | $1.69 | +1104.50 | +1104.50 | +0.00 | +1104.50 |
| 2026-09-03 | `CRK` | 182 | — | $15.70 | +0.00 | $15.54 | -29.12 | -29.12 | +0.00 | -29.12 |
| 2026-09-03 | `MMED` | 124 | — | $22.78 | +0.00 | $23.76 | +121.52 | +121.52 | +0.00 | +121.52 |
| 2026-09-04 | `GPRO` | 2350 | $1.69 | $1.78 | +211.50 | $1.39 | -916.50 | -705.00 | +1316.00 | +399.50 |
| 2026-09-04 | `CRK` | 182 | $15.54 | $15.45 | -16.38 | $14.95 | -91.00 | -107.38 | -45.50 | -136.50 |
| 2026-09-04 | `MMED` | 124 | $23.76 | $23.88 | +14.88 | $23.84 | -4.96 | +9.92 | +136.40 | +131.44 |
| 2026-09-04 | `BAK` | 3 | — | $1.95 | +0.00 | $1.94 | -0.03 | -0.03 | +0.00 | -0.03 |
| 2026-09-04 | `EOSE` | 1 | — | $3.57 | +0.00 | $3.50 | -0.07 | -0.07 | +0.00 | -0.07 |
| 2026-09-07 | `GPRO` | 2350 | $1.39 | $1.48 | +211.50 | $1.70 | +517.00 | +728.50 | +611.00 | +1128.00 |
| 2026-09-07 | `CRK` | 182 | $14.95 | $15.00 | +9.10 | $15.26 | +47.32 | +56.42 | -127.40 | -80.08 |
| 2026-09-07 | `MMED` | 124 | $23.84 | $23.84 | +0.00 | $23.28 | -69.44 | -69.44 | +131.44 | +62.00 |
| 2026-09-07 | `BAK` | 3 | $1.94 | $1.94 | +0.00 | $1.89 | -0.15 | -0.15 | -0.03 | -0.18 |
| 2026-09-07 | `EOSE` | 1 | $3.50 | $3.52 | +0.02 | $3.88 | +0.36 | +0.38 | -0.05 | +0.31 |
| 2026-09-07 | `CHGG` | 2 | — | $0.95 | +0.00 | $0.85 | -0.20 | -0.20 | +0.00 | -0.20 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -38.70 | ANGX, HYLN, WDC, ADUR, ALGM | — | $493.79 | $9,942.67 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 |
| 2026-08-17 | +2.25 | $493.79 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 | $10,107.31 | +164.64 | +39.81 | CDNL, ABX, VERA, OCC, ALM | — | $111.60 | $10,143.27 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 |
| 2026-08-18 | -6.20 | $111.60 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 | $9,860.11 | -283.16 | -122.04 | — | — | $111.60 | $9,738.07 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 |
| 2026-08-19 | -7.20 | $111.60 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 | $9,742.74 | +4.67 | -10.33 | — | ANGX, HYLN, WDC, ADUR, ALGM | $9,341.61 | $9,713.51 | CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 |
| 2026-08-20 | +1.12 | $9,341.61 | CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 | $9,710.08 | -3.43 | -24.55 | DNA, MSTR, EXK, SCZM, NG, BLSH | CDNL, ABX, VERA, OCC, ALM | $33.36 | $9,667.18 | DNA×217, MSTR×14, EXK×150, SCZM×171, NG×193, BLSH×55 |
| 2026-08-21 | +3.25 | $33.36 | DNA×217, MSTR×14, EXK×150, SCZM×171, NG×193, BLSH×55 | $10,080.12 | +412.94 | -168.77 | BTBT, ORBS, GORO | — | $14.23 | $9,911.12 | DNA×217, MSTR×14, EXK×150, SCZM×171, NG×193, BLSH×55, BTBT×4, ORBS×7, GORO×2 |
| 2026-08-24 | -5.17 | $14.23 | DNA×217, MSTR×14, EXK×150, SCZM×171, NG×193, BLSH×55, BTBT×4, ORBS×7, GORO×2 | $10,019.51 | +108.39 | -4.68 | — | — | $14.23 | $10,014.83 | DNA×217, MSTR×14, EXK×150, SCZM×171, NG×193, BLSH×55, BTBT×4, ORBS×7, GORO×2 |
| 2026-08-25 | +1.80 | $14.23 | DNA×217, MSTR×14, EXK×150, SCZM×171, NG×193, BLSH×55, BTBT×4, ORBS×7, GORO×2 | $10,023.31 | +8.48 | +87.33 | NPWR, ALVO, ALIT, ZURA | DNA, MSTR, EXK, SCZM, NG, BLSH | $2.93 | $10,066.17 | BTBT×4, ORBS×7, GORO×2, NPWR×1248, ALVO×478, ALIT×168, ZURA×387 |
| 2026-08-26 | +2.02 | $2.93 | BTBT×4, ORBS×7, GORO×2, NPWR×1248, ALVO×478, ALIT×168, ZURA×387 | $10,066.17 | -0.00 | +0.00 | — | — | $2.93 | $10,066.17 | BTBT×4, ORBS×7, GORO×2, NPWR×1248, ALVO×478, ALIT×168, ZURA×387 |
| 2026-08-27 | — | $2.93 | BTBT×4, ORBS×7, GORO×2, NPWR×1248, ALVO×478, ALIT×168, ZURA×387 | $9,678.38 | -387.79 | -324.76 | — | BTBT, ORBS, GORO | $21.89 | $9,353.32 | NPWR×1248, ALVO×478, ALIT×168, ZURA×387 |
| 2026-08-28 | +0.75 | $21.89 | NPWR×1248, ALVO×478, ALIT×168, ZURA×387 | $9,410.83 | +57.51 | -189.14 | ANF, BHVN, BZ, LVWR, GRRR | NPWR, ALVO, ALIT, ZURA | $144.32 | $9,164.98 | ANF×12, BHVN×110, BZ×101, LVWR×1359, GRRR×117 |
| 2026-08-31 | -5.85 | $144.32 | ANF×12, BHVN×110, BZ×101, LVWR×1359, GRRR×117 | $8,970.92 | -194.06 | -50.88 | — | — | $144.32 | $8,920.04 | ANF×12, BHVN×110, BZ×101, LVWR×1359, GRRR×117 |
| 2026-09-01 | -6.30 | $144.32 | ANF×12, BHVN×110, BZ×101, LVWR×1359, GRRR×117 | $8,726.66 | -193.38 | -97.45 | — | — | $144.32 | $8,629.21 | ANF×12, BHVN×110, BZ×101, LVWR×1359, GRRR×117 |
| 2026-09-02 | -3.83 | $144.32 | ANF×12, BHVN×110, BZ×101, LVWR×1359, GRRR×117 | $8,630.47 | +1.26 | +0.00 | — | ANF, BHVN, BZ, LVWR, GRRR | $8,603.60 | $8,603.60 | — |
| 2026-09-03 | -0.90 | $8,603.60 | — | $8,603.60 | -0.00 | +1,196.90 | GPRO, CRK, MMED | — | $19.27 | $9,765.29 | GPRO×2350, CRK×182, MMED×124 |
| 2026-09-04 | — | $19.27 | GPRO×2350, CRK×182, MMED×124 | $9,975.29 | +210.00 | -1,012.56 | BAK, EOSE | — | $9.74 | $8,962.62 | GPRO×2350, CRK×182, MMED×124, BAK×3, EOSE×1 |
| 2026-09-07 | — | $9.74 | GPRO×2350, CRK×182, MMED×124, BAK×3, EOSE×1 | $9,183.24 | +220.62 | +494.89 | CHGG | — | $7.81 | $9,678.10 | GPRO×2350, CRK×182, MMED×124, BAK×3, EOSE×1, CHGG×2 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $7,994.17 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 478 | $4.18 | $6.17 | — | $5,989.97 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 3 | $503.50 | $2.00 | — | $4,477.47 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ⚪; ret5=+7.9; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 121 | $16.50 | $2.35 | — | $2,478.62 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 45 | $44.06 | $2.12 | — | $493.79 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+3.9; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $493.79 | ▼ close $9,942.67 vs 09:30 $10,000.00 (session -38.70) | 16:00 close · cash $493.79 · equity $9,942.67 vs 09:30 $10,000.00 (-57.33; session marks -38.70) · 5 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.31 → close $4.37 +27.84; HYLN×478 09:30 $4.18 → close $4.06 -57.36; WDC×3 09:30 $503.50 → close $508.80 +15.90; ADUR×121 09:30 $16.50 → close $16.17 -39.93; ALGM×45 09:30 $44.06 → close $44.39 +14.85 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $493.79 | ▲ 09:30 equity $10,107.31 vs yday $9,942.67 (+164.64) | 09:30 open · cash $493.79 (unchanged overnight, no fees) · equity $10,107.31 vs prior close $9,942.67 (+164.64) · 5 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; HYLN×478 yday $4.06 → 09:30 $4.10 +19.12; WDC×3 yday $508.80 → 09:30 $525.53 +50.19; ADUR×121 yday $16.17 → 09:30 $15.73 -53.24; ALGM×45 yday $44.39 → 09:30 $45.32 +41.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 2 | $39.85 | $0.80 | — | $413.29 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $82.30 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 9 | $9.12 | $0.85 | — | $330.36 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $82.30 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 2 | $31.30 | $0.63 | — | $267.13 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-3.8; leftover $82.30 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 4 | $18.24 | $0.74 | — | $193.43 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $82.30 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 5 | $16.20 | $0.82 | — | $111.60 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $82.30 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.60 | ▲ close $10,143.27 vs 09:30 $10,107.31 (session +39.81) | 16:00 close · cash $111.60 · equity $10,143.27 vs 09:30 $10,107.31 (+35.96; session marks +39.81) · 10 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.60 → close $4.71 +51.04; HYLN×478 09:30 $4.10 → close $4.09 -4.78; WDC×3 09:30 $525.53 → close $536.01 +31.44; ADUR×121 09:30 $15.73 → close $15.85 +14.52; ALGM×45 09:30 $45.32 → close $44.25 -48.15; CDNL×2 09:30 $39.85 → close $39.23 -1.24; ABX×9 09:30 $9.12 → close $9.12 +0.00; VERA×2 09:30 $31.30 → close $31.63 +0.66; OCC×4 09:30 $18.24 → close $17.12 -4.48; ALM×5 09:30 $16.20 → close $16.36 +0.80 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.60 | ▼ 09:30 equity $9,860.11 vs yday $10,143.27 (-283.16) | 09:30 open · cash $111.60 (unchanged overnight, no fees) · equity $9,860.11 vs prior close $10,143.27 (-283.16) · 10 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.71 → 09:30 $4.79 +37.12; HYLN×478 yday $4.09 → 09:30 $3.95 -66.92; WDC×3 yday $536.01 → 09:30 $496.07 -119.82; ADUR×121 yday $15.85 → 09:30 $15.41 -53.24; ALGM×45 yday $44.25 → 09:30 $42.54 -76.95; CDNL×2 yday $39.23 → 09:30 $41.57 +4.68; ABX×9 yday $9.12 → 09:30 $9.03 -0.81; VERA×2 yday $31.63 → 09:30 $31.31 -0.64; OCC×4 yday $17.12 → 09:30 $16.20 -3.68; ALM×5 yday $16.36 → 09:30 $15.78 -2.90 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.60 | ▼ close $9,738.07 vs 09:30 $9,860.11 (session -122.04) | 16:00 close · cash $111.60 · equity $9,738.07 vs 09:30 $9,860.11 (-122.04; session marks -122.04) · 10 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.79 → close $4.85 +27.84; HYLN×478 09:30 $3.95 → close $3.86 -43.02; WDC×3 09:30 $496.07 → close $496.16 +0.27; ADUR×121 09:30 $15.41 → close $15.63 +26.62; ALGM×45 09:30 $42.54 → close $39.39 -141.75; CDNL×2 09:30 $41.57 → close $45.14 +7.14; ABX×9 09:30 $9.03 → close $9.01 -0.18; VERA×2 09:30 $31.31 → close $32.28 +1.94; OCC×4 09:30 $16.20 → close $16.20 +0.00; ALM×5 09:30 $15.78 → close $15.60 -0.90 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.60 | ▲ 09:30 equity $9,742.74 vs yday $9,738.07 (+4.67) | 09:30 open · cash $111.60 (unchanged overnight, no fees) · equity $9,742.74 vs prior close $9,738.07 (+4.67) · 10 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.85 → 09:30 $4.79 -27.84; HYLN×478 yday $3.86 → 09:30 $3.87 +4.78; WDC×3 yday $496.16 → 09:30 $494.28 -5.64; ADUR×121 yday $15.63 → 09:30 $15.65 +2.42; ALGM×45 yday $39.39 → 09:30 $40.00 +27.45; CDNL×2 yday $45.14 → 09:30 $44.83 -0.62; ABX×9 yday $9.01 → 09:30 $9.08 +0.63; VERA×2 yday $32.28 → 09:30 $32.88 +1.20; OCC×4 yday $16.20 → 09:30 $16.21 +0.04; ALM×5 yday $15.60 → 09:30 $16.05 +2.25 | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 464 | $4.79 | $6.08 | $+210.65 | $2,328.08 | ▲ +210.65 after sell → book $9,736.66; vs 09:30 mark -6.08 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 478 | $3.87 | $6.26 | $-160.61 | $4,171.68 | ▼ -160.61 after sell → book $9,730.40; vs 09:30 mark -6.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `WDC` | 3 | $494.28 | $2.02 | $-31.68 | $5,652.50 | ▼ -31.68 after sell → book $9,728.38; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ADUR` | 121 | $15.65 | $2.39 | $-107.59 | $7,543.76 | ▼ -107.59 after sell → book $9,725.99; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ALGM` | 45 | $40.00 | $2.15 | $-186.97 | $9,341.61 | ▼ -186.97 after sell → book $9,723.84; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,341.61 | ▼ close $9,713.51 vs 09:30 $9,742.74 (session -10.33) | 16:00 close · cash $9,341.61 · equity $9,713.51 vs 09:30 $9,742.74 (-29.23; session marks -10.33) · 5 name(s) marked open→close (per-name table). CDNL×2 09:30 $44.83 → close $43.33 -3.00; ABX×9 09:30 $9.08 → close $9.15 +0.63; VERA×2 09:30 $32.88 → close $32.27 -1.21; OCC×4 09:30 $16.21 → close $14.36 -7.40; ALM×5 09:30 $16.05 → close $16.18 +0.65 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,341.61 | ▼ 09:30 equity $9,710.08 vs yday $9,713.51 (-3.43) | 09:30 open · cash $9,341.61 (unchanged overnight, no fees) · equity $9,710.08 vs prior close $9,713.51 (-3.43) · 5 name(s) re-marked at the open (per-name table). CDNL×2 yday $43.33 → 09:30 $43.13 -0.40; ABX×9 yday $9.15 → 09:30 $9.13 -0.18; VERA×2 yday $32.27 → 09:30 $32.30 +0.04; OCC×4 yday $14.36 → 09:30 $14.10 -1.04; ALM×5 yday $16.18 → 09:30 $15.81 -1.85 | — |
| 2026-08-20 09:30 ET | **SELL** | `CDNL` | 2 | $43.13 | $0.89 | $+4.87 | $9,426.98 | ▲ +4.87 after sell → book $9,709.19; vs 09:30 mark -0.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ABX` | 9 | $9.13 | $0.87 | $-1.63 | $9,508.29 | ▼ -1.63 after sell → book $9,708.33; vs 09:30 mark -0.86 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `VERA` | 2 | $32.30 | $0.67 | $+0.69 | $9,572.20 | ▲ +0.69 after sell → book $9,707.65; vs 09:30 mark -0.68 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `OCC` | 4 | $14.10 | $0.60 | $-17.90 | $9,628.01 | ▼ -17.90 after sell → book $9,707.06; vs 09:30 mark -0.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ALM` | 5 | $15.81 | $0.83 | $-3.60 | $9,706.23 | ▼ -3.60 after sell → book $9,706.23; vs 09:30 mark -0.83 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 217 | $7.45 | $2.80 | — | $8,086.78 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1617.71 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 14 | $113.23 | $2.03 | — | $6,499.53 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1617.71 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 150 | $10.77 | $2.44 | — | $4,881.59 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1617.71 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 171 | $9.46 | $2.50 | — | $3,261.43 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1617.71 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 193 | $8.38 | $2.57 | — | $1,641.52 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1617.71 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 55 | $29.20 | $2.15 | — | $33.36 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1617.71 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.36 | ▼ close $9,667.18 vs 09:30 $9,710.08 (session -24.55) | 16:00 close · cash $33.36 · equity $9,667.18 vs 09:30 $9,710.08 (-42.90; session marks -24.55) · 6 name(s) marked open→close (per-name table). DNA×217 09:30 $7.45 → close $6.96 -106.33; MSTR×14 09:30 $113.23 → close $112.39 -11.76; EXK×150 09:30 $10.77 → close $10.97 +30.00; SCZM×171 09:30 $9.46 → close $9.76 +51.30; NG×193 09:30 $8.38 → close $8.66 +54.04; BLSH×55 09:30 $29.20 → close $28.44 -41.80 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.36 | ▲ 09:30 equity $10,080.12 vs yday $9,667.18 (+412.94) | 09:30 open · cash $33.36 (unchanged overnight, no fees) · equity $10,080.12 vs prior close $9,667.18 (+412.94) · 6 name(s) re-marked at the open (per-name table). DNA×217 yday $6.96 → 09:30 $7.09 +28.21; MSTR×14 yday $112.39 → 09:30 $119.69 +102.20; EXK×150 yday $10.97 → 09:30 $11.34 +55.50; SCZM×171 yday $9.76 → 09:30 $10.26 +85.50; NG×193 yday $8.66 → 09:30 $9.02 +69.48; BLSH×55 yday $28.44 → 09:30 $29.75 +72.05 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 4 | $1.66 | $0.08 | — | $26.65 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $6.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 7 | $0.86 | $0.08 | — | $20.52 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $6.67 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 2 | $3.11 | $0.07 | — | $14.23 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+7.1; leftover $6.67 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.23 | ▼ close $9,911.12 vs 09:30 $10,080.12 (session -168.77) | 16:00 close · cash $14.23 · equity $9,911.12 vs 09:30 $10,080.12 (-169.00; session marks -168.77) · 9 name(s) marked open→close (per-name table). DNA×217 09:30 $7.09 → close $7.40 +67.27; MSTR×14 09:30 $119.69 → close $119.25 -6.16; EXK×150 09:30 $11.34 → close $10.62 -108.00; SCZM×171 09:30 $10.26 → close $9.68 -100.03; NG×193 09:30 $9.02 → close $8.72 -57.90; BLSH×55 09:30 $29.75 → close $30.41 +36.30; BTBT×4 09:30 $1.66 → close $1.53 -0.52; ORBS×7 09:30 $0.86 → close $0.88 +0.11; GORO×2 09:30 $3.11 → close $3.19 +0.16 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.23 | ▲ 09:30 equity $10,019.51 vs yday $9,911.12 (+108.39) | 09:30 open · cash $14.23 (unchanged overnight, no fees) · equity $10,019.51 vs prior close $9,911.12 (+108.39) · 9 name(s) re-marked at the open (per-name table). DNA×217 yday $7.40 → 09:30 $7.26 -30.38; MSTR×14 yday $119.25 → 09:30 $121.76 +35.14; EXK×150 yday $10.62 → 09:30 $11.01 +58.50; SCZM×171 yday $9.68 → 09:30 $9.82 +24.79; NG×193 yday $8.72 → 09:30 $8.89 +32.81; BLSH×55 yday $30.41 → 09:30 $30.18 -12.65; BTBT×4 yday $1.53 → 09:30 $1.55 +0.08; ORBS×7 yday $0.88 → 09:30 $0.89 +0.07; GORO×2 yday $3.19 → 09:30 $3.20 +0.02 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.23 | ▼ close $10,014.83 vs 09:30 $10,019.51 (session -4.68) | 16:00 close · cash $14.23 · equity $10,014.83 vs 09:30 $10,019.51 (-4.68; session marks -4.68) · 9 name(s) marked open→close (per-name table). DNA×217 09:30 $7.26 → close $6.98 -60.76; MSTR×14 09:30 $121.76 → close $124.59 +39.62; EXK×150 09:30 $11.01 → close $10.74 -40.50; SCZM×171 09:30 $9.82 → close $9.53 -49.59; NG×193 09:30 $8.89 → close $9.24 +67.55; BLSH×55 09:30 $30.18 → close $30.88 +38.50; BTBT×4 09:30 $1.55 → close $1.56 +0.04; ORBS×7 09:30 $0.89 → close $0.85 -0.28; GORO×2 09:30 $3.20 → close $3.57 +0.74 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.23 | ▲ 09:30 equity $10,023.31 vs yday $10,014.83 (+8.48) | 09:30 open · cash $14.23 (unchanged overnight, no fees) · equity $10,023.31 vs prior close $10,014.83 (+8.48) · 9 name(s) re-marked at the open (per-name table). DNA×217 yday $6.98 → 09:30 $6.82 -34.72; MSTR×14 yday $124.59 → 09:30 $125.56 +13.58; EXK×150 yday $10.74 → 09:30 $10.72 -3.00; SCZM×171 yday $9.53 → 09:30 $9.57 +6.84; NG×193 yday $9.24 → 09:30 $9.34 +19.30; BLSH×55 yday $30.88 → 09:30 $31.00 +6.60; BTBT×4 yday $1.56 → 09:30 $1.55 -0.04; ORBS×7 yday $0.85 → 09:30 $0.85 +0.00; GORO×2 yday $3.57 → 09:30 $3.53 -0.08 | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 217 | $6.82 | $2.85 | $-142.36 | $1,491.32 | ▼ -142.36 after sell → book $10,020.46; vs 09:30 mark -2.85 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MSTR` | 14 | $125.56 | $2.06 | $+168.53 | $3,247.10 | ▲ +168.53 after sell → book $10,018.40; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 150 | $10.72 | $2.48 | $-12.42 | $4,852.63 | ▼ -12.42 after sell → book $10,015.93; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SCZM` | 171 | $9.57 | $2.54 | $+13.76 | $6,486.55 | ▲ +13.76 after sell → book $10,013.38; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NG` | 193 | $9.34 | $2.62 | $+180.10 | $8,286.56 | ▲ +180.10 after sell → book $10,010.77; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BLSH` | 55 | $31.00 | $2.18 | $+94.67 | $9,989.38 | ▲ +94.67 after sell → book $10,008.59; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 1248 | $2.00 | $16.10 | — | $7,477.28 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $2497.34 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 478 | $5.22 | $6.17 | — | $4,975.95 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $2497.34 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 168 | $14.86 | $2.49 | — | $2,476.98 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $2497.34 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 387 | $6.38 | $4.99 | — | $2.93 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $2497.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.93 | ▲ close $10,066.17 vs 09:30 $10,023.31 (session +87.33) | 16:00 close · cash $2.93 · equity $10,066.17 vs 09:30 $10,023.31 (+42.86; session marks +87.33) · 7 name(s) marked open→close (per-name table). BTBT×4 09:30 $1.55 → close $1.53 -0.08; ORBS×7 09:30 $0.85 → close $0.84 -0.07; GORO×2 09:30 $3.53 → close $3.56 +0.06; NPWR×1248 09:30 $2.00 → close $2.02 +24.96; ALVO×478 09:30 $5.22 → close $5.25 +14.34; ALIT×168 09:30 $14.86 → close $14.87 +1.68; ZURA×387 09:30 $6.38 → close $6.50 +46.44 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.93 | ▲ 09:30 equity $10,066.17 vs yday $10,066.17 (-0.00) | 09:30 open · cash $2.93 (unchanged overnight, no fees) · equity $10,066.17 vs prior close $10,066.17 (-0.00) · 7 name(s) re-marked at the open (per-name table). BTBT×4 yday $1.53 → 09:30 $1.53 +0.00; ORBS×7 yday $0.84 → 09:30 $0.84 +0.00; GORO×2 yday $3.56 → 09:30 $3.56 +0.00; NPWR×1248 yday $2.02 → 09:30 $2.02 +0.00; ALVO×478 yday $5.25 → 09:30 $5.25 +0.00; ALIT×168 yday $14.87 → 09:30 $14.87 +0.00; ZURA×387 yday $6.50 → 09:30 $6.50 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.93 | ▲ close $10,066.17 vs 09:30 $10,066.17 (session +0.00) | 16:00 close · cash $2.93 · equity $10,066.17 vs 09:30 $10,066.17 (-0.00; session marks +0.00) · 7 name(s) marked open→close (per-name table). BTBT×4 09:30 $1.53 → close $1.53 +0.00; ORBS×7 09:30 $0.84 → close $0.84 +0.00; GORO×2 09:30 $3.56 → close $3.56 +0.00; NPWR×1248 09:30 $2.02 → close $2.02 +0.00; ALVO×478 09:30 $5.25 → close $5.25 +0.00; ALIT×168 09:30 $14.87 → close $14.87 +0.00; ZURA×387 09:30 $6.50 → close $6.50 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.93 | ▼ 09:30 equity $9,678.38 vs yday $10,066.17 (-387.79) | 09:30 open · cash $2.93 (unchanged overnight, no fees) · equity $9,678.38 vs prior close $10,066.17 (-387.79) · 7 name(s) re-marked at the open (per-name table). BTBT×4 yday $1.53 → 09:30 $1.53 +0.00; ORBS×7 yday $0.84 → 09:30 $0.80 -0.28; GORO×2 yday $3.56 → 09:30 $3.77 +0.42; NPWR×1248 yday $2.02 → 09:30 $1.93 -112.32; ALVO×478 yday $5.25 → 09:30 $4.98 -129.06; ALIT×168 yday $14.87 → 09:30 $14.85 -3.36; ZURA×387 yday $6.50 → 09:30 $6.13 -143.19 | — |
| 2026-08-27 09:30 ET | **SELL** | `BTBT` | 4 | $1.53 | $0.09 | $-0.69 | $8.95 | ▼ -0.69 after sell → book $9,678.28; vs 09:30 mark -0.10 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ORBS` | 7 | $0.80 | $0.10 | $-0.63 | $14.46 | ▼ -0.63 after sell → book $9,678.19; vs 09:30 mark -0.09 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `GORO` | 2 | $3.77 | $0.10 | $+1.15 | $21.89 | ▲ +1.15 after sell → book $9,678.08; vs 09:30 mark -0.11 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.89 | ▼ close $9,353.32 vs 09:30 $9,678.38 (session -324.76) | 16:00 close · cash $21.89 · equity $9,353.32 vs 09:30 $9,678.38 (-325.06; session marks -324.76) · 4 name(s) marked open→close (per-name table). NPWR×1248 09:30 $1.93 → close $1.81 -149.76; ALVO×478 09:30 $4.98 → close $4.91 -33.46; ALIT×168 09:30 $14.85 → close $14.33 -87.36; ZURA×387 09:30 $6.13 → close $5.99 -54.18 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.89 | ▲ 09:30 equity $9,410.83 vs yday $9,353.32 (+57.51) | 09:30 open · cash $21.89 (unchanged overnight, no fees) · equity $9,410.83 vs prior close $9,353.32 (+57.51) · 4 name(s) re-marked at the open (per-name table). NPWR×1248 yday $1.81 → 09:30 $1.83 +24.96; ALVO×478 yday $4.91 → 09:30 $4.88 -14.34; ALIT×168 yday $14.33 → 09:30 $14.54 +35.28; ZURA×387 yday $5.99 → 09:30 $6.02 +11.61 | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 1248 | $1.83 | $16.32 | $-244.58 | $2,289.41 | ▼ -244.58 after sell → book $9,394.51; vs 09:30 mark -16.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 478 | $4.88 | $6.26 | $-174.95 | $4,615.79 | ▼ -174.95 after sell → book $9,388.25; vs 09:30 mark -6.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALIT` | 168 | $14.54 | $2.54 | $-58.80 | $7,055.96 | ▼ -58.80 after sell → book $9,385.70; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 387 | $6.02 | $5.08 | $-149.39 | $9,380.63 | ▼ -149.39 after sell → book $9,380.63; vs 09:30 mark -5.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 12 | $144.70 | $2.03 | — | $7,642.20 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1876.13 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 110 | $16.95 | $2.32 | — | $5,775.38 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1876.13 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 101 | $18.50 | $2.29 | — | $3,904.59 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1876.13 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 1359 | $1.38 | $17.53 | — | $2,011.64 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1876.13 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 117 | $15.94 | $2.34 | — | $144.32 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1876.13 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $144.32 | ▼ close $9,164.98 vs 09:30 $9,410.83 (session -189.14) | 16:00 close · cash $144.32 · equity $9,164.98 vs 09:30 $9,410.83 (-245.85; session marks -189.14) · 5 name(s) marked open→close (per-name table). ANF×12 09:30 $144.70 → close $145.75 +12.60; BHVN×110 09:30 $16.95 → close $16.12 -91.30; BZ×101 09:30 $18.50 → close $18.00 -50.50; LVWR×1359 09:30 $1.38 → close $1.36 -27.18; GRRR×117 09:30 $15.94 → close $15.66 -32.76 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $144.32 | ▼ 09:30 equity $8,970.92 vs yday $9,164.98 (-194.06) | 09:30 open · cash $144.32 (unchanged overnight, no fees) · equity $8,970.92 vs prior close $9,164.98 (-194.06) · 5 name(s) re-marked at the open (per-name table). ANF×12 yday $145.75 → 09:30 $148.67 +35.04; BHVN×110 yday $16.12 → 09:30 $15.44 -74.80; BZ×101 yday $18.00 → 09:30 $17.89 -11.11; LVWR×1359 yday $1.36 → 09:30 $1.37 +13.59; GRRR×117 yday $15.66 → 09:30 $14.32 -156.78 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $144.32 | ▼ close $8,920.04 vs 09:30 $8,970.92 (session -50.88) | 16:00 close · cash $144.32 · equity $8,920.04 vs 09:30 $8,970.92 (-50.88; session marks -50.88) · 5 name(s) marked open→close (per-name table). ANF×12 09:30 $148.67 → close $149.28 +7.32; BHVN×110 09:30 $15.44 → close $15.40 -4.40; BZ×101 09:30 $17.89 → close $17.90 +1.01; LVWR×1359 09:30 $1.37 → close $1.34 -40.77; GRRR×117 09:30 $14.32 → close $14.20 -14.04 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $144.32 | ▼ 09:30 equity $8,726.66 vs yday $8,920.04 (-193.38) | 09:30 open · cash $144.32 (unchanged overnight, no fees) · equity $8,726.66 vs prior close $8,920.04 (-193.38) · 5 name(s) re-marked at the open (per-name table). ANF×12 yday $149.28 → 09:30 $142.47 -81.72; BHVN×110 yday $15.40 → 09:30 $15.45 +5.50; BZ×101 yday $17.90 → 09:30 $17.37 -53.53; LVWR×1359 yday $1.34 → 09:30 $1.22 -163.08; GRRR×117 yday $14.20 → 09:30 $15.05 +99.45 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $144.32 | ▼ close $8,629.21 vs 09:30 $8,726.66 (session -97.45) | 16:00 close · cash $144.32 · equity $8,629.21 vs 09:30 $8,726.66 (-97.45; session marks -97.45) · 5 name(s) marked open→close (per-name table). ANF×12 09:30 $142.47 → close $143.00 +6.36; BHVN×110 09:30 $15.45 → close $15.45 +0.00; BZ×101 09:30 $17.37 → close $17.17 -20.20; LVWR×1359 09:30 $1.22 → close $1.18 -54.36; GRRR×117 09:30 $15.05 → close $14.80 -29.25 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $144.32 | ▲ 09:30 equity $8,630.47 vs yday $8,629.21 (+1.26) | 09:30 open · cash $144.32 (unchanged overnight, no fees) · equity $8,630.47 vs prior close $8,629.21 (+1.26) · 5 name(s) re-marked at the open (per-name table). ANF×12 yday $143.00 → 09:30 $142.00 -12.00; BHVN×110 yday $15.45 → 09:30 $15.39 -6.60; BZ×101 yday $17.17 → 09:30 $17.29 +12.12; LVWR×1359 yday $1.18 → 09:30 $1.19 +13.59; GRRR×117 yday $14.80 → 09:30 $14.75 -5.85 | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 12 | $142.00 | $2.05 | $-36.48 | $1,846.27 | ▼ -36.48 after sell → book $8,628.42; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 110 | $15.39 | $2.35 | $-176.27 | $3,536.82 | ▼ -176.27 after sell → book $8,626.07; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 101 | $17.29 | $2.32 | $-126.83 | $5,280.78 | ▼ -126.83 after sell → book $8,623.74; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LVWR` | 1359 | $1.19 | $17.77 | $-293.51 | $6,880.22 | ▼ -293.51 after sell → book $8,605.97; vs 09:30 mark -17.77 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 117 | $14.75 | $2.37 | $-143.95 | $8,603.60 | ▼ -143.95 after sell → book $8,603.60; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,603.60 | ▲ close $8,603.60 vs 09:30 $8,630.47 (session +0.00) | 16:00 close · cash $8,603.60 · no lots left · equity $8,603.60. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,603.60 | ▲ 09:30 equity $8,603.60 vs yday $8,603.60 (-0.00) | 09:30 open · cash $8,603.60 · no holdings · equity $8,603.60 vs prior close $8,603.60 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 2350 | $1.22 | $30.32 | — | $5,706.28 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $2867.87 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 182 | $15.70 | $2.54 | — | $2,846.35 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $2867.87 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 124 | $22.78 | $2.36 | — | $19.27 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $2867.87 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.27 | ▲ close $9,765.29 vs 09:30 $8,603.60 (session +1,196.90) | 16:00 close · cash $19.27 · equity $9,765.29 vs 09:30 $8,603.60 (+1161.69; session marks +1196.90) · 3 name(s) marked open→close (per-name table). GPRO×2350 09:30 $1.22 → close $1.69 +1104.50; CRK×182 09:30 $15.70 → close $15.54 -29.12; MMED×124 09:30 $22.78 → close $23.76 +121.52 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.27 | ▲ 09:30 equity $9,975.29 vs yday $9,765.29 (+210.00) | 09:30 open · cash $19.27 (unchanged overnight, no fees) · equity $9,975.29 vs prior close $9,765.29 (+210.00) · 3 name(s) re-marked at the open (per-name table). GPRO×2350 yday $1.69 → 09:30 $1.78 +211.50; CRK×182 yday $15.54 → 09:30 $15.45 -16.38; MMED×124 yday $23.76 → 09:30 $23.88 +14.88 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 3 | $1.95 | $0.07 | — | $13.35 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $6.42 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 1 | $3.57 | $0.04 | — | $9.74 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $6.42 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.74 | ▼ close $8,962.62 vs 09:30 $9,975.29 (session -1,012.56) | 16:00 close · cash $9.74 · equity $8,962.62 vs 09:30 $9,975.29 (-1012.67; session marks -1012.56) · 5 name(s) marked open→close (per-name table). GPRO×2350 09:30 $1.78 → close $1.39 -916.50; CRK×182 09:30 $15.45 → close $14.95 -91.00; MMED×124 09:30 $23.88 → close $23.84 -4.96; BAK×3 09:30 $1.95 → close $1.94 -0.03; EOSE×1 09:30 $3.57 → close $3.50 -0.07 | — |
| 2026-09-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.74 | ▲ 09:30 equity $9,183.24 vs yday $8,962.62 (+220.62) | 09:30 open · cash $9.74 (unchanged overnight, no fees) · equity $9,183.24 vs prior close $8,962.62 (+220.62) · 5 name(s) re-marked at the open (per-name table). GPRO×2350 yday $1.39 → 09:30 $1.48 +211.50; CRK×182 yday $14.95 → 09:30 $15.00 +9.10; MMED×124 yday $23.84 → 09:30 $23.84 +0.00; BAK×3 yday $1.94 → 09:30 $1.94 +0.00; EOSE×1 yday $3.50 → 09:30 $3.52 +0.02 | — |
| 2026-09-07 09:30 ET | **BUY** | `CHGG` | 2 | $0.95 | $0.03 | — | $7.81 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $2.43 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.81 | ▲ close $9,678.10 vs 09:30 $9,183.24 (session +494.89) | 16:00 close · cash $7.81 · equity $9,678.10 vs 09:30 $9,183.24 (+494.86; session marks +494.89) · 6 name(s) marked open→close (per-name table). GPRO×2350 09:30 $1.48 → close $1.70 +517.00; CRK×182 09:30 $15.00 → close $15.26 +47.32; MMED×124 09:30 $23.84 → close $23.28 -69.44; BAK×3 09:30 $1.94 → close $1.89 -0.15; EOSE×1 09:30 $3.52 → close $3.88 +0.36; CHGG×2 09:30 $0.95 → close $0.85 -0.20 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `WDC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ALGM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CELC` | cash | leftover split 82.30 < 1 share @ 92.99 |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `WDC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ALGM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CDNL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `VERA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OCC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `CDNL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `VERA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OCC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ALM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MSTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BLSH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DE` | cash | leftover split 6.67 < 1 share @ 623.26 |
| 2026-08-21 | `QDEL` | cash | leftover split 6.67 < 1 share @ 14.96 |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BLSH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `BTBT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ORBS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GORO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 6.42 < 1 share @ 486.31 |
| 2026-09-07 | `GPRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-07 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-07 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-07 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-07 | `EOSE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-07 | `CHPT` | cash | leftover split 2.43 < 1 share @ 9.28 |
| 2026-09-07 | `SMMT` | cash | leftover split 2.43 < 1 share @ 16.93 |
| 2026-09-07 | `SNOW` | cash | leftover split 2.43 < 1 share @ 353.63 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 2350 | 2026-09-03 @ $1.22 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $2867.87 |
| `CRK` | 182 | 2026-09-03 @ $15.70 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $2867.87 |
| `MMED` | 124 | 2026-09-03 @ $22.78 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $2867.87 |
| `BAK` | 3 | 2026-09-04 @ $1.95 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $6.42 |
| `EOSE` | 1 | 2026-09-04 @ $3.57 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $6.42 |
| `CHGG` | 2 | 2026-09-07 @ $0.95 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $2.43 |
