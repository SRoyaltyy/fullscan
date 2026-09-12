# Factor mine action — `probable_probable_ok_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-17.29%** ($8,271) · signal-only (no cash/fees) was +9.93%. Starts YES **6/21**. Fills 60 · skips 102 · realized $-1728.66.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,271.34.

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
| 2026-08-21 | `BTBT` | 3 | — | $1.66 | +0.00 | $1.53 | -0.39 | -0.39 | +0.00 | -0.39 |
| 2026-08-21 | `ORBS` | 6 | — | $0.86 | +0.00 | $0.88 | +0.10 | +0.10 | +0.00 | +0.10 |
| 2026-08-21 | `GORO` | 1 | — | $3.11 | +0.00 | $3.19 | +0.08 | +0.08 | +0.00 | +0.08 |
| 2026-08-24 | `DNA` | 217 | $7.40 | $7.25 | -32.55 | $6.78 | -101.99 | -134.54 | -43.40 | -145.39 |
| 2026-08-24 | `MSTR` | 14 | $119.25 | $121.84 | +36.26 | $122.63 | +11.06 | +47.32 | +120.54 | +131.60 |
| 2026-08-24 | `EXK` | 150 | $10.62 | $10.97 | +52.50 | $10.76 | -31.50 | +21.00 | +30.00 | -1.50 |
| 2026-08-24 | `SCZM` | 171 | $9.68 | $9.76 | +13.68 | $9.57 | -31.64 | -17.96 | +50.44 | +18.81 |
| 2026-08-24 | `NG` | 193 | $8.72 | $8.89 | +32.81 | $9.35 | +88.78 | +121.59 | +98.43 | +187.21 |
| 2026-08-24 | `BLSH` | 55 | $30.41 | $30.18 | -12.65 | $30.61 | +23.65 | +11.00 | +53.90 | +77.55 |
| 2026-08-24 | `BTBT` | 3 | $1.53 | $1.55 | +0.06 | $1.51 | -0.12 | -0.06 | -0.33 | -0.45 |
| 2026-08-24 | `ORBS` | 6 | $0.88 | $0.89 | +0.06 | $0.84 | -0.30 | -0.24 | +0.16 | -0.14 |
| 2026-08-24 | `GORO` | 1 | $3.19 | $3.20 | +0.01 | $3.53 | +0.33 | +0.34 | +0.09 | +0.42 |
| 2026-08-25 | `DNA` | 217 | $6.78 | $6.94 | +34.72 | — | +0.00 | +34.72 | -110.67 | — |
| 2026-08-25 | `MSTR` | 14 | $122.63 | $119.11 | -49.28 | — | +0.00 | -49.28 | +82.32 | — |
| 2026-08-25 | `EXK` | 150 | $10.76 | $10.44 | -48.00 | — | +0.00 | -48.00 | -49.50 | — |
| 2026-08-25 | `SCZM` | 171 | $9.57 | $9.45 | -20.52 | — | +0.00 | -20.52 | -1.71 | — |
| 2026-08-25 | `NG` | 193 | $9.35 | $9.31 | -7.72 | — | +0.00 | -7.72 | +179.49 | — |
| 2026-08-25 | `BLSH` | 55 | $30.61 | $30.00 | -33.55 | — | +0.00 | -33.55 | +44.00 | — |
| 2026-08-25 | `BTBT` | 3 | $1.51 | $1.51 | +0.00 | $1.58 | +0.21 | +0.21 | -0.45 | -0.24 |
| 2026-08-25 | `ORBS` | 6 | $0.84 | $0.83 | -0.06 | $0.80 | -0.18 | -0.24 | -0.20 | -0.38 |
| 2026-08-25 | `GORO` | 1 | $3.53 | $3.55 | +0.02 | $3.87 | +0.32 | +0.34 | +0.44 | +0.76 |
| 2026-08-25 | `SAFX` | 3913 | — | $0.36 | +0.00 | $0.35 | -15.65 | -15.65 | +0.00 | -15.65 |
| 2026-08-25 | `VITL` | 125 | — | $11.12 | +0.00 | $11.11 | -1.25 | -1.25 | +0.00 | -1.25 |
| 2026-08-25 | `KURA` | 103 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CCOI` | 147 | — | $9.49 | +0.00 | $9.88 | +57.33 | +57.33 | +0.00 | +57.33 |
| 2026-08-25 | `LIFE` | 37 | — | $36.96 | +0.00 | $38.56 | +59.20 | +59.20 | +0.00 | +59.20 |
| 2026-08-25 | `ZIP` | 307 | — | $4.55 | +0.00 | $4.35 | -61.40 | -61.40 | +0.00 | -61.40 |
| 2026-08-25 | `ADIG` | 64 | — | $21.79 | +0.00 | $22.27 | +30.72 | +30.72 | +0.00 | +30.72 |
| 2026-08-26 | `BTBT` | 3 | $1.58 | $1.53 | -0.15 | — | +0.00 | -0.15 | -0.39 | — |
| 2026-08-26 | `ORBS` | 6 | $0.80 | $0.80 | -0.02 | — | +0.00 | -0.02 | -0.41 | — |
| 2026-08-26 | `GORO` | 1 | $3.87 | $3.77 | -0.10 | — | +0.00 | -0.10 | +0.66 | — |
| 2026-08-26 | `SAFX` | 3913 | $0.35 | $0.35 | -3.91 | $0.39 | +129.13 | +125.22 | -19.57 | +109.56 |
| 2026-08-26 | `VITL` | 125 | $11.11 | $11.03 | -10.00 | $10.94 | -11.25 | -21.25 | -11.25 | -22.50 |
| 2026-08-26 | `KURA` | 103 | $13.59 | $13.63 | +4.12 | $13.06 | -58.71 | -54.59 | +4.12 | -54.59 |
| 2026-08-26 | `CCOI` | 147 | $9.88 | $9.89 | +1.47 | $10.05 | +23.52 | +24.99 | +58.80 | +82.32 |
| 2026-08-26 | `LIFE` | 37 | $38.56 | $38.24 | -11.84 | $39.11 | +32.19 | +20.35 | +47.36 | +79.55 |
| 2026-08-26 | `ZIP` | 307 | $4.35 | $4.31 | -12.28 | $4.31 | +0.00 | -12.28 | -73.68 | -73.68 |
| 2026-08-26 | `ADIG` | 64 | $22.27 | $21.78 | -31.36 | $21.59 | -12.16 | -43.52 | -0.64 | -12.80 |
| 2026-08-27 | `SAFX` | 3913 | $0.39 | $0.39 | +23.48 | $0.37 | -90.00 | -66.52 | +133.04 | +43.04 |
| 2026-08-27 | `VITL` | 125 | $10.94 | $10.90 | -5.00 | $10.44 | -57.50 | -62.50 | -27.50 | -85.00 |
| 2026-08-27 | `KURA` | 103 | $13.06 | $12.98 | -8.24 | $13.18 | +20.60 | +12.36 | -62.83 | -42.23 |
| 2026-08-27 | `CCOI` | 147 | $10.05 | $9.83 | -32.34 | $9.67 | -23.52 | -55.86 | +49.98 | +26.46 |
| 2026-08-27 | `LIFE` | 37 | $39.11 | $39.40 | +10.73 | $39.44 | +1.48 | +12.21 | +90.28 | +91.76 |
| 2026-08-27 | `ZIP` | 307 | $4.31 | $4.30 | -3.07 | $4.29 | -3.07 | -6.14 | -76.75 | -79.82 |
| 2026-08-27 | `ADIG` | 64 | $21.59 | $21.98 | +24.96 | $21.88 | -6.40 | +18.56 | +12.16 | +5.76 |
| 2026-08-28 | `SAFX` | 3913 | $0.37 | $0.36 | -15.65 | — | +0.00 | -15.65 | +27.39 | — |
| 2026-08-28 | `VITL` | 125 | $10.44 | $10.47 | +3.75 | — | +0.00 | +3.75 | -81.25 | — |
| 2026-08-28 | `KURA` | 103 | $13.18 | $13.05 | -13.39 | — | +0.00 | -13.39 | -55.62 | — |
| 2026-08-28 | `CCOI` | 147 | $9.67 | $9.70 | +4.41 | — | +0.00 | +4.41 | +30.87 | — |
| 2026-08-28 | `LIFE` | 37 | $39.44 | $39.60 | +5.92 | — | +0.00 | +5.92 | +97.68 | — |
| 2026-08-28 | `ZIP` | 307 | $4.29 | $4.21 | -24.56 | — | +0.00 | -24.56 | -104.38 | — |
| 2026-08-28 | `ADIG` | 64 | $21.88 | $22.10 | +14.08 | — | +0.00 | +14.08 | +19.84 | — |
| 2026-08-28 | `OPTX` | 1121 | — | $8.61 | +0.00 | $8.52 | -100.89 | -100.89 | +0.00 | -100.89 |
| 2026-08-31 | `OPTX` | 1121 | $8.52 | $8.52 | +0.00 | $8.19 | -369.93 | -369.93 | -100.89 | -470.82 |
| 2026-09-01 | `OPTX` | 1121 | $8.19 | $7.94 | -280.25 | $7.28 | -739.86 | -1020.11 | -751.07 | -1490.93 |
| 2026-09-02 | `OPTX` | 1121 | $7.28 | $7.25 | -33.63 | — | +0.00 | -33.63 | -1524.56 | — |
| 2026-09-03 | `ARCT` | 161 | — | $16.77 | +0.00 | $15.56 | -194.81 | -194.81 | +0.00 | -194.81 |
| 2026-09-03 | `CRDL` | 1241 | — | $2.18 | +0.00 | $2.16 | -24.82 | -24.82 | +0.00 | -24.82 |
| 2026-09-03 | `CLYM` | 192 | — | $13.96 | +0.00 | $14.59 | +120.96 | +120.96 | +0.00 | +120.96 |
| 2026-09-04 | `ARCT` | 161 | $15.56 | $15.61 | +8.05 | $15.82 | +33.81 | +41.86 | -186.76 | -152.95 |
| 2026-09-04 | `CRDL` | 1241 | $2.16 | $2.16 | +0.00 | $2.20 | +49.64 | +49.64 | -24.82 | +24.82 |
| 2026-09-04 | `CLYM` | 192 | $14.59 | $14.49 | -19.20 | $15.52 | +197.76 | +178.56 | +101.76 | +299.52 |
| 2026-09-08 | `ARCT` | 161 | $15.82 | $15.47 | -56.35 | $15.63 | +25.76 | -30.59 | -209.30 | -183.54 |
| 2026-09-08 | `CRDL` | 1241 | $2.20 | $2.20 | +0.00 | $2.22 | +24.82 | +24.82 | +24.82 | +49.64 |
| 2026-09-08 | `CLYM` | 192 | $15.52 | $15.62 | +19.20 | $15.90 | +53.76 | +72.96 | +318.72 | +372.48 |
| 2026-09-09 | `ARCT` | 161 | $15.63 | $15.46 | -27.37 | — | +0.00 | -27.37 | -210.91 | — |
| 2026-09-09 | `CRDL` | 1241 | $2.22 | $2.22 | +0.00 | — | +0.00 | +0.00 | +49.64 | — |
| 2026-09-09 | `CLYM` | 192 | $15.90 | $15.82 | -15.36 | — | +0.00 | -15.36 | +357.12 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -38.70 | ANGX, HYLN, WDC, ADUR, ALGM | — | $493.79 | $9,942.67 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 |
| 2026-08-17 | +2.25 | $493.79 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 | $10,107.31 | +164.64 | +39.81 | CDNL, ABX, VERA, OCC, ALM | — | $111.60 | $10,143.27 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 |
| 2026-08-18 | -6.20 | $111.60 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 | $9,860.11 | -283.16 | -122.04 | — | — | $111.60 | $9,738.07 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 |
| 2026-08-19 | -7.20 | $111.60 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 | $9,742.74 | +4.67 | -10.33 | — | ANGX, HYLN, WDC, ADUR, ALGM | $9,341.61 | $9,713.51 | CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 |
| 2026-08-20 | +1.12 | $9,341.61 | CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 | $9,710.08 | -3.43 | -24.55 | DNA, MSTR, EXK, SCZM, NG, BLSH | CDNL, ABX, VERA, OCC, ALM | $33.36 | $9,667.18 | DNA×217, MSTR×14, EXK×150, SCZM×171, NG×193, BLSH×55 |
| 2026-08-21 | +3.25 | $33.36 | DNA×217, MSTR×14, EXK×150, SCZM×171, NG×193, BLSH×55 | $10,080.12 | +412.94 | -168.73 | BTBT, ORBS, GORO | — | $19.93 | $9,911.22 | DNA×217, MSTR×14, EXK×150, SCZM×171, NG×193, BLSH×55, BTBT×3, ORBS×6, GORO×1 |
| 2026-08-24 | -5.17 | $19.93 | DNA×217, MSTR×14, EXK×150, SCZM×171, NG×193, BLSH×55, BTBT×3, ORBS×6, GORO×1 | $10,001.40 | +90.18 | -41.73 | — | — | $19.93 | $9,959.68 | DNA×217, MSTR×14, EXK×150, SCZM×171, NG×193, BLSH×55, BTBT×3, ORBS×6, GORO×1 |
| 2026-08-25 | +1.80 | $19.93 | DNA×217, MSTR×14, EXK×150, SCZM×171, NG×193, BLSH×55, BTBT×3, ORBS×6, GORO×1 | $9,835.29 | -124.39 | +69.30 | SAFX, VITL, KURA, CCOI, LIFE, ZIP, ADIG | DNA, MSTR, EXK, SCZM, NG, BLSH | $21.84 | $9,848.78 | BTBT×3, ORBS×6, GORO×1, SAFX×3913, VITL×125, KURA×103, CCOI×147, LIFE×37, ZIP×307, ADIG×64 |
| 2026-08-26 | +2.02 | $21.84 | BTBT×3, ORBS×6, GORO×1, SAFX×3913, VITL×125, KURA×103, CCOI×147, LIFE×37, ZIP×307, ADIG×64 | $9,784.70 | -64.08 | +102.72 | — | BTBT, ORBS, GORO | $34.75 | $9,887.20 | SAFX×3913, VITL×125, KURA×103, CCOI×147, LIFE×37, ZIP×307, ADIG×64 |
| 2026-08-27 | — | $34.75 | SAFX×3913, VITL×125, KURA×103, CCOI×147, LIFE×37, ZIP×307, ADIG×64 | $9,897.72 | +10.52 | -158.41 | — | — | $34.75 | $9,739.31 | SAFX×3913, VITL×125, KURA×103, CCOI×147, LIFE×37, ZIP×307, ADIG×64 |
| 2026-08-28 | +0.75 | $34.75 | SAFX×3913, VITL×125, KURA×103, CCOI×147, LIFE×37, ZIP×307, ADIG×64 | $9,713.87 | -25.44 | -100.89 | OPTX | SAFX, VITL, KURA, CCOI, LIFE, ZIP, ADIG | $5.38 | $9,556.30 | OPTX×1121 |
| 2026-08-31 | -5.85 | $5.38 | OPTX×1121 | $9,556.30 | -0.00 | -369.93 | — | — | $5.38 | $9,186.37 | OPTX×1121 |
| 2026-09-01 | -6.30 | $5.38 | OPTX×1121 | $8,906.12 | -280.25 | -739.86 | — | — | $5.38 | $8,166.26 | OPTX×1121 |
| 2026-09-02 | -3.83 | $5.38 | OPTX×1121 | $8,132.63 | -33.63 | +0.00 | — | OPTX | $8,117.92 | $8,117.92 | — |
| 2026-09-03 | -0.90 | $8,117.92 | — | $8,117.92 | -0.00 | -98.67 | ARCT, CRDL, CLYM | — | $11.20 | $7,998.20 | ARCT×161, CRDL×1241, CLYM×192 |
| 2026-09-04 | +2.25 | $11.20 | ARCT×161, CRDL×1241, CLYM×192 | $7,987.05 | -11.15 | +281.21 | — | — | $11.20 | $8,268.26 | ARCT×161, CRDL×1241, CLYM×192 |
| 2026-09-08 | -11.47 | $11.20 | ARCT×161, CRDL×1241, CLYM×192 | $8,231.11 | -37.15 | +104.34 | — | — | $11.20 | $8,335.45 | ARCT×161, CRDL×1241, CLYM×192 |
| 2026-09-09 | -13.95 | $11.20 | ARCT×161, CRDL×1241, CLYM×192 | $8,292.72 | -42.73 | +0.00 | — | ARCT, CRDL, CLYM | $8,271.34 | $8,271.34 | — |
| 2026-09-10 | -13.28 | $8,271.34 | — | $8,271.34 | -0.00 | +0.00 | — | — | $8,271.34 | $8,271.34 | — |
| 2026-09-11 | +0.50 | $8,271.34 | — | $8,271.34 | -0.00 | +0.00 | — | — | $8,271.34 | $8,271.34 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $7,994.17 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 478 | $4.18 | $6.17 | — | $5,989.97 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 3 | $503.50 | $2.00 | — | $4,477.47 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ⚪; ret5=+7.9; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 121 | $16.50 | $2.35 | — | $2,478.62 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 45 | $44.06 | $2.12 | — | $493.79 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+3.9; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $493.79 | ▼ close $9,942.67 vs 09:30 $10,000.00 (session -38.70) | 16:00 close · cash $493.79 · equity $9,942.67 vs 09:30 $10,000.00 (-57.33; session marks -38.70) · 5 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.31 → close $4.37 +27.84; HYLN×478 09:30 $4.18 → close $4.06 -57.36; WDC×3 09:30 $503.50 → close $508.80 +15.90; ADUR×121 09:30 $16.50 → close $16.17 -39.93; ALGM×45 09:30 $44.06 → close $44.39 +14.85 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $493.79 | ▲ 09:30 equity $10,107.31 vs yday $9,942.67 (+164.64) | 09:30 open · cash $493.79 (unchanged overnight, no fees) · equity $10,107.31 vs prior close $9,942.67 (+164.64) · 5 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; HYLN×478 yday $4.06 → 09:30 $4.10 +19.12; WDC×3 yday $508.80 → 09:30 $525.53 +50.19; ADUR×121 yday $16.17 → 09:30 $15.73 -53.24; ALGM×45 yday $44.39 → 09:30 $45.32 +41.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 2 | $39.85 | $0.80 | — | $413.29 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $82.30 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 9 | $9.12 | $0.85 | — | $330.36 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $82.30 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 2 | $31.30 | $0.63 | — | $267.13 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-3.8; leftover $82.30 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 4 | $18.24 | $0.74 | — | $193.43 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $82.30 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
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
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 3 | $1.66 | $0.06 | — | $28.32 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $5.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 6 | $0.86 | $0.07 | — | $23.07 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $5.56 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 1 | $3.11 | $0.03 | — | $19.93 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+7.1; leftover $5.56 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.93 | ▼ close $9,911.22 vs 09:30 $10,080.12 (session -168.73) | 16:00 close · cash $19.93 · equity $9,911.22 vs 09:30 $10,080.12 (-168.90; session marks -168.73) · 9 name(s) marked open→close (per-name table). DNA×217 09:30 $7.09 → close $7.40 +67.27; MSTR×14 09:30 $119.69 → close $119.25 -6.16; EXK×150 09:30 $11.34 → close $10.62 -108.00; SCZM×171 09:30 $10.26 → close $9.68 -100.03; NG×193 09:30 $9.02 → close $8.72 -57.90; BLSH×55 09:30 $29.75 → close $30.41 +36.30; BTBT×3 09:30 $1.66 → close $1.53 -0.39; ORBS×6 09:30 $0.86 → close $0.88 +0.10; GORO×1 09:30 $3.11 → close $3.19 +0.08 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.93 | ▲ 09:30 equity $10,001.40 vs yday $9,911.22 (+90.18) | 09:30 open · cash $19.93 (unchanged overnight, no fees) · equity $10,001.40 vs prior close $9,911.22 (+90.18) · 9 name(s) re-marked at the open (per-name table). DNA×217 yday $7.40 → 09:30 $7.25 -32.55; MSTR×14 yday $119.25 → 09:30 $121.84 +36.26; EXK×150 yday $10.62 → 09:30 $10.97 +52.50; SCZM×171 yday $9.68 → 09:30 $9.76 +13.68; NG×193 yday $8.72 → 09:30 $8.89 +32.81; BLSH×55 yday $30.41 → 09:30 $30.18 -12.65; BTBT×3 yday $1.53 → 09:30 $1.55 +0.06; ORBS×6 yday $0.88 → 09:30 $0.89 +0.06; GORO×1 yday $3.19 → 09:30 $3.20 +0.01 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.93 | ▼ close $9,959.68 vs 09:30 $10,001.40 (session -41.73) | 16:00 close · cash $19.93 · equity $9,959.68 vs 09:30 $10,001.40 (-41.72; session marks -41.73) · 9 name(s) marked open→close (per-name table). DNA×217 09:30 $7.25 → close $6.78 -101.99; MSTR×14 09:30 $121.84 → close $122.63 +11.06; EXK×150 09:30 $10.97 → close $10.76 -31.50; SCZM×171 09:30 $9.76 → close $9.57 -31.64; NG×193 09:30 $8.89 → close $9.35 +88.78; BLSH×55 09:30 $30.18 → close $30.61 +23.65; BTBT×3 09:30 $1.55 → close $1.51 -0.12; ORBS×6 09:30 $0.89 → close $0.84 -0.30; GORO×1 09:30 $3.20 → close $3.53 +0.33 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.93 | ▼ 09:30 equity $9,835.29 vs yday $9,959.68 (-124.39) | 09:30 open · cash $19.93 (unchanged overnight, no fees) · equity $9,835.29 vs prior close $9,959.68 (-124.39) · 9 name(s) re-marked at the open (per-name table). DNA×217 yday $6.78 → 09:30 $6.94 +34.72; MSTR×14 yday $122.63 → 09:30 $119.11 -49.28; EXK×150 yday $10.76 → 09:30 $10.44 -48.00; SCZM×171 yday $9.57 → 09:30 $9.45 -20.52; NG×193 yday $9.35 → 09:30 $9.31 -7.72; BLSH×55 yday $30.61 → 09:30 $30.00 -33.55; BTBT×3 yday $1.51 → 09:30 $1.51 +0.00; ORBS×6 yday $0.84 → 09:30 $0.83 -0.06; GORO×1 yday $3.53 → 09:30 $3.55 +0.02 | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 217 | $6.94 | $2.85 | $-116.32 | $1,523.06 | ▼ -116.32 after sell → book $9,832.44; vs 09:30 mark -2.85 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MSTR` | 14 | $119.11 | $2.06 | $+78.23 | $3,188.54 | ▲ +78.23 after sell → book $9,830.38; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 150 | $10.44 | $2.48 | $-54.42 | $4,752.07 | ▼ -54.42 after sell → book $9,827.91; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SCZM` | 171 | $9.45 | $2.54 | $-6.76 | $6,365.47 | ▼ -6.76 after sell → book $9,825.36; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NG` | 193 | $9.31 | $2.62 | $+174.31 | $8,159.69 | ▲ +174.31 after sell → book $9,822.75; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BLSH` | 55 | $30.00 | $2.18 | $+39.67 | $9,807.51 | ▲ +39.67 after sell → book $9,820.57; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3913 | $0.36 | $25.75 | — | $8,380.91 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-15.6; leftover $1401.07 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 125 | $11.12 | $2.37 | — | $6,988.54 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.7; leftover $1401.07 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 103 | $13.59 | $2.30 | — | $5,586.47 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1401.07 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 147 | $9.49 | $2.43 | — | $4,189.01 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1401.07 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 37 | $36.96 | $2.10 | — | $2,819.39 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1401.07 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 307 | $4.55 | $3.96 | — | $1,418.58 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1401.07 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ADIG` | 64 | $21.79 | $2.18 | — | $21.84 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+3.1; leftover $1401.07 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.84 | ▲ close $9,848.78 vs 09:30 $9,835.29 (session +69.30) | 16:00 close · cash $21.84 · equity $9,848.78 vs 09:30 $9,835.29 (+13.49; session marks +69.30) · 10 name(s) marked open→close (per-name table). BTBT×3 09:30 $1.51 → close $1.58 +0.21; ORBS×6 09:30 $0.83 → close $0.80 -0.18; GORO×1 09:30 $3.55 → close $3.87 +0.32; SAFX×3913 09:30 $0.36 → close $0.35 -15.65; VITL×125 09:30 $11.12 → close $11.11 -1.25; KURA×103 09:30 $13.59 → close $13.59 +0.00; CCOI×147 09:30 $9.49 → close $9.88 +57.33; LIFE×37 09:30 $36.96 → close $38.56 +59.20; ZIP×307 09:30 $4.55 → close $4.35 -61.40; ADIG×64 09:30 $21.79 → close $22.27 +30.72 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.84 | ▼ 09:30 equity $9,784.70 vs yday $9,848.78 (-64.08) | 09:30 open · cash $21.84 (unchanged overnight, no fees) · equity $9,784.70 vs prior close $9,848.78 (-64.08) · 10 name(s) re-marked at the open (per-name table). BTBT×3 yday $1.58 → 09:30 $1.53 -0.15; ORBS×6 yday $0.80 → 09:30 $0.80 -0.02; GORO×1 yday $3.87 → 09:30 $3.77 -0.10; SAFX×3913 yday $0.35 → 09:30 $0.35 -3.91; VITL×125 yday $11.11 → 09:30 $11.03 -10.00; KURA×103 yday $13.59 → 09:30 $13.63 +4.12; CCOI×147 yday $9.88 → 09:30 $9.89 +1.47; LIFE×37 yday $38.56 → 09:30 $38.24 -11.84; ZIP×307 yday $4.35 → 09:30 $4.31 -12.28; ADIG×64 yday $22.27 → 09:30 $21.78 -31.36 | — |
| 2026-08-26 09:30 ET | **SELL** | `BTBT` | 3 | $1.53 | $0.07 | $-0.52 | $26.35 | ▼ -0.52 after sell → book $9,784.63; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ORBS` | 6 | $0.80 | $0.09 | $-0.56 | $31.04 | ▼ -0.56 after sell → book $9,784.54; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 1 | $3.77 | $0.06 | $+0.57 | $34.75 | ▲ +0.57 after sell → book $9,784.48; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.75 | ▲ close $9,887.20 vs 09:30 $9,784.70 (session +102.72) | 16:00 close · cash $34.75 · equity $9,887.20 vs 09:30 $9,784.70 (+102.50; session marks +102.72) · 7 name(s) marked open→close (per-name table). SAFX×3913 09:30 $0.35 → close $0.39 +129.13; VITL×125 09:30 $11.03 → close $10.94 -11.25; KURA×103 09:30 $13.63 → close $13.06 -58.71; CCOI×147 09:30 $9.89 → close $10.05 +23.52; LIFE×37 09:30 $38.24 → close $39.11 +32.19; ZIP×307 09:30 $4.31 → close $4.31 +0.00; ADIG×64 09:30 $21.78 → close $21.59 -12.16 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.75 | ▲ 09:30 equity $9,897.72 vs yday $9,887.20 (+10.52) | 09:30 open · cash $34.75 (unchanged overnight, no fees) · equity $9,897.72 vs prior close $9,887.20 (+10.52) · 7 name(s) re-marked at the open (per-name table). SAFX×3913 yday $0.39 → 09:30 $0.39 +23.48; VITL×125 yday $10.94 → 09:30 $10.90 -5.00; KURA×103 yday $13.06 → 09:30 $12.98 -8.24; CCOI×147 yday $10.05 → 09:30 $9.83 -32.34; LIFE×37 yday $39.11 → 09:30 $39.40 +10.73; ZIP×307 yday $4.31 → 09:30 $4.30 -3.07; ADIG×64 yday $21.59 → 09:30 $21.98 +24.96 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.75 | ▼ close $9,739.31 vs 09:30 $9,897.72 (session -158.41) | 16:00 close · cash $34.75 · equity $9,739.31 vs 09:30 $9,897.72 (-158.41; session marks -158.41) · 7 name(s) marked open→close (per-name table). SAFX×3913 09:30 $0.39 → close $0.37 -90.00; VITL×125 09:30 $10.90 → close $10.44 -57.50; KURA×103 09:30 $12.98 → close $13.18 +20.60; CCOI×147 09:30 $9.83 → close $9.67 -23.52; LIFE×37 09:30 $39.40 → close $39.44 +1.48; ZIP×307 09:30 $4.30 → close $4.29 -3.07; ADIG×64 09:30 $21.98 → close $21.88 -6.40 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.75 | ▼ 09:30 equity $9,713.87 vs yday $9,739.31 (-25.44) | 09:30 open · cash $34.75 (unchanged overnight, no fees) · equity $9,713.87 vs prior close $9,739.31 (-25.44) · 7 name(s) re-marked at the open (per-name table). SAFX×3913 yday $0.37 → 09:30 $0.36 -15.65; VITL×125 yday $10.44 → 09:30 $10.47 +3.75; KURA×103 yday $13.18 → 09:30 $13.05 -13.39; CCOI×147 yday $9.67 → 09:30 $9.70 +4.41; LIFE×37 yday $39.44 → 09:30 $39.60 +5.92; ZIP×307 yday $4.29 → 09:30 $4.21 -24.56; ADIG×64 yday $21.88 → 09:30 $22.10 +14.08 | — |
| 2026-08-28 09:30 ET | **SELL** | `SAFX` | 3913 | $0.36 | $26.68 | $-25.04 | $1,436.32 | ▼ -25.04 after sell → book $9,687.19; vs 09:30 mark -26.68 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `VITL` | 125 | $10.47 | $2.40 | $-86.01 | $2,742.67 | ▼ -86.01 after sell → book $9,684.79; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 103 | $13.05 | $2.33 | $-60.25 | $4,084.49 | ▼ -60.25 after sell → book $9,682.46; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CCOI` | 147 | $9.70 | $2.47 | $+25.97 | $5,507.93 | ▲ +25.97 after sell → book $9,680.00; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 37 | $39.60 | $2.12 | $+93.46 | $6,971.00 | ▲ +93.46 after sell → book $9,677.87; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZIP` | 307 | $4.21 | $4.02 | $-112.36 | $8,259.45 | ▼ -112.36 after sell → book $9,673.85; vs 09:30 mark -4.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ADIG` | 64 | $22.10 | $2.20 | $+15.45 | $9,671.65 | ▲ +15.45 after sell → book $9,671.65; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 1121 | $8.61 | $14.46 | — | $5.38 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.7; leftover $9671.65 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.38 | ▼ close $9,556.30 vs 09:30 $9,713.87 (session -100.89) | 16:00 close · cash $5.38 · equity $9,556.30 vs 09:30 $9,713.87 (-157.57; session marks -100.89) · 1 name(s) marked open→close (per-name table). OPTX×1121 09:30 $8.61 → close $8.52 -100.89 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.38 | ▲ 09:30 equity $9,556.30 vs yday $9,556.30 (-0.00) | 09:30 open · cash $5.38 (unchanged overnight, no fees) · equity $9,556.30 vs prior close $9,556.30 (-0.00) · 1 name(s) re-marked at the open (per-name table). OPTX×1121 yday $8.52 → 09:30 $8.52 +0.00 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.38 | ▼ close $9,186.37 vs 09:30 $9,556.30 (session -369.93) | 16:00 close · cash $5.38 · equity $9,186.37 vs 09:30 $9,556.30 (-369.93; session marks -369.93) · 1 name(s) marked open→close (per-name table). OPTX×1121 09:30 $8.52 → close $8.19 -369.93 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.38 | ▼ 09:30 equity $8,906.12 vs yday $9,186.37 (-280.25) | 09:30 open · cash $5.38 (unchanged overnight, no fees) · equity $8,906.12 vs prior close $9,186.37 (-280.25) · 1 name(s) re-marked at the open (per-name table). OPTX×1121 yday $8.19 → 09:30 $7.94 -280.25 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.38 | ▼ close $8,166.26 vs 09:30 $8,906.12 (session -739.86) | 16:00 close · cash $5.38 · equity $8,166.26 vs 09:30 $8,906.12 (-739.86; session marks -739.86) · 1 name(s) marked open→close (per-name table). OPTX×1121 09:30 $7.94 → close $7.28 -739.86 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.38 | ▼ 09:30 equity $8,132.63 vs yday $8,166.26 (-33.63) | 09:30 open · cash $5.38 (unchanged overnight, no fees) · equity $8,132.63 vs prior close $8,166.26 (-33.63) · 1 name(s) re-marked at the open (per-name table). OPTX×1121 yday $7.28 → 09:30 $7.25 -33.63 | — |
| 2026-09-02 09:30 ET | **SELL** | `OPTX` | 1121 | $7.25 | $14.71 | $-1553.73 | $8,117.92 | ▼ -1,553.73 after sell → book $8,117.92; vs 09:30 mark -14.71 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,117.92 | ▲ close $8,117.92 vs 09:30 $8,132.63 (session +0.00) | 16:00 close · cash $8,117.92 · no lots left · equity $8,117.92. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,117.92 | ▲ 09:30 equity $8,117.92 vs yday $8,117.92 (-0.00) | 09:30 open · cash $8,117.92 · no holdings · equity $8,117.92 vs prior close $8,117.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 161 | $16.77 | $2.47 | — | $5,415.47 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $2705.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 1241 | $2.18 | $16.01 | — | $2,694.08 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $2705.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 192 | $13.96 | $2.57 | — | $11.20 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-6.4; leftover $2705.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.20 | ▼ close $7,998.20 vs 09:30 $8,117.92 (session -98.67) | 16:00 close · cash $11.20 · equity $7,998.20 vs 09:30 $8,117.92 (-119.72; session marks -98.67) · 3 name(s) marked open→close (per-name table). ARCT×161 09:30 $16.77 → close $15.56 -194.81; CRDL×1241 09:30 $2.18 → close $2.16 -24.82; CLYM×192 09:30 $13.96 → close $14.59 +120.96 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.20 | ▼ 09:30 equity $7,987.05 vs yday $7,998.20 (-11.15) | 09:30 open · cash $11.20 (unchanged overnight, no fees) · equity $7,987.05 vs prior close $7,998.20 (-11.15) · 3 name(s) re-marked at the open (per-name table). ARCT×161 yday $15.56 → 09:30 $15.61 +8.05; CRDL×1241 yday $2.16 → 09:30 $2.16 +0.00; CLYM×192 yday $14.59 → 09:30 $14.49 -19.20 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.20 | ▲ close $8,268.26 vs 09:30 $7,987.05 (session +281.21) | 16:00 close · cash $11.20 · equity $8,268.26 vs 09:30 $7,987.05 (+281.21; session marks +281.21) · 3 name(s) marked open→close (per-name table). ARCT×161 09:30 $15.61 → close $15.82 +33.81; CRDL×1241 09:30 $2.16 → close $2.20 +49.64; CLYM×192 09:30 $14.49 → close $15.52 +197.76 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.20 | ▼ 09:30 equity $8,231.11 vs yday $8,268.26 (-37.15) | 09:30 open · cash $11.20 (unchanged overnight, no fees) · equity $8,231.11 vs prior close $8,268.26 (-37.15) · 3 name(s) re-marked at the open (per-name table). ARCT×161 yday $15.82 → 09:30 $15.47 -56.35; CRDL×1241 yday $2.20 → 09:30 $2.20 +0.00; CLYM×192 yday $15.52 → 09:30 $15.62 +19.20 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.20 | ▲ close $8,335.45 vs 09:30 $8,231.11 (session +104.34) | 16:00 close · cash $11.20 · equity $8,335.45 vs 09:30 $8,231.11 (+104.34; session marks +104.34) · 3 name(s) marked open→close (per-name table). ARCT×161 09:30 $15.47 → close $15.63 +25.76; CRDL×1241 09:30 $2.20 → close $2.22 +24.82; CLYM×192 09:30 $15.62 → close $15.90 +53.76 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.20 | ▼ 09:30 equity $8,292.72 vs yday $8,335.45 (-42.73) | 09:30 open · cash $11.20 (unchanged overnight, no fees) · equity $8,292.72 vs prior close $8,335.45 (-42.73) · 3 name(s) re-marked at the open (per-name table). ARCT×161 yday $15.63 → 09:30 $15.46 -27.37; CRDL×1241 yday $2.22 → 09:30 $2.22 +0.00; CLYM×192 yday $15.90 → 09:30 $15.82 -15.36 | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 161 | $15.46 | $2.52 | $-215.90 | $2,497.74 | ▼ -215.90 after sell → book $8,290.20; vs 09:30 mark -2.52 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 1241 | $2.22 | $16.24 | $+17.39 | $5,236.52 | ▲ +17.39 after sell → book $8,273.96; vs 09:30 mark -16.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CLYM` | 192 | $15.82 | $2.62 | $+351.93 | $8,271.34 | ▲ +351.93 after sell → book $8,271.34; vs 09:30 mark -2.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,271.34 | ▲ close $8,271.34 vs 09:30 $8,292.72 (session +0.00) | 16:00 close · cash $8,271.34 · no lots left · equity $8,271.34. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,271.34 | ▲ 09:30 equity $8,271.34 vs yday $8,271.34 (-0.00) | 09:30 open · cash $8,271.34 · no holdings · equity $8,271.34 vs prior close $8,271.34 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,271.34 | ▲ close $8,271.34 vs 09:30 $8,271.34 (session +0.00) | 16:00 close · cash $8,271.34 · no lots left · equity $8,271.34. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,271.34 | ▲ 09:30 equity $8,271.34 vs yday $8,271.34 (-0.00) | 09:30 open · cash $8,271.34 · no holdings · equity $8,271.34 vs prior close $8,271.34 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,271.34 | ▲ close $8,271.34 vs 09:30 $8,271.34 (session +0.00) | 16:00 close · cash $8,271.34 · no lots left · equity $8,271.34. | — |

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
| 2026-08-21 | `DE` | cash | leftover split 5.56 < 1 share @ 623.26 |
| 2026-08-21 | `QDEL` | cash | leftover split 5.56 < 1 share @ 14.96 |
| 2026-08-21 | `CF` | cash | leftover split 5.56 < 1 share @ 127.43 |
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
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `VITL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CCOI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZIP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ADIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AVBP` | cash | leftover split 8.69 < 1 share @ 31.21 |
| 2026-08-26 | `ABX` | cash | leftover split 8.69 < 1 share @ 9.83 |
| 2026-08-26 | `ITG` | cash | leftover split 8.69 < 1 share @ 12.04 |
| 2026-08-26 | `SENS` | cash | leftover split 8.69 < 1 share @ 9.48 |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `VITL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CCOI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZIP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ADIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `OPTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `OPTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `IRD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 11.20 < 1 share @ 513.78 |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CLYM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XHLD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HELP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `CMRC` | no_price | no 09:30 open |
| 2026-09-11 | `CLOV` | no_price | no 09:30 open |
| 2026-09-11 | `APPS` | no_price | no 09:30 open |
