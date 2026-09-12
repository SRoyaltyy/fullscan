# Factor mine action — `probable_probable_ok_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-17.21%** ($8,279) · signal-only (no cash/fees) was +9.59%. Starts YES **11/21**. Fills 73 · skips 104 · realized $-1842.55.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11.53.

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
| 2026-08-20 | `DNA` | 186 | — | $7.45 | +0.00 | $6.96 | -91.14 | -91.14 | +0.00 | -91.14 |
| 2026-08-20 | `MSTR` | 12 | — | $113.23 | +0.00 | $112.39 | -10.08 | -10.08 | +0.00 | -10.08 |
| 2026-08-20 | `EXK` | 128 | — | $10.77 | +0.00 | $10.97 | +25.60 | +25.60 | +0.00 | +25.60 |
| 2026-08-20 | `SCZM` | 146 | — | $9.46 | +0.00 | $9.76 | +43.80 | +43.80 | +0.00 | +43.80 |
| 2026-08-20 | `NG` | 165 | — | $8.38 | +0.00 | $8.66 | +46.20 | +46.20 | +0.00 | +46.20 |
| 2026-08-20 | `BLSH` | 47 | — | $29.20 | +0.00 | $28.44 | -35.72 | -35.72 | +0.00 | -35.72 |
| 2026-08-20 | `HYMC` | 50 | — | $27.25 | +0.00 | $26.14 | -55.50 | -55.50 | +0.00 | -55.50 |
| 2026-08-21 | `DNA` | 186 | $6.96 | $7.09 | +24.18 | $7.40 | +57.66 | +81.84 | -66.96 | -9.30 |
| 2026-08-21 | `MSTR` | 12 | $112.39 | $119.69 | +87.60 | $119.25 | -5.28 | +82.32 | +77.52 | +72.24 |
| 2026-08-21 | `EXK` | 128 | $10.97 | $11.34 | +47.36 | $10.62 | -92.16 | -44.80 | +72.96 | -19.20 |
| 2026-08-21 | `SCZM` | 146 | $9.76 | $10.26 | +73.00 | $9.68 | -85.41 | -12.41 | +116.80 | +31.39 |
| 2026-08-21 | `NG` | 165 | $8.66 | $9.02 | +59.40 | $8.72 | -49.50 | +9.90 | +105.60 | +56.10 |
| 2026-08-21 | `BLSH` | 47 | $28.44 | $29.75 | +61.57 | $30.41 | +31.02 | +92.59 | +25.85 | +56.87 |
| 2026-08-21 | `HYMC` | 50 | $26.14 | $27.40 | +63.00 | $27.07 | -16.50 | +46.50 | +7.50 | -9.00 |
| 2026-08-21 | `BTBT` | 6 | — | $1.66 | +0.00 | $1.53 | -0.78 | -0.78 | +0.00 | -0.78 |
| 2026-08-21 | `ORBS` | 13 | — | $0.86 | +0.00 | $0.88 | +0.21 | +0.21 | +0.00 | +0.21 |
| 2026-08-21 | `GORO` | 3 | — | $3.11 | +0.00 | $3.19 | +0.24 | +0.24 | +0.00 | +0.24 |
| 2026-08-24 | `DNA` | 186 | $7.40 | $7.25 | -27.90 | $6.78 | -87.42 | -115.32 | -37.20 | -124.62 |
| 2026-08-24 | `MSTR` | 12 | $119.25 | $121.84 | +31.08 | $122.63 | +9.48 | +40.56 | +103.32 | +112.80 |
| 2026-08-24 | `EXK` | 128 | $10.62 | $10.97 | +44.80 | $10.76 | -26.88 | +17.92 | +25.60 | -1.28 |
| 2026-08-24 | `SCZM` | 146 | $9.68 | $9.76 | +11.68 | $9.57 | -27.01 | -15.33 | +43.07 | +16.06 |
| 2026-08-24 | `NG` | 165 | $8.72 | $8.89 | +28.05 | $9.35 | +75.90 | +103.95 | +84.15 | +160.05 |
| 2026-08-24 | `BLSH` | 47 | $30.41 | $30.18 | -10.81 | $30.61 | +20.21 | +9.40 | +46.06 | +66.27 |
| 2026-08-24 | `HYMC` | 50 | $27.07 | $27.22 | +7.50 | $25.74 | -74.00 | -66.50 | -1.50 | -75.50 |
| 2026-08-24 | `BTBT` | 6 | $1.53 | $1.55 | +0.12 | $1.51 | -0.24 | -0.12 | -0.66 | -0.90 |
| 2026-08-24 | `ORBS` | 13 | $0.88 | $0.89 | +0.13 | $0.84 | -0.65 | -0.52 | +0.34 | -0.31 |
| 2026-08-24 | `GORO` | 3 | $3.19 | $3.20 | +0.03 | $3.53 | +0.99 | +1.02 | +0.27 | +1.26 |
| 2026-08-25 | `DNA` | 186 | $6.78 | $6.94 | +29.76 | — | +0.00 | +29.76 | -94.86 | — |
| 2026-08-25 | `MSTR` | 12 | $122.63 | $119.11 | -42.24 | — | +0.00 | -42.24 | +70.56 | — |
| 2026-08-25 | `EXK` | 128 | $10.76 | $10.44 | -40.96 | — | +0.00 | -40.96 | -42.24 | — |
| 2026-08-25 | `SCZM` | 146 | $9.57 | $9.45 | -17.52 | — | +0.00 | -17.52 | -1.46 | — |
| 2026-08-25 | `NG` | 165 | $9.35 | $9.31 | -6.60 | — | +0.00 | -6.60 | +153.45 | — |
| 2026-08-25 | `BLSH` | 47 | $30.61 | $30.00 | -28.67 | — | +0.00 | -28.67 | +37.60 | — |
| 2026-08-25 | `HYMC` | 50 | $25.74 | $24.91 | -41.75 | — | +0.00 | -41.75 | -117.25 | — |
| 2026-08-25 | `BTBT` | 6 | $1.51 | $1.51 | +0.00 | $1.58 | +0.42 | +0.42 | -0.90 | -0.48 |
| 2026-08-25 | `ORBS` | 13 | $0.84 | $0.83 | -0.13 | $0.80 | -0.39 | -0.52 | -0.44 | -0.83 |
| 2026-08-25 | `GORO` | 3 | $3.53 | $3.55 | +0.06 | $3.87 | +0.96 | +1.02 | +1.32 | +2.28 |
| 2026-08-25 | `SAFX` | 3850 | — | $0.36 | +0.00 | $0.35 | -15.40 | -15.40 | +0.00 | -15.40 |
| 2026-08-25 | `VITL` | 123 | — | $11.12 | +0.00 | $11.11 | -1.23 | -1.23 | +0.00 | -1.23 |
| 2026-08-25 | `KURA` | 101 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CCOI` | 145 | — | $9.49 | +0.00 | $9.88 | +56.55 | +56.55 | +0.00 | +56.55 |
| 2026-08-25 | `LIFE` | 37 | — | $36.96 | +0.00 | $38.56 | +59.20 | +59.20 | +0.00 | +59.20 |
| 2026-08-25 | `ZIP` | 302 | — | $4.55 | +0.00 | $4.35 | -60.40 | -60.40 | +0.00 | -60.40 |
| 2026-08-25 | `ADIG` | 62 | — | $21.79 | +0.00 | $22.27 | +29.76 | +29.76 | +0.00 | +29.76 |
| 2026-08-26 | `BTBT` | 6 | $1.58 | $1.53 | -0.30 | — | +0.00 | -0.30 | -0.78 | — |
| 2026-08-26 | `ORBS` | 13 | $0.80 | $0.80 | -0.05 | — | +0.00 | -0.05 | -0.88 | — |
| 2026-08-26 | `GORO` | 3 | $3.87 | $3.77 | -0.30 | — | +0.00 | -0.30 | +1.98 | — |
| 2026-08-26 | `SAFX` | 3850 | $0.35 | $0.35 | -3.85 | $0.39 | +127.05 | +123.20 | -19.25 | +107.80 |
| 2026-08-26 | `VITL` | 123 | $11.11 | $11.03 | -9.84 | $10.94 | -11.07 | -20.91 | -11.07 | -22.14 |
| 2026-08-26 | `KURA` | 101 | $13.59 | $13.63 | +4.04 | $13.06 | -57.57 | -53.53 | +4.04 | -53.53 |
| 2026-08-26 | `CCOI` | 145 | $9.88 | $9.89 | +1.45 | $10.05 | +23.20 | +24.65 | +58.00 | +81.20 |
| 2026-08-26 | `LIFE` | 37 | $38.56 | $38.24 | -11.84 | $39.11 | +32.19 | +20.35 | +47.36 | +79.55 |
| 2026-08-26 | `ZIP` | 302 | $4.35 | $4.31 | -12.08 | $4.31 | +0.00 | -12.08 | -72.48 | -72.48 |
| 2026-08-26 | `ADIG` | 62 | $22.27 | $21.78 | -30.38 | $21.59 | -11.78 | -42.16 | -0.62 | -12.40 |
| 2026-08-26 | `ABX` | 1 | — | $9.83 | +0.00 | $9.78 | -0.05 | -0.05 | +0.00 | -0.05 |
| 2026-08-26 | `ITG` | 1 | — | $12.04 | +0.00 | $12.45 | +0.41 | +0.41 | +0.00 | +0.41 |
| 2026-08-26 | `SENS` | 1 | — | $9.48 | +0.00 | $9.34 | -0.14 | -0.14 | +0.00 | -0.14 |
| 2026-08-27 | `SAFX` | 3850 | $0.39 | $0.39 | +23.10 | $0.37 | -88.55 | -65.45 | +130.90 | +42.35 |
| 2026-08-27 | `VITL` | 123 | $10.94 | $10.90 | -4.92 | $10.44 | -56.58 | -61.50 | -27.06 | -83.64 |
| 2026-08-27 | `KURA` | 101 | $13.06 | $12.98 | -8.08 | $13.18 | +20.20 | +12.12 | -61.61 | -41.41 |
| 2026-08-27 | `CCOI` | 145 | $10.05 | $9.83 | -31.90 | $9.67 | -23.20 | -55.10 | +49.30 | +26.10 |
| 2026-08-27 | `LIFE` | 37 | $39.11 | $39.40 | +10.73 | $39.44 | +1.48 | +12.21 | +90.28 | +91.76 |
| 2026-08-27 | `ZIP` | 302 | $4.31 | $4.30 | -3.02 | $4.29 | -3.02 | -6.04 | -75.50 | -78.52 |
| 2026-08-27 | `ADIG` | 62 | $21.59 | $21.98 | +24.18 | $21.88 | -6.20 | +17.98 | +11.78 | +5.58 |
| 2026-08-27 | `ABX` | 1 | $9.78 | $9.68 | -0.10 | $9.83 | +0.15 | +0.05 | -0.15 | +0.00 |
| 2026-08-27 | `ITG` | 1 | $12.45 | $12.36 | -0.09 | $12.87 | +0.51 | +0.42 | +0.32 | +0.83 |
| 2026-08-27 | `SENS` | 1 | $9.34 | $9.33 | -0.01 | $9.40 | +0.07 | +0.06 | -0.15 | -0.08 |
| 2026-08-28 | `SAFX` | 3850 | $0.37 | $0.36 | -15.40 | — | +0.00 | -15.40 | +26.95 | — |
| 2026-08-28 | `VITL` | 123 | $10.44 | $10.47 | +3.69 | — | +0.00 | +3.69 | -79.95 | — |
| 2026-08-28 | `KURA` | 101 | $13.18 | $13.05 | -13.13 | — | +0.00 | -13.13 | -54.54 | — |
| 2026-08-28 | `CCOI` | 145 | $9.67 | $9.70 | +4.35 | — | +0.00 | +4.35 | +30.45 | — |
| 2026-08-28 | `LIFE` | 37 | $39.44 | $39.60 | +5.92 | — | +0.00 | +5.92 | +97.68 | — |
| 2026-08-28 | `ZIP` | 302 | $4.29 | $4.21 | -24.16 | — | +0.00 | -24.16 | -102.68 | — |
| 2026-08-28 | `ADIG` | 62 | $21.88 | $22.10 | +13.64 | — | +0.00 | +13.64 | +19.22 | — |
| 2026-08-28 | `ABX` | 1 | $9.83 | $9.88 | +0.05 | $9.74 | -0.14 | -0.09 | +0.05 | -0.09 |
| 2026-08-28 | `ITG` | 1 | $12.87 | $12.79 | -0.08 | $12.30 | -0.49 | -0.57 | +0.75 | +0.26 |
| 2026-08-28 | `SENS` | 1 | $9.40 | $9.39 | -0.01 | $9.33 | -0.06 | -0.07 | -0.09 | -0.15 |
| 2026-08-28 | `OPTX` | 1101 | — | $8.61 | +0.00 | $8.52 | -99.09 | -99.09 | +0.00 | -99.09 |
| 2026-08-31 | `ABX` | 1 | $9.74 | $9.74 | +0.00 | — | +0.00 | +0.00 | -0.09 | — |
| 2026-08-31 | `ITG` | 1 | $12.30 | $12.30 | +0.00 | — | +0.00 | +0.00 | +0.26 | — |
| 2026-08-31 | `SENS` | 1 | $9.33 | $9.29 | -0.04 | — | +0.00 | -0.04 | -0.19 | — |
| 2026-08-31 | `OPTX` | 1101 | $8.52 | $8.52 | +0.00 | $8.19 | -363.33 | -363.33 | -99.09 | -462.42 |
| 2026-09-01 | `OPTX` | 1101 | $8.19 | $7.94 | -275.25 | $7.28 | -726.66 | -1001.91 | -737.67 | -1464.33 |
| 2026-09-02 | `OPTX` | 1101 | $7.28 | $7.25 | -33.03 | — | +0.00 | -33.03 | -1497.36 | — |
| 2026-09-03 | `ARCT` | 159 | — | $16.77 | +0.00 | $15.56 | -192.39 | -192.39 | +0.00 | -192.39 |
| 2026-09-03 | `CRDL` | 1224 | — | $2.18 | +0.00 | $2.16 | -24.48 | -24.48 | +0.00 | -24.48 |
| 2026-09-03 | `CLYM` | 189 | — | $13.96 | +0.00 | $14.59 | +119.07 | +119.07 | +0.00 | +119.07 |
| 2026-09-04 | `ARCT` | 159 | $15.56 | $15.61 | +7.95 | $15.82 | +33.39 | +41.34 | -184.44 | -151.05 |
| 2026-09-04 | `CRDL` | 1224 | $2.16 | $2.16 | +0.00 | $2.20 | +48.96 | +48.96 | -24.48 | +24.48 |
| 2026-09-04 | `CLYM` | 189 | $14.59 | $14.49 | -18.90 | $15.52 | +194.67 | +175.77 | +100.17 | +294.84 |
| 2026-09-08 | `ARCT` | 159 | $15.82 | $15.47 | -55.65 | $15.63 | +25.44 | -30.21 | -206.70 | -181.26 |
| 2026-09-08 | `CRDL` | 1224 | $2.20 | $2.20 | +0.00 | $2.22 | +24.48 | +24.48 | +24.48 | +48.96 |
| 2026-09-08 | `CLYM` | 189 | $15.52 | $15.62 | +18.90 | $15.90 | +52.92 | +71.82 | +313.74 | +366.66 |
| 2026-09-09 | `ARCT` | 159 | $15.63 | $15.46 | -27.03 | — | +0.00 | -27.03 | -208.29 | — |
| 2026-09-09 | `CRDL` | 1224 | $2.22 | $2.22 | +0.00 | — | +0.00 | +0.00 | +48.96 | — |
| 2026-09-09 | `CLYM` | 189 | $15.90 | $15.82 | -15.12 | — | +0.00 | -15.12 | +351.54 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `CMRC` | 521 | — | $3.13 | +0.00 | $3.50 | +195.38 | +195.38 | +0.00 | +195.38 |
| 2026-09-11 | `AMTX` | 799 | — | $2.04 | +0.00 | $2.01 | -23.97 | -23.97 | +0.00 | -23.97 |
| 2026-09-11 | `CLOV` | 343 | — | $4.75 | +0.00 | $4.82 | +24.01 | +24.01 | +0.00 | +24.01 |
| 2026-09-11 | `BAK` | 769 | — | $2.12 | +0.00 | $2.08 | -30.76 | -30.76 | +0.00 | -30.76 |
| 2026-09-11 | `APPS` | 134 | — | $11.88 | +0.00 | $11.81 | -9.38 | -9.38 | +0.00 | -9.38 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -38.70 | ANGX, HYLN, WDC, ADUR, ALGM | — | $493.79 | $9,942.67 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 |
| 2026-08-17 | +2.25 | $493.79 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 | $10,107.31 | +164.64 | +39.81 | CDNL, ABX, VERA, OCC, ALM | — | $111.60 | $10,143.27 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 |
| 2026-08-18 | -6.20 | $111.60 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 | $9,860.11 | -283.16 | -122.04 | — | — | $111.60 | $9,738.07 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 |
| 2026-08-19 | -7.20 | $111.60 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 | $9,742.74 | +4.67 | -10.33 | — | ANGX, HYLN, WDC, ADUR, ALGM | $9,341.61 | $9,713.51 | CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 |
| 2026-08-20 | +1.12 | $9,341.61 | CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 | $9,710.08 | -3.43 | -76.84 | DNA, MSTR, EXK, SCZM, NG, BLSH, HYMC | CDNL, ABX, VERA, OCC, ALM | $68.32 | $9,613.26 | DNA×186, MSTR×12, EXK×128, SCZM×146, NG×165, BLSH×47, HYMC×50 |
| 2026-08-21 | +3.25 | $68.32 | DNA×186, MSTR×12, EXK×128, SCZM×146, NG×165, BLSH×47, HYMC×50 | $10,029.37 | +416.11 | -160.50 | BTBT, ORBS, GORO | — | $37.43 | $9,868.50 | DNA×186, MSTR×12, EXK×128, SCZM×146, NG×165, BLSH×47, HYMC×50, BTBT×6, ORBS×13, GORO×3 |
| 2026-08-24 | -5.17 | $37.43 | DNA×186, MSTR×12, EXK×128, SCZM×146, NG×165, BLSH×47, HYMC×50, BTBT×6, ORBS×13, GORO×3 | $9,953.18 | +84.68 | -109.62 | — | — | $37.43 | $9,843.56 | DNA×186, MSTR×12, EXK×128, SCZM×146, NG×165, BLSH×47, HYMC×50, BTBT×6, ORBS×13, GORO×3 |
| 2026-08-25 | +1.80 | $37.43 | DNA×186, MSTR×12, EXK×128, SCZM×146, NG×165, BLSH×47, HYMC×50, BTBT×6, ORBS×13, GORO×3 | $9,695.51 | -148.05 | +69.47 | SAFX, VITL, KURA, CCOI, LIFE, ZIP, ADIG | DNA, MSTR, EXK, SCZM, NG, BLSH, HYMC | $20.78 | $9,708.05 | BTBT×6, ORBS×13, GORO×3, SAFX×3850, VITL×123, KURA×101, CCOI×145, LIFE×37, ZIP×302, ADIG×62 |
| 2026-08-26 | +2.02 | $20.78 | BTBT×6, ORBS×13, GORO×3, SAFX×3850, VITL×123, KURA×101, CCOI×145, LIFE×37, ZIP×302, ADIG×62 | $9,644.90 | -63.15 | +102.24 | ABX, ITG, SENS | BTBT, ORBS, GORO | $19.51 | $9,746.38 | SAFX×3850, VITL×123, KURA×101, CCOI×145, LIFE×37, ZIP×302, ADIG×62, ABX×1, ITG×1, SENS×1 |
| 2026-08-27 | — | $19.51 | SAFX×3850, VITL×123, KURA×101, CCOI×145, LIFE×37, ZIP×302, ADIG×62, ABX×1, ITG×1, SENS×1 | $9,756.27 | +9.89 | -155.14 | — | — | $19.51 | $9,601.13 | SAFX×3850, VITL×123, KURA×101, CCOI×145, LIFE×37, ZIP×302, ADIG×62, ABX×1, ITG×1, SENS×1 |
| 2026-08-28 | +0.75 | $19.51 | SAFX×3850, VITL×123, KURA×101, CCOI×145, LIFE×37, ZIP×302, ADIG×62, ABX×1, ITG×1, SENS×1 | $9,576.00 | -25.13 | -99.78 | OPTX | SAFX, VITL, KURA, CCOI, LIFE, ZIP, ADIG | $8.43 | $9,420.32 | ABX×1, ITG×1, SENS×1, OPTX×1101 |
| 2026-08-31 | -5.85 | $8.43 | ABX×1, ITG×1, SENS×1, OPTX×1101 | $9,420.28 | -0.04 | -363.33 | — | ABX, ITG, SENS | $39.38 | $9,056.57 | OPTX×1101 |
| 2026-09-01 | -6.30 | $39.38 | OPTX×1101 | $8,781.32 | -275.25 | -726.66 | — | — | $39.38 | $8,054.66 | OPTX×1101 |
| 2026-09-02 | -3.83 | $39.38 | OPTX×1101 | $8,021.63 | -33.03 | +0.00 | — | OPTX | $8,007.18 | $8,007.18 | — |
| 2026-09-03 | -0.90 | $8,007.18 | — | $8,007.18 | -0.00 | -97.80 | ARCT, CRDL, CLYM | — | $13.17 | $7,888.56 | ARCT×159, CRDL×1224, CLYM×189 |
| 2026-09-04 | +2.25 | $13.17 | ARCT×159, CRDL×1224, CLYM×189 | $7,877.61 | -10.95 | +277.02 | — | — | $13.17 | $8,154.63 | ARCT×159, CRDL×1224, CLYM×189 |
| 2026-09-08 | -11.47 | $13.17 | ARCT×159, CRDL×1224, CLYM×189 | $8,117.88 | -36.75 | +102.84 | — | — | $13.17 | $8,220.72 | ARCT×159, CRDL×1224, CLYM×189 |
| 2026-09-09 | -13.95 | $13.17 | ARCT×159, CRDL×1224, CLYM×189 | $8,178.57 | -42.15 | +0.00 | — | ARCT, CRDL, CLYM | $8,157.43 | $8,157.43 | — |
| 2026-09-10 | -13.28 | $8,157.43 | — | $8,157.43 | +0.00 | +0.00 | — | — | $8,157.43 | $8,157.43 | — |
| 2026-09-11 | +0.50 | $8,157.43 | — | $8,157.43 | +0.00 | +155.28 | CMRC, AMTX, CLOV, BAK, APPS | — | $11.53 | $8,278.95 | CMRC×521, AMTX×799, CLOV×343, BAK×769, APPS×134 |

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
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 186 | $7.45 | $2.55 | — | $8,317.98 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1386.60 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 12 | $113.23 | $2.03 | — | $6,957.20 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1386.60 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 128 | $10.77 | $2.37 | — | $5,576.26 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1386.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 146 | $9.46 | $2.43 | — | $4,192.68 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1386.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 165 | $8.38 | $2.48 | — | $2,807.49 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1386.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 47 | $29.20 | $2.13 | — | $1,432.96 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1386.60 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HYMC` | 50 | $27.25 | $2.14 | — | $68.32 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+1.6; leftover $1386.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $68.32 | ▼ close $9,613.26 vs 09:30 $9,710.08 (session -76.84) | 16:00 close · cash $68.32 · equity $9,613.26 vs 09:30 $9,710.08 (-96.82; session marks -76.84) · 7 name(s) marked open→close (per-name table). DNA×186 09:30 $7.45 → close $6.96 -91.14; MSTR×12 09:30 $113.23 → close $112.39 -10.08; EXK×128 09:30 $10.77 → close $10.97 +25.60; SCZM×146 09:30 $9.46 → close $9.76 +43.80; NG×165 09:30 $8.38 → close $8.66 +46.20; BLSH×47 09:30 $29.20 → close $28.44 -35.72; HYMC×50 09:30 $27.25 → close $26.14 -55.50 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $68.32 | ▲ 09:30 equity $10,029.37 vs yday $9,613.26 (+416.11) | 09:30 open · cash $68.32 (unchanged overnight, no fees) · equity $10,029.37 vs prior close $9,613.26 (+416.11) · 7 name(s) re-marked at the open (per-name table). DNA×186 yday $6.96 → 09:30 $7.09 +24.18; MSTR×12 yday $112.39 → 09:30 $119.69 +87.60; EXK×128 yday $10.97 → 09:30 $11.34 +47.36; SCZM×146 yday $9.76 → 09:30 $10.26 +73.00; NG×165 yday $8.66 → 09:30 $9.02 +59.40; BLSH×47 yday $28.44 → 09:30 $29.75 +61.57; HYMC×50 yday $26.14 → 09:30 $27.40 +63.00 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 6 | $1.66 | $0.12 | — | $58.24 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $11.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 13 | $0.86 | $0.15 | — | $46.86 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $11.39 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 3 | $3.11 | $0.10 | — | $37.43 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+7.1; leftover $11.39 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.43 | ▼ close $9,868.50 vs 09:30 $10,029.37 (session -160.50) | 16:00 close · cash $37.43 · equity $9,868.50 vs 09:30 $10,029.37 (-160.87; session marks -160.50) · 10 name(s) marked open→close (per-name table). DNA×186 09:30 $7.09 → close $7.40 +57.66; MSTR×12 09:30 $119.69 → close $119.25 -5.28; EXK×128 09:30 $11.34 → close $10.62 -92.16; SCZM×146 09:30 $10.26 → close $9.68 -85.41; NG×165 09:30 $9.02 → close $8.72 -49.50; BLSH×47 09:30 $29.75 → close $30.41 +31.02; HYMC×50 09:30 $27.40 → close $27.07 -16.50; BTBT×6 09:30 $1.66 → close $1.53 -0.78; ORBS×13 09:30 $0.86 → close $0.88 +0.21; GORO×3 09:30 $3.11 → close $3.19 +0.24 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.43 | ▲ 09:30 equity $9,953.18 vs yday $9,868.50 (+84.68) | 09:30 open · cash $37.43 (unchanged overnight, no fees) · equity $9,953.18 vs prior close $9,868.50 (+84.68) · 10 name(s) re-marked at the open (per-name table). DNA×186 yday $7.40 → 09:30 $7.25 -27.90; MSTR×12 yday $119.25 → 09:30 $121.84 +31.08; EXK×128 yday $10.62 → 09:30 $10.97 +44.80; SCZM×146 yday $9.68 → 09:30 $9.76 +11.68; NG×165 yday $8.72 → 09:30 $8.89 +28.05; BLSH×47 yday $30.41 → 09:30 $30.18 -10.81; HYMC×50 yday $27.07 → 09:30 $27.22 +7.50; BTBT×6 yday $1.53 → 09:30 $1.55 +0.12; ORBS×13 yday $0.88 → 09:30 $0.89 +0.13; GORO×3 yday $3.19 → 09:30 $3.20 +0.03 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.43 | ▼ close $9,843.56 vs 09:30 $9,953.18 (session -109.62) | 16:00 close · cash $37.43 · equity $9,843.56 vs 09:30 $9,953.18 (-109.62; session marks -109.62) · 10 name(s) marked open→close (per-name table). DNA×186 09:30 $7.25 → close $6.78 -87.42; MSTR×12 09:30 $121.84 → close $122.63 +9.48; EXK×128 09:30 $10.97 → close $10.76 -26.88; SCZM×146 09:30 $9.76 → close $9.57 -27.01; NG×165 09:30 $8.89 → close $9.35 +75.90; BLSH×47 09:30 $30.18 → close $30.61 +20.21; HYMC×50 09:30 $27.22 → close $25.74 -74.00; BTBT×6 09:30 $1.55 → close $1.51 -0.24; ORBS×13 09:30 $0.89 → close $0.84 -0.65; GORO×3 09:30 $3.20 → close $3.53 +0.99 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.43 | ▼ 09:30 equity $9,695.51 vs yday $9,843.56 (-148.05) | 09:30 open · cash $37.43 (unchanged overnight, no fees) · equity $9,695.51 vs prior close $9,843.56 (-148.05) · 10 name(s) re-marked at the open (per-name table). DNA×186 yday $6.78 → 09:30 $6.94 +29.76; MSTR×12 yday $122.63 → 09:30 $119.11 -42.24; EXK×128 yday $10.76 → 09:30 $10.44 -40.96; SCZM×146 yday $9.57 → 09:30 $9.45 -17.52; NG×165 yday $9.35 → 09:30 $9.31 -6.60; BLSH×47 yday $30.61 → 09:30 $30.00 -28.67; HYMC×50 yday $25.74 → 09:30 $24.91 -41.75; BTBT×6 yday $1.51 → 09:30 $1.51 +0.00; ORBS×13 yday $0.84 → 09:30 $0.83 -0.13; GORO×3 yday $3.53 → 09:30 $3.55 +0.06 | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 186 | $6.94 | $2.59 | $-100.00 | $1,325.68 | ▼ -100.00 after sell → book $9,692.92; vs 09:30 mark -2.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MSTR` | 12 | $119.11 | $2.05 | $+66.49 | $2,752.95 | ▲ +66.49 after sell → book $9,690.87; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 128 | $10.44 | $2.41 | $-47.02 | $4,086.86 | ▼ -47.02 after sell → book $9,688.46; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SCZM` | 146 | $9.45 | $2.46 | $-6.35 | $5,464.10 | ▼ -6.35 after sell → book $9,686.00; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NG` | 165 | $9.31 | $2.52 | $+148.44 | $6,997.73 | ▲ +148.44 after sell → book $9,683.48; vs 09:30 mark -2.52 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BLSH` | 47 | $30.00 | $2.15 | $+33.32 | $8,405.57 | ▲ +33.32 after sell → book $9,681.32; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HYMC` | 50 | $24.91 | $2.16 | $-121.55 | $9,648.66 | ▼ -121.55 after sell → book $9,679.16; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3850 | $0.36 | $25.33 | — | $8,245.03 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-15.6; leftover $1378.38 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 123 | $11.12 | $2.36 | — | $6,874.91 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.7; leftover $1378.38 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 101 | $13.59 | $2.29 | — | $5,500.03 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1378.38 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 145 | $9.49 | $2.42 | — | $4,121.55 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1378.38 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 37 | $36.96 | $2.10 | — | $2,751.93 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1378.38 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 302 | $4.55 | $3.90 | — | $1,373.94 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1378.38 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ADIG` | 62 | $21.79 | $2.18 | — | $20.78 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+3.1; leftover $1378.38 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.78 | ▲ close $9,708.05 vs 09:30 $9,695.51 (session +69.47) | 16:00 close · cash $20.78 · equity $9,708.05 vs 09:30 $9,695.51 (+12.54; session marks +69.47) · 10 name(s) marked open→close (per-name table). BTBT×6 09:30 $1.51 → close $1.58 +0.42; ORBS×13 09:30 $0.83 → close $0.80 -0.39; GORO×3 09:30 $3.55 → close $3.87 +0.96; SAFX×3850 09:30 $0.36 → close $0.35 -15.40; VITL×123 09:30 $11.12 → close $11.11 -1.23; KURA×101 09:30 $13.59 → close $13.59 +0.00; CCOI×145 09:30 $9.49 → close $9.88 +56.55; LIFE×37 09:30 $36.96 → close $38.56 +59.20; ZIP×302 09:30 $4.55 → close $4.35 -60.40; ADIG×62 09:30 $21.79 → close $22.27 +29.76 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.78 | ▼ 09:30 equity $9,644.90 vs yday $9,708.05 (-63.15) | 09:30 open · cash $20.78 (unchanged overnight, no fees) · equity $9,644.90 vs prior close $9,708.05 (-63.15) · 10 name(s) re-marked at the open (per-name table). BTBT×6 yday $1.58 → 09:30 $1.53 -0.30; ORBS×13 yday $0.80 → 09:30 $0.80 -0.05; GORO×3 yday $3.87 → 09:30 $3.77 -0.30; SAFX×3850 yday $0.35 → 09:30 $0.35 -3.85; VITL×123 yday $11.11 → 09:30 $11.03 -9.84; KURA×101 yday $13.59 → 09:30 $13.63 +4.04; CCOI×145 yday $9.88 → 09:30 $9.89 +1.45; LIFE×37 yday $38.56 → 09:30 $38.24 -11.84; ZIP×302 yday $4.35 → 09:30 $4.31 -12.08; ADIG×62 yday $22.27 → 09:30 $21.78 -30.38 | — |
| 2026-08-26 09:30 ET | **SELL** | `BTBT` | 6 | $1.53 | $0.13 | $-1.03 | $29.83 | ▼ -1.03 after sell → book $9,644.77; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ORBS` | 13 | $0.80 | $0.16 | $-1.20 | $40.02 | ▼ -1.20 after sell → book $9,644.61; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 3 | $3.77 | $0.14 | $+1.74 | $51.18 | ▲ +1.74 after sell → book $9,644.46; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 1 | $9.83 | $0.10 | — | $41.25 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $12.80 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ITG` | 1 | $12.04 | $0.12 | — | $29.09 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-5.1; leftover $12.80 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SENS` | 1 | $9.48 | $0.10 | — | $19.51 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $12.80 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.51 | ▲ close $9,746.38 vs 09:30 $9,644.90 (session +102.24) | 16:00 close · cash $19.51 · equity $9,746.38 vs 09:30 $9,644.90 (+101.48; session marks +102.24) · 10 name(s) marked open→close (per-name table). SAFX×3850 09:30 $0.35 → close $0.39 +127.05; VITL×123 09:30 $11.03 → close $10.94 -11.07; KURA×101 09:30 $13.63 → close $13.06 -57.57; CCOI×145 09:30 $9.89 → close $10.05 +23.20; LIFE×37 09:30 $38.24 → close $39.11 +32.19; ZIP×302 09:30 $4.31 → close $4.31 +0.00; ADIG×62 09:30 $21.78 → close $21.59 -11.78; ABX×1 09:30 $9.83 → close $9.78 -0.05; ITG×1 09:30 $12.04 → close $12.45 +0.41; SENS×1 09:30 $9.48 → close $9.34 -0.14 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.51 | ▲ 09:30 equity $9,756.27 vs yday $9,746.38 (+9.89) | 09:30 open · cash $19.51 (unchanged overnight, no fees) · equity $9,756.27 vs prior close $9,746.38 (+9.89) · 10 name(s) re-marked at the open (per-name table). SAFX×3850 yday $0.39 → 09:30 $0.39 +23.10; VITL×123 yday $10.94 → 09:30 $10.90 -4.92; KURA×101 yday $13.06 → 09:30 $12.98 -8.08; CCOI×145 yday $10.05 → 09:30 $9.83 -31.90; LIFE×37 yday $39.11 → 09:30 $39.40 +10.73; ZIP×302 yday $4.31 → 09:30 $4.30 -3.02; ADIG×62 yday $21.59 → 09:30 $21.98 +24.18; ABX×1 yday $9.78 → 09:30 $9.68 -0.10; ITG×1 yday $12.45 → 09:30 $12.36 -0.09; SENS×1 yday $9.34 → 09:30 $9.33 -0.01 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.51 | ▼ close $9,601.13 vs 09:30 $9,756.27 (session -155.14) | 16:00 close · cash $19.51 · equity $9,601.13 vs 09:30 $9,756.27 (-155.14; session marks -155.14) · 10 name(s) marked open→close (per-name table). SAFX×3850 09:30 $0.39 → close $0.37 -88.55; VITL×123 09:30 $10.90 → close $10.44 -56.58; KURA×101 09:30 $12.98 → close $13.18 +20.20; CCOI×145 09:30 $9.83 → close $9.67 -23.20; LIFE×37 09:30 $39.40 → close $39.44 +1.48; ZIP×302 09:30 $4.30 → close $4.29 -3.02; ADIG×62 09:30 $21.98 → close $21.88 -6.20; ABX×1 09:30 $9.68 → close $9.83 +0.15; ITG×1 09:30 $12.36 → close $12.87 +0.51; SENS×1 09:30 $9.33 → close $9.40 +0.07 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.51 | ▼ 09:30 equity $9,576.00 vs yday $9,601.13 (-25.13) | 09:30 open · cash $19.51 (unchanged overnight, no fees) · equity $9,576.00 vs prior close $9,601.13 (-25.13) · 10 name(s) re-marked at the open (per-name table). SAFX×3850 yday $0.37 → 09:30 $0.36 -15.40; VITL×123 yday $10.44 → 09:30 $10.47 +3.69; KURA×101 yday $13.18 → 09:30 $13.05 -13.13; CCOI×145 yday $9.67 → 09:30 $9.70 +4.35; LIFE×37 yday $39.44 → 09:30 $39.60 +5.92; ZIP×302 yday $4.29 → 09:30 $4.21 -24.16; ADIG×62 yday $21.88 → 09:30 $22.10 +13.64; ABX×1 yday $9.83 → 09:30 $9.88 +0.05; ITG×1 yday $12.87 → 09:30 $12.79 -0.08; SENS×1 yday $9.40 → 09:30 $9.39 -0.01 | — |
| 2026-08-28 09:30 ET | **SELL** | `SAFX` | 3850 | $0.36 | $26.25 | $-24.64 | $1,398.51 | ▼ -24.64 after sell → book $9,549.75; vs 09:30 mark -26.25 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `VITL` | 123 | $10.47 | $2.39 | $-84.70 | $2,683.93 | ▼ -84.70 after sell → book $9,547.36; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 101 | $13.05 | $2.32 | $-59.15 | $3,999.66 | ▼ -59.15 after sell → book $9,545.04; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CCOI` | 145 | $9.70 | $2.46 | $+25.56 | $5,403.70 | ▲ +25.56 after sell → book $9,542.58; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 37 | $39.60 | $2.12 | $+93.46 | $6,866.78 | ▲ +93.46 after sell → book $9,540.46; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZIP` | 302 | $4.21 | $3.96 | $-110.53 | $8,134.24 | ▼ -110.53 after sell → book $9,536.50; vs 09:30 mark -3.96 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ADIG` | 62 | $22.10 | $2.20 | $+14.85 | $9,502.24 | ▲ +14.85 after sell → book $9,534.30; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 1101 | $8.61 | $14.20 | — | $8.43 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.7; leftover $9502.24 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.43 | ▼ close $9,420.32 vs 09:30 $9,576.00 (session -99.78) | 16:00 close · cash $8.43 · equity $9,420.32 vs 09:30 $9,576.00 (-155.68; session marks -99.78) · 4 name(s) marked open→close (per-name table). ABX×1 09:30 $9.88 → close $9.74 -0.14; ITG×1 09:30 $12.79 → close $12.30 -0.49; SENS×1 09:30 $9.39 → close $9.33 -0.06; OPTX×1101 09:30 $8.61 → close $8.52 -99.09 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.43 | ▼ 09:30 equity $9,420.28 vs yday $9,420.32 (-0.04) | 09:30 open · cash $8.43 (unchanged overnight, no fees) · equity $9,420.28 vs prior close $9,420.32 (-0.04) · 4 name(s) re-marked at the open (per-name table). ABX×1 yday $9.74 → 09:30 $9.74 +0.00; ITG×1 yday $12.30 → 09:30 $12.30 +0.00; SENS×1 yday $9.33 → 09:30 $9.29 -0.04; OPTX×1101 yday $8.52 → 09:30 $8.52 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `ABX` | 1 | $9.74 | $0.12 | $-0.31 | $18.05 | ▼ -0.31 after sell → book $9,420.16; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ITG` | 1 | $12.30 | $0.15 | $-0.01 | $30.20 | ▼ -0.01 after sell → book $9,420.01; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `SENS` | 1 | $9.29 | $0.12 | $-0.40 | $39.38 | ▼ -0.40 after sell → book $9,419.90; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.38 | ▼ close $9,056.57 vs 09:30 $9,420.28 (session -363.33) | 16:00 close · cash $39.38 · equity $9,056.57 vs 09:30 $9,420.28 (-363.71; session marks -363.33) · 1 name(s) marked open→close (per-name table). OPTX×1101 09:30 $8.52 → close $8.19 -363.33 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.38 | ▼ 09:30 equity $8,781.32 vs yday $9,056.57 (-275.25) | 09:30 open · cash $39.38 (unchanged overnight, no fees) · equity $8,781.32 vs prior close $9,056.57 (-275.25) · 1 name(s) re-marked at the open (per-name table). OPTX×1101 yday $8.19 → 09:30 $7.94 -275.25 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.38 | ▼ close $8,054.66 vs 09:30 $8,781.32 (session -726.66) | 16:00 close · cash $39.38 · equity $8,054.66 vs 09:30 $8,781.32 (-726.66; session marks -726.66) · 1 name(s) marked open→close (per-name table). OPTX×1101 09:30 $7.94 → close $7.28 -726.66 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.38 | ▼ 09:30 equity $8,021.63 vs yday $8,054.66 (-33.03) | 09:30 open · cash $39.38 (unchanged overnight, no fees) · equity $8,021.63 vs prior close $8,054.66 (-33.03) · 1 name(s) re-marked at the open (per-name table). OPTX×1101 yday $7.28 → 09:30 $7.25 -33.03 | — |
| 2026-09-02 09:30 ET | **SELL** | `OPTX` | 1101 | $7.25 | $14.45 | $-1526.01 | $8,007.18 | ▼ -1,526.01 after sell → book $8,007.18; vs 09:30 mark -14.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,007.18 | ▲ close $8,007.18 vs 09:30 $8,021.63 (session +0.00) | 16:00 close · cash $8,007.18 · no lots left · equity $8,007.18. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,007.18 | ▲ 09:30 equity $8,007.18 vs yday $8,007.18 (-0.00) | 09:30 open · cash $8,007.18 · no holdings · equity $8,007.18 vs prior close $8,007.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 159 | $16.77 | $2.47 | — | $5,338.28 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $2669.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 1224 | $2.18 | $15.79 | — | $2,654.17 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $2669.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 189 | $13.96 | $2.56 | — | $13.17 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-6.4; leftover $2669.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.17 | ▼ close $7,888.56 vs 09:30 $8,007.18 (session -97.80) | 16:00 close · cash $13.17 · equity $7,888.56 vs 09:30 $8,007.18 (-118.62; session marks -97.80) · 3 name(s) marked open→close (per-name table). ARCT×159 09:30 $16.77 → close $15.56 -192.39; CRDL×1224 09:30 $2.18 → close $2.16 -24.48; CLYM×189 09:30 $13.96 → close $14.59 +119.07 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.17 | ▼ 09:30 equity $7,877.61 vs yday $7,888.56 (-10.95) | 09:30 open · cash $13.17 (unchanged overnight, no fees) · equity $7,877.61 vs prior close $7,888.56 (-10.95) · 3 name(s) re-marked at the open (per-name table). ARCT×159 yday $15.56 → 09:30 $15.61 +7.95; CRDL×1224 yday $2.16 → 09:30 $2.16 +0.00; CLYM×189 yday $14.59 → 09:30 $14.49 -18.90 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.17 | ▲ close $8,154.63 vs 09:30 $7,877.61 (session +277.02) | 16:00 close · cash $13.17 · equity $8,154.63 vs 09:30 $7,877.61 (+277.02; session marks +277.02) · 3 name(s) marked open→close (per-name table). ARCT×159 09:30 $15.61 → close $15.82 +33.39; CRDL×1224 09:30 $2.16 → close $2.20 +48.96; CLYM×189 09:30 $14.49 → close $15.52 +194.67 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.17 | ▼ 09:30 equity $8,117.88 vs yday $8,154.63 (-36.75) | 09:30 open · cash $13.17 (unchanged overnight, no fees) · equity $8,117.88 vs prior close $8,154.63 (-36.75) · 3 name(s) re-marked at the open (per-name table). ARCT×159 yday $15.82 → 09:30 $15.47 -55.65; CRDL×1224 yday $2.20 → 09:30 $2.20 +0.00; CLYM×189 yday $15.52 → 09:30 $15.62 +18.90 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.17 | ▲ close $8,220.72 vs 09:30 $8,117.88 (session +102.84) | 16:00 close · cash $13.17 · equity $8,220.72 vs 09:30 $8,117.88 (+102.84; session marks +102.84) · 3 name(s) marked open→close (per-name table). ARCT×159 09:30 $15.47 → close $15.63 +25.44; CRDL×1224 09:30 $2.20 → close $2.22 +24.48; CLYM×189 09:30 $15.62 → close $15.90 +52.92 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.17 | ▼ 09:30 equity $8,178.57 vs yday $8,220.72 (-42.15) | 09:30 open · cash $13.17 (unchanged overnight, no fees) · equity $8,178.57 vs prior close $8,220.72 (-42.15) · 3 name(s) re-marked at the open (per-name table). ARCT×159 yday $15.63 → 09:30 $15.46 -27.03; CRDL×1224 yday $2.22 → 09:30 $2.22 +0.00; CLYM×189 yday $15.90 → 09:30 $15.82 -15.12 | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 159 | $15.46 | $2.51 | $-213.27 | $2,468.80 | ▼ -213.27 after sell → book $8,176.06; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 1224 | $2.22 | $16.01 | $+17.16 | $5,170.07 | ▲ +17.16 after sell → book $8,160.05; vs 09:30 mark -16.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CLYM` | 189 | $15.82 | $2.61 | $+346.37 | $8,157.43 | ▲ +346.37 after sell → book $8,157.43; vs 09:30 mark -2.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,157.43 | ▲ close $8,157.43 vs 09:30 $8,178.57 (session +0.00) | 16:00 close · cash $8,157.43 · no lots left · equity $8,157.43. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,157.43 | ▲ 09:30 equity $8,157.43 vs yday $8,157.43 (+0.00) | 09:30 open · cash $8,157.43 · no holdings · equity $8,157.43 vs prior close $8,157.43 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,157.43 | ▲ close $8,157.43 vs 09:30 $8,157.43 (session +0.00) | 16:00 close · cash $8,157.43 · no lots left · equity $8,157.43. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,157.43 | ▲ 09:30 equity $8,157.43 vs yday $8,157.43 (+0.00) | 09:30 open · cash $8,157.43 · no holdings · equity $8,157.43 vs prior close $8,157.43 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 521 | $3.13 | $6.72 | — | $6,519.98 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+6.2; leftover $1631.49 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 799 | $2.04 | $10.31 | — | $4,879.72 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.8; leftover $1631.49 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 343 | $4.75 | $4.42 | — | $3,246.04 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.4; leftover $1631.49 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 769 | $2.12 | $9.92 | — | $1,605.84 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1631.49 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `APPS` | 134 | $11.88 | $2.39 | — | $11.53 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+5.0; leftover $1631.49 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.53 | ▲ close $8,278.95 vs 09:30 $8,157.43 (session +155.28) | 16:00 close · cash $11.53 · equity $8,278.95 vs 09:30 $8,157.43 (+121.52; session marks +155.28) · 5 name(s) marked open→close (per-name table). CMRC×521 09:30 $3.13 → close $3.50 +195.38; AMTX×799 09:30 $2.04 → close $2.01 -23.97; CLOV×343 09:30 $4.75 → close $4.82 +24.01; BAK×769 09:30 $2.12 → close $2.08 -30.76; APPS×134 09:30 $11.88 → close $11.81 -9.38 | — |

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
| 2026-08-21 | `HYMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DE` | cash | leftover split 11.39 < 1 share @ 623.26 |
| 2026-08-21 | `QDEL` | cash | leftover split 11.39 < 1 share @ 14.96 |
| 2026-08-21 | `CF` | cash | leftover split 11.39 < 1 share @ 127.43 |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BLSH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HYMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-08-26 | `AVBP` | cash | leftover split 12.80 < 1 share @ 31.21 |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `VITL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CCOI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZIP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ADIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ABX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ITG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `SENS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ITG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `DELL` | cash | leftover split 13.17 < 1 share @ 513.78 |
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

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CMRC` | 521 | 2026-09-11 @ $3.13 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+6.2; leftover $1631.49 |
| `AMTX` | 799 | 2026-09-11 @ $2.04 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.8; leftover $1631.49 |
| `CLOV` | 343 | 2026-09-11 @ $4.75 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.4; leftover $1631.49 |
| `BAK` | 769 | 2026-09-11 @ $2.12 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1631.49 |
| `APPS` | 134 | 2026-09-11 @ $11.88 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+5.0; leftover $1631.49 |
