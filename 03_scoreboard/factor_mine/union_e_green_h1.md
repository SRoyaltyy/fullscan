# Factor mine action — `union_e_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+11.27%** ($11,127) · signal-only (no cash/fees) was +4.66%. Starts YES **15/17**. Fills 94 · skips 28 · realized $+1129.63.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `earn_react=True,last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $198.12.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | INO | — | $2.29 | $10,958.40 | $10,960.69 | INO×12176 | BUY INO x12176 @ 0.81 |
| 2026-08-14 | +5.50 | $2.29 | INO×12176 | NMAX, AIRJ, BRUN, BZAI, DLO, ENHA, FIRY, GEMI | INO | $1.85 | $10,568.77 | $10,570.62 | NMAX×141, AIRJ×253, BRUN×53, BZAI×1823, DLO×91, ENHA×604, FIRY×143, GEMI×352 | SELL INO (dropped from list after 1 sess (min 1)); BUY NMAX x141 @ 9.89; BUY AIRJ x253 @ 5.51; BUY BRUN x53 @ 26.25; BUY BZAI x1823 @ 0.77; BUY DLO x91 @ 15.28; BUY ENHA x604 @ 2.31; BUY FIRY x143 @ 9.74; BUY GEMI x352 @ 3.90 |
| 2026-08-17 | +2.25 | $1.85 | NMAX×141, AIRJ×253, BRUN×53, BZAI×1823, DLO×91, ENHA×604, FIRY×143, GEMI×352 | — | NMAX, AIRJ, BRUN, BZAI, DLO, ENHA, FIRY, GEMI | $10,589.05 | $0.00 | $10,589.05 | — | SELL NMAX (dropped from list after 1 sess (min 1)); SELL AIRJ (dropped from list after 1 sess (min 1)); SELL BRUN (dropped from list after 1 sess (min 1)); SELL BZAI (dropped from list after 1 sess (min 1)); SELL DLO (dropped from list after 1 sess (min 1)); SELL ENHA (dropped from list after 1 sess (min 1)); SELL FIRY (dropped from list after 1 sess (min 1)); SELL GEMI (dropped from list after 1 sess (min 1)) |
| 2026-08-18 | -6.20 | $10,589.05 | — | — | — | $10,589.05 | $0.00 | $10,589.05 | — | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,589.05 | — | — | — | $10,589.05 | $0.00 | $10,589.05 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,589.05 | — | ATAT, ATHM, BABA, BULL, COTY, DQ, FUTU, IOND | — | $171.56 | $10,472.67 | $10,644.23 | ATAT×38, ATHM×58, BABA×10, BULL×133, COTY×519, DQ×91, FUTU×11, IOND×20 | BUY ATAT x38 @ 34.05; BUY ATHM x58 @ 22.44; BUY BABA x10 @ 123.47; BUY BULL x133 @ 9.94; BUY COTY x519 @ 2.55; BUY DQ x91 @ 14.44; BUY FUTU x11 @ 117.65; BUY IOND x20 @ 65.60 |
| 2026-08-21 | +3.25 | $171.56 | ATAT×38, ATHM×58, BABA×10, BULL×133, COTY×519, DQ×91, FUTU×11, IOND×20 | BJ, BKE, PSEC | ATAT, ATHM, BABA, BULL, COTY, DQ, FUTU, IOND | $75.44 | $10,692.70 | $10,768.14 | BJ×37, BKE×81, PSEC×1535 | SELL ATAT (dropped from list after 1 sess (min 1)); SELL ATHM (dropped from list after 1 sess (min 1)); SELL BABA (dropped from list after 1 sess (min 1)); SELL BULL (dropped from list after 1 sess (min 1)); SELL COTY (dropped from list after 1 sess (min 1)); SELL DQ (dropped from list after 1 sess (min 1)); SELL FUTU (dropped from list after 1 sess (min 1)); SELL IOND (dropped from list after 1 sess (min 1)); BUY BJ x37 @ 93.98; BUY BKE x81 @ 43.08; BUY PSEC x1535 @ 2.30 |
| 2026-08-24 | -5.17 | $75.44 | BJ×37, BKE×81, PSEC×1535 | — | BJ, BKE, PSEC | $10,840.32 | $0.00 | $10,840.32 | — | SELL BJ (dropped from list after 1 sess (min 1)); SELL BKE (dropped from list after 1 sess (min 1)); SELL PSEC (dropped from list after 1 sess (min 1)) |
| 2026-08-25 | +1.80 | $10,840.32 | — | BNS, BZ, DKS, GRRR, SHMD, TUYA, VIPS | — | $192.62 | $10,621.25 | $10,813.87 | BNS×17, BZ×100, DKS×8, GRRR×108, SHMD×328, TUYA×874, VIPS×111 | BUY BNS x17 @ 86.86; BUY BZ x100 @ 15.34; BUY DKS x8 @ 179.33; BUY GRRR x108 @ 14.26; BUY SHMD x328 @ 4.71; BUY TUYA x874 @ 1.77; BUY VIPS x111 @ 13.91 |
| 2026-08-26 | +2.02 | $192.62 | BNS×17, BZ×100, DKS×8, GRRR×108, SHMD×328, TUYA×874, VIPS×111 | — | — | $192.62 | $10,621.21 | $10,813.83 | BNS×17, BZ×100, DKS×8, GRRR×108, SHMD×328, TUYA×874, VIPS×111 | hold BNS,BZ,DKS,GRRR,SHMD,TUYA,VIPS |
| 2026-08-27 | — | $192.62 | BNS×17, BZ×100, DKS×8, GRRR×108, SHMD×328, TUYA×874, VIPS×111 | — | BNS, BZ, DKS, GRRR, SHMD, TUYA, VIPS | $10,126.22 | $0.00 | $10,126.22 | — | SELL BNS (dropped from list after 2 sess (min 1)); SELL BZ (dropped from list after 2 sess (min 1)); SELL DKS (dropped from list after 2 sess (min 1)); SELL GRRR (dropped from list after 2 sess (min 1)); SELL SHMD (dropped from list after 2 sess (min 1)); SELL TUYA (dropped from list after 2 sess (min 1)); SELL VIPS (dropped from list after 2 sess (min 1)) |
| 2026-08-28 | +0.75 | $10,126.22 | — | ADSK, ESTC, HAFN, PD, RBRK, S, ULTA, WDAY | — | $567.88 | $9,783.49 | $10,351.37 | ADSK×4, ESTC×15, HAFN×160, PD×101, RBRK×12, S×58, ULTA×2, WDAY×6 | BUY ADSK x4 @ 261.47; BUY ESTC x15 @ 82.64; BUY HAFN x160 @ 7.91; BUY PD x101 @ 12.45; BUY RBRK x12 @ 101.99; BUY S x58 @ 21.80; BUY ULTA x2 @ 536.07; BUY WDAY x6 @ 195.40 |
| 2026-08-31 | -5.85 | $567.88 | ADSK×4, ESTC×15, HAFN×160, PD×101, RBRK×12, S×58, ULTA×2, WDAY×6 | — | ADSK, ESTC, HAFN, PD, RBRK, S, ULTA, WDAY | $10,447.38 | $0.00 | $10,447.38 | — | SELL ADSK (dropped from list after 1 sess (min 1)); SELL ESTC (dropped from list after 1 sess (min 1)); SELL HAFN (dropped from list after 1 sess (min 1)); SELL PD (dropped from list after 1 sess (min 1)); SELL RBRK (dropped from list after 1 sess (min 1)); SELL S (dropped from list after 1 sess (min 1)); SELL ULTA (dropped from list after 1 sess (min 1)); SELL WDAY (dropped from list after 1 sess (min 1)) |
| 2026-09-01 | -6.30 | $10,447.38 | — | — | — | $10,447.38 | $0.00 | $10,447.38 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $10,447.38 | — | — | — | $10,447.38 | $0.00 | $10,447.38 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,447.38 | — | CHPT, FIVE, HPE, MOMO, NTSK, PHR, PVH, SNOW | — | $218.58 | $10,140.03 | $10,358.61 | CHPT×246, FIVE×5, HPE×25, MOMO×240, NTSK×93, PHR×110, PVH×17, SNOW×4 | BUY CHPT x246 @ 5.30; BUY FIVE x5 @ 244.98; BUY HPE x25 @ 51.99; BUY MOMO x240 @ 5.43; BUY NTSK x93 @ 13.94; BUY PHR x110 @ 11.79; BUY PVH x17 @ 73.10; BUY SNOW x4 @ 310.54 |
| 2026-09-04 | — | $218.58 | CHPT×246, FIVE×5, HPE×25, MOMO×240, NTSK×93, PHR×110, PVH×17, SNOW×4 | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | CHPT, FIVE, HPE, MOMO, NTSK, PHR, PVH, SNOW | $198.12 | $10,928.89 | $11,127.01 | AMBA×20, ASAN×136, DOCU×20, DOMO×368, GWRE×7, IOT×36, LULU×11, MAMA×89 | SELL CHPT (dropped from list after 1 sess (min 1)); SELL FIVE (dropped from list after 1 sess (min 1)); SELL HPE (dropped from list after 1 sess (min 1)); SELL MOMO (dropped from list after 1 sess (min 1)); SELL NTSK (dropped from list after 1 sess (min 1)); SELL PHR (dropped from list after 1 sess (min 1)); SELL PVH (dropped from list after 1 sess (min 1)); SELL SNOW (dropped from list after 1 sess (min 1)); BUY AMBA x20 @ 66.61; BUY ASAN x136 @ 10.16; BUY DOCU x20 @ 67.06; BUY DOMO x368 @ 3.78; BUY GWRE x7 @ 198.00; BUY IOT x36 @ 37.69; BUY LULU x11 @ 121.15; BUY MAMA x89 @ 15.62 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `INO` | 12176 | $0.81 | $135.15 | — | $2.29 | combo gate; gate earn_react=True,last_green=True; list flatten; ⚪; ret5=+13.2; leftover $10000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 12176 | $0.93 | $151.88 | $+1174.09 | $11,174.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 141 | $9.89 | $2.41 | — | $9,776.48 | combo gate; gate earn_react=True,last_green=True; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRJ` | 253 | $5.51 | $3.26 | — | $8,379.19 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+13.1; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 53 | $26.25 | $2.15 | — | $6,986.05 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1823 | $0.77 | $19.43 | — | $5,570.20 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DLO` | 91 | $15.28 | $2.26 | — | $4,177.46 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=-0.1; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ENHA` | 604 | $2.31 | $7.79 | — | $2,774.43 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=-5.3; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FIRY` | 143 | $9.74 | $2.42 | — | $1,379.19 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+1.2; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `GEMI` | 352 | $3.90 | $4.54 | — | $1.85 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+8.0; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 141 | $10.97 | $2.45 | $+146.71 | $1,546.17 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `AIRJ` | 253 | $6.22 | $3.32 | $+173.05 | $3,116.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 53 | $23.00 | $2.17 | $-176.30 | $4,333.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1823 | $0.55 | $15.84 | $-425.40 | $5,323.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DLO` | 91 | $14.23 | $2.29 | $-100.10 | $6,616.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ENHA` | 604 | $2.01 | $7.90 | $-196.89 | $7,822.57 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `FIRY` | 143 | $9.82 | $2.45 | $+6.57 | $9,224.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `GEMI` | 352 | $3.89 | $4.61 | $-12.67 | $10,589.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 38 | $34.05 | $2.10 | — | $9,293.05 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+9.3; leftover $1323.63 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 58 | $22.44 | $2.16 | — | $7,989.36 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.1; leftover $1323.63 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 10 | $123.47 | $2.02 | — | $6,752.64 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+2.9; leftover $1323.63 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 133 | $9.94 | $2.39 | — | $5,428.23 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+12.6; leftover $1323.63 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `COTY` | 519 | $2.55 | $6.70 | — | $4,098.09 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+9.8; leftover $1323.63 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DQ` | 91 | $14.44 | $2.26 | — | $2,781.78 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-3.8; leftover $1323.63 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 11 | $117.65 | $2.02 | — | $1,485.61 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+4.1; leftover $1323.63 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IOND` | 20 | $65.60 | $2.05 | — | $171.56 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+3.7; leftover $1323.63 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 38 | $34.31 | $2.12 | $+5.65 | $1,473.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 58 | $22.20 | $2.18 | $-18.27 | $2,758.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BABA` | 10 | $125.35 | $2.04 | $+14.74 | $4,010.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BULL` | 133 | $8.99 | $2.42 | $-131.16 | $5,203.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `COTY` | 519 | $2.71 | $6.79 | $+69.55 | $6,603.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DQ` | 91 | $15.00 | $2.29 | $+46.41 | $7,965.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `FUTU` | 11 | $115.18 | $2.04 | $-31.24 | $9,230.69 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `IOND` | 20 | $68.41 | $2.07 | $+52.08 | $10,596.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 37 | $93.98 | $2.10 | — | $7,117.45 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.4; leftover $3532.27 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 81 | $43.08 | $2.23 | — | $3,625.74 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-4.9; leftover $3532.27 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 1535 | $2.30 | $19.80 | — | $75.44 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-3.0; leftover $3532.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 37 | $97.02 | $2.14 | $+108.24 | $3,663.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 81 | $44.54 | $2.28 | $+113.75 | $7,268.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 1535 | $2.34 | $20.09 | $+21.51 | $10,840.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 17 | $86.86 | $2.04 | — | $9,361.66 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-4.3; leftover $1548.62 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 100 | $15.34 | $2.29 | — | $7,825.37 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+2.8; leftover $1548.62 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 8 | $179.33 | $2.01 | — | $6,388.72 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-9.3; leftover $1548.62 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 108 | $14.26 | $2.31 | — | $4,846.32 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-1.9; leftover $1548.62 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 328 | $4.71 | $4.23 | — | $3,297.21 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-9.9; leftover $1548.62 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TUYA` | 874 | $1.77 | $11.27 | — | $1,738.96 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-1.1; leftover $1548.62 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VIPS` | 111 | $13.91 | $2.32 | — | $192.62 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+2.5; leftover $1548.62 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `BNS` | 17 | $92.64 | $2.06 | $+94.16 | $1,765.44 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 100 | $16.77 | $2.32 | $+138.39 | $3,440.12 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 8 | $121.87 | $2.03 | $-463.73 | $4,413.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GRRR` | 108 | $14.03 | $2.34 | $-29.50 | $5,925.94 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SHMD` | 328 | $3.38 | $4.30 | $-444.77 | $7,030.29 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TUYA` | 874 | $1.78 | $11.43 | $-13.97 | $8,574.57 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `VIPS` | 111 | $14.00 | $2.35 | $+5.31 | $10,126.22 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.47 | $2.00 | — | $9,078.34 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.9; leftover $1265.78 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 15 | $82.64 | $2.04 | — | $7,836.70 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-0.9; leftover $1265.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 160 | $7.91 | $2.47 | — | $6,568.63 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+5.4; leftover $1265.78 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PD` | 101 | $12.45 | $2.29 | — | $5,308.89 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+3.5; leftover $1265.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `RBRK` | 12 | $101.99 | $2.03 | — | $4,082.98 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.1; leftover $1265.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `S` | 58 | $21.80 | $2.16 | — | $2,816.42 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-8.3; leftover $1265.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 2 | $536.07 | $2.00 | — | $1,742.28 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+2.1; leftover $1265.78 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `WDAY` | 6 | $195.40 | $2.01 | — | $567.88 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.7; leftover $1265.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $258.50 | $2.02 | $-15.90 | $1,599.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 15 | $99.99 | $2.06 | $+256.16 | $3,097.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 160 | $8.43 | $2.51 | $+78.22 | $4,443.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PD` | 101 | $13.92 | $2.32 | $+143.86 | $5,847.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RBRK` | 12 | $92.46 | $2.05 | $-118.43 | $6,955.01 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `S` | 58 | $21.48 | $2.18 | $-22.91 | $8,198.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ULTA` | 2 | $517.50 | $2.02 | $-41.15 | $9,231.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `WDAY` | 6 | $202.96 | $2.03 | $+41.32 | $10,447.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 246 | $5.30 | $3.17 | — | $9,140.41 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+1.1; leftover $1305.92 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 5 | $244.98 | $2.00 | — | $7,913.51 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+2.3; leftover $1305.92 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 25 | $51.99 | $2.06 | — | $6,611.69 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-9.0; leftover $1305.92 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MOMO` | 240 | $5.43 | $3.10 | — | $5,305.39 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+0.0; leftover $1305.92 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NTSK` | 93 | $13.94 | $2.27 | — | $4,006.71 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-8.2; leftover $1305.92 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PHR` | 110 | $11.79 | $2.32 | — | $2,707.49 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-1.3; leftover $1305.92 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PVH` | 17 | $73.10 | $2.04 | — | $1,462.74 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-4.8; leftover $1305.92 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SNOW` | 4 | $310.54 | $2.00 | — | $218.58 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+1.2; leftover $1305.92 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 246 | $6.90 | $3.23 | $+387.20 | $1,912.75 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 5 | $256.99 | $2.03 | $+56.02 | $3,195.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 25 | $47.60 | $2.08 | $-113.90 | $4,383.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MOMO` | 240 | $5.50 | $3.15 | $+10.56 | $5,700.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NTSK` | 93 | $15.51 | $2.30 | $+141.45 | $7,140.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PHR` | 110 | $11.02 | $2.35 | $-89.37 | $8,350.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PVH` | 17 | $74.96 | $2.06 | $+27.52 | $9,622.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SNOW` | 4 | $377.24 | $2.02 | $+262.77 | $11,129.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 20 | $66.61 | $2.05 | — | $9,795.38 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-10.1; leftover $1391.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 136 | $10.16 | $2.40 | — | $8,411.22 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+4.8; leftover $1391.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 20 | $67.06 | $2.05 | — | $7,067.97 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-0.1; leftover $1391.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 368 | $3.78 | $4.75 | — | $5,672.18 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-2.8; leftover $1391.20 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 7 | $198.00 | $2.01 | — | $4,284.17 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+7.7; leftover $1391.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 36 | $37.69 | $2.10 | — | $2,925.23 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.4; leftover $1391.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 11 | $121.15 | $2.02 | — | $1,590.56 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+1.3; leftover $1391.20 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 89 | $15.62 | $2.26 | — | $198.12 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-4.7; leftover $1391.20 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SQM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `YMM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-26 | `BNS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BZ` | no_price | no 09:30 open — carry |
| 2026-08-26 | `DKS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GRRR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SHMD` | no_price | no 09:30 open — carry |
| 2026-08-26 | `TUYA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `VIPS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `TIGR` | no_price | no 09:30 open |
| 2026-08-26 | `ANF` | no_price | no 09:30 open |
| 2026-08-26 | `BOX` | no_price | no 09:30 open |
| 2026-08-26 | `HEI` | no_price | no 09:30 open |
| 2026-08-26 | `INTU` | no_price | no 09:30 open |
| 2026-08-26 | `KSS` | no_price | no 09:30 open |
| 2026-08-26 | `NCNO` | no_price | no 09:30 open |
| 2026-08-26 | `QMLS` | no_price | no 09:30 open |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `AMBA` | 20 | 2026-09-04 @ $66.61 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-10.1; leftover $1391.20 |
| `ASAN` | 136 | 2026-09-04 @ $10.16 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+4.8; leftover $1391.20 |
| `DOCU` | 20 | 2026-09-04 @ $67.06 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-0.1; leftover $1391.20 |
| `DOMO` | 368 | 2026-09-04 @ $3.78 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-2.8; leftover $1391.20 |
| `GWRE` | 7 | 2026-09-04 @ $198.00 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+7.7; leftover $1391.20 |
| `IOT` | 36 | 2026-09-04 @ $37.69 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.4; leftover $1391.20 |
| `LULU` | 11 | 2026-09-04 @ $121.15 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+1.3; leftover $1391.20 |
| `MAMA` | 89 | 2026-09-04 @ $15.62 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-4.7; leftover $1391.20 |
