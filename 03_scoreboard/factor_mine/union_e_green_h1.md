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

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | INO | — | $2.29 | $10,960.69 | INO×12176 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $2.29 | INO×12176 | $11,325.97 | +365.28 | NMAX, AIRJ, BRUN, BZAI, DLO, ENHA, FIRY, GEMI | INO | $1.85 | $10,570.62 | NMAX×141, AIRJ×253, BRUN×53, BZAI×1823, DLO×91, ENHA×604, FIRY×143, GEMI×352 | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $11,325.97 vs prior close $10,960.69 (+365.28) because holdings re-marked: INO×12176 yday $0.90 → 09:30 $0.93 +365.28 |
| 2026-08-17 | +2.25 | $1.85 | NMAX×141, AIRJ×253, BRUN×53, BZAI×1823, DLO×91, ENHA×604, FIRY×143, GEMI×352 | $10,630.08 | +59.46 | — | NMAX, AIRJ, BRUN, BZAI, DLO, ENHA, FIRY, GEMI | $10,589.05 | $10,589.05 | — | 09:30 open · cash $1.85 (unchanged overnight, no fees) · equity $10,630.08 vs prior close $10,570.62 (+59.46) because holdings re-marked: NMAX×141 yday $10.87 → 09:30 $10.97 +14.10; AIRJ×253 yday $6.04 → 09:30 $6.22 +45.54; BRUN×53 yday $22.93 → 09:30 $23.00 +3.71; BZAI×1823 yday $0.59 → 09:30 $0.55 -74.74; DLO×91 yday $14.17 → 09:30 $14.23 +5.46; ENHA×604 yday $1.96 → 09:30 $2.01 +30.20; FIRY×143 yday $9.50 → 09:30 $9.82 +45.76; GEMI×352 yday $3.92 → 09:30 $3.89 -10.56 |
| 2026-08-18 | -6.20 | $10,589.05 | — | $10,589.05 | -0.00 | — | — | $10,589.05 | $10,589.05 | — | 09:30 open · cash $10,589.05 · no holdings · equity $10,589.05 vs prior close $10,589.05 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-19 | -7.20 | $10,589.05 | — | $10,589.05 | -0.00 | — | — | $10,589.05 | $10,589.05 | — | 09:30 open · cash $10,589.05 · no holdings · equity $10,589.05 vs prior close $10,589.05 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $10,589.05 | — | $10,589.05 | -0.00 | ATAT, ATHM, BABA, BULL, COTY, DQ, FUTU, IOND | — | $171.56 | $10,644.23 | ATAT×38, ATHM×58, BABA×10, BULL×133, COTY×519, DQ×91, FUTU×11, IOND×20 | 09:30 open · cash $10,589.05 · no holdings · equity $10,589.05 vs prior close $10,589.05 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $171.56 | ATAT×38, ATHM×58, BABA×10, BULL×133, COTY×519, DQ×91, FUTU×11, IOND×20 | $10,618.78 | -25.45 | BJ, BKE, PSEC | ATAT, ATHM, BABA, BULL, COTY, DQ, FUTU, IOND | $75.44 | $10,768.14 | BJ×37, BKE×81, PSEC×1535 | 09:30 open · cash $171.56 (unchanged overnight, no fees) · equity $10,618.78 vs prior close $10,644.23 (-25.45) because holdings re-marked: ATAT×38 yday $34.25 → 09:30 $34.31 +2.28; ATHM×58 yday $22.12 → 09:30 $22.20 +4.64; BABA×10 yday $130.53 → 09:30 $125.35 -51.80; BULL×133 yday $8.85 → 09:30 $8.99 +18.62; COTY×519 yday $2.75 → 09:30 $2.71 -20.76; DQ×91 yday $14.98 → 09:30 $15.00 +1.82; FUTU×11 yday $112.73 → 09:30 $115.18 +26.95; IOND×20 yday $68.77 → 09:30 $68.41 -7.20 |
| 2026-08-24 | -5.17 | $75.44 | BJ×37, BKE×81, PSEC×1535 | $10,864.82 | +96.68 | — | BJ, BKE, PSEC | $10,840.32 | $10,840.32 | — | 09:30 open · cash $75.44 (unchanged overnight, no fees) · equity $10,864.82 vs prior close $10,768.14 (+96.68) because holdings re-marked: BJ×37 yday $96.42 → 09:30 $97.02 +22.20; BKE×81 yday $43.81 → 09:30 $44.54 +59.13; PSEC×1535 yday $2.33 → 09:30 $2.34 +15.35 |
| 2026-08-25 | +1.80 | $10,840.32 | — | $10,840.32 | +0.00 | BNS, BZ, DKS, GRRR, SHMD, TUYA, VIPS | — | $192.62 | $10,813.87 | BNS×17, BZ×100, DKS×8, GRRR×108, SHMD×328, TUYA×874, VIPS×111 | 09:30 open · cash $10,840.32 · no holdings · equity $10,840.32 vs prior close $10,840.32 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $192.62 | BNS×17, BZ×100, DKS×8, GRRR×108, SHMD×328, TUYA×874, VIPS×111 | $10,813.87 | +0.00 | — | — | $192.62 | $10,813.83 | BNS×17, BZ×100, DKS×8, GRRR×108, SHMD×328, TUYA×874, VIPS×111 | 09:30 open · cash $192.62 (unchanged overnight, no fees) · equity $10,813.87 vs prior close $10,813.87 (+0.00) because holdings re-marked: BNS×17 yday $90.08 → 09:30 $90.08 +0.00; BZ×100 yday $16.32 → 09:30 $16.32 +0.00; DKS×8 yday $156.70 → 09:30 $156.70 +0.00; GRRR×108 yday $14.20 → 09:30 $14.20 +0.00; SHMD×328 yday $4.71 → 09:30 $4.71 +0.00; TUYA×874 yday $1.82 → 09:30 $1.82 +0.00; VIPS×111 yday $13.83 → 09:30 $13.83 +0.00 |
| 2026-08-27 | — | $192.62 | BNS×17, BZ×100, DKS×8, GRRR×108, SHMD×328, TUYA×874, VIPS×111 | $10,153.06 | -660.77 | — | BNS, BZ, DKS, GRRR, SHMD, TUYA, VIPS | $10,126.22 | $10,126.22 | — | 09:30 open · cash $192.62 (unchanged overnight, no fees) · equity $10,153.06 vs prior close $10,813.83 (-660.77) because holdings re-marked: BNS×17 yday $90.08 → 09:30 $92.64 +43.52; BZ×100 yday $16.32 → 09:30 $16.77 +45.00; DKS×8 yday $156.70 → 09:30 $121.87 -278.64; GRRR×108 yday $14.20 → 09:30 $14.03 -18.36; SHMD×328 yday $4.71 → 09:30 $3.38 -436.24; TUYA×874 yday $1.82 → 09:30 $1.78 -34.96; VIPS×111 yday $13.83 → 09:30 $14.00 +18.87 |
| 2026-08-28 | +0.75 | $10,126.22 | — | $10,126.22 | -0.00 | ADSK, ESTC, HAFN, PD, RBRK, S, ULTA, WDAY | — | $567.88 | $10,351.37 | ADSK×4, ESTC×15, HAFN×160, PD×101, RBRK×12, S×58, ULTA×2, WDAY×6 | 09:30 open · cash $10,126.22 · no holdings · equity $10,126.22 vs prior close $10,126.22 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-31 | -5.85 | $567.88 | ADSK×4, ESTC×15, HAFN×160, PD×101, RBRK×12, S×58, ULTA×2, WDAY×6 | $10,464.57 | +113.20 | — | ADSK, ESTC, HAFN, PD, RBRK, S, ULTA, WDAY | $10,447.38 | $10,447.38 | — | 09:30 open · cash $567.88 (unchanged overnight, no fees) · equity $10,464.57 vs prior close $10,351.37 (+113.20) because holdings re-marked: ADSK×4 yday $270.58 → 09:30 $258.50 -48.32; ESTC×15 yday $83.74 → 09:30 $99.99 +243.75; HAFN×160 yday $8.29 → 09:30 $8.43 +22.40; PD×101 yday $12.63 → 09:30 $13.92 +130.29; RBRK×12 yday $107.02 → 09:30 $92.46 -174.72; S×58 yday $22.71 → 09:30 $21.48 -71.34; ULTA×2 yday $540.10 → 09:30 $517.50 -45.20; WDAY×6 yday $193.57 → 09:30 $202.96 +56.34 |
| 2026-09-01 | -6.30 | $10,447.38 | — | $10,447.38 | +0.00 | — | — | $10,447.38 | $10,447.38 | — | 09:30 open · cash $10,447.38 · no holdings · equity $10,447.38 vs prior close $10,447.38 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-02 | -3.83 | $10,447.38 | — | $10,447.38 | +0.00 | — | — | $10,447.38 | $10,447.38 | — | 09:30 open · cash $10,447.38 · no holdings · equity $10,447.38 vs prior close $10,447.38 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $10,447.38 | — | $10,447.38 | +0.00 | CHPT, FIVE, HPE, MOMO, NTSK, PHR, PVH, SNOW | — | $218.58 | $10,358.61 | CHPT×246, FIVE×5, HPE×25, MOMO×240, NTSK×93, PHR×110, PVH×17, SNOW×4 | 09:30 open · cash $10,447.38 · no holdings · equity $10,447.38 vs prior close $10,447.38 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $218.58 | CHPT×246, FIVE×5, HPE×25, MOMO×240, NTSK×93, PHR×110, PVH×17, SNOW×4 | $11,148.84 | +790.23 | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | CHPT, FIVE, HPE, MOMO, NTSK, PHR, PVH, SNOW | $198.12 | $11,127.01 | AMBA×20, ASAN×136, DOCU×20, DOMO×368, GWRE×7, IOT×36, LULU×11, MAMA×89 | 09:30 open · cash $218.58 (unchanged overnight, no fees) · equity $11,148.84 vs prior close $10,358.61 (+790.23) because holdings re-marked: CHPT×246 yday $5.19 → 09:30 $6.90 +420.66; FIVE×5 yday $243.08 → 09:30 $256.99 +69.55; HPE×25 yday $51.83 → 09:30 $47.60 -105.75; MOMO×240 yday $5.49 → 09:30 $5.50 +2.40; NTSK×93 yday $13.75 → 09:30 $15.51 +163.68; PHR×110 yday $11.85 → 09:30 $11.02 -91.30; PVH×17 yday $72.29 → 09:30 $74.96 +45.39; SNOW×4 yday $305.84 → 09:30 $377.24 +285.60 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 12176 | $0.81 | $135.15 | — | $2.29 | — | combo gate; gate earn_react=True,last_green=True; list flatten; ⚪; ret5=+13.2; leftover $10000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.29 | ▲ 09:30 equity $11,325.97 vs yday $10,960.69 (+365.28) | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $11,325.97 vs prior close $10,960.69 (+365.28) because holdings re-marked: INO×12176 yday $0.90 → 09:30 $0.93 +365.28 | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 12176 | $0.93 | $151.88 | $+1174.09 | $11,174.09 | ▲ +1,174.09 after sell → book $11,174.09; vs 09:30 mark -151.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 141 | $9.89 | $2.41 | — | $9,776.48 | — | combo gate; gate earn_react=True,last_green=True; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRJ` | 253 | $5.51 | $3.26 | — | $8,379.19 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+13.1; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 53 | $26.25 | $2.15 | — | $6,986.05 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1823 | $0.77 | $19.43 | — | $5,570.20 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DLO` | 91 | $15.28 | $2.26 | — | $4,177.46 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=-0.1; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ENHA` | 604 | $2.31 | $7.79 | — | $2,774.43 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=-5.3; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FIRY` | 143 | $9.74 | $2.42 | — | $1,379.19 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+1.2; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `GEMI` | 352 | $3.90 | $4.54 | — | $1.85 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+8.0; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.85 | ▲ 09:30 equity $10,630.08 vs yday $10,570.62 (+59.46) | 09:30 open · cash $1.85 (unchanged overnight, no fees) · equity $10,630.08 vs prior close $10,570.62 (+59.46) because holdings re-marked: NMAX×141 yday $10.87 → 09:30 $10.97 +14.10; AIRJ×253 yday $6.04 → 09:30 $6.22 +45.54; BRUN×53 yday $22.93 → 09:30 $23.00 +3.71; BZAI×1823 yday $0.59 → 09:30 $0.55 -74.74; DLO×91 yday $14.17 → 09:30 $14.23 +5.46; ENHA×604 yday $1.96 → 09:30 $2.01 +30.20; FIRY×143 yday $9.50 → 09:30 $9.82 +45.76; GEMI×352 yday $3.92 → 09:30 $3.89 -10.56 | — |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 141 | $10.97 | $2.45 | $+146.71 | $1,546.17 | ▲ +146.71 after sell → book $10,627.64; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `AIRJ` | 253 | $6.22 | $3.32 | $+173.05 | $3,116.51 | ▲ +173.05 after sell → book $10,624.32; vs 09:30 mark -3.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 53 | $23.00 | $2.17 | $-176.30 | $4,333.34 | ▼ -176.30 after sell → book $10,622.15; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1823 | $0.55 | $15.84 | $-425.40 | $5,323.79 | ▼ -425.40 after sell → book $10,606.30; vs 09:30 mark -15.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DLO` | 91 | $14.23 | $2.29 | $-100.10 | $6,616.44 | ▼ -100.10 after sell → book $10,604.02; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ENHA` | 604 | $2.01 | $7.90 | $-196.89 | $7,822.57 | ▼ -196.89 after sell → book $10,596.11; vs 09:30 mark -7.91 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `FIRY` | 143 | $9.82 | $2.45 | $+6.57 | $9,224.38 | ▲ +6.57 after sell → book $10,593.66; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `GEMI` | 352 | $3.89 | $4.61 | $-12.67 | $10,589.05 | ▼ -12.67 after sell → book $10,589.05; vs 09:30 mark -4.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,589.05 | ▲ 09:30 equity $10,589.05 vs yday $10,589.05 (-0.00) | 09:30 open · cash $10,589.05 · no holdings · equity $10,589.05 vs prior close $10,589.05 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,589.05 | ▲ 09:30 equity $10,589.05 vs yday $10,589.05 (-0.00) | 09:30 open · cash $10,589.05 · no holdings · equity $10,589.05 vs prior close $10,589.05 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,589.05 | ▲ 09:30 equity $10,589.05 vs yday $10,589.05 (-0.00) | 09:30 open · cash $10,589.05 · no holdings · equity $10,589.05 vs prior close $10,589.05 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 38 | $34.05 | $2.10 | — | $9,293.05 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+9.3; leftover $1323.63 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 58 | $22.44 | $2.16 | — | $7,989.36 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.1; leftover $1323.63 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 10 | $123.47 | $2.02 | — | $6,752.64 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+2.9; leftover $1323.63 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 133 | $9.94 | $2.39 | — | $5,428.23 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+12.6; leftover $1323.63 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `COTY` | 519 | $2.55 | $6.70 | — | $4,098.09 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+9.8; leftover $1323.63 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DQ` | 91 | $14.44 | $2.26 | — | $2,781.78 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-3.8; leftover $1323.63 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 11 | $117.65 | $2.02 | — | $1,485.61 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+4.1; leftover $1323.63 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IOND` | 20 | $65.60 | $2.05 | — | $171.56 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+3.7; leftover $1323.63 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $171.56 | ▼ 09:30 equity $10,618.78 vs yday $10,644.23 (-25.45) | 09:30 open · cash $171.56 (unchanged overnight, no fees) · equity $10,618.78 vs prior close $10,644.23 (-25.45) because holdings re-marked: ATAT×38 yday $34.25 → 09:30 $34.31 +2.28; ATHM×58 yday $22.12 → 09:30 $22.20 +4.64; BABA×10 yday $130.53 → 09:30 $125.35 -51.80; BULL×133 yday $8.85 → 09:30 $8.99 +18.62; COTY×519 yday $2.75 → 09:30 $2.71 -20.76; DQ×91 yday $14.98 → 09:30 $15.00 +1.82; FUTU×11 yday $112.73 → 09:30 $115.18 +26.95; IOND×20 yday $68.77 → 09:30 $68.41 -7.20 | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 38 | $34.31 | $2.12 | $+5.65 | $1,473.22 | ▲ +5.65 after sell → book $10,616.66; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 58 | $22.20 | $2.18 | $-18.27 | $2,758.63 | ▼ -18.27 after sell → book $10,614.47; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BABA` | 10 | $125.35 | $2.04 | $+14.74 | $4,010.09 | ▲ +14.74 after sell → book $10,612.43; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BULL` | 133 | $8.99 | $2.42 | $-131.16 | $5,203.34 | ▼ -131.16 after sell → book $10,610.01; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `COTY` | 519 | $2.71 | $6.79 | $+69.55 | $6,603.04 | ▲ +69.55 after sell → book $10,603.22; vs 09:30 mark -6.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DQ` | 91 | $15.00 | $2.29 | $+46.41 | $7,965.75 | ▲ +46.41 after sell → book $10,600.93; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `FUTU` | 11 | $115.18 | $2.04 | $-31.24 | $9,230.69 | ▼ -31.24 after sell → book $10,598.89; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `IOND` | 20 | $68.41 | $2.07 | $+52.08 | $10,596.82 | ▲ +52.08 after sell → book $10,596.82; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 37 | $93.98 | $2.10 | — | $7,117.45 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.4; leftover $3532.27 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 81 | $43.08 | $2.23 | — | $3,625.74 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-4.9; leftover $3532.27 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 1535 | $2.30 | $19.80 | — | $75.44 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-3.0; leftover $3532.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.44 | ▲ 09:30 equity $10,864.82 vs yday $10,768.14 (+96.68) | 09:30 open · cash $75.44 (unchanged overnight, no fees) · equity $10,864.82 vs prior close $10,768.14 (+96.68) because holdings re-marked: BJ×37 yday $96.42 → 09:30 $97.02 +22.20; BKE×81 yday $43.81 → 09:30 $44.54 +59.13; PSEC×1535 yday $2.33 → 09:30 $2.34 +15.35 | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 37 | $97.02 | $2.14 | $+108.24 | $3,663.04 | ▲ +108.24 after sell → book $10,862.68; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 81 | $44.54 | $2.28 | $+113.75 | $7,268.51 | ▲ +113.75 after sell → book $10,860.41; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 1535 | $2.34 | $20.09 | $+21.51 | $10,840.32 | ▲ +21.51 after sell → book $10,840.32; vs 09:30 mark -20.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,840.32 | ▲ 09:30 equity $10,840.32 vs yday $10,840.32 (+0.00) | 09:30 open · cash $10,840.32 · no holdings · equity $10,840.32 vs prior close $10,840.32 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 17 | $86.86 | $2.04 | — | $9,361.66 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-4.3; leftover $1548.62 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 100 | $15.34 | $2.29 | — | $7,825.37 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+2.8; leftover $1548.62 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 8 | $179.33 | $2.01 | — | $6,388.72 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-9.3; leftover $1548.62 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 108 | $14.26 | $2.31 | — | $4,846.32 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-1.9; leftover $1548.62 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 328 | $4.71 | $4.23 | — | $3,297.21 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-9.9; leftover $1548.62 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TUYA` | 874 | $1.77 | $11.27 | — | $1,738.96 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-1.1; leftover $1548.62 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VIPS` | 111 | $13.91 | $2.32 | — | $192.62 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+2.5; leftover $1548.62 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $192.62 | ▲ 09:30 equity $10,813.87 vs yday $10,813.87 (+0.00) | 09:30 open · cash $192.62 (unchanged overnight, no fees) · equity $10,813.87 vs prior close $10,813.87 (+0.00) because holdings re-marked: BNS×17 yday $90.08 → 09:30 $90.08 +0.00; BZ×100 yday $16.32 → 09:30 $16.32 +0.00; DKS×8 yday $156.70 → 09:30 $156.70 +0.00; GRRR×108 yday $14.20 → 09:30 $14.20 +0.00; SHMD×328 yday $4.71 → 09:30 $4.71 +0.00; TUYA×874 yday $1.82 → 09:30 $1.82 +0.00; VIPS×111 yday $13.83 → 09:30 $13.83 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $192.62 | ▼ 09:30 equity $10,153.06 vs yday $10,813.83 (-660.77) | 09:30 open · cash $192.62 (unchanged overnight, no fees) · equity $10,153.06 vs prior close $10,813.83 (-660.77) because holdings re-marked: BNS×17 yday $90.08 → 09:30 $92.64 +43.52; BZ×100 yday $16.32 → 09:30 $16.77 +45.00; DKS×8 yday $156.70 → 09:30 $121.87 -278.64; GRRR×108 yday $14.20 → 09:30 $14.03 -18.36; SHMD×328 yday $4.71 → 09:30 $3.38 -436.24; TUYA×874 yday $1.82 → 09:30 $1.78 -34.96; VIPS×111 yday $13.83 → 09:30 $14.00 +18.87 | — |
| 2026-08-27 09:30 ET | **SELL** | `BNS` | 17 | $92.64 | $2.06 | $+94.16 | $1,765.44 | ▲ +94.16 after sell → book $10,151.00; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 100 | $16.77 | $2.32 | $+138.39 | $3,440.12 | ▲ +138.39 after sell → book $10,148.68; vs 09:30 mark -2.32 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 8 | $121.87 | $2.03 | $-463.73 | $4,413.04 | ▼ -463.73 after sell → book $10,146.64; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GRRR` | 108 | $14.03 | $2.34 | $-29.50 | $5,925.94 | ▼ -29.50 after sell → book $10,144.30; vs 09:30 mark -2.34 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SHMD` | 328 | $3.38 | $4.30 | $-444.77 | $7,030.29 | ▼ -444.77 after sell → book $10,140.01; vs 09:30 mark -4.29 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TUYA` | 874 | $1.78 | $11.43 | $-13.97 | $8,574.57 | ▼ -13.97 after sell → book $10,128.57; vs 09:30 mark -11.44 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `VIPS` | 111 | $14.00 | $2.35 | $+5.31 | $10,126.22 | ▲ +5.31 after sell → book $10,126.22; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,126.22 | ▲ 09:30 equity $10,126.22 vs yday $10,126.22 (-0.00) | 09:30 open · cash $10,126.22 · no holdings · equity $10,126.22 vs prior close $10,126.22 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.47 | $2.00 | — | $9,078.34 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.9; leftover $1265.78 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 15 | $82.64 | $2.04 | — | $7,836.70 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-0.9; leftover $1265.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 160 | $7.91 | $2.47 | — | $6,568.63 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+5.4; leftover $1265.78 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PD` | 101 | $12.45 | $2.29 | — | $5,308.89 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+3.5; leftover $1265.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `RBRK` | 12 | $101.99 | $2.03 | — | $4,082.98 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.1; leftover $1265.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `S` | 58 | $21.80 | $2.16 | — | $2,816.42 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-8.3; leftover $1265.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 2 | $536.07 | $2.00 | — | $1,742.28 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+2.1; leftover $1265.78 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `WDAY` | 6 | $195.40 | $2.01 | — | $567.88 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.7; leftover $1265.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $567.88 | ▲ 09:30 equity $10,464.57 vs yday $10,351.37 (+113.20) | 09:30 open · cash $567.88 (unchanged overnight, no fees) · equity $10,464.57 vs prior close $10,351.37 (+113.20) because holdings re-marked: ADSK×4 yday $270.58 → 09:30 $258.50 -48.32; ESTC×15 yday $83.74 → 09:30 $99.99 +243.75; HAFN×160 yday $8.29 → 09:30 $8.43 +22.40; PD×101 yday $12.63 → 09:30 $13.92 +130.29; RBRK×12 yday $107.02 → 09:30 $92.46 -174.72; S×58 yday $22.71 → 09:30 $21.48 -71.34; ULTA×2 yday $540.10 → 09:30 $517.50 -45.20; WDAY×6 yday $193.57 → 09:30 $202.96 +56.34 | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $258.50 | $2.02 | $-15.90 | $1,599.85 | ▼ -15.90 after sell → book $10,462.54; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 15 | $99.99 | $2.06 | $+256.16 | $3,097.65 | ▲ +256.16 after sell → book $10,460.49; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 160 | $8.43 | $2.51 | $+78.22 | $4,443.94 | ▲ +78.22 after sell → book $10,457.98; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PD` | 101 | $13.92 | $2.32 | $+143.86 | $5,847.54 | ▲ +143.86 after sell → book $10,455.66; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RBRK` | 12 | $92.46 | $2.05 | $-118.43 | $6,955.01 | ▼ -118.43 after sell → book $10,453.61; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `S` | 58 | $21.48 | $2.18 | $-22.91 | $8,198.67 | ▼ -22.91 after sell → book $10,451.43; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ULTA` | 2 | $517.50 | $2.02 | $-41.15 | $9,231.65 | ▼ -41.15 after sell → book $10,449.41; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `WDAY` | 6 | $202.96 | $2.03 | $+41.32 | $10,447.38 | ▲ +41.32 after sell → book $10,447.38; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,447.38 | ▲ 09:30 equity $10,447.38 vs yday $10,447.38 (+0.00) | 09:30 open · cash $10,447.38 · no holdings · equity $10,447.38 vs prior close $10,447.38 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,447.38 | ▲ 09:30 equity $10,447.38 vs yday $10,447.38 (+0.00) | 09:30 open · cash $10,447.38 · no holdings · equity $10,447.38 vs prior close $10,447.38 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,447.38 | ▲ 09:30 equity $10,447.38 vs yday $10,447.38 (+0.00) | 09:30 open · cash $10,447.38 · no holdings · equity $10,447.38 vs prior close $10,447.38 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 246 | $5.30 | $3.17 | — | $9,140.41 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+1.1; leftover $1305.92 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 5 | $244.98 | $2.00 | — | $7,913.51 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+2.3; leftover $1305.92 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 25 | $51.99 | $2.06 | — | $6,611.69 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-9.0; leftover $1305.92 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MOMO` | 240 | $5.43 | $3.10 | — | $5,305.39 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+0.0; leftover $1305.92 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NTSK` | 93 | $13.94 | $2.27 | — | $4,006.71 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-8.2; leftover $1305.92 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PHR` | 110 | $11.79 | $2.32 | — | $2,707.49 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-1.3; leftover $1305.92 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PVH` | 17 | $73.10 | $2.04 | — | $1,462.74 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-4.8; leftover $1305.92 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SNOW` | 4 | $310.54 | $2.00 | — | $218.58 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+1.2; leftover $1305.92 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $218.58 | ▲ 09:30 equity $11,148.84 vs yday $10,358.61 (+790.23) | 09:30 open · cash $218.58 (unchanged overnight, no fees) · equity $11,148.84 vs prior close $10,358.61 (+790.23) because holdings re-marked: CHPT×246 yday $5.19 → 09:30 $6.90 +420.66; FIVE×5 yday $243.08 → 09:30 $256.99 +69.55; HPE×25 yday $51.83 → 09:30 $47.60 -105.75; MOMO×240 yday $5.49 → 09:30 $5.50 +2.40; NTSK×93 yday $13.75 → 09:30 $15.51 +163.68; PHR×110 yday $11.85 → 09:30 $11.02 -91.30; PVH×17 yday $72.29 → 09:30 $74.96 +45.39; SNOW×4 yday $305.84 → 09:30 $377.24 +285.60 | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 246 | $6.90 | $3.23 | $+387.20 | $1,912.75 | ▲ +387.20 after sell → book $11,145.61; vs 09:30 mark -3.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 5 | $256.99 | $2.03 | $+56.02 | $3,195.68 | ▲ +56.02 after sell → book $11,143.59; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 25 | $47.60 | $2.08 | $-113.90 | $4,383.59 | ▼ -113.90 after sell → book $11,141.50; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MOMO` | 240 | $5.50 | $3.15 | $+10.56 | $5,700.45 | ▲ +10.56 after sell → book $11,138.36; vs 09:30 mark -3.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NTSK` | 93 | $15.51 | $2.30 | $+141.45 | $7,140.58 | ▲ +141.45 after sell → book $11,136.06; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PHR` | 110 | $11.02 | $2.35 | $-89.37 | $8,350.43 | ▼ -89.37 after sell → book $11,133.71; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PVH` | 17 | $74.96 | $2.06 | $+27.52 | $9,622.69 | ▲ +27.52 after sell → book $11,131.65; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SNOW` | 4 | $377.24 | $2.02 | $+262.77 | $11,129.63 | ▲ +262.77 after sell → book $11,129.63; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 20 | $66.61 | $2.05 | — | $9,795.38 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-10.1; leftover $1391.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 136 | $10.16 | $2.40 | — | $8,411.22 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+4.8; leftover $1391.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 20 | $67.06 | $2.05 | — | $7,067.97 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-0.1; leftover $1391.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 368 | $3.78 | $4.75 | — | $5,672.18 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-2.8; leftover $1391.20 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 7 | $198.00 | $2.01 | — | $4,284.17 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+7.7; leftover $1391.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 36 | $37.69 | $2.10 | — | $2,925.23 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.4; leftover $1391.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 11 | $121.15 | $2.02 | — | $1,590.56 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+1.3; leftover $1391.20 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 89 | $15.62 | $2.26 | — | $198.12 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-4.7; leftover $1391.20 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |

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
