# Factor mine action — `union_e_green_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+47.92%** ($14,792) · signal-only (no cash/fees) was +23.38%. Starts YES **15/17**. Fills 64 · skips 96 · realized $+3500.43.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `earn_react=True,last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $213.89.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | INO | — | $2.29 | $10,960.69 | INO×12176 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $2.29 | INO×12176 | $11,325.97 | +365.28 | — | — | $2.29 | $13,274.13 | INO×12176 | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $11,325.97 vs prior close $10,960.69 (+365.28) because holdings re-marked: INO×12176 yday $0.90 → 09:30 $0.93 +365.28 |
| 2026-08-17 | +2.25 | $2.29 | INO×12176 | $13,030.61 | -243.52 | — | — | $2.29 | $14,004.69 | INO×12176 | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $13,030.61 vs prior close $13,274.13 (-243.52) because holdings re-marked: INO×12176 yday $1.09 → 09:30 $1.07 -243.52 |
| 2026-08-18 | -6.20 | $2.29 | INO×12176 | $13,882.93 | -121.76 | — | INO | $13,723.72 | $13,723.72 | — | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $13,882.93 vs prior close $14,004.69 (-121.76) because holdings re-marked: INO×12176 yday $1.15 → 09:30 $1.14 -121.76 |
| 2026-08-19 | -7.20 | $13,723.72 | — | $13,723.72 | +0.00 | — | — | $13,723.72 | $13,723.72 | — | 09:30 open · cash $13,723.72 · no holdings · equity $13,723.72 vs prior close $13,723.72 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $13,723.72 | — | $13,723.72 | +0.00 | ATAT, ATHM, BABA, BULL, COTY, DQ, FUTU, IOND | — | $206.77 | $13,801.36 | ATAT×50, ATHM×76, BABA×13, BULL×172, COTY×672, DQ×118, FUTU×14, IOND×26 | 09:30 open · cash $13,723.72 · no holdings · equity $13,723.72 vs prior close $13,723.72 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $206.77 | ATAT×50, ATHM×76, BABA×13, BULL×172, COTY×672, DQ×118, FUTU×14, IOND×26 | $13,767.60 | -33.76 | BKE, PSEC | — | $95.80 | $13,656.64 | ATAT×50, ATHM×76, BABA×13, BULL×172, COTY×672, DQ×118, FUTU×14, IOND×26, BKE×1, PSEC×29 | 09:30 open · cash $206.77 (unchanged overnight, no fees) · equity $13,767.60 vs prior close $13,801.36 (-33.76) because holdings re-marked: ATAT×50 yday $34.25 → 09:30 $34.31 +3.00; ATHM×76 yday $22.12 → 09:30 $22.20 +6.08; BABA×13 yday $130.53 → 09:30 $125.35 -67.34; BULL×172 yday $8.85 → 09:30 $8.99 +24.08; COTY×672 yday $2.75 → 09:30 $2.71 -26.88; DQ×118 yday $14.98 → 09:30 $15.00 +2.36; FUTU×14 yday $112.73 → 09:30 $115.18 +34.30; IOND×26 yday $68.77 → 09:30 $68.41 -9.36 |
| 2026-08-24 | -5.17 | $95.80 | ATAT×50, ATHM×76, BABA×13, BULL×172, COTY×672, DQ×118, FUTU×14, IOND×26, BKE×1, PSEC×29 | $13,491.40 | -165.24 | — | — | $95.80 | $13,655.47 | ATAT×50, ATHM×76, BABA×13, BULL×172, COTY×672, DQ×118, FUTU×14, IOND×26, BKE×1, PSEC×29 | 09:30 open · cash $95.80 (unchanged overnight, no fees) · equity $13,491.40 vs prior close $13,656.64 (-165.24) because holdings re-marked: ATAT×50 yday $34.75 → 09:30 $34.70 -2.50; ATHM×76 yday $22.22 → 09:30 $21.78 -33.44; BABA×13 yday $119.34 → 09:30 $116.80 -33.02; BULL×172 yday $8.78 → 09:30 $8.54 -41.28; COTY×672 yday $2.74 → 09:30 $2.72 -13.44; DQ×118 yday $13.58 → 09:30 $13.55 -3.54; FUTU×14 yday $123.64 → 09:30 $120.87 -38.78; IOND×26 yday $68.73 → 09:30 $68.72 -0.26; BKE×1 yday $43.81 → 09:30 $44.54 +0.73; PSEC×29 yday $2.33 → 09:30 $2.34 +0.29 |
| 2026-08-25 | +1.80 | $95.80 | ATAT×50, ATHM×76, BABA×13, BULL×172, COTY×672, DQ×118, FUTU×14, IOND×26, BKE×1, PSEC×29 | $13,552.77 | -102.70 | BNS, BZ, DKS, GRRR, SHMD, TUYA, VIPS | ATAT, ATHM, BABA, BULL, COTY, DQ, FUTU, IOND | $136.04 | $13,499.47 | BKE×1, PSEC×29, BNS×22, BZ×124, DKS×10, GRRR×134, SHMD×406, TUYA×1082, VIPS×137 | 09:30 open · cash $95.80 (unchanged overnight, no fees) · equity $13,552.77 vs prior close $13,655.47 (-102.70) because holdings re-marked: ATAT×50 yday $34.83 → 09:30 $34.75 -4.00; ATHM×76 yday $21.85 → 09:30 $21.85 +0.00; BABA×13 yday $119.46 → 09:30 $116.36 -40.30; BULL×172 yday $8.73 → 09:30 $8.54 -32.68; COTY×672 yday $2.78 → 09:30 $2.80 +13.44; DQ×118 yday $14.15 → 09:30 $14.04 -12.98; FUTU×14 yday $116.49 → 09:30 $118.02 +21.42; IOND×26 yday $70.11 → 09:30 $68.27 -47.84; BKE×1 yday $44.46 → 09:30 $44.41 -0.05; PSEC×29 yday $2.31 → 09:30 $2.32 +0.29 |
| 2026-08-26 | +2.02 | $136.04 | BKE×1, PSEC×29, BNS×22, BZ×124, DKS×10, GRRR×134, SHMD×406, TUYA×1082, VIPS×137 | $13,499.47 | -0.00 | — | — | $136.04 | $13,498.02 | BKE×1, PSEC×29, BNS×22, BZ×124, DKS×10, GRRR×134, SHMD×406, TUYA×1082, VIPS×137 | 09:30 open · cash $136.04 (unchanged overnight, no fees) · equity $13,499.47 vs prior close $13,499.47 (-0.00) because holdings re-marked: BKE×1 yday $44.41 → 09:30 $44.41 +0.00; PSEC×29 yday $2.33 → 09:30 $2.33 +0.00; BNS×22 yday $90.08 → 09:30 $90.08 +0.00; BZ×124 yday $16.32 → 09:30 $16.32 +0.00; DKS×10 yday $156.70 → 09:30 $156.70 +0.00; GRRR×134 yday $14.20 → 09:30 $14.20 +0.00; SHMD×406 yday $4.71 → 09:30 $4.71 +0.00; TUYA×1082 yday $1.82 → 09:30 $1.82 +0.00; VIPS×137 yday $13.83 → 09:30 $13.83 +0.00 |
| 2026-08-27 | — | $136.04 | BKE×1, PSEC×29, BNS×22, BZ×124, DKS×10, GRRR×134, SHMD×406, TUYA×1082, VIPS×137 | $12,681.10 | -816.92 | — | BKE, PSEC | $247.32 | $13,205.40 | BNS×22, BZ×124, DKS×10, GRRR×134, SHMD×406, TUYA×1082, VIPS×137 | 09:30 open · cash $136.04 (unchanged overnight, no fees) · equity $12,681.10 vs prior close $13,498.02 (-816.92) because holdings re-marked: BKE×1 yday $44.41 → 09:30 $44.39 -0.02; PSEC×29 yday $2.33 → 09:30 $2.35 +0.58; BNS×22 yday $90.08 → 09:30 $92.64 +56.32; BZ×124 yday $16.32 → 09:30 $16.77 +55.80; DKS×10 yday $156.70 → 09:30 $121.87 -348.30; GRRR×134 yday $14.20 → 09:30 $14.03 -22.78; SHMD×406 yday $4.71 → 09:30 $3.38 -539.98; TUYA×1082 yday $1.82 → 09:30 $1.78 -43.28; VIPS×137 yday $13.83 → 09:30 $14.00 +23.29 |
| 2026-08-28 | +0.75 | $247.32 | BNS×22, BZ×124, DKS×10, GRRR×134, SHMD×406, TUYA×1082, VIPS×137 | $13,224.68 | +19.28 | ADSK, ESTC, HAFN, PD, RBRK, S, ULTA, WDAY | BNS, BZ, DKS, GRRR, SHMD, TUYA, VIPS | $310.52 | $13,500.97 | ADSK×6, ESTC×19, HAFN×208, PD×132, RBRK×16, S×75, ULTA×3, WDAY×8 | 09:30 open · cash $247.32 (unchanged overnight, no fees) · equity $13,224.68 vs prior close $13,205.40 (+19.28) because holdings re-marked: BNS×22 yday $93.59 → 09:30 $93.52 -1.54; BZ×124 yday $18.84 → 09:30 $18.50 -42.16; DKS×10 yday $129.66 → 09:30 $128.73 -9.30; GRRR×134 yday $15.45 → 09:30 $15.94 +65.66; SHMD×406 yday $3.17 → 09:30 $3.16 -4.06; TUYA×1082 yday $1.83 → 09:30 $1.85 +21.64; VIPS×137 yday $14.08 → 09:30 $14.00 -10.96 |
| 2026-08-31 | -5.85 | $310.52 | ADSK×6, ESTC×19, HAFN×208, PD×132, RBRK×16, S×75, ULTA×3, WDAY×8 | $13,618.75 | +117.78 | — | — | $310.52 | $13,584.32 | ADSK×6, ESTC×19, HAFN×208, PD×132, RBRK×16, S×75, ULTA×3, WDAY×8 | 09:30 open · cash $310.52 (unchanged overnight, no fees) · equity $13,618.75 vs prior close $13,500.97 (+117.78) because holdings re-marked: ADSK×6 yday $270.58 → 09:30 $258.50 -72.48; ESTC×19 yday $83.74 → 09:30 $99.99 +308.75; HAFN×208 yday $8.29 → 09:30 $8.43 +29.12; PD×132 yday $12.63 → 09:30 $13.92 +170.28; RBRK×16 yday $107.02 → 09:30 $92.46 -232.96; S×75 yday $22.71 → 09:30 $21.48 -92.25; ULTA×3 yday $540.10 → 09:30 $517.50 -67.80; WDAY×8 yday $193.57 → 09:30 $202.96 +75.12 |
| 2026-09-01 | -6.30 | $310.52 | ADSK×6, ESTC×19, HAFN×208, PD×132, RBRK×16, S×75, ULTA×3, WDAY×8 | $13,597.54 | +13.22 | — | — | $310.52 | $13,554.19 | ADSK×6, ESTC×19, HAFN×208, PD×132, RBRK×16, S×75, ULTA×3, WDAY×8 | 09:30 open · cash $310.52 (unchanged overnight, no fees) · equity $13,597.54 vs prior close $13,584.32 (+13.22) because holdings re-marked: ADSK×6 yday $259.14 → 09:30 $258.17 -5.82; ESTC×19 yday $99.00 → 09:30 $96.54 -46.74; HAFN×208 yday $8.45 → 09:30 $8.43 -4.16; PD×132 yday $13.70 → 09:30 $13.89 +25.08; RBRK×16 yday $92.46 → 09:30 $90.89 -25.12; S×75 yday $21.50 → 09:30 $22.11 +45.75; ULTA×3 yday $517.50 → 09:30 $538.75 +63.75; WDAY×8 yday $203.45 → 09:30 $198.51 -39.52 |
| 2026-09-02 | -3.83 | $310.52 | ADSK×6, ESTC×19, HAFN×208, PD×132, RBRK×16, S×75, ULTA×3, WDAY×8 | $13,518.04 | -36.15 | — | ADSK, ESTC, HAFN, PD, RBRK, S, ULTA, WDAY | $13,500.43 | $13,500.43 | — | 09:30 open · cash $310.52 (unchanged overnight, no fees) · equity $13,518.04 vs prior close $13,554.19 (-36.15) because holdings re-marked: ADSK×6 yday $259.89 → 09:30 $253.48 -38.46; ESTC×19 yday $96.07 → 09:30 $95.76 -5.89; HAFN×208 yday $8.41 → 09:30 $8.56 +31.20; PD×132 yday $13.89 → 09:30 $13.91 +2.64; RBRK×16 yday $91.50 → 09:30 $91.70 +3.20; S×75 yday $21.84 → 09:30 $21.72 -9.00; ULTA×3 yday $537.60 → 09:30 $527.84 -29.28; WDAY×8 yday $195.18 → 09:30 $196.36 +9.44 |
| 2026-09-03 | -0.90 | $13,500.43 | — | $13,500.43 | -0.00 | CHPT, FIVE, HPE, MOMO, NTSK, PHR, PVH, SNOW | — | $370.42 | $13,389.95 | CHPT×318, FIVE×6, HPE×32, MOMO×310, NTSK×121, PHR×143, PVH×23, SNOW×5 | 09:30 open · cash $13,500.43 · no holdings · equity $13,500.43 vs prior close $13,500.43 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $370.42 | CHPT×318, FIVE×6, HPE×32, MOMO×310, NTSK×121, PHR×143, PVH×23, SNOW×5 | $14,397.61 | +1,007.66 | ASAN, DOMO, IOT, MAMA | — | $213.89 | $14,792.05 | CHPT×318, FIVE×6, HPE×32, MOMO×310, NTSK×121, PHR×143, PVH×23, SNOW×5, ASAN×4, DOMO×12, IOT×1, MAMA×2 | 09:30 open · cash $370.42 (unchanged overnight, no fees) · equity $14,397.61 vs prior close $13,389.95 (+1007.66) because holdings re-marked: CHPT×318 yday $5.19 → 09:30 $6.90 +543.78; FIVE×6 yday $243.08 → 09:30 $256.99 +83.46; HPE×32 yday $51.83 → 09:30 $47.60 -135.36; MOMO×310 yday $5.49 → 09:30 $5.50 +3.10; NTSK×121 yday $13.75 → 09:30 $15.51 +212.96; PHR×143 yday $11.85 → 09:30 $11.02 -118.69; PVH×23 yday $72.29 → 09:30 $74.96 +61.41; SNOW×5 yday $305.84 → 09:30 $377.24 +357.00 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 12176 | $0.81 | $135.15 | — | $2.29 | — | combo gate; gate earn_react=True,last_green=True; list flatten; ⚪; ret5=+13.2; leftover $10000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.29 | ▲ 09:30 equity $11,325.97 vs yday $10,960.69 (+365.28) | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $11,325.97 vs prior close $10,960.69 (+365.28) because holdings re-marked: INO×12176 yday $0.90 → 09:30 $0.93 +365.28 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.29 | ▼ 09:30 equity $13,030.61 vs yday $13,274.13 (-243.52) | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $13,030.61 vs prior close $13,274.13 (-243.52) because holdings re-marked: INO×12176 yday $1.09 → 09:30 $1.07 -243.52 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.29 | ▼ 09:30 equity $13,882.93 vs yday $14,004.69 (-121.76) | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $13,882.93 vs prior close $14,004.69 (-121.76) because holdings re-marked: INO×12176 yday $1.15 → 09:30 $1.14 -121.76 | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 12176 | $1.14 | $159.20 | $+3723.72 | $13,723.72 | ▲ +3,723.72 after sell → book $13,723.72; vs 09:30 mark -159.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,723.72 | ▲ 09:30 equity $13,723.72 vs yday $13,723.72 (+0.00) | 09:30 open · cash $13,723.72 · no holdings · equity $13,723.72 vs prior close $13,723.72 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,723.72 | ▲ 09:30 equity $13,723.72 vs yday $13,723.72 (+0.00) | 09:30 open · cash $13,723.72 · no holdings · equity $13,723.72 vs prior close $13,723.72 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 50 | $34.05 | $2.14 | — | $12,019.08 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+9.3; leftover $1715.47 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 76 | $22.44 | $2.22 | — | $10,311.43 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.1; leftover $1715.47 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 13 | $123.47 | $2.03 | — | $8,704.29 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+2.9; leftover $1715.47 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 172 | $9.94 | $2.51 | — | $6,992.10 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+12.6; leftover $1715.47 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `COTY` | 672 | $2.55 | $8.67 | — | $5,269.83 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+9.8; leftover $1715.47 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DQ` | 118 | $14.44 | $2.34 | — | $3,563.57 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-3.8; leftover $1715.47 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 14 | $117.65 | $2.03 | — | $1,914.44 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+4.1; leftover $1715.47 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IOND` | 26 | $65.60 | $2.07 | — | $206.77 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+3.7; leftover $1715.47 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $206.77 | ▼ 09:30 equity $13,767.60 vs yday $13,801.36 (-33.76) | 09:30 open · cash $206.77 (unchanged overnight, no fees) · equity $13,767.60 vs prior close $13,801.36 (-33.76) because holdings re-marked: ATAT×50 yday $34.25 → 09:30 $34.31 +3.00; ATHM×76 yday $22.12 → 09:30 $22.20 +6.08; BABA×13 yday $130.53 → 09:30 $125.35 -67.34; BULL×172 yday $8.85 → 09:30 $8.99 +24.08; COTY×672 yday $2.75 → 09:30 $2.71 -26.88; DQ×118 yday $14.98 → 09:30 $15.00 +2.36; FUTU×14 yday $112.73 → 09:30 $115.18 +34.30; IOND×26 yday $68.77 → 09:30 $68.41 -9.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 1 | $43.08 | $0.43 | — | $163.25 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-4.9; leftover $68.92 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 29 | $2.30 | $0.75 | — | $95.80 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-3.0; leftover $68.92 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.80 | ▼ 09:30 equity $13,491.40 vs yday $13,656.64 (-165.24) | 09:30 open · cash $95.80 (unchanged overnight, no fees) · equity $13,491.40 vs prior close $13,656.64 (-165.24) because holdings re-marked: ATAT×50 yday $34.75 → 09:30 $34.70 -2.50; ATHM×76 yday $22.22 → 09:30 $21.78 -33.44; BABA×13 yday $119.34 → 09:30 $116.80 -33.02; BULL×172 yday $8.78 → 09:30 $8.54 -41.28; COTY×672 yday $2.74 → 09:30 $2.72 -13.44; DQ×118 yday $13.58 → 09:30 $13.55 -3.54; FUTU×14 yday $123.64 → 09:30 $120.87 -38.78; IOND×26 yday $68.73 → 09:30 $68.72 -0.26; BKE×1 yday $43.81 → 09:30 $44.54 +0.73; PSEC×29 yday $2.33 → 09:30 $2.34 +0.29 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.80 | ▼ 09:30 equity $13,552.77 vs yday $13,655.47 (-102.70) | 09:30 open · cash $95.80 (unchanged overnight, no fees) · equity $13,552.77 vs prior close $13,655.47 (-102.70) because holdings re-marked: ATAT×50 yday $34.83 → 09:30 $34.75 -4.00; ATHM×76 yday $21.85 → 09:30 $21.85 +0.00; BABA×13 yday $119.46 → 09:30 $116.36 -40.30; BULL×172 yday $8.73 → 09:30 $8.54 -32.68; COTY×672 yday $2.78 → 09:30 $2.80 +13.44; DQ×118 yday $14.15 → 09:30 $14.04 -12.98; FUTU×14 yday $116.49 → 09:30 $118.02 +21.42; IOND×26 yday $70.11 → 09:30 $68.27 -47.84; BKE×1 yday $44.46 → 09:30 $44.41 -0.05; PSEC×29 yday $2.31 → 09:30 $2.32 +0.29 | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 50 | $34.75 | $2.16 | $+30.70 | $1,831.14 | ▲ +30.70 after sell → book $13,550.61; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 76 | $21.85 | $2.24 | $-49.30 | $3,489.49 | ▼ -49.30 after sell → book $13,548.36; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BABA` | 13 | $116.36 | $2.05 | $-96.51 | $5,000.12 | ▼ -96.51 after sell → book $13,546.31; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BULL` | 172 | $8.54 | $2.55 | $-245.85 | $6,466.45 | ▼ -245.85 after sell → book $13,543.76; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `COTY` | 672 | $2.80 | $8.80 | $+150.54 | $8,339.26 | ▲ +150.54 after sell → book $13,534.97; vs 09:30 mark -8.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DQ` | 118 | $14.04 | $2.38 | $-51.92 | $9,993.60 | ▼ -51.92 after sell → book $13,532.59; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `FUTU` | 14 | $118.02 | $2.06 | $+1.09 | $11,643.83 | ▲ +1.09 after sell → book $13,530.54; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `IOND` | 26 | $68.27 | $2.09 | $+65.26 | $13,416.76 | ▲ +65.26 after sell → book $13,528.45; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 22 | $86.86 | $2.06 | — | $11,503.78 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-4.3; leftover $1916.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 124 | $15.34 | $2.36 | — | $9,599.26 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+2.8; leftover $1916.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 10 | $179.33 | $2.02 | — | $7,803.94 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-9.3; leftover $1916.68 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 134 | $14.26 | $2.39 | — | $5,890.71 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-1.9; leftover $1916.68 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 406 | $4.71 | $5.24 | — | $3,973.21 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-9.9; leftover $1916.68 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TUYA` | 1082 | $1.77 | $13.96 | — | $2,044.11 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-1.1; leftover $1916.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VIPS` | 137 | $13.91 | $2.40 | — | $136.04 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+2.5; leftover $1916.68 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $136.04 | ▲ 09:30 equity $13,499.47 vs yday $13,499.47 (-0.00) | 09:30 open · cash $136.04 (unchanged overnight, no fees) · equity $13,499.47 vs prior close $13,499.47 (-0.00) because holdings re-marked: BKE×1 yday $44.41 → 09:30 $44.41 +0.00; PSEC×29 yday $2.33 → 09:30 $2.33 +0.00; BNS×22 yday $90.08 → 09:30 $90.08 +0.00; BZ×124 yday $16.32 → 09:30 $16.32 +0.00; DKS×10 yday $156.70 → 09:30 $156.70 +0.00; GRRR×134 yday $14.20 → 09:30 $14.20 +0.00; SHMD×406 yday $4.71 → 09:30 $4.71 +0.00; TUYA×1082 yday $1.82 → 09:30 $1.82 +0.00; VIPS×137 yday $13.83 → 09:30 $13.83 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $136.04 | ▼ 09:30 equity $12,681.10 vs yday $13,498.02 (-816.92) | 09:30 open · cash $136.04 (unchanged overnight, no fees) · equity $12,681.10 vs prior close $13,498.02 (-816.92) because holdings re-marked: BKE×1 yday $44.41 → 09:30 $44.39 -0.02; PSEC×29 yday $2.33 → 09:30 $2.35 +0.58; BNS×22 yday $90.08 → 09:30 $92.64 +56.32; BZ×124 yday $16.32 → 09:30 $16.77 +55.80; DKS×10 yday $156.70 → 09:30 $121.87 -348.30; GRRR×134 yday $14.20 → 09:30 $14.03 -22.78; SHMD×406 yday $4.71 → 09:30 $3.38 -539.98; TUYA×1082 yday $1.82 → 09:30 $1.78 -43.28; VIPS×137 yday $13.83 → 09:30 $14.00 +23.29 | — |
| 2026-08-27 09:30 ET | **SELL** | `BKE` | 1 | $44.39 | $0.47 | $+0.41 | $179.96 | ▲ +0.41 after sell → book $12,680.63; vs 09:30 mark -0.47 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `PSEC` | 29 | $2.35 | $0.79 | $-0.09 | $247.32 | ▼ -0.09 after sell → book $12,679.84; vs 09:30 mark -0.79 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $247.32 | ▲ 09:30 equity $13,224.68 vs yday $13,205.40 (+19.28) | 09:30 open · cash $247.32 (unchanged overnight, no fees) · equity $13,224.68 vs prior close $13,205.40 (+19.28) because holdings re-marked: BNS×22 yday $93.59 → 09:30 $93.52 -1.54; BZ×124 yday $18.84 → 09:30 $18.50 -42.16; DKS×10 yday $129.66 → 09:30 $128.73 -9.30; GRRR×134 yday $15.45 → 09:30 $15.94 +65.66; SHMD×406 yday $3.17 → 09:30 $3.16 -4.06; TUYA×1082 yday $1.83 → 09:30 $1.85 +21.64; VIPS×137 yday $14.08 → 09:30 $14.00 -10.96 | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 22 | $93.52 | $2.08 | $+142.38 | $2,302.68 | ▲ +142.38 after sell → book $13,222.60; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 124 | $18.50 | $2.40 | $+387.08 | $4,594.28 | ▲ +387.08 after sell → book $13,220.20; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 10 | $128.73 | $2.04 | $-510.06 | $5,879.54 | ▼ -510.06 after sell → book $13,218.16; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 134 | $15.94 | $2.43 | $+220.30 | $8,013.07 | ▲ +220.30 after sell → book $13,215.73; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 406 | $3.16 | $5.32 | $-639.85 | $9,290.71 | ▼ -639.85 after sell → book $13,210.41; vs 09:30 mark -5.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `TUYA` | 1082 | $1.85 | $14.15 | $+58.45 | $11,278.26 | ▲ +58.45 after sell → book $13,196.26; vs 09:30 mark -14.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `VIPS` | 137 | $14.00 | $2.44 | $+7.49 | $13,193.82 | ▲ +7.49 after sell → book $13,193.82; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 6 | $261.47 | $2.01 | — | $11,622.99 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.9; leftover $1649.23 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 19 | $82.64 | $2.05 | — | $10,050.79 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-0.9; leftover $1649.23 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 208 | $7.91 | $2.68 | — | $8,402.82 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+5.4; leftover $1649.23 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PD` | 132 | $12.45 | $2.39 | — | $6,757.04 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+3.5; leftover $1649.23 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `RBRK` | 16 | $101.99 | $2.04 | — | $5,123.16 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.1; leftover $1649.23 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `S` | 75 | $21.80 | $2.21 | — | $3,485.94 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-8.3; leftover $1649.23 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 3 | $536.07 | $2.00 | — | $1,875.73 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+2.1; leftover $1649.23 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `WDAY` | 8 | $195.40 | $2.01 | — | $310.52 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.7; leftover $1649.23 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $310.52 | ▲ 09:30 equity $13,618.75 vs yday $13,500.97 (+117.78) | 09:30 open · cash $310.52 (unchanged overnight, no fees) · equity $13,618.75 vs prior close $13,500.97 (+117.78) because holdings re-marked: ADSK×6 yday $270.58 → 09:30 $258.50 -72.48; ESTC×19 yday $83.74 → 09:30 $99.99 +308.75; HAFN×208 yday $8.29 → 09:30 $8.43 +29.12; PD×132 yday $12.63 → 09:30 $13.92 +170.28; RBRK×16 yday $107.02 → 09:30 $92.46 -232.96; S×75 yday $22.71 → 09:30 $21.48 -92.25; ULTA×3 yday $540.10 → 09:30 $517.50 -67.80; WDAY×8 yday $193.57 → 09:30 $202.96 +75.12 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $310.52 | ▲ 09:30 equity $13,597.54 vs yday $13,584.32 (+13.22) | 09:30 open · cash $310.52 (unchanged overnight, no fees) · equity $13,597.54 vs prior close $13,584.32 (+13.22) because holdings re-marked: ADSK×6 yday $259.14 → 09:30 $258.17 -5.82; ESTC×19 yday $99.00 → 09:30 $96.54 -46.74; HAFN×208 yday $8.45 → 09:30 $8.43 -4.16; PD×132 yday $13.70 → 09:30 $13.89 +25.08; RBRK×16 yday $92.46 → 09:30 $90.89 -25.12; S×75 yday $21.50 → 09:30 $22.11 +45.75; ULTA×3 yday $517.50 → 09:30 $538.75 +63.75; WDAY×8 yday $203.45 → 09:30 $198.51 -39.52 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $310.52 | ▼ 09:30 equity $13,518.04 vs yday $13,554.19 (-36.15) | 09:30 open · cash $310.52 (unchanged overnight, no fees) · equity $13,518.04 vs prior close $13,554.19 (-36.15) because holdings re-marked: ADSK×6 yday $259.89 → 09:30 $253.48 -38.46; ESTC×19 yday $96.07 → 09:30 $95.76 -5.89; HAFN×208 yday $8.41 → 09:30 $8.56 +31.20; PD×132 yday $13.89 → 09:30 $13.91 +2.64; RBRK×16 yday $91.50 → 09:30 $91.70 +3.20; S×75 yday $21.84 → 09:30 $21.72 -9.00; ULTA×3 yday $537.60 → 09:30 $527.84 -29.28; WDAY×8 yday $195.18 → 09:30 $196.36 +9.44 | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 6 | $253.48 | $2.03 | $-51.98 | $1,829.37 | ▼ -51.98 after sell → book $13,516.01; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 19 | $95.76 | $2.07 | $+245.16 | $3,646.74 | ▲ +245.16 after sell → book $13,513.94; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 208 | $8.56 | $2.73 | $+129.78 | $5,424.49 | ▲ +129.78 after sell → book $13,511.21; vs 09:30 mark -2.73 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `PD` | 132 | $13.91 | $2.42 | $+187.91 | $7,258.18 | ▲ +187.91 after sell → book $13,508.78; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `RBRK` | 16 | $91.70 | $2.06 | $-168.74 | $8,723.32 | ▼ -168.74 after sell → book $13,506.72; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `S` | 75 | $21.72 | $2.24 | $-10.46 | $10,350.08 | ▼ -10.46 after sell → book $13,504.48; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ULTA` | 3 | $527.84 | $2.02 | $-28.71 | $11,931.58 | ▼ -28.71 after sell → book $13,502.46; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `WDAY` | 8 | $196.36 | $2.04 | $+3.63 | $13,500.43 | ▲ +3.63 after sell → book $13,500.43; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,500.43 | ▲ 09:30 equity $13,500.43 vs yday $13,500.43 (-0.00) | 09:30 open · cash $13,500.43 · no holdings · equity $13,500.43 vs prior close $13,500.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 318 | $5.30 | $4.10 | — | $11,810.92 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+1.1; leftover $1687.55 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 6 | $244.98 | $2.01 | — | $10,339.04 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+2.3; leftover $1687.55 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 32 | $51.99 | $2.09 | — | $8,673.27 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-9.0; leftover $1687.55 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MOMO` | 310 | $5.43 | $4.00 | — | $6,985.97 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+0.0; leftover $1687.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NTSK` | 121 | $13.94 | $2.35 | — | $5,296.88 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-8.2; leftover $1687.55 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PHR` | 143 | $11.79 | $2.42 | — | $3,608.49 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-1.3; leftover $1687.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PVH` | 23 | $73.10 | $2.06 | — | $1,925.13 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-4.8; leftover $1687.55 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SNOW` | 5 | $310.54 | $2.00 | — | $370.42 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+1.2; leftover $1687.55 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $370.42 | ▲ 09:30 equity $14,397.61 vs yday $13,389.95 (+1,007.66) | 09:30 open · cash $370.42 (unchanged overnight, no fees) · equity $14,397.61 vs prior close $13,389.95 (+1007.66) because holdings re-marked: CHPT×318 yday $5.19 → 09:30 $6.90 +543.78; FIVE×6 yday $243.08 → 09:30 $256.99 +83.46; HPE×32 yday $51.83 → 09:30 $47.60 -135.36; MOMO×310 yday $5.49 → 09:30 $5.50 +3.10; NTSK×121 yday $13.75 → 09:30 $15.51 +212.96; PHR×143 yday $11.85 → 09:30 $11.02 -118.69; PVH×23 yday $72.29 → 09:30 $74.96 +61.41; SNOW×5 yday $305.84 → 09:30 $377.24 +357.00 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 4 | $10.16 | $0.42 | — | $329.37 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+4.8; leftover $46.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 12 | $3.78 | $0.49 | — | $283.52 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-2.8; leftover $46.30 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 1 | $37.69 | $0.38 | — | $245.45 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.4; leftover $46.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 2 | $15.62 | $0.32 | — | $213.89 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-4.7; leftover $46.30 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `NMAX` | cash | leftover split 0.29 < 1 share @ 9.89 |
| 2026-08-14 | `AIRJ` | cash | leftover split 0.29 < 1 share @ 5.51 |
| 2026-08-14 | `BRUN` | cash | leftover split 0.29 < 1 share @ 26.25 |
| 2026-08-14 | `BZAI` | cash | leftover split 0.29 < 1 share @ 0.77 |
| 2026-08-14 | `DLO` | cash | leftover split 0.29 < 1 share @ 15.28 |
| 2026-08-14 | `ENHA` | cash | leftover split 0.29 < 1 share @ 2.31 |
| 2026-08-14 | `FIRY` | cash | leftover split 0.29 < 1 share @ 9.74 |
| 2026-08-14 | `GEMI` | cash | leftover split 0.29 < 1 share @ 3.90 |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SQM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `YMM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `ATAT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ATHM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BULL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `COTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `FUTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IOND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BJ` | cash | leftover split 68.92 < 1 share @ 93.98 |
| 2026-08-24 | `ATAT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ATHM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BULL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `COTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `FUTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IOND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BKE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `PSEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `BKE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `PSEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `BKE` | no_price | no 09:30 open — carry |
| 2026-08-26 | `PSEC` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BNS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SHMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `TUYA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `VIPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `TIGR` | no_price | no 09:30 open |
| 2026-08-26 | `ANF` | no_price | no 09:30 open |
| 2026-08-26 | `BOX` | no_price | no 09:30 open |
| 2026-08-26 | `HEI` | no_price | no 09:30 open |
| 2026-08-26 | `INTU` | no_price | no 09:30 open |
| 2026-08-26 | `KSS` | no_price | no 09:30 open |
| 2026-08-26 | `NCNO` | no_price | no 09:30 open |
| 2026-08-26 | `QMLS` | no_price | no 09:30 open |
| 2026-08-27 | `BNS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SHMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `TUYA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `VIPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ESTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RBRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `S` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ULTA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `WDAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ESTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `PD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `RBRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `S` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ULTA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `WDAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `CHPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HPE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MOMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NTSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PVH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SNOW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AMBA` | cash | leftover split 46.30 < 1 share @ 66.61 |
| 2026-09-04 | `DOCU` | cash | leftover split 46.30 < 1 share @ 67.06 |
| 2026-09-04 | `GWRE` | cash | leftover split 46.30 < 1 share @ 198.00 |
| 2026-09-04 | `LULU` | cash | leftover split 46.30 < 1 share @ 121.15 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CHPT` | 318 | 2026-09-03 @ $5.30 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+1.1; leftover $1687.55 |
| `FIVE` | 6 | 2026-09-03 @ $244.98 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+2.3; leftover $1687.55 |
| `HPE` | 32 | 2026-09-03 @ $51.99 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-9.0; leftover $1687.55 |
| `MOMO` | 310 | 2026-09-03 @ $5.43 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+0.0; leftover $1687.55 |
| `NTSK` | 121 | 2026-09-03 @ $13.94 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-8.2; leftover $1687.55 |
| `PHR` | 143 | 2026-09-03 @ $11.79 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-1.3; leftover $1687.55 |
| `PVH` | 23 | 2026-09-03 @ $73.10 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-4.8; leftover $1687.55 |
| `SNOW` | 5 | 2026-09-03 @ $310.54 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+1.2; leftover $1687.55 |
| `ASAN` | 4 | 2026-09-04 @ $10.16 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+4.8; leftover $46.30 |
| `DOMO` | 12 | 2026-09-04 @ $3.78 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-2.8; leftover $46.30 |
| `IOT` | 1 | 2026-09-04 @ $37.69 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.4; leftover $46.30 |
| `MAMA` | 2 | 2026-09-04 @ $15.62 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-4.7; leftover $46.30 |
