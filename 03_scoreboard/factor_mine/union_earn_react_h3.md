# Factor mine action — `union_earn_react_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ earn_react, no 🚨

Cash book **+22.58%** ($12,258) · signal-only (no cash/fees) was -26.79%. Starts YES **15/17**. Fills 74 · skips 113 · realized $+1646.20.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `earn_react=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $190.90.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | INO, VOR | — | $21.06 | $10,769.53 | INO×6172, VOR×223 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $21.06 | INO×6172, VOR×223 | $10,963.61 | +194.08 | BZAI, DEFT | — | $16.35 | $11,883.74 | INO×6172, VOR×223, BZAI×3, DEFT×5 | 09:30 open · cash $21.06 (unchanged overnight, no fees) · equity $10,963.61 vs prior close $10,769.53 (+194.08) because holdings re-marked: INO×6172 yday $0.90 → 09:30 $0.93 +185.16; VOR×223 yday $23.29 → 09:30 $23.33 +8.92 |
| 2026-08-17 | +2.25 | $16.35 | INO×6172, VOR×223, BZAI×3, DEFT×5 | $11,733.35 | -150.39 | — | — | $16.35 | $12,249.26 | INO×6172, VOR×223, BZAI×3, DEFT×5 | 09:30 open · cash $16.35 (unchanged overnight, no fees) · equity $11,733.35 vs prior close $11,883.74 (-150.39) because holdings re-marked: INO×6172 yday $1.09 → 09:30 $1.07 -123.44; VOR×223 yday $23.03 → 09:30 $22.91 -26.76; BZAI×3 yday $0.59 → 09:30 $0.55 -0.12; DEFT×5 yday $0.49 → 09:30 $0.47 -0.07 |
| 2026-08-18 | -6.20 | $16.35 | INO×6172, VOR×223, BZAI×3, DEFT×5 | $12,145.01 | -104.25 | — | INO, VOR | $12,057.63 | $12,061.49 | BZAI×3, DEFT×5 | 09:30 open · cash $16.35 (unchanged overnight, no fees) · equity $12,145.01 vs prior close $12,249.26 (-104.25) because holdings re-marked: INO×6172 yday $1.15 → 09:30 $1.14 -61.72; VOR×223 yday $23.01 → 09:30 $22.82 -42.37; BZAI×3 yday $0.52 → 09:30 $0.49 -0.09; DEFT×5 yday $0.47 → 09:30 $0.45 -0.08 |
| 2026-08-19 | -7.20 | $12,057.63 | BZAI×3, DEFT×5 | $12,061.52 | +0.03 | — | BZAI, DEFT | $12,061.41 | $12,061.41 | — | 09:30 open · cash $12,057.63 (unchanged overnight, no fees) · equity $12,061.52 vs prior close $12,061.49 (+0.03) because holdings re-marked: BZAI×3 yday $0.56 → 09:30 $0.57 +0.04; DEFT×5 yday $0.44 → 09:30 $0.43 -0.01 |
| 2026-08-20 | +1.12 | $12,061.41 | — | $12,061.41 | +0.00 | AAP, AEG, ALVO, ATAT, ATHM, BABA, BILL, BULL | — | $77.42 | $11,904.81 | AAP×32, AEG×167, ALVO×387, ATAT×44, ATHM×67, BABA×12, BILL×30, BULL×151 | 09:30 open · cash $12,061.41 · no holdings · equity $12,061.41 vs prior close $12,061.41 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $77.42 | AAP×32, AEG×167, ALVO×387, ATAT×44, ATHM×67, BABA×12, BILL×30, BULL×151 | $11,899.79 | -5.02 | PSEC | — | $63.47 | $11,865.95 | AAP×32, AEG×167, ALVO×387, ATAT×44, ATHM×67, BABA×12, BILL×30, BULL×151, PSEC×6 | 09:30 open · cash $77.42 (unchanged overnight, no fees) · equity $11,899.79 vs prior close $11,904.81 (-5.02) because holdings re-marked: AAP×32 yday $42.39 → 09:30 $42.41 +0.64; AEG×167 yday $9.01 → 09:30 $9.04 +5.01; ALVO×387 yday $4.27 → 09:30 $4.32 +19.35; ATAT×44 yday $34.25 → 09:30 $34.31 +2.64; ATHM×67 yday $22.12 → 09:30 $22.20 +5.36; BABA×12 yday $130.53 → 09:30 $125.35 -62.16; BILL×30 yday $47.40 → 09:30 $47.50 +3.00; BULL×151 yday $8.85 → 09:30 $8.99 +21.14 |
| 2026-08-24 | -5.17 | $63.47 | AAP×32, AEG×167, ALVO×387, ATAT×44, ATHM×67, BABA×12, BILL×30, BULL×151, PSEC×6 | $11,952.56 | +86.61 | — | — | $63.47 | $12,202.79 | AAP×32, AEG×167, ALVO×387, ATAT×44, ATHM×67, BABA×12, BILL×30, BULL×151, PSEC×6 | 09:30 open · cash $63.47 (unchanged overnight, no fees) · equity $11,952.56 vs prior close $11,865.95 (+86.61) because holdings re-marked: AAP×32 yday $42.58 → 09:30 $43.10 +16.64; AEG×167 yday $8.99 → 09:30 $9.16 +28.39; ALVO×387 yday $4.43 → 09:30 $4.79 +139.32; ATAT×44 yday $34.75 → 09:30 $34.70 -2.20; ATHM×67 yday $22.22 → 09:30 $21.78 -29.48; BABA×12 yday $119.34 → 09:30 $116.80 -30.48; BILL×30 yday $47.82 → 09:30 $47.84 +0.60; BULL×151 yday $8.78 → 09:30 $8.54 -36.24; PSEC×6 yday $2.33 → 09:30 $2.34 +0.06 |
| 2026-08-25 | +1.80 | $63.47 | AAP×32, AEG×167, ALVO×387, ATAT×44, ATHM×67, BABA×12, BILL×30, BULL×151, PSEC×6 | $12,168.09 | -34.70 | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | AAP, AEG, ALVO, ATAT, ATHM, BABA, BILL, BULL | $297.98 | $12,053.42 | PSEC×6, BMO×8, BNS×17, BZ×98, DKS×8, EH×272, GFI×31, GRRR×106, SHMD×322 | 09:30 open · cash $63.47 (unchanged overnight, no fees) · equity $12,168.09 vs prior close $12,202.79 (-34.70) because holdings re-marked: AAP×32 yday $43.83 → 09:30 $43.61 -7.04; AEG×167 yday $9.19 → 09:30 $9.29 +16.70; ALVO×387 yday $5.15 → 09:30 $5.22 +27.09; ATAT×44 yday $34.83 → 09:30 $34.75 -3.52; ATHM×67 yday $21.85 → 09:30 $21.85 +0.00; BABA×12 yday $119.46 → 09:30 $116.36 -37.20; BILL×30 yday $48.23 → 09:30 $48.16 -2.10; BULL×151 yday $8.73 → 09:30 $8.54 -28.69; PSEC×6 yday $2.31 → 09:30 $2.32 +0.06 |
| 2026-08-26 | +2.02 | $297.98 | PSEC×6, BMO×8, BNS×17, BZ×98, DKS×8, EH×272, GFI×31, GRRR×106, SHMD×322 | $12,053.42 | +0.00 | — | — | $297.98 | $12,126.98 | PSEC×6, BMO×8, BNS×17, BZ×98, DKS×8, EH×272, GFI×31, GRRR×106, SHMD×322 | 09:30 open · cash $297.98 (unchanged overnight, no fees) · equity $12,053.42 vs prior close $12,053.42 (+0.00) because holdings re-marked: PSEC×6 yday $2.33 → 09:30 $2.33 +0.00; BMO×8 yday $175.00 → 09:30 $175.00 +0.00; BNS×17 yday $90.08 → 09:30 $90.08 +0.00; BZ×98 yday $16.32 → 09:30 $16.32 +0.00; DKS×8 yday $156.70 → 09:30 $156.70 +0.00; EH×272 yday $5.28 → 09:30 $5.28 +0.00; GFI×31 yday $48.36 → 09:30 $48.36 +0.00; GRRR×106 yday $14.20 → 09:30 $14.20 +0.00; SHMD×322 yday $4.71 → 09:30 $4.71 +0.00 |
| 2026-08-27 | — | $297.98 | PSEC×6, BMO×8, BNS×17, BZ×98, DKS×8, EH×272, GFI×31, GRRR×106, SHMD×322 | $11,259.56 | -867.42 | NVDA | PSEC | $97.27 | $11,627.54 | BMO×8, BNS×17, BZ×98, DKS×8, EH×272, GFI×31, GRRR×106, SHMD×322, NVDA×1 | 09:30 open · cash $297.98 (unchanged overnight, no fees) · equity $11,259.56 vs prior close $12,126.98 (-867.42) because holdings re-marked: PSEC×6 yday $2.33 → 09:30 $2.35 +0.12; BMO×8 yday $175.00 → 09:30 $173.22 -14.24; BNS×17 yday $90.08 → 09:30 $92.64 +43.52; BZ×98 yday $16.32 → 09:30 $16.77 +44.10; DKS×8 yday $156.70 → 09:30 $121.87 -278.64; EH×272 yday $5.28 → 09:30 $4.77 -138.72; GFI×31 yday $48.36 → 09:30 $48.24 -3.72; GRRR×106 yday $14.20 → 09:30 $14.03 -18.02; SHMD×322 yday $4.71 → 09:30 $3.38 -428.26 |
| 2026-08-28 | +0.75 | $97.27 | BMO×8, BNS×17, BZ×98, DKS×8, EH×272, GFI×31, GRRR×106, SHMD×322, NVDA×1 | $11,661.40 | +33.86 | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | $177.41 | $11,683.79 | NVDA×1, ADSK×5, BBAR×95, ESTC×17, FINV×335, FRO×33, GAP×68, HAFN×180, IREN×35 | 09:30 open · cash $97.27 (unchanged overnight, no fees) · equity $11,661.40 vs prior close $11,627.54 (+33.86) because holdings re-marked: BMO×8 yday $172.90 → 09:30 $172.85 -0.40; BNS×17 yday $93.59 → 09:30 $93.52 -1.19; BZ×98 yday $18.84 → 09:30 $18.50 -33.32; DKS×8 yday $129.66 → 09:30 $128.73 -7.44; EH×272 yday $4.86 → 09:30 $4.90 +10.88; GFI×31 yday $47.82 → 09:30 $47.93 +3.41; GRRR×106 yday $15.45 → 09:30 $15.94 +51.94; SHMD×322 yday $3.17 → 09:30 $3.16 -3.22; NVDA×1 yday $209.66 → 09:30 $222.86 +13.20 |
| 2026-08-31 | -5.85 | $177.41 | NVDA×1, ADSK×5, BBAR×95, ESTC×17, FINV×335, FRO×33, GAP×68, HAFN×180, IREN×35 | $11,685.93 | +2.14 | — | — | $177.41 | $11,723.26 | NVDA×1, ADSK×5, BBAR×95, ESTC×17, FINV×335, FRO×33, GAP×68, HAFN×180, IREN×35 | 09:30 open · cash $177.41 (unchanged overnight, no fees) · equity $11,685.93 vs prior close $11,683.79 (+2.14) because holdings re-marked: NVDA×1 yday $227.98 → 09:30 $219.00 -8.98; ADSK×5 yday $270.58 → 09:30 $258.50 -60.40; BBAR×95 yday $14.60 → 09:30 $14.50 -9.50; ESTC×17 yday $83.74 → 09:30 $99.99 +276.25; FINV×335 yday $4.02 → 09:30 $3.46 -187.60; FRO×33 yday $43.75 → 09:30 $43.54 -6.93; GAP×68 yday $20.79 → 09:30 $22.89 +142.80; HAFN×180 yday $8.29 → 09:30 $8.43 +25.20; IREN×35 yday $40.53 → 09:30 $35.71 -168.70 |
| 2026-09-01 | -6.30 | $177.41 | NVDA×1, ADSK×5, BBAR×95, ESTC×17, FINV×335, FRO×33, GAP×68, HAFN×180, IREN×35 | $11,760.14 | +36.88 | — | NVDA | $395.60 | $11,729.27 | ADSK×5, BBAR×95, ESTC×17, FINV×335, FRO×33, GAP×68, HAFN×180, IREN×35 | 09:30 open · cash $177.41 (unchanged overnight, no fees) · equity $11,760.14 vs prior close $11,723.26 (+36.88) because holdings re-marked: NVDA×1 yday $218.93 → 09:30 $220.21 +1.28; ADSK×5 yday $259.14 → 09:30 $258.17 -4.85; BBAR×95 yday $14.50 → 09:30 $15.14 +60.80; ESTC×17 yday $99.00 → 09:30 $96.54 -41.82; FINV×335 yday $3.46 → 09:30 $3.67 +70.35; FRO×33 yday $44.09 → 09:30 $43.60 -16.17; GAP×68 yday $23.30 → 09:30 $22.28 -69.36; HAFN×180 yday $8.45 → 09:30 $8.43 -3.60; IREN×35 yday $35.75 → 09:30 $36.90 +40.25 |
| 2026-09-02 | -3.83 | $395.60 | ADSK×5, BBAR×95, ESTC×17, FINV×335, FRO×33, GAP×68, HAFN×180, IREN×35 | $11,665.99 | -63.28 | — | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | $11,646.20 | $11,646.20 | — | 09:30 open · cash $395.60 (unchanged overnight, no fees) · equity $11,665.99 vs prior close $11,729.27 (-63.28) because holdings re-marked: ADSK×5 yday $259.89 → 09:30 $253.48 -32.05; BBAR×95 yday $15.14 → 09:30 $14.82 -30.40; ESTC×17 yday $96.07 → 09:30 $95.76 -5.27; FINV×335 yday $3.67 → 09:30 $3.58 -30.15; FRO×33 yday $43.66 → 09:30 $44.39 +24.09; GAP×68 yday $22.20 → 09:30 $22.05 -10.20; HAFN×180 yday $8.41 → 09:30 $8.56 +27.00; IREN×35 yday $36.26 → 09:30 $36.08 -6.30 |
| 2026-09-03 | -0.90 | $11,646.20 | — | $11,646.20 | +0.00 | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | — | $613.43 | $11,584.29 | AI×141, AVGO×3, CHPT×274, CIEN×4, CPB×61, FIVE×5, HPE×28, MEI×79 | 09:30 open · cash $11,646.20 · no holdings · equity $11,646.20 vs prior close $11,646.20 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $613.43 | AI×141, AVGO×3, CHPT×274, CIEN×4, CPB×61, FIVE×5, HPE×28, MEI×79 | $11,662.93 | +78.64 | AMBA, ASAN, DOCU, DOMO, IOT, MAMA | — | $190.90 | $12,257.88 | AI×141, AVGO×3, CHPT×274, CIEN×4, CPB×61, FIVE×5, HPE×28, MEI×79, AMBA×1, ASAN×7, DOCU×1, DOMO×20, IOT×2, MAMA×4 | 09:30 open · cash $613.43 (unchanged overnight, no fees) · equity $11,662.93 vs prior close $11,584.29 (+78.64) because holdings re-marked: AI×141 yday $10.52 → 09:30 $10.74 +31.02; AVGO×3 yday $367.24 → 09:30 $351.74 -46.50; CHPT×274 yday $5.19 → 09:30 $6.90 +468.54; CIEN×4 yday $354.16 → 09:30 $354.49 +1.32; CPB×61 yday $23.78 → 09:30 $22.32 -89.06; FIVE×5 yday $243.08 → 09:30 $256.99 +69.55; HPE×28 yday $51.83 → 09:30 $47.60 -118.44; MEI×79 yday $18.10 → 09:30 $15.09 -237.79 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 6172 | $0.81 | $68.51 | — | $4,932.17 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten; ⚪; ret5=+13.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 223 | $22.01 | $2.88 | — | $21.06 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.06 | ▲ 09:30 equity $10,963.61 vs yday $10,769.53 (+194.08) | 09:30 open · cash $21.06 (unchanged overnight, no fees) · equity $10,963.61 vs prior close $10,769.53 (+194.08) because holdings re-marked: INO×6172 yday $0.90 → 09:30 $0.93 +185.16; VOR×223 yday $23.29 → 09:30 $23.33 +8.92 | — |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 3 | $0.77 | $0.03 | — | $18.73 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $2.63 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DEFT` | 5 | $0.47 | $0.04 | — | $16.35 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+11.1; leftover $2.63 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.35 | ▼ 09:30 equity $11,733.35 vs yday $11,883.74 (-150.39) | 09:30 open · cash $16.35 (unchanged overnight, no fees) · equity $11,733.35 vs prior close $11,883.74 (-150.39) because holdings re-marked: INO×6172 yday $1.09 → 09:30 $1.07 -123.44; VOR×223 yday $23.03 → 09:30 $22.91 -26.76; BZAI×3 yday $0.59 → 09:30 $0.55 -0.12; DEFT×5 yday $0.49 → 09:30 $0.47 -0.07 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.35 | ▼ 09:30 equity $12,145.01 vs yday $12,249.26 (-104.25) | 09:30 open · cash $16.35 (unchanged overnight, no fees) · equity $12,145.01 vs prior close $12,249.26 (-104.25) because holdings re-marked: INO×6172 yday $1.15 → 09:30 $1.14 -61.72; VOR×223 yday $23.01 → 09:30 $22.82 -42.37; BZAI×3 yday $0.52 → 09:30 $0.49 -0.09; DEFT×5 yday $0.47 → 09:30 $0.45 -0.08 | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 6172 | $1.14 | $80.70 | $+1887.55 | $6,971.73 | ▲ +1,887.55 after sell → book $12,064.31; vs 09:30 mark -80.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 223 | $22.82 | $2.95 | $+174.80 | $12,057.63 | ▲ +174.80 after sell → book $12,061.35; vs 09:30 mark -2.96 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,057.63 | ▲ 09:30 equity $12,061.52 vs yday $12,061.49 (+0.03) | 09:30 open · cash $12,057.63 (unchanged overnight, no fees) · equity $12,061.52 vs prior close $12,061.49 (+0.03) because holdings re-marked: BZAI×3 yday $0.56 → 09:30 $0.57 +0.04; DEFT×5 yday $0.44 → 09:30 $0.43 -0.01 | — |
| 2026-08-19 09:30 ET | **SELL** | `BZAI` | 3 | $0.57 | $0.05 | $-0.67 | $12,059.30 | ▼ -0.67 after sell → book $12,061.47; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DEFT` | 5 | $0.43 | $0.06 | $-0.27 | $12,061.41 | ▼ -0.27 after sell → book $12,061.41; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,061.41 | ▲ 09:30 equity $12,061.41 vs yday $12,061.41 (+0.00) | 09:30 open · cash $12,061.41 · no holdings · equity $12,061.41 vs prior close $12,061.41 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 32 | $46.85 | $2.09 | — | $10,560.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+5.0; leftover $1507.68 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 167 | $9.01 | $2.49 | — | $9,052.97 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $1507.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 387 | $3.89 | $4.99 | — | $7,542.54 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.5; leftover $1507.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 44 | $34.05 | $2.12 | — | $6,042.22 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+9.3; leftover $1507.68 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 67 | $22.44 | $2.19 | — | $4,536.55 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.1; leftover $1507.68 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 12 | $123.47 | $2.03 | — | $3,052.89 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+2.9; leftover $1507.68 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BILL` | 30 | $49.00 | $2.08 | — | $1,580.81 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.0; leftover $1507.68 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 151 | $9.94 | $2.44 | — | $77.42 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+12.6; leftover $1507.68 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $77.42 | ▼ 09:30 equity $11,899.79 vs yday $11,904.81 (-5.02) | 09:30 open · cash $77.42 (unchanged overnight, no fees) · equity $11,899.79 vs prior close $11,904.81 (-5.02) because holdings re-marked: AAP×32 yday $42.39 → 09:30 $42.41 +0.64; AEG×167 yday $9.01 → 09:30 $9.04 +5.01; ALVO×387 yday $4.27 → 09:30 $4.32 +19.35; ATAT×44 yday $34.25 → 09:30 $34.31 +2.64; ATHM×67 yday $22.12 → 09:30 $22.20 +5.36; BABA×12 yday $130.53 → 09:30 $125.35 -62.16; BILL×30 yday $47.40 → 09:30 $47.50 +3.00; BULL×151 yday $8.85 → 09:30 $8.99 +21.14 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 6 | $2.30 | $0.16 | — | $63.47 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.0; leftover $15.48 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.47 | ▲ 09:30 equity $11,952.56 vs yday $11,865.95 (+86.61) | 09:30 open · cash $63.47 (unchanged overnight, no fees) · equity $11,952.56 vs prior close $11,865.95 (+86.61) because holdings re-marked: AAP×32 yday $42.58 → 09:30 $43.10 +16.64; AEG×167 yday $8.99 → 09:30 $9.16 +28.39; ALVO×387 yday $4.43 → 09:30 $4.79 +139.32; ATAT×44 yday $34.75 → 09:30 $34.70 -2.20; ATHM×67 yday $22.22 → 09:30 $21.78 -29.48; BABA×12 yday $119.34 → 09:30 $116.80 -30.48; BILL×30 yday $47.82 → 09:30 $47.84 +0.60; BULL×151 yday $8.78 → 09:30 $8.54 -36.24; PSEC×6 yday $2.33 → 09:30 $2.34 +0.06 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.47 | ▼ 09:30 equity $12,168.09 vs yday $12,202.79 (-34.70) | 09:30 open · cash $63.47 (unchanged overnight, no fees) · equity $12,168.09 vs prior close $12,202.79 (-34.70) because holdings re-marked: AAP×32 yday $43.83 → 09:30 $43.61 -7.04; AEG×167 yday $9.19 → 09:30 $9.29 +16.70; ALVO×387 yday $5.15 → 09:30 $5.22 +27.09; ATAT×44 yday $34.83 → 09:30 $34.75 -3.52; ATHM×67 yday $21.85 → 09:30 $21.85 +0.00; BABA×12 yday $119.46 → 09:30 $116.36 -37.20; BILL×30 yday $48.23 → 09:30 $48.16 -2.10; BULL×151 yday $8.73 → 09:30 $8.54 -28.69; PSEC×6 yday $2.31 → 09:30 $2.32 +0.06 | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 32 | $43.61 | $2.11 | $-107.87 | $1,456.88 | ▼ -107.87 after sell → book $12,165.98; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 167 | $9.29 | $2.53 | $+41.74 | $3,005.78 | ▲ +41.74 after sell → book $12,163.45; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 387 | $5.22 | $5.07 | $+504.64 | $5,020.85 | ▲ +504.64 after sell → book $12,158.38; vs 09:30 mark -5.07 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 44 | $34.75 | $2.14 | $+26.53 | $6,547.70 | ▲ +26.53 after sell → book $12,156.23; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 67 | $21.85 | $2.21 | $-43.93 | $8,009.44 | ▼ -43.93 after sell → book $12,154.02; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BABA` | 12 | $116.36 | $2.05 | $-89.39 | $9,403.71 | ▼ -89.39 after sell → book $12,151.97; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BILL` | 30 | $48.16 | $2.10 | $-29.38 | $10,846.41 | ▼ -29.38 after sell → book $12,149.87; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BULL` | 151 | $8.54 | $2.48 | $-216.32 | $12,133.47 | ▼ -216.32 after sell → book $12,147.39; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 8 | $172.40 | $2.01 | — | $10,752.26 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.1; leftover $1516.68 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 17 | $86.86 | $2.04 | — | $9,273.60 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-4.3; leftover $1516.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 98 | $15.34 | $2.28 | — | $7,767.99 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+2.8; leftover $1516.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 8 | $179.33 | $2.01 | — | $6,331.34 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-9.3; leftover $1516.68 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 272 | $5.57 | $3.51 | — | $4,812.79 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-7.1; leftover $1516.68 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 31 | $47.68 | $2.08 | — | $3,332.63 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ⚪; ret5=+18.8; leftover $1516.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 106 | $14.26 | $2.31 | — | $1,818.76 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-1.9; leftover $1516.68 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 322 | $4.71 | $4.15 | — | $297.98 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-9.9; leftover $1516.68 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $297.98 | ▲ 09:30 equity $12,053.42 vs yday $12,053.42 (+0.00) | 09:30 open · cash $297.98 (unchanged overnight, no fees) · equity $12,053.42 vs prior close $12,053.42 (+0.00) because holdings re-marked: PSEC×6 yday $2.33 → 09:30 $2.33 +0.00; BMO×8 yday $175.00 → 09:30 $175.00 +0.00; BNS×17 yday $90.08 → 09:30 $90.08 +0.00; BZ×98 yday $16.32 → 09:30 $16.32 +0.00; DKS×8 yday $156.70 → 09:30 $156.70 +0.00; EH×272 yday $5.28 → 09:30 $5.28 +0.00; GFI×31 yday $48.36 → 09:30 $48.36 +0.00; GRRR×106 yday $14.20 → 09:30 $14.20 +0.00; SHMD×322 yday $4.71 → 09:30 $4.71 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $297.98 | ▼ 09:30 equity $11,259.56 vs yday $12,126.98 (-867.42) | 09:30 open · cash $297.98 (unchanged overnight, no fees) · equity $11,259.56 vs prior close $12,126.98 (-867.42) because holdings re-marked: PSEC×6 yday $2.33 → 09:30 $2.35 +0.12; BMO×8 yday $175.00 → 09:30 $173.22 -14.24; BNS×17 yday $90.08 → 09:30 $92.64 +43.52; BZ×98 yday $16.32 → 09:30 $16.77 +44.10; DKS×8 yday $156.70 → 09:30 $121.87 -278.64; EH×272 yday $5.28 → 09:30 $4.77 -138.72; GFI×31 yday $48.36 → 09:30 $48.24 -3.72; GRRR×106 yday $14.20 → 09:30 $14.03 -18.02; SHMD×322 yday $4.71 → 09:30 $3.38 -428.26 | — |
| 2026-08-27 09:30 ET | **SELL** | `PSEC` | 6 | $2.35 | $0.18 | $-0.03 | $311.90 | ▼ -0.03 after sell → book $11,259.38; vs 09:30 mark -0.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 1 | $212.64 | $1.99 | — | $97.27 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list mover_buy; 🔵; ret5=-4.6; leftover $311.90 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.27 | ▲ 09:30 equity $11,661.40 vs yday $11,627.54 (+33.86) | 09:30 open · cash $97.27 (unchanged overnight, no fees) · equity $11,661.40 vs prior close $11,627.54 (+33.86) because holdings re-marked: BMO×8 yday $172.90 → 09:30 $172.85 -0.40; BNS×17 yday $93.59 → 09:30 $93.52 -1.19; BZ×98 yday $18.84 → 09:30 $18.50 -33.32; DKS×8 yday $129.66 → 09:30 $128.73 -7.44; EH×272 yday $4.86 → 09:30 $4.90 +10.88; GFI×31 yday $47.82 → 09:30 $47.93 +3.41; GRRR×106 yday $15.45 → 09:30 $15.94 +51.94; SHMD×322 yday $3.17 → 09:30 $3.16 -3.22; NVDA×1 yday $209.66 → 09:30 $222.86 +13.20 | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 8 | $172.85 | $2.04 | $-0.45 | $1,478.04 | ▼ -0.45 after sell → book $11,659.37; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 17 | $93.52 | $2.06 | $+109.12 | $3,065.81 | ▲ +109.12 after sell → book $11,657.30; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 98 | $18.50 | $2.31 | $+305.08 | $4,876.50 | ▲ +305.08 after sell → book $11,654.99; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 8 | $128.73 | $2.03 | $-408.85 | $5,904.30 | ▼ -408.85 after sell → book $11,652.95; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 272 | $4.90 | $3.56 | $-189.31 | $7,233.54 | ▼ -189.31 after sell → book $11,649.39; vs 09:30 mark -3.56 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 31 | $47.93 | $2.10 | $+3.56 | $8,717.26 | ▲ +3.56 after sell → book $11,647.28; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 106 | $15.94 | $2.34 | $+173.43 | $10,404.57 | ▲ +173.43 after sell → book $11,644.95; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 322 | $3.16 | $4.22 | $-507.47 | $11,417.87 | ▼ -507.47 after sell → book $11,640.73; vs 09:30 mark -4.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.47 | $2.00 | — | $10,108.51 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.9; leftover $1427.23 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 95 | $14.96 | $2.27 | — | $8,685.04 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-8.4; leftover $1427.23 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 17 | $82.64 | $2.04 | — | $7,278.12 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-0.9; leftover $1427.23 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 335 | $4.26 | $4.32 | — | $5,846.70 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-0.7; leftover $1427.23 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 33 | $42.51 | $2.09 | — | $4,441.78 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+6.0; leftover $1427.23 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 68 | $20.75 | $2.19 | — | $3,028.58 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.9; leftover $1427.23 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 180 | $7.91 | $2.53 | — | $1,602.25 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.4; leftover $1427.23 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 35 | $40.65 | $2.10 | — | $177.41 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.9; leftover $1427.23 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $177.41 | ▲ 09:30 equity $11,685.93 vs yday $11,683.79 (+2.14) | 09:30 open · cash $177.41 (unchanged overnight, no fees) · equity $11,685.93 vs prior close $11,683.79 (+2.14) because holdings re-marked: NVDA×1 yday $227.98 → 09:30 $219.00 -8.98; ADSK×5 yday $270.58 → 09:30 $258.50 -60.40; BBAR×95 yday $14.60 → 09:30 $14.50 -9.50; ESTC×17 yday $83.74 → 09:30 $99.99 +276.25; FINV×335 yday $4.02 → 09:30 $3.46 -187.60; FRO×33 yday $43.75 → 09:30 $43.54 -6.93; GAP×68 yday $20.79 → 09:30 $22.89 +142.80; HAFN×180 yday $8.29 → 09:30 $8.43 +25.20; IREN×35 yday $40.53 → 09:30 $35.71 -168.70 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $177.41 | ▲ 09:30 equity $11,760.14 vs yday $11,723.26 (+36.88) | 09:30 open · cash $177.41 (unchanged overnight, no fees) · equity $11,760.14 vs prior close $11,723.26 (+36.88) because holdings re-marked: NVDA×1 yday $218.93 → 09:30 $220.21 +1.28; ADSK×5 yday $259.14 → 09:30 $258.17 -4.85; BBAR×95 yday $14.50 → 09:30 $15.14 +60.80; ESTC×17 yday $99.00 → 09:30 $96.54 -41.82; FINV×335 yday $3.46 → 09:30 $3.67 +70.35; FRO×33 yday $44.09 → 09:30 $43.60 -16.17; GAP×68 yday $23.30 → 09:30 $22.28 -69.36; HAFN×180 yday $8.45 → 09:30 $8.43 -3.60; IREN×35 yday $35.75 → 09:30 $36.90 +40.25 | — |
| 2026-09-01 09:30 ET | **SELL** | `NVDA` | 1 | $220.21 | $2.01 | $+3.56 | $395.60 | ▲ +3.56 after sell → book $11,758.12; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $395.60 | ▼ 09:30 equity $11,665.99 vs yday $11,729.27 (-63.28) | 09:30 open · cash $395.60 (unchanged overnight, no fees) · equity $11,665.99 vs prior close $11,729.27 (-63.28) because holdings re-marked: ADSK×5 yday $259.89 → 09:30 $253.48 -32.05; BBAR×95 yday $15.14 → 09:30 $14.82 -30.40; ESTC×17 yday $96.07 → 09:30 $95.76 -5.27; FINV×335 yday $3.67 → 09:30 $3.58 -30.15; FRO×33 yday $43.66 → 09:30 $44.39 +24.09; GAP×68 yday $22.20 → 09:30 $22.05 -10.20; HAFN×180 yday $8.41 → 09:30 $8.56 +27.00; IREN×35 yday $36.26 → 09:30 $36.08 -6.30 | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 5 | $253.48 | $2.03 | $-43.98 | $1,660.98 | ▼ -43.98 after sell → book $11,663.97; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 95 | $14.82 | $2.30 | $-17.88 | $3,066.58 | ▼ -17.88 after sell → book $11,661.67; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 17 | $95.76 | $2.06 | $+218.93 | $4,692.43 | ▲ +218.93 after sell → book $11,659.60; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 335 | $3.58 | $4.39 | $-236.51 | $5,887.35 | ▼ -236.51 after sell → book $11,655.22; vs 09:30 mark -4.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 33 | $44.39 | $2.11 | $+57.84 | $7,350.11 | ▲ +57.84 after sell → book $11,653.11; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 68 | $22.05 | $2.22 | $+83.99 | $8,847.29 | ▲ +83.99 after sell → book $11,650.89; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 180 | $8.56 | $2.57 | $+111.90 | $10,385.52 | ▲ +111.90 after sell → book $11,648.32; vs 09:30 mark -2.57 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 35 | $36.08 | $2.12 | $-164.16 | $11,646.20 | ▼ -164.16 after sell → book $11,646.20; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,646.20 | ▲ 09:30 equity $11,646.20 vs yday $11,646.20 (+0.00) | 09:30 open · cash $11,646.20 · no holdings · equity $11,646.20 vs prior close $11,646.20 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 141 | $10.30 | $2.41 | — | $10,191.49 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+3.7; leftover $1455.78 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $369.68 | $2.00 | — | $9,080.45 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-6.2; leftover $1455.78 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 274 | $5.30 | $3.53 | — | $7,624.71 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+1.1; leftover $1455.78 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 4 | $357.25 | $2.00 | — | $6,193.71 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-7.7; leftover $1455.78 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 61 | $23.80 | $2.17 | — | $4,739.74 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.5; leftover $1455.78 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 5 | $244.98 | $2.00 | — | $3,512.83 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+2.3; leftover $1455.78 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 28 | $51.99 | $2.07 | — | $2,055.04 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-9.0; leftover $1455.78 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 79 | $18.22 | $2.23 | — | $613.43 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-16.7; leftover $1455.78 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $613.43 | ▲ 09:30 equity $11,662.93 vs yday $11,584.29 (+78.64) | 09:30 open · cash $613.43 (unchanged overnight, no fees) · equity $11,662.93 vs prior close $11,584.29 (+78.64) because holdings re-marked: AI×141 yday $10.52 → 09:30 $10.74 +31.02; AVGO×3 yday $367.24 → 09:30 $351.74 -46.50; CHPT×274 yday $5.19 → 09:30 $6.90 +468.54; CIEN×4 yday $354.16 → 09:30 $354.49 +1.32; CPB×61 yday $23.78 → 09:30 $22.32 -89.06; FIVE×5 yday $243.08 → 09:30 $256.99 +69.55; HPE×28 yday $51.83 → 09:30 $47.60 -118.44; MEI×79 yday $18.10 → 09:30 $15.09 -237.79 | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 1 | $66.61 | $0.67 | — | $546.15 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-10.1; leftover $76.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 7 | $10.16 | $0.73 | — | $474.30 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+4.8; leftover $76.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 1 | $67.06 | $0.67 | — | $406.57 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.1; leftover $76.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 20 | $3.78 | $0.82 | — | $330.15 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.8; leftover $76.68 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 2 | $37.69 | $0.76 | — | $254.01 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.4; leftover $76.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 4 | $15.62 | $0.64 | — | $190.90 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.7; leftover $76.68 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `NMAX` | cash | leftover split 2.63 < 1 share @ 9.89 |
| 2026-08-14 | `AIRJ` | cash | leftover split 2.63 < 1 share @ 5.51 |
| 2026-08-14 | `AMAT` | cash | leftover split 2.63 < 1 share @ 499.40 |
| 2026-08-14 | `AMPG` | cash | leftover split 2.63 < 1 share @ 4.37 |
| 2026-08-14 | `BRUN` | cash | leftover split 2.63 < 1 share @ 26.25 |
| 2026-08-14 | `DGXX` | cash | leftover split 2.63 < 1 share @ 3.92 |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BZAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BZAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `LOW` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `LZB` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MRCY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AEG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ATAT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ATHM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BILL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BULL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BEKE` | cash | leftover split 15.48 < 1 share @ 17.93 |
| 2026-08-21 | `BJ` | cash | leftover split 15.48 < 1 share @ 93.98 |
| 2026-08-21 | `BKE` | cash | leftover split 15.48 < 1 share @ 43.08 |
| 2026-08-21 | `ROST` | cash | leftover split 15.48 < 1 share @ 243.85 |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AEG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ATAT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ATHM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BILL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BULL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `PSEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `PSEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `PSEC` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BNS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SHMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `TIGR` | no_price | no 09:30 open |
| 2026-08-26 | `ANF` | no_price | no 09:30 open |
| 2026-08-26 | `BBWI` | no_price | no 09:30 open |
| 2026-08-26 | `BOX` | no_price | no 09:30 open |
| 2026-08-26 | `DY` | no_price | no 09:30 open |
| 2026-08-26 | `FSCO` | no_price | no 09:30 open |
| 2026-08-26 | `HEI` | no_price | no 09:30 open |
| 2026-08-26 | `INTU` | no_price | no 09:30 open |
| 2026-08-27 | `BMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BNS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SHMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `NVDA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NVDA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBAR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ESTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FINV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBAR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ESTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FINV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CHPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CPB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HPE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MEI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `GWRE` | cash | leftover split 76.68 < 1 share @ 198.00 |
| 2026-09-04 | `LULU` | cash | leftover split 76.68 < 1 share @ 121.15 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `AI` | 141 | 2026-09-03 @ $10.30 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+3.7; leftover $1455.78 |
| `AVGO` | 3 | 2026-09-03 @ $369.68 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-6.2; leftover $1455.78 |
| `CHPT` | 274 | 2026-09-03 @ $5.30 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+1.1; leftover $1455.78 |
| `CIEN` | 4 | 2026-09-03 @ $357.25 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-7.7; leftover $1455.78 |
| `CPB` | 61 | 2026-09-03 @ $23.80 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.5; leftover $1455.78 |
| `FIVE` | 5 | 2026-09-03 @ $244.98 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+2.3; leftover $1455.78 |
| `HPE` | 28 | 2026-09-03 @ $51.99 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-9.0; leftover $1455.78 |
| `MEI` | 79 | 2026-09-03 @ $18.22 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-16.7; leftover $1455.78 |
| `AMBA` | 1 | 2026-09-04 @ $66.61 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-10.1; leftover $76.68 |
| `ASAN` | 7 | 2026-09-04 @ $10.16 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+4.8; leftover $76.68 |
| `DOCU` | 1 | 2026-09-04 @ $67.06 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.1; leftover $76.68 |
| `DOMO` | 20 | 2026-09-04 @ $3.78 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.8; leftover $76.68 |
| `IOT` | 2 | 2026-09-04 @ $37.69 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.4; leftover $76.68 |
| `MAMA` | 4 | 2026-09-04 @ $15.62 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.7; leftover $76.68 |
