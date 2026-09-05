# Factor mine action — `union_coil_off_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ coil_off hold 5, no 🚨

Cash book **+9.72%** ($10,972) · signal-only (no cash/fees) was +7.47%. Starts YES **10/17**. Fills 76 · skips 199 · realized $+698.88.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $101.23.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | TPG, VOR | — | $37.44 | $10,677.03 | TPG×98, VOR×227 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $37.44 | TPG×98, VOR×227 | $10,751.77 | +74.74 | LDI, BTBT, ANGX, HYLN | — | $20.51 | $10,461.99 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1 | 09:30 open · cash $37.44 (unchanged overnight, no fees) · equity $10,751.77 vs prior close $10,677.03 (+74.74) because holdings re-marked: TPG×98 yday $54.62 → 09:30 $55.29 +65.66; VOR×227 yday $23.29 → 09:30 $23.33 +9.08 |
| 2026-08-17 | +2.25 | $20.51 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1 | $10,399.63 | -62.36 | DNN | — | $17.24 | $10,334.26 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 | 09:30 open · cash $20.51 (unchanged overnight, no fees) · equity $10,399.63 vs prior close $10,461.99 (-62.36) because holdings re-marked: TPG×98 yday $53.03 → 09:30 $52.67 -35.28; VOR×227 yday $23.03 → 09:30 $22.91 -27.24; LDI×4 yday $0.90 → 09:30 $0.91 +0.04; BTBT×3 yday $1.57 → 09:30 $1.52 -0.15; ANGX×1 yday $4.37 → 09:30 $4.60 +0.23; HYLN×1 yday $4.06 → 09:30 $4.10 +0.04 |
| 2026-08-18 | -6.20 | $17.24 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 | $10,290.79 | -43.47 | — | — | $17.24 | $10,419.40 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 | 09:30 open · cash $17.24 (unchanged overnight, no fees) · equity $10,290.79 vs prior close $10,334.26 (-43.47) because holdings re-marked: TPG×98 yday $51.77 → 09:30 $51.77 +0.00; VOR×227 yday $23.01 → 09:30 $22.82 -43.13; LDI×4 yday $0.88 → 09:30 $0.87 -0.02; BTBT×3 yday $1.60 → 09:30 $1.54 -0.18; ANGX×1 yday $4.71 → 09:30 $4.79 +0.08; HYLN×1 yday $4.09 → 09:30 $3.95 -0.14; DNN×1 yday $3.19 → 09:30 $3.11 -0.08 |
| 2026-08-19 | -7.20 | $17.24 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 | $10,617.70 | +198.30 | — | — | $17.24 | $10,600.73 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 | 09:30 open · cash $17.24 (unchanged overnight, no fees) · equity $10,617.70 vs prior close $10,419.40 (+198.30) because holdings re-marked: TPG×98 yday $52.02 → 09:30 $52.26 +23.52; VOR×227 yday $23.28 → 09:30 $24.05 +174.79; LDI×4 yday $0.86 → 09:30 $0.88 +0.09; BTBT×3 yday $1.45 → 09:30 $1.42 -0.09; ANGX×1 yday $4.85 → 09:30 $4.79 -0.06; HYLN×1 yday $3.86 → 09:30 $3.87 +0.01; DNN×1 yday $3.15 → 09:30 $3.19 +0.04 |
| 2026-08-20 | +1.12 | $17.24 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 | $10,468.70 | -132.03 | AG, BHP, HDSN, IAG, KGC, NFGC, DNA, EXK | TPG, VOR | $33.66 | $10,544.20 | LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1, AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121 | 09:30 open · cash $17.24 (unchanged overnight, no fees) · equity $10,468.70 vs prior close $10,600.73 (-132.03) because holdings re-marked: TPG×98 yday $53.18 → 09:30 $53.06 -11.76; VOR×227 yday $23.58 → 09:30 $23.05 -120.31; LDI×4 yday $0.88 → 09:30 $0.87 -0.02; BTBT×3 yday $1.40 → 09:30 $1.46 +0.17; ANGX×1 yday $4.60 → 09:30 $4.57 -0.03; HYLN×1 yday $3.67 → 09:30 $3.61 -0.06; DNN×1 yday $3.22 → 09:30 $3.20 -0.02 |
| 2026-08-21 | +3.25 | $33.66 | LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1, AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121 | $10,815.32 | +271.12 | ORBS, EMBC, HITI | LDI, ANGX, HYLN | $28.25 | $10,801.44 | BTBT×3, DNN×1, AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121, ORBS×7, EMBC×1, HITI×2 | 09:30 open · cash $33.66 (unchanged overnight, no fees) · equity $10,815.32 vs prior close $10,544.20 (+271.12) because holdings re-marked: LDI×4 yday $0.87 → 09:30 $0.87 -0.01; BTBT×3 yday $1.59 → 09:30 $1.66 +0.19; ANGX×1 yday $4.37 → 09:30 $4.43 +0.06; HYLN×1 yday $3.37 → 09:30 $3.42 +0.05; DNN×1 yday $3.14 → 09:30 $3.23 +0.09; AG×63 yday $21.19 → 09:30 $21.90 +44.73; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; HDSN×226 yday $5.57 → 09:30 $5.67 +22.60; IAG×66 yday $20.50 → 09:30 $21.17 +44.22; KGC×44 yday $31.43 → 09:30 $32.17 +32.56; NFGC×746 yday $1.75 → 09:30 $1.79 +29.84; DNA×175 yday $6.96 → 09:30 $7.09 +22.75; EXK×121 yday $10.97 → 09:30 $11.34 +44.77 |
| 2026-08-24 | -5.17 | $28.25 | BTBT×3, DNN×1, AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121, ORBS×7, EMBC×1, HITI×2 | $10,920.60 | +119.16 | — | BTBT | $32.83 | $10,737.17 | DNN×1, AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121, ORBS×7, EMBC×1, HITI×2 | 09:30 open · cash $28.25 (unchanged overnight, no fees) · equity $10,920.60 vs prior close $10,801.44 (+119.16) because holdings re-marked: BTBT×3 yday $1.53 → 09:30 $1.55 +0.06; DNN×1 yday $3.50 → 09:30 $3.50 +0.00; AG×63 yday $21.09 → 09:30 $21.47 +23.94; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; HDSN×226 yday $5.63 → 09:30 $5.69 +13.56; IAG×66 yday $21.14 → 09:30 $21.44 +19.80; KGC×44 yday $32.76 → 09:30 $33.21 +19.80; NFGC×746 yday $1.84 → 09:30 $1.86 +14.92; DNA×175 yday $7.40 → 09:30 $7.26 -24.50; EXK×121 yday $10.62 → 09:30 $11.01 +47.19; ORBS×7 yday $0.88 → 09:30 $0.89 +0.07; EMBC×1 yday $5.23 → 09:30 $5.21 -0.02; HITI×2 yday $2.45 → 09:30 $2.45 +0.00 |
| 2026-08-25 | +1.80 | $32.83 | DNN×1, AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121, ORBS×7, EMBC×1, HITI×2 | $10,735.80 | -1.37 | BMEA | DNN | $33.03 | $10,704.62 | AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121, ORBS×7, EMBC×1, HITI×2, BMEA×2 | 09:30 open · cash $32.83 (unchanged overnight, no fees) · equity $10,735.80 vs prior close $10,737.17 (-1.37) because holdings re-marked: DNN×1 yday $3.54 → 09:30 $3.54 +0.00; AG×63 yday $20.57 → 09:30 $20.73 +10.08; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; HDSN×226 yday $5.57 → 09:30 $5.53 -9.04; IAG×66 yday $21.36 → 09:30 $21.63 +17.82; KGC×44 yday $32.47 → 09:30 $32.76 +12.76; NFGC×746 yday $1.90 → 09:30 $1.91 +7.46; DNA×175 yday $6.98 → 09:30 $6.82 -28.00; EXK×121 yday $10.74 → 09:30 $10.72 -2.42; ORBS×7 yday $0.85 → 09:30 $0.85 +0.00; EMBC×1 yday $5.08 → 09:30 $4.99 -0.09; HITI×2 yday $2.46 → 09:30 $2.46 +0.00 |
| 2026-08-26 | +2.02 | $33.03 | AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121, ORBS×7, EMBC×1, HITI×2, BMEA×2 | $10,704.62 | +0.00 | — | — | $33.03 | $10,735.70 | AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121, ORBS×7, EMBC×1, HITI×2, BMEA×2 | 09:30 open · cash $33.03 (unchanged overnight, no fees) · equity $10,704.62 vs prior close $10,704.62 (+0.00) because holdings re-marked: AG×63 yday $20.68 → 09:30 $20.68 +0.00; BHP×14 yday $96.05 → 09:30 $96.05 +0.00; HDSN×226 yday $5.49 → 09:30 $5.49 +0.00; IAG×66 yday $21.48 → 09:30 $21.48 +0.00; KGC×44 yday $32.55 → 09:30 $32.55 +0.00; NFGC×746 yday $1.90 → 09:30 $1.90 +0.00; DNA×175 yday $6.89 → 09:30 $6.89 +0.00; EXK×121 yday $10.67 → 09:30 $10.67 +0.00; ORBS×7 yday $0.84 → 09:30 $0.84 +0.00; EMBC×1 yday $5.19 → 09:30 $5.19 +0.00; HITI×2 yday $2.46 → 09:30 $2.46 +0.00; BMEA×2 yday $1.61 → 09:30 $1.61 +0.00 |
| 2026-08-27 | — | $33.03 | AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121, ORBS×7, EMBC×1, HITI×2, BMEA×2 | $10,914.87 | +179.17 | RRC, CRK, SLI, TX, DLO, GEN, MRVL, PGY | AG, BHP, HDSN, IAG, KGC, NFGC, DNA, EXK | $214.08 | $11,008.97 | ORBS×7, EMBC×1, HITI×2, BMEA×2, RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61 | 09:30 open · cash $33.03 (unchanged overnight, no fees) · equity $10,914.87 vs prior close $10,735.70 (+179.17) because holdings re-marked: AG×63 yday $20.68 → 09:30 $20.63 -3.15; BHP×14 yday $96.05 → 09:30 $96.99 +13.16; HDSN×226 yday $5.49 → 09:30 $5.51 +4.52; IAG×66 yday $21.48 → 09:30 $21.64 +10.56; KGC×44 yday $32.55 → 09:30 $32.90 +15.40; NFGC×746 yday $1.90 → 09:30 $2.00 +74.60; DNA×175 yday $6.89 → 09:30 $7.33 +77.00; EXK×121 yday $10.67 → 09:30 $10.82 +18.15; ORBS×7 yday $0.84 → 09:30 $0.80 -0.28; EMBC×1 yday $5.19 → 09:30 $4.98 -0.21; HITI×2 yday $2.46 → 09:30 $2.57 +0.22; BMEA×2 yday $1.61 → 09:30 $1.75 +0.28 |
| 2026-08-28 | +0.75 | $214.08 | ORBS×7, EMBC×1, HITI×2, BMEA×2, RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61 | $11,075.13 | +66.16 | BZ, BBWI, GENB, ADBT | ORBS, EMBC, HITI | $74.19 | $11,105.01 | BMEA×2, RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61, BZ×2, BBWI×2, GENB×2, ADBT×9 | 09:30 open · cash $214.08 (unchanged overnight, no fees) · equity $11,075.13 vs prior close $11,008.97 (+66.16) because holdings re-marked: ORBS×7 yday $0.80 → 09:30 $0.82 +0.14; EMBC×1 yday $4.96 → 09:30 $4.90 -0.06; HITI×2 yday $2.52 → 09:30 $2.52 +0.00; BMEA×2 yday $1.71 → 09:30 $1.74 +0.06; RRC×33 yday $41.55 → 09:30 $41.44 -3.63; CRK×96 yday $14.50 → 09:30 $14.42 -7.68; SLI×524 yday $2.61 → 09:30 $2.60 -5.24; TX×24 yday $55.13 → 09:30 $55.25 +2.88; DLO×87 yday $15.36 → 09:30 $15.33 -2.61; GEN×47 yday $29.64 → 09:30 $29.83 +8.93; MRVL×5 yday $245.11 → 09:30 $253.44 +41.65; PGY×61 yday $22.41 → 09:30 $22.93 +31.72 |
| 2026-08-31 | -5.85 | $74.19 | BMEA×2, RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61, BZ×2, BBWI×2, GENB×2, ADBT×9 | $10,772.72 | -332.29 | — | — | $74.19 | $10,814.20 | BMEA×2, RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61, BZ×2, BBWI×2, GENB×2, ADBT×9 | 09:30 open · cash $74.19 (unchanged overnight, no fees) · equity $10,772.72 vs prior close $11,105.01 (-332.29) because holdings re-marked: BMEA×2 yday $1.68 → 09:30 $1.71 +0.06; RRC×33 yday $41.64 → 09:30 $41.11 -17.49; CRK×96 yday $14.62 → 09:30 $14.56 -5.76; SLI×524 yday $2.64 → 09:30 $2.51 -68.12; TX×24 yday $55.83 → 09:30 $54.84 -23.76; DLO×87 yday $15.14 → 09:30 $15.01 -11.31; GEN×47 yday $30.50 → 09:30 $31.02 +24.44; MRVL×5 yday $241.45 → 09:30 $216.69 -123.80; PGY×61 yday $23.26 → 09:30 $21.51 -106.75; BZ×2 yday $18.00 → 09:30 $17.89 -0.22; BBWI×2 yday $18.65 → 09:30 $19.30 +1.30; GENB×2 yday $15.77 → 09:30 $15.33 -0.88; ADBT×9 yday $4.99 → 09:30 $4.99 +0.00 |
| 2026-09-01 | -6.30 | $74.19 | BMEA×2, RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61, BZ×2, BBWI×2, GENB×2, ADBT×9 | $10,799.64 | -14.56 | — | BMEA | $77.43 | $10,785.40 | RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61, BZ×2, BBWI×2, GENB×2, ADBT×9 | 09:30 open · cash $74.19 (unchanged overnight, no fees) · equity $10,799.64 vs prior close $10,814.20 (-14.56) because holdings re-marked: BMEA×2 yday $1.71 → 09:30 $1.65 -0.12; RRC×33 yday $41.78 → 09:30 $41.32 -15.18; CRK×96 yday $14.51 → 09:30 $14.31 -19.20; SLI×524 yday $2.51 → 09:30 $2.70 +99.56; TX×24 yday $54.84 → 09:30 $54.82 -0.48; DLO×87 yday $15.00 → 09:30 $14.88 -10.44; GEN×47 yday $31.02 → 09:30 $30.56 -21.62; MRVL×5 yday $216.35 → 09:30 $210.57 -28.90; PGY×61 yday $21.95 → 09:30 $21.73 -13.42; BZ×2 yday $17.90 → 09:30 $17.37 -1.06; BBWI×2 yday $19.22 → 09:30 $19.10 -0.24; GENB×2 yday $15.35 → 09:30 $15.51 +0.32; ADBT×9 yday $4.99 → 09:30 $4.57 -3.78 |
| 2026-09-02 | -3.83 | $77.43 | RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61, BZ×2, BBWI×2, GENB×2, ADBT×9 | $10,859.05 | +73.65 | — | — | $77.43 | $10,822.85 | RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61, BZ×2, BBWI×2, GENB×2, ADBT×9 | 09:30 open · cash $77.43 (unchanged overnight, no fees) · equity $10,859.05 vs prior close $10,785.40 (+73.65) because holdings re-marked: RRC×33 yday $41.32 → 09:30 $41.94 +20.46; CRK×96 yday $14.90 → 09:30 $15.82 +88.32; SLI×524 yday $2.70 → 09:30 $2.67 -15.72; TX×24 yday $54.82 → 09:30 $54.76 -1.44; DLO×87 yday $14.70 → 09:30 $14.61 -7.83; GEN×47 yday $30.56 → 09:30 $30.73 +7.99; MRVL×5 yday $205.90 → 09:30 $205.11 -3.95; PGY×61 yday $21.25 → 09:30 $21.02 -14.03; BZ×2 yday $17.17 → 09:30 $17.29 +0.24; BBWI×2 yday $19.10 → 09:30 $18.77 -0.66; GENB×2 yday $15.30 → 09:30 $15.12 -0.36; ADBT×9 yday $4.38 → 09:30 $4.45 +0.63 |
| 2026-09-03 | -0.90 | $77.43 | RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61, BZ×2, BBWI×2, GENB×2, ADBT×9 | $10,764.03 | -58.82 | RVTY, GPRO, MMED, EIX, CLYM, CNXC, BMEA | RRC, SLI, TX, DLO, GEN, MRVL, PGY | $102.72 | $11,389.62 | CRK×96, BZ×2, BBWI×2, GENB×2, ADBT×9, RVTY×10, GPRO×1065, MMED×57, EIX×22, CLYM×87, CNXC×40, BMEA×722 | 09:30 open · cash $77.43 (unchanged overnight, no fees) · equity $10,764.03 vs prior close $10,822.85 (-58.82) because holdings re-marked: RRC×33 yday $42.40 → 09:30 $42.10 -9.90; CRK×96 yday $16.02 → 09:30 $15.70 -30.72; SLI×524 yday $2.49 → 09:30 $2.49 +0.00; TX×24 yday $55.67 → 09:30 $56.17 +12.00; DLO×87 yday $14.83 → 09:30 $14.82 -0.87; GEN×47 yday $30.02 → 09:30 $30.04 +0.94; MRVL×5 yday $210.39 → 09:30 $205.25 -25.70; PGY×61 yday $20.94 → 09:30 $20.88 -3.66; BZ×2 yday $17.55 → 09:30 $17.65 +0.20; BBWI×2 yday $18.61 → 09:30 $18.41 -0.40; GENB×2 yday $15.79 → 09:30 $15.75 -0.08; ADBT×9 yday $3.68 → 09:30 $3.61 -0.63 |
| 2026-09-04 | — | $102.72 | CRK×96, BZ×2, BBWI×2, GENB×2, ADBT×9, RVTY×10, GPRO×1065, MMED×57, EIX×22, CLYM×87, CNXC×40, BMEA×722 | $11,427.98 | +38.36 | BVS, MLYS, SGLD, FMC, TARS, SCZM, PLAY | CRK, BZ, BBWI, GENB, ADBT | $101.23 | $10,972.11 | RVTY×10, GPRO×1065, MMED×57, EIX×22, CLYM×87, CNXC×40, BMEA×722, BVS×16, MLYS×8, SGLD×37, FMC×18, TARS×2, SCZM×23, PLAY×25 | 09:30 open · cash $102.72 (unchanged overnight, no fees) · equity $11,427.98 vs prior close $11,389.62 (+38.36) because holdings re-marked: CRK×96 yday $15.54 → 09:30 $15.45 -8.64; BZ×2 yday $17.30 → 09:30 $17.31 +0.02; BBWI×2 yday $18.53 → 09:30 $18.59 +0.12; GENB×2 yday $16.28 → 09:30 $16.40 +0.24; ADBT×9 yday $1.72 → 09:30 $1.52 -1.80; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1065 yday $1.69 → 09:30 $1.78 +95.85; MMED×57 yday $23.76 → 09:30 $23.88 +6.84; EIX×22 yday $55.19 → 09:30 $55.42 +5.06; CLYM×87 yday $15.05 → 09:30 $13.96 -94.83; CNXC×40 yday $32.37 → 09:30 $32.88 +20.40; BMEA×722 yday $1.93 → 09:30 $1.93 +0.00 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 98 | $50.62 | $2.28 | — | $5,036.64 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 227 | $22.01 | $2.93 | — | $37.44 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.44 | ▲ 09:30 equity $10,751.77 vs yday $10,677.03 (+74.74) | 09:30 open · cash $37.44 (unchanged overnight, no fees) · equity $10,751.77 vs prior close $10,677.03 (+74.74) because holdings re-marked: TPG×98 yday $54.62 → 09:30 $55.29 +65.66; VOR×227 yday $23.29 → 09:30 $23.33 +9.08 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 4 | $0.94 | $0.05 | — | $33.65 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 3 | $1.50 | $0.05 | — | $29.09 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 1 | $4.31 | $0.05 | — | $24.74 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 1 | $4.18 | $0.04 | — | $20.51 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.51 | ▼ 09:30 equity $10,399.63 vs yday $10,461.99 (-62.36) | 09:30 open · cash $20.51 (unchanged overnight, no fees) · equity $10,399.63 vs prior close $10,461.99 (-62.36) because holdings re-marked: TPG×98 yday $53.03 → 09:30 $52.67 -35.28; VOR×227 yday $23.03 → 09:30 $22.91 -27.24; LDI×4 yday $0.90 → 09:30 $0.91 +0.04; BTBT×3 yday $1.57 → 09:30 $1.52 -0.15; ANGX×1 yday $4.37 → 09:30 $4.60 +0.23; HYLN×1 yday $4.06 → 09:30 $4.10 +0.04 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $17.24 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $4.10 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.24 | ▼ 09:30 equity $10,290.79 vs yday $10,334.26 (-43.47) | 09:30 open · cash $17.24 (unchanged overnight, no fees) · equity $10,290.79 vs prior close $10,334.26 (-43.47) because holdings re-marked: TPG×98 yday $51.77 → 09:30 $51.77 +0.00; VOR×227 yday $23.01 → 09:30 $22.82 -43.13; LDI×4 yday $0.88 → 09:30 $0.87 -0.02; BTBT×3 yday $1.60 → 09:30 $1.54 -0.18; ANGX×1 yday $4.71 → 09:30 $4.79 +0.08; HYLN×1 yday $4.09 → 09:30 $3.95 -0.14; DNN×1 yday $3.19 → 09:30 $3.11 -0.08 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.24 | ▲ 09:30 equity $10,617.70 vs yday $10,419.40 (+198.30) | 09:30 open · cash $17.24 (unchanged overnight, no fees) · equity $10,617.70 vs prior close $10,419.40 (+198.30) because holdings re-marked: TPG×98 yday $52.02 → 09:30 $52.26 +23.52; VOR×227 yday $23.28 → 09:30 $24.05 +174.79; LDI×4 yday $0.86 → 09:30 $0.88 +0.09; BTBT×3 yday $1.45 → 09:30 $1.42 -0.09; ANGX×1 yday $4.85 → 09:30 $4.79 -0.06; HYLN×1 yday $3.86 → 09:30 $3.87 +0.01; DNN×1 yday $3.15 → 09:30 $3.19 +0.04 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.24 | ▼ 09:30 equity $10,468.70 vs yday $10,600.73 (-132.03) | 09:30 open · cash $17.24 (unchanged overnight, no fees) · equity $10,468.70 vs prior close $10,600.73 (-132.03) because holdings re-marked: TPG×98 yday $53.18 → 09:30 $53.06 -11.76; VOR×227 yday $23.58 → 09:30 $23.05 -120.31; LDI×4 yday $0.88 → 09:30 $0.87 -0.02; BTBT×3 yday $1.40 → 09:30 $1.46 +0.17; ANGX×1 yday $4.60 → 09:30 $4.57 -0.03; HYLN×1 yday $3.67 → 09:30 $3.61 -0.06; DNN×1 yday $3.22 → 09:30 $3.20 -0.02 | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 98 | $53.06 | $2.34 | $+234.18 | $5,214.77 | ▲ +234.18 after sell → book $10,466.35; vs 09:30 mark -2.35 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `VOR` | 227 | $23.05 | $3.01 | $+230.14 | $10,444.12 | ▲ +230.14 after sell → book $10,463.35; vs 09:30 mark -3.00 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 63 | $20.55 | $2.18 | — | $9,147.29 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1305.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,871.12 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1305.51 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 226 | $5.77 | $2.92 | — | $6,564.18 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1305.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 66 | $19.63 | $2.19 | — | $5,266.41 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1305.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 44 | $29.63 | $2.12 | — | $3,960.57 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1305.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 746 | $1.75 | $9.62 | — | $2,645.45 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1305.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 175 | $7.45 | $2.52 | — | $1,339.18 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1305.51 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 121 | $10.77 | $2.35 | — | $33.66 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1305.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.66 | ▲ 09:30 equity $10,815.32 vs yday $10,544.20 (+271.12) | 09:30 open · cash $33.66 (unchanged overnight, no fees) · equity $10,815.32 vs prior close $10,544.20 (+271.12) because holdings re-marked: LDI×4 yday $0.87 → 09:30 $0.87 -0.01; BTBT×3 yday $1.59 → 09:30 $1.66 +0.19; ANGX×1 yday $4.37 → 09:30 $4.43 +0.06; HYLN×1 yday $3.37 → 09:30 $3.42 +0.05; DNN×1 yday $3.14 → 09:30 $3.23 +0.09; AG×63 yday $21.19 → 09:30 $21.90 +44.73; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; HDSN×226 yday $5.57 → 09:30 $5.67 +22.60; IAG×66 yday $20.50 → 09:30 $21.17 +44.22; KGC×44 yday $31.43 → 09:30 $32.17 +32.56; NFGC×746 yday $1.75 → 09:30 $1.79 +29.84; DNA×175 yday $6.96 → 09:30 $7.09 +22.75; EXK×121 yday $10.97 → 09:30 $11.34 +44.77 | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 4 | $0.87 | $0.07 | $-0.40 | $37.06 | ▼ -0.40 after sell → book $10,815.25; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 1 | $4.43 | $0.07 | $+0.01 | $41.42 | ▲ +0.01 after sell → book $10,815.18; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 1 | $3.42 | $0.06 | $-0.86 | $44.79 | ▼ -0.86 after sell → book $10,815.13; vs 09:30 mark -0.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 7 | $0.86 | $0.08 | — | $38.66 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $6.40 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 1 | $5.43 | $0.06 | — | $33.17 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $6.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 2 | $2.43 | $0.05 | — | $28.25 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $6.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.25 | ▲ 09:30 equity $10,920.60 vs yday $10,801.44 (+119.16) | 09:30 open · cash $28.25 (unchanged overnight, no fees) · equity $10,920.60 vs prior close $10,801.44 (+119.16) because holdings re-marked: BTBT×3 yday $1.53 → 09:30 $1.55 +0.06; DNN×1 yday $3.50 → 09:30 $3.50 +0.00; AG×63 yday $21.09 → 09:30 $21.47 +23.94; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; HDSN×226 yday $5.63 → 09:30 $5.69 +13.56; IAG×66 yday $21.14 → 09:30 $21.44 +19.80; KGC×44 yday $32.76 → 09:30 $33.21 +19.80; NFGC×746 yday $1.84 → 09:30 $1.86 +14.92; DNA×175 yday $7.40 → 09:30 $7.26 -24.50; EXK×121 yday $10.62 → 09:30 $11.01 +47.19; ORBS×7 yday $0.88 → 09:30 $0.89 +0.07; EMBC×1 yday $5.23 → 09:30 $5.21 -0.02; HITI×2 yday $2.45 → 09:30 $2.45 +0.00 | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 3 | $1.55 | $0.08 | $+0.02 | $32.83 | ▲ +0.02 after sell → book $10,920.53; vs 09:30 mark -0.07 | dropped from list after 6 sess (min 5) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.83 | ▼ 09:30 equity $10,735.80 vs yday $10,737.17 (-1.37) | 09:30 open · cash $32.83 (unchanged overnight, no fees) · equity $10,735.80 vs prior close $10,737.17 (-1.37) because holdings re-marked: DNN×1 yday $3.54 → 09:30 $3.54 +0.00; AG×63 yday $20.57 → 09:30 $20.73 +10.08; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; HDSN×226 yday $5.57 → 09:30 $5.53 -9.04; IAG×66 yday $21.36 → 09:30 $21.63 +17.82; KGC×44 yday $32.47 → 09:30 $32.76 +12.76; NFGC×746 yday $1.90 → 09:30 $1.91 +7.46; DNA×175 yday $6.98 → 09:30 $6.82 -28.00; EXK×121 yday $10.74 → 09:30 $10.72 -2.42; ORBS×7 yday $0.85 → 09:30 $0.85 +0.00; EMBC×1 yday $5.08 → 09:30 $4.99 -0.09; HITI×2 yday $2.46 → 09:30 $2.46 +0.00 | — |
| 2026-08-25 09:30 ET | **SELL** | `DNN` | 1 | $3.54 | $0.06 | $+0.21 | $36.31 | ▲ +0.21 after sell → book $10,735.74; vs 09:30 mark -0.06 | dropped from list after 6 sess (min 5) | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 2 | $1.62 | $0.04 | — | $33.03 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $4.54 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.03 | ▲ 09:30 equity $10,704.62 vs yday $10,704.62 (+0.00) | 09:30 open · cash $33.03 (unchanged overnight, no fees) · equity $10,704.62 vs prior close $10,704.62 (+0.00) because holdings re-marked: AG×63 yday $20.68 → 09:30 $20.68 +0.00; BHP×14 yday $96.05 → 09:30 $96.05 +0.00; HDSN×226 yday $5.49 → 09:30 $5.49 +0.00; IAG×66 yday $21.48 → 09:30 $21.48 +0.00; KGC×44 yday $32.55 → 09:30 $32.55 +0.00; NFGC×746 yday $1.90 → 09:30 $1.90 +0.00; DNA×175 yday $6.89 → 09:30 $6.89 +0.00; EXK×121 yday $10.67 → 09:30 $10.67 +0.00; ORBS×7 yday $0.84 → 09:30 $0.84 +0.00; EMBC×1 yday $5.19 → 09:30 $5.19 +0.00; HITI×2 yday $2.46 → 09:30 $2.46 +0.00; BMEA×2 yday $1.61 → 09:30 $1.61 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.03 | ▲ 09:30 equity $10,914.87 vs yday $10,735.70 (+179.17) | 09:30 open · cash $33.03 (unchanged overnight, no fees) · equity $10,914.87 vs prior close $10,735.70 (+179.17) because holdings re-marked: AG×63 yday $20.68 → 09:30 $20.63 -3.15; BHP×14 yday $96.05 → 09:30 $96.99 +13.16; HDSN×226 yday $5.49 → 09:30 $5.51 +4.52; IAG×66 yday $21.48 → 09:30 $21.64 +10.56; KGC×44 yday $32.55 → 09:30 $32.90 +15.40; NFGC×746 yday $1.90 → 09:30 $2.00 +74.60; DNA×175 yday $6.89 → 09:30 $7.33 +77.00; EXK×121 yday $10.67 → 09:30 $10.82 +18.15; ORBS×7 yday $0.84 → 09:30 $0.80 -0.28; EMBC×1 yday $5.19 → 09:30 $4.98 -0.21; HITI×2 yday $2.46 → 09:30 $2.57 +0.22; BMEA×2 yday $1.61 → 09:30 $1.75 +0.28 | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 63 | $20.63 | $2.20 | $+0.66 | $1,330.52 | ▲ +0.66 after sell → book $10,912.67; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $96.99 | $2.05 | $+79.64 | $2,686.33 | ▲ +79.64 after sell → book $10,910.62; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 226 | $5.51 | $2.96 | $-64.64 | $3,928.63 | ▼ -64.64 after sell → book $10,907.66; vs 09:30 mark -2.96 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 66 | $21.64 | $2.21 | $+128.26 | $5,354.66 | ▲ +128.26 after sell → book $10,905.45; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 44 | $32.90 | $2.14 | $+139.61 | $6,800.11 | ▲ +139.61 after sell → book $10,903.30; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 746 | $2.00 | $9.76 | $+167.12 | $8,282.35 | ▲ +167.12 after sell → book $10,893.54; vs 09:30 mark -9.76 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `DNA` | 175 | $7.33 | $2.55 | $-26.07 | $9,562.55 | ▼ -26.07 after sell → book $10,890.99; vs 09:30 mark -2.55 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `EXK` | 121 | $10.82 | $2.38 | $+1.31 | $10,869.39 | ▲ +1.31 after sell → book $10,888.61; vs 09:30 mark -2.38 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $40.72 | $2.09 | — | $9,523.54 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+1.8; leftover $1358.67 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 96 | $14.09 | $2.28 | — | $8,168.62 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+1.1; leftover $1358.67 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 524 | $2.59 | $6.76 | — | $6,804.70 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+4.2; leftover $1358.67 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 24 | $55.20 | $2.06 | — | $5,477.84 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+3.0; leftover $1358.67 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 87 | $15.60 | $2.25 | — | $4,118.39 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+7.1; leftover $1358.67 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 47 | $28.89 | $2.13 | — | $2,758.42 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+1.6; leftover $1358.67 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $240.00 | $2.00 | — | $1,556.42 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+6.8; leftover $1358.67 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 61 | $21.97 | $2.17 | — | $214.08 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+0.6; leftover $1358.67 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $214.08 | ▲ 09:30 equity $11,075.13 vs yday $11,008.97 (+66.16) | 09:30 open · cash $214.08 (unchanged overnight, no fees) · equity $11,075.13 vs prior close $11,008.97 (+66.16) because holdings re-marked: ORBS×7 yday $0.80 → 09:30 $0.82 +0.14; EMBC×1 yday $4.96 → 09:30 $4.90 -0.06; HITI×2 yday $2.52 → 09:30 $2.52 +0.00; BMEA×2 yday $1.71 → 09:30 $1.74 +0.06; RRC×33 yday $41.55 → 09:30 $41.44 -3.63; CRK×96 yday $14.50 → 09:30 $14.42 -7.68; SLI×524 yday $2.61 → 09:30 $2.60 -5.24; TX×24 yday $55.13 → 09:30 $55.25 +2.88; DLO×87 yday $15.36 → 09:30 $15.33 -2.61; GEN×47 yday $29.64 → 09:30 $29.83 +8.93; MRVL×5 yday $245.11 → 09:30 $253.44 +41.65; PGY×61 yday $22.41 → 09:30 $22.93 +31.72 | — |
| 2026-08-28 09:30 ET | **SELL** | `ORBS` | 7 | $0.82 | $0.10 | $-0.49 | $219.72 | ▼ -0.49 after sell → book $11,075.03; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `EMBC` | 1 | $4.90 | $0.07 | $-0.66 | $224.55 | ▼ -0.66 after sell → book $11,074.96; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `HITI` | 2 | $2.52 | $0.08 | $+0.05 | $229.51 | ▲ +0.05 after sell → book $11,074.88; vs 09:30 mark -0.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 2 | $18.50 | $0.38 | — | $192.13 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $45.90 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 2 | $18.68 | $0.38 | — | $154.39 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+0.2; leftover $45.90 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GENB` | 2 | $17.10 | $0.35 | — | $119.85 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+3.1; leftover $45.90 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADBT` | 9 | $5.02 | $0.48 | — | $74.19 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+0.0; leftover $45.90 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.19 | ▼ 09:30 equity $10,772.72 vs yday $11,105.01 (-332.29) | 09:30 open · cash $74.19 (unchanged overnight, no fees) · equity $10,772.72 vs prior close $11,105.01 (-332.29) because holdings re-marked: BMEA×2 yday $1.68 → 09:30 $1.71 +0.06; RRC×33 yday $41.64 → 09:30 $41.11 -17.49; CRK×96 yday $14.62 → 09:30 $14.56 -5.76; SLI×524 yday $2.64 → 09:30 $2.51 -68.12; TX×24 yday $55.83 → 09:30 $54.84 -23.76; DLO×87 yday $15.14 → 09:30 $15.01 -11.31; GEN×47 yday $30.50 → 09:30 $31.02 +24.44; MRVL×5 yday $241.45 → 09:30 $216.69 -123.80; PGY×61 yday $23.26 → 09:30 $21.51 -106.75; BZ×2 yday $18.00 → 09:30 $17.89 -0.22; BBWI×2 yday $18.65 → 09:30 $19.30 +1.30; GENB×2 yday $15.77 → 09:30 $15.33 -0.88; ADBT×9 yday $4.99 → 09:30 $4.99 +0.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.19 | ▼ 09:30 equity $10,799.64 vs yday $10,814.20 (-14.56) | 09:30 open · cash $74.19 (unchanged overnight, no fees) · equity $10,799.64 vs prior close $10,814.20 (-14.56) because holdings re-marked: BMEA×2 yday $1.71 → 09:30 $1.65 -0.12; RRC×33 yday $41.78 → 09:30 $41.32 -15.18; CRK×96 yday $14.51 → 09:30 $14.31 -19.20; SLI×524 yday $2.51 → 09:30 $2.70 +99.56; TX×24 yday $54.84 → 09:30 $54.82 -0.48; DLO×87 yday $15.00 → 09:30 $14.88 -10.44; GEN×47 yday $31.02 → 09:30 $30.56 -21.62; MRVL×5 yday $216.35 → 09:30 $210.57 -28.90; PGY×61 yday $21.95 → 09:30 $21.73 -13.42; BZ×2 yday $17.90 → 09:30 $17.37 -1.06; BBWI×2 yday $19.22 → 09:30 $19.10 -0.24; GENB×2 yday $15.35 → 09:30 $15.51 +0.32; ADBT×9 yday $4.99 → 09:30 $4.57 -3.78 | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 2 | $1.65 | $0.06 | $-0.04 | $77.43 | ▼ -0.04 after sell → book $10,799.58; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $77.43 | ▲ 09:30 equity $10,859.05 vs yday $10,785.40 (+73.65) | 09:30 open · cash $77.43 (unchanged overnight, no fees) · equity $10,859.05 vs prior close $10,785.40 (+73.65) because holdings re-marked: RRC×33 yday $41.32 → 09:30 $41.94 +20.46; CRK×96 yday $14.90 → 09:30 $15.82 +88.32; SLI×524 yday $2.70 → 09:30 $2.67 -15.72; TX×24 yday $54.82 → 09:30 $54.76 -1.44; DLO×87 yday $14.70 → 09:30 $14.61 -7.83; GEN×47 yday $30.56 → 09:30 $30.73 +7.99; MRVL×5 yday $205.90 → 09:30 $205.11 -3.95; PGY×61 yday $21.25 → 09:30 $21.02 -14.03; BZ×2 yday $17.17 → 09:30 $17.29 +0.24; BBWI×2 yday $19.10 → 09:30 $18.77 -0.66; GENB×2 yday $15.30 → 09:30 $15.12 -0.36; ADBT×9 yday $4.38 → 09:30 $4.45 +0.63 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $77.43 | ▼ 09:30 equity $10,764.03 vs yday $10,822.85 (-58.82) | 09:30 open · cash $77.43 (unchanged overnight, no fees) · equity $10,764.03 vs prior close $10,822.85 (-58.82) because holdings re-marked: RRC×33 yday $42.40 → 09:30 $42.10 -9.90; CRK×96 yday $16.02 → 09:30 $15.70 -30.72; SLI×524 yday $2.49 → 09:30 $2.49 +0.00; TX×24 yday $55.67 → 09:30 $56.17 +12.00; DLO×87 yday $14.83 → 09:30 $14.82 -0.87; GEN×47 yday $30.02 → 09:30 $30.04 +0.94; MRVL×5 yday $210.39 → 09:30 $205.25 -25.70; PGY×61 yday $20.94 → 09:30 $20.88 -3.66; BZ×2 yday $17.55 → 09:30 $17.65 +0.20; BBWI×2 yday $18.61 → 09:30 $18.41 -0.40; GENB×2 yday $15.79 → 09:30 $15.75 -0.08; ADBT×9 yday $3.68 → 09:30 $3.61 -0.63 | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 33 | $42.10 | $2.11 | $+41.34 | $1,464.62 | ▲ +41.34 after sell → book $10,761.92; vs 09:30 mark -2.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 524 | $2.49 | $6.86 | $-66.02 | $2,762.52 | ▼ -66.02 after sell → book $10,755.06; vs 09:30 mark -6.86 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `TX` | 24 | $56.17 | $2.08 | $+19.14 | $4,108.52 | ▲ +19.14 after sell → book $10,752.98; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | join🔴 sector🟢 gen🟡 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SELL** | `DLO` | 87 | $14.82 | $2.28 | $-72.39 | $5,395.58 | ▼ -72.39 after sell → book $10,750.70; vs 09:30 mark -2.28 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `GEN` | 47 | $30.04 | $2.15 | $+49.77 | $6,805.31 | ▲ +49.77 after sell → book $10,748.55; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MRVL` | 5 | $205.25 | $2.02 | $-177.78 | $7,829.54 | ▼ -177.78 after sell → book $10,746.53; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `PGY` | 61 | $20.88 | $2.19 | $-70.86 | $9,101.02 | ▼ -70.86 after sell → book $10,744.33; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $7,839.60 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1300.15 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1065 | $1.22 | $13.74 | — | $6,526.56 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1300.15 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 57 | $22.78 | $2.16 | — | $5,225.94 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1300.15 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $56.78 | $2.06 | — | $3,974.73 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+0.3; leftover $1300.15 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 87 | $14.79 | $2.25 | — | $2,685.75 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+5.8; leftover $1300.15 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 40 | $31.80 | $2.11 | — | $1,411.64 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.7; leftover $1300.15 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 722 | $1.80 | $9.31 | — | $102.72 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1300.15 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $102.72 | ▲ 09:30 equity $11,427.98 vs yday $11,389.62 (+38.36) | 09:30 open · cash $102.72 (unchanged overnight, no fees) · equity $11,427.98 vs prior close $11,389.62 (+38.36) because holdings re-marked: CRK×96 yday $15.54 → 09:30 $15.45 -8.64; BZ×2 yday $17.30 → 09:30 $17.31 +0.02; BBWI×2 yday $18.53 → 09:30 $18.59 +0.12; GENB×2 yday $16.28 → 09:30 $16.40 +0.24; ADBT×9 yday $1.72 → 09:30 $1.52 -1.80; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1065 yday $1.69 → 09:30 $1.78 +95.85; MMED×57 yday $23.76 → 09:30 $23.88 +6.84; EIX×22 yday $55.19 → 09:30 $55.42 +5.06; CLYM×87 yday $15.05 → 09:30 $13.96 -94.83; CNXC×40 yday $32.37 → 09:30 $32.88 +20.40; BMEA×722 yday $1.93 → 09:30 $1.93 +0.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 96 | $15.45 | $2.31 | $+125.98 | $1,583.62 | ▲ +125.98 after sell → book $11,425.68; vs 09:30 mark -2.30 | dropped from list after 6 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BZ` | 2 | $17.31 | $0.37 | $-3.13 | $1,617.86 | ▼ -3.13 after sell → book $11,425.30; vs 09:30 mark -0.38 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BBWI` | 2 | $18.59 | $0.40 | $-0.96 | $1,654.65 | ▼ -0.96 after sell → book $11,424.91; vs 09:30 mark -0.39 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `GENB` | 2 | $16.40 | $0.35 | $-2.10 | $1,687.09 | ▼ -2.10 after sell → book $11,424.55; vs 09:30 mark -0.36 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `ADBT` | 9 | $1.52 | $0.18 | $-32.16 | $1,700.59 | ▼ -32.16 after sell → book $11,424.37; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 16 | $14.50 | $2.04 | — | $1,466.55 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.8; leftover $242.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 8 | $29.15 | $2.01 | — | $1,231.34 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $242.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SGLD` | 37 | $6.48 | $2.10 | — | $989.48 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,yday_mover; ret5=+0.0; leftover $242.94 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `FMC` | 18 | $13.30 | $2.04 | — | $748.03 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,yday_mover; ret5=+8.6; leftover $242.94 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 2 | $82.76 | $1.66 | — | $580.85 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $242.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SCZM` | 23 | $10.50 | $2.06 | — | $337.29 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+9.3; leftover $242.94 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `PLAY` | 25 | $9.36 | $2.06 | — | $101.23 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+0.6; leftover $242.94 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 4.68 < 1 share @ 359.83 |
| 2026-08-14 | `SLG` | cash | leftover split 4.68 < 1 share @ 57.61 |
| 2026-08-14 | `WDC` | cash | leftover split 4.68 < 1 share @ 503.50 |
| 2026-08-14 | `ADUR` | cash | leftover split 4.68 < 1 share @ 16.50 |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 4.10 < 1 share @ 46.18 |
| 2026-08-17 | `OCC` | cash | leftover split 4.10 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 4.10 < 1 share @ 16.20 |
| 2026-08-17 | `NEWP` | cash | leftover split 4.10 < 1 share @ 6.94 |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `VOR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `VOR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ANGX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HYLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MXL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HYLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `DNN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CRSP` | cash | leftover split 6.40 < 1 share @ 59.72 |
| 2026-08-21 | `TXG` | cash | leftover split 6.40 < 1 share @ 64.39 |
| 2026-08-21 | `DXYZ` | cash | leftover split 6.40 < 1 share @ 34.89 |
| 2026-08-21 | `BEKE` | cash | leftover split 6.40 < 1 share @ 17.93 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `EMBC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `HITI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABAT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BHP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `HDSN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `IAG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NFGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `DNA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `EXK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `EMBC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `HITI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRMD` | cash | leftover split 4.54 < 1 share @ 8.28 |
| 2026-08-25 | `HCA` | cash | leftover split 4.54 < 1 share @ 429.24 |
| 2026-08-25 | `ALIT` | cash | leftover split 4.54 < 1 share @ 14.86 |
| 2026-08-25 | `ZURA` | cash | leftover split 4.54 < 1 share @ 6.38 |
| 2026-08-25 | `JANX` | cash | leftover split 4.54 < 1 share @ 18.52 |
| 2026-08-25 | `KURA` | cash | leftover split 4.54 < 1 share @ 13.30 |
| 2026-08-25 | `EZPW` | cash | leftover split 4.54 < 1 share @ 34.48 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `DNA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `EXK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `ORBS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `EMBC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `HITI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `HCA` | no_price | no 09:30 open |
| 2026-08-26 | `CRMD` | no_price | no 09:30 open |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `BZ` | no_price | no 09:30 open |
| 2026-08-26 | `CNTN` | no_price | no 09:30 open |
| 2026-08-26 | `OSUR` | no_price | no 09:30 open |
| 2026-08-26 | `INO` | no_price | no 09:30 open |
| 2026-08-27 | `ORBS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `EMBC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `HITI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `TX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `DLO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `GEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `MRVL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `PGY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `ANF` | cash | leftover split 45.90 < 1 share @ 144.70 |
| 2026-08-31 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `TX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `GEN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MRVL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `PGY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `GENB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `ADBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `INO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DINO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `TX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `DLO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `GEN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `MRVL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `PGY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `GENB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `ADBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OHI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `XLAB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `RRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `CRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `SLI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `TX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `DLO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `GEN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `MRVL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `PGY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `BZ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BBWI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `GENB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `ADBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HELP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SCZM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `BZ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BBWI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `GENB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `ADBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CLYM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CNXC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RVTY` | 10 | 2026-09-03 @ $125.94 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1300.15 |
| `GPRO` | 1065 | 2026-09-03 @ $1.22 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1300.15 |
| `MMED` | 57 | 2026-09-03 @ $22.78 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1300.15 |
| `EIX` | 22 | 2026-09-03 @ $56.78 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+0.3; leftover $1300.15 |
| `CLYM` | 87 | 2026-09-03 @ $14.79 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+5.8; leftover $1300.15 |
| `CNXC` | 40 | 2026-09-03 @ $31.80 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.7; leftover $1300.15 |
| `BMEA` | 722 | 2026-09-03 @ $1.80 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1300.15 |
| `BVS` | 16 | 2026-09-04 @ $14.50 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.8; leftover $242.94 |
| `MLYS` | 8 | 2026-09-04 @ $29.15 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $242.94 |
| `SGLD` | 37 | 2026-09-04 @ $6.48 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,yday_mover; ret5=+0.0; leftover $242.94 |
| `FMC` | 18 | 2026-09-04 @ $13.30 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,yday_mover; ret5=+8.6; leftover $242.94 |
| `TARS` | 2 | 2026-09-04 @ $82.76 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $242.94 |
| `SCZM` | 23 | 2026-09-04 @ $10.50 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+9.3; leftover $242.94 |
| `PLAY` | 25 | 2026-09-04 @ $9.36 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+0.6; leftover $242.94 |
