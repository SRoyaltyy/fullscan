# Factor mine action — `union_coil_off_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ coil_off, no 🚨

Cash book **+4.00%** ($10,400) · signal-only (no cash/fees) was +5.43%. Starts YES **8/17**. Fills 76 · skips 153 · realized $+123.20.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $44.40.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | TPG, VOR | — | $37.44 | $10,677.03 | TPG×98, VOR×227 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $37.44 | TPG×98, VOR×227 | $10,751.77 | +74.74 | LDI, BTBT, ANGX, HYLN | — | $20.51 | $10,461.99 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1 | 09:30 open · cash $37.44 (unchanged overnight, no fees) · equity $10,751.77 vs prior close $10,677.03 (+74.74) because holdings re-marked: TPG×98 yday $54.62 → 09:30 $55.29 +65.66; VOR×227 yday $23.29 → 09:30 $23.33 +9.08 |
| 2026-08-17 | +2.25 | $20.51 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1 | $10,399.63 | -62.36 | DNN | — | $17.24 | $10,334.26 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 | 09:30 open · cash $20.51 (unchanged overnight, no fees) · equity $10,399.63 vs prior close $10,461.99 (-62.36) because holdings re-marked: TPG×98 yday $53.03 → 09:30 $52.67 -35.28; VOR×227 yday $23.03 → 09:30 $22.91 -27.24; LDI×4 yday $0.90 → 09:30 $0.91 +0.04; BTBT×3 yday $1.57 → 09:30 $1.52 -0.15; ANGX×1 yday $4.37 → 09:30 $4.60 +0.23; HYLN×1 yday $4.06 → 09:30 $4.10 +0.04 |
| 2026-08-18 | -6.20 | $17.24 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 | $10,290.79 | -43.47 | — | TPG, VOR | $10,265.49 | $10,285.13 | LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 | 09:30 open · cash $17.24 (unchanged overnight, no fees) · equity $10,290.79 vs prior close $10,334.26 (-43.47) because holdings re-marked: TPG×98 yday $51.77 → 09:30 $51.77 +0.00; VOR×227 yday $23.01 → 09:30 $22.82 -43.13; LDI×4 yday $0.88 → 09:30 $0.87 -0.02; BTBT×3 yday $1.60 → 09:30 $1.54 -0.18; ANGX×1 yday $4.71 → 09:30 $4.79 +0.08; HYLN×1 yday $4.09 → 09:30 $3.95 -0.14; DNN×1 yday $3.19 → 09:30 $3.11 -0.08 |
| 2026-08-19 | -7.20 | $10,265.49 | LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 | $10,285.12 | -0.01 | — | LDI, BTBT, ANGX, HYLN | $10,281.66 | $10,284.88 | DNN×1 | 09:30 open · cash $10,265.49 (unchanged overnight, no fees) · equity $10,285.12 vs prior close $10,285.13 (-0.01) because holdings re-marked: LDI×4 yday $0.86 → 09:30 $0.88 +0.09; BTBT×3 yday $1.45 → 09:30 $1.42 -0.09; ANGX×1 yday $4.85 → 09:30 $4.79 -0.06; HYLN×1 yday $3.86 → 09:30 $3.87 +0.01; DNN×1 yday $3.15 → 09:30 $3.19 +0.04 |
| 2026-08-20 | +1.12 | $10,281.66 | DNN×1 | $10,284.86 | -0.02 | AG, BHP, HDSN, IAG, KGC, NFGC, DNA, EXK | DNN | $32.35 | $10,364.53 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119 | 09:30 open · cash $10,281.66 (unchanged overnight, no fees) · equity $10,284.86 vs prior close $10,284.88 (-0.02) because holdings re-marked: DNN×1 yday $3.22 → 09:30 $3.20 -0.02 |
| 2026-08-21 | +3.25 | $32.35 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119 | $10,631.13 | +266.60 | BTBT, ORBS, HITI | — | $23.04 | $10,617.85 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119, BTBT×2, ORBS×4, HITI×1 | 09:30 open · cash $32.35 (unchanged overnight, no fees) · equity $10,631.13 vs prior close $10,364.53 (+266.60) because holdings re-marked: AG×62 yday $21.19 → 09:30 $21.90 +44.02; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; HDSN×222 yday $5.57 → 09:30 $5.67 +22.20; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×734 yday $1.75 → 09:30 $1.79 +29.36; DNA×172 yday $6.96 → 09:30 $7.09 +22.36; EXK×119 yday $10.97 → 09:30 $11.34 +44.03 |
| 2026-08-24 | -5.17 | $23.04 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119, BTBT×2, ORBS×4, HITI×1 | $10,735.01 | +117.16 | — | — | $23.04 | $10,554.97 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119, BTBT×2, ORBS×4, HITI×1 | 09:30 open · cash $23.04 (unchanged overnight, no fees) · equity $10,735.01 vs prior close $10,617.85 (+117.16) because holdings re-marked: AG×62 yday $21.09 → 09:30 $21.47 +23.56; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; HDSN×222 yday $5.63 → 09:30 $5.69 +13.32; IAG×65 yday $21.14 → 09:30 $21.44 +19.50; KGC×43 yday $32.76 → 09:30 $33.21 +19.35; NFGC×734 yday $1.84 → 09:30 $1.86 +14.68; DNA×172 yday $7.40 → 09:30 $7.26 -24.08; EXK×119 yday $10.62 → 09:30 $11.01 +46.41; BTBT×2 yday $1.53 → 09:30 $1.55 +0.04; ORBS×4 yday $0.88 → 09:30 $0.89 +0.04; HITI×1 yday $2.45 → 09:30 $2.45 +0.00 |
| 2026-08-25 | +1.80 | $23.04 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119, BTBT×2, ORBS×4, HITI×1 | $10,553.51 | -1.46 | CRMD, HCA, BMEA, ALIT, ZURA, JANX, KURA, EZPW | AG, BHP, HDSN, IAG, KGC, NFGC, DNA, EXK | $50.31 | $10,584.56 | BTBT×2, ORBS×4, HITI×1, CRMD×158, HCA×3, BMEA×811, ALIT×88, ZURA×206, JANX×70, KURA×98, EZPW×38 | 09:30 open · cash $23.04 (unchanged overnight, no fees) · equity $10,553.51 vs prior close $10,554.97 (-1.46) because holdings re-marked: AG×62 yday $20.57 → 09:30 $20.73 +9.92; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; HDSN×222 yday $5.57 → 09:30 $5.53 -8.88; IAG×65 yday $21.36 → 09:30 $21.63 +17.55; KGC×43 yday $32.47 → 09:30 $32.76 +12.47; NFGC×734 yday $1.90 → 09:30 $1.91 +7.34; DNA×172 yday $6.98 → 09:30 $6.82 -27.52; EXK×119 yday $10.74 → 09:30 $10.72 -2.38; BTBT×2 yday $1.56 → 09:30 $1.55 -0.02; ORBS×4 yday $0.85 → 09:30 $0.85 +0.00; HITI×1 yday $2.46 → 09:30 $2.46 +0.00 |
| 2026-08-26 | +2.02 | $50.31 | BTBT×2, ORBS×4, HITI×1, CRMD×158, HCA×3, BMEA×811, ALIT×88, ZURA×206, JANX×70, KURA×98, EZPW×38 | $10,584.56 | +0.00 | — | — | $50.31 | $10,501.05 | BTBT×2, ORBS×4, HITI×1, CRMD×158, HCA×3, BMEA×811, ALIT×88, ZURA×206, JANX×70, KURA×98, EZPW×38 | 09:30 open · cash $50.31 (unchanged overnight, no fees) · equity $10,584.56 vs prior close $10,584.56 (+0.00) because holdings re-marked: BTBT×2 yday $1.53 → 09:30 $1.53 +0.00; ORBS×4 yday $0.84 → 09:30 $0.84 +0.00; HITI×1 yday $2.46 → 09:30 $2.46 +0.00; CRMD×158 yday $8.28 → 09:30 $8.28 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; BMEA×811 yday $1.61 → 09:30 $1.61 +0.00; ALIT×88 yday $14.87 → 09:30 $14.87 +0.00; ZURA×206 yday $6.50 → 09:30 $6.50 +0.00; JANX×70 yday $18.99 → 09:30 $18.99 +0.00; KURA×98 yday $13.58 → 09:30 $13.58 +0.00; EZPW×38 yday $34.69 → 09:30 $34.69 +0.00 |
| 2026-08-27 | — | $50.31 | BTBT×2, ORBS×4, HITI×1, CRMD×158, HCA×3, BMEA×811, ALIT×88, ZURA×206, JANX×70, KURA×98, EZPW×38 | $10,682.91 | +181.86 | SLI | BTBT, ORBS, HITI | $53.73 | $10,438.22 | CRMD×158, HCA×3, BMEA×811, ALIT×88, ZURA×206, JANX×70, KURA×98, EZPW×38, SLI×2 | 09:30 open · cash $50.31 (unchanged overnight, no fees) · equity $10,682.91 vs prior close $10,501.05 (+181.86) because holdings re-marked: BTBT×2 yday $1.53 → 09:30 $1.53 +0.00; ORBS×4 yday $0.84 → 09:30 $0.80 -0.16; HITI×1 yday $2.46 → 09:30 $2.57 +0.11; CRMD×158 yday $8.28 → 09:30 $8.60 +50.56; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; BMEA×811 yday $1.61 → 09:30 $1.75 +113.54; ALIT×88 yday $14.87 → 09:30 $14.85 -1.76; ZURA×206 yday $6.50 → 09:30 $6.13 -76.22; JANX×70 yday $18.99 → 09:30 $18.59 -28.00; KURA×98 yday $13.58 → 09:30 $13.63 +4.90; EZPW×38 yday $34.69 → 09:30 $35.70 +38.38 |
| 2026-08-28 | +0.75 | $53.73 | CRMD×158, HCA×3, BMEA×811, ALIT×88, ZURA×206, JANX×70, KURA×98, EZPW×38, SLI×2 | $10,480.00 | +41.78 | RRC, CRK, ANF, BZ, BBWI, GENB, ADBT | CRMD, HCA, BMEA, ALIT, ZURA, JANX, KURA, EZPW | $72.62 | $10,307.67 | SLI×2, RRC×36, CRK×103, ANF×10, BZ×80, BBWI×79, GENB×87, ADBT×297 | 09:30 open · cash $53.73 (unchanged overnight, no fees) · equity $10,480.00 vs prior close $10,438.22 (+41.78) because holdings re-marked: CRMD×158 yday $8.39 → 09:30 $8.49 +15.80; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; BMEA×811 yday $1.71 → 09:30 $1.74 +24.33; ALIT×88 yday $14.33 → 09:30 $14.54 +18.48; ZURA×206 yday $5.99 → 09:30 $6.02 +6.18; JANX×70 yday $18.89 → 09:30 $19.00 +7.70; KURA×98 yday $13.06 → 09:30 $12.98 -7.84; EZPW×38 yday $33.90 → 09:30 $33.50 -15.20; SLI×2 yday $2.61 → 09:30 $2.60 -0.02 |
| 2026-08-31 | -5.85 | $72.62 | SLI×2, RRC×36, CRK×103, ANF×10, BZ×80, BBWI×79, GENB×87, ADBT×297 | $10,315.62 | +7.95 | — | — | $72.62 | $10,336.91 | SLI×2, RRC×36, CRK×103, ANF×10, BZ×80, BBWI×79, GENB×87, ADBT×297 | 09:30 open · cash $72.62 (unchanged overnight, no fees) · equity $10,315.62 vs prior close $10,307.67 (+7.95) because holdings re-marked: SLI×2 yday $2.64 → 09:30 $2.51 -0.26; RRC×36 yday $41.64 → 09:30 $41.11 -19.08; CRK×103 yday $14.62 → 09:30 $14.56 -6.18; ANF×10 yday $145.75 → 09:30 $148.67 +29.20; BZ×80 yday $18.00 → 09:30 $17.89 -8.80; BBWI×79 yday $18.65 → 09:30 $19.30 +51.35; GENB×87 yday $15.77 → 09:30 $15.33 -38.28; ADBT×297 yday $4.99 → 09:30 $4.99 +0.00 |
| 2026-09-01 | -6.30 | $72.62 | SLI×2, RRC×36, CRK×103, ANF×10, BZ×80, BBWI×79, GENB×87, ADBT×297 | $10,069.33 | -267.58 | — | SLI | $77.94 | $10,044.62 | RRC×36, CRK×103, ANF×10, BZ×80, BBWI×79, GENB×87, ADBT×297 | 09:30 open · cash $72.62 (unchanged overnight, no fees) · equity $10,069.33 vs prior close $10,336.91 (-267.58) because holdings re-marked: SLI×2 yday $2.51 → 09:30 $2.70 +0.38; RRC×36 yday $41.78 → 09:30 $41.32 -16.56; CRK×103 yday $14.51 → 09:30 $14.31 -20.60; ANF×10 yday $149.28 → 09:30 $142.47 -68.10; BZ×80 yday $17.90 → 09:30 $17.37 -42.40; BBWI×79 yday $19.22 → 09:30 $19.10 -9.48; GENB×87 yday $15.35 → 09:30 $15.51 +13.92; ADBT×297 yday $4.99 → 09:30 $4.57 -124.74 |
| 2026-09-02 | -3.83 | $77.94 | RRC×36, CRK×103, ANF×10, BZ×80, BBWI×79, GENB×87, ADBT×297 | $10,140.36 | +95.74 | — | RRC, CRK, ANF, BZ, BBWI, GENB, ADBT | $10,123.20 | $10,123.20 | — | 09:30 open · cash $77.94 (unchanged overnight, no fees) · equity $10,140.36 vs prior close $10,044.62 (+95.74) because holdings re-marked: RRC×36 yday $41.32 → 09:30 $41.94 +22.32; CRK×103 yday $14.90 → 09:30 $15.82 +94.76; ANF×10 yday $143.00 → 09:30 $142.00 -10.00; BZ×80 yday $17.17 → 09:30 $17.29 +9.60; BBWI×79 yday $19.10 → 09:30 $18.77 -26.07; GENB×87 yday $15.30 → 09:30 $15.12 -15.66; ADBT×297 yday $4.38 → 09:30 $4.45 +20.79 |
| 2026-09-03 | -0.90 | $10,123.20 | — | $10,123.20 | -0.00 | RVTY, GPRO, CRK, MMED, EIX, CLYM, CNXC, BMEA | — | $44.40 | $10,767.05 | RVTY×10, GPRO×1037, CRK×80, MMED×55, EIX×22, CLYM×85, CNXC×39, BMEA×702 | 09:30 open · cash $10,123.20 · no holdings · equity $10,123.20 vs prior close $10,123.20 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $44.40 | RVTY×10, GPRO×1037, CRK×80, MMED×55, EIX×22, CLYM×85, CNXC×39, BMEA×702 | $10,807.18 | +40.13 | — | — | $44.40 | $10,400.05 | RVTY×10, GPRO×1037, CRK×80, MMED×55, EIX×22, CLYM×85, CNXC×39, BMEA×702 | 09:30 open · cash $44.40 (unchanged overnight, no fees) · equity $10,807.18 vs prior close $10,767.05 (+40.13) because holdings re-marked: RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1037 yday $1.69 → 09:30 $1.78 +93.33; CRK×80 yday $15.54 → 09:30 $15.45 -7.20; MMED×55 yday $23.76 → 09:30 $23.88 +6.60; EIX×22 yday $55.19 → 09:30 $55.42 +5.06; CLYM×85 yday $15.05 → 09:30 $13.96 -92.65; CNXC×39 yday $32.37 → 09:30 $32.88 +19.89; BMEA×702 yday $1.93 → 09:30 $1.93 +0.00 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 98 | $50.62 | $2.28 | — | $5,036.64 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 227 | $22.01 | $2.93 | — | $37.44 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.44 | ▲ 09:30 equity $10,751.77 vs yday $10,677.03 (+74.74) | 09:30 open · cash $37.44 (unchanged overnight, no fees) · equity $10,751.77 vs prior close $10,677.03 (+74.74) because holdings re-marked: TPG×98 yday $54.62 → 09:30 $55.29 +65.66; VOR×227 yday $23.29 → 09:30 $23.33 +9.08 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 4 | $0.94 | $0.05 | — | $33.65 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 3 | $1.50 | $0.05 | — | $29.09 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 1 | $4.31 | $0.05 | — | $24.74 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 1 | $4.18 | $0.04 | — | $20.51 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.51 | ▼ 09:30 equity $10,399.63 vs yday $10,461.99 (-62.36) | 09:30 open · cash $20.51 (unchanged overnight, no fees) · equity $10,399.63 vs prior close $10,461.99 (-62.36) because holdings re-marked: TPG×98 yday $53.03 → 09:30 $52.67 -35.28; VOR×227 yday $23.03 → 09:30 $22.91 -27.24; LDI×4 yday $0.90 → 09:30 $0.91 +0.04; BTBT×3 yday $1.57 → 09:30 $1.52 -0.15; ANGX×1 yday $4.37 → 09:30 $4.60 +0.23; HYLN×1 yday $4.06 → 09:30 $4.10 +0.04 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $17.24 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $4.10 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.24 | ▼ 09:30 equity $10,290.79 vs yday $10,334.26 (-43.47) | 09:30 open · cash $17.24 (unchanged overnight, no fees) · equity $10,290.79 vs prior close $10,334.26 (-43.47) because holdings re-marked: TPG×98 yday $51.77 → 09:30 $51.77 +0.00; VOR×227 yday $23.01 → 09:30 $22.82 -43.13; LDI×4 yday $0.88 → 09:30 $0.87 -0.02; BTBT×3 yday $1.60 → 09:30 $1.54 -0.18; ANGX×1 yday $4.71 → 09:30 $4.79 +0.08; HYLN×1 yday $4.09 → 09:30 $3.95 -0.14; DNN×1 yday $3.19 → 09:30 $3.11 -0.08 | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 98 | $51.77 | $2.34 | $+107.76 | $5,088.36 | ▲ +107.76 after sell → book $10,288.45; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 227 | $22.82 | $3.01 | $+177.93 | $10,265.49 | ▲ +177.93 after sell → book $10,285.44; vs 09:30 mark -3.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,265.49 | ▼ 09:30 equity $10,285.12 vs yday $10,285.13 (-0.01) | 09:30 open · cash $10,265.49 (unchanged overnight, no fees) · equity $10,285.12 vs prior close $10,285.13 (-0.01) because holdings re-marked: LDI×4 yday $0.86 → 09:30 $0.88 +0.09; BTBT×3 yday $1.45 → 09:30 $1.42 -0.09; ANGX×1 yday $4.85 → 09:30 $4.79 -0.06; HYLN×1 yday $3.86 → 09:30 $3.87 +0.01; DNN×1 yday $3.15 → 09:30 $3.19 +0.04 | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 4 | $0.88 | $0.07 | $-0.34 | $10,268.94 | ▼ -0.34 after sell → book $10,285.05; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 3 | $1.42 | $0.07 | $-0.37 | $10,273.13 | ▼ -0.37 after sell → book $10,284.98; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 1 | $4.79 | $0.07 | $+0.36 | $10,277.85 | ▲ +0.36 after sell → book $10,284.91; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 1 | $3.87 | $0.06 | $-0.42 | $10,281.66 | ▼ -0.42 after sell → book $10,284.85; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,281.66 | ▼ 09:30 equity $10,284.86 vs yday $10,284.88 (-0.02) | 09:30 open · cash $10,281.66 (unchanged overnight, no fees) · equity $10,284.86 vs prior close $10,284.88 (-0.02) because holdings re-marked: DNN×1 yday $3.22 → 09:30 $3.20 -0.02 | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 1 | $3.20 | $0.06 | $-0.13 | $10,284.80 | ▼ -0.13 after sell → book $10,284.80; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,008.53 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1285.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,732.35 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1285.60 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 222 | $5.77 | $2.86 | — | $6,448.55 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1285.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $5,170.41 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1285.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $3,894.21 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1285.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 734 | $1.75 | $9.47 | — | $2,600.24 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1285.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 172 | $7.45 | $2.51 | — | $1,316.33 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1285.60 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 119 | $10.77 | $2.35 | — | $32.35 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1285.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.35 | ▲ 09:30 equity $10,631.13 vs yday $10,364.53 (+266.60) | 09:30 open · cash $32.35 (unchanged overnight, no fees) · equity $10,631.13 vs prior close $10,364.53 (+266.60) because holdings re-marked: AG×62 yday $21.19 → 09:30 $21.90 +44.02; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; HDSN×222 yday $5.57 → 09:30 $5.67 +22.20; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×734 yday $1.75 → 09:30 $1.79 +29.36; DNA×172 yday $6.96 → 09:30 $7.09 +22.36; EXK×119 yday $10.97 → 09:30 $11.34 +44.03 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 2 | $1.66 | $0.04 | — | $29.00 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $4.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 4 | $0.86 | $0.05 | — | $25.49 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $4.04 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 1 | $2.43 | $0.03 | — | $23.04 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $4.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.04 | ▲ 09:30 equity $10,735.01 vs yday $10,617.85 (+117.16) | 09:30 open · cash $23.04 (unchanged overnight, no fees) · equity $10,735.01 vs prior close $10,617.85 (+117.16) because holdings re-marked: AG×62 yday $21.09 → 09:30 $21.47 +23.56; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; HDSN×222 yday $5.63 → 09:30 $5.69 +13.32; IAG×65 yday $21.14 → 09:30 $21.44 +19.50; KGC×43 yday $32.76 → 09:30 $33.21 +19.35; NFGC×734 yday $1.84 → 09:30 $1.86 +14.68; DNA×172 yday $7.40 → 09:30 $7.26 -24.08; EXK×119 yday $10.62 → 09:30 $11.01 +46.41; BTBT×2 yday $1.53 → 09:30 $1.55 +0.04; ORBS×4 yday $0.88 → 09:30 $0.89 +0.04; HITI×1 yday $2.45 → 09:30 $2.45 +0.00 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.04 | ▼ 09:30 equity $10,553.51 vs yday $10,554.97 (-1.46) | 09:30 open · cash $23.04 (unchanged overnight, no fees) · equity $10,553.51 vs prior close $10,554.97 (-1.46) because holdings re-marked: AG×62 yday $20.57 → 09:30 $20.73 +9.92; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; HDSN×222 yday $5.57 → 09:30 $5.53 -8.88; IAG×65 yday $21.36 → 09:30 $21.63 +17.55; KGC×43 yday $32.47 → 09:30 $32.76 +12.47; NFGC×734 yday $1.90 → 09:30 $1.91 +7.34; DNA×172 yday $6.98 → 09:30 $6.82 -27.52; EXK×119 yday $10.74 → 09:30 $10.72 -2.38; BTBT×2 yday $1.56 → 09:30 $1.55 -0.02; ORBS×4 yday $0.85 → 09:30 $0.85 +0.00; HITI×1 yday $2.46 → 09:30 $2.46 +0.00 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.73 | $2.20 | $+6.79 | $1,306.10 | ▲ +6.79 after sell → book $10,551.31; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.95 | $2.05 | $+65.08 | $2,647.35 | ▲ +65.08 after sell → book $10,549.26; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 222 | $5.53 | $2.91 | $-59.05 | $3,872.10 | ▼ -59.05 after sell → book $10,546.35; vs 09:30 mark -2.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.63 | $2.21 | $+125.61 | $5,275.84 | ▲ +125.61 after sell → book $10,544.14; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.76 | $2.14 | $+130.33 | $6,682.38 | ▲ +130.33 after sell → book $10,542.00; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 734 | $1.91 | $9.60 | $+98.37 | $8,074.72 | ▲ +98.37 after sell → book $10,532.40; vs 09:30 mark -9.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 172 | $6.82 | $2.54 | $-113.41 | $9,245.21 | ▼ -113.41 after sell → book $10,529.85; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 119 | $10.72 | $2.38 | $-10.67 | $10,518.51 | ▼ -10.67 after sell → book $10,527.47; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 158 | $8.28 | $2.46 | — | $9,207.81 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1314.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $7,918.09 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+6.1; leftover $1314.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 811 | $1.62 | $10.46 | — | $6,593.81 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1314.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 88 | $14.86 | $2.25 | — | $5,283.88 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1314.81 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 206 | $6.38 | $2.66 | — | $3,966.94 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1314.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `JANX` | 70 | $18.52 | $2.20 | — | $2,668.34 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+7.9; leftover $1314.81 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 98 | $13.30 | $2.28 | — | $1,362.65 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+9.5; leftover $1314.81 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 38 | $34.48 | $2.10 | — | $50.31 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1314.81 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.31 | ▲ 09:30 equity $10,584.56 vs yday $10,584.56 (+0.00) | 09:30 open · cash $50.31 (unchanged overnight, no fees) · equity $10,584.56 vs prior close $10,584.56 (+0.00) because holdings re-marked: BTBT×2 yday $1.53 → 09:30 $1.53 +0.00; ORBS×4 yday $0.84 → 09:30 $0.84 +0.00; HITI×1 yday $2.46 → 09:30 $2.46 +0.00; CRMD×158 yday $8.28 → 09:30 $8.28 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; BMEA×811 yday $1.61 → 09:30 $1.61 +0.00; ALIT×88 yday $14.87 → 09:30 $14.87 +0.00; ZURA×206 yday $6.50 → 09:30 $6.50 +0.00; JANX×70 yday $18.99 → 09:30 $18.99 +0.00; KURA×98 yday $13.58 → 09:30 $13.58 +0.00; EZPW×38 yday $34.69 → 09:30 $34.69 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.31 | ▲ 09:30 equity $10,682.91 vs yday $10,501.05 (+181.86) | 09:30 open · cash $50.31 (unchanged overnight, no fees) · equity $10,682.91 vs prior close $10,501.05 (+181.86) because holdings re-marked: BTBT×2 yday $1.53 → 09:30 $1.53 +0.00; ORBS×4 yday $0.84 → 09:30 $0.80 -0.16; HITI×1 yday $2.46 → 09:30 $2.57 +0.11; CRMD×158 yday $8.28 → 09:30 $8.60 +50.56; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; BMEA×811 yday $1.61 → 09:30 $1.75 +113.54; ALIT×88 yday $14.87 → 09:30 $14.85 -1.76; ZURA×206 yday $6.50 → 09:30 $6.13 -76.22; JANX×70 yday $18.99 → 09:30 $18.59 -28.00; KURA×98 yday $13.58 → 09:30 $13.63 +4.90; EZPW×38 yday $34.69 → 09:30 $35.70 +38.38 | — |
| 2026-08-27 09:30 ET | **SELL** | `BTBT` | 2 | $1.53 | $0.06 | $-0.36 | $53.31 | ▼ -0.36 after sell → book $10,682.85; vs 09:30 mark -0.06 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ORBS` | 4 | $0.80 | $0.06 | $-0.37 | $56.45 | ▼ -0.37 after sell → book $10,682.79; vs 09:30 mark -0.06 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `HITI` | 1 | $2.57 | $0.05 | $+0.06 | $58.97 | ▲ +0.06 after sell → book $10,682.74; vs 09:30 mark -0.05 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 2 | $2.59 | $0.06 | — | $53.73 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+4.2; leftover $7.37 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.73 | ▲ 09:30 equity $10,480.00 vs yday $10,438.22 (+41.78) | 09:30 open · cash $53.73 (unchanged overnight, no fees) · equity $10,480.00 vs prior close $10,438.22 (+41.78) because holdings re-marked: CRMD×158 yday $8.39 → 09:30 $8.49 +15.80; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; BMEA×811 yday $1.71 → 09:30 $1.74 +24.33; ALIT×88 yday $14.33 → 09:30 $14.54 +18.48; ZURA×206 yday $5.99 → 09:30 $6.02 +6.18; JANX×70 yday $18.89 → 09:30 $19.00 +7.70; KURA×98 yday $13.06 → 09:30 $12.98 -7.84; EZPW×38 yday $33.90 → 09:30 $33.50 -15.20; SLI×2 yday $2.61 → 09:30 $2.60 -0.02 | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 158 | $8.49 | $2.50 | $+28.22 | $1,392.65 | ▲ +28.22 after sell → book $10,477.50; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-17.91 | $2,664.46 | ▼ -17.91 after sell → book $10,475.48; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 811 | $1.74 | $10.61 | $+76.25 | $4,065.00 | ▲ +76.25 after sell → book $10,464.88; vs 09:30 mark -10.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALIT` | 88 | $14.54 | $2.28 | $-32.69 | $5,342.24 | ▼ -32.69 after sell → book $10,462.60; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 206 | $6.02 | $2.70 | $-79.52 | $6,579.65 | ▼ -79.52 after sell → book $10,459.89; vs 09:30 mark -2.71 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `JANX` | 70 | $19.00 | $2.22 | $+29.18 | $7,907.43 | ▲ +29.18 after sell → book $10,457.67; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 98 | $12.98 | $2.31 | $-35.95 | $9,177.16 | ▼ -35.95 after sell → book $10,455.36; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 38 | $33.50 | $2.12 | $-41.47 | $10,448.04 | ▼ -41.47 after sell → book $10,453.24; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 36 | $41.44 | $2.10 | — | $8,954.10 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+1.8; leftover $1492.58 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 103 | $14.42 | $2.30 | — | $7,466.54 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+1.1; leftover $1492.58 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 10 | $144.70 | $2.02 | — | $6,017.52 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1492.58 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 80 | $18.50 | $2.23 | — | $4,535.29 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1492.58 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 79 | $18.68 | $2.23 | — | $3,057.34 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+0.2; leftover $1492.58 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GENB` | 87 | $17.10 | $2.25 | — | $1,567.39 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+3.1; leftover $1492.58 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADBT` | 297 | $5.02 | $3.83 | — | $72.62 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+0.0; leftover $1492.58 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.62 | ▲ 09:30 equity $10,315.62 vs yday $10,307.67 (+7.95) | 09:30 open · cash $72.62 (unchanged overnight, no fees) · equity $10,315.62 vs prior close $10,307.67 (+7.95) because holdings re-marked: SLI×2 yday $2.64 → 09:30 $2.51 -0.26; RRC×36 yday $41.64 → 09:30 $41.11 -19.08; CRK×103 yday $14.62 → 09:30 $14.56 -6.18; ANF×10 yday $145.75 → 09:30 $148.67 +29.20; BZ×80 yday $18.00 → 09:30 $17.89 -8.80; BBWI×79 yday $18.65 → 09:30 $19.30 +51.35; GENB×87 yday $15.77 → 09:30 $15.33 -38.28; ADBT×297 yday $4.99 → 09:30 $4.99 +0.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.62 | ▼ 09:30 equity $10,069.33 vs yday $10,336.91 (-267.58) | 09:30 open · cash $72.62 (unchanged overnight, no fees) · equity $10,069.33 vs prior close $10,336.91 (-267.58) because holdings re-marked: SLI×2 yday $2.51 → 09:30 $2.70 +0.38; RRC×36 yday $41.78 → 09:30 $41.32 -16.56; CRK×103 yday $14.51 → 09:30 $14.31 -20.60; ANF×10 yday $149.28 → 09:30 $142.47 -68.10; BZ×80 yday $17.90 → 09:30 $17.37 -42.40; BBWI×79 yday $19.22 → 09:30 $19.10 -9.48; GENB×87 yday $15.35 → 09:30 $15.51 +13.92; ADBT×297 yday $4.99 → 09:30 $4.57 -124.74 | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 2 | $2.70 | $0.08 | $+0.08 | $77.94 | ▲ +0.08 after sell → book $10,069.25; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $77.94 | ▲ 09:30 equity $10,140.36 vs yday $10,044.62 (+95.74) | 09:30 open · cash $77.94 (unchanged overnight, no fees) · equity $10,140.36 vs prior close $10,044.62 (+95.74) because holdings re-marked: RRC×36 yday $41.32 → 09:30 $41.94 +22.32; CRK×103 yday $14.90 → 09:30 $15.82 +94.76; ANF×10 yday $143.00 → 09:30 $142.00 -10.00; BZ×80 yday $17.17 → 09:30 $17.29 +9.60; BBWI×79 yday $19.10 → 09:30 $18.77 -26.07; GENB×87 yday $15.30 → 09:30 $15.12 -15.66; ADBT×297 yday $4.38 → 09:30 $4.45 +20.79 | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 36 | $41.94 | $2.12 | $+13.78 | $1,585.66 | ▲ +13.78 after sell → book $10,138.24; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 103 | $15.82 | $2.33 | $+139.57 | $3,212.79 | ▲ +139.57 after sell → book $10,135.91; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 10 | $142.00 | $2.04 | $-31.06 | $4,630.75 | ▼ -31.06 after sell → book $10,133.87; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 80 | $17.29 | $2.25 | $-101.28 | $6,011.70 | ▼ -101.28 after sell → book $10,131.62; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 79 | $18.77 | $2.25 | $+2.63 | $7,492.27 | ▲ +2.63 after sell → book $10,129.36; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GENB` | 87 | $15.12 | $2.28 | $-176.79 | $8,805.44 | ▼ -176.79 after sell → book $10,127.09; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADBT` | 297 | $4.45 | $3.89 | $-177.01 | $10,123.20 | ▼ -177.01 after sell → book $10,123.20; vs 09:30 mark -3.89 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,123.20 | ▲ 09:30 equity $10,123.20 vs yday $10,123.20 (-0.00) | 09:30 open · cash $10,123.20 · no holdings · equity $10,123.20 vs prior close $10,123.20 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $8,861.78 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1265.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1037 | $1.22 | $13.38 | — | $7,583.26 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1265.40 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 80 | $15.70 | $2.23 | — | $6,325.03 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1265.40 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 55 | $22.78 | $2.15 | — | $5,069.98 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1265.40 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $56.78 | $2.06 | — | $3,818.76 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+0.3; leftover $1265.40 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 85 | $14.79 | $2.25 | — | $2,559.36 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+5.8; leftover $1265.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 39 | $31.80 | $2.11 | — | $1,317.06 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.7; leftover $1265.40 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 702 | $1.80 | $9.06 | — | $44.40 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1265.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $44.40 | ▲ 09:30 equity $10,807.18 vs yday $10,767.05 (+40.13) | 09:30 open · cash $44.40 (unchanged overnight, no fees) · equity $10,807.18 vs prior close $10,767.05 (+40.13) because holdings re-marked: RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1037 yday $1.69 → 09:30 $1.78 +93.33; CRK×80 yday $15.54 → 09:30 $15.45 -7.20; MMED×55 yday $23.76 → 09:30 $23.88 +6.60; EIX×22 yday $55.19 → 09:30 $55.42 +5.06; CLYM×85 yday $15.05 → 09:30 $13.96 -92.65; CNXC×39 yday $32.37 → 09:30 $32.88 +19.89; BMEA×702 yday $1.93 → 09:30 $1.93 +0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 4.68 < 1 share @ 359.83 |
| 2026-08-14 | `SLG` | cash | leftover split 4.68 < 1 share @ 57.61 |
| 2026-08-14 | `WDC` | cash | leftover split 4.68 < 1 share @ 503.50 |
| 2026-08-14 | `ADUR` | cash | leftover split 4.68 < 1 share @ 16.50 |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 4.10 < 1 share @ 46.18 |
| 2026-08-17 | `OCC` | cash | leftover split 4.10 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 4.10 < 1 share @ 16.20 |
| 2026-08-17 | `NEWP` | cash | leftover split 4.10 < 1 share @ 6.94 |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MXL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRSP` | cash | leftover split 4.04 < 1 share @ 59.72 |
| 2026-08-21 | `EMBC` | cash | leftover split 4.04 < 1 share @ 5.43 |
| 2026-08-21 | `TXG` | cash | leftover split 4.04 < 1 share @ 64.39 |
| 2026-08-21 | `DXYZ` | cash | leftover split 4.04 < 1 share @ 34.89 |
| 2026-08-21 | `BEKE` | cash | leftover split 4.04 < 1 share @ 17.93 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HITI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABAT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `HITI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `BTBT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ORBS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `HITI` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `JANX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `BZ` | no_price | no 09:30 open |
| 2026-08-26 | `CNTN` | no_price | no 09:30 open |
| 2026-08-26 | `OSUR` | no_price | no 09:30 open |
| 2026-08-26 | `INO` | no_price | no 09:30 open |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `JANX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 7.37 < 1 share @ 40.72 |
| 2026-08-27 | `CRK` | cash | leftover split 7.37 < 1 share @ 14.09 |
| 2026-08-27 | `TX` | cash | leftover split 7.37 < 1 share @ 55.20 |
| 2026-08-27 | `DLO` | cash | leftover split 7.37 < 1 share @ 15.60 |
| 2026-08-27 | `GEN` | cash | leftover split 7.37 < 1 share @ 28.89 |
| 2026-08-27 | `MRVL` | cash | leftover split 7.37 < 1 share @ 240.00 |
| 2026-08-27 | `PGY` | cash | leftover split 7.37 < 1 share @ 21.97 |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GENB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ADBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `INO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DINO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DLO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GENB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ADBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OHI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `XLAB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HELP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SCZM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CNXC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BVS` | cash | leftover split 6.34 < 1 share @ 14.50 |
| 2026-09-04 | `MLYS` | cash | leftover split 6.34 < 1 share @ 29.15 |
| 2026-09-04 | `SGLD` | cash | leftover split 6.34 < 1 share @ 6.48 |
| 2026-09-04 | `FMC` | cash | leftover split 6.34 < 1 share @ 13.30 |
| 2026-09-04 | `TARS` | cash | leftover split 6.34 < 1 share @ 82.76 |
| 2026-09-04 | `SCZM` | cash | leftover split 6.34 < 1 share @ 10.50 |
| 2026-09-04 | `PLAY` | cash | leftover split 6.34 < 1 share @ 9.36 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RVTY` | 10 | 2026-09-03 @ $125.94 | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1265.40 |
| `GPRO` | 1037 | 2026-09-03 @ $1.22 | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1265.40 |
| `CRK` | 80 | 2026-09-03 @ $15.70 | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1265.40 |
| `MMED` | 55 | 2026-09-03 @ $22.78 | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1265.40 |
| `EIX` | 22 | 2026-09-03 @ $56.78 | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+0.3; leftover $1265.40 |
| `CLYM` | 85 | 2026-09-03 @ $14.79 | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+5.8; leftover $1265.40 |
| `CNXC` | 39 | 2026-09-03 @ $31.80 | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.7; leftover $1265.40 |
| `BMEA` | 702 | 2026-09-03 @ $1.80 | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1265.40 |
