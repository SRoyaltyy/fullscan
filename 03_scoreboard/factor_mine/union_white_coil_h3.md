# Factor mine action — `union_white_coil_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+4.51%** ($10,451) · signal-only (no cash/fees) was +1.34%. Starts YES **8/17**. Fills 94 · skips 107 · realized $+201.75.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `zero_red=True,ret_5_max=10.0,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $42.93.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, TPG, TGTX, SLS, HIMS, VOR | — | $134.73 | $10,203.79 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $134.73 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75 | $10,217.42 | +13.63 | MARA, LDI, BTBT, BETR, ANGX, HYLN | — | $47.87 | $10,222.63 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75, MARA×1, LDI×17, BTBT×11, BETR×1, ANGX×3, HYLN×4 | 09:30 open · cash $134.73 (unchanged overnight, no fees) · equity $10,217.42 vs prior close $10,203.79 (+13.63) because holdings re-marked: BTSG×27 yday $60.23 → 09:30 $59.65 -15.66; TPG×32 yday $54.62 → 09:30 $55.29 +21.44; TGTX×33 yday $47.94 → 09:30 $47.27 -22.11; SLS×142 yday $12.36 → 09:30 $12.40 +5.68; HIMS×56 yday $28.77 → 09:30 $29.15 +21.28; VOR×75 yday $23.29 → 09:30 $23.33 +3.00 |
| 2026-08-17 | +2.25 | $47.87 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75, MARA×1, LDI×17, BTBT×11, BETR×1, ANGX×3, HYLN×4 | $10,201.44 | -21.19 | TMC, DNN, MRLN | — | $36.71 | $10,220.48 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75, MARA×1, LDI×17, BTBT×11, BETR×1, ANGX×3, HYLN×4, TMC×1, DNN×1, MRLN×1 | 09:30 open · cash $47.87 (unchanged overnight, no fees) · equity $10,201.44 vs prior close $10,222.63 (-21.19) because holdings re-marked: BTSG×27 yday $61.71 → 09:30 $61.69 -0.54; TPG×32 yday $53.03 → 09:30 $52.67 -11.52; TGTX×33 yday $48.74 → 09:30 $48.74 +0.00; SLS×142 yday $12.78 → 09:30 $12.78 +0.00; HIMS×56 yday $28.15 → 09:30 $28.14 -0.56; VOR×75 yday $23.03 → 09:30 $22.91 -9.00; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×17 yday $0.90 → 09:30 $0.91 +0.17; BTBT×11 yday $1.57 → 09:30 $1.52 -0.55; BETR×1 yday $13.73 → 09:30 $13.67 -0.06; ANGX×3 yday $4.37 → 09:30 $4.60 +0.69; HYLN×4 yday $4.06 → 09:30 $4.10 +0.16 |
| 2026-08-18 | -6.20 | $36.71 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75, MARA×1, LDI×17, BTBT×11, BETR×1, ANGX×3, HYLN×4, TMC×1, DNN×1, MRLN×1 | $10,103.21 | -117.27 | — | BTSG, TPG, TGTX, SLS, HIMS, VOR | $9,995.22 | $10,088.14 | MARA×1, LDI×17, BTBT×11, BETR×1, ANGX×3, HYLN×4, TMC×1, DNN×1, MRLN×1 | 09:30 open · cash $36.71 (unchanged overnight, no fees) · equity $10,103.21 vs prior close $10,220.48 (-117.27) because holdings re-marked: BTSG×27 yday $60.38 → 09:30 $60.00 -10.26; TPG×32 yday $51.77 → 09:30 $51.77 +0.00; TGTX×33 yday $49.28 → 09:30 $49.28 +0.00; SLS×142 yday $13.00 → 09:30 $12.66 -48.28; HIMS×56 yday $28.61 → 09:30 $27.85 -42.56; VOR×75 yday $23.01 → 09:30 $22.82 -14.25; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×17 yday $0.88 → 09:30 $0.87 -0.09; BTBT×11 yday $1.60 → 09:30 $1.54 -0.66; BETR×1 yday $13.54 → 09:30 $13.21 -0.33; ANGX×3 yday $4.71 → 09:30 $4.79 +0.24; HYLN×4 yday $4.09 → 09:30 $3.95 -0.56; TMC×1 yday $3.77 → 09:30 $3.72 -0.05; DNN×1 yday $3.19 → 09:30 $3.11 -0.08; MRLN×1 yday $3.54 → 09:30 $3.50 -0.04 |
| 2026-08-19 | -7.20 | $9,995.22 | MARA×1, LDI×17, BTBT×11, BETR×1, ANGX×3, HYLN×4, TMC×1, DNN×1, MRLN×1 | $10,088.11 | -0.03 | — | MARA, LDI, BTBT, BETR, ANGX, HYLN | $10,076.54 | $10,087.04 | TMC×1, DNN×1, MRLN×1 | 09:30 open · cash $9,995.22 (unchanged overnight, no fees) · equity $10,088.11 vs prior close $10,088.14 (-0.03) because holdings re-marked: MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×17 yday $0.86 → 09:30 $0.88 +0.37; BTBT×11 yday $1.45 → 09:30 $1.42 -0.33; BETR×1 yday $13.05 → 09:30 $13.03 -0.02; ANGX×3 yday $4.85 → 09:30 $4.79 -0.18; HYLN×4 yday $3.86 → 09:30 $3.87 +0.04; TMC×1 yday $3.92 → 09:30 $3.93 +0.01; DNN×1 yday $3.15 → 09:30 $3.19 +0.04; MRLN×1 yday $3.31 → 09:30 $3.40 +0.09 |
| 2026-08-20 | +1.12 | $10,076.54 | TMC×1, DNN×1, MRLN×1 | $10,086.96 | -0.08 | AG, BHP, HDSN, IAG, KGC, NFGC, MRVI, SCZM | TMC, DNN, MRLN | $93.20 | $10,411.58 | AG×61, BHP×13, HDSN×218, IAG×64, KGC×42, NFGC×720, MRVI×170, SCZM×133 | 09:30 open · cash $10,076.54 (unchanged overnight, no fees) · equity $10,086.96 vs prior close $10,087.04 (-0.08) because holdings re-marked: TMC×1 yday $3.97 → 09:30 $3.92 -0.05; DNN×1 yday $3.22 → 09:30 $3.20 -0.02; MRLN×1 yday $3.31 → 09:30 $3.30 -0.02 |
| 2026-08-21 | +3.25 | $93.20 | AG×61, BHP×13, HDSN×218, IAG×64, KGC×42, NFGC×720, MRVI×170, SCZM×133 | $10,662.92 | +251.34 | EMBC, BEKE, HITI | — | $41.42 | $10,686.68 | AG×61, BHP×13, HDSN×218, IAG×64, KGC×42, NFGC×720, MRVI×170, SCZM×133, EMBC×3, BEKE×1, HITI×7 | 09:30 open · cash $93.20 (unchanged overnight, no fees) · equity $10,662.92 vs prior close $10,411.58 (+251.34) because holdings re-marked: AG×61 yday $21.19 → 09:30 $21.90 +43.31; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; HDSN×218 yday $5.57 → 09:30 $5.67 +21.80; IAG×64 yday $20.50 → 09:30 $21.17 +42.88; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×720 yday $1.75 → 09:30 $1.79 +28.80; MRVI×170 yday $8.26 → 09:30 $8.20 -10.20; SCZM×133 yday $9.76 → 09:30 $10.26 +66.50 |
| 2026-08-24 | -5.17 | $41.42 | AG×61, BHP×13, HDSN×218, IAG×64, KGC×42, NFGC×720, MRVI×170, SCZM×133, EMBC×3, BEKE×1, HITI×7 | $10,780.31 | +93.63 | — | — | $41.42 | $10,587.79 | AG×61, BHP×13, HDSN×218, IAG×64, KGC×42, NFGC×720, MRVI×170, SCZM×133, EMBC×3, BEKE×1, HITI×7 | 09:30 open · cash $41.42 (unchanged overnight, no fees) · equity $10,780.31 vs prior close $10,686.68 (+93.63) because holdings re-marked: AG×61 yday $21.09 → 09:30 $21.47 +23.18; BHP×13 yday $97.03 → 09:30 $97.34 +4.03; HDSN×218 yday $5.63 → 09:30 $5.69 +13.08; IAG×64 yday $21.14 → 09:30 $21.44 +19.20; KGC×42 yday $32.76 → 09:30 $33.21 +18.90; NFGC×720 yday $1.84 → 09:30 $1.86 +14.40; MRVI×170 yday $8.70 → 09:30 $8.59 -18.70; SCZM×133 yday $9.68 → 09:30 $9.82 +19.28; EMBC×3 yday $5.23 → 09:30 $5.21 -0.06; BEKE×1 yday $17.75 → 09:30 $18.06 +0.31; HITI×7 yday $2.45 → 09:30 $2.45 +0.00 |
| 2026-08-25 | +1.80 | $41.42 | AG×61, BHP×13, HDSN×218, IAG×64, KGC×42, NFGC×720, MRVI×170, SCZM×133, EMBC×3, BEKE×1, HITI×7 | $10,629.51 | +41.72 | CRMD, BMEA, ZURA, EZPW, BZ, VIPS, RHI, SUZ | AG, BHP, HDSN, IAG, KGC, NFGC, MRVI, SCZM | $34.48 | $10,693.70 | EMBC×3, BEKE×1, HITI×7, CRMD×159, BMEA×814, ZURA×206, EZPW×38, BZ×86, VIPS×94, RHI×29, SUZ×145 | 09:30 open · cash $41.42 (unchanged overnight, no fees) · equity $10,629.51 vs prior close $10,587.79 (+41.72) because holdings re-marked: AG×61 yday $20.57 → 09:30 $20.73 +9.76; BHP×13 yday $96.66 → 09:30 $95.95 -9.23; HDSN×218 yday $5.57 → 09:30 $5.53 -8.72; IAG×64 yday $21.36 → 09:30 $21.63 +17.28; KGC×42 yday $32.47 → 09:30 $32.76 +12.18; NFGC×720 yday $1.90 → 09:30 $1.91 +7.20; MRVI×170 yday $8.26 → 09:30 $8.31 +8.50; SCZM×133 yday $9.53 → 09:30 $9.57 +5.32; EMBC×3 yday $5.08 → 09:30 $4.99 -0.27; BEKE×1 yday $17.83 → 09:30 $17.53 -0.30; HITI×7 yday $2.46 → 09:30 $2.46 +0.00 |
| 2026-08-26 | +2.02 | $34.48 | EMBC×3, BEKE×1, HITI×7, CRMD×159, BMEA×814, ZURA×206, EZPW×38, BZ×86, VIPS×94, RHI×29, SUZ×145 | $10,693.70 | +0.00 | — | — | $34.48 | $10,576.93 | EMBC×3, BEKE×1, HITI×7, CRMD×159, BMEA×814, ZURA×206, EZPW×38, BZ×86, VIPS×94, RHI×29, SUZ×145 | 09:30 open · cash $34.48 (unchanged overnight, no fees) · equity $10,693.70 vs prior close $10,693.70 (+0.00) because holdings re-marked: EMBC×3 yday $5.19 → 09:30 $5.19 +0.00; BEKE×1 yday $17.59 → 09:30 $17.59 +0.00; HITI×7 yday $2.46 → 09:30 $2.46 +0.00; CRMD×159 yday $8.28 → 09:30 $8.28 +0.00; BMEA×814 yday $1.61 → 09:30 $1.61 +0.00; ZURA×206 yday $6.50 → 09:30 $6.50 +0.00; EZPW×38 yday $34.69 → 09:30 $34.69 +0.00; BZ×86 yday $16.32 → 09:30 $16.32 +0.00; VIPS×94 yday $13.83 → 09:30 $13.83 +0.00; RHI×29 yday $44.48 → 09:30 $44.48 +0.00; SUZ×145 yday $9.18 → 09:30 $9.18 +0.00 |
| 2026-08-27 | — | $34.48 | EMBC×3, BEKE×1, HITI×7, CRMD×159, BMEA×814, ZURA×206, EZPW×38, BZ×86, VIPS×94, RHI×29, SUZ×145 | $10,849.97 | +273.04 | — | EMBC, BEKE, HITI | $84.95 | $10,864.76 | CRMD×159, BMEA×814, ZURA×206, EZPW×38, BZ×86, VIPS×94, RHI×29, SUZ×145 | 09:30 open · cash $34.48 (unchanged overnight, no fees) · equity $10,849.97 vs prior close $10,576.93 (+273.04) because holdings re-marked: EMBC×3 yday $5.19 → 09:30 $4.98 -0.63; BEKE×1 yday $17.59 → 09:30 $18.14 +0.55; HITI×7 yday $2.46 → 09:30 $2.57 +0.77; CRMD×159 yday $8.28 → 09:30 $8.60 +50.88; BMEA×814 yday $1.61 → 09:30 $1.75 +113.96; ZURA×206 yday $6.50 → 09:30 $6.13 -76.22; EZPW×38 yday $34.69 → 09:30 $35.70 +38.38; BZ×86 yday $16.32 → 09:30 $16.77 +38.70; VIPS×94 yday $13.83 → 09:30 $14.00 +15.98; RHI×29 yday $44.48 → 09:30 $44.33 -4.35; SUZ×145 yday $9.18 → 09:30 $9.03 -21.75 |
| 2026-08-28 | +0.75 | $84.95 | CRMD×159, BMEA×814, ZURA×206, EZPW×38, BZ×86, VIPS×94, RHI×29, SUZ×145 | $10,846.83 | -17.93 | SMTC, TTMI, KEYS, AVT, CGNX, COHR, LSCC, MEI | CRMD, BMEA, ZURA, EZPW, BZ, VIPS, RHI, SUZ | $400.36 | $10,729.89 | SMTC×9, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11, MEI×78 | 09:30 open · cash $84.95 (unchanged overnight, no fees) · equity $10,846.83 vs prior close $10,864.76 (-17.93) because holdings re-marked: CRMD×159 yday $8.39 → 09:30 $8.49 +15.90; BMEA×814 yday $1.71 → 09:30 $1.74 +24.42; ZURA×206 yday $5.99 → 09:30 $6.02 +6.18; EZPW×38 yday $33.90 → 09:30 $33.50 -15.20; BZ×86 yday $18.84 → 09:30 $18.50 -29.24; VIPS×94 yday $14.08 → 09:30 $14.00 -7.52; RHI×29 yday $44.54 → 09:30 $44.41 -3.77; SUZ×145 yday $8.94 → 09:30 $8.88 -8.70 |
| 2026-08-31 | -5.85 | $400.36 | SMTC×9, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11, MEI×78 | $10,366.51 | -363.38 | — | — | $400.36 | $10,384.99 | SMTC×9, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11, MEI×78 | 09:30 open · cash $400.36 (unchanged overnight, no fees) · equity $10,366.51 vs prior close $10,729.89 (-363.38) because holdings re-marked: SMTC×9 yday $142.43 → 09:30 $133.04 -84.51; TTMI×10 yday $124.73 → 09:30 $117.20 -75.30; KEYS×4 yday $325.82 → 09:30 $324.14 -6.72; AVT×14 yday $91.51 → 09:30 $88.63 -40.32; CGNX×21 yday $62.97 → 09:30 $60.31 -55.86; COHR×4 yday $295.39 → 09:30 $274.13 -85.04; LSCC×11 yday $120.47 → 09:30 $116.00 -49.17; MEI×78 yday $17.78 → 09:30 $18.21 +33.54 |
| 2026-09-01 | -6.30 | $400.36 | SMTC×9, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11, MEI×78 | $10,402.08 | +17.09 | — | — | $400.36 | $10,320.52 | SMTC×9, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11, MEI×78 | 09:30 open · cash $400.36 (unchanged overnight, no fees) · equity $10,402.08 vs prior close $10,384.99 (+17.09) because holdings re-marked: SMTC×9 yday $132.54 → 09:30 $131.65 -8.01; TTMI×10 yday $120.19 → 09:30 $119.79 -4.00; KEYS×4 yday $319.02 → 09:30 $323.71 +18.76; AVT×14 yday $88.63 → 09:30 $89.90 +17.78; CGNX×21 yday $60.31 → 09:30 $61.00 +14.49; COHR×4 yday $281.26 → 09:30 $277.23 -16.12; LSCC×11 yday $114.64 → 09:30 $113.97 -7.37; MEI×78 yday $18.21 → 09:30 $18.23 +1.56 |
| 2026-09-02 | -3.83 | $400.36 | SMTC×9, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11, MEI×78 | $10,218.29 | -102.23 | — | SMTC, TTMI, KEYS, AVT, CGNX, COHR, LSCC, MEI | $10,201.75 | $10,201.75 | — | 09:30 open · cash $400.36 (unchanged overnight, no fees) · equity $10,218.29 vs prior close $10,320.52 (-102.23) because holdings re-marked: SMTC×9 yday $129.50 → 09:30 $127.63 -16.83; TTMI×10 yday $116.94 → 09:30 $116.68 -2.60; KEYS×4 yday $322.70 → 09:30 $321.47 -4.92; AVT×14 yday $89.90 → 09:30 $88.58 -18.48; CGNX×21 yday $60.57 → 09:30 $59.72 -17.85; COHR×4 yday $272.07 → 09:30 $270.50 -6.28; LSCC×11 yday $113.97 → 09:30 $113.60 -4.07; MEI×78 yday $18.23 → 09:30 $17.83 -31.20 |
| 2026-09-03 | -0.90 | $10,201.75 | — | $10,201.75 | +0.00 | HRMY, VSTM, RVTY, MMED, CRDL, BMEA, VIR, NEOV | — | $57.21 | $10,498.19 | HRMY×30, VSTM×165, RVTY×10, MMED×55, CRDL×590, BMEA×708, VIR×109, NEOV×348 | 09:30 open · cash $10,201.75 · no holdings · equity $10,201.75 vs prior close $10,201.75 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $57.21 | HRMY×30, VSTM×165, RVTY×10, MMED×55, CRDL×590, BMEA×708, VIR×109, NEOV×348 | $10,530.42 | +32.23 | LENZ, INO | — | $42.93 | $10,451.01 | HRMY×30, VSTM×165, RVTY×10, MMED×55, CRDL×590, BMEA×708, VIR×109, NEOV×348, LENZ×1, INO×6 | 09:30 open · cash $57.21 (unchanged overnight, no fees) · equity $10,530.42 vs prior close $10,498.19 (+32.23) because holdings re-marked: HRMY×30 yday $42.86 → 09:30 $42.93 +2.10; VSTM×165 yday $8.02 → 09:30 $8.03 +1.65; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; MMED×55 yday $23.76 → 09:30 $23.88 +6.60; CRDL×590 yday $2.17 → 09:30 $2.18 +5.90; BMEA×708 yday $1.93 → 09:30 $1.93 +0.00; VIR×109 yday $11.50 → 09:30 $11.54 +4.36; NEOV×348 yday $3.78 → 09:30 $3.77 -3.48 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 27 | $59.80 | $2.07 | — | $8,383.33 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $6,761.30 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 33 | $49.70 | $2.09 | — | $5,119.11 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 142 | $11.70 | $2.42 | — | $3,455.30 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 56 | $29.74 | $2.16 | — | $1,787.70 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 75 | $22.01 | $2.21 | — | $134.73 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $134.73 | ▲ 09:30 equity $10,217.42 vs yday $10,203.79 (+13.63) | 09:30 open · cash $134.73 (unchanged overnight, no fees) · equity $10,217.42 vs prior close $10,203.79 (+13.63) because holdings re-marked: BTSG×27 yday $60.23 → 09:30 $59.65 -15.66; TPG×32 yday $54.62 → 09:30 $55.29 +21.44; TGTX×33 yday $47.94 → 09:30 $47.27 -22.11; SLS×142 yday $12.36 → 09:30 $12.40 +5.68; HIMS×56 yday $28.77 → 09:30 $29.15 +21.28; VOR×75 yday $23.29 → 09:30 $23.33 +3.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $125.63 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-13.5; leftover $16.84 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 17 | $0.94 | $0.21 | — | $109.49 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $16.84 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 11 | $1.50 | $0.20 | — | $92.79 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $16.84 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 1 | $14.80 | $0.15 | — | $77.84 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-9.9; leftover $16.84 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 3 | $4.31 | $0.14 | — | $64.77 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $16.84 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 4 | $4.18 | $0.18 | — | $47.87 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $16.84 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.87 | ▼ 09:30 equity $10,201.44 vs yday $10,222.63 (-21.19) | 09:30 open · cash $47.87 (unchanged overnight, no fees) · equity $10,201.44 vs prior close $10,222.63 (-21.19) because holdings re-marked: BTSG×27 yday $61.71 → 09:30 $61.69 -0.54; TPG×32 yday $53.03 → 09:30 $52.67 -11.52; TGTX×33 yday $48.74 → 09:30 $48.74 +0.00; SLS×142 yday $12.78 → 09:30 $12.78 +0.00; HIMS×56 yday $28.15 → 09:30 $28.14 -0.56; VOR×75 yday $23.03 → 09:30 $22.91 -9.00; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×17 yday $0.90 → 09:30 $0.91 +0.17; BTBT×11 yday $1.57 → 09:30 $1.52 -0.55; BETR×1 yday $13.73 → 09:30 $13.67 -0.06; ANGX×3 yday $4.37 → 09:30 $4.60 +0.69; HYLN×4 yday $4.06 → 09:30 $4.10 +0.16 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $43.78 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-12.3; leftover $5.98 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $40.50 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $5.98 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `MRLN` | 1 | $3.75 | $0.04 | — | $36.71 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=-15.4; leftover $5.98 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.71 | ▼ 09:30 equity $10,103.21 vs yday $10,220.48 (-117.27) | 09:30 open · cash $36.71 (unchanged overnight, no fees) · equity $10,103.21 vs prior close $10,220.48 (-117.27) because holdings re-marked: BTSG×27 yday $60.38 → 09:30 $60.00 -10.26; TPG×32 yday $51.77 → 09:30 $51.77 +0.00; TGTX×33 yday $49.28 → 09:30 $49.28 +0.00; SLS×142 yday $13.00 → 09:30 $12.66 -48.28; HIMS×56 yday $28.61 → 09:30 $27.85 -42.56; VOR×75 yday $23.01 → 09:30 $22.82 -14.25; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×17 yday $0.88 → 09:30 $0.87 -0.09; BTBT×11 yday $1.60 → 09:30 $1.54 -0.66; BETR×1 yday $13.54 → 09:30 $13.21 -0.33; ANGX×3 yday $4.71 → 09:30 $4.79 +0.24; HYLN×4 yday $4.09 → 09:30 $3.95 -0.56; TMC×1 yday $3.77 → 09:30 $3.72 -0.05; DNN×1 yday $3.19 → 09:30 $3.11 -0.08; MRLN×1 yday $3.54 → 09:30 $3.50 -0.04 | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 27 | $60.00 | $2.09 | $+1.24 | $1,654.62 | ▲ +1.24 after sell → book $10,101.12; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 32 | $51.77 | $2.11 | $+32.50 | $3,309.15 | ▲ +32.50 after sell → book $10,099.01; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 33 | $49.28 | $2.11 | $-18.06 | $4,933.28 | ▼ -18.06 after sell → book $10,096.90; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 142 | $12.66 | $2.45 | $+131.45 | $6,728.55 | ▲ +131.45 after sell → book $10,094.44; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 56 | $27.85 | $2.18 | $-110.18 | $8,285.96 | ▼ -110.18 after sell → book $10,092.26; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 75 | $22.82 | $2.24 | $+56.29 | $9,995.22 | ▲ +56.29 after sell → book $10,090.02; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,995.22 | ▼ 09:30 equity $10,088.11 vs yday $10,088.14 (-0.03) | 09:30 open · cash $9,995.22 (unchanged overnight, no fees) · equity $10,088.11 vs prior close $10,088.14 (-0.03) because holdings re-marked: MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×17 yday $0.86 → 09:30 $0.88 +0.37; BTBT×11 yday $1.45 → 09:30 $1.42 -0.33; BETR×1 yday $13.05 → 09:30 $13.03 -0.02; ANGX×3 yday $4.85 → 09:30 $4.79 -0.18; HYLN×4 yday $3.86 → 09:30 $3.87 +0.04; TMC×1 yday $3.92 → 09:30 $3.93 +0.01; DNN×1 yday $3.15 → 09:30 $3.19 +0.04; MRLN×1 yday $3.31 → 09:30 $3.40 +0.09 | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,004.02 | ▼ -0.31 after sell → book $10,088.00; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 17 | $0.88 | $0.22 | $-1.40 | $10,018.76 | ▼ -1.40 after sell → book $10,087.78; vs 09:30 mark -0.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 11 | $1.42 | $0.21 | $-1.29 | $10,034.17 | ▼ -1.29 after sell → book $10,087.57; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BETR` | 1 | $13.03 | $0.15 | $-2.07 | $10,047.05 | ▼ -2.07 after sell → book $10,087.42; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 3 | $4.79 | $0.17 | $+1.13 | $10,061.25 | ▲ +1.13 after sell → book $10,087.25; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 4 | $3.87 | $0.19 | $-1.61 | $10,076.54 | ▼ -1.61 after sell → book $10,087.06; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,076.54 | ▼ 09:30 equity $10,086.96 vs yday $10,087.04 (-0.08) | 09:30 open · cash $10,076.54 (unchanged overnight, no fees) · equity $10,086.96 vs prior close $10,087.04 (-0.08) because holdings re-marked: TMC×1 yday $3.97 → 09:30 $3.92 -0.05; DNN×1 yday $3.22 → 09:30 $3.20 -0.02; MRLN×1 yday $3.31 → 09:30 $3.30 -0.02 | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 1 | $3.92 | $0.06 | $-0.24 | $10,080.40 | ▼ -0.24 after sell → book $10,086.90; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 1 | $3.20 | $0.06 | $-0.13 | $10,083.54 | ▼ -0.13 after sell → book $10,086.84; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `MRLN` | 1 | $3.30 | $0.06 | $-0.55 | $10,086.79 | ▼ -0.55 after sell → book $10,086.79; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,831.06 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1260.85 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,645.90 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1260.85 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 218 | $5.77 | $2.81 | — | $6,385.23 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1260.85 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $5,126.73 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1260.85 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $3,880.15 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1260.85 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 720 | $1.75 | $9.29 | — | $2,610.87 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1260.85 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 170 | $7.38 | $2.50 | — | $1,353.77 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1260.85 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 133 | $9.46 | $2.39 | — | $93.20 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1260.85 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.20 | ▲ 09:30 equity $10,662.92 vs yday $10,411.58 (+251.34) | 09:30 open · cash $93.20 (unchanged overnight, no fees) · equity $10,662.92 vs prior close $10,411.58 (+251.34) because holdings re-marked: AG×61 yday $21.19 → 09:30 $21.90 +43.31; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; HDSN×218 yday $5.57 → 09:30 $5.67 +21.80; IAG×64 yday $20.50 → 09:30 $21.17 +42.88; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×720 yday $1.75 → 09:30 $1.79 +28.80; MRVI×170 yday $8.26 → 09:30 $8.20 -10.20; SCZM×133 yday $9.76 → 09:30 $10.26 +66.50 | — |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 3 | $5.43 | $0.17 | — | $76.73 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $18.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 1 | $17.93 | $0.18 | — | $58.62 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $18.64 | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 7 | $2.43 | $0.19 | — | $41.42 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $18.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.42 | ▲ 09:30 equity $10,780.31 vs yday $10,686.68 (+93.63) | 09:30 open · cash $41.42 (unchanged overnight, no fees) · equity $10,780.31 vs prior close $10,686.68 (+93.63) because holdings re-marked: AG×61 yday $21.09 → 09:30 $21.47 +23.18; BHP×13 yday $97.03 → 09:30 $97.34 +4.03; HDSN×218 yday $5.63 → 09:30 $5.69 +13.08; IAG×64 yday $21.14 → 09:30 $21.44 +19.20; KGC×42 yday $32.76 → 09:30 $33.21 +18.90; NFGC×720 yday $1.84 → 09:30 $1.86 +14.40; MRVI×170 yday $8.70 → 09:30 $8.59 -18.70; SCZM×133 yday $9.68 → 09:30 $9.82 +19.28; EMBC×3 yday $5.23 → 09:30 $5.21 -0.06; BEKE×1 yday $17.75 → 09:30 $18.06 +0.31; HITI×7 yday $2.45 → 09:30 $2.45 +0.00 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.42 | ▲ 09:30 equity $10,629.51 vs yday $10,587.79 (+41.72) | 09:30 open · cash $41.42 (unchanged overnight, no fees) · equity $10,629.51 vs prior close $10,587.79 (+41.72) because holdings re-marked: AG×61 yday $20.57 → 09:30 $20.73 +9.76; BHP×13 yday $96.66 → 09:30 $95.95 -9.23; HDSN×218 yday $5.57 → 09:30 $5.53 -8.72; IAG×64 yday $21.36 → 09:30 $21.63 +17.28; KGC×42 yday $32.47 → 09:30 $32.76 +12.18; NFGC×720 yday $1.90 → 09:30 $1.91 +7.20; MRVI×170 yday $8.26 → 09:30 $8.31 +8.50; SCZM×133 yday $9.53 → 09:30 $9.57 +5.32; EMBC×3 yday $5.08 → 09:30 $4.99 -0.27; BEKE×1 yday $17.83 → 09:30 $17.53 -0.30; HITI×7 yday $2.46 → 09:30 $2.46 +0.00 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 61 | $20.73 | $2.19 | $+6.61 | $1,303.75 | ▲ +6.61 after sell → book $10,627.31; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $2,549.05 | ▲ +60.14 after sell → book $10,625.26; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 218 | $5.53 | $2.86 | $-57.99 | $3,751.74 | ▼ -57.99 after sell → book $10,622.41; vs 09:30 mark -2.85 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 64 | $21.63 | $2.20 | $+123.61 | $5,133.85 | ▲ +123.61 after sell → book $10,620.20; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 42 | $32.76 | $2.14 | $+127.21 | $6,507.63 | ▲ +127.21 after sell → book $10,618.06; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 720 | $1.91 | $9.42 | $+96.49 | $7,873.42 | ▲ +96.49 after sell → book $10,608.65; vs 09:30 mark -9.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRVI` | 170 | $8.31 | $2.54 | $+153.06 | $9,283.58 | ▲ +153.06 after sell → book $10,606.11; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SCZM` | 133 | $9.57 | $2.42 | $+9.82 | $10,553.97 | ▲ +9.82 after sell → book $10,603.69; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 159 | $8.28 | $2.47 | — | $9,234.98 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1319.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 814 | $1.62 | $10.50 | — | $7,905.80 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1319.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 206 | $6.38 | $2.66 | — | $6,588.86 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1319.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 38 | $34.48 | $2.10 | — | $5,276.52 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1319.25 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 86 | $15.34 | $2.25 | — | $3,955.03 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+2.8; leftover $1319.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `VIPS` | 94 | $13.91 | $2.27 | — | $2,645.22 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+2.5; leftover $1319.25 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 29 | $44.52 | $2.08 | — | $1,352.06 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+3.5; leftover $1319.25 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 145 | $9.07 | $2.42 | — | $34.48 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; ⚪; ret5=+8.3; leftover $1319.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.48 | ▲ 09:30 equity $10,693.70 vs yday $10,693.70 (+0.00) | 09:30 open · cash $34.48 (unchanged overnight, no fees) · equity $10,693.70 vs prior close $10,693.70 (+0.00) because holdings re-marked: EMBC×3 yday $5.19 → 09:30 $5.19 +0.00; BEKE×1 yday $17.59 → 09:30 $17.59 +0.00; HITI×7 yday $2.46 → 09:30 $2.46 +0.00; CRMD×159 yday $8.28 → 09:30 $8.28 +0.00; BMEA×814 yday $1.61 → 09:30 $1.61 +0.00; ZURA×206 yday $6.50 → 09:30 $6.50 +0.00; EZPW×38 yday $34.69 → 09:30 $34.69 +0.00; BZ×86 yday $16.32 → 09:30 $16.32 +0.00; VIPS×94 yday $13.83 → 09:30 $13.83 +0.00; RHI×29 yday $44.48 → 09:30 $44.48 +0.00; SUZ×145 yday $9.18 → 09:30 $9.18 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.48 | ▲ 09:30 equity $10,849.97 vs yday $10,576.93 (+273.04) | 09:30 open · cash $34.48 (unchanged overnight, no fees) · equity $10,849.97 vs prior close $10,576.93 (+273.04) because holdings re-marked: EMBC×3 yday $5.19 → 09:30 $4.98 -0.63; BEKE×1 yday $17.59 → 09:30 $18.14 +0.55; HITI×7 yday $2.46 → 09:30 $2.57 +0.77; CRMD×159 yday $8.28 → 09:30 $8.60 +50.88; BMEA×814 yday $1.61 → 09:30 $1.75 +113.96; ZURA×206 yday $6.50 → 09:30 $6.13 -76.22; EZPW×38 yday $34.69 → 09:30 $35.70 +38.38; BZ×86 yday $16.32 → 09:30 $16.77 +38.70; VIPS×94 yday $13.83 → 09:30 $14.00 +15.98; RHI×29 yday $44.48 → 09:30 $44.33 -4.35; SUZ×145 yday $9.18 → 09:30 $9.03 -21.75 | — |
| 2026-08-27 09:30 ET | **SELL** | `EMBC` | 3 | $4.98 | $0.18 | $-1.70 | $49.25 | ▼ -1.70 after sell → book $10,849.80; vs 09:30 mark -0.17 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `BEKE` | 1 | $18.14 | $0.20 | $-0.18 | $67.18 | ▼ -0.18 after sell → book $10,849.59; vs 09:30 mark -0.21 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `HITI` | 7 | $2.57 | $0.22 | $+0.57 | $84.95 | ▲ +0.57 after sell → book $10,849.37; vs 09:30 mark -0.22 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $84.95 | ▼ 09:30 equity $10,846.83 vs yday $10,864.76 (-17.93) | 09:30 open · cash $84.95 (unchanged overnight, no fees) · equity $10,846.83 vs prior close $10,864.76 (-17.93) because holdings re-marked: CRMD×159 yday $8.39 → 09:30 $8.49 +15.90; BMEA×814 yday $1.71 → 09:30 $1.74 +24.42; ZURA×206 yday $5.99 → 09:30 $6.02 +6.18; EZPW×38 yday $33.90 → 09:30 $33.50 -15.20; BZ×86 yday $18.84 → 09:30 $18.50 -29.24; VIPS×94 yday $14.08 → 09:30 $14.00 -7.52; RHI×29 yday $44.54 → 09:30 $44.41 -3.77; SUZ×145 yday $8.94 → 09:30 $8.88 -8.70 | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 159 | $8.49 | $2.50 | $+28.42 | $1,432.36 | ▲ +28.42 after sell → book $10,844.33; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 814 | $1.74 | $10.65 | $+76.53 | $2,838.07 | ▲ +76.53 after sell → book $10,833.68; vs 09:30 mark -10.65 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 206 | $6.02 | $2.70 | $-79.52 | $4,075.49 | ▼ -79.52 after sell → book $10,830.98; vs 09:30 mark -2.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 38 | $33.50 | $2.12 | $-41.47 | $5,346.36 | ▼ -41.47 after sell → book $10,828.85; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 86 | $18.50 | $2.27 | $+267.24 | $6,935.09 | ▲ +267.24 after sell → book $10,826.58; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `VIPS` | 94 | $14.00 | $2.30 | $+3.89 | $8,248.79 | ▲ +3.89 after sell → book $10,824.28; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RHI` | 29 | $44.41 | $2.10 | $-7.36 | $9,534.58 | ▼ -7.36 after sell → book $10,822.18; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUZ` | 145 | $8.88 | $2.46 | $-32.43 | $10,819.72 | ▼ -32.43 after sell → book $10,819.72; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $149.40 | $2.02 | — | $9,473.11 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1352.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $127.07 | $2.02 | — | $8,200.39 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $1352.47 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $323.82 | $2.00 | — | $6,903.10 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-11.7; leftover $1352.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.11 | $2.03 | — | $5,625.53 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-7.4; leftover $1352.47 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 21 | $62.80 | $2.05 | — | $4,304.68 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-7.8; leftover $1352.47 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $303.67 | $2.00 | — | $3,088.00 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-11.1; leftover $1352.47 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 11 | $121.13 | $2.02 | — | $1,753.54 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-9.8; leftover $1352.47 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MEI` | 78 | $17.32 | $2.22 | — | $400.36 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-16.7; leftover $1352.47 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $400.36 | ▼ 09:30 equity $10,366.51 vs yday $10,729.89 (-363.38) | 09:30 open · cash $400.36 (unchanged overnight, no fees) · equity $10,366.51 vs prior close $10,729.89 (-363.38) because holdings re-marked: SMTC×9 yday $142.43 → 09:30 $133.04 -84.51; TTMI×10 yday $124.73 → 09:30 $117.20 -75.30; KEYS×4 yday $325.82 → 09:30 $324.14 -6.72; AVT×14 yday $91.51 → 09:30 $88.63 -40.32; CGNX×21 yday $62.97 → 09:30 $60.31 -55.86; COHR×4 yday $295.39 → 09:30 $274.13 -85.04; LSCC×11 yday $120.47 → 09:30 $116.00 -49.17; MEI×78 yday $17.78 → 09:30 $18.21 +33.54 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $400.36 | ▲ 09:30 equity $10,402.08 vs yday $10,384.99 (+17.09) | 09:30 open · cash $400.36 (unchanged overnight, no fees) · equity $10,402.08 vs prior close $10,384.99 (+17.09) because holdings re-marked: SMTC×9 yday $132.54 → 09:30 $131.65 -8.01; TTMI×10 yday $120.19 → 09:30 $119.79 -4.00; KEYS×4 yday $319.02 → 09:30 $323.71 +18.76; AVT×14 yday $88.63 → 09:30 $89.90 +17.78; CGNX×21 yday $60.31 → 09:30 $61.00 +14.49; COHR×4 yday $281.26 → 09:30 $277.23 -16.12; LSCC×11 yday $114.64 → 09:30 $113.97 -7.37; MEI×78 yday $18.21 → 09:30 $18.23 +1.56 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $400.36 | ▼ 09:30 equity $10,218.29 vs yday $10,320.52 (-102.23) | 09:30 open · cash $400.36 (unchanged overnight, no fees) · equity $10,218.29 vs prior close $10,320.52 (-102.23) because holdings re-marked: SMTC×9 yday $129.50 → 09:30 $127.63 -16.83; TTMI×10 yday $116.94 → 09:30 $116.68 -2.60; KEYS×4 yday $322.70 → 09:30 $321.47 -4.92; AVT×14 yday $89.90 → 09:30 $88.58 -18.48; CGNX×21 yday $60.57 → 09:30 $59.72 -17.85; COHR×4 yday $272.07 → 09:30 $270.50 -6.28; LSCC×11 yday $113.97 → 09:30 $113.60 -4.07; MEI×78 yday $18.23 → 09:30 $17.83 -31.20 | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 9 | $127.63 | $2.04 | $-199.98 | $1,546.99 | ▼ -199.98 after sell → book $10,216.25; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TTMI` | 10 | $116.68 | $2.04 | $-107.96 | $2,711.75 | ▼ -107.96 after sell → book $10,214.21; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 4 | $321.47 | $2.02 | $-13.42 | $3,995.61 | ▼ -13.42 after sell → book $10,212.19; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `AVT` | 14 | $88.58 | $2.05 | $-39.50 | $5,233.68 | ▼ -39.50 after sell → book $10,210.14; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CGNX` | 21 | $59.72 | $2.07 | $-68.81 | $6,485.73 | ▼ -68.81 after sell → book $10,208.07; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `COHR` | 4 | $270.50 | $2.02 | $-136.70 | $7,565.70 | ▼ -136.70 after sell → book $10,206.04; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LSCC` | 11 | $113.60 | $2.04 | $-86.90 | $8,813.26 | ▼ -86.90 after sell → book $10,204.00; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MEI` | 78 | $17.83 | $2.25 | $+35.31 | $10,201.75 | ▲ +35.31 after sell → book $10,201.75; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,201.75 | ▲ 09:30 equity $10,201.75 vs yday $10,201.75 (+0.00) | 09:30 open · cash $10,201.75 · no holdings · equity $10,201.75 vs prior close $10,201.75 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $41.31 | $2.08 | — | $8,960.37 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1275.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 165 | $7.70 | $2.48 | — | $7,687.39 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1275.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $6,425.97 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1275.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 55 | $22.78 | $2.15 | — | $5,170.91 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1275.22 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 590 | $2.16 | $7.61 | — | $3,888.90 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1275.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 708 | $1.80 | $9.13 | — | $2,605.37 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1275.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 109 | $11.63 | $2.32 | — | $1,335.38 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+5.8; leftover $1275.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NEOV` | 348 | $3.66 | $4.49 | — | $57.21 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; 🔵; ⚪; ret5=-8.0; leftover $1275.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.21 | ▲ 09:30 equity $10,530.42 vs yday $10,498.19 (+32.23) | 09:30 open · cash $57.21 (unchanged overnight, no fees) · equity $10,530.42 vs prior close $10,498.19 (+32.23) because holdings re-marked: HRMY×30 yday $42.86 → 09:30 $42.93 +2.10; VSTM×165 yday $8.02 → 09:30 $8.03 +1.65; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; MMED×55 yday $23.76 → 09:30 $23.88 +6.60; CRDL×590 yday $2.17 → 09:30 $2.18 +5.90; BMEA×708 yday $1.93 → 09:30 $1.93 +0.00; VIR×109 yday $11.50 → 09:30 $11.54 +4.36; NEOV×348 yday $3.78 → 09:30 $3.77 -3.48 | — |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 1 | $5.90 | $0.06 | — | $51.25 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=-1.1; leftover $9.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `INO` | 6 | $1.37 | $0.10 | — | $42.93 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+8.3; leftover $9.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `DAVE` | cash | leftover split 16.84 < 1 share @ 330.91 |
| 2026-08-14 | `WDC` | cash | leftover split 16.84 < 1 share @ 503.50 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TGB` | cash | leftover split 5.98 < 1 share @ 8.46 |
| 2026-08-17 | `CDNL` | cash | leftover split 5.98 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 5.98 < 1 share @ 9.12 |
| 2026-08-17 | `OCC` | cash | leftover split 5.98 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 5.98 < 1 share @ 16.20 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `MRLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `MRLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRSP` | cash | leftover split 18.64 < 1 share @ 59.72 |
| 2026-08-21 | `TXG` | cash | leftover split 18.64 < 1 share @ 64.39 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EMBC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BEKE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HITI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `EMBC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BEKE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `HITI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `EMBC` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BEKE` | no_price | no 09:30 open — carry |
| 2026-08-26 | `HITI` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `VIPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RHI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SUZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `VIPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RHI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CGNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `COHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LSCC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MEI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CGNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `COHR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LSCC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MEI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NEOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BVS` | cash | leftover split 9.54 < 1 share @ 14.50 |
| 2026-09-04 | `DELL` | cash | leftover split 9.54 < 1 share @ 486.31 |
| 2026-09-04 | `MLYS` | cash | leftover split 9.54 < 1 share @ 29.15 |
| 2026-09-04 | `TARS` | cash | leftover split 9.54 < 1 share @ 82.76 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `HRMY` | 30 | 2026-09-03 @ $41.31 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1275.22 |
| `VSTM` | 165 | 2026-09-03 @ $7.70 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1275.22 |
| `RVTY` | 10 | 2026-09-03 @ $125.94 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1275.22 |
| `MMED` | 55 | 2026-09-03 @ $22.78 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1275.22 |
| `CRDL` | 590 | 2026-09-03 @ $2.16 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1275.22 |
| `BMEA` | 708 | 2026-09-03 @ $1.80 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1275.22 |
| `VIR` | 109 | 2026-09-03 @ $11.63 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+5.8; leftover $1275.22 |
| `NEOV` | 348 | 2026-09-03 @ $3.66 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; 🔵; ⚪; ret5=-8.0; leftover $1275.22 |
| `LENZ` | 1 | 2026-09-04 @ $5.90 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=-1.1; leftover $9.54 |
| `INO` | 6 | 2026-09-04 @ $1.37 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+8.3; leftover $9.54 |
