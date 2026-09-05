# Factor mine action — `union_white_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ white, no 🚨

Cash book **+2.69%** ($10,269) · signal-only (no cash/fees) was +2.15%. Starts YES **6/17**. Fills 98 · skips 111 · realized $-19.61.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `zero_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $58.66.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | MARA, LDI, BTBT, ANGX, HYLN | — | $46.78 | $10,435.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2 | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 |
| 2026-08-17 | +2.25 | $46.78 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2 | $10,415.02 | -20.10 | TMC, DNN | — | $39.42 | $10,525.84 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1 | 09:30 open · cash $46.78 (unchanged overnight, no fees) · equity $10,415.02 vs prior close $10,435.12 (-20.10) because holdings re-marked: BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×13 yday $0.90 → 09:30 $0.91 +0.13; BTBT×8 yday $1.57 → 09:30 $1.52 -0.40; ANGX×2 yday $4.37 → 09:30 $4.60 +0.46; HYLN×2 yday $4.06 → 09:30 $4.10 +0.08 |
| 2026-08-18 | -6.20 | $39.42 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1 | $10,392.48 | -133.36 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $10,300.03 | $10,356.24 | MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1 | 09:30 open · cash $39.42 (unchanged overnight, no fees) · equity $10,392.48 vs prior close $10,525.84 (-133.36) because holdings re-marked: BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.60 → 09:30 $1.54 -0.48; ANGX×2 yday $4.71 → 09:30 $4.79 +0.16; HYLN×2 yday $4.09 → 09:30 $3.95 -0.28; TMC×1 yday $3.77 → 09:30 $3.72 -0.05; DNN×1 yday $3.19 → 09:30 $3.11 -0.08 |
| 2026-08-19 | -7.20 | $10,300.03 | MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1 | $10,356.18 | -0.06 | — | MARA, LDI, BTBT, ANGX, HYLN | $10,348.39 | $10,355.58 | TMC×1, DNN×1 | 09:30 open · cash $10,300.03 (unchanged overnight, no fees) · equity $10,356.18 vs prior close $10,356.24 (-0.06) because holdings re-marked: MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×13 yday $0.86 → 09:30 $0.88 +0.29; BTBT×8 yday $1.45 → 09:30 $1.42 -0.24; ANGX×2 yday $4.85 → 09:30 $4.79 -0.12; HYLN×2 yday $3.86 → 09:30 $3.87 +0.02; TMC×1 yday $3.92 → 09:30 $3.93 +0.01; DNN×1 yday $3.15 → 09:30 $3.19 +0.04 |
| 2026-08-20 | +1.12 | $10,348.39 | TMC×1, DNN×1 | $10,355.51 | -0.07 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | TMC, DNN | $209.64 | $10,569.98 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8 | 09:30 open · cash $10,348.39 (unchanged overnight, no fees) · equity $10,355.51 vs prior close $10,355.58 (-0.07) because holdings re-marked: TMC×1 yday $3.97 → 09:30 $3.92 -0.05; DNN×1 yday $3.22 → 09:30 $3.20 -0.02 |
| 2026-08-21 | +3.25 | $209.64 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8 | $10,845.87 | +275.89 | AUPH, ARCT, AUTL, CRDL, CYPH | — | $94.03 | $10,844.88 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | 09:30 open · cash $209.64 (unchanged overnight, no fees) · equity $10,845.87 vs prior close $10,569.98 (+275.89) because holdings re-marked: AG×62 yday $21.19 → 09:30 $21.90 +44.02; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×62 yday $21.11 → 09:30 $21.75 +39.68; HDSN×224 yday $5.57 → 09:30 $5.67 +22.40; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×739 yday $1.75 → 09:30 $1.79 +29.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 |
| 2026-08-24 | -5.17 | $94.03 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | $10,974.26 | +129.38 | — | — | $94.03 | $10,815.86 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | 09:30 open · cash $94.03 (unchanged overnight, no fees) · equity $10,974.26 vs prior close $10,844.88 (+129.38) because holdings re-marked: AG×62 yday $21.09 → 09:30 $21.47 +23.56; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×62 yday $20.97 → 09:30 $21.26 +17.98; HDSN×224 yday $5.63 → 09:30 $5.69 +13.44; IAG×65 yday $21.14 → 09:30 $21.44 +19.50; KGC×43 yday $32.76 → 09:30 $33.21 +19.35; NFGC×739 yday $1.84 → 09:30 $1.86 +14.78; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; CRDL×13 yday $1.86 → 09:30 $1.87 +0.13; CYPH×19 yday $1.42 → 09:30 $1.83 +7.79 |
| 2026-08-25 | +1.80 | $94.03 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | $10,885.72 | +69.86 | MOS, CRMD, BMEA, ALVO, ZURA, SUJA, DEFT | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $0.27 | $10,731.86 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×63, CRMD×185, BMEA×946, ALVO×293, ZURA×240, SUJA×174, DEFT×2374 | 09:30 open · cash $94.03 (unchanged overnight, no fees) · equity $10,885.72 vs prior close $10,815.86 (+69.86) because holdings re-marked: AG×62 yday $20.57 → 09:30 $20.73 +9.92; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×62 yday $20.49 → 09:30 $20.85 +22.32; HDSN×224 yday $5.57 → 09:30 $5.53 -8.96; IAG×65 yday $21.36 → 09:30 $21.63 +17.55; KGC×43 yday $32.47 → 09:30 $32.76 +12.47; NFGC×739 yday $1.90 → 09:30 $1.91 +7.39; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; CRDL×13 yday $1.80 → 09:30 $1.90 +1.30; CYPH×19 yday $1.64 → 09:30 $1.70 +1.14 |
| 2026-08-26 | +2.02 | $0.27 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×63, CRMD×185, BMEA×946, ALVO×293, ZURA×240, SUJA×174, DEFT×2374 | $10,731.86 | +0.00 | — | — | $0.27 | $10,811.66 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×63, CRMD×185, BMEA×946, ALVO×293, ZURA×240, SUJA×174, DEFT×2374 | 09:30 open · cash $0.27 (unchanged overnight, no fees) · equity $10,731.86 vs prior close $10,731.86 (+0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; CRDL×13 yday $1.90 → 09:30 $1.90 +0.00; CYPH×19 yday $1.64 → 09:30 $1.64 +0.00; MOS×63 yday $23.75 → 09:30 $23.75 +0.00; CRMD×185 yday $8.28 → 09:30 $8.28 +0.00; BMEA×946 yday $1.61 → 09:30 $1.61 +0.00; ALVO×293 yday $5.25 → 09:30 $5.25 +0.00; ZURA×240 yday $6.50 → 09:30 $6.50 +0.00; SUJA×174 yday $8.54 → 09:30 $8.54 +0.00; DEFT×2374 yday $0.62 → 09:30 $0.62 +0.00 |
| 2026-08-27 | — | $0.27 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×63, CRMD×185, BMEA×946, ALVO×293, ZURA×240, SUJA×174, DEFT×2374 | $10,928.48 | +116.82 | — | AUPH, ARCT, AUTL, CRDL, CYPH | $126.94 | $10,738.28 | MOS×63, CRMD×185, BMEA×946, ALVO×293, ZURA×240, SUJA×174, DEFT×2374 | 09:30 open · cash $0.27 (unchanged overnight, no fees) · equity $10,928.48 vs prior close $10,811.66 (+116.82) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; CRDL×13 yday $1.90 → 09:30 $2.03 +1.69; CYPH×19 yday $1.64 → 09:30 $1.60 -0.76; MOS×63 yday $23.75 → 09:30 $24.84 +68.67; CRMD×185 yday $8.28 → 09:30 $8.60 +59.20; BMEA×946 yday $1.61 → 09:30 $1.75 +132.44; ALVO×293 yday $5.25 → 09:30 $4.98 -79.11; ZURA×240 yday $6.50 → 09:30 $6.13 -88.80; SUJA×174 yday $8.54 → 09:30 $9.39 +147.90; DEFT×2374 yday $0.62 → 09:30 $0.60 -47.48 |
| 2026-08-28 | +0.75 | $126.94 | MOS×63, CRMD×185, BMEA×946, ALVO×293, ZURA×240, SUJA×174, DEFT×2374 | $10,792.01 | +53.73 | SMTC, SIMO, TTMI, KEYS, AVT, CGNX, COHR, LSCC | MOS, CRMD, BMEA, ALVO, ZURA, SUJA, DEFT | $736.76 | $10,557.34 | SMTC×8, SIMO×4, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11 | 09:30 open · cash $126.94 (unchanged overnight, no fees) · equity $10,792.01 vs prior close $10,738.28 (+53.73) because holdings re-marked: MOS×63 yday $24.16 → 09:30 $24.00 -10.08; CRMD×185 yday $8.39 → 09:30 $8.49 +18.50; BMEA×946 yday $1.71 → 09:30 $1.74 +28.38; ALVO×293 yday $4.91 → 09:30 $4.88 -8.79; ZURA×240 yday $5.99 → 09:30 $6.02 +7.20; SUJA×174 yday $9.44 → 09:30 $9.41 -5.22; DEFT×2374 yday $0.59 → 09:30 $0.60 +23.74 |
| 2026-08-31 | -5.85 | $736.76 | SMTC×8, SIMO×4, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11 | $10,136.65 | -420.69 | — | — | $736.76 | $10,155.63 | SMTC×8, SIMO×4, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11 | 09:30 open · cash $736.76 (unchanged overnight, no fees) · equity $10,136.65 vs prior close $10,557.34 (-420.69) because holdings re-marked: SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; SIMO×4 yday $255.08 → 09:30 $246.79 -33.16; TTMI×10 yday $124.73 → 09:30 $117.20 -75.30; KEYS×4 yday $325.82 → 09:30 $324.14 -6.72; AVT×14 yday $91.51 → 09:30 $88.63 -40.32; CGNX×21 yday $62.97 → 09:30 $60.31 -55.86; COHR×4 yday $295.39 → 09:30 $274.13 -85.04; LSCC×11 yday $120.47 → 09:30 $116.00 -49.17 |
| 2026-09-01 | -6.30 | $736.76 | SMTC×8, SIMO×4, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11 | $10,175.01 | +19.38 | — | — | $736.76 | $10,070.28 | SMTC×8, SIMO×4, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11 | 09:30 open · cash $736.76 (unchanged overnight, no fees) · equity $10,175.01 vs prior close $10,155.63 (+19.38) because holdings re-marked: SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; SIMO×4 yday $246.79 → 09:30 $247.53 +2.96; TTMI×10 yday $120.19 → 09:30 $119.79 -4.00; KEYS×4 yday $319.02 → 09:30 $323.71 +18.76; AVT×14 yday $88.63 → 09:30 $89.90 +17.78; CGNX×21 yday $60.31 → 09:30 $61.00 +14.49; COHR×4 yday $281.26 → 09:30 $277.23 -16.12; LSCC×11 yday $114.64 → 09:30 $113.97 -7.37 |
| 2026-09-02 | -3.83 | $736.76 | SMTC×8, SIMO×4, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11 | $9,996.68 | -73.60 | — | SMTC, SIMO, TTMI, KEYS, AVT, CGNX, COHR, LSCC | $9,980.37 | $9,980.37 | — | 09:30 open · cash $736.76 (unchanged overnight, no fees) · equity $9,996.68 vs prior close $10,070.28 (-73.60) because holdings re-marked: SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; SIMO×4 yday $241.20 → 09:30 $240.09 -4.44; TTMI×10 yday $116.94 → 09:30 $116.68 -2.60; KEYS×4 yday $322.70 → 09:30 $321.47 -4.92; AVT×14 yday $89.90 → 09:30 $88.58 -18.48; CGNX×21 yday $60.57 → 09:30 $59.72 -17.85; COHR×4 yday $272.07 → 09:30 $270.50 -6.28; LSCC×11 yday $113.97 → 09:30 $113.60 -4.07 |
| 2026-09-03 | -0.90 | $9,980.37 | — | $9,980.37 | +0.00 | ATRC, HRMY, CABA, VSTM, RVTY, MMED, SLN, CRDL | — | $133.71 | $10,349.62 | ATRC×25, HRMY×30, CABA×381, VSTM×162, RVTY×9, MMED×54, SLN×84, CRDL×577 | 09:30 open · cash $9,980.37 · no holdings · equity $9,980.37 vs prior close $9,980.37 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $133.71 | ATRC×25, HRMY×30, CABA×381, VSTM×162, RVTY×9, MMED×54, SLN×84, CRDL×577 | $10,414.33 | +64.71 | NVAX, BVS, IRD, OABI | — | $58.66 | $10,268.75 | ATRC×25, HRMY×30, CABA×381, VSTM×162, RVTY×9, MMED×54, SLN×84, CRDL×577, NVAX×2, BVS×1, IRD×4, OABI×4 | 09:30 open · cash $133.71 (unchanged overnight, no fees) · equity $10,414.33 vs prior close $10,349.62 (+64.71) because holdings re-marked: ATRC×25 yday $52.59 → 09:30 $52.88 +7.25; HRMY×30 yday $42.86 → 09:30 $42.93 +2.10; CABA×381 yday $3.57 → 09:30 $3.63 +22.86; VSTM×162 yday $8.02 → 09:30 $8.03 +1.62; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; MMED×54 yday $23.76 → 09:30 $23.88 +6.48; SLN×84 yday $14.79 → 09:30 $14.85 +5.04; CRDL×577 yday $2.17 → 09:30 $2.18 +5.77 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 2 | $4.31 | $0.09 | — | $55.23 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 2 | $4.18 | $0.09 | — | $46.78 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.78 | ▼ 09:30 equity $10,415.02 vs yday $10,435.12 (-20.10) | 09:30 open · cash $46.78 (unchanged overnight, no fees) · equity $10,415.02 vs prior close $10,435.12 (-20.10) because holdings re-marked: BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×13 yday $0.90 → 09:30 $0.91 +0.13; BTBT×8 yday $1.57 → 09:30 $1.52 -0.40; ANGX×2 yday $4.37 → 09:30 $4.60 +0.46; HYLN×2 yday $4.06 → 09:30 $4.10 +0.08 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $42.69 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $5.85 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $39.42 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+0.3; leftover $5.85 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.42 | ▼ 09:30 equity $10,392.48 vs yday $10,525.84 (-133.36) | 09:30 open · cash $39.42 (unchanged overnight, no fees) · equity $10,392.48 vs prior close $10,525.84 (-133.36) because holdings re-marked: BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.60 → 09:30 $1.54 -0.48; ANGX×2 yday $4.71 → 09:30 $4.79 +0.16; HYLN×2 yday $4.09 → 09:30 $3.95 -0.28; TMC×1 yday $3.77 → 09:30 $3.72 -0.05; DNN×1 yday $3.19 → 09:30 $3.11 -0.08 | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $1,237.35 | ▼ -0.12 after sell → book $10,390.41; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $2,411.37 | ▼ -69.50 after sell → book $10,388.31; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,651.77 | ▲ +23.38 after sell → book $10,386.23; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $4,881.69 | ▼ -14.65 after sell → book $10,384.15; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,221.31 | ▲ +97.12 after sell → book $10,381.81; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $7,388.88 | ▼ -83.63 after sell → book $10,379.68; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $9,127.72 | ▲ +471.89 after sell → book $10,359.50; vs 09:30 mark -20.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $10,300.03 | ▼ -66.33 after sell → book $10,357.33; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,300.03 | ▼ 09:30 equity $10,356.18 vs yday $10,356.24 (-0.06) | 09:30 open · cash $10,300.03 (unchanged overnight, no fees) · equity $10,356.18 vs prior close $10,356.24 (-0.06) because holdings re-marked: MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×13 yday $0.86 → 09:30 $0.88 +0.29; BTBT×8 yday $1.45 → 09:30 $1.42 -0.24; ANGX×2 yday $4.85 → 09:30 $4.79 -0.12; HYLN×2 yday $3.86 → 09:30 $3.87 +0.02; TMC×1 yday $3.92 → 09:30 $3.93 +0.01; DNN×1 yday $3.15 → 09:30 $3.19 +0.04 | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,308.83 | ▼ -0.31 after sell → book $10,356.07; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 13 | $0.88 | $0.17 | $-1.08 | $10,320.10 | ▼ -1.08 after sell → book $10,355.90; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 8 | $1.42 | $0.16 | $-0.94 | $10,331.30 | ▼ -0.94 after sell → book $10,355.74; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 2 | $4.79 | $0.12 | $+0.75 | $10,340.76 | ▲ +0.75 after sell → book $10,355.62; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 2 | $3.87 | $0.10 | $-0.81 | $10,348.39 | ▼ -0.81 after sell → book $10,355.51; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,348.39 | ▼ 09:30 equity $10,355.51 vs yday $10,355.58 (-0.07) | 09:30 open · cash $10,348.39 (unchanged overnight, no fees) · equity $10,355.51 vs prior close $10,355.58 (-0.07) because holdings re-marked: TMC×1 yday $3.97 → 09:30 $3.92 -0.05; DNN×1 yday $3.22 → 09:30 $3.20 -0.02 | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 1 | $3.92 | $0.06 | $-0.24 | $10,352.25 | ▼ -0.24 after sell → book $10,355.45; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 1 | $3.20 | $0.06 | $-0.13 | $10,355.40 | ▼ -0.13 after sell → book $10,355.40; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,079.12 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,802.95 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 62 | $20.65 | $2.18 | — | $6,520.47 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 224 | $5.77 | $2.89 | — | $5,225.10 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,946.97 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,670.76 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 739 | $1.75 | $9.53 | — | $1,367.97 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $209.64 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.64 | ▲ 09:30 equity $10,845.87 vs yday $10,569.98 (+275.89) | 09:30 open · cash $209.64 (unchanged overnight, no fees) · equity $10,845.87 vs prior close $10,569.98 (+275.89) because holdings re-marked: AG×62 yday $21.19 → 09:30 $21.90 +44.02; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×62 yday $21.11 → 09:30 $21.75 +39.68; HDSN×224 yday $5.57 → 09:30 $5.67 +22.40; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×739 yday $1.75 → 09:30 $1.79 +29.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $192.27 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $26.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $169.78 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $26.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $144.80 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $26.21 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $119.42 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $26.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $94.03 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $26.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.03 | ▲ 09:30 equity $10,974.26 vs yday $10,844.88 (+129.38) | 09:30 open · cash $94.03 (unchanged overnight, no fees) · equity $10,974.26 vs prior close $10,844.88 (+129.38) because holdings re-marked: AG×62 yday $21.09 → 09:30 $21.47 +23.56; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×62 yday $20.97 → 09:30 $21.26 +17.98; HDSN×224 yday $5.63 → 09:30 $5.69 +13.44; IAG×65 yday $21.14 → 09:30 $21.44 +19.50; KGC×43 yday $32.76 → 09:30 $33.21 +19.35; NFGC×739 yday $1.84 → 09:30 $1.86 +14.78; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; CRDL×13 yday $1.86 → 09:30 $1.87 +0.13; CYPH×19 yday $1.42 → 09:30 $1.83 +7.79 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.03 | ▲ 09:30 equity $10,885.72 vs yday $10,815.86 (+69.86) | 09:30 open · cash $94.03 (unchanged overnight, no fees) · equity $10,885.72 vs prior close $10,815.86 (+69.86) because holdings re-marked: AG×62 yday $20.57 → 09:30 $20.73 +9.92; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×62 yday $20.49 → 09:30 $20.85 +22.32; HDSN×224 yday $5.57 → 09:30 $5.53 -8.96; IAG×65 yday $21.36 → 09:30 $21.63 +17.55; KGC×43 yday $32.47 → 09:30 $32.76 +12.47; NFGC×739 yday $1.90 → 09:30 $1.91 +7.39; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; CRDL×13 yday $1.80 → 09:30 $1.90 +1.30; CYPH×19 yday $1.64 → 09:30 $1.70 +1.14 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.73 | $2.20 | $+6.79 | $1,377.10 | ▲ +6.79 after sell → book $10,883.53; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.95 | $2.05 | $+65.08 | $2,718.34 | ▲ +65.08 after sell → book $10,881.47; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 62 | $20.85 | $2.20 | $+8.03 | $4,008.85 | ▲ +8.03 after sell → book $10,879.28; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 224 | $5.53 | $2.94 | $-59.59 | $5,244.63 | ▼ -59.59 after sell → book $10,876.34; vs 09:30 mark -2.94 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.63 | $2.21 | $+125.61 | $6,648.37 | ▲ +125.61 after sell → book $10,874.13; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.76 | $2.14 | $+130.33 | $8,054.91 | ▲ +130.33 after sell → book $10,871.99; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 739 | $1.91 | $9.67 | $+99.04 | $9,456.74 | ▲ +99.04 after sell → book $10,862.33; vs 09:30 mark -9.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $10,734.70 | ▲ +119.63 after sell → book $10,860.29; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 63 | $24.00 | $2.18 | — | $9,220.52 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+13.0; leftover $1533.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 185 | $8.28 | $2.54 | — | $7,686.18 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1533.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 946 | $1.62 | $12.20 | — | $6,141.45 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1533.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 293 | $5.22 | $3.78 | — | $4,608.21 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1533.53 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 240 | $6.38 | $3.10 | — | $3,073.92 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1533.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 174 | $8.79 | $2.51 | — | $1,541.95 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $1533.53 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2374 | $0.64 | $22.32 | — | $0.27 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1533.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.27 | ▲ 09:30 equity $10,731.86 vs yday $10,731.86 (+0.00) | 09:30 open · cash $0.27 (unchanged overnight, no fees) · equity $10,731.86 vs prior close $10,731.86 (+0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; CRDL×13 yday $1.90 → 09:30 $1.90 +0.00; CYPH×19 yday $1.64 → 09:30 $1.64 +0.00; MOS×63 yday $23.75 → 09:30 $23.75 +0.00; CRMD×185 yday $8.28 → 09:30 $8.28 +0.00; BMEA×946 yday $1.61 → 09:30 $1.61 +0.00; ALVO×293 yday $5.25 → 09:30 $5.25 +0.00; ZURA×240 yday $6.50 → 09:30 $6.50 +0.00; SUJA×174 yday $8.54 → 09:30 $8.54 +0.00; DEFT×2374 yday $0.62 → 09:30 $0.62 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.27 | ▲ 09:30 equity $10,928.48 vs yday $10,811.66 (+116.82) | 09:30 open · cash $0.27 (unchanged overnight, no fees) · equity $10,928.48 vs prior close $10,811.66 (+116.82) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; CRDL×13 yday $1.90 → 09:30 $2.03 +1.69; CYPH×19 yday $1.64 → 09:30 $1.60 -0.76; MOS×63 yday $23.75 → 09:30 $24.84 +68.67; CRMD×185 yday $8.28 → 09:30 $8.60 +59.20; BMEA×946 yday $1.61 → 09:30 $1.75 +132.44; ALVO×293 yday $5.25 → 09:30 $4.98 -79.11; ZURA×240 yday $6.50 → 09:30 $6.13 -88.80; SUJA×174 yday $8.54 → 09:30 $9.39 +147.90; DEFT×2374 yday $0.62 → 09:30 $0.60 -47.48 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $16.68 | ▼ -0.96 after sell → book $10,928.29; vs 09:30 mark -0.19 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $47.05 | ▲ +7.88 after sell → book $10,927.96; vs 09:30 mark -0.33 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $70.86 | ▼ -1.17 after sell → book $10,927.67; vs 09:30 mark -0.29 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 13 | $2.03 | $0.32 | $+0.69 | $96.92 | ▲ +0.69 after sell → book $10,927.34; vs 09:30 mark -0.33 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 19 | $1.60 | $0.38 | $+4.63 | $126.94 | ▲ +4.63 after sell → book $10,926.96; vs 09:30 mark -0.38 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.94 | ▲ 09:30 equity $10,792.01 vs yday $10,738.28 (+53.73) | 09:30 open · cash $126.94 (unchanged overnight, no fees) · equity $10,792.01 vs prior close $10,738.28 (+53.73) because holdings re-marked: MOS×63 yday $24.16 → 09:30 $24.00 -10.08; CRMD×185 yday $8.39 → 09:30 $8.49 +18.50; BMEA×946 yday $1.71 → 09:30 $1.74 +28.38; ALVO×293 yday $4.91 → 09:30 $4.88 -8.79; ZURA×240 yday $5.99 → 09:30 $6.02 +7.20; SUJA×174 yday $9.44 → 09:30 $9.41 -5.22; DEFT×2374 yday $0.59 → 09:30 $0.60 +23.74 | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 63 | $24.00 | $2.20 | $-4.38 | $1,636.74 | ▼ -4.38 after sell → book $10,789.81; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 185 | $8.49 | $2.59 | $+33.72 | $3,204.80 | ▲ +33.72 after sell → book $10,787.22; vs 09:30 mark -2.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 946 | $1.74 | $12.37 | $+88.94 | $4,838.47 | ▲ +88.94 after sell → book $10,774.85; vs 09:30 mark -12.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 293 | $4.88 | $3.84 | $-107.24 | $6,264.47 | ▼ -107.24 after sell → book $10,771.01; vs 09:30 mark -3.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 240 | $6.02 | $3.15 | $-92.64 | $7,706.12 | ▼ -92.64 after sell → book $10,767.86; vs 09:30 mark -3.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUJA` | 174 | $9.41 | $2.55 | $+102.81 | $9,340.91 | ▲ +102.81 after sell → book $10,765.31; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DEFT` | 2374 | $0.60 | $21.77 | $-139.05 | $10,743.54 | ▼ -139.05 after sell → book $10,743.54; vs 09:30 mark -21.77 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $9,546.32 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1342.94 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 4 | $272.00 | $2.00 | — | $8,456.32 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer; ⚪; ret5=-3.9; leftover $1342.94 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $127.07 | $2.02 | — | $7,183.60 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $1342.94 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $323.82 | $2.00 | — | $5,886.32 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-11.7; leftover $1342.94 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.11 | $2.03 | — | $4,608.75 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-7.4; leftover $1342.94 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 21 | $62.80 | $2.05 | — | $3,287.89 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-7.8; leftover $1342.94 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $303.67 | $2.00 | — | $2,071.21 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-11.1; leftover $1342.94 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 11 | $121.13 | $2.02 | — | $736.76 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-9.8; leftover $1342.94 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $736.76 | ▼ 09:30 equity $10,136.65 vs yday $10,557.34 (-420.69) | 09:30 open · cash $736.76 (unchanged overnight, no fees) · equity $10,136.65 vs prior close $10,557.34 (-420.69) because holdings re-marked: SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; SIMO×4 yday $255.08 → 09:30 $246.79 -33.16; TTMI×10 yday $124.73 → 09:30 $117.20 -75.30; KEYS×4 yday $325.82 → 09:30 $324.14 -6.72; AVT×14 yday $91.51 → 09:30 $88.63 -40.32; CGNX×21 yday $62.97 → 09:30 $60.31 -55.86; COHR×4 yday $295.39 → 09:30 $274.13 -85.04; LSCC×11 yday $120.47 → 09:30 $116.00 -49.17 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $736.76 | ▲ 09:30 equity $10,175.01 vs yday $10,155.63 (+19.38) | 09:30 open · cash $736.76 (unchanged overnight, no fees) · equity $10,175.01 vs prior close $10,155.63 (+19.38) because holdings re-marked: SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; SIMO×4 yday $246.79 → 09:30 $247.53 +2.96; TTMI×10 yday $120.19 → 09:30 $119.79 -4.00; KEYS×4 yday $319.02 → 09:30 $323.71 +18.76; AVT×14 yday $88.63 → 09:30 $89.90 +17.78; CGNX×21 yday $60.31 → 09:30 $61.00 +14.49; COHR×4 yday $281.26 → 09:30 $277.23 -16.12; LSCC×11 yday $114.64 → 09:30 $113.97 -7.37 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $736.76 | ▼ 09:30 equity $9,996.68 vs yday $10,070.28 (-73.60) | 09:30 open · cash $736.76 (unchanged overnight, no fees) · equity $9,996.68 vs prior close $10,070.28 (-73.60) because holdings re-marked: SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; SIMO×4 yday $241.20 → 09:30 $240.09 -4.44; TTMI×10 yday $116.94 → 09:30 $116.68 -2.60; KEYS×4 yday $322.70 → 09:30 $321.47 -4.92; AVT×14 yday $89.90 → 09:30 $88.58 -18.48; CGNX×21 yday $60.57 → 09:30 $59.72 -17.85; COHR×4 yday $272.07 → 09:30 $270.50 -6.28; LSCC×11 yday $113.97 → 09:30 $113.60 -4.07 | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $127.63 | $2.03 | $-178.21 | $1,755.77 | ▼ -178.21 after sell → book $9,994.65; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SIMO` | 4 | $240.09 | $2.02 | $-131.66 | $2,714.10 | ▼ -131.66 after sell → book $9,992.62; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TTMI` | 10 | $116.68 | $2.04 | $-107.96 | $3,878.86 | ▼ -107.96 after sell → book $9,990.58; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 4 | $321.47 | $2.02 | $-13.42 | $5,162.72 | ▼ -13.42 after sell → book $9,988.56; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `AVT` | 14 | $88.58 | $2.05 | $-39.50 | $6,400.79 | ▼ -39.50 after sell → book $9,986.51; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CGNX` | 21 | $59.72 | $2.07 | $-68.81 | $7,652.84 | ▼ -68.81 after sell → book $9,984.44; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `COHR` | 4 | $270.50 | $2.02 | $-136.70 | $8,732.81 | ▼ -136.70 after sell → book $9,982.41; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LSCC` | 11 | $113.60 | $2.04 | $-86.90 | $9,980.37 | ▼ -86.90 after sell → book $9,980.37; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,980.37 | ▲ 09:30 equity $9,980.37 vs yday $9,980.37 (+0.00) | 09:30 open · cash $9,980.37 · no holdings · equity $9,980.37 vs prior close $9,980.37 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $49.76 | $2.06 | — | $8,734.31 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1247.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $41.31 | $2.08 | — | $7,492.93 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1247.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 381 | $3.27 | $4.91 | — | $6,242.14 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1247.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 162 | $7.70 | $2.48 | — | $4,992.27 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1247.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $3,856.79 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1247.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 54 | $22.78 | $2.15 | — | $2,624.52 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1247.55 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 84 | $14.70 | $2.24 | — | $1,387.47 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1247.55 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 577 | $2.16 | $7.44 | — | $133.71 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1247.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.71 | ▲ 09:30 equity $10,414.33 vs yday $10,349.62 (+64.71) | 09:30 open · cash $133.71 (unchanged overnight, no fees) · equity $10,414.33 vs prior close $10,349.62 (+64.71) because holdings re-marked: ATRC×25 yday $52.59 → 09:30 $52.88 +7.25; HRMY×30 yday $42.86 → 09:30 $42.93 +2.10; CABA×381 yday $3.57 → 09:30 $3.63 +22.86; VSTM×162 yday $8.02 → 09:30 $8.03 +1.62; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; MMED×54 yday $23.76 → 09:30 $23.88 +6.48; SLN×84 yday $14.79 → 09:30 $14.85 +5.04; CRDL×577 yday $2.17 → 09:30 $2.18 +5.77 | — |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 2 | $10.41 | $0.21 | — | $112.68 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $22.29 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 1 | $14.50 | $0.15 | — | $98.03 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $22.29 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 4 | $4.66 | $0.20 | — | $79.19 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $22.29 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 4 | $5.08 | $0.22 | — | $58.66 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $22.29 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `DAVE` | cash | leftover split 12.19 < 1 share @ 330.91 |
| 2026-08-14 | `BETR` | cash | leftover split 12.19 < 1 share @ 14.80 |
| 2026-08-14 | `WDC` | cash | leftover split 12.19 < 1 share @ 503.50 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TGB` | cash | leftover split 5.85 < 1 share @ 8.46 |
| 2026-08-17 | `CDNL` | cash | leftover split 5.85 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 5.85 < 1 share @ 9.12 |
| 2026-08-17 | `OCC` | cash | leftover split 5.85 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 5.85 < 1 share @ 16.20 |
| 2026-08-17 | `UMAC` | cash | leftover split 5.85 < 1 share @ 32.55 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 26.21 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 26.21 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 26.21 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AUPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `MOS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SUJA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-27 | `MOS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CGNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `COHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LSCC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CGNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `COHR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LSCC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 22.29 < 1 share @ 486.31 |
| 2026-09-04 | `MLYS` | cash | leftover split 22.29 < 1 share @ 29.15 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 25 | 2026-09-03 @ $49.76 | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1247.55 |
| `HRMY` | 30 | 2026-09-03 @ $41.31 | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1247.55 |
| `CABA` | 381 | 2026-09-03 @ $3.27 | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1247.55 |
| `VSTM` | 162 | 2026-09-03 @ $7.70 | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1247.55 |
| `RVTY` | 9 | 2026-09-03 @ $125.94 | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1247.55 |
| `MMED` | 54 | 2026-09-03 @ $22.78 | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1247.55 |
| `SLN` | 84 | 2026-09-03 @ $14.70 | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1247.55 |
| `CRDL` | 577 | 2026-09-03 @ $2.16 | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1247.55 |
| `NVAX` | 2 | 2026-09-04 @ $10.41 | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $22.29 |
| `BVS` | 1 | 2026-09-04 @ $14.50 | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $22.29 |
| `IRD` | 4 | 2026-09-04 @ $4.66 | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $22.29 |
| `OABI` | 4 | 2026-09-04 @ $5.08 | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $22.29 |
