# Factor mine action — `union_white_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ white hold 5, no 🚨

Cash book **+6.18%** ($10,618) · signal-only (no cash/fees) was +14.63%. Starts YES **8/17**. Fills 97 · skips 187 · realized $+738.46.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `zero_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $228.82.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | MARA, LDI, BTBT, ANGX, HYLN | — | $46.78 | $10,435.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2 | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 |
| 2026-08-17 | +2.25 | $46.78 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2 | $10,415.02 | -20.10 | TMC, DNN | — | $39.42 | $10,525.84 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1 | 09:30 open · cash $46.78 (unchanged overnight, no fees) · equity $10,415.02 vs prior close $10,435.12 (-20.10) because holdings re-marked: BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×13 yday $0.90 → 09:30 $0.91 +0.13; BTBT×8 yday $1.57 → 09:30 $1.52 -0.40; ANGX×2 yday $4.37 → 09:30 $4.60 +0.46; HYLN×2 yday $4.06 → 09:30 $4.10 +0.08 |
| 2026-08-18 | -6.20 | $39.42 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1 | $10,392.48 | -133.36 | — | — | $39.42 | $10,572.87 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1 | 09:30 open · cash $39.42 (unchanged overnight, no fees) · equity $10,392.48 vs prior close $10,525.84 (-133.36) because holdings re-marked: BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.60 → 09:30 $1.54 -0.48; ANGX×2 yday $4.71 → 09:30 $4.79 +0.16; HYLN×2 yday $4.09 → 09:30 $3.95 -0.28; TMC×1 yday $3.77 → 09:30 $3.72 -0.05; DNN×1 yday $3.19 → 09:30 $3.11 -0.08 |
| 2026-08-19 | -7.20 | $39.42 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1 | $10,710.43 | +137.56 | — | — | $39.42 | $11,030.39 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1 | 09:30 open · cash $39.42 (unchanged overnight, no fees) · equity $10,710.43 vs prior close $10,572.87 (+137.56) because holdings re-marked: BTSG×20 yday $59.50 → 09:30 $60.15 +13.00; IREN×27 yday $42.00 → 09:30 $41.41 -15.80; TPG×24 yday $52.02 → 09:30 $52.26 +5.76; TGTX×25 yday $50.26 → 09:30 $51.62 +34.00; SLS×106 yday $13.10 → 09:30 $13.46 +38.16; HIMS×42 yday $27.39 → 09:30 $27.55 +6.72; INO×1543 yday $1.20 → 09:30 $1.22 +30.86; TNDM×53 yday $23.73 → 09:30 $24.20 +24.91; MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×13 yday $0.86 → 09:30 $0.88 +0.29; BTBT×8 yday $1.45 → 09:30 $1.42 -0.24; ANGX×2 yday $4.85 → 09:30 $4.79 -0.12; HYLN×2 yday $3.86 → 09:30 $3.87 +0.02; TMC×1 yday $3.92 → 09:30 $3.93 +0.01; DNN×1 yday $3.15 → 09:30 $3.19 +0.04 |
| 2026-08-20 | +1.12 | $39.42 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1 | $10,965.47 | -64.92 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $172.56 | $11,159.93 | MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1, AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9 | 09:30 open · cash $39.42 (unchanged overnight, no fees) · equity $10,965.47 vs prior close $11,030.39 (-64.92) because holdings re-marked: BTSG×20 yday $59.33 → 09:30 $58.64 -13.80; IREN×27 yday $42.84 → 09:30 $42.46 -10.26; TPG×24 yday $53.18 → 09:30 $53.06 -2.88; TGTX×25 yday $51.69 → 09:30 $51.65 -1.00; SLS×106 yday $13.85 → 09:30 $13.84 -1.06; HIMS×42 yday $31.09 → 09:30 $30.66 -18.06; INO×1543 yday $1.30 → 09:30 $1.30 +0.00; TNDM×53 yday $23.46 → 09:30 $23.11 -18.55; MARA×1 yday $9.65 → 09:30 $10.21 +0.56; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.40 → 09:30 $1.46 +0.44; ANGX×2 yday $4.60 → 09:30 $4.57 -0.06; HYLN×2 yday $3.67 → 09:30 $3.61 -0.12; TMC×1 yday $3.97 → 09:30 $3.92 -0.05; DNN×1 yday $3.22 → 09:30 $3.20 -0.02 |
| 2026-08-21 | +3.25 | $172.56 | MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1, AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9 | $11,453.24 | +293.31 | AUPH, ARCT, AUTL, CRDL, CYPH | MARA, LDI, BTBT, ANGX, HYLN | $101.08 | $11,452.47 | TMC×1, DNN×1, AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9, AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21 | 09:30 open · cash $172.56 (unchanged overnight, no fees) · equity $11,453.24 vs prior close $11,159.93 (+293.31) because holdings re-marked: MARA×1 yday $11.15 → 09:30 $11.70 +0.55; LDI×13 yday $0.87 → 09:30 $0.87 -0.04; BTBT×8 yday $1.59 → 09:30 $1.66 +0.52; ANGX×2 yday $4.37 → 09:30 $4.43 +0.12; HYLN×2 yday $3.37 → 09:30 $3.42 +0.10; TMC×1 yday $3.97 → 09:30 $4.10 +0.13; DNN×1 yday $3.14 → 09:30 $3.23 +0.09; AG×66 yday $21.19 → 09:30 $21.90 +46.86; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×65 yday $21.11 → 09:30 $21.75 +41.60; HDSN×235 yday $5.57 → 09:30 $5.67 +23.50; IAG×69 yday $20.50 → 09:30 $21.17 +46.23; KGC×45 yday $31.43 → 09:30 $32.17 +33.30; NFGC×776 yday $1.75 → 09:30 $1.79 +31.04; WPM×9 yday $150.25 → 09:30 $154.70 +40.05 |
| 2026-08-24 | -5.17 | $101.08 | TMC×1, DNN×1, AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9, AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21 | $11,589.48 | +137.01 | — | TMC, DNN | $109.03 | $11,422.02 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9, AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21 | 09:30 open · cash $101.08 (unchanged overnight, no fees) · equity $11,589.48 vs prior close $11,452.47 (+137.01) because holdings re-marked: TMC×1 yday $4.79 → 09:30 $4.57 -0.22; DNN×1 yday $3.50 → 09:30 $3.50 +0.00; AG×66 yday $21.09 → 09:30 $21.47 +25.08; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×65 yday $20.97 → 09:30 $21.26 +18.85; HDSN×235 yday $5.63 → 09:30 $5.69 +14.10; IAG×69 yday $21.14 → 09:30 $21.44 +20.70; KGC×45 yday $32.76 → 09:30 $33.21 +20.25; NFGC×776 yday $1.84 → 09:30 $1.86 +15.52; WPM×9 yday $157.78 → 09:30 $158.96 +10.62; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×11 yday $2.41 → 09:30 $2.36 -0.55; CRDL×14 yday $1.86 → 09:30 $1.87 +0.14; CYPH×21 yday $1.42 → 09:30 $1.83 +8.61 |
| 2026-08-25 | +1.80 | $109.03 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9, AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21 | $11,497.35 | +75.33 | CRMD, BMEA, ALVO, ZURA, SUJA, DEFT | — | $38.00 | $11,431.00 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9, AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21, CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24 | 09:30 open · cash $109.03 (unchanged overnight, no fees) · equity $11,497.35 vs prior close $11,422.02 (+75.33) because holdings re-marked: AG×66 yday $20.57 → 09:30 $20.73 +10.56; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×65 yday $20.49 → 09:30 $20.85 +23.40; HDSN×235 yday $5.57 → 09:30 $5.53 -9.40; IAG×69 yday $21.36 → 09:30 $21.63 +18.63; KGC×45 yday $32.47 → 09:30 $32.76 +13.05; NFGC×776 yday $1.90 → 09:30 $1.91 +7.76; WPM×9 yday $158.00 → 09:30 $160.00 +18.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×11 yday $2.38 → 09:30 $2.32 -0.66; CRDL×14 yday $1.80 → 09:30 $1.90 +1.40; CYPH×21 yday $1.64 → 09:30 $1.70 +1.26 |
| 2026-08-26 | +2.02 | $38.00 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9, AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21, CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24 | $11,431.00 | -0.00 | — | — | $38.00 | $11,496.53 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9, AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21, CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24 | 09:30 open · cash $38.00 (unchanged overnight, no fees) · equity $11,431.00 vs prior close $11,431.00 (-0.00) because holdings re-marked: AG×66 yday $20.68 → 09:30 $20.68 +0.00; BHP×14 yday $96.05 → 09:30 $96.05 +0.00; CDE×65 yday $20.71 → 09:30 $20.71 +0.00; HDSN×235 yday $5.49 → 09:30 $5.49 +0.00; IAG×69 yday $21.48 → 09:30 $21.48 +0.00; KGC×45 yday $32.55 → 09:30 $32.55 +0.00; NFGC×776 yday $1.90 → 09:30 $1.90 +0.00; WPM×9 yday $158.25 → 09:30 $158.25 +0.00; AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×11 yday $2.34 → 09:30 $2.34 +0.00; CRDL×14 yday $1.90 → 09:30 $1.90 +0.00; CYPH×21 yday $1.64 → 09:30 $1.64 +0.00; CRMD×1 yday $8.28 → 09:30 $8.28 +0.00; BMEA×9 yday $1.61 → 09:30 $1.61 +0.00; ALVO×2 yday $5.25 → 09:30 $5.25 +0.00; ZURA×2 yday $6.50 → 09:30 $6.50 +0.00; SUJA×1 yday $8.54 → 09:30 $8.54 +0.00; DEFT×24 yday $0.62 → 09:30 $0.62 +0.00 |
| 2026-08-27 | — | $38.00 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9, AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21, CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24 | $11,597.51 | +100.98 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $11,365.21 | $11,572.41 | AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21, CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24 | 09:30 open · cash $38.00 (unchanged overnight, no fees) · equity $11,597.51 vs prior close $11,496.53 (+100.98) because holdings re-marked: AG×66 yday $20.68 → 09:30 $20.63 -3.30; BHP×14 yday $96.05 → 09:30 $96.99 +13.16; CDE×65 yday $20.71 → 09:30 $21.00 +18.85; HDSN×235 yday $5.49 → 09:30 $5.51 +4.70; IAG×69 yday $21.48 → 09:30 $21.64 +11.04; KGC×45 yday $32.55 → 09:30 $32.90 +15.75; NFGC×776 yday $1.90 → 09:30 $2.00 +77.60; WPM×9 yday $158.25 → 09:30 $160.93 +24.12; AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×11 yday $2.34 → 09:30 $2.41 +0.77; CRDL×14 yday $1.90 → 09:30 $2.03 +1.82; CYPH×21 yday $1.64 → 09:30 $1.60 -0.84; CRMD×1 yday $8.28 → 09:30 $8.60 +0.32; BMEA×9 yday $1.61 → 09:30 $1.75 +1.26; ALVO×2 yday $5.25 → 09:30 $4.98 -0.54; ZURA×2 yday $6.50 → 09:30 $6.13 -0.74; SUJA×1 yday $8.54 → 09:30 $9.39 +0.85; DEFT×24 yday $0.62 → 09:30 $0.60 -0.48 |
| 2026-08-28 | +0.75 | $11,365.21 | AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21, CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24 | $11,574.45 | +2.04 | SMTC, SIMO, TTMI, KEYS, AVT, CGNX, COHR, LSCC | AUPH, ARCT, AUTL, CRDL, CYPH | $793.88 | $11,360.67 | CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24, SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11 | 09:30 open · cash $11,365.21 (unchanged overnight, no fees) · equity $11,574.45 vs prior close $11,572.41 (+2.04) because holdings re-marked: AUPH×1 yday $16.54 → 09:30 $16.47 -0.07; ARCT×2 yday $15.83 → 09:30 $15.74 -0.18; AUTL×11 yday $2.33 → 09:30 $2.32 -0.11; CRDL×14 yday $2.14 → 09:30 $2.09 -0.70; CYPH×21 yday $1.63 → 09:30 $1.75 +2.52; CRMD×1 yday $8.39 → 09:30 $8.49 +0.10; BMEA×9 yday $1.71 → 09:30 $1.74 +0.27; ALVO×2 yday $4.91 → 09:30 $4.88 -0.06; ZURA×2 yday $5.99 → 09:30 $6.02 +0.06; SUJA×1 yday $9.44 → 09:30 $9.41 -0.03; DEFT×24 yday $0.59 → 09:30 $0.60 +0.24 |
| 2026-08-31 | -5.85 | $793.88 | CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24, SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11 | $10,909.37 | -451.30 | — | — | $793.88 | $10,931.07 | CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24, SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11 | 09:30 open · cash $793.88 (unchanged overnight, no fees) · equity $10,909.37 vs prior close $11,360.67 (-451.30) because holdings re-marked: CRMD×1 yday $8.31 → 09:30 $8.29 -0.02; BMEA×9 yday $1.68 → 09:30 $1.71 +0.27; ALVO×2 yday $4.88 → 09:30 $4.98 +0.20; ZURA×2 yday $5.85 → 09:30 $5.51 -0.68; SUJA×1 yday $9.00 → 09:30 $10.09 +1.09; DEFT×24 yday $0.65 → 09:30 $0.62 -0.72; SMTC×9 yday $142.43 → 09:30 $133.04 -84.51; SIMO×5 yday $255.08 → 09:30 $246.79 -41.45; TTMI×11 yday $124.73 → 09:30 $117.20 -82.83; KEYS×4 yday $325.82 → 09:30 $324.14 -6.72; AVT×15 yday $91.51 → 09:30 $88.63 -43.20; CGNX×22 yday $62.97 → 09:30 $60.31 -58.52; COHR×4 yday $295.39 → 09:30 $274.13 -85.04; LSCC×11 yday $120.47 → 09:30 $116.00 -49.17 |
| 2026-09-01 | -6.30 | $793.88 | CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24, SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11 | $10,950.26 | +19.19 | — | CRMD, BMEA, ALVO, ZURA, SUJA, DEFT | $861.22 | $10,832.85 | SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11 | 09:30 open · cash $793.88 (unchanged overnight, no fees) · equity $10,950.26 vs prior close $10,931.07 (+19.19) because holdings re-marked: CRMD×1 yday $8.30 → 09:30 $8.26 -0.04; BMEA×9 yday $1.71 → 09:30 $1.65 -0.54; ALVO×2 yday $4.96 → 09:30 $5.24 +0.56; ZURA×2 yday $5.64 → 09:30 $5.60 -0.08; SUJA×1 yday $10.09 → 09:30 $9.31 -0.78; DEFT×24 yday $0.62 → 09:30 $0.59 -0.72; SMTC×9 yday $132.54 → 09:30 $131.65 -8.01; SIMO×5 yday $246.79 → 09:30 $247.53 +3.70; TTMI×11 yday $120.19 → 09:30 $119.79 -4.40; KEYS×4 yday $319.02 → 09:30 $323.71 +18.76; AVT×15 yday $88.63 → 09:30 $89.90 +19.05; CGNX×22 yday $60.31 → 09:30 $61.00 +15.18; COHR×4 yday $281.26 → 09:30 $277.23 -16.12; LSCC×11 yday $114.64 → 09:30 $113.97 -7.37 |
| 2026-09-02 | -3.83 | $861.22 | SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11 | $10,753.84 | -79.01 | — | — | $861.22 | $10,763.32 | SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11 | 09:30 open · cash $861.22 (unchanged overnight, no fees) · equity $10,753.84 vs prior close $10,832.85 (-79.01) because holdings re-marked: SMTC×9 yday $129.50 → 09:30 $127.63 -16.83; SIMO×5 yday $241.20 → 09:30 $240.09 -5.55; TTMI×11 yday $116.94 → 09:30 $116.68 -2.86; KEYS×4 yday $322.70 → 09:30 $321.47 -4.92; AVT×15 yday $89.90 → 09:30 $88.58 -19.80; CGNX×22 yday $60.57 → 09:30 $59.72 -18.70; COHR×4 yday $272.07 → 09:30 $270.50 -6.28; LSCC×11 yday $113.97 → 09:30 $113.60 -4.07 |
| 2026-09-03 | -0.90 | $861.22 | SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11 | $10,739.81 | -23.51 | ATRC, HRMY, CABA, VSTM, MMED, SLN, CRDL | — | $167.28 | $10,852.47 | SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11, ATRC×2, HRMY×2, CABA×32, VSTM×13, MMED×4, SLN×7, CRDL×49 | 09:30 open · cash $861.22 (unchanged overnight, no fees) · equity $10,739.81 vs prior close $10,763.32 (-23.51) because holdings re-marked: SMTC×9 yday $132.27 → 09:30 $133.00 +6.57; SIMO×5 yday $237.35 → 09:30 $235.71 -8.20; TTMI×11 yday $115.33 → 09:30 $114.22 -12.21; KEYS×4 yday $319.27 → 09:30 $318.04 -4.92; AVT×15 yday $89.39 → 09:30 $89.39 +0.00; CGNX×22 yday $60.08 → 09:30 $60.37 +6.38; COHR×4 yday $272.03 → 09:30 $268.12 -15.64; LSCC×11 yday $111.68 → 09:30 $112.09 +4.51 |
| 2026-09-04 | — | $167.28 | SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11, ATRC×2, HRMY×2, CABA×32, VSTM×13, MMED×4, SLN×7, CRDL×49 | $10,779.28 | -73.19 | NVAX, BVS, DELL, MLYS, IRD, OABI | SMTC, SIMO, TTMI, KEYS, AVT, CGNX, COHR, LSCC | $228.82 | $10,617.80 | ATRC×2, HRMY×2, CABA×32, VSTM×13, MMED×4, SLN×7, CRDL×49, NVAX×160, BVS×115, DELL×3, MLYS×57, IRD×359, OABI×329 | 09:30 open · cash $167.28 (unchanged overnight, no fees) · equity $10,779.28 vs prior close $10,852.47 (-73.19) because holdings re-marked: SMTC×9 yday $133.85 → 09:30 $133.10 -6.75; SIMO×5 yday $242.51 → 09:30 $239.05 -17.30; TTMI×11 yday $115.60 → 09:30 $115.21 -4.29; KEYS×4 yday $321.58 → 09:30 $319.09 -9.96; AVT×15 yday $89.99 → 09:30 $89.02 -14.55; CGNX×22 yday $59.94 → 09:30 $59.96 +0.44; COHR×4 yday $268.64 → 09:30 $266.86 -7.12; LSCC×11 yday $113.88 → 09:30 $112.26 -17.82; ATRC×2 yday $52.59 → 09:30 $52.88 +0.58; HRMY×2 yday $42.86 → 09:30 $42.93 +0.14; CABA×32 yday $3.57 → 09:30 $3.63 +1.92; VSTM×13 yday $8.02 → 09:30 $8.03 +0.13; MMED×4 yday $23.76 → 09:30 $23.88 +0.48; SLN×7 yday $14.79 → 09:30 $14.85 +0.42; CRDL×49 yday $2.17 → 09:30 $2.18 +0.49 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 2 | $4.31 | $0.09 | — | $55.23 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 2 | $4.18 | $0.09 | — | $46.78 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.78 | ▼ 09:30 equity $10,415.02 vs yday $10,435.12 (-20.10) | 09:30 open · cash $46.78 (unchanged overnight, no fees) · equity $10,415.02 vs prior close $10,435.12 (-20.10) because holdings re-marked: BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×13 yday $0.90 → 09:30 $0.91 +0.13; BTBT×8 yday $1.57 → 09:30 $1.52 -0.40; ANGX×2 yday $4.37 → 09:30 $4.60 +0.46; HYLN×2 yday $4.06 → 09:30 $4.10 +0.08 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $42.69 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $5.85 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $39.42 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+0.3; leftover $5.85 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.42 | ▼ 09:30 equity $10,392.48 vs yday $10,525.84 (-133.36) | 09:30 open · cash $39.42 (unchanged overnight, no fees) · equity $10,392.48 vs prior close $10,525.84 (-133.36) because holdings re-marked: BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.60 → 09:30 $1.54 -0.48; ANGX×2 yday $4.71 → 09:30 $4.79 +0.16; HYLN×2 yday $4.09 → 09:30 $3.95 -0.28; TMC×1 yday $3.77 → 09:30 $3.72 -0.05; DNN×1 yday $3.19 → 09:30 $3.11 -0.08 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.42 | ▲ 09:30 equity $10,710.43 vs yday $10,572.87 (+137.56) | 09:30 open · cash $39.42 (unchanged overnight, no fees) · equity $10,710.43 vs prior close $10,572.87 (+137.56) because holdings re-marked: BTSG×20 yday $59.50 → 09:30 $60.15 +13.00; IREN×27 yday $42.00 → 09:30 $41.41 -15.80; TPG×24 yday $52.02 → 09:30 $52.26 +5.76; TGTX×25 yday $50.26 → 09:30 $51.62 +34.00; SLS×106 yday $13.10 → 09:30 $13.46 +38.16; HIMS×42 yday $27.39 → 09:30 $27.55 +6.72; INO×1543 yday $1.20 → 09:30 $1.22 +30.86; TNDM×53 yday $23.73 → 09:30 $24.20 +24.91; MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×13 yday $0.86 → 09:30 $0.88 +0.29; BTBT×8 yday $1.45 → 09:30 $1.42 -0.24; ANGX×2 yday $4.85 → 09:30 $4.79 -0.12; HYLN×2 yday $3.86 → 09:30 $3.87 +0.02; TMC×1 yday $3.92 → 09:30 $3.93 +0.01; DNN×1 yday $3.15 → 09:30 $3.19 +0.04 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.42 | ▼ 09:30 equity $10,965.47 vs yday $11,030.39 (-64.92) | 09:30 open · cash $39.42 (unchanged overnight, no fees) · equity $10,965.47 vs prior close $11,030.39 (-64.92) because holdings re-marked: BTSG×20 yday $59.33 → 09:30 $58.64 -13.80; IREN×27 yday $42.84 → 09:30 $42.46 -10.26; TPG×24 yday $53.18 → 09:30 $53.06 -2.88; TGTX×25 yday $51.69 → 09:30 $51.65 -1.00; SLS×106 yday $13.85 → 09:30 $13.84 -1.06; HIMS×42 yday $31.09 → 09:30 $30.66 -18.06; INO×1543 yday $1.30 → 09:30 $1.30 +0.00; TNDM×53 yday $23.46 → 09:30 $23.11 -18.55; MARA×1 yday $9.65 → 09:30 $10.21 +0.56; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.40 → 09:30 $1.46 +0.44; ANGX×2 yday $4.60 → 09:30 $4.57 -0.06; HYLN×2 yday $3.67 → 09:30 $3.61 -0.12; TMC×1 yday $3.97 → 09:30 $3.92 -0.05; DNN×1 yday $3.22 → 09:30 $3.20 -0.02 | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 20 | $58.64 | $2.07 | $-27.32 | $1,210.15 | ▼ -27.32 after sell → book $10,963.40; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 27 | $42.46 | $2.09 | $-99.20 | $2,354.47 | ▼ -99.20 after sell → book $10,961.31; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 24 | $53.06 | $2.08 | $+54.34 | $3,625.83 | ▲ +54.34 after sell → book $10,959.23; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 25 | $51.65 | $2.09 | $+44.60 | $4,915.00 | ▲ +44.60 after sell → book $10,957.14; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 106 | $13.84 | $2.34 | $+222.19 | $6,379.70 | ▲ +222.19 after sell → book $10,954.80; vs 09:30 mark -2.34 | dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 42 | $30.66 | $2.14 | $+34.39 | $7,665.28 | ▲ +34.39 after sell → book $10,952.67; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1543 | $1.30 | $20.18 | $+718.77 | $9,651.01 | ▲ +718.77 after sell → book $10,932.49; vs 09:30 mark -20.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 53 | $23.11 | $2.17 | $-15.98 | $10,873.67 | ▼ -15.98 after sell → book $10,930.32; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 66 | $20.55 | $2.19 | — | $9,515.18 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1359.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,239.01 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1359.21 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 65 | $20.65 | $2.19 | — | $6,894.57 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1359.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 235 | $5.77 | $3.03 | — | $5,535.59 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1359.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 69 | $19.63 | $2.20 | — | $4,178.92 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1359.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 45 | $29.63 | $2.12 | — | $2,843.45 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1359.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 776 | $1.75 | $10.01 | — | $1,475.44 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1359.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $172.56 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1359.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $172.56 | ▲ 09:30 equity $11,453.24 vs yday $11,159.93 (+293.31) | 09:30 open · cash $172.56 (unchanged overnight, no fees) · equity $11,453.24 vs prior close $11,159.93 (+293.31) because holdings re-marked: MARA×1 yday $11.15 → 09:30 $11.70 +0.55; LDI×13 yday $0.87 → 09:30 $0.87 -0.04; BTBT×8 yday $1.59 → 09:30 $1.66 +0.52; ANGX×2 yday $4.37 → 09:30 $4.43 +0.12; HYLN×2 yday $3.37 → 09:30 $3.42 +0.10; TMC×1 yday $3.97 → 09:30 $4.10 +0.13; DNN×1 yday $3.14 → 09:30 $3.23 +0.09; AG×66 yday $21.19 → 09:30 $21.90 +46.86; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×65 yday $21.11 → 09:30 $21.75 +41.60; HDSN×235 yday $5.57 → 09:30 $5.67 +23.50; IAG×69 yday $20.50 → 09:30 $21.17 +46.23; KGC×45 yday $31.43 → 09:30 $32.17 +33.30; NFGC×776 yday $1.75 → 09:30 $1.79 +31.04; WPM×9 yday $150.25 → 09:30 $154.70 +40.05 | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $184.12 | ▲ +2.46 after sell → book $11,453.10; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 13 | $0.87 | $0.17 | $-1.24 | $195.22 | ▼ -1.24 after sell → book $11,452.93; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 8 | $1.66 | $0.18 | $+0.96 | $208.32 | ▲ +0.96 after sell → book $11,452.75; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 2 | $4.43 | $0.11 | $+0.03 | $217.07 | ▲ +0.03 after sell → book $11,452.64; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 2 | $3.42 | $0.09 | $-1.70 | $223.81 | ▼ -1.70 after sell → book $11,452.54; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $206.44 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $27.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $183.95 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $27.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 11 | $2.47 | $0.30 | — | $156.48 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $27.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 14 | $1.93 | $0.31 | — | $129.14 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $27.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 21 | $1.32 | $0.34 | — | $101.08 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $27.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $101.08 | ▲ 09:30 equity $11,589.48 vs yday $11,452.47 (+137.01) | 09:30 open · cash $101.08 (unchanged overnight, no fees) · equity $11,589.48 vs prior close $11,452.47 (+137.01) because holdings re-marked: TMC×1 yday $4.79 → 09:30 $4.57 -0.22; DNN×1 yday $3.50 → 09:30 $3.50 +0.00; AG×66 yday $21.09 → 09:30 $21.47 +25.08; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×65 yday $20.97 → 09:30 $21.26 +18.85; HDSN×235 yday $5.63 → 09:30 $5.69 +14.10; IAG×69 yday $21.14 → 09:30 $21.44 +20.70; KGC×45 yday $32.76 → 09:30 $33.21 +20.25; NFGC×776 yday $1.84 → 09:30 $1.86 +15.52; WPM×9 yday $157.78 → 09:30 $158.96 +10.62; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×11 yday $2.41 → 09:30 $2.36 -0.55; CRDL×14 yday $1.86 → 09:30 $1.87 +0.14; CYPH×21 yday $1.42 → 09:30 $1.83 +8.61 | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 1 | $4.57 | $0.07 | $+0.41 | $105.59 | ▲ +0.41 after sell → book $11,589.42; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 1 | $3.50 | $0.06 | $+0.17 | $109.03 | ▲ +0.17 after sell → book $11,589.36; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟡 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.03 | ▲ 09:30 equity $11,497.35 vs yday $11,422.02 (+75.33) | 09:30 open · cash $109.03 (unchanged overnight, no fees) · equity $11,497.35 vs prior close $11,422.02 (+75.33) because holdings re-marked: AG×66 yday $20.57 → 09:30 $20.73 +10.56; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×65 yday $20.49 → 09:30 $20.85 +23.40; HDSN×235 yday $5.57 → 09:30 $5.53 -9.40; IAG×69 yday $21.36 → 09:30 $21.63 +18.63; KGC×45 yday $32.47 → 09:30 $32.76 +13.05; NFGC×776 yday $1.90 → 09:30 $1.91 +7.76; WPM×9 yday $158.00 → 09:30 $160.00 +18.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×11 yday $2.38 → 09:30 $2.32 -0.66; CRDL×14 yday $1.80 → 09:30 $1.90 +1.40; CYPH×21 yday $1.64 → 09:30 $1.70 +1.26 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 1 | $8.28 | $0.09 | — | $100.66 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $15.58 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 9 | $1.62 | $0.17 | — | $85.91 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $15.58 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 2 | $5.22 | $0.11 | — | $75.36 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $15.58 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 2 | $6.38 | $0.13 | — | $62.46 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $15.58 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 1 | $8.79 | $0.09 | — | $53.58 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $15.58 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 24 | $0.64 | $0.23 | — | $38.00 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $15.58 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.00 | ▲ 09:30 equity $11,431.00 vs yday $11,431.00 (-0.00) | 09:30 open · cash $38.00 (unchanged overnight, no fees) · equity $11,431.00 vs prior close $11,431.00 (-0.00) because holdings re-marked: AG×66 yday $20.68 → 09:30 $20.68 +0.00; BHP×14 yday $96.05 → 09:30 $96.05 +0.00; CDE×65 yday $20.71 → 09:30 $20.71 +0.00; HDSN×235 yday $5.49 → 09:30 $5.49 +0.00; IAG×69 yday $21.48 → 09:30 $21.48 +0.00; KGC×45 yday $32.55 → 09:30 $32.55 +0.00; NFGC×776 yday $1.90 → 09:30 $1.90 +0.00; WPM×9 yday $158.25 → 09:30 $158.25 +0.00; AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×11 yday $2.34 → 09:30 $2.34 +0.00; CRDL×14 yday $1.90 → 09:30 $1.90 +0.00; CYPH×21 yday $1.64 → 09:30 $1.64 +0.00; CRMD×1 yday $8.28 → 09:30 $8.28 +0.00; BMEA×9 yday $1.61 → 09:30 $1.61 +0.00; ALVO×2 yday $5.25 → 09:30 $5.25 +0.00; ZURA×2 yday $6.50 → 09:30 $6.50 +0.00; SUJA×1 yday $8.54 → 09:30 $8.54 +0.00; DEFT×24 yday $0.62 → 09:30 $0.62 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.00 | ▲ 09:30 equity $11,597.51 vs yday $11,496.53 (+100.98) | 09:30 open · cash $38.00 (unchanged overnight, no fees) · equity $11,597.51 vs prior close $11,496.53 (+100.98) because holdings re-marked: AG×66 yday $20.68 → 09:30 $20.63 -3.30; BHP×14 yday $96.05 → 09:30 $96.99 +13.16; CDE×65 yday $20.71 → 09:30 $21.00 +18.85; HDSN×235 yday $5.49 → 09:30 $5.51 +4.70; IAG×69 yday $21.48 → 09:30 $21.64 +11.04; KGC×45 yday $32.55 → 09:30 $32.90 +15.75; NFGC×776 yday $1.90 → 09:30 $2.00 +77.60; WPM×9 yday $158.25 → 09:30 $160.93 +24.12; AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×11 yday $2.34 → 09:30 $2.41 +0.77; CRDL×14 yday $1.90 → 09:30 $2.03 +1.82; CYPH×21 yday $1.64 → 09:30 $1.60 -0.84; CRMD×1 yday $8.28 → 09:30 $8.60 +0.32; BMEA×9 yday $1.61 → 09:30 $1.75 +1.26; ALVO×2 yday $5.25 → 09:30 $4.98 -0.54; ZURA×2 yday $6.50 → 09:30 $6.13 -0.74; SUJA×1 yday $8.54 → 09:30 $9.39 +0.85; DEFT×24 yday $0.62 → 09:30 $0.60 -0.48 | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 66 | $20.63 | $2.21 | $+0.88 | $1,397.37 | ▲ +0.88 after sell → book $11,595.30; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $96.99 | $2.05 | $+79.64 | $2,753.18 | ▲ +79.64 after sell → book $11,593.25; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 65 | $21.00 | $2.21 | $+18.36 | $4,115.97 | ▲ +18.36 after sell → book $11,591.04; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 235 | $5.51 | $3.08 | $-67.21 | $5,407.74 | ▼ -67.21 after sell → book $11,587.96; vs 09:30 mark -3.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 69 | $21.64 | $2.22 | $+134.27 | $6,898.68 | ▲ +134.27 after sell → book $11,585.74; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 45 | $32.90 | $2.15 | $+142.88 | $8,377.03 | ▲ +142.88 after sell → book $11,583.59; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 776 | $2.00 | $10.15 | $+173.84 | $9,918.88 | ▲ +173.84 after sell → book $11,573.44; vs 09:30 mark -10.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 9 | $160.93 | $2.04 | $+143.45 | $11,365.21 | ▲ +143.45 after sell → book $11,571.40; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,365.21 | ▲ 09:30 equity $11,574.45 vs yday $11,572.41 (+2.04) | 09:30 open · cash $11,365.21 (unchanged overnight, no fees) · equity $11,574.45 vs prior close $11,572.41 (+2.04) because holdings re-marked: AUPH×1 yday $16.54 → 09:30 $16.47 -0.07; ARCT×2 yday $15.83 → 09:30 $15.74 -0.18; AUTL×11 yday $2.33 → 09:30 $2.32 -0.11; CRDL×14 yday $2.14 → 09:30 $2.09 -0.70; CYPH×21 yday $1.63 → 09:30 $1.75 +2.52; CRMD×1 yday $8.39 → 09:30 $8.49 +0.10; BMEA×9 yday $1.71 → 09:30 $1.74 +0.27; ALVO×2 yday $4.91 → 09:30 $4.88 -0.06; ZURA×2 yday $5.99 → 09:30 $6.02 +0.06; SUJA×1 yday $9.44 → 09:30 $9.41 -0.03; DEFT×24 yday $0.59 → 09:30 $0.60 +0.24 | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.47 | $0.19 | $-1.09 | $11,381.49 | ▼ -1.09 after sell → book $11,574.26; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.74 | $0.34 | $+8.65 | $11,412.63 | ▲ +8.65 after sell → book $11,573.92; vs 09:30 mark -0.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 11 | $2.32 | $0.31 | $-2.26 | $11,437.84 | ▼ -2.26 after sell → book $11,573.61; vs 09:30 mark -0.31 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 14 | $2.09 | $0.35 | $+1.57 | $11,466.75 | ▲ +1.57 after sell → book $11,573.26; vs 09:30 mark -0.35 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 21 | $1.75 | $0.45 | $+8.24 | $11,503.05 | ▲ +8.24 after sell → book $11,572.81; vs 09:30 mark -0.45 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $149.40 | $2.02 | — | $10,156.43 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1437.88 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $272.00 | $2.00 | — | $8,794.43 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer; ⚪; ret5=-3.9; leftover $1437.88 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 11 | $127.07 | $2.02 | — | $7,394.63 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $1437.88 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $323.82 | $2.00 | — | $6,097.35 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-11.7; leftover $1437.88 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 15 | $91.11 | $2.04 | — | $4,728.67 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-7.4; leftover $1437.88 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 22 | $62.80 | $2.06 | — | $3,345.01 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-7.8; leftover $1437.88 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $303.67 | $2.00 | — | $2,128.33 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-11.1; leftover $1437.88 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 11 | $121.13 | $2.02 | — | $793.88 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-9.8; leftover $1437.88 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $793.88 | ▼ 09:30 equity $10,909.37 vs yday $11,360.67 (-451.30) | 09:30 open · cash $793.88 (unchanged overnight, no fees) · equity $10,909.37 vs prior close $11,360.67 (-451.30) because holdings re-marked: CRMD×1 yday $8.31 → 09:30 $8.29 -0.02; BMEA×9 yday $1.68 → 09:30 $1.71 +0.27; ALVO×2 yday $4.88 → 09:30 $4.98 +0.20; ZURA×2 yday $5.85 → 09:30 $5.51 -0.68; SUJA×1 yday $9.00 → 09:30 $10.09 +1.09; DEFT×24 yday $0.65 → 09:30 $0.62 -0.72; SMTC×9 yday $142.43 → 09:30 $133.04 -84.51; SIMO×5 yday $255.08 → 09:30 $246.79 -41.45; TTMI×11 yday $124.73 → 09:30 $117.20 -82.83; KEYS×4 yday $325.82 → 09:30 $324.14 -6.72; AVT×15 yday $91.51 → 09:30 $88.63 -43.20; CGNX×22 yday $62.97 → 09:30 $60.31 -58.52; COHR×4 yday $295.39 → 09:30 $274.13 -85.04; LSCC×11 yday $120.47 → 09:30 $116.00 -49.17 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $793.88 | ▲ 09:30 equity $10,950.26 vs yday $10,931.07 (+19.19) | 09:30 open · cash $793.88 (unchanged overnight, no fees) · equity $10,950.26 vs prior close $10,931.07 (+19.19) because holdings re-marked: CRMD×1 yday $8.30 → 09:30 $8.26 -0.04; BMEA×9 yday $1.71 → 09:30 $1.65 -0.54; ALVO×2 yday $4.96 → 09:30 $5.24 +0.56; ZURA×2 yday $5.64 → 09:30 $5.60 -0.08; SUJA×1 yday $10.09 → 09:30 $9.31 -0.78; DEFT×24 yday $0.62 → 09:30 $0.59 -0.72; SMTC×9 yday $132.54 → 09:30 $131.65 -8.01; SIMO×5 yday $246.79 → 09:30 $247.53 +3.70; TTMI×11 yday $120.19 → 09:30 $119.79 -4.40; KEYS×4 yday $319.02 → 09:30 $323.71 +18.76; AVT×15 yday $88.63 → 09:30 $89.90 +19.05; CGNX×22 yday $60.31 → 09:30 $61.00 +15.18; COHR×4 yday $281.26 → 09:30 $277.23 -16.12; LSCC×11 yday $114.64 → 09:30 $113.97 -7.37 | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 1 | $8.26 | $0.11 | $-0.21 | $802.03 | ▼ -0.21 after sell → book $10,950.15; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 9 | $1.65 | $0.20 | $-0.10 | $816.68 | ▼ -0.10 after sell → book $10,949.95; vs 09:30 mark -0.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ALVO` | 2 | $5.24 | $0.13 | $-0.20 | $827.03 | ▼ -0.20 after sell → book $10,949.82; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `ZURA` | 2 | $5.60 | $0.14 | $-1.83 | $838.10 | ▼ -1.83 after sell → book $10,949.69; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `SUJA` | 1 | $9.31 | $0.12 | $+0.31 | $847.29 | ▲ +0.31 after sell → book $10,949.57; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `DEFT` | 24 | $0.59 | $0.23 | $-1.66 | $861.22 | ▼ -1.66 after sell → book $10,949.34; vs 09:30 mark -0.23 | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $861.22 | ▼ 09:30 equity $10,753.84 vs yday $10,832.85 (-79.01) | 09:30 open · cash $861.22 (unchanged overnight, no fees) · equity $10,753.84 vs prior close $10,832.85 (-79.01) because holdings re-marked: SMTC×9 yday $129.50 → 09:30 $127.63 -16.83; SIMO×5 yday $241.20 → 09:30 $240.09 -5.55; TTMI×11 yday $116.94 → 09:30 $116.68 -2.86; KEYS×4 yday $322.70 → 09:30 $321.47 -4.92; AVT×15 yday $89.90 → 09:30 $88.58 -19.80; CGNX×22 yday $60.57 → 09:30 $59.72 -18.70; COHR×4 yday $272.07 → 09:30 $270.50 -6.28; LSCC×11 yday $113.97 → 09:30 $113.60 -4.07 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $861.22 | ▼ 09:30 equity $10,739.81 vs yday $10,763.32 (-23.51) | 09:30 open · cash $861.22 (unchanged overnight, no fees) · equity $10,739.81 vs prior close $10,763.32 (-23.51) because holdings re-marked: SMTC×9 yday $132.27 → 09:30 $133.00 +6.57; SIMO×5 yday $237.35 → 09:30 $235.71 -8.20; TTMI×11 yday $115.33 → 09:30 $114.22 -12.21; KEYS×4 yday $319.27 → 09:30 $318.04 -4.92; AVT×15 yday $89.39 → 09:30 $89.39 +0.00; CGNX×22 yday $60.08 → 09:30 $60.37 +6.38; COHR×4 yday $272.03 → 09:30 $268.12 -15.64; LSCC×11 yday $111.68 → 09:30 $112.09 +4.51 | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 2 | $49.76 | $1.00 | — | $760.70 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $107.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 2 | $41.31 | $0.83 | — | $677.24 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $107.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 32 | $3.27 | $1.14 | — | $571.46 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $107.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 13 | $7.70 | $1.04 | — | $470.32 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $107.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 4 | $22.78 | $0.92 | — | $378.28 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $107.65 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 7 | $14.70 | $1.05 | — | $274.33 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $107.65 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 49 | $2.16 | $1.21 | — | $167.28 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $107.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $167.28 | ▼ 09:30 equity $10,779.28 vs yday $10,852.47 (-73.19) | 09:30 open · cash $167.28 (unchanged overnight, no fees) · equity $10,779.28 vs prior close $10,852.47 (-73.19) because holdings re-marked: SMTC×9 yday $133.85 → 09:30 $133.10 -6.75; SIMO×5 yday $242.51 → 09:30 $239.05 -17.30; TTMI×11 yday $115.60 → 09:30 $115.21 -4.29; KEYS×4 yday $321.58 → 09:30 $319.09 -9.96; AVT×15 yday $89.99 → 09:30 $89.02 -14.55; CGNX×22 yday $59.94 → 09:30 $59.96 +0.44; COHR×4 yday $268.64 → 09:30 $266.86 -7.12; LSCC×11 yday $113.88 → 09:30 $112.26 -17.82; ATRC×2 yday $52.59 → 09:30 $52.88 +0.58; HRMY×2 yday $42.86 → 09:30 $42.93 +0.14; CABA×32 yday $3.57 → 09:30 $3.63 +1.92; VSTM×13 yday $8.02 → 09:30 $8.03 +0.13; MMED×4 yday $23.76 → 09:30 $23.88 +0.48; SLN×7 yday $14.79 → 09:30 $14.85 +0.42; CRDL×49 yday $2.17 → 09:30 $2.18 +0.49 | — |
| 2026-09-04 09:30 ET | **SELL** | `SMTC` | 9 | $133.10 | $2.04 | $-150.75 | $1,363.14 | ▼ -150.75 after sell → book $10,777.24; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SIMO` | 5 | $239.05 | $2.02 | $-168.78 | $2,556.37 | ▼ -168.78 after sell → book $10,775.22; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `TTMI` | 11 | $115.21 | $2.04 | $-134.53 | $3,821.64 | ▼ -134.53 after sell → book $10,773.18; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `KEYS` | 4 | $319.09 | $2.02 | $-22.94 | $5,095.97 | ▼ -22.94 after sell → book $10,771.15; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVT` | 15 | $89.02 | $2.06 | $-35.44 | $6,429.22 | ▼ -35.44 after sell → book $10,769.10; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `CGNX` | 22 | $59.96 | $2.08 | $-66.61 | $7,746.26 | ▼ -66.61 after sell → book $10,767.02; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `COHR` | 4 | $266.86 | $2.02 | $-151.26 | $8,811.68 | ▼ -151.26 after sell → book $10,765.00; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `LSCC` | 11 | $112.26 | $2.04 | $-101.64 | $10,044.50 | ▼ -101.64 after sell → book $10,762.96; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 160 | $10.41 | $2.47 | — | $8,376.43 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1674.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 115 | $14.50 | $2.33 | — | $6,706.59 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1674.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $5,245.66 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1674.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 57 | $29.15 | $2.16 | — | $3,581.95 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1674.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 359 | $4.66 | $4.63 | — | $1,904.38 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1674.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 329 | $5.08 | $4.24 | — | $228.82 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1674.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `DAVE` | cash | leftover split 12.19 < 1 share @ 330.91 |
| 2026-08-14 | `BETR` | cash | leftover split 12.19 < 1 share @ 14.80 |
| 2026-08-14 | `WDC` | cash | leftover split 12.19 < 1 share @ 503.50 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `TGB` | cash | leftover split 5.85 < 1 share @ 8.46 |
| 2026-08-17 | `CDNL` | cash | leftover split 5.85 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 5.85 < 1 share @ 9.12 |
| 2026-08-17 | `OCC` | cash | leftover split 5.85 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 5.85 < 1 share @ 16.20 |
| 2026-08-17 | `UMAC` | cash | leftover split 5.85 < 1 share @ 32.55 |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `HIMS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-19 | `BTSG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `IREN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TGTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `SLS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `HIMS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `INO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TNDM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ANGX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HYLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-20 | `MARA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HYLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `TMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `TMC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `DNN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 27.98 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 27.98 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 27.98 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-25 | `AG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BHP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `CDE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `HDSN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `IAG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NFGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `WPM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MOS` | cash | leftover split 15.58 < 1 share @ 24.00 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `SUJA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `DEFT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ALVO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ZURA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `SUJA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `DEFT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ALVO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ZURA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `SUJA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `DEFT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `CGNX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `COHR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `LSCC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `AVT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `CGNX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `COHR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `LSCC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-02 | `SMTC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SIMO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `TTMI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `KEYS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `AVT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `CGNX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `COHR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `LSCC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-03 | `SMTC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SIMO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `TTMI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `KEYS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `AVT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `CGNX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `COHR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `LSCC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `RVTY` | cash | leftover split 107.65 < 1 share @ 125.94 |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 2 | 2026-09-03 @ $49.76 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $107.65 |
| `HRMY` | 2 | 2026-09-03 @ $41.31 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $107.65 |
| `CABA` | 32 | 2026-09-03 @ $3.27 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $107.65 |
| `VSTM` | 13 | 2026-09-03 @ $7.70 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $107.65 |
| `MMED` | 4 | 2026-09-03 @ $22.78 | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $107.65 |
| `SLN` | 7 | 2026-09-03 @ $14.70 | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $107.65 |
| `CRDL` | 49 | 2026-09-03 @ $2.16 | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $107.65 |
| `NVAX` | 160 | 2026-09-04 @ $10.41 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1674.08 |
| `BVS` | 115 | 2026-09-04 @ $14.50 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1674.08 |
| `DELL` | 3 | 2026-09-04 @ $486.31 | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1674.08 |
| `MLYS` | 57 | 2026-09-04 @ $29.15 | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1674.08 |
| `IRD` | 359 | 2026-09-04 @ $4.66 | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1674.08 |
| `OABI` | 329 | 2026-09-04 @ $5.08 | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1674.08 |
