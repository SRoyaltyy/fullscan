# Factor mine action — `union_h3_half`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `half` · sell `list` · S-boost `none` · deploy half leftover

Cash book **+4.95%** ($10,495) · signal-only (no cash/fees) was +34.19%. Starts YES **16/17**. Fills 125 · skips 168 · realized $+360.44.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `half` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $2,865.39.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $5,101.72 | $10,071.15 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $5,101.72 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26 | $10,084.41 | +13.26 | VST, NRG, SLG, MARA, LDI, BTBT | — | $3,312.91 | $10,212.64 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212 | 09:30 open · cash $5,101.72 (unchanged overnight, no fees) · equity $10,084.41 vs prior close $10,071.15 (+13.26) because holdings re-marked: BTSG×10 yday $60.23 → 09:30 $59.65 -5.80; IREN×13 yday $44.76 → 09:30 $44.09 -8.71; TPG×12 yday $54.62 → 09:30 $55.29 +8.04; TGTX×12 yday $47.94 → 09:30 $47.27 -8.04; SLS×53 yday $12.36 → 09:30 $12.40 +2.12; HIMS×21 yday $28.77 → 09:30 $29.15 +7.98; INO×771 yday $0.90 → 09:30 $0.93 +23.13; TNDM×26 yday $23.13 → 09:30 $22.92 -5.46 |
| 2026-08-17 | +2.25 | $3,312.91 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212 | $10,196.68 | -15.96 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | — | $1,765.50 | $10,250.96 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | 09:30 open · cash $3,312.91 (unchanged overnight, no fees) · equity $10,196.68 vs prior close $10,212.64 (-15.96) because holdings re-marked: BTSG×10 yday $61.71 → 09:30 $61.69 -0.20; IREN×13 yday $44.06 → 09:30 $45.23 +15.21; TPG×12 yday $53.03 → 09:30 $52.67 -4.32; TGTX×12 yday $48.74 → 09:30 $48.74 +0.00; SLS×53 yday $12.78 → 09:30 $12.78 +0.00; HIMS×21 yday $28.15 → 09:30 $28.14 -0.21; INO×771 yday $1.09 → 09:30 $1.07 -15.42; TNDM×26 yday $22.72 → 09:30 $22.50 -5.72; VST×2 yday $148.13 → 09:30 $149.37 +2.48; NRG×2 yday $126.24 → 09:30 $127.40 +2.32; SLG×5 yday $56.09 → 09:30 $55.37 -3.60; MARA×35 yday $9.20 → 09:30 $9.22 +0.70; LDI×340 yday $0.90 → 09:30 $0.91 +3.40; BTBT×212 yday $1.57 → 09:30 $1.52 -10.60 |
| 2026-08-18 | -6.20 | $1,765.50 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | $10,145.54 | -105.42 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $6,830.71 | $10,078.11 | VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | 09:30 open · cash $1,765.50 (unchanged overnight, no fees) · equity $10,145.54 vs prior close $10,250.96 (-105.42) because holdings re-marked: BTSG×10 yday $60.38 → 09:30 $60.00 -3.80; IREN×13 yday $44.90 → 09:30 $43.56 -17.42; TPG×12 yday $51.77 → 09:30 $51.77 +0.00; TGTX×12 yday $49.28 → 09:30 $49.28 +0.00; SLS×53 yday $13.00 → 09:30 $12.66 -18.02; HIMS×21 yday $28.61 → 09:30 $27.85 -15.96; INO×771 yday $1.15 → 09:30 $1.14 -7.71; TNDM×26 yday $22.25 → 09:30 $22.16 -2.47; VST×2 yday $146.11 → 09:30 $144.50 -3.22; NRG×2 yday $122.37 → 09:30 $121.92 -0.90; SLG×5 yday $56.11 → 09:30 $56.00 -0.55; MARA×35 yday $9.72 → 09:30 $9.36 -12.60; LDI×340 yday $0.88 → 09:30 $0.87 -1.70; BTBT×212 yday $1.60 → 09:30 $1.54 -12.72; DVN×4 yday $47.57 → 09:30 $48.00 +1.72; EOG×1 yday $146.15 → 09:30 $148.04 +1.89; FANG×1 yday $206.29 → 09:30 $208.93 +2.64; TMC×51 yday $3.77 → 09:30 $3.72 -2.55; TGB×24 yday $8.77 → 09:30 $8.55 -5.28; ELF×2 yday $93.66 → 09:30 $93.44 -0.44; DNN×63 yday $3.19 → 09:30 $3.11 -5.04; HNST×43 yday $4.70 → 09:30 $4.67 -1.29 |
| 2026-08-19 | -7.20 | $6,830.71 | VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | $10,106.36 | +28.25 | — | VST, NRG, SLG, MARA, LDI, BTBT | $8,529.15 | $10,103.71 | DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | 09:30 open · cash $6,830.71 (unchanged overnight, no fees) · equity $10,106.36 vs prior close $10,078.11 (+28.25) because holdings re-marked: VST×2 yday $140.52 → 09:30 $140.74 +0.44; NRG×2 yday $115.56 → 09:30 $116.20 +1.28; SLG×5 yday $56.84 → 09:30 $57.50 +3.30; MARA×35 yday $8.96 → 09:30 $8.91 -1.75; LDI×340 yday $0.86 → 09:30 $0.88 +7.48; BTBT×212 yday $1.45 → 09:30 $1.42 -6.36; DVN×4 yday $47.83 → 09:30 $48.22 +1.56; EOG×1 yday $148.70 → 09:30 $149.86 +1.16; FANG×1 yday $210.02 → 09:30 $210.84 +0.82; TMC×51 yday $3.92 → 09:30 $3.93 +0.51; TGB×24 yday $8.36 → 09:30 $8.70 +8.16; ELF×2 yday $92.51 → 09:30 $96.00 +6.98; DNN×63 yday $3.15 → 09:30 $3.19 +2.52; HNST×43 yday $4.75 → 09:30 $4.80 +2.15 |
| 2026-08-20 | +1.12 | $8,529.15 | DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | $10,102.55 | -1.16 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | $5,197.63 | $10,182.57 | AG×30, BHP×6, CDE×30, HDSN×109, IAG×32, KGC×21, NFGC×360, WPM×4 | 09:30 open · cash $8,529.15 (unchanged overnight, no fees) · equity $10,102.55 vs prior close $10,103.71 (-1.16) because holdings re-marked: DVN×4 yday $48.19 → 09:30 $49.02 +3.32; EOG×1 yday $149.48 → 09:30 $151.45 +1.97; FANG×1 yday $208.55 → 09:30 $213.51 +4.96; TMC×51 yday $3.97 → 09:30 $3.92 -2.55; TGB×24 yday $8.47 → 09:30 $8.35 -2.88; ELF×2 yday $99.65 → 09:30 $98.15 -3.00; DNN×63 yday $3.22 → 09:30 $3.20 -1.26; HNST×43 yday $5.02 → 09:30 $4.98 -1.72 |
| 2026-08-21 | +3.25 | $5,197.63 | AG×30, BHP×6, CDE×30, HDSN×109, IAG×32, KGC×21, NFGC×360, WPM×4 | $10,315.69 | +133.12 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | — | $2,820.80 | $10,359.67 | AG×30, BHP×6, CDE×30, HDSN×109, IAG×32, KGC×21, NFGC×360, WPM×4, AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246 | 09:30 open · cash $5,197.63 (unchanged overnight, no fees) · equity $10,315.69 vs prior close $10,182.57 (+133.12) because holdings re-marked: AG×30 yday $21.19 → 09:30 $21.90 +21.30; BHP×6 yday $93.63 → 09:30 $95.72 +12.54; CDE×30 yday $21.11 → 09:30 $21.75 +19.20; HDSN×109 yday $5.57 → 09:30 $5.67 +10.90; IAG×32 yday $20.50 → 09:30 $21.17 +21.44; KGC×21 yday $31.43 → 09:30 $32.17 +15.54; NFGC×360 yday $1.75 → 09:30 $1.79 +14.40; WPM×4 yday $150.25 → 09:30 $154.70 +17.80 |
| 2026-08-24 | -5.17 | $2,820.80 | AG×30, BHP×6, CDE×30, HDSN×109, IAG×32, KGC×21, NFGC×360, WPM×4, AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246 | $10,504.70 | +145.03 | — | — | $2,820.80 | $10,372.49 | AG×30, BHP×6, CDE×30, HDSN×109, IAG×32, KGC×21, NFGC×360, WPM×4, AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246 | 09:30 open · cash $2,820.80 (unchanged overnight, no fees) · equity $10,504.70 vs prior close $10,359.67 (+145.03) because holdings re-marked: AG×30 yday $21.09 → 09:30 $21.47 +11.40; BHP×6 yday $97.03 → 09:30 $97.34 +1.86; CDE×30 yday $20.97 → 09:30 $21.26 +8.70; HDSN×109 yday $5.63 → 09:30 $5.69 +6.54; IAG×32 yday $21.14 → 09:30 $21.44 +9.60; KGC×21 yday $32.76 → 09:30 $33.21 +9.45; NFGC×360 yday $1.84 → 09:30 $1.86 +7.20; WPM×4 yday $157.78 → 09:30 $158.96 +4.72; AU×2 yday $121.22 → 09:30 $120.50 -1.44; AUPH×18 yday $16.65 → 09:30 $16.60 -0.90; AEM×1 yday $216.06 → 09:30 $217.03 +0.97; ARCT×29 yday $13.45 → 09:30 $13.26 -5.51; AUTL×131 yday $2.41 → 09:30 $2.36 -6.55; CRDL×168 yday $1.86 → 09:30 $1.87 +1.68; CRSP×5 yday $59.50 → 09:30 $58.79 -3.55; CYPH×246 yday $1.42 → 09:30 $1.83 +100.86 |
| 2026-08-25 | +1.80 | $2,820.80 | AG×30, BHP×6, CDE×30, HDSN×109, IAG×32, KGC×21, NFGC×360, WPM×4, AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246 | $10,436.75 | +64.26 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $4,052.89 | $10,396.63 | AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246, MOS×20, OCUL×45, INSP×8, CRMD×59, RZLT×94, HCA×1, BMEA×306, NPWR×247 | 09:30 open · cash $2,820.80 (unchanged overnight, no fees) · equity $10,436.75 vs prior close $10,372.49 (+64.26) because holdings re-marked: AG×30 yday $20.57 → 09:30 $20.73 +4.80; BHP×6 yday $96.66 → 09:30 $95.95 -4.26; CDE×30 yday $20.49 → 09:30 $20.85 +10.80; HDSN×109 yday $5.57 → 09:30 $5.53 -4.36; IAG×32 yday $21.36 → 09:30 $21.63 +8.64; KGC×21 yday $32.47 → 09:30 $32.76 +6.09; NFGC×360 yday $1.90 → 09:30 $1.91 +3.60; WPM×4 yday $158.00 → 09:30 $160.00 +8.00; AU×2 yday $118.66 → 09:30 $119.46 +1.60; AUPH×18 yday $16.60 → 09:30 $16.71 +1.98; AEM×1 yday $214.08 → 09:30 $200.48 -13.60; ARCT×29 yday $13.76 → 09:30 $14.34 +16.82; AUTL×131 yday $2.38 → 09:30 $2.32 -7.86; CRDL×168 yday $1.80 → 09:30 $1.90 +16.80; CRSP×5 yday $56.91 → 09:30 $57.00 +0.45; CYPH×246 yday $1.64 → 09:30 $1.70 +14.76 |
| 2026-08-26 | +2.02 | $4,052.89 | AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246, MOS×20, OCUL×45, INSP×8, CRMD×59, RZLT×94, HCA×1, BMEA×306, NPWR×247 | $10,396.63 | -0.00 | — | — | $4,052.89 | $10,397.51 | AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246, MOS×20, OCUL×45, INSP×8, CRMD×59, RZLT×94, HCA×1, BMEA×306, NPWR×247 | 09:30 open · cash $4,052.89 (unchanged overnight, no fees) · equity $10,396.63 vs prior close $10,396.63 (-0.00) because holdings re-marked: AU×2 yday $118.55 → 09:30 $118.55 +0.00; AUPH×18 yday $16.71 → 09:30 $16.71 +0.00; AEM×1 yday $215.40 → 09:30 $215.40 +0.00; ARCT×29 yday $14.21 → 09:30 $14.21 +0.00; AUTL×131 yday $2.34 → 09:30 $2.34 +0.00; CRDL×168 yday $1.90 → 09:30 $1.90 +0.00; CRSP×5 yday $57.03 → 09:30 $57.03 +0.00; CYPH×246 yday $1.64 → 09:30 $1.64 +0.00; MOS×20 yday $23.75 → 09:30 $23.75 +0.00; OCUL×45 yday $10.92 → 09:30 $10.92 +0.00; INSP×8 yday $61.47 → 09:30 $61.47 +0.00; CRMD×59 yday $8.28 → 09:30 $8.28 +0.00; RZLT×94 yday $5.29 → 09:30 $5.29 +0.00; HCA×1 yday $428.50 → 09:30 $428.50 +0.00; BMEA×306 yday $1.61 → 09:30 $1.61 +0.00; NPWR×247 yday $2.02 → 09:30 $2.02 +0.00 |
| 2026-08-27 | — | $4,052.89 | AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246, MOS×20, OCUL×45, INSP×8, CRMD×59, RZLT×94, HCA×1, BMEA×306, NPWR×247 | $10,488.15 | +90.64 | RRC, CRK, SLI, ACMR, GGB, MT | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $3,870.07 | $10,422.03 | MOS×20, OCUL×45, INSP×8, CRMD×59, RZLT×94, HCA×1, BMEA×306, NPWR×247, RRC×11, CRK×33, SLI×181, ACMR×5, GGB×106, MT×6 | 09:30 open · cash $4,052.89 (unchanged overnight, no fees) · equity $10,488.15 vs prior close $10,397.51 (+90.64) because holdings re-marked: AU×2 yday $118.55 → 09:30 $119.80 +2.50; AUPH×18 yday $16.71 → 09:30 $16.60 -1.98; AEM×1 yday $215.40 → 09:30 $219.50 +4.10; ARCT×29 yday $14.21 → 09:30 $15.35 +33.06; AUTL×131 yday $2.34 → 09:30 $2.41 +9.17; CRDL×168 yday $1.90 → 09:30 $2.03 +21.84; CRSP×5 yday $57.03 → 09:30 $60.18 +15.75; CYPH×246 yday $1.64 → 09:30 $1.60 -9.84; MOS×20 yday $23.75 → 09:30 $24.84 +21.80; OCUL×45 yday $10.92 → 09:30 $10.79 -5.85; INSP×8 yday $61.47 → 09:30 $60.07 -11.20; CRMD×59 yday $8.28 → 09:30 $8.60 +18.88; RZLT×94 yday $5.29 → 09:30 $5.01 -26.32; HCA×1 yday $428.50 → 09:30 $427.50 -1.00; BMEA×306 yday $1.61 → 09:30 $1.75 +42.84; NPWR×247 yday $2.02 → 09:30 $1.93 -22.23 |
| 2026-08-28 | +0.75 | $3,870.07 | MOS×20, OCUL×45, INSP×8, CRMD×59, RZLT×94, HCA×1, BMEA×306, NPWR×247, RRC×11, CRK×33, SLI×181, ACMR×5, GGB×106, MT×6 | $10,453.98 | +31.95 | ANF, BHVN, BZ, CAPR | OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $3,650.10 | $10,470.81 | MOS×20, RRC×11, CRK×33, SLI×181, ACMR×5, GGB×106, MT×6, ANF×6, BHVN×53, BZ×48, CAPR×98 | 09:30 open · cash $3,870.07 (unchanged overnight, no fees) · equity $10,453.98 vs prior close $10,422.03 (+31.95) because holdings re-marked: MOS×20 yday $24.16 → 09:30 $24.00 -3.20; OCUL×45 yday $10.77 → 09:30 $10.63 -6.30; INSP×8 yday $61.80 → 09:30 $62.10 +2.40; CRMD×59 yday $8.39 → 09:30 $8.49 +5.90; RZLT×94 yday $5.04 → 09:30 $5.07 +2.82; HCA×1 yday $427.16 → 09:30 $424.61 -2.55; BMEA×306 yday $1.71 → 09:30 $1.74 +9.18; NPWR×247 yday $1.81 → 09:30 $1.83 +4.94; RRC×11 yday $41.55 → 09:30 $41.44 -1.21; CRK×33 yday $14.50 → 09:30 $14.42 -2.64; SLI×181 yday $2.61 → 09:30 $2.60 -1.81; ACMR×5 yday $79.11 → 09:30 $81.65 +12.70; GGB×106 yday $4.46 → 09:30 $4.57 +11.66; MT×6 yday $74.53 → 09:30 $74.54 +0.06 |
| 2026-08-31 | -5.85 | $3,650.10 | MOS×20, RRC×11, CRK×33, SLI×181, ACMR×5, GGB×106, MT×6, ANF×6, BHVN×53, BZ×48, CAPR×98 | $10,314.50 | -156.31 | — | MOS | $4,123.03 | $10,311.87 | RRC×11, CRK×33, SLI×181, ACMR×5, GGB×106, MT×6, ANF×6, BHVN×53, BZ×48, CAPR×98 | 09:30 open · cash $3,650.10 (unchanged overnight, no fees) · equity $10,314.50 vs prior close $10,470.81 (-156.31) because holdings re-marked: MOS×20 yday $23.76 → 09:30 $23.75 -0.20; RRC×11 yday $41.64 → 09:30 $41.11 -5.83; CRK×33 yday $14.62 → 09:30 $14.56 -1.98; SLI×181 yday $2.64 → 09:30 $2.51 -23.53; ACMR×5 yday $80.49 → 09:30 $75.10 -26.95; GGB×106 yday $4.70 → 09:30 $4.55 -15.90; MT×6 yday $74.63 → 09:30 $75.07 +2.64; ANF×6 yday $145.75 → 09:30 $148.67 +17.52; BHVN×53 yday $16.12 → 09:30 $15.44 -36.04; BZ×48 yday $18.00 → 09:30 $17.89 -5.28; CAPR×98 yday $10.06 → 09:30 $9.44 -60.76 |
| 2026-09-01 | -6.30 | $4,123.03 | RRC×11, CRK×33, SLI×181, ACMR×5, GGB×106, MT×6, ANF×6, BHVN×53, BZ×48, CAPR×98 | $10,358.77 | +46.90 | — | RRC, CRK, SLI, ACMR, GGB, MT | $6,816.09 | $10,315.72 | ANF×6, BHVN×53, BZ×48, CAPR×98 | 09:30 open · cash $4,123.03 (unchanged overnight, no fees) · equity $10,358.77 vs prior close $10,311.87 (+46.90) because holdings re-marked: RRC×11 yday $41.78 → 09:30 $41.32 -5.06; CRK×33 yday $14.51 → 09:30 $14.31 -6.60; SLI×181 yday $2.51 → 09:30 $2.70 +34.39; ACMR×5 yday $75.02 → 09:30 $71.24 -18.90; GGB×106 yday $4.55 → 09:30 $4.61 +6.36; MT×6 yday $75.06 → 09:30 $74.31 -4.50; ANF×6 yday $149.28 → 09:30 $142.47 -40.86; BHVN×53 yday $15.40 → 09:30 $15.45 +2.65; BZ×48 yday $17.90 → 09:30 $17.37 -25.44; CAPR×98 yday $9.36 → 09:30 $10.43 +104.86 |
| 2026-09-02 | -3.83 | $6,816.09 | ANF×6, BHVN×53, BZ×48, CAPR×98 | $10,369.14 | +53.42 | — | ANF, BHVN, BZ, CAPR | $10,360.47 | $10,360.47 | — | 09:30 open · cash $6,816.09 (unchanged overnight, no fees) · equity $10,369.14 vs prior close $10,315.72 (+53.42) because holdings re-marked: ANF×6 yday $143.00 → 09:30 $142.00 -6.00; BHVN×53 yday $15.45 → 09:30 $15.39 -3.18; BZ×48 yday $17.17 → 09:30 $17.29 +5.76; CAPR×98 yday $10.19 → 09:30 $10.77 +56.84 |
| 2026-09-03 | -0.90 | $10,360.47 | — | $10,360.47 | +0.00 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $5,213.74 | $10,737.69 | ATRC×13, HRMY×15, CABA×198, VSTM×84, RVTY×5, GPRO×530, FRVO×35, CRK×41 | 09:30 open · cash $10,360.47 · no holdings · equity $10,360.47 vs prior close $10,360.47 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $5,213.74 | ATRC×13, HRMY×15, CABA×198, VSTM×84, RVTY×5, GPRO×530, FRVO×35, CRK×41 | $10,816.94 | +79.25 | ASND, OSCR, NVAX, BVS, BAK | — | $2,865.39 | $10,494.81 | ATRC×13, HRMY×15, CABA×198, VSTM×84, RVTY×5, GPRO×530, FRVO×35, CRK×41, ASND×1, OSCR×17, NVAX×50, BVS×35, BAK×267 | 09:30 open · cash $5,213.74 (unchanged overnight, no fees) · equity $10,816.94 vs prior close $10,737.69 (+79.25) because holdings re-marked: ATRC×13 yday $52.59 → 09:30 $52.88 +3.77; HRMY×15 yday $42.86 → 09:30 $42.93 +1.05; CABA×198 yday $3.57 → 09:30 $3.63 +11.88; VSTM×84 yday $8.02 → 09:30 $8.03 +0.84; RVTY×5 yday $130.94 → 09:30 $132.45 +7.55; GPRO×530 yday $1.69 → 09:30 $1.78 +47.70; FRVO×35 yday $17.98 → 09:30 $18.27 +10.15; CRK×41 yday $15.54 → 09:30 $15.45 -3.69 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 10 | $59.80 | $2.02 | — | $9,399.98 | — | deploy half leftover; list flatten; ⚪; ret5=-5.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 13 | $45.98 | $2.03 | — | $8,800.21 | — | deploy half leftover; list flatten; ⚪; ret5=+12.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 12 | $50.62 | $2.03 | — | $8,190.71 | — | deploy half leftover; list flatten; ⚪; ret5=+6.2; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 12 | $49.70 | $2.03 | — | $7,592.28 | — | deploy half leftover; list flatten; ⚪; ret5=-0.8; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 53 | $11.70 | $2.15 | — | $6,970.03 | — | deploy half leftover; list flatten; ⚪; ret5=-0.8; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 21 | $29.74 | $2.05 | — | $6,343.44 | — | deploy half leftover; list flatten; ⚪; ret5=-5.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 771 | $0.81 | $8.56 | — | $5,710.37 | — | deploy half leftover; list flatten; ⚪; ret5=+13.2; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 26 | $23.33 | $2.07 | — | $5,101.72 | — | deploy half leftover; list flatten; ⚪; ret5=+19.7; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,101.72 | ▲ 09:30 equity $10,084.41 vs yday $10,071.15 (+13.26) | 09:30 open · cash $5,101.72 (unchanged overnight, no fees) · equity $10,084.41 vs prior close $10,071.15 (+13.26) because holdings re-marked: BTSG×10 yday $60.23 → 09:30 $59.65 -5.80; IREN×13 yday $44.76 → 09:30 $44.09 -8.71; TPG×12 yday $54.62 → 09:30 $55.29 +8.04; TGTX×12 yday $47.94 → 09:30 $47.27 -8.04; SLS×53 yday $12.36 → 09:30 $12.40 +2.12; HIMS×21 yday $28.77 → 09:30 $29.15 +7.98; INO×771 yday $0.90 → 09:30 $0.93 +23.13; TNDM×26 yday $23.13 → 09:30 $22.92 -5.46 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 2 | $146.90 | $2.00 | — | $4,805.93 | — | deploy half leftover; list flatten; 🔵; ret5=+3.6; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 2 | $120.00 | $2.00 | — | $4,563.93 | — | deploy half leftover; list flatten; 🔵; ret5=+0.6; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 5 | $57.61 | $2.00 | — | $4,273.88 | — | deploy half leftover; list flatten; 🔵; ret5=+5.7; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 35 | $9.01 | $2.10 | — | $3,956.43 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=-13.5; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 340 | $0.94 | $4.21 | — | $3,633.64 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.5; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 212 | $1.50 | $2.73 | — | $3,312.91 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,312.91 | ▼ 09:30 equity $10,196.68 vs yday $10,212.64 (-15.96) | 09:30 open · cash $3,312.91 (unchanged overnight, no fees) · equity $10,196.68 vs prior close $10,212.64 (-15.96) because holdings re-marked: BTSG×10 yday $61.71 → 09:30 $61.69 -0.20; IREN×13 yday $44.06 → 09:30 $45.23 +15.21; TPG×12 yday $53.03 → 09:30 $52.67 -4.32; TGTX×12 yday $48.74 → 09:30 $48.74 +0.00; SLS×53 yday $12.78 → 09:30 $12.78 +0.00; HIMS×21 yday $28.15 → 09:30 $28.14 -0.21; INO×771 yday $1.09 → 09:30 $1.07 -15.42; TNDM×26 yday $22.72 → 09:30 $22.50 -5.72; VST×2 yday $148.13 → 09:30 $149.37 +2.48; NRG×2 yday $126.24 → 09:30 $127.40 +2.32; SLG×5 yday $56.09 → 09:30 $55.37 -3.60; MARA×35 yday $9.20 → 09:30 $9.22 +0.70; LDI×340 yday $0.90 → 09:30 $0.91 +3.40; BTBT×212 yday $1.57 → 09:30 $1.52 -10.60 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 4 | $46.18 | $1.86 | — | $3,126.33 | — | deploy half leftover; list flatten; 🔵; ret5=+6.7; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 1 | $142.77 | $1.43 | — | $2,982.13 | — | deploy half leftover; list flatten; 🔵; ret5=+5.8; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $2,777.44 | — | deploy half leftover; list flatten; 🔵; ret5=+8.3; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 51 | $4.05 | $2.14 | — | $2,568.74 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=-12.3; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 24 | $8.46 | $2.06 | — | $2,363.64 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.4; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 2 | $90.54 | $1.82 | — | $2,180.75 | — | deploy half leftover; list flatten; ret5=-7.2; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 63 | $3.24 | $2.18 | — | $1,974.45 | — | deploy half leftover; list flatten; ⚪; ret5=+0.3; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 43 | $4.81 | $2.12 | — | $1,765.50 | — | deploy half leftover; list flatten; ⚪; ret5=-11.4; leftover $207.06 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,765.50 | ▼ 09:30 equity $10,145.54 vs yday $10,250.96 (-105.42) | 09:30 open · cash $1,765.50 (unchanged overnight, no fees) · equity $10,145.54 vs prior close $10,250.96 (-105.42) because holdings re-marked: BTSG×10 yday $60.38 → 09:30 $60.00 -3.80; IREN×13 yday $44.90 → 09:30 $43.56 -17.42; TPG×12 yday $51.77 → 09:30 $51.77 +0.00; TGTX×12 yday $49.28 → 09:30 $49.28 +0.00; SLS×53 yday $13.00 → 09:30 $12.66 -18.02; HIMS×21 yday $28.61 → 09:30 $27.85 -15.96; INO×771 yday $1.15 → 09:30 $1.14 -7.71; TNDM×26 yday $22.25 → 09:30 $22.16 -2.47; VST×2 yday $146.11 → 09:30 $144.50 -3.22; NRG×2 yday $122.37 → 09:30 $121.92 -0.90; SLG×5 yday $56.11 → 09:30 $56.00 -0.55; MARA×35 yday $9.72 → 09:30 $9.36 -12.60; LDI×340 yday $0.88 → 09:30 $0.87 -1.70; BTBT×212 yday $1.60 → 09:30 $1.54 -12.72; DVN×4 yday $47.57 → 09:30 $48.00 +1.72; EOG×1 yday $146.15 → 09:30 $148.04 +1.89; FANG×1 yday $206.29 → 09:30 $208.93 +2.64; TMC×51 yday $3.77 → 09:30 $3.72 -2.55; TGB×24 yday $8.77 → 09:30 $8.55 -5.28; ELF×2 yday $93.66 → 09:30 $93.44 -0.44; DNN×63 yday $3.19 → 09:30 $3.11 -5.04; HNST×43 yday $4.70 → 09:30 $4.67 -1.29 | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 10 | $60.00 | $2.04 | $-2.06 | $2,363.46 | ▼ -2.06 after sell → book $10,143.50; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 13 | $43.56 | $2.05 | $-35.54 | $2,927.69 | ▼ -35.54 after sell → book $10,141.45; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 12 | $51.77 | $2.05 | $+9.69 | $3,546.88 | ▲ +9.69 after sell → book $10,139.40; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 12 | $49.28 | $2.05 | $-9.11 | $4,136.20 | ▼ -9.11 after sell → book $10,137.36; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 53 | $12.66 | $2.17 | $+46.56 | $4,805.01 | ▲ +46.56 after sell → book $10,135.19; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 21 | $27.85 | $2.07 | $-43.82 | $5,387.78 | ▼ -43.82 after sell → book $10,133.11; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 771 | $1.14 | $10.08 | $+235.79 | $6,256.64 | ▲ +235.79 after sell → book $10,123.03; vs 09:30 mark -10.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 26 | $22.16 | $2.09 | $-34.58 | $6,830.71 | ▼ -34.58 after sell → book $10,120.94; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,830.71 | ▲ 09:30 equity $10,106.36 vs yday $10,078.11 (+28.25) | 09:30 open · cash $6,830.71 (unchanged overnight, no fees) · equity $10,106.36 vs prior close $10,078.11 (+28.25) because holdings re-marked: VST×2 yday $140.52 → 09:30 $140.74 +0.44; NRG×2 yday $115.56 → 09:30 $116.20 +1.28; SLG×5 yday $56.84 → 09:30 $57.50 +3.30; MARA×35 yday $8.96 → 09:30 $8.91 -1.75; LDI×340 yday $0.86 → 09:30 $0.88 +7.48; BTBT×212 yday $1.45 → 09:30 $1.42 -6.36; DVN×4 yday $47.83 → 09:30 $48.22 +1.56; EOG×1 yday $148.70 → 09:30 $149.86 +1.16; FANG×1 yday $210.02 → 09:30 $210.84 +0.82; TMC×51 yday $3.92 → 09:30 $3.93 +0.51; TGB×24 yday $8.36 → 09:30 $8.70 +8.16; ELF×2 yday $92.51 → 09:30 $96.00 +6.98; DNN×63 yday $3.15 → 09:30 $3.19 +2.52; HNST×43 yday $4.75 → 09:30 $4.80 +2.15 | — |
| 2026-08-19 09:30 ET | **SELL** | `VST` | 2 | $140.74 | $2.02 | $-16.33 | $7,110.18 | ▼ -16.33 after sell → book $10,104.35; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NRG` | 2 | $116.20 | $2.02 | $-11.61 | $7,340.56 | ▼ -11.61 after sell → book $10,102.33; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SLG` | 5 | $57.50 | $2.02 | $-4.58 | $7,626.04 | ▼ -4.58 after sell → book $10,100.31; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 35 | $8.91 | $2.12 | $-7.71 | $7,935.77 | ▼ -7.71 after sell → book $10,098.19; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 340 | $0.88 | $4.08 | $-27.66 | $8,230.89 | ▼ -27.66 after sell → book $10,094.11; vs 09:30 mark -4.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 212 | $1.42 | $2.78 | $-22.47 | $8,529.15 | ▼ -22.47 after sell → book $10,091.33; vs 09:30 mark -2.78 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,529.15 | ▼ 09:30 equity $10,102.55 vs yday $10,103.71 (-1.16) | 09:30 open · cash $8,529.15 (unchanged overnight, no fees) · equity $10,102.55 vs prior close $10,103.71 (-1.16) because holdings re-marked: DVN×4 yday $48.19 → 09:30 $49.02 +3.32; EOG×1 yday $149.48 → 09:30 $151.45 +1.97; FANG×1 yday $208.55 → 09:30 $213.51 +4.96; TMC×51 yday $3.97 → 09:30 $3.92 -2.55; TGB×24 yday $8.47 → 09:30 $8.35 -2.88; ELF×2 yday $99.65 → 09:30 $98.15 -3.00; DNN×63 yday $3.22 → 09:30 $3.20 -1.26; HNST×43 yday $5.02 → 09:30 $4.98 -1.72 | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 4 | $49.02 | $1.99 | $+7.51 | $8,723.24 | ▲ +7.51 after sell → book $10,100.56; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `EOG` | 1 | $151.45 | $1.54 | $+5.71 | $8,873.15 | ▲ +5.71 after sell → book $10,099.02; vs 09:30 mark -1.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `FANG` | 1 | $213.51 | $2.01 | $+6.80 | $9,084.65 | ▲ +6.80 after sell → book $10,097.01; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 51 | $3.92 | $2.16 | $-10.94 | $9,282.41 | ▼ -10.94 after sell → book $10,094.85; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 24 | $8.35 | $2.08 | $-6.78 | $9,480.72 | ▼ -6.78 after sell → book $10,092.76; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ELF` | 2 | $98.15 | $1.99 | $+11.41 | $9,675.03 | ▲ +11.41 after sell → book $10,090.77; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 63 | $3.20 | $2.20 | $-6.90 | $9,874.44 | ▼ -6.90 after sell → book $10,088.58; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `HNST` | 43 | $4.98 | $2.14 | $+3.05 | $10,086.44 | ▲ +3.05 after sell → book $10,086.44; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 30 | $20.55 | $2.08 | — | $9,467.86 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 6 | $91.01 | $2.01 | — | $8,919.79 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 30 | $20.65 | $2.08 | — | $8,298.21 | — | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 109 | $5.77 | $2.32 | — | $7,666.96 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 32 | $19.63 | $2.09 | — | $7,036.72 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 21 | $29.63 | $2.05 | — | $6,412.43 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 360 | $1.75 | $4.64 | — | $5,777.79 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $5,197.63 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,197.63 | ▲ 09:30 equity $10,315.69 vs yday $10,182.57 (+133.12) | 09:30 open · cash $5,197.63 (unchanged overnight, no fees) · equity $10,315.69 vs prior close $10,182.57 (+133.12) because holdings re-marked: AG×30 yday $21.19 → 09:30 $21.90 +21.30; BHP×6 yday $93.63 → 09:30 $95.72 +12.54; CDE×30 yday $21.11 → 09:30 $21.75 +19.20; HDSN×109 yday $5.57 → 09:30 $5.67 +10.90; IAG×32 yday $20.50 → 09:30 $21.17 +21.44; KGC×21 yday $31.43 → 09:30 $32.17 +15.54; NFGC×360 yday $1.75 → 09:30 $1.79 +14.40; WPM×4 yday $150.25 → 09:30 $154.70 +17.80 | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 2 | $119.43 | $2.00 | — | $4,956.77 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 18 | $17.20 | $2.04 | — | $4,645.13 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 1 | $216.30 | $1.99 | — | $4,426.83 | — | deploy half leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 29 | $11.13 | $2.08 | — | $4,101.99 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 131 | $2.47 | $2.38 | — | $3,776.03 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 168 | $1.93 | $2.49 | — | $3,449.30 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 5 | $59.72 | $2.00 | — | $3,148.69 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 246 | $1.32 | $3.17 | — | $2,820.80 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,820.80 | ▲ 09:30 equity $10,504.70 vs yday $10,359.67 (+145.03) | 09:30 open · cash $2,820.80 (unchanged overnight, no fees) · equity $10,504.70 vs prior close $10,359.67 (+145.03) because holdings re-marked: AG×30 yday $21.09 → 09:30 $21.47 +11.40; BHP×6 yday $97.03 → 09:30 $97.34 +1.86; CDE×30 yday $20.97 → 09:30 $21.26 +8.70; HDSN×109 yday $5.63 → 09:30 $5.69 +6.54; IAG×32 yday $21.14 → 09:30 $21.44 +9.60; KGC×21 yday $32.76 → 09:30 $33.21 +9.45; NFGC×360 yday $1.84 → 09:30 $1.86 +7.20; WPM×4 yday $157.78 → 09:30 $158.96 +4.72; AU×2 yday $121.22 → 09:30 $120.50 -1.44; AUPH×18 yday $16.65 → 09:30 $16.60 -0.90; AEM×1 yday $216.06 → 09:30 $217.03 +0.97; ARCT×29 yday $13.45 → 09:30 $13.26 -5.51; AUTL×131 yday $2.41 → 09:30 $2.36 -6.55; CRDL×168 yday $1.86 → 09:30 $1.87 +1.68; CRSP×5 yday $59.50 → 09:30 $58.79 -3.55; CYPH×246 yday $1.42 → 09:30 $1.83 +100.86 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,820.80 | ▲ 09:30 equity $10,436.75 vs yday $10,372.49 (+64.26) | 09:30 open · cash $2,820.80 (unchanged overnight, no fees) · equity $10,436.75 vs prior close $10,372.49 (+64.26) because holdings re-marked: AG×30 yday $20.57 → 09:30 $20.73 +4.80; BHP×6 yday $96.66 → 09:30 $95.95 -4.26; CDE×30 yday $20.49 → 09:30 $20.85 +10.80; HDSN×109 yday $5.57 → 09:30 $5.53 -4.36; IAG×32 yday $21.36 → 09:30 $21.63 +8.64; KGC×21 yday $32.47 → 09:30 $32.76 +6.09; NFGC×360 yday $1.90 → 09:30 $1.91 +3.60; WPM×4 yday $158.00 → 09:30 $160.00 +8.00; AU×2 yday $118.66 → 09:30 $119.46 +1.60; AUPH×18 yday $16.60 → 09:30 $16.71 +1.98; AEM×1 yday $214.08 → 09:30 $200.48 -13.60; ARCT×29 yday $13.76 → 09:30 $14.34 +16.82; AUTL×131 yday $2.38 → 09:30 $2.32 -7.86; CRDL×168 yday $1.80 → 09:30 $1.90 +16.80; CRSP×5 yday $56.91 → 09:30 $57.00 +0.45; CYPH×246 yday $1.64 → 09:30 $1.70 +14.76 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 30 | $20.73 | $2.10 | $+1.22 | $3,440.60 | ▲ +1.22 after sell → book $10,434.65; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 6 | $95.95 | $2.03 | $+25.60 | $4,014.27 | ▲ +25.60 after sell → book $10,432.62; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 30 | $20.85 | $2.10 | $+1.82 | $4,637.67 | ▲ +1.82 after sell → book $10,430.52; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 109 | $5.53 | $2.35 | $-30.82 | $5,238.10 | ▼ -30.82 after sell → book $10,428.18; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 32 | $21.63 | $2.11 | $+59.81 | $5,928.15 | ▲ +59.81 after sell → book $10,426.07; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 21 | $32.76 | $2.07 | $+61.60 | $6,614.04 | ▲ +61.60 after sell → book $10,424.00; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 360 | $1.91 | $4.71 | $+48.24 | $7,296.93 | ▲ +48.24 after sell → book $10,419.29; vs 09:30 mark -4.71 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 4 | $160.00 | $2.02 | $+57.82 | $7,934.90 | ▲ +57.82 after sell → book $10,417.26; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 20 | $24.00 | $2.05 | — | $7,452.85 | — | deploy half leftover; list flatten; ⚪; ret5=+13.0; leftover $495.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 45 | $10.92 | $2.12 | — | $6,959.33 | — | deploy half leftover; list flatten; 🔵; ret5=+10.4; leftover $495.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 8 | $61.47 | $2.01 | — | $6,465.55 | — | deploy half leftover; list flatten; 🔵; ret5=+9.2; leftover $495.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 59 | $8.28 | $2.17 | — | $5,974.87 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+8.8; leftover $495.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 94 | $5.23 | $2.27 | — | $5,480.98 | — | deploy half leftover; list flatten; ret5=+10.7; leftover $495.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 1 | $429.24 | $1.99 | — | $5,049.74 | — | deploy half leftover; list flatten; ret5=+6.1; leftover $495.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 306 | $1.62 | $3.95 | — | $4,550.07 | — | deploy half leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $495.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 247 | $2.00 | $3.19 | — | $4,052.89 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $495.93 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,052.89 | ▲ 09:30 equity $10,396.63 vs yday $10,396.63 (-0.00) | 09:30 open · cash $4,052.89 (unchanged overnight, no fees) · equity $10,396.63 vs prior close $10,396.63 (-0.00) because holdings re-marked: AU×2 yday $118.55 → 09:30 $118.55 +0.00; AUPH×18 yday $16.71 → 09:30 $16.71 +0.00; AEM×1 yday $215.40 → 09:30 $215.40 +0.00; ARCT×29 yday $14.21 → 09:30 $14.21 +0.00; AUTL×131 yday $2.34 → 09:30 $2.34 +0.00; CRDL×168 yday $1.90 → 09:30 $1.90 +0.00; CRSP×5 yday $57.03 → 09:30 $57.03 +0.00; CYPH×246 yday $1.64 → 09:30 $1.64 +0.00; MOS×20 yday $23.75 → 09:30 $23.75 +0.00; OCUL×45 yday $10.92 → 09:30 $10.92 +0.00; INSP×8 yday $61.47 → 09:30 $61.47 +0.00; CRMD×59 yday $8.28 → 09:30 $8.28 +0.00; RZLT×94 yday $5.29 → 09:30 $5.29 +0.00; HCA×1 yday $428.50 → 09:30 $428.50 +0.00; BMEA×306 yday $1.61 → 09:30 $1.61 +0.00; NPWR×247 yday $2.02 → 09:30 $2.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,052.89 | ▲ 09:30 equity $10,488.15 vs yday $10,397.51 (+90.64) | 09:30 open · cash $4,052.89 (unchanged overnight, no fees) · equity $10,488.15 vs prior close $10,397.51 (+90.64) because holdings re-marked: AU×2 yday $118.55 → 09:30 $119.80 +2.50; AUPH×18 yday $16.71 → 09:30 $16.60 -1.98; AEM×1 yday $215.40 → 09:30 $219.50 +4.10; ARCT×29 yday $14.21 → 09:30 $15.35 +33.06; AUTL×131 yday $2.34 → 09:30 $2.41 +9.17; CRDL×168 yday $1.90 → 09:30 $2.03 +21.84; CRSP×5 yday $57.03 → 09:30 $60.18 +15.75; CYPH×246 yday $1.64 → 09:30 $1.60 -9.84; MOS×20 yday $23.75 → 09:30 $24.84 +21.80; OCUL×45 yday $10.92 → 09:30 $10.79 -5.85; INSP×8 yday $61.47 → 09:30 $60.07 -11.20; CRMD×59 yday $8.28 → 09:30 $8.60 +18.88; RZLT×94 yday $5.29 → 09:30 $5.01 -26.32; HCA×1 yday $428.50 → 09:30 $427.50 -1.00; BMEA×306 yday $1.61 → 09:30 $1.75 +42.84; NPWR×247 yday $2.02 → 09:30 $1.93 -22.23 | — |
| 2026-08-27 09:30 ET | **SELL** | `AU` | 2 | $119.80 | $2.02 | $-3.27 | $4,290.47 | ▼ -3.27 after sell → book $10,486.13; vs 09:30 mark -2.02 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 18 | $16.60 | $2.06 | $-14.91 | $4,587.21 | ▼ -14.91 after sell → book $10,484.07; vs 09:30 mark -2.06 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AEM` | 1 | $219.50 | $2.01 | $-0.81 | $4,804.70 | ▼ -0.81 after sell → book $10,482.06; vs 09:30 mark -2.01 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 29 | $15.35 | $2.10 | $+118.21 | $5,247.75 | ▲ +118.21 after sell → book $10,479.96; vs 09:30 mark -2.10 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 131 | $2.41 | $2.41 | $-12.66 | $5,561.04 | ▼ -12.66 after sell → book $10,477.54; vs 09:30 mark -2.42 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 168 | $2.03 | $2.53 | $+11.77 | $5,899.55 | ▲ +11.77 after sell → book $10,475.01; vs 09:30 mark -2.53 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRSP` | 5 | $60.18 | $2.02 | $-1.73 | $6,198.43 | ▼ -1.73 after sell → book $10,472.99; vs 09:30 mark -2.02 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 246 | $1.60 | $3.22 | $+62.48 | $6,588.80 | ▲ +62.48 after sell → book $10,469.76; vs 09:30 mark -3.23 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 11 | $40.72 | $2.02 | — | $6,138.86 | — | deploy half leftover; list flatten; ret5=+1.8; leftover $470.63 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 33 | $14.09 | $2.09 | — | $5,671.80 | — | deploy half leftover; list flatten; ret5=+1.1; leftover $470.63 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 181 | $2.59 | $2.53 | — | $5,200.48 | — | deploy half leftover; list flatten; ret5=+4.2; leftover $470.63 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 5 | $80.97 | $2.00 | — | $4,793.62 | — | deploy half leftover; list mover_buy; 🔵; ret5=-1.3; leftover $470.63 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 106 | $4.42 | $2.31 | — | $4,322.79 | — | deploy half leftover; list mover_buy; 🔵; ret5=-8.6; leftover $470.63 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 6 | $75.12 | $2.01 | — | $3,870.07 | — | deploy half leftover; list mover_buy; 🔵; ret5=-2.2; leftover $470.63 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,870.07 | ▲ 09:30 equity $10,453.98 vs yday $10,422.03 (+31.95) | 09:30 open · cash $3,870.07 (unchanged overnight, no fees) · equity $10,453.98 vs prior close $10,422.03 (+31.95) because holdings re-marked: MOS×20 yday $24.16 → 09:30 $24.00 -3.20; OCUL×45 yday $10.77 → 09:30 $10.63 -6.30; INSP×8 yday $61.80 → 09:30 $62.10 +2.40; CRMD×59 yday $8.39 → 09:30 $8.49 +5.90; RZLT×94 yday $5.04 → 09:30 $5.07 +2.82; HCA×1 yday $427.16 → 09:30 $424.61 -2.55; BMEA×306 yday $1.71 → 09:30 $1.74 +9.18; NPWR×247 yday $1.81 → 09:30 $1.83 +4.94; RRC×11 yday $41.55 → 09:30 $41.44 -1.21; CRK×33 yday $14.50 → 09:30 $14.42 -2.64; SLI×181 yday $2.61 → 09:30 $2.60 -1.81; ACMR×5 yday $79.11 → 09:30 $81.65 +12.70; GGB×106 yday $4.46 → 09:30 $4.57 +11.66; MT×6 yday $74.53 → 09:30 $74.54 +0.06 | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 45 | $10.63 | $2.15 | $-17.32 | $4,346.27 | ▼ -17.32 after sell → book $10,451.83; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 8 | $62.10 | $2.03 | $+0.99 | $4,841.04 | ▲ +0.99 after sell → book $10,449.80; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 59 | $8.49 | $2.19 | $+8.04 | $5,339.76 | ▲ +8.04 after sell → book $10,447.61; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 94 | $5.07 | $2.30 | $-19.61 | $5,814.04 | ▼ -19.61 after sell → book $10,445.31; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 1 | $424.61 | $2.01 | $-8.64 | $6,236.64 | ▼ -8.64 after sell → book $10,443.30; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 306 | $1.74 | $4.01 | $+28.76 | $6,765.07 | ▲ +28.76 after sell → book $10,439.29; vs 09:30 mark -4.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 247 | $1.83 | $3.24 | $-48.41 | $7,213.84 | ▼ -48.41 after sell → book $10,436.05; vs 09:30 mark -3.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 6 | $144.70 | $2.01 | — | $6,343.64 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $901.73 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 53 | $16.95 | $2.15 | — | $5,443.14 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $901.73 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 48 | $18.50 | $2.13 | — | $4,553.00 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $901.73 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 98 | $9.19 | $2.28 | — | $3,650.10 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $901.73 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,650.10 | ▼ 09:30 equity $10,314.50 vs yday $10,470.81 (-156.31) | 09:30 open · cash $3,650.10 (unchanged overnight, no fees) · equity $10,314.50 vs prior close $10,470.81 (-156.31) because holdings re-marked: MOS×20 yday $23.76 → 09:30 $23.75 -0.20; RRC×11 yday $41.64 → 09:30 $41.11 -5.83; CRK×33 yday $14.62 → 09:30 $14.56 -1.98; SLI×181 yday $2.64 → 09:30 $2.51 -23.53; ACMR×5 yday $80.49 → 09:30 $75.10 -26.95; GGB×106 yday $4.70 → 09:30 $4.55 -15.90; MT×6 yday $74.63 → 09:30 $75.07 +2.64; ANF×6 yday $145.75 → 09:30 $148.67 +17.52; BHVN×53 yday $16.12 → 09:30 $15.44 -36.04; BZ×48 yday $18.00 → 09:30 $17.89 -5.28; CAPR×98 yday $10.06 → 09:30 $9.44 -60.76 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 20 | $23.75 | $2.07 | $-9.12 | $4,123.03 | ▼ -9.12 after sell → book $10,312.43; vs 09:30 mark -2.07 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,123.03 | ▲ 09:30 equity $10,358.77 vs yday $10,311.87 (+46.90) | 09:30 open · cash $4,123.03 (unchanged overnight, no fees) · equity $10,358.77 vs prior close $10,311.87 (+46.90) because holdings re-marked: RRC×11 yday $41.78 → 09:30 $41.32 -5.06; CRK×33 yday $14.51 → 09:30 $14.31 -6.60; SLI×181 yday $2.51 → 09:30 $2.70 +34.39; ACMR×5 yday $75.02 → 09:30 $71.24 -18.90; GGB×106 yday $4.55 → 09:30 $4.61 +6.36; MT×6 yday $75.06 → 09:30 $74.31 -4.50; ANF×6 yday $149.28 → 09:30 $142.47 -40.86; BHVN×53 yday $15.40 → 09:30 $15.45 +2.65; BZ×48 yday $17.90 → 09:30 $17.37 -25.44; CAPR×98 yday $9.36 → 09:30 $10.43 +104.86 | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 11 | $41.32 | $2.04 | $+2.53 | $4,575.51 | ▲ +2.53 after sell → book $10,356.73; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 33 | $14.31 | $2.11 | $+3.06 | $5,045.63 | ▲ +3.06 after sell → book $10,354.62; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 181 | $2.70 | $2.57 | $+14.80 | $5,531.75 | ▲ +14.80 after sell → book $10,352.04; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `ACMR` | 5 | $71.24 | $2.02 | $-52.68 | $5,885.93 | ▼ -52.68 after sell → book $10,350.02; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 106 | $4.61 | $2.34 | $+15.50 | $6,372.25 | ▲ +15.50 after sell → book $10,347.68; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MT` | 6 | $74.31 | $2.03 | $-8.90 | $6,816.09 | ▼ -8.90 after sell → book $10,345.66; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,816.09 | ▲ 09:30 equity $10,369.14 vs yday $10,315.72 (+53.42) | 09:30 open · cash $6,816.09 (unchanged overnight, no fees) · equity $10,369.14 vs prior close $10,315.72 (+53.42) because holdings re-marked: ANF×6 yday $143.00 → 09:30 $142.00 -6.00; BHVN×53 yday $15.45 → 09:30 $15.39 -3.18; BZ×48 yday $17.17 → 09:30 $17.29 +5.76; CAPR×98 yday $10.19 → 09:30 $10.77 +56.84 | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 6 | $142.00 | $2.03 | $-20.24 | $7,666.06 | ▼ -20.24 after sell → book $10,367.11; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 53 | $15.39 | $2.17 | $-87.00 | $8,479.56 | ▼ -87.00 after sell → book $10,364.94; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 48 | $17.29 | $2.15 | $-62.37 | $9,307.32 | ▼ -62.37 after sell → book $10,362.78; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 98 | $10.77 | $2.31 | $+150.25 | $10,360.47 | ▲ +150.25 after sell → book $10,360.47; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,360.47 | ▲ 09:30 equity $10,360.47 vs yday $10,360.47 (+0.00) | 09:30 open · cash $10,360.47 · no holdings · equity $10,360.47 vs prior close $10,360.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 13 | $49.76 | $2.03 | — | $9,711.57 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $647.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 15 | $41.31 | $2.04 | — | $9,089.88 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $647.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 198 | $3.27 | $2.58 | — | $8,439.84 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $647.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 84 | $7.70 | $2.24 | — | $7,790.79 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $647.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 5 | $125.94 | $2.00 | — | $7,159.09 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $647.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 530 | $1.22 | $6.84 | — | $6,505.65 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $647.53 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 35 | $18.40 | $2.10 | — | $5,859.56 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $647.53 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 41 | $15.70 | $2.11 | — | $5,213.74 | — | deploy half leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $647.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,213.74 | ▲ 09:30 equity $10,816.94 vs yday $10,737.69 (+79.25) | 09:30 open · cash $5,213.74 (unchanged overnight, no fees) · equity $10,816.94 vs prior close $10,737.69 (+79.25) because holdings re-marked: ATRC×13 yday $52.59 → 09:30 $52.88 +3.77; HRMY×15 yday $42.86 → 09:30 $42.93 +1.05; CABA×198 yday $3.57 → 09:30 $3.63 +11.88; VSTM×84 yday $8.02 → 09:30 $8.03 +0.84; RVTY×5 yday $130.94 → 09:30 $132.45 +7.55; GPRO×530 yday $1.69 → 09:30 $1.78 +47.70; FRVO×35 yday $17.98 → 09:30 $18.27 +10.15; CRK×41 yday $15.54 → 09:30 $15.45 -3.69 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 1 | $266.94 | $1.99 | — | $4,944.81 | — | deploy half leftover; list flatten; ret5=+1.9; leftover $521.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 17 | $30.65 | $2.04 | — | $4,421.72 | — | deploy half leftover; list flatten; 🔵; ret5=-2.2; leftover $521.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 50 | $10.41 | $2.14 | — | $3,899.08 | — | deploy half leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $521.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 35 | $14.50 | $2.10 | — | $3,389.49 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $521.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 267 | $1.95 | $3.44 | — | $2,865.39 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $521.37 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| 2026-08-14 | `TLN` | cash | leftover split 318.86 < 1 share @ 359.83 |
| 2026-08-14 | `DAVE` | cash | leftover split 318.86 < 1 share @ 330.91 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SLG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `VST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SLG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `FANG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ELF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `FANG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ELF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRSP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AU` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AEM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRSP` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `MU` | cash | leftover split 470.63 < 1 share @ 925.74 |
| 2026-08-28 | `ACMR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ACMR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 13 | 2026-09-03 @ $49.76 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $647.53 |
| `HRMY` | 15 | 2026-09-03 @ $41.31 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $647.53 |
| `CABA` | 198 | 2026-09-03 @ $3.27 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $647.53 |
| `VSTM` | 84 | 2026-09-03 @ $7.70 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $647.53 |
| `RVTY` | 5 | 2026-09-03 @ $125.94 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $647.53 |
| `GPRO` | 530 | 2026-09-03 @ $1.22 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $647.53 |
| `FRVO` | 35 | 2026-09-03 @ $18.40 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $647.53 |
| `CRK` | 41 | 2026-09-03 @ $15.70 | deploy half leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $647.53 |
| `ASND` | 1 | 2026-09-04 @ $266.94 | deploy half leftover; list flatten; ret5=+1.9; leftover $521.37 |
| `OSCR` | 17 | 2026-09-04 @ $30.65 | deploy half leftover; list flatten; 🔵; ret5=-2.2; leftover $521.37 |
| `NVAX` | 50 | 2026-09-04 @ $10.41 | deploy half leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $521.37 |
| `BVS` | 35 | 2026-09-04 @ $14.50 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $521.37 |
| `BAK` | 267 | 2026-09-04 @ $1.95 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $521.37 |
