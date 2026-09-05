# Factor mine action — `flatten_h3_half`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `half` · sell `list` · S-boost `none` · deploy half leftover

Cash book **+7.04%** ($10,704) · signal-only (no cash/fees) was +44.29%. Starts YES **16/17**. Fills 103 · skips 138 · realized $+480.78.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `half` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $2,795.10.

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
| 2026-08-25 | +1.80 | $2,820.80 | AG×30, BHP×6, CDE×30, HDSN×109, IAG×32, KGC×21, NFGC×360, WPM×4, AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246 | $10,436.75 | +64.26 | MOS, OCUL, INSP, CRMD, RZLT, HCA | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $4,261.81 | $10,401.82 | AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246, MOS×27, OCUL×60, INSP×10, CRMD×79, RZLT×126, HCA×1 | 09:30 open · cash $2,820.80 (unchanged overnight, no fees) · equity $10,436.75 vs prior close $10,372.49 (+64.26) because holdings re-marked: AG×30 yday $20.57 → 09:30 $20.73 +4.80; BHP×6 yday $96.66 → 09:30 $95.95 -4.26; CDE×30 yday $20.49 → 09:30 $20.85 +10.80; HDSN×109 yday $5.57 → 09:30 $5.53 -4.36; IAG×32 yday $21.36 → 09:30 $21.63 +8.64; KGC×21 yday $32.47 → 09:30 $32.76 +6.09; NFGC×360 yday $1.90 → 09:30 $1.91 +3.60; WPM×4 yday $158.00 → 09:30 $160.00 +8.00; AU×2 yday $118.66 → 09:30 $119.46 +1.60; AUPH×18 yday $16.60 → 09:30 $16.71 +1.98; AEM×1 yday $214.08 → 09:30 $200.48 -13.60; ARCT×29 yday $13.76 → 09:30 $14.34 +16.82; AUTL×131 yday $2.38 → 09:30 $2.32 -7.86; CRDL×168 yday $1.80 → 09:30 $1.90 +16.80; CRSP×5 yday $56.91 → 09:30 $57.00 +0.45; CYPH×246 yday $1.64 → 09:30 $1.70 +14.76 |
| 2026-08-26 | +2.02 | $4,261.81 | AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246, MOS×27, OCUL×60, INSP×10, CRMD×79, RZLT×126, HCA×1 | $10,401.82 | +0.00 | — | — | $4,261.81 | $10,404.41 | AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246, MOS×27, OCUL×60, INSP×10, CRMD×79, RZLT×126, HCA×1 | 09:30 open · cash $4,261.81 (unchanged overnight, no fees) · equity $10,401.82 vs prior close $10,401.82 (+0.00) because holdings re-marked: AU×2 yday $118.55 → 09:30 $118.55 +0.00; AUPH×18 yday $16.71 → 09:30 $16.71 +0.00; AEM×1 yday $215.40 → 09:30 $215.40 +0.00; ARCT×29 yday $14.21 → 09:30 $14.21 +0.00; AUTL×131 yday $2.34 → 09:30 $2.34 +0.00; CRDL×168 yday $1.90 → 09:30 $1.90 +0.00; CRSP×5 yday $57.03 → 09:30 $57.03 +0.00; CYPH×246 yday $1.64 → 09:30 $1.64 +0.00; MOS×27 yday $23.75 → 09:30 $23.75 +0.00; OCUL×60 yday $10.92 → 09:30 $10.92 +0.00; INSP×10 yday $61.47 → 09:30 $61.47 +0.00; CRMD×79 yday $8.28 → 09:30 $8.28 +0.00; RZLT×126 yday $5.29 → 09:30 $5.29 +0.00; HCA×1 yday $428.50 → 09:30 $428.50 +0.00 |
| 2026-08-27 | — | $4,261.81 | AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246, MOS×27, OCUL×60, INSP×10, CRMD×79, RZLT×126, HCA×1 | $10,473.05 | +68.64 | RRC, CRK, SLI | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $3,429.32 | $10,493.27 | MOS×27, OCUL×60, INSP×10, CRMD×79, RZLT×126, HCA×1, RRC×27, CRK×80, SLI×437 | 09:30 open · cash $4,261.81 (unchanged overnight, no fees) · equity $10,473.05 vs prior close $10,404.41 (+68.64) because holdings re-marked: AU×2 yday $118.55 → 09:30 $119.80 +2.50; AUPH×18 yday $16.71 → 09:30 $16.60 -1.98; AEM×1 yday $215.40 → 09:30 $219.50 +4.10; ARCT×29 yday $14.21 → 09:30 $15.35 +33.06; AUTL×131 yday $2.34 → 09:30 $2.41 +9.17; CRDL×168 yday $1.90 → 09:30 $2.03 +21.84; CRSP×5 yday $57.03 → 09:30 $60.18 +15.75; CYPH×246 yday $1.64 → 09:30 $1.60 -9.84; MOS×27 yday $23.75 → 09:30 $24.84 +29.43; OCUL×60 yday $10.92 → 09:30 $10.79 -7.80; INSP×10 yday $61.47 → 09:30 $60.07 -14.00; CRMD×79 yday $8.28 → 09:30 $8.60 +25.28; RZLT×126 yday $5.29 → 09:30 $5.01 -35.28; HCA×1 yday $428.50 → 09:30 $427.50 -1.00 |
| 2026-08-28 | +0.75 | $3,429.32 | MOS×27, OCUL×60, INSP×10, CRMD×79, RZLT×126, HCA×1, RRC×27, CRK×80, SLI×437 | $10,478.94 | -14.33 | — | OCUL, INSP, CRMD, RZLT, HCA | $6,411.37 | $10,500.45 | MOS×27, RRC×27, CRK×80, SLI×437 | 09:30 open · cash $3,429.32 (unchanged overnight, no fees) · equity $10,478.94 vs prior close $10,493.27 (-14.33) because holdings re-marked: MOS×27 yday $24.16 → 09:30 $24.00 -4.32; OCUL×60 yday $10.77 → 09:30 $10.63 -8.40; INSP×10 yday $61.80 → 09:30 $62.10 +3.00; CRMD×79 yday $8.39 → 09:30 $8.49 +7.90; RZLT×126 yday $5.04 → 09:30 $5.07 +3.78; HCA×1 yday $427.16 → 09:30 $424.61 -2.55; RRC×27 yday $41.55 → 09:30 $41.44 -2.97; CRK×80 yday $14.50 → 09:30 $14.42 -6.40; SLI×437 yday $2.61 → 09:30 $2.60 -4.37 |
| 2026-08-31 | -5.85 | $6,411.37 | MOS×27, RRC×27, CRK×80, SLI×437 | $10,424.26 | -76.19 | — | MOS | $7,050.53 | $10,436.26 | RRC×27, CRK×80, SLI×437 | 09:30 open · cash $6,411.37 (unchanged overnight, no fees) · equity $10,424.26 vs prior close $10,500.45 (-76.19) because holdings re-marked: MOS×27 yday $23.76 → 09:30 $23.75 -0.27; RRC×27 yday $41.64 → 09:30 $41.11 -14.31; CRK×80 yday $14.62 → 09:30 $14.56 -4.80; SLI×437 yday $2.64 → 09:30 $2.51 -56.81 |
| 2026-09-01 | -6.30 | $7,050.53 | RRC×27, CRK×80, SLI×437 | $10,490.87 | +54.61 | — | RRC, CRK, SLI | $10,480.80 | $10,480.80 | — | 09:30 open · cash $7,050.53 (unchanged overnight, no fees) · equity $10,490.87 vs prior close $10,436.26 (+54.61) because holdings re-marked: RRC×27 yday $41.78 → 09:30 $41.32 -12.42; CRK×80 yday $14.51 → 09:30 $14.31 -16.00; SLI×437 yday $2.51 → 09:30 $2.70 +83.03 |
| 2026-09-02 | -3.83 | $10,480.80 | — | $10,480.80 | +0.00 | — | — | $10,480.80 | $10,480.80 | — | 09:30 open · cash $10,480.80 · no holdings · equity $10,480.80 vs prior close $10,480.80 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $10,480.80 | — | $10,480.80 | +0.00 | ATRC, HRMY, CABA, VSTM, RVTY | — | $5,289.31 | $10,745.84 | ATRC×21, HRMY×25, CABA×320, VSTM×136, RVTY×8 | 09:30 open · cash $10,480.80 · no holdings · equity $10,480.80 vs prior close $10,480.80 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $5,289.31 | ATRC×21, HRMY×25, CABA×320, VSTM×136, RVTY×8 | $10,786.32 | +40.48 | ASND, OSCR, NVAX, BVS | — | $2,795.10 | $10,704.08 | ATRC×21, HRMY×25, CABA×320, VSTM×136, RVTY×8, ASND×2, OSCR×21, NVAX×63, BVS×45 | 09:30 open · cash $5,289.31 (unchanged overnight, no fees) · equity $10,786.32 vs prior close $10,745.84 (+40.48) because holdings re-marked: ATRC×21 yday $52.59 → 09:30 $52.88 +6.09; HRMY×25 yday $42.86 → 09:30 $42.93 +1.75; CABA×320 yday $3.57 → 09:30 $3.63 +19.20; VSTM×136 yday $8.02 → 09:30 $8.03 +1.36; RVTY×8 yday $130.94 → 09:30 $132.45 +12.08 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 10 | $59.80 | $2.02 | — | $9,399.98 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 13 | $45.98 | $2.03 | — | $8,800.21 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 12 | $50.62 | $2.03 | — | $8,190.71 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 12 | $49.70 | $2.03 | — | $7,592.28 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 53 | $11.70 | $2.15 | — | $6,970.03 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 21 | $29.74 | $2.05 | — | $6,343.44 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 771 | $0.81 | $8.56 | — | $5,710.37 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 26 | $23.33 | $2.07 | — | $5,101.72 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,101.72 | ▲ 09:30 equity $10,084.41 vs yday $10,071.15 (+13.26) | 09:30 open · cash $5,101.72 (unchanged overnight, no fees) · equity $10,084.41 vs prior close $10,071.15 (+13.26) because holdings re-marked: BTSG×10 yday $60.23 → 09:30 $59.65 -5.80; IREN×13 yday $44.76 → 09:30 $44.09 -8.71; TPG×12 yday $54.62 → 09:30 $55.29 +8.04; TGTX×12 yday $47.94 → 09:30 $47.27 -8.04; SLS×53 yday $12.36 → 09:30 $12.40 +2.12; HIMS×21 yday $28.77 → 09:30 $29.15 +7.98; INO×771 yday $0.90 → 09:30 $0.93 +23.13; TNDM×26 yday $23.13 → 09:30 $22.92 -5.46 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 2 | $146.90 | $2.00 | — | $4,805.93 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+3.6; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 2 | $120.00 | $2.00 | — | $4,563.93 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+0.6; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 5 | $57.61 | $2.00 | — | $4,273.88 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+5.7; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 35 | $9.01 | $2.10 | — | $3,956.43 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 340 | $0.94 | $4.21 | — | $3,633.64 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 212 | $1.50 | $2.73 | — | $3,312.91 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,312.91 | ▼ 09:30 equity $10,196.68 vs yday $10,212.64 (-15.96) | 09:30 open · cash $3,312.91 (unchanged overnight, no fees) · equity $10,196.68 vs prior close $10,212.64 (-15.96) because holdings re-marked: BTSG×10 yday $61.71 → 09:30 $61.69 -0.20; IREN×13 yday $44.06 → 09:30 $45.23 +15.21; TPG×12 yday $53.03 → 09:30 $52.67 -4.32; TGTX×12 yday $48.74 → 09:30 $48.74 +0.00; SLS×53 yday $12.78 → 09:30 $12.78 +0.00; HIMS×21 yday $28.15 → 09:30 $28.14 -0.21; INO×771 yday $1.09 → 09:30 $1.07 -15.42; TNDM×26 yday $22.72 → 09:30 $22.50 -5.72; VST×2 yday $148.13 → 09:30 $149.37 +2.48; NRG×2 yday $126.24 → 09:30 $127.40 +2.32; SLG×5 yday $56.09 → 09:30 $55.37 -3.60; MARA×35 yday $9.20 → 09:30 $9.22 +0.70; LDI×340 yday $0.90 → 09:30 $0.91 +3.40; BTBT×212 yday $1.57 → 09:30 $1.52 -10.60 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 4 | $46.18 | $1.86 | — | $3,126.33 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+6.7; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 1 | $142.77 | $1.43 | — | $2,982.13 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+5.8; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $2,777.44 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+8.3; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 51 | $4.05 | $2.14 | — | $2,568.74 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 24 | $8.46 | $2.06 | — | $2,363.64 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 2 | $90.54 | $1.82 | — | $2,180.75 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=-7.2; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 63 | $3.24 | $2.18 | — | $1,974.45 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 43 | $4.81 | $2.12 | — | $1,765.50 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $207.06 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 30 | $20.55 | $2.08 | — | $9,467.86 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 6 | $91.01 | $2.01 | — | $8,919.79 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 30 | $20.65 | $2.08 | — | $8,298.21 | — | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 109 | $5.77 | $2.32 | — | $7,666.96 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 32 | $19.63 | $2.09 | — | $7,036.72 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 21 | $29.63 | $2.05 | — | $6,412.43 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 360 | $1.75 | $4.64 | — | $5,777.79 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $5,197.63 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,197.63 | ▲ 09:30 equity $10,315.69 vs yday $10,182.57 (+133.12) | 09:30 open · cash $5,197.63 (unchanged overnight, no fees) · equity $10,315.69 vs prior close $10,182.57 (+133.12) because holdings re-marked: AG×30 yday $21.19 → 09:30 $21.90 +21.30; BHP×6 yday $93.63 → 09:30 $95.72 +12.54; CDE×30 yday $21.11 → 09:30 $21.75 +19.20; HDSN×109 yday $5.57 → 09:30 $5.67 +10.90; IAG×32 yday $20.50 → 09:30 $21.17 +21.44; KGC×21 yday $31.43 → 09:30 $32.17 +15.54; NFGC×360 yday $1.75 → 09:30 $1.79 +14.40; WPM×4 yday $150.25 → 09:30 $154.70 +17.80 | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 2 | $119.43 | $2.00 | — | $4,956.77 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+20.4; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 18 | $17.20 | $2.04 | — | $4,645.13 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 1 | $216.30 | $1.99 | — | $4,426.83 | — | deploy half leftover; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 29 | $11.13 | $2.08 | — | $4,101.99 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 131 | $2.47 | $2.38 | — | $3,776.03 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 168 | $1.93 | $2.49 | — | $3,449.30 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 5 | $59.72 | $2.00 | — | $3,148.69 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 246 | $1.32 | $3.17 | — | $2,820.80 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
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
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 27 | $24.00 | $2.07 | — | $7,284.83 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; leftover $661.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 60 | $10.92 | $2.17 | — | $6,627.46 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+10.4; leftover $661.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 10 | $61.47 | $2.02 | — | $6,010.74 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.2; leftover $661.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 79 | $8.28 | $2.23 | — | $5,354.40 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.8; leftover $661.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 126 | $5.23 | $2.37 | — | $4,693.05 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+10.7; leftover $661.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 1 | $429.24 | $1.99 | — | $4,261.81 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.1; leftover $661.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,261.81 | ▲ 09:30 equity $10,401.82 vs yday $10,401.82 (+0.00) | 09:30 open · cash $4,261.81 (unchanged overnight, no fees) · equity $10,401.82 vs prior close $10,401.82 (+0.00) because holdings re-marked: AU×2 yday $118.55 → 09:30 $118.55 +0.00; AUPH×18 yday $16.71 → 09:30 $16.71 +0.00; AEM×1 yday $215.40 → 09:30 $215.40 +0.00; ARCT×29 yday $14.21 → 09:30 $14.21 +0.00; AUTL×131 yday $2.34 → 09:30 $2.34 +0.00; CRDL×168 yday $1.90 → 09:30 $1.90 +0.00; CRSP×5 yday $57.03 → 09:30 $57.03 +0.00; CYPH×246 yday $1.64 → 09:30 $1.64 +0.00; MOS×27 yday $23.75 → 09:30 $23.75 +0.00; OCUL×60 yday $10.92 → 09:30 $10.92 +0.00; INSP×10 yday $61.47 → 09:30 $61.47 +0.00; CRMD×79 yday $8.28 → 09:30 $8.28 +0.00; RZLT×126 yday $5.29 → 09:30 $5.29 +0.00; HCA×1 yday $428.50 → 09:30 $428.50 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,261.81 | ▲ 09:30 equity $10,473.05 vs yday $10,404.41 (+68.64) | 09:30 open · cash $4,261.81 (unchanged overnight, no fees) · equity $10,473.05 vs prior close $10,404.41 (+68.64) because holdings re-marked: AU×2 yday $118.55 → 09:30 $119.80 +2.50; AUPH×18 yday $16.71 → 09:30 $16.60 -1.98; AEM×1 yday $215.40 → 09:30 $219.50 +4.10; ARCT×29 yday $14.21 → 09:30 $15.35 +33.06; AUTL×131 yday $2.34 → 09:30 $2.41 +9.17; CRDL×168 yday $1.90 → 09:30 $2.03 +21.84; CRSP×5 yday $57.03 → 09:30 $60.18 +15.75; CYPH×246 yday $1.64 → 09:30 $1.60 -9.84; MOS×27 yday $23.75 → 09:30 $24.84 +29.43; OCUL×60 yday $10.92 → 09:30 $10.79 -7.80; INSP×10 yday $61.47 → 09:30 $60.07 -14.00; CRMD×79 yday $8.28 → 09:30 $8.60 +25.28; RZLT×126 yday $5.29 → 09:30 $5.01 -35.28; HCA×1 yday $428.50 → 09:30 $427.50 -1.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `AU` | 2 | $119.80 | $2.02 | $-3.27 | $4,499.40 | ▼ -3.27 after sell → book $10,471.04; vs 09:30 mark -2.01 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 18 | $16.60 | $2.06 | $-14.91 | $4,796.13 | ▼ -14.91 after sell → book $10,468.97; vs 09:30 mark -2.07 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AEM` | 1 | $219.50 | $2.01 | $-0.81 | $5,013.62 | ▼ -0.81 after sell → book $10,466.96; vs 09:30 mark -2.01 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 29 | $15.35 | $2.10 | $+118.21 | $5,456.67 | ▲ +118.21 after sell → book $10,464.86; vs 09:30 mark -2.10 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 131 | $2.41 | $2.41 | $-12.66 | $5,769.97 | ▼ -12.66 after sell → book $10,462.45; vs 09:30 mark -2.41 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 168 | $2.03 | $2.53 | $+11.77 | $6,108.48 | ▲ +11.77 after sell → book $10,459.92; vs 09:30 mark -2.53 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRSP` | 5 | $60.18 | $2.02 | $-1.73 | $6,407.35 | ▼ -1.73 after sell → book $10,457.89; vs 09:30 mark -2.03 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 246 | $1.60 | $3.22 | $+62.48 | $6,797.73 | ▲ +62.48 after sell → book $10,454.67; vs 09:30 mark -3.22 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 27 | $40.72 | $2.07 | — | $5,696.22 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.8; leftover $1132.95 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 80 | $14.09 | $2.23 | — | $4,566.79 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.1; leftover $1132.95 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 437 | $2.59 | $5.64 | — | $3,429.32 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.2; leftover $1132.95 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,429.32 | ▼ 09:30 equity $10,478.94 vs yday $10,493.27 (-14.33) | 09:30 open · cash $3,429.32 (unchanged overnight, no fees) · equity $10,478.94 vs prior close $10,493.27 (-14.33) because holdings re-marked: MOS×27 yday $24.16 → 09:30 $24.00 -4.32; OCUL×60 yday $10.77 → 09:30 $10.63 -8.40; INSP×10 yday $61.80 → 09:30 $62.10 +3.00; CRMD×79 yday $8.39 → 09:30 $8.49 +7.90; RZLT×126 yday $5.04 → 09:30 $5.07 +3.78; HCA×1 yday $427.16 → 09:30 $424.61 -2.55; RRC×27 yday $41.55 → 09:30 $41.44 -2.97; CRK×80 yday $14.50 → 09:30 $14.42 -6.40; SLI×437 yday $2.61 → 09:30 $2.60 -4.37 | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 60 | $10.63 | $2.19 | $-21.76 | $4,064.93 | ▼ -21.76 after sell → book $10,476.75; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 10 | $62.10 | $2.04 | $+2.24 | $4,683.89 | ▲ +2.24 after sell → book $10,474.71; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 79 | $8.49 | $2.25 | $+12.11 | $5,352.35 | ▲ +12.11 after sell → book $10,472.46; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 126 | $5.07 | $2.40 | $-24.93 | $5,988.77 | ▼ -24.93 after sell → book $10,470.06; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 1 | $424.61 | $2.01 | $-8.64 | $6,411.37 | ▼ -8.64 after sell → book $10,468.05; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,411.37 | ▼ 09:30 equity $10,424.26 vs yday $10,500.45 (-76.19) | 09:30 open · cash $6,411.37 (unchanged overnight, no fees) · equity $10,424.26 vs prior close $10,500.45 (-76.19) because holdings re-marked: MOS×27 yday $23.76 → 09:30 $23.75 -0.27; RRC×27 yday $41.64 → 09:30 $41.11 -14.31; CRK×80 yday $14.62 → 09:30 $14.56 -4.80; SLI×437 yday $2.64 → 09:30 $2.51 -56.81 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 27 | $23.75 | $2.09 | $-10.91 | $7,050.53 | ▼ -10.91 after sell → book $10,422.17; vs 09:30 mark -2.09 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,050.53 | ▲ 09:30 equity $10,490.87 vs yday $10,436.26 (+54.61) | 09:30 open · cash $7,050.53 (unchanged overnight, no fees) · equity $10,490.87 vs prior close $10,436.26 (+54.61) because holdings re-marked: RRC×27 yday $41.78 → 09:30 $41.32 -12.42; CRK×80 yday $14.51 → 09:30 $14.31 -16.00; SLI×437 yday $2.51 → 09:30 $2.70 +83.03 | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 27 | $41.32 | $2.09 | $+12.04 | $8,164.08 | ▲ +12.04 after sell → book $10,488.78; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 80 | $14.31 | $2.25 | $+13.12 | $9,306.62 | ▲ +13.12 after sell → book $10,486.52; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 437 | $2.70 | $5.72 | $+36.71 | $10,480.80 | ▲ +36.71 after sell → book $10,480.80; vs 09:30 mark -5.72 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,480.80 | ▲ 09:30 equity $10,480.80 vs yday $10,480.80 (+0.00) | 09:30 open · cash $10,480.80 · no holdings · equity $10,480.80 vs prior close $10,480.80 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,480.80 | ▲ 09:30 equity $10,480.80 vs yday $10,480.80 (+0.00) | 09:30 open · cash $10,480.80 · no holdings · equity $10,480.80 vs prior close $10,480.80 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 21 | $49.76 | $2.05 | — | $9,433.79 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $1048.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 25 | $41.31 | $2.06 | — | $8,398.97 | — | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $1048.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 320 | $3.27 | $4.13 | — | $7,348.45 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $1048.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 136 | $7.70 | $2.40 | — | $6,298.85 | — | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $1048.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 8 | $125.94 | $2.01 | — | $5,289.31 | — | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $1048.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,289.31 | ▲ 09:30 equity $10,786.32 vs yday $10,745.84 (+40.48) | 09:30 open · cash $5,289.31 (unchanged overnight, no fees) · equity $10,786.32 vs prior close $10,745.84 (+40.48) because holdings re-marked: ATRC×21 yday $52.59 → 09:30 $52.88 +6.09; HRMY×25 yday $42.86 → 09:30 $42.93 +1.75; CABA×320 yday $3.57 → 09:30 $3.63 +19.20; VSTM×136 yday $8.02 → 09:30 $8.03 +1.36; RVTY×8 yday $130.94 → 09:30 $132.45 +12.08 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 2 | $266.94 | $2.00 | — | $4,753.44 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.9; leftover $661.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 21 | $30.65 | $2.05 | — | $4,107.74 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-2.2; leftover $661.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 63 | $10.41 | $2.18 | — | $3,449.73 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $661.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 45 | $14.50 | $2.12 | — | $2,795.10 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.8; leftover $661.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

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
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 21 | 2026-09-03 @ $49.76 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $1048.08 |
| `HRMY` | 25 | 2026-09-03 @ $41.31 | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $1048.08 |
| `CABA` | 320 | 2026-09-03 @ $3.27 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $1048.08 |
| `VSTM` | 136 | 2026-09-03 @ $7.70 | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $1048.08 |
| `RVTY` | 8 | 2026-09-03 @ $125.94 | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $1048.08 |
| `ASND` | 2 | 2026-09-04 @ $266.94 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.9; leftover $661.16 |
| `OSCR` | 21 | 2026-09-04 @ $30.65 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-2.2; leftover $661.16 |
| `NVAX` | 63 | 2026-09-04 @ $10.41 | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $661.16 |
| `BVS` | 45 | 2026-09-04 @ $14.50 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.8; leftover $661.16 |
