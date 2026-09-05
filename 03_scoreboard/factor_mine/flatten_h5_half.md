# Factor mine action — `flatten_h5_half`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `half` · sell `list` · S-boost `none` · deploy half leftover

Cash book **+12.43%** ($11,243) · signal-only (no cash/fees) was +67.92%. Starts YES **16/17**. Fills 101 · skips 222 · realized $+1010.50.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `half` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $3,019.19.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $5,101.72 | $10,071.15 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $5,101.72 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26 | $10,084.41 | +13.26 | VST, NRG, SLG, MARA, LDI, BTBT | — | $3,312.91 | $10,212.64 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212 | 09:30 open · cash $5,101.72 (unchanged overnight, no fees) · equity $10,084.41 vs prior close $10,071.15 (+13.26) because holdings re-marked: BTSG×10 yday $60.23 → 09:30 $59.65 -5.80; IREN×13 yday $44.76 → 09:30 $44.09 -8.71; TPG×12 yday $54.62 → 09:30 $55.29 +8.04; TGTX×12 yday $47.94 → 09:30 $47.27 -8.04; SLS×53 yday $12.36 → 09:30 $12.40 +2.12; HIMS×21 yday $28.77 → 09:30 $29.15 +7.98; INO×771 yday $0.90 → 09:30 $0.93 +23.13; TNDM×26 yday $23.13 → 09:30 $22.92 -5.46 |
| 2026-08-17 | +2.25 | $3,312.91 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212 | $10,196.68 | -15.96 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | — | $1,765.50 | $10,250.96 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | 09:30 open · cash $3,312.91 (unchanged overnight, no fees) · equity $10,196.68 vs prior close $10,212.64 (-15.96) because holdings re-marked: BTSG×10 yday $61.71 → 09:30 $61.69 -0.20; IREN×13 yday $44.06 → 09:30 $45.23 +15.21; TPG×12 yday $53.03 → 09:30 $52.67 -4.32; TGTX×12 yday $48.74 → 09:30 $48.74 +0.00; SLS×53 yday $12.78 → 09:30 $12.78 +0.00; HIMS×21 yday $28.15 → 09:30 $28.14 -0.21; INO×771 yday $1.09 → 09:30 $1.07 -15.42; TNDM×26 yday $22.72 → 09:30 $22.50 -5.72; VST×2 yday $148.13 → 09:30 $149.37 +2.48; NRG×2 yday $126.24 → 09:30 $127.40 +2.32; SLG×5 yday $56.09 → 09:30 $55.37 -3.60; MARA×35 yday $9.20 → 09:30 $9.22 +0.70; LDI×340 yday $0.90 → 09:30 $0.91 +3.40; BTBT×212 yday $1.57 → 09:30 $1.52 -10.60 |
| 2026-08-18 | -6.20 | $1,765.50 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | $10,145.54 | -105.42 | — | — | $1,765.50 | $10,192.93 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | 09:30 open · cash $1,765.50 (unchanged overnight, no fees) · equity $10,145.54 vs prior close $10,250.96 (-105.42) because holdings re-marked: BTSG×10 yday $60.38 → 09:30 $60.00 -3.80; IREN×13 yday $44.90 → 09:30 $43.56 -17.42; TPG×12 yday $51.77 → 09:30 $51.77 +0.00; TGTX×12 yday $49.28 → 09:30 $49.28 +0.00; SLS×53 yday $13.00 → 09:30 $12.66 -18.02; HIMS×21 yday $28.61 → 09:30 $27.85 -15.96; INO×771 yday $1.15 → 09:30 $1.14 -7.71; TNDM×26 yday $22.25 → 09:30 $22.16 -2.47; VST×2 yday $146.11 → 09:30 $144.50 -3.22; NRG×2 yday $122.37 → 09:30 $121.92 -0.90; SLG×5 yday $56.11 → 09:30 $56.00 -0.55; MARA×35 yday $9.72 → 09:30 $9.36 -12.60; LDI×340 yday $0.88 → 09:30 $0.87 -1.70; BTBT×212 yday $1.60 → 09:30 $1.54 -12.72; DVN×4 yday $47.57 → 09:30 $48.00 +1.72; EOG×1 yday $146.15 → 09:30 $148.04 +1.89; FANG×1 yday $206.29 → 09:30 $208.93 +2.64; TMC×51 yday $3.77 → 09:30 $3.72 -2.55; TGB×24 yday $8.77 → 09:30 $8.55 -5.28; ELF×2 yday $93.66 → 09:30 $93.44 -0.44; DNN×63 yday $3.19 → 09:30 $3.11 -5.04; HNST×43 yday $4.70 → 09:30 $4.67 -1.29 |
| 2026-08-19 | -7.20 | $1,765.50 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | $10,289.35 | +96.42 | — | — | $1,765.50 | $10,495.12 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | 09:30 open · cash $1,765.50 (unchanged overnight, no fees) · equity $10,289.35 vs prior close $10,192.93 (+96.42) because holdings re-marked: BTSG×10 yday $59.50 → 09:30 $60.15 +6.50; IREN×13 yday $42.00 → 09:30 $41.41 -7.61; TPG×12 yday $52.02 → 09:30 $52.26 +2.88; TGTX×12 yday $50.26 → 09:30 $51.62 +16.32; SLS×53 yday $13.10 → 09:30 $13.46 +19.08; HIMS×21 yday $27.39 → 09:30 $27.55 +3.36; INO×771 yday $1.20 → 09:30 $1.22 +15.42; TNDM×26 yday $23.73 → 09:30 $24.20 +12.22; VST×2 yday $140.52 → 09:30 $140.74 +0.44; NRG×2 yday $115.56 → 09:30 $116.20 +1.28; SLG×5 yday $56.84 → 09:30 $57.50 +3.30; MARA×35 yday $8.96 → 09:30 $8.91 -1.75; LDI×340 yday $0.86 → 09:30 $0.88 +7.48; BTBT×212 yday $1.45 → 09:30 $1.42 -6.36; DVN×4 yday $47.83 → 09:30 $48.22 +1.56; EOG×1 yday $148.70 → 09:30 $149.86 +1.16; FANG×1 yday $210.02 → 09:30 $210.84 +0.82; TMC×51 yday $3.92 → 09:30 $3.93 +0.51; TGB×24 yday $8.36 → 09:30 $8.70 +8.16; ELF×2 yday $92.51 → 09:30 $96.00 +6.98; DNN×63 yday $3.15 → 09:30 $3.19 +2.52; HNST×43 yday $4.75 → 09:30 $4.80 +2.15 |
| 2026-08-20 | +1.12 | $1,765.50 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | $10,488.06 | -7.06 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $3,670.69 | $10,582.85 | VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43, AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3 | 09:30 open · cash $1,765.50 (unchanged overnight, no fees) · equity $10,488.06 vs prior close $10,495.12 (-7.06) because holdings re-marked: BTSG×10 yday $59.33 → 09:30 $58.64 -6.90; IREN×13 yday $42.84 → 09:30 $42.46 -4.94; TPG×12 yday $53.18 → 09:30 $53.06 -1.44; TGTX×12 yday $51.69 → 09:30 $51.65 -0.48; SLS×53 yday $13.85 → 09:30 $13.84 -0.53; HIMS×21 yday $31.09 → 09:30 $30.66 -9.03; INO×771 yday $1.30 → 09:30 $1.30 +0.00; TNDM×26 yday $23.46 → 09:30 $23.11 -9.10; VST×2 yday $142.70 → 09:30 $142.70 +0.00; NRG×2 yday $120.58 → 09:30 $119.96 -1.24; SLG×5 yday $57.65 → 09:30 $57.29 -1.80; MARA×35 yday $9.65 → 09:30 $10.21 +19.60; LDI×340 yday $0.88 → 09:30 $0.87 -1.70; BTBT×212 yday $1.40 → 09:30 $1.46 +11.66; DVN×4 yday $48.19 → 09:30 $49.02 +3.32; EOG×1 yday $149.48 → 09:30 $151.45 +1.97; FANG×1 yday $208.55 → 09:30 $213.51 +4.96; TMC×51 yday $3.97 → 09:30 $3.92 -2.55; TGB×24 yday $8.47 → 09:30 $8.35 -2.88; ELF×2 yday $99.65 → 09:30 $98.15 -3.00; DNN×63 yday $3.22 → 09:30 $3.20 -1.26; HNST×43 yday $5.02 → 09:30 $4.98 -1.72 |
| 2026-08-21 | +3.25 | $3,670.69 | VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43, AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3 | $10,738.62 | +155.77 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | VST, NRG, SLG, MARA, LDI, BTBT | $3,036.72 | $10,834.53 | DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43, AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261 | 09:30 open · cash $3,670.69 (unchanged overnight, no fees) · equity $10,738.62 vs prior close $10,582.85 (+155.77) because holdings re-marked: VST×2 yday $138.94 → 09:30 $139.99 +2.10; NRG×2 yday $115.36 → 09:30 $116.58 +2.44; SLG×5 yday $58.10 → 09:30 $58.63 +2.65; MARA×35 yday $11.15 → 09:30 $11.70 +19.25; LDI×340 yday $0.87 → 09:30 $0.87 -1.02; BTBT×212 yday $1.59 → 09:30 $1.66 +13.78; DVN×4 yday $49.30 → 09:30 $49.45 +0.60; EOG×1 yday $152.19 → 09:30 $152.29 +0.10; FANG×1 yday $211.02 → 09:30 $211.84 +0.82; TMC×51 yday $3.97 → 09:30 $4.10 +6.63; TGB×24 yday $8.69 → 09:30 $9.00 +7.44; ELF×2 yday $98.46 → 09:30 $99.02 +1.12; DNN×63 yday $3.14 → 09:30 $3.23 +5.67; HNST×43 yday $4.96 → 09:30 $4.97 +0.43; AG×21 yday $21.19 → 09:30 $21.90 +14.91; BHP×4 yday $93.63 → 09:30 $95.72 +8.36; CDE×21 yday $21.11 → 09:30 $21.75 +13.44; HDSN×77 yday $5.57 → 09:30 $5.67 +7.70; IAG×22 yday $20.50 → 09:30 $21.17 +14.74; KGC×15 yday $31.43 → 09:30 $32.17 +11.10; NFGC×254 yday $1.75 → 09:30 $1.79 +10.16; WPM×3 yday $150.25 → 09:30 $154.70 +13.35 |
| 2026-08-24 | -5.17 | $3,036.72 | DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43, AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261 | $10,954.46 | +119.93 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | $4,674.03 | $10,825.40 | AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261 | 09:30 open · cash $3,036.72 (unchanged overnight, no fees) · equity $10,954.46 vs prior close $10,834.53 (+119.93) because holdings re-marked: DVN×4 yday $49.10 → 09:30 $48.84 -1.04; EOG×1 yday $153.05 → 09:30 $152.61 -0.44; FANG×1 yday $210.72 → 09:30 $209.47 -1.25; TMC×51 yday $4.79 → 09:30 $4.57 -11.22; TGB×24 yday $9.19 → 09:30 $9.26 +1.68; ELF×2 yday $101.94 → 09:30 $101.53 -0.82; DNN×63 yday $3.50 → 09:30 $3.50 +0.00; HNST×43 yday $5.05 → 09:30 $5.05 +0.00; AG×21 yday $21.09 → 09:30 $21.47 +7.98; BHP×4 yday $97.03 → 09:30 $97.34 +1.24; CDE×21 yday $20.97 → 09:30 $21.26 +6.09; HDSN×77 yday $5.63 → 09:30 $5.69 +4.62; IAG×22 yday $21.14 → 09:30 $21.44 +6.60; KGC×15 yday $32.76 → 09:30 $33.21 +6.75; NFGC×254 yday $1.84 → 09:30 $1.86 +5.08; WPM×3 yday $157.78 → 09:30 $158.96 +3.54; AU×2 yday $121.22 → 09:30 $120.50 -1.44; AUPH×20 yday $16.65 → 09:30 $16.60 -1.00; AEM×1 yday $216.06 → 09:30 $217.03 +0.97; ARCT×30 yday $13.45 → 09:30 $13.26 -5.70; AUTL×139 yday $2.41 → 09:30 $2.36 -6.95; CRDL×178 yday $1.86 → 09:30 $1.87 +1.78; CRSP×5 yday $59.50 → 09:30 $58.79 -3.55; CYPH×261 yday $1.42 → 09:30 $1.83 +107.01 |
| 2026-08-25 | +1.80 | $4,674.03 | AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261 | $10,882.40 | +57.00 | MOS, OCUL, INSP, CRMD, RZLT | — | $2,752.35 | $10,847.92 | AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261, MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74 | 09:30 open · cash $4,674.03 (unchanged overnight, no fees) · equity $10,882.40 vs prior close $10,825.40 (+57.00) because holdings re-marked: AG×21 yday $20.57 → 09:30 $20.73 +3.36; BHP×4 yday $96.66 → 09:30 $95.95 -2.84; CDE×21 yday $20.49 → 09:30 $20.85 +7.56; HDSN×77 yday $5.57 → 09:30 $5.53 -3.08; IAG×22 yday $21.36 → 09:30 $21.63 +5.94; KGC×15 yday $32.47 → 09:30 $32.76 +4.35; NFGC×254 yday $1.90 → 09:30 $1.91 +2.54; WPM×3 yday $158.00 → 09:30 $160.00 +6.00; AU×2 yday $118.66 → 09:30 $119.46 +1.60; AUPH×20 yday $16.60 → 09:30 $16.71 +2.20; AEM×1 yday $214.08 → 09:30 $200.48 -13.60; ARCT×30 yday $13.76 → 09:30 $14.34 +17.40; AUTL×139 yday $2.38 → 09:30 $2.32 -8.34; CRDL×178 yday $1.80 → 09:30 $1.90 +17.80; CRSP×5 yday $56.91 → 09:30 $57.00 +0.45; CYPH×261 yday $1.64 → 09:30 $1.70 +15.66 |
| 2026-08-26 | +2.02 | $2,752.35 | AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261, MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74 | $10,847.92 | -0.00 | — | — | $2,752.35 | $10,871.92 | AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261, MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74 | 09:30 open · cash $2,752.35 (unchanged overnight, no fees) · equity $10,847.92 vs prior close $10,847.92 (-0.00) because holdings re-marked: AG×21 yday $20.68 → 09:30 $20.68 +0.00; BHP×4 yday $96.05 → 09:30 $96.05 +0.00; CDE×21 yday $20.71 → 09:30 $20.71 +0.00; HDSN×77 yday $5.49 → 09:30 $5.49 +0.00; IAG×22 yday $21.48 → 09:30 $21.48 +0.00; KGC×15 yday $32.55 → 09:30 $32.55 +0.00; NFGC×254 yday $1.90 → 09:30 $1.90 +0.00; WPM×3 yday $158.25 → 09:30 $158.25 +0.00; AU×2 yday $118.55 → 09:30 $118.55 +0.00; AUPH×20 yday $16.71 → 09:30 $16.71 +0.00; AEM×1 yday $215.40 → 09:30 $215.40 +0.00; ARCT×30 yday $14.21 → 09:30 $14.21 +0.00; AUTL×139 yday $2.34 → 09:30 $2.34 +0.00; CRDL×178 yday $1.90 → 09:30 $1.90 +0.00; CRSP×5 yday $57.03 → 09:30 $57.03 +0.00; CYPH×261 yday $1.64 → 09:30 $1.64 +0.00; MOS×16 yday $23.75 → 09:30 $23.75 +0.00; OCUL×35 yday $10.92 → 09:30 $10.92 +0.00; INSP×6 yday $61.47 → 09:30 $61.47 +0.00; CRMD×47 yday $8.28 → 09:30 $8.28 +0.00; RZLT×74 yday $5.29 → 09:30 $5.29 +0.00 |
| 2026-08-27 | — | $2,752.35 | AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261, MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74 | $10,976.06 | +104.14 | RRC, CRK, SLI | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $3,194.34 | $11,016.17 | AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261, MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74, RRC×26, CRK×75, SLI×410 | 09:30 open · cash $2,752.35 (unchanged overnight, no fees) · equity $10,976.06 vs prior close $10,871.92 (+104.14) because holdings re-marked: AG×21 yday $20.68 → 09:30 $20.63 -1.05; BHP×4 yday $96.05 → 09:30 $96.99 +3.76; CDE×21 yday $20.71 → 09:30 $21.00 +6.09; HDSN×77 yday $5.49 → 09:30 $5.51 +1.54; IAG×22 yday $21.48 → 09:30 $21.64 +3.52; KGC×15 yday $32.55 → 09:30 $32.90 +5.25; NFGC×254 yday $1.90 → 09:30 $2.00 +25.40; WPM×3 yday $158.25 → 09:30 $160.93 +8.04; AU×2 yday $118.55 → 09:30 $119.80 +2.50; AUPH×20 yday $16.71 → 09:30 $16.60 -2.20; AEM×1 yday $215.40 → 09:30 $219.50 +4.10; ARCT×30 yday $14.21 → 09:30 $15.35 +34.20; AUTL×139 yday $2.34 → 09:30 $2.41 +9.73; CRDL×178 yday $1.90 → 09:30 $2.03 +23.14; CRSP×5 yday $57.03 → 09:30 $60.18 +15.75; CYPH×261 yday $1.64 → 09:30 $1.60 -10.44; MOS×16 yday $23.75 → 09:30 $24.84 +17.44; OCUL×35 yday $10.92 → 09:30 $10.79 -4.55; INSP×6 yday $61.47 → 09:30 $60.07 -8.40; CRMD×47 yday $8.28 → 09:30 $8.60 +15.04; RZLT×74 yday $5.29 → 09:30 $5.01 -20.72 |
| 2026-08-28 | +0.75 | $3,194.34 | AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261, MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74, RRC×26, CRK×75, SLI×410 | $11,019.52 | +3.35 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $5,873.08 | $11,017.49 | MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74, RRC×26, CRK×75, SLI×410 | 09:30 open · cash $3,194.34 (unchanged overnight, no fees) · equity $11,019.52 vs prior close $11,016.17 (+3.35) because holdings re-marked: AU×2 yday $118.11 → 09:30 $117.41 -1.40; AUPH×20 yday $16.54 → 09:30 $16.47 -1.40; AEM×1 yday $214.04 → 09:30 $214.11 +0.07; ARCT×30 yday $15.83 → 09:30 $15.74 -2.70; AUTL×139 yday $2.33 → 09:30 $2.32 -1.39; CRDL×178 yday $2.14 → 09:30 $2.09 -8.90; CRSP×5 yday $59.23 → 09:30 $59.12 -0.55; CYPH×261 yday $1.63 → 09:30 $1.75 +31.32; MOS×16 yday $24.16 → 09:30 $24.00 -2.56; OCUL×35 yday $10.77 → 09:30 $10.63 -4.90; INSP×6 yday $61.80 → 09:30 $62.10 +1.80; CRMD×47 yday $8.39 → 09:30 $8.49 +4.70; RZLT×74 yday $5.04 → 09:30 $5.07 +2.22; RRC×26 yday $41.55 → 09:30 $41.44 -2.86; CRK×75 yday $14.50 → 09:30 $14.42 -6.00; SLI×410 yday $2.61 → 09:30 $2.60 -4.10 |
| 2026-08-31 | -5.85 | $5,873.08 | MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74, RRC×26, CRK×75, SLI×410 | $10,905.79 | -111.70 | — | — | $5,873.08 | $10,920.41 | MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74, RRC×26, CRK×75, SLI×410 | 09:30 open · cash $5,873.08 (unchanged overnight, no fees) · equity $10,905.79 vs prior close $11,017.49 (-111.70) because holdings re-marked: MOS×16 yday $23.76 → 09:30 $23.75 -0.16; OCUL×35 yday $10.82 → 09:30 $10.36 -16.10; INSP×6 yday $60.82 → 09:30 $61.44 +3.72; CRMD×47 yday $8.31 → 09:30 $8.29 -0.94; RZLT×74 yday $4.98 → 09:30 $4.62 -26.64; RRC×26 yday $41.64 → 09:30 $41.11 -13.78; CRK×75 yday $14.62 → 09:30 $14.56 -4.50; SLI×410 yday $2.64 → 09:30 $2.51 -53.30 |
| 2026-09-01 | -6.30 | $5,873.08 | MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74, RRC×26, CRK×75, SLI×410 | $10,992.38 | +71.97 | — | MOS, OCUL, INSP, CRMD, RZLT | $7,727.22 | $11,026.04 | RRC×26, CRK×75, SLI×410 | 09:30 open · cash $5,873.08 (unchanged overnight, no fees) · equity $10,992.38 vs prior close $10,920.41 (+71.97) because holdings re-marked: MOS×16 yday $23.78 → 09:30 $24.00 +3.52; OCUL×35 yday $10.36 → 09:30 $10.49 +4.55; INSP×6 yday $61.44 → 09:30 $63.05 +9.66; CRMD×47 yday $8.30 → 09:30 $8.26 -1.88; RZLT×74 yday $4.62 → 09:30 $4.69 +5.18; RRC×26 yday $41.78 → 09:30 $41.32 -11.96; CRK×75 yday $14.51 → 09:30 $14.31 -15.00; SLI×410 yday $2.51 → 09:30 $2.70 +77.90 |
| 2026-09-02 | -3.83 | $7,727.22 | RRC×26, CRK×75, SLI×410 | $11,098.86 | +72.82 | — | — | $7,727.22 | $11,052.02 | RRC×26, CRK×75, SLI×410 | 09:30 open · cash $7,727.22 (unchanged overnight, no fees) · equity $11,098.86 vs prior close $11,026.04 (+72.82) because holdings re-marked: RRC×26 yday $41.32 → 09:30 $41.94 +16.12; CRK×75 yday $14.90 → 09:30 $15.82 +69.00; SLI×410 yday $2.70 → 09:30 $2.67 -12.30 |
| 2026-09-03 | -0.90 | $7,727.22 | RRC×26, CRK×75, SLI×410 | $11,020.22 | -31.80 | ATRC, HRMY, CABA, VSTM, RVTY | RRC, CRK, SLI | $5,629.22 | $11,286.44 | ATRC×22, HRMY×26, CABA×336, VSTM×142, RVTY×8 | 09:30 open · cash $7,727.22 (unchanged overnight, no fees) · equity $11,020.22 vs prior close $11,052.02 (-31.80) because holdings re-marked: RRC×26 yday $42.40 → 09:30 $42.10 -7.80; CRK×75 yday $16.02 → 09:30 $15.70 -24.00; SLI×410 yday $2.49 → 09:30 $2.49 +0.00 |
| 2026-09-04 | — | $5,629.22 | ATRC×22, HRMY×26, CABA×336, VSTM×142, RVTY×8 | $11,328.30 | +41.86 | ASND, OSCR, NVAX, BVS | — | $3,019.19 | $11,242.73 | ATRC×22, HRMY×26, CABA×336, VSTM×142, RVTY×8, ASND×2, OSCR×22, NVAX×67, BVS×48 | 09:30 open · cash $5,629.22 (unchanged overnight, no fees) · equity $11,328.30 vs prior close $11,286.44 (+41.86) because holdings re-marked: ATRC×22 yday $52.59 → 09:30 $52.88 +6.38; HRMY×26 yday $42.86 → 09:30 $42.93 +1.82; CABA×336 yday $3.57 → 09:30 $3.63 +20.16; VSTM×142 yday $8.02 → 09:30 $8.03 +1.42; RVTY×8 yday $130.94 → 09:30 $132.45 +12.08 |

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
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,765.50 | ▲ 09:30 equity $10,289.35 vs yday $10,192.93 (+96.42) | 09:30 open · cash $1,765.50 (unchanged overnight, no fees) · equity $10,289.35 vs prior close $10,192.93 (+96.42) because holdings re-marked: BTSG×10 yday $59.50 → 09:30 $60.15 +6.50; IREN×13 yday $42.00 → 09:30 $41.41 -7.61; TPG×12 yday $52.02 → 09:30 $52.26 +2.88; TGTX×12 yday $50.26 → 09:30 $51.62 +16.32; SLS×53 yday $13.10 → 09:30 $13.46 +19.08; HIMS×21 yday $27.39 → 09:30 $27.55 +3.36; INO×771 yday $1.20 → 09:30 $1.22 +15.42; TNDM×26 yday $23.73 → 09:30 $24.20 +12.22; VST×2 yday $140.52 → 09:30 $140.74 +0.44; NRG×2 yday $115.56 → 09:30 $116.20 +1.28; SLG×5 yday $56.84 → 09:30 $57.50 +3.30; MARA×35 yday $8.96 → 09:30 $8.91 -1.75; LDI×340 yday $0.86 → 09:30 $0.88 +7.48; BTBT×212 yday $1.45 → 09:30 $1.42 -6.36; DVN×4 yday $47.83 → 09:30 $48.22 +1.56; EOG×1 yday $148.70 → 09:30 $149.86 +1.16; FANG×1 yday $210.02 → 09:30 $210.84 +0.82; TMC×51 yday $3.92 → 09:30 $3.93 +0.51; TGB×24 yday $8.36 → 09:30 $8.70 +8.16; ELF×2 yday $92.51 → 09:30 $96.00 +6.98; DNN×63 yday $3.15 → 09:30 $3.19 +2.52; HNST×43 yday $4.75 → 09:30 $4.80 +2.15 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,765.50 | ▼ 09:30 equity $10,488.06 vs yday $10,495.12 (-7.06) | 09:30 open · cash $1,765.50 (unchanged overnight, no fees) · equity $10,488.06 vs prior close $10,495.12 (-7.06) because holdings re-marked: BTSG×10 yday $59.33 → 09:30 $58.64 -6.90; IREN×13 yday $42.84 → 09:30 $42.46 -4.94; TPG×12 yday $53.18 → 09:30 $53.06 -1.44; TGTX×12 yday $51.69 → 09:30 $51.65 -0.48; SLS×53 yday $13.85 → 09:30 $13.84 -0.53; HIMS×21 yday $31.09 → 09:30 $30.66 -9.03; INO×771 yday $1.30 → 09:30 $1.30 +0.00; TNDM×26 yday $23.46 → 09:30 $23.11 -9.10; VST×2 yday $142.70 → 09:30 $142.70 +0.00; NRG×2 yday $120.58 → 09:30 $119.96 -1.24; SLG×5 yday $57.65 → 09:30 $57.29 -1.80; MARA×35 yday $9.65 → 09:30 $10.21 +19.60; LDI×340 yday $0.88 → 09:30 $0.87 -1.70; BTBT×212 yday $1.40 → 09:30 $1.46 +11.66; DVN×4 yday $48.19 → 09:30 $49.02 +3.32; EOG×1 yday $149.48 → 09:30 $151.45 +1.97; FANG×1 yday $208.55 → 09:30 $213.51 +4.96; TMC×51 yday $3.97 → 09:30 $3.92 -2.55; TGB×24 yday $8.47 → 09:30 $8.35 -2.88; ELF×2 yday $99.65 → 09:30 $98.15 -3.00; DNN×63 yday $3.22 → 09:30 $3.20 -1.26; HNST×43 yday $5.02 → 09:30 $4.98 -1.72 | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 10 | $58.64 | $2.04 | $-15.66 | $2,349.86 | ▼ -15.66 after sell → book $10,486.02; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 13 | $42.46 | $2.05 | $-49.84 | $2,899.79 | ▼ -49.84 after sell → book $10,483.97; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 12 | $53.06 | $2.05 | $+25.17 | $3,534.46 | ▲ +25.17 after sell → book $10,481.92; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 12 | $51.65 | $2.05 | $+19.33 | $4,152.22 | ▲ +19.33 after sell → book $10,479.88; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 53 | $13.84 | $2.17 | $+109.10 | $4,883.57 | ▲ +109.10 after sell → book $10,477.71; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 21 | $30.66 | $2.07 | $+15.19 | $5,525.35 | ▲ +15.19 after sell → book $10,475.63; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 771 | $1.30 | $10.08 | $+359.15 | $6,517.57 | ▲ +359.15 after sell → book $10,465.55; vs 09:30 mark -10.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 26 | $23.11 | $2.09 | $-9.88 | $7,116.34 | ▼ -9.88 after sell → book $10,463.46; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 21 | $20.55 | $2.05 | — | $6,682.74 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 4 | $91.01 | $2.00 | — | $6,316.70 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 21 | $20.65 | $2.05 | — | $5,880.99 | — | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 77 | $5.77 | $2.22 | — | $5,434.48 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 22 | $19.63 | $2.06 | — | $5,000.57 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 15 | $29.63 | $2.04 | — | $4,554.08 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 254 | $1.75 | $3.28 | — | $4,106.31 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 3 | $144.54 | $2.00 | — | $3,670.69 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,670.69 | ▲ 09:30 equity $10,738.62 vs yday $10,582.85 (+155.77) | 09:30 open · cash $3,670.69 (unchanged overnight, no fees) · equity $10,738.62 vs prior close $10,582.85 (+155.77) because holdings re-marked: VST×2 yday $138.94 → 09:30 $139.99 +2.10; NRG×2 yday $115.36 → 09:30 $116.58 +2.44; SLG×5 yday $58.10 → 09:30 $58.63 +2.65; MARA×35 yday $11.15 → 09:30 $11.70 +19.25; LDI×340 yday $0.87 → 09:30 $0.87 -1.02; BTBT×212 yday $1.59 → 09:30 $1.66 +13.78; DVN×4 yday $49.30 → 09:30 $49.45 +0.60; EOG×1 yday $152.19 → 09:30 $152.29 +0.10; FANG×1 yday $211.02 → 09:30 $211.84 +0.82; TMC×51 yday $3.97 → 09:30 $4.10 +6.63; TGB×24 yday $8.69 → 09:30 $9.00 +7.44; ELF×2 yday $98.46 → 09:30 $99.02 +1.12; DNN×63 yday $3.14 → 09:30 $3.23 +5.67; HNST×43 yday $4.96 → 09:30 $4.97 +0.43; AG×21 yday $21.19 → 09:30 $21.90 +14.91; BHP×4 yday $93.63 → 09:30 $95.72 +8.36; CDE×21 yday $21.11 → 09:30 $21.75 +13.44; HDSN×77 yday $5.57 → 09:30 $5.67 +7.70; IAG×22 yday $20.50 → 09:30 $21.17 +14.74; KGC×15 yday $31.43 → 09:30 $32.17 +11.10; NFGC×254 yday $1.75 → 09:30 $1.79 +10.16; WPM×3 yday $150.25 → 09:30 $154.70 +13.35 | — |
| 2026-08-21 09:30 ET | **SELL** | `VST` | 2 | $139.99 | $2.02 | $-17.83 | $3,948.65 | ▼ -17.83 after sell → book $10,736.60; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `NRG` | 2 | $116.58 | $2.02 | $-10.85 | $4,179.79 | ▼ -10.85 after sell → book $10,734.58; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `SLG` | 5 | $58.63 | $2.02 | $+1.07 | $4,470.92 | ▲ +1.07 after sell → book $10,732.56; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 35 | $11.70 | $2.12 | $+89.94 | $4,878.30 | ▲ +89.94 after sell → book $10,730.44; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 340 | $0.87 | $4.03 | $-32.04 | $5,169.05 | ▼ -32.04 after sell → book $10,726.41; vs 09:30 mark -4.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 212 | $1.66 | $2.78 | $+28.41 | $5,518.19 | ▲ +28.41 after sell → book $10,723.63; vs 09:30 mark -2.78 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 2 | $119.43 | $2.00 | — | $5,277.33 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+20.4; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 20 | $17.20 | $2.05 | — | $4,931.28 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 1 | $216.30 | $1.99 | — | $4,712.99 | — | deploy half leftover; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 30 | $11.13 | $2.08 | — | $4,377.01 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 139 | $2.47 | $2.41 | — | $4,031.27 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 178 | $1.93 | $2.52 | — | $3,685.21 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 5 | $59.72 | $2.00 | — | $3,384.61 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 261 | $1.32 | $3.37 | — | $3,036.72 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,036.72 | ▲ 09:30 equity $10,954.46 vs yday $10,834.53 (+119.93) | 09:30 open · cash $3,036.72 (unchanged overnight, no fees) · equity $10,954.46 vs prior close $10,834.53 (+119.93) because holdings re-marked: DVN×4 yday $49.10 → 09:30 $48.84 -1.04; EOG×1 yday $153.05 → 09:30 $152.61 -0.44; FANG×1 yday $210.72 → 09:30 $209.47 -1.25; TMC×51 yday $4.79 → 09:30 $4.57 -11.22; TGB×24 yday $9.19 → 09:30 $9.26 +1.68; ELF×2 yday $101.94 → 09:30 $101.53 -0.82; DNN×63 yday $3.50 → 09:30 $3.50 +0.00; HNST×43 yday $5.05 → 09:30 $5.05 +0.00; AG×21 yday $21.09 → 09:30 $21.47 +7.98; BHP×4 yday $97.03 → 09:30 $97.34 +1.24; CDE×21 yday $20.97 → 09:30 $21.26 +6.09; HDSN×77 yday $5.63 → 09:30 $5.69 +4.62; IAG×22 yday $21.14 → 09:30 $21.44 +6.60; KGC×15 yday $32.76 → 09:30 $33.21 +6.75; NFGC×254 yday $1.84 → 09:30 $1.86 +5.08; WPM×3 yday $157.78 → 09:30 $158.96 +3.54; AU×2 yday $121.22 → 09:30 $120.50 -1.44; AUPH×20 yday $16.65 → 09:30 $16.60 -1.00; AEM×1 yday $216.06 → 09:30 $217.03 +0.97; ARCT×30 yday $13.45 → 09:30 $13.26 -5.70; AUTL×139 yday $2.41 → 09:30 $2.36 -6.95; CRDL×178 yday $1.86 → 09:30 $1.87 +1.78; CRSP×5 yday $59.50 → 09:30 $58.79 -3.55; CYPH×261 yday $1.42 → 09:30 $1.83 +107.01 | — |
| 2026-08-24 09:30 ET | **SELL** | `DVN` | 4 | $48.84 | $1.99 | $+6.80 | $3,230.09 | ▲ +6.80 after sell → book $10,952.47; vs 09:30 mark -1.99 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `EOG` | 1 | $152.61 | $1.55 | $+6.86 | $3,381.15 | ▲ +6.86 after sell → book $10,950.92; vs 09:30 mark -1.55 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `FANG` | 1 | $209.47 | $2.01 | $+2.76 | $3,588.61 | ▲ +2.76 after sell → book $10,948.91; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 51 | $4.57 | $2.16 | $+22.21 | $3,819.52 | ▲ +22.21 after sell → book $10,946.75; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 24 | $9.26 | $2.08 | $+15.06 | $4,039.68 | ▲ +15.06 after sell → book $10,944.67; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `ELF` | 2 | $101.53 | $2.02 | $+18.15 | $4,240.72 | ▲ +18.15 after sell → book $10,942.65; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 63 | $3.50 | $2.20 | $+12.00 | $4,459.02 | ▲ +12.00 after sell → book $10,940.45; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟡 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 43 | $5.05 | $2.14 | $+6.06 | $4,674.03 | ▲ +6.06 after sell → book $10,938.31; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,674.03 | ▲ 09:30 equity $10,882.40 vs yday $10,825.40 (+57.00) | 09:30 open · cash $4,674.03 (unchanged overnight, no fees) · equity $10,882.40 vs prior close $10,825.40 (+57.00) because holdings re-marked: AG×21 yday $20.57 → 09:30 $20.73 +3.36; BHP×4 yday $96.66 → 09:30 $95.95 -2.84; CDE×21 yday $20.49 → 09:30 $20.85 +7.56; HDSN×77 yday $5.57 → 09:30 $5.53 -3.08; IAG×22 yday $21.36 → 09:30 $21.63 +5.94; KGC×15 yday $32.47 → 09:30 $32.76 +4.35; NFGC×254 yday $1.90 → 09:30 $1.91 +2.54; WPM×3 yday $158.00 → 09:30 $160.00 +6.00; AU×2 yday $118.66 → 09:30 $119.46 +1.60; AUPH×20 yday $16.60 → 09:30 $16.71 +2.20; AEM×1 yday $214.08 → 09:30 $200.48 -13.60; ARCT×30 yday $13.76 → 09:30 $14.34 +17.40; AUTL×139 yday $2.38 → 09:30 $2.32 -8.34; CRDL×178 yday $1.80 → 09:30 $1.90 +17.80; CRSP×5 yday $56.91 → 09:30 $57.00 +0.45; CYPH×261 yday $1.64 → 09:30 $1.70 +15.66 | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 16 | $24.00 | $2.04 | — | $4,287.99 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; leftover $389.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 35 | $10.92 | $2.10 | — | $3,903.70 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+10.4; leftover $389.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 6 | $61.47 | $2.01 | — | $3,532.87 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.2; leftover $389.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 47 | $8.28 | $2.13 | — | $3,141.58 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.8; leftover $389.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 74 | $5.23 | $2.21 | — | $2,752.35 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+10.7; leftover $389.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,752.35 | ▲ 09:30 equity $10,847.92 vs yday $10,847.92 (-0.00) | 09:30 open · cash $2,752.35 (unchanged overnight, no fees) · equity $10,847.92 vs prior close $10,847.92 (-0.00) because holdings re-marked: AG×21 yday $20.68 → 09:30 $20.68 +0.00; BHP×4 yday $96.05 → 09:30 $96.05 +0.00; CDE×21 yday $20.71 → 09:30 $20.71 +0.00; HDSN×77 yday $5.49 → 09:30 $5.49 +0.00; IAG×22 yday $21.48 → 09:30 $21.48 +0.00; KGC×15 yday $32.55 → 09:30 $32.55 +0.00; NFGC×254 yday $1.90 → 09:30 $1.90 +0.00; WPM×3 yday $158.25 → 09:30 $158.25 +0.00; AU×2 yday $118.55 → 09:30 $118.55 +0.00; AUPH×20 yday $16.71 → 09:30 $16.71 +0.00; AEM×1 yday $215.40 → 09:30 $215.40 +0.00; ARCT×30 yday $14.21 → 09:30 $14.21 +0.00; AUTL×139 yday $2.34 → 09:30 $2.34 +0.00; CRDL×178 yday $1.90 → 09:30 $1.90 +0.00; CRSP×5 yday $57.03 → 09:30 $57.03 +0.00; CYPH×261 yday $1.64 → 09:30 $1.64 +0.00; MOS×16 yday $23.75 → 09:30 $23.75 +0.00; OCUL×35 yday $10.92 → 09:30 $10.92 +0.00; INSP×6 yday $61.47 → 09:30 $61.47 +0.00; CRMD×47 yday $8.28 → 09:30 $8.28 +0.00; RZLT×74 yday $5.29 → 09:30 $5.29 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,752.35 | ▲ 09:30 equity $10,976.06 vs yday $10,871.92 (+104.14) | 09:30 open · cash $2,752.35 (unchanged overnight, no fees) · equity $10,976.06 vs prior close $10,871.92 (+104.14) because holdings re-marked: AG×21 yday $20.68 → 09:30 $20.63 -1.05; BHP×4 yday $96.05 → 09:30 $96.99 +3.76; CDE×21 yday $20.71 → 09:30 $21.00 +6.09; HDSN×77 yday $5.49 → 09:30 $5.51 +1.54; IAG×22 yday $21.48 → 09:30 $21.64 +3.52; KGC×15 yday $32.55 → 09:30 $32.90 +5.25; NFGC×254 yday $1.90 → 09:30 $2.00 +25.40; WPM×3 yday $158.25 → 09:30 $160.93 +8.04; AU×2 yday $118.55 → 09:30 $119.80 +2.50; AUPH×20 yday $16.71 → 09:30 $16.60 -2.20; AEM×1 yday $215.40 → 09:30 $219.50 +4.10; ARCT×30 yday $14.21 → 09:30 $15.35 +34.20; AUTL×139 yday $2.34 → 09:30 $2.41 +9.73; CRDL×178 yday $1.90 → 09:30 $2.03 +23.14; CRSP×5 yday $57.03 → 09:30 $60.18 +15.75; CYPH×261 yday $1.64 → 09:30 $1.60 -10.44; MOS×16 yday $23.75 → 09:30 $24.84 +17.44; OCUL×35 yday $10.92 → 09:30 $10.79 -4.55; INSP×6 yday $61.47 → 09:30 $60.07 -8.40; CRMD×47 yday $8.28 → 09:30 $8.60 +15.04; RZLT×74 yday $5.29 → 09:30 $5.01 -20.72 | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 21 | $20.63 | $2.07 | $-2.45 | $3,183.50 | ▼ -2.45 after sell → book $10,973.98; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 4 | $96.99 | $2.02 | $+19.90 | $3,569.44 | ▲ +19.90 after sell → book $10,971.96; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 21 | $21.00 | $2.07 | $+3.22 | $4,008.37 | ▲ +3.22 after sell → book $10,969.89; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 77 | $5.51 | $2.24 | $-24.48 | $4,430.40 | ▼ -24.48 after sell → book $10,967.65; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 22 | $21.64 | $2.08 | $+40.09 | $4,904.40 | ▲ +40.09 after sell → book $10,965.57; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 15 | $32.90 | $2.06 | $+44.96 | $5,395.84 | ▲ +44.96 after sell → book $10,963.51; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 254 | $2.00 | $3.33 | $+56.89 | $5,900.52 | ▲ +56.89 after sell → book $10,960.19; vs 09:30 mark -3.32 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 3 | $160.93 | $2.02 | $+45.15 | $6,381.29 | ▲ +45.15 after sell → book $10,958.17; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 26 | $40.72 | $2.07 | — | $5,320.50 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.8; leftover $1063.55 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 75 | $14.09 | $2.21 | — | $4,261.53 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.1; leftover $1063.55 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 410 | $2.59 | $5.29 | — | $3,194.34 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.2; leftover $1063.55 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,194.34 | ▲ 09:30 equity $11,019.52 vs yday $11,016.17 (+3.35) | 09:30 open · cash $3,194.34 (unchanged overnight, no fees) · equity $11,019.52 vs prior close $11,016.17 (+3.35) because holdings re-marked: AU×2 yday $118.11 → 09:30 $117.41 -1.40; AUPH×20 yday $16.54 → 09:30 $16.47 -1.40; AEM×1 yday $214.04 → 09:30 $214.11 +0.07; ARCT×30 yday $15.83 → 09:30 $15.74 -2.70; AUTL×139 yday $2.33 → 09:30 $2.32 -1.39; CRDL×178 yday $2.14 → 09:30 $2.09 -8.90; CRSP×5 yday $59.23 → 09:30 $59.12 -0.55; CYPH×261 yday $1.63 → 09:30 $1.75 +31.32; MOS×16 yday $24.16 → 09:30 $24.00 -2.56; OCUL×35 yday $10.77 → 09:30 $10.63 -4.90; INSP×6 yday $61.80 → 09:30 $62.10 +1.80; CRMD×47 yday $8.39 → 09:30 $8.49 +4.70; RZLT×74 yday $5.04 → 09:30 $5.07 +2.22; RRC×26 yday $41.55 → 09:30 $41.44 -2.86; CRK×75 yday $14.50 → 09:30 $14.42 -6.00; SLI×410 yday $2.61 → 09:30 $2.60 -4.10 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 2 | $117.41 | $2.02 | $-8.05 | $3,427.15 | ▼ -8.05 after sell → book $11,017.51; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 20 | $16.47 | $2.07 | $-18.72 | $3,754.48 | ▼ -18.72 after sell → book $11,015.44; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEM` | 1 | $214.11 | $2.01 | $-6.20 | $3,966.58 | ▼ -6.20 after sell → book $11,013.43; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 30 | $15.74 | $2.10 | $+134.12 | $4,436.68 | ▲ +134.12 after sell → book $11,011.33; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 139 | $2.32 | $2.44 | $-25.70 | $4,756.72 | ▼ -25.70 after sell → book $11,008.89; vs 09:30 mark -2.44 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 178 | $2.09 | $2.56 | $+23.39 | $5,126.17 | ▲ +23.39 after sell → book $11,006.32; vs 09:30 mark -2.57 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CRSP` | 5 | $59.12 | $2.02 | $-7.03 | $5,419.75 | ▼ -7.03 after sell → book $11,004.30; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 261 | $1.75 | $3.42 | $+105.44 | $5,873.08 | ▲ +105.44 after sell → book $11,000.88; vs 09:30 mark -3.42 | dropped from list after 5 sess (min 5) | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,873.08 | ▼ 09:30 equity $10,905.79 vs yday $11,017.49 (-111.70) | 09:30 open · cash $5,873.08 (unchanged overnight, no fees) · equity $10,905.79 vs prior close $11,017.49 (-111.70) because holdings re-marked: MOS×16 yday $23.76 → 09:30 $23.75 -0.16; OCUL×35 yday $10.82 → 09:30 $10.36 -16.10; INSP×6 yday $60.82 → 09:30 $61.44 +3.72; CRMD×47 yday $8.31 → 09:30 $8.29 -0.94; RZLT×74 yday $4.98 → 09:30 $4.62 -26.64; RRC×26 yday $41.64 → 09:30 $41.11 -13.78; CRK×75 yday $14.62 → 09:30 $14.56 -4.50; SLI×410 yday $2.64 → 09:30 $2.51 -53.30 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,873.08 | ▲ 09:30 equity $10,992.38 vs yday $10,920.41 (+71.97) | 09:30 open · cash $5,873.08 (unchanged overnight, no fees) · equity $10,992.38 vs prior close $10,920.41 (+71.97) because holdings re-marked: MOS×16 yday $23.78 → 09:30 $24.00 +3.52; OCUL×35 yday $10.36 → 09:30 $10.49 +4.55; INSP×6 yday $61.44 → 09:30 $63.05 +9.66; CRMD×47 yday $8.30 → 09:30 $8.26 -1.88; RZLT×74 yday $4.62 → 09:30 $4.69 +5.18; RRC×26 yday $41.78 → 09:30 $41.32 -11.96; CRK×75 yday $14.51 → 09:30 $14.31 -15.00; SLI×410 yday $2.51 → 09:30 $2.70 +77.90 | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 16 | $24.00 | $2.06 | $-4.10 | $6,255.02 | ▼ -4.10 after sell → book $10,990.32; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 35 | $10.49 | $2.12 | $-19.26 | $6,620.05 | ▼ -19.26 after sell → book $10,988.20; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `INSP` | 6 | $63.05 | $2.03 | $+5.44 | $6,996.33 | ▲ +5.44 after sell → book $10,986.18; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 47 | $8.26 | $2.15 | $-5.22 | $7,382.39 | ▼ -5.22 after sell → book $10,984.02; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 74 | $4.69 | $2.23 | $-44.41 | $7,727.22 | ▼ -44.41 after sell → book $10,981.79; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,727.22 | ▲ 09:30 equity $11,098.86 vs yday $11,026.04 (+72.82) | 09:30 open · cash $7,727.22 (unchanged overnight, no fees) · equity $11,098.86 vs prior close $11,026.04 (+72.82) because holdings re-marked: RRC×26 yday $41.32 → 09:30 $41.94 +16.12; CRK×75 yday $14.90 → 09:30 $15.82 +69.00; SLI×410 yday $2.70 → 09:30 $2.67 -12.30 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,727.22 | ▼ 09:30 equity $11,020.22 vs yday $11,052.02 (-31.80) | 09:30 open · cash $7,727.22 (unchanged overnight, no fees) · equity $11,020.22 vs prior close $11,052.02 (-31.80) because holdings re-marked: RRC×26 yday $42.40 → 09:30 $42.10 -7.80; CRK×75 yday $16.02 → 09:30 $15.70 -24.00; SLI×410 yday $2.49 → 09:30 $2.49 +0.00 | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 26 | $42.10 | $2.09 | $+31.72 | $8,819.73 | ▲ +31.72 after sell → book $11,018.13; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 75 | $15.70 | $2.24 | $+116.30 | $9,995.00 | ▲ +116.30 after sell → book $11,015.90; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 410 | $2.49 | $5.37 | $-51.66 | $11,010.53 | ▼ -51.66 after sell → book $11,010.53; vs 09:30 mark -5.37 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 22 | $49.76 | $2.06 | — | $9,913.75 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $1101.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 26 | $41.31 | $2.07 | — | $8,837.62 | — | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $1101.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 336 | $3.27 | $4.33 | — | $7,734.57 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $1101.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 142 | $7.70 | $2.42 | — | $6,638.75 | — | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $1101.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 8 | $125.94 | $2.01 | — | $5,629.22 | — | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $1101.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,629.22 | ▲ 09:30 equity $11,328.30 vs yday $11,286.44 (+41.86) | 09:30 open · cash $5,629.22 (unchanged overnight, no fees) · equity $11,328.30 vs prior close $11,286.44 (+41.86) because holdings re-marked: ATRC×22 yday $52.59 → 09:30 $52.88 +6.38; HRMY×26 yday $42.86 → 09:30 $42.93 +1.82; CABA×336 yday $3.57 → 09:30 $3.63 +20.16; VSTM×142 yday $8.02 → 09:30 $8.03 +1.42; RVTY×8 yday $130.94 → 09:30 $132.45 +12.08 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 2 | $266.94 | $2.00 | — | $5,093.34 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.9; leftover $703.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 22 | $30.65 | $2.06 | — | $4,416.99 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-2.2; leftover $703.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 67 | $10.41 | $2.19 | — | $3,717.33 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $703.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 48 | $14.50 | $2.13 | — | $3,019.19 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.8; leftover $703.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

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
| 2026-08-14 | `TLN` | cash | leftover split 318.86 < 1 share @ 359.83 |
| 2026-08-14 | `DAVE` | cash | leftover split 318.86 < 1 share @ 330.91 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `VST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `SLG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `HIMS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `VST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `SLG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `FANG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `ELF` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BTSG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `IREN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TGTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `SLS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `HIMS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `INO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TNDM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `VST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `NRG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `SLG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `FANG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `ELF` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `VST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `NRG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `SLG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `MARA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `DVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `EOG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `FANG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `ELF` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `HNST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `DVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `EOG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `FANG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TMC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TGB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `ELF` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `DNN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `HNST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRSP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BHP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `CDE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `HDSN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `IAG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NFGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `WPM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AEM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRSP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `HCA` | cash | leftover split 389.50 < 1 share @ 429.24 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AEM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRSP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `HCA` | no_price | no 09:30 open |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AEM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRSP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `OCUL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `INSP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `OCUL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `INSP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `RRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `CRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `SLI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 22 | 2026-09-03 @ $49.76 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $1101.05 |
| `HRMY` | 26 | 2026-09-03 @ $41.31 | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $1101.05 |
| `CABA` | 336 | 2026-09-03 @ $3.27 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $1101.05 |
| `VSTM` | 142 | 2026-09-03 @ $7.70 | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $1101.05 |
| `RVTY` | 8 | 2026-09-03 @ $125.94 | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $1101.05 |
| `ASND` | 2 | 2026-09-04 @ $266.94 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.9; leftover $703.65 |
| `OSCR` | 22 | 2026-09-04 @ $30.65 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-2.2; leftover $703.65 |
| `NVAX` | 67 | 2026-09-04 @ $10.41 | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $703.65 |
| `BVS` | 48 | 2026-09-04 @ $14.50 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.8; leftover $703.65 |
