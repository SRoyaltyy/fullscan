# Factor mine action — `union_white_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ white hold 5, no 🚨

Cash book **+4.04%** ($10,404) · signal-only (no cash/fees) was +12.71%. Starts YES **8/17**. Fills 97 · skips 187 · realized $+738.46.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `zero_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7.75.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,055.59 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | BUY BTSG x20 @ 59.80; BUY IREN x27 @ 45.98; BUY TPG x24 @ 50.62; BUY TGTX x25 @ 49.70; BUY SLS x106 @ 11.70; BUY HIMS x42 @ 29.74; BUY INO x1543 @ 0.81; BUY TNDM x53 @ 23.33 |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | MARA, LDI, BTBT, ANGX, HYLN | — | $46.78 | $10,388.33 | $10,435.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2 | BUY MARA x1 @ 9.01; BUY LDI x13 @ 0.94; BUY BTBT x8 @ 1.50; BUY ANGX x2 @ 4.31; BUY HYLN x2 @ 4.18 |
| 2026-08-17 | +2.25 | $46.78 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2 | TMC, DNN | — | $39.42 | $10,486.42 | $10,525.84 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1 | BUY TMC x1 @ 4.05; BUY DNN x1 @ 3.24 |
| 2026-08-18 | -6.20 | $39.42 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1 | — | — | $39.42 | $10,533.45 | $10,572.87 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1 | hard-red sit S=-6.20 |
| 2026-08-19 | -7.20 | $39.42 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1 | — | — | $39.42 | $10,990.98 | $11,030.39 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1 | hard-red sit S=-7.20 |
| 2026-08-20 | +1.12 | $39.42 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $172.56 | $10,987.37 | $11,159.93 | MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1, AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9 | SELL BTSG (dropped from list after 5 sess (min 5)); SELL IREN (dropped from list after 5 sess (min 5)); SELL TPG (dropped from list after 5 sess (min 5)); SELL TGTX (dropped from list after 5 sess (min 5)); SELL SLS (dropped from list after 5 sess (min 5)); SELL HIMS (dropped from list after 5 sess (min 5)); SELL INO (dropped from list after 5 sess (min 5)); SELL TNDM (dropped from list after 5 sess (min 5)); BUY AG x66 @ 20.55; BUY BHP x14 @ 91.01; BUY CDE x65 @ 20.65; BUY HDSN x235 @ 5.77; BUY IAG x69 @ 19.63; BUY KGC x45 @ 29.63; BUY NFGC x776 @ 1.75; BUY WPM x9 @ 144.54 |
| 2026-08-21 | +3.25 | $172.56 | MARA×1, LDI×13, BTBT×8, ANGX×2, HYLN×2, TMC×1, DNN×1, AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9 | AUPH, ARCT, AUTL, CRDL, CYPH | MARA, LDI, BTBT, ANGX, HYLN | $101.08 | $11,351.39 | $11,452.47 | TMC×1, DNN×1, AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9, AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21 | SELL MARA (dropped from list after 5 sess (min 5)); SELL LDI (dropped from list after 5 sess (min 5)); SELL BTBT (dropped from list after 5 sess (min 5)); SELL ANGX (dropped from list after 5 sess (min 5)); SELL HYLN (dropped from list after 5 sess (min 5)); BUY AUPH x1 @ 17.20; BUY ARCT x2 @ 11.13; BUY AUTL x11 @ 2.47; BUY CRDL x14 @ 1.93; BUY CYPH x21 @ 1.32 |
| 2026-08-24 | -5.17 | $101.08 | TMC×1, DNN×1, AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9, AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21 | — | TMC, DNN | $109.03 | $11,312.99 | $11,422.02 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9, AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21 | SELL TMC (dropped from list after 5 sess (min 5)); SELL DNN (dropped from list after 5 sess (min 5)) |
| 2026-08-25 | +1.80 | $109.03 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9, AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21 | CRMD, BMEA, ALVO, ZURA, SUJA, DEFT | — | $38.00 | $11,393.00 | $11,431.00 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9, AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21, CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24 | BUY CRMD x1 @ 8.28; BUY BMEA x9 @ 1.62; BUY ALVO x2 @ 5.22; BUY ZURA x2 @ 6.38; BUY SUJA x1 @ 8.79; BUY DEFT x24 @ 0.64 |
| 2026-08-26 | +2.02 | $38.00 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9, AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21, CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24 | — | — | $38.00 | $11,458.53 | $11,496.53 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9, AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21, CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24 | hold AG,BHP,CDE,HDSN,IAG,KGC,NFGC,WPM,AUPH,ARCT,AUTL,CRDL,CYPH,CRMD,BMEA,ALVO,ZURA,SUJA,DEFT |
| 2026-08-27 | — | $38.00 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×776, WPM×9, AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21, CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $11,365.21 | $207.20 | $11,572.41 | AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21, CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24 | SELL AG (dropped from list after 5 sess (min 5)); SELL BHP (dropped from list after 5 sess (min 5)); SELL CDE (dropped from list after 5 sess (min 5)); SELL HDSN (dropped from list after 5 sess (min 5)); SELL IAG (dropped from list after 5 sess (min 5)); SELL KGC (dropped from list after 5 sess (min 5)); SELL NFGC (dropped from list after 5 sess (min 5)); SELL WPM (dropped from list after 5 sess (min 5)) |
| 2026-08-28 | +0.75 | $11,365.21 | AUPH×1, ARCT×2, AUTL×11, CRDL×14, CYPH×21, CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24 | SMTC, SIMO, TTMI, KEYS, AVT, CGNX, COHR, LSCC | AUPH, ARCT, AUTL, CRDL, CYPH | $793.88 | $10,566.79 | $11,360.67 | CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24, SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11 | SELL AUPH (dropped from list after 5 sess (min 5)); SELL ARCT (dropped from list after 5 sess (min 5)); SELL AUTL (dropped from list after 5 sess (min 5)); SELL CRDL (dropped from list after 5 sess (min 5)); SELL CYPH (dropped from list after 5 sess (min 5)); BUY SMTC x9 @ 149.40; BUY SIMO x5 @ 272.00; BUY TTMI x11 @ 127.07; BUY KEYS x4 @ 323.82; BUY AVT x15 @ 91.11; BUY CGNX x22 @ 62.80; BUY COHR x4 @ 303.67; BUY LSCC x11 @ 121.13 |
| 2026-08-31 | -5.85 | $793.88 | CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24, SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11 | — | — | $793.88 | $10,137.19 | $10,931.07 | CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24, SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11 | hard-red sit S=-5.85 |
| 2026-09-01 | -6.30 | $793.88 | CRMD×1, BMEA×9, ALVO×2, ZURA×2, SUJA×1, DEFT×24, SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11 | — | CRMD, BMEA, ALVO, ZURA, SUJA, DEFT | $861.22 | $9,971.63 | $10,832.85 | SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11 | SELL CRMD (dropped from list after 5 sess (min 5)); SELL BMEA (dropped from list after 5 sess (min 5)); SELL ALVO (dropped from list after 5 sess (min 5)); SELL ZURA (dropped from list after 5 sess (min 5)); SELL SUJA (dropped from list after 5 sess (min 5)); SELL DEFT (dropped from list after 5 sess (min 5)) |
| 2026-09-02 | -3.83 | $861.22 | SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11 | — | — | $861.22 | $9,902.10 | $10,763.32 | SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11 | hard-red sit S=-3.83 |
| 2026-09-03 | -0.90 | $861.22 | SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11 | ATRC, HRMY, CABA, VSTM, MMED, SLN, CRDL | — | $167.28 | $10,685.19 | $10,852.47 | SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11, ATRC×2, HRMY×2, CABA×32, VSTM×13, MMED×4, SLN×7, CRDL×49 | BUY ATRC x2 @ 49.76; BUY HRMY x2 @ 41.31; BUY CABA x32 @ 3.27; BUY VSTM x13 @ 7.70; BUY MMED x4 @ 22.78; BUY SLN x7 @ 14.70; BUY CRDL x49 @ 2.16 |
| 2026-09-04 | — | $167.28 | SMTC×9, SIMO×5, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×11, ATRC×2, HRMY×2, CABA×32, VSTM×13, MMED×4, SLN×7, CRDL×49 | NVAX, BVS, MLYS, IRD, OABI, ALEC | SMTC, SIMO, TTMI, KEYS, AVT, CGNX, COHR, LSCC | $7.75 | $10,396.01 | $10,403.76 | ATRC×2, HRMY×2, CABA×32, VSTM×13, MMED×4, SLN×7, CRDL×49, NVAX×160, BVS×115, MLYS×57, IRD×359, OABI×329, ALEC×620 | SELL SMTC (dropped from list after 5 sess (min 5)); SELL SIMO (dropped from list after 5 sess (min 5)); SELL TTMI (dropped from list after 5 sess (min 5)); SELL KEYS (dropped from list after 5 sess (min 5)); SELL AVT (dropped from list after 5 sess (min 5)); SELL CGNX (dropped from list after 5 sess (min 5)); SELL COHR (dropped from list after 5 sess (min 5)); SELL LSCC (dropped from list after 5 sess (min 5)); BUY NVAX x160 @ 10.41; BUY BVS x115 @ 14.50; BUY MLYS x57 @ 29.15; BUY IRD x359 @ 4.66; BUY OABI x329 @ 5.08; BUY ALEC x620 @ 2.70 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 2 | $4.31 | $0.09 | — | $55.23 | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 2 | $4.18 | $0.09 | — | $46.78 | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $42.69 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $5.85 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $39.42 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+0.3; leftover $5.85 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 20 | $58.64 | $2.07 | $-27.32 | $1,210.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 27 | $42.46 | $2.09 | $-99.20 | $2,354.47 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 24 | $53.06 | $2.08 | $+54.34 | $3,625.83 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 25 | $51.65 | $2.09 | $+44.60 | $4,915.00 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 106 | $13.84 | $2.34 | $+222.19 | $6,379.70 | dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 42 | $30.66 | $2.14 | $+34.39 | $7,665.28 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1543 | $1.30 | $20.18 | $+718.77 | $9,651.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 53 | $23.11 | $2.17 | $-15.98 | $10,873.67 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 66 | $20.55 | $2.19 | — | $9,515.18 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1359.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,239.01 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1359.21 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 65 | $20.65 | $2.19 | — | $6,894.57 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1359.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 235 | $5.77 | $3.03 | — | $5,535.59 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1359.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 69 | $19.63 | $2.20 | — | $4,178.92 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1359.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 45 | $29.63 | $2.12 | — | $2,843.45 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1359.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 776 | $1.75 | $10.01 | — | $1,475.44 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1359.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $172.56 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1359.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $184.12 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 13 | $0.87 | $0.17 | $-1.24 | $195.22 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 8 | $1.66 | $0.18 | $+0.96 | $208.32 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 2 | $4.43 | $0.11 | $+0.03 | $217.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 2 | $3.42 | $0.09 | $-1.70 | $223.81 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $206.44 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $27.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $183.95 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $27.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 11 | $2.47 | $0.30 | — | $156.48 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $27.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 14 | $1.93 | $0.31 | — | $129.14 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $27.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 21 | $1.32 | $0.34 | — | $101.08 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $27.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 1 | $4.57 | $0.07 | $+0.41 | $105.59 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 1 | $3.50 | $0.06 | $+0.17 | $109.03 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟡 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 1 | $8.28 | $0.09 | — | $100.66 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $15.58 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 9 | $1.62 | $0.17 | — | $85.91 | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $15.58 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 2 | $5.22 | $0.11 | — | $75.36 | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $15.58 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 2 | $6.38 | $0.13 | — | $62.46 | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $15.58 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 1 | $8.79 | $0.09 | — | $53.58 | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $15.58 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 24 | $0.64 | $0.23 | — | $38.00 | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $15.58 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 66 | $20.63 | $2.21 | $+0.88 | $1,397.37 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $96.99 | $2.05 | $+79.64 | $2,753.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 65 | $21.00 | $2.21 | $+18.36 | $4,115.97 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 235 | $5.51 | $3.08 | $-67.21 | $5,407.74 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 69 | $21.64 | $2.22 | $+134.27 | $6,898.68 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 45 | $32.90 | $2.15 | $+142.88 | $8,377.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 776 | $2.00 | $10.15 | $+173.84 | $9,918.88 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 9 | $160.93 | $2.04 | $+143.45 | $11,365.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.47 | $0.19 | $-1.09 | $11,381.49 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.74 | $0.34 | $+8.65 | $11,412.63 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 11 | $2.32 | $0.31 | $-2.26 | $11,437.84 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 14 | $2.09 | $0.35 | $+1.57 | $11,466.75 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 21 | $1.75 | $0.45 | $+8.24 | $11,503.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $149.40 | $2.02 | — | $10,156.43 | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1437.88 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $272.00 | $2.00 | — | $8,794.43 | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer; ⚪; ret5=-3.9; leftover $1437.88 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 11 | $127.07 | $2.02 | — | $7,394.63 | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $1437.88 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $323.82 | $2.00 | — | $6,097.35 | union ∩ white hold 5, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-11.7; leftover $1437.88 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 15 | $91.11 | $2.04 | — | $4,728.67 | union ∩ white hold 5, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-7.4; leftover $1437.88 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 22 | $62.80 | $2.06 | — | $3,345.01 | union ∩ white hold 5, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-7.8; leftover $1437.88 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $303.67 | $2.00 | — | $2,128.33 | union ∩ white hold 5, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-11.1; leftover $1437.88 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 11 | $121.13 | $2.02 | — | $793.88 | union ∩ white hold 5, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-9.8; leftover $1437.88 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 1 | $8.26 | $0.11 | $-0.21 | $802.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 9 | $1.65 | $0.20 | $-0.10 | $816.68 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ALVO` | 2 | $5.24 | $0.13 | $-0.20 | $827.03 | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `ZURA` | 2 | $5.60 | $0.14 | $-1.83 | $838.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `SUJA` | 1 | $9.31 | $0.12 | $+0.31 | $847.29 | dropped from list after 5 sess (min 5) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `DEFT` | 24 | $0.59 | $0.23 | $-1.66 | $861.22 | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 2 | $49.76 | $1.00 | — | $760.70 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $107.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 2 | $41.31 | $0.83 | — | $677.24 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $107.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 32 | $3.27 | $1.14 | — | $571.46 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $107.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 13 | $7.70 | $1.04 | — | $470.32 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $107.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 4 | $22.78 | $0.92 | — | $378.28 | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $107.65 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 7 | $14.70 | $1.05 | — | $274.33 | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $107.65 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 49 | $2.16 | $1.21 | — | $167.28 | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $107.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `SMTC` | 9 | $133.10 | $2.04 | $-150.75 | $1,363.14 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SIMO` | 5 | $239.05 | $2.02 | $-168.78 | $2,556.37 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `TTMI` | 11 | $115.21 | $2.04 | $-134.53 | $3,821.64 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `KEYS` | 4 | $319.09 | $2.02 | $-22.94 | $5,095.97 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVT` | 15 | $89.02 | $2.06 | $-35.44 | $6,429.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `CGNX` | 22 | $59.96 | $2.08 | $-66.61 | $7,746.26 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `COHR` | 4 | $266.86 | $2.02 | $-151.26 | $8,811.68 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `LSCC` | 11 | $112.26 | $2.04 | $-101.64 | $10,044.50 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 160 | $10.41 | $2.47 | — | $8,376.43 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1674.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 115 | $14.50 | $2.33 | — | $6,706.59 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1674.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 57 | $29.15 | $2.16 | — | $5,042.88 | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1674.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 359 | $4.66 | $4.63 | — | $3,365.31 | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1674.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 329 | $5.08 | $4.24 | — | $1,689.75 | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1674.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 620 | $2.70 | $8.00 | — | $7.75 | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1674.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| `MLYS` | 57 | 2026-09-04 @ $29.15 | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1674.08 |
| `IRD` | 359 | 2026-09-04 @ $4.66 | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1674.08 |
| `OABI` | 329 | 2026-09-04 @ $5.08 | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1674.08 |
| `ALEC` | 620 | 2026-09-04 @ $2.70 | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1674.08 |
