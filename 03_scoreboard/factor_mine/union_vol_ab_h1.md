# Factor mine action — `union_vol_ab_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-1.48%** ($9,852) · signal-only (no cash/fees) was -13.14%. Starts YES **0/17**. Fills 88 · skips 27 · realized $+259.64.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=good,ab=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $320.37.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $186.91 | $10,208.28 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $186.91 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8 | $10,475.50 | +267.22 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $158.85 | $10,673.53 | AU×10, AUPH×75, AEM×6, ARCT×117, AUTL×528, CRDL×676, CRSP×21, CYPH×989 | 09:30 open · cash $186.91 (unchanged overnight, no fees) · equity $10,475.50 vs prior close $10,208.28 (+267.22) because holdings re-marked: AG×60 yday $21.19 → 09:30 $21.90 +42.60; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×60 yday $21.11 → 09:30 $21.75 +38.40; HDSN×216 yday $5.57 → 09:30 $5.67 +21.60; IAG×63 yday $20.50 → 09:30 $21.17 +42.21; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×714 yday $1.75 → 09:30 $1.79 +28.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 |
| 2026-08-24 | -5.17 | $158.85 | AU×10, AUPH×75, AEM×6, ARCT×117, AUTL×528, CRDL×676, CRSP×21, CYPH×989 | $11,017.11 | +343.58 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,977.67 | $10,977.67 | — | 09:30 open · cash $158.85 (unchanged overnight, no fees) · equity $11,017.11 vs prior close $10,673.53 (+343.58) because holdings re-marked: AU×10 yday $121.22 → 09:30 $120.50 -7.20; AUPH×75 yday $16.65 → 09:30 $16.60 -3.75; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ARCT×117 yday $13.45 → 09:30 $13.26 -22.23; AUTL×528 yday $2.41 → 09:30 $2.36 -26.40; CRDL×676 yday $1.86 → 09:30 $1.87 +6.76; CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; CYPH×989 yday $1.42 → 09:30 $1.83 +405.49 |
| 2026-08-25 | +1.80 | $10,977.67 | — | $10,977.67 | -0.00 | BMEA, ALVO, ZURA, CYPH, DEFT, RUM, KURA, EZPW | — | $20.95 | $10,892.44 | BMEA×847, ALVO×262, ZURA×215, CYPH×807, DEFT×2144, RUM×146, KURA×103, EZPW×38 | 09:30 open · cash $10,977.67 · no holdings · equity $10,977.67 vs prior close $10,977.67 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $20.95 | BMEA×847, ALVO×262, ZURA×215, CYPH×807, DEFT×2144, RUM×146, KURA×103, EZPW×38 | $10,892.44 | +0.00 | — | — | $20.95 | $10,923.19 | BMEA×847, ALVO×262, ZURA×215, CYPH×807, DEFT×2144, RUM×146, KURA×103, EZPW×38 | 09:30 open · cash $20.95 (unchanged overnight, no fees) · equity $10,892.44 vs prior close $10,892.44 (+0.00) because holdings re-marked: BMEA×847 yday $1.61 → 09:30 $1.61 +0.00; ALVO×262 yday $5.25 → 09:30 $5.25 +0.00; ZURA×215 yday $6.50 → 09:30 $6.50 +0.00; CYPH×807 yday $1.64 → 09:30 $1.64 +0.00; DEFT×2144 yday $0.62 → 09:30 $0.62 +0.00; RUM×146 yday $9.35 → 09:30 $9.35 +0.00; KURA×103 yday $13.58 → 09:30 $13.58 +0.00; EZPW×38 yday $34.69 → 09:30 $34.69 +0.00 |
| 2026-08-27 | — | $20.95 | BMEA×847, ALVO×262, ZURA×215, CYPH×807, DEFT×2144, RUM×146, KURA×103, EZPW×38 | $10,934.22 | +11.03 | — | BMEA, ALVO, ZURA, CYPH, DEFT, RUM, KURA, EZPW | $10,879.76 | $10,879.76 | — | 09:30 open · cash $20.95 (unchanged overnight, no fees) · equity $10,934.22 vs prior close $10,923.19 (+11.03) because holdings re-marked: BMEA×847 yday $1.61 → 09:30 $1.75 +118.58; ALVO×262 yday $5.25 → 09:30 $4.98 -70.74; ZURA×215 yday $6.50 → 09:30 $6.13 -79.55; CYPH×807 yday $1.64 → 09:30 $1.60 -32.28; DEFT×2144 yday $0.62 → 09:30 $0.60 -42.88; RUM×146 yday $9.35 → 09:30 $10.07 +105.12; KURA×103 yday $13.58 → 09:30 $13.63 +5.15; EZPW×38 yday $34.69 → 09:30 $35.70 +38.38 |
| 2026-08-28 | +0.75 | $10,879.76 | — | $10,879.76 | -0.00 | ANF, BZ, SMTC, URBN, BBWI, CRDL, TIGR, FINV | — | $114.55 | $10,496.81 | ANF×9, BZ×73, SMTC×9, URBN×16, BBWI×72, CRDL×650, TIGR×247, FINV×319 | 09:30 open · cash $10,879.76 · no holdings · equity $10,879.76 vs prior close $10,879.76 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-31 | -5.85 | $114.55 | ANF×9, BZ×73, SMTC×9, URBN×16, BBWI×72, CRDL×650, TIGR×247, FINV×319 | $10,245.81 | -251.00 | — | ANF, BZ, SMTC, URBN, BBWI, CRDL, TIGR, FINV | $10,219.30 | $10,219.30 | — | 09:30 open · cash $114.55 (unchanged overnight, no fees) · equity $10,245.81 vs prior close $10,496.81 (-251.00) because holdings re-marked: ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BZ×73 yday $18.00 → 09:30 $17.89 -8.03; SMTC×9 yday $142.43 → 09:30 $133.04 -84.51; URBN×16 yday $78.79 → 09:30 $81.09 +36.80; BBWI×72 yday $18.65 → 09:30 $19.30 +46.80; CRDL×650 yday $2.06 → 09:30 $1.96 -65.00; TIGR×247 yday $5.06 → 09:30 $4.96 -24.70; FINV×319 yday $4.02 → 09:30 $3.46 -178.64 |
| 2026-09-01 | -6.30 | $10,219.30 | — | $10,219.30 | +0.00 | — | — | $10,219.30 | $10,219.30 | — | 09:30 open · cash $10,219.30 · no holdings · equity $10,219.30 vs prior close $10,219.30 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-02 | -3.83 | $10,219.30 | — | $10,219.30 | +0.00 | — | — | $10,219.30 | $10,219.30 | — | 09:30 open · cash $10,219.30 · no holdings · equity $10,219.30 vs prior close $10,219.30 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $10,219.30 | — | $10,219.30 | +0.00 | RVTY, CRK, MMED, EIX, CRDL, MRNA, ARCT, NVAX | — | $112.01 | $10,282.50 | RVTY×10, CRK×81, MMED×56, EIX×22, CRDL×591, MRNA×8, ARCT×77, NVAX×124 | 09:30 open · cash $10,219.30 · no holdings · equity $10,219.30 vs prior close $10,219.30 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $112.01 | RVTY×10, CRK×81, MMED×56, EIX×22, CRDL×591, MRNA×8, ARCT×77, NVAX×124 | $10,282.59 | +0.09 | CABA, BAK, DELL, MLYS, SGLD, IRD, OABI, ALEC | RVTY, CRK, MMED, EIX, CRDL, MRNA, ARCT, NVAX | $320.37 | $9,852.33 | CABA×353, BAK×657, DELL×2, MLYS×43, SGLD×197, IRD×275, OABI×252, ALEC×474 | 09:30 open · cash $112.01 (unchanged overnight, no fees) · equity $10,282.59 vs prior close $10,282.50 (+0.09) because holdings re-marked: RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; CRK×81 yday $15.54 → 09:30 $15.45 -7.29; MMED×56 yday $23.76 → 09:30 $23.88 +6.72; EIX×22 yday $55.19 → 09:30 $55.42 +5.06; CRDL×591 yday $2.17 → 09:30 $2.18 +5.91; MRNA×8 yday $150.81 → 09:30 $145.95 -38.88; ARCT×77 yday $16.74 → 09:30 $16.77 +2.31; NVAX×124 yday $10.32 → 09:30 $10.41 +11.16 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,764.83 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,579.67 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,338.50 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 216 | $5.77 | $2.79 | — | $5,089.39 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,850.53 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,603.95 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 714 | $1.75 | $9.21 | — | $1,345.24 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $186.91 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.91 | ▲ 09:30 equity $10,475.50 vs yday $10,208.28 (+267.22) | 09:30 open · cash $186.91 (unchanged overnight, no fees) · equity $10,475.50 vs prior close $10,208.28 (+267.22) because holdings re-marked: AG×60 yday $21.19 → 09:30 $21.90 +42.60; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×60 yday $21.11 → 09:30 $21.75 +38.40; HDSN×216 yday $5.57 → 09:30 $5.67 +21.60; IAG×63 yday $20.50 → 09:30 $21.17 +42.21; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×714 yday $1.75 → 09:30 $1.79 +28.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 60 | $21.90 | $2.19 | $+76.64 | $1,498.71 | ▲ +76.64 after sell → book $10,473.30; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,741.03 | ▲ +57.15 after sell → book $10,471.26; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 60 | $21.75 | $2.19 | $+61.64 | $4,043.84 | ▲ +61.64 after sell → book $10,469.07; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 216 | $5.67 | $2.83 | $-27.22 | $5,265.72 | ▼ -27.22 after sell → book $10,466.23; vs 09:30 mark -2.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 63 | $21.17 | $2.20 | $+92.64 | $6,597.23 | ▲ +92.64 after sell → book $10,464.03; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $7,946.24 | ▲ +102.43 after sell → book $10,461.90; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 714 | $1.79 | $9.34 | $+10.01 | $9,214.96 | ▲ +10.01 after sell → book $10,452.56; vs 09:30 mark -9.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,450.52 | ▲ +77.23 after sell → book $10,450.52; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,254.20 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 75 | $17.20 | $2.21 | — | $7,961.99 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,662.18 | — | combo gate; gate vol=good,ab=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 117 | $11.13 | $2.34 | — | $5,357.63 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 528 | $2.47 | $6.81 | — | $4,046.66 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 676 | $1.93 | $8.72 | — | $2,733.26 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,477.08 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 989 | $1.32 | $12.76 | — | $158.85 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.85 | ▲ 09:30 equity $11,017.11 vs yday $10,673.53 (+343.58) | 09:30 open · cash $158.85 (unchanged overnight, no fees) · equity $11,017.11 vs prior close $10,673.53 (+343.58) because holdings re-marked: AU×10 yday $121.22 → 09:30 $120.50 -7.20; AUPH×75 yday $16.65 → 09:30 $16.60 -3.75; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ARCT×117 yday $13.45 → 09:30 $13.26 -22.23; AUTL×528 yday $2.41 → 09:30 $2.36 -26.40; CRDL×676 yday $1.86 → 09:30 $1.87 +6.76; CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; CYPH×989 yday $1.42 → 09:30 $1.83 +405.49 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $1,361.81 | ▲ +6.64 after sell → book $11,015.07; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 75 | $16.60 | $2.24 | $-49.45 | $2,604.57 | ▼ -49.45 after sell → book $11,012.83; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,904.72 | ▲ +0.34 after sell → book $11,010.80; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 117 | $13.26 | $2.37 | $+244.50 | $5,453.77 | ▲ +244.50 after sell → book $11,008.43; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 528 | $2.36 | $6.91 | $-71.80 | $6,692.94 | ▼ -71.80 after sell → book $11,001.52; vs 09:30 mark -6.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 676 | $1.87 | $8.84 | $-58.12 | $7,948.22 | ▼ -58.12 after sell → book $10,992.68; vs 09:30 mark -8.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.79 | $2.07 | $-23.66 | $9,180.73 | ▼ -23.66 after sell → book $10,990.60; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 989 | $1.83 | $12.94 | $+478.70 | $10,977.67 | ▲ +478.70 after sell → book $10,977.67; vs 09:30 mark -12.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,977.67 | ▲ 09:30 equity $10,977.67 vs yday $10,977.67 (-0.00) | 09:30 open · cash $10,977.67 · no holdings · equity $10,977.67 vs prior close $10,977.67 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 847 | $1.62 | $10.93 | — | $9,594.60 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1372.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 262 | $5.22 | $3.38 | — | $8,223.58 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1372.21 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 215 | $6.38 | $2.77 | — | $6,849.11 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1372.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 807 | $1.70 | $10.41 | — | $5,466.80 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1372.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2144 | $0.64 | $20.15 | — | $4,074.48 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1372.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 146 | $9.36 | $2.43 | — | $2,705.49 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1372.21 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 103 | $13.30 | $2.30 | — | $1,333.30 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=+9.5; leftover $1372.21 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 38 | $34.48 | $2.10 | — | $20.95 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1372.21 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.95 | ▲ 09:30 equity $10,892.44 vs yday $10,892.44 (+0.00) | 09:30 open · cash $20.95 (unchanged overnight, no fees) · equity $10,892.44 vs prior close $10,892.44 (+0.00) because holdings re-marked: BMEA×847 yday $1.61 → 09:30 $1.61 +0.00; ALVO×262 yday $5.25 → 09:30 $5.25 +0.00; ZURA×215 yday $6.50 → 09:30 $6.50 +0.00; CYPH×807 yday $1.64 → 09:30 $1.64 +0.00; DEFT×2144 yday $0.62 → 09:30 $0.62 +0.00; RUM×146 yday $9.35 → 09:30 $9.35 +0.00; KURA×103 yday $13.58 → 09:30 $13.58 +0.00; EZPW×38 yday $34.69 → 09:30 $34.69 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.95 | ▲ 09:30 equity $10,934.22 vs yday $10,923.19 (+11.03) | 09:30 open · cash $20.95 (unchanged overnight, no fees) · equity $10,934.22 vs prior close $10,923.19 (+11.03) because holdings re-marked: BMEA×847 yday $1.61 → 09:30 $1.75 +118.58; ALVO×262 yday $5.25 → 09:30 $4.98 -70.74; ZURA×215 yday $6.50 → 09:30 $6.13 -79.55; CYPH×807 yday $1.64 → 09:30 $1.60 -32.28; DEFT×2144 yday $0.62 → 09:30 $0.60 -42.88; RUM×146 yday $9.35 → 09:30 $10.07 +105.12; KURA×103 yday $13.58 → 09:30 $13.63 +5.15; EZPW×38 yday $34.69 → 09:30 $35.70 +38.38 | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 847 | $1.75 | $11.08 | $+88.10 | $1,492.12 | ▲ +88.10 after sell → book $10,923.14; vs 09:30 mark -11.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 262 | $4.98 | $3.43 | $-69.69 | $2,793.45 | ▼ -69.69 after sell → book $10,919.71; vs 09:30 mark -3.43 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 215 | $6.13 | $2.82 | $-59.34 | $4,108.58 | ▼ -59.34 after sell → book $10,916.89; vs 09:30 mark -2.82 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 807 | $1.60 | $10.55 | $-101.66 | $5,389.22 | ▼ -101.66 after sell → book $10,906.33; vs 09:30 mark -10.56 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DEFT` | 2144 | $0.60 | $19.66 | $-125.58 | $6,655.96 | ▼ -125.58 after sell → book $10,886.67; vs 09:30 mark -19.66 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RUM` | 146 | $10.07 | $2.46 | $+98.77 | $8,123.72 | ▲ +98.77 after sell → book $10,884.21; vs 09:30 mark -2.46 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `KURA` | 103 | $13.63 | $2.33 | $+29.36 | $9,525.28 | ▲ +29.36 after sell → book $10,881.88; vs 09:30 mark -2.33 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `EZPW` | 38 | $35.70 | $2.12 | $+42.13 | $10,879.76 | ▲ +42.13 after sell → book $10,879.76; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,879.76 | ▲ 09:30 equity $10,879.76 vs yday $10,879.76 (-0.00) | 09:30 open · cash $10,879.76 · no holdings · equity $10,879.76 vs prior close $10,879.76 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $9,575.44 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1359.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 73 | $18.50 | $2.21 | — | $8,222.73 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1359.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $149.40 | $2.02 | — | $6,876.11 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1359.97 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $82.70 | $2.04 | — | $5,550.88 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $1359.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 72 | $18.68 | $2.21 | — | $4,203.71 | — | combo gate; gate vol=good,ab=good; list yday_gainer; ret5=+0.2; leftover $1359.97 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CRDL` | 650 | $2.09 | $8.38 | — | $2,836.82 | — | combo gate; gate vol=good,ab=good; list yday_gainer; ret5=+3.3; leftover $1359.97 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TIGR` | 247 | $5.49 | $3.19 | — | $1,477.61 | — | combo gate; gate vol=good,ab=good; list ohlc_hot; ret5=+15.9; leftover $1359.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 319 | $4.26 | $4.12 | — | $114.55 | — | combo gate; gate vol=good,ab=good; list earn_react; ret5=-0.7; leftover $1359.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $114.55 | ▼ 09:30 equity $10,245.81 vs yday $10,496.81 (-251.00) | 09:30 open · cash $114.55 (unchanged overnight, no fees) · equity $10,245.81 vs prior close $10,496.81 (-251.00) because holdings re-marked: ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BZ×73 yday $18.00 → 09:30 $17.89 -8.03; SMTC×9 yday $142.43 → 09:30 $133.04 -84.51; URBN×16 yday $78.79 → 09:30 $81.09 +36.80; BBWI×72 yday $18.65 → 09:30 $19.30 +46.80; CRDL×650 yday $2.06 → 09:30 $1.96 -65.00; TIGR×247 yday $5.06 → 09:30 $4.96 -24.70; FINV×319 yday $4.02 → 09:30 $3.46 -178.64 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.67 | $2.04 | $+31.68 | $1,450.55 | ▲ +31.68 after sell → book $10,243.78; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 73 | $17.89 | $2.23 | $-48.97 | $2,754.28 | ▼ -48.97 after sell → book $10,241.54; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $133.04 | $2.04 | $-151.29 | $3,949.61 | ▼ -151.29 after sell → book $10,239.51; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $81.09 | $2.06 | $-29.86 | $5,244.99 | ▼ -29.86 after sell → book $10,237.45; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 72 | $19.30 | $2.23 | $+40.20 | $6,632.36 | ▲ +40.20 after sell → book $10,235.22; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRDL` | 650 | $1.96 | $8.50 | $-101.39 | $7,897.86 | ▼ -101.39 after sell → book $10,226.72; vs 09:30 mark -8.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 247 | $4.96 | $3.24 | $-137.33 | $9,119.74 | ▼ -137.33 after sell → book $10,223.48; vs 09:30 mark -3.24 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 319 | $3.46 | $4.18 | $-263.49 | $10,219.30 | ▼ -263.49 after sell → book $10,219.30; vs 09:30 mark -4.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,219.30 | ▲ 09:30 equity $10,219.30 vs yday $10,219.30 (+0.00) | 09:30 open · cash $10,219.30 · no holdings · equity $10,219.30 vs prior close $10,219.30 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,219.30 | ▲ 09:30 equity $10,219.30 vs yday $10,219.30 (+0.00) | 09:30 open · cash $10,219.30 · no holdings · equity $10,219.30 vs prior close $10,219.30 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,219.30 | ▲ 09:30 equity $10,219.30 vs yday $10,219.30 (+0.00) | 09:30 open · cash $10,219.30 · no holdings · equity $10,219.30 vs prior close $10,219.30 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $8,957.88 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1277.41 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 81 | $15.70 | $2.23 | — | $7,683.95 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1277.41 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 56 | $22.78 | $2.16 | — | $6,406.11 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1277.41 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $56.78 | $2.06 | — | $5,154.89 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; ret5=+0.3; leftover $1277.41 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 591 | $2.16 | $7.62 | — | $3,870.71 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1277.41 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $151.40 | $2.01 | — | $2,657.50 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1277.41 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 77 | $16.46 | $2.22 | — | $1,387.85 | — | combo gate; gate vol=good,ab=good; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1277.41 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 124 | $10.27 | $2.36 | — | $112.01 | — | combo gate; gate vol=good,ab=good; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1277.41 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $112.01 | ▲ 09:30 equity $10,282.59 vs yday $10,282.50 (+0.09) | 09:30 open · cash $112.01 (unchanged overnight, no fees) · equity $10,282.59 vs prior close $10,282.50 (+0.09) because holdings re-marked: RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; CRK×81 yday $15.54 → 09:30 $15.45 -7.29; MMED×56 yday $23.76 → 09:30 $23.88 +6.72; EIX×22 yday $55.19 → 09:30 $55.42 +5.06; CRDL×591 yday $2.17 → 09:30 $2.18 +5.91; MRNA×8 yday $150.81 → 09:30 $145.95 -38.88; ARCT×77 yday $16.74 → 09:30 $16.77 +2.31; NVAX×124 yday $10.32 → 09:30 $10.41 +11.16 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $132.45 | $2.04 | $+61.04 | $1,434.47 | ▲ +61.04 after sell → book $10,280.55; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 81 | $15.45 | $2.26 | $-24.74 | $2,683.67 | ▼ -24.74 after sell → book $10,278.30; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 56 | $23.88 | $2.18 | $+57.26 | $4,018.77 | ▲ +57.26 after sell → book $10,276.12; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 22 | $55.42 | $2.08 | $-34.05 | $5,235.93 | ▼ -34.05 after sell → book $10,274.04; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 591 | $2.18 | $7.73 | $-3.54 | $6,516.58 | ▼ -3.54 after sell → book $10,266.31; vs 09:30 mark -7.73 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $145.95 | $2.03 | $-47.65 | $7,682.14 | ▼ -47.65 after sell → book $10,264.27; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 77 | $16.77 | $2.24 | $+19.40 | $8,971.19 | ▲ +19.40 after sell → book $10,262.03; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 124 | $10.41 | $2.39 | $+12.61 | $10,259.64 | ▲ +12.61 after sell → book $10,259.64; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 353 | $3.63 | $4.55 | — | $8,973.69 | — | combo gate; gate vol=good,ab=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1282.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 657 | $1.95 | $8.48 | — | $7,684.07 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1282.45 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $6,709.45 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1282.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 43 | $29.15 | $2.12 | — | $5,453.88 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1282.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SGLD` | 197 | $6.48 | $2.58 | — | $4,174.74 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+0.0; leftover $1282.45 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 275 | $4.66 | $3.55 | — | $2,889.70 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1282.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 252 | $5.08 | $3.25 | — | $1,606.28 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1282.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 474 | $2.70 | $6.11 | — | $320.37 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1282.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `QSI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `DEFT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RUM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `KURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `EZPW` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SLQT` | no_price | no 09:30 open |
| 2026-08-26 | `DKS` | no_price | no 09:30 open |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CABA` | 353 | 2026-09-04 @ $3.63 | combo gate; gate vol=good,ab=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1282.45 |
| `BAK` | 657 | 2026-09-04 @ $1.95 | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1282.45 |
| `DELL` | 2 | 2026-09-04 @ $486.31 | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1282.45 |
| `MLYS` | 43 | 2026-09-04 @ $29.15 | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1282.45 |
| `SGLD` | 197 | 2026-09-04 @ $6.48 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+0.0; leftover $1282.45 |
| `IRD` | 275 | 2026-09-04 @ $4.66 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1282.45 |
| `OABI` | 252 | 2026-09-04 @ $5.08 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1282.45 |
| `ALEC` | 474 | 2026-09-04 @ $2.70 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1282.45 |
