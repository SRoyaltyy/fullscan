# Open gates board — beyond J (lag candles + open-44 tallies)

_Generated 2026-09-08 · tip `e6fa4e33` · **research only** · live frozen._

## Plain English

This cut expands past #153’s J-only overlays. Morning books are the same combine (join top-8, green-pile, weighted, long Yahoo liquid `vol_top8` / `prior_green_top8`). Gates are **avoid / elevate / presence** from Excel features that are open-knowable under the clock lock.

**Hard clock:** candles **DF / DG / DH / BB** and tallies **BQ / BU** are close same-row. They may gate an open entry only as **lag t−1+**. Same-row DF / BB / BQ at 9:30 is a **leak** — that path aborts. Open same-row substitutes already in the 44: AH, JB, JC, FQ, FR, ER, EP, EN (prior-print tallies). Never same-row H/I, M number, `core_score`, H paint, or close landmines.

**Cyrus bar:** >55% fire win-rate **and ≥30 fires**. Fire = the rule changes the book vs the same-day no-rule set. Win = rule-book mean after-fee H beats that no-rule book. Ties do not beat. n<30 that prints >55% is **PROVISIONAL** (demoted). After-fee H is the equal-weight day-mean minus 15 bp Futubull, not dollar-weighted. H+ / I+ after fees is a separate name-day hit rate — it is **not** the fire bar and is typically ~41–48% even on CLEARs. Live stays unwired.

## Clock lock

- Fill OPEN same-row: `A, B, C, G, J, K, L, M, O, IR, IS, IT`
- Value OPEN same-row (44): `A, C, J, Q, Z, AC, AH, BT, BV, CG, CH, DC, DE, EB, EK, EN, EP, EQ, ER, ES, ET, EU, EV, FQ, FR, FS, FU, GD, GE, GF, HF, HG, HW, II, IR, IT, IY, IZ, JB, JC, JD, JE, JF, JL`
- Lag-only close (this cut): `DF, DG, DH, BB, BQ, BU`
- Same-row leak abort: `DF, BB, BQ`
- Open-44 tally subs: `AH, JB, JC, FQ, FR, ER, EP, EN`
- Leak check: **PASS**
- Live: `flatten_robust` not imported, not written.

## Inventory (open-knowable + this-cut close-lag)

| col | kind | same-row | what | this cut |
|---|---|---|---|---|
| **A** | value+fill | open | date / STOCKHISTORY alias | skip — not a threshold |
| **C** | value+fill | open | open price / IT | skip — raw price |
| **J** | value+fill | open | open-to-open return | scored (#153 control) |
| **Q** | value | open* | avg of prior P (warmup rows read G) | later — warmup leak on first rows |
| **Z** | value | open | prior-row EM only | later — EM chain |
| **AC** | value | open | prior D vs CJ | later — needs CJ |
| **AH** | value | open | count of prior H ≤ −5% (6d) | scored (open-44 tally sub) |
| **BT** | value | open | prior BR (RSI-like) | later — BR chain |
| **BV** | value | open | same-row Q+BT | later — Q/BT |
| **CG** | value | open | prior CG carry | later |
| **CH** | value | open | prior CD/CE | later |
| **DC** | value | open | prior CV/CX/CY wick flags | later |
| **DE** | value | open | prior DB + vol + H | later |
| **EB** | value | open | last nonblank EB | later |
| **EK** | value | open | prior H average gap | later |
| **EN** | value | open | prior vol buckets + |H| median + CP | scored (open-44 tally sub) |
| **EP** | value | open | weighted |H[t−2]|/|H[t−1]| × N[t−1] boost | scored (open-44 tally sub) |
| **EQ** | value | open | S/L from prior CP (text) | open-44; reconstructed; not scored this cut |
| **ER** | value | open | prior H/I signed 5%/−3% | scored (open-44 tally sub) |
| **ES** | value | open | carried ER | later — carry |
| **ET** | value | open | prior W/X flags | later |
| **EU** | value | open | carried ET | later |
| **EV** | value | open | Z band hits | later |
| **FQ** | value | open | prior H > 3% | scored (open-44 tally sub) |
| **FR** | value | open | prior vol median >1M and/or G≥3 | scored (open-44 tally sub) |
| **FS** | value | open | prior CP run tally | open-44; reconstructed; not scored this cut |
| **FU** | value | open | prior B/F quality veto | later — #REF in older rows |
| **GD** | value | open | DN-gated prior H | later — DN |
| **GE** | value | open | DN-gated prior vol median | later |
| **GF** | value | open | DN-gated prior close | later |
| **HF** | value | open | stdev of prior close | later |
| **HG** | value | open | quiet-range flag from HF/GU/H | later |
| **HW** | value | open | two red GU + prior CP=2 | later |
| **II** | value | open | prior IH | later |
| **IR** | value+fill | open | STOCKHISTORY date | skip — date |
| **IT** | value+fill | open | STOCKHISTORY open | skip — raw open |
| **IY** | value | open | external VIX cache | later — not on Yahoo tape |
| **IZ** | value | open | prior H vs IY | later |
| **JB** | value | open | 7/8 prior |H|>3% | scored (open-44 tally sub) |
| **JC** | value | open | no prior H < −3% in 8d | scored (open-44 tally sub) |
| **JD** | value | open | prior HO/GQ regime | later — HO killed-ghost family |
| **JE** | value | open | JD flip | later |
| **JF** | value | open | JE + prior H/U | later |
| **JL** | value | open | prior H>0 wick combo | later |
| **B** | fill only | fill open / value close | close price fill | fill gate only; number OUT |
| **G** | fill only | fill open / value close | rel vol fill | G[t−1] number is fair; same-row number OUT |
| **K** | fill only | fill open / value close | upper wick / open | same-row number OUT |
| **L** | fill only | fill open / value close | tally fill | L number unknown→close; L[t−1] fair |
| **M** | fill only | fill open / value close | lower wick fill | M number OUT always same-row |
| **O** | fill only | fill open / value close | green O fill; O number uses same-row DD | O green is standing keep. O number same-row OUT. Not scored this cut (BQ/BU instead) |
| **IS** | fill only | fill open / value close | STOCKHISTORY close alias | fill only |
| **DF** | text | close same-row — LEAK if used at 9:30 | 1-bar patterns (Doji, Hammer, …) | lag t−1+ only; same-row abort |
| **DG** | text | close same-row / lag fair | 2-bar (Engulfing, Harami, …) | lag t−1+ only |
| **DH** | text | close same-row / lag fair | 3-bar (Morning/Evening Star, …) | lag t−1+ only |
| **BB** | num | close same-row — LEAK if used at 9:30 | SEARCH(doji, DF) | lag t−1+ only (BB_l1). Same-row abort. Not a highlight reopen |
| **BQ** | num | close same-row — LEAK if used at 9:30 | (open+high+low)/3 × vol × BU | lag t−1+ only; same-row abort |
| **BU** | num | close same-row / lag fair | sign of typical-price change | lag t−1+ only |
| **EL** | num | close same-row / lag fair | Bullish−Bearish + same-row DD | EL[t] OUT (DD). el_candles = DF:DH[t−1] count — lag |
| **CP** | num | close same-row / lag fair | high/low vs 20d close bands | CP[t] OUT. CP[t−1] feeds EQ/FS — not primary this cut |
| **N** | num | close | EL × |H|≥3% | N[t] OUT. N[t−1] stand-in inside EP only |
| **H** | label | close | intraday % | never same-row feature |
| **I** | label | close | daily % | never same-row feature |
| **core_score** | landmine | close | A..J includes D,E,F,H,I | never |

EQ / FS are open-44 and reconstructed (prior CP) but **not scored** this cut — they were not in the user’s open-44 tally list.
Worth gating later (open 44, not scored here): Q warmup, Z/AC/BT/BV chains, CG/CH/DC/DE/EB/EK carry, ES–EV, FU, GD–GF, HF/HG/HW, II, IY/IZ (VIX), JD–JF/JL (HO family). Fill-only A/B/C/G/K/L/M/O/IR/IS/IT need Excel CF dumps — Yahoo cannot reconstruct fills. O green stays the standing keep; O number same-row stays OUT.

## Which CLEAR (≥30 fires and >55%)?

**Confirmed** = liquid ranked (`vol_top8` ≈ weighted book, `prior_green_top8` ≈ green pile) CLEAR on **long and y2025**. J elev CLEARs long and fails y2025 — these do not.

### Confirmed (long + y2025, liquid ranked)

- `elev_cap2_lag_hammer` — vol_top8 long 57.4% (97/169 fires); vol_top8 y2025 60.5% (49/81 fires); prior_green_top8 y2025 55.4% (56/101 fires)
- `avoid_AH_ge1` — vol_top8 long 56.7% (253/446 fires); prior_green_top8 long 56.8% (167/294 fires); prior_green_top8 y2025 57.6% (72/125 fires)
- `avoid_FQ` — vol_top8 long 60.3% (191/317 fires); prior_green_top8 long 61.4% (191/311 fires); vol_top8 y2025 56.3% (80/142 fires); prior_green_top8 y2025 63.3% (88/139 fires)
- `elev_cap2_FR_ge1` — vol_top8 long 56.4% (155/275 fires); vol_top8 y2025 56.5% (65/115 fires)
- `avoid_ER_p1` — vol_top8 long 59.3% (172/290 fires); prior_green_top8 long 63.0% (184/292 fires); prior_green_top8 y2025 57.4% (74/129 fires)
- `avoid_EP_ge03` — vol_top8 long 57.0% (259/454 fires); prior_green_top8 long 62.4% (199/319 fires); prior_green_top8 y2025 61.1% (88/144 fires)

### Long CLEAR, y2025 did not confirm (liquid ranked)

- `elev_cap2_J_le-1` — vol_top8 55.9% (156/279 fires); prior_green_top8 55.7% (141/253 fires)
- `avoid_lag_bear_engulf` — vol_top8 57.2% (131/229 fires)
- `elev_cap2_lag_bull_engulf` — vol_top8 55.2% (153/277 fires); prior_green_top8 55.4% (154/278 fires)
- `elev_cap2_ER_m1` — vol_top8 55.9% (156/279 fires)
- `presence_lag_doji` — prior_green_top8 55.9% (80/143 fires)

### All long-tape CLEARs (includes unranked / pre813)

- ohlc_liq vol_top8 long `elev_cap2_J_le-1`: 55.9% (156/279 fires) · H+ 44.8% n=2887 · I+ 40.4% n=2887
- ohlc_liq vol_top8 long `avoid_lag_bear_engulf`: 57.2% (131/229 fires) · H+ 45.5% n=2813 · I+ 43.9% n=2813
- ohlc_liq vol_top8 long `elev_cap2_lag_bull_engulf`: 55.2% (153/277 fires) · H+ 45.2% n=2884 · I+ 42.9% n=2884
- ohlc_liq vol_top8 long `elev_cap2_lag_hammer`: 57.4% (97/169 fires) · H+ 44.9% n=2805 · I+ 43.7% n=2805
- ohlc_liq vol_top8 long `avoid_AH_ge1`: 56.7% (253/446 fires) · H+ 46.6% n=2696 · I+ 48.0% n=2696
- ohlc_liq vol_top8 long `avoid_FQ`: 60.3% (191/317 fires) · H+ 45.3% n=2818 · I+ 44.5% n=2818
- ohlc_liq vol_top8 long `elev_cap2_FR_ge1`: 56.4% (155/275 fires) · H+ 45.5% n=2887 · I+ 43.7% n=2887
- ohlc_liq vol_top8 long `avoid_ER_p1`: 59.3% (172/290 fires) · H+ 45.2% n=2840 · I+ 44.3% n=2840
- ohlc_liq vol_top8 long `elev_cap2_ER_m1`: 55.9% (156/279 fires) · H+ 45.0% n=2885 · I+ 44.0% n=2885
- ohlc_liq vol_top8 long `avoid_EP_ge03`: 57.0% (259/454 fires) · H+ 45.4% n=2694 · I+ 47.3% n=2694
- ohlc_liq prior_green_top8 long `elev_cap2_J_le-1`: 55.7% (141/253 fires) · H+ 45.6% n=2480 · I+ 39.4% n=2480
- ohlc_liq prior_green_top8 long `presence_lag_doji`: 55.9% (80/143 fires) · H+ 43.0% n=200 · I+ 45.0% n=200
- ohlc_liq prior_green_top8 long `elev_cap2_lag_bull_engulf`: 55.4% (154/278 fires) · H+ 45.7% n=2515 · I+ 45.6% n=2515
- ohlc_liq prior_green_top8 long `avoid_AH_ge1`: 56.8% (167/294 fires) · H+ 47.8% n=2448 · I+ 48.6% n=2448
- ohlc_liq prior_green_top8 long `avoid_FQ`: 61.4% (191/311 fires) · H+ 47.0% n=2447 · I+ 47.9% n=2447
- ohlc_liq prior_green_top8 long `avoid_ER_p1`: 63.0% (184/292 fires) · H+ 47.7% n=2469 · I+ 48.2% n=2469
- ohlc_liq prior_green_top8 long `avoid_EP_ge03`: 62.4% (199/319 fires) · H+ 47.6% n=2435 · I+ 48.7% n=2435
- ohlc_liq vol_top8 y2025 `avoid_lag_doji`: 56.1% (46/82 fires) · H+ 45.6% n=1192 · I+ 45.6% n=1192
- ohlc_liq vol_top8 y2025 `elev_cap2_lag_hammer`: 60.5% (49/81 fires) · H+ 45.9% n=1177 · I+ 45.8% n=1177
- ohlc_liq vol_top8 y2025 `avoid_FQ`: 56.3% (80/142 fires) · H+ 45.1% n=1176 · I+ 46.7% n=1176
- ohlc_liq vol_top8 y2025 `elev_cap2_FR_ge1`: 56.5% (65/115 fires) · H+ 45.8% n=1213 · I+ 46.4% n=1213
- ohlc_liq prior_green_top8 y2025 `elev_cap2_lag_hammer`: 55.4% (56/101 fires) · H+ 44.8% n=1048 · I+ 45.8% n=1048
- ohlc_liq prior_green_top8 y2025 `avoid_AH_ge1`: 57.6% (72/125 fires) · H+ 45.8% n=1034 · I+ 47.6% n=1034
- ohlc_liq prior_green_top8 y2025 `avoid_FQ`: 63.3% (88/139 fires) · H+ 45.9% n=1033 · I+ 47.3% n=1033
- ohlc_liq prior_green_top8 y2025 `avoid_ER_p1`: 57.4% (74/129 fires) · H+ 45.7% n=1042 · I+ 47.2% n=1042
- ohlc_liq prior_green_top8 y2025 `avoid_EP_ge03`: 61.1% (88/144 fires) · H+ 46.5% n=1025 · I+ 47.6% n=1025
- ohlc_liq unranked y2025 `avoid_FQ`: 58.8% (90/153 fires) · H+ 43.3% n=217137 · I+ 45.8% n=217137
- ohlc_liq unranked y2025 `avoid_ER_p1`: 60.0% (87/145 fires) · H+ 43.4% n=229760 · I+ 45.9% n=229760
- ohlc_liq vol_top8 pre813 `avoid_J_ge0`: 60.9% (92/151 fires) · H+ 46.7% n=1224 · I+ 33.5% n=1224
- ohlc_liq vol_top8 pre813 `elev_cap2_J_le-1`: 56.9% (87/153 fires) · H+ 46.2% n=1224 · I+ 37.4% n=1224
- ohlc_liq vol_top8 pre813 `elev_cap2_lag_doji`: 55.3% (84/152 fires) · H+ 46.1% n=1221 · I+ 42.0% n=1221
- ohlc_liq vol_top8 pre813 `avoid_lag_bear_engulf`: 57.1% (56/98 fires) · H+ 46.5% n=1224 · I+ 42.3% n=1224
- ohlc_liq vol_top8 pre813 `elev_cap2_lag_bull_engulf`: 57.5% (88/153 fires) · H+ 47.0% n=1222 · I+ 41.5% n=1222
- ohlc_liq vol_top8 pre813 `elev_cap2_lag_hammer`: 55.3% (47/85 fires) · H+ 45.3% n=1180 · I+ 42.6% n=1180
- ohlc_liq vol_top8 pre813 `avoid_BU_l1_neg`: 56.6% (81/143 fires) · H+ 47.4% n=1224 · I+ 40.2% n=1224
- ohlc_liq vol_top8 pre813 `elev_cap2_BU_l1_pos`: 56.2% (86/153 fires) · H+ 47.4% n=1224 · I+ 41.7% n=1224
- ohlc_liq vol_top8 pre813 `avoid_BQ_l1_neg`: 56.6% (81/143 fires) · H+ 47.4% n=1224 · I+ 40.2% n=1224
- ohlc_liq vol_top8 pre813 `elev_cap2_BQ_l1_pos`: 56.2% (86/153 fires) · H+ 47.4% n=1224 · I+ 41.7% n=1224
- ohlc_liq vol_top8 pre813 `avoid_AH_ge1`: 55.9% (85/152 fires) · H+ 46.4% n=1224 · I+ 46.9% n=1224
- ohlc_liq vol_top8 pre813 `elev_cap2_JC`: 56.2% (86/153 fires) · H+ 46.7% n=1224 · I+ 43.1% n=1224
- ohlc_liq vol_top8 pre813 `avoid_FQ`: 61.3% (84/137 fires) · H+ 46.8% n=1224 · I+ 43.5% n=1224
- ohlc_liq vol_top8 pre813 `elev_cap2_FR_ge1`: 56.2% (86/153 fires) · H+ 47.3% n=1224 · I+ 42.2% n=1224
- ohlc_liq vol_top8 pre813 `avoid_ER_p1`: 61.8% (84/136 fires) · H+ 47.1% n=1224 · I+ 43.6% n=1224
- ohlc_liq vol_top8 pre813 `elev_cap2_ER_m1`: 60.8% (93/153 fires) · H+ 48.1% n=1224 · I+ 44.0% n=1224
- ohlc_liq vol_top8 pre813 `elev_cap2_EN_ge2`: 57.5% (88/153 fires) · H+ 47.1% n=1224 · I+ 43.0% n=1224
- ohlc_liq vol_top8 pre813 `avoid_EP_ge03`: 55.6% (85/153 fires) · H+ 46.4% n=1224 · I+ 46.4% n=1224
- ohlc_liq prior_green_top8 pre813 `avoid_J_ge0`: 56.9% (87/153 fires) · H+ 46.6% n=1224 · I+ 20.0% n=1224
- ohlc_liq prior_green_top8 pre813 `elev_cap2_J_le-1`: 56.8% (83/146 fires) · H+ 47.8% n=1210 · I+ 37.9% n=1210
- ohlc_liq prior_green_top8 pre813 `presence_lag_doji`: 57.7% (45/78 fires) · H+ 44.2% n=104 · I+ 49.0% n=104
- ohlc_liq prior_green_top8 pre813 `elev_cap2_lag_bull_engulf`: 59.5% (91/153 fires) · H+ 48.0% n=1224 · I+ 46.4% n=1224
- ohlc_liq prior_green_top8 pre813 `avoid_FQ`: 57.9% (88/152 fires) · H+ 48.0% n=1224 · I+ 47.9% n=1224
- ohlc_liq prior_green_top8 pre813 `avoid_ER_p1`: 65.5% (97/148 fires) · H+ 49.5% n=1224 · I+ 48.6% n=1224
- ohlc_liq prior_green_top8 pre813 `avoid_EP_ge03`: 60.9% (92/151 fires) · H+ 48.4% n=1224 · I+ 49.0% n=1224
- ohlc_liq unranked pre813 `presence_lag_bullish`: 59.5% (91/153 fires) · H+ 44.8% n=168289 · I+ 45.6% n=168289
- ohlc_liq unranked pre813 `avoid_BU_l1_neg`: 55.6% (85/153 fires) · H+ 44.9% n=183390 · I+ 45.5% n=183390
- ohlc_liq unranked pre813 `avoid_BQ_l1_neg`: 55.6% (85/153 fires) · H+ 44.9% n=183390 · I+ 45.5% n=183390
- ohlc_liq unranked pre813 `avoid_FR_eq0`: 57.5% (88/153 fires) · H+ 45.2% n=335454 · I+ 46.6% n=335454

BQ/BU lag recipes **FAIL** the long liquid ranked tape (~53–54%). They only CLEAR pre813 — not a pooled-long call. #153 J elev still CLEARs long `vol_top8` / `prior_green_top8` and still fails y2025.

### Fee H+ / I+ caveat

After-fee H is the equal-weight day-mean minus 15 bp Futubull, not dollar-weighted. H+ after fees is the share of the rule’s name-days with after-fee H>0 (I+ the same for close-to-close). A fire CLEAR can still have H+ well under 55%. Do not read H+ as the Cyrus bar.

## Long Yahoo liquid (material n)

| circumstance | window | recipe | fire bar | fire win-rate | H+ after fees | I+ after fees |
|---|---|---|---|---|---|---|
| ohlc_liq vol_top8 | long | `avoid_J_ge0` | **FAIL** | 53.4% (217/406 fires) | 45.3% n=2569 | 36.0% n=2569 |
| ohlc_liq vol_top8 | long | `elev_cap2_J_le-1` | **CLEAR** | 55.9% (156/279 fires) | 44.8% n=2887 | 40.4% n=2887 |
| ohlc_liq vol_top8 | long | `avoid_lag_bearish` | **FAIL** | 48.0% (189/394 fires) | 45.3% n=2614 | 43.5% n=2614 |
| ohlc_liq vol_top8 | long | `elev_cap2_lag_bullish` | **FAIL** | 50.5% (141/279 fires) | 45.2% n=2887 | 44.1% n=2887 |
| ohlc_liq vol_top8 | long | `presence_lag_bullish` | **FAIL** | 44.5% (178/400 fires) | 41.8% n=1401 | 41.6% n=1401 |
| ohlc_liq vol_top8 | long | `avoid_lag_doji` | **FAIL** | 52.5% (106/202 fires) | 44.6% n=2829 | 43.5% n=2829 |
| ohlc_liq vol_top8 | long | `elev_cap2_lag_doji` | **FAIL** | 53.6% (149/278 fires) | 44.6% n=2881 | 43.1% n=2881 |
| ohlc_liq vol_top8 | long | `presence_lag_doji` | **FAIL** | 50.0% (101/202 fires) | 39.5% n=263 | 39.2% n=263 |
| ohlc_liq vol_top8 | long | `avoid_lag_bear_engulf` | **CLEAR** | 57.2% (131/229 fires) | 45.5% n=2813 | 43.9% n=2813 |
| ohlc_liq vol_top8 | long | `elev_cap2_lag_bull_engulf` | **CLEAR** | 55.2% (153/277 fires) | 45.2% n=2884 | 42.9% n=2884 |
| ohlc_liq vol_top8 | long | `elev_cap2_lag_hammer` | **CLEAR** | 57.4% (97/169 fires) | 44.9% n=2805 | 43.7% n=2805 |
| ohlc_liq vol_top8 | long | `avoid_lag_evening` | **FAIL** | 48.0% (86/179 fires) | 44.5% n=2838 | 43.0% n=2838 |
| ohlc_liq vol_top8 | long | `elev_cap2_lag_morning` | **FAIL** | 52.2% (142/272 fires) | 45.3% n=2859 | 42.9% n=2859 |
| ohlc_liq vol_top8 | long | `avoid_BU_l1_neg` | **FAIL** | 52.9% (210/397 fires) | 45.6% n=2552 | 42.3% n=2552 |
| ohlc_liq vol_top8 | long | `elev_cap2_BU_l1_pos` | **FAIL** | 54.0% (150/278 fires) | 45.4% n=2887 | 43.6% n=2887 |
| ohlc_liq vol_top8 | long | `avoid_BQ_l1_neg` | **FAIL** | 52.9% (210/397 fires) | 45.6% n=2552 | 42.3% n=2552 |
| ohlc_liq vol_top8 | long | `elev_cap2_BQ_l1_pos` | **FAIL** | 54.0% (150/278 fires) | 45.4% n=2887 | 43.6% n=2887 |
| ohlc_liq vol_top8 | long | `avoid_AH_ge1` | **CLEAR** | 56.7% (253/446 fires) | 46.6% n=2696 | 48.0% n=2696 |
| ohlc_liq vol_top8 | long | `elev_cap2_JC` | **FAIL** | 54.5% (152/279 fires) | 45.6% n=2887 | 44.5% n=2887 |
| ohlc_liq vol_top8 | long | `avoid_FQ` | **CLEAR** | 60.3% (191/317 fires) | 45.3% n=2818 | 44.5% n=2818 |
| ohlc_liq vol_top8 | long | `avoid_FR_eq0` | **PROVISIONAL** | 100.0% (2/2 fires) | 44.0% n=2885 | 42.9% n=2885 |
| ohlc_liq vol_top8 | long | `elev_cap2_FR_ge1` | **CLEAR** | 56.4% (155/275 fires) | 45.5% n=2887 | 43.7% n=2887 |
| ohlc_liq vol_top8 | long | `avoid_ER_p1` | **CLEAR** | 59.3% (172/290 fires) | 45.2% n=2840 | 44.3% n=2840 |
| ohlc_liq vol_top8 | long | `elev_cap2_ER_m1` | **CLEAR** | 55.9% (156/279 fires) | 45.0% n=2885 | 44.0% n=2885 |
| ohlc_liq vol_top8 | long | `avoid_EN_le0` | **FAIL** | 50.5% (48/95 fires) | 44.0% n=2350 | 43.0% n=2350 |
| ohlc_liq vol_top8 | long | `elev_cap2_EN_ge2` | **FAIL** | 52.5% (145/276 fires) | 45.0% n=2887 | 43.7% n=2887 |
| ohlc_liq vol_top8 | long | `avoid_EP_ge03` | **CLEAR** | 57.0% (259/454 fires) | 45.4% n=2694 | 47.3% n=2694 |
| ohlc_liq vol_top8 | long | `elev_cap2_JB` | **FAIL** | 49.6% (132/266 fires) | 43.7% n=2863 | 42.1% n=2863 |
| ohlc_liq prior_green_top8 | long | `avoid_J_ge0` | **FAIL** | 53.7% (158/294 fires) | 44.9% n=2263 | 19.5% n=2263 |
| ohlc_liq prior_green_top8 | long | `elev_cap2_J_le-1` | **CLEAR** | 55.7% (141/253 fires) | 45.6% n=2480 | 39.4% n=2480 |
| ohlc_liq prior_green_top8 | long | `elev_cap2_lag_bullish` | **FAIL** | 52.2% (145/278 fires) | 46.0% n=2515 | 45.2% n=2515 |
| ohlc_liq prior_green_top8 | long | `presence_lag_bullish` | **FAIL** | 40.5% (83/205 fires) | 43.9% n=2133 | 44.8% n=2133 |
| ohlc_liq prior_green_top8 | long | `avoid_lag_doji` | **FAIL** | 53.1% (76/143 fires) | 46.0% n=2487 | 45.6% n=2487 |
| ohlc_liq prior_green_top8 | long | `elev_cap2_lag_doji` | **FAIL** | 51.1% (142/278 fires) | 44.9% n=2505 | 44.7% n=2505 |
| ohlc_liq prior_green_top8 | long | `presence_lag_doji` | **CLEAR** | 55.9% (80/143 fires) | 43.0% n=200 | 45.0% n=200 |
| ohlc_liq prior_green_top8 | long | `elev_cap2_lag_bull_engulf` | **CLEAR** | 55.4% (154/278 fires) | 45.7% n=2515 | 45.6% n=2515 |
| ohlc_liq prior_green_top8 | long | `elev_cap2_lag_hammer` | **FAIL** | 53.7% (124/231 fires) | 45.5% n=2460 | 46.0% n=2460 |
| ohlc_liq prior_green_top8 | long | `elev_cap2_lag_morning` | **FAIL** | 49.6% (137/276 fires) | 44.6% n=2508 | 45.0% n=2508 |
| ohlc_liq prior_green_top8 | long | `avoid_BU_l1_neg` | **FAIL** | 51.4% (133/259 fires) | 46.1% n=2409 | 45.6% n=2409 |
| ohlc_liq prior_green_top8 | long | `elev_cap2_BU_l1_pos` | **FAIL** | 50.4% (140/278 fires) | 46.0% n=2515 | 45.1% n=2515 |
| ohlc_liq prior_green_top8 | long | `avoid_BQ_l1_neg` | **FAIL** | 51.4% (133/259 fires) | 46.1% n=2409 | 45.6% n=2409 |
| ohlc_liq prior_green_top8 | long | `elev_cap2_BQ_l1_pos` | **FAIL** | 50.4% (140/278 fires) | 46.0% n=2515 | 45.1% n=2515 |
| ohlc_liq prior_green_top8 | long | `avoid_AH_ge1` | **CLEAR** | 56.8% (167/294 fires) | 47.8% n=2448 | 48.6% n=2448 |
| ohlc_liq prior_green_top8 | long | `elev_cap2_JC` | **FAIL** | 52.5% (146/278 fires) | 45.7% n=2515 | 46.1% n=2515 |
| ohlc_liq prior_green_top8 | long | `avoid_FQ` | **CLEAR** | 61.4% (191/311 fires) | 47.0% n=2447 | 47.9% n=2447 |
| ohlc_liq prior_green_top8 | long | `elev_cap2_FR_ge1` | **FAIL** | 52.0% (143/275 fires) | 45.9% n=2515 | 45.2% n=2515 |
| ohlc_liq prior_green_top8 | long | `avoid_ER_p1` | **CLEAR** | 63.0% (184/292 fires) | 47.7% n=2469 | 48.2% n=2469 |
| ohlc_liq prior_green_top8 | long | `avoid_EN_le0` | **FAIL** | 36.0% (9/25 fires) | 45.3% n=2275 | 45.7% n=2275 |
| ohlc_liq prior_green_top8 | long | `elev_cap2_EN_ge2` | **FAIL** | 53.6% (148/276 fires) | 44.8% n=2515 | 45.0% n=2515 |
| ohlc_liq prior_green_top8 | long | `avoid_EP_ge03` | **CLEAR** | 62.4% (199/319 fires) | 47.6% n=2435 | 48.7% n=2435 |
| ohlc_liq prior_green_top8 | long | `elev_cap2_JB` | **FAIL** | 47.8% (117/245 fires) | 44.9% n=2471 | 44.2% n=2471 |
| ohlc_liq unranked | long | `avoid_J_ge0` | **FAIL** | 48.7% (201/413 fires) | 45.5% n=298877 | 39.0% n=298877 |
| ohlc_liq unranked | long | `avoid_lag_bearish` | **FAIL** | 51.3% (213/415 fires) | 44.1% n=387740 | 45.6% n=387740 |
| ohlc_liq unranked | long | `presence_lag_bullish` | **FAIL** | 53.4% (219/410 fires) | 44.0% n=287934 | 45.4% n=287934 |
| ohlc_liq unranked | long | `avoid_lag_doji` | **FAIL** | 53.6% (178/332 fires) | 44.6% n=552986 | 46.5% n=552986 |
| ohlc_liq unranked | long | `presence_lag_doji` | **FAIL** | 46.4% (154/332 fires) | 42.9% n=64381 | 44.3% n=64381 |
| ohlc_liq unranked | long | `avoid_lag_bear_engulf` | **FAIL** | 53.6% (179/334 fires) | 44.3% n=543678 | 46.1% n=543678 |
| ohlc_liq unranked | long | `avoid_lag_evening` | **FAIL** | 50.5% (162/321 fires) | 44.4% n=566814 | 46.3% n=566814 |
| ohlc_liq unranked | long | `avoid_BU_l1_neg` | **FAIL** | 52.4% (221/422 fires) | 43.4% n=320332 | 44.9% n=320332 |
| ohlc_liq unranked | long | `avoid_BQ_l1_neg` | **FAIL** | 52.4% (221/422 fires) | 43.4% n=320332 | 44.9% n=320332 |
| ohlc_liq unranked | long | `avoid_AH_ge1` | **FAIL** | 53.8% (242/450 fires) | 43.9% n=478192 | 46.2% n=478192 |
| ohlc_liq unranked | long | `avoid_FQ` | **FAIL** | 53.2% (183/344 fires) | 44.1% n=541153 | 46.3% n=541153 |
| ohlc_liq unranked | long | `avoid_FR_eq0` | **FAIL** | 53.1% (147/277 fires) | 44.3% n=579120 | 46.2% n=579120 |
| ohlc_liq unranked | long | `avoid_ER_p1` | **FAIL** | 53.7% (173/322 fires) | 44.3% n=577773 | 46.4% n=577773 |
| ohlc_liq unranked | long | `avoid_EN_le0` | **FAIL** | 50.0% (187/374 fires) | 44.0% n=215323 | 45.8% n=215323 |
| ohlc_liq unranked | long | `avoid_EP_ge03` | **FAIL** | 52.9% (240/454 fires) | 43.6% n=464327 | 46.0% n=464327 |
| ohlc_liq vol_top8 | y2025 | `avoid_J_ge0` | **FAIL** | 51.5% (86/167 fires) | 45.2% n=1084 | 40.0% n=1084 |
| ohlc_liq vol_top8 | y2025 | `elev_cap2_J_le-1` | **FAIL** | 53.8% (64/119 fires) | 44.8% n=1213 | 43.4% n=1213 |
| ohlc_liq vol_top8 | y2025 | `avoid_lag_bearish` | **FAIL** | 48.1% (77/160 fires) | 44.5% n=1106 | 43.9% n=1106 |
| ohlc_liq vol_top8 | y2025 | `elev_cap2_lag_bullish` | **FAIL** | 52.1% (62/119 fires) | 45.7% n=1213 | 45.7% n=1213 |
| ohlc_liq vol_top8 | y2025 | `presence_lag_bullish` | **FAIL** | 47.8% (77/161 fires) | 42.0% n=615 | 43.6% n=615 |
| ohlc_liq vol_top8 | y2025 | `avoid_lag_doji` | **CLEAR** | 56.1% (46/82 fires) | 45.6% n=1192 | 45.6% n=1192 |
| ohlc_liq vol_top8 | y2025 | `elev_cap2_lag_doji` | **FAIL** | 53.8% (64/119 fires) | 44.5% n=1210 | 45.3% n=1210 |
| ohlc_liq vol_top8 | y2025 | `presence_lag_doji` | **FAIL** | 41.5% (34/82 fires) | 34.0% n=103 | 38.8% n=103 |
| ohlc_liq vol_top8 | y2025 | `avoid_lag_bear_engulf` | **FAIL** | 53.1% (52/98 fires) | 45.4% n=1180 | 45.8% n=1180 |
| ohlc_liq vol_top8 | y2025 | `elev_cap2_lag_bull_engulf` | **FAIL** | 53.8% (63/117 fires) | 45.1% n=1212 | 44.9% n=1212 |
| ohlc_liq vol_top8 | y2025 | `elev_cap2_lag_hammer` | **CLEAR** | 60.5% (49/81 fires) | 45.9% n=1177 | 45.8% n=1177 |
| ohlc_liq vol_top8 | y2025 | `avoid_lag_evening` | **FAIL** | 46.8% (36/77 fires) | 44.9% n=1193 | 44.9% n=1193 |
| ohlc_liq vol_top8 | y2025 | `elev_cap2_lag_morning` | **FAIL** | 51.3% (59/115 fires) | 45.3% n=1202 | 44.9% n=1202 |
| ohlc_liq vol_top8 | y2025 | `avoid_BU_l1_neg` | **FAIL** | 52.4% (86/164 fires) | 45.3% n=1084 | 45.0% n=1084 |
| ohlc_liq vol_top8 | y2025 | `elev_cap2_BU_l1_pos` | **FAIL** | 50.0% (59/118 fires) | 45.4% n=1213 | 46.4% n=1213 |
| ohlc_liq vol_top8 | y2025 | `avoid_BQ_l1_neg` | **FAIL** | 52.4% (86/164 fires) | 45.3% n=1084 | 45.0% n=1084 |
| ohlc_liq vol_top8 | y2025 | `elev_cap2_BQ_l1_pos` | **FAIL** | 50.0% (59/118 fires) | 45.4% n=1213 | 46.4% n=1213 |
| ohlc_liq vol_top8 | y2025 | `avoid_AH_ge1` | **FAIL** | 51.6% (98/190 fires) | 47.7% n=1122 | 49.8% n=1122 |
| ohlc_liq vol_top8 | y2025 | `elev_cap2_JC` | **FAIL** | 54.6% (65/119 fires) | 46.8% n=1213 | 47.6% n=1213 |
| ohlc_liq vol_top8 | y2025 | `avoid_FQ` | **CLEAR** | 56.3% (80/142 fires) | 45.1% n=1176 | 46.7% n=1176 |
| ohlc_liq vol_top8 | y2025 | `avoid_FR_eq0` | **PROVISIONAL** | 100.0% (2/2 fires) | 44.5% n=1211 | 45.1% n=1211 |
| ohlc_liq vol_top8 | y2025 | `elev_cap2_FR_ge1` | **CLEAR** | 56.5% (65/115 fires) | 45.8% n=1213 | 46.4% n=1213 |
| ohlc_liq vol_top8 | y2025 | `avoid_ER_p1` | **FAIL** | 51.5% (67/130 fires) | 44.5% n=1184 | 46.0% n=1184 |
| ohlc_liq vol_top8 | y2025 | `elev_cap2_ER_m1` | **FAIL** | 48.7% (58/119 fires) | 43.0% n=1211 | 44.9% n=1211 |
| ohlc_liq vol_top8 | y2025 | `avoid_EN_le0` | **FAIL** | 50.0% (22/44 fires) | 44.0% n=1009 | 45.1% n=1009 |
| ohlc_liq vol_top8 | y2025 | `elev_cap2_EN_ge2` | **FAIL** | 47.4% (55/116 fires) | 44.9% n=1213 | 45.7% n=1213 |
| ohlc_liq vol_top8 | y2025 | `avoid_EP_ge03` | **FAIL** | 54.0% (107/198 fires) | 45.9% n=1121 | 49.7% n=1121 |
| ohlc_liq vol_top8 | y2025 | `elev_cap2_JB` | **FAIL** | 42.5% (45/106 fires) | 42.9% n=1197 | 43.3% n=1197 |
| ohlc_liq prior_green_top8 | y2025 | `avoid_J_ge0` | **FAIL** | 47.2% (60/127 fires) | 42.7% n=963 | 18.5% n=963 |
| ohlc_liq prior_green_top8 | y2025 | `elev_cap2_J_le-1` | **FAIL** | 53.0% (53/100 fires) | 43.5% n=1050 | 40.4% n=1050 |
| ohlc_liq prior_green_top8 | y2025 | `elev_cap2_lag_bullish` | **FAIL** | 50.8% (60/118 fires) | 45.3% n=1070 | 45.5% n=1070 |
| ohlc_liq prior_green_top8 | y2025 | `presence_lag_bullish` | **FAIL** | 42.2% (35/83 fires) | 43.4% n=914 | 45.4% n=914 |
| ohlc_liq prior_green_top8 | y2025 | `avoid_lag_doji` | **FAIL** | 54.9% (28/51 fires) | 44.5% n=1060 | 46.0% n=1060 |
| ohlc_liq prior_green_top8 | y2025 | `elev_cap2_lag_doji` | **FAIL** | 52.5% (62/118 fires) | 44.0% n=1068 | 45.1% n=1068 |
| ohlc_liq prior_green_top8 | y2025 | `presence_lag_doji` | **FAIL** | 49.0% (25/51 fires) | 39.4% n=71 | 39.4% n=71 |
| ohlc_liq prior_green_top8 | y2025 | `elev_cap2_lag_bull_engulf` | **FAIL** | 49.2% (58/118 fires) | 43.5% n=1070 | 44.6% n=1070 |
| ohlc_liq prior_green_top8 | y2025 | `elev_cap2_lag_hammer` | **CLEAR** | 55.4% (56/101 fires) | 44.8% n=1048 | 45.8% n=1048 |
| ohlc_liq prior_green_top8 | y2025 | `elev_cap2_lag_morning` | **FAIL** | 47.9% (56/117 fires) | 43.4% n=1068 | 44.6% n=1068 |
| ohlc_liq prior_green_top8 | y2025 | `avoid_BU_l1_neg` | **FAIL** | 53.2% (59/111 fires) | 46.6% n=1022 | 47.0% n=1022 |
| ohlc_liq prior_green_top8 | y2025 | `elev_cap2_BU_l1_pos` | **FAIL** | 48.3% (57/118 fires) | 44.8% n=1070 | 45.3% n=1070 |
| ohlc_liq prior_green_top8 | y2025 | `avoid_BQ_l1_neg` | **FAIL** | 53.2% (59/111 fires) | 46.6% n=1022 | 47.0% n=1022 |
| ohlc_liq prior_green_top8 | y2025 | `elev_cap2_BQ_l1_pos` | **FAIL** | 48.3% (57/118 fires) | 44.8% n=1070 | 45.3% n=1070 |
| ohlc_liq prior_green_top8 | y2025 | `avoid_AH_ge1` | **CLEAR** | 57.6% (72/125 fires) | 45.8% n=1034 | 47.6% n=1034 |
| ohlc_liq prior_green_top8 | y2025 | `elev_cap2_JC` | **FAIL** | 53.4% (63/118 fires) | 44.0% n=1070 | 45.6% n=1070 |
| ohlc_liq prior_green_top8 | y2025 | `avoid_FQ` | **CLEAR** | 63.3% (88/139 fires) | 45.9% n=1033 | 47.3% n=1033 |
| ohlc_liq prior_green_top8 | y2025 | `elev_cap2_FR_ge1` | **FAIL** | 48.7% (56/115 fires) | 45.1% n=1070 | 45.6% n=1070 |
| ohlc_liq prior_green_top8 | y2025 | `avoid_ER_p1` | **CLEAR** | 57.4% (74/129 fires) | 45.7% n=1042 | 47.2% n=1042 |
| ohlc_liq prior_green_top8 | y2025 | `avoid_EN_le0` | **FAIL** | 37.5% (6/16 fires) | 43.9% n=971 | 45.8% n=971 |
| ohlc_liq prior_green_top8 | y2025 | `elev_cap2_EN_ge2` | **FAIL** | 52.6% (61/116 fires) | 43.5% n=1070 | 44.9% n=1070 |
| ohlc_liq prior_green_top8 | y2025 | `avoid_EP_ge03` | **CLEAR** | 61.1% (88/144 fires) | 46.5% n=1025 | 47.6% n=1025 |
| ohlc_liq prior_green_top8 | y2025 | `elev_cap2_JB` | **FAIL** | 50.5% (48/95 fires) | 43.1% n=1051 | 44.4% n=1051 |
| ohlc_liq unranked | y2025 | `avoid_J_ge0` | **FAIL** | 51.2% (88/172 fires) | 44.9% n=116611 | 39.8% n=116611 |
| ohlc_liq unranked | y2025 | `avoid_lag_bearish` | **FAIL** | 51.1% (89/174 fires) | 43.0% n=151681 | 45.4% n=151681 |
| ohlc_liq unranked | y2025 | `presence_lag_bullish` | **FAIL** | 47.0% (79/168 fires) | 42.9% n=112419 | 45.3% n=112419 |
| ohlc_liq unranked | y2025 | `avoid_lag_doji` | **FAIL** | 55.0% (77/140 fires) | 43.5% n=218096 | 45.9% n=218096 |
| ohlc_liq unranked | y2025 | `presence_lag_doji` | **FAIL** | 45.0% (63/140 fires) | 42.2% n=25151 | 44.6% n=25151 |
| ohlc_liq unranked | y2025 | `avoid_lag_bear_engulf` | **FAIL** | 49.0% (71/145 fires) | 43.3% n=213829 | 45.7% n=213829 |
| ohlc_liq unranked | y2025 | `avoid_lag_evening` | **FAIL** | 52.2% (71/136 fires) | 43.4% n=223299 | 45.8% n=223299 |
| ohlc_liq unranked | y2025 | `avoid_BU_l1_neg` | **FAIL** | 52.5% (94/179 fires) | 41.5% n=128524 | 44.2% n=128524 |
| ohlc_liq unranked | y2025 | `avoid_BQ_l1_neg` | **FAIL** | 52.5% (94/179 fires) | 41.5% n=128524 | 44.2% n=128524 |
| ohlc_liq unranked | y2025 | `avoid_AH_ge1` | **FAIL** | 50.3% (97/193 fires) | 42.9% n=193362 | 45.5% n=193362 |
| ohlc_liq unranked | y2025 | `avoid_FQ` | **CLEAR** | 58.8% (90/153 fires) | 43.3% n=217137 | 45.8% n=217137 |
| ohlc_liq unranked | y2025 | `avoid_FR_eq0` | **FAIL** | 47.9% (56/117 fires) | 43.3% n=228306 | 45.7% n=228306 |
| ohlc_liq unranked | y2025 | `avoid_ER_p1` | **CLEAR** | 60.0% (87/145 fires) | 43.4% n=229760 | 45.9% n=229760 |
| ohlc_liq unranked | y2025 | `avoid_EN_le0` | **FAIL** | 48.5% (79/163 fires) | 42.3% n=82635 | 44.9% n=82635 |
| ohlc_liq unranked | y2025 | `avoid_EP_ge03` | **FAIL** | 50.5% (100/198 fires) | 42.7% n=190088 | 45.5% n=190088 |
| ohlc_liq vol_top8 | pre813 | `avoid_J_ge0` | **CLEAR** | 60.9% (92/151 fires) | 46.7% n=1224 | 33.5% n=1224 |
| ohlc_liq vol_top8 | pre813 | `elev_cap2_J_le-1` | **CLEAR** | 56.9% (87/153 fires) | 46.2% n=1224 | 37.4% n=1224 |
| ohlc_liq vol_top8 | pre813 | `avoid_lag_bearish` | **FAIL** | 46.6% (69/148 fires) | 46.7% n=1224 | 43.1% n=1224 |
| ohlc_liq vol_top8 | pre813 | `elev_cap2_lag_bullish` | **FAIL** | 50.3% (77/153 fires) | 46.3% n=1224 | 43.8% n=1224 |
| ohlc_liq vol_top8 | pre813 | `presence_lag_bullish` | **FAIL** | 35.1% (53/151 fires) | 41.4% n=592 | 39.0% n=592 |
| ohlc_liq vol_top8 | pre813 | `avoid_lag_doji` | **FAIL** | 50.6% (42/83 fires) | 44.9% n=1224 | 42.3% n=1224 |
| ohlc_liq vol_top8 | pre813 | `elev_cap2_lag_doji` | **CLEAR** | 55.3% (84/152 fires) | 46.1% n=1221 | 42.0% n=1221 |
| ohlc_liq vol_top8 | pre813 | `presence_lag_doji` | **FAIL** | 54.2% (45/83 fires) | 44.7% n=114 | 37.7% n=114 |
| ohlc_liq vol_top8 | pre813 | `avoid_lag_bear_engulf` | **CLEAR** | 57.1% (56/98 fires) | 46.5% n=1224 | 42.3% n=1224 |
| ohlc_liq vol_top8 | pre813 | `elev_cap2_lag_bull_engulf` | **CLEAR** | 57.5% (88/153 fires) | 47.0% n=1222 | 41.5% n=1222 |
| ohlc_liq vol_top8 | pre813 | `elev_cap2_lag_hammer` | **CLEAR** | 55.3% (47/85 fires) | 45.3% n=1180 | 42.6% n=1180 |
| ohlc_liq vol_top8 | pre813 | `avoid_lag_evening` | **FAIL** | 49.3% (37/75 fires) | 44.9% n=1224 | 41.4% n=1224 |
| ohlc_liq vol_top8 | pre813 | `elev_cap2_lag_morning` | **FAIL** | 53.3% (80/150 fires) | 47.0% n=1207 | 41.8% n=1207 |
| ohlc_liq vol_top8 | pre813 | `avoid_BU_l1_neg` | **CLEAR** | 56.6% (81/143 fires) | 47.4% n=1224 | 40.2% n=1224 |
| ohlc_liq vol_top8 | pre813 | `elev_cap2_BU_l1_pos` | **CLEAR** | 56.2% (86/153 fires) | 47.4% n=1224 | 41.7% n=1224 |
| ohlc_liq vol_top8 | pre813 | `avoid_BQ_l1_neg` | **CLEAR** | 56.6% (81/143 fires) | 47.4% n=1224 | 40.2% n=1224 |
| ohlc_liq vol_top8 | pre813 | `elev_cap2_BQ_l1_pos` | **CLEAR** | 56.2% (86/153 fires) | 47.4% n=1224 | 41.7% n=1224 |
| ohlc_liq vol_top8 | pre813 | `avoid_AH_ge1` | **CLEAR** | 55.9% (85/152 fires) | 46.4% n=1224 | 46.9% n=1224 |
| ohlc_liq vol_top8 | pre813 | `elev_cap2_JC` | **CLEAR** | 56.2% (86/153 fires) | 46.7% n=1224 | 43.1% n=1224 |
| ohlc_liq vol_top8 | pre813 | `avoid_FQ` | **CLEAR** | 61.3% (84/137 fires) | 46.8% n=1224 | 43.5% n=1224 |
| ohlc_liq vol_top8 | pre813 | `elev_cap2_FR_ge1` | **CLEAR** | 56.2% (86/153 fires) | 47.3% n=1224 | 42.2% n=1224 |
| ohlc_liq vol_top8 | pre813 | `avoid_ER_p1` | **CLEAR** | 61.8% (84/136 fires) | 47.1% n=1224 | 43.6% n=1224 |
| ohlc_liq vol_top8 | pre813 | `elev_cap2_ER_m1` | **CLEAR** | 60.8% (93/153 fires) | 48.1% n=1224 | 44.0% n=1224 |
| ohlc_liq vol_top8 | pre813 | `elev_cap2_EN_ge2` | **CLEAR** | 57.5% (88/153 fires) | 47.1% n=1224 | 43.0% n=1224 |
| ohlc_liq vol_top8 | pre813 | `avoid_EP_ge03` | **CLEAR** | 55.6% (85/153 fires) | 46.4% n=1224 | 46.4% n=1224 |
| ohlc_liq vol_top8 | pre813 | `elev_cap2_JB` | **FAIL** | 54.2% (83/153 fires) | 45.6% n=1216 | 41.3% n=1216 |
| ohlc_liq prior_green_top8 | pre813 | `avoid_J_ge0` | **CLEAR** | 56.9% (87/153 fires) | 46.6% n=1224 | 20.0% n=1224 |
| ohlc_liq prior_green_top8 | pre813 | `elev_cap2_J_le-1` | **CLEAR** | 56.8% (83/146 fires) | 47.8% n=1210 | 37.9% n=1210 |
| ohlc_liq prior_green_top8 | pre813 | `elev_cap2_lag_bullish` | **FAIL** | 51.0% (78/153 fires) | 47.0% n=1224 | 44.7% n=1224 |
| ohlc_liq prior_green_top8 | pre813 | `presence_lag_bullish` | **FAIL** | 34.9% (38/109 fires) | 44.6% n=1028 | 44.6% n=1028 |
| ohlc_liq prior_green_top8 | pre813 | `avoid_lag_doji` | **FAIL** | 52.6% (41/78 fires) | 48.0% n=1224 | 45.4% n=1224 |
| ohlc_liq prior_green_top8 | pre813 | `elev_cap2_lag_doji` | **FAIL** | 49.7% (76/153 fires) | 46.0% n=1216 | 44.3% n=1216 |
| ohlc_liq prior_green_top8 | pre813 | `presence_lag_doji` | **CLEAR** | 57.7% (45/78 fires) | 44.2% n=104 | 49.0% n=104 |
| ohlc_liq prior_green_top8 | pre813 | `elev_cap2_lag_bull_engulf` | **CLEAR** | 59.5% (91/153 fires) | 48.0% n=1224 | 46.4% n=1224 |
| ohlc_liq prior_green_top8 | pre813 | `elev_cap2_lag_hammer` | **FAIL** | 51.6% (64/124 fires) | 46.6% n=1193 | 46.5% n=1193 |
| ohlc_liq prior_green_top8 | pre813 | `elev_cap2_lag_morning` | **FAIL** | 52.6% (80/152 fires) | 46.0% n=1219 | 45.4% n=1219 |
| ohlc_liq prior_green_top8 | pre813 | `avoid_BU_l1_neg` | **FAIL** | 51.2% (65/127 fires) | 46.7% n=1224 | 44.7% n=1224 |
| ohlc_liq prior_green_top8 | pre813 | `elev_cap2_BU_l1_pos` | **FAIL** | 51.0% (78/153 fires) | 47.6% n=1224 | 44.6% n=1224 |
| ohlc_liq prior_green_top8 | pre813 | `avoid_BQ_l1_neg` | **FAIL** | 51.2% (65/127 fires) | 46.7% n=1224 | 44.7% n=1224 |
| ohlc_liq prior_green_top8 | pre813 | `elev_cap2_BQ_l1_pos` | **FAIL** | 51.0% (78/153 fires) | 47.6% n=1224 | 44.6% n=1224 |
| ohlc_liq prior_green_top8 | pre813 | `avoid_AH_ge1` | **FAIL** | 54.9% (79/144 fires) | 49.4% n=1224 | 49.0% n=1224 |
| ohlc_liq prior_green_top8 | pre813 | `elev_cap2_JC` | **FAIL** | 51.0% (78/153 fires) | 47.5% n=1224 | 46.5% n=1224 |
| ohlc_liq prior_green_top8 | pre813 | `avoid_FQ` | **CLEAR** | 57.9% (88/152 fires) | 48.0% n=1224 | 47.9% n=1224 |
| ohlc_liq prior_green_top8 | pre813 | `elev_cap2_FR_ge1` | **FAIL** | 52.3% (80/153 fires) | 46.8% n=1224 | 44.8% n=1224 |
| ohlc_liq prior_green_top8 | pre813 | `avoid_ER_p1` | **CLEAR** | 65.5% (97/148 fires) | 49.5% n=1224 | 48.6% n=1224 |
| ohlc_liq prior_green_top8 | pre813 | `elev_cap2_EN_ge2` | **FAIL** | 53.6% (82/153 fires) | 46.2% n=1224 | 45.1% n=1224 |
| ohlc_liq prior_green_top8 | pre813 | `avoid_EP_ge03` | **CLEAR** | 60.9% (92/151 fires) | 48.4% n=1224 | 49.0% n=1224 |
| ohlc_liq prior_green_top8 | pre813 | `elev_cap2_JB` | **FAIL** | 46.9% (67/143 fires) | 46.9% n=1199 | 43.9% n=1199 |
| ohlc_liq unranked | pre813 | `avoid_J_ge0` | **FAIL** | 49.7% (76/153 fires) | 46.3% n=174214 | 38.7% n=174214 |
| ohlc_liq unranked | pre813 | `avoid_lag_bearish` | **FAIL** | 52.9% (81/153 fires) | 45.0% n=226176 | 45.9% n=226176 |
| ohlc_liq unranked | pre813 | `presence_lag_bullish` | **CLEAR** | 59.5% (91/153 fires) | 44.8% n=168289 | 45.6% n=168289 |
| ohlc_liq unranked | pre813 | `avoid_lag_doji` | **FAIL** | 53.6% (82/153 fires) | 45.4% n=320292 | 47.0% n=320292 |
| ohlc_liq unranked | pre813 | `presence_lag_doji` | **FAIL** | 46.4% (71/153 fires) | 43.6% n=37517 | 44.1% n=37517 |
| ohlc_liq unranked | pre813 | `avoid_lag_bear_engulf` | **FAIL** | 54.9% (84/153 fires) | 45.1% n=315407 | 46.4% n=315407 |
| ohlc_liq unranked | pre813 | `avoid_lag_evening` | **FAIL** | 49.7% (76/153 fires) | 45.2% n=328507 | 46.7% n=328507 |
| ohlc_liq unranked | pre813 | `avoid_BU_l1_neg` | **CLEAR** | 55.6% (85/153 fires) | 44.9% n=183390 | 45.5% n=183390 |
| ohlc_liq unranked | pre813 | `avoid_BQ_l1_neg` | **CLEAR** | 55.6% (85/153 fires) | 44.9% n=183390 | 45.5% n=183390 |
| ohlc_liq unranked | pre813 | `avoid_AH_ge1` | **FAIL** | 49.0% (75/153 fires) | 44.9% n=272307 | 46.7% n=272307 |
| ohlc_liq unranked | pre813 | `avoid_FQ` | **FAIL** | 44.4% (68/153 fires) | 44.9% n=309738 | 46.7% n=309738 |
| ohlc_liq unranked | pre813 | `avoid_FR_eq0` | **CLEAR** | 57.5% (88/153 fires) | 45.2% n=335454 | 46.6% n=335454 |
| ohlc_liq unranked | pre813 | `avoid_ER_p1` | **FAIL** | 43.8% (67/153 fires) | 45.1% n=332827 | 46.8% n=332827 |
| ohlc_liq unranked | pre813 | `avoid_EN_le0` | **FAIL** | 51.6% (79/153 fires) | 45.3% n=127016 | 46.4% n=127016 |
| ohlc_liq unranked | pre813 | `avoid_EP_ge03` | **FAIL** | 47.1% (72/153 fires) | 44.4% n=262113 | 46.4% n=262113 |

## Native dumps (cannot reach 30 — demoted)

Finviz / join / stock_book window is ~16 weekdays. Any >55% print here is **PROVISIONAL**.

| circumstance | window | recipe | fire bar | fire win-rate | H+ | I+ |
|---|---|---|---|---|---|---|

## Cards

| recipe | family | atoms | status |
|---|---|---|---|
| `avoid_J_ge0` | avoid | J[t] | CLEAR (see table) |
| `elev_cap2_J_le-1` | elevate | J[t] | CLEAR (see table) |
| `avoid_lag_bearish` | avoid | DF[t−1], DG[t−1], DH[t−1] | research · not live · not KEEP holds |
| `elev_cap2_lag_bullish` | elevate | DF[t−1], DG[t−1], DH[t−1] | research · not live · not KEEP holds |
| `presence_lag_bullish` | presence | DF[t−1], DG[t−1], DH[t−1] | CLEAR (see table) |
| `avoid_lag_doji` | avoid | DF[t−1], BB[t−1] | CLEAR (see table) |
| `elev_cap2_lag_doji` | elevate | DF[t−1], BB[t−1] | CLEAR (see table) |
| `presence_lag_doji` | presence | BB[t−1], DF[t−1] | CLEAR (see table) |
| `avoid_lag_bear_engulf` | avoid | DG[t−1] | CLEAR (see table) |
| `elev_cap2_lag_bull_engulf` | elevate | DG[t−1] | CLEAR (see table) |
| `elev_cap2_lag_hammer` | elevate | DF[t−1] | CLEAR (see table) |
| `avoid_lag_evening` | avoid | DH[t−1] | research · not live · not KEEP holds |
| `elev_cap2_lag_morning` | elevate | DH[t−1] | research · not live · not KEEP holds |
| `avoid_BU_l1_neg` | avoid | BU[t−1] | CLEAR (see table) |
| `elev_cap2_BU_l1_pos` | elevate | BU[t−1] | CLEAR (see table) |
| `avoid_BQ_l1_neg` | avoid | BQ[t−1] | CLEAR (see table) |
| `elev_cap2_BQ_l1_pos` | elevate | BQ[t−1] | CLEAR (see table) |
| `avoid_AH_ge1` | avoid | AH[t] | CLEAR (see table) |
| `elev_cap2_JC` | elevate | JC[t] | CLEAR (see table) |
| `avoid_FQ` | avoid | FQ[t] | CLEAR (see table) |
| `avoid_FR_eq0` | avoid | FR[t] | CLEAR (see table) |
| `elev_cap2_FR_ge1` | elevate | FR[t] | CLEAR (see table) |
| `avoid_ER_p1` | avoid | ER[t] | CLEAR (see table) |
| `elev_cap2_ER_m1` | elevate | ER[t] | CLEAR (see table) |
| `avoid_EN_le0` | avoid | EN[t] | research · not live · not KEEP holds |
| `elev_cap2_EN_ge2` | elevate | EN[t] | CLEAR (see table) |
| `avoid_EP_ge03` | avoid | EP[t] | CLEAR (see table) |
| `elev_cap2_JB` | elevate | JB[t] | research · not live · not KEEP holds |

## Explicitly not live

No recipe is wired into `flatten_robust` or cash/paper. Docs are not live. Do not treat a long-tape CLEAR as a ship. y2025 must still confirm a liquid ranked recipe before anyone talks wire.

## Source

`CLOCK_MAP.md` / `OPEN_SAME_ROW_LABELS.md` / `excel_clock_gate.py` · PR #153 J-only · this board tip `e6fa4e33`. Research only. Live frozen.
