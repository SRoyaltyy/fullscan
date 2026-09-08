# A–JL column clocks (open / close / unknown)

_Generated 2026-09-07 · live `flatten_robust` is not changed._

## Method

1. **Measured fills A–O** — `timing_test.py` / NOTES.md (BBAI, 5 days, OHLCV ×0.4–4.0). Open fills: A,B,C,G,J,K,L,M,O. Close fills: D,E,F,H,I,N.
2. **Measured values A–O** — formula inputs, not fills. Open values: A (date), C (open), J (open-to-open). Close values: B (close), D/E/F (high/low/vol), H/I (same-day ret), G/K/M (vol/wick — fill may be open, **value is close**), N.
3. **Dependency walk** on `model.json` formulas + CF expressions. Same-row refs inherit the worse clock. Prior-row refs are already known. Unparsed / future-row → **unknown**.
4. **STOCKHISTORY** spill IR:IW aliases A:F. Weekly AP:AU treated close (contains high/low/vol).

**Unknown is not open.** Unmeasured fills (past A–O) mine at **CLOSE** even if CF deps look open — only `timing_test` licenses open entry on a fill. Value gates may be open when the formula walk proves no same-row close input. `core_score` / any def that reads D,E,F,H,I on the same row is CLOSE. That is the landmine.

## Counts

| kind | open | close | unknown |
|---|---:|---:|---:|
| fill (raw) | 99 | 17 | 159 |
| value (raw) | 44 | 149 | 82 |
| **fill mine** (unk→close) | 12 | 263 | 0 |
| **value mine** (unk→close) | 44 | 231 | 0 |
| mixed-feature conservative | 43 | 232 | 0 |

## Groups

**Fill OPEN (timing or proven):** A,B,C,G,J,K,L,M,O,IR,IS,IT

**Value OPEN:** A,C,J,Q,Z,AC,AH,BT,BV,CG,CH,DC,DE,EB,EK,EN,EP,EQ,ER,ES,ET,EU,EV,FQ,FR,FS,FU,GD,GE,GF,HF,HG,HW,II,IR,IT,IY,IZ,JB,JC … (+4)

**Fill CLOSE or unknown→close:** D,E,F,H,I,N,P,Q,R,S,T,U,V,W,X,Y,Z,AA,AB,AC,AD,AE,AF,AG,AH,AI,AJ,AK,AL,AM,AN,AO,AP,AQ,AR,AS,AT,AU,AV,AW,AX,AY,AZ,BA,BB,BC,BD,BE,BF,BG,BH,BI,BJ,BK,BL,BM,BN,BO,BP,BQ … (+203)

**Value CLOSE or unknown→close:** B,D,E,F,G,H,I,K,L,M,N,O,P,R,S,T,U,V,W,X,Y,AA,AB,AD,AE,AF,AG,AI,AJ,AK,AL,AM,AN,AO,AP,AQ,AR,AS,AT,AU,AV,AW,AX,AY,AZ,BA,BB,BC,BD,BE,BF,BG,BH,BI,BJ,BK,BL,BM,BN,BO … (+171)

## Fair inputs at the 9:30 open

Upper rows (lag≥1) of any column are knowable at 9:30. Same-row (lag 0) only if the formula walk is value-open or the fill was timing-tested open. Unknown is not open.

Standing five-cell light + green O ± AH/FR is the **baseline**, not this search space. Highlight ghosts (T/BA, weekly+lag, same-day fill counts) stay KILL unless they return as **numeric** lags or pairs.

## Same-row open-knowable formulas

Excel teammate may label more. This table is the **formula walk only** — no peek. Same-row refs inherit the worse clock. Prior-row refs are already known.

| col | what the same-row formula is | same-row refs |
|---|---|---|
| **A** | STOCKHISTORY date alias (IR) | IR |
| **C** | open price (IT, or prior IT if today's is an error) | IT |
| **J** | open-to-open return (C[t] vs C[t−1]) | C |
| **Q** | average of prior P (after the first few G-reading rows) | — (prior/external only) |
| **Z** | prior-row only (EM) | — (prior/external only) |
| **AC** | prior-row only (D,CJ,CJ) | — (prior/external only) |
| **AH** | count of prior H prints ≤ −5% | — (prior/external only) |
| **BT** | prior-row only (BR) | — (prior/external only) |
| **BV** | same-row Q,BT (those cols are value-open) plus prior — | Q,BT |
| **CG** | prior-row only (CG,CG) | — (prior/external only) |
| **CH** | prior-row only (CD,CE,CD,CD,CE,CE) | — (prior/external only) |
| **DC** | prior-row only (CV,CX,CV,CX,CX,CY) | — (prior/external only) |
| **DE** | prior-row only (DB,DB,DB,DB,F,F) | — (prior/external only) |
| **EB** | prior-row only (EB,EB,EB,EB) | — (prior/external only) |
| **EK** | prior-row only (H,H,H,H) | — (prior/external only) |
| **EN** | prior-row only (F,F,F,F,F,F) | — (prior/external only) |
| **EP** | prior-row only (H,H,H,N) | — (prior/external only) |
| **EQ** | S or L from prior CP (text) | — (prior/external only) |
| **ER** | prior-row only (H,I,H,I) | — (prior/external only) |
| **ES** | carried ER (prior-day signed move) | ER |
| **ET** | prior W/X flags | — (prior/external only) |
| **EU** | carried ET | ET |
| **EV** | same-row Z,Z,Z,Z,Z,Z (those cols are value-open) plus prior — | Z |
| **FQ** | prior-row only (H) | — (prior/external only) |
| **FR** | prior volume median over 1M and/or prior G ≥ 3 | — (prior/external only) |
| **FS** | prior-row only (CP,CP,CP,CP,CP) | — (prior/external only) |
| **FU** | prior-row only (B,F,F) | — (prior/external only) |
| **GD** | prior-row only (DN,H) | — (prior/external only) |
| **GE** | prior-row only (DN,F,F) | — (prior/external only) |
| **GF** | prior-row only (DN,B) | — (prior/external only) |
| **HF** | prior-row only (B,B) | — (prior/external only) |
| **HG** | same-row HF,HF (those cols are value-open) plus prior GU,H,H | HF |
| **HW** | prior-row only (GU,GU,CP) | — (prior/external only) |
| **II** | prior-row only (IH) | — (prior/external only) |
| **IR** | STOCKHISTORY date spill | — (prior/external only) |
| **IT** | STOCKHISTORY open spill | — (prior/external only) |
| **IY** | external VIX print (static cache) | — (prior/external only) |
| **IZ** | prior H vs prior IY | — (prior/external only) |
| **JB** | prior-row only (H,H,H,H) | — (prior/external only) |
| **JC** | prior-row only (H,H) | — (prior/external only) |
| **JD** | prior-row only (HO,HO,GQ) | — (prior/external only) |
| **JE** | same-row JD,JD (those cols are value-open) plus prior JD,JD,JD,JD | JD |
| **JF** | same-row JE (those cols are value-open) plus prior H,U,U | JE |
| **JL** | prior-row only (H,JJ,JJ,JK,JK) | — (prior/external only) |

### Same-row candidates not yet licensed as open

| col | formula shape | why it stays close at lag 0 |
|---|---|---|
| **AA** | ES[t] + 1 if N[t−1]<0 else 0 | Daily formula reads same-row ES (value-open) and prior N. Parser leaves it unknown because of _xlfn.IFS — mined close until a teammate label or a parsed walk proves it. AA[t−k], k≥1, is fair at the open either way. |
| **O** | composite of prior H/F/N/EL/CP plus same-row DD | Fill is timing-tested open. Value reads same-row DD (close/unknown) → value-close. O[t] as a number is close-entry. O[t−k], k≥1, is fair at the open. |
| **Q** | rows 3–8: IF(G[t]>2.5); row 9+: AVERAGE(prior P) | Representative daily formula is prior-P average (open). The first few rows read same-row G (close). Warmup rows are a leak if mined as open; the rest of the tape is fair. |

## Landmine

core_score = A..J includes D,E,F,H,I → CLOSE entry only

A-keyed fill defs may enter at open. Mixing A with any close-mine column forces close. G/K/M *value* gates are close even though their *fills* tested open.

## Per-column (first 80 + any measured)

| col | fill | value | fill mine | value mine | source |
|---|---|---|---|---|---|
| A | open | open | **open** | **open** | timing |
| B | open | close | **open** | **close** | timing |
| C | open | open | **open** | **open** | timing |
| D | close | close | **close** | **close** | timing |
| E | close | close | **close** | **close** | timing |
| F | close | close | **close** | **close** | timing |
| G | open | close | **open** | **close** | timing |
| H | close | close | **close** | **close** | timing |
| I | close | close | **close** | **close** | timing |
| J | open | open | **open** | **open** | timing |
| K | open | close | **open** | **close** | timing |
| L | open | unknown | **open** | **close** | timing |
| M | open | close | **open** | **close** | timing |
| N | close | close | **close** | **close** | timing |
| O | open | unknown | **open** | **close** | timing |
| Q | open | open | **close** | **open** | seed/dep |
| Z | open | open | **close** | **open** | seed/dep |
| AC | unknown | open | **close** | **open** | dep |
| AH | open | open | **close** | **open** | seed/dep |
| BT | open | open | **close** | **open** | seed/dep |
| BV | unknown | open | **close** | **open** | seed/dep |
| CG | unknown | open | **close** | **open** | seed/dep |
| CH | unknown | open | **close** | **open** | seed/dep |
| DC | unknown | open | **close** | **open** | seed/dep |
| DE | open | open | **close** | **open** | seed/dep |
| EB | unknown | open | **close** | **open** | seed/dep |
| EK | open | open | **close** | **open** | seed/dep |
| EN | unknown | open | **close** | **open** | seed/dep |
| EP | unknown | open | **close** | **open** | seed/dep |
| EQ | unknown | open | **close** | **open** | seed/dep |
| ER | unknown | open | **close** | **open** | seed/dep |
| ES | unknown | open | **close** | **open** | seed/dep |
| ET | unknown | open | **close** | **open** | seed/dep |
| EU | unknown | open | **close** | **open** | seed/dep |
| EV | unknown | open | **close** | **open** | seed/dep |
| FQ | open | open | **close** | **open** | seed/dep |
| FR | open | open | **close** | **open** | seed/dep |
| FS | open | open | **close** | **open** | seed/dep |
| FU | open | open | **close** | **open** | seed/dep |
| GD | unknown | open | **close** | **open** | seed/dep |
| GE | unknown | open | **close** | **open** | seed/dep |
| GF | unknown | open | **close** | **open** | seed/dep |
| HF | open | open | **close** | **open** | seed/dep |
| HG | unknown | open | **close** | **open** | seed/dep |
| HW | open | open | **close** | **open** | seed/dep |
| II | unknown | open | **close** | **open** | seed/dep |
| IR | open | open | **open** | **open** | dep |
| IS | open | close | **open** | **close** | dep |
| IT | open | open | **open** | **open** | dep |
| IY | unknown | open | **close** | **open** | seed/dep |
| IZ | unknown | open | **close** | **open** | seed/dep |
| JB | unknown | open | **close** | **open** | seed/dep |
| JC | unknown | open | **close** | **open** | seed/dep |
| JD | unknown | open | **close** | **open** | seed/dep |
| JE | unknown | open | **close** | **open** | seed/dep |
| JF | unknown | open | **close** | **open** | seed/dep |
| JL | unknown | open | **close** | **open** | seed/dep |

Full machine map: `clock_map.json`. Research only.

