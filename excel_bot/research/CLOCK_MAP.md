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

