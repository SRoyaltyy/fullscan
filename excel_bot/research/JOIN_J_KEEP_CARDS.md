# Research cards — Excel J × fullscan join (post-8-13)

_Generated 2026-09-08 · tip `bd1d298b` / `JOIN_POST_813.md` · **research cards only** · live frozen._

## Plain English

Post-8-13 was discovery. Prove (weekday sessions after 2026-08-25, J vs prior weekday Open, Sunday dumps held out) **does not re-clear** the ship bar. Family is **CONDITIONAL** — not KEEP holds. Do not wire.

Discovery half still printed. Pooled weekday leftover still prints. The later half does not. Flatten does not confirm a blanket overlay.

Clock: **J value is open** (today Open vs prior weekday session Open). Never same-row H/I or H-fill paint.

## J clock / leak

**Verdict: PASS.** The J number used by both recipes is open-knowable at that day’s 9:30 open.

| check | result |
|---|---|
| Excel map | `CLOCK_MAP.md`: J value-open = C[t] vs C[t−1]. C = Open / IT. In `value_mine_open` (44). |
| Dump formula | `J = (Finviz Open[t] − Finviz Open[prior weekday]) / prior Open` |
| Same-row H/I | labels only — never features. J ≠ H and J ≠ I on checked name-days. |
| M number / H paint / `core_score` | not used |
| High / Low / Close / Price | loaded as labels / reconstruction; **not** in J |
| File clock | Finviz CSVs are EOD; Open column is still the 09:30 print |
| Weekend / Sunday | Sat/Sun Finviz Opens skipped; Sunday join dumps held out |
| Stale | 08-13 vs 04-26 Open unused. 08-26 missing Finviz → 08-27 J uses 08-25 Open (hole, not future). |

pick_book reads only J flags + join rank. See `JOIN_POST_813.md` leak section.

## Case studies (prove window)

### Avoid — 2026-08-27 FIGR → EMBJ (`avoid_J_ge0`)

Join top-8 that morning: MNDY, RELY, **FIGR**, ECO, NVDA, SKHY, HPE, CRDO.

| | FIGR (dropped) | EMBJ (refilled) |
|---|---|---|
| join rank | 3 | 9 |
| J at open | **+3.82%** (Open 40.50 vs 2026-08-25 Open 39.01) | **−2.09%** (Open 75.78 vs 08-25 Open 77.40) |
| without J | stays in the eight | stays out (rank 9) |
| with J | dropped (J≥0) | refilled (J<0) |
| H after 15 bp | **−8.59%** (raw −8.44%) | **−0.82%** (raw −0.67%) |
| I after 15 bp | **−10.02%** (raw −9.87%) | **+0.71%** (raw +0.86%) |

08-26 has join but no Finviz, so this J is vs Tuesday Open, not Wednesday. Still prior, not a close peek.

### Elevate — 2026-09-04 HRMY → AVAH (`elev_cap2_J_le-1`)

Join top-8 that morning: **HRMY**, HALO, CDNA, WAY, PLMR, ONC, KKR, NU.

| | HRMY (dropped) | AVAH (elevated) |
|---|---|---|
| join rank | 1 | 13 |
| J at open | **+3.92%** (Open 42.93 vs 2026-09-03 Open 41.31) | **−2.29%** (Open 13.22 vs 09-03 Open 13.53) |
| without J | stays #1 in the eight | stays out (rank 13) |
| with J | swapped out (J≥0, one of ≤2) | swapped in (J≤−1% from ranks 9–80) |
| H after 15 bp | **−2.64%** (raw −2.49%) | **+3.03%** (raw +3.18%) |
| I after 15 bp | **−2.48%** (raw −2.33%) | **+3.18%** (raw +3.33%) |

These two name-days illustrate the rule. They are not a new holdout. Family stays **CONDITIONAL**.

## Card 1 — avoid J≥0

| field | value |
|---|---|
| name | `research_join_avoid_J_ge0` |
| recipe | drop join top-8 buys with J≥0; refill from J<0 |
| entry | open (join rank + Excel J) |
| label | same-day H after Futubull 0.15% |
| discovery (08-14→08-25, 8 weekdays) | +0.63% / **+0.72 pp** vs same-window top-8 (n=64, ghost PASS/PASS/FAIL) |
| prove (08-26→09-07 weekdays) | +0.22% / **+0.04 pp** (n=64, ghost PASS/FAIL/FAIL) |
| pooled weekdays (16d, includes discovery) | +0.43% / +0.38 pp (n=128, ghost PASS/PASS/PASS) |
| wide prove (top-80) | +0.12 pp (n=640, ghost PASS) |
| code | `avoid_J_ge0` |
| verdict | **CONDITIONAL** (prove failed 20 bp + ghost) |
| status | research card · not live · not KEEP holds |

## Card 2 — elevate J≤−1% (cap 2)

| field | value |
|---|---|
| name | `research_join_elev_cap2_J_le-1` |
| recipe | swap ≤2 J≥0 names in top-8 for J≤−1% from ranks 9–80 |
| entry | open |
| label | same-day H after fees |
| discovery (08-14→08-25, 8 weekdays) | +0.28% / **+0.36 pp** (n=64, ghost PASS/PASS/FAIL) |
| prove (08-26→09-07 weekdays) | +0.33% / **+0.15 pp** (n=64, ghost PASS/FAIL/FAIL) |
| pooled weekdays (16d, includes discovery) | +0.30% / +0.26 pp (n=128, ghost PASS/PASS/PASS) |
| wide prove (top-80) | +0.00 pp (n=640; cap=2 is a 2.5% swap) |
| code | `elev_cap2_J_le-1` |
| verdict | **CONDITIONAL** (prove failed 20 bp + ghost) |
| status | research card · not live · not KEEP holds |

## Flatten / sleeve (KEEP recipes, not J≥+1%)

| cut | n | sleeve P&L | vs all sleeve | same-day H | vs all H |
|---|---:|---:|---:|---:|---:|
| all tickets | 30 | +2.06% | — | +1.40% | — |
| `avoid_J_ge0` (keep J<0) | 11 | +0.27% | **−1.79 pp** | +0.81% | −0.59 pp |
| elev_cap2 analogue (drop ≤2 J≥0 / day) | 23 | +1.87% | −0.19 pp | +1.59% | +0.19 pp |
| io_core `avoid_J_ge0` | 7 | +0.92% | **+2.23 pp vs io** | +0.24% | −0.07 pp |
| fair: J-avoid on io only, movers untouched | 27 | +3.01% | **+0.95 pp** | +1.50% | +0.10 pp |

Movers on 08-20/21 gapped up (J>0) and paid sleeve. Blanket J-avoid fights that thesis. H and sleeve disagree (CYPH sleeve +25%, H −3%). Flatten does not confirm a blanket KEEP.

## Explicitly not carded / caveats

| item | note |
|---|---|
| Sunday join dumps 08-30 / 09-06 | not a 1d session; held out of prove |
| 08-13 J | stale (prior Open 04-26); not used |
| flatten_robust overlay | blanket KEEP recipes hurt sleeve; io-only fair test helps |
| avoid_incomplete | fullscan-only control, not Excel |
| replace_J_lt0 | not the Manager-named pair |
| shade / M mid / H-fill | demoted / leak — out of scope |

## Source

`JOIN_POST_813.md` tip `bd1d298b` · PR #153. Gate: `OPEN_SAME_ROW_LABELS.md` / `CLOCK_MAP.md`. Research only. Live frozen.
