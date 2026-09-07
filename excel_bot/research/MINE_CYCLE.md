# Excel emulator mine — clock-aware cycle

_Generated 2026-09-07 · live `flatten_robust` is not changed._

## Protocol

document → timed `--all-cols` sample (or rebuild A–O) → mine with ship bar →
keep / iterate. No live card emission. No merge without Cyrus.

## Ship bar

PASS needs **all** of: discovery n≥300 t≥3 avg>0; holdout n≥100 t≥2 avg>0
same sign; ≥50 tickers; ≥20 entry dates; no single trade >25% of gross wins;
trimmed mean (drop best trade) >0; early and late tape same sign when both
n≥40; SPY-up and SPY-down both positive when both n≥40; hold1 must also
PASS hold2. Clock labeled on every row. Open entry only when the feature
reads no close-knowable fill.

## This cycle (timed `--all-cols` sample) — THIN

Prior agent on this branch produced a 25-ticker THIN table on disk and
never committed it. This run regenerated a timed sample on the same
pipeline (`capture_all_cols.py` + `mine_all_cols.py`).

| field | value |
|---|---|
| capture | lean A–JL (275 cols × rows 2–145), latest-anchor seed from excel-state rows |
| N | **35** tickers (**25 discovery** / 10 holdout) |
| minutes/ticker | **0.02034** (~**1.22 s/ticker**) · 35/35 ok · 140 days each |
| cost | futubull 0.15% long / 0.20% short |
| holds | sleeve-native 1 / 2 / 3 / 5 / 8 |
| clocks | deeper L/O/EL/V/AD/JA/IZ + past-O fills = **close**; yesterday L/EL + today A = **open** |
| verdicts | **PASS 0 · FAIL 0 · THIN 90** |
| why THIN | N=35 < 50-ticker ship bar. Honest ceiling this cycle, not a keep. |
| live | `flatten_robust` untouched |

Full PASS/FAIL/THIN table (n, effect, tape split): `ALL_COLS_MINE.md`
and `03_scoreboard/EXCEL_BOT_MINE.md`. Machine payload:
`all_cols_mine.json`. Timing: `all_cols_sample/_meta.json`.

Top THIN cells (still not keepers):

| def | clock | exit | n | avg net | t | tape |
|---|---|---|---:|---:|---:|---|
| `AD_ge1` | close | hold8 | 1192 | +0.93% | 4.0 | early + late same sign; spy↑/↓ both + |
| `deeper_g5` | close | hold8 | 4295 | +0.41% | 3.7 | late t=1.8; spy both + |
| `IZ_eq1` | close | hold8 | 466 | +1.27% | 3.4 | late t=0.9; spy both + |

Do **not** promote. Need ≥50 tickers before any cell can leave THIN.

## What this cycle mines

- Existing card defs, A-keyed at **open** and again at **close** (A–O miner).
- New `open_score` / `open_core` (no D,E,F,H,I,N fills).
- Color combos and majority of open-knowable fills.
- Lag combos: yesterday H/I/N + today's A (open clock) on A–O; yesterday
  L/EL + today's A on the all-cols sample.
- Formula-state gates from stored OHLCV (A–O) and from A–JL values
  (L/O/EL/V/AD/JA/IZ + past-O fill counts).
- Sleeve holds 1/2/3/5/8. Sample mine costs: `futubull`.

Research only. No merge without Cyrus. Live flatten_robust untouched.
