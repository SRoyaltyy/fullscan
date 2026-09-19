# H/I correlation mine (A–F Yahoo seed)

_Generated 2026-09-07. Research only. Live `flatten_robust` frozen. Labels are Excel H and I._

## Verdict

Not “zero correlations.” **No new number-only trade card.** The spreadsheet’s H and I are just A–F (close vs open, close vs yesterday). After feeding those columns from Yahoo for **3,531** liquid names and **1.16 million** name-days (2024-08-20 → 2026-09-04), and after throwing out same-row algebra:

1. **#144’s morning five-cell light + green O (± AH/FR)** is still the only fill-based same-day H recipe this repo has hardened. This pass did not rebuild colors.
2. **Numbers from A–F** (gap, J, lagged H/I, AH, heat, FR-like volume) do **not** produce a second leak-free gate that beats buy-everyone in **both 2025 and 2026**, both SPY tapes, after fees.
3. There **is** a small, real continuous effect: overnight gaps **fade a bit** in that day’s H (biggest down-gaps +0.14% H, biggest up-gaps −0.18% H, holdout). It is too weak / too 2025-heavy to ship as “gap ≤ −2% → buy.”
4. The visual green/red **heat** (prior five-day mean I) is not a continuation code. The hottest quintile’s next H is **worse** (−0.10%). So “the sheet looks green” in raw I-average is not the same thing as #144’s fill hysteresis.

Why #144 heard crickets besides one rule: 275 columns are a formula DAG on six OHLCV fields. Discrete eq1/green-fill mining reprints twins of H/I/gap or 2026 ghosts. That is the ceiling unless you add a second data source or the fill engine (which this pass left as #144’s keep).

## Plain English

H is the same-day open-to-close move. I is the close versus yesterday. Both are computed from columns A–F the way the spreadsheet does `(close−open)/open` and `(close−prior close)/prior close`. Features never peek at the same row's H or I. Open rules use only what you can know at 09:30 (overnight gap, yesterday's H/I, AH, prior volume, five-day heat). Close rules may use today's H/I to forecast later sessions.

Universe: **3531** names, **1158552** liquid name-days, 2024-08-20 → 2026-09-04. Everyone-else holdout same-day H -0.15% (n=472529); same-day I -0.08%.

Board: **KEEP 0 · KILL 307 · THIN 0** across 307 gate×label cells.

### What held

No new leak-free **number** gate cleared the ship bar on this tape (ticker holdout, 2025 and 2026 both ahead of buy-everyone, both SPY tapes, n + effect, no lottery / name ghost, not a half-the-book sleeve).

The real number-side finding is **continuous, not a card**: a larger overnight gap tends to fade a bit in that same day's H (biggest down-gaps: H about +0.14%; biggest up-gaps: H about −0.18% on the holdout). Yesterday’s I vs today’s H is a weak mean-reversion (ρ ≈ −0.03). None of the binary “gap ≤ −2%” recipes kept that leftover H in **2026** after fees.

### Algebra, not a forecast (same-day I)

At 09:30 you already know the overnight gap. Excel’s I is `gap + H×(1+gap)`. A name that **opens +2%** will print a green I that morning unless it crashes more than about 2% from the open. Scoring “gap up → same-day I is green” is that identity, not a prediction. Those rows are **not keeps**. The only fair same-day label at the open is **H** (the leftover move after the gap).

### Standing #144 check

This pass does **not** rebuild fill colors. The morning five-cell light + green O family stays the fill-based research keep from #144. What we tested here is whether **numbers** from A–F (gap, lagged H/I, AH, heat, FR-like volume) predict H/I without those highlights.

### Strongest holdout Spearman (continuous)

| feature | predicts | ρ holdout | ρ discovery | n | agree? |
|---|---|---:|---:|---:|---|
| `gap` | `y_h1` | -0.044 | -0.043 | 472529 | yes |
| `JB` | `y_i_stack_1w` | -0.042 | -0.047 | 463983 | yes |
| `AH` | `y_i_stack_1w` | -0.038 | -0.039 | 465780 | yes |
| `J` | `y_h1` | -0.033 | -0.033 | 472529 | yes |
| `JB` | `y_from_open_2d` | -0.031 | -0.037 | 467879 | yes |
| `gap` | `y_from_open_2d` | -0.030 | -0.027 | 471566 | yes |
| `AH` | `y_from_open_2d` | -0.029 | -0.031 | 469676 | yes |
| `I_l1` | `y_i1` | -0.028 | -0.026 | 471576 | yes |
| `AH` | `y_i_stack_1d` | -0.028 | -0.028 | 469676 | yes |
| `vol20` | `y_i_stack_1w` | -0.027 | -0.027 | 458479 | yes |
| `AH` | `y_h1` | -0.027 | -0.029 | 470639 | yes |
| `JB` | `y_i_stack_1d` | -0.027 | -0.029 | 467879 | yes |
| `AH` | `y_i1` | -0.026 | -0.029 | 470639 | yes |
| `JB` | `y_h1` | -0.026 | -0.030 | 468842 | yes |
| `I_l1` | `y_h1` | -0.025 | -0.021 | 471576 | yes |
| `JB` | `y_i1` | -0.025 | -0.030 | 468842 | yes |
| `vol20` | `y_i1` | -0.025 | -0.027 | 463338 | yes |
| `I` | `y_i_lead_1d` | -0.024 | -0.022 | 471566 | yes |

Same-row `gap` vs `y_i1` is omitted on purpose — I contains the overnight gap by algebra. Same-row H vs I is the leftover of that identity, not a forecast.

### Soft regimes (green / red heat)

Visual green/red regions are proxied by the prior five-day mean of I (knowable at the next open). We do not force a perfect coder. Quintile 0 = cold tape, quintile 4 = hot tape.

| heat quintile | same-day H | n | t |
|---:|---:|---:|---:|
| 0 | +0.00% | 93946 | 0.12 |
| 1 | -0.01% | 93946 | -1.78 |
| 2 | -0.01% | 93945 | -2.01 |
| 3 | +0.00% | 93946 | 0.06 |
| 4 | -0.10% | 93946 | -5.31 |

Overnight gap quintiles vs same-day **H** (the leftover after the gap):

| gap quintile (0=most down) | same-day H | n | t |
|---:|---:|---:|---:|
| 0 | +0.14% | 94506 | 8.74 |
| 1 | -0.01% | 94506 | -1.13 |
| 2 | -0.05% | 94505 | -5.41 |
| 3 | -0.05% | 94507 | -5.39 |
| 4 | -0.18% | 94505 | -10.49 |

### Near-misses (holdout looked good — killed on harden)

| when | predicts | holdout | why killed |
|---|---|---:|---|
| opened ≥2% down on a name that had no −3% H in the prior eight days | buy open, sell close ~2 weeks later | +0.79% (n=7538) | y2026_no_edge |
| opened ≥2% down after a −3% or worse yesterday | buy open, sell close 3 sessions later | +0.54% (n=6937) | disc_t, disc_sign, tape |
| recent volume was over ~1M and/or yesterday's relative volume ≥ 3 (Excel FR-like) | buy open, sell close ~2 weeks later | +0.51% (n=208270) | too_wide |
| the last five completed daily I prints averaged −0.8% or worse | buy open, sell close ~2 weeks later | +0.47% (n=90302) | tape, y2026, y2026_no_edge |
| AH ≥ 1 and the FR-like volume flag is on | buy open, sell close ~2 weeks later | +0.46% (n=46990) | late, y2026, y2026_no_edge |
| the last three completed sessions were all down on I | buy open, sell close ~2 weeks later | +0.45% (n=50966) | tape, y2026_no_edge |
| the stock opened at least 1% below yesterday's close | buy open, sell close ~2 weeks later | +0.43% (n=66503) | q1, y2026, y2026_no_edge, no_edge |
| the stock opened below yesterday's close | buy open, sell close ~2 weeks later | +0.42% (n=205575) | no_edge, too_wide |
| opened ≥2% down after a −3% or worse yesterday | same-day column H (open→close) | +0.40% (n=6938) | disc_t, tape, late, y2026 |
| opened ≥2% down after a −3% or worse yesterday | same-day H (buy open, sell that close) | +0.40% (n=6938) | disc_t, tape, late, y2026 |

## Method

A–F seed: `data/rows` (Yahoo / excel-state, extended to ~514 sessions). Engine check: AAPL / MSFT / BBAI H and I match the replica **139/139** days (`engine/check_af_hi.py`). Liquid filter: prior close ≥ $2 and prior dollar volume ≥ $1M. Costs: Futubull 10 bp if Finviz mcap ≥ $300M else 30 bp (20 bp if unknown). Discovery/holdout tickers from `holdout_split.json`. Year split 2025 vs 2026; Q3 cut 2026-07-01; Q1 cut 2026-04-01. Same-day I is not an open-entry label.

Research only. No cards. No live wire.
