# Hard-red strategy mine

Research only. Same leak-free 09:30 catalog that produced the factor-mine combo sleeves. Live `flatten_robust` and Webull `combo_sh_macd_5050_shared` stay on full hard-red sit.

Window `2026-08-13 → 2026-09-15` (23 sessions). Hard-red mornings (S≤-3): **11** — `2026-08-18` S=-6.2, `2026-08-19` S=-7.2, `2026-08-24` S=-5.175, `2026-08-31` S=-5.85, `2026-09-01` S=-6.3, `2026-09-02` S=-3.825, `2026-09-08` S=-11.475, `2026-09-09` S=-13.95, `2026-09-10` S=-13.275, `2026-09-14` S=-11.002, `2026-09-15` S=-3.836.

Calendar time-split: cutoff `2026-09-04` (hold_frac=0.3). Discovery fires need the entry **and** the horizon close strictly before cutoff. Holdout = entry on/after cutoff. Missing open/exit fail closed. Yahoo regular-session overlay filled `1208` holes on ['2026-09-14', '2026-09-15'] (research only — parquet was not written).

## What this is grading

Each recipe's 09:30 list on a hard-red morning, filled at the official open, exited at the hold-th session close (1 / 2 / 3 / 5). 1 share, Futubull round-trip. Close grades — it does not trigger. A fat all-day Book% that sits every red morning is **not** a hard-red edge.

**Published KEEP** (same as HARD_RED_SIT): ≥30 fires and >55% after fees. Thin n is KILL. This board does not wire a live policy change.

**Research survivor** (reporting only): discovery and holdout each ≥8 graded fires and >55% after fees.

Recipes scored: **233**. Combo specs: **78**. Name-day rows: **1322**.

## Holdout survivors

None. No recipe or combo cleared the research survivor bar on the hidden hard-red window. Live sit stays.

## Holdout leaders (hidden window, not a KEEP)

Best after-fee win% on hard-red entries on/after cutoff. A holdout tease that missed discovery is still KILL — we would not have picked it with the earlier tape only.

| Sleeve | Side | Hold | Disc n/win | Holdout n/win | All-red n/win | All-red $ | Research | Live KEEP |
|---|---|---:|---:|---:|---:|---:|---|---|
| `short_alarm_h1` | short | 5 | 24/50.0% | 32/65.6% | 76/48.7% | +415.74 | no | KILL |
| `short_alarm_h3` | short | 5 | 24/50.0% | 32/65.6% | 76/48.7% | +415.74 | no | KILL |
| `combo_sh_macd_5050_shared` | mix | baked | 33/27.3% | 25/56.0% | 59/39.0% | -11.52 | no | KILL |
| `short_extended_h1` | short | 5 | 24/37.5% | 37/51.3% | 78/44.9% | +237.66 | no | KILL |
| `short_extended_h3` | short | 5 | 24/37.5% | 37/51.3% | 78/44.9% | +237.66 | no | KILL |
| `combo_sh_5050_shared` | mix | baked | 36/25.0% | 28/50.0% | 66/34.8% | -28.37 | no | KILL |
| `combo_sh_7030_shared` | mix | baked | 36/25.0% | 28/50.0% | 66/34.8% | -28.37 | no | KILL |
| `combo_sh_3070_shared` | mix | baked | 36/25.0% | 28/50.0% | 66/34.8% | -28.37 | no | KILL |
| `union_hot_n4_h1` | long | 1 | 24/29.2% | 20/50.0% | 44/38.6% | -13.22 | no | KILL |
| `short_news_or_h3` | short | 5 | 12/16.7% | 8/50.0% | 22/27.3% | -13.44 | no | KILL |
| `short_news_r_h1` | short | 5 | 12/16.7% | 8/50.0% | 22/27.3% | -13.44 | no | KILL |
| `short_news_r_h3` | short | 5 | 12/16.7% | 8/50.0% | 22/27.3% | -13.44 | no | KILL |

## Discovery leaders (not confirmed)

Best discovery win% at each recipe's best hold. Holdout is shown so a full-sample tease that dies hidden is visible.

| Sleeve | Side | Hold | Disc n/win | Holdout n/win | All-red n/win | All-red $ | Research | Live KEEP |
|---|---|---:|---:|---:|---:|---:|---|---|
| `union_news_head_h1` | long | 5 | 15/60.0% | 18/44.4% | 52/42.3% | -1.02 | no | KILL |
| `union_news_head_h3` | long | 5 | 15/60.0% | 18/44.4% | 52/42.3% | -1.02 | no | KILL |
| `union_news_or_h1` | long | 5 | 19/57.9% | 22/36.4% | 65/38.5% | -253.05 | no | KILL |
| `union_news_g_cond_h1` | long | 5 | 19/57.9% | 22/36.4% | 65/38.5% | -253.05 | no | KILL |
| `union_news_or_h3` | long | 5 | 19/57.9% | 22/36.4% | 65/38.5% | -253.05 | no | KILL |
| `union_news_g_cond_h3` | long | 5 | 19/57.9% | 22/36.4% | 65/38.5% | -253.05 | no | KILL |
| `union_hot_score_h1` | long | 5 | 24/54.2% | 40/32.5% | 88/42.0% | -136.58 | no | KILL |
| `union_hot_score_h3` | long | 5 | 24/54.2% | 40/32.5% | 88/42.0% | -136.58 | no | KILL |
| `probable_probable_ok_h1` | long | 5 | 15/53.3% | 16/43.8% | 37/48.6% | +21.12 | no | KILL |
| `probable_probable_ok_h3` | long | 5 | 15/53.3% | 16/43.8% | 37/48.6% | +21.12 | no | KILL |
| `union_news_g_h1` | long | 5 | 19/52.6% | 22/31.8% | 65/36.9% | -202.79 | no | KILL |
| `union_news_g_h3` | long | 5 | 19/52.6% | 22/31.8% | 65/36.9% | -202.79 | no | KILL |

## Live combo vs all-day sit book

`combo_sh_macd_5050_shared` is the Webull paper sleeve. Sit is the published all-day cash book (red mornings take no new lots). Allow is the research counterfactual: same leftover book, 09:30 fills on hard-red too.

| Book | Book% | Book win | Hard-red fires | Hard-red win | Hard-red $ | Audit |
|---|---:|---:|---:|---:|---:|---|
| sit (live / all-day) | +42.16 | 61.4% | 0 | — | +0.00 | PASS |
| allow (research) | +126.30 | 60.0% | 43 | 48.8% | +5776.88 | PASS |

## Cash books for research survivors / leaders

Sit = published all-day path. Allow = trade through red at the 09:30 open. Hard-red $ is only the red-morning fills.

| Sleeve | Sit Book% | Sit hard-red n/win | Allow Book% | Allow hard-red n/win | Allow hard-red $ |
|---|---:|---:|---:|---:|---:|
| `union_news_head_h1` | -4.83 | 0/— | +198.41 | 49/63.3% | +20474.89 |
| `union_news_head_h3` | -14.99 | 0/— | +122.92 | 32/46.9% | +15025.54 |
| `union_news_or_h1` | +3.70 | 0/— | +212.80 | 62/59.7% | +20538.30 |
| `union_news_g_cond_h1` | +2.84 | 0/— | +209.69 | 62/59.7% | +20540.74 |
| `union_news_or_h3` | -7.72 | 0/— | +168.52 | 50/52.0% | +18634.51 |
| `union_news_g_cond_h3` | -8.56 | 0/— | +168.24 | 50/52.0% | +18629.89 |
| `short_alarm_h1` | +1.71 | 0/— | -0.76 | 73/42.5% | -264.64 |
| `short_alarm_h3` | +2.59 | 0/— | -3.43 | 73/49.3% | -630.61 |
| `combo_sh_macd_5050_shared` | +42.16 | 0/— | +126.30 | 43/48.8% | +5776.88 |
| `short_extended_h1` | -11.16 | 0/— | -11.40 | 69/55.1% | -103.20 |

## Gate (do not change live)

Hard-red S≤−3 still blocks new lots in `combo_broker.size_combo_tickets`, `simulate_shared` (default sit), and `simulate_book` / `flatten_robust`. This mine adds opt-in `hard_red_mode=allow` for research books only.

**Survivors: 0.** No holdout survivor. Discovery teases that miss the hidden red window stay KILL. Live sit stands.
