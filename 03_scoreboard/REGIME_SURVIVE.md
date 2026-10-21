# Regime-survival mine — leak-free 09:30

_Generated 2026-09-06T07:11:52.280339-04:00 · research only · live `flatten_robust` untouched._

## Claim

No robust leak-free invariant across regimes. Closest powered lead: `blue` survives 5/8 joint weather×SPY-prior cells (1d hit 51.7% vs peer 42.8%, edge +0.19, n=5949). Joint fails: joint=mixed|spy:flat, joint=mixed|spy:up, joint=off|spy:flat.

gen_s and weather_risk are the same morning S (down==off). Survival is therefore scored on joint weather×prior-SPY cells so a tag that is only tested on up-tape is not an invariant. `hot+ab+peer` can look perfect on the cells it reaches and still be thin on risk-off. That is a clean-enough null for a live promote, with a ranked short-list for the next window.

## How validated

- Script: `python -m src.regime_survive_mine --write`
- Seed panel: `data/feature_asof/*_feature_asof.csv` (27072 name-days, 10 sessions).
- Dates: 2026-08-13, 2026-08-14, 2026-08-17, 2026-08-18, 2026-08-19, 2026-08-20, 2026-08-21, 2026-08-27, 2026-08-31, 2026-09-01.
- Morning S: `03_scoreboard/factor_mine.json` → `mornings` (same 09:30 packet flatten lookback uses).
- Weather risk: derived from that S with `00_grounding/weather_rules.json` thresholds (on ≥ 4.0, off ≤ -4.0). Dated `01_daily/weather/*.json` files are often stamped **after** 09:30 and are not used as gates.
- SPY 1d prior: prior-session Finviz `Change%` for SPY (HIT_BOARD prior actual only if that export is missing). Same-day SPY close is never an input.
- Forwards: feature_asof `ret_1d` / `ret_3d` when present; missing labels filled from later-session Finviz Prices (native 1d n=16174 → 27068; 3d n=10791 → 24354). Same-day Change% is never a selection input. Means are winsorized at ±30% so one name does not dominate; hit rate is unclipped. Excess is recomputed vs the liquid session median after the fill.
- Peer baseline: every other printed name in the **same** morning bucket (not the pooled sample).
- Fill / side cut: existing flatten_h5 vs flatten_live_h5 cash books (09:30 open, whole shares, Futubull fees).

## Leak-free contract

Selection features used: `join`, `gen`, `sector`, `ab`, `peer`, `vol`, `heat`, `cond`, `region`, `blue`, `white`, `alarm`, `fade`, `first_crack`, `steady`, `fat`, `ab_good`, `peer_good`, `vol_good`, `join_good`, `catal`, `ins_buy`, `ins_sell`, `qc_hot`, `rsi_b`, `relvol_b`, `sma20_b`, `short_b`, `gap_b`, `perf_w_b`, `earn_b`, `n_print`, `n_red`, `sector_name`.

Explicitly **not** used as inputs: `Change`, `Change%`, `Close`, `Gap`, `RelVol`, `close`, `day_change`, `gainer_change`, `outcome`, `ret_1d`, `ret_1w`, `ret_2d`, `ret_2w`, `ret_3d`, `xs_1d`, `xs_1w`, `xs_2d`, `xs_2w`, `xs_3d`.

## Macro buckets (PIT morning)

| Date | S | gen S | weather risk | SPY from | prior % | Joint | Route | Flatten? | n asof | n 1d |
|---|---|---|---|---|---|---|---|---|---|---|
| 2026-08-13 | +8.53 | up | on | 2026-08-12 | +0.26 | on|spy:up | io | no | 2747 | 2746 |
| 2026-08-14 | +5.50 | up | on | 2026-08-13 | +0.46 | on|spy:up | io | no | 2748 | 2747 |
| 2026-08-17 | +2.25 | up | mixed | 2026-08-14 | -0.18 | mixed|spy:flat | io | no | 2697 | 2696 |
| 2026-08-18 | -6.20 | down | off | 2026-08-17 | -0.19 | off|spy:flat | hold | no | 2698 | 2698 |
| 2026-08-19 | -7.20 | down | off | 2026-08-18 | -0.55 | off|spy:down | hold | no | 2702 | 2702 |
| 2026-08-20 | +1.12 | up | mixed | 2026-08-19 | +0.41 | mixed|spy:up | mover | yes | 2707 | 2707 |
| 2026-08-21 | +3.25 | up | mixed | 2026-08-20 | -0.47 | mixed|spy:down | mover | yes | 2707 | 2706 |
| 2026-08-27 | — | flat | unknown | 2026-08-25 | +0.40 | unknown|spy:up | io | no | 2698 | 2698 |
| 2026-08-31 | -5.85 | down | off | 2026-08-28 | +0.66 | off|spy:up | hold | no | 2685 | 2685 |
| 2026-09-01 | -6.30 | down | off | 2026-08-31 | -0.12 | off|spy:flat | hold | no | 2683 | 2683 |

Missing feature_asof sessions in the flatten window (still used for the flatten side cut, not the market-wide mine): 2026-08-24, 2026-08-25, 2026-08-26, 2026-08-28, 2026-09-02, 2026-09-03, 2026-09-04.

08-13 / 08-14 dumps are ~11k names; later sessions are the ~2.7k liquid universe. Ranked tables use the **liquid subset** (later sessions + overlap on the early dumps).

## Ranked survival — 1d excess vs same-bucket peers

A bucket **passes** when the factor's mean excess beats the peer mean excess **and** hit rate is not below the peer (WIN = both) with n ≥ min_n. Thin buckets are reported but do not count as pass or fail.

`gen_s` and `weather_risk` are the same morning S (`down` == `off`, `on` ⊂ `up`, blank S = `flat` + `unknown`). Headline survival uses the **joint** `weather_risk × spy_prior` cell so those cuts are not double-counted.

| Factor | n | 1d hit | hit vs peer | 1d xs | edge | joint | cut survive | joint FAIL |
|---|---|---|---|---|---|---|---|---|
| `blue` | 5949 | 51.7% | +0.089 | +0.24 | +0.19 | 5/8 | 12/18 | joint=mixed|spy:flat, joint=mixed|spy:up, joint=off|spy:flat |
| `blue+not_alarm` | 5948 | 51.7% | +0.089 | +0.24 | +0.19 | 5/8 | 12/18 | joint=mixed|spy:flat, joint=mixed|spy:up, joint=off|spy:flat |
| `region=neutral` | 10451 | 44.6% | +0.018 | +0.20 | +0.16 | 5/8 | 12/18 | joint=mixed|spy:down, joint=off|spy:down, joint=unknown|spy:up |
| `join=neutral` | 1770 | 44.1% | +0.013 | +0.22 | +0.17 | 5/8 | 11/18 | joint=off|spy:flat, joint=off|spy:up, joint=unknown|spy:up |
| `cond=good` | 12182 | 44.8% | +0.020 | +0.04 | -0.00 | 5/8 | 11/18 | joint=mixed|spy:flat, joint=mixed|spy:up, joint=on|spy:up |
| `perf_w=extended` | 2483 | 45.9% | +0.031 | +0.05 | +0.00 | 5/8 | 10/18 | joint=mixed|spy:down, joint=mixed|spy:flat, joint=off|spy:flat |
| `relvol=hot` | 876 | 48.2% | +0.054 | +0.34 | +0.29 | 4/8 | 10/18 | joint=mixed|spy:flat, joint=off|spy:down, joint=off|spy:up, joint=on|spy:up |
| `sector=bad` | 12195 | 42.0% | -0.008 | +0.14 | +0.09 | 4/8 | 9/18 | joint=mixed|spy:up, joint=off|spy:flat, joint=off|spy:up, joint=unknown|spy:up |
| `join=good` | 15007 | 43.9% | +0.011 | +0.12 | +0.08 | 4/8 | 9/18 | joint=mixed|spy:flat, joint=mixed|spy:up, joint=off|spy:flat, joint=on|spy:up |
| `join_good` | 15007 | 43.9% | +0.011 | +0.12 | +0.08 | 4/8 | 9/18 | joint=mixed|spy:flat, joint=mixed|spy:up, joint=off|spy:flat, joint=on|spy:up |
| `steady` | 8253 | 44.5% | +0.017 | +0.03 | -0.02 | 4/8 | 9/18 | joint=mixed|spy:flat, joint=mixed|spy:up, joint=off|spy:up, joint=on|spy:up |
| `sector=good` | 12204 | 42.6% | -0.002 | -0.03 | -0.07 | 4/8 | 8/18 | joint=mixed|spy:down, joint=mixed|spy:flat, joint=off|spy:down, joint=on|spy:up |
| `region=good` | 10179 | 43.4% | +0.006 | -0.07 | -0.11 | 4/7 | 9/17 | joint=mixed|spy:flat, joint=mixed|spy:up, joint=on|spy:up |
| `blue+white` | 1750 | 52.7% | +0.099 | +0.24 | +0.19 | 4/5 | 11/13 | joint=mixed|spy:flat |
| `cond=neutral` | 5647 | 43.2% | +0.004 | +0.17 | +0.13 | 3/8 | 8/18 | joint=mixed|spy:down, joint=mixed|spy:up, joint=off|spy:flat, joint=off|spy:up, joint=unknown|spy:up |
| `short=high` | 10863 | 42.9% | +0.002 | -0.01 | -0.06 | 3/8 | 7/18 | joint=mixed|spy:down, joint=mixed|spy:flat, joint=off|spy:down, joint=off|spy:up, joint=on|spy:up |
| `cond=bad` | 9239 | 39.9% | -0.029 | -0.03 | -0.07 | 3/7 | 6/16 | joint=mixed|spy:down, joint=mixed|spy:flat, joint=off|spy:down, joint=off|spy:up |
| `alarm` | 7249 | 43.9% | +0.011 | +0.03 | -0.02 | 3/7 | 4/17 | joint=off|spy:flat, joint=off|spy:up, joint=on|spy:up, joint=unknown|spy:up |

### 3d survival (same factors, liquid subset)

| Factor | n | 3d hit | 3d xs | edge | survive | FAIL buckets |
|---|---|---|---|---|---|---|
| `blue` | 4741 | 51.4% | +0.36 | +0.27 | 14/17 | gen_s=down, weather_risk=off, joint=off|spy:flat |
| `blue+not_alarm` | 4740 | 51.3% | +0.36 | +0.27 | 14/17 | gen_s=down, weather_risk=off, joint=off|spy:flat |
| `join=neutral` | 1701 | 47.6% | +0.46 | +0.36 | 12/17 | gen_s=down, weather_risk=mixed, weather_risk=off, spy_prior=flat, joint=off|spy:flat |
| `sector=bad` | 11209 | 48.1% | +0.55 | +0.45 | 12/17 | gen_s=flat, weather_risk=unknown, joint=mixed|spy:up, joint=off|spy:up, joint=unknown|spy:up |
| `region=neutral` | 9356 | 48.0% | +0.59 | +0.49 | 11/17 | gen_s=flat, weather_risk=unknown, spy_prior=down, joint=mixed|spy:up, joint=off|spy:down, joint=unknown|spy:up |
| `join=good` | 13645 | 43.2% | +0.11 | +0.01 | 10/17 | gen_s=up, weather_risk=mixed, weather_risk=on, spy_prior=flat, joint=mixed|spy:flat, joint=mixed|spy:up, joint=on|spy:up |
| `join_good` | 13645 | 43.2% | +0.11 | +0.01 | 10/17 | gen_s=up, weather_risk=mixed, weather_risk=on, spy_prior=flat, joint=mixed|spy:flat, joint=mixed|spy:up, joint=on|spy:up |
| `blue+white` | 1261 | 54.3% | +0.78 | +0.69 | 11/11 | — |
| `cond=neutral` | 5422 | 40.6% | +0.15 | +0.06 | 8/17 | gen_s=down, gen_s=flat, weather_risk=off, weather_risk=unknown, spy_prior=up, joint=mixed|spy:up, joint=off|spy:flat, joint=off|spy:up, joint=unknown|spy:up |
| `cond=good` | 10323 | 42.6% | -0.06 | -0.16 | 7/17 | gen_s=down, gen_s=up, weather_risk=mixed, weather_risk=off, weather_risk=on, spy_prior=flat, joint=mixed|spy:flat, joint=mixed|spy:up, joint=off|spy:flat, joint=on|spy:up |
| `steady` | 7398 | 38.7% | -0.19 | -0.28 | 6/17 | gen_s=down, gen_s=up, weather_risk=mixed, weather_risk=off, weather_risk=on, spy_prior=flat, spy_prior=up, joint=mixed|spy:flat, joint=mixed|spy:up, joint=off|spy:flat, joint=on|spy:up |
| `perf_w=extended` | 2273 | 42.4% | -0.46 | -0.55 | 4/17 | gen_s=down, gen_s=flat, gen_s=up, weather_risk=mixed, weather_risk=off, weather_risk=on, weather_risk=unknown, spy_prior=flat, spy_prior=up, joint=mixed|spy:flat, joint=off|spy:flat, joint=on|spy:up, joint=unknown|spy:up |

## Top claims — failing buckets spelled out

### `blue`

n=5949 · 1d hit 51.7% vs peer 42.8% · edge +0.19 · joint 5/8 · cuts 12/18.

Joint fails: `joint=mixed|spy:flat`, `joint=mixed|spy:up`, `joint=off|spy:flat`.

Cut fails: `gen_s=down`, `weather_risk=off`, `spy_prior=flat`, `joint=mixed|spy:flat`, `joint=mixed|spy:up`, `joint=off|spy:flat`.

- `gen_s=down` · n=916 · hit 36.9% vs 38.3% · edge +0.31 · fail
- `gen_s=flat` · n=790 · hit 48.9% vs 45.9% · edge +0.30 · pass
- `gen_s=up` · n=4243 · hit 55.4% vs 45.7% · edge +0.14 · pass
- `weather_risk=mixed` · n=3956 · hit 56.2% vs 47.4% · edge +0.23 · pass
- `weather_risk=off` · n=916 · hit 36.9% vs 38.3% · edge +0.31 · fail
- `weather_risk=on` · n=287 · hit 43.9% vs 43.2% · edge +0.19 · pass
- `weather_risk=unknown` · n=790 · hit 48.9% vs 45.9% · edge +0.30 · pass
- `spy_prior=down` · n=1363 · hit 43.9% vs 35.5% · edge +0.15 · pass
- `spy_prior=flat` · n=891 · hit 31.2% vs 42.4% · edge -0.09 · fail
- `spy_prior=up` · n=3695 · hit 59.5% vs 45.9% · edge +0.23 · pass
- `joint=mixed|spy:down` · n=1207 · hit 44.7% vs 41.4% · edge +0.18 · pass
- `joint=mixed|spy:flat` · n=175 · hit 10.3% vs 35.7% · edge -1.79 · fail
- `joint=mixed|spy:up` · n=2574 · hit 64.8% vs 65.0% · edge -0.02 · fail
- `joint=off|spy:down` · n=156 · hit 37.8% vs 29.5% · edge +0.45 · pass
- `joint=off|spy:flat` · n=716 · hit 36.3% vs 45.8% · edge +0.23 · fail
- `joint=off|spy:up` · n=44 · hit 43.2% vs 31.9% · edge +0.14 · pass
- `joint=on|spy:up` · n=287 · hit 43.9% vs 43.2% · edge +0.19 · pass
- `joint=unknown|spy:up` · n=790 · hit 48.9% vs 45.9% · edge +0.30 · pass

### `blue+not_alarm`

n=5948 · 1d hit 51.7% vs peer 42.8% · edge +0.19 · joint 5/8 · cuts 12/18.

Joint fails: `joint=mixed|spy:flat`, `joint=mixed|spy:up`, `joint=off|spy:flat`.

Cut fails: `gen_s=down`, `weather_risk=off`, `spy_prior=flat`, `joint=mixed|spy:flat`, `joint=mixed|spy:up`, `joint=off|spy:flat`.

- `gen_s=down` · n=915 · hit 36.9% vs 38.3% · edge +0.31 · fail
- `gen_s=flat` · n=790 · hit 48.9% vs 45.9% · edge +0.30 · pass
- `gen_s=up` · n=4243 · hit 55.4% vs 45.7% · edge +0.14 · pass
- `weather_risk=mixed` · n=3956 · hit 56.2% vs 47.4% · edge +0.23 · pass
- `weather_risk=off` · n=915 · hit 36.9% vs 38.3% · edge +0.31 · fail
- `weather_risk=on` · n=287 · hit 43.9% vs 43.2% · edge +0.19 · pass
- `weather_risk=unknown` · n=790 · hit 48.9% vs 45.9% · edge +0.30 · pass
- `spy_prior=down` · n=1363 · hit 43.9% vs 35.5% · edge +0.15 · pass
- `spy_prior=flat` · n=891 · hit 31.2% vs 42.4% · edge -0.09 · fail
- `spy_prior=up` · n=3694 · hit 59.5% vs 45.9% · edge +0.23 · pass
- `joint=mixed|spy:down` · n=1207 · hit 44.7% vs 41.4% · edge +0.18 · pass
- `joint=mixed|spy:flat` · n=175 · hit 10.3% vs 35.7% · edge -1.79 · fail
- `joint=mixed|spy:up` · n=2574 · hit 64.8% vs 65.0% · edge -0.02 · fail
- `joint=off|spy:down` · n=156 · hit 37.8% vs 29.5% · edge +0.45 · pass
- `joint=off|spy:flat` · n=716 · hit 36.3% vs 45.8% · edge +0.23 · fail
- `joint=off|spy:up` · n=43 · hit 44.2% vs 31.9% · edge +0.15 · pass
- `joint=on|spy:up` · n=287 · hit 43.9% vs 43.2% · edge +0.19 · pass
- `joint=unknown|spy:up` · n=790 · hit 48.9% vs 45.9% · edge +0.30 · pass

### `region=neutral`

n=10451 · 1d hit 44.6% vs peer 42.8% · edge +0.16 · joint 5/8 · cuts 12/18.

Joint fails: `joint=mixed|spy:down`, `joint=off|spy:down`, `joint=unknown|spy:up`.

Cut fails: `gen_s=flat`, `weather_risk=unknown`, `spy_prior=down`, `joint=mixed|spy:down`, `joint=off|spy:down`, `joint=unknown|spy:up`.

- `gen_s=down` · n=4830 · hit 40.1% vs 38.3% · edge +0.07 · pass
- `gen_s=flat` · n=1239 · hit 43.1% vs 45.9% · edge -0.21 · fail
- `gen_s=up` · n=4382 · hit 49.8% vs 45.7% · edge +0.32 · pass
- `weather_risk=mixed` · n=2966 · hit 49.2% vs 47.4% · edge +0.25 · pass
- `weather_risk=off` · n=4830 · hit 40.1% vs 38.3% · edge +0.07 · pass
- `weather_risk=on` · n=1416 · hit 51.2% vs 43.2% · edge +0.53 · pass
- `weather_risk=unknown` · n=1239 · hit 43.1% vs 45.9% · edge -0.21 · fail
- `spy_prior=down` · n=2524 · hit 32.1% vs 35.5% · edge -0.24 · fail
- `spy_prior=flat` · n=3195 · hit 48.6% vs 42.4% · edge +0.55 · pass
- `spy_prior=up` · n=4732 · hit 48.5% vs 45.9% · edge +0.14 · pass
- `joint=mixed|spy:down` · n=1093 · hit 36.0% vs 41.4% · edge -0.23 · fail
- `joint=mixed|spy:flat` · n=786 · hit 45.6% vs 35.7% · edge +1.10 · pass
- `joint=mixed|spy:up` · n=1087 · hit 65.1% vs 65.0% · edge +0.03 · pass
- `joint=off|spy:down` · n=1431 · hit 29.2% vs 29.5% · edge -0.25 · fail
- `joint=off|spy:flat` · n=2409 · hit 49.6% vs 45.8% · edge +0.30 · pass
- `joint=off|spy:up` · n=990 · hit 33.0% vs 31.9% · edge +0.02 · pass
- `joint=on|spy:up` · n=1416 · hit 51.2% vs 43.2% · edge +0.53 · pass
- `joint=unknown|spy:up` · n=1239 · hit 43.1% vs 45.9% · edge -0.21 · fail

### `join=neutral`

n=1770 · 1d hit 44.1% vs peer 42.8% · edge +0.17 · joint 5/8 · cuts 11/18.

Joint fails: `joint=off|spy:flat`, `joint=off|spy:up`, `joint=unknown|spy:up`.

Cut fails: `gen_s=flat`, `gen_s=up`, `weather_risk=mixed`, `weather_risk=unknown`, `joint=off|spy:flat`, `joint=off|spy:up`, `joint=unknown|spy:up`.

- `gen_s=down` · n=584 · hit 43.0% vs 38.3% · edge +0.18 · pass
- `gen_s=flat` · n=89 · hit 44.9% vs 45.9% · edge -0.11 · fail
- `gen_s=up` · n=1097 · hit 44.6% vs 45.7% · edge +0.23 · fail
- `weather_risk=mixed` · n=1056 · hit 44.3% vs 47.4% · edge +0.33 · fail
- `weather_risk=off` · n=584 · hit 43.0% vs 38.3% · edge +0.18 · pass
- `weather_risk=on` · n=41 · hit 51.2% vs 43.2% · edge +0.06 · pass
- `weather_risk=unknown` · n=89 · hit 44.9% vs 45.9% · edge -0.11 · fail
- `spy_prior=down` · n=281 · hit 41.6% vs 35.5% · edge +0.80 · pass
- `spy_prior=flat` · n=1154 · hit 42.8% vs 42.4% · edge +0.17 · pass
- `spy_prior=up` · n=335 · hit 50.4% vs 45.9% · edge +0.10 · pass
- `joint=mixed|spy:down` · n=68 · hit 52.9% vs 41.4% · edge +0.90 · pass
- `joint=mixed|spy:flat` · n=874 · hit 40.6% vs 35.7% · edge +0.58 · pass
- `joint=mixed|spy:up` · n=114 · hit 67.5% vs 65.0% · edge +0.39 · pass
- `joint=off|spy:down` · n=213 · hit 38.0% vs 29.5% · edge +0.71 · pass
- `joint=off|spy:flat` · n=280 · hit 49.6% vs 45.8% · edge -0.07 · fail
- `joint=off|spy:up` · n=91 · hit 34.1% vs 31.9% · edge -0.20 · fail
- `joint=on|spy:up` · n=41 · hit 51.2% vs 43.2% · edge +0.06 · pass
- `joint=unknown|spy:up` · n=89 · hit 44.9% vs 45.9% · edge -0.11 · fail

### `cond=good`

n=12182 · 1d hit 44.8% vs peer 42.8% · edge -0.00 · joint 5/8 · cuts 11/18.

Joint fails: `joint=mixed|spy:flat`, `joint=mixed|spy:up`, `joint=on|spy:up`.

Cut fails: `gen_s=up`, `weather_risk=mixed`, `weather_risk=on`, `spy_prior=flat`, `joint=mixed|spy:flat`, `joint=mixed|spy:up`, `joint=on|spy:up`.

- `gen_s=down` · n=1545 · hit 43.2% vs 38.3% · edge +0.38 · pass
- `gen_s=flat` · n=269 · hit 57.2% vs 45.9% · edge +0.85 · pass
- `gen_s=up` · n=10368 · hit 44.7% vs 45.7% · edge -0.02 · fail
- `weather_risk=mixed` · n=4917 · hit 46.4% vs 47.4% · edge -0.12 · fail
- `weather_risk=off` · n=1545 · hit 43.2% vs 38.3% · edge +0.38 · pass
- `weather_risk=on` · n=5451 · hit 43.1% vs 43.2% · edge -0.00 · fail
- `weather_risk=unknown` · n=269 · hit 57.2% vs 45.9% · edge +0.85 · pass
- `spy_prior=down` · n=2059 · hit 43.3% vs 35.5% · edge +0.15 · pass
- `spy_prior=flat` · n=2338 · hit 38.9% vs 42.4% · edge -0.37 · fail
- `spy_prior=up` · n=7785 · hit 46.9% vs 45.9% · edge +0.00 · pass
- `joint=mixed|spy:down` · n=1855 · hit 44.5% vs 41.4% · edge +0.19 · pass
- `joint=mixed|spy:flat` · n=1689 · hit 33.3% vs 35.7% · edge -0.34 · fail
- `joint=mixed|spy:up` · n=1373 · hit 65.0% vs 65.0% · edge -0.11 · fail
- `joint=off|spy:down` · n=204 · hit 31.9% vs 29.5% · edge +0.46 · pass
- `joint=off|spy:flat` · n=649 · hit 53.3% vs 45.8% · edge +0.39 · pass
- `joint=off|spy:up` · n=692 · hit 37.1% vs 31.9% · edge +0.35 · pass
- `joint=on|spy:up` · n=5451 · hit 43.1% vs 43.2% · edge -0.00 · fail
- `joint=unknown|spy:up` · n=269 · hit 57.2% vs 45.9% · edge +0.85 · pass

### `perf_w=extended`

n=2483 · 1d hit 45.9% vs peer 42.8% · edge +0.00 · joint 5/8 · cuts 10/18.

Joint fails: `joint=mixed|spy:down`, `joint=mixed|spy:flat`, `joint=off|spy:flat`.

Cut fails: `gen_s=down`, `weather_risk=mixed`, `weather_risk=off`, `spy_prior=down`, `spy_prior=flat`, `joint=mixed|spy:down`, `joint=mixed|spy:flat`, `joint=off|spy:flat`.

- `gen_s=down` · n=662 · hit 39.0% vs 38.3% · edge -0.60 · fail
- `gen_s=flat` · n=244 · hit 65.2% vs 45.9% · edge +1.10 · pass
- `gen_s=up` · n=1577 · hit 45.8% vs 45.7% · edge +0.10 · pass
- `weather_risk=mixed` · n=733 · hit 43.4% vs 47.4% · edge -0.17 · fail
- `weather_risk=off` · n=662 · hit 39.0% vs 38.3% · edge -0.60 · fail
- `weather_risk=on` · n=844 · hit 47.9% vs 43.2% · edge +0.28 · pass
- `weather_risk=unknown` · n=244 · hit 65.2% vs 45.9% · edge +1.10 · pass
- `spy_prior=down` · n=392 · hit 34.2% vs 35.5% · edge -0.24 · fail
- `spy_prior=flat` · n=661 · hit 34.6% vs 42.4% · edge -1.16 · fail
- `spy_prior=up` · n=1430 · hit 54.3% vs 45.9% · edge +0.56 · pass
- `joint=mixed|spy:down` · n=210 · hit 33.3% vs 41.4% · edge -0.51 · fail
- `joint=mixed|spy:flat` · n=277 · hit 29.2% vs 35.7% · edge -0.99 · fail
- `joint=mixed|spy:up` · n=246 · hit 67.9% vs 65.0% · edge +1.07 · pass
- `joint=off|spy:down` · n=182 · hit 35.2% vs 29.5% · edge +0.09 · pass
- `joint=off|spy:flat` · n=384 · hit 38.5% vs 45.8% · edge -1.19 · fail
- `joint=off|spy:up` · n=96 · hit 47.9% vs 31.9% · edge +0.35 · pass
- `joint=on|spy:up` · n=844 · hit 47.9% vs 43.2% · edge +0.28 · pass
- `joint=unknown|spy:up` · n=244 · hit 65.2% vs 45.9% · edge +1.10 · pass

### `relvol=hot`

n=876 · 1d hit 48.2% vs peer 42.8% · edge +0.29 · joint 4/8 · cuts 10/18.

Joint fails: `joint=mixed|spy:flat`, `joint=off|spy:down`, `joint=off|spy:up`, `joint=on|spy:up`.

Cut fails: `gen_s=down`, `weather_risk=off`, `weather_risk=on`, `spy_prior=flat`, `joint=mixed|spy:flat`, `joint=off|spy:down`, `joint=off|spy:up`, `joint=on|spy:up`.

- `gen_s=down` · n=228 · hit 40.4% vs 38.3% · edge -0.08 · fail
- `gen_s=flat` · n=102 · hit 64.7% vs 45.9% · edge +1.37 · pass
- `gen_s=up` · n=546 · hit 48.4% vs 45.7% · edge +0.24 · pass
- `weather_risk=mixed` · n=330 · hit 53.0% vs 47.4% · edge +0.49 · pass
- `weather_risk=off` · n=228 · hit 40.4% vs 38.3% · edge -0.08 · fail
- `weather_risk=on` · n=216 · hit 41.2% vs 43.2% · edge -0.13 · fail
- `weather_risk=unknown` · n=102 · hit 64.7% vs 45.9% · edge +1.37 · pass
- `spy_prior=down` · n=188 · hit 38.3% vs 35.5% · edge +0.11 · pass
- `spy_prior=flat` · n=164 · hit 45.1% vs 42.4% · edge -0.08 · fail
- `spy_prior=up` · n=524 · hit 52.7% vs 45.9% · edge +0.43 · pass
- `joint=mixed|spy:down` · n=101 · hit 48.5% vs 41.4% · edge +0.54 · pass
- `joint=mixed|spy:flat` · n=84 · hit 29.8% vs 35.7% · edge -0.71 · fail
- `joint=mixed|spy:up` · n=145 · hit 69.7% vs 65.0% · edge +0.96 · pass
- `joint=off|spy:down` · n=87 · hit 26.4% vs 29.5% · edge -0.37 · fail
- `joint=off|spy:flat` · n=80 · hit 61.3% vs 45.8% · edge +0.81 · pass
- `joint=off|spy:up` · n=61 · hit 32.8% vs 31.9% · edge -0.70 · fail
- `joint=on|spy:up` · n=216 · hit 41.2% vs 43.2% · edge -0.13 · fail
- `joint=unknown|spy:up` · n=102 · hit 64.7% vs 45.9% · edge +1.37 · pass

### `sector=bad`

n=12195 · 1d hit 42.0% vs peer 42.8% · edge +0.09 · joint 4/8 · cuts 9/18.

Joint fails: `joint=mixed|spy:up`, `joint=off|spy:flat`, `joint=off|spy:up`, `joint=unknown|spy:up`.

Cut fails: `gen_s=down`, `gen_s=flat`, `weather_risk=off`, `weather_risk=unknown`, `spy_prior=up`, `joint=mixed|spy:up`, `joint=off|spy:flat`, `joint=off|spy:up`, `joint=unknown|spy:up`.

- `gen_s=down` · n=7081 · hit 36.9% vs 38.3% · edge -0.02 · fail
- `gen_s=flat` · n=1024 · hit 43.6% vs 45.9% · edge -0.38 · fail
- `gen_s=up` · n=4090 · hit 50.2% vs 45.7% · edge +0.40 · pass
- `weather_risk=mixed` · n=2666 · hit 49.5% vs 47.4% · edge +0.35 · pass
- `weather_risk=off` · n=7081 · hit 36.9% vs 38.3% · edge -0.02 · fail
- `weather_risk=on` · n=1424 · hit 51.5% vs 43.2% · edge +0.54 · pass
- `weather_risk=unknown` · n=1024 · hit 43.6% vs 45.9% · edge -0.38 · fail
- `spy_prior=down` · n=2516 · hit 36.2% vs 35.5% · edge +0.33 · pass
- `spy_prior=flat` · n=4392 · hit 43.2% vs 42.4% · edge +0.19 · pass
- `spy_prior=up` · n=5287 · hit 43.7% vs 45.9% · edge -0.05 · fail
- `joint=mixed|spy:down` · n=986 · hit 42.2% vs 41.4% · edge +0.19 · pass
- `joint=mixed|spy:flat` · n=786 · hit 45.6% vs 35.7% · edge +1.10 · pass
- `joint=mixed|spy:up` · n=894 · hit 61.2% vs 65.0% · edge -0.16 · fail
- `joint=off|spy:down` · n=1530 · hit 32.3% vs 29.5% · edge +0.39 · pass
- `joint=off|spy:flat` · n=3606 · hit 42.7% vs 45.8% · edge -0.12 · fail
- `joint=off|spy:up` · n=1945 · hit 30.0% vs 31.9% · edge -0.17 · fail
- `joint=on|spy:up` · n=1424 · hit 51.5% vs 43.2% · edge +0.54 · pass
- `joint=unknown|spy:up` · n=1024 · hit 43.6% vs 45.9% · edge -0.38 · fail

## Flatten side cut — extra lots vs cash

Live `flatten_robust` sits on io/HOLD. `flatten_h5` still buys the wish-list (except hard-red S ≤ −3). This cut does **not** recommend turning the sit rule off.

Ungated io mornings where flatten_h5 bought and flatten_live sat: **7** (2026-08-13, 2026-08-14, 2026-08-17, 2026-08-25, 2026-08-27, 2026-09-03, 2026-09-04). Extra lots **28**.

- Equal-weight 1d of those extra names: n=23 · hit 47.8% · mean +1.04%.
- Equal-weight 3d: n=21 · hit 52.4% · mean +1.45%.
- Same-day session $ (blotter, after fees on the book): sum +895.64 · mean +31.99.
- End-of-window book gap flatten_h5 − flatten_live_h5: +1699.30 ($12284.34 vs $10585.04). That gap also includes later mark-to-market of lots opened on those io mornings — not a same-day cash ticket.

### Per morning

| Date | S | gen | risk | SPY prior | Extra names | book gap $ | EW 1d |
|---|---|---|---|---|---|---|---|
| 2026-08-13 | +8.53 | up | on | up | `BTSG`, `IREN`, `TPG`, `TGTX`, `SLS`, `HIMS`, `INO`, `TNDM` | +153.12 | +2.53 |
| 2026-08-14 | +5.50 | up | on | up | `MARA`, `LDI`, `BTBT` | +435.42 | +1.70 |
| 2026-08-17 | +2.25 | up | mixed | flat | `TMC`, `DNN`, `HNST` | +525.15 | +1.26 |
| 2026-08-25 | +1.80 | up | mixed | flat | `OCUL`, `CRMD`, `RZLT` | +977.96 | — |
| 2026-08-27 | — | flat | unknown | up | `RRC`, `CRK`, `MOS`, `SLI` | +1053.04 | +0.13 |
| 2026-09-03 | -0.90 | flat | mixed | down | `ATRC`, `HRMY`, `CABA`, `VSTM`, `RVTY` | +1846.27 | -1.17 |
| 2026-09-04 | — | flat | unknown | up | `NVAX`, `BVS` | +1699.30 | — |

### Fingerprint of extra lots

n=28 · join🟢 96.4% · vol🟢 10.7% · vol missing 28.6% · AB🟢 50.0% · AB missing 50.0% · 🔵 67.9% · 🚨 3.6% · E-react 3.6% · named setup 25.0%.

Extra io lots are mostly flatten wish-list / join🟢 names, not the market-wide `vol=good|ab=good` tag. No stable camera fingerprint that would justify ungating sit.

## Honest limits

- Window is ~10 liquid sessions on the asof panel (Aug 17 → Sep 1 plus overlap). That is too short to call a structural invariant.
- 08-13/14 asof files are a wider dump; the liquid subset is the fair comparison.
- Several flatten dates have no feature_asof file, so market-wide survival cannot see 08-24 / 08-25 / 08-26 / 08-28 / 09-02 / 09-03 / 09-04. 08-21 / 08-27 / 08-31 / 09-01 asof rows had empty `ret_*`; those forwards are filled from later weekday Finviz Prices (outcomes only).
- Weather.json `generated_at` is often 11:40–22:00 ET. Using those files as 09:30 risk would leak. We do not.
- No recipe was changed. No live promote.

