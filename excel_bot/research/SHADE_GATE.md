# Shade clock-gate pairs — M fill × open 44 / lags

_Generated 2026-09-08 · live `flatten_robust` frozen · research only · no live push._

## Plain English

Excel clock gate is the source of truth (`OPEN_SAME_ROW_LABELS.md` + `CLOCK_MAP.md`). Mid-M `#95CA82` stays **open-fill only**. This beat pairs that fill with the locked 44 `value_mine_open` cols we can compute on the expand panel (J, AH, ER, FQ, JB, JC) and with **lags** of any letter (H/I/M/K/G/J from rows above). Same-row M/B/G/K/O numbers, D/E/F/H/I, and `core_score` are OUT.

**Family verdict: null** — KEEP 0 · KILL 24 · THIN 0.

M mid `#95CA82` fill × locked open 44 / lags does not clear the ship bar (KEEP 0 · KILL 24 · THIN 0). Parent fill stays DEMOTE. Live frozen.

### Excel clock gate (enforced)

- Shades/fills at open: **A B C G J K L M O IR IS IT**
- Numbers/text at open: the **44 `value_mine_open` cols** (never same-row H/I)
- Lags: any letter from rows above is fair
- OUT: M’s number, B/G/K/M/O numbers, D/E/F/H/I same-row, `core_score`
- Parent feature is M **fill** `#95CA82`. CF `IZ=1` is a lag. M’s number is never a feature.

Panel **5223** tickers · 2018-09-10 → 2026-09-04 · 8537720 name-days. Calendar days **2008**. Futubull 0.15% long. Beat book and parent by ≥20 bp.

**Ghost / name check: GHOST FAIL**

Clock-gate pair ghost: parent `M_hex_95CA82` GHOST FAIL holdout -0.19%; pair PASS 0 / 24. Live frozen.

### Same-day H (open entry)

| meaning | holdout | vs book | vs parent | Q1 | SPY↑ | SPY↓ | verdict | why | code |
|---|---|---|---|---|---|---|---|---|---|
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and today's open-to-open J ≤ −1% (44, same-row open) | +0.71% (n=14758) | +0.81 pp | +0.90 pp | +0.66% (n=34265) | +1.73% (n=10048) | +0.09% (n=21670) | **KILL** | month_split | `M_hex_95CA82__and__J_l0_le-1` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and today's open-to-open J is negative (44, same-row open) | +0.18% (n=42294) | +0.28 pp | +0.37 pp | +0.17% (n=99331) | +0.98% (n=29621) | -0.29% (n=59438) | **KILL** | spy_regime,month_split | `M_hex_95CA82__and__J_l0_lt0` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and ER = −1 — yesterday H or I was a −3% print (44) | +0.15% (n=3646) | +0.25 pp | +0.34 pp | +0.01% (n=8552) | +0.31% (n=4146) | -0.65% (n=3671) | **KILL** | disc_t,hold_t,disc_sign,lottery_day,tape_split,spy_regime,q3_split,no_edge_vs_uncond,month_split | `M_hex_95CA82__and__ER_l0_eq-1` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and JB = 1 — ≥7 of prior-8 H were |H|>3% (44) | +0.04% (n=6614) | +0.14 pp | +0.23 pp | +0.08% (n=14997) | +0.63% (n=7777) | -0.58% (n=6062) | **KILL** | disc_t,hold_t,tape_split,spy_regime,q3_sign,q3_split,no_edge_vs_uncond,month_split,ticker_ghost | `M_hex_95CA82__and__JB_l0_eq1` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and yesterday's M number (low wick) ≥ 5% — lag, not today | -0.05% (n=9187) | +0.05 pp | +0.14 pp | -0.09% (n=20781) | +0.42% (n=9794) | -0.73% (n=9182) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__M_l1_ge05` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and yesterday's I (daily %) was negative — lag | -0.11% (n=43440) | -0.01 pp | +0.08 pp | -0.10% (n=103005) | +0.39% (n=52175) | -0.78% (n=40319) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__I_l1_lt0` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and AH ≥ 2 — two or more prior-6 H ≤ −5% (44, open walk) | -0.13% (n=18217) | -0.03 pp | +0.06 pp | -0.20% (n=41355) | +0.64% (n=21398) | -1.14% (n=16743) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_split,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__AH_l0_ge2` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and yesterday's K number (high wick) ≥ 5% — lag | -0.14% (n=66909) | -0.04 pp | +0.05 pp | -0.18% (n=150283) | +0.43% (n=73978) | -0.87% (n=62976) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__K_l1_ge05` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and FQ = 1 — yesterday's H > +3% (44, prior H only) | -0.14% (n=73203) | -0.04 pp | +0.05 pp | -0.18% (n=165159) | +0.38% (n=81332) | -0.82% (n=69527) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__FQ_l0_eq1` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and H two sessions ago was positive — lag (t−1 H is IZ) | -0.16% (n=152116) | -0.06 pp | +0.03 pp | -0.18% (n=354825) | +0.27% (n=169067) | -0.70% (n=141885) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__H_l2_gt0` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and ER = 1 — yesterday H or I was a +5% print (44) | -0.16% (n=47611) | -0.07 pp | +0.02 pp | -0.22% (n=106559) | +0.39% (n=52768) | -0.85% (n=45569) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__ER_l0_eq1` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and yesterday's M number (low wick) ≥ 2% — lag, not today | -0.18% (n=49974) | -0.08 pp | +0.01 pp | -0.21% (n=113895) | +0.45% (n=55693) | -0.97% (n=48468) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_split,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__M_l1_ge02` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and JC = 1 — none of prior-8 H were < −3% (44) | -0.18% (n=172920) | -0.08 pp | +0.01 pp | -0.18% (n=406839) | +0.17% (n=190048) | -0.58% (n=163249) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__JC_l0_eq1` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and AH ≥ 1 — at least one prior-6 H ≤ −5% (44, open walk) | -0.18% (n=63169) | -0.08 pp | +0.01 pp | -0.23% (n=144349) | +0.50% (n=73318) | -1.05% (n=58398) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__AH_l0_ge1` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number | -0.19% (n=320499) | -0.09 pp | — | -0.21% (n=746098) | +0.27% (n=356712) | -0.75% (n=302578) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split | `M_hex_95CA82` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and AH = 0 — no prior-6 H print ≤ −5% (44, open walk) | -0.19% (n=257330) | -0.09 pp | -0.00 pp | -0.20% (n=601749) | +0.21% (n=283394) | -0.67% (n=244180) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__AH_l0_eq0` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and yesterday's J was negative — lag | -0.19% (n=160663) | -0.09 pp | -0.00 pp | -0.20% (n=374265) | +0.29% (n=182210) | -0.79% (n=151395) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__J_l1_lt0` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and yesterday's K number (high wick) ≥ 2% — lag | -0.19% (n=182247) | -0.09 pp | -0.00 pp | -0.22% (n=415831) | +0.35% (n=202539) | -0.85% (n=173911) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__K_l1_ge02` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and yesterday's J was positive — lag | -0.20% (n=152114) | -0.10 pp | -0.01 pp | -0.22% (n=353492) | +0.24% (n=165946) | -0.71% (n=143820) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__J_l1_gt0` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and yesterday's I (daily %) was positive — lag | -0.20% (n=270069) | -0.10 pp | -0.02 pp | -0.23% (n=626576) | +0.25% (n=296712) | -0.75% (n=255611) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__I_l1_gt0` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and H two sessions ago was negative — lag | -0.23% (n=157779) | -0.13 pp | -0.04 pp | -0.24% (n=365994) | +0.26% (n=175743) | -0.82% (n=150616) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__H_l2_lt0` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and yesterday's G (vol ratio) ≥ 2 — lag | -0.24% (n=36161) | -0.14 pp | -0.06 pp | -0.28% (n=84105) | +0.08% (n=40934) | -0.72% (n=32564) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__G_l1_ge2` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and today's open-to-open J is positive (44, same-row open) | -0.26% (n=271787) | -0.16 pp | -0.07 pp | -0.28% (n=631585) | +0.19% (n=321225) | -0.88% (n=235781) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__J_l0_gt0` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and yesterday's G (vol ratio) ≥ 3 — lag | -0.29% (n=17517) | -0.19 pp | -0.10 pp | -0.35% (n=40589) | -0.09% (n=19787) | -0.71% (n=15442) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__G_l1_ge3` |
| morning cell M is fill hex #95CA82 (mid green, open-knowable). Not M's number, and today's open-to-open J ≥ +1% (44, same-row open) | -0.30% (n=189669) | -0.20 pp | -0.11 pp | -0.32% (n=436045) | +0.18% (n=236881) | -1.02% (n=154124) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82__and__J_l0_ge1` |

### Near-miss (green holdout, beats book and parent by ≥20 bp, not KEEP)

- `M_hex_95CA82__and__J_l0_le-1` holdout +0.71% (n=14758), vs book +0.81 pp, vs parent +0.90 pp. Killed by month_split. Not a card.
- `M_hex_95CA82__and__J_l0_lt0` holdout +0.18% (n=42294), vs book +0.28 pp, vs parent +0.37 pp. Killed by spy_regime,month_split. Not a card.
- `M_hex_95CA82__and__ER_l0_eq-1` holdout +0.15% (n=3646), vs book +0.25 pp, vs parent +0.34 pp. Killed by disc_t,hold_t,disc_sign,lottery_day,tape_split,spy_regime,q3_split,no_edge_vs_uncond,month_split. Not a card.

### What this does not change

- Live `flatten_robust` is frozen. No card under `strategies/`.
- Standing M mid fill on the expand stays **DEMOTE** as a parent.
- Pair+lag open on the smaller dump (KEEP 0 / KILL 4266) is not reopened.
- Close-entry values stay out of the open clock.

Research only. Live frozen.
