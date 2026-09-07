## Color harden (light + open fill vs light alone)

_Generated 2026-09-07. Color-only beat — no Finviz / AB / weather / book. Futubull fees kept. Live `flatten_robust` frozen. No cards._

### Question

When the morning green light first turns on, does **also** requiring an open-knowable cell to be green (or red) add money after fees, versus the light alone?

Open-knowable paints: **A, B, C, G, J, K, L, M, O**. Close paints D, E, F, H, I, N never start this trade. Same ship bar as the KEEP-6 harden: discovery/holdout, both 2026 halves (cut 2026-05-01), SPY up and down, top-day lottery under 25%, beat the parent light by at least 20 bps. Grids are the Yahoo/rows rebuild (`source: rows_cache`), **3598** files. No Excel cached A–F.

**KEEP 3** recipes · **KILL 3** · **THIN 0**. Color folds that themselves KEEP: **3** (all of them are a green **O**).

**A green O on top of the light can add about +24–45 bp.** Other morning greens (A, B, G, J, K, L, M) do not clear +20 bp versus the light itself. A color by itself, without the light, already failed this bar (late 2026 goes red on same-day holds).

### Per recipe (light alone vs light + best open color)

| meaning | hold | light alone | light + best color | extra after fees | verdict | code |
|---|---|---|---|---|---|---|
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. Plus morning cell O is also highlighted green (known at 9:30). | next 1 | +1.74% (n=10931) | +1.98% (n=4812) | +0.24 pp | **KEEP** | `hyst_open_core_e5_x2__O_green` |
| all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. Plus morning cell M is also highlighted green (known at 9:30). | next 1 | +1.33% (n=10862) | +1.49% (n=3267) | +0.16 pp | **KILL** | `hyst_open_score_e5_x2__M_green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to 0. Plus morning cell L is also highlighted green (known at 9:30). | next 1 | +1.28% (n=11120) | +1.46% (n=6403) | +0.18 pp | **KILL** | `hyst_open_core_e5_x0__L_green` |
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. Plus morning cell O is also highlighted green (known at 9:30). | next 2 | +1.62% (n=10931) | +1.88% (n=4812) | +0.27 pp | **KEEP** | `hyst_open_core_e5_x2__O_green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to 0. Plus morning cell O is also highlighted green (known at 9:30). | next 2 | +1.21% (n=11120) | +1.38% (n=4903) | +0.17 pp | **KILL** | `hyst_open_core_e5_x0__O_green` |
| all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. Plus morning cell O is also highlighted green (known at 9:30). | next 2 | +1.54% (n=10862) | +1.99% (n=5264) | +0.45 pp | **KEEP** | `hyst_open_score_e5_x2__O_green` |

### Every open-letter fold (same six lights)

| light | hold | color | holdout | vs light | verdict | why | code |
|---|---|---|---|---|---|---|---|
| `hyst_open_score_e5_x2` | next 2 | O_green | +1.99% (n=5264) | +0.45 pp | **KEEP** | — | `hyst_open_score_e5_x2__O_green` |
| `hyst_open_core_e5_x2` | next 1 | O_green | +1.98% (n=4812) | +0.24 pp | **KEEP** | — | `hyst_open_core_e5_x2__O_green` |
| `hyst_open_core_e5_x2` | next 2 | O_green | +1.88% (n=4812) | +0.27 pp | **KEEP** | — | `hyst_open_core_e5_x2__O_green` |
| `hyst_open_core_e5_x2` | next 1 | L_green | +1.92% (n=6667) | +0.18 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__L_green` |
| `hyst_open_core_e5_x2` | next 1 | G_green | +1.90% (n=9073) | +0.16 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__G_green` |
| `hyst_open_core_e5_x2` | next 2 | G_green | +1.76% (n=9073) | +0.15 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__G_green` |
| `hyst_open_core_e5_x2` | next 1 | B_green | +1.76% (n=10518) | +0.03 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__B_green` |
| `hyst_open_core_e5_x2` | next 1 | J_green | +1.76% (n=8900) | +0.02 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__J_green` |
| `hyst_open_core_e5_x2` | next 1 | A_green | +1.74% (n=10931) | +0.00 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__A_green` |
| `hyst_open_core_e5_x2` | next 1 | M_green | +1.68% (n=2485) | -0.05 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__M_green` |
| `hyst_open_core_e5_x2` | next 2 | L_green | +1.66% (n=6667) | +0.04 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__L_green` |
| `hyst_open_core_e5_x2` | next 2 | B_green | +1.63% (n=10518) | +0.01 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__B_green` |
| `hyst_open_core_e5_x2` | next 2 | A_green | +1.62% (n=10931) | +0.00 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__A_green` |
| `hyst_open_score_e5_x2` | next 2 | J_green | +1.60% (n=8827) | +0.05 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__J_green` |
| `hyst_open_core_e5_x2` | next 2 | J_green | +1.60% (n=8900) | -0.02 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__J_green` |
| `hyst_open_score_e5_x2` | next 2 | B_green | +1.58% (n=9887) | +0.03 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__B_green` |
| `hyst_open_core_e5_x2` | next 1 | K_green | +1.57% (n=6099) | -0.17 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__K_green` |
| `hyst_open_score_e5_x2` | next 2 | A_green | +1.54% (n=10861) | -0.00 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__A_green` |
| `hyst_open_score_e5_x2` | next 1 | M_green | +1.49% (n=3267) | +0.16 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__M_green` |
| `hyst_open_core_e5_x0` | next 1 | L_green | +1.46% (n=6403) | +0.18 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__L_green` |
| `hyst_open_core_e5_x0` | next 1 | O_green | +1.45% (n=4903) | +0.17 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__O_green` |
| `hyst_open_core_e5_x2` | next 2 | K_green | +1.45% (n=6099) | -0.17 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__K_green` |
| `hyst_open_core_e5_x0` | next 1 | G_green | +1.43% (n=8940) | +0.14 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__G_green` |
| `hyst_open_score_e5_x2` | next 1 | L_green | +1.42% (n=7184) | +0.09 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__L_green` |
| `hyst_open_core_e5_x2` | next 2 | M_green | +1.39% (n=2485) | -0.23 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__M_green` |
| `hyst_open_core_e5_x0` | next 2 | O_green | +1.38% (n=4903) | +0.17 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__O_green` |
| `hyst_open_score_e5_x2` | next 1 | O_green | +1.37% (n=5264) | +0.04 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__O_green` |
| `hyst_open_score_e5_x2` | next 1 | B_green | +1.35% (n=9887) | +0.02 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__B_green` |
| `hyst_open_score_e5_x2` | next 1 | J_green | +1.34% (n=8827) | +0.01 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__J_green` |
| `hyst_open_core_e5_x0` | next 2 | G_green | +1.33% (n=8940) | +0.12 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__G_green` |
| `hyst_open_score_e5_x2` | next 1 | A_green | +1.33% (n=10861) | -0.00 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__A_green` |
| `hyst_open_core_e5_x0` | next 1 | M_green | +1.33% (n=2513) | +0.04 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__M_green` |
| `hyst_open_score_e5_x2` | next 1 | G_green | +1.32% (n=8239) | -0.01 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__G_green` |
| `hyst_open_core_e5_x0` | next 1 | J_green | +1.30% (n=9186) | +0.02 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__J_green` |
| `hyst_open_core_e5_x0` | next 1 | B_green | +1.30% (n=10834) | +0.01 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__B_green` |
| `hyst_open_core_e5_x0` | next 1 | A_green | +1.28% (n=11120) | +0.00 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__A_green` |
| `hyst_open_core_e5_x0` | next 2 | L_green | +1.26% (n=6403) | +0.05 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__L_green` |
| `hyst_open_score_e5_x2` | next 2 | G_green | +1.22% (n=8239) | -0.33 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__G_green` |
| `hyst_open_core_e5_x0` | next 2 | M_green | +1.22% (n=2513) | +0.01 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__M_green` |
| `hyst_open_core_e5_x0` | next 2 | A_green | +1.21% (n=11120) | +0.00 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__A_green` |
| `hyst_open_core_e5_x0` | next 2 | B_green | +1.21% (n=10834) | -0.00 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__B_green` |
| `hyst_open_core_e5_x0` | next 2 | J_green | +1.20% (n=9186) | -0.01 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__J_green` |
| `hyst_open_score_e5_x2` | next 2 | L_green | +1.18% (n=7184) | -0.37 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__L_green` |
| `hyst_open_core_e5_x0` | next 1 | K_green | +1.17% (n=5714) | -0.12 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__K_green` |
| `hyst_open_score_e5_x2` | next 2 | M_green | +1.13% (n=3267) | -0.41 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__M_green` |
| `hyst_open_core_e5_x0` | next 2 | K_green | +1.12% (n=5714) | -0.09 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__K_green` |
| `hyst_open_score_e5_x2` | next 1 | K_green | +0.99% (n=6244) | -0.34 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__K_green` |
| `hyst_open_score_e5_x2` | next 2 | K_green | +0.95% (n=6244) | -0.60 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__K_green` |

KEEP is still research-only: one 2026 window, Futubull model, no card, no live wire. Joins (Finviz / AB / weather / book) are out of scope for this beat.

