# Excel color + join mine

## Color + join mine (open-knowable fills, PIT joins)

_Generated 2026-09-07. Colors are signals, not decoration. Futubull fees kept. Live `flatten_robust` frozen. No cards._

### What a color means

The sheet paints a cell green or red. That paint is the signal. At 9:30 we can already see columns **A, B, C, G, J, K, L, M, O**. Columns D, E, F, H, I, N wait until the close — they never start an open trade. Green = buy at the open. Red = short at the open. Sell after 1 session (same-day close) or 2 sessions (next close).

Everyone-else baseline, same clock and fees: next-1 **−0.07%**, next-2 **+0.41%**.

### Joins (yesterday's tape only)

AB / weather / overnight book use the last file **dated before** the entry date. Same-day files are not knowable at 9:30. AB coverage 14 days (2026-08-19–2026-09-06); book 15 days; weather 20 days. Finviz cohorts are the **current snapshot**, not 2026 history.

Grids **3598**. Cells scored **122**. **KEEP 9** · **THIN 0** · **KILL 113**.

**Color alone does not clear the bar.** One morning cell being green is not enough — the late half of 2026 goes red on same-day holds. The already-proved light **plus a green O** adds about +20–45 bp over the light itself. High-vol Finviz names add +24–64 bp, but that tag is today's snapshot, not 2026 history. AB / weather / overnight book only exist for late Aug–Sep, so they fail walk-forward (no first half).

### Color → next-N-days (open letters only)

| meaning | hold | side | holdout after fees | vs everyone | first half | second half | fattest day | verdict | code |
|---|---|---|---|---|---|---|---|---|---|
| Morning cell L is highlighted green (known at 9:30). Buy at that open and sell at the next day's close. | next 2 | long | +1.06% (n=67329) | — | +0.21% (n=79261) | +0.63% (n=87265) | 11.2% | **KILL** | `color_L_green` |
| Morning cell O is highlighted green (known at 9:30). Buy at that open and sell at the next day's close. | next 2 | long | +0.96% (n=35984) | — | +0.37% (n=40310) | +0.61% (n=49128) | 13.5% | **KILL** | `color_O_green` |
| Morning cell G is highlighted green (known at 9:30). Buy at that open and sell at the next day's close. | next 2 | long | +0.51% (n=61404) | — | +0.39% (n=59896) | +0.18% (n=92424) | 10.6% | **KILL** | `color_G_green` |
| Morning cell J is highlighted green (known at 9:30). Buy at that open and sell at the next day's close. | next 2 | long | +0.48% (n=70544) | — | +0.17% (n=96161) | +0.69% (n=77567) | 12.3% | **KILL** | `color_J_green` |
| Morning cell B is highlighted green (known at 9:30). Buy at that open and sell at the next day's close. | next 2 | long | +0.44% (n=91063) | — | +0.03% (n=103443) | +0.44% (n=120427) | 12.7% | **KILL** | `color_B_green` |
| Morning cell A is highlighted green (known at 9:30). Buy at that open and sell at the next day's close. | next 2 | long | +0.41% (n=152822) | — | +0.11% (n=174580) | +0.28% (n=200812) | 9.5% | **KILL** | `color_A_green` |
| Morning cell O is highlighted green (known at 9:30). Buy at that open and sell at the same day's close. | next 1 | long | +0.41% (n=36139) | — | +0.38% (n=40310) | -0.14% (n=49506) | 6.1% | **KILL** | `color_O_green` |
| Morning cell L is highlighted green (known at 9:30). Buy at that open and sell at the same day's close. | next 1 | long | +0.21% (n=67560) | — | +0.17% (n=79261) | -0.14% (n=87800) | 5.3% | **KILL** | `color_L_green` |
| Morning cell G is highlighted green (known at 9:30). Buy at that open and sell at the same day's close. | next 1 | long | +0.20% (n=61695) | — | +0.25% (n=59896) | -0.15% (n=93145) | 5.8% | **KILL** | `color_G_green` |
| Morning cell J is highlighted green (known at 9:30). Buy at that open and sell at the same day's close. | next 1 | long | +0.17% (n=70849) | — | +0.11% (n=96161) | -0.12% (n=78304) | 7.0% | **KILL** | `color_J_green` |
| Morning cell A is highlighted green (known at 9:30). Buy at that open and sell at the same day's close. | next 1 | long | +0.12% (n=152838) | — | +0.10% (n=174580) | -0.15% (n=200853) | 5.0% | **KILL** | `color_A_green` |
| Morning cell B is highlighted green (known at 9:30). Buy at that open and sell at the same day's close. | next 1 | long | +0.08% (n=91076) | — | +0.09% (n=103443) | -0.16% (n=120453) | 6.1% | **KILL** | `color_B_green` |
| Morning cell M is highlighted green (known at 9:30). Buy at that open and sell at the same day's close. | next 1 | long | +0.01% (n=28328) | — | +0.14% (n=55157) | -0.35% (n=14490) | 9.3% | **KILL** | `color_M_green` |
| Morning cell M is highlighted green (known at 9:30). Buy at that open and sell at the next day's close. | next 2 | long | -0.03% (n=28327) | — | -0.02% (n=55157) | -0.06% (n=14489) | 13.3% | **KILL** | `color_M_green` |
| Morning cell K is highlighted green (known at 9:30). Buy at that open and sell at the next day's close. | next 2 | long | -0.05% (n=45745) | — | -0.05% (n=45513) | -0.06% (n=68391) | 4.8% | **KILL** | `color_K_green` |
| Morning cell K is highlighted green (known at 9:30). Buy at that open and sell at the same day's close. | next 1 | long | -0.16% (n=46080) | — | -0.12% (n=45513) | -0.16% (n=69185) | 4.9% | **KILL** | `color_K_green` |
| Morning cell A is highlighted red (known at 9:30). Short at that open and sell at the same day's close. | next 1 | short | -0.21% (n=78522) | — | -0.14% (n=83046) | -0.16% (n=111267) | 6.3% | **KILL** | `color_A_red` |
| Morning cell B is highlighted red (known at 9:30). Short at that open and sell at the same day's close. | next 1 | short | -0.26% (n=81292) | — | -0.16% (n=86917) | -0.17% (n=114147) | 6.5% | **KILL** | `color_B_red` |
| Morning cell O is highlighted red (known at 9:30). Short at that open and sell at the same day's close. | next 1 | short | -0.31% (n=181969) | — | -0.28% (n=200116) | -0.18% (n=246904) | 5.2% | **KILL** | `color_O_red` |
| Morning cell J is highlighted red (known at 9:30). Short at that open and sell at the same day's close. | next 1 | short | -0.35% (n=137776) | — | -0.29% (n=139736) | -0.17% (n=199040) | 6.4% | **KILL** | `color_J_red` |
| Morning cell L is highlighted red (known at 9:30). Short at that open and sell at the same day's close. | next 1 | short | -0.59% (n=47817) | — | -0.56% (n=46451) | -0.16% (n=72320) | 5.8% | **KILL** | `color_L_red` |
| Morning cell O is highlighted red (known at 9:30). Short at that open and sell at the next day's close. | next 2 | short | -0.91% (n=180728) | — | -0.32% (n=200116) | -1.01% (n=243852) | 6.5% | **KILL** | `color_O_red` |
| Morning cell J is highlighted red (known at 9:30). Short at that open and sell at the next day's close. | next 2 | short | -1.16% (n=136727) | — | -0.30% (n=139736) | -1.20% (n=196437) | 7.5% | **KILL** | `color_J_red` |
| Morning cell B is highlighted red (known at 9:30). Short at that open and sell at the next day's close. | next 2 | short | -1.28% (n=81279) | — | -0.22% (n=86917) | -1.70% (n=114115) | 7.0% | **KILL** | `color_B_red` |

### Light + a green cell (fold into the 6 KEEP hysteresis names)

| meaning | hold | holdout | vs parent light | verdict | code |
|---|---|---|---|---|---|
| Same morning light as `hyst_open_score_e5_x2`, and morning cell O is also green. | next 2 | +1.99% (n=5264) | +0.45 pp | **KEEP** | `hyst_open_score_e5_x2__O_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell O is also green. | next 1 | +1.98% (n=4812) | +0.24 pp | **KEEP** | `hyst_open_core_e5_x2__O_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell O is also green. | next 2 | +1.88% (n=4812) | +0.27 pp | **KEEP** | `hyst_open_core_e5_x2__O_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell L is also green. | next 1 | +1.92% (n=6667) | +0.18 pp | **KILL** | `hyst_open_core_e5_x2__L_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell G is also green. | next 1 | +1.90% (n=9073) | +0.16 pp | **KILL** | `hyst_open_core_e5_x2__G_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell G is also green. | next 2 | +1.76% (n=9073) | +0.15 pp | **KILL** | `hyst_open_core_e5_x2__G_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell B is also green. | next 1 | +1.76% (n=10518) | +0.03 pp | **KILL** | `hyst_open_core_e5_x2__B_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell J is also green. | next 1 | +1.76% (n=8900) | +0.02 pp | **KILL** | `hyst_open_core_e5_x2__J_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell A is also green. | next 1 | +1.74% (n=10931) | +0.00 pp | **KILL** | `hyst_open_core_e5_x2__A_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell M is also green. | next 1 | +1.68% (n=2485) | -0.05 pp | **KILL** | `hyst_open_core_e5_x2__M_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell L is also green. | next 2 | +1.66% (n=6667) | +0.04 pp | **KILL** | `hyst_open_core_e5_x2__L_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell B is also green. | next 2 | +1.63% (n=10518) | +0.01 pp | **KILL** | `hyst_open_core_e5_x2__B_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell A is also green. | next 2 | +1.62% (n=10931) | +0.00 pp | **KILL** | `hyst_open_core_e5_x2__A_green` |
| Same morning light as `hyst_open_score_e5_x2`, and morning cell J is also green. | next 2 | +1.60% (n=8827) | +0.05 pp | **KILL** | `hyst_open_score_e5_x2__J_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell J is also green. | next 2 | +1.60% (n=8900) | -0.02 pp | **KILL** | `hyst_open_core_e5_x2__J_green` |
| Same morning light as `hyst_open_score_e5_x2`, and morning cell B is also green. | next 2 | +1.58% (n=9887) | +0.03 pp | **KILL** | `hyst_open_score_e5_x2__B_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell K is also green. | next 1 | +1.57% (n=6099) | -0.17 pp | **KILL** | `hyst_open_core_e5_x2__K_green` |
| Same morning light as `hyst_open_score_e5_x2`, and morning cell A is also green. | next 2 | +1.54% (n=10861) | -0.00 pp | **KILL** | `hyst_open_score_e5_x2__A_green` |

### Light + AB / weather / book / Finviz (useful combo = win)

| meaning | hold | holdout | vs parent | dates | verdict | why | code |
|---|---|---|---|---:|---|---|---|
| Same morning light as `hyst_open_core_e5_x2`, and Finviz snapshot cohort volM (not a historical as-of). | next 1 | +2.22% (n=5287) | +0.48 pp | 160 | **KEEP** | — | `hyst_open_core_e5_x2__fz_volM` |
| Same morning light as `hyst_open_score_e5_x2`, and Finviz snapshot cohort volM (not a historical as-of). | next 2 | +2.19% (n=5238) | +0.64 pp | 160 | **KEEP** | — | `hyst_open_score_e5_x2__fz_volM` |
| Same morning light as `hyst_open_core_e5_x2`, and Finviz snapshot cohort volM (not a historical as-of). | next 2 | +2.02% (n=5287) | +0.40 pp | 160 | **KEEP** | — | `hyst_open_core_e5_x2__fz_volM` |
| Same morning light as `hyst_open_score_e5_x2`, and Finviz snapshot cohort volM (not a historical as-of). | next 1 | +1.65% (n=5238) | +0.32 pp | 160 | **KEEP** | — | `hyst_open_score_e5_x2__fz_volM` |
| Same morning light as `hyst_open_core_e5_x0`, and Finviz snapshot cohort volM (not a historical as-of). | next 1 | +1.61% (n=5553) | +0.33 pp | 160 | **KEEP** | — | `hyst_open_core_e5_x0__fz_volM` |
| Same morning light as `hyst_open_core_e5_x0`, and Finviz snapshot cohort volM (not a historical as-of). | next 2 | +1.45% (n=5553) | +0.24 pp | 160 | **KEEP** | — | `hyst_open_core_e5_x0__fz_volM` |
| Same morning light as `hyst_open_core_e5_x2`, and prior weather was risk-off. | next 2 | +2.21% (n=496) | +0.59 pp | 9 | **KILL** | date_bar,lottery_day,tape_split | `hyst_open_core_e5_x2__wx_risk_off` |
| Same morning light as `hyst_open_core_e5_x2`, and prior weather was risk-off. | next 1 | +2.07% (n=496) | +0.34 pp | 9 | **KILL** | date_bar,tape_split | `hyst_open_core_e5_x2__wx_risk_off` |
| Same morning light as `hyst_open_core_e5_x0`, and prior weather was risk-off. | next 2 | +1.71% (n=505) | +0.50 pp | 9 | **KILL** | date_bar,lottery_day,tape_split | `hyst_open_core_e5_x0__wx_risk_off` |
| Same morning light as `hyst_open_core_e5_x2`, and the overnight 1-day book had it as a buy. | next 2 | +1.69% (n=7) | +0.07 pp | 10 | **KILL** | thin_disc,thin_hold,disc_t,hold_t,ticker_bar,date_bar,lottery_trade,lottery_day,tape_split,no_edge_vs_parent | `hyst_open_core_e5_x2__book_1d_buy` |
| Same morning light as `hyst_open_core_e5_x0`, and the overnight 1-day book had it as a buy. | next 1 | +1.68% (n=7) | +0.40 pp | 10 | **KILL** | thin_disc,thin_hold,disc_t,hold_t,ticker_bar,date_bar,lottery_trade,lottery_day,tape_split | `hyst_open_core_e5_x0__book_1d_buy` |
| Same morning light as `hyst_open_core_e5_x2`, and the overnight 1-day book had it as a buy. | next 1 | +1.65% (n=7) | -0.08 pp | 10 | **KILL** | thin_disc,thin_hold,hold_t,ticker_bar,date_bar,lottery_trade,lottery_day,tape_split,no_edge_vs_parent | `hyst_open_core_e5_x2__book_1d_buy` |
| Same morning light as `hyst_open_score_e5_x2`, and prior weather was risk-off. | next 2 | +1.65% (n=457) | +0.11 pp | 9 | **KILL** | disc_t,date_bar,lottery_day,tape_split,spy_regime,no_edge_vs_parent | `hyst_open_score_e5_x2__wx_risk_off` |
| Same morning light as `hyst_open_score_e5_x2`, and prior weather was risk-off. | next 1 | +1.54% (n=457) | +0.22 pp | 9 | **KILL** | date_bar,lottery_day,tape_split | `hyst_open_score_e5_x2__wx_risk_off` |
| Same morning light as `hyst_open_core_e5_x0`, and prior weather was risk-off. | next 1 | +1.51% (n=505) | +0.22 pp | 9 | **KILL** | date_bar,lottery_day,tape_split | `hyst_open_core_e5_x0__wx_risk_off` |
| Same morning light as `hyst_open_core_e5_x2`, and prior weather was risk-on. | next 1 | +1.46% (n=157) | -0.28 pp | 2 | **KILL** | thin_disc,date_bar,lottery_day,tape_split,spy_regime,no_edge_vs_parent | `hyst_open_core_e5_x2__wx_risk_on` |
| Same morning light as `hyst_open_core_e5_x0`, and the overnight 1-day book had it as a buy. | next 2 | +1.44% (n=7) | +0.23 pp | 10 | **KILL** | thin_disc,thin_hold,disc_t,hold_t,ticker_bar,date_bar,lottery_trade,lottery_day,tape_split | `hyst_open_core_e5_x0__book_1d_buy` |
| Same morning light as `hyst_open_core_e5_x2`, and yesterday's AB tape already liked the name. | next 2 | +1.27% (n=251) | -0.35 pp | 11 | **KILL** | date_bar,lottery_day,tape_split,no_edge_vs_parent | `hyst_open_core_e5_x2__ab_good` |
| Same morning light as `hyst_open_core_e5_x2`, and Finviz snapshot cohort opt (not a historical as-of). | next 1 | +1.23% (n=7990) | -0.51 pp | 160 | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__fz_opt` |
| Same morning light as `hyst_open_core_e5_x2`, and Finviz snapshot cohort opt (not a historical as-of). | next 2 | +1.19% (n=7990) | -0.42 pp | 160 | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__fz_opt` |
| Same morning light as `hyst_open_core_e5_x2`, and Finviz snapshot cohort mid(1-10B) (not a historical as-of). | next 2 | +1.05% (n=3664) | -0.57 pp | 160 | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__fz_mid(1-10B)` |
| Same morning light as `hyst_open_core_e5_x2`, and yesterday's AB tape already liked the name. | next 1 | +1.04% (n=251) | -0.70 pp | 11 | **KILL** | date_bar,tape_split,no_edge_vs_parent | `hyst_open_core_e5_x2__ab_good` |
| Same morning light as `hyst_open_core_e5_x2`, and Finviz snapshot cohort mid(1-10B) (not a historical as-of). | next 1 | +1.03% (n=3664) | -0.71 pp | 160 | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__fz_mid(1-10B)` |
| Same morning light as `hyst_open_core_e5_x0`, and Finviz snapshot cohort opt (not a historical as-of). | next 2 | +0.94% (n=7763) | -0.27 pp | 160 | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__fz_opt` |

KEEP still research-only. Join tapes only exist for late Aug–Sep 2026, so most join cells are THIN on dates. Color cells use the full Jan–Sep window. No live wire.

