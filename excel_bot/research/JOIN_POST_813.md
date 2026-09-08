# JOIN Excel open-gate × fullscan — post-8-13 holdout

_Generated 2026-09-08 · live `flatten_robust` frozen · research only · no live push._

## Plain English

On the post-2026-08-13 window, joining Excel open-gate numbers (J / AH / ER / FQ / JB / JC and H/I/J/G lags) onto the morning join rank **does** beat fullscan-alone. Best Excel KEEP `avoid_J_ge0` holdout H +0.50% vs join top-8 -0.01% (+0.51 pp, n=121). avoid_incomplete is a fullscan-only control (not Excel). Live flatten_robust stays frozen.

**Family verdict: KEEP**

Excel KEEP (vs join top-8, after fees, ghost pass, both tapes green):

- `avoid_J_ge0` — +0.50% (n=121) · +0.51 pp · win 57.9% · Excel: drop join buys with J≥0, refill from J<0
- `elev_cap2_J_le-1` — +0.26% (n=144) · +0.27 pp · win 51.4% · Excel: swap ≤2 J≥0 names in top-8 for J≤−1% from ranks 9–80

Flatten_robust tickets on this same window do **not** confirm: dropping J≥+1% names from the live book **hurts** sleeve-native P&L (movers on 08-20/21 gapped up and paid). Primary label is the join 1d H clock, not flatten 3d.

### What was joined

**Excel (clock gate, open-only):**

- Same-row numbers from the locked 44: **J, AH, ER, FQ, JB, JC** (asserted via `excel_clock_gate.py`).
- Lags (any letter from rows above): **H[t−1], I[t−1], J[t−1], G[t−1]**.
- Fills A B C G J K L M O IR IS IT are legal but **not on these dumps** (no Excel grid / M-hex / O-green / five-cell light in the morning files). Not invented.
- OUT: same-row H/I, M number, B/G/K/O numbers, D/E/F, `core_score`.

**Fullscan (standing dumps, open or earliest fair clock):**

- `data/join/YYYY-MM-DD_ranked.csv` — morning rank, `total_score`, `families_known` / incomplete. Same-day file is the 09:30 ranker.
- `data/stock_book/YYYY-MM-DD_stock_book.json` **1d buy, prior date only** (same-day book is stamped afternoon — not an open feature).
- `data/feature_asof/` morning tags (`join_good`, `blue`, `ab_good`) when the dated file exists. Forward `ret_*` are labels only.
- `data/sleeve_merge/trades.csv` — live flatten_robust tickets (overlay).

### Labels

- **Primary:** same-day Excel **H** = Finviz Change from Open, minus 0.15 pp Futubull. This is the join / stock-book 1d clock.
- **Secondary:** feature_asof `ret_1d` (sleeve-native forward) when the asof file exists; flatten ticket `ret_pct` (already fee-native).

### Window

- KEEP decision: names/days **after 2026-08-13** only.
- Join days with a Finviz label: **18** (2026-08-14 → 2026-09-07).
- Panel name-days with H: **129045**.
- Finviz history dates (for lags): **22** (2026-04-26 → 2026-09-07).
- Recipes were pre-specified from earlier Excel mines (J≤−1, AH, ER, JB, FQ). They were not searched on this holdout.

### Ship bar

Beat fullscan-alone (join top-8) by ≥20 bp after Futubull 15 bp, ghost (name/month/day), leak-free. Both SPY tapes if each has n≥8; otherwise tape-thin is noted, not a KILL. KEEP only if the join elevates and/or avoids better than fullscan alone.

### Fullscan-alone baseline (primary)

| book | n | after-fee H | win | t | ghost |
|---|---:|---:|---:|---:|---|
| join top-8 | 144 | -0.01% | 43.8% | -0.05 | PASS/FAIL/PASS |
| join top-15 | 270 | -0.09% | 44.8% | -0.61 | PASS/FAIL/PASS |

### Recipes vs join top-8

| recipe | kind | holdout H | vs fullscan | win | n | ghost | tapes | asof 1d | verdict | why |
|---|---|---:|---:|---:|---:|---|---|---:|---|---|
| `avoid_J_ge1` | avoid | +0.17% | +0.18 pp | 45.1% | 144 | PASS/PASS/PASS | ↑+0.16%/↓+0.16% | +1.20% (n=29) | **KILL** | no_edge_vs_fullscan |
| `avoid_J_ge0` | avoid | +0.50% | +0.51 pp | 57.9% | 121 | PASS/PASS/PASS | ↑+0.31%/↓+0.71% | +1.56% (n=33) | **KEEP** | — |
| `avoid_JB` | avoid | -0.01% | +0.00 pp | 43.8% | 144 | PASS/FAIL/PASS | ↑+0.30%/↓-0.11% | +0.47% (n=32) | **KILL** | hold_sign,no_edge_vs_fullscan,ghost:month_split,spy_tape |
| `avoid_FQ` | avoid | -0.09% | -0.09 pp | 43.1% | 144 | PASS/FAIL/PASS | ↑+0.12%/↓-0.15% | -0.15% (n=31) | **KILL** | hold_sign,no_edge_vs_fullscan,ghost:month_split,spy_tape |
| `avoid_ER_p1` | avoid | +0.07% | +0.08 pp | 45.1% | 144 | PASS/FAIL/PASS | ↑+0.31%/↓+0.02% | +0.55% (n=32) | **KILL** | no_edge_vs_fullscan,ghost:month_split |
| `avoid_incomplete` | control | +0.30% | +0.31 pp | 53.7% | 136 | PASS/PASS/PASS | ↑+0.46%/↓+0.01% | +1.08% (n=27) | **KEEP** | — |
| `avoid_incomplete_or_Jge1` | control | +0.26% | +0.27 pp | 51.5% | 136 | PASS/PASS/PASS | ↑+0.42%/↓+0.21% | -0.07% (n=25) | **KILL** | no_edge_vs_incomplete_control |
| `elev_cap2_J_le-1` | elevate | +0.26% | +0.27 pp | 51.4% | 144 | PASS/PASS/PASS | ↑+0.46%/↓+0.12% | +0.90% (n=31) | **KEEP** | — |
| `elev_cap2_J_lt0` | elevate | +0.19% | +0.20 pp | 48.6% | 144 | PASS/PASS/PASS | ↑+0.30%/↓+0.12% | +0.98% (n=31) | **KILL** | no_edge_vs_fullscan |
| `elev_cap2_ER_m1` | elevate | -0.10% | -0.09 pp | 44.7% | 141 | PASS/PASS/PASS | ↑+0.44%/↓-0.32% | +0.54% (n=30) | **KILL** | hold_sign,no_edge_vs_fullscan,spy_tape |
| `elev_cap2_AH_ge1` | elevate | -0.24% | -0.23 pp | 45.1% | 142 | PASS/FAIL/PASS | ↑+0.18%/↓-0.37% | +0.30% (n=30) | **KILL** | hold_sign,no_edge_vs_fullscan,ghost:month_split,spy_tape |
| `replace_J_lt0` | replace | +0.34% | +0.35 pp | 54.9% | 144 | PASS/PASS/PASS | ↑+0.31%/↓+0.39% | +1.56% (n=33) | **KEEP** | — |
| `top8_and_J_lt0` | intersect | +0.47% | +0.48 pp | 45.7% | 46 | FAIL/PASS/FAIL | ↑+0.05%/↓+0.93% | +1.88% (n=10) | **KILL** | ghost:name_win_share,lottery_day |
| `top8_and_J_le-1` | intersect | +0.57% | +0.58 pp | 44.8% | 29 | FAIL/PASS/FAIL | ↑-0.16%/↓+1.24% | +2.64% (n=6) | **KILL** | ghost:name_win_share,lottery_day,spy_tape |
| `top8_and_ER_m1` | intersect | +0.21% | +0.22 pp | 30.8% | 13 | FAIL/PASS/FAIL | thin | -4.05% (n=2) | **KILL** | ghost:name:SM=2/13,name_win_share,lottery_day |

### Flatten_robust overlay (sleeve-native P&L, already fee-native)

Live tickets after 2026-08-13: **30** (mean +2.06%). Avoid/elevate on this book does not change live.

| cut | n | mean ret | leftover vs all |
|---|---:|---:|---:|
| all tickets | 30 | +2.06% | +0.00 pp |
| avoid J≥+1% | 13 | -0.39% | -2.45 pp |
| avoid JB | 30 | +2.06% | +0.00 pp |
| keep J≤−1% only | 7 | +1.57% | -0.49 pp |
| keep ER=−1 only | 4 | -4.38% | -6.44 pp |

### What this does not do

- Does not wire live `flatten_robust` / `LIVE_POLICY` / `join_rules.json`.
- Does not use same-row H/I, H paint, or M’s number.
- Does not treat afternoon stock-book prints as 09:30 features.
- Does not claim CE/CD (need 43 sessions of High/Low; Finviz tape is too short).
- Does not re-open shade hex / light+O (fills not in these dumps).

Gate: `OPEN_SAME_ROW_LABELS + CLOCK_MAP`. Fills open: A, B, C, G, J, K, L, M, O, IR, IS, IT. Live frozen.

Research only.
