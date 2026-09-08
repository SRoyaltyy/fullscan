# JOIN Excel open-gate × fullscan — prove (post-8-13 KEEP)

_Generated 2026-09-08 · live `flatten_robust` frozen · research only · no live push._

## Plain English

J clock leak **PASS**. Prove (weekday 2026-08-26→2026-09-07) does **not** re-clear the ship bar. `avoid_J_ge0` n=64 H +0.22% (+0.04 pp vs same-window top-8, ghost PASS/FAIL/FAIL). `elev_cap2_J_le-1` n=64 H +0.33% (+0.15 pp, ghost PASS/FAIL/FAIL). Discovery half still prints (`avoid_J_ge0` +0.72 pp n=64) — that is the peek, not prove. Pooled weekday leftover is `avoid_J_ge0` +0.38 pp n=128 / `elev_cap2_J_le-1` +0.26 pp n=128 (includes discovery; not a holdout). Wider book (top-80) prove `avoid_J_ge0` +0.12 pp n=640. Join dumps do not add sessions before 8-13 with a fresh J (08-13 prior Open is 04-26). Family is **CONDITIONAL**: not KEEP holds, not a full KILL of the discovery print. Live flatten_robust stays frozen. Do not wire. Expanded prove: the J overlay does **not** hold as a general rule. Yahoo liquid names (prior-session volume ≥ 1M, 2024–2026) are about flat (+2 to +5 bp, ghost month fail). The all-name Yahoo tape’s large mean is a microcap lottery (ghost FAIL). Join-full / membership on the Finviz window is +5 to +13 bp (under 20 bp). membership_liq is flat to negative. Join top-8 stays **CONDITIONAL** (discovery only).

**Family verdict: CONDITIONAL**

Standing recipes (research only, not live): `avoid_J_ge0` and `elev_cap2_J_le-1` on morning join top-8. Open Excel **J only** (clock-clean prior-session Open). Live stays frozen.

### J clock / leak re-audit

**Clock verdict: PASS** (1280 name-days reconstructed).

- Formula: `J = (Finviz Open[t] − Finviz Open[prior weekday]) / Open[prior weekday]`
- Excel map: CLOCK_MAP J = C[t] vs C[t−1] (value-open). C = Open / IT.
- Inputs: Finviz Open only (09:30 print). Prior bar skips Sat/Sun dumps.
- Not inputs: same-row H (Change from Open) — label only; same-row I (Change) — label only; High / Low / Close / Price; M number / H paint / core_score
- File clock: Finviz CSVs are EOD dumps; the Open column is still the 09:30 print (Excel C). Using it at the open is not a close peek.
- Hole (not a future peek): 2026-08-26 has join but no Finviz — 08-27 J uses 08-25 Open (missing bar, not future).
- Hole (not a future peek): 2026-08-13 J vs 2026-04-26 is stale and is not used.

Same-row H/I, M number, H paint, and `core_score` are not features. pick_book reads only J flags (`J_ge0` / `J_lt0` / `J_le-1`) plus join rank.

### Case studies (prove window)

**avoid_J_ge0** — 2026-08-27 `FIGR` → `EMBJ`

- Without J: join top-8 keeps FIGR (rank 3).
- With J: avoid_J_ge0 drops FIGR (J≥0) and refills EMBJ (J<0, rank 9).
- `FIGR` at open: J +3.82% (Open 40.5 vs 2026-08-25 Open 39.01), join rank 3. After fees: H -8.59% (raw -8.44%), I -10.02% (raw -9.87%).
- `EMBJ` at open: J -2.09% (Open 75.78 vs 2026-08-25 Open 77.4), join rank 9. After fees: H -0.82% (raw -0.67%), I +0.71% (raw +0.86%).

**elev_cap2_J_le-1** — 2026-09-04 `HRMY` → `AVAH`

- Without J: join top-8 keeps HRMY (rank 1).
- With J: elev_cap2 swaps HRMY (J≥0) for AVAH (J≤−1%, rank 13).
- `HRMY` at open: J +3.92% (Open 42.93 vs 2026-09-03 Open 41.31), join rank 1. After fees: H -2.64% (raw -2.49%), I -2.48% (raw -2.33%).
- `AVAH` at open: J -2.29% (Open 13.22 vs 2026-09-03 Open 13.53), join rank 13. After fees: H +3.03% (raw +3.18%), I +3.18% (raw +3.33%).

### Prove (time holdout, weekday sessions)

Post-8-13 was discovery. Prove = later weekday sessions (2026-08-26 → 2026-09-07), J vs prior weekday Open. Sunday join dumps (2026-08-30, 2026-09-06) are **not** a 1d clock and are held out. 2026-08-13 J is stale (only prior Open is 2026-04-26) and is not used.

| window | book | recipe | n | days | after-fee H | vs fullscan | win | ghost | bar |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| discovery | top-8 | `join_top8` | 64 | 8 | -0.08% | — | 37.5% | FAIL/PASS/FAIL | **—** |
| discovery | top-8 | `avoid_J_ge0` | 64 | 8 | +0.63% | +0.72 pp | 64.1% | PASS/PASS/FAIL | **KILL** |
| discovery | top-8 | `elev_cap2_J_le-1` | 64 | 8 | +0.28% | +0.36 pp | 51.6% | PASS/PASS/FAIL | **KILL** |
| prove | top-8 | `join_top8` | 64 | 8 | +0.18% | — | 50.0% | PASS/FAIL/FAIL | **—** |
| prove | top-8 | `avoid_J_ge0` | 64 | 8 | +0.22% | +0.04 pp | 48.4% | PASS/FAIL/FAIL | **KILL** |
| prove | top-8 | `elev_cap2_J_le-1` | 64 | 8 | +0.33% | +0.15 pp | 51.6% | PASS/FAIL/FAIL | **KILL** |
| pooled_sessions | top-8 | `join_top8` | 128 | 16 | +0.05% | — | 43.8% | PASS/FAIL/PASS | **—** |
| pooled_sessions | top-8 | `avoid_J_ge0` | 128 | 16 | +0.43% | +0.38 pp | 56.2% | PASS/PASS/PASS | **KEEP** |
| pooled_sessions | top-8 | `elev_cap2_J_le-1` | 128 | 16 | +0.30% | +0.26 pp | 51.6% | PASS/PASS/PASS | **KEEP** |

### Wider book (more name-days on the same dumps)

Join ranked files on disk stop at 2026-08-12 / 2026-09-07. No older weekday join+Finviz pair exists (04-26 has Finviz+membership, no weather, no ranked file). Wider book = ranks 1–80 on the same sessions.

| window | recipe | n | days | after-fee H | vs top-80 | win | ghost | bar |
|---|---|---:|---:|---:|---:|---:|---|---|
| discovery | `join_top80` | 640 | 8 | +0.06% | — | 45.6% | PASS/PASS/FAIL | **—** |
| discovery | `avoid_J_ge0` | 640 | 8 | +0.10% | +0.04 pp | 49.1% | PASS/PASS/FAIL | **KILL** |
| discovery | `elev_cap2_J_le-1` | 640 | 8 | +0.06% | +0.00 pp | 45.6% | PASS/PASS/FAIL | **KILL** |
| prove | `join_top80` | 640 | 8 | +0.25% | — | 50.2% | PASS/PASS/PASS | **—** |
| prove | `avoid_J_ge0` | 640 | 8 | +0.37% | +0.12 pp | 49.4% | PASS/PASS/PASS | **KILL** |
| prove | `elev_cap2_J_le-1` | 640 | 8 | +0.25% | +0.00 pp | 50.2% | PASS/PASS/PASS | **KILL** |
| pooled_sessions | `join_top80` | 1280 | 16 | +0.15% | — | 47.9% | PASS/PASS/PASS | **—** |
| pooled_sessions | `avoid_J_ge0` | 1280 | 16 | +0.24% | +0.08 pp | 49.2% | PASS/PASS/PASS | **KILL** |
| pooled_sessions | `elev_cap2_J_le-1` | 1280 | 16 | +0.15% | +0.00 pp | 47.9% | PASS/PASS/PASS | **KILL** |

cap=2 on an 80-name book is a 2.5% swap — elev_cap2 is a top-8 recipe and is not expected to move the wide book.

### Flatten / sleeve confirm (KEEP recipes, not J≥+1%)

First flatten cut used J≥+1%, not the KEEP recipes. Correct recipes: `avoid_J_ge0` on all post-8-13 tickets is n=11, sleeve +0.27% (-1.79 pp vs all) and same-day H +0.81% (-0.59 pp). Movers on 08-20/21 gapped up (J>0) and paid sleeve; blanket J-avoid removes those winners. Fair clock — J-avoid on io_core only, movers untouched — is n=27, sleeve +3.01% (+0.95 pp vs all). H and sleeve disagree on names like CYPH (sleeve +25%, H −3%). Flatten does not confirm a blanket KEEP. Do not wire.

Live tickets after 2026-08-13: **30**. KEEP is a join top-8 1d H overlay. Flatten is io_core 3d + mover_long 1d sleeve-native P&L. Blanket J-avoid fights the mover thesis (buy gap-up). Fair test applies J-avoid to io only. Avoid/elevate here does not change live.

| cut | n | sleeve P&L | vs sleeve | same-day H | vs H |
|---|---:|---:|---:|---:|---:|
| all tickets | 30 | +2.06% | — | +1.40% | — |
| avoid_J_ge0 (keep J<0) | 11 | +0.27% | -1.79 pp | +0.81% | -0.59 pp |
| elev_cap2 analogue (drop ≤2 J≥0 / day) | 23 | +1.87% | -0.19 pp | +1.59% | +0.19 pp |
| io_core only | 10 | -1.31% | -3.37 pp | +0.32% | -1.08 pp |
| mover_long only | 20 | +3.74% | +1.68 pp | +1.94% | +0.54 pp |
| io_core avoid_J_ge0 | 7 | +0.92% | +2.23 pp | +0.24% | -0.07 pp |
| fair: J-avoid on io only, movers untouched | 27 | +3.01% | +0.95 pp | +1.50% | +0.10 pp |

### Universes beyond join top-8

Same open J. Each universe beats **its own** fullscan-alone book (not join top-8). Yahoo OHLC (`data/prices/ohlc.parquet`) covers 2024-03-04 → 2026-08-21. Join ranked files start 2026-08-12. Fresh Finviz J starts 2026-08-14.

| universe | clock | holdout slice | avoid n | avoid vs | ghost | filter J≤−1 vs | family |
|---|---|---|---:|---:|---|---:|---|
| `join_full` | Finviz Open J | prove | 24822 | +0.13 pp | PASS/PASS/PASS | +0.18 pp | **DEMOTE** |
| `join_top15` | Finviz Open J | prove | 53 | -0.01 pp | PASS/FAIL/FAIL | -0.06 pp | **CONDITIONAL** |
| `membership` | Finviz Open J | prove | 24822 | +0.13 pp | PASS/PASS/PASS | +0.18 pp | **DEMOTE** |
| `membership_liq` | Finviz Open J | prove | 4591 | +0.00 pp | PASS/FAIL/PASS | +0.04 pp | **DEMOTE** |
| `book_1d_prior` | Finviz Open J | prove | 43 | +0.54 pp | FAIL/FAIL/FAIL | +0.63 pp | **DEMOTE** |
| `book_3d_prior` | Finviz Open J | prove | 49 | +0.14 pp | PASS/FAIL/FAIL | -0.08 pp | **DEMOTE** |
| `ohlc_all` | Yahoo OHLC Open J | pre813 | 771148 | +0.64 pp | FAIL/FAIL/FAIL | +1.70 pp | **DEMOTE** |
| `ohlc_liq` | Yahoo OHLC Open J | pre813 | 174214 | +0.02 pp | PASS/FAIL/PASS | +0.06 pp | **DEMOTE** |
| `mem_20260426` | Yahoo OHLC Open J | pre813 | 750330 | +0.66 pp | FAIL/FAIL/FAIL | +1.75 pp | **DEMOTE** |
| `join_top8` | Finviz Open J | prove | 64 | +0.04 pp | PASS/FAIL/FAIL | +0.15 pp | **CONDITIONAL** |
| `join_top80` | Finviz Open J | prove | 640 | +0.12 pp | PASS/PASS/PASS | +0.00 pp | **DEMOTE** |

Expanded prove: the J overlay does **not** hold as a general rule. Yahoo liquid names (prior-session volume ≥ 1M, 2024–2026) are about flat (+2 to +5 bp, ghost month fail). The all-name Yahoo tape’s large mean is a microcap lottery (ghost FAIL). Join-full / membership on the Finviz window is +5 to +13 bp (under 20 bp). membership_liq is flat to negative. Join top-8 stays **CONDITIONAL** (discovery only).

### What was joined

**Excel (clock gate, open-only):**

- Same-row numbers from the locked 44: **J** is the standing recipe (AH / ER / FQ / JB / JC still asserted legal, not in the KEEP pair). J = (today Open − prior *weekday session* Open) / prior Open.
- Lags (any letter from rows above): **H[t−1], I[t−1], J[t−1], G[t−1]** from weekday bars only.
- Fills A B C G J K L M O IR IS IT are legal but **not on these dumps**. Not invented.
- OUT: same-row H/I, M number, B/G/K/O numbers, D/E/F, `core_score`, H paint.

**Fullscan (standing dumps, open or earliest fair clock):**

- `data/join/YYYY-MM-DD_ranked.csv` — morning rank. Sunday files (08-30, 09-06) exist but are held out of prove.
- `data/stock_book/` **1d buy, prior date only**.
- `data/feature_asof/` morning tags when present. `ret_*` labels only.
- `data/sleeve_merge/trades.csv` — flatten_robust tickets (overlay).

### Labels

- **Primary:** same-day Excel **H** = Finviz Change from Open, minus 0.15 pp Futubull. Join / stock-book 1d clock.
- **Secondary:** flatten ticket `ret_pct` (already fee-native; io 3d / mover 1d). feature_asof `ret_1d` when present.

### Window / dumps

- Discovery (in-sample peek): weekday sessions 2026-08-14 → 2026-08-25.
- Prove (time holdout): weekday sessions 2026-08-26 → 2026-09-07 (08-26 has join, no Finviz H — skipped).
- Published 18-day tape included Sundays; session-clean pooled tape is **16** weekdays (2026-08-14 → 2026-09-07).
- Panel name-days with H (weekday): **117214**.
- Finviz history dates: **22** (2026-04-26 → 2026-09-07); Sat/Sun dumps are not used as prior Open.
- Recipes were pre-specified. They were not re-searched on prove.

### Ship bar

Beat same-window fullscan-alone (join top-8) by ≥20 bp after Futubull 15 bp, ghost (name/month/day), leak-free. KEEP holds only if the **prove** window clears that bar. Pooled leftover that still includes discovery is not a holdout.

### Discovery-pool recipes vs join top-8 (session-clean, not a holdout)

| recipe | kind | H | vs fullscan | win | n | ghost | tapes | asof 1d | verdict | why |
|---|---|---:|---:|---:|---:|---|---|---:|---|---|
| `avoid_J_ge1` | avoid | +0.28% | +0.23 pp | 45.3% | 128 | PASS/PASS/FAIL | ↑+0.16%/↓+0.36% | +1.20% (n=29) | **KILL** | ghost:lottery_day |
| `avoid_J_ge0` | avoid | +0.43% | +0.38 pp | 56.2% | 128 | PASS/PASS/PASS | ↑+0.31%/↓+0.57% | +1.56% (n=33) | **KEEP** | — |
| `avoid_JB` | avoid | +0.05% | +0.00 pp | 43.8% | 128 | PASS/FAIL/PASS | ↑+0.30%/↓-0.03% | +0.47% (n=32) | **KILL** | no_edge_vs_fullscan,ghost:month_split,spy_tape |
| `avoid_FQ` | avoid | +0.01% | -0.04 pp | 43.8% | 128 | PASS/FAIL/PASS | ↑+0.12%/↓+0.02% | -0.15% (n=31) | **KILL** | no_edge_vs_fullscan,ghost:month_split |
| `avoid_ER_p1` | avoid | +0.14% | +0.09 pp | 45.3% | 128 | PASS/FAIL/PASS | ↑+0.31%/↓+0.13% | +0.55% (n=32) | **KILL** | no_edge_vs_fullscan,ghost:month_split |
| `avoid_incomplete` | control | +0.34% | +0.29 pp | 53.3% | 120 | PASS/PASS/PASS | ↑+0.46%/↓+0.01% | +1.08% (n=27) | **KEEP** | — |
| `avoid_incomplete_or_Jge1` | control | +0.32% | +0.27 pp | 50.0% | 120 | PASS/PASS/PASS | ↑+0.42%/↓+0.29% | -0.07% (n=25) | **KILL** | no_edge_vs_incomplete_control |
| `elev_cap2_J_le-1` | elevate | +0.30% | +0.26 pp | 51.6% | 128 | PASS/PASS/PASS | ↑+0.46%/↓+0.17% | +0.90% (n=31) | **KEEP** | — |
| `elev_cap2_J_lt0` | elevate | +0.22% | +0.17 pp | 47.7% | 128 | PASS/PASS/PASS | ↑+0.30%/↓+0.15% | +0.98% (n=31) | **KILL** | no_edge_vs_fullscan |
| `elev_cap2_ER_m1` | elevate | +0.11% | +0.06 pp | 47.2% | 125 | PASS/FAIL/PASS | ↑+0.44%/↓-0.02% | +0.54% (n=30) | **KILL** | no_edge_vs_fullscan,ghost:month_split,spy_tape |
| `elev_cap2_AH_ge1` | elevate | -0.04% | -0.09 pp | 47.2% | 125 | PASS/FAIL/PASS | ↑+0.42%/↓-0.21% | +0.30% (n=30) | **KILL** | hold_sign,no_edge_vs_fullscan,ghost:month_split,spy_tape |
| `replace_J_lt0` | replace | +0.43% | +0.38 pp | 56.2% | 128 | PASS/PASS/PASS | ↑+0.31%/↓+0.57% | +1.56% (n=33) | **KEEP** | — |
| `top8_and_J_lt0` | intersect | +0.32% | +0.27 pp | 43.1% | 51 | FAIL/FAIL/FAIL | ↑+0.05%/↓+0.57% | +1.88% (n=10) | **KILL** | ghost:name_win_share,month_split,lottery_day |
| `top8_and_J_le-1` | intersect | +0.34% | +0.30 pp | 42.4% | 33 | FAIL/PASS/FAIL | ↑-0.16%/↓+0.70% | +2.64% (n=6) | **KILL** | ghost:name_win_share,lottery_day,spy_tape |
| `top8_and_ER_m1` | intersect | +1.17% | +1.12 pp | 50.0% | 8 | FAIL/PASS/FAIL | thin | -4.05% (n=2) | **KILL** | ghost:name_win_share,lottery_day |

### What this does not do

- Does not wire live `flatten_robust` / `LIVE_POLICY` / `join_rules.json`.
- Does not use same-row H/I, H paint, or M’s number.
- Does not treat afternoon stock-book prints as 09:30 features.
- Does not score Sunday join dumps as a 1d session.
- Does not claim CE/CD (need 43 sessions of High/Low).
- Does not re-open shade hex / light+O (fills not in these dumps).

Gate: `OPEN_SAME_ROW_LABELS + CLOCK_MAP`. Fills open: A, B, C, G, J, K, L, M, O, IR, IS, IT. Live frozen.

Tip `77d4973e` · family **CONDITIONAL**.

Research only.
