# JOIN Excel open-gate × fullscan — prove (post-8-13 KEEP)

_Generated 2026-09-08 · live `flatten_robust` frozen · research only · no live push._

## Plain English

J clock leak **PASS**. Prove (weekday 2026-08-26→2026-09-07) does **not** re-clear the ship bar. `avoid_J_ge0` n=64 H +0.22% (+0.04 pp vs same-window top-8, ghost PASS/FAIL/FAIL). `elev_cap2_J_le-1` n=64 H +0.33% (+0.15 pp, ghost PASS/FAIL/FAIL). Discovery half still prints (`avoid_J_ge0` +0.72 pp n=64) — that is the peek, not prove. Pooled weekday leftover is `avoid_J_ge0` +0.38 pp n=128 / `elev_cap2_J_le-1` +0.26 pp n=128 (includes discovery; not a holdout). Wider book (top-80) prove `avoid_J_ge0` +0.12 pp n=640. Join dumps do not add sessions before 8-13 with a fresh J (08-13 prior Open is 04-26). Family is **CONDITIONAL**: not KEEP holds, not a full KILL of the discovery print. Live flatten_robust stays frozen. Do not wire. Expanded prove: the J overlay does **not** hold as a general rule. Yahoo liquid names (prior-session volume ≥ 1M, 2024–2026) are about flat (+2 to +5 bp, ghost month fail). The all-name Yahoo tape’s large mean is a microcap lottery (ghost FAIL). Join-full / membership on the Finviz window is +5 to +13 bp (under 20 bp). membership_liq is flat to negative. Join top-8 stays **CONDITIONAL** (discovery only). Dashboard sleeves (open J only): 0 KEEP / 68 KILL / 83 CONDITIONAL / 88 null of 239 scored rows. Question is after-fee H vs that sleeve alone. Live flatten_robust stays frozen. flatten_robust: **KILL** — pooled avoid -0.59 pp n=11; elev +0.19 pp n=23. live tickets; io often 16:00 / 3d. Blanket J-avoid fights mover gap-up. flatten_h5: **KILL** — pooled avoid -1.26 pp n=10; elev -0.63 pp n=19. factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for context, overlay is on fills (cash book) flatten_live_h5: **null** — pooled avoid -2.09 pp n=2; elev -1.61 pp n=9. factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for context, overlay is on fills (cash book) green_pile_prior: **KILL** — prove avoid +0.20 pp n=50; elev +0.14 pp n=61. prior-day green live_buy with in_pile=true (PIT). green_book_prior: **KILL** — prove avoid -0.39 pp n=61; elev -0.37 pp n=64. prior-day green.json live_buy (PIT). Afternoon stamp is not a same-day open feature. weighted_book_1d_prior: **CONDITIONAL** — prove avoid +0.49 pp n=37; elev +0.06 pp n=64. prior-day stock_book 1d buy (weighted). Same clock as book_1d_prior universe. unweighted_book_prior: **KILL** — prove avoid -0.39 pp n=61; elev -0.37 pp n=64. prior-day unweighted.json live_buy (PIT).

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

### Dashboard sleeves (open J vs sleeve-alone)

Same leak bar: open J only. Each sleeve is scored against **its own** morning picks after Futubull 15 bp — not against join top-8. List books (green / weighted / unweighted) refill from the ranked leftover. Ticket books (flatten fills, factor-mine BUYs, paper) filter the names they actually took; elev is drop ≤2 J≥0 / day. Shorts and excel/`strategies/` are null. Live stays frozen.

Dashboard sleeves (open J only): 0 KEEP / 68 KILL / 83 CONDITIONAL / 88 null of 239 scored rows. Question is after-fee H vs that sleeve alone. Live flatten_robust stays frozen. flatten_robust: **KILL** — pooled avoid -0.59 pp n=11; elev +0.19 pp n=23. live tickets; io often 16:00 / 3d. Blanket J-avoid fights mover gap-up. flatten_h5: **KILL** — pooled avoid -1.26 pp n=10; elev -0.63 pp n=19. factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for context, overlay is on fills (cash book) flatten_live_h5: **null** — pooled avoid -2.09 pp n=2; elev -1.61 pp n=9. factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for context, overlay is on fills (cash book) green_pile_prior: **KILL** — prove avoid +0.20 pp n=50; elev +0.14 pp n=61. prior-day green live_buy with in_pile=true (PIT). green_book_prior: **KILL** — prove avoid -0.39 pp n=61; elev -0.37 pp n=64. prior-day green.json live_buy (PIT). Afternoon stamp is not a same-day open feature. weighted_book_1d_prior: **CONDITIONAL** — prove avoid +0.49 pp n=37; elev +0.06 pp n=64. prior-day stock_book 1d buy (weighted). Same clock as book_1d_prior universe. unweighted_book_prior: **KILL** — prove avoid -0.39 pp n=61; elev -0.37 pp n=64. prior-day unweighted.json live_buy (PIT).

#### Featured

| sleeve | family | verdict | n | avoid vs sleeve | elev vs sleeve | ghost | note |
|---|---|---|---:|---:|---:|---|---|
| `flatten_robust` | sleeve merge | **KILL** | 11 | -0.59 pp | +0.19 pp | FAIL/PASS/FAIL | live tickets; io often 16:00 / 3d. Blanket J-avoid fights mover gap-up. |
| `flatten_h5` | factor mine | **KILL** | 10 | -1.26 pp | -0.63 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h5` | factor mine | **null** | 2 | -2.09 pp | -1.61 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `green_book_prior` | stock book | **KILL** | 61 | -0.39 pp | -0.37 pp | PASS/PASS/PASS | prior-day green.json live_buy (PIT). Afternoon stamp is not a same-day open feat |
| `green_pile_prior` | stock book | **KILL** | 50 | +0.20 pp | +0.14 pp | PASS/FAIL/FAIL | prior-day green live_buy with in_pile=true (PIT). |
| `weighted_book_1d_prior` | stock book | **CONDITIONAL** | 37 | +0.49 pp | +0.06 pp | FAIL/FAIL/FAIL | prior-day stock_book 1d buy (weighted). Same clock as book_1d_prior universe. |
| `unweighted_book_prior` | stock book | **KILL** | 61 | -0.39 pp | -0.37 pp | PASS/PASS/PASS | prior-day unweighted.json live_buy (PIT). |

#### All dashboard / STRATEGY_BOARD sleeves

| sleeve | family | verdict | n | avoid vs sleeve | elev vs sleeve | ghost | note |
|---|---|---|---:|---:|---:|---|---|
| `flatten_robust` | sleeve merge | **KILL** | 11 | -0.59 pp | +0.19 pp | FAIL/PASS/FAIL | live tickets; io often 16:00 / 3d. Blanket J-avoid fights mover gap-up. |
| `coil_h3_exit_alarm` | factor mine | **CONDITIONAL** | 4 | +5.43 pp | +0.77 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `flatten_h1` | factor mine | **KILL** | 15 | -1.13 pp | -0.06 pp | PASS/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h3` | factor mine | **KILL** | 13 | -1.51 pp | -0.90 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h3_cut` | factor mine | **KILL** | 13 | -1.51 pp | -0.90 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h3_half` | factor mine | **KILL** | 14 | -1.19 pp | -0.10 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h3_rankw` | factor mine | **KILL** | 12 | -1.44 pp | -0.84 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h3_sboost` | factor mine | **KILL** | 13 | -1.53 pp | -0.92 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h3_sizeup` | factor mine | **KILL** | 13 | -1.51 pp | -0.90 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h3_time` | factor mine | **KILL** | 14 | -1.45 pp | -0.83 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h3_topheavy` | factor mine | **KILL** | 13 | -0.92 pp | -0.06 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h3_trail` | factor mine | **KILL** | 13 | -1.51 pp | -0.90 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h5` | factor mine | **KILL** | 10 | -1.26 pp | -0.63 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h5_cut` | factor mine | **KILL** | 10 | -1.26 pp | -0.63 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h5_half` | factor mine | **KILL** | 13 | -1.19 pp | -0.09 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h5_rankw` | factor mine | **KILL** | 9 | -0.85 pp | -0.02 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h5_sboost` | factor mine | **KILL** | 10 | -1.28 pp | -0.64 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h5_sizeup` | factor mine | **KILL** | 10 | -1.26 pp | -0.63 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h5_time` | factor mine | **KILL** | 10 | -1.26 pp | -0.63 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h5_topheavy` | factor mine | **KILL** | 11 | -1.57 pp | -0.82 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h5_trail` | factor mine | **KILL** | 10 | -1.26 pp | -0.63 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h1` | factor mine | **null** | 2 | -2.01 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h1_cut` | factor mine | **null** | 2 | -2.01 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h1_half` | factor mine | **null** | 2 | -2.01 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h1_rankw` | factor mine | **null** | 2 | -2.01 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h1_sboost` | factor mine | **null** | 2 | -2.01 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h1_sizeup` | factor mine | **null** | 2 | -2.01 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h1_time` | factor mine | **null** | 2 | -2.01 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h1_topheavy` | factor mine | **null** | 2 | -2.01 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h1_trail` | factor mine | **null** | 2 | -2.01 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h3` | factor mine | **null** | 2 | -2.09 pp | -1.61 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h5` | factor mine | **null** | 2 | -2.09 pp | -1.61 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_vol_g_h3` | factor mine | **null** | 4 | -1.62 pp | -0.86 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `ohlc_hot_coil_h1` | factor mine | **null** | 2 | +0.08 pp | -0.47 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `ohlc_hot_h1` | factor mine | **KILL** | 8 | +0.94 pp | +0.16 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `ohlc_hot_h3` | factor mine | **KILL** | 5 | +0.31 pp | -0.08 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `ohlc_hot_h5` | factor mine | **KILL** | 4 | +0.58 pp | -0.43 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `probable_h1` | factor mine | **CONDITIONAL** | 2 | +17.66 pp | +1.25 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `probable_h3` | factor mine | **CONDITIONAL** | 6 | +6.14 pp | +0.94 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `probable_h5` | factor mine | **CONDITIONAL** | 2 | +17.87 pp | +1.33 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `probable_probable_ok_h1` | factor mine | **CONDITIONAL** | 2 | +17.66 pp | +1.05 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `probable_probable_ok_h3` | factor mine | **CONDITIONAL** | 2 | +17.75 pp | +1.62 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `short_alarm_h1` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `short_alarm_h3` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `short_extended_h1` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `short_extended_h3` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `short_last_red_h1` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `short_last_red_h3` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `short_news_r_h1` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `short_news_r_h3` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `short_r_down_h1` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `short_r_down_h3` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `union_ab_g_h1` | factor mine | **CONDITIONAL** | 4 | +0.60 pp | +0.20 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_ab_g_h3` | factor mine | **KILL** | 3 | +0.57 pp | +0.02 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_blue_coil_h1` | factor mine | **CONDITIONAL** | 3 | +3.24 pp | +1.22 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_blue_coil_h3` | factor mine | **CONDITIONAL** | 2 | +2.44 pp | +1.47 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_blue_h1` | factor mine | **CONDITIONAL** | 3 | +2.64 pp | +1.33 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_blue_h3` | factor mine | **CONDITIONAL** | 2 | +1.74 pp | +1.42 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_blue_vol_h1` | factor mine | **KILL** | 2 | +3.38 pp | +0.81 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_blue_vol_h3` | factor mine | **KILL** | 2 | +3.77 pp | +0.75 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_break10_h1` | factor mine | **CONDITIONAL** | 6 | +1.08 pp | +0.57 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_break10_h3` | factor mine | **CONDITIONAL** | 10 | -0.03 pp | +0.61 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_candle_h1` | factor mine | **CONDITIONAL** | 6 | +2.70 pp | +0.91 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_candle_h3` | factor mine | **CONDITIONAL** | 5 | +2.10 pp | +1.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_candle_score_h1` | factor mine | **CONDITIONAL** | 10 | +0.80 pp | +0.55 pp | PASS/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_candle_score_h3` | factor mine | **CONDITIONAL** | 9 | +0.81 pp | +0.65 pp | PASS/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_catal_present_h1` | factor mine | **null** | 2 | -0.57 pp | -0.57 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_catal_present_h3` | factor mine | **null** | 2 | -1.11 pp | -1.11 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_coil_green_h1` | factor mine | **CONDITIONAL** | 12 | +1.82 pp | +0.83 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_coil_green_h3` | factor mine | **CONDITIONAL** | 11 | +0.77 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_coil_off_h1` | factor mine | **KILL** | 9 | +2.21 pp | +0.16 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_coil_off_h3` | factor mine | **CONDITIONAL** | 8 | +2.95 pp | +0.37 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_coil_off_h5` | factor mine | **CONDITIONAL** | 8 | +2.66 pp | +0.26 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_cond_h1` | factor mine | **CONDITIONAL** | 2 | +5.41 pp | +0.64 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_cond_h3` | factor mine | **CONDITIONAL** | 1 | +4.63 pp | +0.88 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_cond_n4_h3` | factor mine | **null** | 6 | -1.30 pp | +0.46 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_e_fresh_h1` | factor mine | **CONDITIONAL** | 10 | +0.57 pp | +0.02 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_e_fresh_h3` | factor mine | **KILL** | 7 | +0.65 pp | -0.17 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_e_green_h1` | factor mine | **KILL** | 11 | -0.31 pp | -0.06 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_e_green_h3` | factor mine | **CONDITIONAL** | 9 | -0.27 pp | -0.08 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_earn_react_h1` | factor mine | **KILL** | 10 | +0.57 pp | +0.02 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_earn_react_h3` | factor mine | **KILL** | 9 | +0.49 pp | -0.03 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h1` | factor mine | **CONDITIONAL** | 5 | +6.20 pp | +1.01 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h1_cut` | factor mine | **CONDITIONAL** | 5 | +6.20 pp | +1.01 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h1_half` | factor mine | **CONDITIONAL** | 5 | +6.13 pp | +1.12 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h1_rankw` | factor mine | **CONDITIONAL** | 5 | +6.13 pp | +1.12 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h1_sboost` | factor mine | **CONDITIONAL** | 5 | +6.20 pp | +1.01 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h1_sizeup` | factor mine | **CONDITIONAL** | 5 | +6.20 pp | +1.01 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h1_time` | factor mine | **CONDITIONAL** | 6 | +6.11 pp | +1.01 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h1_topheavy` | factor mine | **CONDITIONAL** | 5 | +6.13 pp | +1.12 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h1_trail` | factor mine | **CONDITIONAL** | 5 | +6.20 pp | +1.01 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3` | factor mine | **KILL** | 14 | +0.70 pp | -0.12 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_cut` | factor mine | **KILL** | 14 | +0.70 pp | -0.12 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_exit_alarm` | factor mine | **KILL** | 14 | +1.08 pp | -0.12 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_exit_news_r` | factor mine | **KILL** | 13 | +1.04 pp | +0.04 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_exit_red` | factor mine | **CONDITIONAL** | 7 | +4.73 pp | +1.35 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_half` | factor mine | **CONDITIONAL** | 5 | +6.13 pp | +1.12 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_rankw` | factor mine | **KILL** | 13 | +0.72 pp | -0.19 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_sboost` | factor mine | **KILL** | 14 | +0.78 pp | -0.08 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_sizeup` | factor mine | **KILL** | 14 | +0.70 pp | -0.12 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_time` | factor mine | **CONDITIONAL** | 5 | +5.36 pp | +1.24 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_topheavy` | factor mine | **CONDITIONAL** | 14 | +0.92 pp | +0.53 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_trail` | factor mine | **KILL** | 14 | +0.70 pp | -0.12 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5` | factor mine | **CONDITIONAL** | 4 | +8.85 pp | +0.98 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5_cut` | factor mine | **CONDITIONAL** | 4 | +8.85 pp | +0.98 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5_exit_alarm` | factor mine | **KILL** | 4 | +8.48 pp | +1.33 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5_half` | factor mine | **CONDITIONAL** | 4 | +8.51 pp | +1.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5_rankw` | factor mine | **CONDITIONAL** | 4 | +8.78 pp | +1.08 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5_sboost` | factor mine | **CONDITIONAL** | 4 | +8.85 pp | +0.98 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5_sizeup` | factor mine | **CONDITIONAL** | 4 | +8.85 pp | +0.98 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5_time` | factor mine | **CONDITIONAL** | 5 | +6.38 pp | +0.92 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5_topheavy` | factor mine | **KILL** | 4 | +8.48 pp | +1.33 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5_trail` | factor mine | **CONDITIONAL** | 4 | +8.85 pp | +0.98 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_hot_n12_h1` | factor mine | **CONDITIONAL** | 17 | +1.30 pp | +0.58 pp | PASS/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_hot_n4_h1` | factor mine | **KILL** | 11 | +1.46 pp | +0.83 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_hot_score_h1` | factor mine | **CONDITIONAL** | 11 | +1.75 pp | +0.85 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_hot_score_h3` | factor mine | **CONDITIONAL** | 10 | +1.91 pp | +0.87 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_join_g_h1` | factor mine | **KILL** | 4 | +0.90 pp | +0.17 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_join_g_h3` | factor mine | **KILL** | 3 | +1.14 pp | +0.38 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_join_present_h1` | factor mine | **CONDITIONAL** | 5 | +6.20 pp | +1.01 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_join_present_h3` | factor mine | **KILL** | 13 | +1.04 pp | +0.04 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_join_vol_green_h1` | factor mine | **KILL** | 4 | +1.02 pp | +0.60 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_join_vol_green_h3` | factor mine | **KILL** | 5 | +1.07 pp | -0.10 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_last_green_h1` | factor mine | **CONDITIONAL** | 7 | +4.86 pp | +1.36 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_last_green_h3` | factor mine | **CONDITIONAL** | 7 | +4.73 pp | +1.35 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_last_green_h5` | factor mine | **CONDITIONAL** | 6 | +6.41 pp | +1.40 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_last_red_h1` | factor mine | **CONDITIONAL** | 3 | -0.40 pp | +0.27 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_last_red_h3` | factor mine | **KILL** | 1 | +0.85 pp | +0.50 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_news_g_h1` | factor mine | **CONDITIONAL** | 5 | +1.07 pp | +0.23 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_news_g_h3` | factor mine | **CONDITIONAL** | 10 | +1.76 pp | +0.58 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_news_g_h5` | factor mine | **CONDITIONAL** | 9 | +1.74 pp | +0.32 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_news_missing_h1` | factor mine | **null** | 2 | +3.76 pp | +0.55 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_news_missing_h3` | factor mine | **null** | 2 | +3.76 pp | +0.55 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_news_present_h1` | factor mine | **CONDITIONAL** | 7 | +3.29 pp | +0.85 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_news_present_h3` | factor mine | **CONDITIONAL** | 4 | +7.14 pp | +1.04 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_news_vol_h1` | factor mine | **KILL** | 8 | +2.33 pp | +0.03 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_news_vol_h3` | factor mine | **KILL** | 7 | +1.98 pp | +0.28 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_r_up_h1` | factor mine | **null** | 0 | — | — | — | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_r_up_h3` | factor mine | **null** | 0 | — | — | — | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_ret_5_h1` | factor mine | **CONDITIONAL** | 12 | +0.39 pp | +1.02 pp | PASS/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_ret_5_h3` | factor mine | **CONDITIONAL** | 12 | +0.36 pp | +1.14 pp | PASS/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_vol_ab_h1` | factor mine | **KILL** | 3 | -0.89 pp | -0.42 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_vol_ab_h3` | factor mine | **KILL** | 3 | -0.57 pp | -0.59 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_vol_g_h1` | factor mine | **CONDITIONAL** | 4 | +6.72 pp | +1.13 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_vol_g_h3` | factor mine | **KILL** | 4 | +7.24 pp | +1.15 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_vol_g_h5` | factor mine | **CONDITIONAL** | 3 | +8.31 pp | +1.19 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_vol_green_h1` | factor mine | **CONDITIONAL** | 4 | +8.17 pp | +0.67 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_vol_green_h3` | factor mine | **CONDITIONAL** | 4 | +8.47 pp | +0.64 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_vol_missing_h1` | factor mine | **null** | 0 | — | — | — | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_vol_missing_h3` | factor mine | **null** | 0 | — | — | — | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_w_hot_candle_h1` | factor mine | **CONDITIONAL** | 11 | +2.17 pp | +0.83 pp | PASS/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_w_hot_candle_h3` | factor mine | **CONDITIONAL** | 11 | +1.57 pp | +0.12 pp | PASS/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_w_hot_cond_h1` | factor mine | **CONDITIONAL** | 7 | +2.29 pp | +0.93 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_w_hot_cond_h3` | factor mine | **CONDITIONAL** | 5 | +3.01 pp | +1.10 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_white_coil_h1` | factor mine | **CONDITIONAL** | 2 | +2.67 pp | +0.73 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_white_coil_h3` | factor mine | **CONDITIONAL** | 2 | +2.61 pp | +0.81 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_white_h1` | factor mine | **CONDITIONAL** | 1 | +3.46 pp | +1.05 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_white_h3` | factor mine | **KILL** | 1 | +3.57 pp | +1.16 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_white_h5` | factor mine | **CONDITIONAL** | 0 | — | +0.81 pp | FAIL/FAIL/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `yday_gainer_h1` | factor mine | **CONDITIONAL** | 2 | +17.66 pp | +1.25 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `yday_gainer_h3` | factor mine | **CONDITIONAL** | 2 | +17.97 pp | +1.90 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `yday_gainer_h5` | factor mine | **CONDITIONAL** | 2 | +18.04 pp | +1.19 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `green_book_prior` | stock book | **KILL** | 61 | -0.39 pp | -0.37 pp | PASS/PASS/PASS | prior-day green.json live_buy (PIT). Afternoon stamp is not a same-day open feat |
| `green_pile_prior` | stock book | **KILL** | 50 | +0.20 pp | +0.14 pp | PASS/FAIL/FAIL | prior-day green live_buy with in_pile=true (PIT). |
| `weighted_book_1d_prior` | stock book | **CONDITIONAL** | 37 | +0.49 pp | +0.06 pp | FAIL/FAIL/FAIL | prior-day stock_book 1d buy (weighted). Same clock as book_1d_prior universe. |
| `unweighted_book_prior` | stock book | **KILL** | 61 | -0.39 pp | -0.37 pp | PASS/PASS/PASS | prior-day unweighted.json live_buy (PIT). |
| `1d_size` | .io paper | **CONDITIONAL** | 8 | +1.95 pp | +0.72 pp | FAIL/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `1d_top` | .io paper | **CONDITIONAL** | 12 | +0.81 pp | +0.17 pp | FAIL/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `1m_size` | .io paper | **null** | 4 | -0.84 pp | -0.44 pp | FAIL/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `1m_top` | .io paper | **null** | 9 | -0.17 pp | -0.46 pp | FAIL/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `1w_size` | .io paper | **KILL** | 6 | +2.05 pp | -0.09 pp | FAIL/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `1w_top` | .io paper | **KILL** | 5 | +0.70 pp | -0.14 pp | FAIL/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `2w_size` | .io paper | **CONDITIONAL** | 10 | +1.45 pp | +0.23 pp | FAIL/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `2w_top` | .io paper | **null** | 13 | -0.29 pp | -0.25 pp | FAIL/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `3d_size` | .io paper | **CONDITIONAL** | 9 | +2.31 pp | -0.07 pp | PASS/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `3d_top` | .io paper | **CONDITIONAL** | 9 | +1.32 pp | +0.05 pp | FAIL/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `sleeve_combine_bt` | sleeve combine | **KILL** | 7 | +1.62 pp | -0.02 pp | FAIL/PASS/FAIL | on-disk bt_trades.csv (io often 16:00). Other catalog combines share these fills |
| `sleeve_combine_io` | sleeve combine | **KILL** | 7 | +1.62 pp | -0.02 pp | FAIL/PASS/FAIL | bt_trades source=io |
| `sleeve_combine_mover` | sleeve combine | **null** | 0 | — | — | — | bt_trades source=mover |
| `book_paper_1w` | book paper | **CONDITIONAL** | 5 | +1.12 pp | +0.24 pp | FAIL/PASS/FAIL | book paper: catalog says close entry / 1w hold |
| `mover_paper_live` | mover stitch | **KILL** | 11 | +0.25 pp | +0.06 pp | FAIL/PASS/FAIL | mover paper 09:30 fills |
| `flatten_rotate` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_switch_recycle` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_hard_red` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_switch_full` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_carry_book` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_cash_mover` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_blank_cash` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_skip_blank_io` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_switch_70` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_rich` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_overlap` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_overlap_55` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_switch_60` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_switch` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Combine 3d io_boost` | sleeve combine | **KILL** | 7 | +1.62 pp | -0.02 pp | FAIL/PASS/FAIL | filtered from shared bt_trades.csv |
| `Flatten core50_switch` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Flatten concentrated_switch` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Combine 3d overlay_boost` | sleeve combine | **KILL** | 7 | +1.62 pp | -0.02 pp | FAIL/PASS/FAIL | filtered from shared bt_trades.csv |
| `Combine 3d io_only` | sleeve combine | **KILL** | 7 | +1.62 pp | -0.02 pp | FAIL/PASS/FAIL | filtered from shared bt_trades.csv |
| `Flatten core_switch` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Flatten switch_70` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Flatten switch_80` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_3d` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Combine 3d overlay` | sleeve combine | **KILL** | 7 | +1.62 pp | -0.02 pp | FAIL/PASS/FAIL | filtered from shared bt_trades.csv |
| `Flatten io_3d_switch` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Flatten switch_no_short` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Combine 1w overlay_boost` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 1w io_boost` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Flatten mover_heavy` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Combine 1d overlay_boost` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 1w io_only` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 1w overlay` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Flatten hard_red_shorts` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Flatten switch_80_overlap` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Combine 1d overlay` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 3d dual` | sleeve combine | **KILL** | 7 | +1.62 pp | -0.02 pp | FAIL/PASS/FAIL | filtered from shared bt_trades.csv |
| `Flatten switch_90_overlap` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Combine 1d io_boost` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 2w io_only` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 1d io_only` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 1w dual` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 1w mover_only` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 1w combine` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 3d mover_only` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 1d dual` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 3d combine` | sleeve combine | **KILL** | 7 | +1.62 pp | -0.02 pp | FAIL/PASS/FAIL | filtered from shared bt_trades.csv |
| `1d mover × soft-red 1d .io` | mover stitch | **null** | 0 | — | — | — | no separate per-trade dump (see mover_paper_live) |
| `Combine 1d mover_only` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 1d combine` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Empty BUY list + skip → live 2w_size` | mover stitch | **null** | 0 | — | — | — | no separate per-trade dump (see mover_paper_live) |
| `Skip-day → live 2w_size` | mover stitch | **null** | 0 | — | — | — | no separate per-trade dump (see mover_paper_live) |
| `Mover paper v2 (old sim)` | mover | **null** | 0 | — | — | — | no separate per-trade dump (see mover_paper_live) |
| `Excel L3_long_green_hold2_midcap` | excel | **null** | 0 | — | — | — | excel / strategies/ frozen — not scored |
| `Excel live ledger (all cards)` | excel | **null** | 0 | — | — | — | excel / strategies/ frozen — not scored |
| `Excel L1_long_green_tp8_lowvol` | excel | **null** | 0 | — | — | — | excel / strategies/ frozen — not scored |
| `Excel L2_long_green_tp3_lowvol` | excel | **null** | 0 | — | — | — | excel / strategies/ frozen — not scored |
| `.io SPY (benchmark)` | .io paper | **null** | 0 | — | — | — | benchmark, not a name-picking sleeve |
| `Excel L5_long_green_hold2_midhibeta` | excel | **null** | 0 | — | — | — | excel / strategies/ frozen — not scored |

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
- `03_scoreboard/factor_mine/*.md` — 09:30 BUY fills (flatten_h5 / live / unions).
- `data/stock_book/*_green.json` / `*_unweighted.json` / 1d buy — prior-day PIT.
- `data/paper/roundtrips.csv`, `data/sleeve_combine/bt_trades.csv`, book/mover paper fills.

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

Tip `d57799a6` · family **CONDITIONAL**.

Research only.
