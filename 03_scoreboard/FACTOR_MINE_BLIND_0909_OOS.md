# Factor mine blind 9/9 — fee-aware OOS (Excel lane)

status=DONE verdict=**FAIL** 0 KEEP cutoff=2026-09-09 OOS=2026-09-10→2026-09-18 FEE_RT=0.0015 KEEP=0 FAIL=31

Research only. Live `flatten_robust` / `dashboard/factor-mine/` / `03_scoreboard/factor_mine.json` were not written. Recipe names are the #286 IS freeze — OOS dates did not re-select.

## KEEP bar

Cyrus OOS KEEP (plain): **IS Cyrus featured** (Starts YES + Book% on 8/13–9/9) **and** continued OOS Book% > 0 **and** OOS start-day YES ≥85% (7 sessions → ≥6/7). Same start rule as IS (≥17/19 when n≥19).

**Win% > 55% is not enough by itself.** After-fee H = same-session open→close minus 15 bp Futubull (`FEE_RT=0.0015`). Shorts pay the 15 bp (they do not collect it). Cash Book% already uses the Futubull order-fee schedule (`00_grounding/futubull_fees.json`). n≥30 is the IS trade-count bar; the 7-session window cannot meet it and does not KEEP.

OOS **continued** Book% is copied from #286 `holdout.json` (9/9 cash book walked forward). OOS **start-day YES** wakes $10k empty lots on each session in 2026-09-10–2026-09-18. Start 9/10 is the fresh $10k path.

## Headline

FAIL. No frozen ≤9/9 Cyrus name keeps positive continued Book% **and** enough OOS start-day wins (2026-09-10–2026-09-18).

Taskforce continued-Book% survivors (not a KEEP bar): **6** of 22 Cyrus IS names. Win%>55% traps (WR clears, Cyrus FAIL): **0**.

## Taskforce 6 — continued Book% > 0 on #286

| Strategy | Cyrus OOS | Book-only | OOS starts | Cont book% | Fresh $10k | Fresh WR | After-fee H WR | n H | Why |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| `combo_sh_5050_shared` | **FAIL** | yes | 2/7 | +1.11 | +0.43 | — | 38% | 8 | OOS starts 2/7 < Cyrus ≥85% (7 sessions → ≥6/7) |
| `combo_sh_3070_shared` | **FAIL** | yes | 2/7 | +1.62 | +1.18 | — | 38% | 8 | OOS starts 2/7 < Cyrus ≥85% (7 sessions → ≥6/7) |
| `combo_sh_7030_shared` | **FAIL** | yes | 0/7 | +0.64 | -0.03 | — | 38% | 8 | OOS starts 0/7 < Cyrus ≥85% (7 sessions → ≥6/7) |
| `combo_seh_451540_shared` | **FAIL** | yes | 2/7 | +0.66 | +0.17 | — | 40% | 15 | OOS starts 2/7 < Cyrus ≥85% (7 sessions → ≥6/7) |
| `combo_seh_601525_shared` | **FAIL** | yes | 0/7 | +0.26 | -0.25 | — | 40% | 15 | OOS starts 0/7 < Cyrus ≥85% (7 sessions → ≥6/7) |
| `combo_seh_502525_shared` | **FAIL** | yes | 0/7 | +0.27 | -0.07 | — | 44% | 16 | OOS starts 0/7 < Cyrus ≥85% (7 sessions → ≥6/7) |

### Start-day paths (Taskforce 6)

Each cell is the fee-aware $10k book that **starts** that morning (empty lots) through 9/18. YES = Book% > 0.

| Strategy | 09-10 | 09-11 | 09-14 | 09-15 | 09-16 | 09-17 | 09-18 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `combo_sh_5050_shared` | YES +0.43 | YES +0.43 | no +0.00 | no +0.00 | no +0.00 | no +0.00 | no +0.00 |
| `combo_sh_3070_shared` | YES +1.18 | YES +1.18 | no +0.00 | no +0.00 | no +0.00 | no +0.00 | no +0.00 |
| `combo_sh_7030_shared` | no -0.03 | no -0.03 | no +0.00 | no +0.00 | no +0.00 | no +0.00 | no +0.00 |
| `combo_seh_451540_shared` | YES +0.17 | YES +0.17 | no +0.00 | no +0.00 | no +0.00 | no +0.00 | no +0.00 |
| `combo_seh_601525_shared` | no -0.25 | no -0.25 | no +0.00 | no +0.00 | no +0.00 | no +0.00 | no +0.00 |
| `combo_seh_502525_shared` | no -0.07 | no -0.07 | no +0.00 | no +0.00 | no +0.00 | no +0.00 | no +0.00 |

## Frozen Cyrus featured — KEEP / FAIL

| Strategy | Side | IS start | IS book% | IS win% | OOS starts | Cont book% | Fresh $10k | After-fee H WR | WR-only | Verdict |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|
| `combo_sh_5050_shared` | mix | 17/19 | +33.46 | 62% | 2/7 | +1.11 | +0.43 | 38% |  | **FAIL** |
| `combo_sh_3070_shared` | mix | 17/19 | +29.81 | 60% | 2/7 | +1.62 | +1.18 | 38% |  | **FAIL** |
| `combo_sh_7030_shared` | mix | 17/19 | +28.70 | 62% | 0/7 | +0.64 | -0.03 | 38% |  | **FAIL** |
| `combo_seh_451540_shared` | mix | 17/19 | +27.38 | 50% | 2/7 | +0.66 | +0.17 | 40% |  | **FAIL** |
| `combo_seh_601525_shared` | mix | 17/19 | +27.01 | 50% | 0/7 | +0.26 | -0.25 | 40% |  | **FAIL** |
| `combo_seh_502525_shared` | mix | 17/19 | +25.80 | 50% | 0/7 | +0.27 | -0.07 | 44% |  | **FAIL** |
| `combo_form_jovogrh1snerh3_5050_shared` | mix | 17/19 | +21.25 | 57% | 0/7 | -0.77 | -1.46 | 33% |  | **FAIL** |
| `combo_sj_5050_shared` | mix | 17/19 | +21.25 | 57% | 0/7 | -0.77 | -1.46 | 33% |  | **FAIL** |
| `combo_sj_3070_shared` | mix | 17/19 | +20.50 | 57% | 0/7 | -0.86 | -1.43 | 33% |  | **FAIL** |
| `combo_sj_7030_shared` | mix | 17/19 | +16.61 | 54% | 0/7 | -0.52 | -1.21 | 33% |  | **FAIL** |
| `combo_form_nevoh1snerh3_5050_shared` | mix | 17/19 | +16.23 | 60% | 0/7 | -2.92 | -3.58 | 29% |  | **FAIL** |
| `combo_form_negh1jovogrh1snerh3_333_shared` | mix | 17/19 | +15.05 | 54% | 0/7 | -1.09 | -1.91 | 29% |  | **FAIL** |
| `combo_snj_333_shared` | mix | 17/19 | +15.05 | 54% | 0/7 | -1.09 | -1.91 | 29% |  | **FAIL** |
| `combo_form_negh1snerh3_5050_shared` | mix | 17/19 | +14.13 | 60% | 0/7 | -1.06 | -1.77 | 22% |  | **FAIL** |
| `combo_sn_5050_shared` | mix | 17/19 | +14.13 | 60% | 0/7 | -1.06 | -1.77 | 22% |  | **FAIL** |
| `combo_form_negh1snerh3_7030_shared` | mix | 17/19 | +12.88 | 58% | 0/7 | -1.41 | -1.95 | 22% |  | **FAIL** |
| `combo_sn_3070_shared` | mix | 17/19 | +12.88 | 58% | 0/7 | -1.41 | -1.95 | 22% |  | **FAIL** |
| `short_news_r_h3` | short | 17/19 | +11.88 | 57% | 0/7 | -0.04 | -0.69 | 25% |  | **FAIL** |
| `combo_form_negh1snerh3_3070_shared` | mix | 17/19 | +11.42 | 60% | 0/7 | -0.76 | -1.32 | 22% |  | **FAIL** |
| `combo_sn_7030_shared` | mix | 17/19 | +11.42 | 60% | 0/7 | -0.76 | -1.32 | 22% |  | **FAIL** |
| `combo_form_jovogrh1snerh1_5050_shared` | mix | 17/19 | +5.91 | 48% | 0/7 | -1.45 | -1.46 | 33% |  | **FAIL** |
| `combo_form_negh1snerh1_5050_shared` | mix | 17/19 | +1.79 | 53% | 0/7 | -1.75 | -1.77 | 22% |  | **FAIL** |

## Formal-bar IS KEEP that missed Cyrus featuring

These cleared WORKABLE_BAR on 9/9 (Win% / $days / start≥50%) but not Starts YES ≥17/19. OOS cannot promote them.

| Strategy | IS start | IS book% | IS win% | OOS starts | Cont book% | Fresh $10k | Verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `combo_jse_333_shared` | 16/19 | +30.34 | 60% | — | -0.78 | -1.08 | **FAIL** |
| `combo_se_5050_skip` | 14/19 | +26.07 | 62% | — | -0.17 | -0.56 | **FAIL** |
| `combo_se1_5050_shared` | 16/19 | +24.44 | 58% | — | +0.14 | -0.56 | **FAIL** |
| `combo_se_5050_shared` | 14/19 | +24.21 | 60% | — | -0.23 | -0.56 | **FAIL** |
| `combo_se_5050_weather` | 14/19 | +24.21 | 60% | — | -0.23 | -0.56 | **FAIL** |
| `combo_ser_5050_shared` | 15/19 | +22.03 | 56% | — | -0.25 | -0.59 | **FAIL** |
| `combo_se_7030_shared` | 13/19 | +21.42 | 62% | — | -0.30 | -0.73 | **FAIL** |
| `combo_e1s_7030_shared` | 16/19 | +19.92 | 57% | — | +0.34 | -0.28 | **FAIL** |

## Win% > 55% alone

Win% is not the Cyrus bar. On 9/9, `combo_sh_5050_shared` IS WR 62%, `combo_sh_3070_shared` IS WR 60%, `combo_sh_7030_shared` IS WR 62%, `combo_form_jovogrh1snerh3_5050_shared` IS WR 57%, `combo_sj_5050_shared` IS WR 57%, `combo_sj_3070_shared` IS WR 57%, `combo_form_nevoh1snerh3_5050_shared` IS WR 60%, `combo_form_negh1snerh3_5050_shared` IS WR 60%, `combo_sn_5050_shared` IS WR 60%, `combo_form_negh1snerh3_7030_shared` IS WR 58%, `combo_sn_3070_shared` IS WR 58%, `short_news_r_h3` IS WR 57%, `combo_form_negh1snerh3_3070_shared` IS WR 60%, `combo_sn_7030_shared` IS WR 60% would pass a Win%>55% screen; every one **FAIL**s OOS starts + Book%. The `combo_seh_*` mixes were Cyrus-featured with IS WR 50% — Win% alone would have dropped them on 9/9 too (starts 17/19 + Book% kept them).

No frozen name clears **OOS** after-fee H WR or fresh cash-trade WR > 55%. Best Taskforce-6 H WR is well under the 55% screen. WR is still not the KEEP bar.

## Contamination vs today's full-sample board

Live board window **2026-08-13 → 2026-09-18** (63 featured pins). That pack saw 9/10–9/18 while ranking / pinning. This OOS board does not.

- **hot4 / `union_hot_n4_h1`:** In the 9/9 auto grid, **not** Cyrus featured (IS starts 11/19, book +18.84%). Live featured pin: no. Full-sample book +31.97% starts 26/26. A 9/9 researcher would not have featured it; OOS shine cannot promote it here.
- **holdup / `union_hot_n4_holdup`:** Not in the 9/9 recipe menu (landed 2026-09-19). Live featured pin: yes. No holdup twin was invented. Not scored.
- **Post-9/9 live pins (Clock-B / overnight / macd / holdup):** `union_hot_n4_holdup`, `overnight_mega_h1`, `overnight_mega_h2`, `overnight_h1`, `overnight_mega_green_h1`, `combo_oh_5050_shared`, `combo_sh_macd_5050_shared`.
- **Taskforce 6 on the live featured pin list:** none. Live pinned the macd / holdup / overnight family instead of these 9/9 start-day mixes.

## Confirm vs #286 holdout books

Continued Book% copied from #286 `holdout.json`. Fresh $10k start (9/10) matches holdout within 0.05 pp for every replayed name.

Recipe-definition freeze: `cb7f09ae`. Side paths only.

