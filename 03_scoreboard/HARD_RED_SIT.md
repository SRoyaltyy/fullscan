# Hard-red sit experiment

**Short-only: KILL.** **Best dip-scoop X=1.0%: KILL.**

Research only. Live `flatten_robust` buys and Webull `combo_sh_macd_5050_shared` stay on full hard-red sit. This board does not wire either idea.

## Board — what would have happened

KILL hard-red short-only: thin n=11 fires (bar ≥30). After-fee win 50.0%. webull n=11 win=50%; flatten n=0 win=—. Do not change live sit on this sample.

KILL hard-red dip-scoop X=1.0: 55 fires but after-fee win 49.1% ≤ 55%. webull n=26 win=54%; flatten n=29 win=45%

On **2026-09-14** morning S was **-11.002** (hard-red). Live policy `combo_sh_macd_5050_shared` (#232) and `flatten_robust` sat every new lot.

Webull longs that sat: CMRC (open 3.510, low −0.85% — would scoop at 0.5%, last 3.535 (not 16:00)); GPRO (open 1.370, low −4.38% — would scoop at 0.5%, 1.0%, 1.5%, 2.0%, 3.0%, last 1.342 (not 16:00)); VERI (open 1.035, low −5.30% — would scoop at 0.5%, 1.0%, 1.5%, 2.0%, 3.0%, last 1.050 (not 16:00)); HUT (open 92.300, low −2.95% — would scoop at 0.5%, 1.0%, 1.5%, 2.0%, last 91.581 (not 16:00)); INDP (open 2.800, low −1.07% — would scoop at 0.5%, 1.0%, last 2.840 (not 16:00)).
Webull shorts that sat (the short kid): BKV (short at open 24.26 → last 24.25; 1-share after-fee -0.50, fee-dominated, not cash-book size); AMD (short at open 486.13 → last 487.77; 1-share after-fee -5.65, fee-dominated, not cash-book size).
Flatten/io would-haves that sat: CVE (open 33.640, low −0.51% — would scoop at 0.5%, last 33.790 (not 16:00)); BG (open 123.850, low −0.33% — no X on the grid hit, last 124.610 (not 16:00)); NVT (open 150.000, low −2.80% — would scoop at 0.5%, 1.0%, 1.5%, 2.0%, last 147.880 (not 16:00)); DK (open 77.760, low −3.07% — would scoop at 0.5%, 1.0%, 1.5%, 2.0%, 3.0%, last 78.305 (not 16:00)).

A sits all. B fires shorts at the clock-clean 09:30 open. C fires a long only if the session low reached open−X%; fill is that limit. 09-14 prices, when parquet has not landed, use Yahoo's regular-session open/high/low; the close column is the last print so far — not a 16:00 mark. Flatten's live card used last-close for DK; that is not the scoop reference. Scoop uses the official 09:30 open.

Window `2026-08-13 → 2026-09-14` (22 sessions). Hard-red mornings (S≤-3): **10** — `2026-08-18` S=-6.2, `2026-08-19` S=-7.2, `2026-08-24` S=-5.175, `2026-08-31` S=-5.85, `2026-09-01` S=-6.3, `2026-09-02` S=-3.825, `2026-09-08` S=-11.475, `2026-09-09` S=-13.95, `2026-09-10` S=-13.275, `2026-09-14` S=-11.002.

Book% on the continuous $10k combo path is **not** the KEEP bar. Scoop / short-only books look richer because those extra lots stay held into later non-red days. KEEP only grades the hard-red **fires** after Futubull fees. A fat Book% with a coin-flip hard-red win rate is still KILL.

## KEEP bar

Need **≥30 fires** where the tape can print them, **> 55% after Futubull fees**, and **both tapes** (Webull combo + flatten/io) when that tape can actually fire. Walk-forward mines X on hidden windows; a full-sample winner that dies OOS is KILL. Thin n is KILL.

After-fee caveat: every graded fire pays the Futubull US round-trip. Dip fills assume that if the official session **low** printed at or through open×(1−X%), the limit filled at that target. Daily OHLC cannot prove the print happened after 09:30, so scoop P&L is slightly optimistic. A missing 09:30 open is a skip — never Gap, last, or prior close. 09-14 may lack a 16:00 mark; those rows stay ungraded.

## Webull combo tape

Cash book is the audited `simulate_shared` ledger on `combo_sh_macd_5050_shared` (short news🔴 ∩ MACD-up + hot-4, shared 50/50, $10k, whole shares, sell first). (A) sit is the live gate. (B) lets the short kid fire on hard-red. (C) lets longs limit-buy after a clock-clean open−X%.

| Variant | Mode | Book% | Hard-red fires | After-fee win | Hard-red $ | Book win | Audit |
|---|---|---:|---:|---:|---:|---:|---|
| (A) full sit | `sit` | +37.68 | 0 | — | +0.00 | 61.2% | PASS |
| (B) short-only | `short_only` | +51.83 | 11 | 50.0% | -904.58 | 62.2% | PASS |
| (C) scoop 0.5% | `dip_scoop 0.5%` | +74.18 | 26 | 50.0% | +723.23 | 59.6% | PASS |
| (C) scoop 1% | `dip_scoop 1%` | +83.24 | 26 | 54.2% | +1387.90 | 60.7% | PASS |
| (C) scoop 1.5% | `dip_scoop 1.5%` | +88.87 | 25 | 52.2% | +1835.80 | 60.2% | PASS |
| (C) scoop 2% | `dip_scoop 2%` | +66.45 | 23 | 45.5% | +2263.94 | 58.0% | PASS |
| (C) scoop 3% | `dip_scoop 3%` | +63.30 | 20 | 47.4% | +2065.04 | 58.8% | PASS |

## Flatten / .io tape

Long book only (`flatten_robust` has no short kid). (A) sit and (B) short-only print **zero** new lots — same as live. (C) scoops the hard-red would-have .io names after open−X%, 1 share each, exit at the 3d horizon close.

| Variant | Hard-red fires | After-fee win | After-fee $ |
|---|---:|---:|---:|
| (A) full sit | 0 | — | +0.00 |
| (B) short-only (N/A — long book) | 0 | — | +0.00 |
| (C) scoop 0.5% | 34 | 32.4% | -17.09 |
| (C) scoop 1% | 29 | 44.8% | -14.15 |
| (C) scoop 1.5% | 23 | 43.5% | -4.35 |
| (C) scoop 2% | 15 | 40.0% | -3.20 |
| (C) scoop 3% | 8 | 50.0% | -1.81 |

## Walk-forward (mine X, freeze, score hidden)

4 folds — same cut as `walkforward_factor_mine` (first cutoff 2026-08-20, step 4, forward 4). Each cutoff picks the IS scoop X with the best after-fee hard-red win%, then scores that logic on the hidden window. Short-only has nothing to mine; OOS fires are pooled.

| Cutoff | Hidden | IS best X | IS win | OOS short n/win | OOS 0.5% | OOS 1% | OOS 1.5% | OOS 2% | OOS 3% |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `2026-08-20` | 2026-08-21 → 2026-08-26 | 1.0 | 80.0% | 3/33.3% | 4/25.0% | 4/25.0% | 4/25.0% | 4/25.0% | 4/75.0% |
| `2026-08-26` | 2026-08-27 → 2026-09-01 | 3.0 | 66.7% | 0/— | 6/50.0% | 6/50.0% | 6/50.0% | 5/40.0% | 5/60.0% |
| `2026-09-01` | 2026-09-02 → 2026-09-08 | 3.0 | 63.6% | 3/0.0% | 8/37.5% | 8/50.0% | 8/62.5% | 7/57.1% | 6/50.0% |
| `2026-09-08` | 2026-09-09 → 2026-09-14 | 1.5 | 57.1% | 1/— | 4/0.0% | 4/0.0% | 4/0.0% | 3/0.0% | 3/0.0% |

Pooled OOS short-only: n=7 win=16.7% $=-791.82.

Pooled OOS scoop by X:

| X | n | After-fee win | $ |
|---:|---:|---:|---:|
| 0.5% | 22 | 35.0% | +118.28 |
| 1% | 22 | 40.0% | +485.07 |
| 1.5% | 22 | 45.0% | +871.04 |
| 2% | 19 | 38.9% | +801.71 |
| 3% | 18 | 52.9% | +1411.26 |

## Gate (do not change live)

Hard-red S≤−3 currently blocks **long and short** new lots in:

1. `src/combo_broker.py` `size_combo_tickets` — Webull paper `combo_sh_macd_5050_shared` tickets (`hard_red` → skip every kid).
2. `src/factor_mine_combo.py` `simulate_shared` — the cash book used to grade that combo.
3. `src/factor_mine_book.py` `simulate_book` / `flatten_robust` `hard_red_no_new` — flatten/io sits new buys.

This experiment adds opt-in `hard_red_mode` (`sit` / `short_only` / `dip_scoop` / `short_and_scoop`) with default **sit**. Live callers do not pass a mode.

## KEEP / KILL

**Short-only: KILL.** KILL hard-red short-only: thin n=11 fires (bar ≥30). After-fee win 50.0%. webull n=11 win=50%; flatten n=0 win=—. Do not change live sit on this sample.

**Dip-scoop X=1.0: KILL.** KILL hard-red dip-scoop X=1.0: 55 fires but after-fee win 49.1% ≤ 55%. webull n=26 win=54%; flatten n=29 win=45%

Do not merge a live policy change from this PR.
