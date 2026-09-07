# Excel emulator mine — standing cycle

_Generated 2026-09-07 · live `flatten_robust` is not changed. No merge without Cyrus._

## Inventory → mine plan

| surface | what | this cycle |
|---|---|---|
| VISIBLE A..O (15) | daily/backtest grids, `signal_colors`, ~139d OHLCV+fills; excel-state = rows only | **mined** (3603 rebuilt; done_grids ~3445) |
| ALL_COLS A..JL (275) | `run.py --all-cols`, not daily | phase 2 — 35-ticker pilot **THIN 90** |
| PIT | open A,B,C,G,J,K,L,M,O · close D,E,F,H,I,N · `core_score` = close | enforced |
| live 2026-09-05 | L1/L2 −0.55%, L3 +0.35%, L5 −4% | refresh L3 then S1/S2; defer L4/L5 |

## Ship bar

PASS needs **all** of: discovery n≥300 t≥3 avg>0; holdout n≥100 t≥2 avg>0 same sign; ≥50 tickers; ≥20 entry dates; no single trade >25% of gross wins; trimmed mean (drop best trade) >0; early and late tape same sign when both n≥40; SPY-up and SPY-down both positive when both n≥40; hold1 must also PASS hold2; hold3/5/8 need hold2 short-horizon edge vs uncond; beat uncond by ≥20 bp. Open entry only when the feature reads no close-knowable fill.

A–O first mine: patterns **67** · cells **2283** · **PASS 240** · **FAIL 2043** · **THIN 0**.
hold5/8 tape-rides demoted: **546**. Focus L3 / S1–S2: **all FAIL** (`no_edge_vs_uncond` or sign/t).

Honest sleeve-shaped keepers (ALL × hold1/2): **6**, all hysteresis `open_core` / `open_score` open-long. Same-day hold1 +1.28% to +1.74% vs uncond −0.07%. Research only — one regime, no cards.

Full table: `AO_FIRST_MINE.md` / `03_scoreboard/EXCEL_BOT_MINE.md`. All-cols pilot: `ALL_COLS_MINE.md`.

Research only. No merge without Cyrus. Live flatten_robust untouched.

