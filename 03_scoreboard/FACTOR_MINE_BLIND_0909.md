# Factor mine blind formation — as-of 2026-09-09

This is a **blind formation remine**, not a KEEP-selection cut of the
already-known live menu (that was [PR #285](https://github.com/SRoyaltyy/fullscan/pull/285)).

In-sample formation window: **2026-08-13 → 2026-09-09**.
Out-of-sample (frozen discoveries only): **2026-09-10 → 2026-09-18**.

Results table and verdict are filled after the side-path remine
(`python -m src.factor_mine --blind-0909 --write --holdout --out-root …`).
This file is the method contract until that run lands.

## What was frozen

- **Panel cutoff:** `2026-08-13` → `2026-09-09` only. 9/10–9/18 never
  enters ranking, tweaking, or featuring.
- **Recipe-definition freeze:** yes. Recipe + combo-spec definitions
  frozen to commit `cb7f09ae` (2026-09-09, Factor-mine combo books #175).
  Current cash-book / mark / fee engine scores those frozen definitions.
- **Excluded from the seed set:** Clock-B catalogue pins, `union_hot_n4_holdup`,
  overnight_mega / overnight_h1, `combo_oh_5050_shared`,
  `combo_sh_macd_5050_shared`, `short_news_r_macd_h3`, WORKABLE_ALWAYS
  extras, FOCUS / LONG_LED_PIN live featured pins.
- **Kept:** generic 9/9 auto-grid primitives (universe / hold / gate /
  rank / side / top_n / exit / size / sell / S-boost as of 9/9).
  `union_hot_n4_h1` is in that 9/9 menu (added 2026-09-05) — it is not
  injected as a live FOCUS seed.
- **Holdup primitive:** `s_boost=holdup` did not exist on 9/9 (landed
  2026-09-19). It is not swept and no holdup twin is invented.

## Method (same as the current board, knowledge-capped)

1. Leak-free 09:30 panel / $10k cash books (current engine).
2. Default **auto** slice + **auto-tweak** neighbor sweep on the frozen
   9/9 recipe menu.
3. Combo construction from the 9/9 combo-engine specs plus extra shared
   50/50 / 70/30 / 333 mixes **formed from IS singles**.
4. Rank / KEEP from IS only. Cyrus would-have-featured: Starts YES
   ≥17/19 (or ≥85% when start_n < 19), Book% > 0, n ≥ 30. Formal
   WORKABLE_BAR is reported beside it and does **not** pin FOCUS /
   WORKABLE_ALWAYS / hot4-holdup.
5. Frozen discoveries replayed OOS continued + fresh $10k.

Live `dashboard/factor-mine/` and `03_scoreboard/factor_mine.json` are
not written.

## Status

Remine not yet written into this file. Side paths:

- `03_scoreboard/factor_mine_blind_0909/`
- `dashboard/factor-mine-blind-0909/`
