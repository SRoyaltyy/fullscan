# Excel stat mine — crash-resume

The whole-sheet miner no longer waits until the last ticker to write.

Every 25 tickers (or 8 minutes, or SIGTERM / atexit) it atomically writes:

- `03_scoreboard/excel_stat_mine_ckpt.json` — phase, processed tickers, running 2x2 counts
- `03_scoreboard/EXCEL_STAT_MINE.md` — PARTIAL board ranked on counts so far
- `03_scoreboard/excel_stat_mine.json` — slim copy of the same

`scripts/safe_git_push.sh` lands those three onto `main` during the job, so a
6h timeout or runner kill still leaves a readable board.

Next Action restores the ckpt from `origin/main` and skips `processed`.
Phases: `disc_uni` → `hold_uni` → `pairs_disc` → `pairs_hold` → `done`.

`--reset` (workflow input `reset=true`) starts a fresh walk.
The `continue` job re-dispatches this workflow up to 8 generations when `done` is not set.

Shade flags (`_pale` / `_mid` / `_deepg` / `_g_not_deep` and red twins)
are first-class. Changing the pixel factory requires `--reset` so new
keys are not mixed into an old 2x2 count table.
