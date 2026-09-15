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
The `continue` job re-dispatches this workflow up to 200 generations when
`done` is not set, including after a job timeout/cancel. A backup cron
fires every 6 hours in addition to Saturday 11:20 UTC.

Holdout is a **calendar time-split** (last 30% of session dates). Discovery
may use a row only when the feature date AND the label window (I1 / I5 last
close, or I10 / hold-region last close on the deep-corr scan) are strictly
before the cutoff. A ticker-name split is rejected: both sets share a
calendar, so those boards are not usable.

`split_kind=time` and `cutoff` are persisted on the checkpoint. A restored
ckpt missing those fields is discarded and the walk starts at `disc_uni`.
