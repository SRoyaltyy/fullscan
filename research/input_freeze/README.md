# Pre-open input freeze (append-only)

Each NYSE session at ~09:20 ET, `.github/workflows/input_freeze.yml` copies the
newest version of every daily model input into `research/input_freeze/<YYYY-MM-DD>/`
so the all-stocks up/down model trains on history that was knowable before the open
(IRONCLAD C.9).

```
research/input_freeze/<date>/manifest.json
research/input_freeze/<date>/files/<repo>/<source path>[.gz]
```

`manifest.json` lists each input's category, source repo and path, `as_of_date`,
`source_commit`, `source_commit_time_utc`, `sha256` and `bytes` of the original file,
plus the stored copy (`stored_path`, `gzip`, `stored_sha256`). `freeze_time_utc` is the
cutoff, and only versions committed before it are used. Statuses:

- `copied`: stored in the folder (gzip above 64 KB, deterministic `mtime=0`).
- `pointer_only`: larger than 12 MB raw. It is pinned by source commit and sha256 and
  is not copied. Git history keeps it, since force-push is banned.
- `stale`: the newest version is more than 4 calendar days old, or older than the day
  for same-day files. It is pinned but not copied.
- `missing`: nothing was committed before the freeze, or the input isn't in a repo
  (Grok Update sheets).

## Rules

- A day folder, once written, is never rewritten. If the folder is already on main,
  the run exits success with no changes. The `check` job fails a PR or push that
  changes, adds to, or deletes any existing day folder.
- A trigger at or after 09:28 ET with no folder writes only
  `{"status": "late: not frozen"}`. A trigger more than 210 minutes before 09:20 ET
  exits without writing.
- Weekends and NYSE holidays write nothing. Before 2026-10-07 nothing is written.
- 09:30 ET comes from `zoneinfo`, so the November DST switch needs no edit.

## Run by hand

```
python3 research/input_freeze/tool/freeze_inputs.py plan
python3 research/input_freeze/tool/freeze_inputs.py freeze --date 2026-10-06 --at-target \
  --fullscan . --fullscan-ref origin/main --theme-radar ../theme-radar --dry-run
python3 research/input_freeze/tool/test_freeze_inputs.py
```

## Replay check (decision freeze)

`.github/workflows/decision_freeze_check.yml` runs
`research/input_freeze/tool/check_decision_freeze.py`, which verifies that the
sealed `data/day_board/<date>_strategy_tickets.json` was built from exactly the
frozen inputs: every `decision_readiness.inputs` digest must equal the freeze
manifest row's sha256 (or be a recorded `absent`/`missing` pair). A mismatch is
a hard FAIL when the ticket was completed after the freeze cutoff, INFO when the
ticket legitimately predates it. Stored copies are re-hashed so a tampered
append-only folder also fails. From schema `input_freeze/v2` the SPEC covers
every fingerprinted input (`data/factor_mine/panel.json`,
`data/sleeve_merge/today.json`); v1 folders only WARN on uncovered inputs.
