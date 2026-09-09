# Pre-Open ALL — unattended morning + incremental land

Unattended clock is ECS systemd `fullscan-preopen.timer` at **05:55 ET**.
GitHub `preopen_all.yml` is a poke / ubuntu heal, not the primary clock.

## Incremental land (write A → QC A → push A)

Do **not** wait until the end of the job to commit. After each step
passes QC, `src.land_file` pushes those dated files to `main` and
updates `data/day_board/{date}.json`. A later timeout, 402, or
sleeve-merge rebase cannot erase weather / parse / book that already
landed.

The YAML "Commit predictive artifacts" / "Commit everything" steps are
**leftover sweeps** only.

Live board (no Action click):
https://sroyaltyy.github.io/fullscan/dashboard/day-board/

JSON (updates as soon as `main` has the file, Pages rebuild not required):
`https://raw.githubusercontent.com/SRoyaltyy/fullscan/main/data/day_board/latest.json`

Factor mine shows the same 1d BUY/SELL strip:
https://sroyaltyy.github.io/fullscan/dashboard/factor-mine/

## Morning checklist

1. **HALT is reverted.** `preopen_all.yml`, `stock_book_all.yml`, and
   `daily_orchestrator.yml` must not contain `if: false` / HALT.
2. **One ubuntu writer.** Group `preopen-all-ubuntu` or `preopen-all-ecs`.
   Never `ubuntu-HHMM`. Ubuntu cancels twins. ECS does not.
3. **On-time 09:25 gate stays.** Late push 09:25–12:00 ET uses
   `--bypass-cutoff`. Only `force=true` rewrites quality-ok files.
4. **DeepSeek preflight.** A 402 skips essays; weather / join / book
   still land. Top up the key before 05:55 if the preflight fails.
5. **Holiday / missing night heat.** Overlay copies last session groups
   instead of hard-failing (Labor Day 09-07 hole).
6. **ECS git.** `safe_git_push.sh` pins `x-access-token` from
   `GITHUB_TOKEN` so `could not read Username` cannot drop a packet.
7. **Day board.** Open the .io page; do not run Stock Book readiness
   just to see which processes exist.

## Cancel auth

- `permissions: actions: write` so the same ubuntu group can cancel.
- Cloud `gh run cancel` is often 403. Owner Force cancel for zombies.
- Do not invent `ubuntu-HHMM`.

## Files

- `src/land_file.py` `src/day_board.py` — incremental QC+push + .io board
- `.github/workflows/preopen_all.yml` — concurrency, late-heal, leftover sweep
- `scripts/safe_git_push.sh` — dashboard/sleeve-merge take main; token remote
- `src/db.py` / `src/news_parse.py` / `src/output_qc.py` — DB timeout
- `src/preopen.py` / `src/run_preopen_all.py` — `--bypass-cutoff` + land
- `src/map_heat.py` / `src/map_heat_refresh.py` — last-session overlay + passthrough
