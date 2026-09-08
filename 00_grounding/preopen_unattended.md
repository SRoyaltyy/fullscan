# Pre-Open ALL — unattended morning + cancel auth

Unattended clock is ECS systemd `fullscan-preopen.timer` at **05:55 ET**.
GitHub `preopen_all.yml` is a poke / ubuntu heal, not the primary clock.

## Tomorrow morning checklist (2026-09-09)

1. **HALT is reverted on this branch.** `preopen_all.yml`, `stock_book_all.yml`,
   and `daily_orchestrator.yml` must not contain `if: false` / HALT.
2. **One ubuntu writer.** Concurrency group is `preopen-all-ubuntu` (push or
   `runner=ubuntu`) or `preopen-all-ecs`. Never `ubuntu-HHMM`. A new ubuntu
   poke **cancels** the prior ubuntu run. ECS does **not** cancel-in-progress.
3. **On-time 09:25 gate stays.** Essays skip after 09:25 unless:
   - `workflow_dispatch` `force=true` → `--force` (rewrite + cutoff), or
   - a **push poke between 09:25 and 12:00 ET** → `--bypass-cutoff`
     (essays may run; skip-if-good still keeps quality-ok files).
   Morning ECS / on-time ubuntu runs do **not** pass `--force`.
4. **Do not poke live salvage** after this harden lands. Watch the 05:55 ET
   ECS start and the ~05:40 ET Finviz scrape. If ubuntu heal is needed,
   touch only `preopen_all.yml` so it joins the **same** `preopen-all-ubuntu`
   group (GitHub cancels the prior run).
5. **QC tokens to read** in `01_daily/YYYY-MM-DD_preopen_status.md`:
   - `news_parse` `db_timeout` = Postgres statement timeout after retries
     (not a silent empty parse).
   - `map_heat_research` passthrough = search flake; night baseline stands;
     packet continues (no 21-minute block).

## Cancel auth (fix #1 / #F)

- This workflow has `permissions: actions: write` so **the same concurrency
  group** can cancel the previous ubuntu run. That is the supported cancel path.
- `workflow_dispatch` and `gh run cancel` from a cloud/integration token are
  often **403**. That is not a paywall — the token lacks `Actions: write` on
  the repo, or the hung run is in a **different** concurrency group.
- **Owner UI → Force cancel** is required when:
  - the token is 403, or
  - a zombie sits in another group (`ubuntu-0945` vs `ubuntu-0950` was the
    2026-09-08 hole; those forks are gone), or
  - GitHub concurrency cancel was submitted but the runner did not die.
- Do not invent a new `ubuntu-HHMM` group to “supersede” a hang. Use the
  stable `ubuntu` group so cancel-in-progress works.

## Files

- `.github/workflows/preopen_all.yml` — concurrency, late-heal, packet commit
- `scripts/safe_git_push.sh` — dashboard/sleeve-merge take main
- `src/db.py` / `src/news_parse.py` / `src/output_qc.py` — DB timeout
- `src/preopen.py` / `src/run_preopen_all.py` — `--bypass-cutoff`
- `src/map_heat_refresh.py` — `--passthrough` after timeout
