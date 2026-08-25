#!/usr/bin/env bash
# Commit staged-by-path changes and push to main without losing 01_daily
# artifacts or rebase-clobbering 03_scoreboard/scoreboard.json.
#
# The 08-24 failure: sector_daily and daily_pipeline both rewrote
# scoreboard.json from a stale base; `git pull --rebase` died on the
# conflict and the push never happened, so the next orchestrator pass
# re-ran every sector AFTER the open.
#
# The 08-25 Pre-Open ALL failure: python wrote a full quality day, then
# this script hit `fatal: not in a git directory` (stale GIT_DIR from
# actions/checkout) plus `dubious ownership` on the self-hosted work
# dir, `git commit` failed, and `exit 0` painted the job green.
#
# Run #6: `git config --global` died `fatal: $HOME not set` because the
# root self-hosted runner has no HOME. Pin it.
#
# Strategy:
#   0. pin HOME, drop GIT_DIR, mark the work dir safe
#   1. commit the named paths
#   2. snapshot OUR scoreboard.json
#   3. rebase onto origin/main
#   4. on conflict: keep OUR 01_daily/02_lessons, take main's scoreboard
#      then union our (date, topic) entries back in via src.scoreboard
#   5. push; one retry
#
# Usage: bash scripts/safe_git_push.sh "commit message" path [path ...]
set -uo pipefail

export HOME="${HOME:-${FULLSCAN_HOME:-/home/gha}}"
export GIT_TERMINAL_PROMPT=0

MSG="${1:-auto: update}"
shift || true

unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE || true
if [ -n "${GITHUB_WORKSPACE:-}" ] && [ -d "${GITHUB_WORKSPACE}/.git" ]; then
  cd "$GITHUB_WORKSPACE"
fi
ROOT="$(pwd)"
git config --global --add safe.directory "$ROOT" || true
git config --global --add safe.directory /home/gha/actions-runner/_work/fullscan/fullscan || true
git config --global --add safe.directory /home/gha/fullscan || true
git config --global --add safe.directory '*' || true
git config --local --add safe.directory "$ROOT" || true

if [ ! -d .git ] && [ ! -f .git ]; then
  echo "[safe-push] FATAL: $ROOT is not a git checkout"
  exit 1
fi

git config user.name "Market-Bot-Automaton"
git config user.email "bot@users.noreply.github.com"

if [ "$#" -lt 1 ]; then
  echo "[safe-push] no paths given — nothing to do"
  exit 0
fi

git add "$@" || true
if git diff --staged --quiet; then
  echo "[safe-push] no staged changes"
  git status -sb || true
  exit 0
fi

if ! git commit -m "$MSG"; then
  echo "[safe-push] FATAL: git commit failed (files are on the runner, not on GitHub)"
  git status -sb || true
  exit 1
fi

OURS_SB=""
if [ -f 03_scoreboard/scoreboard.json ]; then
  OURS_SB=$(mktemp)
  cp 03_scoreboard/scoreboard.json "$OURS_SB"
fi
LOCAL=$(git rev-parse HEAD)

resolve_scoreboard() {
  if [ -n "$OURS_SB" ] && [ -f "$OURS_SB" ]; then
    python3 -m src.scoreboard --merge-ours "$OURS_SB" || true
    git add 03_scoreboard/scoreboard.json || true
  fi
}

restore_ours_daily() {
  git checkout "$LOCAL" -- 01_daily 02_lessons 01_daily/_transcripts 2>/dev/null || true
  git add 01_daily 02_lessons || true
}

try_rebase() {
  git fetch origin main || return 1
  # Unstaged leftover files on the self-hosted work tree (clean:false)
  # made run #7 rebase abort and then dump 60 extra files. Stash them.
  git stash push --keep-index -u -m "safe-push-unstaged" >/dev/null 2>&1 || true
  if git rebase origin/main; then
    git stash drop >/dev/null 2>&1 || true
    return 0
  fi
  echo "[safe-push] rebase conflict — keeping our 01_daily, merging scoreboard"
  git checkout --theirs -- 01_daily 02_lessons 2>/dev/null || restore_ours_daily
  git checkout --ours -- 03_scoreboard/scoreboard.json 2>/dev/null || true
  resolve_scoreboard
  git add 01_daily 02_lessons 03_scoreboard 2>/dev/null || git add -A
  if GIT_EDITOR=true git rebase --continue; then
    git stash drop >/dev/null 2>&1 || true
    return 0
  fi
  echo "[safe-push] rebase --continue failed; aborting"
  git rebase --abort || true
  git stash drop >/dev/null 2>&1 || true
  return 1
}

try_merge() {
  git fetch origin main || return 1
  git stash push --keep-index -u -m "safe-push-unstaged" >/dev/null 2>&1 || true
  if git merge origin/main --no-edit; then
    resolve_scoreboard
    # Only commit if resolve_scoreboard staged something. Do NOT git-add
    # dirty 01_daily leftovers from the self-hosted work tree.
    if ! git diff --staged --quiet; then
      git commit -m "merge main (scoreboard union)" || true
    fi
    git stash drop >/dev/null 2>&1 || true
    return 0
  fi
  echo "[safe-push] merge conflict — ours daily + union scoreboard"
  restore_ours_daily
  git checkout origin/main -- 03_scoreboard/scoreboard.json 2>/dev/null || true
  resolve_scoreboard
  git add 01_daily 02_lessons 03_scoreboard 2>/dev/null || git add -A
  git commit -m "merge main (ours daily + merged scoreboard)" || true
  git stash drop >/dev/null 2>&1 || true
  return 0
}


if ! try_rebase; then
  try_merge || true
fi

if git push origin main; then
  echo "[safe-push] pushed $(git rev-parse --short HEAD)"
  [ -n "$OURS_SB" ] && rm -f "$OURS_SB"
  exit 0
fi

echo "[safe-push] first push rejected — fetch/rebase/retry"
if try_rebase || try_merge; then
  git push origin main && echo "[safe-push] pushed on retry $(git rev-parse --short HEAD)" \
    || { echo "[safe-push] FATAL: push failed after retry — files are on the runner"; exit 1; }
else
  echo "[safe-push] FATAL: could not rebase or merge — files are on the runner"
  exit 1
fi
[ -n "$OURS_SB" ] && rm -f "$OURS_SB"
exit 0
