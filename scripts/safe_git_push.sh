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
# 2026-09-08 (fix #2): twin Pre-Open writers + sleeve-merge live card
# raced at Commit predictive artifacts. dashboard/sleeve-merge/index.html
# conflicts aborted rebase/merge and the LLM packet never landed. Take
# origin/main for dashboard HTML; keep OUR 01_daily / dated ranker;
# reset to a clean LOCAL if rebase leaves an unmerged index.
#
# Strategy:
#   0. pin HOME, drop GIT_DIR, mark the work dir safe
#   1. commit the named paths
#   2. snapshot OUR scoreboard.json
#   3. rebase onto origin/main
#   4. on conflict: keep OUR 01_daily/02_lessons/ranker, take main's
#      dashboard + scoreboard (then union scoreboard)
#   5. if still dirty: reset --hard LOCAL and merge
#   6. push; retries
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

# Add each path on its own. `git add a b missing` fails the whole
# add when one pathspec is absent, so a listed note file can drop
# dashboard/ + essays even when they were written on the runner.
added=0
for p in "$@"; do
  if [ -e "$p" ]; then
    if git add -- "$p"; then
      added=$((added + 1))
    else
      echo "[safe-push] WARN: git add failed for $p"
    fi
  else
    echo "[safe-push] skip missing $p"
  fi
done
if [ "$added" -eq 0 ] || git diff --staged --quiet; then
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

# Dated ranker / weather / book this job just wrote. Main's copies are
# the ubuntu land vs Stock Book ALL race. Take ours so rebase/merge
# can finish; scoreboard still unions separately.
RANKER_PATHS=(
  data/stock_book data/join data/universe data/ab_checklist
  data/peers data/paper data/exports data/catalyst
  01_daily/weather
)

# Sleeve-merge / Pages HTML is written by a different job. A conflict
# here must not drop the LLM packet (2026-09-08 twin writers).
DASHBOARD_PATHS=(
  dashboard
  dashboard/sleeve-merge
  dashboard/index.html
)

restore_ours_daily() {
  git checkout "$LOCAL" -- 01_daily 02_lessons 01_daily/_transcripts 2>/dev/null || true
  git add 01_daily 02_lessons || true
}

restore_ours_ranker() {
  git checkout "$LOCAL" -- "${RANKER_PATHS[@]}" 2>/dev/null || true
  git add "${RANKER_PATHS[@]}" 2>/dev/null || true
}

take_main_dashboard() {
  echo "[safe-push] dashboard conflict — keeping origin/main (sleeve-merge / Pages)"
  git checkout origin/main -- dashboard 2>/dev/null \
    || git checkout --ours -- dashboard 2>/dev/null || true
  git add dashboard 2>/dev/null || true
}

# Mark every leftover unmerged path so rebase/merge can finish.
# Packet paths → LOCAL. dashboard → origin/main. else → LOCAL if we
# have it, else origin/main.
resolve_unmerged() {
  local unmerged
  unmerged=$(git diff --name-only --diff-filter=U 2>/dev/null || true)
  if [ -z "$unmerged" ]; then
    return 0
  fi
  echo "[safe-push] resolving leftover unmerged paths"
  while IFS= read -r f; do
    [ -z "$f" ] && continue
    case "$f" in
      dashboard|dashboard/*)
        git checkout origin/main -- "$f" 2>/dev/null \
          || git checkout --ours -- "$f" 2>/dev/null \
          || git rm -f -- "$f" 2>/dev/null || true
        ;;
      03_scoreboard/scoreboard.json)
        git checkout origin/main -- "$f" 2>/dev/null || true
        ;;
      01_daily/*|02_lessons/*|data/stock_book/*|data/join/*|data/universe/*|data/ab_checklist/*|data/peers/*|data/paper/*|data/exports/*|data/catalyst/*)
        git checkout "$LOCAL" -- "$f" 2>/dev/null \
          || git checkout --theirs -- "$f" 2>/dev/null || true
        ;;
      *)
        git checkout "$LOCAL" -- "$f" 2>/dev/null \
          || git checkout origin/main -- "$f" 2>/dev/null || true
        ;;
    esac
    git add -- "$f" 2>/dev/null || true
  done <<< "$unmerged"
  git add -u 2>/dev/null || true
}

clean_to_local() {
  git rebase --abort >/dev/null 2>&1 || true
  git merge --abort >/dev/null 2>&1 || true
  git reset --hard "$LOCAL" >/dev/null 2>&1 || true
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
  echo "[safe-push] rebase conflict — keeping our 01_daily + dated ranker, merging scoreboard"
  git checkout --theirs -- 01_daily 02_lessons "${RANKER_PATHS[@]}" 2>/dev/null || restore_ours_daily
  restore_ours_ranker
  take_main_dashboard
  git checkout --ours -- 03_scoreboard/scoreboard.json 2>/dev/null || true
  resolve_scoreboard
  resolve_unmerged
  git add 01_daily 02_lessons 03_scoreboard "${RANKER_PATHS[@]}" "${DASHBOARD_PATHS[@]}" 2>/dev/null || git add -A
  if GIT_EDITOR=true git rebase --continue; then
    git stash drop >/dev/null 2>&1 || true
    return 0
  fi
  echo "[safe-push] rebase --continue failed; aborting back to LOCAL"
  git rebase --abort >/dev/null 2>&1 || true
  git stash drop >/dev/null 2>&1 || true
  clean_to_local
  return 1
}

try_merge() {
  clean_to_local
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
  echo "[safe-push] merge conflict — ours daily + dated ranker; main dashboard"
  restore_ours_daily
  restore_ours_ranker
  take_main_dashboard
  git checkout origin/main -- 03_scoreboard/scoreboard.json 2>/dev/null || true
  resolve_scoreboard
  resolve_unmerged
  git add 01_daily 02_lessons 03_scoreboard "${RANKER_PATHS[@]}" "${DASHBOARD_PATHS[@]}" 2>/dev/null || git add -A
  if git commit -m "merge main (ours daily + merged scoreboard)"; then
    git stash drop >/dev/null 2>&1 || true
    return 0
  fi
  echo "[safe-push] merge commit failed — packet files stay on LOCAL; dashboard dropped"
  git merge --abort >/dev/null 2>&1 || true
  git stash drop >/dev/null 2>&1 || true
  clean_to_local
  return 1
}


if ! try_rebase; then
  try_merge || true
fi

push_once() {
  git push origin main
}

if push_once; then
  echo "[safe-push] pushed $(git rev-parse --short HEAD)"
  [ -n "$OURS_SB" ] && rm -f "$OURS_SB"
  exit 0
fi

for attempt in 1 2 3 4; do
  echo "[safe-push] push rejected — fetch/rebase/retry ${attempt}/4"
  sleep $((4 * attempt))
  clean_to_local
  if try_rebase || try_merge; then
    if push_once; then
      echo "[safe-push] pushed on retry ${attempt} $(git rev-parse --short HEAD)"
      [ -n "$OURS_SB" ] && rm -f "$OURS_SB"
      exit 0
    fi
  fi
done
echo "[safe-push] FATAL: push failed after retries — files are on the runner"
[ -n "$OURS_SB" ] && rm -f "$OURS_SB"
exit 1
