#!/usr/bin/env bash
# Land the named paths on origin/main WITHOUT touching the work tree.
#
# History of why this is not `git add && git commit && git pull --rebase`:
#   08-24  rebase died on a scoreboard.json conflict; the push never
#          happened and every sector re-ran after the open.
#   08-25  stale GIT_DIR from actions/checkout + dubious ownership on
#          the self-hosted box; `git commit` failed under `exit 0`.
#   run #6 `git config --global` died `$HOME not set` on the root runner.
#   09-08  twin Pre-Open writers + sleeve-merge live card raced; the
#          dashboard/sleeve-merge/index.html conflict aborted the rebase
#          and the LLM packet never landed.
#   09-09  `stash push -u` + drop erased the 11MB Elite export
#          (finviz_2026-09-09.csv); a later failed `stash pop` left
#          UU 01_daily/_channel1/<date>_predict.json, so every later
#          land's `git commit` died on "unmerged files" and the book
#          commit step painted the job red.
#
# Every one of those came from moving HEAD / the index / the work tree
# under a python process that is still writing files. So: do not.
#
# Strategy (plumbing only, work tree is read-only to this script):
#   0. pin HOME, drop GIT_DIR, mark the work dir safe, token remote
#   1. candidates = files under the named paths that differ from the
#      INDEX (modified) or are untracked. The index is the base: it holds
#      what this job checked out (HEAD, or `git checkout origin/main --`
#      pulls) plus what an earlier land of this job already pushed.
#   2. fetch origin/main; build a TEMP index from its tree
#   3. per candidate, three-way on blobs (base=index, ours=work tree,
#      upstream=origin/main):
#        ours == upstream            → already there, skip
#        upstream == base            → only we changed it → ours
#        both changed → policy:
#          dashboard/day-board/**    → ours (this job's live strip)
#          dashboard/**              → upstream (sleeve-merge / Pages job)
#          03_scoreboard/scoreboard.json → union (src.scoreboard --merge-ours)
#          data/day_board/**         → union (src.day_board --merge-ours)
#          everything else           → ours (dated packet / ranker files)
#      Never delete anything on main.
#   4. write-tree → commit-tree -p origin/main → push SHA:refs/heads/main
#   5. on rejection (someone else pushed) re-fetch and redo from 2;
#      backoff, 6 attempts
#   6. on success record the pushed blobs in the real index so the next
#      land of this job treats them as base (no re-fighting upstream).
#      HEAD never moves. The work tree is never modified except that the
#      union merges leave the merged scoreboard / day_board on disk.
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

# ECS 2026-09-07/08: `could not read Username for 'https://github.com'`.
# GITHUB_TOKEN is already on the job; pin origin so push is not interactive.
TOKEN="${GITHUB_TOKEN:-${GH_TOKEN:-}}"
REPO="${GITHUB_REPOSITORY:-SRoyaltyy/fullscan}"
if [ -n "$TOKEN" ]; then
  git remote set-url origin \
    "https://x-access-token:${TOKEN}@github.com/${REPO}.git" 2>/dev/null || true
fi

if [ "$#" -lt 1 ]; then
  echo "[safe-push] no paths given — nothing to do"
  exit 0
fi

ATTEMPTS="${SAFE_PUSH_ATTEMPTS:-6}"
TMP_INDEX=""
OURS_SB=""
OURS_DB=""
cleanup() {
  [ -n "$TMP_INDEX" ] && rm -f "$TMP_INDEX" "$TMP_INDEX.lock" 2>/dev/null
  [ -n "$OURS_SB" ] && rm -f "$OURS_SB" 2>/dev/null
  [ -n "$OURS_DB" ] && rm -rf "$OURS_DB" 2>/dev/null
  return 0
}
trap cleanup EXIT

# A prior tool (old stash/rebase land, hand merge) can leave UU entries in
# the real index. `git update-index` refuses them. Resolve the INDEX to
# HEAD's entry; leave the work tree file exactly as it is on disk.
clear_leftover_unmerged() {
  local unmerged
  unmerged=$(git diff --name-only --diff-filter=U 2>/dev/null || true)
  if [ -z "$unmerged" ]; then
    return 0
  fi
  echo "[safe-push] leftover unmerged from prior land — resolving index only (work tree untouched)"
  while IFS= read -r f; do
    [ -z "$f" ] && continue
    git reset -q -- "$f" 2>/dev/null \
      || git rm -q --cached -- "$f" 2>/dev/null || true
  done <<< "$unmerged"
}
clear_leftover_unmerged

# ---------------------------------------------------------------------
# 1. candidates
# ---------------------------------------------------------------------
declare -A OURS=()     # path -> work-tree blob
declare -A BASE=()     # path -> index blob ("" when untracked)
declare -A UP=()       # path -> origin/main blob ("" when absent)
declare -A TAKE=()     # path -> blob to land this attempt
declare -a CAND=()

add_candidate() {
  local f="$1"
  [ -z "$f" ] && return 0
  [ -f "$f" ] || return 0            # never land deletions / dirs / symlinks
  [ -L "$f" ] && return 0
  if [ -z "${OURS[$f]+x}" ]; then
    OURS["$f"]=""
    CAND+=("$f")
  fi
}

for p in "$@"; do
  if [ ! -e "$p" ]; then
    echo "[safe-push] skip missing $p"
    continue
  fi
  # Each path on its own: one absent pathspec must not drop the rest
  # (2026-09-08 finish-holes: `git add a b missing` staged nothing).
  while IFS= read -r -d '' f; do add_candidate "$f"; done \
    < <(git ls-files -z --others --exclude-standard -- "$p" 2>/dev/null || true)
  while IFS= read -r -d '' f; do add_candidate "$f"; done \
    < <(git diff -z --name-only -- "$p" 2>/dev/null || true)
done

if [ "${#CAND[@]}" -eq 0 ]; then
  echo "[safe-push] no changes under named paths"
  exit 0
fi

hash_ours() {
  # Bulk hash the work tree copies into the object DB.
  local out
  out=$(printf '%s\n' "${CAND[@]}" | git hash-object -w --stdin-paths 2>/dev/null) || {
    echo "[safe-push] FATAL: hash-object failed"
    return 1
  }
  local i=0
  while IFS= read -r sha; do
    [ -z "$sha" ] && continue
    OURS["${CAND[$i]}"]="$sha"
    i=$((i + 1))
  done <<< "$out"
  return 0
}
hash_ours || exit 1

while IFS= read -r -d '' rec; do
  # "<mode> <sha> <stage>\t<path>"
  meta="${rec%%$'\t'*}"
  path="${rec#*$'\t'}"
  sha="${meta#* }"; sha="${sha%% *}"
  [ -n "${OURS[$path]+x}" ] && BASE["$path"]="$sha"
done < <(git ls-files -s -z -- "${CAND[@]}" 2>/dev/null || true)

mode_of() {
  if [ -x "$1" ]; then echo 100755; else echo 100644; fi
}

# ---------------------------------------------------------------------
# union merges (both sides changed). They read/write the WORK TREE copy
# via the python helpers, so the disk ends up holding the union — which
# is exactly what this job would want to read afterwards anyway.
# ---------------------------------------------------------------------
write_upstream_to_disk() {
  local f="$1" sha="$2"
  mkdir -p "$(dirname "$f")"
  git cat-file blob "$sha" > "$f" 2>/dev/null
}

rehash_one() {
  local f="$1" sha
  sha=$(git hash-object -w -- "$f" 2>/dev/null) || return 1
  OURS["$f"]="$sha"
}

union_scoreboard() {
  local f=03_scoreboard/scoreboard.json
  echo "[safe-push] scoreboard.json changed on both sides — union"
  [ -n "$OURS_SB" ] || OURS_SB=$(mktemp)
  cp -f "$f" "$OURS_SB"
  if write_upstream_to_disk "$f" "${UP[$f]}" \
     && python3 -m src.scoreboard --merge-ours "$OURS_SB"; then
    rehash_one "$f" || cp -f "$OURS_SB" "$f"
  else
    echo "[safe-push] WARN: scoreboard union failed — keeping ours"
    cp -f "$OURS_SB" "$f"
    rehash_one "$f" || true
  fi
}

union_day_board() {
  echo "[safe-push] data/day_board changed on both sides — union"
  [ -n "$OURS_DB" ] || OURS_DB=$(mktemp -d)
  rm -rf "$OURS_DB"/* 2>/dev/null || true
  cp -a data/day_board/. "$OURS_DB/" 2>/dev/null || true
  local f ok=1
  for f in "${CAND[@]}"; do
    case "$f" in data/day_board/*) ;; *) continue ;; esac
    local u="${UP[$f]:-}" b="${BASE[$f]:-}"
    if [ -n "$u" ] && [ "$u" != "$b" ] && [ "$u" != "${OURS[$f]}" ]; then
      write_upstream_to_disk "$f" "$u" || ok=0
    fi
  done
  if [ "$ok" -eq 1 ] && python3 -m src.day_board --merge-ours "$OURS_DB"; then
    :
  else
    echo "[safe-push] WARN: day_board union failed — keeping ours"
    cp -a "$OURS_DB/." data/day_board/ 2>/dev/null || true
  fi
  for f in "${CAND[@]}"; do
    case "$f" in data/day_board/*) rehash_one "$f" || true ;; esac
  done
  # The union may have rewritten latest/today that were not candidates.
  while IFS= read -r -d '' f; do
    add_candidate "$f"
    [ -z "${OURS[$f]}" ] && rehash_one "$f"
  done < <(git ls-files -z --others --exclude-standard -- data/day_board 2>/dev/null; \
           git diff -z --name-only -- data/day_board 2>/dev/null)
}

# ---------------------------------------------------------------------
# 2-4. one attempt against the current origin/main
# ---------------------------------------------------------------------
fetch_main() {
  local n
  for n in 1 2 3; do
    if git fetch -q origin main; then return 0; fi
    echo "[safe-push] fetch failed (${n}/3) — retry"
    sleep $((5 * n))
  done
  return 1
}

load_upstream() {
  local upsha="$1" rec meta path sha
  UP=()
  while IFS= read -r -d '' rec; do
    meta="${rec%%$'\t'*}"
    path="${rec#*$'\t'}"
    sha="${meta##* }"
    [ -n "${OURS[$path]+x}" ] && UP["$path"]="$sha"
  done < <(git ls-tree -r -z "$upsha" 2>/dev/null || true)
}

policy() {
  # both sides changed since base — who wins?
  case "$1" in
    dashboard/day-board/*)        echo ours ;;
    dashboard|dashboard/*)        echo upstream ;;
    03_scoreboard/scoreboard.json) echo union_sb ;;
    data/day_board/*)             echo union_db ;;
    *)                            echo ours ;;
  esac
}

# `git update-index --index-info -z` records: "<mode> <sha>\t<path>\0".
# Bash strings cannot hold NUL, so stream them instead of building a string.
emit_spec() {
  local f
  for f in "${!TAKE[@]}"; do
    printf '%s %s\t%s\0' "$(mode_of "$f")" "${TAKE[$f]}" "$f"
  done
}

attempt() {
  local upsha="$1" f o u b verdict n_take=0 n_skip=0 n_up=0
  load_upstream "$upsha"
  TAKE=()

  # Union passes first (they can rewrite OURS[] for whole groups).
  local need_sb=0 need_db=0
  for f in "${CAND[@]}"; do
    o="${OURS[$f]}"; u="${UP[$f]:-}"; b="${BASE[$f]:-}"
    [ "$o" = "$u" ] && continue
    [ "$u" = "$b" ] && continue
    case "$(policy "$f")" in
      union_sb) need_sb=1 ;;
      union_db) need_db=1 ;;
    esac
  done
  [ "$need_sb" -eq 1 ] && union_scoreboard
  [ "$need_db" -eq 1 ] && union_day_board

  for f in "${CAND[@]}"; do
    o="${OURS[$f]}"; u="${UP[$f]:-}"; b="${BASE[$f]:-}"
    [ -z "$o" ] && continue
    if [ "$o" = "$u" ]; then
      n_skip=$((n_skip + 1)); continue
    fi
    if [ "$u" = "$b" ]; then
      TAKE["$f"]="$o"; n_take=$((n_take + 1)); continue
    fi
    verdict=$(policy "$f")
    case "$verdict" in
      upstream)
        echo "[safe-push] dashboard conflict — keeping origin/main (sleeve-merge / Pages): $f"
        n_up=$((n_up + 1))
        ;;
      *)
        TAKE["$f"]="$o"; n_take=$((n_take + 1))
        ;;
    esac
  done
  echo "[safe-push] ${n_take} to land, ${n_skip} already on main, ${n_up} kept from main"
  if [ "$n_take" -eq 0 ]; then
    echo "[safe-push] nothing new for main"
    return 2
  fi

  [ -n "$TMP_INDEX" ] || TMP_INDEX=$(mktemp)
  rm -f "$TMP_INDEX"
  local tree new
  if ! GIT_INDEX_FILE="$TMP_INDEX" git read-tree "$upsha"; then
    echo "[safe-push] FATAL: read-tree failed"
    return 1
  fi
  if ! emit_spec | GIT_INDEX_FILE="$TMP_INDEX" git update-index -z --add --index-info; then
    echo "[safe-push] FATAL: update-index failed"
    return 1
  fi
  tree=$(GIT_INDEX_FILE="$TMP_INDEX" git write-tree) || return 1
  if [ "$tree" = "$(git rev-parse "${upsha}^{tree}")" ]; then
    echo "[safe-push] tree identical to origin/main — nothing to push"
    return 2
  fi
  new=$(git commit-tree "$tree" -p "$upsha" -m "$MSG") || return 1
  if git push -q origin "$new:refs/heads/main"; then
    git update-ref refs/remotes/origin/main "$new" 2>/dev/null || true
    # Real index: these blobs are now the base for the next land.
    emit_spec | git update-index -z --add --index-info 2>/dev/null || true
    echo "[safe-push] pushed ${new:0:8} (${n_take} files) onto ${upsha:0:8}"
    return 0
  fi
  return 3
}

for n in $(seq 1 "$ATTEMPTS"); do
  if ! fetch_main; then
    echo "[safe-push] fetch origin main failed — attempt ${n}/${ATTEMPTS}"
    sleep $((4 * n))
    continue
  fi
  UPSHA=$(git rev-parse origin/main 2>/dev/null || git rev-parse FETCH_HEAD)
  attempt "$UPSHA"
  rc=$?
  case "$rc" in
    0) exit 0 ;;
    2) exit 0 ;;
    1) echo "[safe-push] FATAL: plumbing error (files are on the runner, not on GitHub)"; exit 1 ;;
    *) echo "[safe-push] push rejected (main moved) — retry ${n}/${ATTEMPTS}"; sleep $((4 * n)) ;;
  esac
done
echo "[safe-push] FATAL: push failed after retries — files are on the runner"
exit 1
