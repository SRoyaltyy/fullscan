#!/usr/bin/env bash
# Push an already-made commit on top of origin/main, retrying races.
#
# 2026-10-23 holdup open fill (run 39999229122) died on a rebase conflict in
# PRICE_LEDGER.jsonl / price_revisions.jsonl because "Seal the next holdup
# plan." landed under it. Those files are append-only line logs, so a
# conflict there is two writers each appending lines. Resolve it the only
# add-only way: keep every upstream line in order, then append this run's
# new lines that upstream does not already have. Nothing is removed or
# reordered.
#
# Files with a seq / sealed chain (LEDGER.jsonl, *_log.jsonl) are NOT
# auto-resolved: a conflict there still aborts and fails, never forces.
# Generated dashboard JSON (dashboard/h1|holdup/*.json) is rebuilt every
# run; on conflict take this run's copy.
#
# Usage: bash scripts/append_only_rebase_push.sh [attempts]
set -uo pipefail
ATTEMPTS="${1:-8}"

UNION_RE='(^|/)(PRICE_LEDGER|price_revisions|prices|skips)\.jsonl$'
OURS_RE='^dashboard/(h1|holdup)/[^/]+\.json$'

union_merge() {  # stage 2 = upstream, stage 3 = this run's commit
  python3 - "$1" <<'PY'
import subprocess, sys
p = sys.argv[1]
def stage(n):
    r = subprocess.run(["git", "show", f":{n}:{p}"], capture_output=True, text=True)
    return r.stdout.splitlines() if r.returncode == 0 else []
base, up, mine = stage(1), stage(2), stage(3)
have, old = set(up), set(base)
out = list(up)
for line in mine:
    if line in old or line in have:
        continue
    out.append(line); have.add(line)
open(p, "w").write("\n".join(out) + ("\n" if out else ""))
print(f"[append-only] {p}: kept {len(up)} upstream lines, appended {len(out)-len(up)}")
PY
}

resolve_or_fail() {
  local f bad=0
  while IFS= read -r f; do
    [ -z "$f" ] && continue
    if [[ "$f" =~ $UNION_RE ]]; then
      { union_merge "$f" && git add -- "$f"; } || bad=1
    elif [[ "$f" =~ $OURS_RE ]]; then
      { git checkout --theirs -- "$f" && git add -- "$f"; } || bad=1
    else
      echo "::error::record file conflicted: $f (not auto-resolved)"
      bad=1
    fi
  done < <(git diff --name-only --diff-filter=U)
  return $bad
}

in_rebase() {
  [ -d "$(git rev-parse --git-path rebase-merge)" ] || [ -d "$(git rev-parse --git-path rebase-apply)" ]
}

for attempt in $(seq 1 "$ATTEMPTS"); do
  if ! git fetch origin main; then
    echo "fetch failed on attempt ${attempt}"; sleep $((3 * attempt)); continue
  fi
  if ! git rebase origin/main; then
    ok=1
    guard=0
    while in_rebase; do
      guard=$((guard + 1))
      if [ "$guard" -gt 50 ] || ! resolve_or_fail; then ok=0; break; fi
      GIT_EDITOR=true git rebase --continue || true
    done
    if [ "$ok" != 1 ]; then
      git rebase --abort || true
      echo "::error::rebase conflict on a record file (attempt ${attempt}); not forcing"
      exit 1
    fi
  fi
  if git push origin HEAD:main; then
    echo "pushed on attempt ${attempt}"
    exit 0
  fi
  echo "push rejected on attempt ${attempt}"
  sleep $(( 2 * attempt + RANDOM % 5 ))
done
echo "::error::push failed after ${ATTEMPTS} attempts"
exit 1
