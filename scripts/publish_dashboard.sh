#!/usr/bin/env bash
# Publish dashboard/ (buy/sell paper book) to the gh-pages branch.
# Used by Pre-Open ALL and Stock Book ALL so suggestions land on
# https://SRoyaltyy.github.io/fullscan/dashboard/ without waiting
# for a second workflow. Never prints tokens. Publication failures fail
# the job so a green run means the site was actually updated.
#
# 2026-09-10: deploy-dashboard.yml (peaceiris force_orphan) and this
# script force-pushed gh-pages at the same second →
# `cannot lock ref 'refs/heads/gh-pages'`. Both paths now go through
# here: PAGES_OUT_DIR=<dir> publishes a prebuilt tree, and the push
# retries with backoff instead of giving up after one 8s nap.
set -uo pipefail
PUBLISH_SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

export HOME="${HOME:-${FULLSCAN_HOME:-/home/gha}}"
export GIT_TERMINAL_PROMPT=0
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE || true
if [ -n "${GITHUB_WORKSPACE:-}" ] && [ -d "${GITHUB_WORKSPACE}/.git" ]; then
  cd "$GITHUB_WORKSPACE"
fi

TOKEN="${GITHUB_TOKEN:-${GH_TOKEN:-}}"
REPO="${GITHUB_REPOSITORY:-SRoyaltyy/fullscan}"
if [ -z "$TOKEN" ]; then
  echo "[pages] WARN: no GITHUB_TOKEN — skip gh-pages push"
  exit 0
fi

# holdup and h1 are root aliases (/holdup/ /h1/) as well as dashboard/
# folders. Every force-push, including free-news-intake, copies both
# from this checkout onto gh-pages.
SUBS="boring-winners ticker-lookback gainer-lookback flatten-lookback mover-lookback sleeve-combine sleeve-merge mover-paper book-paper paper-book strategy-board factor-mine day-board down-day-mine hard-red-exceptions day-movers excel-cam news-intake webull-sim holdup h1 scanner-backtest"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
OUT="${PAGES_OUT_DIR:-}"
if [ -n "$OUT" ] && [ -f "$OUT/dashboard/index.html" ]; then
  echo "[pages] publishing prebuilt $OUT"
  cp -a "$OUT/." "$TMP/"
else
  if [ ! -f dashboard/index.html ]; then
    echo "[pages] WARN: dashboard/index.html missing — not touching gh-pages"
    exit 0
  fi
  mkdir -p "$TMP/dashboard"
  cp -a dashboard/. "$TMP/dashboard/"
  cp dashboard/index.html "$TMP/index.html"
fi
touch "$TMP/.nojekyll"

# Always overlay sub-dashes from this checkout. A stale artifact
# day-movers/index.html used to skip the copy and 404 cam.html.
for sub in $SUBS; do
  if [ -f "dashboard/${sub}/index.html" ]; then
    mkdir -p "$TMP/dashboard/${sub}"
    cp -a "dashboard/${sub}/." "$TMP/dashboard/${sub}/"
    echo "[pages] overlay dashboard/${sub} from main"
  fi
  if [ -d "$TMP/dashboard/${sub}" ]; then
    mkdir -p "$TMP/${sub}"
    cp -a "$TMP/dashboard/${sub}/." "$TMP/${sub}/"
  fi
done
# The overlay above copies dashboard/factor-mine from main, which drops
# the deploy-time patches (cash-start replay, ovcal, pre-open labels and
# replay panel). Re-apply them to both published copies. Each patch is
# idempotent and add-only.
for page in "$TMP/dashboard/factor-mine/index.html" "$TMP/factor-mine/index.html"; do
  if [ -f "$page" ]; then
    python3 scripts/patch_fm_ovcal.py "$page" || true
    python3 scripts/patch_fm_holdup.py "$page" || true
    python3 scripts/patch_fm_preopen_labels.py "$page" dashboard/factor-mine/replay_preopen.json || true
    python3 scripts/patch_fm_pending_day.py "$page" data/day_board/today_strategies.json || true
  fi
done
git -C "$TMP" init -q
git -C "$TMP" checkout -q -b gh-pages
git -C "$TMP" config user.name "Market-Bot-Automaton"
git -C "$TMP" config user.email "bot@users.noreply.github.com"
git -C "$TMP" add -A
if git -C "$TMP" diff --cached --quiet; then
  echo "[pages] nothing new to publish"
  exit 0
fi
git -C "$TMP" commit -qm "deploy dashboard ${GITHUB_SHA:-local}"
git -C "$TMP" remote add origin \
  "https://x-access-token:${TOKEN}@github.com/${REPO}.git"

ATTEMPTS="${PAGES_PUSH_ATTEMPTS:-5}"
for n in $(seq 1 "$ATTEMPTS"); do
  # The saved filing view is published separately and is absent from main.
  # Preserve it from the current remote tree on every retry, then use a lease
  # so a concurrent publisher cannot be silently overwritten.
  if ! remote_head=$(git -C "$TMP" ls-remote --heads origin refs/heads/gh-pages); then
    echo "[pages] WARN: cannot read current Pages tree; retaining the published site"
    sleep $((6 * n))
    continue
  fi
  prior=''
  if [ -n "$remote_head" ]; then
    if ! git -C "$TMP" fetch -q --depth=1 --filter=blob:none origin gh-pages; then
      echo "[pages] WARN: cannot fetch current Pages tree; retaining the published site"
      sleep $((6 * n))
      continue
    fi
    prior=$(git -C "$TMP" rev-parse FETCH_HEAD)
    if ! python3 "$PUBLISH_SCRIPT_DIR/preserve_completed_parser_view.py" --site "$TMP" --ref "$prior"; then
      echo "[pages] ERROR: cannot preserve completed parser results; retaining the published site"
      exit 1
    fi
  fi
  git -C "$TMP" add -A
  if ! git -C "$TMP" diff --cached --quiet; then
    git -C "$TMP" commit -qm "Preserve published completed parser review"
  fi
  if git -C "$TMP" push -q --force-with-lease="refs/heads/gh-pages:$prior" origin gh-pages; then
    echo "[pages] published https://SRoyaltyy.github.io/fullscan/dashboard/ (attempt ${n})"
    exit 0
  fi
  echo "[pages] WARN: gh-pages push failed (${n}/${ATTEMPTS}) — another deploy holds the ref; retry"
  sleep $((6 * n))
done
echo "[pages] ERROR: gh-pages publication failed after ${ATTEMPTS} attempts — previous site retained"
exit 1
