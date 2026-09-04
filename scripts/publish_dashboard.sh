#!/usr/bin/env bash
# Publish dashboard/ (buy/sell paper book) to the gh-pages branch.
# Used by Pre-Open ALL and Stock Book ALL so suggestions land on
# https://SRoyaltyy.github.io/fullscan/dashboard/ without waiting
# for a second workflow. Never prints tokens. Never fails the job
# if Pages cannot be updated — the git commit already saved the files.
#
# 2026-09-10: deploy-dashboard.yml (peaceiris force_orphan) and this
# script force-pushed gh-pages at the same second →
# `cannot lock ref 'refs/heads/gh-pages'`. Both paths now go through
# here: PAGES_OUT_DIR=<dir> publishes a prebuilt tree, and the push
# retries with backoff instead of giving up after one 8s nap.
set -uo pipefail

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

SUBS="boring-winners ticker-lookback gainer-lookback flatten-lookback mover-lookback sleeve-combine sleeve-merge mover-paper book-paper strategy-board factor-mine day-board down-day-mine"

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

# Every sub-dashboard on main rides along so an orphan deploy cannot 404
# a page another job just published.
for sub in $SUBS; do
  if [ -f "dashboard/${sub}/index.html" ] && [ ! -f "$TMP/dashboard/${sub}/index.html" ]; then
    mkdir -p "$TMP/dashboard/${sub}"
    cp -a "dashboard/${sub}/." "$TMP/dashboard/${sub}/"
  fi
  # Root copy: the paper book is also published at /fullscan/ so
  # href="sleeve-merge/" from the homepage must not 404.
  # deploy-dashboard.yml also mirrors into pages_out/${sub}.
  if [ -d "$TMP/dashboard/${sub}" ]; then
    mkdir -p "$TMP/${sub}"
    cp -a "$TMP/dashboard/${sub}/." "$TMP/${sub}/"
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
  if git -C "$TMP" push -q --force origin gh-pages; then
    echo "[pages] published https://SRoyaltyy.github.io/fullscan/dashboard/ (attempt ${n})"
    exit 0
  fi
  echo "[pages] WARN: gh-pages push failed (${n}/${ATTEMPTS}) — another deploy holds the ref; retry"
  sleep $((6 * n))
done
echo "[pages] WARN: gh-pages push failed after ${ATTEMPTS} attempts — files are on main"
exit 0
