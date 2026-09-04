#!/usr/bin/env bash
# Publish dashboard/ (buy/sell paper book) to the gh-pages branch.
# Used by Pre-Open ALL and Stock Book ALL so suggestions land on
# https://SRoyaltyy.github.io/fullscan/dashboard/ without waiting
# for a second workflow. Never prints tokens. Never fails the job
# if Pages cannot be updated — the git commit already saved the files.
set -uo pipefail

export HOME="${HOME:-${FULLSCAN_HOME:-/home/gha}}"
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE || true
if [ -n "${GITHUB_WORKSPACE:-}" ] && [ -d "${GITHUB_WORKSPACE}/.git" ]; then
  cd "$GITHUB_WORKSPACE"
fi

if [ ! -f dashboard/index.html ]; then
  echo "[pages] WARN: dashboard/index.html missing — not touching gh-pages"
  exit 0
fi

TOKEN="${GITHUB_TOKEN:-${GH_TOKEN:-}}"
REPO="${GITHUB_REPOSITORY:-SRoyaltyy/fullscan}"
if [ -z "$TOKEN" ]; then
  echo "[pages] WARN: no GITHUB_TOKEN — skip gh-pages push"
  exit 0
fi

mkdir -p pages_out/dashboard
touch pages_out/.nojekyll
cp -a dashboard/. pages_out/dashboard/
cp dashboard/index.html pages_out/index.html

for sub in boring-winners ticker-lookback gainer-lookback mover-lookback sleeve-combine mover-paper book-paper; do
  if [ -f "dashboard/${sub}/index.html" ]; then
    mkdir -p "pages_out/dashboard/${sub}"
    cp -a "dashboard/${sub}/." "pages_out/dashboard/${sub}/"
  fi
done

TMP=$(mktemp -d)
cp -a pages_out/. "$TMP/"
touch "$TMP/.nojekyll"
git -C "$TMP" init
git -C "$TMP" checkout -b gh-pages
git -C "$TMP" config user.name "Market-Bot-Automaton"
git -C "$TMP" config user.email "bot@users.noreply.github.com"
git -C "$TMP" add -A
if git -C "$TMP" diff --cached --quiet; then
  echo "[pages] nothing new to publish"
  rm -rf "$TMP"
  exit 0
fi
git -C "$TMP" commit -m "deploy dashboard ${GITHUB_SHA:-local}"
git -C "$TMP" remote add origin \
  "https://x-access-token:${TOKEN}@github.com/${REPO}.git"
if git -C "$TMP" push --force origin gh-pages; then
  echo "[pages] published https://SRoyaltyy.github.io/fullscan/dashboard/"
else
  echo "[pages] WARN: gh-pages push failed — files are on main"
fi
rm -rf "$TMP"
exit 0
