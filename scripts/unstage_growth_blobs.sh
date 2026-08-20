#!/usr/bin/env bash
# Unstage bulky generated artifacts after `git add -A`.
# Used by daily_pipeline / weekly_consolidation / monthly_distillation
# so those jobs cannot keep growing ohlc.parquet, dated Finviz dumps, or
# 01_daily/_transcripts/.
set +e
while IFS= read -r f; do
  [ -n "$f" ] || continue
  git restore --staged -- "$f" 2>/dev/null || true
done < <(git diff --cached --name-only | grep -E \
  '^(01_daily/_transcripts/|data/prices/ohlc\.parquet$|data/exports/finviz_[0-9]{4}-[0-9]{2}-[0-9]{2}\.csv$)')
exit 0
