# Factor-mine dashboard is FROZEN

Cyrus 2026-09-22: calendar + start-day equity chart are done.
Research PRs kept regenerating `dashboard/factor-mine/index.html` and wiping them.

## Frozen paths — do not edit unless Cyrus names this file

- `dashboard/factor-mine/index.html`
- `dashboard/factor-mine/*.html`
- `src/factor_mine.py` dashboard HTML emitters / chart JS
- `src/strategy_board.py` page shell if it writes the same UI

Allowed: `today.json`, `hold_live_px.json`, ticket JSON, panel data.
Forbidden: replacing the page, removing the calendar, making the line chart ignore start-day.

PR #298 (merged) is the last UI change. News-impact / theme-radar / Lane PRs must not touch these paths.

Grok Bot / Cursor: if a diff includes `dashboard/factor-mine/index.html`, drop that file from the commit.
