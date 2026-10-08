# Live ticket 09:30 lock

`manifest.jsonl` is append-only. Rows are never edited or removed.

- `seal`: the bytes `data/factor_mine/strategy_tickets.json` held once its
  session locked (09:30 ET or the paper-send journal). Written by
  `src.strategy_tickets.write` through `src.ticket_session_lock.seal_locked`.
- `void`: a body that differs from the seal and is on (or was on) disk. It is
  recorded, not valid, and it is not reverted.
- `seal_reference` / `note`: documentation only.

Rule (`src/ticket_session_lock.py`): a live copy holding session D takes
another D body only before D's 09:30 ET and before D's send journal. A newer
session may replace it (the next morning). An older session never does.
After the lock, rebuilds go only to
`data/day_board/<D>_strategy_tickets_draft.json`, marked `draft: true` and
`not_for_trading: true`.

## 2026-10-07

Post-close runs at 17:11, 20:24 and 20:32:57 ET (579936ff, 5949b934,
ed32867f) rewrote `data/factor_mine/strategy_tickets.json` (still dated
2026-10-07) from the 2026-10-07 close panel. **Those regenerations are void.**

- Authoritative send-time copy: **907da2b3** (generated 06:27:09 ET), byte
  identical to the sealed dated file
  `data/day_board/2026-10-07_strategy_tickets.json` (sha256 `521f47b9…`).
- Copy on main at the close: **c608437f** (generated 16:55:05 ET from the
  #512 pre-open look, panel_bake_date 2026-10-06).
- The status page (`data/strategy_status/2026-10-07.json`, read from the dated
  file) and the sealed 2026-10-07 Factor Mine ledgers did not change.
- Baseline: the lock was added after this session and the file on main fails
  it, so 2026-10-07 is sealed on the latest pre-09:30 ET commit for that file
  (907da2b3). The current body is covered by a `void` row and is left as is;
  the 2026-10-08 morning write replaces it.
