# hot_n4_clean_v1 results

The jump check halted the window on 2026-08-13, the first session. No day card was written. No recipe was scored. Luck N stays 22,011.

## What halted

The 2026-08-13 candidate list includes MVIS and SION. The bars read for those names, the same 60-session window `ohlc.features` passes to `from_bars`, contain an open more than 3 times away from the previous stored close. The pinned retro split table has no row for either ticker. Section 5 says an unexplained leg halts the window: `days/D.json` is not written and the ledger is not appended.

| ticker | jumping bar | leg | previous stored bar | open | previous close | ratio | how it entered the list |
| --- | --- | --- | --- | --- | --- | --- | --- |
| MVIS | 2026-08-03 | open over previous close | 2026-07-30 | 4.06 | 0.24 | 16.92 | yesterday gainer and yesterday mover |
| SION | 2026-08-10 | open over previous close | 2026-08-07 | 4.85 | 51.04 | 0.095 | yesterday gainer, yesterday mover, and probable |

The missing-bar count for 2026-08-13 matches the lock: 21 tickers, 3 of them inside the liquid gate. That count is separate from this halt.

The four legs named in `SPLITS.md` (XHG on 2026-08-13, EYPT on 2026-08-17, SMJF on 2026-08-27, ADBT on 2026-09-03) are not the legs that stopped this session. XHG is not liquid on 2026-08-13, and the other three bars are later than this session.

## Sessions

All 31 sessions, 2026-08-13 through 2026-09-25, are unwritten.

- 2026-08-13 halted on the MVIS and SION legs above.
- 2026-08-14 through 2026-09-25 were not written. A later session file requires every earlier session file, and the ledger has to stay a prefix of the session list with no gap.
- 2026-08-28 was not reached. That session still has no frozen row, so it would have taken no new buy. The walk did not get there.

## Scores

There is no total return, trade count, win rate, Wilson interval, return without the best stock, IWM line, or RANDOM4 line for either recipe. Both windows are unscored.

IRONCLAD rule 21 is not met. The keep bar is at least 30 closed trades and a win rate above 55% after fees. This run has no closed trades.

IRONCLAD rule 23 is not met. That bar is about 20 locked sessions that beat RANDOM4 and IWM after fees, including without the best stock. This run has no scored sessions. Every session in the window is also before the study date 2026-09-27, so each one is `designed_after` and would stay out of a real-money record even after a score.
