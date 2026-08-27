# Book lookback — 2026-08-20

_Generated 2026-08-27T02:44:57.759580-04:00_

Question: **on this trading day, before 09:30 ET, what did the pipeline
that feeds the Stock Book Ranker know about a name — and for names that
then went up 1d/2d/3d/1w, did ANY input fire?**

Honest timing: the ranker file itself is usually written **after the open**.
News / judge / digest / events / map-heat / predicts / catalyst are the
pre-09:30 packet. Join, AB, peers, weather may land later the same day
and still moved that day's book.

Classes: **in_buy_book** = ranker picked it · **outweighed** = something
fired but the name was not in a buy book · **gated_out** = micro / <$400M
· **blind** = no ticker-specific signal (news, AB, peers, heat, digest,
catalyst, volume spike).

## Inputs present for this date

| Resource | Found | Lands in |
|----------|-------|----------|
| Finviz Elite export | yes | liquidity + labels + AB proxy + digest |
| Labels / membership | yes | join + mid_opp + earnings/range |
| Weather (tape + FRED/DXY/VIX) | yes | join × weather |
| Channel 1 raw | yes | via weather |
| Join ranked universe | yes | s_join |
| News parse + actions | yes | s_news |
| News judge | yes | s_news ticker tilts |
| Finviz daily digest | yes | s_news company headlines |
| General predict | yes | s_general × beta |
| Sector LLM essays | yes | s_sector (0 if essays missing) |
| AB checklist + P01–P04 | yes | s_ab |
| Peer RS | yes | s_peer |
| Ticker checklist (rebound) | yes | rebound_floor (dated file, else latest — can be stale) |
| Event scanner | yes | sector tilt + weather |
| Map heat research | NO | s_heat nested override + captains |
| Catalyst overlays | NO | not in ranker — separate chart workflow |
| Insider / politician flow | NO | no daily file in repo |
| Industry predict | yes | not scored (ad-hoc only) |
| Learnings / mutable policy | yes | next predict prompt, not a ticker score |
| Catalyst dossiers | NO | pre-open layer 3; merged into news actions |
| Map heat tables | NO | pre-open overlay / post-close tables |
| Captain research | NO | s_heat captains — missing → bootstrap stub |
| Stock book CSV | yes | ranker snapshot (usually written after the open) |

## Winners — did the ranker see *something*?

### 1d (next 1 session(s))

20 names ≥ threshold · 0 already in a buy book · **2 blind**.

| Ticker | fwd% | class | size | sector | what fired |
|--------|------|-------|------|--------|------------|
| USDE | +85.2 | gated_out | micro | Financial | s_ab=-0.12, relvol=2.35 |
| CAN | +27.2 | gated_out | micro | Technology | s_ab=+0.24, s_peer=+1.00, relvol=11.27 |
| CRML | +22.6 | outweighed | small | Basic Materials | s_ab=-0.36, s_peer=-0.96 |
| ARCT | +22.4 | gated_out | small | Healthcare | s_ab=+0.64, s_peer=+1.00, relvol=4.53 |
| TMC | +20.6 | outweighed | small | Basic Materials | s_ab=-0.24, s_peer=-0.97 |
| CYPH | +19.3 | gated_out | micro | Healthcare | s_ab=+0.76, s_peer=+1.00, relvol=2.92 |
| ELMT | +19.3 | outweighed | small | Industrials | s_ab=+0.46 |
| SLS | +15.4 | outweighed | mid | Healthcare | s_ab=+0.64, s_peer=+0.95 |
| AIFC | +15.2 | gated_out | micro | Technology | s_ab=+0.12, relvol=4.11 |
| IQMX | +14.8 | blind | small | Technology | — |
| TII | +14.6 | gated_out | micro | Basic Materials | s_ab=+0.24 |
| HQ | +14.5 | blind | small | Technology | — |
| BKKT | +14.5 | gated_out | small | Technology | s_ab=+0.24, s_peer=-0.08, relvol=3.29 |
| UEC | +14.4 | outweighed | mid | Energy | s_ab=-0.12, s_peer=+0.18 |
| EU | +14.3 | gated_out | micro | Energy | s_ab=-0.64, s_peer=-0.70 |
| LAR | +14.2 | outweighed | small | Basic Materials | s_ab=-0.55, s_peer=-0.76 |
| GUTS | +13.9 | gated_out | micro | Healthcare | s_ab=+0.36, s_peer=-0.54 |
| HOOD | +13.7 | outweighed | large | Financial | s_ab=+0.36, s_peer=-0.64 |
| PROK | +13.4 | outweighed | small | Healthcare | s_ab=+0.46, s_peer=+0.22, relvol=1.62 |
| ASST | +13.0 | outweighed | small | Financial | s_peer=+1.00, relvol=2.94 |

### 2d (next 2 session(s))

_No realized winners at this gain threshold (or prices not in yet)._

### 3d (next 3 session(s))

_No realized winners at this gain threshold (or prices not in yet)._

### 1w (next 5 session(s))

_No realized winners at this gain threshold (or prices not in yet)._

## Files

- `data/stock_book/2026-08-20_lookback.json`
- `01_daily/2026-08-20_lookback.md`
- `03_scoreboard/BOOK_LOOKBACK.md` (this report, latest run)

