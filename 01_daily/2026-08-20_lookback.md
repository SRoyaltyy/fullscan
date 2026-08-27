# Book lookback — 2026-08-20

_Generated 2026-08-27T10:02:36.183802-04:00_

Winner bar: **3% per session** (1d ≥ 3% · 2d ≥ 6% · 3d ≥ 9% · 1w ≥ 15%).

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

Boxes: 🟢 good (helped / fired bullish) · 🟡 neutral (present, flat) · 🔴 bad (fired against the name) · ⬛ missing (that day's file was not there).

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

### 1d (next 1 session(s), bar 3%)

80 names ≥ 3% · 1 already in a buy book · **3 blind**.

| Ticker | fwd | join | sect | gen | news | dig | jdg | AB | peer | heat | vol | cat | buy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| USDE | +85.2% | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟡 | ⬛ | 🟢 | ⬛ | 🟡 |
| CAN | +27.2% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| CRML | +22.6% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| ARCT | +22.4% | 🟡 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| TMC | +20.6% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| CYPH | +19.3% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| ELMT | +19.3% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| SLS | +15.4% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟡 | ⬛ | 🟡 |
| AIFC | +15.2% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🟢 | ⬛ | 🟡 |
| IQMX | +14.8% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| TII | +14.6% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| HQ | +14.5% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| BKKT | +14.5% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🟢 | ⬛ | 🟡 |
| UEC | +14.4% | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟢 | ⬛ | 🔴 | ⬛ | 🟡 |
| EU | +14.3% | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| LAR | +14.2% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| GUTS | +13.9% | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| HOOD | +13.7% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| PROK | +13.4% | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| ASST | +13.0% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| ALOY | +12.9% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟡 | ⬛ | 🟡 | ⬛ | 🟡 |
| USAR | +12.6% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟡 | ⬛ | 🟡 | ⬛ | 🟡 |
| OI | +12.5% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| INFQ | +12.5% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| EZPW | +12.0% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟡 |
| DEFT | +11.7% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🟡 | ⬛ | 🟡 |
| DK | +11.6% | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| SGML | +11.6% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| RGTI | +11.5% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| DNN | +11.5% | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| VIRT | +11.0% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| BLMN | +11.0% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| LPTH | +10.8% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟢 | ⬛ | 🔴 | ⬛ | 🟡 |
| JELD | +10.6% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟡 | ⬛ | 🟡 |
| ABAT | +10.3% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| CVI | +10.3% | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟡 |
| EOSE | +10.1% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟢 | ⬛ | 🟡 |
| SID | +10.1% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟢 | ⬛ | 🔴 | ⬛ | 🟡 |
| SENS | +10.1% | 🟡 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🟢 | ⬛ | 🟡 | ⬛ | 🟡 |
| BCAR | +9.9% | 🟡 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| FSLY | +9.9% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| PACB | +9.8% | 🟡 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟡 | ⬛ | 🟡 |
| FUTU | +9.7% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| QUBT | +9.6% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| IE | +9.5% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| NEXA | +9.4% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| ERO | +9.4% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| CHGG | +9.3% | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| QMLS | +9.3% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🟡 | ⬛ | 🟡 | ⬛ | 🟡 |
| MB | +9.3% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| SLI | +9.2% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| RZLT | +9.2% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟡 |
| ABUS | +9.2% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟡 |
| MP | +9.1% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🟡 | ⬛ | 🟡 | ⬛ | 🟡 |
| TMQ | +9.1% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| PARR | +9.1% | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| TEM | +9.1% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| BTDR | +9.0% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| UUUU | +8.9% | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| BGC | +8.9% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| PSNL | +8.9% | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| MRNA | +8.9% | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| ACDC | +8.8% | 🟡 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| NMG | +8.7% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| SCCO | +8.7% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| EROC | +8.6% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🟡 | ⬛ | 🟡 |
| QMCO | +8.6% | 🟡 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| AXGN | +8.5% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| QBTS | +8.5% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| FIGR | +8.4% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🟡 | ⬛ | 🟢 |
| CNH | +8.4% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| XNDU | +8.3% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| TRLV | +8.3% | 🟡 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🟡 | ⬛ | 🟡 |
| CSAN | +8.3% | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🔴 | ⬛ | 🟢 | ⬛ | 🟡 |
| ONT | +8.2% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| COIN | +8.2% | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| DCH | +8.2% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| NB | +8.2% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| IONQ | +8.0% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| AGEN | +8.0% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟡 |

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
| ALOY | +12.9 | outweighed | small | Basic Materials | s_ab=-0.24 |
| USAR | +12.6 | outweighed | mid | Basic Materials | s_ab=-0.36 |
| OI | +12.5 | outweighed | small | Consumer Cyclical | s_ab=-0.70, s_peer=-0.68 |
| INFQ | +12.5 | outweighed | mid | Technology | s_ab=-0.12 |
| EZPW | +12.0 | outweighed | small | Financial | s_ab=+0.36, s_peer=+0.35 |
| DEFT | +11.7 | gated_out | micro | Financial | s_ab=+0.46 |
| DK | +11.6 | outweighed | mid | Energy | s_ab=+0.64, s_peer=-0.77 |
| SGML | +11.6 | outweighed | small | Basic Materials | s_ab=-0.36, s_peer=-0.65 |
| RGTI | +11.5 | outweighed | mid | Technology | s_ab=-0.46, s_peer=-0.89 |
| DNN | +11.5 | outweighed | mid | Energy | s_peer=-0.21 |
| VIRT | +11.0 | outweighed | mid | Financial | s_ab=+0.64, s_peer=+0.50, relvol=3.82 |
| BLMN | +11.0 | outweighed | small | Consumer Cyclical | s_peer=-0.09 |
| LPTH | +10.8 | outweighed | small | Technology | s_ab=-0.24, s_peer=+0.62 |
| JELD | +10.6 | gated_out | micro | Industrials | s_ab=+0.12, s_peer=+0.69 |
| ABAT | +10.3 | gated_out | small | Industrials | s_ab=-0.36, s_peer=-0.76 |
| CVI | +10.3 | outweighed | mid | Energy | s_ab=+0.70, s_peer=+0.10 |
| EOSE | +10.1 | outweighed | small | Industrials | s_ab=-0.70, s_peer=-0.95, relvol=1.5 |
| SID | +10.1 | outweighed | small | Basic Materials | s_ab=-0.36, s_peer=+0.74 |
| SENS | +10.1 | outweighed | small | Healthcare | s_peer=+0.97 |
| BCAR | +9.9 | gated_out | micro | Financial | s_ab=-0.12 |
| FSLY | +9.9 | outweighed | mid | Technology | s_peer=-0.98 |
| PACB | +9.8 | gated_out | small | Healthcare | s_ab=+0.24, s_peer=+0.42 |
| FUTU | +9.7 | outweighed | large | Financial | s_ab=+0.55, s_peer=+0.79, relvol=2.22 |
| QUBT | +9.6 | outweighed | small | Technology | s_ab=-0.55, s_peer=-0.44 |
| IE | +9.5 | outweighed | small | Basic Materials | s_ab=+0.12, s_peer=-0.63 |
| NEXA | +9.4 | outweighed | small | Basic Materials | s_ab=-0.12, s_peer=-0.65 |
| ERO | +9.4 | outweighed | mid | Basic Materials | s_ab=+0.70, s_peer=-0.35 |
| CHGG | +9.3 | gated_out | micro | Consumer Defensive | s_ab=-0.46, s_peer=-0.63 |
| QMLS | +9.3 | gated_out | micro | Technology | — |
| MB | +9.3 | gated_out | micro | Consumer Cyclical | s_ab=-0.36 |
| SLI | +9.2 | outweighed | small | Basic Materials | s_ab=+0.24, s_peer=-0.36 |
| RZLT | +9.2 | outweighed | small | Healthcare | s_ab=+0.76, s_peer=+0.17 |
| ABUS | +9.2 | outweighed | small | Healthcare | s_ab=+0.70, s_peer=+0.09 |
| MP | +9.1 | blind | mid | Basic Materials | — |
| TMQ | +9.1 | outweighed | small | Basic Materials | s_ab=-0.24, s_peer=-0.88 |
| PARR | +9.1 | outweighed | mid | Energy | s_ab=+0.64, s_peer=-0.94 |
| TEM | +9.1 | outweighed | large | Healthcare | s_ab=+0.55, s_peer=+0.96, relvol=5.44 |
| BTDR | +9.0 | outweighed | mid | Technology | s_ab=+0.24, s_peer=+0.95, relvol=2.78 |
| UUUU | +8.9 | outweighed | mid | Energy | s_ab=-0.46, s_peer=-0.34 |
| BGC | +8.9 | outweighed | mid | Financial | s_ab=+0.46 |
| PSNL | +8.9 | outweighed | small | Healthcare | s_ab=+0.46, s_peer=+0.81, relvol=6.42 |
| MRNA | +8.9 | outweighed | large | Healthcare | s_ab=+0.46, s_peer=+1.00, relvol=13.48 |
| ACDC | +8.8 | outweighed | small | Energy | s_ab=-0.64, s_peer=-0.98 |
| NMG | +8.7 | outweighed | small | Basic Materials | s_ab=+0.24, s_peer=-0.92 |
| SCCO | +8.7 | outweighed | large | Basic Materials | s_ab=+0.46, s_peer=-0.34 |
| EROC | +8.6 | outweighed | mid | Industrials | s_ab=+0.46 |
| QMCO | +8.6 | outweighed | small | Technology | s_ab=+0.12, s_peer=-0.92 |
| AXGN | +8.5 | outweighed | mid | Healthcare | s_ab=+0.12, s_peer=-0.63 |
| QBTS | +8.5 | outweighed | mid | Technology | s_ab=-0.46, s_peer=-0.72 |
| FIGR | +8.4 | in_buy_book | mid | Financial | s_ab=+0.55 |
| CNH | +8.4 | outweighed | large | Industrials | s_ab=+0.55, s_peer=+0.57, relvol=2.61 |
| XNDU | +8.3 | outweighed | mid | Technology | s_ab=+0.12 |
| TRLV | +8.3 | outweighed | small | Healthcare | s_ab=+0.36 |
| CSAN | +8.3 | outweighed | mid | Energy | s_peer=-0.41, relvol=2.92 |
| ONT | +8.2 | outweighed | small | Industrials | s_ab=-0.55 |
| COIN | +8.2 | outweighed | large | Financial | s_ab=+0.46, s_peer=+0.85, relvol=3.1 |
| DCH | +8.2 | outweighed | small | Consumer Cyclical | s_ab=+0.24 |
| NB | +8.2 | outweighed | small | Basic Materials | s_ab=+0.12, s_peer=-0.99 |
| IONQ | +8.0 | outweighed | large | Technology | s_ab=-0.55, s_peer=-0.36 |
| AGEN | +8.0 | gated_out | small | Healthcare | s_ab=+0.70, s_peer=+0.65 |

### 2d (next 2 session(s), bar 6%)

_No realized winners at this gain threshold (or prices not in yet)._

### 3d (next 3 session(s), bar 9%)

_No realized winners at this gain threshold (or prices not in yet)._

### 1w (next 5 session(s), bar 15%)

_No realized winners at this gain threshold (or prices not in yet)._

## Book picks — scores that day

What the ranker actually put in buy/sell, with every layer score and the same color boxes.
`book` is the combined ranker score. 1d/2d/3d/1w are realized close-to-close after the signal.
1w uses the last available close if a full 5 sessions are not in yet.

### 1d book

**BUY** (15)

| # | Ticker | book | join | sect | gen | news | AB | peer | heat | 1d | 2d | 3d | 1w |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | ELF | +1.123 | +0.87 | +0.00 | +0.47 | +0.00 | +0.56 | +0.81 | +0.00 | +3.5% | +7.6% | +7.4% | +8.6% |
| 2 | MOS | +1.066 | +0.98 | +0.00 | +0.23 | +0.00 | +0.64 | +0.05 | +0.00 | +4.5% | +2.8% | +3.9% | +0.2% |
| 3 | AUPH | +1.024 | +0.93 | +0.00 | +0.47 | +0.00 | +0.81 | +0.76 | +0.00 | -3.6% | -4.0% | -3.0% | -4.4% |
| 4 | CE | +1.020 | +0.95 | +0.00 | +0.07 | +0.00 | +0.64 | +0.51 | +0.00 | -0.4% | -3.7% | -5.2% | -6.2% |
| 5 | OCUL | +1.019 | +0.54 | +0.00 | +0.23 | +0.00 | +0.70 | +0.80 | +0.00 | -0.1% | -2.8% | -1.9% | -3.9% |
| 6 | EPAM | +1.015 | -0.15 | +0.40 | +0.47 | +0.00 | +0.46 | +0.80 | +0.00 | +3.4% | +4.2% | +4.8% | +5.4% |
| 7 | WRBY | +1.005 | +0.90 | +0.00 | +0.47 | +0.00 | +0.76 | +0.75 | +0.00 | -0.1% | -4.5% | -5.3% | -4.8% |
| 8 | CELH | +1.000 | +0.73 | +0.00 | +0.23 | +0.00 | +0.64 | +0.87 | +0.00 | +2.5% | +7.7% | +8.6% | +3.9% |
| 9 | IRTC | +0.994 | +0.82 | +0.00 | +0.47 | +0.00 | +0.46 | +0.51 | +0.00 | +0.4% | -2.1% | -3.8% | -7.9% |
| 10 | CALX | +0.986 | +0.01 | +0.40 | +0.23 | +0.00 | +0.36 | +0.79 | +0.00 | -0.6% | -1.4% | -1.3% | -5.8% |
| 11 | NHI | +0.971 | +0.75 | -0.24 | +0.07 | +0.00 | +0.81 | +0.28 | +0.00 | -0.7% | -1.1% | -0.8% | -1.5% |
| 12 | GSHD | +0.964 | +0.91 | +0.00 | +0.47 | +0.00 | +0.81 | +0.47 | +0.00 | +2.6% | +3.2% | +2.3% | +0.5% |
| 13 | OGS | +0.945 | +0.81 | +0.00 | +0.07 | +0.00 | +0.85 | +0.15 | +0.00 | -3.1% | -1.3% | -1.2% | -2.1% |
| 14 | FIGR | +0.941 | +0.89 | +0.00 | +0.19 | +0.00 | +0.56 | +0.00 | +0.00 | +8.4% | +7.2% | +14.1% | +3.7% |
| 15 | HLNE | +0.938 | +0.94 | +0.00 | +0.23 | +0.00 | +0.46 | +0.25 | +0.00 | +1.2% | +1.1% | +2.7% | +4.1% |

| Ticker | join | sect | gen | news | dig | jdg | AB | peer | heat | vol | cat | buy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ELF | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| MOS | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟢 |
| AUPH | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟢 |
| CE | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| OCUL | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| EPAM | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| WRBY | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| CELH | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| IRTC | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| CALX | 🟡 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| NHI | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| GSHD | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| OGS | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| FIGR | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🟡 | ⬛ | 🟢 |
| HLNE | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |

| Ticker | size | sector | reasons |
|--------|------|--------|---------|
| ELF | mid | Consumer Defensive | `join=+0.87; gen1d=+0.47; ab=+0.55; LEAD,peers↓,ind↓; peer=+0.81; mid_opp=+0.60; rebound_floor` |
| MOS | mid | Basic Materials | `join=+0.98; gen1d=+0.23; ab=+0.64; LEAD,peers↑,ind↑; peer=+0.05; mid_opp=+0.68; rebound_floor` |
| AUPH | mid | Healthcare | `join=+0.93; gen1d=+0.47; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.76; mid_opp=+0.52` |
| CE | mid | Basic Materials | `join=+0.95; gen1d=+0.07; ab=+0.64; LEAD,peers↑,ind↑; peer=+0.51; mid_opp=+0.64` |
| OCUL | mid | Healthcare | `join=+0.54; gen1d=+0.23; ab=+0.70; LEAD,peers↑,ind↑; peer=+0.80; mid_opp=+0.60` |
| EPAM | mid | Technology | `join=-0.15; sector1d=+0.40; gen1d=+0.47; ab=+0.46; LEAD,peers↓,ind↓; peer=+0.80; mid_opp=+0.68` |
| WRBY | mid | Healthcare | `join=+0.90; gen1d=+0.47; ab=+0.76; LEAD,peers↓,ind↑; peer=+0.75; mid_opp=+0.52` |
| CELH | mid | Consumer Defensive | `join=+0.73; gen1d=+0.23; ab=+0.64; LEAD,peers↑,ind↑; peer=+0.87; mid_opp=+0.56` |
| IRTC | mid | Healthcare | `join=+0.82; gen1d=+0.47; ab=+0.46; LEAD,peers↓,ind↓; peer=+0.51; mid_opp=+0.64` |
| CALX | mid | Technology | `sector1d=+0.40; gen1d=+0.23; ab=+0.36; LEAD,peers↓,ind↓; peer=+0.78; mid_opp=+0.68` |
| NHI | mid | Real Estate | `join=+0.75; sector1d=-0.24; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.28; mid_opp=+0.64` |
| GSHD | mid | Financial | `join=+0.91; gen1d=+0.47; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.47; mid_opp=+0.52` |
| OGS | mid | Utilities | `join=+0.81; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.15; mid_opp=+0.60` |
| FIGR | mid | Financial | `join=+0.89; gen1d=+0.19; ab=+0.55; ind↓; mid_opp=+0.68` |
| HLNE | mid | Financial | `join=+0.95; gen1d=+0.23; ab=+0.46; LEAD,peers↓,ind↓; peer=+0.25; mid_opp=+0.64` |

**SELL** (15)

| # | Ticker | book | join | sect | gen | news | AB | peer | heat | 1d | 2d | 3d | 1w |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | GEV | -0.531 | -0.89 | -0.24 | +0.23 | +0.00 | -0.24 | -0.69 | +0.00 | -0.9% | -2.5% | -4.1% | -1.7% |
| 2 | INTC | -0.446 | -0.89 | +0.40 | +0.47 | -0.31 | -0.12 | -0.84 | +0.00 | -2.2% | -5.3% | -5.0% | -2.7% |
| 3 | AUR | -0.410 | -0.96 | -0.80 | +0.47 | +0.00 | -0.46 | -0.83 | +0.00 | +2.0% | -7.0% | -5.7% | -5.6% |
| 4 | ARM | -0.381 | -0.70 | +0.40 | +0.47 | -0.31 | +0.00 | -0.79 | +0.00 | -3.0% | -4.8% | -3.6% | +5.5% |
| 5 | META | -0.375 | -0.80 | +0.00 | +0.23 | +0.00 | -0.36 | -0.74 | +0.00 | +0.8% | +2.4% | +4.4% | +6.4% |
| 6 | SW | -0.371 | -0.61 | -0.80 | +0.23 | +0.00 | -0.12 | -0.18 | +0.00 | +2.5% | +1.0% | +2.6% | +2.0% |
| 7 | DELL | -0.370 | -0.15 | +0.40 | +0.47 | +0.00 | +0.24 | -0.75 | +0.00 | +1.7% | -0.4% | +3.9% | +7.6% |
| 8 | BWEN | -0.349 | -0.74 | -0.24 | +0.47 | +0.00 | -0.36 | -0.92 | +0.00 | -1.1% | -2.4% | -2.9% | +2.4% |
| 9 | PKG | -0.340 | -0.63 | -0.80 | +0.23 | +0.00 | +0.00 | -0.16 | +0.00 | +1.3% | -0.3% | -1.2% | -2.2% |
| 10 | MDB | -0.326 | -0.61 | +0.40 | +0.47 | +0.00 | +0.00 | -0.80 | +0.00 | +2.5% | -4.2% | -3.7% | +3.7% |
| 11 | RY | -0.323 | +0.59 | +0.00 | +0.07 | +0.00 | -0.12 | -0.14 | +0.00 | +0.0% | -0.6% | +0.9% | -1.1% |
| 12 | PANW | -0.318 | -0.46 | +0.40 | +0.23 | +0.00 | +0.12 | -0.06 | +0.00 | +2.4% | +0.4% | -2.8% | +7.2% |
| 13 | WMT | -0.312 | +0.13 | +0.00 | +0.07 | -0.29 | -0.12 | -0.85 | +0.00 | +0.1% | +2.8% | +1.7% | -0.7% |
| 14 | BIDU | -0.312 | -0.95 | +0.00 | +0.07 | +0.00 | -0.56 | -0.87 | +0.00 | +1.4% | +0.2% | +1.6% | +4.8% |
| 15 | SES | -0.308 | -0.71 | -0.80 | +0.23 | +0.00 | -0.56 | -0.91 | +0.00 | +6.1% | +6.1% | +7.1% | +7.6% |

| Ticker | join | sect | gen | news | dig | jdg | AB | peer | heat | vol | cat | buy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| GEV | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| INTC | 🔴 | 🟢 | 🟢 | 🔴 | 🟢 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| AUR | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| ARM | 🔴 | 🟢 | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| META | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| SW | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| DELL | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| BWEN | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| PKG | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| MDB | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| RY | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| PANW | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| WMT | 🟢 | 🟡 | 🟢 | 🔴 | 🟡 | 🔴 | 🔴 | 🔴 | ⬛ | 🟢 | ⬛ | 🟡 |
| BIDU | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟢 | ⬛ | 🟡 |
| SES | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟢 | ⬛ | 🟡 |

| Ticker | size | sector | reasons |
|--------|------|--------|---------|
| GEV | mega | Industrials | `join=-0.88; sector1d=-0.24; gen1d=+0.23; ab=-0.24; LAG,peers↓,ind↓; peer=-0.69` |
| INTC | mega | Technology | `join=-0.88; sector1d=+0.40; gen1d=+0.47; news=-0.31; ev=finviz_digest; ab=-0.12; LAG,peers↓,ind↓; peer=-0.84` |
| AUR | large | Consumer Cyclical | `join=-0.96; sector1d=-0.80; gen1d=+0.47; ab=-0.46; LAG,peers↓,ind↓; peer=-0.83` |
| ARM | mega | Technology | `join=-0.70; sector1d=+0.40; gen1d=+0.47; news=-0.31; ev=finviz_digest; peer=-0.79` |
| META | mega | Communication Services | `join=-0.80; gen1d=+0.23; ab=-0.36; LAG,peers↓,ind↓; peer=-0.74` |
| SW | large | Consumer Cyclical | `join=-0.61; sector1d=-0.80; gen1d=+0.23; ab=-0.12; LAG,peers↓,ind↓; peer=-0.18` |
| DELL | mega | Technology | `join=-0.15; sector1d=+0.40; gen1d=+0.47; ab=+0.24; LAG,peers↓,ind↓; peer=-0.75` |
| BWEN | micro | Industrials | `join=-0.74; sector1d=-0.24; gen1d=+0.47; ab=-0.36; LAG,peers↓,ind↓; peer=-0.92` |
| PKG | large | Consumer Cyclical | `join=-0.63; sector1d=-0.80; gen1d=+0.23; peer=-0.16` |
| MDB | large | Technology | `join=-0.61; sector1d=+0.40; gen1d=+0.47; peer=-0.80` |
| RY | mega | Financial | `join=+0.59; gen1d=+0.07; ab=-0.12; LAG,peers↓,ind↓; peer=-0.14` |
| PANW | mega | Technology | `join=-0.45; sector1d=+0.40; gen1d=+0.23; ab=+0.12; LAG,peers↓,ind↓; peer=-0.06` |
| WMT | mega | Consumer Defensive | `join=+0.13; gen1d=+0.07; news=-0.29; ev=news_judge; ab=-0.12; LAG,peers↓,ind↓; peer=-0.85` |
| BIDU | large | Communication Services | `join=-0.95; gen1d=+0.07; ab=-0.55; LAG,peers↓,ind↓; peer=-0.87; mid_opp=+0.11` |
| SES | micro | Consumer Cyclical | `join=-0.71; sector1d=-0.80; gen1d=+0.23; ab=-0.55; LAG,peers↓,ind↓; peer=-0.91; mid_opp=+0.16` |

## Same 1d movers — book score + later sessions

`book` is that day's ranker score (`score_1d` in the CSV). Buys started around +0.94.
A ripper with book +0.40 was seen and ranked too low. A ripper with book — was not scored.

| Ticker | class | book | join | sect | gen | news | AB | peer | heat | 1d | 2d | 3d | 1w |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| USDE | gated_out | +0.103 | -0.26 | +0.00 | +0.07 | +0.00 | -0.12 | +0.00 | +0.00 | +85.2% | +56.8% | +51.5% | +58.5% |
| CAN | gated_out | +0.423 | -0.63 | +0.40 | +0.47 | +0.00 | +0.24 | +1.00 | +0.00 | +27.2% | +29.0% | +49.5% | +55.2% |
| CRML | outweighed | +0.260 | +0.87 | +0.00 | +0.47 | +0.00 | -0.36 | -0.96 | +0.00 | +22.6% | +15.0% | +39.7% | +43.0% |
| ARCT | gated_out | +0.800 | +0.04 | +0.00 | +0.47 | +0.00 | +0.64 | +1.00 | +0.00 | +22.4% | +30.5% | +40.5% | +40.4% |
| TMC | outweighed | +0.359 | +0.81 | +0.00 | +0.47 | +0.00 | -0.24 | -0.97 | +0.00 | +20.6% | +19.3% | +24.9% | +27.5% |
| CYPH | gated_out | +0.605 | +0.08 | +0.00 | +0.07 | +0.00 | +0.76 | +1.00 | +0.00 | +19.3% | +41.2% | +37.8% | +69.8% |
| ELMT | outweighed | +0.535 | -0.10 | -0.24 | +0.19 | +0.00 | +0.46 | +0.00 | +0.00 | +19.3% | +5.9% | +6.5% | +7.0% |
| SLS | outweighed | +0.720 | +0.45 | +0.00 | +0.47 | +0.00 | +0.64 | +0.95 | +0.00 | +15.4% | +6.9% | +5.2% | +9.9% |
| AIFC | gated_out | +0.178 | -0.75 | +0.40 | +0.47 | +0.00 | +0.12 | +0.00 | +0.00 | +15.2% | +11.9% | +8.5% | +0.9% |
| IQMX | blind | +0.353 | -0.77 | +0.40 | +0.07 | +0.00 | +0.00 | +0.00 | +0.00 | +14.8% | +7.8% | +10.5% | +14.8% |
| TII | gated_out | +0.292 | +0.88 | +0.00 | +0.07 | +0.00 | +0.24 | +0.00 | +0.00 | +14.6% | +15.8% | +19.1% | +22.4% |
| HQ | blind | +0.346 | -0.83 | +0.40 | +0.07 | +0.00 | +0.00 | +0.00 | +0.00 | +14.5% | +11.7% | +11.4% | -0.9% |
| BKKT | gated_out | +0.625 | -0.15 | +0.40 | +0.47 | +0.00 | +0.24 | -0.08 | +0.00 | +14.5% | +9.1% | +15.1% | +14.2% |
| UEC | outweighed | +0.613 | +0.91 | -0.80 | +0.23 | +0.00 | -0.12 | +0.18 | +0.00 | +14.4% | +12.2% | +19.0% | +19.6% |
| EU | gated_out | -0.130 | +0.43 | -0.80 | +0.47 | +0.00 | -0.64 | -0.70 | +0.00 | +14.3% | +18.1% | +31.4% | +27.5% |
| LAR | outweighed | +0.209 | +0.84 | +0.00 | +0.47 | +0.00 | -0.56 | -0.76 | +0.00 | +14.2% | +12.0% | +12.4% | +12.8% |
| GUTS | gated_out | +0.156 | -0.19 | +0.00 | +0.47 | +0.00 | +0.36 | -0.54 | +0.00 | +13.9% | +9.5% | +14.4% | +10.0% |
| HOOD | outweighed | +0.178 | +0.90 | +0.00 | +0.47 | +0.00 | +0.36 | -0.64 | +0.00 | +13.7% | +9.0% | +17.9% | +16.8% |
| PROK | outweighed | +0.585 | -0.10 | +0.00 | +0.47 | +0.00 | +0.46 | +0.22 | +0.00 | +13.4% | +13.4% | +21.8% | +25.3% |
| ASST | outweighed | +0.674 | +0.30 | +0.00 | +0.47 | +0.00 | +0.00 | +1.00 | +0.00 | +13.0% | +22.3% | +32.6% | +44.9% |
| ALOY | outweighed | +0.420 | +0.96 | +0.00 | +0.07 | +0.00 | -0.24 | +0.00 | +0.00 | +12.9% | +0.2% | +9.8% | +3.4% |
| USAR | outweighed | +0.605 | +0.81 | +0.00 | +0.47 | +0.00 | -0.36 | +0.00 | +0.00 | +12.6% | +6.9% | +13.4% | +11.6% |
| OI | outweighed | -0.103 | -0.97 | -0.80 | +0.07 | +0.00 | -0.70 | -0.68 | +0.00 | +12.5% | +13.6% | +17.7% | +13.5% |
| INFQ | outweighed | +0.504 | -0.85 | +0.40 | +0.47 | +0.00 | -0.12 | +0.00 | +0.00 | +12.5% | +4.0% | +10.2% | +12.8% |
| EZPW | outweighed | +0.699 | +0.79 | +0.00 | +0.07 | +0.00 | +0.36 | +0.35 | +0.00 | +12.0% | +21.9% | +22.5% | +17.3% |
| DEFT | gated_out | +0.354 | +0.34 | +0.00 | +0.47 | +0.00 | +0.46 | +0.00 | +0.00 | +11.7% | +20.4% | +17.3% | +19.7% |
| DK | outweighed | +0.449 | +0.99 | -0.80 | +0.07 | +0.00 | +0.64 | -0.77 | +0.00 | +11.6% | +6.3% | +5.5% | +8.6% |
| SGML | outweighed | +0.232 | +0.71 | +0.00 | +0.07 | +0.00 | -0.36 | -0.65 | +0.00 | +11.6% | +13.8% | +14.9% | +15.1% |
| RGTI | outweighed | +0.229 | -0.96 | +0.40 | +0.47 | +0.00 | -0.46 | -0.89 | +0.00 | +11.5% | +1.9% | +5.5% | +2.3% |
| DNN | outweighed | +0.694 | +0.98 | -0.80 | +0.23 | +0.00 | +0.00 | -0.21 | +0.00 | +11.5% | +12.7% | +18.5% | +16.7% |
| VIRT | outweighed | +0.772 | +0.90 | +0.00 | +0.07 | +0.00 | +0.64 | +0.50 | +0.00 | +11.0% | +8.3% | +6.5% | +8.1% |
| BLMN | outweighed | +0.262 | -0.15 | -0.80 | +0.23 | +0.00 | +0.00 | -0.10 | +0.00 | +11.0% | +9.2% | +5.3% | +1.9% |
| LPTH | outweighed | +0.443 | -0.15 | +0.40 | +0.47 | +0.00 | -0.24 | +0.62 | +0.00 | +10.8% | +1.6% | +5.9% | +4.8% |
| JELD | gated_out | +0.272 | -0.59 | -0.24 | +0.47 | +0.00 | +0.12 | +0.69 | +0.00 | +10.6% | +19.7% | +15.4% | +14.2% |
| ABAT | gated_out | +0.116 | -0.97 | -0.24 | +0.23 | +0.00 | -0.36 | -0.76 | +0.00 | +10.3% | +3.0% | +11.2% | +13.1% |
| CVI | outweighed | +0.774 | +0.99 | -0.80 | +0.23 | +0.00 | +0.70 | +0.10 | +0.00 | +10.3% | +6.1% | +5.8% | +10.0% |
| EOSE | outweighed | -0.070 | -0.98 | -0.24 | +0.47 | +0.00 | -0.70 | -0.95 | +0.00 | +10.1% | +0.0% | +2.3% | +0.4% |
| SID | outweighed | +0.666 | +0.41 | +0.00 | +0.47 | +0.00 | -0.36 | +0.74 | +0.00 | +10.1% | +15.7% | +14.6% | +20.2% |
| SENS | outweighed | +0.455 | +0.01 | +0.00 | +0.23 | +0.00 | +0.00 | +0.97 | +0.00 | +10.1% | +6.3% | +7.9% | +5.6% |
| BCAR | gated_out | +0.144 | -0.03 | +0.00 | +0.23 | +0.00 | -0.12 | +0.00 | +0.00 | +9.9% | +7.4% | +6.2% | +11.1% |
| FSLY | outweighed | +0.433 | -0.15 | +0.40 | +0.07 | +0.00 | +0.00 | -0.97 | +0.00 | +9.9% | +1.0% | +1.3% | +8.9% |
| PACB | gated_out | +0.584 | +0.01 | +0.00 | +0.47 | +0.00 | +0.24 | +0.42 | +0.00 | +9.8% | +4.1% | +23.6% | +24.0% |
| FUTU | outweighed | +0.393 | +0.17 | +0.00 | +0.07 | +0.00 | +0.56 | +0.79 | +0.00 | +9.7% | +2.7% | +11.4% | +12.6% |
| QUBT | outweighed | +0.135 | -0.97 | +0.40 | +0.47 | +0.00 | -0.56 | -0.43 | +0.00 | +9.6% | +1.2% | +3.9% | +5.4% |
| IE | outweighed | +0.433 | +0.91 | +0.00 | +0.23 | +0.00 | +0.12 | -0.63 | +0.00 | +9.5% | +9.4% | +15.7% | +14.4% |
| NEXA | outweighed | +0.216 | +0.99 | +0.00 | +0.23 | +0.00 | -0.12 | -0.65 | +0.00 | +9.4% | +8.2% | +12.4% | +0.8% |
| ERO | outweighed | +0.723 | +0.99 | +0.00 | +0.23 | +0.00 | +0.70 | -0.35 | +0.00 | +9.4% | +7.0% | +12.2% | +9.4% |
| CHGG | gated_out | -0.057 | -0.10 | +0.00 | +0.47 | +0.00 | -0.46 | -0.63 | +0.00 | +9.3% | +6.7% | +5.3% | +7.0% |
| QMLS | gated_out | +0.153 | -0.51 | +0.40 | +0.19 | +0.00 | +0.00 | +0.00 | +0.00 | +9.3% | +9.9% | +17.0% | +15.6% |
| MB | gated_out | -0.073 | -0.84 | -0.80 | +0.47 | +0.00 | -0.36 | +0.00 | +0.00 | +9.3% | -1.5% | +3.2% | +4.3% |
| SLI | outweighed | +0.646 | +0.83 | +0.00 | +0.47 | +0.00 | +0.24 | -0.36 | +0.00 | +9.2% | +7.0% | +13.2% | +17.2% |
| RZLT | outweighed | +0.760 | +0.41 | +0.00 | +0.07 | +0.00 | +0.76 | +0.17 | +0.00 | +9.2% | +7.5% | +7.5% | +6.2% |
| ABUS | outweighed | +0.512 | +0.60 | +0.00 | +0.07 | +0.00 | +0.70 | +0.09 | +0.00 | +9.2% | +9.0% | +9.0% | +8.7% |
| MP | blind | +0.675 | +0.98 | +0.00 | +0.47 | +0.00 | +0.00 | +0.00 | +0.00 | +9.1% | +4.3% | +9.3% | +6.1% |
| TMQ | outweighed | +0.281 | +0.67 | +0.00 | +0.47 | +0.00 | -0.24 | -0.88 | +0.00 | +9.1% | +3.6% | +5.8% | +4.0% |
| PARR | outweighed | +0.415 | +0.99 | -0.80 | +0.07 | +0.00 | +0.64 | -0.94 | +0.00 | +9.1% | +2.6% | +1.9% | +3.0% |
| TEM | outweighed | +0.572 | +0.78 | +0.00 | +0.47 | +0.00 | +0.56 | +0.96 | +0.00 | +9.1% | -0.7% | +3.1% | +3.2% |
| BTDR | outweighed | +0.870 | -0.15 | +0.40 | +0.47 | +0.00 | +0.24 | +0.95 | +0.00 | +9.0% | +4.6% | +8.2% | +10.0% |
| UUUU | outweighed | +0.409 | +0.96 | -0.80 | +0.47 | +0.00 | -0.46 | -0.34 | +0.00 | +8.9% | +7.0% | +14.9% | +14.0% |
| BGC | outweighed | +0.690 | +0.59 | +0.00 | +0.23 | +0.00 | +0.46 | +0.03 | +0.00 | +8.9% | +9.8% | +8.5% | +9.2% |
| PSNL | outweighed | +0.488 | -0.06 | +0.00 | +0.47 | +0.00 | +0.46 | +0.81 | +0.00 | +8.9% | +1.1% | +2.2% | +3.3% |
| MRNA | outweighed | +0.278 | -0.06 | +0.00 | +0.23 | +0.00 | +0.46 | +1.00 | +0.00 | +8.9% | +4.2% | +19.1% | +7.8% |
| ACDC | outweighed | -0.042 | -0.04 | -0.80 | +0.47 | +0.00 | -0.64 | -0.98 | +0.00 | +8.8% | +8.6% | +5.2% | +10.2% |
| NMG | outweighed | +0.507 | +0.75 | +0.00 | +0.23 | +0.00 | +0.24 | -0.92 | +0.00 | +8.7% | +5.1% | +7.2% | +6.2% |
| SCCO | outweighed | +0.014 | +0.98 | +0.00 | +0.23 | +0.00 | +0.46 | -0.34 | +0.00 | +8.7% | +7.8% | +10.6% | +9.4% |
| EROC | outweighed | +0.688 | -0.15 | -0.24 | +0.19 | +0.00 | +0.46 | +0.00 | +0.00 | +8.6% | +0.8% | -0.5% | -3.1% |
| QMCO | outweighed | +0.201 | -0.03 | +0.40 | +0.47 | +0.00 | +0.12 | -0.92 | +0.00 | +8.6% | +5.2% | +9.3% | +15.3% |
| AXGN | outweighed | +0.291 | +0.72 | +0.00 | +0.23 | +0.00 | +0.12 | -0.63 | +0.00 | +8.5% | +6.0% | +7.5% | +11.0% |
| QBTS | outweighed | +0.262 | -0.97 | +0.40 | +0.47 | +0.00 | -0.46 | -0.72 | +0.00 | +8.5% | -0.6% | +2.9% | -3.9% |
| FIGR | in_buy_book | +0.941 | +0.89 | +0.00 | +0.19 | +0.00 | +0.56 | +0.00 | +0.00 | +8.4% | +7.2% | +14.1% | +3.7% |
| CNH | outweighed | +0.238 | -0.32 | -0.24 | +0.23 | +0.00 | +0.56 | +0.57 | +0.00 | +8.4% | +7.4% | +5.6% | +4.5% |
| XNDU | outweighed | +0.770 | -0.15 | +0.40 | +0.47 | +0.00 | +0.12 | +0.00 | +0.00 | +8.3% | +3.4% | +5.0% | +3.9% |
| TRLV | outweighed | +0.362 | -0.04 | +0.00 | +0.47 | +0.00 | +0.36 | +0.00 | +0.00 | +8.3% | +8.7% | +10.4% | +10.6% |
| CSAN | outweighed | +0.456 | +0.44 | -0.80 | +0.07 | +0.00 | +0.00 | -0.41 | +0.00 | +8.3% | +5.5% | +11.4% | +11.6% |
| ONT | outweighed | +0.156 | -0.99 | -0.24 | +0.47 | +0.00 | -0.56 | +0.00 | +0.00 | +8.2% | +4.6% | +9.1% | +14.8% |
| COIN | outweighed | +0.362 | -0.60 | +0.00 | +0.47 | +0.00 | +0.46 | +0.85 | +0.00 | +8.2% | +4.1% | +8.6% | +9.4% |
| DCH | outweighed | +0.561 | -0.15 | -0.80 | +0.47 | +0.00 | +0.24 | +0.00 | +0.00 | +8.2% | +1.6% | +1.1% | -0.6% |
| NB | outweighed | +0.555 | +0.97 | +0.00 | +0.07 | +0.00 | +0.12 | -0.99 | +0.00 | +8.2% | -0.9% | +4.2% | +3.1% |
| IONQ | outweighed | -0.178 | -0.96 | +0.40 | +0.47 | +0.00 | -0.56 | -0.36 | +0.00 | +8.0% | -1.1% | +1.2% | +0.5% |
| AGEN | gated_out | +0.396 | +0.11 | +0.00 | +0.47 | +0.00 | +0.70 | +0.65 | +0.00 | +8.0% | +2.4% | +11.6% | +10.8% |

## Unweighted book — same day, weights stripped

mean(join, sector, general, news, AB, peer) — no weights, no opp/rebound.
mean(AB, peer) — name-specific only.
same as live: skip micro/<$400M, max 4 large, 4/sector, 3/industry.

| Book | names kept vs live | entered | dropped | avg 1d | avg 1w |
|------|--------------------|---------|---------|--------|--------|
| live weighted | 15 | — | — | +1.20% | -0.67% |
| standalone equal-mean | overlap 5 | AU, FRPT, HCC, HL, LAUR, MEOH, MGTX, NMRK, TEM, TRU | CALX, CE, EPAM, FIGR, HLNE, IRTC, MOS, NHI, OCUL, OGS | +1.61% | +0.62% |
| tape AB+peer only | overlap 1 | CPRT, DOW, ETON, HCC, HTHT, MEOH, MGTX, MRVI, MTA, OABI, OBE, SM, TALO, TRGP | AUPH, CALX, CE, ELF, EPAM, FIGR, GSHD, HLNE, IRTC, MOS, NHI, OCUL, OGS, WRBY | +0.53% | -1.37% |

### Live weighted BUY (what actually printed)

| # | Ticker | stand | tape | book | join | AB | peer | opp | 1d | 1w | size |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | ELF | +0.451 | +0.684 | +1.123 | +0.87 | +0.55 | +0.81 | +0.60 | +3.5% | +8.8% | mid |
| 2 | MOS | +0.317 | +0.344 | +1.066 | +0.98 | +0.64 | +0.05 | +0.68 | +4.5% | +0.2% | mid |
| 3 | AUPH | +0.495 | +0.786 | +1.024 | +0.93 | +0.81 | +0.76 | +0.52 | -3.6% | -4.4% | mid |
| 4 | CE | +0.360 | +0.572 | +1.020 | +0.95 | +0.64 | +0.51 | +0.64 | -0.4% | -6.2% | mid |
| 5 | OCUL | +0.379 | +0.750 | +1.019 | +0.54 | +0.70 | +0.80 | +0.60 | -0.1% | -4.0% | mid |
| 6 | EPAM | +0.330 | +0.631 | +1.015 | -0.15 | +0.46 | +0.80 | +0.68 | +3.4% | +5.4% | mid |
| 7 | WRBY | +0.480 | +0.754 | +1.005 | +0.90 | +0.76 | +0.75 | +0.52 | -0.1% | -4.8% | mid |
| 8 | CELH | +0.412 | +0.754 | +1.000 | +0.73 | +0.64 | +0.87 | +0.56 | +2.5% | +4.0% | mid |
| 9 | IRTC | +0.377 | +0.487 | +0.994 | +0.82 | +0.46 | +0.51 | +0.64 | +0.4% | -7.9% | mid |
| 10 | CALX | +0.297 | +0.572 | +0.986 | +0.01 | +0.36 | +0.78 | +0.68 | -0.6% | -5.8% | mid |
| 11 | NHI | +0.279 | +0.546 | +0.971 | +0.75 | +0.81 | +0.28 | +0.64 | -0.7% | -1.5% | mid |
| 12 | GSHD | +0.444 | +0.642 | +0.964 | +0.91 | +0.81 | +0.47 | +0.52 | +2.6% | +0.5% | mid |
| 13 | OGS | +0.313 | +0.501 | +0.945 | +0.81 | +0.85 | +0.15 | +0.60 | -3.1% | -2.1% | mid |
| 14 | FIGR | +0.272 | +0.277 | +0.941 | +0.89 | +0.55 | +0.00 | +0.68 | +8.4% | +3.7% | mid |
| 15 | HLNE | +0.316 | +0.358 | +0.938 | +0.95 | +0.46 | +0.25 | +0.64 | +1.2% | +4.1% | mid |

### Standalone BUY (equal mean of 6 layers, same gates)

| # | Ticker | stand | tape | book | join | AB | peer | opp | 1d | 1w | size |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | AU | +0.548 | +0.672 | +0.480 | +0.99 | +0.70 | +0.64 | -0.17 | +5.9% | +2.9% | large |
| 2 | AUPH | +0.495 | +0.786 | +1.024 | +0.93 | +0.81 | +0.76 | +0.52 | -3.6% | -4.4% | mid |
| 3 | HL | +0.485 | +0.729 | +0.516 | +0.99 | +0.76 | +0.70 | +0.03 | -0.5% | +1.1% | large |
| 4 | MGTX | +0.483 | +0.859 | +0.758 | +0.94 | +0.85 | +0.87 | +0.24 | -1.4% | -1.4% | small |
| 5 | WRBY | +0.480 | +0.754 | +1.005 | +0.90 | +0.76 | +0.75 | +0.52 | -0.1% | -4.8% | mid |
| 6 | TEM | +0.460 | +0.758 | +0.572 | +0.78 | +0.55 | +0.96 | +0.03 | +9.1% | +3.2% | large |
| 7 | ELF | +0.451 | +0.684 | +1.123 | +0.87 | +0.55 | +0.81 | +0.60 | +3.5% | +8.8% | mid |
| 8 | MEOH | +0.445 | +0.806 | +0.765 | +0.99 | +0.76 | +0.85 | +0.28 | +2.4% | -3.3% | mid |
| 9 | GSHD | +0.444 | +0.642 | +0.964 | +0.91 | +0.81 | +0.47 | +0.52 | +2.6% | +0.5% | mid |
| 10 | HCC | +0.441 | +0.795 | +0.883 | +0.99 | +0.81 | +0.78 | +0.40 | +3.3% | +4.1% | mid |
| 11 | TRU | +0.426 | +0.582 | +0.373 | +0.92 | +0.85 | +0.32 | -0.05 | -0.6% | +0.6% | large |
| 12 | CELH | +0.412 | +0.754 | +1.000 | +0.73 | +0.64 | +0.87 | +0.56 | +2.5% | +4.0% | mid |
| 13 | LAUR | +0.402 | +0.708 | +0.708 | +0.92 | +0.55 | +0.86 | +0.28 | -0.3% | +2.8% | mid |
| 14 | NMRK | +0.390 | +0.595 | +0.880 | +0.92 | +0.76 | +0.43 | +0.48 | +1.4% | -2.4% | mid |
| 15 | FRPT | +0.387 | +0.467 | +0.892 | +0.92 | +0.76 | +0.17 | +0.52 | -0.2% | -2.5% | mid |

### Tape BUY (mean of AB + peer only, same gates)

| # | Ticker | stand | tape | book | join | AB | peer | opp | 1d | 1w | size |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | ETON | +0.377 | +0.924 | +0.613 | +0.18 | +0.85 | +1.00 | +0.16 | +5.9% | +4.5% | small |
| 2 | TRGP | +0.337 | +0.883 | +0.270 | +0.99 | +0.85 | +0.92 | -0.17 | -1.0% | -4.4% | large |
| 3 | HTHT | +0.133 | +0.872 | +0.237 | -0.21 | +0.76 | +0.98 | -0.05 | -0.8% | -2.4% | large |
| 4 | MGTX | +0.483 | +0.859 | +0.758 | +0.94 | +0.85 | +0.87 | +0.24 | -1.4% | -1.4% | small |
| 5 | MRVI | +0.404 | +0.852 | +0.840 | +0.65 | +0.70 | +1.00 | +0.38 | +4.2% | +5.9% | mid |
| 6 | OABI | +0.310 | +0.851 | +0.612 | +0.09 | +0.70 | +1.00 | +0.22 | +0.7% | +9.2% | small |
| 7 | MEOH | +0.445 | +0.806 | +0.765 | +0.99 | +0.76 | +0.85 | +0.28 | +2.4% | -3.3% | mid |
| 8 | HCC | +0.441 | +0.795 | +0.883 | +0.99 | +0.81 | +0.78 | +0.40 | +3.3% | +4.1% | mid |
| 9 | SM | +0.306 | +0.789 | +0.860 | +0.99 | +0.81 | +0.77 | +0.46 | -1.2% | -6.2% | mid |
| 10 | TALO | +0.303 | +0.781 | +0.854 | +0.99 | +0.76 | +0.80 | +0.46 | -1.6% | -7.1% | mid |
| 11 | OBE | +0.278 | +0.761 | +0.733 | +0.88 | +0.76 | +0.76 | +0.36 | -1.3% | -7.2% | small |
| 12 | CPRT | +0.122 | +0.759 | +0.303 | -0.78 | +0.55 | +0.96 | +0.07 | -1.5% | -5.2% | large |
| 13 | CELH | +0.412 | +0.754 | +1.000 | +0.73 | +0.64 | +0.87 | +0.56 | +2.5% | +4.0% | mid |
| 14 | MTA | +0.354 | +0.750 | +0.484 | +0.16 | +0.55 | +0.95 | +0.10 | -0.5% | -2.5% | small |
| 15 | DOW | +0.421 | +0.748 | +0.486 | +0.96 | +0.70 | +0.79 | +0.03 | -1.7% | -8.5% | large |

### Watch names — where they sit with no weights

| Ticker | stand | tape | book | raw stand rank | raw tape rank | gated out? | 1d | 1w |
|---|---|---|---|---|---|---|---|---|
| SLS | +0.417 | +0.792 | +0.720 | 34 | 22 | no | +15.4% | +9.9% |
| ARCT | +0.356 | +0.818 | +0.800 | 97 | 16 | yes | +22.4% | +40.4% |
| CYPH | +0.318 | +0.881 | +0.605 | 168 | 3 | yes | +19.3% | +70.2% |
| ASST | +0.295 | +0.499 | +0.673 | 226 | 270 | no | +13.0% | +45.3% |
| BTDR | +0.318 | +0.597 | +0.870 | 165 | 144 | no | +9.0% | +10.0% |
| VIRT | +0.351 | +0.567 | +0.772 | 107 | 181 | no | +11.0% | +8.1% |
| FIGR | +0.272 | +0.277 | +0.941 | 304 | 734 | no | +8.4% | +3.7% |
| ELF | +0.451 | +0.684 | +1.123 | 8 | 76 | no | +3.5% | +8.8% |
| AUPH | +0.495 | +0.786 | +1.024 | 2 | 24 | no | -3.6% | -4.4% |

## Files

- `data/stock_book/2026-08-20_lookback.json`
- `01_daily/2026-08-20_lookback.md`
- `03_scoreboard/BOOK_LOOKBACK.md` (this report, latest run)

