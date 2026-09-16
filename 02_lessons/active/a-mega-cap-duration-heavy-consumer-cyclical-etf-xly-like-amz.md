---
trigger_pattern: "A mega-cap duration-heavy consumer cyclical ETF (XLY-like: AMZN+TSLA+HD dominate, no semis) prints a net-negative S0–S4 card with a confirming prior-close relative lag into an unprinted same-session FOMC+SEP+Chair path-binary, live index futures on the operator tape (Finviz/Reuters) are only modestly green (< +0.5%), and the sector-owned spend print is still unprinted. The v2 engine still builds a large positive tape_anchor from yfinance ES/NQ vs prior close (overnight already in) and, with calendar_size_gate and/or sector_rs_veto, emits official flat — even after the write-up has discarded that futures object."
corrected_behavior: "When live futures are inside ±0.5% and the factor card + 1d rel already agree down, keep overlay direction = down. calendar_size_gate / size_gate may cap magnitude to mild/flat only. Do not treat engine divergence against a discarded prior-close ES/NQ anchor as a real leading-vs-tape fight. Do not import XLK/NQ beta (08-27 still binds)."
falsifier: "Next FOMC/path-binary morning with live Finviz ES/NQ inside ±0.5%, net-negative XLY card, confirming 1d rel, official kept down — and cash XLY finishes up because the unprinted spend beat plus a non-hawkish Chair actually bid the book. Also falsified if yfinance ES/NQ vs prior close matches live Finviz (no stale-anchor split) and the only flatten is a pure size-gate, yet cash is still down — then the object is the gate, not the futures series."
current_behavior: "Treat the engine’s official flat/flat (and divergence_flagged vs that tape_anchor) as the call, while the overlay already wanted down-not-notable and DIVERGENCE 0. Size-gate is allowed to flip direction, not just cap band."
evidence_cited: "Official scored call flat/flat (engine 2.692, tape_anchor 7.368 from yfinance ES +1.14%/NQ +1.50%, overlay −6.0, calendar_size_gate + sector_rs_veto). Actual XLY −0.63% / SPY −0.44% / rel −0.19% = down/mild. Path: open ~+0.2% then fade after hawkish SEP/Warsh (median 2026 FFR 4.1% vs June 3.8%). August retail +1.2%/control +1.4% printed hot and did not lift the book. Live Finviz ES +0.20%/NQ +0.41%. Nasdaq −0.01% vs XLY −0.63%. LLM S0=−1 not −2 and “do not manufacture notable” matched the mild red; the miss is the official direction flatten."
error_category: "D"
scope: "ops"
date: "2026-09-16"
status: "active"
occurrences: "1"
promoted_on: "2026-09-16"
sources: "['2026-09-16_sector_consumer_cyclical_lesson.md']"
schema_ok: "true"
---

## RULE
When live futures are inside ±0.5% and the factor card + 1d rel already agree down, keep overlay direction = down. calendar_size_gate / size_gate may cap magnitude to mild/flat only. Do not treat engine divergence against a discarded prior-close ES/NQ anchor as a real leading-vs-tape fight. Do not import XLK/NQ beta (08-27 still binds).

## WHEN IT FIRES
A mega-cap duration-heavy consumer cyclical ETF (XLY-like: AMZN+TSLA+HD dominate, no semis) prints a net-negative S0–S4 card with a confirming prior-close relative lag into an unprinted same-session FOMC+SEP+Chair path-binary, live index futures on the operator tape (Finviz/Reuters) are only modestly green (< +0.5%), and the sector-owned spend print is still unprinted. The v2 engine still builds a large positive tape_anchor from yfinance ES/NQ vs prior close (overnight already in) and, with calendar_size_gate and/or sector_rs_veto, emits official flat — even after the write-up has discarded that futures object.

## WRONG IF
Next FOMC/path-binary morning with live Finviz ES/NQ inside ±0.5%, net-negative XLY card, confirming 1d rel, official kept down — and cash XLY finishes up because the unprinted spend beat plus a non-hawkish Chair actually bid the book. Also falsified if yfinance ES/NQ vs prior close matches live Finviz (no stale-anchor split) and the only flatten is a pure size-gate, yet cash is still down — then the object is the gate, not the futures series.

## EVIDENCE
Official scored call flat/flat (engine 2.692, tape_anchor 7.368 from yfinance ES +1.14%/NQ +1.50%, overlay −6.0, calendar_size_gate + sector_rs_veto). Actual XLY −0.63% / SPY −0.44% / rel −0.19% = down/mild. Path: open ~+0.2% then fade after hawkish SEP/Warsh (median 2026 FFR 4.1% vs June 3.8%). August retail +1.2%/control +1.4% printed hot and did not lift the book. Live Finviz ES +0.20%/NQ +0.41%. Nasdaq −0.01% vs XLY −0.63%. LLM S0=−1 not −2 and “do not manufacture notable” matched the mild red; the miss is the official direction flatten.

(learn_cycle promote)
