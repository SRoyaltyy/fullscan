# Sector Outcome — Utilities — 2026-09-14

Actuals: {'etf': 'XLU', 'pct': -1.3446560581065081, 'spy_pct': -0.446162221482016, 'rel': -0.8984938366244921, 'open': 42.380001068115234, 'close': 41.81999969482422, 'source': 'yf_download'}

# Sector Post-Session Review — Utilities (XLU) — 2026-09-14

## 0. FACTS

**Channel 1 actuals (deterministic):**

| Metric | Value |
|---|---|
| XLU % | **−1.34%** |
| SPY % | **−0.45%** |
| Relative % | **−0.90%** |
| Open | 42.38 |
| Close | 41.82 |
| Path | Open 42.38 → day range 41.81–42.44 → close 41.82 (closed near the low) |

**Corroboration (web):**
- CLAIM: XLU closed near session lows on 2026-09-14, down ~1.06% intraday at 3:02pm EDT before settling lower.
  URL: https://www.marketwatch.com/investing/fund/xlu
  PUBLISHED: 2026-09-14
  QUOTE: "Real time quote $41.94 −0.45 −1.06% Previous Close $42.39. Open $42.38 Day Range 41.81 – 42.44"
  SUMMARY: Confirms open 42.38, low 41.81, and a close at/near the low — a one-way down day, not a round-trip.

- CLAIM: Broad market closed modestly lower; the 10Y yield hit 5%.
  URL: https://finance.yahoo.com/
  PUBLISHED: 2026-09-14
  QUOTE: "S&P 500 7,619.98 −37.00 −0.48% … yield hits 5%"
  SUMMARY: SPY −0.45% matches the deterministic SPY print; the "yield hits 5%" headline confirms the rates channel was the day's macro object.

- CLAIM: Utilities have "hit a wall" in H2 2026 on three headwinds.
  URL: https://www.moneyshow.com/articles/tradingidea-65460/spy-watch-oil-and-rates-if-youre-trading-stocks-here/
  PUBLISHED: 2026-09-13/14
  QUOTE: "After a robust first half of 2026, it's fair to say utility stocks have hit a wall since. I see three major headwinds hurting returns."
  SUMMARY: Independent confirmation that the multi-horizon XLU lag is a recognized regime, not a one-day artifact.

**Direction:** down. **Magnitude:** mild-to-notable — |−1.34%| absolute is a notable single-day move for a low-beta utility ETF, and the −0.90% relative lag is a clean, unambiguous underperformance. The morning band was **mild**; the realized absolute is at the upper edge of mild and arguably into notable for this instrument. Direction **HIT**, magnitude **HIT at the boundary** (mild band, realized ~1.3% absolute / ~0.9% relative).

---

## 1. What drove the sector today

**Primary driver: the rates/duration channel, exactly as the morning read framed it.** The 10Y "hits 5%" headline is the day's macro object. XLU is a bond proxy; a 10Y pushing to 5% with the long end (30Y 5.37%) already stressed is a direct, mechanical duration headwind. The morning panel had 10Y 4.95% (+25 bp 1m), 30Y 5.37%, real 10Y 2.55% — all rising on every window. Today that backup continued into the round-number 5% zone.

**Secondary driver: the oil-inflation transmission.** WTI $102.29 / Brent $107.33, +3% on the day, feeding the long end. Per the 09-08 lesson, at elevated oil this is an inflation/duration negative for a bond proxy, not a flight-to-safety bid. Today validated that: oil spiked, yields rose, XLU fell.

**Taxonomy alignment:**
- *Rates rising (bond-proxy selloff)* — **HIT**, dominant, full weight. One shock (Warsh hawkish repricing + oil spike), counted once.
- *Real yields rising* — **HIT**. DFII10 2.55%, +12 bp 1m.
- *Risk-off tape / flight to safety* — **PARTIAL, and it failed to protect.** The tape was risk-off (NQ leading down, Asia red), and XLU premarket was +0.33% — but the defensive bid did not survive the open. This is the key intraday fact: the premarket green was **mean-reversion fuel**, exactly as the 09-09 lesson warned for a defensive bid built on a *static* shock.
- *Sector rotation out of utilities / breadth failure* — **HIT**. XLU underperformed SPY by 0.90% on a down day, i.e., it did not even function as a defensive hedge.
- *Data-center load growth / AI-power* — **STALE**, correctly not weighted. It was a 1d dampener at best and did nothing today.

---

## 2. Audit of morning S0–S4 reads against reality

**S0_SHARED_MACRO = −1. VERDICT: CORRECT, and arguably underweighted.**
The morning call was that rates + oil + backwardated VIX dominate, and that the premarket green is a *relative, not absolute*, tell. Reality: rates pushed to 5%, oil spiked, XLU fell 1.34%. The sign was right. If anything, S0 = −1 was conservative — the rates channel delivered more than a −1 implies. The morning explicitly refused to let the +0.33% premarket green flip the absolute call, and that refusal was the single most valuable judgment of the session.

**S1_SECTOR_FACTORS = −1.5. VERDICT: CORRECT.**
Rates-rising HIT at full weight + oil-inflation transmission, with the relative defensive bid scored as a partial offset, not a positive. Reality matched: the defensive bid was relative-only and did not hold. The −1.5 was well-calibrated — it correctly refused to promote the premarket green into a positive.

**S2_BREADTH = −1. VERDICT: CORRECT.**
The morning flagged the multi-horizon relative lag (1d −1.16%, 3d −2.22%, 1w −0.34%, 1m −2.25%) as a durable breadth failure, not a one-day artifact, and refused to treat the premarket +0.33% as broad constituent expansion. Reality: XLU lagged by another 0.90%. The breadth read was right and the "single-name-level green" caveat was exactly correct.

**S3_FLOWS_POSITIONING = 0. VERDICT: CORRECT (neutral, no information).**
No confirmed same-day flow signal; 1m rel −2.25% read as de-risked, not crowded-long. Nothing today contradicted that. A 0 here was honest — no false precision.

**S4_ETF_TAPE = −0.5. VERDICT: CORRECT but UNDERWEIGHTED.**
The morning scored the tape −0.5 while noting it was "decisively red across 1d/3d/1w/1m." A tape that is negative on *every* horizon and had just printed a −1.16% relative lag on 09-11 deserved more than −0.5. This is the one place the morning left points on the table. The realized −0.90% relative lag is a continuation of precisely the pattern S4 was describing.

**Multiplier 0.9 / confidence 0.62. VERDICT: reasonable, slightly conservative.** The divergence flag was True and the engine's leading_sum (−8.5) vs. the LLM overlay (−6.0) shows the overlay *shrank* the signal. Given the outcome, the overlay's shrinkage was the wrong direction — the deterministic engine was closer to right.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check:** The morning counted the Warsh-hawkish + oil-spike complex **once** as the rates-rising shock, and separately scored the oil-inflation transmission in S1. That is a legitimate two-channel decomposition (rates level vs. inflation impulse feeding the long end), not a double-count — they are distinct transmission paths that happened to share a root cause. No double-count penalty warranted.

**Interaction that mattered:** The **defensive bid vs. duration headwind** interaction resolved decisively in favor of duration. The morning's framing — "the premarket green is a relative, not absolute, tell" — was the correct resolution of this interaction *ex ante*. The 09-10 gate (VIX ≥ ~20 or explicit FTS before granting a relative-beat claim) **fired correctly**: VIX 17.67 backwardated failed the gate, so the morning refused to write a relative-beat clause. Reality: XLU did not beat SPY on any basis — it lagged. The gate saved the call from a wrong relative-beat clause.

**Knowable-at-open test: YES.**
Everything needed to call this was available before the open:
- 10Y 4.95% rising on every window, 30Y 5.37%, real 2.55% — all in the morning panel.
- Oil +3%, Brent $107 — in the morning panel.
- VIX 17.67 backwardated — in the morning panel, and the 09-10 gate was explicitly applied.
- Multi-horizon relative lag (1d/3d/1w/1m all negative) — in Channel 1.
- FOMC next week as an asymmetric-negative binary — flagged.

The only thing that *looked* like it might argue the other way — XLU premarket +0.33% — was correctly identified as a relative tell and correctly discounted. **This was a fully knowable-at-open call.** The morning did not need post-hoc information; it needed only to trust its own S0/S1/S4 reads, which it largely did.

**Where the morning was too timid:** S4 = −0.5 and the overlay's shrinkage of the deterministic −8.5 to −6.0 both under-weighted a tape that was unambiguously negative on every horizon. The realized −1.34% absolute is a *notable* move for XLU, and the morning capped itself at mild. The direction and the mechanism were right; the magnitude was slightly under-called.

---

## 4. Outliers inside the sector

- **XLU closed at/near the session low (41.82 vs. low 41.81).** No intraday recovery — a one-way down day. This is the signature of a *persistent* macro driver (rates), not a headline spike-and-fade. It argues the rates channel has staying power into the FOMC.
- **The premarket green (+0.33%) fully reversed.** The defensive bid was a trap for anyone who anchored on it. This is the cleanest single lesson of the day: for a bond proxy in a rising-rate regime, a premarket defensive bid is mean-reversion fuel, not a floor.
- **XLU underperformed SPY on a down day (−0.90% rel).** Utilities did not function as a defensive hedge. That is the defining outlier: in a risk-off tape, the classic defensive *lagged*. This confirms the 08-18/09-10 framing that risk-off + rising long end = relative lag, not relative beat.
- **No single-name regulatory item drove the ETF** — consistent with the 08-28 rule. The move was sector-wide and macro-driven, which is why it was clean and knowable.

---

## 5. Verdict and carry-forward

**Direction HIT. Magnitude HIT at the boundary (mild band, realized ~1.3% absolute — arguably notable for XLU). Mechanism HIT. Knowable-at-open YES.**

The morning's core judgment — *rates + oil dominate the absolute; the premarket green is relative-only; the 09-10 gate fails so no relative-beat clause* — was correct on every count. The two places to tighten:

1. **S4 under-weighting.** A tape negative on 1d/3d/1w/1m with a fresh −1.16% relative lag should score below −0.5. Raise the floor on multi-horizon-negative tapes.
2. **Overlay shrinkage.** The LLM overlay pulled −8.5 → −6.0. Today the deterministic engine was closer to right. When the divergence flag is True *and* the tape is unidirectional, shrink less.

**Carry-forward for the next XLU session:** The FOMC binary is still ahead and still asymmetric-negative. The 10Y at 5% is the operative object. Until the long end stabilizes or the FOMC resolves dovish, the default XLU read remains **down/mild with a relative-lag clause** — and a premarket defensive green should continue to be treated as mean-reversion fuel, not a floor.

OUTCOME_BEGIN
SECTOR: Utilities
ETF: XLU
ETF_PCT: -1.34
SPY_PCT: -0.45
REL_PCT: -0.90
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: 10Y yield pushing to 5% (rates/duration channel) compounded by a +3% oil spike feeding the long end; the premarket defensive bid reversed and XLU lagged SPY on a down day.
KEY_INTERACTION: Defensive bid vs. duration headwind resolved decisively for duration — the +0.33% premarket green was mean-reversion fuel, not a floor, and XLU failed to hedge a risk-off tape.
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction, mechanism, and the refusal to write a relative-beat clause were all correct; S4 (−0.5) and the overlay shrinkage under-weighted a tape negative on every horizon, so magnitude was slightly under-called.
OUTCOME_END