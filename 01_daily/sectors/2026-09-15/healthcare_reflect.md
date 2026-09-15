# Sector Reflect — Healthcare — 2026-09-15

# Healthcare / XLV — 2026-09-15 reflect

**TRIAGE:** Reasoning failure, not a missing-fetch. Rate shock, oil spike, AZN/AMGN, repaired 3d/1w/1m rel, PM:XLV **+0.04%**, and the Finviz-red vs ES-green conflict were all in the morning packet. The miss is how those were scored.

Official grade: predicted **down/mild** (narrative + engine `predicted_direction: down`) vs actual XLV **−0.054%** → **flat/flat** (`|pct| < 0.1%` dir, `< 0.3%` mag). Both axes miss. (Scoreboard text saying `up/mild` contradicts the predict file; same miss either way. Treat as bookkeeping noise, not the cause.)

**Category B.** Evidence was retrieved; S0/S1/S2 were too negative, and a non-idiosyncratic macro shock was allowed to override a flat sector tape. Not A (drivers were present). Not C (components were wrong, not just the multiplier). Not D as the primary layer: Finviz ES **−0.54%** actually tracked SPY **−0.46%**; the sector error was overweighting that index shock into XLV.

---

**CHECK 1 — LESSON MATCH.** Partial match, not a retrieval miss. **09-14 HC** (duration-led risk-off → defensive *destination*, S0+) was retrieved and then over-narrowed: “NQ only 8bp worse than ES ⇒ precondition fails ⇒ S0 negative.” That inference is the bug — “not duration-led” ≠ “absolute down for XLV.” **08-17 HC** (oil/geopolitical risk-off → relative bid, not reversal/lag) was live (Brent ~$108, Hormuz) and only half-applied: relative cushion was named, absolute S0 still went **−0.5**. **08-13 reversal-tell** was applied without its required tech-led *risk-on* tape. **08-10 defensive tape-cap** (negative call, no tape confirmation → flat/mild not a fat down score) was not applied; PM:XLV was already flat. Closest unused cousin in today’s candidate list is **09-15 basic materials** (resolve leading-vs-anchor *sign* conflict toward the factor sum). New lesson is a **narrow generalization of 09-14**, not a “never learned this” retrieval failure.

**CHECK 2 — BACKWARD TEST.** Mixed unless narrowed. **09-14** XLV **+1.45%** after down/mild: a blanket “any de-risking → S0=0 / flat” would still have missed that rip; that day needed the *duration-led + under-owned* destination rule, which must stay. **09-09 / 09-10** down/mild HITs (−0.33%, −0.55%) would be hurt if this rule fired on every red-macro morning. **09-11** flat vs −0.18% is already near the flat gate. Correction only helps when **all** of: low-beta defensive, **no** sector-specific catalyst, **already re-extended** (not 09-14’s lag), **broad uniform** risk-off (not NQ-led), **flat PM ETF**. Otherwise discard as one-day.

**CHECK 3 — CONFLICT SCAN.** No irreconcilable clash if scoped. **09-14 HC** keeps duration-led (NQ−ES ≥ ~50bp) *and* under-owned → S0 positive. This lesson is the complement: broad/rate-driven *and* already-owned → S0 ~0, not −0.5 and not a fresh destination bid. **08-13** stays tech-led risk-on only; do not use it to license absolute down on risk-off. **08-18 XLRE** (live long-end shock → down/mild for a *pure* bond proxy) does not map onto XLV’s mixed book (~4–5% XBI). **08-11 HC** (fresh high-weight policy shock + negative 1d tape) did not fire. **08-17** supports the relative-bid half.

**CHECK 4 — APPLIED-LESSON REVIEW.**
- **09-14 destination:** applied as “precondition fail.” **Hurt.** Should have been S0 ~0, not −0.5.
- **09-11 funding-source:** correctly off (futures red, oil up).
- **09-10 decay cap (~−1.5):** **helped** (blocked notable/severe); still left down/mild vs flat.
- **08-13 reversal-tell:** **misapplied** (no tech-led risk-on). Caps magnitude; does not justify S0/S1 negative.
- **08-17 oil FTS bid:** correctly off at $104+.
- **08-28 leftover-stack:** **helped** on 3d/1w rel, **incomplete** — trailing w1 devices/diagnostics still went into S2.
- **08-14 policy / 08-21 trial:** correctly off / AZN not dominate. **Helped** (flat ETF confirms single-name miss is a non-event).

**CHECK 5 — FALSIFIER.** If this exact setup recurs (re-extended XLV, no HC catalyst, broad non-duration-led risk-off, PM ETF ~flat) and XLV still closes **≤ −0.3%** while SPY is only modestly red, the “S0≈0 / trust flat tape” rule is wrong and must be revised, not defended.

**Divergence:** flagged. Leading overlay **−3.7** vs tape_anchor **+0.535** (ES +0.34%, PM:XLV +0.04%). Index followed the red Finviz tape; **XLV followed the flat sector print**. For this sector, **futures/tape were closer**. Knowable at open: **yes** — no 9am-shock discount.

**Verdict:** Direction and band both miss vs official flat/flat. Fair morning call was **flat/flat** (or down/flat at most). ~2.5 pts too bearish from (i) S0 sign on a low-beta name, (ii) rate shock double-counted into S1 XBI duration, (iii) trailing breadth in S2, (iv) overlay beating a flat PM ETF.

LESSON_BEGIN
ERROR_CATEGORY: B
TRIGGER_PATTERN: A low-beta defensive (XLV-like) that is already re-extended on 3d/1w/1m rel, with no fresh sector-specific catalyst, enters a broad uniform (non-duration-led) risk-off driven by a rate/oil macro shock, while the sector’s own premarket ETF print is flat (~0%).
CURRENT_BEHAVIOR: Treats the rate/oil shock as a net absolute negative in S0, re-scores the same impulse as XBI duration drag in S1, copies trailing weekly pocket weakness into S2, and on a leading-vs-tape sign conflict “trusts factors over tape,” emitting down/mild.
CORRECTED_BEHAVIOR: For this setup, S0 is ~0 to +0.25 (flat absolute, positive relative) — not negative. Do not double-count the rate impulse in S1; XBI duration drag is the S0 manifestation, diluted at XLV weights. S2 scores same-session internal breadth only. If PM ETF is flat and the shock is non-idiosyncratic, do not override tape_anchor; emit flat/flat (or down/flat), not down/mild. Keep 09-14 S0-positive destination only when duration-led (NQ leading ES by a wide spread) AND the sector is still under-owned. 08-13 reversal-tell does not fire on a risk-off tape.
EVIDENCE: 2026-09-15 XLV −0.054% vs SPY −0.46% (rel +0.41%), actual flat/flat; predicted down/mild (S0/S1/S2 = −0.5, total −4.136). PM:XLV +0.04% matched the close; overlay −3.712 did not. 10Y briefly >5% then retreated; AZN SERENA-4 did not move the ETF.
LESSON_MATCH_CHECK: partial match to 09-14 HC (applied too narrowly: not-duration-led ⇒ S0 negative) and 08-17 HC (relative bid named, absolute still −0.5); 08-13 applied without tech-led risk-on; 08-10 defensive tape-cap not applied. Not a retrieval failure — new lesson is the 09-14 complement, not a duplicate.
BACKWARD_CHECK: mixed on recent similar days — would still miss 09-14’s +1.45% if it replaced the duration-led+under-owned destination rule; could hurt 09-09/09-10 down/mild HITs unless restricted to re-extended + flat PM ETF + no sector catalyst + broad (not NQ-led) risk-off.
CONFLICT_CHECK: none if scoped — 09-14 remains duration-led AND under-owned → S0+; this lesson is broad/rate-driven AND already-owned → S0~0. 08-18 XLRE down/mild on a pure bond-proxy rate shock does not apply to mixed-book XLV. 08-13 stays tech-led risk-on only.
FALSIFIER: If this trigger recurs (re-extended XLV, no HC catalyst, broad non-duration-led risk-off, PM ETF ~flat) and XLV still closes ≤ −0.3% with only a modest SPY drawdown, revise the S0≈0 / trust-tape rule rather than defend it.
DIVERGENCE_VERDICT: futures_right
ACTIVE_LESSON_REVIEW: 09-14 applied as fail → hurt (S0 −0.5 instead of ~0); 09-10 cap helped magnitude only; 08-13 misapplied; 08-17/08-28/08-21 helped relative/single-name discipline; 09-11/08-14 correctly off.
SECTOR: Healthcare
LESSON_END

⚠️ 🛠️ Exec failed: `python3 inline script (heredoc) (in ~/fullscan)`
