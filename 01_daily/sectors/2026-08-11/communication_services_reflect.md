# Sector Reflect — Communication Services — 2026-08-11

LESSON_BEGIN
ERROR_CATEGORY: B — REASONING / SIGNAL-WEIGHTING. (Not a pure tool/data failure: the deterministic pipeline faithfully summed the supplied component scores; the miss came from scoring S0 as risk_on while a knowable geopolitical/oil risk-off signal existed, and from double-counting related positives in S1/S3.)

TRIGGER_PATTERN: An effectively two-stock sector ETF (Meta + Alphabet heavy) has a bullish case built from structural positives that are really one underlying thesis, prior-week flows are treated as same-day support, the largest holdings have unresolved capex/FCF vulnerability, and a fresh geopolitical/oil supply-shock risk is active at the open while futures are flat. The model emits up/notable from structural positives instead of flat/down caution.

CURRENT_BEHAVIOR: S0_SHARED_MACRO is scored 0 because premarket futures are flat and real yields eased on 1d/1w; S1 counts digital-ad recovery and AI monetization as separate positives; S3 treats last week’s $3.8B inflow/rotation as same-day support; the active lesson “mega-cap-earnings-over-macro-drag” is used to override macro concerns. Total score 7.5 → up/notable.

CORRECTED_BEHAVIOR: Deduplicate sector positives: ad-spend recovery + AI monetization = one ad/AI thesis; rotation + sector inflows = one flow observation. Before emitting an up call on XLC, check for knowable-at-open geopolitical/oil/high-impact-print suppressors. If a live geopolitical risk-off signal is present, score S0 negatively and cap the call at flat/down caution. Flat futures should be treated as non-confirmation, not as bullish confirmation. Do not extend “mega-cap-earnings-over-macro-drag” to a live geopolitical supply shock.

EVIDENCE: Predicted up/notable but actual XLC fell roughly -0.4% to -0.5% while SPY fell -0.32%; relative return was negative. Direction MISS and magnitude MISS. The Strait of Hormuz standoff and oil near $83 were knowable at the open, plus CPI caution was looming. Positive ad/AI and prior-week inflow facts did not protect a concentrated growth/duration sector in a risk-off tape. The morning’s S1/S3 scoring also double-counted the same ad/AI/flow story, inflating the score.

LESSON_MATCH_CHECK: Exact match to candidate lesson 2026-08-11_sector_communication_services_lesson.md: duplicate HITs of the same fundamental story, prior-week flow treated as same-day support, mega-cap capex/FCF vulnerability, and open-tape risk-off cues not acted on. It also matches the broader 2026-08-11 sector-lesson family about live geopolitical/oil suppressors being knowable at the open.

BACKWARD_CHECK: Pass. This corrected behavior would have capped the 2026-08-11 call at flat/down caution and avoided the up/notable miss. It does not contradict the 2026-08-10 down/mild miss because that was already a cautious call; the lesson only blocks unearned bullish calls when a live risk-off signal is present. Sample size is small (n=2), but no older correct decision is contradicted.

CONFLICT_CHECK: No conflict with the active lesson “mega-cap-earnings-over-macro-drag”; this lesson narrows its boundary so it does not override a live geopolitical/oil shock. No conflict with the other 2026-08-11 candidate sector lessons; they share the same risk-off/open-tape theme. If any active lesson claims flows/rotation always override macro, it would need to be constrained by this rule.

FALSIFIER: The rule would be falsified if an identical setup — live Hormuz-style geopolitical/oil risk, flat futures, positive ad/AI fundamentals, prior-week inflows — still produced an XLC up/notable close. It would also be weakened if prior-week flows were shown to reliably protect a two-stock sector on same-day risk-off opens.

DIVERGENCE_VERDICT: futures_right — the divergence was flagged, but the model sided with the leading structural positives; the macro/futures/non-confirmation side, including the geopolitical/oil signal, was the side that matched the actual risk-off close.

ACTIVE_LESSON_REVIEW: The active lesson “mega-cap-earnings-over-macro-drag” was applied and likely contributed to the miss by being stretched beyond its scope. It should be amended: it applies to ordinary macro drag, but not to fresh knowable geopolitical supply shocks. The Communication Services candidate lesson should be activated as an active sector lesson.

SECTOR: Communication Services
LESSON_END
