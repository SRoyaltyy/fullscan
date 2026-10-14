---
trigger_pattern: "A concentrated cyclical ETF card (XLI-like) lets a top-weight regulatory/certification headline govern S1 as a fresh same-morning negative even though HIT_GRID already marks that item miss/unverified/already-traded and no same-morning dated source shows the action is still in force."
current_behavior: "Counted BA 737 MAX 10 certification halt as fresh knowable-at-open S1=−1 (“MAX 10 = 10”) on a pre-CPI, S4=0, green-PM session; HIT_GRID already flagged the halt as unverified/already-traded. FAA had cleared the FMS glitch on Oct 2. Same-day BA tape was mixed (deliveries up, prior-day PT cut). Official label stayed down/mild via S0=−1 + calendar gate + divergence; the ETF close was CAT, not a live halt."
corrected_behavior: "Before a top-weight regulatory item can set S1≠0, require a same-morning dated primary source that the action is still live. If HIT_GRID says miss/unverified/already-traded, that item cannot govern S1 or satisfy a fresh-catalysis / “MAX 10 = 10” rule — leftover overhang at most. Keep the down lean in S0/calendar/divergence. Do not stack hawkish-Fed and a machinery multiple-cut as two industrials shocks (one cost-of-capital channel). Enforce the existing 08-14 stale-catalyst checklist on negative headlines, not only positives."
evidence_cited: "2026-10-13 XLI −0.2838% vs SPY −0.0116% (rel −0.2722%). Predicted down/mild; direction HIT; magnitude MISS (published Flat <0.3%, print is flat not mild). CAT ~−3.3% ≈ −22 bp of the ETF; BA −1.5% on deliveries/PT-cut, not a 10-13 halt; RTX −2.0% sell-the-news. S0=−1 and S4=0 matched; overlay −3.15 was too hot for −28 bp."
error_category: "B"
falsifier: "If an unverified top-weight regulatory item is held out of S1 and that name still drops ≥2% on the same overhang and takes XLI through mild-down (≥0.3%) while the rest of the book is flat, the same-morning confirmation bar is too strict."
sector: "Industrials"
date: "2026-10-13"
status: "candidate"
---

# Sector Reflection — Industrials — 2026-10-13

**TRIAGE:** Reasoning, not a tool/data outage. Direction **HIT** (XLI −0.284%). Magnitude **MISS** by the published bands (Flat <0.3% | Mild 0.3–1.0%): |−0.2838%| is **flat**, 1.6 bp under the mild floor. Scoreboard `magnitude_hit: False` is correct. Outcome’s `ACTUAL_MAGNITUDE: mild` is a classification error — do **not** treat this as the 08-14 scoreboard-accounting bug (that lesson’s falsifier fires). S0=−1, S4=0, divergence, and the CPI size-gate are what kept the official label from notable. S1 cited the **wrong** name: BA MAX 10 halt was not live (FAA clear 10-02). The ETF print was almost entirely **CAT ~−3.3%**, which was **not** knowable at open — discount missing CAT; do not discount the stale-halt miss.

---

**CHECK 1 — LESSON MATCH:** Matches the active 08-14 stale-catalyst rule (`a-catalyst-is-labeled-fresh-positive-based-on-deal-size-and`): verify whether the item already traded / is still live before it can govern S1. Morning HIT_GRID already marked `BA MAX 10 already traded yesterday` as **miss/unverified**, then still let “MAX 10 = 10” set S1=−1. That is an **enforcement/retrieval failure** of an existing lesson, not a new family. Sibling 08-13 Industrials oil-tape rule **did** fire (Hormuz live, CL offered → S0=−1 not −2). 08-25 candidate (S4=−1 **and** a *live* single-name negative → down/mild) did **not** fire: S4 was 0.

**CHECK 2 — BACKWARD TEST:** Injected Industrials book is a down-overcall tape (dir=0.3 n=10): 09-22 / 09-25 / 10-01 down/mild vs up closes. Blocking unverified top-weight headlines from governing S1 would have **helped** 10-13’s band (S1→0 leaves S0/calendar as a softer down/flat lean, which matches −28 bp) and would not have flipped 10-13’s direction. No other day in the last-10 list is a MAX-10-style halt. Broader “stale single-name cannot run S1” is not a one-day fit; 09-24’s down/mild HIT (−0.75%) is the only plausible hurt if that day needed a live single-name — unknown here, so **mixed / no exact analog**.

**CHECK 3 — CONFLICT:** None if “fresh” is dated and still in force. 08-25 stays: S4=−1 **plus** a confirmed same-morning top-weight negative → down/mild. 08-14 stale-positive is the same checklist on the other sign. 08-11/08-12 Hormuz caps still govern oil/S0, not BA certification.

**CHECK 4 — APPLIED-LESSON REVIEW:**
- 08-13 oil-down / stale Hormuz (Industrials): **applied, helped** — S0=−1, not a cost-push smash; WTI later +0.4%, still not a separate shock.
- 08-11/08-12 Hormuz magnitude cap: **applied, helped** — no up/severe, no notable.
- 09-16 score-fights-tape / S4=0 + divergence: **applied, helped** — blocked notable; close −28 bp after a −1.5% washout.
- Calendar size-gate (CPI/Powell unsigned): **mixed** — correctly blocked notable (smash was **10-14**); the “band ≥ mild” half is why the official label missed a flat print.
- 08-14 stale-catalyst: **matched, only half-applied** (oil yes, BA halt no).
- 08-14 mag-accounting: trigger looks similar; **falsifier fires** — 0.284% is outside mild.

**CHECK 5 — FALSIFIER:** If HIT_GRID flags a top-weight regulatory item unverified, S1 is held at 0, and that name still drops ≥2% on the *same* unresolved overhang and pulls XLI through mild-down (≥0.3%) while the rest of the book is flat, the “same-morning dated confirmation” bar is too high.

**Divergence:** Flagged. Factors right on **sign** (down); tape/PM right that it was **not** a smash. **leading_right**.

**Knowable-at-open discount:** CAT/RTX/oil-reversal/intraday −1.5% recover — no. MAX 10 as a *fresh* 10-13 halt — yes, knowably wrong.

LESSON_BEGIN
ERROR_CATEGORY: B
TRIGGER_PATTERN: A concentrated cyclical ETF card (XLI-like) lets a top-weight regulatory/certification headline govern S1 as a fresh same-morning negative even though HIT_GRID already marks that item miss/unverified/already-traded and no same-morning dated source shows the action is still in force.
CURRENT_BEHAVIOR: Counted BA 737 MAX 10 certification halt as fresh knowable-at-open S1=−1 (“MAX 10 = 10”) on a pre-CPI, S4=0, green-PM session; HIT_GRID already flagged the halt as unverified/already-traded. FAA had cleared the FMS glitch on Oct 2. Same-day BA tape was mixed (deliveries up, prior-day PT cut). Official label stayed down/mild via S0=−1 + calendar gate + divergence; the ETF close was CAT, not a live halt.
CORRECTED_BEHAVIOR: Before a top-weight regulatory item can set S1≠0, require a same-morning dated primary source that the action is still live. If HIT_GRID says miss/unverified/already-traded, that item cannot govern S1 or satisfy a fresh-catalysis / “MAX 10 = 10” rule — leftover overhang at most. Keep the down lean in S0/calendar/divergence. Do not stack hawkish-Fed and a machinery multiple-cut as two industrials shocks (one cost-of-capital channel). Enforce the existing 08-14 stale-catalyst checklist on negative headlines, not only positives.
EVIDENCE: 2026-10-13 XLI −0.2838% vs SPY −0.0116% (rel −0.2722%). Predicted down/mild; direction HIT; magnitude MISS (published Flat <0.3%, print is flat not mild). CAT ~−3.3% ≈ −22 bp of the ETF; BA −1.5% on deliveries/PT-cut, not a 10-13 halt; RTX −2.0% sell-the-news. S0=−1 and S4=0 matched; overlay −3.15 was too hot for −28 bp.
LESSON_MATCH_CHECK: matches 08-14 stale-catalyst verification — oil/Hormuz side applied; BA halt not applied despite HIT_GRID miss/unverified — retrieval/enforcement failure, not a new lesson family. 08-25 candidate does not match (S4 was 0).
BACKWARD_CHECK: mixed on recent Industrials (dir=0.3): stricter stale-S1 would likely help 09-22/09-25/10-01 down-overcalls; no exact halt analog in last 10; 09-24 down/mild HIT is the only plausible hurt if that day needed a live single-name.
CONFLICT_CHECK: none — 08-25 still fires only when S4=−1 AND the single-name negative is confirmed live; 08-14 is the sibling (stale positive); 08-11/08-12 remain oil/S0 caps.
FALSIFIER: If an unverified top-weight regulatory item is held out of S1 and that name still drops ≥2% on the same overhang and takes XLI through mild-down (≥0.3%) while the rest of the book is flat, the same-morning confirmation bar is too strict.
DIVERGENCE_VERDICT: leading_right
ACTIVE_LESSON_REVIEW: 08-13 oil-demand helped (S0=−1 not −2); 08-11/08-12 Hormuz cap helped (no notable); 09-16 S4=0/divergence helped (blocked notable); calendar ≥mild mixed (blocked 10-14 smash sizing, missed a flat print); 08-14 stale-catalyst half-applied (hurt S1 attribution); 08-14 mag-accounting falsifier fires so scoreboard mag miss stands.
SECTOR: Industrials
LESSON_END
