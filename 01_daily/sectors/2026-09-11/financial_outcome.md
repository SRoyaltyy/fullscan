# Sector Outcome — Financial — 2026-09-11

Actuals: {'etf': 'XLF', 'pct': 0.6681925008832357, 'spy_pct': 0.8524287494320992, 'rel': -0.18423624854886356, 'open': 57.439998626708984, 'close': 57.25, 'source': 'yf_download'}

# Sector Post-Session Review — Financial (XLF) — 2026-09-11

## 0. FACTS

**Channel 1 (actuals, deterministic):**
- XLF: **+0.67%** (open 57.44 → close 57.25; note the close is *below* the open, so the session was a fade from a gap-up)
- SPY: **+0.85%**
- Relative: **−0.18%** (XLF underperformed SPY)
- Path: gapped up, faded through the session, closed near/below the open

**Cross-check (search):**
- CLAIM: XLF closed 57.25, +0.38, +0.67% on 09/11/2026.
  URL: https://stockanalysis.com/etf/xlf/ (aggregated quote)
  PUBLISHED: 2026-09-11
  QUOTE: "XLF 57.25 +0.38 +0.67% 09/11/2026"
  SUMMARY: Confirms the deterministic ETF_PCT of +0.67%. Also notes RSI 46, price below 50-day ($57.11) — i.e., XLF is *not* in an uptrend despite the green day.

- CLAIM: August CPI rose 0.4% m/m and 3.4% y/y, released 8:30 ET 09/11/2026; gasoline rose.
  URL: https://www.bls.gov/cpi/
  PUBLISHED: 2026-09-11
  QUOTE: "CPI for all items increases 0.4% in August; gasoline rises... rose 0.4 percent... and rose 3.4 percent over the last 12 months."
  SUMMARY: The pending binary resolved **hot** (3.4% y/y, well above target). This is the single most important fact the morning note flagged as "load-bearing."

- CLAIM: Investors believe the Fed is very likely to **raise** rates at next week's meeting.
  URL: https://www.nytimes.com/live/2026/09/11/business/inflation-cpi-report
  PUBLISHED: 2026-09-11
  QUOTE: "U.S. inflation showed little improvement in August, running at a 3.4 percent annual rate. Investors believe the Federal Reserve is very likely to raise rates at its meeting next week."
  SUMMARY: The hot CPI did **not** produce a risk-off tape — SPY rose +0.85%. The market absorbed a hike-odds increase as a "good news is good news" / nominal-growth read rather than a duration shock.

**Direction:** up. **Magnitude:** mild (XLF +0.67%, but *relative* −0.18% — a mild absolute up, a mild relative miss).

---

## 1. What drove the sector today

**Primary driver: the CPI binary resolved hot, and the tape traded it as a nominal-growth / reflation day, not a duration day — but financials captured less of that beta than the index.**

Taxonomy-aligned decomposition:

1. **Shared macro (S0) — the dominant force, and it flipped sign intraday relative to the morning's "neutral" read.** The morning note correctly identified CPI as the load-bearing binary but scored S0 = 0 (mixed). In reality the hot print (3.4% y/y, 0.4% m/m, gasoline-led) plus a *green* futures tape produced a broad risk-on session: SPY +0.85%. The oil-relief thesis (WTI −2.5% pre-open) held, and the market chose to read higher-for-longer rates as confirmation of nominal strength rather than as a discount-rate shock. That is a **risk-on tape / equity beta expansion** outcome — the exact HIT_GRID row the morning scored HIT at 0.70.

2. **Sector factors (S1) — the long-end/credit channel was a genuine but modest *relative* headwind, and it showed up in the rel number.** The morning capped S1 at 0 per the 09-10 lesson (1d rel +0.27% < the 08-18 +0.4% gate → transmission not confirmed). The outcome validates the *cap* but not the *sign*: XLF underperformed by −0.18%. A hot CPI that raises hike odds steepens the front end and keeps the long end (30Y 5.28%) pinned; that is a **relative** drag on a rate-sensitive, long-duration-equity sector even on an up day. The morning's instinct to not stack S1 as an independent negative was right for *absolute* direction, but the −0.18% rel shows the channel was live at the margin.

3. **Breadth (S2) — no live confirmation either way.** The morning scored S2 = 0 (no premarket BKX/XLF breakdown). Outcome: XLF rose but lagged, consistent with **large-cap leadership inside the sector** (money-center banks carrying the ETF) rather than broad regional participation. The HIT_GRID "large-cap leadership" HIT at 0.55 was directionally correct; "small/mid leadership" MISS was correct.

4. **Flows (S3) — trailing outflows persisted; no fresh inflow.** Scored 0. Outcome neutral-to-slightly-negative for relative performance; consistent with the lag.

5. **ETF tape (S4) — the +0.5 leading input was the only positive score, and it was the right one.** The 1d rel +0.27% base case ("neutral-to-positive relative") was *too generous*: XLF actually printed −0.18% rel. The tape signal was real but the magnitude was overstated.

**Net:** the sector went **up** (absolute) but **underperformed** (relative). The morning's "flat" call was wrong on absolute direction (it was up, mildly) and wrong on relative (it was a mild miss, not flat). The single biggest miss was **underweighting the hot-CPI → risk-on beta expansion** path.

---

## 2. Audit of morning S0–S4 reads against reality

Using the **morning numbers as written**, not post-close rewrites:

| Score | Morning value | Morning rationale | Reality | Verdict |
|---|---|---|---|---|
| **S0** | 0 | "Mixed, binary pending, oil relief vs long-end stress" | Hot CPI + green futures → broad risk-on; SPY +0.85% | **MISS on magnitude/sign.** The binary resolved and the market took it risk-on. S0 should have been **+1** (mild positive), not 0. |
| **S1** | 0 | Capped per 09-10 lesson; transmission not confirmed by tape | XLF rel −0.18% → the long-end/credit channel *was* a mild relative drag | **Cap was defensible, sign was wrong.** S1 should have been **−0.5** (mild relative negative), not 0. |
| **S2** | 0 | No live breakdown | Large-cap-led, regionals lag | **HIT.** 0 was correct. |
| **S3** | 0 | Trailing outflows, no spike | Outflows persisted | **HIT.** 0 was correct. |
| **S4** | +0.5 | 1d rel +0.27% → neutral-to-positive | Actual rel −0.18% | **MISS on sign.** S4 should have been **0 to −0.5**; the tape was a *fade*, not a base. |

**Leading sum (S0–S3) = 0 vs S4 = +0.5 → the morning flagged a "mild divergence" and chose to trust the live macro over the modest tape.** That choice was **half right**: trusting the macro (green futures, oil relief) correctly anticipated an *up* absolute day. But the divergence resolution should have been **"up but lagging,"** not "flat." The morning collapsed a two-sided setup into a single flat band.

**The critical error:** the morning treated CPI as a *symmetric* binary ("cool print relieves, hot print re-spikes yields and unwinds relief") and therefore held magnitude at flat. In reality the hot print did **not** unwind the risk-on tape — SPY rose. The asymmetry the morning *should* have pre-scored was: **hot CPI + green futures = risk-on nominal-growth day, financials participate but lag on rate sensitivity.** That is a mild-up / mild-rel-miss configuration, not flat.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check:**
- Oil/yields were counted **once** in S0 (correct — no restack into S1/S2/S3).
- The 3d/1w rel lag was explicitly *not* triple-counted into S2/S3/S4 (per 08-28 lesson) — correct discipline.
- The long-end steepener was scored in S0 context only, not as NIM+ (per 08-17) — correct.
- **No material double-count found.** The scoring architecture was clean; the *inputs* were mis-weighted, not double-counted.

**Knowable-at-open test:**
- **CPI was released at 8:30 ET, before the open.** The hot print (3.4% y/y) was **fully knowable at the open**. The morning note was written *pre-print* and explicitly held magnitude at flat "pre-print." That is a legitimate process choice — but it means the *flat* call was a **pre-catalyst placeholder**, not a post-catalyst read. Once CPI printed hot and futures stayed green, the correct at-open read was **mild up / mild rel-miss**, not flat.
- **The gap-and-fade path was knowable at open:** XLF opened 57.44 and closed 57.25 — it gapped up and sold off. The morning's own note flagged "XLF trailing outflows" and "3d/1w rel still lag" — both consistent with a fade. The tape told you the gap would not hold; the morning chose to trust the macro instead.
- **Verdict: KNOWABLE_AT_OPEN = partially.** The *direction* (up) was knowable once CPI printed hot + futures green. The *relative miss* was knowable from the persistent 3d/1w rel lag + trailing outflows + the rate-sensitivity of the sector to a hot print. The morning had all the pieces but resolved them to "flat" instead of "up/lagging."

---

## 4. Outliers inside the sector

- **The gap-and-fade itself is the outlier.** XLF opened +0.67% above prior close and closed *below* its open. On a day SPY rose +0.85%, XLF's inability to hold its gap is the defining intra-sector feature. This is a **relative-weakness signature**, not a beta-expansion signature.
- **Large-cap vs regional dispersion:** the morning's HIT on "large-cap leadership" and MISS on "small/mid leadership" implies money-center banks (JPM, BAC, WFC, C) carried the ETF while regionals lagged. On a hot-CPI / higher-for-longer day, that is the expected pattern — large banks have less NIM sensitivity to front-end hike odds and more capital-markets/IB torque, while regionals carry CRE/funding overhang. **No single-name blowup is evidenced**; the dispersion is structural, not idiosyncratic.
- **Foreign/Canadian bank headlines (BBVA, BCS, BNS)** were correctly excluded from S1 by the morning note — they are not XLF drivers. No evidence they moved the ETF. **Correct exclusion.**
- **No credit event, no deposit-flight headline, no delinquency spike** — the tail risks the HIT_GRID scored MISS all stayed MISS. The sector's *downside* tail did not fire; the miss was purely a **relative** one.

---

## 5. Scorecard and lessons

**Morning call:** flat / flat, total 0.225, confidence 0.5.
**Actual:** up / mild, rel −0.18%.

- **Direction: MISS** (called flat, got up).
- **Magnitude: MISS** (called flat, got mild).
- **Relative: MISS** (implied flat rel, got −0.18%).

This is a **clean miss on all three axes**, driven by one root cause: **the morning under-weighted the hot-CPI → risk-on beta-expansion path and over-weighted the "binary pending → hold flat" prior.**

**Binding lessons for the Financial scoreboard:**

1. **When CPI is the load-bearing binary and it prints hot *with green futures*, do not hold flat.** The market's revealed preference on 09-11 was to trade higher-for-longer as nominal-growth-positive. Pre-score the *post-print* configuration, not the pre-print placeholder. A hot print + green tape = **mild up, mild rel-miss** for financials (rate-sensitive lag), not flat.

2. **The 09-10 "cap S1 at 0" lesson needs a sign qualifier.** Capping S1 at 0 was right to prevent *stacking* a macro narrative as an independent negative — but on a hot-CPI day the long-end/credit channel is a **real relative drag** even when absolute direction is up. The cap should be **"cap magnitude, not sign"**: allow S1 = −0.5 (mild relative negative) when the rate channel is confirmed by a hot print, while still refusing to stack it into a −2.

3. **A gap-up that fades is a relative-weakness tell.** The morning had trailing outflows + 3d/1w rel lag + a sub-gate 1d rel (+0.27% < +0.4%). That combination should have biased the *relative* read negative, not neutral. The 08-18 gate correctly said "don't call two-sided rotation *in*" — but it should also have said "don't call relative *flat* when the tape is fading."

4. **The 08-21 "green-futures ban-on-down" rule worked** — the morning correctly refused a down call. But it over-corrected into flat when the evidence supported mild-up. The rule bans *down*, it does not mandate *flat*.

**One-line verdict:** The morning correctly identified CPI as the binary and correctly refused a down call, but it resolved a hot-print + green-futures setup into "flat" instead of "mild up / mild relative miss" — missing all three axes on a day the sector rose +0.67% while lagging SPY by −0.18%.

OUTCOME_BEGIN
SECTOR: Financial
ETF: XLF
ETF_PCT: 0.67
SPY_PCT: 0.85
REL_PCT: -0.18
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Hot August CPI (3.4% y/y) + green futures traded as risk-on nominal growth; financials participated but lagged on rate sensitivity
KEY_INTERACTION: Hot CPI raised hike odds (long-end/credit drag → relative underperformance) while the broad tape read it as nominal-growth-positive (absolute up) — the two channels split the outcome into up-but-lagging
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Correctly flagged CPI as the binary and refused a down call, but resolved a hot-print + green-futures setup to "flat" instead of "mild up / mild rel-miss" — missed direction, magnitude, and relative
OUTCOME_END