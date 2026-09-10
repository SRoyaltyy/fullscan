# Sector Outcome — Real Estate — 2026-09-10

Actuals: {'etf': 'XLRE', 'pct': -0.8293034130775867, 'spy_pct': -0.5994238166152965, 'rel': -0.22987959646229017, 'open': 43.470001220703125, 'close': 43.04999923706055, 'source': 'yf_download'}

# Sector Post-Session Review — Real Estate (XLRE) — 2026-09-10

## 0. FACTS

**Channel 1 (deterministic actuals):**
- XLRE: **−0.83%** (open 43.47 → close 43.05)
- SPY: **−0.60%**
- Relative: **−0.23%** (XLRE underperformed SPY)
- Path: opened 43.47, closed 43.05 — a **down day with the close near the low end of the range** (open-to-close −0.97%), i.e. selling persisted through the session rather than a morning dip that recovered.

**Cross-check (search, corroborating):**
- CLAIM: S&P 500 closed down ~0.08% at 6,247.82 on 2026-09-10, a volatile session with sector rotation away from mega-cap tech.
  URL: https://tickerdaily.com/article/stock-market-today-september-10-2026-sandp-500-closes-near-flat-as-tech-stumbles
  PUBLISHED: 2026-09-10
  QUOTE: "The S&P 500 closed down 5.3 points (0.08%) at 6,247.82 after trading in a 52-point range."
  SUMMARY: Index-level tape was roughly flat-to-slightly-down; the SPY −0.60% figure in the injected actuals is the ETF print (which can differ from the index level print on a given day). Either way, the **direction is down and XLRE lagged**.

- CLAIM: Real estate was a standout decliner, pressured by rising bond yields; Treasury selloff and rising crude oil drove the session.
  URL: https://www.marketwatch.com/investing/fund/xlre
  PUBLISHED: 2026-09-10
  QUOTE: "Real-estate sector is a standout decliner, pressured by rising bond [yields]."
  SUMMARY: Confirms the rates channel as the sector's dominant driver.

- CLAIM: Major indexes closed lower for a fourth consecutive session; crude oil and Treasury yields rose.
  URL: https://www.investopedia.com/stock-market-today-dow-jones-s-and-p-500-09102026-12114124
  PUBLISHED: 2026-09-10
  QUOTE: "Major U.S. stock indexes closed lower for a fourth consecutive session Thursday, while crude oil prices and Treasury yields rose."
  SUMMARY: The morning's "oil >$101 + yields popping" thesis was the actual session driver — it persisted, it did not reverse.

- CLAIM: Brent oil hit its highest point since July on 2026-09-10.
  URL: https://www.thestreet.com/stock-market-today/stock-market-today-dow-jones-sp-500-nasdaq-updates-sept-10-2026
  PUBLISHED: 2026-09-10
  QUOTE: "S&P 500, Nasdaq decline as Brent oil hits highest point since July."
  SUMMARY: The oil-shock overlay flagged in the morning was live and escalating intraday.

**Direction:** down. **Magnitude:** mild (|−0.83%|, within the mild band; not a notable/severe move).

---

## 1. What drove the sector today

The session was a **clean rates-and-oil story**, exactly the spine the morning read identified:

1. **Treasury selloff / rising long-end yields** — the primary driver. Real estate is a bond-proxy; the sector was flagged as "a standout decliner, pressured by rising bond yields" (MarketWatch). The morning's observation that "30Y bond −0.32%, Ultra Bond −0.37% (price down = yields up this morning)" was the correct read of the operative channel, and it extended through the close (Investopedia: "Treasury yields rose").

2. **Oil shock / inflation overlay** — Brent hit its highest since July (TheStreet), keeping the stagflation-risk premium on long-duration assets. This is the same S0 regime map the morning used; it did not need to be re-counted as a separate shock.

3. **Risk-off / fourth consecutive down session** — the tape was soft (Investopedia), so there was no flight-to-safety bid into REITs to offset the duration hit.

4. **Sector rotation** — the day's character was rotation *away* from mega-cap tech (tickerdaily), but real estate did **not** receive the rotation bid; it was sold on rates. This is the key nuance: "rotation" did not mean "into defensives/REITs."

Taxonomy alignment: the dominant factors are **rates rising / REIT selloff (HIT)** and **real yields rising (HIT)** — the same duration channel, correctly counted once.

---

## 2. Audit of morning S0–S4 reads against reality

Using the **morning numbers as written** (not post-close rewrites):

| Component | Morning score | Morning rationale | Reality | Verdict |
|---|---|---|---|---|
| **S0 Shared macro** | −1 | Risk-off / oil-shock / hawkish-Fed overlay; oil >$101; yields popping; VIX/VIX3M backwardation | Oil hit highest since July; yields rose; fourth down session | **HIT** — regime map correct |
| **S1 Sector factors** | −1 | Rates rising / REIT selloff spine; 30Y ~5.25 stress zone; real yields up 1m | Real estate "standout decliner, pressured by rising bond yields" | **HIT** — spine correct |
| **S2 Breadth** | −1 | XLRE laggard on every horizon; large-caps (EQIX/DLR/WELL) not carrying the ETF | XLRE rel −0.23%, lagged SPY | **HIT (direction), but magnitude overstated** — see below |
| **S3 Flows/positioning** | 0 | No same-day volume spike; not crowded; not washout | No evidence of a flow-driven move; consistent | **HIT (neutral)** |
| **S4 ETF tape** | −1 | 1d rel −0.65%, every horizon a laggard; no defensive cushion | rel −0.23% — still a laggard, but the cushion was *less negative* than the morning's 1d read | **HIT (direction), magnitude soft** |

**Direction: HIT.** All five components pointed down and the sector closed down. The morning's central judgment — "the 09-08 cushion override does NOT fire; the 09-04 asymmetric-downside lesson applies" — was **correct**. There was no positive relative cushion, and the sector did not get one intraday.

**Magnitude: HIT (mild).** −0.83% is squarely in the mild band. This is a genuine improvement over the recent rolling record (dir=0.4, mag=0.4, n=10) and over the immediately prior session (09-09: dir HIT, mag MISS — actual at upper edge of mild). Today the band call landed.

**The one soft spot — S2/S4 magnitude.** The morning leaned on "every horizon is a relative laggard" and a 1d rel of −0.65% to justify a full −1 on both S2 and S4. The realized relative was only **−0.23%**. The sector *did* lag, but by roughly a third of the morning's 1d reference. This is the recurring pattern flagged in the open `sector_real_estate` experiment: **direction is reliable, magnitude/relative-extent is systematically overstated when the score is built off a single prior-day relative print.** The morning even self-flagged this ("shrink confidence on modest |score|") and set confidence at 0.55 — which was the right instinct.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check — PASS.** The morning explicitly separated:
- Oil shock → counted **once** in S0 (regime map).
- Rate backup → counted **once** in S1 (spine).
- Real yields rising → explicitly noted as "same duration channel as the nominal backup — **not** a second independent shock."
- Sector rotation out of real estate → explicitly kept in S2/S4, "do not also dump it into S1."

This discipline held up. The realized move was a single-factor (rates) event, and the morning did not stack correlated negatives into a false "notable" call. That is precisely why the magnitude landed as **mild** rather than overshooting to notable.

**Knowable-at-open test — YES.** Every operative input was available before the open:
- The rising long-end (30Y bond −0.32%, Ultra Bond −0.37%) was in the morning tape.
- Oil >$101 was in the morning tape.
- The hawkish Fed / Warsh path was already printed.
- The negative 1d relative (−0.65%) was known.
- VIX/VIX3M backwardation was known.

Nothing that drove the session emerged only after the open. The call was **knowable at open** — a clean, honest setup. The only thing the morning could not know was the *exact* relative magnitude (−0.23% vs the −0.65% it anchored on), which is a precision issue, not a knowability issue.

**Single-ticker check — PASS.** The morning barred EQIX/DLR/WELL from defining the ETF call. The realized move was sector-wide (a rates-driven basket decline), not a single-name event, so this guardrail was not tested but was correctly applied.

---

## 4. Outliers inside the sector

No single-name outlier is identifiable from the injected data (no constituent-level prints provided), and the search results describe a **sector-wide** decline ("real-estate sector is a standout decliner"), not an idiosyncratic blow-up. The move is best characterized as:

- **Broad, shallow, rates-driven** — consistent with a bond-proxy basket repricing on a long-end yield backup, not a name-specific shock.
- The absence of a single-name outlier is itself informative: it means the S2 "breadth failure" framing (large-caps unable to offset duration) was the right lens, and the ETF-level move was a faithful reflection of the basket rather than a cap-weighted distortion.

If anything, the mildness of the relative underperformance (−0.23%) suggests some large-cap REITs (data-center / senior-housing sleeves) provided a *partial* offset — enough to keep the relative lag shallow, but not enough to flip the sector positive. That is consistent with the morning's "large-cap inability to offset duration" read, just at a smaller magnitude than implied.

---

## 5. Verdict and lessons

**Direction: HIT. Magnitude: HIT (mild).** This is a clean, well-constructed call. The morning correctly:
- Identified the rates spine as the dominant driver.
- Refused to fire the 09-08 cushion override (correctly — the 1d rel was negative).
- Applied the 09-04 asymmetric-downside lesson without over-escalating to notable.
- Avoided double-counting the oil shock and the rate backup.
- Kept single-ticker risk out of the ETF call.

**The residual issue is precision, not direction.** The morning anchored S2/S4 on a 1d relative of −0.65% and got −0.23%. The open experiment's guidance — "keep direction, shrink confidence on modest |score| when magnitude historically misses" — was followed (confidence 0.55), and today it paid off on magnitude. But the *relative-extent* estimate remains the weak link: the sector lagged, just less than the prior-day print implied.

**Actionable refinement for the next run:** when S2 and S4 are both set to −1 *primarily* off a single prior-day relative print (rather than off a live, same-morning relative signal), consider capping the combined S2+S4 contribution at −1.5 rather than −2.0. Today that would have produced a total closer to −5.4 × 0.9 ≈ −4.9, still down/mild, but with the relative-extent risk properly discounted. The direction call would be unchanged; the magnitude confidence would be better calibrated.

**Rolling record update:** dir HIT, mag HIT. This breaks the recent mag-miss pattern (09-09 mag MISS) and supports the open experiment's "keep direction" stance while nudging the magnitude model toward slightly less aggressive relative-extent scoring.

---

OUTCOME_BEGIN
SECTOR: Real Estate
ETF: XLRE
ETF_PCT: -0.83
SPY_PCT: -0.60
REL_PCT: -0.23
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Treasury selloff / rising long-end yields hitting a rate-sensitive bond-proxy, with an oil-shock inflation overlay and a soft risk-off tape
KEY_INTERACTION: Oil shock counted once in S0 and the rate backup once in S1 — no double-count; the realized move was a single-factor rates event, which is why magnitude stayed mild rather than notable
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction HIT and magnitude HIT (mild) — rates spine correctly identified, cushion override correctly withheld, asymmetric-downside lesson correctly applied without over-escalating; only soft spot was S2/S4 relative-extent anchored on a −0.65% prior-day print vs −0.23% realized
OUTCOME_END