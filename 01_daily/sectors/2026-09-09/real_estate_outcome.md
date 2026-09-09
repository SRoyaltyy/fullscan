# Sector Outcome — Real Estate — 2026-09-09

Actuals: {'etf': 'XLRE', 'pct': -1.1161769053195658, 'spy_pct': -0.46477589645284034, 'rel': -0.6514010088667255, 'open': 43.75, 'close': 43.40999984741211, 'source': 'yf_download'}

# Sector Post-Session Review — Real Estate (XLRE) — 2026-09-09

## 0. FACTS

- **XLRE**: −1.12% (open 43.75 → close 43.41)
- **SPY**: −0.46%
- **Relative**: −0.65% (XLRE underperformed SPY by 65 bps)
- **Path**: Opened at 43.75, closed at 43.41 — a steady grind lower through the session with no meaningful intraday bounce. The close was near the lows, suggesting persistent selling pressure rather than a dip-buying tape.

## 1. What Drove the Sector Today

The dominant driver was the **negative rate spine** — the exact mechanism predicted in the morning analysis. The macro backdrop was a live risk-off / oil-shock / hawkish-Fed day:

- **Oil >$100** (Brent ~$100, WTI ~$94–99) on US strikes on Iranian oil tankers and Iran's attack on a US base in Jordan — an escalating geopolitical/oil supply shock that adds inflation/stagflation risk to long-duration assets.
- **Gold down >3%** after Fed Chair Warsh's Jackson Hole hawkish comments boosted September rate-hike expectations — a regime-level real-yield/duration stress signal.
- **Key CPI data due this week** — "This Week's Inflation Data Will Decide If the Fed Hikes Rates" — keeping rate-sensitive sectors on edge.
- **US markets pointed lower** — risk-off equity tape.

For XLRE — a rate-sensitive bond-proxy with 1w/1m relative lags and **no defensive cushion** (1d rel was −0.21% at the morning read) — the transmission was direct: rising long-end yields + oil-driven inflation + hawkish Fed repricing + risk-off tape = REITs sold off.

The magnitude (−1.12% vs SPY −0.46%, rel −0.65%) is consistent with the **asymmetric-downside lesson (09-04)**: when the structural backdrop is hawkish/unresolved and the sector is a rate-sensitive bond-proxy with lags, pre-score downside rather than treating a flat-to-easing open as a symmetric offset. There was no cushion today, so the 09-08 cushion override correctly did not fire.

## 2. Audit of Morning S0–S4 Reads

**S0_SHARED_MACRO (−1): HIT.** The morning correctly identified this as a risk-off / oil-shock / hawkish-Fed day. Oil >$100, gold crushed, US futures lower — all confirmed. The regime call of `risk_off` was accurate. The macro map was not a re-derivation of the same shock but a genuine regime assessment.

**S1_SECTOR_FACTORS (−1): HIT.** The spine negative — "Rates rising / REIT selloff" — was the correct call. The morning correctly counted the oil>100 + hawkish-Fed + gold-crush as ONE rate/duration shock, not multiple independent shocks. The secondary factors (DC sleeve, industrial, office) were correctly treated as nested exceptions that should not define the ETF. No double-counting occurred.

**S2_BREADTH (−1): HIT.** The morning read MAP HEAT as showing a broad REIT complex weak-to-flat with only a narrow DC/specialty exception. The actual tape — XLRE down 1.12% with no intraday bounce — is consistent with sector-wide duration weakness, not a narrow single-name story.

**S3_FLOWS_POSITIONING (0): HIT.** The morning scored this neutral — no same-day volume spike, not a crowded long. The actual move was macro-driven, not flow-driven. Neutral was appropriate.

**S4_ETF_TAPE (−1): HIT.** The morning correctly read the tape as a relative laggard at every horizon (1d/3d/1w/1m all negative relative) with **no defensive cushion**. The 09-08 cushion override (which requires ≥ +0.4% 1d relative) correctly did NOT fire because the 1d rel was −0.21%. The asymmetric-downside lesson applied instead.

**Multiplier (0.9):** Applied correctly per the 08-14 reconcile lesson — modest |score| with historical magnitude misses warrants shrinking confidence.

## 3. Interactions / Double-Count / Knowable-at-Open Test

**Interactions:** The key interaction was between the oil shock and the hawkish Fed repricing. Both feed the same duration channel — rising long-end yields and inflation expectations hurt bond-proxy REITs. The morning correctly treated this as ONE rate/duration shock in S1, with S0 as the regime map rather than a second copy of the same backup. No double-counting.

**Knowable-at-open test:** Was this move knowable at the open? **Yes.** The macro inputs (oil >$100, gold −3%, hawkish Fed repricing, risk-off futures) were all live before the US cash open. The morning analysis identified the correct mechanism (negative rate spine, asymmetric downside) and the correct magnitude band (mild, not notable). The actual move of −1.12% with rel −0.65% is squarely within the predicted down/mild band.

**One nuance:** The magnitude was at the upper end of "mild" — the relative underperformance of −0.65% was larger than the 1d rel of −0.21% suggested at the morning read. This is consistent with the 09-04 asymmetric-downside lesson: when the structural backdrop is hawkish/unresolved, the sector can underperform more than the recent 1d tape would suggest. The morning's cap at mild (not notable) was disciplined given the modest |score|, but the actual move shows the downside asymmetry was real.

## 4. Outliers Inside the Sector

From the Yahoo Finance sector data available: Real Estate Services was down −2.07% (worse than the ETF), while REIT-Mortgage was down −0.98% (better than the ETF). This is consistent with the morning's MAP HEAT read: Mortgage REITs were a relative firm spot (up in the heat map), while the broader complex was weak. The DC/specialty sleeve (EQIX/AMT) likely held up better than the index given the AI demand narrative, but this was a nested exception that did not define the ETF — consistent with the morning's single-ticker discipline.

---

OUTCOME_BEGIN
SECTOR: Real Estate
ETF: XLRE
ETF_PCT: -1.12
SPY_PCT: -0.46
REL_PCT: -0.65
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Negative rate spine — oil >$100 geopolitical shock + Warsh hawkish Fed repricing (gold −3%) + risk-off tape into CPI week
KEY_INTERACTION: Oil shock and hawkish Fed repricing both feed the same duration channel — counted once in S1, with S0 as regime map (no double-count)
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction and magnitude both HIT — down/mild correctly predicted; asymmetric-downside lesson (09-04) applied correctly over the cushion override (09-08), which properly did not fire
OUTCOME_END