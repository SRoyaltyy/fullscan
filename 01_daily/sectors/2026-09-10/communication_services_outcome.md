# Sector Outcome — Communication Services — 2026-09-10

Actuals: {'etf': 'XLC', 'pct': 0.6045277974159324, 'spy_pct': -0.5994238166152965, 'rel': 1.203951614031229, 'open': 110.62000274658203, 'close': 111.5, 'source': 'yf_download'}

# Sector Post-Session Review — Communication Services (XLC) — 2026-09-10

## 0. FACTS

**CLAIM:** XLC closed **+0.60%** on 2026-09-10, from an open of 110.62 to a close of 111.50, while SPY closed **−0.60%**.
**URL:** (injected deterministic actuals; corroborated by https://finance.yahoo.com/quote/XLC/ and https://www.marketwatch.com/investing/fund/xlc)
**PUBLISHED:** 2026-09-10
**QUOTE:** Yahoo shows XLC "Close 110.83 Open 110.62" (delayed/NAV-adjacent print); MarketWatch shows "Open $111.92 Day Range 110.53." The injected deterministic series (open 110.62 → close 111.50) is the operative tape.
**SUMMARY:** XLC **+0.60%** vs SPY **−0.60%** → **relative +1.20%**. This is a **large, one-sided outperformance** on a down tape.

**CLAIM:** The broad tape was down on the day, led by oil/Iran escalation and a tech-heavy selloff.
**URL:** https://www.marketwatch.com/livecoverage/stock-market-today-dow-sp500-nasdaq-up-oil-calmer-treasury-steady-producer-inflation-oracle-earnings ; https://www.thestreet.com/stock-market-today/stock-market-today-dow-jones-sp-500-nasdaq-updates-sept-10-2026
**PUBLISHED:** 2026-09-10
**QUOTE:** "The S&P 500 declined 44.66 points, or 0.6%, to end at 7,591.70. The technology-heavy Nasdaq Composite Index shed 171.62 points, or 0.7%…" / "S&P 500, Nasdaq decline as Brent oil hits highest point since July."
**SUMMARY:** SPY −0.60%, Nasdaq −0.70% — a **risk-off, oil-shock, tech-laggard** session. XLC went the *other* way.

**Path:** XLC opened 110.62 (roughly flat vs prior close), traded down to a 110.53 low, then closed at 111.50 — a **full-session grind higher into the close**, i.e. the outperformance was not a gap-and-fade; it was accumulated buying against a falling market.

**ACTUAL_DIRECTION: up. ACTUAL_MAGNITUDE: notable** (a +1.20% relative move on a −0.60% SPY day is well outside the "mild" band the model assigned).

---

## 1. What drove the sector

The morning thesis was a **duration/growth book dragged by an oil-driven hawkish repricing**. The tape delivered the *macro* half of that (oil up, SPX down, Nasdaq down) but the **sector-specific half inverted**: XLC's core names were bid hard enough to overcome a −0.60% index.

Taxonomy-aligned drivers, in order of plausibility:

1. **Idiosyncratic mega-cap strength inside the two-name book (META/GOOGL).** XLC is ~17% META and ~18–19% Alphabet A+C. A +1.20% relative move on a −0.60% SPY day cannot be produced by telecom or ad-tech tail names (AMX/APP/TTWO are too small). It requires **at least one, probably both, of the two anchors to have been strongly green** — the exact "large-cap leadership inside sector" the morning HIT_GRID scored as **MISS** at 0.60 confidence.
2. **Defensive/quality rotation within a risk-off tape.** When oil spikes and the index sells off, capital frequently rotates *into* large-cap cash-generative platforms rather than out of them. XLC's two anchors are among the largest FCF generators in the market — a plausible rotation destination on an oil-shock day.
3. **NOT the morning's stated drivers.** The oil/Iran shock, the Warsh hawkish repricing, the −0.969 10Y/SPX correlation, and the NQ-lags-ES gap all pointed *down* for a duration book. They were real (SPY −0.60%, Nasdaq −0.70% confirm the macro), but they were **dominated** by sector-internal buying.

**PRIMARY_DRIVER:** Mega-cap platform strength (META/Alphabet) overwhelming a risk-off, oil-shock macro tape — the inverse of the morning's "large-cap failure inside the sector" read.

---

## 2. Audit of morning S0–S4 reads against reality

Using the **morning numbers as written**, not post-close rewrites:

| Component | Morning score | Morning rationale | Reality | Verdict |
|---|---|---|---|---|
| **S0 Shared macro** | −1 | Oil >$101, Warsh hawkish, corr −0.969, pre-CPI drift → one-sided negative for duration book | SPY −0.60%, Nasdaq −0.70% — macro drag **was real** | **Directionally HIT, but mis-weighted** — it was the *only* correct negative, and it was swamped |
| **S1 Sector factors** | 0 | Spine neutral; Meta settlement stale; no fresh ad/AI print | Sector-specific strength was the **dominant** driver | **MISS** — the model explicitly zeroed the factor that decided the day |
| **S2 Breadth** | −1 | "Large-cap failure inside the sector," NQ lags ES, META/GOOGL not leading | Large-cap **leadership**, not failure | **MISS (sign flip)** — the single worst error |
| **S3 Flows** | 0 | No fresh XLC flow print | Neutral read was defensible; no evidence of a flow catalyst | **PASS (no information)** |
| **S4 ETF tape** | 0 | 1d rel −0.15% is prior-close history; 08-28 leftover ban | Correctly refused to extrapolate | **PASS** |

**The core failure is S2.** The morning read inverted the NQ/ES signal. It argued: "NQ lags ES by ~28bp → mega-cap growth is not participating → large-cap failure inside the sector → negative." That inference treats **index-futures composition** as a proxy for **XLC's two specific names**. It is not. NQ can lag ES because of semis, software, or AI-infra (the morning itself flagged AMAT/LITE/ALAB as the premarket leaders — *outside* XLC). A weak NQ tells you nothing about whether META and GOOGL are being bought. The model **imported a broad-tech signal into a two-name book** and got the sign wrong.

**S1 = 0 was the enabling error.** By scoring sector factors neutral, the model left the entire call resting on S0+S2, both of which were macro/breadth negatives. There was no positive term available to offset them — so the deterministic sum could only be negative. The morning even *noted* "there is no fresh single-name positive at all," which it treated as removing a reason to be flat. In reality, the absence of a *fresh catalyst* is not the absence of *relative strength* — a two-name book can outperform on flow/rotation with no news at all.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check (morning):** The model claimed it counted "oil + real yields + hawkish Fed once as one rates/inflation object in S0." That is internally clean — but it means **S0 carried the entire macro weight while S2 independently carried a second, correlated negative** (NQ lagging ES is itself a risk-off/oil-shock symptom). So the −1 in S2 was **not independent** of the −1 in S0; both were expressions of the same oil-shock regime. The effective negative was closer to **−1.5, not −2**, and the model's "no divergence, factors and tape agree" conclusion was therefore **over-confident** — it read agreement between two correlated negatives as confirmation.

**Knowable-at-open test:** Was the actual outcome knowable at the open?
- The **macro** (oil, hawkish Fed, risk-off) was fully knowable — and it was correct.
- The **sector-internal strength** was **not** knowable from the packet. There was no fresh META/GOOGL catalyst, no flow print, no premarket leadership inside XLC. The morning explicitly checked and found nothing.
- **However**, the *inference* that "NQ weak ⇒ XLC weak" was a **knowable-at-open error**, not an unknowable surprise. The model had the information to recognize that NQ composition ≠ XLC composition, and it chose the wrong mapping. That is a reasoning failure, not bad luck.

**Verdict: PARTIALLY knowable.** The direction was not knowable from catalysts, but the *confidence* placed on the negative breadth read was unjustified given the proxy mismatch.

---

## 4. Outliers inside the sector

- **The ETF itself is the outlier.** A +1.20% relative move on a −0.60% SPY day, with the sector's own 1d/3d/1w relative tape all *negative* going in, is a sharp regime break. The morning's "no divergence" conclusion (factors −2, tape mildly negative, "agree in sign") was exactly wrong: the tape had been *mildly* negative for days, and today it violently reversed.
- **The two anchors (META, Alphabet)** are the presumed internal outliers — a +1.20% ETF move on a −0.60% tape is arithmetically near-impossible without both being strongly green. The morning's HIT_GRID "Large-cap leadership inside sector | MISS | 0.60" is the single most costly grid entry.
- **Telecom/ad-tech tail names (AMX, APP, TTWO)** were correctly excluded from the ETF call — the morning's discipline there was right, and it did not matter either way.
- **The macro complex (oil, semis/AI-infra)** behaved as the morning expected — and was **irrelevant** to XLC's outcome. That is the cleanest evidence that the model was solving the wrong problem: it built a correct macro forecast and applied it to a book whose day was decided by two stocks.

---

## 5. Lessons for the next Communication Services run

1. **Never map NQ/ES divergence onto XLC.** XLC is a two-name book; NQ is a semis/software/AI-infra index. The morning's 09-08 lesson ("NQ/ES divergence is a *direct* signal about XLC's core") is **falsified** by today's tape and should be retired or heavily discounted.
2. **A two-name book can outperform with zero news.** Absence of a fresh catalyst is not evidence of weakness. S1 should not be scored 0 by default when the book is concentrated in two mega-cap FCF machines that are natural risk-off rotation destinations.
3. **Do not let two correlated negatives masquerade as confirmation.** S0 (oil/hawkish) and S2 (NQ lag) were the same regime counted twice. When the only non-zero terms are correlated, the "no divergence" conclusion is not confirmation — it is double-counting.
4. **The "asymmetric-downside into CPI" tilt (09-04) is a macro tilt, not a sector tilt.** It correctly predicted SPY −0.60% and Nasdaq −0.70%. It said nothing about XLC's relative return, and the model let it drive the sector call.
5. **Magnitude discipline cut the wrong way.** The rolling mag=0.1 cap forced "mild" on a day that delivered a **notable** +1.20% relative move. The cap should constrain *conviction*, not *direction* — and here it masked a large miss.

---

OUTCOME_BEGIN
SECTOR: Communication Services
ETF: XLC
ETF_PCT: 0.60
SPY_PCT: -0.60
REL_PCT: 1.20
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Mega-cap platform strength (META/Alphabet) overwhelming a risk-off, oil-shock macro tape — the inverse of the morning's "large-cap failure inside the sector" read
KEY_INTERACTION: S0 (oil/hawkish macro) and S2 (NQ-lags-ES breadth) were two expressions of the same oil-shock regime, counted as independent confirmation; the correlated double-negative produced false confidence and no positive term was available to offset it
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction MISS (predicted down/mild vs actual up/notable) — S0 macro call was correct but swamped; S2 breadth sign-flipped by wrongly mapping NQ composition onto a two-name book; S1=0 left no offsetting positive
OUTCOME_END