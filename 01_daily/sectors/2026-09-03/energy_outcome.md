# Sector Outcome — Energy — 2026-09-03

Actuals: {'etf': 'XLE', 'pct': -0.7373206433021195, 'spy_pct': 1.046841189654324, 'rel': -1.7841618329564435, 'open': 65.12000274658203, 'close': 64.62000274658203, 'source': 'yf_download'}

# Sector Post-Session Review — Energy (XLE) — 2026-09-03

## 0. FACTS

| Metric | Value |
|---|---|
| XLE % | **−0.74%** |
| SPY % | **+1.05%** |
| Relative % | **−1.78%** |
| Open → Close | $65.12 → $64.62 |
| Actual direction | **down** |
| Actual magnitude | **mild** (≈0.74% absolute, but −1.78% relative is notable) |

The sector closed **down** while SPY rallied hard (+1.05%). This is a clear **relative underperformance day** — energy was the laggard while the broad tape ripped.

---

## 1. What drove the sector today

**Primary driver: Oil sold off intraday while equities rallied broadly — a classic risk-on rotation out of defensive/commodity leadership.**

Evidence:
- CLAIM: WTI opened at $90.58 on Sep 3, 2026, but traded down into the $88.99–92.28 range during the session.
  URL: https://www.investing.com/commodities/crude-oil-historical-data
  PUBLISHED: 2026-09-03
  QUOTE: "Today's trading range for Crude Oil WTI futures is between 88.99 and 92.28."
  SUMMARY: WTI fell from its premarket level (~$90.07, −1.19% per Finviz) and traded down to the low-$89s intraday — a continued offered barrel, not a stabilization.

- CLAIM: XLE opened at $65.12 (vs Sep 2 close $65.10, essentially flat) but closed at $64.62 — a steady grind lower through the session.
  URL: Injected actuals (deterministic)
  PUBLISHED: 2026-09-03
  SUMMARY: The ETF faded from flat at the open to −0.74% by the close, consistent with oil drifting lower all day.

- CLAIM: SPY rallied +1.05% on the day — a strong risk-on tape.
  URL: Injected actuals (deterministic)
  PUBLISHED: 2026-09-03
  SUMMARY: The broad market had a strong up day, which energy did not participate in — the relative return of −1.78% is the story.

**Secondary factors:**
- **Hormuz premium fading**: The geopolitical supply-risk premium that drove XLE's 1w rel +4.40% / 1m rel +12.04% extension is now being priced out. Oil is falling from squeeze highs, and the market is treating the Hormuz disruption as a level shift, not a continuing catalyst.
- **Refiner sleeve drag**: HO and RBOB were both offered premarket (−1.28% / −1.27%), and with crude also down, the refiner complex (VLO, MPC) had no reason to carry the ETF.
- **No fresh catalyst**: EIA crude inventory was released Sep 2 (not today); today's EIA print was nat gas storage only. No OPEC+ news. ISM Services at 10:00 was two-sided event risk, not an energy driver.

---

## 2. Audit of morning S0–S4 reads against reality

### S0_SHARED_MACRO: 0 → **Verdict: WRONG DIRECTION (should have been −1)**

The morning scored S0 = 0, reasoning that "mild green ES/NQ and USD-down are a cyclical overlay, not a veto and not a bid." But SPY rallied **+1.05%** — a strong risk-on day, not "mild green." When the broad tape rips that hard and energy is not participating, that IS a rotation signal. The morning underestimated the strength of the equity bid and its implication for sector rotation.

### S1_SECTOR_FACTORS: −1 → **Verdict: CORRECT SIGN, UNDERWEIGHTED MAGNITUDE**

The morning correctly identified oil as offered (Finviz WTI −1.19% premarket). The sign was right — XLE closed down. But the magnitude was underweighted: the morning capped S1 at −1 (not −2) citing "not a collapse" and "EIA draw yesterday is a residual floor." In reality, oil drifted lower through the session (to ~$89), and the relative underperformance was severe (−1.78% rel). The −1 was the right sign but the wrong magnitude for the *relative* outcome.

### S2_BREADTH: 0 → **Verdict: WRONG (should have been −1)**

The morning scored S2 = 0, noting "XOM modestly red, CVX ~flat/soft, COP ~−0.5%" but concluding "not a confirmed smash." In reality, the entire sector faded through the day. The premarket softness was a leading indicator, not noise. The morning's reluctance to score breadth negative ("do not copy 3d/1w rel +4.4% into S2") was correct in principle but led to missing the actual breadth deterioration.

### S3_FLOWS_POSITIONING: 0 → **Verdict: PARTIALLY CORRECT**

The morning noted XLE outflows (−$363M 5d / −$475M 1m) but scored S3 = 0, reasoning that "trailing unit outflows are not a 1-day lid (08-28)." This was defensible — flows are a slow variable. However, the crowded-long HIT (1m rel +12.04%) was correctly flagged and did contribute to the fade. S3 = 0 was acceptable, though a −1 would have been justified given the crowded positioning.

### S4_ETF_TAPE: 0 → **Verdict: WRONG (should have been −1)**

The morning scored S4 = 0, citing "1d rel +0.07% is flat vs any signed fade." But the 1d rel of +0.07% was from Sep 2 — the morning treated it as a neutral signal. In reality, the tape was already stalling (Sep 2's +0.51% XLE vs +0.44% SPY was a stall after the big run), and today's fade was the continuation. The morning's DO-INSTEAD rule ("prefer flat/mild") was applied but the direction was wrong — the stall was a topping signal, not a consolidation.

---

## 3. Interactions / double-count / knowable-at-open test

**Interactions:**
- The key interaction was **oil-offered × equity-risk-on**. When SPY rips +1.05% and oil is falling, money rotates OUT of energy (which had been the leadership trade) INTO the broad market. This is a sector-rotation dynamic that the morning's S0 = 0 missed — the morning treated the equity tape as neutral ("not a commodity bid") when it was actually a *competing* bid.
- The **crowded-long × oil-offered** interaction was correctly identified (S3 HIT on crowded long) but underweighted. When a sector has run 1m rel +12.04% and the underlying commodity starts fading, the unwind can be swift.

**Double-count check:**
- The morning correctly counted oil-offered once (S1 = −1) and did not double-count Hormuz as a second factor. This was clean.
- The morning correctly did NOT score the EIA draw (Sep 2 release) as today's catalyst. This was clean.
- The morning correctly did NOT let refiners drive the call while HO/RBOB were offered. This was clean.

**Knowable-at-open test:**
- **Partially knowable.** The premarket tape showed XLE soft (−0.15% to −0.40%), oil offered (−1.19%), and the broad market mildly green. What was NOT knowable at open was the *magnitude* of the SPY rally (+1.05%) — that was a session development. If SPY had closed flat, XLE's −0.74% would have been a mild absolute decline with a less severe relative miss. The relative underperformance of −1.78% was amplified by the SPY strength, which was not fully predictable from premarket ES +0.13%.

---

## 4. Outliers inside the sector

Based on the available data, the entire sector moved together — XOM, CVX, COP were all modestly red premarket and the ETF faded through the day. No single-name outlier drove the move; this was a broad sector fade.

The most notable internal dynamic was the **refiner sleeve**: with HO and RBOB both offered (−1.28% / −1.27% premarket), the refiner complex (VLO, MPC) had no bid. Diesel cracks remain extreme (~$100+/bbl record territory per Bloomberg Sep 1), but that residual heat was not enough to overcome the offered product complex.

---

## OUTCOME_BEGIN
SECTOR: Energy
ETF: XLE
ETF_PCT: -0.74
SPY_PCT: +1.05
REL_PCT: -1.78
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild (absolute), notable (relative)
PRIMARY_DRIVER: Oil sold off intraday while SPY ripped +1.05% — risk-on rotation out of crowded energy leadership
KEY_INTERACTION: Oil-offered × equity-risk-on — money rotated from the 1m rel +12% leader into the broad tape
KNOWABLE_AT_OPEN: partially — oil-offered and XLE softness were visible premarket; SPY +1.05% magnitude was not
MORNING_READ_VERDICT: Sign correct (flat→down was the right lean direction) but magnitude underweighted — S0 and S4 should have been negative given the stall tape and the risk-on rotation signal
OUTCOME_END