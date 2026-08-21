# Sector Outcome — Industrials — 2026-08-21

Actuals: {'etf': 'XLI', 'pct': 0.2670054603834737, 'spy_pct': 0.40912604472485015, 'rel': -0.14212058434137642, 'open': 180.9499969482422, 'close': 180.25}

I have enough to compile the review. Let me note the key facts:

**Actuals:**
- XLI +0.267%, SPY +0.409%, rel -0.142%
- Open 180.95, Close 180.25 (gapped up, faded intraday)
- Prior close ≈ 179.77

**Morning prediction:** down/mild (score -3.15)
**Actual:** up/mild (XLI +0.27%, but underperformed SPY)

Direction MISS (predicted down, actual up). Magnitude MISS (predicted mild down, actual mild up — opposite sign).

**What drove the sector:**
- The market bounced after the prior day's 700-point Dow rout (recovery attempt, as the morning noted futures were up).
- CNH Industrial surged +8.3% on guiding 2026 outlook to high end — a notable single-name industrial mover.
- GE Aerospace up ~1.2-2.15% (JPMorgan raised PT to $400).
- But XLI faded from its open (180.95 → 180.25), closing up only +0.27% while SPY rose +0.41% — industrials underperformed, consistent with the hawkish-Fed/rising-yields macro drag that the morning flagged.
- The utilities and industrials sectors declined on a weekly basis (CNBC).

**Audit of morning S0-S4:**
- S0_SHARED_MACRO (-2): Correct that macro was risk-off/hawkish. But the market bounced today (recovery), so the macro overlay didn't push XLI down on the day. The macro drag did keep XLI from outperforming (rel -0.14%). Partially validated — the direction of the macro call was right (it capped upside) but the magnitude of the negative impact was overstated for a single day.
- S1_SECTOR_FACTORS (+1): Correct to keep at +1. CNH guidance to high end and GE Aerospace strength were positives that helped XLI close up.
- S2_BREADTH (-1): Validated — XLI underperformed SPY (rel -0.14%), confirming the sector breadth failure.
- S3_FLOWS_POSITIONING (0): Neutral, fine.
- S4_ETF_TAPE (-1): The tape was negative on medium/long timeframes, but the 1d bounce was real. The morning noted 1d rel +0.08% (flat) — the actual 1d rel was -0.14%, slightly negative. The tape call was directionally right on relative terms.

**Knowable at open:** Partially. The futures bounce (ES +0.35%) was knowable and argued against a down day. The morning acknowledged it was a recovery attempt but chose to weight the macro overlay and negative tape more heavily. The CNH guidance pop and GE Aerospace strength were single-name positives that were knowable but not weighted enough.

**Key interaction:** The morning correctly identified the macro drag (hawkish Fed, rising yields) which kept XLI underperforming SPY, but the market-wide recovery bounce (after the Dow rout) lifted all sectors including XLI, so the "down" call missed the absolute direction even though the relative underperformance was correct.

Let me write the review.

OUTCOME_BEGIN
SECTOR: Industrials
ETF: XLI
ETF_PCT: 0.267
SPY_PCT: 0.409
REL_PCT: -0.142
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Market-wide recovery bounce after the prior day's 700-point Dow rout, lifting all sectors; XLI gapped up (open 180.95) but faded to close +0.27%, underperforming SPY (+0.41%) on the hawkish-Fed/rising-yields macro drag.
KEY_INTERACTION: The macro overlay (hawkish Fed minutes, rising yields) was correctly negative and capped XLI's relative performance (rel -0.14%), but the broad risk-on recovery bounce overwhelmed it for the absolute day, so the down call missed direction while the relative underperformance was right.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction MISS (predicted down, actual up) and magnitude MISS (predicted mild down, actual mild up); the relative underperformance call was validated, but the morning over-weighted the macro drag vs. the knowable futures bounce and single-name positives (CNH +8.3% guidance, GE Aerospace PT raise).
OUTCOME_END

---

## Sector Post-Session Review — Industrials (XLI) — 2026-08-21

### 0. FACTS
- **XLI:** +0.267% (close 180.25 vs prior close ≈179.77)
- **SPY:** +0.409%
- **REL:** **-0.142%** (XLI underperformed SPY)
- **Path:** Gapped up to open 180.95, faded intraday to close 180.25 — a gap-up-and-fade, closing up on the day but giving back most of the early gain.

### 1. What drove the sector today
- **Market-wide recovery bounce** after the prior session's ~700-point Dow rout. Futures were up premarket (ES +0.35%, NQ +0.49%), and the S&P 500 closed +0.49% — a genuine risk-on recovery day. This lifted all sectors, including XLI.
- **Single-name positives within industrials:** CNH Industrial surged ~+8.3% after guiding its 2026 outlook to the high end (CLAIM: CNH guides 2026 outlook to high end, shares pop / PUBLISHED: 2026-08-21 / SUMMARY: agriculture segment outlook raised, stock +8.3%). GE Aerospace traded up ~1.2–2.15% on a JPMorgan price-target raise to $400 (CLAIM: JPM raises GE Aerospace PT to $400, overweight / PUBLISHED: 2026-08-21).
- **Macro drag capped relative performance:** The hawkish-Fed-minutes / rising-yields backdrop (DGS30 5.19%, DGS10 4.65%) kept XLI from keeping pace with SPY — rel -0.14%. CNBC noted utilities and industrials declined on a weekly basis even as the index bounced Friday.

### 2. Audit of morning S0–S4 reads
- **S0_SHARED_MACRO (-2):** Directionally correct — the macro overlay was genuinely risk-off/hawkish, and it did cap XLI's relative performance. But the magnitude was overstated for a single day: the market-wide recovery bounce overwhelmed the macro drag for the absolute move. **Partially validated.**
- **S1_SECTOR_FACTORS (+1):** Correct to hold at +1. The CNH guidance pop and GE Aerospace strength were real positives that helped XLI close green. **Validated.**
- **S2_BREADTH (-1):** Validated — XLI underperformed SPY (rel -0.14%), confirming the sector breadth failure / rotation-out signal. **Validated.**
- **S3_FLOWS_POSITIONING (0):** Neutral, no fresh signal. **Validated.**
- **S4_ETF_TAPE (-1):** The medium/long-term tape was decisively negative (1m rel -4.43%), and the 1d relative was slightly negative (-0.14%), so the tape call was directionally right on relative terms. But the morning's own note that 1d was "flat/bounce" was the more relevant signal for the absolute day. **Partially validated.**

### 3. Interactions / double-count / knowable-at-open test
- **No double-count:** Hawkish Fed and Trump economic-warfare were correctly treated as one correlated risk-off overlay in S0.
- **Knowable at open — partially:** The futures bounce (ES +0.35%) was knowable and argued against a down day; the morning acknowledged it but chose to weight the macro overlay and negative tape more heavily. The CNH guidance and GE Aerospace PT raise were also knowable single-name positives. The relative underperformance (rel -0.14%) was the correct, knowable call; the absolute down call was not.

### 4. Outliers inside the sector
- **CNH Industrial (+8.3%):** Guided 2026 outlook to high end — a notable single-name positive that helped XLI close green.
- **GE Aerospace (+1.2–2.15%):** JPMorgan PT raise to $400 — defense/aerospace strength.
- These positives were partially offset by the broader industrials basket fading from the open, consistent with the macro drag.

### Verdict
The morning's **relative** read was correct — XLI underperformed SPY, confirming the sector breadth failure and macro drag. But the **absolute** call (down/mild) missed: the market-wide recovery bounce after the Dow rout lifted XLI to a +0.27% close. The lesson: on a day with a strong, knowable futures bounce following a sharp sell-off, the recovery impulse can dominate the macro overlay for the absolute move even when the macro drag correctly caps relative performance. The morning over-weighted the macro overlay and under-weighted the knowable bounce and single-name positives.