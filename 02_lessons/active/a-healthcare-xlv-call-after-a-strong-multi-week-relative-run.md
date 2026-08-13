---
trigger_pattern: "A Healthcare/XLV call after a strong multi-week relative run with many carried positive factors, but the pre-fetched 1d tape is negative absolute and a fresh, knowable-at-open negative catalyst hits a high-weight XLV sub-industry — e.g., a Medicare Advantage rate proposal shock to managed care (UNH, HUM, CVS). The analysis may cite the negative catalyst but treat it as a “caution” or “partial offset” while keeping S1/S4 positive and emitting up/notable."
corrected_behavior: "When a fresh negative policy/regulatory catalyst directly hits a high-weight sub-industry and the 1d XLV tape is already negative, reweight S1 to neutral/negative, set S4 to ≤0 for absolute direction, and do not use 3d/1w/1m relative strength to justify an absolute up call. The sector may still outperform SPY relatively, so the call should be flat/down unless a larger same-day positive catalyst is explicitly present."
falsifier: "Fails if a future XLV setup has a negative 1d tape and negative policy shock but a larger same-day positive catalyst — e.g., a mega-cap healthcare earnings blowout — drives XLV up. The lesson must require the fresh negative catalyst to be the dominant same-day signal."
current_behavior: "The model carries forward positive sector factors (rotation, earnings, biotech risk-on), scores S1 +2.0 and S4 +0.5, labels the negative 1d tape “natural consolidation,” and emits up/notable. It underweights the known negative catalyst even when it is large enough to hit the ETF’s biggest sub-sector."
evidence_cited: "On 2026-08-11, XLV closed -0.26% while SPY closed -0.32%; XLV outperformed relatively (+0.06%) but the absolute move was down. The morning had the Morningstar report on the near-flat 2027 Medicare Advantage proposal and still scored S1 +2.0, S4 +0.5, multiplier 1.1, total 11.0, up/notable. UNH/HUM/CVS fell on the policy shock; biotech/pharma strength only limited the decline."
error_category: "B"
scope: "general"
date: "2026-08-11"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-11_sector_healthcare_lesson.md']"
schema_ok: "true"
---

## RULE
When a fresh negative policy/regulatory catalyst directly hits a high-weight sub-industry and the 1d XLV tape is already negative, reweight S1 to neutral/negative, set S4 to ≤0 for absolute direction, and do not use 3d/1w/1m relative strength to justify an absolute up call. The sector may still outperform SPY relatively, so the call should be flat/down unless a larger same-day positive catalyst is explicitly present.

## WHEN IT FIRES
A Healthcare/XLV call after a strong multi-week relative run with many carried positive factors, but the pre-fetched 1d tape is negative absolute and a fresh, knowable-at-open negative catalyst hits a high-weight XLV sub-industry — e.g., a Medicare Advantage rate proposal shock to managed care (UNH, HUM, CVS). The analysis may cite the negative catalyst but treat it as a “caution” or “partial offset” while keeping S1/S4 positive and emitting up/notable.

## WRONG IF
Fails if a future XLV setup has a negative 1d tape and negative policy shock but a larger same-day positive catalyst — e.g., a mega-cap healthcare earnings blowout — drives XLV up. The lesson must require the fresh negative catalyst to be the dominant same-day signal.

## EVIDENCE
On 2026-08-11, XLV closed -0.26% while SPY closed -0.32%; XLV outperformed relatively (+0.06%) but the absolute move was down. The morning had the Morningstar report on the near-flat 2027 Medicare Advantage proposal and still scored S1 +2.0, S4 +0.5, multiplier 1.1, total 11.0, up/notable. UNH/HUM/CVS fell on the policy shock; biotech/pharma strength only limited the decline.

(learn_cycle promote)
