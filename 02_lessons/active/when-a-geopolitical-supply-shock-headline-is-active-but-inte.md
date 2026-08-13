---
trigger_pattern: "When a geopolitical supply-shock headline is active but internally conflicting — one source says a deal is agreed / strikes called off, another says it is stalled / demands unresolved — and the pre-fetched Channel 1 oil-futures tape shows a move consistent with the premature/deal-resolved headline, do not treat that pre-fetched tape as authoritative for an Energy call. The oil-price sign is the load-bearing factor for S1; if it is stale or wrong, it cascades into S1, the divergence check, the multiplier, and the final direction call."
corrected_behavior: "For Energy, before scoring S1 and final direction, verify the current oil-price sign against at least one independent live source. If the pre-fetched Channel 1 oil tape conflicts with active headlines or with independent quotes, do not treat it as authoritative; re-score using the live-verified sign, or downgrade confidence/neutral if unresolved. If live oil is up and the geopolitical premium is re-expanding, set S1 positive even if the pre-fetched tape says otherwise. Also resolve prose-vs-pipeline divergence flags before locking the call: a “divergence” built on a stale factor sign is not a real divergence."
falsifier: "The corrected behavior is falsified if, on a future Energy day, live independent sources clearly show oil DOWN and Hormuz deal progress real, yet XLE still rallies >1% with strong relative outperformance driven by a non-oil factor (e.g., refiners/crack spreads alone), because then live oil-sign verification alone would not be sufficient to avoid the miss. It is also weakened if live-verified oil-up signs still produce flat/down outcomes at high frequency intraday."
current_behavior: "The model trusts the pre-fetched CL=F/BZ=F tape as ground truth, even when active headlines conflict. It then builds the entire Energy thesis on that one sign — here, CL=F -0.66% / BZ=F -0.65% and “Hormuz deal progressing” — marks the geopolitical premium as fading, scores S1 negative, flags a divergence against the positive ETF tape, and emits flat/flat. It does not re-verify the live oil tape at/near the open against Reuters, Bloomberg, NYT, or futures pages."
evidence_cited: "2026-08-11 Energy: morning predicted flat/flat from pre-fetched oil tape showing oil down and assuming Hormuz deal progress. Actual: oil touched a one-week high, Brent reached ~$90/bbl intraday, US-Iran deal hopes faded, and the geopolitical premium re-expanded. XLE closed +1.25% while SPY fell -0.32%, relative +1.57%. Direction and magnitude both missed. The morning’s Channel 1 oil tape was contradicted by Reuters, NYT, and EIA reports published before/around the open."
error_category: "A"
scope: "general"
date: "2026-08-11"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-11_sector_energy_lesson.md']"
schema_ok: "true"
---

## RULE
For Energy, before scoring S1 and final direction, verify the current oil-price sign against at least one independent live source. If the pre-fetched Channel 1 oil tape conflicts with active headlines or with independent quotes, do not treat it as authoritative; re-score using the live-verified sign, or downgrade confidence/neutral if unresolved. If live oil is up and the geopolitical premium is re-expanding, set S1 positive even if the pre-fetched tape says otherwise. Also resolve prose-vs-pipeline divergence flags before locking the call: a “divergence” built on a stale factor sign is not a real divergence.

## WHEN IT FIRES
When a geopolitical supply-shock headline is active but internally conflicting — one source says a deal is agreed / strikes called off, another says it is stalled / demands unresolved — and the pre-fetched Channel 1 oil-futures tape shows a move consistent with the premature/deal-resolved headline, do not treat that pre-fetched tape as authoritative for an Energy call. The oil-price sign is the load-bearing factor for S1; if it is stale or wrong, it cascades into S1, the divergence check, the multiplier, and the final direction call.

## WRONG IF
The corrected behavior is falsified if, on a future Energy day, live independent sources clearly show oil DOWN and Hormuz deal progress real, yet XLE still rallies >1% with strong relative outperformance driven by a non-oil factor (e.g., refiners/crack spreads alone), because then live oil-sign verification alone would not be sufficient to avoid the miss. It is also weakened if live-verified oil-up signs still produce flat/down outcomes at high frequency intraday.

## EVIDENCE
2026-08-11 Energy: morning predicted flat/flat from pre-fetched oil tape showing oil down and assuming Hormuz deal progress. Actual: oil touched a one-week high, Brent reached ~$90/bbl intraday, US-Iran deal hopes faded, and the geopolitical premium re-expanded. XLE closed +1.25% while SPY fell -0.32%, relative +1.57%. Direction and magnitude both missed. The morning’s Channel 1 oil tape was contradicted by Reuters, NYT, and EIA reports published before/around the open.

(learn_cycle promote)
