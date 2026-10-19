# Evidence-v2 validation checkpoint — 2026-10-19

The 90% production relevance target is **not established**. Evidence-v2 is
opt-in, and the production CLI keeps the legacy default. The implementation
and evidence routes can be reviewed separately from release approval.

## Available inputs

The recovered profile build contains 2,032 screened companies and consumes
775,477,612 characters from dated sections published on or before 2026-10-01.
It is an incomplete historical section cache, not the complete PR642 archive.
The full-text reader separately processed approximately 25 million characters
from available archive probes and correctly reported their incompleteness.
Finviz's complete description/name file was not available locally; missing
off-board identity expectations therefore cannot be compared as equal-input
regressions. Hosted validation exports that file and an untouched news sample.

## What the checks show

- The real RSS development set's original v1 labels score 20/20 exact sets,
  2/2 positive links, no extra links, and no positive abstentions in the final
  local full-universe run. There are only two positives. The 95% precision
  interval is approximately 34–100%, and the accuracy interval is 84–100%.
  This cannot establish the release bar. Labels are self-graded and reused.
- The separate SEC issuer identity probe finds all 40 expected issuers.
  It uses generated filing titles and measures issuer recall, not all-links
  accuracy. Liberty Global share classes also duplicate an issuer event.
- The original 25-case frozen corpus remains available unchanged. The available
  cache run passed 11 regression assertions, failed 8, achieved all 3 target
  assertions, and left 3 unlabeled. This is not a satisfactory regression result.
  Differences include missing off-board names, an unsupported clinical event
  verb, missing person aliases, broader documented relationships, and changed
  industry-context semantics. Every discrepancy remains in the result file.
- A v2 alternative label explicitly records a possible Artivion competitive
  exposure in the pediatric-valve story. The model currently misses that
  proposed link. Both label versions are retained; the higher score must not
  be used to conceal this unresolved relevance-boundary question.

## Failures found and corrected during this development round

The first full-universe suture-recall probe falsely passed 25 companies.
Uppercase risk statements had become product identities, and generic device
classes had become competitive exposure. The same cache initially scored that
headline in about 154 seconds. Indexed lookup reduced it to about 2.6 seconds
on a warm development run and removed the false links.

Further failures included an alleged NCAA product owned by a bookmaker,
ordinary “Next” matching the NeXT brand, employer/role labels becoming people,
an agency abbreviation becoming a company alias, and one expanded synonym
counting as several independent anchors. Regressions cover these mechanisms.
Published results distinguish warm scan time from cold profile-loading time.

## Remaining release work

Run the same complete stock universe and current production sources in both
engines. Freeze complete labels on fresh, deduplicated articles with meaningful
positive cases across direct identity, owned products, competitors, suppliers,
geographic restrictions and unnamed policy exposure. Report precision, recall,
exact-set accuracy, abstention and category-level results, then repeat on a fresh
holdout. Do not merge or enable the candidate based on unit tests or these small
development probes alone.

Known partial areas include grammatical/event eligibility, global geographic
coverage, historical ownership, and extraction of all product/person variants.
“Implemented” in the coverage matrix means a guard exists, not exhaustive
coverage or measured accuracy for that entire category.
