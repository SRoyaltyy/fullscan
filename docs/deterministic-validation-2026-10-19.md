# Evidence-v2 validation checkpoint - 2026-10-19

The 90% production target is **not established**. Evidence-v2 stays opt-in.
Production and evaluator defaults remain legacy.

## Equal-input checks

The full exported Finviz input has 5,962 rows and 5,961 distinct tickers.
On the unchanged 25-case repository corpus, legacy passes 12/19 regressions
and 2/3 targets. The final local candidate passes 19/19 regressions and 3/3
targets; three cases remain unlabeled. These partial assertions are excluded
from strict accuracy denominators. The final local suite passes 120 tests,
including repeatability across independent processes.

Hosted commit 2a2f975 passed 117 tests, 19/19 regressions and all 3 targets.
CI reruns the candidate and both
equal-input evaluations in isolated Python with site packages, DNS and socket
connections disabled. Dictionary bootstrap is a separate one-time build.

## Scanner headline development checkpoint

The seed-19019 export selected 200 primary headlines from 10,825 normalized
titles. Initial assistant labels were frozen before output inspection. A later
audit found an outdated assumption: SpaceX is included as SPCX in the supplied
universe, so three Starlink headlines had been incorrectly labeled negative.
Starlink's own [legal notice](https://www.starlink.com/legal/documents/DOC-1158-18408-72)
confirms its SpaceX ownership. Version 2 corrects those positives and marks two
entertainment-ownership cases and an unnamed satellite-network case unresolved.
Original labels/results are retained. The earlier 160/160 version-1 score is
an obsolete annotation checkpoint, not an accuracy estimate.

Version 2 contains 157 provisional complete sets: 34 positive headlines and
40 expected links. Forty-three uncertain cases are explicitly excluded.
Labels remain human_checked:false, and all negative/ownership judgments still
need independent review.

| Checkpoint | Exact sets | Precision | Recall | False links | Missed links |
|---|---:|---:|---:|---:|---:|
| Legacy, audited labels | 123/157 (78.3%) | undefined (no passes) | 0% | 0 | 40 |
| Adapted candidate, audited labels | 154/157 (98.1%) | 100% | 92.5% | 0 | 3 |

The final 95% Wilson intervals are 94.5-99.4% for exact sets, 90.6-100% for
precision and 80.1-97.4% for recall. There are 123 negatives. The subset meets
the numerical point-estimate bar but was used for development. It does not
validate excluded cases or the production distribution. The label audit reduced
the score and exposed three real source/alias misses. Benchmark article files
are not included in this PR; published changes contain code and aggregate findings.

Original version-1 label SHA256 (superseded):
af7984c6f34bf95c1d564cd6dc88d9fde1481ab7f92f857a155cf3b62de025a2

Audited version-2 label SHA256:
32c913a0558e2cf8868f8aecc5a91f2b5a1c66ffbb9ea3558fa0151648f3587b

Original 200-input export SHA256:
002dec67617acffbe555704a59fbc3a3ff398f7bdf2335813e5e2ebad5e04a52

## Failures found and repaired

- Ordinary event forms and shortened company names were missed. Raw company
  prefixes, ADR suffixes, financial lists, punctuation and corporate predicates
  now have explicit guards.
- Isolated unnamed incidents passed whole product industries. The unnamed
  business route now requires a broad constraint/activity and two rare sourced
  anchors. Unspecified approvals cannot become generic business passes.
- Industry vocabulary vetoed niche business evidence. Rare anchors must instead
  occur together in a valid stock witness, including a contiguous short business
  paragraph. Sections, dates and typed counterparty statements cannot be joined.
- Compound suffixes, geographic prefixes, foreign-bank qualifiers and blank-check
  shorthand were mistaken for operating-company identities.
- Disease indications and generic financial acronyms became owned products.
  Raw brand shape and ownership are now required; another issuer's name/ticker
  cannot silently become a product. Case and inventory-based plurals distinguish
  brands such as Mac and MAC.

## Input and archive limitations

The earlier length-based claim of 100 genuine bodies was incorrect. Many
inputs concatenate related headlines. The exporter now reports **unverified
extended text**, exports primary headlines separately, and discloses that related
stories may remain. The scan CLI supports explicit --headline-only scoring.

The recovered cache contains 2,032 screened stocks and 775,477,612 source
characters through 2026-10-01. It is incomplete historical section data, not the
whole PR642 archive. Full-reader probes processed about 25 million characters
and reported incompleteness. Rebuild extracted profiles after extractor changes.
Neither the Finviz-only check nor this old cache certifies full-archive behavior.

Earlier reused RSS checks (20 headlines, two positives) and a generated-title
40-issuer recall probe remain development evidence. The alternative Artivion
competitive-exposure label remains unresolved.

## Remaining release work

The first untouched-headline run at engine commit 2a2f975 **fails the target**.
Of 200 titles, 158 have provisional complete labels (34 positives, 40 links)
and 42 are unresolved. Exact sets are 128/158 (81.0%); there are 17 TP,
43 FP and 23 FN, giving 28.3% precision and 42.5% recall. Five outputs abstain.
Known related-story cases are flagged separately: the provisional novel-story
subset is 113/136 exact, with 16 TP, 42 FP and 15 FN. It also fails.
This sample has not been used to tune the engine at this checkpoint. Its label
SHA256 is 24558bccbb7dea6b46fcef9297d367cde72376c2f0fb67317db48fe65887b0b1.
These results invalidate any suggestion that the adapted development score
generalizes to new headlines. Alias attribution, noun/verb event scope and
missing stock-side corporate/product identities remain serious gaps.

CI exports a separate seed-19020 sample excluding all 200 development headlines.
It contains no labels or program outputs. Related stories may still overlap and
must be grouped before an independent holdout claim. Freeze labels and score it
without tuning on the result. Adjudicate the uncertain cases, especially
unnamed policy exposure, and evaluate complete dated production stock sources.
Report exact sets, precision, recall, abstention and per-category coverage.
Historical ownership, geography and general grammar remain partial. Do not merge
or enable the candidate based on the adapted 160-case score.
