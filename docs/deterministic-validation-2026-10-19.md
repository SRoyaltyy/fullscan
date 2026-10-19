# Evidence-v2 validation checkpoint - 2026-10-19

The 90% production target is **not established**. Evidence-v2 stays opt-in.
Production and evaluator defaults remain legacy.

## Equal-input checks

The full exported Finviz input has 5,962 rows and 5,961 distinct tickers.
On the unchanged 25-case repository corpus, legacy passes 12/19 regressions
and 2/3 targets. The final local candidate passes 19/19 regressions and 3/3
targets; three cases remain unlabeled. These partial assertions are excluded
from strict accuracy denominators. The final local suite passes 117 tests,
including repeatability across independent processes.

Hosted commit dfb7fdb passed 100 tests and 19/19 regressions, with 2/3 targets
before the business-paragraph repair. CI reruns the candidate and both
equal-input evaluations in isolated Python with site packages, DNS and socket
connections disabled. Dictionary bootstrap is a separate one-time build.

## Scanner headline development checkpoint

The seed-19019 export selected 200 primary headlines from 10,825 normalized
titles. Assistant annotations were frozen before inspecting matcher outputs:
160 provisional complete sets, including 31 positive headlines and 37 expected
links. Forty uncertain ownership, off-universe and broad policy/commodity cases
are explicitly excluded. Labels remain human_checked:false.

| Checkpoint | Exact sets | Precision | Recall | False links | Missed links |
|---|---:|---:|---:|---:|---:|
| First candidate on frozen labels | 119/160 (74.4%) | 2.5% | 27.0% | 393 | 27 |
| Adapted final candidate, unchanged labels | 160/160 (100%) | 100% | 100% | 0 | 0 |

The final 95% Wilson intervals are 97.7-100% for exact sets and 90.6-100% for
precision and recall. There are 129 negatives; one thin negative abstention
remains in the denominator. This subset meets the numerical sample bar but
has been used for development. It does not validate the forty excluded cases
or the production distribution. No label was changed to obtain the higher
score. Benchmark article files are not included in this PR; the published
changes contain code and aggregate findings.

Frozen-label SHA256:
af7984c6f34bf95c1d564cd6dc88d9fde1481ab7f92f857a155cf3b62de025a2

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

CI exports a separate seed-19020 sample excluding all 200 development headlines.
It contains no labels or program outputs. Related stories may still overlap and
must be grouped before an independent holdout claim. Freeze labels and score it
without tuning on the result. Adjudicate the forty uncertain cases, especially
unnamed policy exposure, and evaluate complete dated production stock sources.
Report exact sets, precision, recall, abstention and per-category coverage.
Historical ownership, geography and general grammar remain partial. Do not merge
or enable the candidate based on the adapted 160-case score.
