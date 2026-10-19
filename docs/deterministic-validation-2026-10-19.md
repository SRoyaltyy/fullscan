# Evidence-v2 validation checkpoint - 2026-10-19

The 90% production target is **not established**. Evidence-v2 stays opt-in.
Production and evaluator defaults remain legacy.

## Equal-input checks

The full exported Finviz input has 5,962 rows and 5,961 distinct tickers.
On the unchanged 25-case repository corpus, legacy passes 12/19 regressions
and 2/3 targets. The final local candidate passes 19/19 regressions and 3/3
targets; three cases remain unlabeled. These partial assertions are excluded
from strict accuracy denominators. That local checkpoint passed 120 tests,
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
This sample had not been used to tune the engine at that checkpoint. Its label
SHA256 is 24558bccbb7dea6b46fcef9297d367cde72376c2f0fb67317db48fe65887b0b1.
These results invalidate any suggestion that the adapted development score
generalizes to new headlines. Alias attribution, noun/verb event scope and
missing stock-side corporate/product identities remain serious gaps.

## Follow-up repair checkpoint

The seed-19020 sample is now development data. Its failed first-run result
above is preserved; it must never be relabeled as an unseen success after repair.
The next candidate has **135 passing local regression tests**, run with isolated
Python, site packages disabled, and DNS/socket connections denied. The new
regressions cover history/calendar leakage, launch verb scope, exchange and
quote-directory context, nested issuer names, corporate grammar, share classes,
ambiguous short names, asset codes, sourced DBA/initialisms/operating brands,
and stale compiler-cache rejection. They establish these behaviors, not broad
headline accuracy.

Full post-repair metrics are **not available**. Automatic approval review blocked
retrieval from a local full-universe evaluation session because it reported
unexpected external telemetry to a Visual Studio endpoint. The checked-in
runner denies Python networking; no post-repair output file was produced at
this checkpoint. Do not substitute regression pass counts for missing metrics.

CI reserves a seed-19021 sample excluding primary headlines from both prior
samples. Related stories may still overlap and must be grouped before an
independent holdout claim. Freeze labels before inspecting candidate outputs.
Adjudicate uncertain cases, especially unnamed policy exposure, and evaluate
complete dated production sources. Historical ownership, geography and general
grammar remain partial. Do not merge or enable the candidate before validation.

## Candidate 2.2 - 2026-10-20 HKT

Hosted run 39281473188 at commit 805f8ced passed 135 tests, all 19 existing
regressions and all three target assertions. The next candidate passes 142
local tests. It repairs the single-company/sector-policy distinction, joint
issuer subjects, exact-brand precedence, issuer nickname definitions, and
publisher-tail handling. Source compiler 2.2 includes registration statements
and explicit owned-network glossary definitions. The full historical cache
is being rebuilt; it remains incomplete historical data.

The earlier local evaluation produced a file despite blocked live-session
retrieval. Reading the completed output through the first-party file route
resolved access to the metrics. On the now-adapted seed19020 labels, candidate
2.2's Finviz-only checkpoint is **153/158 exact sets (96.8%), 35 TP, zero FP,
five FN; precision 100%, recall 87.5%**. All 124 labeled negatives are exact.
Four cases abstain. The recall interval is approximately 73.9-94.5%; this is
development evidence and still below the requested recall target. A preceding
repair checkpoint scored149/158 exact,30TP75FP10FN; its75FP all came from one
bank-specific restriction being mistaken for a sector-wide policy.

Candidate 2.2 code was frozen before inspecting outputs on seed19021. Labels
were then frozen:139 provisional complete sets,37 positive cases,41 expected
links;61 uncertain cases excluded. Known story overlaps are flagged, leaving
29 provisional novel-story positives. Label SHA256:
52aa30c933780652f898f62f352e8cb40bfbfea2b0e818126b8668c8b34689ac.
No candidate output from that sample has been inspected at this checkpoint.
Labels are assistant-authored, not independently human-validated. The 90%
production bar remains unproven.

## Candidate2.2 unseen result and regression tradeoffs

Hosted run39286561182 at e8352267 completed142 tests and all existing corpus
assertions. Frozen candidate source was unchanged. On seed19021, candidate2.2
scored **112/139 exact sets (80.6%),16TP5FP25FN; precision76.2%,recall39.0%**.
Only15/37 positive cases were exact. The provisional novel-story subset was
105/128 exact,10TP4FP22FN. Legacy on identical inputs was100/139 exact,
0TP36FP41FN. Sixty-one uncertain cases were excluded; this does not establish
accuracy on their policy/ownership categories.

The original audited development set also regressed from154/157 exact at the
older engine to149/157 exact (33TP1FP7FN). Candidate2.2 therefore improved the
adapted second set while losing earlier cases and failing the third sample.
All results and original annotations are retained. Do not claim generalization
or enable the candidate. Seed19021 is now development data for the next round.

Candidate2.3 replaces the narrow event-verb bottleneck with local dictionary
POS/inflection cues, normalizes exact legal-token concatenations, and tightens
generic-segment, public-officer, geographic-short-name, quote-page and industry
type guards. The current local suites pass157 tests including seven source
transport tests. CI will measure all three prior samples and reserve seed19022,
excluding600 prior primary headlines, for a new blind test. No seed19022 titles
or outputs have been inspected at this checkpoint.

The complete PR642 archive is available in12 published Actions artifacts:
23,635 filings for2,032 tickers. The old section cache must not be confused
with this complete pinned archive, or with the wider5,961-ticker Finviz universe.
A selective verified text reader and two full-issuer ingestion probes are added
to make the complete text available to preprocessing without transferring every
binary original. Full-source relevance accuracy remains unmeasured.
