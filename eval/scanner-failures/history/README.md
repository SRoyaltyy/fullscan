# Fullscan: reserved-issuer keyword audit — 23 October 2026

**Result: FAILED. Do not merge or publish this candidate as a validated fix.**

The deployed shared policy still makes semantic and relationship errors on other tickers. A locally developed candidate improves its development sample but does not generalize to the reserved issuers. Source evidence was reviewed directly from immutable stored scanner records; no new web extraction or filing collection was used.

## What was tested

- Random draw of 20 filing-backed tickers from the published catalog; 10 used for development, 10 reserved before the candidate was frozen.
- Seed: `202610231749`. Population after excluding 18 identifiable previously reviewed issuers: 2,014.
- The original earlier 20-ticker sample could not be recovered. This is a new draw. Unknown members of that previous sample could overlap; we cannot claim all issuers were never previously reviewed.
- Replayed all 6,030 archived rows across this draw. Baseline decisions exactly match the served assets on every row.
- Reviewed 200 rows against their stored evidence: 50 previously admitted and 50 rejected per split. Selection rotates through relationship classes within each ticker using a fixed random order. These are stratified diagnostic samples, not population-weighted accuracy estimates.
- Judge: assistant evidence review. This is not an external human audit.
- The candidate source SHA-256 was frozen before reserved examples were inspected. The reserved results were not used to change that candidate.

## Results

| Sample | Version | Correct decisions | Precision among admitted sample rows | Recall among labelled relevant existing rows |
|---|---|---:|---:|---:|
| Development | Deployed baseline | 77 / 100 | 68.0% | 82.9% |
| Development | Frozen candidate | 96 / 100 | 91.1% | 100.0% |
| Reserved issuers | Deployed baseline | 78 / 100 | 62.0% | 91.2% |
| Reserved issuers | Frozen candidate | **79 / 100** | **62.7%** | **94.1%** |

The candidate misses the 90% acceptance threshold for precision and exact gate decisions on the reserved sample. These figures measure term/relationship admission on existing extracted rows. They do **not** measure exhaustive recovery from whole filings, entity alignment in articles, financial impact accuracy, or overall market accuracy.

Four reserved judgments are particularly sensitive to specificity requirements: unnamed Internet infrastructure, a shortened Bribery Act citation, clipped GLP-1 versus GLP-1 receptor agonist therapies, and unnamed starting materials. Even accepting all four would only yield 83/100 correct decisions and 70.6% precision; the candidate would still fail.

## Tickers

Development: VFC, INOD, MFP, NUS, AUTL, STAA, KGC, TRN, VRRM, APG.

Reserved: BTG, STOK, JXN, ALOY, IBM, HCAT, PROK, CRMD, ASM, CYTK.

## Confirmed shared failure modes

| Example from reserved issuers | Current output problem | Underlying requirement |
|---|---|---|
| BTG → Agnico Eagle → customer | Source describes sale of a joint-venture ownership interest | Separate divestiture counterparties from operating customers |
| CYTK → COMET-HF → partner | Source identifies a clinical trial | Resolve what the entity is and attach the relation to its own clause |
| HCAT → DOS → customer | Source identifies the company's platform and clients migrating from it | Separate a product modifier from the customer argument |
| IBM → DB pension plans → commercial product | Source describes retirement benefits | Identify who receives the offering and in what capacity |
| ALOY → FINRA → regulatory exposure | Source says the issuer does not believe it is subject to oversight | Scope reported negation and uncertainty to the actual issuer assertion |
| JXN → equity options → upstream input | Source describes financial derivatives | Classify the purchase object and transaction purpose |
| PROK → ProKidney Acquisition Company → hidden subsidiary | Source explicitly lists Company subsidiaries | Recover coordinated named arguments without treating other names as issuer aliases |

## Tradeoffs discovered beyond the labelled sample

The full-row difference review found newly admitted garbage, including IBM's generic `plans`, BTG's `projections`, and ALOY's `the “Merger` fragment. It also found a valid JXN `Jackson Brooke LLC` subsidiary suppressed by the candidate's owner check. Thus the candidate is not just incomplete: some of its changes introduce new errors.

The physical WordNet category check is especially weak: a word with any physical sense can be admitted in an unrelated financial or abstract sense. A correct material sense for `natural gas` does not justify treating `plans` or `support` as physical products. Shared rules and synthetic ticker invariance alone do not establish semantic generalization.

## Work completed and release status

- Implemented locally: a candidate separating some financing and employee-plan contexts, improving named appositions, and keeping specific legal titles and commodity products.
- Tested: 57 regression tests and the browser DOM/matching parity suite pass. Those tests do not override the failed reserved audit.
- Audit integrity: 6,030 ticker substitutions changed zero admission decisions. Across all replayed profiles, zero non-policy source-record fields were modified. The 131 changed decisions include the 20 sampled issuers and five earlier diagnostic issuers.
- **Not merged. Not deployed.** The published scanner remains on the prior version. The candidate is retained for review and reproduction, not presented as ready.
- Current catalog coverage remains 2,032 filing-backed and 3,964 description-only tickers. No completeness claim is made for the extracted inventory.

## Required methodological work

Before further publication, extraction needs an evidence frame that binds the issuer, actor, action, complete object identity, transaction capacity, negation/modality, and ownership/temporal scope within the same proposition. Term specificity and semantic type must be checked in that frame, rather than inferred from capitalization, dictionary membership, or a nearby relationship word. Recovery of valid hidden terms must be tested alongside removal of bad terms. The consumed reserved set is now regression material; any revised method needs a new frozen issuer holdout.

## Reproduce

From the candidate branch, bootstrap the pinned resources as the existing workflow does, then run:

```sh
python -m scripts.evaluate_keyword_generalization --download --output reproduced-metrics.json --require-pass
```

The command intentionally exits 1 for the frozen candidate's failed acceptance result. Immutable source URLs, per-profile checksums, the random seed, candidate code hash, all judgments and their full source witnesses are stored under `eval/keyword-generalization/`. The reader can challenge any individual judgment; no cases have been removed after seeing the reserved results.
