# Round 005 judgment

Draw: 20 articles, seed `20261019`, from `data/news_intake/2026-10-17/documents.json` (11,213 eligible). The draw is `eval/pool/round-004-random.jsonl`. The same 20 were scored twice. Program output is `round-004-sample.json` (before the alias and industry-word fixes) and `round-005-sample.json` (after).

Judge: the rubric in `eval/rubric.md`. A pass is topical relatedness. A name hit is not relatedness. Unknown is correct when the text has no topic. This draw is mostly headlines repeated as the body, so unknown is the right answer more often than a pass.

## What the frozen corpus did

`eval_diff` from round 004 to round 005: regression pass rate stayed 1.0. No regression flipped. No achieved target was lost. `launches-inflection` is still the one achieved target. New locked cases from this draw all pass: `defence-spelling-match`, `ap-news-not-a-ticker`, `moderna-possessive`, `md-is-a-title`, `san-juan-dateline-agencies`.

Previous failures that did not come back: the space memo still passes Rocket Lab; office space still does not; Washington Post is still not `POST`; `$POST` still hits; DraftKings links stay second-order; the Klook publisher line still names nobody.

## The 20, after the fixes

| id | judgment | class |
|---|---|---|
| r4-01 suture recall, no company | correct unknown | — |
| r4-02 brush fires, AP News | correct unknown. First pass named ticker AP | was ALIAS_FP, fixed |
| r4-03 office move, company unnamed | correct unknown | — |
| r4-04 NCAA sues a governor | correct unknown | — |
| r4-05 Tebra breach, private company | correct unknown | — |
| r4-06 Tulsa King television teaser | correct unknown | — |
| r4-07 fraud task force | correct: no pass, no name. First pass named SAN, IRS, CI, HHS, DEA | was ALIAS_FP, fixed |
| r4-08 "MD Peter Woods" | correct unknown. First pass named ticker MD | was ALIAS_FP, fixed |
| r4-09 press secretary steps down | correct unknown. The old "steps down" false pass did not return | — |
| r4-10 Schneider, data centers | acceptable unknown. Headline only, and Schneider is more than one company | open, ambiguous name |
| r4-11 Biohaven epilepsy license | name hit BHVN is right. GENB, ASND, AVTX, RVMD, KOD, XENE, VSTM, INSM also passed on `drug` / `readout` | FALSE_PASS, F7, target |
| r4-12 museum acquires a sculpture | correct unknown | — |
| r4-13 earthquake drill | correct unknown | — |
| r4-14 private real-estate partners | correct unknown | — |
| r4-15 crude inventory headline | correct unknown. No oil producer was dragged in | — |
| r4-16 local cruise-night winners | correct unknown | — |
| r4-17 disability-platform pilot | correct unknown | — |
| r4-18 Moderna's supply chain | name hit MRNA, no topical pass. Right for a headline. First pass missed the name because of the apostrophe | was MISSING_NAME, fixed |
| r4-19 school cyberattack | correct unknown | — |
| r4-20 FDA heart-valve headline | CYTK passed on the single word `valve`. The headline names no company | FALSE_PASS, F7, target |

16 of 20 are correct. Two were wrong and are now fixed and locked. Two are still wrong and are targets, not silent misses.

## Fixes this round

- UK spellings fold onto the US form in `src/spelling.py` (`defence`/`defense`, `-our`/`-or`, `-ise`/`-ize`, with `hour` and `promise` left alone).
- True synonyms fold to one token in `src/synonyms.py` (`quick`/`fast`, `lawyer`/`attorney`). A few near pairs (`reentry`/`splashdown`, `administer`/`administration`) add discounted description credit and do not cast an industry vote. Homonyms are not grouped.
- A trailing ` - Publisher`, a dateline (`SAN JUAN,`), an agency abbreviation (`IRS`, `DEA`, `FDA`), and `MD` before a person's name are not tickers. `Moderna's` still matches Moderna. `$TICKER` and `BA, LMT` still match.
- A word used by at least 45% of a large industry can vote for that industry and does not also score every member.

A within-industry z-score was tried against the biotech bleed. It cleared r4-11 and failed the space memo, the launches target, and the defence match, so it was removed. That attempt is why F7 is still open.

## Still open

F7, the biotech subset (`bhvn-not-every-biotech`, `heart-valve-not-cytokinetics`). A4 beyond the frozen synonym list (no embedding table). A3 beyond three families. A5 filing acronyms. B2 metonymy. B4 slang. E2 subsidiaries. F1 cutoff calibration. F4 topical scoring still uses the liquid universe. The OKX, Pentagon, and Barron's excerpts are still unlabeled. The 2026-10-17 day file is still the pre-fix batch.

Most of this random draw had no article body, only a repeated headline. The next 20 should be drawn at random from documents whose body is longer than the title, or the round will mostly reconfirm that unknown stays unknown.
