# Problem taxonomy — what will break before it does

`eval/rubric.md` classifies **observed** verdicts (the failure taxonomy).
This document is the prospective mirror: categories of problems a
deterministic lexical linker is *expected* to hit, each with the
countermeasure that owns it and its current status.

Loop rule: when a new failure appears, find its home here first. If it has
no home, add the category, then add a corpus case (`kind: target` until the
countermeasure lands), then build the countermeasure. A failure that never
gets a home will come back.

Status legend: **SOLVED** (countermeasure in the scorer today) ·
**MITIGATED** (partial) · **OPEN** (no countermeasure yet)

---

## A. Surface form — the words don't match even though the meaning does

| # | problem | example | countermeasure | status |
|---|---|---|---|---|
| A1 | US/UK spelling variants | defence/defense, licence/license, aluminium | variant-normalization table applied at prepare() | **OPEN** (observed: Pentagon article) |
| A2 | Inflections | launches/launch | inflection folding (Lockheed-safe) | **SOLVED** |
| A3 | Derivational families | administer/administration/administrator | keep distinct; add discounted WordNet derivational links (~0.5 credit) | **OPEN** |
| A4 | Synonyms with zero lexical overlap | fast/quick; reentry/splashdown; lawyer/attorney; lender/bank | WordNet synsets (discounted) → offline embedding neighbor table, same-industry paths only | **OPEN** — this is the sanctioned embeddings residual layer |
| A5 | Abbreviations & institutional aliases | "the Fed" vs Federal Reserve; FAA, NATO, SEC | acronym table extracted free from filings' first-use pattern: `Federal Aviation Administration ("FAA")` | **OPEN** — filings hand this to us |
| A6 | Register mismatch press ↔ filings | "stocks" ↔ "equity securities"; "fired" ↔ "terminated" | corpus PMI between filings vocabulary and news vocabulary; feeds the same machinery as A4 | **OPEN** |
| A7 | Numbers, dates, units | "$1.5B" ↔ "1.5 billion"; "Q3" ↔ "third quarter"; 24/7 | normalize units and quarter forms at prepare() | **OPEN** |

## B. Sense & reference — the words match, the meaning doesn't

| # | problem | example | countermeasure | status |
|---|---|---|---|---|
| B1 | Industry-crossing homonyms | space (aerospace/office), cell, bank, charge, delta | industry gate + same-sentence context vote — the core design | **MITIGATED** |
| B2 | Metonymy | "Washington announced…", "Detroit", "Hollywood", "the Street" | metonym→industry gazetteer; agency names handled by the office-title rule | **OPEN** |
| B3 | Proper noun vs ticker collision | Washington Post/POST; Apple, Delta, Tesla (person), Mercury | word-ticker table + publisher/org gazetteer + tiered syntax match | **MITIGATED** — publishers masked, dictionary-word aliases need a capital or `$TICKER`; person/place metonymy is still B2 |
| B4 | Finance slang & figurative language | "RKLB to the moon", "dead cat bounce", "rocket" (up 20%) | the cruelest category for a lexical system: slang literally points at the wrong industries. Keyness vs the day + sentence-context gate blunt it; a finance-PMI outlier stoplist is the real fix | **OPEN** |
| B5 | Brand stylization & tokenization | eBay, iPhone, L'Oréal, "24/7" | tokenization rules + diacritic/hyphen folding | **OPEN** |

## C. Sentence structure — the parse loses the topic

| # | problem | example | countermeasure | status |
|---|---|---|---|---|
| C1 | Negation & denial | "Boeing denies the report" — still about Boeing | negation changes event type, not relatedness; keep the name hit | **OPEN** |
| C2 | Hypotheticals & questions | "Could SpaceX go bankrupt?" | mood ≠ relatedness; same rule as C1 | **OPEN** |
| C3 | Comparisons | "Unlike Boeing, Airbus…" — who is the subject? | persistence across the document (recurrence) beats syntactic position; position is a weak prior only | **OPEN** |
| C4 | Coordinated lists | "BA, LMT and RTX" | each member gets a name hit; topical pass comes from description overlap, never from co-mention alone | **SOLVED** for names |
| C5 | Coreference | "Rocket Lab reported… The company said…" | deterministic anaphora: "the company / the firm / the maker / it" resolves to the last named company and extends its evidence | **OPEN** |
| C6 | Headline-ese | "RKLB Jumps; Powell Signals Cuts" | heading weight + phrase rules | **MITIGATED** |
| C7 | Passives & buried actors | "…was awarded a contract by NASA" | actor/action/target extraction — deliberately deferred (build order) | **OPEN** |

## D. Document context — whole-article assumptions break

| # | problem | example | countermeasure | status |
|---|---|---|---|---|
| D1 | Multi-theme articles | omnibus bills; "Stocks that moved today" roundups | multi-industry votes allowed by design — but roundup/index-inclusion policy is needed: **index membership ≠ topical** | **OPEN** |
| D2 | Page furniture poisons scraped pages | sidebar "Read also: Tesla…" pollutes the context universe | boilerplate-zone stripping at ingest, before correlation ever sees the text | **OPEN** — lives upstream in the parser, not the scorer |
| D3 | Syndication & duplicates | one Reuters story × five outlets | near-title dedupe at intake | **OPEN** |
| D4 | Corporate identity drift | Twitter→X, Facebook→Meta, GOOG→GOOGL, "formerly known as" | corporate-actions alias registry (offline resource, maintained) | **OPEN** |
| D5 | Partial relevance | one relevant paragraph inside a long piece | passage-level fallback vs whole-doc vote — decide during calibration with real failures in hand | **OPEN** |

## E. Entities & relations — what filings mention vs what articles mean

| # | problem | example | countermeasure | status |
|---|---|---|---|---|
| E1 | Relation typing beyond three roles | regulator, parent, subsidiary, JV, shareholder, auditor, landlord, index provider | extend the relation graph from a cue-word table; **subsidiaries are the biggest miss** (an article about Google Cloud is about GOOGL) | **OPEN** (3 roles today) |
| E2 | Subsidiary ↔ parent | "LinkedIn cuts staff" → MSFT | Exhibit 21 (subsidiary exhibit) is structured and free; plus Item 1 "we own X%" sentences | **OPEN** — highest-value free resource in this table |
| E3 | Private companies | SpaceX, OpenAI, Cargill — major news drivers with no filing | private-name registry built from public filers' relation edges + a hand-seeded list | **OPEN** (the SPCX false hit is this gap in the wild) |
| E4 | Government as counterparty | "NASA awards $500M contract" — the agency is a customer, not a stock | agency gazetteer + typed relation (regulator / major-customer) with second-order semantics | **OPEN** (title-stripping exists; typing doesn't) |
| E5 | Individuals | "Musk says…", "the CEO of Rocket Lab…" | officer lists from filings' cover/signature pages → person→company map | **OPEN** |
| E6 | Index & ETF holdings | "S&P 500 adds RKLB" | typed relation index-member; second-order at most | **OPEN** |
| E7 | Ungated second-order relations | Playboy's filing mentions DraftKings; the article is about gambling, not Playboy | dependency rows carry `order: 2` and surface only when the article's industry vote is the partner's industry | **MITIGATED** |

## F. Scoring methodology — the math's own weak joints

| # | problem | example | countermeasure | status |
|---|---|---|---|---|
| F1 | Arbitrary thresholds | z≥2.5, batch-z≥2.0, 75% absorption, purity≥0.55, circle floor 0.35 | percentile-of-score vs per-stock random-article background; freeze during a dedicated calibration round against the corpus | **OPEN** |
| F2 | Coverage punishes single-filing phrases | "steps down" → near-zero weight (documented) | weight floor for name/alias terms; cap coverage's role for multiword phrases | **OPEN** |
| F3 | Keyness control corpus choice | the day's news kills words trending across the whole sector; a tiny batch judges itself | fixed reference window (e.g. 30 days of news) + min-token guard | **MITIGATED** (day reference supported; window not built) |
| F4 | Universe screen bias | LMT/RTX/NOC/GD off-board via the ATR screen; ICE off-board | name hits are looked up on the full description list and marked `off_board`; topical scoring still uses the liquid universe | **MITIGATED** |
| F5 | Industry granularity | conglomerates (BRK, AMZN) dilute a single industry pocket | segment-level profiles from 10-K segment disclosures | **OPEN** |
| F6 | Batch-z activates only at 8+ articles | corpus runs don't exercise it | accepted: calibrate batch behavior separately on full days, not on the corpus | **DOCUMENTED** |

## G. Resource gaps — the offline data the system needs

Ticked items exist in some form today. Everything else is a build task for
the loop, roughly in payoff order:

- [x] word-ticker table (English-word tickers → stricter matching; `src/data/english_words.txt`)
- [x] publisher / organization gazetteer (Washington Post, Renaissance Capital)
- [ ] US/UK variant pairs + hyphen/diacritic folding
- [ ] WordNet (synsets + derivational links) or equivalent
- [ ] acronym table extracted from filings' `("ACRONYM")` first-use pattern
- [ ] finance-slang stoplist from filings↔news PMI outliers (B4)
- [ ] corporate-actions registry: renames, ticker changes, M&A, delistings (D4)
- [ ] subsidiary list from Exhibit 21 + Item 1 ownership sentences (E2)
- [ ] person→company map from officer lists (E5)
- [ ] private-company seed list (E3)
- [ ] 30-day news reference window for keyness (F3)
- [ ] offline embedding neighbor table over the filing vocabulary (A4)
- [x] places gazetteer (src/places.py)
- [x] inflection folding (src/correlation.py)
- [x] partner role cues: customer/supplier/collaborator (src/filing_parts.py)

---

## How a failure moves through this document

1. Observed in a round → classified by the failure taxonomy (rubric.md).
2. Matched to a row here (or added as a new row/category).
3. Corpus case added with `kind: target`, pointing at the row.
4. Countermeasure built → status flips OPEN → MITIGATED → SOLVED.
5. Case flipped to `kind: regression` once SOLVED, so it can never reopen
   silently — this is the direct answer to "the AI sometimes reopens old
   problems."
