# Stock keyword output contract

`stock-keywords-1.1` projects existing exhaustive filing graphs into short,
directional records: `stock -> relationship -> keyword`. It does not change the
grammar compiler, invalidate completed shards, or silently discard raw claims.

Examples from the archived Backblaze graph:

| Stock | Relationship | Keyword | Interpretation |
|---|---|---|---|
| BLZE | supplier | Western Digital | The filing identifies a hard-drive supplier. |
| BLZE | input | hard drive | Derived from the explicit supplier description; `via_supplier` retains the link. |
| BLZE | dependency | Stripe | Quote identifies reliance on Stripe to process payments. |
| BLZE | financial_metric | Adjusted EBITDA margin | Exact guidance ranges retained; period remains unknown pending review. |

Every record includes assertion state, first/latest filing dates, current-claim
status, review reasons, qualifiers and evidence IDs. Evidence includes the whole
supporting quote, source date, accession, document, section and character offsets.
Legacy dated officer facts carry quote-only evidence with explicitly unavailable
document offsets; they are not assigned fabricated document positions or tenure.
The financial range normalizer only joins values within a fully matched
`23% to 25%, raised from 19% to 21%` construction. It never infers a reporting
period from unrelated table cells. Different supplier/metric contexts remain
separate records.

Auditor voice, administrative notice/update objects, unresolved generic objects,
and labor competition are retained but excluded from active stock-scoring facts.
These are targeted quality controls, not certification of the remaining claims.
Filing assertions are not independently verified present-day business facts.

## Article comparison

Both sides use `entity-alignment-1.0`. Word normalization retrieves candidates;
it does not establish identity. Each stock keyword retains its source entity
type, identity scope, aliases, concept sense, affiliation, and supporting quote.
An article candidate must pass the same type/name/scope or concept-sense gate
before entering `matches`. Contradictions enter `rejected_matches`; insufficient
evidence enters `unresolved_matches`. Both remain inspectable with reasons.

| Filing identity | Article mention | Decision |
|---|---|---|
| PERSON: John McDonald, officer of Acme | Acme Industries CEO McDonald resigns | Aligned within the filing profile. |
| PERSON: John McDonald | McDonald's reports restaurant earnings | Reject the officer link. |
| PERSON: John McDonald | McDonald resigns | Unresolved surname. |
| CONCEPT: semiconductor chip | Potato chips snack prices rise | No aligned concept. |

These examples are synthetic controls, not SEC assertions. Catalog companies
can use `org:TICKER`; other proper names have typed, issuer-scoped identities.
Those local IDs are not independently verified global identifiers. A unique
full person name is unique only within the loaded registry. Same-surname
collisions require more evidence; a bare surname needs an attached person role
and the issuer in the same local context. Current officer tenure still requires
review even when a mention's identity aligns.

Stored headline publication first parses the exact selected text with the same
pinned spaCy/model versions used for filing grammar. Input hashes, entity spans,
labels, dependencies and sentence offsets accompany the parse. NER is evidence,
not an identity registry. Type conflicts reject links before context scoring.
The custom browser comparison uses the shared deterministic gate and abstains
where it lacks typed evidence; it does not run spaCy in JavaScript. The older
person-based stock-pass route also uses the person identity gate.

Context support scores are diagnostic overlap counts, not probabilities or a
replacement K-score formula. Relevance scores cannot override identity/type
conflicts. No claim of perfect disambiguation or measured semantic accuracy is
made without a labelled evaluation set.

Python: `compare_article(board, text, article_date='YYYY-MM-DD')`.
Live API: `POST /api/keywords` with `{"text":"...","article_date":"2026-10-17"}`.
The date is optional; without it the current displayed filing cutoff is used.

The static page also accepts pasted headlines or full articles and uses the same
pinned normalization forms and exported identity rules. Matches retain the stock-side relationship and exact
article offsets. Punctuation boundaries cannot manufacture a multiword match.
Dated offline intake comparisons exclude future evidence and recalculate latest
assertion state. Custom browser comparisons are explicitly current-cutoff
retrieval, not historical backtests. Python/browser parity controls cover these
current-cutoff matches.

Common non-entity keywords are grouped as broad ambiguous overlaps unless the
article names the stock. A keyword match alone never changes a stock pass or
invents a positive/negative price impact. No causal event understanding is
claimed for arbitrary pasted text.

## Publication and archive boundaries

The full grammar artifact retains every paragraph/text block, including unknown
classifications, raw candidates, and full documents. The compact page exports
every candidate keyword and quoted witness, not the multi-GB paragraph archive.
A receipt binds candidate counts to the original hashed index. Missing evidence,
missing covered tickers, checksum mismatch, or excessive publication size fails
the build and retains the old page; there is no silent truncation.

The search index is split into hashed buckets, loaded only for words appearing
in the pasted article. If the rest of the published site leaves insufficient
Pages space, the page loads these compact assets from the exact repository
snapshot commit at `raw.githubusercontent.com`; SHA-256 checks still apply.
The complete compact assets remain committed and archived. Page bootstrap files
remain on Pages. This avoids requiring a new storage account to test keywords.

Until recovery completes, a real Backblaze development sample may be published.
It is explicitly labelled `development_sample`, never whole-archive coverage.
The complete recovery workflow automatically triggers a full-set refresh.

GitHub artifact links may require sign-in and have retention limits. Optional
durable R2 publication uses only the designated existing Actions configuration:
`R2_ENDPOINT_URL`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET`, and
`R2_PUBLIC_BASE_URL` (repository variable). No account/bucket creation or access
policy changes are attempted. Public objects must allow CORS GET/HEAD for
`https://sroyaltyy.github.io` and serve `.gz` as binary without Content-Encoding.
Immutable hash-bound paths, checksums and public headers are checked before the
page is pointed at external graphs. Configuration preflight prints names only.

Remaining limits: missing relationships are unknown; exhaustive text accounting
is not exhaustive semantic extraction; precision/recall are not certified.
The archived release scope is the latest annual filing plus amendments and
subsequent filings, not every historical SEC filing since incorporation.
