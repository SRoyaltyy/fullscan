# Stock keyword output contract

`stock-keywords-1.0` projects existing exhaustive filing graphs into short,
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
The financial range normalizer only joins values within a fully matched
`23% to 25%, raised from 19% to 21%` construction. It never infers a reporting
period from unrelated table cells. Different supplier/metric contexts remain
separate records.

Auditor voice, administrative notice/update objects, unresolved generic objects,
and labor competition are retained but excluded from active stock-scoring facts.
These are targeted quality controls, not certification of the remaining claims.
Filing assertions are not independently verified present-day business facts.

## Article comparison

Python: `compare_article(board, text, article_date='YYYY-MM-DD')`.
Live API: `POST /api/keywords` with `{"text":"...","article_date":"2026-10-17"}`.
The date is optional; without it the current displayed filing cutoff is used.

The static page also accepts pasted headlines or full articles and uses the same
pinned normalization forms. Matches retain the stock-side relationship and exact
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
