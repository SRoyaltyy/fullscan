# Scanner test-page integration

This integrates PR651's page with the published candidate2.7 from PR645.
It does not include the unverified local2.8 work or certify classifier accuracy.

## Live testing

```
python -m src.scanner_backtest --identity-filings /path/to/materialized/objects --as-of 2026-10-19
```

Open http://127.0.0.1:8765. Headline-only is the default. For a separate,
explicit body-inclusive experiment, add `--input-mode full-text`.
Without `--identity-filings`, this is a description-only run. The archive path
must contain the verified PR642 manifests and full-text objects, not the old
capped SQLite cache. Filing input errors fail closed. No SEC fetch happens
inside the matcher. Machine-data views are retained in the archive but are
not interpreted as prose identities. This is an identity overlay, not complete
filing-derived business/exposure coverage.

The live ticker endpoint inspects the SAME compiled board as Test. Each
returned filing witness retains its date, accession, source passage and role.
Profile fact chips have explicit display caps; the underlying source archive
and matcher inputs are not truncated by those display limits.

## GitHub Pages

GitHub Pages cannot execute Python. Its Test button looks up exported results;
it is not a remote live classifier and cannot test arbitrary pasted text.
Generate into a new staging directory, inspect, then replace the deployed
artifact set as one versioned change:

```
python -I -S scripts/offline_evidence_checks.py --backtest-export \
  --identity-filings /path/to/materialized/objects --as-of 2026-10-19 \
  --out-dir /path/to/new-staging-directory
```

The exporter writes results, article display records, actual-board profiles,
ticker index and metadata together. It records source/resource hashes,
compiler version, input mode, news-content hash, profile fingerprint,
dated filing coverage and a snapshot id. It refuses to resume across different
snapshots or append to the old unversioned cache. Resume within the SAME run
is supported; malformed records require explicit repair.

The old standalone EDGAR profile-enrichment script cannot overwrite a
versioned scoring snapshot. Historical unversioned caches remain viewable,
but the page labels them as historical, not the latest candidate, and warns
that their display enrichment was not necessarily used by the scorer.
spaCy/clustering remains an exploratory layer, not another pass-decision path;
old parse artifacts are hidden for new snapshots until a matching parse
export exists.

PR652 preserves this page in main's Pages deployment overlay. The
`Refresh scanner test page` workflow now rebuilds and publishes a verified
headline snapshot after relevant parser updates merge to main. It retains
the new cache on main, updates only the scanner page on gh-pages under the
shared publication lock, requests a normal Pages build, and verifies the
served snapshot id. It does not override the trading dashboard's gates.
Publication refuses changed parser inputs, unverified filings, mixed cached
results, and failing regressions; permission/protection failures stop the run.
The page displays the scoring source commit. The
integration workflow's
CI smoke artifact uses three synthetic examples only, not private benchmark
headlines/labels or proof of generalization. Do not export a ninth validation
sample's predictions before its independent labels are frozen.
