# Full dated company filing archive

This is the full-content replacement for using excerpt SQLite sections as source documents. The old `company-filings-corpus` is not evidence of full-document coverage.

The pinned input is the audited 16 October 2026 universe: 2,032 tickers and 2,026 CIKs. It is recorded in `data/company_filings_universe_2026-10-16.json`, with the originating run. Dual classes share originals but keep all ticker associations. Expanding or refreshing the universe is a separate explicit inventory change.

Coverage per issuer: the latest 10-K, 20-F or 40-F (including canonical registration forms), same-period amendments, and **every** subsequent 10-Q, 8-K and 6-K. There is no 8-K item/description filter and no eight-6-K ceiling. With no annual, the previously defined fund-report/registration/quarterly fallback applies. The fallback inventory is explicit in each company's manifest. This window is not all historical filings since the issuer's inception; Form 4, 13F and 13D remain outside this issuer-report archive.

For each selected accession, download the entire SEC complete-submission `.txt`. Preserve its exact bytes, including every embedded filing document, exhibit, financial table, XML data file and submitted binary asset. Verify the submission terminal marker, every document boundary, original byte length and SHA256. Decode embedded binary files without a size cutoff; the complete original submission remains authoritative and retrievable even alongside the decoded view. Original payloads are never shortened at 25MB, 180,000/350,000 characters or a company-level byte quota.

Each filing manifest contains the accession, CIK, native and canonical form, filing date, reporting-period date when supplied by SEC, acceptance value as published, retrieval timestamp, SEC URL, primary filename, full file inventory, byte counts and checksums. Exhibit files inherit the accession's filing/report metadata. A missing reporting-period date stays null rather than being invented.

The full-text view is separate from the exact original bytes. HTML extraction keeps all visible sections and numeric tables, while omitting scripts, styles and hidden XBRL implementation metadata from that view only. All PDF pages are read; image-only pages use OCR when available, and remaining unreadable pages are reported as `needs_ocr`. Images and other binary assets remain available as originals rather than being described as extracted text. An incomplete download, missing primary document or failed text extraction cannot yield `text_complete=true`.

Storage uses compressed, content-addressed originals and full-text JSON objects per accession, plus manifests. `full_archive.read_original(objects,cik,accession)` returns the **whole original SEC submission**. Pass a filename to return the complete embedded file. `read_filing(...)` returns the filing manifest and every file's full text. These APIs do not use `Store.point_text` or return excerpts. For model batching, create a separate chunk index with source checksums; do not replace or truncate these stored originals.

R2 requires repository Actions settings `R2_ENDPOINT_URL`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET` (endpoint and bucket may be repository variables). The endpoint must be an existing account's R2 endpoint, and the key must permit object reads and writes for the chosen bucket. No bucket or credential is created by this code. Uploaded objects use an MD5 transport checksum, SHA256 metadata and verified byte counts; resume reads back originals/text and checks their hashes before treating them as stored.

Without R2, complete originals are retained in 12 sharded Actions artifacts for 90 days, with `durable=false` in the manifests. That is temporary storage, not a claim that R2 is deployed. Each shard restores its previous artifact before resuming. It stops with explicit incomplete records if disk space is insufficient; it never truncates files to fit. Once R2 settings are provided, the same archive can use the durable backend. Existing local artifacts must be migrated or read through the provided object abstraction before they can be claimed as present in R2.

The workflow initially runs the six full-document probes SUZ, TIGO, CALM, NVO, ASML and RKLB, including prior extraction failures. Set `config/company_filings_archive.json` mode to `full` after the probe passes. Full runs use 12 shards, at most four concurrent, each sharing a 1.5-request/second limiter across its two workers (six SEC requests/second across the archive jobs). Terminal SEC access refusal stops further requests through that client. A daily schedule becomes effective only after the workflow is merged to the default branch.

Per-shard `report.json` lists expected tickers, expected accessions, raw-complete filings, text-complete filings, errors, unreadable files, unprocessed CIKs and durable-storage status. Completion requires all expected CIKs and filings, not merely successful process exit or a company row marked `ok`. LLM calls remain zero during archiving. Tests cover whole-file round trips beyond the former caps, exhibits and binary attachments, table retention, all PDF pages, dates, rejected partial responses, checksum verification, selection coverage and resume.

Original downloads are written and checksummed before parsing begins. A `download.json` checkpoint preserves their dates and object keys even if a reader fails; only a separately verified `manifest.json` establishes complete parsing. The six-company probe additionally compares up to three decoded embedded assets per issuer against the individual SEC files. This checks the SEC uuencoding compatibility against independent original bytes, rather than relying only on hashes generated by our own decoder.

## Reading dates and whole files

Download the relevant `company-filings-full-shard-N` artifact from the verified full run. Its `objects/` directory is the local object-store root. The shard number is `int(sha256(cik.encode()).hexdigest(),16) % 12`; CIKs are zero-padded to ten digits. Use `--r2` instead of `--objects` once the archive is actually present in R2.

```
python scripts/read_company_filings_full.py --ticker RKLB --objects downloaded/objects
python scripts/read_company_filings_full.py --ticker RKLB --objects downloaded/objects --accession ACCESSION --original --output original-submission.txt
python scripts/read_company_filings_full.py --ticker RKLB --objects downloaded/objects --accession ACCESSION --output all-full-text-and-dates.json
python scripts/read_company_filings_full.py --ticker RKLB --objects downloaded/objects --accession ACCESSION --filename DOCUMENT.htm --original --output whole-document.htm
python scripts/read_company_filings_full.py --ticker RKLB --objects downloaded/objects --accession ACCESSION --all-originals --output complete-filing-directory
```

The first command lists dates and completeness. The others export the whole original submission, every embedded file's complete text plus metadata, or one entire original file. These commands never request point excerpts. For binaries, use original export; a binary asset's empty text field does not replace its preserved original.

`--all-originals` creates a new directory containing the exact complete submission, the date/checksum manifest, and **every** decoded embedded original under `originals/`. Open the primary HTML file inside that folder so its relative image references resolve to the accompanying original image files. Existing output directories are refused to avoid overwriting earlier exports.

After configuring R2 credentials, migrate each downloaded shard with `python scripts/migrate_company_filings_full.py --objects downloaded/objects`. This copies and verifies complete originals and full text before publishing durable manifests; it never requests SEC files again. It resumes by verifying existing destination objects. The command migrates the filings present in that shard, and does not by itself assert that all 2,032 tickers are complete.

For the complete transfer inside GitHub Actions, run **Migrate full filing archive to R2** (`company_filings_r2_migration.yml`) with the verified full archive's run ID. It requires the four R2 settings, checks the source completeness report and all twelve unexpired source artifacts, restores four shards at a time, verifies originals and text in R2 before publishing durable company manifests, and produces a new full dates/completeness artifact. This is a manual migration with zero SEC requests; it does not create credentials or a bucket. A successful migration requires `complete=true` and `durable=true`. The workflow is provided for use once credentials exist; its presence is not evidence of a live R2 transfer.

## Research consumers

The original filing is the source of record. Chunking and inference must retain accession, filename, source checksum, filing date and reporting-period date so a claim can be traced back to the complete document. Derived exposure records may use activity, product, customer, place, permission, input and alias as join keys; a later lower-ranked source must not overwrite a filing fact. Do not treat an empty text field for a preserved graphic, spreadsheet or other binary as evidence that it contains no information: read the original or run a separate visual/OCR pass. The text-completeness flag covers textual document views and preserved binary originals; it does not assert that every image has been transcribed or every research inference has been completed.

If an HTML exhibit contains only images, the reader OCRs **every** referenced embedded image in document order. No slide count is capped; missing images or OCR-engine failures remain `needs_ocr`. The OCR view records every image's original checksum and per-image status, and is labeled as OCR rather than native source text. All original images remain available for checking OCR mistakes. Euronet's 75-slide exhibit `0001554855-26-001090/ex991_1.htm` is a live regression probe for this case.

The final workflow verification also checks that each local shard's source artifact was actually published; successful local processing alone cannot establish stored coverage. Scheduled refresh is blocked until durable R2 storage is configured, to prevent daily duplication of the entire temporary archive.

The final `company-filings-full-completeness` artifact includes `company_filings_full_dates.csv`, one row per issuer/accession with all associated tickers, filing date, reporting-period date, acceptance value, original SEC URL, original byte count/checksum and manifest key. It is a date/source index into the full archive, not a substitute for the original documents. Partial runs retain partial indexes alongside the explicit completeness report.

PDF pages with no native text are rendered in full RGB at twice the page's native coordinate resolution before they are classified as uniformly blank. Every pixel must match. A separately identified background-only page must contain exactly one filled rectangle within a quarter point of every page boundary, with no images, other drawings or annotations; this accommodates empty hairline margins in PDF exports. Other non-uniform drawings and images still require text extraction or OCR. Every page remains in the original PDF and full page index. An interrupted download checkpoint without the complete submission's terminal marker is fetched again on resume; it is never treated as a reusable complete filing merely because its saved checksum matches.

`src/company_filings/page_reviews.json` records visually checked pages that contain graphics without text. Reviews apply only to the exact decoded PDF SHA256, page count and page number; they cannot transfer to a modified filing. The reader records the review and evidence in `reviewed_visual_pages`, retains the whole page and does not invent a transcript. Unreviewed image-only pages still need OCR. Enerflex's final decorative back cover is the first review: its entire 152-page PDF was processed, and the rendered final page contains only an abstract background and geometric logo.

Normal pushes do not cancel an active archive run. A commit explicitly tagged `[full-archive-repair]` can supersede a known incomplete verification pass after all base originals are safely published. Restored source objects and per-filing checkpoints remain reusable.

## Finding the verified release

The verified full run is [39053884472](https://github.com/SRoyaltyy/fullscan/actions/runs/39053884472), on code commit `2f78489458e7592b06c5b626c43ba54990ca70f9`. All twelve source batches and the consolidated verifier passed. Coverage is 2,032 tickers / 2,026 issuers, 23,635 complete selected submissions and text views, and 845,848 embedded files. Original selected submissions total 121,966,778,134 bytes before compression. There are no missing or mismatched tickers, incomplete filings or verification errors. All 56 filing/reader/migration tests passed in Actions.

The `company-filings-full-completeness` artifact (ID `12109396961`) contains both `company_filings_full_report.json` and `company_filings_full_dates.csv`. Its 23,635 date-index rows were checked individually against the filing records, including ticker associations, filing/report/acceptance dates, source URL, original SHA256, byte count, manifest key and completeness flags. Every filing date and acceptance value is present; SEC leaves the reporting-period date blank for 150 filings, which remain blank. Storage is published 90-day Actions artifacts with `durable=false`; R2 has not been configured or live-tested.

`data/company_filings_full_release.json` is the small checked release pointer. It records the canonical run, compatible reader commit, coverage counts, all twelve original-source artifact IDs and digests, expiration times, and the dates/completeness artifact. Use that pointer rather than an older chat message or a failed diagnostic run. Corpus objects remain outside git.

The pinned universe uses market capitalization of at least $80 million, average volume of at least 500,000 shares, and ATR of at least 2.5% of price. It excludes ETFs, closed-end debt/equity/foreign funds and shell companies. The snapshot and ticker-to-CIK associations are fixed by the committed universe inventory.

After the R2 migration workflow is merged to the default branch, add the four repository Actions settings and run it with the release pointer's `verified_run_id`. It refuses an incomplete or expired source run. Its `complete=true` and `durable=true` result is the evidence of a completed permanent transfer; merely adding settings or merging this PR does not migrate existing files.

After a successful permanent transfer, update the release pointer to reference the new durable completeness report. Keep the original verified source checksums and the initial artifact provenance.
