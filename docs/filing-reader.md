# Filing reader with extraction overlays

The comparison page makes the stored filing text the primary view. Colors show
predicted organization (including agencies), person, product/platform, law,
place and unclassified/other types. They do not assert business relationships
or certified relevance. The index and relationship records are optional.

Fresh result bundles include all shared stored source sections, once, with
SHA-256 receipts, source metadata and codepoint offsets. The new overlay uses
the extraction-first candidate collector at 6dad95df8f7c2bb3b49df4de62c9ab80813c20db.
Old highlights require exact evidence anchors; no fuzzy name search creates
annotations. Older parser revisions fall back to their grammar inventory.

The reader preserves all source characters and unhighlighted context. Large
sections are paginated rather than shortened. The section chooser exposes
attachments and machine data as well as prose. Text is the stored extracted
filing text, not a reproduction of the SEC HTML/PDF layout. Category filters
remove marks only. Overlapping categories use their actual colors and remain
clickable; the detail area lists all overlapping spans. Full-section search
includes unhighlighted words. Source hashes and span text must agree.

Historical results without full source text explicitly require a fresh parse;
excerpts are never presented as complete filings. The on-demand request workflow
still requires the user's GitHub connection for fresh execution.

Validation commands from the repository root:

    python -m unittest scripts.test_stock_parser_request
    node --test dashboard/stock-comparison/test_request.mjs dashboard/stock-comparison/test_comparison.mjs dashboard/stock-comparison/test_filing_viewer.mjs

DOM interaction checks additionally use the pinned test-only dependency:

    npm install --prefix .viewer-test --no-save --package-lock=false jsdom@26.1.0
    node --test dashboard/stock-comparison/test_filing_viewer_dom.mjs

The authenticated live verification checks served source text against the full
result, old/new highlight switching, and the optional legacy index controls.
The parser accuracy benchmark is unchanged by this presentation work.
