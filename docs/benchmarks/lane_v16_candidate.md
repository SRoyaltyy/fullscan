# V16 development candidate

The completed V15 Grok 4.3 development replay (run 38680245701) scored action agreement 96%, precision 94.44%, recall 85%, Lane agreement 92%, and exact sector match 84%, with 100/100 available. It fails recall and sectors. Acceptance remains 0/10.

V16 adds a required per-row literal evidence inventory for institutional predicates and business subjects inside the existing independent facts batch. The inventory is retained in raw fact-review output, validated against its own title, and never derives action or sector labels. Final facts still come from the model; Python only validates schema/evidence and applies the existing definitions and role filter. There are no article-specific runtime overrides or reference inputs.

The prompts distinguish private pension funds from statutory public institutions, retain secondary official instruments/engagements and intergovernmental supply operations, and preserve explicit industries, catalog-backed issuers and operating businesses in commentary. Lane instructions clarify main predicate versus background and several existing ontology boundaries. No ontology classes, catalog records, frozen references, metrics or thresholds change.

Normal requests remain two prompts per ten titles, with bounded validation recovery. This is a call-count design, not a measured billed-token saving.

Local Windows verification: 77 semantic tests and 5 provider tests pass. Two existing POSIX runner tests are deferred to the required Linux CI job, which will run the complete 79 semantic tests before any model calls. Live V16 accuracy is pending. Fresh holdouts remain unopened for classifier execution; selection novelty corrections are still pending for future batches 05–12 and must be completed before those batches run.
