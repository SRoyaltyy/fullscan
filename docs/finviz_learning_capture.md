# Run the Finviz learning capture from Grok

- **Workflow:** `finviz_learning_capture.yml` — “Finviz learning capture (all stocks)”.
- **Repo/branch:** `SRoyaltyy/fullscan`, `main`.
- **Trigger:** Manual dispatch only. Grok chooses when it runs; there is no competing new cron.
- **Full mode:** Live bulk export, a small authenticated full-panel check, then sequential batches covering the entire declared stock universe. It tries the actual rendered page/expanded headline dialog when server HTML has no full panel. A failed source check archives diagnostics and prevents the fanout.
- **Export mode:** Fast bulk collection of every available short digest. It preserves blank fields and all ticker links, but never declares full narratives captured.
- **Existing credentials:** Uses `FINVIZ_AUTH` / `AUTH_TOKEN_FINVIZ`, or `FINVIZ_EMAIL` + `FINVIZ_PASSWORD`. `FINVIZ_EXPORT` is optional; its URL must be HTTPS on Finviz. Never put secret values in Grok prompts or logs.

## Command

```bash
gh workflow run finviz_learning_capture.yml \
  --repo SRoyaltyy/fullscan --ref main \
  -f mode=full -f phase=auto -f shards=16
```

- Leave `run_date` empty so the Action uses the actual New York date. This is a live collector, not a historical backfill.
- Use `phase=auto` for honest capture clocks. `preopen`, `intraday` or `postclose` require every record to fit that window; a late run still captures data and reports the mismatch rather than silently skipping.
- `shards=16` splits work into 16 **sequential** batches. No ranking, ticker cap or cross-stock text deduplication is used.
- For the fast short-summary collection, change `mode=full` to `mode=export`.

For a GitHub API caller, POST to:

```text
https://api.github.com/repos/SRoyaltyy/fullscan/actions/workflows/finviz_learning_capture.yml/dispatches
```

```json
{"ref":"main","inputs":{"mode":"full","phase":"auto","shards":"16"}}
```

## What Grok should check

- Wait for the final `archive` job. Read its job summary and the saved `coverage.json`; do not report success from the initial export or a green intermediate job.
- Report `expected_tickers`, `export_statuses`, `full_statuses`, `full_text_complete`, `complete_for_requested_mode` and the capture time range.
- Full mode passes only when every expected ticker has a captured full panel or an explicit source statement that no digest exists, with valid phase timing. Missing panels, rate limits, access failures and unprocessed batches remain visible failures.
- If preflight fails, inspect `manifest.json` → `probe` / `browser_setup_error`. Full-panel handling is tested on fixtures; the first authenticated run must validate the site's current structure. Do not claim full coverage before this succeeds.
- A 403/429 stops the affected batch. Do not immediately redispatch it or bypass source limits.
- If publication to GitHub fails, the final artifact still keeps the output and the Action fails. Do not claim the archive landed without checking the commit.

## Saved files

Each run has a unique folder:

```text
data/finviz_learning/<NY-date>/<run-id>-<attempt>/
  manifest.json
  export.csv.gz
  baseline.jsonl.gz
  shards/*.jsonl.gz
  records.jsonl.gz
  coverage.json
  report.md
```

- The original live export is compressed without changing its bytes. The manifest stores its SHA-256 and capture clock.
- Each final record has the ticker, company/industry, original export text, full text when found, text hashes, source URL/field, capture clock, visible source timestamp if provided and a clear status.
- Headline and yfinance fallbacks are not used. Cached CSVs cannot become freshly dated source captures.
- Repeated texts retain every ticker association and every capture version. Source publication time can remain unknown; a live fetch is not proof of newly published news.
- These are learning observations, not verified causal explanations or advance price signals. Price snapshots need an aligned return window before testing a factor.

## Speed and resource use

- No paid news API or LLM is called. The Action uses the existing Elite account and GitHub runners.
- Full mode is not the speed of a bulk CSV. At five-second pacing, roughly 5,900 individual page visits alone have an eight-hour lower-bound start-time budget, plus browser/network/setup time. Sequential shards avoid a six-hour single-job ceiling; each batch has a bounded runtime and saves progress.
- GitHub-hosted minutes use your account's existing quota; daily full mode is not promised to fit a free private-repo allowance. Export mode is the low-resource path. The first full preflight stops early if the required source cannot be verified.

## Implementation and checks

- `src/finviz_learning_capture.py`: live export, strict full-panel parsing, authenticated browser expansion, capture batches, gzip checkpoints and coverage.
- `src/test_finviz_learning_capture.py`: offline source/coverage/failure contract tests.
- `.github/workflows/finviz_learning_capture.yml`: dispatch, artifacts, sequential batches, archive commit and coverage enforcement.
- Tests use synthetic source fixtures; they are not a completed authenticated production run. Grok's first dispatch supplies that live check.
- Finviz documentation explains the [short screener column](https://finviz.com/blog/news-column-and-stock-flags-now-in-the-finviz-screener/) and [click-through detail](https://finviz.com/blog/a-new-era-for-news-on-finviz/).
