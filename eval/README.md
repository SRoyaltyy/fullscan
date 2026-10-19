# Evaluation harness for the article ↔ stock correlation

This package turns the build loop into a measured loop. The scorer changes
every round; this corpus, rubric, and taxonomy do not.

## The loop, with the harness inserted

1. **Change the program** (Cursor or by hand) targeting a named failure
   class from the taxonomy — not a named word.
2. **Run the frozen corpus:**
   ```
   PYTHONPATH=. python3 -m src.eval_correlation \
       --cases eval/cases.jsonl \
       --descriptions excel_bot/data/finviz_with_descriptions.csv \
       --universe /path/to/company_filings_full_dates.csv \
       --filings /path/to/business.json \
       --reference data/news_intake/<latest>/documents.json \
       --changelog "alias guard: wordlist + publisher gazetteer" \
       --out eval/results/round-NNN.json
   ```
   (No filings on this machine? Score with the normal CLI and evaluate the
   output instead: `--links dashboard/news-dir/correlation/latest.json`.)
3. **Diff against the previous round:**
   ```
   PYTHONPATH=. python3 -m src.eval_diff eval/results/round-NNN-1.json eval/results/round-NNN.json
   ```
   Exit code 1 = a regression flipped or a target was lost. That gate is
   the whole point: the diff, not the vibe, decides whether the round kept.
4. **Sample new articles adversarially** (only when the corpus needs fresh
   cases, not every round):
   ```
   PYTHONPATH=. python3 -m src.eval_sample --news data/news_intake/<date>/documents.json \
       --budget 30 --out eval/pool/<date>.jsonl
   ```
5. **Label the sample** with judge Prompt B in `eval/judge_prompt.md`, a
   frontier model drafts labels, **a human spot-checks them**, then append
   the rows to `eval/cases.jsonl`. Every bug found in any round becomes a
   case here before the round that fixes it is accepted.

## File map

| path | role |
|---|---|
| `eval/rubric.md` | Frozen definition of correct + failure taxonomy (FT-1) |
| `eval/problem-taxonomy.md` | Prospective problem categories (A–G) with countermeasures and status; new failures find a home here first |
| `eval/judge_prompt.md` | Paste-ready prompts: A judges a round, B labels new samples |
| `eval/cases.jsonl` | The frozen corpus (13 seed cases; grow, never rewrite) |
| `eval/results/` | One JSON per round; diff any two |
| `src/eval_correlation.py` | Runner + pure comparator |
| `src/eval_diff.py` | Round diff; exit 1 on regression |
| `src/eval_sample.py` | Stratified adversarial sampler |
| `src/test_eval_harness.py` | Harness self-tests (no filings needed) |

## Case schema (one JSON object per line in `eval/cases.jsonl`)

```jsonc
{
  "id": "dkng-second-order-gating",   // stable, never reused
  "kind": "regression" | "target",    // regression = must keep passing;
                                      // target = desired, not yet implemented
  "case_type": "named_company | related_unnamed | homonym_trap | thin_unknown
               | second_order | off_board_name | alias_trap",
  "title": "...", "text": "...",      // full text preferred; see text_status
  "text_status": "full | excerpt",
  "source_url": "...", "date": "...",
  "expected": {
    "status": "scored | unknown",     // optional
    "topical_passes": ["RKLB"],       // must have pass:true
    "must_not_pass": ["AAL"],         // must not have pass:true
    "name_hits": ["DKNG"],            // must have name_hit:true
    "must_not_name": ["POST"],        // must not have name_hit:true
    "no_topical_passes": true,        // nothing may pass
    "no_name_hits": true,             // nothing may be flagged as a name
    "allowed_second_order": [{"ticker": "PLBY", "partner": "DraftKings"}],
                                      // deps allowed ONLY marked order>=2
    "forbidden_dependencies": ["CHDN"]
  },
  "labels": {"author": "", "model": "", "human_checked": false,
             "version": 1, "created": "YYYY-MM-DD", "note": ""}
}
```

## Corpus rules (these are what keep the loop honest)

- A case fails if **any** expectation is violated; it passes only if **all**
  hold. Partial credit within a case is not a thing.
- `kind: target` cases exist so in-progress fixes don't pollute the
  regression count. A round that takes a target from TARGET_FAIL to
  TARGET_ACHIEVED is real progress even if nothing else moved.
- **Never rewrite or delete old labels.** If the definition of correct
  changes, bump `TAXONOMY_VERSION`, bump the case's `labels.version`, and
  write the reason in the round's `--changelog`.
- The corpus is not frozen until every case has `human_checked: true`.
- Adding cases is always allowed and encouraged; each round's newly found
  failures go in before the fix that addresses them is merged.

## Notes and known limitations

- The corpus is scored as a batch of under eight articles, so the program's
  batch-z behavior (which only activates at eight or more) is not exercised
  here. Either accept that, or pass `--reference` with the day's full news
  and treat batch-z calibration as a separate, later exercise.
- `okx-ice-tokenized`, `pentagon-defence-firms`, `barrons-prediction-markets`
  are excerpt placeholders: paste full text from the scanner files, then
  lock expectations per the notes inside each case.
- Off-board name hits (LMT/RTX named but excluded by the ATR screen) have no
  program field yet. When the schema grows one, add
  `"off_board_mentions": ["LMT","RTX"]` to `expected` and teach the
  comparator — until then these are tracked in case notes, not asserted.
- Run the harness self-tests with
  `PYTHONPATH=. python3 -m unittest src.test_eval_harness`.
