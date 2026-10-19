# Judge prompt template

Two paste-ready prompts for the frontier-model steps of the loop. Fill the
`<...>` slots. Both prompts pin the rubric and the taxonomy so the judge
cannot recalibrate itself between rounds.

---

## Prompt A — judge a round (steps 3–4 of the loop)

You are scoring the output of a deterministic program that links news
articles to US stocks. Relatedness means topical closeness only — not
sentiment, not price direction. Apply this rubric exactly; do not invent
your own standards.

RUBRIC (summary — full version in eval/rubric.md):
- A stock "passes" only when the program marks pass:true. That is the only
  claim of relatedness.
- A name hit (ticker or company alias written in the article) is reliable
  evidence the article NAMES the company, but alone is not relatedness.
- A dependency (a name that appears in the stock's SEC filing only as a
  customer, supplier, collaborator, or competitor) is a SECOND-ORDER link.
  It must be marked order:2 / second_order:true and must never count as a
  pass. An unmarked or ungated dependency is a RELATION_LEAK failure.
- "unknown" is the correct answer for thin/generic articles. A forced guess
  fails; unknown does not.
- One article may legitimately pass many stocks and industries.

For each case below you receive: the article, the program's output for it,
and the case's expected block from the frozen corpus. Verdict rules:
- PASS only if every expectation holds. Any violation fails the case.
- FAIL requires exactly one primary failure class from this enum:
  ALIAS_FP (common word/pub/org name treated as ticker or alias),
  RELATION_LEAK (partner/competitor mention treated as topical, or dependency
  ungated/unmarked), GATE_MISS (right stock present but industry gate failed),
  SPELLING_MISS (defence/defense-type variant blocked a match), SENSE_CROSS
  (homonym crossed industries), THRESHOLD (right stock ranked but cut off),
  FALSE_PASS (wrong stock passed), FALSE_UNKNOWN (should have scored/named,
  came back unknown/absent), MISSING_NAME (expected name hit absent),
  SCHEMA_DRIFT (output lacks a field the case needs), NO_TEXT.
- Pick the class naming the FIRST stage that went wrong: alias detection ->
  term extraction -> industry gate -> thresholds -> output schema.
- If the corpus expectation itself looks wrong, say so in "challenge" — do
  not silently re-label. Frozen labels change only by human decision.
- If you can, also name the problem-taxonomy row (eval/problem-taxonomy.md,
  e.g. A1, B4, E2) that covers the failure.

CASES:
<paste: article text + program output + expected block, one per case>

Respond with one JSON object per case, exactly:
{"id": "...", "verdict": "PASS"|"FAIL", "failure_class": "...", "challenge": "...", "notes": "..."}

---

## Prompt B — label new sampled articles (before they enter the corpus)

You are labeling articles for a frozen evaluation corpus. The program under
evaluation decides which US stocks a news article is topically related to,
without using an LLM at read time.

For each article, produce:
- case_type: one of named_company | related_unnamed | homonym_trap |
  thin_unknown | second_order | off_board_name | alias_trap (definitions
  below)
- expected block:
  - status: "scored" or "unknown" (unknown = too thin/generic to link)
  - topical_passes: tickers the article IS genuinely about (industry
    context + company specifics both present, or the company is the clear
    subject). Be strict: a single tangential mention is not relatedness.
  - must_not_pass: tickers that share words with the article but are NOT
    what it is about (the homonym/partner/competitor traps).
  - name_hits: tickers/companies literally named (ticker or full name).
  - must_not_name: ticker-shaped strings that are NOT the company (e.g.
    "Washington Post" vs ticker POST).
  - allowed_second_order: [{ticker, partner}] only when the article is
    about a company's partner/customer/supplier/competitor and the link to
    this stock runs through the filing relation, not the business.
- labels: author "<model>", human_checked false (a human must review before
  the case is frozen).

Definitions:
- related_unnamed: the article is about a company's world (its industry
  event, its regulators, its inputs/outputs) without naming it. Ask: "would
  a trader reading this want to know this stock exists?" If yes → it belongs
  in topical_passes.
- second_order: the article names party A; this stock's filing names party A
  as customer/supplier/collaborator/competitor. That is real but weaker —
  it is a dependency, never a pass.
- When torn between "related" and "not related", choose "not related" and
  explain in notes. A corpus that over-claims relatedness teaches the loop
  to over-link.

ARTICLES:
<paste: sampled articles from src/eval_sample.py output>

Respond with one JSON object per article matching the case schema in
eval/README.md, ready to append to eval/cases.jsonl.
