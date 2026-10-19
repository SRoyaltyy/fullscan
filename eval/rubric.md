# Evaluation rubric — article ↔ stock correlation

This document is the frozen definition of "correct" for the correlation
program. Both the deterministic comparator (`src/eval_correlation.py`) and the
frontier-model judge (`eval/judge_prompt.md`) apply it. If the definition
changes, bump `TAXONOMY_VERSION` in `src/eval_correlation.py` and record the
change in the round results — never silently relabel old cases.

## Definitions

- **Topical pass** — the program returns a stock with `pass: true`. This is
  the only thing that counts as "the article is related to this stock".
- **Name hit** — the article literally writes the ticker (`$POST`, `NYSE:LMT`)
  or the company alias. Reliable, but by itself NOT relatedness: a name drop
  in a tangential sentence does not make the article about the company.
- **Second-order dependency** — the article mentions a name that appears in
  the stock's filing only as a customer, supplier, collaborator, or
  competitor. This is a typed relation, not topical relevance. It may be
  surfaced only when (a) it is explicitly marked `order: 2` /
  `second_order: true`, and (b) the article's industry gate matches the
  dependency's world (a gambling article surfaces Playboy's DraftKings link;
  a space-policy memo must surface none). A dependency is never a pass.
- **Unknown** — the correct output for thin or generic articles. Forcing a
  guess is a failure; returning unknown is not.
- **Off-board mention** — the article names a company that is not on the
  liquid universe board (e.g. LMT, RTX under the ATR screen). Desired
  behavior: reported as an off-board name hit, not silently dropped. (The
  program does not support this yet; treat as a known gap, not a per-case
  failure, until the schema grows the field.)

## Case types (the expected distribution of the world)

| case_type | meaning | example |
|---|---|---|
| `named_company` | article is about company X and names it | "$POST jumps on earnings" |
| `related_unnamed` | article is about X's world but never names X | the space-transportation policy memo → RKLB |
| `homonym_trap` | a word in the article lives in two industries; the link must not cross | "office space" articles must not pass Rocket Lab; "Washington Post" must not hit ticker POST |
| `thin_unknown` | too little text to decide; output must be unknown | "Markets moved." |
| `second_order` | the link runs through a filing relation, not the business itself | gambling article → PLBY's "collaborator: DraftKings" |
| `off_board_name` | article names a company the board excludes | Pentagon article naming BA, LMT, RTX |
| `alias_trap` | text contains a ticker-shaped string that is not the company | "Washington Post", publisher names, common-word tickers |

## Failure taxonomy (fixed enum)

The prospective mirror of this table — problems we expect **before** we see
them, each with its countermeasure and status — lives in
`eval/problem-taxonomy.md`. Every observed failure should find a home there;
a failure with no home will come back.

Every failure, whether caught by the comparator or flagged by the judge,
must be classified into exactly one primary class from this list. Counts of
these classes are what diff between rounds.

| class | meaning | project example |
|---|---|---|
| `ALIAS_FP` | a common word, publication, or org name treated as a ticker/alias | "Washington Post" → POST |
| `RELATION_LEAK` | a partner/competitor mention scored as topical, or a dependency returned without second-order marking/gating | Playboy passing on a gambling article because its filing mentions DraftKings |
| `GATE_MISS` | the right stock is present in the output but the industry gate failed it | defence-spelled title against defense-spelled filings (if that is the proximate cause; otherwise SPELLING_MISS) |
| `SPELLING_MISS` | a spelling variant (defence/defense, licence/license) blocked an otherwise valid match | "defence firms" |
| `SENSE_CROSS` | a homonym crossed industries | office-space REIT article ranking aerospace names |
| `THRESHOLD` | the right stock ranked above the wrong ones but the z/percentile cut rejected it | RKLB #1 but desc_z 2.3 < 2.5 |
| `FALSE_PASS` | a stock passed that must not pass | airline passing on the space memo |
| `FALSE_UNKNOWN` | the article should have scored or named a company but came back unknown / absent | memo about launch returning unknown |
| `MISSING_NAME` | an expected ticker/alias name hit is absent | article says "Lockheed Martin", no LMT row |
| `SCHEMA_DRIFT` | the program output lacks a field the case needs | dependency rows without an `order` field (pre-gating schema) |
| `NO_TEXT` | the case has no scorable text | excerpt placeholder not yet filled |

Auto classes (`SCHEMA_DRIFT`, `NO_TEXT`) are added by the comparator; the
rest are primary classifications. When a judge is unsure between two
classes, pick the one that names the *first* stage that went wrong
(pipeline order: alias detection → term extraction → industry gate →
thresholds → output schema).

## Rules for judging

1. Relatedness is **closeness, not sentiment, not direction**. "Rate hike"
   and "rate cut" are both near banks.
2. Dependencies are never passes. Ungated dependencies are RELATION_LEAK.
3. One article may pass many stocks and many industries; that is not by
   itself an error. The case's `expected` block is the verdict source.
4. Unknown is a legitimate answer for thin articles. Guessing is the error.
5. A case fails if ANY expectation is violated. A case passes only if ALL
   expectations hold. Do not average partial correctness within a case.
6. New failures found in a round become new labeled cases in the next
   round. Old labels are never rewritten; if the definition of correct
   changes, version the corpus.

## Corpus rules

- Every case carries `labels.human_checked`. The corpus is not frozen until
  every case is human-checked.
- `kind: regression` — must keep passing; a failure here blocks the round.
- `kind: target` — desired behavior not yet implemented; tracked separately
  so in-progress fixes do not pollute the regression count.
- Adding a case is always allowed. Removing or rewriting expectations is a
  versioned event recorded in the case's `labels.version` and the round
  result's `changelog`.
