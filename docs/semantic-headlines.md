# Model-backed headline analysis pilot

`SemanticHeadlineClient` calls the existing Lane provider router at its
`news_classify` quality floor. It emits model-produced action records, stages,
sectors and primary/additional Lane event classes from titles. It never imports
the deterministic recognizer, its answers, benchmark reference labels or
assistant corrections. This is an opt-in semantic analysis stage; the existing
free news collector remains an ingestion path.

The same returned JSON preserves actor/action evidence, public-authority status,
concrete-action status, stage, jurisdiction, sector activity/basis, unknowns and
provider/model identity. Python rejects malformed output, nonliteral evidence
and contradictory stage/concrete flags. One schema repair per provider is
bounded. Provider failure remains `unavailable`; it is never converted to
“no action”, `discard` or fabricated sector answers. Provider fallback uses the
existing classification allowlist and payment policy. Tokens, URLs, headers and
raw transport errors are not saved in the results.

The standalone runner receives only numbered titles. It checkpoints raw model
outputs after every title and records unavailable results. Initial validation
uses the first 20 titles of the previously frozen new 100-title holdout, in its
original random order. The judge/reference is evaluated separately after the
workflow completes; it is not present in the runtime input fixture. This sample
has already been inspected during deterministic evaluation, so it is a pilot
comparison, not a fresh acceptance holdout for this new prompt.

The GitHub-hosted pilot reuses repository secrets for direct API providers;
OpenClaw is omitted there because the existing workflow documents that it is
reachable from ECS only. Default standalone routing can still try the existing
OpenClaw classification model when executed on ECS. No provider/model is
claimed to work merely because a credential is configured: actual successful
responses, unavailable outputs and HTTP error codes are preserved.

Run on a host with the configured provider environment:

```
PYTHONPATH=. python scripts/run_semantic_headlines.py \
  --input tests/fixtures/semantic_headline_inputs.json \
  --output semantic_results/program_outputs.json --limit 20
```

Tests validate the integration contracts using synthetic provider responses;
they do not measure model accuracy. The live workflow supplies that evidence.

The 20-title pilot fixture was independently checked against its existing
public `01_daily/news` archive at commit a62a3cc16b0a7303a7c4fa1b855566f75f23bd33.
The payload contains only those already-public headlines and ordinal numbers,
not article bodies, private profiles or reference assessments.

The existing annotation entry point also supports explicit semantic execution:

```python
from src.news_impact.headline_analysis import analyse_headline
from src.news_impact.semantic_headlines import SemanticHeadlineClient
client = SemanticHeadlineClient()
annotations = analyse_headline(title, semantic_client=client)
```

This replaces all three headline annotations with model-produced values for that
call. An unavailable model result remains unavailable; it does not silently
fall back to rules. Raw `semantic_analysis` and provider identity are retained.
The default free collector remains deterministic until a caller opts in.

Initial live pilot: all 20 responses passed validation. Successful providers
were Gemini 2.5 Flash (5) and the existing Gemini 3.5 Flash Lite fallback (15).
Against the frozen assistant reference: action agreement 15/19, recall 3/4,
Lane agreement 11/20, sector coverage 13/16 and exact coarse sectors 11/16.
The rules path on the same titles scored 14/19 actions, recall 0/4, 13/20 Lane
and 4/16 exact sectors. These are small-sample pilot results, not acceptance.
Observed 403/429 provider errors and model fallback show why configured keys
alone do not establish provider availability or consistent model quality.
No prompt/reference corrections were made after observing these outputs.
