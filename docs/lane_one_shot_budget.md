# Lane one-shot 100 overnight budget

Full mode shares the single `ecs-openclaw` runner with Post-Close ALL, pipeline health, and the map-heat queue. The run is only allowed in the overnight window: after the evening post-close pack (usually 17:30–18:30 ET) and done, including merge, by **03:30 ET**, before Pre-Open around 04:00 ET.

Shards run one after another. Each shard's job cap is `(window − gold − merge) / 5`, not 240 minutes. The draw loop stops earlier than that cap, writes `shard_N.json`, and the upload step still runs. Merge runs when a shard succeeds, fails, or is cancelled, and the scoreboard lists per-shard coverage (`complete`, `partial`, or `missing`).

## Measured on run 35929449213

Gold fixtures step: **54 min** (23:36:08Z–00:30:08Z), then success.

Shard 4 draw step: **240 min** (00:30:32Z–04:30:35Z), then the job timeout cancelled it. The upload step did run and stored 171 per-article scratch files. `shard_4.json` was not among them, because that report was only written after the loop. Merge never saw a shard report. Shard 1 was cancelled by hand about three hours in. Shards 0, 2, and 3 never started.

Shard 4 finished **136 articles: 7 kept, 129 rejected** (keep rate 5.1%). One more article (AbbVie) was inside meta when the timeout hit.

Rejects: `lane_discard` 99, `q5_regime` 12, `lane_filter_missing` 8, `lane_analyst_missing` 5, `dividend_only` 2, `lane_meta_missing` 1, `listicle` 1, `reaction_title` 1.

Hops that returned JSON: classify 133, meta 22, filter 20, pack_complete 12, analyst 12. OpenClaw HTTP 200: **215**. No OpenClaw 429. Other providers returned 429 (mistral-small 18, zhipu about 22, gemini 2, openrouter 1). `hop_models` sleeps 2 seconds on each of those, about **1 minute total**.

The four hours are serial `lane::openclaw::xai/grok-4.6` round-trips: 215 calls in 240 minutes is about **67 seconds each**, or about **1.8 minutes per article**. At a 5.1% keep rate, 20 kept rows needs on the order of 390 draws and about **11 hours per shard**. Five serial shards do not fit before Pre-Open. The budget stops early and merges whatever was kept.

## Default for a dispatch at about 18:30 ET

| Piece | Minutes |
| --- | --- |
| Total budget (`run_budget_minutes`) | 540 (18:30 → 03:30 ET) |
| Hard stop (`deadline_et`) | 03:30 America/New_York |
| Gold reserve | 75 (measured 54, plus slack) |
| Merge reserve | 20 |
| Each shard job cap | 89 |
| Each shard draw loop | 81 (8 minutes left for checkout and upload) |

`75 + 89×5 + 20 = 540`. A later dispatch keeps the 03:30 ET stop, so the shard slice shrinks.

```bash
gh workflow run lane_one_shot_100.yml \
  --repo SRoyaltyy/fullscan \
  -f mode=full \
  -f target_per_shard=20 \
  -f max_draws_per_shard=150 \
  -f runner=ecs \
  -f llm_backend=grok \
  -f run_budget_minutes=540 \
  -f deadline_et=03:30 \
  -f gold_budget_minutes=75 \
  -f merge_budget_minutes=20
```

Classify stays OpenClaw on ECS with `xai/grok-4.6` and the full Meta/Analyst prompts. The gold gate is unchanged: tsa, buist, tsv, amrx, and naion must pass before any shard starts.
