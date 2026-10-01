# Lane JEV completion checkpoint

Recovered prior records from codex/jev-lane-hop0. Preserved all failed rounds and the teacher audit.

V8 rounds 42–51 passed six of ten; ending streak one. Exposed data was used only for development.

V9 froze the eligibility cutoff at 0.17 before acceptance_06_gold.json was frozen at 2026-10-01T13:55:04.316854+00:00. Protocol: 50dbd93e96b4f02d08cb695434f217c1006e7dbd6909d80fdabfd69125ff7292.

Fresh acceptance rounds 52–61: all ten passed both class recalls strictly above 80%, with zero API errors or reviews. Useful recall minimum 82.35%; trash rejection minimum 83.78%. All 1000 labels were frozen before JEV predictions. Gold represents independent assistant judgments under the locked Lane contract, not objectively verified truth. Headline-only; event novelty unverified.

Every completed round is committed separately. The live trainer reads and dispatches codex/jev-lane-hop0 so manual grading uses the benchmark candidate. Scheduled trading pipelines are unchanged. Original responses, paired comparisons, blind regrading evidence, development replays and teacher audit are published in dashboard/jev-train.
