# V15 developmental candidate

Acceptance remains 0/10. V14 on Grok 4.3 completed all 100 development titles: 92% action agreement, 80% action precision, 80% action recall, 88% Lane agreement and 79% exact sector matching. The separate strict GLM retry (run 38677069775) was rate-limited through three bounded attempts and completed no analyses; that is not a semantic accuracy measurement.

V15 scans every clause and current institutional instrument before deciding that a title has no public act. It separates new public acts from private accreditation, parties addressing a court, nonreceipt of support and background policies. Lane instructions keep actual public legal/administrative procedures in scope. Sector extraction starts with explicit operating activities and counterparties, independently of action status or headline relevance.

A fixed CC0 Wikidata snapshot adds 16,711 business identities to the existing company catalog. It contains names, business descriptions and natural industry facts, with no eleven-sector, Lane or action answers. The query uses broad business types and no benchmark input. Only matching name candidates reach the model, which still decides identity, relevance and sector. Conservative name matching protects against ordinary-word and namesake matches. The snapshot is incomplete.

The committed snapshot is losslessly compressed with zero gzip timestamp. Its source JSON hash is 1bbf0d62ecb1b3583305d86440be71662a865b9318775a7750a5d5b44af350ac; gzip hash is 6fee2e77925477bc122220ff739adec5bea9272b379be3dbcf611650c3ecc38b. The composite catalog fingerprint covers both source catalogs. Resume also checks the effective OpenClaw agent and HTTP timeout, alongside the existing input/runtime/configuration/catalog checks.

The development replay uses only xai/grok-4.3, a 300-second provider timeout and 650-second per-batch deadline. Readiness must succeed for that exact requested model and production must be idle. Normal execution remains two independent prompts per ten titles; schema/evidence recovery can add calls. No billed token-savings claim is made.

Validation: 77 semantic contracts and 5 provider repair tests pass locally; workflow YAML, shell and embedded Python syntax validate. CI passed all 77 semantic tests, 5 provider tests and 23 workflow contracts. The first V15 attempt requested Grok 4.6, but its health probe returned an onboarding response and the gate skipped headline calls. Grok 4.3 passed; the next full development score is pending. Changes have not earned acceptance credit.

Batches 01 and 02 remain development-only. References for untouched 03–08 are frozen. Inputs 09–12 are unrun and still need independent references frozen before any execution. Batches 11–12 replace the two failed batches so 03–12 can provide ten fresh batches if every one meets all five 90% thresholds. Runtime, configuration and catalog must remain fixed across the streak.
