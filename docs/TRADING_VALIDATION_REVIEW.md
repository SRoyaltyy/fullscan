# Trading validation and learning repair

Audit base: `d15894cb6f37f252826e0048603917fc0171c70a`. Historical replay: August 13–September 16, 2026, 24 sessions, 1,780 candidate rows, 270 recipes. This change is a research repair, not approval to trade or a claim that profitability has been demonstrated.

## What the repository is doing

The repository combines ingestion, generated market commentary, candidate selection, hypothetical portfolios and static dashboards. `src/stock_book.py` produces horizon-specific ranked candidates; `src/book_learn.py` fits weights and trading knobs from historical outcomes. `learn_cycle`, `promote_lessons` and `lesson_select` turn recurring written lessons into future prompt context. Those are different learning mechanisms and need separate evaluation.

`src/factor_mine.py` builds the retrospective candidate panel and recipe grid. `factor_mine_book.py` simulates each stateful account; `factor_mine_combo.py` builds shared-cash or independent-sleeve combinations. `factor_mine_sim.js` supplies browser replay, while the dashboard template also displays precomputed results. `open_bell_slip.py` is a separate one-share execution diagnostic; it was not a faithful replacement for portfolio fills. `data/` supplies cached inputs, `01_daily/` generated daily research, `02_lessons/` written hypotheses, `03_scoreboard/` derived reports, and `dashboard/` generated publication surfaces. Workflows repeatedly refresh these layers. A regenerated historical file is not evidence that its contents were available before the original trade.

## Implemented changes

- Stateful entry and exit execution callbacks, partial fills, missed fills and quantity caps, incorporating the useful implementation from PR #253. Costs now flow through the portfolio that actually holds the shares.
- Aggregate exposure and short-collateral limits, including shared-cash combinations. Short proceeds no longer become unrestricted long buying power. Dated pre-open locates are required by default; assumed availability is a separately labelled research scenario.
- Calendar-day borrow accrual, including weekends, and full-precision ledger prices. Independent reconciliation now handles borrow-only days and partial combo exits.
- Matching conservative defaults in browser simulation; morning-only stop semantics and precomputed combo limitations disclosed.
- Negative-dollar-P&L diagnostics cannot receive a KEEP verdict. One-share diagnostics never authorize strategy deployment, even with a positive hit rate.
- Learner outcome labels use the actual opening entry and requested horizon, with immature sessions excluded. Close-only data cannot silently stand in for an opening entry.
- Immutable observed-at snapshots and versioned proposals. Historical reconstruction keeps its real observation time and cannot retroactively qualify as a pre-open input.
- Lesson execution hooks from PR #250, behind a code-matched prospective gate. Historical trigger returns remain association diagnostics. Unvalidated market prose no longer becomes an active trading instruction.
- Paired shadow portfolio evaluation, engine/fee hashes and a fixed first-candidate, first-252-session gate with 21-session bootstrap blocks. Later candidates cannot win by fishing across the same holdout. A failed first trial stays shadow; restarting requires a deliberately designed fresh evaluation, not extending the window until it passes.
- Quote-time execution replay with displayed-size depletion, plus transparent constant-basis-point sensitivity scenarios. Quote replay still requires broker calibration and demonstrated production-ranker parity before observations can authorize promotion.
- A reproducible 270-recipe validation report, dashboard and read-only CI/manual research workflow.

## Results

All 1,080 scenario portfolio audits passed. These are accounting checks, not validation of signal quality or historical availability.

| Recipe | Strict 10bp adverse fills | Strict 50bp adverse fills | Assumed borrow, 10bp |
|---|---:|---:|---:|
| union_hot_n4_h1 (hot4) | +26.36% | +16.31% | +26.36% |
| combo_sh_5050_shared | +15.11% | +9.45% | +20.69% |
| short_news_r_h3 | 0.00% | 0.00% | +7.62% |

Strict short results are zero because this archive has no qualifying locates, not because an executable short book broke even. Basis-point costs apply adversely on both entry and exit; they are assumptions, not measured latency. Results also reflect corrected allocation/financing and conservative missing-regime handling, so differences from the old dashboard cannot all be attributed to slippage.

None of the 270 recipes was positive from every tested cash-start date. `union_hot_n4_h1` (hot4) and the shared combo were positive from 21 of 24 starts, with a worst start of approximately -1.13%. Some union-news variants had no negative starts but had zero-return starts. The full table is in `03_scoreboard/validation/STRATEGY_VALIDATION.md`; machine-readable scenarios, start windows and prerequisites are in the adjacent compressed JSON (`strategy_validation.json.gz`).

The historical train-select-test exercise is more sobering: the four selected next-window returns were -27.19%, +1.61%, -1.37% and +0.97%. The grid itself was devised retrospectively, so even this is not untouched prospective validation.

A decreasing book-to-end curve is not proof of making money in every market. Every point shares much of the same future price path. A few common winners can lift many starting dates; cash-only starts can make a curve look safe without demonstrating an edge. Compare independent forward increments, exposure, drawdown, turnover and concentration as well as this descriptive curve.

## Review of the new PRs

| PR | Effectiveness and disposition |
|---|---|
| [#250](https://github.com/SRoyaltyy/fullscan/pull/250) | Useful executable lesson wiring. Its original triggered-name vs universe comparison is not a causal test of ranking uplift, and immediate activation is premature. Incorporated with shadow gating and separate prospective paired evaluation. |
| [#253](https://github.com/SRoyaltyy/fullscan/pull/253) | Useful repair of the overlay/portfolio mismatch. Incorporated core callbacks and diagnostics, with additional financing, borrow, partial-exit and precision repairs. Daily OHLC-based fill scenarios are stress assumptions, not measured execution. Its reported market-mid result already undermined the idealized leader. |
| [#255](https://github.com/SRoyaltyy/fullscan/pull/255) | Presentation follow-up carrying #253 implementation. Makes results easier to inspect but does not provide independent evidence. Avoid treating it as a second confirmation. |
| [#252](https://github.com/SRoyaltyy/fullscan/pull/252) | Crash filter had only six blocked out-of-sample examples; reported mean was about -1.3001% versus -1.3005%, essentially unchanged. Repeatedly consulting the holdout to select variants contaminates it; missing features also pass many observations. Keep experimental. |
| [#254](https://github.com/SRoyaltyy/fullscan/pull/254) | Roughly 4,600 variants searched. The highlighted fade-short had eight discovery and 21 holdout examples. This is insufficient after such a search, especially without borrow evidence or intraday first-touch ordering. Keep experimental. |
| [#267](https://github.com/SRoyaltyy/fullscan/pull/267) | Useful negative evidence: 407 combinations, approximately 1.08 million name-days and zero KEEP results at its stated threshold. Fixed 15bp and hit-rate screening do not replace actual fees, sizing and net profit validation. Already merged at review. |
| [#246](https://github.com/SRoyaltyy/fullscan/pull/246) | Useful quote freshness/display repair; a later quote must not be represented as the opening fill. It does not establish trading edge. |
| [#258](https://github.com/SRoyaltyy/fullscan/pull/258) | Hold-mark work substantially overlaps merged #263; reconcile overlap before merging. Marks improve observability, not strategy validation. |
| #260–266 | Research/camera reconstruction and coverage can improve analysis, but reconstructed material is hindsight until supported by an actual pre-decision archive. |

The reviewed PR source snapshots are those available during this audit; concurrent new commits require incremental review. This branch does not merge those PRs or deploy trading changes.

## Remaining evidence requirements

There is no certified strategy and no promise of recursive profitability. This repair separates discovery from authorization; it does not manufacture absent evidence.

1. Collect immutable pre-decision feature snapshots continuously. Existing 1,780 historical rows do not satisfy the availability contract. Late morning rebuilds correctly fail it.
2. Supply timestamped bid/ask and displayed size, actual order acknowledgments/fills, and dated borrow inventory/rates. Daily candles cannot resolve latency, queue position, same-bar stop/target order, recalls or market impact.
3. Establish exact production-ranker/replay parity before setting a calibration record. The shadow evaluator currently uses five equal horizon sleeves and omits some production persistence/bookmark adjustments. It is deliberately unverified by default; calibration must bind the engine hash and include broker-comparison and parity-report hashes. A config flag is not itself proof.
4. Accumulate new untouched sessions. The first trial's fixed 252-session gate is conservative, not a guarantee or a universal statistical optimum. Bootstrap blocks are dependence approximations, not truly independent regimes. Multiple lesson families and later successive tests still require a broader error-budget policy before unattended deployment.
5. Add broker-specific financing, recalls, dividends, settlement and forced-liquidation rules where applicable, plus ongoing drift monitoring and rollback. These are not fully modeled here. The generic collateral constraints are conservative accounting assumptions, not a broker margin agreement.
6. Re-evaluate failed or stale proposals only through a newly precommitted forward experiment. Do not reset failed observations or backfill an execution-validation flag onto old shadow observations.

Useful candidates for further *forward testing* include the union-news families and hot4, but their ranking in this already-seen window is selection-biased. Investigate profit concentration and failure regimes before expanding the recipe grid.

## Reproduce

```sh
python -m pytest -q src/test_research_validation.py src/test_lesson_exec.py src/test_open_bell_slip.py src/test_book_fill_reality.py
node --check src/factor_mine_sim.js
python -m src.strategy_validation --starts all
python -m src.learning_replay --date YYYY-MM-DD
```

The manual Research validation workflow uploads reports without publishing or modifying policies. Existing production runners collect/evaluate shadow observations, but unverified observations cannot promote a proposal. The static factor-mine warning is applied when that dashboard is next rebuilt; the published site remains unchanged until these changes are accepted and deployed.

## Verification scope

127 distinct regression tests passed across the four focused modules and the available factor-mine tests. JavaScript syntax and Python compilation checks passed. All 1,080 scenario accounting audits passed on the archived panel.

Seven existing factor-mine tests require historical fixtures outside this partial checkout: the two morning-score fallback cases, closed-session morning coverage, explicit live-panel end, completed-predict calendar coverage, the August PSEC/ATAT reconstruction, and the INO earnings export case. The broad run failed these fixture-dependent checks; the subsequent source regression run explicitly deselected them. This is not a claim that the entire repository test suite passed. CI covers the focused regression modules; full historical fixture integration remains a review gate.
