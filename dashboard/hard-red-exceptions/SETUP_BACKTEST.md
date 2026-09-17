# Open + camera setup backtest

Long near 50% H1 is not an edge. Shorts are graded as short P&L (− stored long hold). Thin n is said out loud.

Scanned **1247** ticker histories (`dashboard/hard-red-exceptions/t/*.json`) · 2026-08-13 → 2026-09-16 · 29928 session rows · 8268 setups.

Rule (same as `setupOf` / `open_camera_setup`): 5 numeric opens including today; LONG if cheapest or 2nd-cheapest **and** camera net rose vs prior session; SHORT if richest or 2nd-richest **and** net fell. Clean = the other color did not deteriorate. Explore = net moved the right way but positives/negatives moved against the side. No badge without a 5-open window and a prior camera session.

Short P&L flips the stored long hold-1/3/5. KEEP still wants >55% after fees and n≥30. Sample is large enough to read the rates; still not a live wire.

## ALL days

| sleeve | n setups | H1 win / mean | H3 win / mean | H5 win / mean |
|---|---:|---|---|---|
| long clean | 4430 | 48.1% / +0.08% (n=3886) | 45.9% / -0.25% (n=3440) | 42.9% / -0.71% (n=2746) |
| long explore | 652 | 46.9% / -0.09% (n=652) | 39.7% / -1.39% (n=642) | 37.8% / -1.88% (n=571) |
| long all | 5082 | 47.9% / +0.05% (n=4538) | 44.9% / -0.43% (n=4082) | 42.0% / -0.91% (n=3317) |
| short clean | 2736 | 55.5% / +0.64% (n=2718) | 56.1% / +0.82% (n=2398) | 61.6% / +1.66% (n=2321) |
| short explore | 450 | 61.1% / +1.10% (n=450) | 70.7% / +2.62% (n=443) | 69.9% / +3.00% (n=439) |
| short all | 3186 | 56.3% / +0.71% (n=3168) | 58.4% / +1.10% (n=2841) | 62.9% / +1.88% (n=2760) |
| tighter long (lowest + clean + net ≥ +2) | 1892 | 49.6% / +0.35% (n=1575) | 48.6% / +0.20% (n=1419) | 45.6% / -0.24% (n=1168) |

## HARD-RED-ONLY days

| sleeve | n setups | H1 win / mean | H3 win / mean | H5 win / mean |
|---|---:|---|---|---|
| long clean | 1784 | 47.6% / +0.00% (n=1783) | 52.5% / +0.73% (n=1337) | 42.8% / -0.43% (n=1076) |
| long explore | 145 | 35.9% / -1.69% (n=145) | 48.9% / -1.20% (n=135) | 30.7% / -3.03% (n=114) |
| long all | 1929 | 46.7% / -0.12% (n=1928) | 52.2% / +0.56% (n=1472) | 41.6% / -0.68% (n=1190) |
| short clean | 1981 | 54.8% / +0.57% (n=1981) | 55.5% / +0.77% (n=1661) | 65.1% / +2.45% (n=1626) |
| short explore | 171 | 62.0% / +1.45% (n=171) | 69.5% / +2.32% (n=164) | 78.4% / +4.78% (n=162) |
| short all | 2152 | 55.4% / +0.64% (n=2152) | 56.7% / +0.91% (n=1825) | 66.3% / +2.66% (n=1788) |
| tighter long (lowest + clean + net ≥ +2) | 677 | 48.9% / +0.24% (n=677) | 58.2% / +1.40% (n=521) | 44.0% / -0.12% (n=425) |

## Honesty

- Do not dress up a long H1 near 50% as edge.
- Prior recon (~1247 histories) was long ~49% H1, short ~56% H1 (explore short ~60% H1; hard-red shorts H5 ~63.9% +2.12). This scan is the same 1247 files through 2026-09-16: long all H1 47.9% (still a coin-flip, a point worse), short all H1 56.3% (unchanged), explore short H1 61.1% (unchanged), hard-red short H5 66.3% +2.66 (a couple points stronger — more hard-red sessions in the 9/8–9/15 tail). The table above is the source of truth.
- Tighter long is a cheap extra cut (lowest open, not 2nd; clean; net improved by ≥2), reported separately so it is not p-hacked into the main rule.

Dashboard: [hard-red-exceptions](./index.html).
