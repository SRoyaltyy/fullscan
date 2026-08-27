# Pipeline health — postclose / before

date=2026-08-28  source=2026-08-27  target=2026-08-28
generated 2026-08-27T12:43:01.580909-04:00
result=FAIL  required_fails=3  warns=0

## How to read this

- **Door / bugs**: OpenClaw must PONG with the **live 48-char json token**, not the stale 64-char GitHub secret. Model must be Grok. GROK_ONLY must be on or we fall into SearXNG/DeepSeek.
- **Prereqs**: files a *previous* workflow already had to write. Pre-open on date D needs post-close files **dated D** (written the night before) plus the 05:40 GH Finviz scrape.
- **Outputs**: files *this* job is supposed to produce. `SKIP` means `--phase before` (job has not run yet).

## 1. Door

| status | check | required | detail | path |
| --- | --- | --- | --- | --- |
| OK | HOME for OpenClaw/git | yes | /home/gha | `` |
| OK | OpenClaw port 18789 | yes | http://127.0.0.1:18789 listening | `` |
| FAIL | OpenClaw PONG (Grok) | yes | http=200 err=Hey. I just came online.

Who am I? Who are you?

I | `` |
| OK | chatCompletions.enabled | yes | /home/gha/.openclaw/openclaw.json enabled=True | `` |
| OK | Default model is Grok | no | default={'primary': 'xai/grok-4.6'} | `` |

## 1b. Known bugs

| status | check | required | detail | path |
| --- | --- | --- | --- | --- |
| OK | Grok/OpenClaw token 48 vs 64 | yes | live_len=48 tail=864c | `` |
| OK | Chat model is Grok not DeepSeek | yes | model=openclaw/default | `` |
| OK | GROK_ONLY (no DeepSeek analysis) | yes | GROK_ONLY on — DeepSeek must not write essays | `` |
| OK | SearXNG is not the research path | yes | Grok classroom answered; native web/X should be used. SEARXNG_URL=set (fallback only) | `` |
| FAIL | xAI OAuth not expired | yes | openclaw models status: xAI token expiring/expired | `` |

## 2. Prerequisites (inputs from earlier jobs)

| status | check | required | detail | path |
| --- | --- | --- | --- | --- |
| OK | Not colliding with pre-open | yes | preopen=failed | `` |
| FAIL | ECS Finviz Elite HTML (groups/tape) | yes | HTTP 403 cloudflare/403 (ECS Elite HTML is blocked) — post-close `map_heat --force` scrapes on ECS | `` |
| OK | yfinance import (market reaction) | no | installed | `` |
| OK | Captain research prompt on disk | yes | ok | `/home/gha/actions-runner/_work/fullscan/fullscan/00_grounding/map_heat_research_prompt.md` |

## 3. Expected outputs

| status | check | required | detail | path |
| --- | --- | --- | --- | --- |
| SKIP | 2026-08-28_map_heat.json (groups+captains+tape) | yes | not written yet (phase=before) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-28_map_heat.json` |
| SKIP | 2026-08-28_map_heat.md | no | not written yet (phase=before) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-28_map_heat.md` |
| SKIP | 2026-08-28_research_baseline.json (captain cards) | yes | not written yet (phase=before) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-28_research_baseline.json` |
| SKIP | 2026-08-28_research_baseline.md | yes | not written yet (phase=before) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/map_heat/2026-08-28_research_baseline.md` |
| SKIP | post-close synthesis transcript | no | not written yet (phase=before) | `/home/gha/actions-runner/_work/fullscan/fullscan/01_daily/_transcripts/2026-08-28_map_postclose_synthesis.json` |

## Contract (what each job owes the next)

### Post-close (22:00 ET, night of SOURCE, files dated TARGET)

| file | required | consumed by |
| --- | --- | --- |
| `01_daily/map_heat/{TARGET}_map_heat.json` | yes | pre-open overlay + morning refresh |
| `01_daily/map_heat/{TARGET}_map_heat.md` | no | humans |
| `01_daily/map_heat/{TARGET}_research_baseline.json` | yes | morning refresh; missing → bootstrap, s_heat=0 |
| `01_daily/map_heat/{TARGET}_research_baseline.md` | yes | humans / QC |

### GH Finviz scrape (05:40 ET, date = TARGET/today)

| file | required | consumed by |
| --- | --- | --- |
| `01_daily/news/{DATE}_finviz_digest.json` | yes | news parse / digest layer |
| `01_daily/news/{DATE}_finviz_digest.md` | no | humans |
| overlay on `{DATE}_map_heat.json` (`overlay_at`, `tape`, `econ`, `earnings`) | yes | morning refresh; empty tape fails the day |

### Pre-open ALL (05:55 ET, date = today ET)

| file | required |
| --- | --- |
| `01_daily/news/{DATE}_parsed.json` | yes |
| `01_daily/events/{DATE}_events.json` (not carry) | yes |
| `01_daily/news/{DATE}_judge.md` | yes |
| `01_daily/news/{DATE}_actions.json` | no (WARN) |
| `01_daily/map_heat/{DATE}_research.json` + `_research.md` | yes |
| `01_daily/catalyst/{DATE}_dossiers.json` | no |
| `01_daily/general/{DATE}_predict.md` | yes |
| `01_daily/sectors/{DATE}/<11 slugs>_predict.md` (≥8 quality) | yes |
| `01_daily/sectors/{DATE}/_board.json` | no |
| `01_daily/{DATE}_preopen_qc.json` | yes |
| `01_daily/{DATE}_preopen_status.json` | yes |
| `01_daily/{DATE}_grok_review.md` | yes |

