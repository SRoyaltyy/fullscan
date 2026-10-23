# Preflight preopen_all 2026-10-23

Checked 2026-10-23 05:52 ET, run 39933242377

**STOPPED** before writing. 5 item(s) failed:

- ❌ **ecs_code_current**: ecs code stale: code on this ECS checkout (src, scripts, config, .github) must match GitHub main (ecs code stale: HEAD 1228bce3 vs main e5af68ed differ in 2 code file(s): .github/workflows/scanner-backtest-publish.yml, .github/workflows/scanner-keyword-publish.yml)
- ❌ **runner_is_ecs**: Pre-Open is not on the ECS box (GitHub's own servers cannot reach Grok) (runner 'GitHub Actions 1000037978' is GitHub-hosted)
- ❌ **backend_grok**: LLM_BACKEND is not grok (LLM_BACKEND=deepseek)
- ❌ **gateway_up**: OpenClaw gateway 127.0.0.1:18789 did not answer within 5s (http://127.0.0.1:18789/health: URLError: <urlopen error [Errno 111] Connection refused>)
- ❌ **xai_token_outlives_run**: xAI login expires before this run can finish (needs >= 75 min left) (xAI login has unknown left, need 75)

- ✅ no_unapproved_paid_fallback: LLM_FALLBACK_MODEL=(none)
- ✅ no_twin_preopen: no twin
- ✅ read_cap_fits: OPENCLAW_PREDICT_READ_CAP_S=300, need >= 300
