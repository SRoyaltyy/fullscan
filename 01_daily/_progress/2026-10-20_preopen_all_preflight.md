# Preflight preopen_all 2026-10-20

Checked 2026-10-20 04:40 ET, run 39392653949

**STOPPED** before writing. 1 item(s) failed:

- ❌ **ecs_code_current**: ecs code stale: code on this ECS checkout (src, scripts, config, .github) must match GitHub main (ecs code stale: HEAD 422ec395 vs main ce0e2325 differ in 1 code file(s): .github/workflows/openclaw_oauth_guard.yml)

- ✅ runner_is_ecs: self-hosted runner 'ecs-openclaw'
- ✅ backend_grok: LLM_BACKEND=grok
- ✅ gateway_up: http://127.0.0.1:18789/health -> HTTP 200
- ✅ xai_token_outlives_run: 119 min left
- ✅ no_unapproved_paid_fallback: LLM_FALLBACK_MODEL=(none)
- ✅ no_twin_preopen: no twin
- ✅ read_cap_fits: OPENCLAW_PREDICT_READ_CAP_S=300, need >= 300
