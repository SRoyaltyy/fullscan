# Preflight preopen_all 2026-10-16

Checked 2026-10-16 06:34 ET, run 38888029141

**STOPPED** before writing. 1 item(s) failed:

- ❌ **xai_token_outlives_run**: xAI login expires before this run can finish (needs >= 75 min left) (could not read xAI expiry: TimeoutExpired)

- ✅ ecs_code_current: code matches main (HEAD ffda4596, main ffda4596)
- ✅ runner_is_ecs: self-hosted runner 'ecs-openclaw'
- ✅ backend_grok: LLM_BACKEND=grok
- ✅ gateway_up: http://127.0.0.1:18789/health -> HTTP 200
- ✅ no_unapproved_paid_fallback: LLM_FALLBACK_MODEL=(none)
- ✅ no_twin_preopen: no twin
- ✅ read_cap_fits: OPENCLAW_PREDICT_READ_CAP_S=300, need >= 300
