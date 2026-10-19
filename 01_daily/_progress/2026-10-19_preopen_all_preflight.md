# Preflight preopen_all 2026-10-19

Checked 2026-10-19 04:33 ET, run 39221339978

**PASS** all items OK. The run may write.

- ✅ ecs_code_current: code matches main (HEAD 30a1f04c, main 7a3aef3a)
- ✅ runner_is_ecs: self-hosted runner 'ecs-openclaw'
- ✅ backend_grok: LLM_BACKEND=grok
- ✅ gateway_up: http://127.0.0.1:18789/health -> HTTP 200
- ✅ xai_token_outlives_run: 209 min left
- ✅ no_unapproved_paid_fallback: LLM_FALLBACK_MODEL=(none)
- ✅ no_twin_preopen: no twin
- ✅ read_cap_fits: OPENCLAW_PREDICT_READ_CAP_S=300, need >= 300
