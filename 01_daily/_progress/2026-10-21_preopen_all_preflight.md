# Preflight preopen_all 2026-10-21

Checked 2026-10-21 04:28 ET, run 39568288470

**STOPPED** before writing. 4 item(s) failed:

- ❌ **runner_is_ecs**: Pre-Open is not on the ECS box (GitHub's own servers cannot reach Grok) (runner 'GitHub Actions 1000035849' is GitHub-hosted)
- ❌ **backend_grok**: LLM_BACKEND is not grok (LLM_BACKEND=auto)
- ❌ **gateway_up**: OpenClaw gateway 127.0.0.1:18789 did not answer within 5s (http://127.0.0.1:18789/health: URLError: <urlopen error [Errno 111] Connection refused>)
- ❌ **xai_token_outlives_run**: xAI login expires before this run can finish (needs >= 75 min left) (xAI login has unknown left, need 75)

- ✅ ecs_code_current: code matches main (HEAD 706f9ae8, main 16bc8c4a)
- ✅ no_unapproved_paid_fallback: LLM_FALLBACK_MODEL=(none)
- ✅ no_twin_preopen: no twin
- ✅ read_cap_fits: OPENCLAW_PREDICT_READ_CAP_S=300, need >= 300
