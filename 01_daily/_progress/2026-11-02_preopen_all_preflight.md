# Preflight preopen_all 2026-11-02

Checked 2026-11-02 04:35 ET, run 41419239215

**STOPPED** before writing. 1 item(s) failed:

- ❌ **ecs_code_current**: code stale: this checkout's code (src, scripts, config, .github) must match GitHub main (ecs code stale: HEAD fd6562d0 vs main e4ba6c0c differ in 3 code file(s): .github/workflows/h1_open_fill.yml, .github/workflows/holdup_open_fill.yml, scripts/open_fill_gate.py)

- ✅ no_unapproved_paid_fallback: LLM_FALLBACK_MODEL=(none)
- ✅ no_twin_preopen: no twin
- ✅ read_cap_fits: OPENCLAW_PREDICT_READ_CAP_S=300, need >= 300
