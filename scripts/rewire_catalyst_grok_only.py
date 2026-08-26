#!/usr/bin/env python3
"""Idempotent rewire of collectors/catalyst_analysis.py onto Grok-only."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "collectors" / "catalyst_analysis.py"

NEW_HEADER = '''#!/usr/bin/env python3
"""
Catalyst Analysis Engine v2 – Grok-only analysis (OpenClaw / Grok 4.6)
- Ticker(s) and cutoff date set at the top.
- SearXNG still gathers snippets for Step 1 / Step 2. Analysis is Grok.
- Independent verdict + catcher use the same prompts, routed to Grok
  (native web/X search) instead of Gemini.
- DeepSeek is not a provider on this path.
"""
'''

NEW_LLM_SETUP = '''# ── LLM setup (Grok via OpenClaw ONLY) ─────────────────
_OC_URL = os.environ.get("OPENCLAW_GATEWAY_URL", "").rstrip("/")
if not _OC_URL:
    raise SystemExit(
        "OPENCLAW_GATEWAY_URL is required — catalyst_analysis is Grok-only. "
        "DeepSeek / Gemini are not used."
    )
_OC_MODEL = os.environ.get("OPENCLAW_AGENT", "openclaw/default")
_OC_BACKEND = os.environ.get("OPENCLAW_BACKEND_MODEL", "xai/grok-4.6")
client = OpenAI(
    api_key=os.environ.get("OPENCLAW_TOKEN") or "openclaw",
    base_url=_OC_URL + "/v1",
    default_headers={"x-openclaw-model": _OC_BACKEND},
)

# Compat for src.catalyst_daily._prepare_engine (OpenClaw only; no DeepSeek).
_PROVIDERS = [("openclaw", client, _OC_MODEL)]

def _grok_via_pipeline(messages, temperature, max_tokens, tools, stage):
    """OpenClaw/Grok only. Never fall through to DeepSeek from this collector."""
    try:
        from src import deepseek_client
    except Exception:
        return None
    fn = getattr(deepseek_client, "_openclaw_chat", None)
    if fn is None:
        return None
    text = fn(
        messages,
        tools=tools,
        max_tokens=max_tokens,
        temperature=temperature,
        transcript_path=None,
        trace_path=None,
        stage_label=stage,
    )
    return (text or "").strip() or None

def safe_create(**kwargs):
    kwargs["model"] = _OC_MODEL
    last = None
    for attempt in range(3):
        try:
            return client.chat.completions.create(**kwargs)
        except Exception as e:
            last = e
            print(f"  ⚠️  openclaw/Grok API error (attempt {attempt+1}/3): {e}")
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
    raise last
'''

NEW_CALL_LLM = '''def call_llm(prompt, user_msg, temperature=0.3, max_tokens=40000,
             tools=False, stage="catalyst"):
    messages = [{"role": "system", "content": prompt},
                {"role": "user", "content": user_msg}]
    text = _grok_via_pipeline(messages, temperature, max_tokens, tools, stage)
    if text:
        return text
    resp = safe_create(model=MODEL, messages=messages,
                       temperature=temperature, max_tokens=max_tokens)
    return (resp.choices[0].message.content or "").strip()
'''

NEW_VERDICT_FN = '''async def run_verdict_pass(full_name, ticker, cutoff_date):
    """Same verdict prompt, Grok with native web/X search."""
    prompt = build_verdict_prompt(full_name, ticker, cutoff_date)
    print("  🧠 Asking Grok for an independent verdict (parallel with Step 1/2)…")
    try:
        answer = await asyncio.to_thread(
            call_llm,
            prompt,
            f"Verdict for {full_name}. Search first, then answer.",
            0.2,
            8000,
            True,
            f"CATALYST VERDICT {ticker}",
        )
        answer = (answer or "").strip()
        print("  🔍 RAW VERDICT (full answer):")
        print(answer[:1500])
        print("  ── end of raw verdict ──")

        verdict = None
        explanation = ""
        lines = answer.splitlines()
        for i, line in enumerate(lines):
            stripped = line.strip()
            for junk in ["Grok said", "Gemini said", "Gemini", "said", "Defining", "Answer now"]:
                if stripped.lower().startswith(junk.lower()):
                    stripped = stripped[len(junk):].strip()
                    break
            if stripped.lower() in ("bullish", "bearish"):
                verdict = stripped.capitalize()
                explanation = " ".join(
                    l.strip() for l in lines[i+1:] if l.strip()
                )
                break

        if verdict:
            return verdict, explanation[:600]

        match = re.search(r'\\b(Bullish|Bearish)\\b', answer[:500], re.IGNORECASE)
        if match:
            verdict = match.group(1).capitalize()
            rest = answer[match.end():].strip()
            return verdict, rest[:600]

        return "Unclear", answer[:600]
    except Exception as e:
        return None, str(e)
'''

NEW_CATCHER_START = '''async def run_catcher_pass(full_name, ticker, cutoff_date, grid, weighted_taxonomy,
                           net_signal, conviction):
    prompt = build_catcher_prompt(full_name, ticker, cutoff_date, grid, net_signal, conviction)
    print("  🐾 Running Grok catcher (same prompt, native search)…")
    try:
        answer = await asyncio.wait_for(
            asyncio.to_thread(
                call_llm,
                prompt,
                f"Catch missed catalysts for {full_name}. Search the web.",
                0.2,
                8000,
                True,
                f"CATALYST CATCHER {ticker}",
            ),
            timeout=900,
        )
        if not answer:
            print("  ⚠️  Grok catcher returned empty response.")
            return grid
    except asyncio.TimeoutError:
        print("  ⚠️  Grok catcher timed out — using original grid.")
        return grid
    except Exception as e:
        print(f"  ⚠️  Grok catcher failed: {e}")
        return grid
'''


def _replace_between(text: str, start: str, end: str, replacement: str) -> str:
    i = text.find(start)
    j = text.find(end, i + len(start) if i >= 0 else 0)
    if i < 0 or j < 0:
        raise SystemExit(f"markers not found: {start[:40]!r} .. {end[:40]!r}")
    return text[:i] + replacement + text[j:]


def already_rewired(text: str) -> bool:
    return (
        "Grok-only analysis" in text
        and "gemini_catcher" not in text
        and "DEEPSEEK_API_KEY" not in text
        and "_grok_via_pipeline" in text
        and "tools=False, stage=" in text
    )


def rewire(text: str) -> str:
    if already_rewired(text):
        return text

    end_header = text.find("import os, json")
    if end_header < 0:
        raise SystemExit("import block not found")
    text = NEW_HEADER + text[end_header:]

    text = text.replace(
        'MODEL                = "deepseek-chat"',
        'MODEL                = os.environ.get("OPENCLAW_AGENT", "openclaw/default")',
        1,
    )

    text = _replace_between(
        text,
        "# ── LLM setup",
        "# ── Finviz news scrape",
        NEW_LLM_SETUP + "\n",
    )

    text = _replace_between(
        text,
        "def call_llm(",
        "def deduplicate_grid_by_event_id(",
        NEW_CALL_LLM + "\n",
    )

    text = text.replace(
        "#  GEMINI INDEPENDENT VERDICT (NEW – no sector preamble)",
        "#  GROK INDEPENDENT VERDICT (same prompt, Grok native search)",
        1,
    )
    text = text.replace(
        '"""Standalone Gemini prompt: independent Bullish/Bearish verdict using web search."""',
        '"""Standalone verdict prompt: independent Bullish/Bearish using web search."""',
        1,
    )

    if "async def run_verdict_pass(" in text:
        text = _replace_between(
            text,
            "async def run_verdict_pass(",
            "def build_catcher_prompt(",
            NEW_VERDICT_FN
            + "\n\n# ═════════════════════════════════════════════════════\n"
              "#  GROK CATCHER (same prompt)\n"
              "# ═════════════════════════════════════════════════════\n\n",
        )

    if "from gemini_catcher import run_gemini as gemini_catcher_run" in text:
        text = _replace_between(
            text,
            "async def run_catcher_pass(",
            "    try:\n        parsed = json.loads(answer)",
            NEW_CATCHER_START + "\n",
        )

    replacements = {
        "    # ── Fire Gemini verdict in the background NOW ──":
            "    # ── Fire Grok verdict in the background NOW ──",
        "    # ── DeepSeek pipeline (unchanged) ──":
            "    # ── SearXNG gather + Grok Step 1/2/4 ──",
        "    # ── Run Gemini catcher (sequential – needs the grid) ──":
            "    # ── Run Grok catcher (sequential – needs the grid) ──",
        "    # ── Collect Gemini verdict (already running in parallel) ──":
            "    # ── Collect Grok verdict (already running in parallel) ──",
        '    print(f"  🧠 Gemini verdict: {gem_verdict or \'N/A\'}")':
            '    print(f"  🧠 Grok verdict: {gem_verdict or \'N/A\'}")',
    }
    for a, b in replacements.items():
        text = text.replace(a, b)

    if "gemini_catcher" in text or "DEEPSEEK_API_KEY" in text:
        raise SystemExit("rewire incomplete — gemini_catcher or DEEPSEEK_API_KEY still present")
    if "_grok_via_pipeline" not in text:
        raise SystemExit("rewire incomplete — pipeline helper missing")
    return text


def main() -> None:
    target = TARGET if TARGET.exists() else Path("collectors/catalyst_analysis.py")
    if not target.exists():
        raise SystemExit(f"missing {target}")
    original = target.read_text(encoding="utf-8")
    updated = rewire(original)
    if updated == original:
        print(f"already Grok-only ({target.stat().st_size} bytes)")
        return
    target.write_text(updated, encoding="utf-8")
    print(f"rewired {target} bytes={target.stat().st_size}")


if __name__ == "__main__":
    main()
