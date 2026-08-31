"""Runtime overlay: catalyst_analysis uses the shared LLM routing.

Grok native web/X is primary. When GROK_ONLY=0, the public client
automatically falls back to DeepSeek with its web-search tool loop.
"""
from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime
from pathlib import Path


def _native_search_brief(queries):
    lines = [
        "USE THE WEB-SEARCH CAPABILITY AVAILABLE TO YOU. "
        "There is no pre-fetched snippet pack.",
        "Run the queries below (and any better ones you need), then extract.",
        "",
    ]
    for q in queries:
        lines.append(f"- {q}")
    return "\n".join(lines)


def _elite_or_export_news(ca, ticker: str) -> list[dict]:
    """Ticker news from authenticated Elite HTML, then Elite CSV.

    Never calls public finviz.com and never uses SearXNG.
    """
    rows = []
    try:
        from bs4 import BeautifulSoup
        from src import finviz_session
        sess = finviz_session.session()
        if finviz_session.authed(sess):
            resp = finviz_session.get(sess, f"/quote.ashx?t={ticker}", timeout=30)
            if resp is not None:
                soup = BeautifulSoup(resp.text, "html.parser")
                year = datetime.now().year
                last_date = None
                for tr in soup.select("tr.news_table-row, table.fullview-news-outer tr"):
                    cells = tr.find_all("td")
                    link = tr.select_one("a.nn-tab-link") or tr.select_one("a[href^='http']")
                    if not cells or link is None:
                        continue
                    raw_date = cells[0].get_text(" ", strip=True)
                    if re.search(r"\d{2}-\d{2}-\d{2}", raw_date):
                        try:
                            last_date = datetime.strptime(
                                raw_date.split()[0], "%b-%d-%y").date().isoformat()
                        except ValueError:
                            pass
                    elif re.search(r"[A-Z][a-z]{2}-\d{2}", raw_date):
                        try:
                            last_date = datetime.strptime(
                                f"{raw_date.split()[0]}-{year}", "%b-%d-%Y"
                            ).date().isoformat()
                        except ValueError:
                            pass
                    date_str = last_date or ca.TODAY
                    if ca.CUTOFF_DATE and date_str > ca.CUTOFF_DATE:
                        continue
                    title = link.get_text(" ", strip=True)
                    url = link.get("href") or ""
                    rows.append({
                        "event_date": date_str,
                        "headline": title,
                        "description": title,
                        "evidence_excerpt": title[:150],
                        "source_urls": [url] if url.startswith("http") else [],
                        "confidence": 85,
                        "source": "finviz_elite",
                    })
    except Exception as e:
        print(f"  ⚠️  Elite ticker news skipped: {str(e)[:120]}")
    if rows:
        return rows[:80]

    # Same paid export, no HTML dependency.
    try:
        import pandas as pd
        files = sorted(Path("data/exports").glob("finviz_????-??-??.csv"))
        if files:
            df = pd.read_csv(files[-1], low_memory=False)
            hit = df[df["Ticker"].astype(str).str.upper() == ticker.upper()]
            if len(hit):
                r = hit.iloc[0]
                title = str(r.get("News Title") or "").strip()
                url = str(r.get("News URL") or "").strip()
                digest = str(r.get("Daily Digest") or "").strip()
                if title and title not in ("nan", "-"):
                    return [{
                        "event_date": ca.TODAY,
                        "headline": title,
                        "description": digest if digest not in ("nan", "-") else title,
                        "evidence_excerpt": title[:150],
                        "source_urls": [url] if url.startswith("http") else [],
                        "confidence": 75,
                        "source": "finviz_elite_export",
                    }]
    except Exception as e:
        print(f"  ⚠️  Elite export ticker news skipped: {str(e)[:120]}")
    return []


def install(ca) -> None:
    if getattr(ca, "_GROK_NATIVE_SEARCH", False):
        print("[catalyst] Grok native search already installed")
        return

    from src import config
    if not config.has_llm():
        raise SystemExit(
            "Catalyst analysis needs OPENCLAW_GATEWAY_URL or "
            "DEEPSEEK_API_KEY"
        )

    providers_used: set[str] = set()

    def _routed_chat(messages, temperature, max_tokens, tools, stage):
        from src import deepseek_client
        text = deepseek_client.chat(
            messages,
            model=config.MODEL_PREDICT,
            tools=tools,
            max_tokens=max_tokens,
            temperature=temperature,
            transcript_path=None,
            trace_path=None,
            stage_label=stage,
        )
        providers_used.add(deepseek_client.last_provider() or "unknown")
        return (text or "").strip() or None

    def call_llm(prompt, user_msg, temperature=0.3, max_tokens=40000,
                 tools=False, stage="catalyst"):
        messages = [{"role": "system", "content": prompt},
                    {"role": "user", "content": user_msg}]
        text = _routed_chat(messages, temperature, max_tokens, tools, stage)
        if text:
            return text
        raise RuntimeError(
            f"OpenClaw and DeepSeek both returned empty for {stage}"
        )

    async def run_verdict_pass(full_name, ticker, cutoff_date):
        prompt = ca.build_verdict_prompt(full_name, ticker, cutoff_date)
        print("  🧠 Asking the routed LLM for an independent verdict (web search)…")
        try:
            answer = await asyncio.to_thread(
                call_llm, prompt,
                f"Verdict for {full_name}. Search first, then answer.",
                0.2, 8000, True, f"CATALYST VERDICT {ticker}",
            )
            answer = (answer or "").strip()
            print("  🔍 RAW VERDICT:")
            print(answer[:1500])
            verdict = None
            explanation = ""
            lines = answer.splitlines()
            for i, line in enumerate(lines):
                stripped = line.strip()
                if stripped.lower() in ("bullish", "bearish"):
                    verdict = stripped.capitalize()
                    explanation = " ".join(l.strip() for l in lines[i + 1:] if l.strip())
                    break
            if verdict:
                return verdict, explanation[:600]
            match = re.search(r"\b(Bullish|Bearish)\b", answer[:500], re.IGNORECASE)
            if match:
                return match.group(1).capitalize(), answer[match.end():].strip()[:600]
            return "Unclear", answer[:600]
        except Exception as e:
            return None, str(e)

    async def run_catcher_pass(full_name, ticker, cutoff_date, grid, weighted_taxonomy,
                               net_signal, conviction):
        prompt = ca.build_catcher_prompt(full_name, ticker, cutoff_date, grid, net_signal, conviction)
        print("  🐾 Running routed LLM catcher (web search)…")
        try:
            answer = await asyncio.wait_for(
                asyncio.to_thread(
                    call_llm, prompt,
                    f"Catch missed catalysts for {full_name}. Search the web.",
                    0.2, 8000, True, f"CATALYST CATCHER {ticker}",
                ),
                timeout=900,
            )
            if not answer:
                print("  ⚠️  LLM catcher empty — original grid")
                return grid
        except Exception as e:
            print(f"  ⚠️  LLM catcher failed: {e}")
            return grid
        try:
            parsed = ca.parse_json(answer) if hasattr(ca, "parse_json") else None
            if parsed is None:
                import json
                parsed = json.loads(answer)
        except Exception:
            match = re.search(r"\{.*\}", answer, re.DOTALL)
            if not match:
                print("  ⚠️  Catcher returned no JSON")
                return grid
            try:
                import json
                parsed = json.loads(match.group())
            except Exception:
                return grid
        if not isinstance(parsed, dict):
            return grid
        new_hits = parsed.get("new_hits") or []
        corrected = parsed.get("corrected_hits") or []
        for corr in corrected:
            tax = corr.get("taxonomy")
            for entry in grid:
                if entry.get("taxonomy") == tax and corr.get("corrected_status") == "MISS":
                    entry["status"] = "MISS"
                    entry["adjusted_weight"] = 0
                    entry["confidence"] = 0
        for hit in new_hits:
            hit["status"] = "HIT"
            hit.setdefault("type", "positive")
            hit["base_weight"] = ca.CATALYST_WEIGHTS.get(hit.get("taxonomy"), 5)
            mult = weighted_taxonomy.get(hit.get("taxonomy"), {}).get("multiplier", 1.0)
            hit["adjusted_weight"] = max(0, min(10, round(hit["base_weight"] * mult)))
            hit.setdefault("confidence", 80)
            grid.append(hit)
            print(f"    ➕ Catcher added {hit.get('taxonomy')} ({hit.get('event_date', '?')})")
        return grid

    async def analyze_stock_async(ticker, snapshot, searxng_url):
        providers_used.clear()
        db_name = snapshot["profile"].get("company_name", "")
        if db_name and db_name.lower() != ticker.lower() and len(db_name) > 2:
            official_name = db_name
            aliases = []
            print(f"  🏢 Using DB company name: {official_name}")
        else:
            # Do not call the legacy SearXNG company-name resolver. The
            # ticker is sufficient; Grok resolves aliases in native search.
            official_name, aliases = ticker, []
        full_name = f"{official_name} ({ticker})" if official_name.lower() != ticker.lower() else ticker

        verdict_task = asyncio.create_task(run_verdict_pass(full_name, ticker, ca.CUTOFF_DATE))
        finviz_events = _elite_or_export_news(ca, ticker)
        print(f"  📰 Finviz returned {len(finviz_events)} headlines (after cutoff)")

        catalyst_queries = ca._make_catalyst_templates(full_name)
        context_queries = ca._make_context_templates(full_name)
        print(f"  ⏳ {len(catalyst_queries)}+{len(context_queries)} queries "
              "→ routed web search")
        search_results_str = _native_search_brief(catalyst_queries)
        context_str = _native_search_brief(context_queries)
        finviz_json = ca.json.dumps(finviz_events, indent=2)
        prompt1 = ca._format_step1(full_name, ticker, ca.TODAY, ca.LOOKBACK_START,
                                   search_results_str, finviz_json)
        prompt2 = ca._format_step2(full_name, ticker, snapshot, context_str,
                                   "\n".join(ca.TAXONOMY_LIST))

        step1_raw, step2_raw = await asyncio.gather(
            asyncio.to_thread(call_llm, prompt1, f"Extract events for {full_name}. Search first.", 0.3, 40000, True, f"CATALYST STEP1 {ticker}"),
            asyncio.to_thread(call_llm, prompt2, f"Context for {full_name}. Search first.", 0.3, 40000, True, f"CATALYST STEP2 {ticker}"),
        )
        print("  ✅ Step 1 + Step 2 LLM done.")

        try:
            raw_events = ca.parse_json(step1_raw)
            if isinstance(raw_events, dict):
                raw_events = raw_events.get("events", raw_events.get("evidence_grid", []))
            if not isinstance(raw_events, list):
                raise ValueError("not a list")
        except Exception as e:
            print(f"  ❌ Step 1 parse failed: {e}")
            return {"error": "Step 1 parse failure", "raw": step1_raw[:500]}
        if ca.CUTOFF_DATE:
            raw_events = [e for e in raw_events if e.get("event_date", "9999") <= ca.CUTOFF_DATE]

        try:
            context_profile = ca.parse_json(step2_raw)
        except Exception as e:
            print(f"  ❌ Step 2 parse failed: {e}")
            return {"error": "Step 2 parse failure", "raw": step2_raw[:500]}

        # Reuse the rest of the original pipeline from here by temporarily
        # swapping search so the original function is not invoked (it still
        # hits SearXNG). We finish synthesis here.
        sensitivity = context_profile.get("sensitivity_profile", {})
        weighted_taxonomy = {}
        for cat, prof in sensitivity.items():
            base = ca.CATALYST_WEIGHTS.get(cat, 5)
            mult = prof.get("multiplier", 1.0) if isinstance(prof, dict) else 1.0
            adj = round(base * mult)
            weighted_taxonomy[cat] = {
                "base_weight": base, "multiplier": mult,
                "adjusted_weight": max(0, min(10, adj)),
                "rationale": (prof or {}).get("rationale", "") if isinstance(prof, dict) else "",
            }

        merged_events = []
        idx = 0
        for ev in finviz_events:
            merged_events.append({**ev, "id": idx}); idx += 1
        for ev in raw_events:
            merged_events.append({**ev, "id": idx}); idx += 1
        merged_events.sort(key=lambda e: e.get("confidence", 0), reverse=True)
        merged_events = merged_events[:50]

        merged_json = ca.json.dumps([
            {"id": e["id"], "description": e.get("description"), "event_date": e.get("event_date"),
             "evidence_excerpt": e.get("evidence_excerpt", ""), "source_urls": e.get("source_urls", []),
             "confidence": e.get("confidence", 70)} for e in merged_events
        ], indent=2)
        prompt4 = ca._format_step4(full_name, ticker, ca.TODAY, merged_json,
                                   ca.json.dumps(weighted_taxonomy, indent=2),
                                   ca.json.dumps(snapshot, indent=2, default=str))
        final_raw = call_llm(prompt4, f"Finalize {full_name}.", 0.1, 25000, False, f"CATALYST STEP4 {ticker}")
        try:
            final_result = ca.parse_json(final_raw)
        except Exception as e:
            print(f"  ❌ Step 4 parse failed: {e}")
            return {"error": "Step 4 parse failure", "raw": final_raw[:500]}

        grid = final_result.get("catalyst_grid", []) or []
        grid = await run_catcher_pass(full_name, ticker, ca.CUTOFF_DATE, grid,
                                      weighted_taxonomy,
                                      final_result.get("net_signal", "?"),
                                      final_result.get("conviction", 0))
        grid = ca.deduplicate_grid_by_event_id(grid)
        new_signal, new_conviction = ca.recalculate_signal(grid)
        final_result["catalyst_grid"] = grid
        final_result["net_signal"] = new_signal
        final_result["conviction"] = new_conviction
        gem_verdict, gem_reason = await verdict_task
        print(f"  🧠 Routed verdict: {gem_verdict or 'N/A'}")
        if gem_reason:
            print(f"     Reason: {gem_reason[:200]}")
        final_result["grok_verdict"] = gem_verdict
        final_result["search_backend"] = (
            "grok_native"
            if providers_used == {"openclaw"}
            else "deepseek_fallback"
            if "deepseek" in providers_used
            else "routed_search"
        )
        return final_result

    def analyze_stock(ticker, snapshot, searxng_url):
        return asyncio.run(analyze_stock_async(ticker, snapshot, searxng_url))

    ca.call_llm = call_llm
    ca.run_verdict_pass = run_verdict_pass
    ca.run_catcher_pass = run_catcher_pass
    ca.analyze_stock_async = analyze_stock_async
    ca.analyze_stock = analyze_stock
    ca._GROK_NATIVE_SEARCH = True
    print("[catalyst] installed Grok-primary / DeepSeek-fallback search overlay")
