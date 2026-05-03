#!/usr/bin/env python3
"""
Gemini Web Interface Scraper — reuses a saved browser login state.
Handles base64‑encoded GEMINI_BROWSER_STATE secret from GitHub Actions.
"""

import asyncio, json, os, sys, base64
from playwright.async_api import async_playwright

GEMINI_URL   = "https://gemini.google.com"
STATE_FILE   = "gemini_browser_state.json"

# ═══════════════════════════════════════════════════
# CHANGE YOUR PROMPT HERE
# ═══════════════════════════════════════════════════
TEST_PROMPT = (
    "Please do research on the company BBAI (BigBear.ai) using web search "
    "and tell me what you see here. Include: core business, revenue sources, "
    "key products, major contracts, financial situation, leadership, recent news, "
    "and geographic footprint."
)
# ═══════════════════════════════════════════════════


def load_state():
    """Decode base64 GEMINI_BROWSER_STATE secret to a JSON file."""
    env_state = os.environ.get("GEMINI_BROWSER_STATE", "")
    if env_state:
        raw = base64.b64decode(env_state)
        with open(STATE_FILE, "wb") as f:
            f.write(raw)
        print("✅ Browser state decoded from secret.")
    if not os.path.exists(STATE_FILE):
        print("❌ No browser state found. Run gemini_login_setup.py first.")
        sys.exit(1)


async def run_gemini(prompt: str) -> dict:
    """Open Gemini, type prompt, submit, extract response + sources."""
    load_state()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
            ],
        )
        context = await browser.new_context(storage_state=STATE_FILE)
        page = await context.new_page()
        await page.goto(GEMINI_URL, wait_until="domcontentloaded")

        # ── Find the input box ──
        INPUT_SELECTORS = [
            "div[contenteditable='true']",
            "textarea",
            "div.ql-editor",
            "div[role='textbox']",
        ]
        input_box = None
        for sel in INPUT_SELECTORS:
            try:
                input_box = await page.wait_for_selector(sel, timeout=8000)
                if input_box:
                    break
            except Exception:
                continue
        if not input_box:
            print("❌ Could not find Gemini input box. Taking debug screenshot…")
            await page.screenshot(path="gemini_debug.png")
            await browser.close()
            return {"error": "input_not_found"}

        # ── Type the prompt ──
        await input_box.click()
        await input_box.fill(prompt)
        await page.wait_for_timeout(800)

        # ── Submit ──
        sent = False
        SEND_SELECTORS = [
            "button[aria-label='Send message']",
            "button[data-test-id='send-button']",
            "button.send-button",
        ]
        for sel in SEND_SELECTORS:
            try:
                btn = await page.wait_for_selector(sel, timeout=3000)
                if btn:
                    await btn.click()
                    sent = True
                    break
            except Exception:
                continue
        if not sent:
            await input_box.press("Enter")

        # ── Wait for the response to finish streaming ──
        await page.wait_for_timeout(3000)
        try:
            await page.wait_for_selector(
                "div.response-content, div.model-response, div.message-content",
                timeout=20000,
            )
        except Exception:
            pass
        await page.wait_for_timeout(5000)  # extra for streaming

        # ── Extract the full response text ──
        RESPONSE_SELECTORS = [
            "div.response-content",
            "div.model-response",
            "div[class*='response']",
            "div.message-content",
        ]
        full_text = ""
        for sel in RESPONSE_SELECTORS:
            elements = await page.query_selector_all(sel)
            if elements:
                parts = [await el.inner_text() for el in elements if await el.inner_text()]
                full_text = "\n\n".join(parts)
                if len(full_text) > 50:
                    break

        # ── Extract grounding source links ──
        source_links = []
        source_els = await page.query_selector_all("a[href^='http']")
        seen = set()
        for el in source_els:
            href = await el.get_attribute("href")
            if href and href not in seen and "google.com" not in href:
                seen.add(href)
                source_links.append(href)

        await browser.close()
        return {
            "answer": full_text,
            "sources": source_links[:20],
        }


if __name__ == "__main__":
    result = asyncio.run(run_gemini(TEST_PROMPT))
    print("\n" + "=" * 70)
    print("📝 GEMINI RESPONSE")
    print("=" * 70)
    print(result.get("answer", result.get("error", "No answer")))
    if result.get("sources"):
        print("\n" + "=" * 70)
        print("🔗 GROUNDING SOURCES")
        print("=" * 70)
        for i, url in enumerate(result["sources"]):
            print(f"  [{i+1}] {url}")
    print("\n✅ Run complete.")
