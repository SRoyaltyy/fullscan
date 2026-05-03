#!/usr/bin/env python3
"""
Gemini Web Interface Scraper – robust response extraction.
"""

import asyncio, json, os, sys, base64, re
from playwright.async_api import async_playwright

GEMINI_URL   = "https://gemini.google.com"
STATE_FILE   = "gemini_browser_state.json"

TEST_PROMPT = (
    "Please do research on the company BBAI (BigBear.ai) using web search "
    "and tell me what you see here. Include: core business, revenue sources, "
    "key products, major contracts, financial situation, leadership, recent news, "
    "and geographic footprint."
)


def load_state():
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
            await page.screenshot(path="gemini_debug_input.png")
            await browser.close()
            return {"error": "input_not_found"}

        # ── Type the prompt ──
        await input_box.click()
        await input_box.fill(prompt)
        await page.wait_for_timeout(1000)

        # ── Submit ──
        SEND_SELECTORS = [
            "button[aria-label='Send message']",
            "button[data-test-id='send-button']",
            "button.send-button",
            "button[aria-label='Send']",
        ]
        sent = False
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
        # First, wait up to 20 seconds for a response block to appear
        try:
            await page.wait_for_selector(
                "div.response-content, div.model-response, div.message-content, "
                "div[class*='thread-item'] div[class*='text'], div[class*='chat-message']",
                timeout=20000,
            )
        except Exception:
            pass

        # Wait for the streaming indicator to disappear (if it exists)
        try:
            loading = page.locator("[data-test-id='loading-indicator'], .spinner, [class*='loading']")
            await loading.wait_for(state="hidden", timeout=15000)
        except Exception:
            pass

        await page.wait_for_timeout(4000)  # extra settling time

        # ── Take a full‑page screenshot for debugging ──
        await page.screenshot(path="gemini_debug_response.png", full_page=True)

        # ── Extraction: try specific selectors first ──
        RESPONSE_SELECTORS = [
            "div.response-content",
            "div.model-response",
            "div.message-content",
            "div[class*='thread-item'] div[class*='text']",
            "div[class*='chat-message'] div[class*='text']",
            "div[class*='assistant'] div[class*='text']",
        ]
        full_text = ""
        for sel in RESPONSE_SELECTORS:
            elements = await page.query_selector_all(sel)
            if elements:
                parts = []
                for el in elements:
                    txt = await el.inner_text()
                    if txt:
                        parts.append(txt.strip())
                candidate = "\n\n".join(parts)
                if len(candidate) > len(full_text):
                    full_text = candidate

        # ── Fallback: grab all visible text and filter UI lines ──
        if not full_text or len(full_text) < 50:
            body_text = await page.inner_text("body")
            # Remove common Gemini UI labels
            unwanted = [
                "Defining the Approach", "Answer now", "Gemini said",
                "You said", "Send message", "Clear chat", "New chat",
                "Extensions", "Apps", "Settings", "Help", "Feedback",
                "Google apps", "Account", "Search", "Menu", "Close",
                "Upload an image", "Type something", "Chat with Gemini",
                "Please do research", "Attach files",
            ]
            lines = body_text.split("\n")
            clean = []
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                if any(uw in line for uw in unwanted):
                    continue
                clean.append(line)
            full_text = "\n".join(clean)

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
