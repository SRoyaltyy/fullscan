#!/usr/bin/env python3
"""
Gemini Web Interface Scraper – robust submission, 60s fixed wait,
extracts the last assistant message.
"""

import asyncio, json, os, sys, base64
from cloakbrowser import launch_context_async

GEMINI_URL   = "https://gemini.google.com"
STATE_FILE   = "gemini_browser_state.json"




def load_state():
    env_state = os.environ.get("GEMINI_BROWSER_STATE", "")
    if env_state:
        raw = base64.b64decode(env_state)
        with open(STATE_FILE, "wb") as f:
            f.write(raw)
        print("✅ Browser state decoded from secret.")
    if not os.path.exists(STATE_FILE):
        print("❌ No browser state found.")
        sys.exit(1)


async def run_gemini(prompt: str) -> dict:
    load_state()

    ctx = await launch_context_async(
        storage_state=STATE_FILE,
        humanize=True,
        headless=True,
        args=["--no-sandbox", "--disable-setuid-sandbox"],
    )
    page = await ctx.new_page()
    await page.goto(GEMINI_URL, wait_until="domcontentloaded")

    # ── Find input box ──
    INPUT_SELECTORS = [
        "div[contenteditable='true']",
        "div[role='textbox']",
        "div.ql-editor",
        "textarea",
    ]
    input_box = None
    for sel in INPUT_SELECTORS:
        try:
            input_box = await page.wait_for_selector(sel, timeout=8000)
            if input_box and await input_box.is_visible():
                break
            input_box = None
        except Exception:
            continue
    if not input_box:
        await page.screenshot(path="gemini_debug_input.png")
        await ctx.close()
        return {"error": "input_not_found"}

    # ── Type the prompt (fill + follow‑up keystroke for React) ──
    await input_box.click()
    await page.wait_for_timeout(300)
    await input_box.fill(prompt)
    await page.wait_for_timeout(800)
    await input_box.press("Space")
    await input_box.press("Backspace")
    await page.wait_for_timeout(500)

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
            if btn and await btn.is_visible():
                await btn.click()
                sent = True
                break
        except Exception:
            continue
    if not sent:
        await input_box.press("Enter")

    # ── Wait a fixed 60 seconds for the response to stream ──
    print("⏳ Waiting 60 seconds for Gemini to respond…")
    await page.wait_for_timeout(60000)

    # ── Optional: wait for a well‑known response element (soft) ──
    try:
        await page.wait_for_selector(
            "[data-message-author='assistant'], div[class*='assistant'], "
            "div.model-response, div.response-content",
            timeout=5000,
        )
    except Exception:
        pass

    # ── Screenshot for debugging ──
    await page.screenshot(path="gemini_debug_response.png", full_page=True)

    # ── Extract the LAST assistant message ──
    ASSISTANT_SELECTORS = [
        "[data-message-author='assistant']",
        "[data-role='assistant']",
        "div[class*='assistant']",
        "div.model-response",
        "div.response-content",
    ]
    full_text = ""
    for sel in ASSISTANT_SELECTORS:
        elements = await page.query_selector_all(sel)
        if elements:
            # take the last one
            last_el = elements[-1]
            txt = await last_el.inner_text()
            if txt and len(txt.strip()) > 30:
                full_text = txt.strip()
                break

    # ── Fallback: use the full page but filter out UI chrome ──
    if not full_text or len(full_text) < 50:
        body_text = await page.inner_text("body")
        unwanted = [
            "Defining the Approach", "Answer now", "Gemini said",
            "You said", "Send message", "Clear chat", "New chat",
            "Extensions", "Apps", "Settings", "Help", "Feedback",
            "Google apps", "Account", "Search", "Menu", "Close",
            "Upload an image", "Type something", "Chat with Gemini",
            "Attach files", "Conversation with Gemini", "Defining the Scope",
            "Tools", "Pro", "Gemini is AI and can make mistakes.",
        ]
        lines = body_text.split("\n")
        clean = []
        for line in lines:
            line = line.strip()
            if not line or any(uw in line for uw in unwanted):
                continue
            clean.append(line)
        full_text = "\n".join(clean)

    # ── Grounding source links ──
    source_links = []
    source_els = await page.query_selector_all("a[href^='http']")
    seen = set()
    for el in source_els:
        href = await el.get_attribute("href")
        if href and href not in seen and "google.com" not in href:
            seen.add(href)
            source_links.append(href)

    await ctx.close()
    return {"answer": full_text, "sources": source_links[:20]}


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
