#!/usr/bin/env python3
"""
Gemini Web Interface Scraper – Robust submit, 60s wait, fixed JS.
"""

import asyncio, json, os, sys, base64
from cloakbrowser import launch_context_async

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

    # 1. Find the input box
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

    # 2. Type like a human (triggers React state)
    await input_box.click()
    await page.wait_for_timeout(500)
    await input_box.type(prompt, delay=50)
    await page.wait_for_timeout(1000)
    await input_box.press("Space")
    await input_box.press("Backspace")
    await page.wait_for_timeout(500)

    # 3. Count existing assistant messages before sending
    assistant_selector = (
        "[data-message-author='assistant'], [data-role='assistant'], "
        "div[class*='assistant'], div[class*='model-response']"
    )
    assistant_msg_count = await page.locator(assistant_selector).count()

    # 4. Submit
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

    # 5. Wait up to 60 seconds for a new assistant message
    print("⏳ Waiting 60 seconds for Gemini to respond…")
    try:
        async def new_message_appeared():
            count = await page.locator(assistant_selector).count()
            if count <= assistant_msg_count:
                return False
            last_text = await page.locator(assistant_selector).nth(count - 1).inner_text()
            return len(last_text.strip()) > 50

        await page.wait_for_function(
            "async () => {"
            f"const msgs = document.querySelectorAll('{assistant_selector}'.replace(/'/g, \"\\\\'\"));"
            f"if (msgs.length <= {assistant_msg_count}) return false;"
            "const text = msgs[msgs.length - 1].innerText || '';"
            "return text.trim().length > 50;"
            "}",
            timeout=60000
        )
        print("✅ New assistant message detected.")
    except Exception as e:
        print(f"⚠️  Waited 60 seconds, no new message detected: {e}")
        await page.screenshot(path="gemini_debug_timeout.png", full_page=True)
        await ctx.close()
        return {"error": "no_response_after_60s"}

    # 6. Extra settling time
    await page.wait_for_timeout(3000)
    await page.screenshot(path="gemini_debug_response.png", full_page=True)

    # 7. Extract last assistant message
    full_text = ""
    elements = await page.query_selector_all(assistant_selector)
    if elements and len(elements) > assistant_msg_count:
        last_el = elements[-1]
        txt = await last_el.inner_text()
        if txt and len(txt.strip()) > 30:
            full_text = txt.strip()

    # 8. Grounding source links
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
