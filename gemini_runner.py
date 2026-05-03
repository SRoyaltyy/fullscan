
#!/usr/bin/env python3

"""

Gemini Web Interface Scraper — Robust submission & 60s wait.

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

    # 1. FIND THE REAL INPUT BOX

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

    # 2. ROBUST TYPING — use type() with delay for realistic keystrokes

    await input_box.click()

    await page.wait_for_timeout(500)

    await input_box.type(prompt, delay=50)       # 50ms between keystrokes

    await page.wait_for_timeout(1000)

    # Trigger React state update

    await input_box.press("Space")

    await input_box.press("Backspace")

    await page.wait_for_timeout(500)

    # 3. COUNT EXISTING MESSAGES BEFORE SENDING

    assistant_msg_count = await page.locator(

        "[data-message-author='assistant'], [data-role='assistant'], "

        "div[class*='assistant'], div[class*='model-response']"

    ).count()

    # 4. SUBMIT

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

    # 5. WAIT FOR RESPONSE — EXACTLY 60 SECONDS

    print("⏳ Waiting 60 seconds for Gemini to respond…")

    try:

        await page.wait_for_function(

            f"""

            () => {{

                const msgs = document.querySelectorAll(

                    "[data-message-author='assistant'], [data-role='assistant'], "

                    "div[class*='assistant'], div[class*='model-response']"

                );

                return msgs.length > {assistant_msg_count} &&

                       msgs[msgs.length-1].innerText.trim().length > 50;

            }}

            """,

            timeout=60000    # ← 60 seconds max

        )

        print("✅ New assistant message detected.")

    except Exception as e:

        print(f"⚠️  Waited 60 seconds, no new message detected: {e}")

        await page.screenshot(path="gemini_debug_timeout.png", full_page=True)

        await ctx.close()

        return {"error": "no_response_after_60s"}

    # 6. ADDITIONAL SETTLING TIME

    await page.wait_for_timeout(3000)

    await page.screenshot(path="gemini_debug_response.png", full_page=True)

    # 7. EXTRACT LAST ASSISTANT MESSAGE

    full_text = ""

    ASSISTANT_SELECTORS = [

        "[data-message-author='assistant']",

        "[data-role='assistant']",

        "div[class*='assistant']",

        "div[class*='model-response']",

    ]

    for sel in ASSISTANT_SELECTORS:

        elements = await page.query_selector_all(sel)

        if elements and len(elements) > assistant_msg_count:

            last_el = elements[-1]

            txt = await last_el.inner_text()

            if txt and len(txt.strip()) > 30:

                full_text = txt.strip()

                break

    # EXTRACT SOURCE LINKS

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
