#!/usr/bin/env python3
"""
Gemini Catcher – CloakBrowser-based web scraper.
Reusable async function `run_gemini(prompt)` that sends a prompt to the
Gemini web interface and returns the grounded response text + source links.
"""

import asyncio, json, os, sys, base64


GEMINI_URL   = "https://gemini.google.com"
STATE_FILE   = "gemini_browser_state.json"


def load_state():
    """Decode base64 GEMINI_BROWSER_STATE secret to a JSON file.
    Returns True on success, False if state is not available."""
    env_state = os.environ.get("GEMINI_BROWSER_STATE", "")
    if env_state:
        raw = base64.b64decode(env_state)
        with open(STATE_FILE, "wb") as f:
            f.write(raw)
        print("[catcher] ✅ Browser state decoded from secret.")
        return True
    if os.path.exists(STATE_FILE):
        print("[catcher] ✅ Using existing browser state file.")
        return True
    print("[catcher] ❌ No browser state found.")
    return False


async def run_gemini(prompt: str) -> dict:
    """
    Send `prompt` to Gemini via the web interface.
    Returns {"answer": str, "sources": list, "error": str or None}.
    """
    if not load_state():
        return {"answer": "", "sources": [], "error": "state_not_found"}

    from cloakbrowser import launch_context_async

    try:
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
            return {"answer": "", "sources": [], "error": "input_not_found"}

        # ── Type the prompt ──
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

        # ── Wait 60 seconds for the response ──
                
        try:
            await page.wait_for_selector(
                "[data-message-author='assistant'], div[class*='assistant'], "
                "div.model-response, div.response-content",
                timeout=30000,
            )
            # Wait for the loading spinner to disappear (if present)
            try:
                loading = page.locator("[data-test-id='loading-indicator'], .spinner, [class*='loading']")
                await loading.wait_for(state="hidden", timeout=15000)
            except Exception:
                pass
            # Extra settling time for the last part of the stream
            await page.wait_for_timeout(5000)
        except Exception:
            pass

        # ── Extract the last assistant message ──
        ASSISTANT_SELECTORS = [
            "[data-message-author='assistant']",
            "[data-role='assistant']",
            "div[class*='assistant']",
            "div.model-response",
            "div.response-content",
        ]
        answer = ""
        for sel in ASSISTANT_SELECTORS:
            elements = await page.query_selector_all(sel)
            if elements:
                last_el = elements[-1]
                txt = await last_el.inner_text()
                if txt and len(txt.strip()) > 30:
                    answer = txt.strip()
                    break

        if not answer or len(answer) < 50:
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
            clean = [l.strip() for l in lines if l.strip() and not any(u in l for u in unwanted)]
            answer = "\n".join(clean)

        source_links = []
        source_els = await page.query_selector_all("a[href^='http']")
        seen = set()
        for el in source_els:
            href = await el.get_attribute("href")
            if href and href not in seen and "google.com" not in href:
                seen.add(href)
                source_links.append(href)

        await ctx.close()
        return {"answer": answer, "sources": source_links[:20], "error": None}

    except Exception as e:
        return {"answer": "", "sources": [], "error": str(e)}
