#!/usr/bin/env python3
"""
Gemini Catcher – CloakBrowser-based web scraper.
Sends prompts to Gemini via the web interface using native JS input.
"""

import asyncio, json, os, sys, base64, re
from cloakbrowser import launch_context_async

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


def _compact_prompt(text: str) -> str:
    """Collapse whitespace to avoid extra paragraph gaps.
    If you want a single‑line prompt, replace with:
        text = re.sub(r'\s+', ' ', text.strip())
    """
    text = text.strip()
    text = re.sub(r'\n[ \t]*\n', '\n', text)
    text = re.sub(r'[ \t]+\n', '\n', text)
    return text


async def run_gemini(prompt: str) -> dict:
    """
    Send `prompt` to Gemini via the web interface.
    Returns {"answer": str, "sources": list, "error": str or None}.
    """
    if not load_state():
        return {"answer": "", "sources": [], "error": "state_not_found"}

    try:
        ctx = await launch_context_async(
            storage_state=STATE_FILE,
            humanize=True,
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox"],
        )
        page = await ctx.new_page()
        await page.goto(GEMINI_URL, wait_until="domcontentloaded")

        # ── Find the input box (contenteditable div) ──
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

        # ── Insert prompt into the contenteditable div (pure JS, no fill()) ──
        prompt_clean = _compact_prompt(prompt)

        await page.evaluate(
            """(div, text) => {
                // 1. Clear existing content
                div.textContent = '';
                div.focus();

                // 2. Set the new text as plain text (no HTML formatting)
                div.textContent = text;

                // 3. Fire the exact events React/Gemini listeners expect
                div.dispatchEvent(new InputEvent('beforeinput', {
                    inputType: 'insertText',
                    data: text,
                    bubbles: true,
                    cancelable: true,
                }));
                div.dispatchEvent(new InputEvent('input', {
                    inputType: 'insertText',
                    data: text,
                    bubbles: true,
                }));
            }""",
            input_box,
            prompt_clean,
        )
        await page.wait_for_timeout(800)

        # ── Ensure Send button is enabled (safety nudge if still disabled) ──
        SEND_BTN_SELECTOR = "button[aria-label='Send message']"
        try:
            send_btn = await page.wait_for_selector(SEND_BTN_SELECTOR, timeout=3000)
            is_disabled = await send_btn.get_attribute("disabled")
            if is_disabled:
                # Sometimes a real keypress is required – type a space then delete it
                await input_box.press("Space")
                await input_box.press("Backspace")
                await page.wait_for_timeout(500)
        except Exception:
            pass

        # ── Submit ──
        SEND_SELECTORS = [
            "button[aria-label='Send message']",
            "button[data-test-id='send-button']",
            "button.send-button",
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

        # ── Wait for response ──
        print("[catcher] ⏳ Waiting for Gemini response…")
        try:
            await page.wait_for_selector(
                "[data-message-author='assistant'], div[class*='assistant'], "
                "div.model-response, div.response-content",
                timeout=90000,
            )
            # Hide loading indicator if present
            try:
                loading = page.locator(
                    "[data-test-id='loading-indicator'], .spinner, [class*='loading']"
                )
                await loading.wait_for(state="hidden", timeout=30000)
            except Exception:
                pass
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
            # Fallback: extract from whole body, filtering out UI text
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


# ── CLI entry point ─────────────────────────────────────────────
async def main():
    """Read a prompt from the GEMINI_PROMPT env variable (default test prompt)."""
    prompt = os.environ.get("GEMINI_PROMPT", "Explain the current market outlook for NVDA in 2 sentences.")
    result = await run_gemini(prompt)
    if result["error"]:
        print(f"[catcher] ❌ Error: {result['error']}")
        sys.exit(1)
    print("=" * 60)
    print(result["answer"])
    if result["sources"]:
        print("\n--- Sources ---")
        for s in result["sources"]:
            print(s)
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
