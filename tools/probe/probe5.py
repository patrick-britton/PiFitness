"""Loads the app home page in a headless browser at 448x875."""
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(viewport={"width": 448, "height": 875})
    page = context.new_page()
    page.goto("http://localhost:3000/", wait_until="domcontentloaded", timeout=30000)
    print("TITLE:", page.title())
    print("URL:", page.url)
    browser.close()
print("PROBE_BROWSER_OK")
