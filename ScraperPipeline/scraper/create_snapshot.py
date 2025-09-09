# scraper/create_snapshot.py
import os
import pathlib
from playwright.sync_api import sync_playwright

SNAPSHOT_DIR = pathlib.Path("snapshots")
SNAPSHOT_DIR.mkdir(exist_ok=True)

def create_snapshot(url: str) -> str:
    """
    Take a high-quality full-page screenshot of a URL, save as JPEG, return file path.
    """
    try:
        filename = url.replace("https://", "").replace("http://", "").replace("/", "_")
        filepath = SNAPSHOT_DIR / f"{filename}.jpg"

        with sync_playwright() as p:
            browser = p.chromium.launch()
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                device_scale_factor=2,
            )
            page = context.new_page()
            page.goto(url, timeout=30000)
            page.wait_for_load_state("networkidle")
            page.evaluate("document.fonts.ready")

            page.screenshot(path=str(filepath), full_page=True, type="jpeg", quality=90)

            browser.close()

        print(f"Screenshot saved: {filepath}")
        return str(filepath)
    except Exception as e:
        print(f"Failed to create snapshot for {url}: {e}")
        raise

if __name__ == "__main__":
    test_url = "https://www.delarosasf.com/menus-bishop-ranch/#happy-hour-sanramon"
    path = create_snapshot(test_url)
    print(f"Saved screenshot at {path}")