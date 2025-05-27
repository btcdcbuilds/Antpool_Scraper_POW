import os
import asyncio
from typing import Tuple, Optional, Dict, List
from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Playwright

async def setup_browser() -> Browser:
    """Set up browser for scraping.
    
    Returns:
        Browser instance
    """
    # Launch browser
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless=True)
    return browser

async def load_cookies(page: Page, cookies: List[Dict] = None) -> None:
    """Load cookies directly to the page.
    
    Args:
        page: Playwright page
        cookies: List of cookie objects to load (optional)
        
    Returns:
        None
    """
    try:
        if not cookies:
            # Default cookies for Antpool if none provided
            cookies = [
                {
                    "name": "antpool_cookie_consent",
                    "value": "accepted",
                    "domain": ".antpool.com",
                    "path": "/"
                }
            ]
        
        # Set cookies
        await page.context.add_cookies(cookies)
        print(f"Loaded {len(cookies)} cookies")
    except Exception as e:
        print(f"Error loading cookies: {e}")

# Legacy function for backward compatibility
async def handle_cookie_consent(page: Page) -> bool:
    """Legacy function that now uses load_cookies instead of clicking consent dialogs.
    
    Args:
        page: Playwright page
        
    Returns:
        bool: Always returns True
    """
    try:
        await load_cookies(page)
        return True
    except Exception as e:
        print(f"Error in handle_cookie_consent: {e}")
        return False

# Alias for backward compatibility
handle_consent_dialog = handle_cookie_consent

async def take_screenshot(page: Page, file_path: str) -> str:
    """Take a screenshot of the page.
    
    Args:
        page: Playwright page
        file_path: Path to save screenshot
        
    Returns:
        Path to saved screenshot
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
        
        # Take screenshot
        await page.screenshot(path=file_path, full_page=True)
        print(f"Screenshot saved to {file_path}")
        return file_path
    except Exception as e:
        print(f"Error taking screenshot: {e}")
        return ""
