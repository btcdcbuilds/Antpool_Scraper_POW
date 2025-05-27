import os
import asyncio
from typing import Tuple, Optional
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

async def handle_cookie_consent(page: Page) -> bool:
    """Handle cookie consent dialog if present.
    
    Args:
        page: Playwright page
        
    Returns:
        bool: True if consent dialog was handled, False otherwise
    """
    try:
        # Try multiple approaches to handle consent dialog
        
        # Approach 1: Look for common consent button text
        for button_text in ["Accept", "Accept All", "I Accept", "Agree", "I Agree", "OK", "Continue"]:
            try:
                button = page.locator(f"button:text-is('{button_text}')").first
                if await button.count() > 0:
                    await button.click()
                    print(f"Clicked consent button with text: {button_text}")
                    await asyncio.sleep(1)
                    return True
            except Exception as e:
                print(f"Error with button text approach: {e}")
        
        # Approach 2: Look for common consent button classes
        for class_name in [".consent-button", ".accept-button", ".agree-button", ".cookie-accept"]:
            try:
                button = page.locator(class_name).first
                if await button.count() > 0:
                    await button.click()
                    print(f"Clicked consent button with class: {class_name}")
                    await asyncio.sleep(1)
                    return True
            except Exception as e:
                print(f"Error with button class approach: {e}")
        
        # Approach 3: Use JavaScript to remove overlay elements
        try:
            await page.evaluate("""() => {
                // Remove common overlay elements
                document.querySelectorAll('.cookie-banner, .consent-overlay, .cookie-consent, .cookie-dialog, .consent-banner, .gdpr-banner').forEach(el => el.remove());
                
                // Remove any elements with 'cookie' or 'consent' in the class name
                document.querySelectorAll('[class*="cookie"], [class*="consent"], [class*="gdpr"]').forEach(el => el.remove());
                
                // Remove any fixed or sticky positioned elements that might be overlays
                document.querySelectorAll('body > div[style*="position: fixed"], body > div[style*="position:fixed"]').forEach(el => el.remove());
            }""")
            print("Removed overlay elements using JavaScript")
            await asyncio.sleep(1)
        except Exception as e:
            print(f"Error with JavaScript approach: {e}")
        
        # No consent dialog found or handled
        print("No consent dialog found or already accepted")
        return False
        
    except Exception as e:
        print(f"Error handling consent dialog: {e}")
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
