import os
import asyncio
from typing import Tuple, Optional

from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Playwright

async def setup_browser(playwright: Playwright) -> Tuple[Browser, BrowserContext, Page]:
    """Set up browser, context, and page for scraping.
    
    Args:
        playwright: Playwright instance
        
    Returns:
        Tuple of browser, context, and page
    """
    # Launch browser
    browser = await playwright.chromium.launch(headless=True)
    
    # Create context with viewport size
    context = await browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    )
    
    # Create page
    page = await context.new_page()
    
    # Set default timeout
    page.set_default_timeout(60000)
    
    return browser, context, page

async def handle_consent_dialog(page: Page) -> bool:
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
            button = page.locator(f"button:text-is('{button_text}')").first
            if await button.count() > 0:
                await button.click()
                print(f"Clicked consent button with text: {button_text}")
                await asyncio.sleep(1)
                return True
        
        # Approach 2: Look for common consent button classes
        for class_name in [".consent-button", ".accept-button", ".agree-button", ".cookie-accept"]:
            button = page.locator(class_name).first
            if await button.count() > 0:
                await button.click()
                print(f"Clicked consent button with class: {class_name}")
                await asyncio.sleep(1)
                return True
        
        # Approach 3: Use JavaScript to remove overlay elements
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
        
        # No consent dialog found or handled
        print("No consent dialog found or already accepted")
        return False
        
    except Exception as e:
        print(f"Error handling consent dialog: {e}")
        return False
