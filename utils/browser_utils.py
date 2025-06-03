import os
import asyncio
from typing import Tuple, Optional, Dict, List
from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Playwright

async def setup_browser(playwright: Optional[Playwright] = None, headless: bool = True) -> Tuple[Browser, BrowserContext, Page]:
    """Set up browser for scraping.
    
    Args:
        playwright: Optional Playwright instance (if None, will create a new one)
        headless: Whether to run browser in headless mode (default: True)
    
    Returns:
        Tuple of (Browser, BrowserContext, Page)
    """
    print("\nLaunching browser...")
    try:
        # Use provided playwright instance or create a new one
        local_playwright = playwright
        if local_playwright is None:
            local_playwright = await async_playwright().start()
            print("Playwright started successfully")
        
        # Browser arguments from working script
        browser_args = [
            "--start-maximized",
            "--disable-features=site-per-process",
            "--disable-web-security",
            "--disable-gpu"
        ]
        
        browser = await local_playwright.chromium.launch(
            headless=headless,
            args=browser_args,
            timeout=15000,  # 15 second timeout for browser launch (reduced from 60s)
        )
        print("Browser launched successfully")
        
        # Create context and page
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()
        
        return browser, context, page
    except Exception as e:
        print(f"CRITICAL ERROR launching browser: {str(e)}")
        raise

async def handle_informed_consent(page: Page) -> bool:
    """Handle the Antpool INFORMED CONSENT modal dialog using advanced techniques.
    
    Args:
        page: Playwright page
        
    Returns:
        bool: True if consent was handled, False otherwise
    """
    print("Handling consent dialog...")
    try:
        # Wait for the consent dialog to appear
        try:
            # Try to find the consent dialog
            consent_dialog = await page.wait_for_selector("text=\"Got it\"", timeout=5000)
            if consent_dialog:
                print("Consent dialog found")
                
                # Try multiple approaches to dismiss the dialog
                
                # Approach 1: Click the "Got it" button
                try:
                    await page.click("text=\"Got it\"", timeout=5000)
                    print("✅ Clicked 'Got it' button")
                    await asyncio.sleep(0.5)  # Reduced from 1
                    return True
                except Exception as e:
                    print(f"❌ Failed to click 'Got it' button: {str(e)}")
                
                # Approach 2: Click the checkbox and then the button
                try:
                    await page.click(".info-know", timeout=5000)
                    print("✅ Clicked consent checkbox")
                    await asyncio.sleep(0.5)  # Reduced from 1
                    
                    await page.click(".info-btn", timeout=5000)
                    print("✅ Clicked consent button")
                    await asyncio.sleep(0.5)  # Reduced from 1
                    return True
                except Exception as e:
                    print(f"❌ Failed to click checkbox and button: {str(e)}")
                
                # Approach 3: Use JavaScript to dismiss the dialog
                try:
                    await page.evaluate('''
                        () => {
                            // Find and click the checkbox
                            const checkbox = document.querySelector('.info-know');
                            if (checkbox) checkbox.click();
                            
                            // Find and click the button
                            const button = document.querySelector('.info-btn');
                            if (button) button.click();
                            
                            // Remove the modal elements from DOM
                            const modals = document.querySelectorAll('.ivu-modal-wrap, .ivu-modal-mask');
                            modals.forEach(modal => modal.remove());
                            
                            // Fix body styles
                            document.body.classList.remove('ivu-modal-open');
                            document.body.style.overflow = 'auto';
                            document.body.style.paddingRight = '0px';
                        }
                    ''')
                    print("✅ Used JavaScript to dismiss consent dialog")
                    await asyncio.sleep(0.5)  # Reduced from 1
                    return True
                except Exception as e:
                    print(f"❌ JavaScript approach failed: {str(e)}")
                
                # Approach 4: Force remove from DOM
                try:
                    await page.evaluate('''
                        () => {
                            // Force remove all modal elements
                            document.querySelectorAll('.ivu-modal-wrap, .ivu-modal-mask').forEach(el => el.remove());
                            
                            // Fix body styles
                            document.body.classList.remove('ivu-modal-open');
                            document.body.style.overflow = 'auto';
                            document.body.style.paddingRight = '0px';
                            
                            // Add style to prevent future modals
                            const style = document.createElement('style');
                            style.innerHTML = `
                                .ivu-modal-wrap, .ivu-modal-mask, .modal, .modal-backdrop {
                                    display: none !important;
                                    visibility: hidden !important;
                                    opacity: 0 !important;
                                    pointer-events: none !important;
                                }
                                body {
                                    overflow: auto !important;
                                    padding-right: 0 !important;
                                }
                            `;
                            document.head.appendChild(style);
                        }
                    ''')
                    print("✅ Forcibly removed modal elements from DOM")
                    await asyncio.sleep(0.5)  # Reduced from 1
                except Exception as e:
                    print(f"❌ Force DOM removal failed: {str(e)}")
                
                print("⚠️ All approaches to dismiss consent modal attempted")
                
                # Even if we couldn't dismiss the modal, return true to continue with scraping
                # The script will attempt to work with the modal present
                return True
            
        except Exception as e:
            print(f"ℹ️ No consent dialog found: {str(e)}")
            return True
        
    except Exception as e:
        print(f"❌ Error handling consent dialog: {str(e)}")
        # Return true to continue with scraping despite errors
        return True

async def ensure_no_modals(page: Page) -> bool:
    """Ensure no modals are present on the page.
    
    Args:
        page: Playwright page
        
    Returns:
        bool: True if no modals are present or were successfully removed
    """
    print("Ensuring no modals are present...")
    try:
        # Use JavaScript to remove any modals
        await page.evaluate('''
            () => {
                // Remove modal elements
                const modals = document.querySelectorAll('.ivu-modal-wrap, .modal, .dialog, [role="dialog"]');
                modals.forEach(modal => {
                    modal.style.display = 'none';
                });
                
                // Remove modal backdrops
                const backdrops = document.querySelectorAll('.ivu-modal-mask, .modal-backdrop');
                backdrops.forEach(backdrop => {
                    backdrop.style.display = 'none';
                });
                
                // Remove body classes that might prevent scrolling
                document.body.classList.remove('modal-open');
                document.body.style.overflow = 'auto';
            }
        ''')
        print("✅ Removed any modal elements")
        await asyncio.sleep(0.5)  # Reduced from 1
        return True
    except Exception as e:
        print(f"❌ Error ensuring no modals: {str(e)}")
        return False

async def handle_cookie_consent(page: Page) -> bool:
    """Handle cookie consent and informed consent dialogs.
    
    Args:
        page: Playwright page
        
    Returns:
        bool: True if consent was handled, False otherwise
    """
    try:
        # Handle the informed consent modal if present
        consent_handled = await handle_informed_consent(page)
        
        # Check for cookie banner
        try:
            await page.click("button.cookie-btn", timeout=2000)  # Reduced from 3000
            print("✅ Clicked cookie banner button")
            await asyncio.sleep(0.5)  # Reduced from 1
        except Exception:
            print("ℹ️ Cookie banner not found or already accepted")
        
        # Wait a moment for any animations to complete
        await page.wait_for_timeout(500)  # Reduced from 1000
        
        # Ensure any remaining modals are dismissed
        await ensure_no_modals(page)
        
        return consent_handled
    except Exception as e:
        print(f"❌ Error in handle_cookie_consent: {str(e)}")
        return True  # Continue despite errors

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
        print(f"📸 Screenshot saved to {file_path}")
        return file_path
    except Exception as e:
        print(f"❌ Error taking screenshot: {str(e)}")
        return ""
