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

async def handle_informed_consent(page: Page) -> bool:
    """Handle the Antpool INFORMED CONSENT modal dialog using advanced techniques.
    
    Args:
        page: Playwright page
        
    Returns:
        bool: True if consent was handled, False otherwise
    """
    try:
        # Check if the INFORMED CONSENT modal is present
        consent_modal = page.locator("text=INFORMED CONSENT")
        if await consent_modal.count() > 0:
            print("INFORMED CONSENT modal detected")
            
            # Advanced Approach 1: Use JavaScript to directly modify DOM and remove modal
            try:
                await page.evaluate("""
                    // Remove the modal and backdrop completely from DOM
                    document.querySelectorAll('.ivu-modal-wrap').forEach(el => el.remove());
                    document.querySelectorAll('.ivu-modal-mask').forEach(el => el.remove());
                    
                    // Remove any overflow:hidden from body to ensure scrolling works
                    document.body.style.overflow = 'auto';
                    document.body.style.paddingRight = '0px';
                """)
                print("Attempted to remove modal via DOM manipulation")
                await page.wait_for_timeout(1000)
                
                # Check if modal is still present
                if await consent_modal.count() == 0:
                    print("Modal successfully removed with DOM manipulation")
                    return True
            except Exception as e:
                print(f"Advanced Approach 1 failed: {e}")
            
            # Advanced Approach 2: Modify CSS to make modal non-blocking
            try:
                await page.evaluate("""
                    // Create a style element to override modal styles
                    const style = document.createElement('style');
                    style.innerHTML = `
                        .ivu-modal-wrap { pointer-events: none !important; }
                        .ivu-modal-content { pointer-events: none !important; }
                        .ivu-modal-mask { display: none !important; }
                    `;
                    document.head.appendChild(style);
                """)
                print("Applied CSS overrides to make modal non-blocking")
                await page.wait_for_timeout(1000)
            except Exception as e:
                print(f"Advanced Approach 2 failed: {e}")
            
            # Advanced Approach 3: Use keyboard navigation to check the checkbox and confirm
            try:
                # First try to check the checkbox using JavaScript
                await page.evaluate("""
                    // Find all checkboxes and check them
                    document.querySelectorAll('.ivu-checkbox-input').forEach(checkbox => {
                        checkbox.checked = true;
                        
                        // Trigger change event to update UI
                        const event = new Event('change', { bubbles: true });
                        checkbox.dispatchEvent(event);
                    });
                """)
                print("Attempted to check consent checkbox via JavaScript")
                await page.wait_for_timeout(500)
                
                # Then try to enable and click the confirm button
                await page.evaluate("""
                    // Find and enable all disabled buttons
                    document.querySelectorAll('button[disabled]').forEach(button => {
                        button.disabled = false;
                        
                        // If button text contains confirm-related text, click it
                        if (button.innerText.toLowerCase().includes('confirm') || 
                            button.innerText.toLowerCase().includes('got it') ||
                            button.innerText.toLowerCase().includes('ok')) {
                            button.click();
                        }
                    });
                """)
                print("Attempted to enable and click confirm button via JavaScript")
                await page.wait_for_timeout(1000)
                
                # Check if modal is still present
                if await consent_modal.count() == 0:
                    print("Modal dismissed successfully with JavaScript button enabling")
                    return True
            except Exception as e:
                print(f"Advanced Approach 3 failed: {e}")
            
            # Advanced Approach 4: Use keyboard navigation
            try:
                # Press Tab multiple times to navigate to the checkbox
                for _ in range(5):
                    await page.keyboard.press("Tab")
                    await page.wait_for_timeout(100)
                
                # Press Space to check the checkbox
                await page.keyboard.press("Space")
                await page.wait_for_timeout(500)
                
                # Press Tab to navigate to the button
                await page.keyboard.press("Tab")
                await page.wait_for_timeout(100)
                
                # Press Enter to click the button
                await page.keyboard.press("Enter")
                await page.wait_for_timeout(1000)
                
                # Check if modal is still present
                if await consent_modal.count() == 0:
                    print("Modal dismissed successfully with keyboard navigation")
                    return True
            except Exception as e:
                print(f"Advanced Approach 4 failed: {e}")
            
            # Advanced Approach 5: Brute force - try clicking at various positions where buttons might be
            try:
                # Get page dimensions
                dimensions = await page.evaluate("""() => {
                    return {
                        width: window.innerWidth,
                        height: window.innerHeight
                    }
                }""")
                
                # Try clicking at positions where buttons are likely to be
                button_positions = [
                    # Bottom right (common for confirm buttons)
                    {"x": dimensions["width"] * 0.8, "y": dimensions["height"] * 0.8},
                    # Bottom center
                    {"x": dimensions["width"] * 0.5, "y": dimensions["height"] * 0.8},
                    # Center of modal (estimated)
                    {"x": dimensions["width"] * 0.5, "y": dimensions["height"] * 0.5}
                ]
                
                for position in button_positions:
                    await page.mouse.click(position["x"], position["y"])
                    await page.wait_for_timeout(500)
                    
                    # Check if modal is gone
                    if await consent_modal.count() == 0:
                        print(f"Modal dismissed by clicking at position {position}")
                        return True
            except Exception as e:
                print(f"Advanced Approach 5 failed: {e}")
            
            print("All advanced approaches to dismiss consent modal failed")
            
            # Even if we couldn't dismiss the modal, return true to continue with scraping
            # The script will attempt to work with the modal present
            return True
        else:
            print("No consent modal detected")
            return True
    except Exception as e:
        print(f"Error handling informed consent: {e}")
        # Return true to continue with scraping despite errors
        return True

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
        
        # Wait a moment for any animations to complete
        await page.wait_for_timeout(1000)
        
        return consent_handled
    except Exception as e:
        print(f"Error in handle_cookie_consent: {e}")
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
        print(f"Screenshot saved to {file_path}")
        return file_path
    except Exception as e:
        print(f"Error taking screenshot: {e}")
        return ""
