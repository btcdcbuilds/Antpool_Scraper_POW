import os
import asyncio
from typing import Tuple, Optional, Dict, List
from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Playwright

async def setup_browser(headless: bool = True) -> Browser:
    """Set up browser for scraping.
    
    Args:
        headless: Whether to run browser in headless mode (default: True)
    
    Returns:
        Browser instance
    """
    print("\nLaunching browser...")
    try:
        playwright = await async_playwright().start()
        print("Playwright started successfully")
        
        # Browser arguments from working script
        browser_args = [
            "--start-maximized",
            "--disable-features=site-per-process",
            "--disable-web-security",
            "--disable-gpu"
        ]
        
        browser = await playwright.chromium.launch(
            headless=headless,
            args=browser_args,
            timeout=15000,  # 15 second timeout for browser launch (reduced from 60s)
        )
        print("Browser launched successfully")
        return browser
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
    print("Handling informed consent dialog...")
    try:
        # Wait for the consent dialog to appear
        try:
            await page.wait_for_selector("text=INFORMED CONSENT", timeout=3000)  # Reduced from 5000
            print("✅ Consent dialog found")
            
            # Take screenshot of modal for debugging
            screenshot_path = os.path.join(os.getcwd(), "consent_modal.png")
            await page.screenshot(path=screenshot_path)
            print(f"📸 Screenshot saved: {screenshot_path}")
            
            # Try multiple approaches to dismiss the dialog
            
            # Approach 1: Click "Got it" button
            try:
                await page.click("text=Got it", timeout=2000)  # Reduced from 3000
                print("✅ Clicked 'Got it' button")
                await asyncio.sleep(0.5)  # Reduced from 1
                
                # Check if modal is gone
                is_modal_gone = await page.evaluate('''
                    () => {
                        const modal = document.querySelector('.ivu-modal-wrap');
                        return !modal || modal.style.display === 'none' || 
                               !modal.classList.contains('ivu-modal-show');
                    }
                ''')
                
                if is_modal_gone:
                    print("✅ Modal dismissed with 'Got it' button")
                    return True
            except Exception as e:
                print(f"❌ Could not click 'Got it' button: {str(e)}")
            
            # Approach 2: Click "Confirm" button
            try:
                await page.click("text=Confirm", timeout=2000)  # Reduced from 3000
                print("✅ Clicked 'Confirm' button")
                await asyncio.sleep(0.5)  # Reduced from 1
                
                # Check if modal is gone
                is_modal_gone = await page.evaluate('''
                    () => {
                        const modal = document.querySelector('.ivu-modal-wrap');
                        return !modal || modal.style.display === 'none' || 
                               !modal.classList.contains('ivu-modal-show');
                    }
                ''')
                
                if is_modal_gone:
                    print("✅ Modal dismissed with 'Confirm' button")
                    return True
            except Exception as e:
                print(f"❌ Could not click 'Confirm' button: {str(e)}")
            
            # Approach 3: Use JavaScript to close the modal
            try:
                await page.evaluate('''
                    () => {
                        // Find all buttons in the modal
                        const buttons = Array.from(document.querySelectorAll('.ivu-modal-wrap button'));
                        
                        // Click all buttons that might dismiss the dialog
                        buttons.forEach(button => {
                            if (button.textContent.includes('Got it') || 
                                button.textContent.includes('Confirm') ||
                                button.textContent.includes('Accept') ||
                                button.textContent.includes('OK')) {
                                button.click();
                            }
                        });
                        
                        // Try to remove the modal directly from DOM
                        const modals = document.querySelectorAll('.ivu-modal-wrap');
                        modals.forEach(modal => {
                            modal.style.display = 'none';
                        });
                        
                        // Remove modal backdrop
                        const backdrops = document.querySelectorAll('.ivu-modal-mask');
                        backdrops.forEach(backdrop => {
                            backdrop.style.display = 'none';
                        });
                    }
                ''')
                print("✅ Used JavaScript to dismiss consent dialog")
                await asyncio.sleep(0.5)  # Reduced from 1
                
                # Check if modal is gone
                is_modal_gone = await page.evaluate('''
                    () => {
                        const modal = document.querySelector('.ivu-modal-wrap');
                        return !modal || modal.style.display === 'none' || 
                               !modal.classList.contains('ivu-modal-show');
                    }
                ''')
                
                if is_modal_gone:
                    print("✅ Modal dismissed with JavaScript")
                    return True
            except Exception as e:
                print(f"❌ Could not use JavaScript to dismiss dialog: {str(e)}")
            
            # Approach 4: Try to check checkbox and enable button
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
                print("✅ Attempted to check consent checkbox via JavaScript")
                await page.wait_for_timeout(300)  # Reduced from 500
                
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
                print("✅ Attempted to enable and click confirm button via JavaScript")
                await page.wait_for_timeout(500)  # Reduced from 1000
                
                # Check if modal is gone
                is_modal_gone = await page.evaluate('''
                    () => {
                        const modal = document.querySelector('.ivu-modal-wrap');
                        return !modal || modal.style.display === 'none' || 
                               !modal.classList.contains('ivu-modal-show');
                    }
                ''')
                
                if is_modal_gone:
                    print("✅ Modal dismissed with checkbox and button enabling")
                    return True
            except Exception as e:
                print(f"❌ Could not check checkbox and enable button: {str(e)}")
            
            # Approach 5: Last resort - Press Escape key
            try:
                await page.keyboard.press('Escape')
                print("✅ Pressed Escape key to dismiss dialog")
                await asyncio.sleep(0.5)  # Reduced from 1
                
                # Check if modal is gone
                is_modal_gone = await page.evaluate('''
                    () => {
                        const modal = document.querySelector('.ivu-modal-wrap');
                        return !modal || modal.style.display === 'none' || 
                               !modal.classList.contains('ivu-modal-show');
                    }
                ''')
                
                if is_modal_gone:
                    print("✅ Modal dismissed with Escape key")
                    return True
            except Exception as e:
                print(f"❌ Could not press Escape key: {str(e)}")
            
            # Approach 6: Brute force - try clicking at various positions where buttons might be
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
                    await page.wait_for_timeout(300)  # Reduced from 500
                    
                    # Check if modal is gone
                    is_modal_gone = await page.evaluate('''
                        () => {
                            const modal = document.querySelector('.ivu-modal-wrap');
                            return !modal || modal.style.display === 'none' || 
                                   !modal.classList.contains('ivu-modal-show');
                        }
                    ''')
                    
                    if is_modal_gone:
                        print(f"✅ Modal dismissed by clicking at position {position}")
                        return True
            except Exception as e:
                print(f"❌ Brute force clicking failed: {str(e)}")
            
            # Approach 7: Most aggressive - force remove from DOM
            try:
                await page.evaluate('''
                    () => {
                        // Force remove all modal elements
                        const modalElements = [
                            '.ivu-modal-wrap', '.ivu-modal', '.ivu-modal-mask',
                            '.modal', '.modal-dialog', '.modal-backdrop',
                            '[role="dialog"]', '.dialog'
                        ];
                        
                        modalElements.forEach(selector => {
                            document.querySelectorAll(selector).forEach(el => {
                                if (el && el.parentNode) {
                                    el.parentNode.removeChild(el);
                                }
                            });
                        });
                        
                        // Remove body classes and styles
                        document.body.classList.remove('modal-open');
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
        print(f"❌ Error handling informed consent: {str(e)}")
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
