import os
from playwright.async_api import async_playwright

async def setup_browser(playwright):
    """Set up browser with appropriate configuration."""
    print("Playwright started successfully")
    
    # Launch browser
    browser = await playwright.chromium.launch(
        headless=True,
        args=[
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process',
            '--disable-site-isolation-trials'
        ]
    )
    print("Browser launched successfully")
    
    # Create context with viewport and user agent
    context = await browser.new_context(
        viewport={'width': 1920, 'height': 1080},
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    )
    
    # Create page
    page = await context.new_page()
    
    print("Browser setup complete")
    return browser, context, page

async def handle_consent_dialog(page):
    """Handle consent dialog and cookie banners."""
    try:
        # Check for consent dialog
        consent_dialog = await page.locator('text="Cookie Consent"').count()
        if consent_dialog > 0:
            print("Consent dialog found")
            
            # Try clicking "Got it" button
            try:
                await page.click('button:has-text("Got it")', timeout=5000)
                print('Clicked \'Got it\' button')
            except:
                pass
            
            # Try clicking "Confirm" button
            try:
                await page.click('button:has-text("Confirm")', timeout=5000)
                print('Clicked \'Confirm\' button')
            except:
                pass
            
            # Try using JavaScript to dismiss dialog
            await page.evaluate("""() => {
                document.querySelectorAll('button').forEach(button => {
                    if (button.textContent.includes('Accept') || 
                        button.textContent.includes('Got it') || 
                        button.textContent.includes('Confirm') || 
                        button.textContent.includes('I agree')) {
                        button.click();
                    }
                });
            }""")
            print("Used JavaScript to dismiss consent dialog")
            
            # Wait for dialog to disappear
            await page.wait_for_timeout(1000)
            print("Consent dialog successfully dismissed")
        else:
            print("Consent dialog not found")
        
        # Check for cookie banner
        cookie_banner = await page.locator('text="Cookie Policy"').count()
        if cookie_banner > 0:
            # Try clicking "Accept" button
            try:
                await page.click('button:has-text("Accept")', timeout=5000)
                print('Clicked cookie banner \'Accept\' button')
            except:
                print("Cookie banner found but couldn't click Accept button")
        else:
            print("Cookie banner not found or already accepted")
    
    except Exception as e:
        print(f"Error handling consent dialog: {e}")
        # Continue anyway
    
    return True
