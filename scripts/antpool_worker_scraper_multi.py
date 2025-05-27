#!/usr/bin/env python3
"""
Antpool Worker Stats Scraper - Multi-Account Version

This script scrapes worker statistics from Antpool's observer page for multiple accounts
stored in Supabase. It navigates to the Worker tab and extracts data for all workers.

Usage:
    python3 antpool_worker_scraper_multi.py [--skip_supabase] [--debug]
    python3 antpool_worker_scraper_multi.py --access_key=<access_key> --user_id=<observer_user_id> --coin_type=<coin_type> --output_dir=<output_dir>
"""

import argparse
import asyncio
import json
import os
import math
import time
import re
import sys
import traceback
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from utils.browser_utils import setup_browser, handle_consent_dialog, take_screenshot
    from utils.data_utils import save_json_to_file, format_timestamp
    from utils.supabase_utils import get_supabase_client
except ImportError as e:
    print(f"Import error: {e}")
    # Fallback for direct script execution
    try:
        sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
        from utils.browser_utils import setup_browser, handle_consent_dialog, take_screenshot
        from utils.data_utils import save_json_to_file, format_timestamp
        from utils.supabase_utils import get_supabase_client
    except ImportError as e2:
        print(f"Fallback import also failed: {e2}")
        sys.exit(1)

from playwright.async_api import async_playwright


class AntpoolWorkerScraper:
    def __init__(self, access_key, observer_user_id, coin_type="BTC", output_dir=None, debug=False):
        """Initialize the Antpool worker scraper with the given parameters."""
        self.access_key = access_key
        self.observer_user_id = observer_user_id
        self.coin_type = coin_type
        self.output_dir = output_dir or os.path.join(os.getcwd(), "output")
        self.debug = debug

        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)

        self.browser = None
        self.page = None

        # Base URL for the Antpool observer page
        self.base_url = (
            f"https://www.antpool.com/observer?accessKey={self.access_key}"
            f"&coinType={self.coin_type}&observerUserId={self.observer_user_id}"
        )

    async def _setup_browser(self):
        """Set up the browser for scraping."""
        print("\nLaunching browser...")
        try:
            playwright = await async_playwright().start()
            print("Playwright started successfully")
            
            # Launch browser with debugging flags
            browser_args = [
                "--start-maximized",
                "--disable-features=site-per-process",
                "--disable-web-security",
                "--disable-gpu"
            ]
            
            self.browser = await playwright.chromium.launch(
                headless=True,  # Use headless mode for server environments
                args=browser_args,
                timeout=60000,  # 60 second timeout for browser launch
            )
            print("Browser launched successfully")
            
            # Create page with specific viewport size for consistent screenshots
            self.page = await self.browser.new_page(viewport={"width": 1280, "height": 1024})
            self.page.set_default_timeout(60000)  # 60 second timeout for page operations
            
            print("Browser setup complete")
        except Exception as e:
            print(f"CRITICAL ERROR launching browser: {str(e)}")
            raise

    async def handle_consent_dialog(self):
        """Handle the informed consent dialog."""
        print("Handling consent dialog...")
        try:
            # Wait for the consent dialog to appear
            try:
                await self.page.wait_for_selector("text=INFORMED CONSENT", timeout=5000)
                print("Consent dialog found")
                
                # Try multiple approaches to dismiss the dialog
                
                # Approach 1: Click "Got it" button
                try:
                    await self.page.click("text=Got it", timeout=3000)
                    print("Clicked 'Got it' button")
                    await asyncio.sleep(1)
                except Exception as e:
                    print(f"Could not click 'Got it' button: {str(e)}")
                
                # Approach 2: Click "Confirm" button
                try:
                    await self.page.click("text=Confirm", timeout=3000)
                    print("Clicked 'Confirm' button")
                    await asyncio.sleep(1)
                except Exception as e:
                    print(f"Could not click 'Confirm' button: {str(e)}")
                
                # Approach 3: Use JavaScript to close the modal
                try:
                    await self.page.evaluate('''
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
                    print("Used JavaScript to dismiss consent dialog")
                    await asyncio.sleep(1)
                except Exception as e:
                    print(f"Could not use JavaScript to dismiss dialog: {str(e)}")
                
                # Verify the modal is gone
                is_modal_gone = await self.page.evaluate('''
                    () => {
                        const modal = document.querySelector('.ivu-modal-wrap');
                        return !modal || modal.style.display === 'none' || 
                               !modal.classList.contains('ivu-modal-show');
                    }
                ''')
                
                if is_modal_gone:
                    print("Consent dialog successfully dismissed")
                else:
                    print("Consent dialog may still be present")
                    
                    # Last resort: Press Escape key
                    try:
                        await self.page.keyboard.press('Escape')
                        print("Pressed Escape key to dismiss dialog")
                        await asyncio.sleep(1)
                    except Exception as e:
                        print(f"Could not press Escape key: {str(e)}")
            except Exception as e:
                print(f"No consent dialog found: {str(e)}")
            
            # Check for cookie banner
            try:
                await self.page.click("button.cookie-btn", timeout=3000)
                print("Clicked cookie banner button")
                await asyncio.sleep(1)
            except Exception:
                print("Cookie banner not found or already accepted")
                
            print("Consent dialog handling completed")
        except Exception as e:
            print(f"Error during consent dialog handling: {str(e)}")

    async def navigate_to_observer_page(self):
        """Navigate to the Antpool observer page."""
        print(f"Navigating to observer page: {self.base_url}")
        await self.page.goto(self.base_url, wait_until="networkidle")
        print("Page loaded")
        
        # Handle consent dialog
        await self.handle_consent_dialog()

        print("Waiting for hashrate chart...")
        # Wait for hashrate chart with retry logic
        max_retries = 3
        retry_delay = 5
        for attempt in range(max_retries):
            try:
                await self.page.wait_for_selector("#hashrate-chart", timeout=45000)
                print("Hashrate chart loaded successfully")
                break
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    print("Failed to load hashrate chart after multiple attempts")
                    raise
                await asyncio.sleep(retry_delay)
                
        # Ensure any remaining modals are dismissed
        await self.ensure_no_modals()

    async def ensure_no_modals(self):
        """Ensure no modals are present on the page."""
        print("Ensuring no modals are present...")
        try:
            # Use JavaScript to remove any modals
            await self.page.evaluate('''
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
            print("Removed any modal elements")
            await asyncio.sleep(1)
        except Exception as e:
            print(f"Error ensuring no modals: {str(e)}")

    async def navigate_to_worker_tab(self):
        """Navigate to the Worker tab."""
        print("Navigating to Worker tab...")
        try:
            # First ensure no modals are present
            await self.ensure_no_modals()
            
            # Try to find and click the Worker tab using JavaScript
            worker_tab_clicked = await self.page.evaluate('''
                () => {
                    // Find all tab elements
                    const tabs = Array.from(document.querySelectorAll('.ivu-tabs-tab, a'));
                    
                    // Find the Worker tab
                    const workerTab = tabs.find(tab => 
                        tab.textContent.trim().includes('Worker') || 
                        tab.textContent.trim().includes('worker')
                    );
                    
                    // Click it if found
                    if (workerTab) {
                        workerTab.click();
                        return true;
                    }
                    
                    // Try alternative approach - click second tab
                    const allTabs = document.querySelectorAll('.ivu-tabs-tab');
                    if (allTabs.length >= 2) {
                        allTabs[1].click();
                        return true;
                    }
                    
                    return false;
                }
            ''')
            
            if worker_tab_clicked:
                print("Clicked Worker tab using JavaScript")
            else:
                print("Could not click Worker tab with JavaScript")
                
                # Try direct click approach
                try:
                    await self.page.click("div.ivu-tabs-tab:nth-child(2)")
                    print("Clicked second tab assuming it's Worker tab")
                except Exception as e:
                    print(f"Could not click second tab: {str(e)}")
            
            # Wait for the click to take effect
            await asyncio.sleep(5)  # Increased wait time
            
            # Take screenshot after clicking Worker tab
            if self.debug:
                screenshot_path = os.path.join(self.output_dir, f"worker_tab_clicked_{self.observer_user_id}.png")
                await self.page.screenshot(path=screenshot_path)
                print(f"Screenshot saved after clicking Worker tab: {screenshot_path}")
            
            # Wait for worker table to load with retry logic and longer timeout
            max_retries = 5  # Increased retries
            for attempt in range(max_retries):
                try:
                    await self.page.wait_for_selector("table", timeout=15000)  # Increased timeout
                    print("Worker table loaded")
                    break
                except Exception as e:
                    print(f"Attempt {attempt + 1} to find table failed: {e}")
                    if attempt == max_retries - 1:
                        print("Failed to find worker table after multiple attempts")
                        raise
                    await asyncio.sleep(3)  # Increased wait between retries
                    
            # Ensure any remaining modals are dismissed again
            await self.ensure_no_modals()
            
            # Check for iframes that might contain the table
            iframe_count = await self.page.evaluate('''
                () => document.querySelectorAll('iframe').length
            ''')
            
            if iframe_count > 0:
                print(f"Found {iframe_count} iframes, checking for content")
                
                # Try to access iframe content
                for i in range(iframe_count):
                    try:
                        frame = self.page.frames[i + 1]  # Skip main frame
                        if frame:
                            print(f"Checking iframe {i+1}")
                            # Try to find table in iframe
                            table_in_frame = await frame.evaluate('''
                                () => document.querySelectorAll('table').length > 0
                            ''')
                            if table_in_frame:
                                print(f"Found table in iframe {i+1}")
                                # Use this frame for further operations
                                self.frame = frame
                                break
                    except Exception as e:
                        print(f"Error checking iframe {i+1}: {str(e)}")
            
            # Wait for any loading indicators to disappear
            try:
                await self.page.wait_for_function('''
                    () => {
                        const loaders = document.querySelectorAll('.loading, .spinner, [class*="loading"], [class*="spinner"]');
                        return loaders.length === 0 || Array.from(loaders).every(el => 
                            window.getComputedStyle(el).display === 'none' || 
                            window.getComputedStyle(el).visibility === 'hidden'
                        );
                    }
                ''', timeout=20000)
                print("Loading indicators disappeared")
            except Exception as e:
                print(f"Loading indicators may still be present: {str(e)}")
            
            # Scroll to ensure all content is loaded
            await self.page.evaluate('''
                () => {
                    window.scrollTo(0, document.body.scrollHeight / 2);
                }
            ''')
            await asyncio.sleep(2)
            
            await self.page.evaluate('''
                () => {
                    window.scrollTo(0, document.body.scrollHeight);
                }
            ''')
            await asyncio.sleep(2)
            
            await self.page.evaluate('''
                () => {
                    window.scrollTo(0, 0);
                }
            ''')
            await asyncio.sleep(2)
            
        except Exception as e:
            print(f"Error navigating to Worker tab: {str(e)}")
            if self.debug:
                error_screenshot = os.path.join(self.output_dir, f"worker_tab_error_{self.observer_user_id}.png")
                await self.page.screenshot(path=error_screenshot)
                print(f"Saved error screenshot to {error_screenshot}")

    async def set_workers_per_page(self, workers_per_page=80):
        """Set the number of workers per page."""
        print(f"Setting workers per page to {workers_per_page}...")
        try:
            # Try to find and click the page size selector
            await self.page.evaluate(f'''
                () => {{
                    // Find the page size selector
                    const selectors = Array.from(document.querySelectorAll('.ivu-page-options-sizer'));
                    if (selectors.length > 0) {{
                        selectors[0].click();
                        return true;
                    }}
                    return false;
                }}
            ''')
            
            await asyncio.sleep(1)
            
            # Try to select the desired page size
            size_selected = await self.page.evaluate(f'''
                (size) => {{
                    // Find all dropdown items
                    const items = Array.from(document.querySelectorAll('.ivu-select-dropdown .ivu-select-item'));
                    
                    // Find and click the item with the desired page size
                    const targetItem = items.find(item => item.textContent.trim() === size.toString());
                    if (targetItem) {{
                        targetItem.click();
                        return true;
                    }}
                    return false;
                }}
            ''', workers_per_page)
            
            if size_selected:
                print(f"Set page size to {workers_per_page}")
                await asyncio.sleep(3)  # Wait for page to reload with new size
            else:
                print(f"Could not set page size to {workers_per_page}")
        except Exception as e:
            print(f"Error setting workers per page: {str(e)}")

    async def get_worker_stats(self):
        """Get worker statistics from the worker tab."""
        print("Getting worker statistics...")
        worker_stats = []
        active_workers = 0
        inactive_workers = 0
        
        # Determine total pages
        total_pages = await self.page.evaluate('''
            () => {
                // Find pagination elements
                const pagination = document.querySelector('.ivu-page');
                if (!pagination) return 1;  // No pagination, only one page
                
                // Find the last page number
                const pageItems = Array.from(document.querySelectorAll('.ivu-page-item'));
                if (pageItems.length === 0) return 1;
                
                const lastPageItem = pageItems[pageItems.length - 1];
                const lastPage = parseInt(lastPageItem.textContent.trim());
                return isNaN(lastPage) ? 1 : lastPage;
            }
        ''')
        
        print(f"Found {total_pages} pages of workers")
        
        # Process each page
        current_page = 1
        while current_page <= total_pages:
            print(f"Processing page {current_page} of {total_pages}")
            
            # Extract worker data from current page
            page_worker_stats = await self.page.evaluate('''
                () => {
                    const workers = [];
                    
                    // Find the worker table
                    const table = document.querySelector('table');
                    if (!table) return workers;
                    
                    // Get all rows except header
                    const rows = Array.from(table.querySelectorAll('tbody tr'));
                    
                    // Process each row
                    rows.forEach(row => {
                        const cells = Array.from(row.querySelectorAll('td'));
                        if (cells.length < 7) return;  // Skip rows with insufficient cells
                        
                        // Extract data from cells
                        const worker = cells[0].textContent.trim();
                        const tenMinHashrate = cells[1].textContent.trim();
                        const oneHHashrate = cells[2].textContent.trim();
                        const h24Hashrate = cells[3].textContent.trim();
                        const rejectionRate = cells[4].textContent.trim();
                        const lastShareTime = cells[5].textContent.trim();
                        const connections24h = cells[6].textContent.trim();
                        
                        // Determine worker status based on last share time
                        let status = "active";
                        if (lastShareTime.toLowerCase().includes("never") || 
                            lastShareTime.toLowerCase().includes("offline") ||
                            (tenMinHashrate === "0 TH/s" && h24Hashrate === "0 TH/s")) {
                            status = "inactive";
                        }
                        
                        workers.push({
                            worker,
                            ten_min_hashrate: tenMinHashrate,
                            one_h_hashrate: oneHHashrate,
                            h24_hashrate: h24Hashrate,
                            rejection_rate: rejectionRate,
                            last_share_time: lastShareTime,
                            connections_24h: connections24h,
                            status
                        });
                    });
                    
                    return workers;
                }
            ''')
            
            if page_worker_stats and len(page_worker_stats) > 0:
                print(f"Found {len(page_worker_stats)} workers on page {current_page}")
                
                # Add timestamp and user info to each worker stat
                for worker_stat in page_worker_stats:
                    worker_stat["timestamp"] = format_timestamp()
                    worker_stat["observer_user_id"] = self.observer_user_id
                    worker_stat["coin_type"] = self.coin_type
                    worker_stat["hashrate_chart"] = ""  # No chart data
                    
                    # Count active/inactive workers
                    if worker_stat["status"] == "active":
                        active_workers += 1
                    else:
                        inactive_workers += 1
                
                # Add to overall worker stats
                worker_stats.extend(page_worker_stats)
                
                # Print progress update
                print(f"Progress: {len(worker_stats)} workers extracted so far ({active_workers} active, {inactive_workers} inactive)")
                
                # Print first row for verification on first page
                if current_page == 1 and worker_stats and len(worker_stats) > 0:
                    print(f"First worker data sample:")
                    print(f"  Worker: {worker_stats[0]['worker']}")
                    print(f"  Status: {worker_stats[0]['status']}")
                    print(f"  10-Min Hashrate: {worker_stats[0]['ten_min_hashrate']}")
                    print(f"  1H Hashrate: {worker_stats[0]['one_h_hashrate']}")
                    print(f"  24H Hashrate: {worker_stats[0]['h24_hashrate']}")
                    print(f"  Rejection Rate: {worker_stats[0]['rejection_rate']}")
                    print(f"  Last Share Time: {worker_stats[0]['last_share_time']}")
                    print(f"  Connections/24H: {worker_stats[0]['connections_24h']}")
            else:
                print(f"No workers found on page {current_page}")
                
                # Try alternative approach - get raw table HTML
                try:
                    table_html = await self.page.evaluate('''
                        () => {
                            const table = document.querySelector('table');
                            return table ? table.outerHTML : "";
                        }
                    ''')
                    
                    if table_html:
                        print("Table HTML retrieved, but could not extract worker data")
                        
                        # Try to extract data from raw HTML as last resort
                        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL)
                        if rows and len(rows) > 1:  # Skip header row
                            print(f"Found {len(rows) - 1} rows in HTML")
                            
                            for i in range(1, len(rows)):
                                cells = re.findall(r'<td[^>]*>(.*?)</td>', rows[i], re.DOTALL)
                                if cells and len(cells) >= 7:
                                    extract_text = lambda cell: re.sub(r'<[^>]*>', '', cell).strip()
                                    
                                    # Determine worker status based on last share time
                                    last_share_time = extract_text(cells[5])
                                    ten_min_hashrate = extract_text(cells[1])
                                    h24_hashrate = extract_text(cells[3])
                                    
                                    status = "active"
                                    if (last_share_time.lower().find("never") >= 0 or 
                                        last_share_time.lower().find("offline") >= 0 or
                                        (ten_min_hashrate == "0 TH/s" and h24_hashrate == "0 TH/s")):
                                        status = "inactive"
                                    
                                    worker_stat = {
                                        "worker": extract_text(cells[0]),
                                        "ten_min_hashrate": extract_text(cells[1]),
                                        "one_h_hashrate": extract_text(cells[2]),
                                        "h24_hashrate": extract_text(cells[3]),
                                        "rejection_rate": extract_text(cells[4]),
                                        "last_share_time": extract_text(cells[5]),
                                        "connections_24h": extract_text(cells[6]),
                                        "hashrate_chart": "",
                                        "status": status,
                                        "timestamp": format_timestamp(),
                                        "observer_user_id": self.observer_user_id,
                                        "coin_type": self.coin_type
                                    }
                                    worker_stats.append(worker_stat)
                                    
                                    # Count active/inactive workers
                                    if status == "active":
                                        active_workers += 1
                                    else:
                                        inactive_workers += 1
                            
                            if worker_stats:
                                print(f"Extracted {len(worker_stats)} workers from HTML")
                                print(f"Progress: {len(worker_stats)} workers extracted so far ({active_workers} active, {inactive_workers} inactive)")
                                
                                # Print first row for verification if this is the first data
                                if len(worker_stats) > 0 and current_page == 1:
                                    print(f"First worker data sample (from HTML):")
                                    print(f"  Worker: {worker_stats[0]['worker']}")
                                    print(f"  Status: {worker_stats[0]['status']}")
                                    print(f"  10-Min Hashrate: {worker_stats[0]['ten_min_hashrate']}")
                                    print(f"  1H Hashrate: {worker_stats[0]['one_h_hashrate']}")
                                    print(f"  24H Hashrate: {worker_stats[0]['h24_hashrate']}")
                                    print(f"  Rejection Rate: {worker_stats[0]['rejection_rate']}")
                                    print(f"  Last Share Time: {worker_stats[0]['last_share_time']}")
                                    print(f"  Connections/24H: {worker_stats[0]['connections_24h']}")
                    else:
                        print("No table found on the page")
                        
                        # Try to get table from iframe if present
                        iframe_count = await self.page.evaluate('''
                            () => document.querySelectorAll('iframe').length
                        ''')
                        
                        if iframe_count > 0:
                            print(f"Found {iframe_count} iframes, checking for content")
                            
                            for i in range(iframe_count):
                                try:
                                    frame = self.page.frames[i + 1]  # Skip main frame
                                    if frame:
                                        print(f"Checking iframe {i+1}")
                                        # Try to find table in iframe
                                        table_html = await frame.evaluate('''
                                            () => {
                                                const table = document.querySelector('table');
                                                return table ? table.outerHTML : "";
                                            }
                                        ''')
                                        
                                        if table_html:
                                            print(f"Found table in iframe {i+1}")
                                            # Process table HTML from iframe
                                            rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL)
                                            if rows and len(rows) > 1:  # Skip header row
                                                print(f"Found {len(rows) - 1} rows in iframe HTML")
                                                
                                                for j in range(1, len(rows)):
                                                    cells = re.findall(r'<td[^>]*>(.*?)</td>', rows[j], re.DOTALL)
                                                    if cells and len(cells) >= 7:
                                                        extract_text = lambda cell: re.sub(r'<[^>]*>', '', cell).strip()
                                                        
                                                        # Determine worker status based on last share time
                                                        last_share_time = extract_text(cells[5])
                                                        ten_min_hashrate = extract_text(cells[1])
                                                        h24_hashrate = extract_text(cells[3])
                                                        
                                                        status = "active"
                                                        if (last_share_time.lower().find("never") >= 0 or 
                                                            last_share_time.lower().find("offline") >= 0 or
                                                            (ten_min_hashrate == "0 TH/s" and h24_hashrate == "0 TH/s")):
                                                            status = "inactive"
                                                        
                                                        worker_stat = {
                                                            "worker": extract_text(cells[0]),
                                                            "ten_min_hashrate": extract_text(cells[1]),
                                                            "one_h_hashrate": extract_text(cells[2]),
                                                            "h24_hashrate": extract_text(cells[3]),
                                                            "rejection_rate": extract_text(cells[4]),
                                                            "last_share_time": extract_text(cells[5]),
                                                            "connections_24h": extract_text(cells[6]),
                                                            "hashrate_chart": "",
                                                            "status": status,
                                                            "timestamp": format_timestamp(),
                                                            "observer_user_id": self.observer_user_id,
                                                            "coin_type": self.coin_type
                                                        }
                                                        worker_stats.append(worker_stat)
                                                        
                                                        # Count active/inactive workers
                                                        if status == "active":
                                                            active_workers += 1
                                                        else:
                                                            inactive_workers += 1
                                                
                                                if worker_stats:
                                                    print(f"Extracted {len(worker_stats)} workers from iframe HTML")
                                                    print(f"Progress: {len(worker_stats)} workers extracted so far ({active_workers} active, {inactive_workers} inactive)")
                                                    break  # Stop checking other iframes
                                except Exception as e:
                                    print(f"Error checking iframe {i+1}: {str(e)}")
                except Exception as e:
                    print(f"Error getting table HTML: {str(e)}")
            
            # Go to next page if there are multiple pages
            if current_page < total_pages:
                # Ensure no modals are present before navigation
                await self.ensure_no_modals()
                
                next_clicked = await self.page.evaluate('''
                    () => {
                        // Find the next page button
                        const nextButton = document.querySelector('li.ivu-page-next:not(.ivu-page-disabled)');
                        if (nextButton) {
                            nextButton.click();
                            return true;
                        }
                        return false;
                    }
                ''')
                
                if next_clicked:
                    print(f"Navigated to page {current_page + 1}")
                    await asyncio.sleep(5)  # Increased wait time for page to load
                else:
                    print("Next page button not found or disabled")
                    break
            
            current_page += 1
        
        print(f"\n===== Worker Extraction Summary =====")
        print(f"Total workers extracted: {len(worker_stats)}")
        print(f"Active workers: {active_workers}")
        print(f"Inactive workers: {inactive_workers}")
        
        # If no workers were found, create a placeholder entry
        if not worker_stats:
            print("No worker data extracted, creating placeholder entry")
            worker_stats.append({
                "worker": "No workers found",
                "ten_min_hashrate": "0",
                "one_h_hashrate": "0",
                "h24_hashrate": "0",
                "rejection_rate": "0",
                "last_share_time": "",
                "connections_24h": "0",
                "hashrate_chart": "",
                "status": "unknown",
                "timestamp": format_timestamp(),
                "observer_user_id": self.observer_user_id,
                "coin_type": self.coin_type,
                "note": "Data extraction failed, please check the website manually"
            })
        
        return worker_stats

    async def capture_worker_table_screenshot(self):
        """Capture a screenshot of the worker table."""
        if not self.debug:
            return {"screenshot": None}
            
        try:
            # Take screenshot of worker table
            screenshot_path = os.path.join(
                self.output_dir,
                f"worker_table_{self.observer_user_id}_{datetime.now().strftime('%Y%m%d_%H%M')}.png"
            )
            await self.page.screenshot(path=screenshot_path)
            print(f"Worker table screenshot saved to: {screenshot_path}")
            return {"screenshot": screenshot_path}
        except Exception as e:
            print(f"Error capturing worker table screenshot: {str(e)}")
            return {"screenshot": None}

    async def run(self):
        """Run the worker scraper."""
        try:
            await self._setup_browser()
            await self.navigate_to_observer_page()
            
            # Navigate to Worker tab
            await self.navigate_to_worker_tab()
            
            # Set workers per page to 80
            await self.set_workers_per_page(80)
            
            # Get worker stats
            worker_stats = await self.get_worker_stats()
            
            # Save worker stats to file
            current_time = datetime.now().strftime('%Y%m%d_%H%M')
            output_file = os.path.join(
                self.output_dir,
                f"worker_stats_{self.observer_user_id}_{current_time}.json"
            )
            
            with open(output_file, 'w') as f:
                json.dump(worker_stats, f, indent=2)
            
            print(f"Worker stats saved to: {output_file}")
            
            # Capture worker table screenshot
            screenshot_info = await self.capture_worker_table_screenshot()
            
            print("\n===== Worker Scraping Completed Successfully =====")
            print(f"Account: {self.observer_user_id} ({self.coin_type})")
            print(f"Total workers extracted: {len(worker_stats)}")
            print(f"Worker stats saved to: {output_file}")
            if screenshot_info["screenshot"]:
                print(f"Worker table screenshot saved to: {screenshot_info['screenshot']}")
            
            return {
                "worker_stats": worker_stats,
                "worker_stats_file": output_file,
                "screenshot": screenshot_info["screenshot"],
                "worker_count": len(worker_stats)
            }
        except Exception as e:
            print(f"Error during worker scraping: {str(e)}")
            print(traceback.format_exc())
            return {
                "error": str(e),
                "worker_stats": [],
                "worker_stats_file": None,
                "screenshot": None,
                "worker_count": 0
            }
        finally:
            if self.browser:
                await self.browser.close()
                print("Browser closed")


async def save_to_supabase(supabase, workers_data):
    """Save worker data to Supabase."""
    if not workers_data:
        print("No worker data to save to Supabase")
        return False
    
    try:
        # Use batch insert for better performance (max 1000 records per batch)
        batch_size = 100
        total_workers = len(workers_data)
        success_count = 0
        
        print(f"\n===== Uploading {total_workers} Workers to Supabase =====")
        print(f"Using batch size of {batch_size} workers per request")
        
        for i in range(0, total_workers, batch_size):
            batch = workers_data[i:i+batch_size]
            batch_num = i//batch_size + 1
            total_batches = (total_workers+batch_size-1)//batch_size
            print(f"Uploading batch {batch_num}/{total_batches} ({len(batch)} workers)")
            
            try:
                result = supabase.table("mining_workers").insert(batch).execute()
                if hasattr(result, 'data') and result.data:
                    success_count += len(batch)
                    print(f"✅ Successfully uploaded batch {batch_num}/{total_batches}")
                else:
                    print(f"❌ Error uploading batch {batch_num}/{total_batches}: {result}")
            except Exception as batch_error:
                print(f"❌ Error uploading batch {batch_num}/{total_batches}: {batch_error}")
                # Fall back to individual inserts for this batch
                print(f"Falling back to individual inserts for batch {batch_num}")
                batch_success = 0
                
                for worker in batch:
                    try:
                        result = supabase.table("mining_workers").insert(worker).execute()
                        if hasattr(result, 'data') and result.data:
                            success_count += 1
                            batch_success += 1
                        else:
                            print(f"❌ Error saving worker {worker['worker']}")
                    except Exception as worker_error:
                        print(f"❌ Error saving worker {worker['worker']}: {str(worker_error)[:100]}...")
                
                print(f"Individual inserts: {batch_success}/{len(batch)} workers saved successfully")
        
        print(f"\n===== Supabase Upload Summary =====")
        print(f"Total workers: {total_workers}")
        print(f"Successfully uploaded: {success_count}")
        print(f"Failed: {total_workers - success_count}")
        
        if total_workers > 0:
            success_rate = (success_count / total_workers) * 100
            print(f"Success rate: {success_rate:.1f}%")
        
        return success_count > 0
    except Exception as e:
        print(f"❌ Error saving to Supabase: {e}")
        print(traceback.format_exc())
        return False


async def process_account(browser, output_dir, supabase, account, debug=False):
    """Process a single account."""
    try:
        # Extract account details
        account_name = account.get("account_name", "Unknown")
        access_key = account.get("access_key", "")
        user_id = account.get("user_id", "")
        coin_type = account.get("coin_type", "BTC")
        
        print(f"\n===== Processing account: {account_name} ({user_id}) =====")
        
        # Skip if missing required fields
        if not access_key or not user_id:
            print(f"❌ Skipping account {account_name}: Missing required fields")
            return False
        
        # Create a new page for this account
        page = await browser.new_page()
        page.set_default_timeout(60000)  # 60 second timeout for all operations
        
        try:
            # Initialize scraper
            scraper = AntpoolWorkerScraper(
                access_key=access_key,
                observer_user_id=user_id,
                coin_type=coin_type,
                output_dir=output_dir,
                debug=debug
            )
            
            # Set browser and page
            scraper.browser = browser
            scraper.page = page
            
            # Run scraper
            result = await scraper.run()
            
            # Save to Supabase
            if supabase and result.get("worker_stats"):
                await save_to_supabase(supabase, result["worker_stats"])
            
            # Update last_scraped_at in account_credentials
            if supabase:
                try:
                    supabase.table("account_credentials").update({"last_scraped_at": format_timestamp()}).eq("user_id", user_id).execute()
                    print(f"✅ Updated last_scraped_at for {user_id}")
                except Exception as e:
                    print(f"❌ Error updating last_scraped_at: {e}")
            
            print(f"✅ Successfully processed account: {account_name}")
            return True
            
        finally:
            # Close the page
            await page.close()
            
    except Exception as e:
        print(f"❌ Error processing account {account.get('account_name', 'Unknown')}: {e}")
        print(traceback.format_exc())
        return False


async def fetch_accounts_from_supabase(supabase):
    """Fetch accounts from Supabase."""
    try:
        # First try using the RPC function
        try:
            print("Attempting to fetch accounts using RPC function...")
            response = supabase.rpc('get_all_active_accounts').execute()
            accounts = response.data
            if accounts:
                print(f"✅ Successfully fetched {len(accounts)} accounts using RPC function")
                return accounts
        except Exception as rpc_error:
            print(f"❌ Error fetching accounts using RPC function: {rpc_error}")
            # Continue to fallback method
        
        # Fallback: direct query
        print("Falling back to direct query...")
        response = supabase.table("account_credentials").select("*").eq("is_active", True).order("priority.desc,last_scraped_at.asc.nullsfirst").execute()
        accounts = response.data
        print(f"✅ Successfully fetched {len(accounts)} accounts using direct query")
        return accounts
        
    except Exception as e:
        print(f"❌ Error fetching accounts from Supabase: {e}")
        print(traceback.format_exc())
        return []


async def main_async(args):
    """Main async function."""
    print("\n===== Starting Antpool Worker Scraper =====")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    print(f"Output directory: {args.output_dir}")
    
    # Initialize Supabase client
    supabase = None
    if not args.skip_supabase:
        supabase = get_supabase_client()
        if not supabase:
            print("❌ Failed to initialize Supabase client")
            return 1
    
    # Get accounts to scrape
    accounts = []
    if args.access_key and args.user_id:
        # Use command-line arguments
        accounts = [{
            "account_name": args.user_id,
            "access_key": args.access_key,
            "user_id": args.user_id,
            "coin_type": args.coin_type
        }]
        print(f"Using command-line account: {args.user_id}")
    elif supabase:
        # Fetch accounts from Supabase
        accounts = await fetch_accounts_from_supabase(supabase)
    
    if not accounts:
        print("❌ No accounts to scrape. Exiting.")
        return 1
    
    print(f"Found {len(accounts)} accounts to scrape")
    
    # Initialize browser
    print("Initializing browser...")
    browser = await setup_browser(headless=True)
    print("✅ Browser initialized successfully")
    
    try:
        # Process each account
        results = []
        for i, account in enumerate(accounts):
            print(f"\n===== Processing Account {i+1}/{len(accounts)} =====")
            result = await process_account(browser, args.output_dir, supabase, account, args.debug)
            results.append(result)
        
        # Print summary
        success_count = sum(1 for r in results if r)
        print("\n===== Worker Scraper Summary =====")
        print(f"Total accounts processed: {len(accounts)}")
        print(f"Successful: {success_count}")
        print(f"Failed: {len(accounts) - success_count}")
        
        if len(accounts) > 0:
            success_rate = (success_count / len(accounts)) * 100
            print(f"Success rate: {success_rate:.1f}%")
        
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return 0
    finally:
        # Close browser
        await browser.close()
        print("Browser closed")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Antpool Worker Scraper - Multi-Account Version")
    parser.add_argument("--access_key", help="Antpool access key (optional if using Supabase)")
    parser.add_argument("--user_id", help="Antpool observer user ID (optional if using Supabase)")
    parser.add_argument("--coin_type", default="BTC", help="Coin type (default: BTC)")
    parser.add_argument("--output_dir", default="./output", help="Output directory for JSON and screenshots")
    parser.add_argument("--skip_supabase", action="store_true", help="Skip Supabase integration")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    
    args = parser.parse_args()
    
    # Run async main
    try:
        return asyncio.run(main_async(args))
    except Exception as e:
        print(f"❌ Error in main: {e}")
        print(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
