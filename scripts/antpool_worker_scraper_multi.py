#!/usr/bin/env python3
"""
Antpool Worker Stats Scraper

This script scrapes worker statistics from Antpool's observer page and saves the data
to a JSON file and Supabase. It navigates to the Worker tab and extracts data for all workers,
setting the page size to 80 results per page.
"""

import os
import sys
import json
import asyncio
import logging
import re
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from utils.supabase_utils import get_supabase_client
    from utils.browser_utils import setup_browser, handle_consent_dialog, take_screenshot
    from utils.data_utils import save_json_to_file
except ImportError as e:
    logger.error(f"Import error: {e}")
    # Try relative import as fallback
    try:
        from utils.supabase_utils import get_supabase_client
        from utils.browser_utils import setup_browser, handle_consent_dialog, take_screenshot
        from utils.data_utils import save_json_to_file
    except ImportError as e2:
        logger.error(f"Fallback import also failed: {e2}")
        sys.exit(1)

class AntpoolWorkerScraper:
    def __init__(self, access_key, observer_user_id, coin_type="BTC", output_dir=None):
        """Initialize the Antpool worker scraper with the given parameters."""
        self.access_key = access_key
        self.observer_user_id = observer_user_id
        self.coin_type = coin_type
        self.output_dir = output_dir or os.path.join(os.getcwd(), "output")

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
        logger.info("Launching browser...")
        try:
            # Initialize browser - using the same approach as dashboard scraper
            self.browser = await setup_browser(headless=True)
            logger.info("Browser initialized successfully")
            
            # Create a new page - using the same approach as dashboard scraper
            self.page = await self.browser.new_page()
            self.page.set_default_timeout(15000)  # 15 second timeout
            
            logger.info("Browser setup complete")
        except Exception as e:
            logger.error(f"CRITICAL ERROR launching browser: {str(e)}")
            raise

    async def handle_consent_dialog(self):
        """Handle the informed consent dialog."""
        logger.info("Handling consent dialog...")
        try:
            # Wait for the consent dialog to appear
            try:
                await self.page.wait_for_selector("text=INFORMED CONSENT", timeout=5000)
                logger.info("Consent dialog found")
                
                # Try multiple approaches to dismiss the dialog
                
                # Approach 1: Click "Got it" button
                try:
                    await self.page.click("text=Got it", timeout=3000)
                    logger.info("Clicked 'Got it' button")
                    await asyncio.sleep(1)
                except Exception as e:
                    logger.info(f"Could not click 'Got it' button: {str(e)}")
                
                # Approach 2: Click "Confirm" button
                try:
                    await self.page.click("text=Confirm", timeout=3000)
                    logger.info("Clicked 'Confirm' button")
                    await asyncio.sleep(1)
                except Exception as e:
                    logger.info(f"Could not click 'Confirm' button: {str(e)}")
                
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
                    logger.info("Used JavaScript to dismiss consent dialog")
                    await asyncio.sleep(1)
                except Exception as e:
                    logger.info(f"Could not use JavaScript to dismiss dialog: {str(e)}")
                
                # Verify the modal is gone
                is_modal_gone = await self.page.evaluate('''
                    () => {
                        const modal = document.querySelector('.ivu-modal-wrap');
                        return !modal || modal.style.display === 'none' || 
                               !modal.classList.contains('ivu-modal-show');
                    }
                ''')
                
                if is_modal_gone:
                    logger.info("Consent dialog successfully dismissed")
                else:
                    logger.info("Consent dialog may still be present")
                    
                    # Last resort: Press Escape key
                    try:
                        await self.page.keyboard.press('Escape')
                        logger.info("Pressed Escape key to dismiss dialog")
                        await asyncio.sleep(1)
                    except Exception as e:
                        logger.info(f"Could not press Escape key: {str(e)}")
            except Exception as e:
                logger.info(f"No consent dialog found: {str(e)}")
            
            # Check for cookie banner
            try:
                await self.page.click("button.cookie-btn", timeout=3000)
                logger.info("Clicked cookie banner button")
                await asyncio.sleep(1)
            except Exception:
                logger.info("Cookie banner not found or already accepted")
                
            logger.info("Consent dialog handling completed")
        except Exception as e:
            logger.error(f"Error during consent dialog handling: {str(e)}")

    async def navigate_to_observer_page(self):
        """Navigate to the Antpool observer page."""
        logger.info(f"Navigating to observer page: {self.base_url}")
        await self.page.goto(self.base_url, wait_until="networkidle")
        logger.info("Page loaded")
        
        # Handle consent dialog
        await handle_consent_dialog(self.page)

        logger.info("Waiting for hashrate chart...")
        # Wait for hashrate chart with retry logic
        max_retries = 3
        retry_delay = 5
        for attempt in range(max_retries):
            try:
                await self.page.wait_for_selector("#hashrate-chart", timeout=45000)
                logger.info("Hashrate chart loaded successfully")
                break
            except Exception as e:
                logger.info(f"Attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    logger.error("Failed to load hashrate chart after multiple attempts")
                    raise
                await asyncio.sleep(retry_delay)
                
        # Ensure any remaining modals are dismissed
        await self.ensure_no_modals()

    async def ensure_no_modals(self):
        """Ensure no modals are present on the page."""
        logger.info("Ensuring no modals are present...")
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
            logger.info("Removed any modal elements")
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Error ensuring no modals: {str(e)}")

    async def navigate_to_worker_tab(self):
        """Navigate to the Worker tab."""
        logger.info("Navigating to Worker tab...")
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
                logger.info("Clicked Worker tab using JavaScript")
            else:
                logger.info("Could not click Worker tab with JavaScript")
                
                # Try direct click approach
                try:
                    await self.page.click("div.ivu-tabs-tab:nth-child(2)")
                    logger.info("Clicked second tab assuming it's Worker tab")
                except Exception as e:
                    logger.error(f"Could not click second tab: {str(e)}")
            
            # Wait for the click to take effect
            await asyncio.sleep(5)  # Increased wait time
            
            # Take screenshot after clicking Worker tab
            screenshot_path = os.path.join(self.output_dir, "worker_tab_clicked.png")
            await self.page.screenshot(path=screenshot_path)
            logger.info(f"Screenshot saved after clicking Worker tab: {screenshot_path}")
            
            # Wait for worker table to load with retry logic and longer timeout
            max_retries = 5  # Increased retries
            for attempt in range(max_retries):
                try:
                    await self.page.wait_for_selector("table", timeout=15000)  # Increased timeout
                    logger.info("Worker table loaded")
                    break
                except Exception as e:
                    logger.info(f"Attempt {attempt + 1} to find table failed: {e}")
                    if attempt == max_retries - 1:
                        logger.error("Failed to find worker table after multiple attempts")
                        raise
                    await asyncio.sleep(3)  # Increased wait between retries
                    
            # Ensure any remaining modals are dismissed again
            await self.ensure_no_modals()
            
            # Check for iframes that might contain the table
            iframe_count = await self.page.evaluate('''
                () => document.querySelectorAll('iframe').length
            ''')
            
            if iframe_count > 0:
                logger.info(f"Found {iframe_count} iframes, checking for content")
                
                # Try to access iframe content
                for i in range(iframe_count):
                    try:
                        frame = self.page.frames[i + 1]  # Skip main frame
                        if frame:
                            logger.info(f"Checking iframe {i+1}")
                            # Try to find table in iframe
                            table_in_frame = await frame.evaluate('''
                                () => document.querySelectorAll('table').length > 0
                            ''')
                            if table_in_frame:
                                logger.info(f"Found table in iframe {i+1}")
                                # Use this frame for further operations
                                self.frame = frame
                                break
                    except Exception as e:
                        logger.error(f"Error checking iframe {i+1}: {str(e)}")
            
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
                logger.info("Loading indicators disappeared")
            except Exception as e:
                logger.error(f"Loading indicators may still be present: {str(e)}")
            
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
            logger.error(f"Error navigating to Worker tab: {str(e)}")
            raise

    async def set_workers_per_page(self, page_size=80):
        """Set the number of workers displayed per page."""
        logger.info(f"Setting workers per page to {page_size}...")
        try:
            # First ensure no modals are present
            await self.ensure_no_modals()
            
            # Try to find and click the page size dropdown
            try:
                # Try to find the dropdown using various selectors
                selectors = [
                    ".ivu-page-options-sizer",
                    ".ant-pagination-options-size-changer",
                    "[class*='page-size']",
                    "[class*='pageSize']",
                    "select[class*='size']",
                    ".ivu-select"
                ]
                
                dropdown_clicked = False
                for selector in selectors:
                    try:
                        await self.page.click(selector, timeout=3000)
                        logger.info(f"Clicked page size dropdown using selector: {selector}")
                        dropdown_clicked = True
                        break
                    except Exception:
                        continue
                
                if not dropdown_clicked:
                    # Try using JavaScript to find and click the dropdown
                    dropdown_clicked = await self.page.evaluate('''
                        () => {
                            // Find elements that might be the page size dropdown
                            const possibleDropdowns = Array.from(document.querySelectorAll(
                                '.ivu-select, .ant-select, [class*="page-size"], [class*="pageSize"], select'
                            ));
                            
                            // Try to find one that contains text like "10 / page"
                            for (const dropdown of possibleDropdowns) {
                                if (dropdown.textContent.match(/\\d+\\s*\\/\\s*page/) || 
                                    dropdown.textContent.match(/\\d+\\s*per\\s*page/)) {
                                    dropdown.click();
                                    return true;
                                }
                            }
                            
                            // If not found, try clicking any dropdown-like element
                            if (possibleDropdowns.length > 0) {
                                possibleDropdowns[0].click();
                                return true;
                            }
                            
                            return false;
                        }
                    ''')
                    
                    if dropdown_clicked:
                        logger.info("Clicked page size dropdown using JavaScript")
                    else:
                        logger.error("Could not find page size dropdown")
                
                # Wait for dropdown options to appear
                await asyncio.sleep(2)
                
                # Try to select the desired page size
                page_size_selected = False
                
                # Try direct click on the option
                try:
                    # Try various selectors for the option
                    option_selectors = [
                        f".ivu-select-dropdown .ivu-select-item:contains('{page_size}')",
                        f".ant-select-dropdown .ant-select-item:contains('{page_size}')",
                        f"[class*='select-dropdown'] [class*='select-item']:contains('{page_size}')",
                        f"option[value='{page_size}']"
                    ]
                    
                    for selector in option_selectors:
                        try:
                            await self.page.click(selector, timeout=3000)
                            logger.info(f"Selected {page_size} workers per page using selector: {selector}")
                            page_size_selected = True
                            break
                        except Exception:
                            continue
                except Exception as e:
                    logger.info(f"Could not select page size directly: {str(e)}")
                
                if not page_size_selected:
                    # Try using JavaScript to select the option
                    page_size_selected = await self.page.evaluate(f'''
                        () => {{
                            // Find all dropdown options
                            const options = Array.from(document.querySelectorAll(
                                '.ivu-select-dropdown .ivu-select-item, ' +
                                '.ant-select-dropdown .ant-select-item, ' +
                                '[class*="select-dropdown"] [class*="select-item"], ' +
                                'option'
                            ));
                            
                            // Try to find the option with the desired page size
                            for (const option of options) {{
                                if (option.textContent.includes('{page_size}')) {{
                                    option.click();
                                    return true;
                                }}
                            }}
                            
                            return false;
                        }}
                    ''')
                    
                    if page_size_selected:
                        logger.info(f"Selected {page_size} workers per page using JavaScript")
                    else:
                        logger.error(f"Could not select {page_size} workers per page")
                
                # Wait for the page to update
                await asyncio.sleep(3)
                
                # Take screenshot after setting page size
                screenshot_path = os.path.join(self.output_dir, f"page_size_{page_size}.png")
                await self.page.screenshot(path=screenshot_path)
                logger.info(f"Screenshot saved after setting page size: {screenshot_path}")
                
            except Exception as e:
                logger.error(f"Error setting workers per page: {str(e)}")
                
        except Exception as e:
            logger.error(f"Error setting workers per page: {str(e)}")

    async def extract_worker_data(self):
        """Extract worker data from the table."""
        logger.info("Extracting worker data...")
        try:
            # First ensure no modals are present
            await self.ensure_no_modals()
            
            # Get total number of workers
            total_workers_text = await self.page.evaluate('''
                () => {
                    // Try to find text showing total workers
                    const elements = Array.from(document.querySelectorAll('*'));
                    for (const el of elements) {
                        if (el.textContent && el.textContent.match(/Total\\s*:\\s*\\d+/)) {
                            return el.textContent;
                        }
                    }
                    return null;
                }
            ''')
            
            total_workers = 0
            if total_workers_text:
                match = re.search(r'Total\s*:\s*(\d+)', total_workers_text)
                if match:
                    total_workers = int(match.group(1))
                    logger.info(f"Total workers found: {total_workers}")
            
            # Extract worker data from the table
            workers_data = await self.page.evaluate('''
                () => {
                    const workers = [];
                    
                    // Find the table
                    const tables = document.querySelectorAll('table');
                    if (tables.length === 0) return workers;
                    
                    // Use the first table found
                    const table = tables[0];
                    
                    // Get table rows (skip header row)
                    const rows = table.querySelectorAll('tbody tr');
                    
                    // Process each row
                    for (const row of rows) {
                        const cells = row.querySelectorAll('td');
                        if (cells.length < 5) continue;
                        
                        // Extract data from cells
                        const worker = {
                            worker_name: cells[0].textContent.trim(),
                            ten_min_hashrate: cells[1].textContent.trim(),
                            one_h_hashrate: cells[2].textContent.trim(),
                            h24_hashrate: cells[3].textContent.trim(),
                            rejection_rate: cells[4].textContent.trim(),
                            last_share_time: cells[5].textContent.trim(),
                            connections_24h: cells[6].textContent.trim()
                        };
                        
                        workers.push(worker);
                    }
                    
                    return workers;
                }
            ''')
            
            if not workers_data or len(workers_data) == 0:
                logger.warning("No worker data found in table, trying alternative extraction method")
                
                # Try alternative extraction method
                workers_data = await self.page.evaluate('''
                    () => {
                        const workers = [];
                        
                        // Find all elements that might contain worker data
                        const elements = document.querySelectorAll('[class*="row"], [class*="item"], [class*="list-item"]');
                        
                        for (const el of elements) {
                            // Check if this element contains worker data
                            const text = el.textContent;
                            if (!text.includes('TH/s') && !text.includes('PH/s')) continue;
                            
                            // Try to extract worker name and hashrates
                            const workerNameEl = el.querySelector('[class*="name"], [class*="worker"], [class*="id"]');
                            const hashrateEls = el.querySelectorAll('[class*="hashrate"], [class*="rate"]');
                            
                            if (workerNameEl && hashrateEls.length > 0) {
                                const worker = {
                                    worker_name: workerNameEl.textContent.trim(),
                                    ten_min_hashrate: hashrateEls[0]?.textContent.trim() || 'N/A',
                                    one_h_hashrate: hashrateEls[1]?.textContent.trim() || 'N/A',
                                    h24_hashrate: hashrateEls[2]?.textContent.trim() || 'N/A',
                                    rejection_rate: 'N/A',
                                    last_share_time: 'N/A',
                                    connections_24h: 'N/A'
                                };
                                
                                workers.push(worker);
                            }
                        }
                        
                        return workers;
                    }
                ''')
            
            logger.info(f"Extracted {len(workers_data)} workers from current page")
            
            # Process worker data
            processed_workers = []
            for worker in workers_data:
                # Determine if worker is active based on last share time
                is_active = True
                if 'last_share_time' in worker and worker['last_share_time']:
                    # If last share time is more than 24 hours ago, consider inactive
                    try:
                        last_share_time = worker['last_share_time']
                        if 'day' in last_share_time.lower() or 'week' in last_share_time.lower():
                            is_active = False
                    except Exception:
                        pass
                
                processed_worker = {
                    'worker_name': worker.get('worker_name', 'Unknown'),
                    'ten_min_hashrate': worker.get('ten_min_hashrate', 'N/A'),
                    'one_h_hashrate': worker.get('one_h_hashrate', 'N/A'),
                    'h24_hashrate': worker.get('h24_hashrate', 'N/A'),
                    'rejection_rate': worker.get('rejection_rate', 'N/A'),
                    'last_share_time': worker.get('last_share_time', 'N/A'),
                    'connections_24h': worker.get('connections_24h', 'N/A'),
                    'is_active': is_active,
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                processed_workers.append(processed_worker)
            
            return processed_workers
            
        except Exception as e:
            logger.error(f"Error extracting worker data: {str(e)}")
            return []

    async def navigate_to_next_page(self):
        """Navigate to the next page of workers."""
        logger.info("Navigating to next page...")
        try:
            # First ensure no modals are present
            await self.ensure_no_modals()
            
            # Try to find and click the next page button
            next_page_clicked = False
            
            # Try direct click on next page button
            try:
                await self.page.click("li.ivu-page-next", timeout=5000)
                logger.info("Clicked next page button")
                next_page_clicked = True
            except Exception as e:
                logger.info(f"Could not click next page button directly: {str(e)}")
            
            if not next_page_clicked:
                # Try using JavaScript to click next page button
                next_page_clicked = await self.page.evaluate('''
                    () => {
                        // Find elements that might be the next page button
                        const nextButtons = Array.from(document.querySelectorAll(
                            '.ivu-page-next, .ant-pagination-next, [class*="next"], [aria-label="Next Page"]'
                        ));
                        
                        // Try to find one that's not disabled
                        for (const button of nextButtons) {
                            if (!button.classList.contains('disabled') && 
                                !button.classList.contains('ivu-page-disabled') &&
                                !button.hasAttribute('disabled')) {
                                button.click();
                                return true;
                            }
                        }
                        
                        return false;
                    }
                ''')
                
                if next_page_clicked:
                    logger.info("Clicked next page button using JavaScript")
                else:
                    logger.info("Could not find next page button or reached last page")
                    return False
            
            # Wait for the page to update
            await asyncio.sleep(3)
            
            # Check if navigation was successful
            page_changed = await self.page.evaluate('''
                () => {
                    // Find the active page number
                    const activePageEl = document.querySelector('.ivu-page-item-active, .ant-pagination-item-active');
                    if (!activePageEl) return false;
                    
                    // Check if the active page number is greater than 1
                    const pageNum = parseInt(activePageEl.textContent.trim());
                    return !isNaN(pageNum) && pageNum > 1;
                }
            ''')
            
            if page_changed:
                logger.info("Successfully navigated to next page")
                return True
            else:
                logger.info("Navigation to next page failed or reached last page")
                return False
            
        except Exception as e:
            logger.error(f"Error navigating to next page: {str(e)}")
            return False

    async def scrape_workers(self):
        """Scrape worker data from all pages."""
        logger.info("Starting worker scraping process...")
        try:
            # Set up browser
            await self._setup_browser()
            
            # Navigate to observer page
            await self.navigate_to_observer_page()
            
            # Take screenshot of dashboard
            screenshot_path = os.path.join(self.output_dir, f"dashboard_{self.observer_user_id}.png")
            await self.page.screenshot(path=screenshot_path)
            logger.info(f"Dashboard screenshot saved to: {screenshot_path}")
            
            # Navigate to Worker tab
            await self.navigate_to_worker_tab()
            
            # Take screenshot of worker tab
            screenshot_path = os.path.join(self.output_dir, f"worker_tab_{self.observer_user_id}.png")
            await self.page.screenshot(path=screenshot_path)
            logger.info(f"Worker tab screenshot saved to: {screenshot_path}")
            
            # Set workers per page to 80
            await self.set_workers_per_page(80)
            
            # Extract worker data from all pages
            all_workers = []
            page_num = 1
            
            while True:
                logger.info(f"Processing page {page_num}...")
                
                # Extract worker data from current page
                workers = await self.extract_worker_data()
                
                if workers and len(workers) > 0:
                    logger.info(f"Found {len(workers)} workers on page {page_num}")
                    all_workers.extend(workers)
                    
                    # Print first worker data on first page for verification
                    if page_num == 1 and len(workers) > 0:
                        logger.info("First worker data sample:")
                        logger.info(f"  Worker Name: {workers[0]['worker_name']}")
                        logger.info(f"  10-Min Hashrate: {workers[0]['ten_min_hashrate']}")
                        logger.info(f"  24H Hashrate: {workers[0]['h24_hashrate']}")
                        logger.info(f"  Rejection Rate: {workers[0]['rejection_rate']}")
                        logger.info(f"  Last Share Time: {workers[0]['last_share_time']}")
                        logger.info(f"  Status: {'Active' if workers[0]['is_active'] else 'Inactive'}")
                else:
                    logger.warning(f"No workers found on page {page_num}")
                    # If no workers found on first page, something is wrong
                    if page_num == 1:
                        logger.error("No workers found on first page, aborting")
                        break
                
                # Navigate to next page
                has_next_page = await self.navigate_to_next_page()
                if not has_next_page:
                    logger.info("No more pages to process")
                    break
                
                page_num += 1
                
                # Limit to 10 pages as a safety measure
                if page_num > 10:
                    logger.warning("Reached maximum page limit (10), stopping")
                    break
            
            # Close browser
            await self.browser.close()
            
            # Count active and inactive workers
            active_workers = sum(1 for worker in all_workers if worker['is_active'])
            inactive_workers = len(all_workers) - active_workers
            
            logger.info(f"Scraping completed. Total workers: {len(all_workers)}")
            logger.info(f"Active workers: {active_workers}, Inactive workers: {inactive_workers}")
            
            return all_workers
            
        except Exception as e:
            logger.error(f"Error during worker scraping: {str(e)}")
            if self.browser:
                await self.browser.close()
            return []

    async def save_to_file(self, workers):
        """Save worker data to a JSON file."""
        if not workers:
            logger.warning("No worker data to save")
            return None
        
        try:
            # Create timestamp for filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            
            # Create filename
            filename = f"{timestamp}_{self.observer_user_id}_{self.coin_type}_workers.json"
            filepath = os.path.join(self.output_dir, filename)
            
            # Save to file
            with open(filepath, 'w') as f:
                json.dump(workers, f, indent=2)
            
            logger.info(f"Worker data saved to {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving worker data to file: {str(e)}")
            return None

    async def upload_to_supabase(self, workers, supabase):
        """Upload worker data to Supabase."""
        if not workers:
            logger.warning("No worker data to upload to Supabase")
            return 0
        
        try:
            logger.info(f"Uploading {len(workers)} workers to Supabase...")
            
            # Prepare data for upload
            upload_data = []
            for worker in workers:
                # Extract numeric values from hashrates
                ten_min_hashrate = worker['ten_min_hashrate']
                one_h_hashrate = worker['one_h_hashrate']
                h24_hashrate = worker['h24_hashrate']
                rejection_rate = worker['rejection_rate']
                
                # Extract numeric values using regex
                ten_min_value = re.search(r'([\d.]+)', ten_min_hashrate)
                one_h_value = re.search(r'([\d.]+)', one_h_hashrate)
                h24_value = re.search(r'([\d.]+)', h24_hashrate)
                rejection_value = re.search(r'([\d.]+)', rejection_rate)
                
                # Prepare record for Supabase
                record = {
                    'worker_name': worker['worker_name'],
                    'ten_min_hashrate': ten_min_value.group(1) if ten_min_value else '0',
                    'one_h_hashrate': one_h_value.group(1) if one_h_value else '0',
                    'h24_hashrate': h24_value.group(1) if h24_value else '0',
                    'rejection_rate': rejection_value.group(1) if rejection_value else '0',
                    'last_share_time': worker['last_share_time'],
                    'connections_24h': worker['connections_24h'],
                    'is_active': worker['is_active'],
                    'user_id': self.observer_user_id,
                    'coin_type': self.coin_type,
                    'created_at': datetime.now().isoformat()
                }
                
                upload_data.append(record)
            
            # Upload in batches to avoid request size limits
            batch_size = 10
            total_uploaded = 0
            
            for i in range(0, len(upload_data), batch_size):
                batch = upload_data[i:i+batch_size]
                logger.info(f"Uploading batch {i//batch_size + 1}/{(len(upload_data) + batch_size - 1)//batch_size} ({len(batch)} workers)")
                
                try:
                    # Upload batch to Supabase
                    response = supabase.table("mining_workers").insert(batch).execute()
                    
                    # Check for errors
                    if hasattr(response, 'error') and response.error:
                        logger.error(f"Error uploading batch to Supabase: {response.error}")
                    else:
                        total_uploaded += len(batch)
                        logger.info(f"Successfully uploaded batch of {len(batch)} workers")
                        
                except Exception as e:
                    logger.error(f"Error uploading batch to Supabase: {str(e)}")
                    
                    # Try uploading records individually
                    logger.info("Trying to upload records individually...")
                    for record in batch:
                        try:
                            response = supabase.table("mining_workers").insert(record).execute()
                            if hasattr(response, 'error') and response.error:
                                logger.error(f"Error uploading record to Supabase: {response.error}")
                            else:
                                total_uploaded += 1
                                logger.info(f"Successfully uploaded individual record")
                        except Exception as e2:
                            logger.error(f"Error uploading individual record to Supabase: {str(e2)}")
            
            logger.info(f"Supabase upload completed. Total records uploaded: {total_uploaded}/{len(workers)}")
            return total_uploaded
            
        except Exception as e:
            logger.error(f"Error uploading worker data to Supabase: {str(e)}")
            return 0

async def scrape_workers(access_key, observer_user_id, coin_type="BTC", output_dir=None):
    """Scrape worker data from Antpool."""
    logger.info(f"Starting worker scraping for {observer_user_id} ({coin_type})...")
    
    try:
        # Create scraper instance
        scraper = AntpoolWorkerScraper(access_key, observer_user_id, coin_type, output_dir)
        
        # Scrape workers
        workers = await scraper.scrape_workers()
        
        if not workers:
            logger.error("No worker data scraped")
            return False
        
        # Save to file
        filepath = await scraper.save_to_file(workers)
        
        # Get Supabase client
        supabase = get_supabase_client()
        
        # Upload to Supabase
        uploaded = await scraper.upload_to_supabase(workers, supabase)
        
        logger.info(f"Worker scraping completed for {observer_user_id}. Scraped: {len(workers)}, Uploaded: {uploaded}")
        return True
        
    except Exception as e:
        logger.error(f"Error during worker scraping: {str(e)}")
        return False

async def main():
    """Main function to run the scraper."""
    # Create output directory
    output_dir = os.path.join(os.getcwd(), "output")
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Get Supabase client
        supabase = get_supabase_client()
        
        # Fetch accounts from Supabase
        logger.info("Fetching accounts from Supabase...")
        try:
            response = supabase.table("account_credentials").select("*").eq("is_active", True).execute()
            accounts = response.data
            logger.info(f"Found {len(accounts)} active accounts")
        except Exception as e:
            logger.error(f"Error fetching accounts from Supabase: {str(e)}")
            return
        
        # Process each account
        for account in accounts:
            logger.info(f"Processing account: {account['user_id']} ({account['coin_type']})")
            
            # Scrape workers for this account
            result = await scrape_workers(
                account["access_key"],
                account["user_id"],
                account["coin_type"],
                output_dir
            )
            
            if result:
                logger.info(f"Successfully scraped workers for {account['user_id']}")
            else:
                logger.error(f"Failed to scrape workers for {account['user_id']}")
            
            # Wait between accounts to avoid rate limiting
            await asyncio.sleep(5)
        
        logger.info("All accounts processed")
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
