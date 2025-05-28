#!/usr/bin/env python3
"""
Antpool Worker Scraper - Optimized Version

This script scrapes worker statistics from Antpool with improved error handling,
retry logic, and performance optimizations.
"""

import os
import sys
import json
import logging
import asyncio
import re
from datetime import datetime
import traceback
from pathlib import Path
from typing import List, Dict, Optional, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from utils.browser_utils import setup_browser, handle_consent_dialog, take_screenshot
    from utils.data_utils import save_json_to_file, format_timestamp
    from utils.supabase_utils import get_supabase_client
except ImportError:
    # Fallback for direct script execution
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from utils.browser_utils import setup_browser, handle_consent_dialog, take_screenshot
    from utils.data_utils import save_json_to_file, format_timestamp
    from utils.supabase_utils import get_supabase_client

async def scrape_workers(page: Any, access_key: str, user_id: str, coin_type: str, debug: bool = False) -> List[Dict[str, Any]]:
    """Scrape worker statistics from Antpool with retry logic."""
    logger.info(f"Starting worker scrape for {user_id} ({coin_type})")
    
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            # Navigate to observer page
            observer_url = f"https://www.antpool.com/observer?accessKey={access_key}&coinType={coin_type}&observerUserId={user_id}"
            await page.goto(observer_url, wait_until="domcontentloaded")
            logger.info(f"Navigated to observer page for {user_id}")
            
            # Handle informed consent dialog
            try:
                await page.wait_for_selector('text="INFORMED CONSENT"', timeout=10000)
                await page.click('text="Got it"')  # Check the checkbox
                await asyncio.sleep(1)
                await page.click('button:has-text("Confirm")')  # Click confirm
                logger.info("Consent dialog handled")
            except Exception as e:
                logger.debug(f"No consent dialog or error handling it: {e}")
            
            # Wait for page to load completely
            await asyncio.sleep(3)
            
            # The Worker tab should already be active, verify we can see the table
            await page.wait_for_selector('text="Worker"', timeout=15000)
            logger.info("Worker tab found")
            
            # Wait for worker table to load
            await page.wait_for_selector('table', timeout=15000)
            logger.info("Worker table loaded successfully")

            # Set page size to 80 (maximum available)
            try:
                await page.click('text="10 /page"')
                await asyncio.sleep(1)
                await page.click('text="80 /page"')
                await asyncio.sleep(3)  # Wait for table to reload
                logger.info("Page size set to 80")
            except Exception as e:
                logger.warning(f"Could not set page size: {e}")
            
            workers_data = await _extract_worker_data(page, user_id, coin_type, debug)
            return workers_data
            
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(retry_delay)
            continue

async def _extract_worker_data(page: Any, user_id: str, coin_type: str, debug: bool) -> List[Dict[str, Any]]:
    """Extract worker data from the table with proper error handling."""
    workers_data = []
    
    try:
        # Get total number of workers from pagination text
        total_workers = 0
        total_pages = 1
        try:
            # Wait for pagination to load
            await asyncio.sleep(2)
            
            # Try multiple approaches to find pagination info
            pagination_text = None
            
            # Method 1: Look for "Total X items" text anywhere on the page
            try:
                page_content = await page.content()
                total_match = re.search(r'Total (\d+) items', page_content, re.IGNORECASE)
                if total_match:
                    total_workers = int(total_match.group(1))
                    pagination_text = f"Total {total_workers} items"
                    logger.info(f"Found total workers from page content: {total_workers}")
            except Exception as e:
                logger.debug(f"Method 1 failed: {e}")
            
            # Method 2: Look for pagination elements with text content
            if not pagination_text:
                try:
                    # Look for elements containing "Total" and numbers
                    elements = await page.query_selector_all('*')
                    for element in elements:
                        try:
                            text = await element.text_content()
                            if text and 'Total' in text and 'items' in text:
                                match = re.search(r'Total (\d+) items', text, re.IGNORECASE)
                                if match:
                                    total_workers = int(match.group(1))
                                    pagination_text = text.strip()
                                    logger.info(f"Found pagination text: {pagination_text}")
                                    break
                        except:
                            continue
                except Exception as e:
                    logger.debug(f"Method 2 failed: {e}")
            
            # Method 3: Count pagination buttons to estimate pages
            if total_workers == 0:
                try:
                    # Look for numbered pagination buttons
                    page_buttons = await page.query_selector_all('button[class*="pagination"], .ant-pagination-item, a[class*="page"]')
                    page_numbers = []
                    for button in page_buttons:
                        try:
                            text = await button.text_content()
                            if text and text.isdigit():
                                page_numbers.append(int(text))
                        except:
                            continue
                    
                    if page_numbers:
                        estimated_pages = max(page_numbers)
                        total_workers = estimated_pages * 80  # Estimate based on 80 per page
                        total_pages = estimated_pages
                        logger.info(f"Estimated from pagination buttons: {estimated_pages} pages, ~{total_workers} workers")
                except Exception as e:
                    logger.debug(f"Method 3 failed: {e}")
            
            # Calculate total pages if we found total workers
            if total_workers > 0:
                total_pages = (total_workers + 79) // 80  # Ceiling division for 80 per page
            else:
                # Final fallback: check if there are next/pagination buttons
                try:
                    next_buttons = await page.query_selector_all('button[aria-label="Next page"], .ant-pagination-next, button:has-text(">")')
                    total_pages = 2 if next_buttons else 1
                    logger.info(f"Final fallback: Set total_pages to {total_pages} based on next button presence")
                except:
                    total_pages = 1
            
            logger.info(f"Pagination text: {pagination_text}")
            logger.info(f"Total workers: {total_workers}, Total pages: {total_pages}")
        except Exception as e:
            logger.warning(f"Could not get pagination info: {e}")
            total_pages = 1
        
        # Process pages dynamically - continue until no more pages
        page_num = 1
        max_pages = max(total_pages, 10)  # Safety limit
        
        while page_num <= max_pages:
            logger.info(f"Processing page {page_num} (estimated total: {total_pages})")
            
            # Wait for table to be stable
            await asyncio.sleep(2)
            
            # Get table rows
            rows = await page.query_selector_all('table tbody tr')
            logger.info(f"Found {len(rows)} rows on page {page_num}")
            
            # If no rows found, we might be done
            if not rows:
                logger.info(f"No rows found on page {page_num}, stopping pagination")
                break
            
            # Process each row
            page_workers = 0
            for row_idx, row in enumerate(rows):
                try:
                    worker_data = await _process_worker_row(row, user_id, coin_type, page_num, row_idx + 1)
                    if worker_data:
                        workers_data.append(worker_data)
                        page_workers += 1
                        logger.debug(f"Extracted worker: {worker_data['worker']}")
                        
                except Exception as e:
                    logger.error(f"Error processing row {row_idx + 1}: {str(e)}")
                    continue
            
            logger.info(f"Extracted {page_workers} workers from page {page_num}")
            
            # Check if there's a next page
            try:
                # Try multiple selectors for next button
                next_button = None
                next_selectors = [
                    'button[aria-label="Next page"]:not([disabled])',
                    '.ant-pagination-next:not([disabled])',
                    'button:has-text(">"):not([disabled])',
                    'li.ant-pagination-next:not(.ant-pagination-disabled) button',
                    'button[title="Next Page"]:not([disabled])'
                ]
                
                for selector in next_selectors:
                    try:
                        buttons = await page.query_selector_all(selector)
                        if buttons:
                            next_button = buttons[0]
                            logger.debug(f"Found next button with selector: {selector}")
                            break
                    except:
                        continue
                
                if not next_button:
                    # Try to find any clickable pagination element with number > current page
                    try:
                        page_buttons = await page.query_selector_all('button, a')
                        for button in page_buttons:
                            text = await button.text_content()
                            if text and text.isdigit() and int(text) == page_num + 1:
                                next_button = button
                                logger.debug(f"Found next page button with number: {text}")
                                break
                    except:
                        pass
                
                if not next_button:
                    logger.info(f"No enabled next button found, finished at page {page_num}")
                    break
                
                # Check if next button is disabled
                is_disabled = await next_button.get_attribute('disabled')
                class_name = await next_button.get_attribute('class') or ''
                parent_class = ''
                try:
                    parent = await next_button.query_selector('..')
                    if parent:
                        parent_class = await parent.get_attribute('class') or ''
                except:
                    pass
                
                if is_disabled or 'disabled' in class_name or 'disabled' in parent_class:
                    logger.info(f"Next button is disabled, finished at page {page_num}")
                    break
                
                # Click next page
                await next_button.click()
                await asyncio.sleep(3)  # Wait for page to load
                logger.info(f"Navigated to page {page_num + 1}")
                page_num += 1
                
            except Exception as e:
                logger.error(f"Error navigating to next page: {e}")
                break
        
        logger.info(f"Successfully extracted {len(workers_data)} workers")
        return workers_data
        
    except Exception as e:
        logger.error(f"Error extracting worker data: {str(e)}")
        raise

async def _process_worker_row(row: Any, user_id: str, coin_type: str, page_num: int, row_num: int) -> Optional[Dict[str, Any]]:
    """Process a single worker row and return extracted data."""
    cells = await row.query_selector_all("td")
    if len(cells) < 5:
        return None
    
    # Extract text from each cell
    cell_texts = []
    for i, cell in enumerate(cells[:9]):  # Get up to 9 cells
        text = await cell.inner_text()
        cell_texts.append(text.strip())
    
    # Skip header rows, empty rows, or rows without worker name in 3rd cell
    worker_name = cell_texts[2] if len(cell_texts) > 2 else ""
    if not worker_name or "Worker" in worker_name or worker_name == "No filter data":
        return None
    
    # Create worker data with correct cell mapping
    # Based on our testing: [empty, empty, worker_name, 10min_hash, 1h_hash, 24h_hash, rejection, last_share, connections]
    worker_data = {
        "worker": cell_texts[2] if len(cell_texts) > 2 else "",
        "ten_min_hashrate": cell_texts[3] if len(cell_texts) > 3 else "",
        "one_h_hashrate": cell_texts[4] if len(cell_texts) > 4 else "", 
        "h24_hashrate": cell_texts[5] if len(cell_texts) > 5 else "",
        "rejection_rate": cell_texts[6] if len(cell_texts) > 6 else "",
        "last_share_time": cell_texts[7] if len(cell_texts) > 7 else "",
        "connections_24h": cell_texts[8] if len(cell_texts) > 8 else "",
        "timestamp": format_timestamp(),
        "observer_user_id": user_id,
        "coin_type": coin_type
    }
    
    # Determine worker status based on last share time
    last_share = worker_data["last_share_time"].lower()
    worker_data["is_active"] = not ("day" in last_share or "week" in last_share or "month" in last_share)
    worker_data["status"] = "active" if worker_data["is_active"] else "inactive"
    
    return worker_data

async def take_workers_screenshot(page, output_dir, user_id, timestamp_str):
    """Take a screenshot of the workers page."""
    try:
        # Wait for workers table to be visible
        await page.wait_for_selector("table", timeout=10000)
        
        # Take screenshot
        screenshot_path = os.path.join(output_dir, f"{timestamp_str}_Antpool_BTC_workers.png")
        await take_screenshot(page, screenshot_path)
        logger.info(f"Saved workers screenshot to {screenshot_path}")
        return screenshot_path
    except Exception as e:
        logger.error(f"Error taking screenshot: {str(e)}")
        return None

async def save_to_supabase(supabase, workers_data):
    """Save worker data to Supabase with error handling."""
    if not supabase or not workers_data:
        return False
        
    try:
        result = supabase.table("mining_workers").insert(workers_data).execute()
        if hasattr(result, 'data'):
            logger.info(f"Saved {len(workers_data)} workers to Supabase")
            return True
        return False
    except Exception as e:
        logger.error(f"Supabase save error: {str(e)}")
        return False

async def process_single_client(access_key, user_id, coin_type, output_dir, debug=False):
    """Process a single client with proper error handling."""
    logger.info(f"Starting worker scraping for {user_id} ({coin_type})...")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize timestamp for filenames
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M")
    
    # Initialize browser
    browser = None
    page = None
    
    try:
        logger.info("Launching browser...")
        print("Launching browser...")
        
        # Initialize browser
        browser = await setup_browser(headless=True)
        print("Playwright started successfully")
        print("Browser launched successfully")
        
        # Create a new page
        page = await browser.new_page()
        page.set_default_timeout(15000)  # 15 second timeout
        
        # Scrape workers
        workers_data = await scrape_workers(page, access_key, user_id, coin_type, debug)
        
        # Take screenshot of workers page
        screenshot_path = await take_workers_screenshot(page, output_dir, user_id, timestamp_str)
        
        # Save worker data to file
        output_file = os.path.join(output_dir, f"worker_stats_{user_id}_{timestamp_str}.json")
        save_json_to_file(workers_data, output_file)
        logger.info(f"Worker stats saved to: {output_file}")
        
        # If no workers were found, create a placeholder entry
        if not workers_data:
            logger.warning("No worker data extracted, creating placeholder entry")
            workers_data = [{
                "worker": "No workers found",
                "ten_min_hashrate": "0 TH/s",
                "one_h_hashrate": "0 TH/s",
                "h24_hashrate": "0 TH/s",
                "rejection_rate": "0%",
                "last_share_time": "Never",
                "connections_24h": "0",
                "timestamp": format_timestamp(),
                "observer_user_id": user_id,
                "coin_type": coin_type,
                "is_active": False,
                "status": "inactive"
            }]
        
        logger.info("===== Worker Extraction Summary =====")
        logger.info(f"Total workers extracted: {len(workers_data)}")
        active_workers = sum(1 for w in workers_data if w.get("is_active", False))
        inactive_workers = len(workers_data) - active_workers
        logger.info(f"Active workers: {active_workers}")
        logger.info(f"Inactive workers: {inactive_workers}")
        
        # Save to Supabase
        supabase = get_supabase_client()
        if supabase:
            logger.info(f"===== Uploading {len(workers_data)} Workers to Supabase =====")
            
            # Use batch uploads for better performance
            batch_size = 100
            logger.info(f"Using batch size of {batch_size} workers per request")
            
            # Split workers into batches
            batches = [workers_data[i:i + batch_size] for i in range(0, len(workers_data), batch_size)]
            
            success_count = 0
            for i, batch in enumerate(batches):
                try:
                    logger.info(f"Uploading batch {i+1}/{len(batches)} ({len(batch)} workers)")
                    result = supabase.table("mining_workers").insert(batch).execute()
                    if hasattr(result, 'data'):
                        success_count += len(batch)
                        logger.info(f"Batch {i+1}/{len(batches)} uploaded successfully")
                    else:
                        logger.error(f"❌ Error uploading batch {i+1}/{len(batches)}: {result}")
                        # Fallback to individual inserts
                        logger.info(f"Falling back to individual inserts for batch {i+1}")
                        individual_success = 0
                        for worker in batch:
                            try:
                                result = supabase.table("mining_workers").insert(worker).execute()
                                if hasattr(result, 'data'):
                                    success_count += 1
                                    individual_success += 1
                            except Exception as e:
                                logger.error(f"❌ Error saving worker {worker.get('worker', 'unknown')}: {str(e)}")
                        logger.info(f"Individual inserts: {individual_success}/{len(batch)} workers saved successfully")
                except Exception as e:
                    logger.error(f"❌ Error uploading batch {i+1}/{len(batches)}: {str(e)}")
            
            logger.info("===== Supabase Upload Summary =====")
            logger.info(f"Total workers: {len(workers_data)}")
            logger.info(f"Successfully uploaded: {success_count}")
            logger.info(f"Failed to upload: {len(workers_data) - success_count}")
        else:
            logger.warning("Supabase client not available, skipping upload")
        
        logger.info("===== Worker Scraping Completed Successfully =====")
        logger.info(f"Account: {user_id} ({coin_type})")
        logger.info(f"Total workers extracted: {len(workers_data)}")
        logger.info(f"Worker stats saved to: {output_file}")
        
        return {
            "success": True,
            "workers_count": len(workers_data),
            "active_workers": active_workers,
            "inactive_workers": inactive_workers,
            "output_file": output_file,
            "screenshot_path": screenshot_path
        }
        
    except Exception as e:
        logger.error(f"Error during worker scraping: {str(e)}")
        logger.error(f"No worker data scraped")
        return {
            "success": False,
            "error": str(e)
        }
    
    finally:
        # Close browser
        if page:
            await page.close()
        if browser:
            await browser.close()
            logger.info("Browser closed")

async def main():
    """Main entry point for the script."""
    # Get Supabase client
    supabase = get_supabase_client()
    
    if not supabase:
        logger.error("Failed to initialize Supabase client")
        return
    
    # Fetch accounts from Supabase
    try:
        response = supabase.table("account_credentials").select("*").eq("is_active", True).execute()
        accounts = response.data
        logger.info(f"Found {len(accounts)} active accounts")
    except Exception as e:
        logger.error(f"❌ Error fetching accounts from Supabase: {e}")
        return
    
    # Create output directory
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")
    os.makedirs(output_dir, exist_ok=True)
    
    # Process each account
    for account in accounts:
        try:
            logger.info(f"Processing account: {account['user_id']} ({account['coin_type']})")
            result = await process_single_client(
                account["access_key"],
                account["user_id"],
                account["coin_type"],
                output_dir
            )
            
            if not result["success"]:
                logger.error(f"Failed to scrape workers for {account['user_id']}")
            
            # Wait 5 seconds between accounts to avoid rate limiting
            await asyncio.sleep(5)
            
        except Exception as e:
            logger.error(f"Error processing account {account['user_id']}: {str(e)}")
            continue
    
    logger.info("All accounts processed")

if __name__ == "__main__":
    asyncio.run(main())
