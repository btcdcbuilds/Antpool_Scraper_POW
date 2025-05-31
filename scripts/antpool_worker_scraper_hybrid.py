#!/usr/bin/env python3
"""
Antpool Worker Scraper - Hybrid Version

This script combines the proven extraction logic from the working script
with the multi-browser and browser reuse features for optimal performance and reliability.
"""

import os
import sys
import json
import logging
import asyncio
import re
import argparse
from datetime import datetime
import traceback
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple

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
    from utils.supabase_utils import get_supabase_client, filter_schema_fields_list
except ImportError as e:
    logger.error(f"Import error: {e}")
    sys.exit(1)

async def scrape_workers(page: Any, access_key: str, user_id: str, coin_type: str, debug: bool = False) -> List[Dict[str, Any]]:
    """
    Scrape worker statistics from Antpool with retry logic.
    This function uses the exact logic from the working script.
    """
    logger.info(f"Starting worker scrape for {user_id} ({coin_type})")
    
    max_retries = 3
    retry_delay = 2  # seconds
    
    for attempt in range(max_retries):
        try:
            # Navigate to observer page
            observer_url = f"https://www.antpool.com/observer?accessKey={access_key}&coinType={coin_type}&observerUserId={user_id}"
            await page.goto(observer_url, wait_until="domcontentloaded")
            logger.info(f"Navigated to observer page for {user_id}")
            
            # Handle informed consent dialog
            try:
                await page.wait_for_selector('text="INFORMED CONSENT"', timeout=5000)
                await page.click('text="Got it"')
                await asyncio.sleep(0.5)
                await page.click('button:has-text("Confirm")')
                logger.info("Consent dialog handled")
            except Exception as e:
                logger.debug(f"No consent dialog or error handling it: {e}")
            
            # Clear any modals that might be present - critical for reliable scraping
            try:
                await page.evaluate("""() => {
                    document.querySelectorAll('.ant-modal-close').forEach(el => el.click());
                    document.querySelectorAll('.ant-modal-mask').forEach(el => el.remove());
                    document.querySelectorAll('.ant-modal-wrap').forEach(el => el.remove());
                }""")
                logger.info("Cleared any modal elements")
            except Exception as e:
                logger.debug(f"Error clearing modals: {e}")
            
            # Wait for page to load completely
            await asyncio.sleep(1)
            
            # The Worker tab should already be active, verify we can see the text first
            # This is critical - matches the working script's approach
            await page.wait_for_selector('text="Worker"', timeout=10000)
            logger.info("Worker tab found")
            
            # Now wait for worker table to load
            await page.wait_for_selector('table', timeout=10000)
            logger.info("Worker table loaded successfully")
            
            # Set page size to 80 (maximum available)
            try:
                await page.click('text="10 /page"')
                await asyncio.sleep(0.5)
                await page.click('text="80 /page"')
                await asyncio.sleep(1.5)
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
    """
    Extract worker data from the table with proper error handling.
    This function uses the exact logic from the working script.
    """
    workers_data = []
    
    try:
        # Get total number of workers from pagination text
        total_workers = 0
        total_pages = 1
        try:
            # Wait for pagination to load
            await asyncio.sleep(0.5)
            
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
            await asyncio.sleep(0.5)
            
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
                await asyncio.sleep(1.5)
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
    """
    Process a single worker row and return extracted data.
    This function uses the exact logic from the working script.
    """
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
    is_active = not ("day" in last_share or "week" in last_share or "month" in last_share)
    worker_data["status"] = "active" if is_active else "inactive"
    
    return worker_data

async def process_accounts_with_browser_reuse(accounts: List[Dict[str, Any]], output_dir: str, max_concurrent: int = 3, debug: bool = False) -> List[Dict[str, Any]]:
    """
    Process accounts in parallel using browser reuse for optimal performance.
    This function maintains the multi-browser architecture from the cost-optimized script.
    """
    
    # Split accounts into chunks for each browser
    chunk_size = (len(accounts) + max_concurrent - 1) // max_concurrent
    account_chunks = [accounts[i:i + chunk_size] for i in range(0, len(accounts), chunk_size)]
    
    logger.info(f"🔥 Splitting {len(accounts)} accounts into {len(account_chunks)} browser workers")
    for i, chunk in enumerate(account_chunks):
        logger.info(f"   Browser {i+1}: {len(chunk)} accounts ({[acc['user_id'] for acc in chunk]})")
    
    async def process_chunk_with_shared_browser(chunk: List[Dict[str, Any]], browser_id: int) -> List[Dict[str, Any]]:
        """Process a chunk of accounts with a shared browser."""
        browser = None
        results = []
        
        try:
            logger.info(f"🚀 Browser {browser_id}: Starting with {len(chunk)} accounts")
            
            # Create one browser for this chunk
            browser = await setup_browser(headless=True)
            logger.info(f"✅ Browser {browser_id}: Launched successfully")
            
            # Process each account in this chunk sequentially
            for i, account in enumerate(chunk):
                page = None
                try:
                    logger.info(f"🔄 Browser {browser_id}: Processing account {i+1}/{len(chunk)} - {account['user_id']}")
                    
                    # Create new page for this account
                    page = await browser.new_page()
                    
                    # Scrape workers for this account
                    workers_data = await scrape_workers(page, account["access_key"], account["user_id"], account["coin_type"], debug)
                    
                    # Save worker data to file
                    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
                    output_file = os.path.join(output_dir, f"{timestamp_str}_Antpool_{account['coin_type']}_workers_{account['user_id']}.json")
                    save_json_to_file(workers_data, output_file)
                    
                    # Handle empty results
                    if not workers_data:
                        logger.warning(f"⚠️ Browser {browser_id}: No worker data for {account['user_id']}, creating placeholder")
                        workers_data = [{
                            "worker": "No workers found",
                            "ten_min_hashrate": "0 TH/s",
                            "one_h_hashrate": "0 TH/s", 
                            "h24_hashrate": "0 TH/s",
                            "rejection_rate": "0%",
                            "last_share_time": "Never",
                            "connections_24h": "0",
                            "timestamp": format_timestamp(),
                            "observer_user_id": account["user_id"],
                            "coin_type": account["coin_type"],
                            "status": "inactive"
                        }]
                    
                    # Calculate stats
                    active_workers = sum(1 for w in workers_data if w.get("status", "") == "active")
                    inactive_workers = len(workers_data) - active_workers
                    
                    logger.info(f"📊 Browser {browser_id}: {account['user_id']} - {len(workers_data)} total, {active_workers} active, {inactive_workers} inactive")
                    
                    # Save to Supabase
                    supabase = get_supabase_client()
                    if supabase:
                        # Filter and batch upload
                        filtered_workers_data = filter_schema_fields_list(workers_data, "mining_workers")
                        
                        try:
                            # Try batch insert first
                            result = supabase.table("mining_workers").insert(filtered_workers_data).execute()
                            if hasattr(result, 'data'):
                                logger.info(f"📤 Browser {browser_id}: Uploaded {len(workers_data)} workers for {account['user_id']}")
                            else:
                                # Fallback to individual inserts
                                success_count = 0
                                for worker in filtered_workers_data:
                                    try:
                                        result = supabase.table("mining_workers").insert(worker).execute()
                                        if hasattr(result, 'data'):
                                            success_count += 1
                                    except Exception as e:
                                        logger.error(f"❌ Error saving individual worker: {str(e)}")
                                logger.info(f"📤 Browser {browser_id}: Uploaded {success_count}/{len(workers_data)} workers for {account['user_id']} (individual)")
                        except Exception as e:
                            logger.error(f"❌ Browser {browser_id}: Error uploading workers for {account['user_id']}: {str(e)}")
                        
                        # Update last_scrape_time in account_credentials
                        try:
                            supabase.table("account_credentials").update(
                                {"last_scrape_time": format_timestamp()}
                            ).eq("user_id", account["user_id"]).eq("coin_type", account["coin_type"]).execute()
                            logger.info(f"🔄 Browser {browser_id}: Updated last_scrape_time for {account['user_id']}")
                        except Exception as e:
                            logger.error(f"❌ Browser {browser_id}: Error updating last_scrape_time: {str(e)}")
                    
                    # Close the page when done with this account
                    await page.close()
                    logger.info(f"🧹 Browser {browser_id}: Closed page for {account['user_id']}")
                    
                except Exception as e:
                    logger.error(f"❌ Browser {browser_id}: Error processing {account['user_id']}: {str(e)}")
                    if page:
                        try:
                            await page.close()
                        except:
                            pass
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Browser {browser_id}: Fatal error: {str(e)}")
            return []
            
        finally:
            # Always close the browser when done with all accounts in this chunk
            if browser:
                try:
                    await browser.close()
                    logger.info(f"🧹 Browser {browser_id}: Closed browser")
                except:
                    pass
    
    # Process all chunks in parallel
    async def main_parallel():
        tasks = []
        for i, chunk in enumerate(account_chunks):
            task = asyncio.create_task(process_chunk_with_shared_browser(chunk, i+1))
            tasks.append(task)
        
        # Wait for all browser tasks to complete
        await asyncio.gather(*tasks)
    
    # Run the parallel processing
    asyncio.run(main_parallel())
    
    return []

def load_accounts(group: int = None, total_groups: int = None) -> List[Dict[str, Any]]:
    """
    Load accounts from Supabase, optionally filtering by group.
    """
    accounts = []
    
    try:
        # Get Supabase client
        supabase = get_supabase_client()
        if not supabase:
            logger.error("Failed to initialize Supabase client")
            return accounts
        
        # Query account credentials
        query = supabase.table("account_credentials").select("*")
        
        # Apply group filtering if specified
        if group is not None and total_groups is not None:
            if group < 1 or group > total_groups:
                logger.error(f"Invalid group: {group}/{total_groups}")
                return accounts
            
            # Get all accounts first
            response = query.execute()
            all_accounts = response.data if hasattr(response, 'data') else []
            
            # Apply group filtering
            if all_accounts:
                # Sort accounts by user_id for consistent grouping
                all_accounts.sort(key=lambda acc: acc.get('user_id', ''))
                
                # Distribute accounts evenly across groups
                accounts = []
                for i, account in enumerate(all_accounts):
                    if (i % total_groups) + 1 == group:
                        accounts.append(account)
                
                logger.info(f"Loaded {len(accounts)}/{len(all_accounts)} accounts for group {group}/{total_groups}")
            
        else:
            # Get all accounts
            response = query.execute()
            accounts = response.data if hasattr(response, 'data') else []
            logger.info(f"Loaded {len(accounts)} accounts (all groups)")
        
    except Exception as e:
        logger.error(f"Error loading accounts: {str(e)}")
    
    return accounts

async def main():
    """Main function to run the worker scraper."""
    parser = argparse.ArgumentParser(description='Antpool Worker Scraper - Hybrid Version')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--output-dir', '--output_dir', type=str, default='/tmp', help='Output directory for JSON files')
    parser.add_argument('--group', type=int, help='Group number for distributed processing')
    parser.add_argument('--total-groups', '--total_groups', type=int, help='Total number of groups for distributed processing')
    parser.add_argument('--max-concurrent', '--max_concurrent', type=int, default=3, help='Maximum number of concurrent browsers')
    
    # Add compatibility arguments for start.sh
    parser.add_argument('--access_key', type=str, help='Access key for Antpool API (compatibility)')
    parser.add_argument('--user_id', type=str, help='User ID for Antpool API (compatibility)')
    parser.add_argument('--coin_type', type=str, help='Coin type for Antpool API (compatibility)')
    
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Load accounts from Supabase
    accounts = load_accounts(args.group, args.total_groups)
    
    if not accounts:
        logger.error("No accounts found, exiting")
        return
    
    logger.info(f"Processing {len(accounts)} accounts with max {args.max_concurrent} concurrent browsers")
    
    # Process accounts with browser reuse
    await process_accounts_with_browser_reuse(accounts, args.output_dir, args.max_concurrent, args.debug)
    
    logger.info("Worker scraping completed")

if __name__ == "__main__":
    asyncio.run(main())
