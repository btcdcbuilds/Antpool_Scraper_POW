#!/usr/bin/env python3
"""
Antpool Worker Scraper - Hybrid Parallel + Browser Reuse Version

This script scrapes worker statistics from Antpool with optimal performance,
combining parallel processing with browser reuse for maximum efficiency.
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
    """Scrape worker statistics from Antpool with retry logic."""
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
                    // Additional cleanup for other potential overlays
                    document.querySelectorAll('.ant-message').forEach(el => el.remove());
                    document.querySelectorAll('.ant-notification').forEach(el => el.remove());
                }""")
                logger.info("Cleared any modal elements")
            except Exception as e:
                logger.debug(f"Error clearing modals: {e}")
            
            # Wait for page to load
            await asyncio.sleep(1)
            
            # Try multiple approaches to find the worker table
            table_found = False
            
            # Method 1: Direct table selector
            try:
                await page.wait_for_selector('table', timeout=8000)
                table_found = True
                logger.info("Worker table found via direct selector")
            except Exception as e:
                logger.warning(f"Could not find table via direct selector: {e}")
                
                # Method 2: Look for table-related elements
                try:
                    selectors = [
                        'tbody',
                        '.ant-table',
                        '.ant-table-wrapper',
                        'div[class*="table"]',
                        'th'
                    ]
                    
                    for selector in selectors:
                        try:
                            element = await page.wait_for_selector(selector, timeout=5000)
                            if element:
                                table_found = True
                                logger.info(f"Table-related element found via selector: {selector}")
                                break
                        except Exception as inner_e:
                            logger.debug(f"Selector {selector} not found: {inner_e}")
                except Exception as alt_e:
                    logger.warning(f"Alternative table selectors failed: {alt_e}")
                
                # Method 3: Check if we can find pagination elements
                if not table_found:
                    try:
                        pagination_selectors = [
                            '.ant-pagination',
                            'ul[class*="pagination"]',
                            'button[aria-label="Next page"]',
                            '.ant-pagination-item'
                        ]
                        
                        for selector in pagination_selectors:
                            try:
                                element = await page.wait_for_selector(selector, timeout=3000)
                                if element:
                                    table_found = True
                                    logger.info(f"Pagination element found via selector: {selector}")
                                    break
                            except Exception as inner_e:
                                logger.debug(f"Pagination selector {selector} not found: {inner_e}")
                    except Exception as pag_e:
                        logger.warning(f"Pagination selectors failed: {pag_e}")
            
            # If we still haven't found the table, try one last approach - look for worker data in page content
            if not table_found:
                try:
                    # Check if page content contains worker-related text
                    content = await page.content()
                    if "Worker" in content and ("Hashrate" in content or "TH/s" in content or "PH/s" in content):
                        table_found = True
                        logger.info("Worker data found in page content")
                except Exception as content_e:
                    logger.warning(f"Content check failed: {content_e}")
            
            # If debug mode and table not found, take a screenshot
            if debug and not table_found:
                try:
                    screenshot_path = f"/tmp/debug_table_missing_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                    await page.screenshot(path=screenshot_path)
                    logger.info(f"Debug screenshot saved to {screenshot_path}")
                except Exception as ss_e:
                    logger.warning(f"Could not take debug screenshot: {ss_e}")
            
            # If we still haven't found the table, try to refresh the page
            if not table_found:
                try:
                    logger.warning("Table not found, attempting page refresh")
                    await page.reload(wait_until="domcontentloaded")
                    await asyncio.sleep(2)
                    
                    # Clear modals again after refresh
                    await page.evaluate("""() => {
                        document.querySelectorAll('.ant-modal-close').forEach(el => el.click());
                        document.querySelectorAll('.ant-modal-mask').forEach(el => el.remove());
                        document.querySelectorAll('.ant-modal-wrap').forEach(el => el.remove());
                    }""")
                    
                    # Check for table again
                    await page.wait_for_selector('table', timeout=8000)
                    table_found = True
                    logger.info("Worker table found after page refresh")
                except Exception as refresh_e:
                    logger.error(f"Table not found even after refresh: {refresh_e}")
                    raise Exception("Could not find worker table after multiple attempts")
            
            logger.info("Worker table loaded successfully")

            # Set page size to 80 (maximum available) with more robust selectors
            try:
                # Try multiple approaches to set page size
                page_size_set = False
                
                # Method 1: Text-based selector
                try:
                    await page.click('text="10 /page"')
                    await asyncio.sleep(0.5)
                    await page.click('text="80 /page"')
                    page_size_set = True
                except Exception as e:
                    logger.debug(f"Method 1 for page size failed: {e}")
                
                # Method 2: Class-based selector
                if not page_size_set:
                    try:
                        await page.click('.ant-select-selection-item')
                        await asyncio.sleep(0.5)
                        await page.click('div[title="80 / page"]')
                        page_size_set = True
                    except Exception as e:
                        logger.debug(f"Method 2 for page size failed: {e}")
                
                # Method 3: Try to find any element with "page" text
                if not page_size_set:
                    try:
                        elements = await page.query_selector_all('*')
                        for element in elements:
                            text = await element.text_content()
                            if text and "/page" in text:
                                await element.click()
                                await asyncio.sleep(0.5)
                                
                                # Now try to find and click 80/page option
                                options = await page.query_selector_all('div[class*="select-item"]')
                                for option in options:
                                    option_text = await option.text_content()
                                    if "80" in option_text:
                                        await option.click()
                                        page_size_set = True
                                        break
                                
                                if page_size_set:
                                    break
                    except Exception as e:
                        logger.debug(f"Method 3 for page size failed: {e}")
                
                # Wait longer after setting page size to ensure table updates
                await asyncio.sleep(1.5)
                logger.info(f"Page size set to 80: {page_size_set}")
            except Exception as e:
                logger.warning(f"Could not set page size: {e}")
                # Continue anyway - this is not critical
            
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
            await asyncio.sleep(0.3)
            
            # Look for "Total X items" text anywhere on the page
            try:
                page_content = await page.content()
                total_match = re.search(r'Total (\d+) items', page_content, re.IGNORECASE)
                if total_match:
                    total_workers = int(total_match.group(1))
                    logger.info(f"Found total workers: {total_workers}")
            except Exception as e:
                logger.debug(f"Method 1 failed: {e}")
            
            # Calculate total pages if we found total workers
            if total_workers > 0:
                total_pages = (total_workers + 79) // 80  # Ceiling division for 80 per page
            else:
                # Fallback: check if there are next/pagination buttons
                try:
                    next_buttons = await page.query_selector_all('button[aria-label="Next page"], .ant-pagination-next, button:has-text(">")')
                    total_pages = 2 if next_buttons else 1
                    logger.info(f"Fallback: Set total_pages to {total_pages}")
                except:
                    total_pages = 1
            
            logger.info(f"Total workers: {total_workers}, Total pages: {total_pages}")
        except Exception as e:
            logger.warning(f"Could not get pagination info: {e}")
            total_pages = 1
        
        # Process pages dynamically
        page_num = 1
        max_pages = max(total_pages, 10)  # Safety limit
        
        while page_num <= max_pages:
            logger.info(f"Processing page {page_num} (estimated total: {total_pages})")
            
            # Wait for table to be stable
            await asyncio.sleep(0.3)
            
            # Get table rows
            rows = await page.query_selector_all('table tbody tr')
            logger.info(f"Found {len(rows)} rows on page {page_num}")
            
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
                        
                except Exception as e:
                    logger.error(f"Error processing row {row_idx + 1}: {str(e)}")
                    continue
            
            logger.info(f"Extracted {page_workers} workers from page {page_num}")
            
            # Check if there's a next page
            try:
                next_button = None
                next_selectors = [
                    'button[aria-label="Next page"]:not([disabled])',
                    '.ant-pagination-next:not([disabled])',
                    'button:has-text(">"):not([disabled])',
                    'li.ant-pagination-next:not(.ant-pagination-disabled) button'
                ]
                
                for selector in next_selectors:
                    try:
                        buttons = await page.query_selector_all(selector)
                        if buttons:
                            next_button = buttons[0]
                            break
                    except:
                        continue
                
                if not next_button:
                    logger.info(f"No enabled next button found, finished at page {page_num}")
                    break
                
                # Check if next button is disabled
                is_disabled = await next_button.get_attribute('disabled')
                class_name = await next_button.get_attribute('class') or ''
                
                if is_disabled or 'disabled' in class_name:
                    logger.info(f"Next button is disabled, finished at page {page_num}")
                    break
                
                # Click next page
                await next_button.click()
                await asyncio.sleep(1)
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
    for i, cell in enumerate(cells[:9]):
        text = await cell.inner_text()
        cell_texts.append(text.strip())
    
    # Skip header rows, empty rows, or rows without worker name in 3rd cell
    worker_name = cell_texts[2] if len(cell_texts) > 2 else ""
    if not worker_name or "Worker" in worker_name or worker_name == "No filter data":
        return None
    
    # Create worker data
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
    
    # Determine worker status
    last_share = worker_data["last_share_time"].lower()
    is_active = not ("day" in last_share or "week" in last_share or "month" in last_share)
    worker_data["status"] = "active" if is_active else "inactive"
    
    return worker_data

async def process_accounts_with_browser_reuse(accounts: List[Dict[str, Any]], output_dir: str, max_concurrent: int = 3, debug: bool = False) -> List[Dict[str, Any]]:
    """Process accounts in parallel using browser reuse for optimal performance."""
    
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
                        
                        # Update last_scraped_at
                        try:
                            result = supabase.table("account_credentials").update({
                                "last_scraped_at": datetime.now().isoformat()
                            }).eq("user_id", account["user_id"]).execute()
                        except Exception as e:
                            logger.error(f"❌ Error updating last_scraped_at: {str(e)}")
                    
                    # Add result
                    results.append({
                        "account": account,
                        "result": {
                            "success": True,
                            "worker_count": len(workers_data),
                            "active_workers": active_workers,
                            "inactive_workers": inactive_workers
                        }
                    })
                    
                except Exception as e:
                    logger.error(f"❌ Browser {browser_id}: Error processing {account['user_id']}: {str(e)}")
                    results.append({
                        "account": account,
                        "result": {
                            "success": False,
                            "error": str(e)
                        }
                    })
                
                finally:
                    if page:
                        await page.close()
                        logger.info(f"🧹 Browser {browser_id}: Closed page for {account['user_id']}")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Browser {browser_id} error: {str(e)}")
            return []
            
        finally:
            if browser:
                await browser.close()
                logger.info(f"🧹 Browser {browser_id}: Closed browser")
    
    # Create tasks for each chunk
    tasks = []
    for i, chunk in enumerate(account_chunks):
        if chunk:  # Only create tasks for non-empty chunks
            task = asyncio.create_task(process_chunk_with_shared_browser(chunk, i + 1))
            tasks.append(task)
    
    # Wait for all tasks to complete
    results = []
    if tasks:
        chunk_results = await asyncio.gather(*tasks)
        for chunk_result in chunk_results:
            results.extend(chunk_result)
    
    return results

def get_account_group(accounts, group_number, total_groups):
    """Get a subset of accounts for the specified group number."""
    if not accounts:
        return []
    
    # Calculate accounts per group (ceiling division to ensure all accounts are covered)
    accounts_per_group = (len(accounts) + total_groups - 1) // total_groups
    
    # Calculate start and end indices for this group
    start_idx = (group_number - 1) * accounts_per_group
    end_idx = min(start_idx + accounts_per_group, len(accounts))
    
    # Get accounts for this group
    group_accounts = accounts[start_idx:end_idx]
    
    logger.info(f"Group {group_number}/{total_groups}: Processing {len(group_accounts)} accounts (indices {start_idx}-{end_idx-1})")
    return group_accounts

async def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Antpool Worker Scraper - Hybrid Parallel + Browser Reuse Version')
    parser.add_argument('--group', '--group', type=int, default=1, help='Group number to process (default: 1)')
    parser.add_argument('--total-groups', '--total_groups', type=int, default=3, help='Total number of groups (default: 3)')
    parser.add_argument('--max-concurrent', '--max_concurrent', type=int, default=3, help='Maximum concurrent browsers (default: 3)')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode with screenshots')
    parser.add_argument('--output-dir', '--output_dir', type=str, help='Output directory for files')
    # Additional arguments for compatibility with start.sh
    parser.add_argument('--access-key', '--access_key', type=str, help='Access key for Antpool API (compatibility)')
    parser.add_argument('--user-id', '--user_id', type=str, help='User ID for Antpool API (compatibility)')
    parser.add_argument('--coin-type', '--coin_type', type=str, help='Coin type for Antpool API (compatibility)')
    args = parser.parse_args()
    
    # Validate group number
    if args.group < 1 or args.group > args.total_groups:
        logger.error(f"Group number must be between 1 and {args.total_groups}")
        return
    
    # Get Supabase client
    supabase = get_supabase_client()
    
    if not supabase:
        logger.error("Failed to initialize Supabase client")
        return
    
    # Fetch accounts from Supabase
    try:
        response = supabase.table("account_credentials").select("*").eq("is_active", True).execute()
        all_accounts = response.data
        logger.info(f"Found {len(all_accounts)} active accounts")
    except Exception as e:
        logger.error(f"❌ Error fetching accounts from Supabase: {e}")
        return
    
    # Get accounts for this group
    group_accounts = get_account_group(all_accounts, args.group, args.total_groups)
    
    if not group_accounts:
        logger.warning(f"No accounts to process in group {args.group}")
        return
    
    # Create output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")
    os.makedirs(output_dir, exist_ok=True)
    
    # Start timing
    start_time = datetime.now()
    logger.info(f"Starting worker scraper at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"📋 Accounts in this group: {len(group_accounts)}")
    logger.info(f"🔥 Max concurrent browsers: {args.max_concurrent}")
    logger.info(f"💡 Strategy: {args.max_concurrent} browsers, each reused for multiple accounts")
    
    # Process accounts with hybrid approach
    results = await process_accounts_with_browser_reuse(group_accounts, output_dir, args.max_concurrent, args.debug)
    
    # Summarize results
    successful_accounts = sum(1 for r in results if r["result"]["success"])
    failed_accounts = len(results) - successful_accounts
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info("=" * 60)
    logger.info("📊 HYBRID SCRAPER SUMMARY")
    logger.info("=" * 60)
    logger.info(f"🏷️  Group: {args.group}/{args.total_groups}")
    logger.info(f"📋 Total accounts processed: {len(group_accounts)}")
    logger.info(f"✅ Successful: {successful_accounts}")
    logger.info(f"❌ Failed: {failed_accounts}")
    logger.info(f"📈 Success rate: {(successful_accounts / len(group_accounts)) * 100:.1f}%")
    logger.info(f"⏰ Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"🏁 Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"⏱️  Total duration: {duration.total_seconds():.1f} seconds")
    logger.info(f"⚡ Average time per account: {duration.total_seconds() / len(group_accounts):.1f} seconds")
    logger.info(f"🔥 Browser efficiency: {args.max_concurrent} browsers reused across {len(group_accounts)} accounts")
    logger.info("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
