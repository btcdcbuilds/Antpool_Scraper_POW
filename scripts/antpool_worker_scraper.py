#!/usr/bin/env python3
"""
Antpool Worker Stats Scraper

This script scrapes worker statistics from Antpool's observer page and saves the data
to a JSON file. It navigates to the Worker tab and extracts data for all workers,
setting the page size to 80 results per page.

Usage:
    python3 antpool_worker_scraper.py --access_key=<access_key> --user_id=<observer_user_id> --coin_type=<coin_type> --output_dir=<output_dir>
    python3 antpool_worker_scraper.py --use_supabase --output_dir=<output_dir>

Example:
    python3 antpool_worker_scraper.py --access_key=eInFJrwSbrtDheJHTygV --user_id=Mack81 --coin_type=BTC --output_dir=/home/user/output
    python3 antpool_worker_scraper.py --use_supabase --output_dir=/home/user/output
"""

import argparse
import asyncio
import json
import os
import math
import time
import re
from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright

# Import utility modules
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.browser_utils import setup_browser, handle_consent_dialog
from utils.data_utils import save_json_data
from utils.supabase_utils import save_worker_stats, get_active_accounts

async def extract_worker_stats(page, frame, output_dir, observer_user_id, coin_type):
    """Extract worker statistics from the worker table."""
    print("Extracting worker statistics...")
    
    # Ensure no modals are present
    print("Ensuring no modals are present...")
    await page.evaluate("""() => {
        document.querySelectorAll('.ant-modal-close').forEach(el => el.click());
        document.querySelectorAll('.ant-modal-mask').forEach(el => el.remove());
        document.querySelectorAll('.ant-modal-wrap').forEach(el => el.remove());
    }""")
    print("Removed any modal elements")
    
    # Get total workers count
    print("Getting total workers count...")
    total_text = await frame.locator('.ant-pagination-total-text').text_content()
    total_workers_match = re.search(r'Total (\d+) items', total_text)
    total_workers = int(total_workers_match.group(1)) if total_workers_match else 0
    
    # Set page size to 80
    print("Setting page size to 80...")
    print("Ensuring no modals are present...")
    await page.evaluate("""() => {
        document.querySelectorAll('.ant-modal-close').forEach(el => el.click());
        document.querySelectorAll('.ant-modal-mask').forEach(el => el.remove());
        document.querySelectorAll('.ant-modal-wrap').forEach(el => el.remove());
    }""")
    print("Removed any modal elements")
    
    await frame.locator('.ant-select-selection-item').click()
    await frame.locator('div[title="80 / page"]').click()
    print("Selected page size 80")
    
    # Wait for table to update
    await asyncio.sleep(2)
    
    # Capture worker table screenshot
    print("Capturing worker table screenshot...")
    print("Ensuring no modals are present...")
    await page.evaluate("""() => {
        document.querySelectorAll('.ant-modal-close').forEach(el => el.click());
        document.querySelectorAll('.ant-modal-mask').forEach(el => el.remove());
        document.querySelectorAll('.ant-modal-wrap').forEach(el => el.remove());
    }""")
    print("Removed any modal elements")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    screenshot_path = os.path.join(output_dir, f"{timestamp}_Antpool_{coin_type}_workers.png")
    await page.screenshot(path=screenshot_path, full_page=True)
    print(f"Worker table screenshot saved to: {screenshot_path}")
    
    # Recalculate total workers and pages after setting page size
    total_text = await frame.locator('.ant-pagination-total-text').text_content()
    total_workers_match = re.search(r'Total (\d+) items', total_text)
    total_workers = int(total_workers_match.group(1)) if total_workers_match else 0
    total_pages = math.ceil(total_workers / 80)
    
    print(f"Total workers: {total_workers}, total pages: {total_pages}")
    
    all_workers = []
    
    # Process each page
    for page_num in range(1, total_pages + 1):
        print(f"Processing page {page_num} of {total_pages}")
        
        # Ensure no modals are present
        print("Ensuring no modals are present...")
        await page.evaluate("""() => {
            document.querySelectorAll('.ant-modal-close').forEach(el => el.click());
            document.querySelectorAll('.ant-modal-mask').forEach(el => el.remove());
            document.querySelectorAll('.ant-modal-wrap').forEach(el => el.remove());
        }""")
        print("Removed any modal elements")
        
        # Get table rows
        rows = await frame.locator('table tbody tr').all()
        print(f"Found {len(rows)} rows in table")
        
        # Save table screenshot for debugging
        table_screenshot_path = os.path.join(output_dir, f"table_page{page_num}.png")
        await frame.locator('table').screenshot(path=table_screenshot_path)
        print(f"Table screenshot saved to: {table_screenshot_path}")
        
        # Save table HTML for debugging
        table_html = await frame.locator('table').evaluate("el => el.outerHTML")
        table_html_path = os.path.join(output_dir, f"table_html_page{page_num}.html")
        with open(table_html_path, 'w', encoding='utf-8') as f:
            f.write(table_html)
        print(f"Table HTML saved to: {table_html_path}")
        
        # Extract worker data from rows
        workers_data = []
        
        for row in rows:
            try:
                # Skip header rows or empty rows
                if await row.locator('td').count() < 3:
                    continue
                
                # Get all cells in the row
                cells = await row.locator('td').all()
                
                # Extract worker name from the third column (index 2)
                worker_cell = cells[2]
                worker_name = await worker_cell.text_content()
                
                # Clean up worker name
                worker_name = worker_name.strip()
                if "Click to view" in worker_name:
                    # Try to extract just the IP-like part
                    worker_name = worker_name.split("Click to view")[0].strip()
                
                # Extract other data
                ten_min_hashrate = await cells[3].text_content() if len(cells) > 3 else ""
                one_h_hashrate = await cells[4].text_content() if len(cells) > 4 else ""
                h24_hashrate = await cells[5].text_content() if len(cells) > 5 else ""
                rejection_rate = await cells[6].text_content() if len(cells) > 6 else ""
                last_share_time = await cells[7].text_content() if len(cells) > 7 else ""
                connections_24h = await cells[8].text_content() if len(cells) > 8 else ""
                
                # Create worker data dictionary
                worker_data = {
                    "worker": worker_name,
                    "ten_min_hashrate": ten_min_hashrate.strip(),
                    "one_h_hashrate": one_h_hashrate.strip(),
                    "h24_hashrate": h24_hashrate.strip(),
                    "rejection_rate": rejection_rate.strip(),
                    "last_share_time": last_share_time.strip(),
                    "connections_24h": connections_24h.strip(),
                    "hashrate_chart": "",
                    "status": "active",
                    "timestamp": datetime.now().isoformat(),
                    "observer_user_id": observer_user_id,
                    "coin_type": coin_type
                }
                
                workers_data.append(worker_data)
            except Exception as e:
                print(f"Error extracting data from row: {e}")
        
        # Save worker rows debug info
        debug_path = os.path.join(output_dir, f"worker_rows_debug_page{page_num}.json")
        with open(debug_path, 'w', encoding='utf-8') as f:
            json.dump(workers_data, f, indent=2)
        print(f"Worker rows debug info saved to: {debug_path}")
        
        print(f"Found {len(workers_data)} workers on page {page_num}")
        if workers_data:
            print(f"First worker data: {workers_data[0]}")
        
        all_workers.extend(workers_data)
        
        # Navigate to next page if not on the last page
        if page_num < total_pages:
            print(f"Navigating to page {page_num + 1}...")
            await frame.locator('button.ant-pagination-item-link[aria-label="Next page"]').click()
            await asyncio.sleep(2)  # Wait for page to load
    
    print(f"Total workers extracted: {len(all_workers)}")
    return all_workers, screenshot_path

async def process_account(access_key, user_id, coin_type, output_dir):
    """Process a single account."""
    print(f"\n==================================================")
    print(f"Processing account: {user_id} ({coin_type})")
    print(f"==================================================")
    
    async with async_playwright() as playwright:
        # Launch browser
        print("Launching browser...")
        browser, context, page = await setup_browser(playwright)
        
        try:
            # Navigate to observer page
            observer_url = f"https://www.antpool.com/observer?accessKey={access_key}&coinType={coin_type}&observerUserId={user_id}"
            print(f"Navigating to observer page: {observer_url}")
            await page.goto(observer_url)
            print("Page loaded")
            
            # Handle consent dialog
            print("Handling consent dialog...")
            await handle_consent_dialog(page)
            print("Consent dialog handling completed")
            
            # Wait for hashrate chart to load
            print("Waiting for hashrate chart...")
            await page.wait_for_selector(".ant-card-body", timeout=30000)
            print("Hashrate chart loaded successfully")
            
            # Ensure no modals are present
            print("Ensuring no modals are present...")
            await page.evaluate("""() => {
                document.querySelectorAll('.ant-modal-close').forEach(el => el.click());
                document.querySelectorAll('.ant-modal-mask').forEach(el => el.remove());
                document.querySelectorAll('.ant-modal-wrap').forEach(el => el.remove());
            }""")
            print("Removed any modal elements")
            
            # Navigate to Worker tab
            print("Navigating to Worker tab...")
            print("Ensuring no modals are present...")
            await page.evaluate("""() => {
                document.querySelectorAll('.ant-modal-close').forEach(el => el.click());
                document.querySelectorAll('.ant-modal-mask').forEach(el => el.remove());
                document.querySelectorAll('.ant-modal-wrap').forEach(el => el.remove());
            }""")
            print("Removed any modal elements")
            
            # Click Worker tab using JavaScript
            await page.evaluate("""() => {
                const tabs = document.querySelectorAll('.ant-tabs-tab');
                for (const tab of tabs) {
                    if (tab.textContent.includes('Worker')) {
                        tab.click();
                        return true;
                    }
                }
                return false;
            }""")
            print("Clicked Worker tab using JavaScript")
            
            # Take screenshot after clicking Worker tab
            worker_tab_screenshot = os.path.join(output_dir, "worker_tab_clicked.png")
            await page.screenshot(path=worker_tab_screenshot)
            print(f"Screenshot saved after clicking Worker tab: {worker_tab_screenshot}")
            
            # Wait for worker table to load
            await page.wait_for_selector("table", timeout=30000)
            print("Worker table loaded")
            
            # Find the frame containing the worker table
            print("Ensuring no modals are present...")
            await page.evaluate("""() => {
                document.querySelectorAll('.ant-modal-close').forEach(el => el.click());
                document.querySelectorAll('.ant-modal-mask').forEach(el => el.remove());
                document.querySelectorAll('.ant-modal-wrap').forEach(el => el.remove());
            }""")
            print("Removed any modal elements")
            
            frames = page.frames
            print(f"Found {len(frames)} frames on the page")
            
            main_frame = page.main_frame
            print(f"Checking frame 0: {main_frame.name} - URL: {main_frame.url}")
            
            # Count tables in main frame
            tables_count = await main_frame.locator('table').count()
            print(f"Found {tables_count} tables in frame 0")
            
            # Use main frame for extraction
            frame_to_use = main_frame
            print(f"Using frame with URL: {frame_to_use.url}")
            
            # Wait for loading indicators to disappear
            await page.wait_for_function("""() => {
                return !document.querySelector('.ant-spin-spinning') && 
                       !document.querySelector('.ant-spin-dot') &&
                       !document.querySelector('.loading');
            }""")
            print("Loading indicators disappeared")
            
            # Extract worker statistics
            worker_stats, screenshot_path = await extract_worker_stats(
                page, frame_to_use, output_dir, user_id, coin_type
            )
            
            # Save worker statistics to JSON file
            print("Saving worker statistics...")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            output_file = os.path.join(output_dir, f"worker_stats_{user_id}_{timestamp}.json")
            
            save_json_data(worker_stats, output_file)
            print(f"Worker statistics saved to: {output_file}")
            
            # Save to Supabase if environment variables are set
            if os.environ.get("SUPABASE_URL") and os.environ.get("SUPABASE_KEY"):
                try:
                    result = save_worker_stats(worker_stats)
                    print(f"Supabase save result: {result}")
                except Exception as e:
                    print(f"Error saving to Supabase: {e}")
            
            print("Scraping completed successfully!")
            print(f"Total workers extracted: {len(worker_stats)}")
            print(f"Output file: {output_file}")
            print(f"Screenshot: {screenshot_path}")
            
            return True
            
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
            return False
            
        finally:
            # Close browser
            await browser.close()
            print("Browser closed")

async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Antpool Worker Stats Scraper")
    parser.add_argument("--access_key", help="Access key for the observer page")
    parser.add_argument("--user_id", help="Observer user ID")
    parser.add_argument("--coin_type", default="BTC", help="Coin type (default: BTC)")
    parser.add_argument("--output_dir", help="Output directory for JSON and screenshots")
    parser.add_argument("--use_supabase", action="store_true", help="Use Supabase to get account credentials")
    
    args = parser.parse_args()
    
    # Set default output directory if not provided
    if not args.output_dir:
        args.output_dir = os.path.join(os.getcwd(), "output")
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Process accounts
    accounts_processed = 0
    successful_accounts = 0
    
    if args.use_supabase:
        # Get active accounts from Supabase
        active_accounts = get_active_accounts()
        
        if not active_accounts:
            print("No active accounts found in Supabase. Exiting.")
            return
        
        print(f"Retrieved {len(active_accounts)} active accounts from Supabase")
        
        # Process each active account
        for account in active_accounts:
            access_key = account.get("access_key")
            user_id = account.get("user_id")
            coin_type = account.get("coin_type", "BTC")
            
            if not access_key or not user_id:
                print(f"Skipping account with missing credentials: {account}")
                continue
            
            print(f"Starting Antpool worker scraper for {user_id} ({coin_type})...")
            success = await process_account(access_key, user_id, coin_type, args.output_dir)
            accounts_processed += 1
            if success:
                successful_accounts += 1
    else:
        # Use command-line arguments
        if not args.access_key or not args.user_id:
            print("Error: access_key and user_id are required when not using Supabase")
            print("Usage: python3 antpool_worker_scraper.py --access_key=<access_key> --user_id=<user_id> [--coin_type=<coin_type>] [--output_dir=<output_dir>]")
            print("   or: python3 antpool_worker_scraper.py --use_supabase [--output_dir=<output_dir>]")
            return
        
        print(f"Starting Antpool worker scraper for {args.user_id} ({args.coin_type})...")
        success = await process_account(args.access_key, args.user_id, args.coin_type, args.output_dir)
        accounts_processed += 1
        if success:
            successful_accounts += 1
    
    print("Scraping completed successfully!")
    print(f"Total accounts processed: {accounts_processed}")
    print(f"Successful accounts: {successful_accounts}")

if __name__ == "__main__":
    asyncio.run(main())
