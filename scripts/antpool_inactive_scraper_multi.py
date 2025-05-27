#!/usr/bin/env python3
"""
Antpool Inactive Worker Scraper - Multi-Account Version

This script scrapes inactive worker statistics from Antpool for multiple accounts
stored in Supabase.
"""

import os
import sys
import json
import argparse
import asyncio
from datetime import datetime
import traceback
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from utils.browser_utils import setup_browser, handle_cookie_consent, take_screenshot
    from utils.data_utils import save_json_to_file, format_timestamp
    from utils.supabase_utils import get_supabase_client
except ImportError:
    # Fallback for direct script execution
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from utils.browser_utils import setup_browser, handle_cookie_consent, take_screenshot
    from utils.data_utils import save_json_to_file, format_timestamp
    from utils.supabase_utils import get_supabase_client

async def scrape_inactive_workers(page, access_key, user_id, coin_type, debug=False):
    """Scrape inactive worker statistics from Antpool."""
    print(f"Scraping inactive workers for {user_id} ({coin_type})...")
    
    # Navigate to observer page
    observer_url = f"https://www.antpool.com/observer?accessKey={access_key}&coinType={coin_type}&observerUserId={user_id}"
    await page.goto(observer_url, wait_until="networkidle")
    print(f"Navigated to observer page for {user_id}")
    
    # Handle cookie consent if needed
    await handle_cookie_consent(page)
    
    # Navigate to workers page
    await page.click('text="Workers"')
    print("Navigated to workers page")
    
    # Click on inactive workers tab
    try:
        await page.click('text="Inactive Workers"')
        print("Navigated to inactive workers tab")
    except Exception as e:
        print(f"Error clicking inactive workers tab: {e}")
        print("Trying alternative method to access inactive workers")
        
        # Try alternative method - look for tab elements
        tabs = await page.query_selector_all(".ant-tabs-tab")
        for tab in tabs:
            tab_text = await tab.inner_text()
            if "inactive" in tab_text.lower():
                await tab.click()
                print("Found and clicked inactive workers tab")
                break
    
    # Wait for inactive workers table to load
    await page.wait_for_selector(".ant-table-wrapper", timeout=30000)
    print("Inactive workers table loaded")
    
    # Extract inactive worker data
    inactive_workers_data = []
    
    try:
        # Get table rows
        rows = await page.query_selector_all(".ant-table-tbody tr")
        print(f"Found {len(rows)} inactive worker rows")
        
        # Debug: Save table HTML if requested
        if debug:
            table_html = await page.evaluate('() => document.querySelector(".ant-table-wrapper").outerHTML')
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "debug", "inactive_table_html.html"), "w") as f:
                f.write(table_html)
            print("Saved inactive table HTML for debugging")
        
        # Process each row
        for row_idx, row in enumerate(rows):
            try:
                # Extract cells
                cells = await row.query_selector_all("td")
                
                if len(cells) < 5:
                    print(f"Skipping row {row_idx+1}: Not enough cells ({len(cells)})")
                    continue
                
                # Extract worker name (in the third column, index 2)
                worker_cell = cells[2]
                worker_name_element = await worker_cell.query_selector("a")
                
                if worker_name_element:
                    worker_name = await worker_name_element.inner_text()
                else:
                    # Fallback to cell text if no link is found
                    worker_name = await worker_cell.inner_text()
                
                # Extract other metrics
                last_share_time = await cells[3].inner_text() if len(cells) > 3 else ""
                inactive_time = await cells[4].inner_text() if len(cells) > 4 else ""
                h24_hashrate = await cells[5].inner_text() if len(cells) > 5 else ""
                rejection_rate = await cells[6].inner_text() if len(cells) > 6 else ""
                
                # Create inactive worker data dictionary
                inactive_worker_data = {
                    "worker": worker_name,
                    "last_share_time": last_share_time,
                    "inactive_time": inactive_time,
                    "h24_hashrate": h24_hashrate,
                    "rejection_rate": rejection_rate,
                    "status": "inactive",
                    "timestamp": format_timestamp(),
                    "observer_user_id": user_id,
                    "coin_type": coin_type
                }
                
                inactive_workers_data.append(inactive_worker_data)
                print(f"Extracted inactive worker: {worker_name}")
                
            except Exception as e:
                print(f"Error extracting inactive worker row {row_idx+1}: {e}")
                traceback.print_exc()
        
        # Debug: Save inactive worker rows if requested
        if debug:
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "debug", "inactive_worker_rows_debug.json"), "w") as f:
                json.dump(inactive_workers_data, f, indent=2)
            print("Saved inactive worker rows for debugging")
        
    except Exception as e:
        print(f"Error extracting inactive workers: {e}")
        traceback.print_exc()
    
    print(f"Extracted {len(inactive_workers_data)} inactive workers for {user_id}")
    return inactive_workers_data

async def take_inactive_workers_screenshot(page, output_dir, user_id, timestamp_str):
    """Take a screenshot of the inactive workers page."""
    try:
        # Wait for inactive workers table to be visible
        await page.wait_for_selector(".ant-table-wrapper", timeout=10000)
        
        # Take screenshot
        screenshot_path = os.path.join(output_dir, f"{timestamp_str}_Antpool_BTC_inactive_workers.png")
        await take_screenshot(page, screenshot_path)
        print(f"Saved inactive workers screenshot to {screenshot_path}")
        return screenshot_path
    except Exception as e:
        print(f"Error taking inactive workers screenshot: {e}")
        traceback.print_exc()
        return None

async def save_to_supabase(supabase, inactive_workers_data):
    """Save inactive worker data to Supabase."""
    try:
        # Insert data into mining_inactive_workers table
        for worker in inactive_workers_data:
            result = supabase.table("mining_inactive_workers").insert(worker).execute()
            print(f"Saved inactive worker {worker['worker']} to Supabase")
        return True
    except Exception as e:
        print(f"Error saving to Supabase: {e}")
        traceback.print_exc()
        return False

async def process_account(browser, output_dir, supabase, account, debug=False):
    """Process a single account."""
    try:
        # Extract account details
        account_name = account.get("account_name", "Unknown")
        access_key = account.get("access_key", "")
        user_id = account.get("user_id", "")
        coin_type = account.get("coin_type", "BTC")
        
        print(f"Processing account: {account_name} ({user_id})")
        
        # Skip if missing required fields
        if not access_key or not user_id:
            print(f"Skipping account {account_name}: Missing required fields")
            return False
        
        # Create a new page for this account
        page = await browser.new_page()
        
        try:
            # Get current timestamp for filenames
            timestamp = datetime.now()
            timestamp_str = timestamp.strftime("%Y%m%d_%H%M")
            
            # Scrape inactive workers
            inactive_workers_data = await scrape_inactive_workers(page, access_key, user_id, coin_type, debug)
            
            # Take screenshot
            screenshot_path = await take_inactive_workers_screenshot(page, output_dir, user_id, timestamp_str)
            
            # Save to file
            json_path = os.path.join(output_dir, f"inactive_worker_stats_{user_id}_{timestamp_str}.json")
            save_json_to_file(inactive_workers_data, json_path)
            print(f"Saved inactive worker data to {json_path}")
            
            # Save to Supabase
            if supabase and inactive_workers_data:
                await save_to_supabase(supabase, inactive_workers_data)
            
            # Update last_scraped_at in account_credentials
            if supabase:
                try:
                    supabase.table("account_credentials").update({"last_scraped_at": format_timestamp()}).eq("user_id", user_id).execute()
                except Exception as e:
                    print(f"Error updating last_scraped_at: {e}")
            
            print(f"Successfully processed account: {account_name}")
            return True
            
        finally:
            # Close the page
            await page.close()
            
    except Exception as e:
        print(f"Error processing account {account.get('account_name', 'Unknown')}: {e}")
        traceback.print_exc()
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
                print(f"Successfully fetched {len(accounts)} accounts using RPC function")
                return accounts
        except Exception as rpc_error:
            print(f"Error fetching accounts using RPC function: {rpc_error}")
            # Continue to fallback method
        
        # Fallback: direct query
        print("Falling back to direct query...")
        response = supabase.table("account_credentials").select("*").eq("is_active", True).order("priority.desc,last_scraped_at.asc.nullsfirst").execute()
        accounts = response.data
        print(f"Successfully fetched {len(accounts)} accounts using direct query")
        return accounts
        
    except Exception as e:
        print(f"Error fetching accounts from Supabase: {e}")
        traceback.print_exc()
        return []

async def main_async(args):
    """Main async function."""
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Create debug directory if needed
    if args.debug:
        os.makedirs(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "debug"), exist_ok=True)
    
    # Initialize Supabase client
    supabase = None
    if not args.skip_supabase:
        supabase = get_supabase_client()
    
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
    elif supabase:
        # Fetch accounts from Supabase
        accounts = await fetch_accounts_from_supabase(supabase)
    
    if not accounts:
        print("No accounts to scrape. Exiting.")
        return 1
    
    print(f"Found {len(accounts)} accounts to scrape")
    
    # Initialize browser
    async with await setup_browser() as browser:
        # Process each account
        results = []
        for account in accounts:
            result = await process_account(browser, args.output_dir, supabase, account, args.debug)
            results.append(result)
        
        # Print summary
        success_count = sum(1 for r in results if r)
        print(f"Processed {len(accounts)} accounts: {success_count} succeeded, {len(accounts) - success_count} failed")
    
    return 0

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Antpool Inactive Worker Scraper - Multi-Account Version")
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
        print(f"Error in main: {e}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
