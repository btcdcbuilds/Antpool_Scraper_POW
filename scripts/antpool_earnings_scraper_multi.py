#!/usr/bin/env python3
"""
Antpool Earnings Scraper - Multi-Account Version

This script scrapes earnings history from Antpool for multiple accounts
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

async def scrape_earnings(page, access_key, user_id, coin_type, debug=False):
    """Scrape earnings history from Antpool."""
    print(f"Scraping earnings for {user_id} ({coin_type})...")
    
    # Navigate to observer page
    observer_url = f"https://www.antpool.com/observer?accessKey={access_key}&coinType={coin_type}&observerUserId={user_id}"
    await page.goto(observer_url, wait_until="networkidle")
    print(f"Navigated to observer page for {user_id}")
    
    # Handle cookie consent if needed
    await handle_cookie_consent(page)
    
    # Navigate to earnings page
    await page.click('text="Earnings"')
    print("Navigated to earnings page")
    
    # Wait for earnings table to load
    await page.wait_for_selector(".ant-table-wrapper", timeout=30000)
    print("Earnings table loaded")
    
    # Extract earnings data
    earnings_data = []
    
    try:
        # Get table rows
        rows = await page.query_selector_all(".ant-table-tbody tr")
        print(f"Found {len(rows)} earnings rows")
        
        # Debug: Save table HTML if requested
        if debug:
            table_html = await page.evaluate('() => document.querySelector(".ant-table-wrapper").outerHTML')
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "debug", "earnings_table_html.html"), "w") as f:
                f.write(table_html)
            print("Saved earnings table HTML for debugging")
        
        # Process each row
        for row_idx, row in enumerate(rows):
            try:
                # Extract cells
                cells = await row.query_selector_all("td")
                
                if len(cells) < 5:
                    print(f"Skipping row {row_idx+1}: Not enough cells ({len(cells)})")
                    continue
                
                # Extract data from cells
                date = await cells[0].inner_text() if len(cells) > 0 else ""
                daily_hashrate = await cells[1].inner_text() if len(cells) > 1 else ""
                
                # Extract earnings amount and currency
                earnings_text = await cells[2].inner_text() if len(cells) > 2 else ""
                earnings_parts = earnings_text.strip().split(" ")
                if len(earnings_parts) >= 2:
                    earnings_amount = earnings_parts[0]
                    earnings_currency = earnings_parts[1]
                else:
                    earnings_amount = earnings_text
                    earnings_currency = ""
                
                earnings_type = await cells[3].inner_text() if len(cells) > 3 else ""
                payment_status = await cells[4].inner_text() if len(cells) > 4 else ""
                
                # Create earnings data dictionary
                earning_data = {
                    "date": date,
                    "daily_hashrate": daily_hashrate,
                    "earnings_amount": earnings_amount,
                    "earnings_currency": earnings_currency,
                    "earnings_type": earnings_type,
                    "payment_status": payment_status,
                    "timestamp": format_timestamp(),
                    "observer_user_id": user_id,
                    "coin_type": coin_type
                }
                
                earnings_data.append(earning_data)
                print(f"Extracted earnings for {date}")
                
            except Exception as e:
                print(f"Error extracting earnings row {row_idx+1}: {e}")
                traceback.print_exc()
        
        # Debug: Save earnings rows if requested
        if debug:
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "debug", "earnings_rows_debug.json"), "w") as f:
                json.dump(earnings_data, f, indent=2)
            print("Saved earnings rows for debugging")
        
    except Exception as e:
        print(f"Error extracting earnings: {e}")
        traceback.print_exc()
    
    print(f"Extracted {len(earnings_data)} earnings entries for {user_id}")
    return earnings_data

async def take_earnings_screenshot(page, output_dir, user_id, timestamp_str):
    """Take a screenshot of the earnings page."""
    try:
        # Wait for earnings table to be visible
        await page.wait_for_selector(".ant-table-wrapper", timeout=10000)
        
        # Take screenshot
        screenshot_path = os.path.join(output_dir, f"{timestamp_str}_Antpool_BTC_earnings.png")
        await take_screenshot(page, screenshot_path)
        print(f"Saved earnings screenshot to {screenshot_path}")
        return screenshot_path
    except Exception as e:
        print(f"Error taking earnings screenshot: {e}")
        traceback.print_exc()
        return None

async def save_to_supabase(supabase, earnings_data):
    """Save earnings data to Supabase."""
    try:
        # Insert data into mining_earnings table
        for earning in earnings_data:
            result = supabase.table("mining_earnings").insert(earning).execute()
            print(f"Saved earnings for {earning['date']} to Supabase")
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
            
            # Scrape earnings
            earnings_data = await scrape_earnings(page, access_key, user_id, coin_type, debug)
            
            # Take screenshot
            screenshot_path = await take_earnings_screenshot(page, output_dir, user_id, timestamp_str)
            
            # Save to file
            json_path = os.path.join(output_dir, f"earnings_history_{user_id}_{timestamp_str}.json")
            save_json_to_file(earnings_data, json_path)
            print(f"Saved earnings data to {json_path}")
            
            # Save to Supabase
            if supabase and earnings_data:
                await save_to_supabase(supabase, earnings_data)
            
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
    parser = argparse.ArgumentParser(description="Antpool Earnings Scraper - Multi-Account Version")
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
