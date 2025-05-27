#!/usr/bin/env python3
"""
Antpool Dashboard Scraper - Multi-Account Version

This script scrapes dashboard metrics from Antpool for multiple accounts
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

async def scrape_dashboard(page, access_key, user_id, coin_type):
    """Scrape dashboard metrics from Antpool."""
    print(f"Scraping dashboard for {user_id} ({coin_type})...")
    
    # Navigate to observer page
    observer_url = f"https://www.antpool.com/observer?accessKey={access_key}&coinType={coin_type}&observerUserId={user_id}"
    await page.goto(observer_url, wait_until="networkidle")
    print(f"Navigated to observer page for {user_id}")
    
    # Handle cookie consent if needed
    await handle_cookie_consent(page)
    
    # Wait for dashboard to load
    await page.wait_for_selector(".dashboard-container", timeout=30000)
    print("Dashboard loaded")
    
    # Extract dashboard metrics
    dashboard_data = {}
    
    try:
        # Extract hashrate metrics
        hashrate_elements = await page.query_selector_all(".hashrate-item")
        if hashrate_elements and len(hashrate_elements) >= 2:
            # 10-min hashrate
            ten_min_element = hashrate_elements[0]
            ten_min_value = await ten_min_element.query_selector(".value")
            if ten_min_value:
                dashboard_data["ten_min_hashrate"] = await ten_min_value.inner_text()
            
            # 24-hour hashrate
            day_element = hashrate_elements[1]
            day_value = await day_element.query_selector(".value")
            if day_value:
                dashboard_data["day_hashrate"] = await day_value.inner_text()
        
        # Extract worker counts
        worker_elements = await page.query_selector_all(".worker-item")
        if worker_elements and len(worker_elements) >= 2:
            # Active workers
            active_element = worker_elements[0]
            active_value = await active_element.query_selector(".value")
            if active_value:
                dashboard_data["active_workers"] = await active_value.inner_text()
            
            # Inactive workers
            inactive_element = worker_elements[1]
            inactive_value = await inactive_element.query_selector(".value")
            if inactive_value:
                dashboard_data["inactive_workers"] = await inactive_value.inner_text()
        
        # Extract account balance
        balance_element = await page.query_selector(".balance-item .value")
        if balance_element:
            dashboard_data["account_balance"] = await balance_element.inner_text()
        
        # Extract yesterday's earnings
        earnings_element = await page.query_selector(".earnings-item .value")
        if earnings_element:
            dashboard_data["yesterday_earnings"] = await earnings_element.inner_text()
        
    except Exception as e:
        print(f"Error extracting dashboard metrics: {e}")
        traceback.print_exc()
    
    # Add metadata
    dashboard_data["timestamp"] = format_timestamp()
    dashboard_data["observer_user_id"] = user_id
    dashboard_data["coin_type"] = coin_type
    
    print(f"Extracted dashboard metrics for {user_id}: {json.dumps(dashboard_data, indent=2)}")
    return dashboard_data

async def take_dashboard_screenshot(page, output_dir, user_id, timestamp_str):
    """Take a screenshot of the dashboard."""
    try:
        # Wait for dashboard to be visible
        await page.wait_for_selector(".dashboard-container", timeout=10000)
        
        # Take screenshot
        screenshot_path = os.path.join(output_dir, f"{timestamp_str}_Antpool_BTC.png")
        await take_screenshot(page, screenshot_path)
        print(f"Saved dashboard screenshot to {screenshot_path}")
        return screenshot_path
    except Exception as e:
        print(f"Error taking dashboard screenshot: {e}")
        traceback.print_exc()
        return None

async def save_to_supabase(supabase, dashboard_data):
    """Save dashboard data to Supabase."""
    try:
        # Insert data into mining_pool_stats table
        result = supabase.table("mining_pool_stats").insert(dashboard_data).execute()
        print(f"Saved dashboard data to Supabase: {result}")
        return True
    except Exception as e:
        print(f"Error saving to Supabase: {e}")
        traceback.print_exc()
        return False

async def process_account(browser, output_dir, supabase, account):
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
            
            # Scrape dashboard
            dashboard_data = await scrape_dashboard(page, access_key, user_id, coin_type)
            
            # Take screenshot
            screenshot_path = await take_dashboard_screenshot(page, output_dir, user_id, timestamp_str)
            
            # Save to file
            json_path = os.path.join(output_dir, f"pool_stats_{user_id}_{timestamp_str}.json")
            save_json_to_file(dashboard_data, json_path)
            print(f"Saved dashboard data to {json_path}")
            
            # Save to Supabase
            if supabase:
                await save_to_supabase(supabase, dashboard_data)
            
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
            result = await process_account(browser, args.output_dir, supabase, account)
            results.append(result)
        
        # Print summary
        success_count = sum(1 for r in results if r)
        print(f"Processed {len(accounts)} accounts: {success_count} succeeded, {len(accounts) - success_count} failed")
    
    return 0

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Antpool Dashboard Scraper - Multi-Account Version")
    parser.add_argument("--access_key", help="Antpool access key (optional if using Supabase)")
    parser.add_argument("--user_id", help="Antpool observer user ID (optional if using Supabase)")
    parser.add_argument("--coin_type", default="BTC", help="Coin type (default: BTC)")
    parser.add_argument("--output_dir", default="./output", help="Output directory for JSON and screenshots")
    parser.add_argument("--skip_supabase", action="store_true", help="Skip Supabase integration")
    
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
