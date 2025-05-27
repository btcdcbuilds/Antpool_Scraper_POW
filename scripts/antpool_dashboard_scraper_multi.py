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
    
    # Take a screenshot of the page before waiting for selectors
    debug_screenshot_path = f"./debug_{user_id}_before_selectors.png"
    await take_screenshot(page, debug_screenshot_path)
    print(f"Saved debug screenshot to {debug_screenshot_path}")
    
    # Wait for page to load - using multiple possible selectors
    print("Waiting for dashboard elements to load...")
    try:
        # Try multiple selectors that might indicate the dashboard is loaded
        selectors = [
            ".hashrate-item", 
            ".worker-item", 
            "text=Hashrate", 
            "text=Workers Num",
            "text=Total Earnings"
        ]
        
        for selector in selectors:
            try:
                await page.wait_for_selector(selector, timeout=10000)
                print(f"Dashboard element found: {selector}")
                break
            except Exception as e:
                print(f"Selector {selector} not found, trying next...")
                continue
        
        print("Dashboard elements detected, proceeding with data extraction")
    except Exception as e:
        print(f"Error waiting for dashboard elements: {e}")
        # Save HTML for debugging
        html = await page.content()
        with open(f"./debug_{user_id}_html.html", "w") as f:
            f.write(html)
        print(f"Saved HTML content to debug_{user_id}_html.html")
        raise
    
    # Extract dashboard metrics
    dashboard_data = {}
    
    try:
        print("Starting dashboard metrics extraction...")
        
        # Extract hashrate metrics
        print("Extracting hashrate metrics...")
        try:
            # Try to get 10-minute hashrate
            ten_min_hashrate = await page.evaluate("""
                () => {
                    // Try multiple possible selectors
                    const elements = [
                        document.querySelector('.hashrate-item:first-child .value'),
                        document.querySelector('div:has(> div:contains("10-Minute Hashrate")) .value'),
                        document.querySelector('div:has(> div:contains("10-Minute")) .value'),
                        document.querySelector('div:has(> div:contains("Minute Hashrate")) .value')
                    ];
                    
                    for (const el of elements) {
                        if (el && el.textContent) {
                            return el.textContent.trim();
                        }
                    }
                    
                    // If all selectors fail, try to find by position
                    const allValues = document.querySelectorAll('.value');
                    if (allValues.length > 0) {
                        return allValues[0].textContent.trim();
                    }
                    
                    return null;
                }
            """)
            
            if ten_min_hashrate:
                dashboard_data["ten_min_hashrate"] = ten_min_hashrate
                print(f"Found 10-minute hashrate: {ten_min_hashrate}")
            else:
                print("10-minute hashrate not found")
            
            # Try to get 24-hour hashrate
            day_hashrate = await page.evaluate("""
                () => {
                    // Try multiple possible selectors
                    const elements = [
                        document.querySelector('.hashrate-item:nth-child(2) .value'),
                        document.querySelector('div:has(> div:contains("24H Hashrate")) .value'),
                        document.querySelector('div:has(> div:contains("24H")) .value')
                    ];
                    
                    for (const el of elements) {
                        if (el && el.textContent) {
                            return el.textContent.trim();
                        }
                    }
                    
                    // If all selectors fail, try to find by position
                    const allValues = document.querySelectorAll('.value');
                    if (allValues.length > 1) {
                        return allValues[1].textContent.trim();
                    }
                    
                    return null;
                }
            """)
            
            if day_hashrate:
                dashboard_data["day_hashrate"] = day_hashrate
                print(f"Found 24-hour hashrate: {day_hashrate}")
            else:
                print("24-hour hashrate not found")
                
        except Exception as e:
            print(f"Error extracting hashrate metrics: {e}")
        
        # Extract worker counts
        print("Extracting worker counts...")
        try:
            # Try to get active workers
            active_workers = await page.evaluate("""
                () => {
                    // Try multiple possible selectors
                    const elements = [
                        document.querySelector('.worker-item:first-child .value'),
                        document.querySelector('div:has(> div:contains("Active")) .value'),
                        document.querySelector('text=Active').closest('div').querySelector('.value')
                    ];
                    
                    for (const el of elements) {
                        if (el && el.textContent) {
                            return el.textContent.trim();
                        }
                    }
                    
                    return null;
                }
            """)
            
            if active_workers:
                dashboard_data["active_workers"] = active_workers
                print(f"Found active workers: {active_workers}")
            else:
                print("Active workers count not found")
            
            # Try to get inactive workers
            inactive_workers = await page.evaluate("""
                () => {
                    // Try multiple possible selectors
                    const elements = [
                        document.querySelector('.worker-item:nth-child(2) .value'),
                        document.querySelector('div:has(> div:contains("Inactive")) .value'),
                        document.querySelector('text=Inactive').closest('div').querySelector('.value')
                    ];
                    
                    for (const el of elements) {
                        if (el && el.textContent) {
                            return el.textContent.trim();
                        }
                    }
                    
                    return null;
                }
            """)
            
            if inactive_workers:
                dashboard_data["inactive_workers"] = inactive_workers
                print(f"Found inactive workers: {inactive_workers}")
            else:
                print("Inactive workers count not found")
                
        except Exception as e:
            print(f"Error extracting worker counts: {e}")
        
        # Extract account balance
        print("Extracting account balance...")
        try:
            account_balance = await page.evaluate("""
                () => {
                    // Try multiple possible selectors
                    const elements = [
                        document.querySelector('.balance-item .value'),
                        document.querySelector('div:has(> div:contains("Account Balance")) .value'),
                        document.querySelector('div:has(> div:contains("Balance")) .value')
                    ];
                    
                    for (const el of elements) {
                        if (el && el.textContent) {
                            return el.textContent.trim();
                        }
                    }
                    
                    return null;
                }
            """)
            
            if account_balance:
                dashboard_data["account_balance"] = account_balance
                print(f"Found account balance: {account_balance}")
            else:
                print("Account balance not found")
                
        except Exception as e:
            print(f"Error extracting account balance: {e}")
        
        # Extract yesterday's earnings
        print("Extracting yesterday's earnings...")
        try:
            yesterday_earnings = await page.evaluate("""
                () => {
                    // Try multiple possible selectors
                    const elements = [
                        document.querySelector('.earnings-item .value'),
                        document.querySelector('div:has(> div:contains("Yesterday Earnings")) .value'),
                        document.querySelector('div:has(> div:contains("Earnings")) .value')
                    ];
                    
                    for (const el of elements) {
                        if (el && el.textContent) {
                            return el.textContent.trim();
                        }
                    }
                    
                    return null;
                }
            """)
            
            if yesterday_earnings:
                dashboard_data["yesterday_earnings"] = yesterday_earnings
                print(f"Found yesterday's earnings: {yesterday_earnings}")
            else:
                print("Yesterday's earnings not found")
                
        except Exception as e:
            print(f"Error extracting yesterday's earnings: {e}")
        
    except Exception as e:
        print(f"Error extracting dashboard metrics: {e}")
        traceback.print_exc()
    
    # Add metadata
    dashboard_data["timestamp"] = format_timestamp()
    dashboard_data["observer_user_id"] = user_id
    dashboard_data["coin_type"] = coin_type
    
    # Count how many metrics were successfully extracted
    metrics_count = len(dashboard_data) - 3  # Subtract the 3 metadata fields
    print(f"Successfully extracted {metrics_count} dashboard metrics for {user_id}")
    print(f"Dashboard data: {json.dumps(dashboard_data, indent=2)}")
    
    return dashboard_data

async def take_dashboard_screenshot(page, output_dir, user_id, timestamp_str):
    """Take a screenshot of the dashboard."""
    try:
        print(f"Taking dashboard screenshot for {user_id}...")
        
        # Take screenshot without waiting for specific selectors
        screenshot_path = os.path.join(output_dir, f"{timestamp_str}_Antpool_BTC.png")
        await take_screenshot(page, screenshot_path)
        print(f"✅ Saved dashboard screenshot to {screenshot_path}")
        return screenshot_path
    except Exception as e:
        print(f"❌ Error taking dashboard screenshot: {e}")
        traceback.print_exc()
        return None

async def save_to_supabase(supabase, dashboard_data):
    """Save dashboard data to Supabase."""
    try:
        print("Uploading dashboard data to Supabase...")
        
        # Insert data into mining_pool_stats table
        result = supabase.table("mining_pool_stats").insert(dashboard_data).execute()
        
        # Check if the upload was successful
        if hasattr(result, 'data') and result.data:
            print(f"✅ Successfully uploaded dashboard data to Supabase")
            return True
        else:
            print(f"❌ Failed to upload dashboard data to Supabase: No data returned")
            return False
    except Exception as e:
        print(f"❌ Error uploading to Supabase: {e}")
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
        
        print(f"\n===== Processing account: {account_name} ({user_id}) =====")
        
        # Skip if missing required fields
        if not access_key or not user_id:
            print(f"❌ Skipping account {account_name}: Missing required fields")
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
            print(f"✅ Saved dashboard data to {json_path}")
            
            # Save to Supabase
            if supabase:
                supabase_success = await save_to_supabase(supabase, dashboard_data)
                if supabase_success:
                    print(f"✅ Successfully uploaded dashboard data to Supabase for {account_name}")
                else:
                    print(f"❌ Failed to upload dashboard data to Supabase for {account_name}")
            
            # Update last_scraped_at in account_credentials
            if supabase:
                try:
                    print(f"Updating last_scraped_at for {account_name}...")
                    update_result = supabase.table("account_credentials").update({"last_scraped_at": format_timestamp()}).eq("user_id", user_id).execute()
                    if hasattr(update_result, 'data') and update_result.data:
                        print(f"✅ Successfully updated last_scraped_at for {account_name}")
                    else:
                        print(f"❌ Failed to update last_scraped_at for {account_name}")
                except Exception as e:
                    print(f"❌ Error updating last_scraped_at: {e}")
            
            print(f"✅ Successfully processed account: {account_name}")
            return True
            
        finally:
            # Close the page
            await page.close()
            
    except Exception as e:
        print(f"❌ Error processing account {account.get('account_name', 'Unknown')}: {e}")
        traceback.print_exc()
        return False

async def fetch_accounts_from_supabase(supabase):
    """Fetch accounts from Supabase."""
    try:
        # First try using the RPC function
        try:
            print("Attempting to fetch accounts using RPC function...")
            response = supabase.rpc('get_all_active_accounts', {}).execute()
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
        traceback.print_exc()
        return []

async def main_async(args):
    """Main async function."""
    print("\n===== Starting Antpool Dashboard Scraper =====")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    print(f"Output directory: {os.path.abspath(args.output_dir)}")
    
    # Initialize Supabase client
    supabase = None
    if not args.skip_supabase:
        print("Initializing Supabase client...")
        supabase = get_supabase_client()
        if supabase:
            print("✅ Supabase client initialized successfully")
        else:
            print("❌ Failed to initialize Supabase client")
    
    # Get accounts to scrape
    accounts = []
    if args.access_key and args.user_id:
        # Use command-line arguments
        print("Using command-line arguments for account details")
        accounts = [{
            "account_name": args.user_id,
            "access_key": args.access_key,
            "user_id": args.user_id,
            "coin_type": args.coin_type
        }]
    elif supabase:
        # Fetch accounts from Supabase
        print("Fetching accounts from Supabase...")
        accounts = await fetch_accounts_from_supabase(supabase)
    
    if not accounts:
        print("❌ No accounts to scrape. Exiting.")
        return 1
    
    print(f"✅ Found {len(accounts)} accounts to scrape")
    
    # Initialize browser
    print("Initializing browser...")
    async with await setup_browser() as browser:
        print("✅ Browser initialized successfully")
        
        # Process each account
        results = []
        for account in accounts:
            result = await process_account(browser, args.output_dir, supabase, account)
            results.append(result)
        
        # Print summary
        success_count = sum(1 for r in results if r)
        print("\n===== Dashboard Scraper Summary =====")
        print(f"Total accounts processed: {len(accounts)}")
        print(f"Successful: {success_count}")
        print(f"Failed: {len(accounts) - success_count}")
        print(f"Success rate: {success_count/len(accounts)*100:.1f}%")
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
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
        print(f"❌ Error in main: {e}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
