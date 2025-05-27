#!/usr/bin/env python3
"""
Antpool Dashboard Stats Scraper

This script scrapes dashboard statistics from Antpool's observer page and saves the data
to a JSON file. It extracts key metrics like hashrates, worker counts, account balance,
and earnings.

Usage:
    python3 antpool_dashboard_scraper.py --access_key=<access_key> --user_id=<observer_user_id> --coin_type=<coin_type> --output_dir=<output_dir>

Example:
    python3 antpool_dashboard_scraper.py --access_key=eInFJrwSbrtDheJHTygV --user_id=Mack81 --coin_type=BTC --output_dir=/home/user/output
"""

import argparse
import asyncio
import json
import os
import re
from datetime import datetime

from playwright.async_api import async_playwright

# Import utility modules
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.browser_utils import setup_browser, handle_consent_dialog
from utils.data_utils import save_json_data
from utils.supabase_utils import save_pool_stats

async def extract_dashboard_stats(page, output_dir, observer_user_id, coin_type):
    """Extract dashboard statistics from the observer page."""
    print("Extracting dashboard statistics...")
    
    # Ensure no modals are present
    print("Ensuring no modals are present...")
    await page.evaluate("""() => {
        document.querySelectorAll('.ant-modal-close').forEach(el => el.click());
        document.querySelectorAll('.ant-modal-mask').forEach(el => el.remove());
        document.querySelectorAll('.ant-modal-wrap').forEach(el => el.remove());
    }""")
    print("Removed any modal elements")
    
    # Extract hashrate data
    print("Extracting hashrate data...")
    
    # Get 10-minute hashrate
    ten_min_hashrate_element = await page.locator('.ant-card-body .ant-statistic-content-value').first.text_content()
    ten_min_hashrate = ten_min_hashrate_element.strip() if ten_min_hashrate_element else "0"
    
    # Get 24-hour hashrate
    day_hashrate_element = await page.locator('.ant-card-body .ant-statistic-content-value').nth(1).text_content()
    day_hashrate = day_hashrate_element.strip() if day_hashrate_element else "0"
    
    # Extract worker counts
    print("Extracting worker counts...")
    
    # Get active workers count
    active_workers_text = await page.locator('text=Active Workers').locator('xpath=..').text_content()
    active_workers_match = re.search(r'Active Workers\s*(\d+)', active_workers_text)
    active_workers = int(active_workers_match.group(1)) if active_workers_match else 0
    
    # Get inactive workers count
    inactive_workers_text = await page.locator('text=Inactive Workers').locator('xpath=..').text_content()
    inactive_workers_match = re.search(r'Inactive Workers\s*(\d+)', inactive_workers_text)
    inactive_workers = int(inactive_workers_match.group(1)) if inactive_workers_match else 0
    
    # Extract account balance
    print("Extracting account balance...")
    account_balance_element = await page.locator('text=Account Balance').locator('xpath=../..').locator('.ant-statistic-content-value').text_content()
    account_balance = account_balance_element.strip() if account_balance_element else "0"
    
    # Extract yesterday's earnings
    print("Extracting yesterday's earnings...")
    yesterday_earnings_element = await page.locator('text=Yesterday Earnings').locator('xpath=../..').locator('.ant-statistic-content-value').text_content()
    yesterday_earnings = yesterday_earnings_element.strip() if yesterday_earnings_element else "0"
    
    # Create dashboard stats dictionary
    dashboard_stats = {
        "ten_min_hashrate": ten_min_hashrate,
        "day_hashrate": day_hashrate,
        "active_workers": active_workers,
        "inactive_workers": inactive_workers,
        "account_balance": account_balance,
        "yesterday_earnings": yesterday_earnings,
        "timestamp": datetime.now().isoformat(),
        "observer_user_id": observer_user_id,
        "coin_type": coin_type
    }
    
    # Capture dashboard screenshot
    print("Capturing dashboard screenshot...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    screenshot_path = os.path.join(output_dir, f"{timestamp}_Antpool_{coin_type}.png")
    await page.screenshot(path=screenshot_path, full_page=True)
    print(f"Dashboard screenshot saved to: {screenshot_path}")
    
    return dashboard_stats, screenshot_path

async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Antpool Dashboard Stats Scraper")
    parser.add_argument("--access_key", required=True, help="Access key for the observer page")
    parser.add_argument("--user_id", required=True, help="Observer user ID")
    parser.add_argument("--coin_type", default="BTC", help="Coin type (default: BTC)")
    parser.add_argument("--output_dir", help="Output directory for JSON and screenshots")
    
    args = parser.parse_args()
    
    # Set default output directory if not provided
    if not args.output_dir:
        args.output_dir = os.path.join(os.getcwd(), "output")
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    print(f"Starting Antpool dashboard scraper for {args.user_id} ({args.coin_type})...")
    
    async with async_playwright() as playwright:
        # Launch browser
        print("Launching browser...")
        browser, context, page = await setup_browser(playwright)
        
        try:
            # Navigate to observer page
            observer_url = f"https://www.antpool.com/observer?accessKey={args.access_key}&coinType={args.coin_type}&observerUserId={args.user_id}"
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
            
            # Extract dashboard statistics
            dashboard_stats, screenshot_path = await extract_dashboard_stats(
                page, args.output_dir, args.user_id, args.coin_type
            )
            
            # Save dashboard statistics to JSON file
            print("Saving dashboard statistics...")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            output_file = os.path.join(args.output_dir, f"pool_stats_{args.user_id}_{timestamp}.json")
            
            save_json_data([dashboard_stats], output_file)
            print(f"Dashboard statistics saved to: {output_file}")
            
            # Save to Supabase if environment variables are set
            if os.environ.get("SUPABASE_URL") and os.environ.get("SUPABASE_KEY"):
                try:
                    result = save_pool_stats(dashboard_stats)
                    print(f"Supabase save result: {result}")
                except Exception as e:
                    print(f"Error saving to Supabase: {e}")
            
            print("Scraping completed successfully!")
            print(f"Output file: {output_file}")
            print(f"Screenshot: {screenshot_path}")
            
            return 0
            
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
            return 1
            
        finally:
            # Close browser
            await browser.close()
            print("Scraping completed successfully!")

if __name__ == "__main__":
    asyncio.run(main())
