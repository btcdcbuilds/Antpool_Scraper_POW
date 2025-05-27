#!/usr/bin/env python3
"""
Antpool Dashboard Scraper (Multi-Account Version)

This script scrapes dashboard statistics from Antpool's observer page for multiple accounts
stored in Supabase and saves the data to JSON files and the database.

Usage:
    python3 antpool_dashboard_scraper_multi.py --output_dir=<output_dir> [--single_account --access_key=<access_key> --user_id=<observer_user_id> --coin_type=<coin_type>]

Example:
    python3 antpool_dashboard_scraper_multi.py --output_dir=/home/user/output
    python3 antpool_dashboard_scraper_multi.py --output_dir=/home/user/output --single_account --access_key=eInFJrwSbrtDheJHTygV --user_id=Mack81 --coin_type=BTC
"""

import os
import sys
import json
import asyncio
import argparse
import re
from datetime import datetime
from typing import List, Dict, Any, Optional

from playwright.async_api import async_playwright
import requests
from supabase import create_client, Client

# Import utility modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.browser_utils import setup_browser, handle_consent_dialog
from utils.data_utils import save_json_data
from utils.supabase_utils import save_pool_stats

class AntpoolMultiAccountScraper:
    """Base class for Antpool multi-account scrapers."""
    
    def __init__(self, output_dir: str, single_account: bool = False, 
                 access_key: Optional[str] = None, user_id: Optional[str] = None, 
                 coin_type: Optional[str] = None):
        """Initialize the scraper with output directory and optional account details."""
        self.output_dir = output_dir
        self.single_account = single_account
        self.access_key = access_key
        self.user_id = user_id
        self.coin_type = coin_type or "BTC"
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Initialize Supabase client if environment variables are set
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_KEY")
        self.supabase = None
        
        if self.supabase_url and self.supabase_key:
            self.supabase = create_client(self.supabase_url, self.supabase_key)
            print(f"Supabase client initialized with URL: {self.supabase_url}")
        else:
            print("Supabase environment variables not set. Database operations will be skipped.")
    
    async def get_accounts(self) -> List[Dict[str, Any]]:
        """Get all active accounts from Supabase or use the provided account."""
        if self.single_account:
            if not self.access_key or not self.user_id:
                raise ValueError("access_key and user_id must be provided when single_account is True")
            
            return [{
                "account_name": self.user_id,
                "access_key": self.access_key,
                "user_id": self.user_id,
                "coin_type": self.coin_type
            }]
        
        if not self.supabase:
            raise ValueError("Supabase client not initialized. Cannot fetch accounts.")
        
        try:
            # Call the get_all_active_accounts function
            response = self.supabase.rpc('get_all_active_accounts').execute()
            
            if hasattr(response, 'data') and response.data:
                print(f"Found {len(response.data)} active accounts")
                return response.data
            else:
                print("No active accounts found in Supabase")
                return []
        except Exception as e:
            print(f"Error fetching accounts from Supabase: {e}")
            return []
    
    async def update_last_scraped(self, account_id: int) -> None:
        """Update the last_scraped_at timestamp for an account."""
        if not self.supabase:
            return
        
        try:
            self.supabase.table('account_credentials').update({
                "last_scraped_at": datetime.now().isoformat()
            }).eq('id', account_id).execute()
            
            print(f"Updated last_scraped_at for account ID {account_id}")
        except Exception as e:
            print(f"Error updating last_scraped_at: {e}")
    
    async def scrape_account(self, account: Dict[str, Any]) -> None:
        """Scrape a single account. To be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement scrape_account method")
    
    async def run(self) -> int:
        """Run the scraper for all accounts."""
        try:
            accounts = await self.get_accounts()
            
            if not accounts:
                print("No accounts to scrape. Exiting.")
                return 1
            
            print(f"Starting scraping for {len(accounts)} accounts")
            
            for account in accounts:
                print(f"Scraping account: {account['account_name']}")
                try:
                    await self.scrape_account(account)
                    
                    # Update last_scraped_at if not in single account mode
                    if not self.single_account and 'id' in account:
                        await self.update_last_scraped(account['id'])
                except Exception as e:
                    print(f"Error scraping account {account['account_name']}: {e}")
                    import traceback
                    traceback.print_exc()
            
            print("All accounts scraped successfully")
            return 0
        except Exception as e:
            print(f"Error in run method: {e}")
            import traceback
            traceback.print_exc()
            return 1

class AntpoolDashboardScraper(AntpoolMultiAccountScraper):
    """Scraper for Antpool dashboard statistics."""
    
    async def extract_dashboard_stats(self, page, output_dir, observer_user_id, coin_type):
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
        screenshot_path = os.path.join(output_dir, f"{timestamp}_{observer_user_id}_Antpool_{coin_type}.png")
        await page.screenshot(path=screenshot_path, full_page=True)
        print(f"Dashboard screenshot saved to: {screenshot_path}")
        
        return dashboard_stats, screenshot_path
    
    async def scrape_account(self, account: Dict[str, Any]) -> None:
        """Scrape dashboard statistics for a single account."""
        access_key = account['access_key']
        user_id = account['user_id']
        coin_type = account.get('coin_type', 'BTC')
        account_name = account.get('account_name', user_id)
        
        print(f"Scraping dashboard statistics for account {account_name} ({coin_type})...")
        
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
                
                # Extract dashboard statistics
                dashboard_stats, screenshot_path = await self.extract_dashboard_stats(
                    page, self.output_dir, account_name, coin_type
                )
                
                # Save dashboard statistics to JSON file
                print("Saving dashboard statistics...")
                timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                output_file = os.path.join(self.output_dir, f"pool_stats_{account_name}_{timestamp}.json")
                
                save_json_data([dashboard_stats], output_file)
                print(f"Dashboard statistics saved to: {output_file}")
                
                # Save to Supabase if client is initialized
                if self.supabase:
                    try:
                        result = save_pool_stats(dashboard_stats)
                        print(f"Supabase save result: {result}")
                    except Exception as e:
                        print(f"Error saving to Supabase: {e}")
                
                print(f"Scraping completed successfully for account {account_name}!")
                print(f"Output file: {output_file}")
                print(f"Screenshot: {screenshot_path}")
                
            except Exception as e:
                print(f"Error scraping account {account_name}: {e}")
                import traceback
                traceback.print_exc()
                raise
                
            finally:
                # Close browser
                await browser.close()

async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Antpool Dashboard Statistics Scraper")
    parser.add_argument("--access_key", help="Access key for the observer page (for single account mode)")
    parser.add_argument("--user_id", help="Observer user ID (for single account mode)")
    parser.add_argument("--coin_type", default="BTC", help="Coin type (default: BTC)")
    parser.add_argument("--output_dir", help="Output directory for JSON and screenshots")
    parser.add_argument("--single_account", action="store_true", help="Run in single account mode")
    
    args = parser.parse_args()
    
    # Set default output directory if not provided
    if not args.output_dir:
        args.output_dir = os.path.join(os.getcwd(), "output")
    
    # Create scraper instance
    scraper = AntpoolDashboardScraper(
        output_dir=args.output_dir,
        single_account=args.single_account or (args.access_key and args.user_id),
        access_key=args.access_key,
        user_id=args.user_id,
        coin_type=args.coin_type
    )
    
    # Run the scraper
    return await scraper.run()

if __name__ == "__main__":
    asyncio.run(main())
