#!/usr/bin/env python3
"""
Antpool Earnings Scraper (Multi-Account Version)

This script scrapes earnings history from Antpool's observer page for multiple accounts
stored in Supabase and saves the data to JSON files and the database.

Usage:
    python3 antpool_earnings_scraper_multi.py --output_dir=<output_dir> [--single_account --access_key=<access_key> --user_id=<observer_user_id> --coin_type=<coin_type>]

Example:
    python3 antpool_earnings_scraper_multi.py --output_dir=/home/user/output
    python3 antpool_earnings_scraper_multi.py --output_dir=/home/user/output --single_account --access_key=eInFJrwSbrtDheJHTygV --user_id=Mack81 --coin_type=BTC
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
from utils.supabase_utils import save_earnings_history

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

class AntpoolEarningsScraper(AntpoolMultiAccountScraper):
    """Scraper for Antpool earnings history."""
    
    async def extract_earnings_history(self, page, output_dir, observer_user_id, coin_type):
        """Extract earnings history from the earnings tab."""
        print("Extracting earnings history...")
        
        # Ensure no modals are present
        print("Ensuring no modals are present...")
        await page.evaluate("""() => {
            document.querySelectorAll('.ant-modal-close').forEach(el => el.click());
            document.querySelectorAll('.ant-modal-mask').forEach(el => el.remove());
            document.querySelectorAll('.ant-modal-wrap').forEach(el => el.remove());
        }""")
        print("Removed any modal elements")
        
        # Click Earnings tab using JavaScript
        print("Navigating to Earnings tab...")
        await page.evaluate("""() => {
            const tabs = document.querySelectorAll('.ant-tabs-tab');
            for (const tab of tabs) {
                if (tab.textContent.includes('Earnings')) {
                    tab.click();
                    return true;
                }
            }
            return false;
        }""")
        print("Clicked Earnings tab using JavaScript")
        
        # Take screenshot after clicking Earnings tab
        earnings_tab_screenshot = os.path.join(output_dir, f"{observer_user_id}_earnings_tab_clicked.png")
        await page.screenshot(path=earnings_tab_screenshot)
        print(f"Screenshot saved after clicking Earnings tab: {earnings_tab_screenshot}")
        
        # Wait for earnings table to load
        await page.wait_for_selector("table", timeout=30000)
        print("Earnings table loaded")
        
        # Ensure no modals are present
        print("Ensuring no modals are present...")
        await page.evaluate("""() => {
            document.querySelectorAll('.ant-modal-close').forEach(el => el.click());
            document.querySelectorAll('.ant-modal-mask').forEach(el => el.remove());
            document.querySelectorAll('.ant-modal-wrap').forEach(el => el.remove());
        }""")
        print("Removed any modal elements")
        
        # Set page size to 50
        print("Setting page size to 50...")
        await page.locator('.ant-select-selection-item').click()
        await page.locator('div[title="50 / page"]').click()
        print("Selected page size 50")
        
        # Wait for table to update
        await asyncio.sleep(2)
        
        # Capture earnings table screenshot
        print("Capturing earnings table screenshot...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        screenshot_path = os.path.join(output_dir, f"{timestamp}_{observer_user_id}_Antpool_{coin_type}_earnings.png")
        await page.screenshot(path=screenshot_path, full_page=True)
        print(f"Earnings table screenshot saved to: {screenshot_path}")
        
        # Get total pages
        total_text = await page.locator('.ant-pagination-total-text').text_content()
        total_items_match = re.search(r'Total (\d+) items', total_text)
        total_items = int(total_items_match.group(1)) if total_items_match else 0
        total_pages = (total_items + 49) // 50  # Ceiling division
        
        print(f"Total earnings entries: {total_items}, total pages: {total_pages}")
        
        all_earnings = []
        
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
            rows = await page.locator('table tbody tr').all()
            print(f"Found {len(rows)} rows in table")
            
            # Save table screenshot for debugging
            table_screenshot_path = os.path.join(output_dir, f"{observer_user_id}_earnings_table_page{page_num}.png")
            await page.locator('table').screenshot(path=table_screenshot_path)
            print(f"Table screenshot saved to: {table_screenshot_path}")
            
            # Save table HTML for debugging
            table_html = await page.locator('table').evaluate("el => el.outerHTML")
            table_html_path = os.path.join(output_dir, f"{observer_user_id}_earnings_table_html_page{page_num}.html")
            with open(table_html_path, 'w', encoding='utf-8') as f:
                f.write(table_html)
            print(f"Table HTML saved to: {table_html_path}")
            
            # Extract earnings data from rows
            earnings_data = []
            
            for row in rows:
                try:
                    # Skip header rows or empty rows
                    if await row.locator('td').count() < 5:
                        continue
                    
                    # Get all cells in the row
                    cells = await row.locator('td').all()
                    
                    # Extract data from cells
                    date = await cells[0].text_content() if len(cells) > 0 else ""
                    daily_hashrate = await cells[1].text_content() if len(cells) > 1 else ""
                    
                    # Extract earnings amount and currency
                    earnings_text = await cells[2].text_content() if len(cells) > 2 else ""
                    earnings_match = re.search(r'([\d.]+)\s*(\w+)', earnings_text)
                    earnings_amount = earnings_match.group(1) if earnings_match else "0"
                    earnings_currency = earnings_match.group(2) if earnings_match else ""
                    
                    # Extract earnings type
                    earnings_type = await cells[3].text_content() if len(cells) > 3 else ""
                    
                    # Extract payment status
                    payment_status = await cells[4].text_content() if len(cells) > 4 else ""
                    
                    # Create earnings data dictionary
                    earning_data = {
                        "date": date.strip(),
                        "daily_hashrate": daily_hashrate.strip(),
                        "earnings_amount": earnings_amount.strip(),
                        "earnings_currency": earnings_currency.strip(),
                        "earnings_type": earnings_type.strip(),
                        "payment_status": payment_status.strip(),
                        "timestamp": datetime.now().isoformat(),
                        "observer_user_id": observer_user_id,
                        "coin_type": coin_type
                    }
                    
                    earnings_data.append(earning_data)
                except Exception as e:
                    print(f"Error extracting data from row: {e}")
            
            # Save earnings rows debug info
            debug_path = os.path.join(output_dir, f"{observer_user_id}_earnings_rows_debug_page{page_num}.json")
            with open(debug_path, 'w', encoding='utf-8') as f:
                json.dump(earnings_data, f, indent=2)
            print(f"Earnings rows debug info saved to: {debug_path}")
            
            print(f"Found {len(earnings_data)} earnings entries on page {page_num}")
            if earnings_data:
                print(f"First earnings data: {earnings_data[0]}")
            
            all_earnings.extend(earnings_data)
            
            # Navigate to next page if not on the last page
            if page_num < total_pages:
                print(f"Navigating to page {page_num + 1}...")
                await page.locator('button.ant-pagination-item-link[aria-label="Next page"]').click()
                await asyncio.sleep(2)  # Wait for page to load
        
        print(f"Total earnings entries extracted: {len(all_earnings)}")
        return all_earnings, screenshot_path
    
    async def scrape_account(self, account: Dict[str, Any]) -> None:
        """Scrape earnings history for a single account."""
        access_key = account['access_key']
        user_id = account['user_id']
        coin_type = account.get('coin_type', 'BTC')
        account_name = account.get('account_name', user_id)
        
        print(f"Scraping earnings history for account {account_name} ({coin_type})...")
        
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
                
                # Extract earnings history
                earnings_history, screenshot_path = await self.extract_earnings_history(
                    page, self.output_dir, account_name, coin_type
                )
                
                # Save earnings history to JSON file
                print("Saving earnings history...")
                timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                output_file = os.path.join(self.output_dir, f"earnings_history_{account_name}_{timestamp}.json")
                
                save_json_data(earnings_history, output_file)
                print(f"Earnings history saved to: {output_file}")
                
                # Save to Supabase if client is initialized
                if self.supabase:
                    try:
                        result = save_earnings_history(earnings_history)
                        print(f"Supabase save result: {result}")
                    except Exception as e:
                        print(f"Error saving to Supabase: {e}")
                
                print(f"Scraping completed successfully for account {account_name}!")
                print(f"Total earnings entries extracted: {len(earnings_history)}")
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
    parser = argparse.ArgumentParser(description="Antpool Earnings History Scraper")
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
    scraper = AntpoolEarningsScraper(
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
