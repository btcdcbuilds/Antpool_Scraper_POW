#!/usr/bin/env python3
"""
Antpool Combined Daily Report Scraper

This script combines dashboard and earnings scraping into a single daily report,
providing comprehensive pool statistics and earnings history.
"""

import os
import sys
import json
import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from utils.supabase_utils import get_supabase_client
    from utils.browser_utils import setup_browser, handle_cookie_consent, take_screenshot
    from utils.data_utils import save_json_to_file, format_timestamp
except ImportError as e:
    logger.error(f"Import error: {e}")
    sys.exit(1)

async def scrape_dashboard_data(page, access_key, user_id, coin_type):
    """Scrape dashboard statistics from Antpool."""
    logger.info(f"Scraping dashboard for {user_id} ({coin_type})")
    
    # Navigate to observer page
    observer_url = f"https://www.antpool.com/observer?accessKey={access_key}&coinType={coin_type}&observerUserId={user_id}"
    await page.goto(observer_url, wait_until="networkidle")
    
    # Handle cookie consent
    await handle_cookie_consent(page)
    
    # Wait for dashboard to load
    await page.wait_for_selector(".ant-card-body", timeout=30000)
    
    # Extract dashboard metrics using comprehensive JavaScript
    dashboard_data = await page.evaluate("""
        () => {
            const result = {};
            
            // Helper function to find text nodes
            function findTextNodes(searchText) {
                const walker = document.createTreeWalker(
                    document.body,
                    NodeFilter.SHOW_TEXT,
                    { acceptNode: node => node.textContent.includes(searchText) ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT }
                );
                const nodes = [];
                let node;
                while (node = walker.nextNode()) {
                    nodes.push(node);
                }
                return nodes;
            }
            
            // Extract 10-minute hashrate
            try {
                const tenMinNodes = findTextNodes('10-Minute');
                if (tenMinNodes.length > 0) {
                    const elements = document.querySelectorAll('*');
                    for (const el of elements) {
                        if (el.textContent && el.textContent.match(/[\\d.]+\\s*(?:PH\\/s|TH\\/s|H\\/s)/i)) {
                            const rect1 = tenMinNodes[0].parentNode.getBoundingClientRect();
                            const rect2 = el.getBoundingClientRect();
                            if (Math.abs(rect1.top - rect2.top) < 50) {
                                result.tenMinHashrate = el.textContent.trim();
                                break;
                            }
                        }
                    }
                }
            } catch (e) {
                console.error('Error extracting 10-min hashrate:', e);
            }
            
            // Extract 24H hashrate
            try {
                const dayNodes = findTextNodes('24H');
                if (dayNodes.length > 0) {
                    const elements = document.querySelectorAll('*');
                    for (const el of elements) {
                        if (el.textContent && el.textContent.match(/[\\d.]+\\s*(?:PH\\/s|TH\\/s|H\\/s)/i)) {
                            const rect1 = dayNodes[0].parentNode.getBoundingClientRect();
                            const rect2 = el.getBoundingClientRect();
                            if (Math.abs(rect1.top - rect2.top) < 50) {
                                result.dayHashrate = el.textContent.trim();
                                break;
                            }
                        }
                    }
                }
            } catch (e) {
                console.error('Error extracting 24h hashrate:', e);
            }
            
            // Extract worker counts
            try {
                const activeNodes = findTextNodes('Active');
                for (const node of activeNodes) {
                    const parent = node.parentNode;
                    if (parent && parent.textContent.trim() === 'Active') {
                        const siblings = Array.from(parent.parentNode.children);
                        for (const sibling of siblings) {
                            const numericText = sibling.textContent.trim().match(/\\d+/);
                            if (numericText) {
                                result.activeWorkers = numericText[0].trim();
                                break;
                            }
                        }
                    }
                }
            } catch (e) {
                console.error('Error extracting active workers:', e);
            }
            
            // Extract inactive workers
            try {
                const inactiveNodes = findTextNodes('Inactive');
                for (const node of inactiveNodes) {
                    const parent = node.parentNode;
                    if (parent && parent.textContent.trim() === 'Inactive') {
                        const siblings = Array.from(parent.parentNode.children);
                        for (const sibling of siblings) {
                            const numericText = sibling.textContent.trim().match(/\\d+/);
                            if (numericText) {
                                result.inactiveWorkers = numericText[0].trim();
                                break;
                            }
                        }
                    }
                }
            } catch (e) {
                console.error('Error extracting inactive workers:', e);
            }
            
            // Extract account balance
            try {
                const balanceNodes = findTextNodes('Account Balance');
                if (balanceNodes.length > 0) {
                    const parent = balanceNodes[0].parentNode;
                    const siblings = Array.from(parent.parentNode.children);
                    for (const sibling of siblings) {
                        const numericText = sibling.textContent.trim().match(/[\\d.]+/);
                        if (numericText) {
                            result.accountBalance = numericText[0].trim();
                            break;
                        }
                    }
                }
            } catch (e) {
                console.error('Error extracting account balance:', e);
            }
            
            // Extract yesterday earnings
            try {
                const yesterdayNodes = findTextNodes('Yesterday Earnings');
                if (yesterdayNodes.length > 0) {
                    const parent = yesterdayNodes[0].parentNode;
                    const siblings = Array.from(parent.parentNode.children);
                    for (const sibling of siblings) {
                        const numericText = sibling.textContent.trim().match(/[\\d.]+/);
                        if (numericText) {
                            result.yesterdayEarnings = numericText[0].trim();
                            break;
                        }
                    }
                }
            } catch (e) {
                console.error('Error extracting yesterday earnings:', e);
            }
            
            // Extract total earnings
            try {
                const totalNodes = findTextNodes('Total Earnings');
                if (totalNodes.length > 0) {
                    const parent = totalNodes[0].parentNode;
                    const siblings = Array.from(parent.parentNode.children);
                    for (const sibling of siblings) {
                        const numericText = sibling.textContent.trim().match(/[\\d.]+/);
                        if (numericText) {
                            result.totalEarnings = numericText[0].trim();
                            break;
                        }
                    }
                }
            } catch (e) {
                console.error('Error extracting total earnings:', e);
            }
            
            return result;
        }
    """)
    
    return dashboard_data

async def scrape_earnings_data(page, access_key, user_id, coin_type, days_back=7):
    """Scrape recent earnings history from Antpool."""
    logger.info(f"Scraping earnings for {user_id} ({coin_type}) - last {days_back} days")
    
    # Navigate to earnings tab
    await page.click('text="Earnings"')
    await page.wait_for_selector(".ant-table-wrapper", timeout=30000)
    
    # Get table rows for recent earnings
    rows = await page.query_selector_all(".ant-table-tbody tr")
    logger.info(f"Found {len(rows)} earnings rows")
    
    earnings_data = []
    for row_idx, row in enumerate(rows[:days_back]):  # Get last N days
        try:
            cells = await row.query_selector_all("td")
            if len(cells) < 5:
                continue
            
            date = await cells[0].inner_text()
            daily_hashrate = await cells[1].inner_text()
            earnings_text = await cells[2].inner_text()
            earnings_type = await cells[3].inner_text()
            payment_status = await cells[4].inner_text()
            
            # Parse earnings amount and currency
            earnings_parts = earnings_text.strip().split(" ")
            earnings_amount = earnings_parts[0] if len(earnings_parts) >= 1 else earnings_text
            earnings_currency = earnings_parts[1] if len(earnings_parts) >= 2 else ""
            
            earnings_data.append({
                "date": date,
                "daily_hashrate": daily_hashrate,
                "earnings_amount": earnings_amount,
                "earnings_currency": earnings_currency,
                "earnings_type": earnings_type,
                "payment_status": payment_status,
                "timestamp": format_timestamp(),
                "observer_user_id": user_id,
                "coin_type": coin_type
            })
            
        except Exception as e:
            logger.error(f"Error extracting earnings row {row_idx}: {e}")
    
    return earnings_data

async def process_account(account, output_dir):
    """Process a single account for daily report."""
    browser = None
    page = None
    
    try:
        account_name = account.get("account_name", "Unknown")
        access_key = account.get("access_key", "")
        user_id = account.get("user_id", "")
        coin_type = account.get("coin_type", "BTC")
        
        logger.info(f"🏗️ Processing daily report for: {account_name} ({user_id})")
        
        if not access_key or not user_id:
            logger.error(f"❌ Missing credentials for {account_name}")
            return None
        
        # Setup browser
        browser = await setup_browser(headless=True)
        page = await browser.new_page()
        
        # Scrape dashboard data
        dashboard_data = await scrape_dashboard_data(page, access_key, user_id, coin_type)
        
        # Scrape earnings data (last 7 days)
        earnings_data = await scrape_earnings_data(page, access_key, user_id, coin_type, days_back=7)
        
        # Combine into daily report
        daily_report = {
            "account_name": account_name,
            "user_id": user_id,
            "coin_type": coin_type,
            "report_date": datetime.now().strftime("%Y-%m-%d"),
            "timestamp": format_timestamp(),
            "dashboard": {
                "ten_min_hashrate": dashboard_data.get("tenMinHashrate", "0"),
                "day_hashrate": dashboard_data.get("dayHashrate", "0"),
                "active_workers": dashboard_data.get("activeWorkers", "0"),
                "inactive_workers": dashboard_data.get("inactiveWorkers", "0"),
                "account_balance": dashboard_data.get("accountBalance", "0"),
                "yesterday_earnings": dashboard_data.get("yesterdayEarnings", "0"),
                "total_earnings": dashboard_data.get("totalEarnings", "0")
            },
            "recent_earnings": earnings_data
        }
        
        # Save to file
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M")
        output_file = os.path.join(output_dir, f"daily_report_{user_id}_{timestamp_str}.json")
        save_json_to_file(daily_report, output_file)
        
        # Save to Supabase
        supabase = get_supabase_client()
        if supabase:
            try:
                # Save dashboard data
                dashboard_record = {
                    "timestamp": daily_report["timestamp"],
                    "observer_user_id": user_id,
                    "coin_type": coin_type,
                    "ten_min_hashrate": daily_report["dashboard"]["ten_min_hashrate"],
                    "day_hashrate": daily_report["dashboard"]["day_hashrate"],
                    "active_workers": daily_report["dashboard"]["active_workers"],
                    "inactive_workers": daily_report["dashboard"]["inactive_workers"],
                    "account_balance": daily_report["dashboard"]["account_balance"],
                    "yesterday_earnings": daily_report["dashboard"]["yesterday_earnings"],
                    "total_earnings": daily_report["dashboard"]["total_earnings"]
                }
                
                result = supabase.table("mining_pool_stats").insert(dashboard_record).execute()
                logger.info(f"📊 Saved dashboard data for {user_id}")
                
                # Save earnings data
                if earnings_data:
                    result = supabase.table("mining_earnings").insert(earnings_data).execute()
                    logger.info(f"💰 Saved {len(earnings_data)} earnings records for {user_id}")
                
                # Update last_scraped_at
                supabase.table("account_credentials").update({
                    "last_scraped_at": format_timestamp()
                }).eq("user_id", user_id).execute()
                
            except Exception as e:
                logger.error(f"❌ Error saving to Supabase for {user_id}: {e}")
        
        logger.info(f"✅ Completed daily report for: {account_name}")
        return daily_report
        
    except Exception as e:
        logger.error(f"❌ Error processing {account.get('account_name', 'Unknown')}: {e}")
        return None
    
    finally:
        if page:
            await page.close()
        if browser:
            await browser.close()

async def main():
    """Main function for combined daily report scraper."""
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Antpool Combined Daily Report Scraper')
    parser.add_argument('--output-dir', '--output_dir', type=str, help='Output directory for files')
    # Additional arguments for compatibility with start.sh
    parser.add_argument('--access-key', '--access_key', type=str, help='Access key for Antpool API')
    parser.add_argument('--user-id', '--user_id', type=str, help='User ID for Antpool API')
    parser.add_argument('--coin-type', '--coin_type', type=str, help='Coin type for Antpool API')
    args = parser.parse_args()
    
    logger.info("🌅 Starting Antpool Combined Daily Report Scraper")
    
    # Create output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        output_dir = os.path.join(os.getcwd(), "output")
    os.makedirs(output_dir, exist_ok=True)
    
    # Get Supabase client
    supabase = get_supabase_client()
    if not supabase:
        logger.error("❌ Failed to initialize Supabase client")
        return
    
    # Fetch all active accounts
    try:
        response = supabase.table("account_credentials").select("*").eq("is_active", True).execute()
        accounts = response.data
        logger.info(f"📋 Found {len(accounts)} active accounts for daily report")
    except Exception as e:
        logger.error(f"❌ Error fetching accounts: {e}")
        return
    
    # Process each account
    start_time = datetime.now()
    successful_reports = 0
    failed_reports = 0
    
    for account in accounts:
        try:
            result = await process_account(account, output_dir)
            if result:
                successful_reports += 1
            else:
                failed_reports += 1
        except Exception as e:
            logger.error(f"❌ Error processing account: {e}")
            failed_reports += 1
        
        # Small delay between accounts
        await asyncio.sleep(1)
    
    # Summarize results
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info("=" * 60)
    logger.info("📊 DAILY REPORT SUMMARY")
    logger.info("=" * 60)
    logger.info(f"📋 Total accounts processed: {len(accounts)}")
    logger.info(f"✅ Successful: {successful_reports}")
    logger.info(f"❌ Failed: {failed_reports}")
    logger.info(f"📈 Success rate: {(successful_reports / len(accounts)) * 100:.1f}%")
    logger.info(f"⏰ Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"🏁 Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"⏱️  Total duration: {duration.total_seconds():.1f} seconds")
    logger.info(f"⚡ Average time per account: {duration.total_seconds() / len(accounts):.1f} seconds")
    logger.info("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
