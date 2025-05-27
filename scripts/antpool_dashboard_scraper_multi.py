#!/usr/bin/env python3
"""
Antpool Dashboard Scraper with improved extraction logic

This script scrapes dashboard data from Antpool observer accounts
and saves it to files and Supabase.
"""

import os
import sys
import json
import asyncio
import logging
from datetime import datetime
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
    from utils.browser_utils import setup_browser, handle_consent_dialog, take_screenshot
    from utils.data_utils import save_json_to_file
except ImportError as e:
    logger.error(f"Import error: {e}")
    # Try relative import as fallback
    try:
        from utils.supabase_utils import get_supabase_client
        from utils.browser_utils import setup_browser, handle_consent_dialog, take_screenshot
        from utils.data_utils import save_json_to_file
    except ImportError as e2:
        logger.error(f"Fallback import also failed: {e2}")
        sys.exit(1)

async def extract_dashboard_metrics(page):
    """Extract all dashboard metrics using robust DOM traversal.
    
    Args:
        page: Playwright page
        
    Returns:
        dict: Extracted dashboard metrics
    """
    logger.info("Starting dashboard metrics extraction...")
    
    # Initialize results dictionary
    metrics = {}
    
    try:
        # Extract all metrics using a single comprehensive JavaScript function
        # This approach is more reliable as it handles the DOM structure as a whole
        metrics = await page.evaluate("""
            () => {
                const result = {};
                
                // Helper function to find text nodes containing specific text
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
                
                // Helper function to get the closest numeric value near a text node
                function getValueNearText(textNode, maxDistance = 5) {
                    // Check the node itself
                    const nodeText = textNode.textContent.trim();
                    const numericMatch = nodeText.match(/[\\d.]+/);
                    if (numericMatch) {
                        return numericMatch[0];
                    }
                    
                    // Check parent and siblings
                    let currentNode = textNode.parentNode;
                    for (let i = 0; i < maxDistance && currentNode; i++) {
                        // Check all child text nodes
                        const childTexts = Array.from(currentNode.childNodes)
                            .filter(node => node.nodeType === Node.TEXT_NODE)
                            .map(node => node.textContent.trim())
                            .filter(text => text.length > 0);
                            
                        for (const text of childTexts) {
                            const match = text.match(/[\\d.]+/);
                            if (match) {
                                return match[0];
                            }
                        }
                        
                        // Check elements with specific classes that might contain values
                        const valueElements = currentNode.querySelectorAll('.value, .number, [class*="value"], [class*="number"]');
                        for (const el of valueElements) {
                            const match = el.textContent.trim().match(/[\\d.]+/);
                            if (match) {
                                return match[0];
                            }
                        }
                        
                        // Move up the DOM tree
                        currentNode = currentNode.parentNode;
                    }
                    
                    return null;
                }
                
                // Extract 10-Minute Hashrate
                try {
                    const tenMinNodes = findTextNodes('10-Minute');
                    if (tenMinNodes.length > 0) {
                        // First try to find the value in a nearby element
                        let found = false;
                        for (const node of tenMinNodes) {
                            const parent = node.parentNode;
                            if (!parent) continue;
                            
                            // Look for siblings or children with numeric content
                            const siblings = Array.from(parent.parentNode.children);
                            for (const sibling of siblings) {
                                const numericText = sibling.textContent.trim().match(/[\\d.]+\\s*(?:PH\\/s|TH\\/s|H\\/s)/i);
                                if (numericText) {
                                    result.tenMinHashrate = numericText[0].trim();
                                    found = true;
                                    break;
                                }
                            }
                            
                            if (!found) {
                                // Try to find any element with both numeric content and hashrate unit
                                const hashElements = document.querySelectorAll('*');
                                for (const el of hashElements) {
                                    if (el.textContent && el.textContent.match(/[\\d.]+\\s*(?:PH\\/s|TH\\/s|H\\/s)/i)) {
                                        const rect1 = node.parentNode.getBoundingClientRect();
                                        const rect2 = el.getBoundingClientRect();
                                        // Check if they're close to each other vertically
                                        if (Math.abs(rect1.top - rect2.top) < 50) {
                                            result.tenMinHashrate = el.textContent.trim();
                                            found = true;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        
                        // If still not found, look for any element containing both "10-Minute" and a number
                        if (!found) {
                            const elements = document.querySelectorAll('*');
                            for (const el of elements) {
                                if (el.textContent && el.textContent.includes('10-Minute')) {
                                    const numericMatch = el.textContent.match(/[\\d.]+\\s*(?:PH\\/s|TH\\/s|H\\/s)/i);
                                    if (numericMatch) {
                                        result.tenMinHashrate = numericMatch[0].trim();
                                        found = true;
                                        break;
                                    }
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
                        // First try to find the value in a nearby element
                        let found = false;
                        for (const node of dayNodes) {
                            const parent = node.parentNode;
                            if (!parent) continue;
                            
                            // Look for siblings or children with numeric content
                            const siblings = Array.from(parent.parentNode.children);
                            for (const sibling of siblings) {
                                const numericText = sibling.textContent.trim().match(/[\\d.]+\\s*(?:PH\\/s|TH\\/s|H\\/s)/i);
                                if (numericText) {
                                    result.dayHashrate = numericText[0].trim();
                                    found = true;
                                    break;
                                }
                            }
                            
                            if (!found) {
                                // Try to find any element with both numeric content and hashrate unit
                                const hashElements = document.querySelectorAll('*');
                                for (const el of hashElements) {
                                    if (el.textContent && el.textContent.match(/[\\d.]+\\s*(?:PH\\/s|TH\\/s|H\\/s)/i)) {
                                        const rect1 = node.parentNode.getBoundingClientRect();
                                        const rect2 = el.getBoundingClientRect();
                                        // Check if they're close to each other vertically
                                        if (Math.abs(rect1.top - rect2.top) < 50) {
                                            result.dayHashrate = el.textContent.trim();
                                            found = true;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (e) {
                    console.error('Error extracting 24h hashrate:', e);
                }
                
                // Extract Active workers
                try {
                    const activeNodes = findTextNodes('Active');
                    if (activeNodes.length > 0) {
                        for (const node of activeNodes) {
                            // Get parent element
                            const parent = node.parentNode;
                            if (!parent) continue;
                            
                            // Check if this is the worker count section (not a table header)
                            if (parent.textContent.trim() === 'Active') {
                                // Look for siblings with numeric content
                                const siblings = Array.from(parent.parentNode.children);
                                for (const sibling of siblings) {
                                    const numericText = sibling.textContent.trim().match(/\\d+/);
                                    if (numericText) {
                                        result.activeWorkers = numericText[0].trim();
                                        break;
                                    }
                                }
                                
                                // If not found in siblings, check the parent's next sibling
                                if (!result.activeWorkers) {
                                    const nextSibling = parent.nextElementSibling;
                                    if (nextSibling) {
                                        const numericText = nextSibling.textContent.trim().match(/\\d+/);
                                        if (numericText) {
                                            result.activeWorkers = numericText[0].trim();
                                        }
                                    }
                                }
                                
                                // If still not found, look for any nearby element with just a number
                                if (!result.activeWorkers) {
                                    const rect = parent.getBoundingClientRect();
                                    const elements = document.querySelectorAll('*');
                                    for (const el of elements) {
                                        if (el.textContent && /^\\s*\\d+\\s*$/.test(el.textContent)) {
                                            const elRect = el.getBoundingClientRect();
                                            // Check if they're close to each other
                                            if (Math.abs(rect.left - elRect.left) < 200 && Math.abs(rect.top - elRect.top) < 50) {
                                                result.activeWorkers = el.textContent.trim();
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (e) {
                    console.error('Error extracting active workers:', e);
                }
                
                // Extract Inactive workers
                try {
                    const inactiveNodes = findTextNodes('Inactive');
                    if (inactiveNodes.length > 0) {
                        for (const node of inactiveNodes) {
                            // Get parent element
                            const parent = node.parentNode;
                            if (!parent) continue;
                            
                            // Check if this is the worker count section (not a table header)
                            if (parent.textContent.trim() === 'Inactive') {
                                // Look for siblings with numeric content
                                const siblings = Array.from(parent.parentNode.children);
                                for (const sibling of siblings) {
                                    const numericText = sibling.textContent.trim().match(/\\d+/);
                                    if (numericText) {
                                        result.inactiveWorkers = numericText[0].trim();
                                        break;
                                    }
                                }
                                
                                // If not found in siblings, check the parent's next sibling
                                if (!result.inactiveWorkers) {
                                    const nextSibling = parent.nextElementSibling;
                                    if (nextSibling) {
                                        const numericText = nextSibling.textContent.trim().match(/\\d+/);
                                        if (numericText) {
                                            result.inactiveWorkers = numericText[0].trim();
                                        }
                                    }
                                }
                                
                                // If still not found, look for any nearby element with just a number
                                if (!result.inactiveWorkers) {
                                    const rect = parent.getBoundingClientRect();
                                    const elements = document.querySelectorAll('*');
                                    for (const el of elements) {
                                        if (el.textContent && /^\\s*\\d+\\s*$/.test(el.textContent)) {
                                            const elRect = el.getBoundingClientRect();
                                            // Check if they're close to each other
                                            if (Math.abs(rect.left - elRect.left) < 200 && Math.abs(rect.top - elRect.top) < 50) {
                                                result.inactiveWorkers = el.textContent.trim();
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (e) {
                    console.error('Error extracting inactive workers:', e);
                }
                
                // Extract Account Balance
                try {
                    const balanceNodes = findTextNodes('Account Balance');
                    if (balanceNodes.length > 0) {
                        for (const node of balanceNodes) {
                            // Get parent element
                            const parent = node.parentNode;
                            if (!parent) continue;
                            
                            // Look for siblings with numeric content
                            const siblings = Array.from(parent.parentNode.children);
                            for (const sibling of siblings) {
                                const numericText = sibling.textContent.trim().match(/[\\d.]+/);
                                if (numericText) {
                                    result.accountBalance = numericText[0].trim();
                                    break;
                                }
                            }
                            
                            // If not found in siblings, check the parent's next sibling
                            if (!result.accountBalance) {
                                const nextSibling = parent.nextElementSibling;
                                if (nextSibling) {
                                    const numericText = nextSibling.textContent.trim().match(/[\\d.]+/);
                                    if (numericText) {
                                        result.accountBalance = numericText[0].trim();
                                    }
                                }
                            }
                            
                            // If still not found, look for any nearby element with a decimal number
                            if (!result.accountBalance) {
                                const rect = parent.getBoundingClientRect();
                                const elements = document.querySelectorAll('*');
                                for (const el of elements) {
                                    if (el.textContent && /^\\s*[\\d.]+\\s*$/.test(el.textContent)) {
                                        const elRect = el.getBoundingClientRect();
                                        // Check if they're close to each other
                                        if (Math.abs(rect.left - elRect.left) < 200 && Math.abs(rect.top - elRect.top) < 50) {
                                            result.accountBalance = el.textContent.trim();
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (e) {
                    console.error('Error extracting account balance:', e);
                }
                
                // Extract Yesterday Earnings
                try {
                    const yesterdayNodes = findTextNodes('Yesterday Earnings');
                    if (yesterdayNodes.length > 0) {
                        for (const node of yesterdayNodes) {
                            // Get parent element
                            const parent = node.parentNode;
                            if (!parent) continue;
                            
                            // Look for siblings with numeric content
                            const siblings = Array.from(parent.parentNode.children);
                            for (const sibling of siblings) {
                                const numericText = sibling.textContent.trim().match(/[\\d.]+/);
                                if (numericText) {
                                    result.yesterdayEarnings = numericText[0].trim();
                                    break;
                                }
                            }
                            
                            // If not found in siblings, check the parent's next sibling
                            if (!result.yesterdayEarnings) {
                                const nextSibling = parent.nextElementSibling;
                                if (nextSibling) {
                                    const numericText = nextSibling.textContent.trim().match(/[\\d.]+/);
                                    if (numericText) {
                                        result.yesterdayEarnings = numericText[0].trim();
                                    }
                                }
                            }
                            
                            // If still not found, look for any nearby element with a decimal number
                            if (!result.yesterdayEarnings) {
                                const rect = parent.getBoundingClientRect();
                                const elements = document.querySelectorAll('*');
                                for (const el of elements) {
                                    if (el.textContent && /^\\s*[\\d.]+\\s*$/.test(el.textContent)) {
                                        const elRect = el.getBoundingClientRect();
                                        // Check if they're close to each other
                                        if (Math.abs(rect.left - elRect.left) < 200 && Math.abs(rect.top - elRect.top) < 50) {
                                            result.yesterdayEarnings = el.textContent.trim();
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (e) {
                    console.error('Error extracting yesterday earnings:', e);
                }
                
                // Fallback method: extract all numeric values with their labels
                try {
                    // Get all elements with text content
                    const elements = document.querySelectorAll('*');
                    const valueElements = [];
                    
                    // Find elements that contain just numbers or numbers with units
                    for (const el of elements) {
                        if (el.childNodes.length === 1 && 
                            el.childNodes[0].nodeType === Node.TEXT_NODE &&
                            (
                                /^\\s*[\\d.]+\\s*$/.test(el.textContent) || 
                                /^\\s*[\\d.]+\\s*(?:PH\\/s|TH\\/s|H\\/s)\\s*$/i.test(el.textContent)
                            )
                        ) {
                            valueElements.push(el);
                        }
                    }
                    
                    // For each value element, try to find a label nearby
                    for (const el of valueElements) {
                        const rect = el.getBoundingClientRect();
                        const value = el.textContent.trim();
                        
                        // Skip if already found
                        if (
                            (value === result.tenMinHashrate) ||
                            (value === result.dayHashrate) ||
                            (value === result.activeWorkers) ||
                            (value === result.inactiveWorkers) ||
                            (value === result.accountBalance) ||
                            (value === result.yesterdayEarnings)
                        ) {
                            continue;
                        }
                        
                        // Look for labels nearby
                        for (const labelEl of elements) {
                            if (labelEl === el) continue;
                            
                            const labelRect = labelEl.getBoundingClientRect();
                            const labelText = labelEl.textContent.trim();
                            
                            // Check if they're close to each other
                            if (Math.abs(rect.left - labelRect.left) < 200 && Math.abs(rect.top - labelRect.top) < 50) {
                                if (labelText.includes('10-Minute') && !result.tenMinHashrate) {
                                    result.tenMinHashrate = value;
                                } else if ((labelText.includes('24H') || labelText.includes('24-Hour')) && !result.dayHashrate) {
                                    result.dayHashrate = value;
                                } else if (labelText === 'Active' && !result.activeWorkers) {
                                    result.activeWorkers = value;
                                } else if (labelText === 'Inactive' && !result.inactiveWorkers) {
                                    result.inactiveWorkers = value;
                                } else if (labelText.includes('Account Balance') && !result.accountBalance) {
                                    result.accountBalance = value;
                                } else if (labelText.includes('Yesterday Earnings') && !result.yesterdayEarnings) {
                                    result.yesterdayEarnings = value;
                                }
                            }
                        }
                    }
                } catch (e) {
                    console.error('Error in fallback extraction:', e);
                }
                
                return result;
            }
        """)
        
        # Log the extracted metrics
        for key, value in metrics.items():
            logger.info(f"✅ Extracted {key}: {value}")
        
        # Count successful extractions
        success_count = len(metrics.keys())
        logger.info(f"Successfully extracted {success_count} dashboard metrics")
        
        return metrics
    except Exception as e:
        logger.error(f"❌ Error extracting dashboard metrics: {e}")
        return {}

async def scrape_dashboard(access_key, observer_user_id, coin_type="BTC", output_dir="./output"):
    """Scrape dashboard data for a given account."""
    logger.info(f"\n===== Processing account: {observer_user_id} =====")
    logger.info(f"Scraping dashboard for {observer_user_id} ({coin_type})...")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize browser
    logger.info("Initializing browser...")
    browser = await setup_browser(headless=True)
    logger.info("✅ Browser initialized successfully")
    
    # Create a new page
    page = await browser.new_page()
    page.set_default_timeout(15000)  # 15 second timeout
    
    try:
        # Construct the URL
        url = (
            f"https://www.antpool.com/observer?accessKey={access_key}"
            f"&coinType={coin_type}&observerUserId={observer_user_id}"
        )
        
        # Navigate to the page
        await page.goto(url, wait_until="networkidle")
        logger.info(f"Navigated to observer page for {observer_user_id}")
        
        # Take screenshot before handling consent
        initial_screenshot = os.path.join(output_dir, f"initial_{observer_user_id}.png")
        await take_screenshot(page, initial_screenshot)
        
        # Handle consent dialog
        await handle_consent_dialog(page)
        
        # Take screenshot after handling consent
        after_consent_screenshot = os.path.join(output_dir, f"after_consent_{observer_user_id}.png")
        await take_screenshot(page, after_consent_screenshot)
        
        # Wait for dashboard elements to load
        logger.info("Waiting for dashboard elements to load...")
        
        # Try multiple selectors for dashboard elements
        selectors = [
            ".hashrate-item", 
            ".worker-item", 
            "text=Hashrate",
            ".dashboard-container",
            ".dashboard"
        ]
        
        dashboard_found = False
        for selector in selectors:
            try:
                logger.info(f"Trying selector: {selector}")
                await page.wait_for_selector(selector, timeout=5000)
                logger.info(f"Dashboard element found: {selector}")
                dashboard_found = True
                break
            except Exception as e:
                logger.info(f"Selector {selector} not found, trying next...")
        
        if not dashboard_found:
            logger.warning("Could not find any dashboard elements")
            final_screenshot = os.path.join(output_dir, f"final_{observer_user_id}.png")
            await take_screenshot(page, final_screenshot)
            return {"error": "Dashboard elements not found"}
        
        logger.info("Dashboard elements detected, proceeding with data extraction")
        
        # Extract dashboard metrics
        dashboard_metrics = await extract_dashboard_metrics(page)
        
        # Combine all data
        dashboard_data = {
            "timestamp": datetime.now().isoformat(),
            "observer_user_id": observer_user_id,
            "coin_type": coin_type,
            **dashboard_metrics
        }
        
        logger.info(f"Dashboard data: {json.dumps(dashboard_data, indent=2)}")
        
        # Take dashboard screenshot
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        screenshot_path = os.path.join(output_dir, f"{timestamp}_Antpool_{coin_type}.png")
        await take_screenshot(page, screenshot_path)
        logger.info(f"✅ Saved dashboard screenshot to {screenshot_path}")
        
        # Save data to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        data_file = os.path.join(output_dir, f"pool_stats_{observer_user_id}_{timestamp}.json")
        with open(data_file, "w") as f:
            json.dump(dashboard_data, f, indent=2)
        logger.info(f"✅ Saved dashboard data to {data_file}")
        
        # Upload to Supabase
        try:
            supabase = get_supabase_client()
            if supabase:
                # Prepare data for Supabase
                supabase_data = {
                    "timestamp": dashboard_data["timestamp"],
                    "observer_user_id": observer_user_id,
                    "coin_type": coin_type,
                    "ten_min_hashrate": dashboard_data.get("tenMinHashrate", "0"),
                    "day_hashrate": dashboard_data.get("dayHashrate", "0"),
                    "active_workers": dashboard_data.get("activeWorkers", "0"),
                    "inactive_workers": dashboard_data.get("inactiveWorkers", "0"),
                    "account_balance": dashboard_data.get("accountBalance", "0"),
                    "yesterday_earnings": dashboard_data.get("yesterdayEarnings", "0")
                }
                
                # Insert data into Supabase
                response = supabase.table("mining_pool_stats").insert(supabase_data).execute()
                
                if hasattr(response, 'data') and response.data:
                    logger.info(f"✅ Successfully uploaded data to Supabase for {observer_user_id}")
                else:
                    logger.error(f"❌ Error uploading to Supabase: {response}")
        except Exception as e:
            logger.error(f"❌ Error uploading to Supabase: {e}")
        
        return dashboard_data
    
    except Exception as e:
        logger.error(f"❌ Error scraping dashboard: {e}")
        # Take error screenshot
        error_screenshot = os.path.join(output_dir, f"error_{observer_user_id}.png")
        await take_screenshot(page, error_screenshot)
        return {"error": str(e)}
    
    finally:
        # Close browser
        await browser.close()
        logger.info("Browser closed")

async def main():
    """Main function to run the dashboard scraper."""
    logger.info("\n===== Starting Antpool Dashboard Scraper =====")
    logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Set output directory
    output_dir = os.path.join(os.getcwd(), "output")
    logger.info(f"Output directory: {output_dir}")
    os.makedirs(output_dir, exist_ok=True)
    
    # Get Supabase client
    supabase = get_supabase_client()
    
    if not supabase:
        logger.error("❌ Failed to initialize Supabase client")
        return
    
    # Get accounts from Supabase - FIXED: Using account_credentials table instead of accounts
    try:
        response = supabase.table("account_credentials").select("*").eq("is_active", True).execute()
        accounts = response.data
        logger.info(f"✅ Found {len(accounts)} active accounts in Supabase")
    except Exception as e:
        logger.error(f"❌ Error fetching accounts from Supabase: {e}")
        return
    
    results = {}
    success_count = 0
    failure_count = 0
    
    for account in accounts:
        try:
            # FIXED: Using correct field names from account_credentials table
            result = await scrape_dashboard(
                account["access_key"],
                account["user_id"],  # Changed from observer_user_id to user_id
                account["coin_type"],
                output_dir
            )
            
            if "error" in result:
                logger.error(f"❌ Failed to process account: {account['user_id']}")
                failure_count += 1
            else:
                logger.info(f"✅ Successfully processed account: {account['user_id']}")
                success_count += 1
                
            results[account["user_id"]] = result
        except Exception as e:
            logger.error(f"❌ Error processing account {account['user_id']}: {e}")
            failure_count += 1
    
    # Save combined results
    combined_file = os.path.join(output_dir, "dashboard_results.json")
    with open(combined_file, "w") as f:
        json.dump(results, f, indent=2)
    logger.info(f"Saved combined results to {combined_file}")
    
    # Print summary
    logger.info("\n===== Dashboard Scraper Summary =====")
    logger.info(f"Total accounts processed: {len(accounts)}")
    logger.info(f"Successful: {success_count}")
    logger.info(f"Failed: {failure_count}")
    
    if len(accounts) > 0:
        success_rate = (success_count / len(accounts)) * 100
        logger.info(f"Success rate: {success_rate:.1f}%")
    
    logger.info(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return results

if __name__ == "__main__":
    asyncio.run(main())
