import json
import os
from datetime import datetime

def save_json_data(data, output_file):
    """Save data to a JSON file."""
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving JSON data: {e}")
        return False

def format_timestamp(timestamp=None):
    """Format timestamp for filenames."""
    if timestamp is None:
        timestamp = datetime.now()
    return timestamp.strftime("%Y%m%d_%H%M")

def clean_worker_name(worker_name):
    """Clean up worker name by removing extra text."""
    if not worker_name:
        return worker_name
    
    # Remove "Click to view miner connection information" text
    if "Click to view" in worker_name:
        worker_name = worker_name.split("Click to view")[0].strip()
    
    return worker_name

def parse_hashrate(hashrate_str):
    """Parse hashrate string to standardized format."""
    if not hashrate_str:
        return "0 H/s"
    
    # Remove any whitespace
    hashrate_str = hashrate_str.strip()
    
    # Return as is if already in expected format
    return hashrate_str

def parse_earnings_amount(earnings_text):
    """Parse earnings amount and currency from text."""
    if not earnings_text:
        return "0", ""
    
    # Try to extract amount and currency
    import re
    match = re.search(r'([\d.]+)\s*(\w+)', earnings_text)
    if match:
        amount = match.group(1)
        currency = match.group(2)
        return amount, currency
    
    return "0", ""
