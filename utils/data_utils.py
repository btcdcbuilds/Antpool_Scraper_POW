import os
import json
import datetime
from typing import List, Dict, Any, Optional

def save_json_to_file(data: Any, output_file: str) -> None:
    """Save data to JSON file.
    
    Args:
        data: Data to save
        output_file: Path to output file
    """
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
    
    # Save data to JSON file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    
    print(f"Data saved to: {output_file}")

# Alias for backward compatibility
save_json_data = save_json_to_file

def format_timestamp() -> str:
    """Format current timestamp to ISO format.
    
    Returns:
        str: Formatted timestamp string
    """
    return datetime.datetime.now().isoformat()

def parse_hashrate(hashrate_str: str) -> float:
    """Parse hashrate string to float value in TH/s.
    
    Args:
        hashrate_str: Hashrate string (e.g., "123.45 TH/s", "1.23 PH/s")
        
    Returns:
        float: Hashrate in TH/s
    """
    try:
        # Remove any non-numeric characters except decimal point and unit
        hashrate_str = hashrate_str.strip()
        
        # Split by space to separate value and unit
        parts = hashrate_str.split()
        if len(parts) < 2:
            return 0.0
        
        value = float(parts[0])
        unit = parts[1].upper()
        
        # Convert to TH/s based on unit
        if 'PH' in unit:
            value *= 1000  # 1 PH/s = 1000 TH/s
        elif 'EH' in unit:
            value *= 1000000  # 1 EH/s = 1000000 TH/s
        elif 'GH' in unit:
            value /= 1000  # 1 TH/s = 1000 GH/s
        elif 'MH' in unit:
            value /= 1000000  # 1 TH/s = 1000000 MH/s
        
        return value
    
    except Exception as e:
        print(f"Error parsing hashrate '{hashrate_str}': {e}")
        return 0.0

def parse_percentage(percentage_str: str) -> float:
    """Parse percentage string to float value.
    
    Args:
        percentage_str: Percentage string (e.g., "12.34%")
        
    Returns:
        float: Percentage as float
    """
    try:
        # Remove any non-numeric characters except decimal point
        percentage_str = percentage_str.strip()
        percentage_str = percentage_str.replace('%', '')
        
        return float(percentage_str)
    
    except Exception as e:
        print(f"Error parsing percentage '{percentage_str}': {e}")
        return 0.0

def format_datetime(dt: datetime.datetime) -> str:
    """Format datetime object to string.
    
    Args:
        dt: Datetime object
        
    Returns:
        str: Formatted datetime string
    """
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def parse_datetime(dt_str: str) -> datetime.datetime:
    """Parse datetime string to datetime object.
    
    Args:
        dt_str: Datetime string
        
    Returns:
        datetime.datetime: Datetime object
    """
    try:
        # Try different formats
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%d-%m-%Y %H:%M:%S",
            "%d-%m-%Y %H:%M",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
        ]
        
        for fmt in formats:
            try:
                return datetime.datetime.strptime(dt_str, fmt)
            except ValueError:
                continue
        
        # If all formats fail, raise exception
        raise ValueError(f"Could not parse datetime string: {dt_str}")
    
    except Exception as e:
        print(f"Error parsing datetime '{dt_str}': {e}")
        return datetime.datetime.now()
