import os
import sys
import json
from typing import List, Dict, Any, Optional

from supabase import create_client, Client

def get_supabase_client() -> Optional[Client]:
    """Get a Supabase client instance.
    
    Returns:
        Optional[Client]: Supabase client instance or None if credentials are missing
    """
    try:
        # Get Supabase credentials from environment
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_KEY")
        
        if not supabase_url or not supabase_key:
            print("Supabase credentials not found in environment variables")
            return None
        
        # Initialize Supabase client
        print(f"Supabase client initialized with URL: {supabase_url}")
        return create_client(supabase_url, supabase_key)
    
    except Exception as e:
        print(f"Error initializing Supabase client: {e}")
        return None

def get_active_accounts() -> List[Dict[str, Any]]:
    """Get active accounts from Supabase.
    
    Returns:
        List[Dict[str, Any]]: List of active accounts
    """
    try:
        # Get Supabase client
        supabase = get_supabase_client()
        
        if not supabase:
            print("Failed to initialize Supabase client")
            return []
        
        # Query active accounts from account_credentials table
        result = supabase.table("account_credentials").select("*").eq("is_active", True).execute()
        
        if hasattr(result, 'data') and result.data:
            print(f"Retrieved {len(result.data)} active accounts from Supabase")
            return result.data
        else:
            print("No active accounts found in Supabase")
            return []
    
    except Exception as e:
        print(f"Error retrieving active accounts from Supabase: {e}")
        return []

def filter_schema_fields(data: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    """Filter data to include only fields that exist in the specified table schema.
    
    Args:
        data: Dictionary containing data to filter
        table_name: Name of the table to filter fields for
        
    Returns:
        Dict[str, Any]: Filtered data containing only fields in the table schema
    """
    # Define schema fields for each table
    schema_fields = {
        "mining_pool_stats": [
            "id", "observer_user_id", "coin_type", "ten_min_hashrate", "day_hashrate",
            "active_workers", "inactive_workers", "account_balance", "yesterday_earnings",
            "total_earnings", "timestamp", "created_at"
        ],
        "mining_workers": [
            "id", "observer_user_id", "coin_type", "worker", "ten_min_hashrate",
            "one_h_hashrate", "h24_hashrate", "rejection_rate", "last_share_time",
            "connections_24h", "hashrate_chart", "status", "timestamp", "created_at"
        ],
        "mining_inactive_workers": [
            "id", "observer_user_id", "coin_type", "worker", "last_share_time",
            "inactive_time", "h24_hashrate", "rejection_rate", "status",
            "timestamp", "created_at"
        ],
        "mining_earnings": [
            "id", "observer_user_id", "coin_type", "date", "daily_hashrate",
            "earnings_amount", "earnings_currency", "earnings_type", "payment_status",
            "timestamp", "created_at"
        ],
        "account_credentials": [
            "id", "account_name", "access_key", "user_id", "coin_type",
            "is_active", "priority", "last_scraped_at", "created_at", "updated_at"
        ]
    }
    
    # Get schema fields for the specified table
    fields = schema_fields.get(table_name, [])
    
    # If no schema fields defined, return original data
    if not fields:
        return data
    
    # Filter data to include only fields in the schema
    return {k: v for k, v in data.items() if k in fields}

def filter_schema_fields_list(data_list: List[Dict[str, Any]], table_name: str) -> List[Dict[str, Any]]:
    """Filter a list of data dictionaries to include only fields that exist in the specified table schema.
    
    Args:
        data_list: List of dictionaries containing data to filter
        table_name: Name of the table to filter fields for
        
    Returns:
        List[Dict[str, Any]]: Filtered list of data dictionaries
    """
    return [filter_schema_fields(item, table_name) for item in data_list]

def save_pool_stats(pool_stats: Dict[str, Any]) -> bool:
    """Save pool statistics to Supabase.
    
    Args:
        pool_stats: Dictionary containing pool statistics
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get Supabase credentials from environment
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_KEY")
        
        if not supabase_url or not supabase_key:
            print("Supabase credentials not found in environment variables")
            return False
        
        # Initialize Supabase client
        supabase = create_client(supabase_url, supabase_key)
        
        # Filter pool stats to include only fields in the schema
        filtered_pool_stats = filter_schema_fields(pool_stats, "mining_pool_stats")
        
        # Insert pool stats into mining_pool_stats table
        result = supabase.table("mining_pool_stats").insert(filtered_pool_stats).execute()
        
        if hasattr(result, 'data') and result.data:
            print(f"Successfully saved pool stats to Supabase")
            return True
        else:
            print(f"Failed to save pool stats to Supabase")
            return False
    
    except Exception as e:
        print(f"Error saving pool stats to Supabase: {e}")
        return False

def save_worker_stats(worker_stats: List[Dict[str, Any]]) -> bool:
    """Save worker statistics to Supabase.
    
    Args:
        worker_stats: List of dictionaries containing worker statistics
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get Supabase credentials from environment
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_KEY")
        
        if not supabase_url or not supabase_key:
            print("Supabase credentials not found in environment variables")
            return False
        
        # Initialize Supabase client
        supabase = create_client(supabase_url, supabase_key)
        
        # Filter worker stats to include only fields in the schema
        filtered_worker_stats = filter_schema_fields_list(worker_stats, "mining_workers")
        
        # Insert worker stats into mining_workers table
        result = supabase.table("mining_workers").insert(filtered_worker_stats).execute()
        
        if hasattr(result, 'data') and result.data:
            print(f"Successfully saved {len(worker_stats)} worker stats to Supabase")
            return True
        else:
            print(f"Failed to save worker stats to Supabase")
            return False
    
    except Exception as e:
        print(f"Error saving worker stats to Supabase: {e}")
        return False

def save_inactive_worker_stats(inactive_worker_stats: List[Dict[str, Any]]) -> bool:
    """Save inactive worker statistics to Supabase.
    
    Args:
        inactive_worker_stats: List of dictionaries containing inactive worker statistics
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get Supabase credentials from environment
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_KEY")
        
        if not supabase_url or not supabase_key:
            print("Supabase credentials not found in environment variables")
            return False
        
        # Initialize Supabase client
        supabase = create_client(supabase_url, supabase_key)
        
        # Filter inactive worker stats to include only fields in the schema
        filtered_inactive_worker_stats = filter_schema_fields_list(inactive_worker_stats, "mining_inactive_workers")
        
        # Insert inactive worker stats into mining_inactive_workers table
        result = supabase.table("mining_inactive_workers").insert(filtered_inactive_worker_stats).execute()
        
        if hasattr(result, 'data') and result.data:
            print(f"Successfully saved {len(inactive_worker_stats)} inactive worker stats to Supabase")
            return True
        else:
            print(f"Failed to save inactive worker stats to Supabase")
            return False
    
    except Exception as e:
        print(f"Error saving inactive worker stats to Supabase: {e}")
        return False

def save_earnings_history(earnings_history: List[Dict[str, Any]]) -> bool:
    """Save earnings history to Supabase.
    
    Args:
        earnings_history: List of dictionaries containing earnings history
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get Supabase credentials from environment
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_KEY")
        
        if not supabase_url or not supabase_key:
            print("Supabase credentials not found in environment variables")
            return False
        
        # Initialize Supabase client
        supabase = create_client(supabase_url, supabase_key)
        
        # Filter earnings history to include only fields in the schema
        filtered_earnings_history = filter_schema_fields_list(earnings_history, "mining_earnings")
        
        # Insert earnings history into mining_earnings table
        result = supabase.table("mining_earnings").insert(filtered_earnings_history).execute()
        
        if hasattr(result, 'data') and result.data:
            print(f"Successfully saved {len(earnings_history)} earnings entries to Supabase")
            return True
        else:
            print(f"Failed to save earnings history to Supabase")
            return False
    
    except Exception as e:
        print(f"Error saving earnings history to Supabase: {e}")
        return False
