import os
import sys
import json
from typing import List, Dict, Any, Optional

from supabase import create_client, Client

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
        
        # Insert pool stats into mining_pool_stats table
        result = supabase.table("mining_pool_stats").insert(pool_stats).execute()
        
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
        
        # Insert worker stats into mining_workers table
        result = supabase.table("mining_workers").insert(worker_stats).execute()
        
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
        
        # Insert inactive worker stats into mining_inactive_workers table
        result = supabase.table("mining_inactive_workers").insert(inactive_worker_stats).execute()
        
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
        
        # Insert earnings history into mining_earnings table
        result = supabase.table("mining_earnings").insert(earnings_history).execute()
        
        if hasattr(result, 'data') and result.data:
            print(f"Successfully saved {len(earnings_history)} earnings entries to Supabase")
            return True
        else:
            print(f"Failed to save earnings history to Supabase")
            return False
    
    except Exception as e:
        print(f"Error saving earnings history to Supabase: {e}")
        return False
