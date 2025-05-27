import os
from supabase import create_client
from dotenv import load_dotenv

def get_supabase_client():
    """Get Supabase client from environment variables."""
    # Load environment variables
    load_dotenv()
    
    # Get Supabase credentials
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        print("Supabase credentials not found in environment variables")
        return None
    
    # Create Supabase client
    try:
        supabase = create_client(supabase_url, supabase_key)
        return supabase
    except Exception as e:
        print(f"Error creating Supabase client: {e}")
        return None

def save_worker_stats(worker_stats):
    """Save worker statistics to Supabase."""
    supabase = get_supabase_client()
    if not supabase:
        return {"success": False, "error": "Supabase client not initialized"}
    
    try:
        # Insert data into mining_workers table
        for worker in worker_stats:
            data = {
                "worker_name": worker["worker"],
                "ten_min_hashrate": worker["ten_min_hashrate"],
                "one_h_hashrate": worker["one_h_hashrate"],
                "h24_hashrate": worker["h24_hashrate"],
                "rejection_rate": worker["rejection_rate"],
                "last_share_time": worker["last_share_time"],
                "connections_24h": worker["connections_24h"],
                "status": worker["status"],
                "observer_user_id": worker["observer_user_id"],
                "coin_type": worker["coin_type"],
                "timestamp": worker["timestamp"]
            }
            
            response = supabase.table("mining_workers").insert(data).execute()
            
        return {"success": True, "message": f"Inserted {len(worker_stats)} records"}
    except Exception as e:
        return {"success": False, "error": str(e)}

def save_pool_stats(pool_stats):
    """Save pool statistics to Supabase."""
    supabase = get_supabase_client()
    if not supabase:
        return {"success": False, "error": "Supabase client not initialized"}
    
    try:
        data = {
            "ten_min_hashrate": pool_stats["ten_min_hashrate"],
            "day_hashrate": pool_stats["day_hashrate"],
            "active_workers": pool_stats["active_workers"],
            "inactive_workers": pool_stats["inactive_workers"],
            "account_balance": pool_stats["account_balance"],
            "yesterday_earnings": pool_stats["yesterday_earnings"],
            "observer_user_id": pool_stats["observer_user_id"],
            "coin_type": pool_stats["coin_type"],
            "timestamp": pool_stats["timestamp"]
        }
        
        response = supabase.table("mining_pool_stats").insert(data).execute()
        
        return {"success": True, "message": "Pool stats saved successfully"}
    except Exception as e:
        return {"success": False, "error": str(e)}

def save_earnings_history(earnings_history):
    """Save earnings history to Supabase."""
    supabase = get_supabase_client()
    if not supabase:
        return {"success": False, "error": "Supabase client not initialized"}
    
    try:
        # Insert data into mining_earnings table
        for entry in earnings_history:
            data = {
                "date": entry["date"],
                "daily_hashrate": entry["daily_hashrate"],
                "earnings_amount": entry["earnings_amount"],
                "earnings_currency": entry["earnings_currency"],
                "earnings_type": entry["earnings_type"],
                "payment_status": entry["payment_status"],
                "observer_user_id": entry["observer_user_id"],
                "coin_type": entry["coin_type"],
                "timestamp": entry["timestamp"]
            }
            
            response = supabase.table("mining_earnings").insert(data).execute()
            
        return {"success": True, "message": f"Inserted {len(earnings_history)} records"}
    except Exception as e:
        return {"success": False, "error": str(e)}

def save_inactive_workers(inactive_workers):
    """Save inactive worker statistics to Supabase."""
    supabase = get_supabase_client()
    if not supabase:
        return {"success": False, "error": "Supabase client not initialized"}
    
    try:
        # Insert data into mining_inactive_workers table
        for worker in inactive_workers:
            data = {
                "worker_name": worker["worker_name"],
                "last_share_time": worker["last_share_time"],
                "inactive_duration": worker["inactive_duration"],
                "observer_user_id": worker["observer_user_id"],
                "coin_type": worker["coin_type"],
                "timestamp": worker["timestamp"]
            }
            
            response = supabase.table("mining_inactive_workers").insert(data).execute()
            
        return {"success": True, "message": f"Inserted {len(inactive_workers)} records"}
    except Exception as e:
        return {"success": False, "error": str(e)}
