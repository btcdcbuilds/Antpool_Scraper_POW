from fastapi import FastAPI, HTTPException, BackgroundTasks
import asyncio
import os
import sys
import importlib.util
from datetime import datetime

app = FastAPI(title="Antpool Scraper API")

# Import scraper modules dynamically
def import_script(script_name):
    script_path = os.path.join("scripts", script_name)
    spec = importlib.util.spec_from_file_location(script_name.replace(".py", ""), script_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[script_name.replace(".py", "")] = module
    spec.loader.exec_module(module)
    return module

# Background task to run scraper
async def run_scraper_task(script_name, access_key, user_id, coin_type):
    try:
        module = import_script(script_name)
        
        # Create args object to simulate command line arguments
        class Args:
            def __init__(self):
                self.access_key = access_key
                self.user_id = user_id
                self.coin_type = coin_type
                self.output_dir = "/tmp/output"
        
        # Create output directory if it doesn't exist
        os.makedirs("/tmp/output", exist_ok=True)
        
        # Run the scraper
        await module.main(Args())
        
        return {"status": "success", "message": f"{script_name} completed successfully"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/")
async def root():
    return {"message": "Antpool Scraper API is running"}

@app.post("/run/{script_type}")
async def run_scraper(script_type: str, background_tasks: BackgroundTasks):
    # Get environment variables
    access_key = os.environ.get("ACCESS_KEY")
    user_id = os.environ.get("USER_ID")
    coin_type = os.environ.get("COIN_TYPE", "BTC")
    
    if not access_key or not user_id:
        raise HTTPException(status_code=500, detail="Missing required environment variables")
    
    # Map script type to script name
    script_mapping = {
        "worker": "antpool_worker_scraper.py",
        "dashboard": "antpool_dashboard_scraper.py",
        "earnings": "antpool_earnings_scraper.py",
        "inactive": "antpool_inactive_scraper.py"
    }
    
    if script_type not in script_mapping:
        raise HTTPException(status_code=400, detail=f"Invalid script type. Must be one of: {', '.join(script_mapping.keys())}")
    
    script_name = script_mapping[script_type]
    
    # Run the scraper in the background
    background_tasks.add_task(run_scraper_task, script_name, access_key, user_id, coin_type)
    
    return {
        "status": "accepted",
        "message": f"Running {script_type} scraper in the background",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}
