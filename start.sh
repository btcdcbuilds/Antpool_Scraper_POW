#!/bin/bash
# This script serves as the entry point for the container
# It can be used to run different scraper scripts based on environment variables

# Default to worker scraper if no script specified
SCRIPT_NAME=${SCRIPT_NAME:-"antpool_worker_scraper.py"}

# Create output directory
mkdir -p /app/output

# Run the specified script
echo "Starting Antpool scraper: $SCRIPT_NAME"
python scripts/$SCRIPT_NAME --use_supabase --output_dir=/app/output
