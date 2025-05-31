# Antpool Scraper POW

A collection of scripts for scraping Antpool mining pool data.

## Scripts

### Worker Scraper (Hybrid Version)

The `antpool_worker_scraper_hybrid.py` script combines the proven extraction logic with multi-browser optimization for maximum reliability and performance.

#### Features:
- Multi-browser architecture with browser reuse
- Group-based processing for distributed workloads
- Maintains the original working extraction logic
- Optimized for performance with multiple pools

#### Usage:
```bash
python scripts/antpool_worker_scraper_hybrid.py --group=1 --total-groups=3 --max-concurrent=3
```

#### Parameters:
- `--group`: The group number to process (1-based)
- `--total-groups`: Total number of groups to split accounts into
- `--max-concurrent`: Maximum number of concurrent browsers to use
- `--debug`: Enable debug mode with additional logging and screenshots
- `--output-dir`: Directory for output files (default: ./output)

### Combined Daily Report Scraper

The `antpool_combined_daily_scraper.py` script combines dashboard metrics and earnings history into a comprehensive daily report.

#### Features:
- Single browser session per account for efficiency
- Extracts dashboard metrics using JavaScript evaluation
- Captures earnings data for the last 7 days
- Saves data to both local files and Supabase tables

#### Usage:
```bash
python scripts/antpool_combined_daily_scraper.py
```

## Deployment

The project is configured for deployment on Render.com using the included `render.yaml` file.

### Services:

1. **Worker Scraper (3 instances)**:
   - Split into 3 groups with staggered schedules
   - Run every 3 hours with 5-minute offsets
   - Each instance uses 3 concurrent browsers

2. **Daily Report Scraper**:
   - Runs daily at 1 AM
   - Combines dashboard and earnings data

3. **Inactive Workers Scraper**:
   - Runs every 30 minutes
   - Monitors for inactive workers

4. **API Service**:
   - On-demand API for manual scraping requests

### Environment Variables:

The following environment variables must be set in Render.com:

- `SUPABASE_URL`: Your Supabase project URL
- `SUPABASE_KEY`: Your Supabase API key
