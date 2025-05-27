# Antpool Scraper

This repository contains scripts for scraping mining statistics from Antpool's observer page. It's designed to be deployed on Render.com for automated scraping and data collection.

## Features

- Scrapes worker statistics, dashboard metrics, earnings history, and inactive workers
- Automatically saves data to Supabase database
- Includes scheduled jobs via Render.com cron services
- Provides an API for on-demand scraping
- Fully containerized for easy deployment

## Scripts

- `antpool_worker_scraper.py`: Scrapes active worker statistics
- `antpool_dashboard_scraper.py`: Scrapes dashboard metrics (hashrates, worker counts, etc.)
- `antpool_earnings_scraper.py`: Scrapes earnings history
- `antpool_inactive_scraper.py`: Scrapes inactive worker statistics

## Deployment

This repository is configured for easy deployment on Render.com using the included `render.yaml` blueprint.

### Prerequisites

1. A Supabase account with tables created using the SQL scripts
2. A Render.com account
3. Your Antpool observer access credentials

### Deployment Steps

1. Fork or clone this repository
2. Connect your GitHub repository to Render.com
3. Set up the required environment variables:
   - `ACCESS_KEY`: Your Antpool access key
   - `USER_ID`: Your Antpool user ID
   - `COIN_TYPE`: Cryptocurrency type (default: BTC)
   - `SUPABASE_URL`: Your Supabase project URL
   - `SUPABASE_KEY`: Your Supabase API key

## Local Development

### Setup

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   python -m playwright install chromium
   ```

3. Create a `.env` file with your credentials:
   ```
   ACCESS_KEY=your_access_key
   USER_ID=your_user_id
   COIN_TYPE=BTC
   SUPABASE_URL=your_supabase_url
   SUPABASE_KEY=your_supabase_key
   ```

### Running Scripts

Run any of the scripts with:

```bash
python scripts/antpool_worker_scraper.py --access_key=YOUR_ACCESS_KEY --user_id=YOUR_USER_ID --coin_type=BTC --output_dir=./output
```

## API Usage

When deployed, the API provides endpoints for on-demand scraping:

- `GET /`: Check if the API is running
- `GET /health`: Check API health status
- `POST /run/worker`: Run the worker scraper
- `POST /run/dashboard`: Run the dashboard scraper
- `POST /run/earnings`: Run the earnings scraper
- `POST /run/inactive`: Run the inactive workers scraper

## License

MIT
