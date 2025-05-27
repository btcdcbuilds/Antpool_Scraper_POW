# Antpool Multi-Account Scraper

This repository contains scripts for scraping Antpool mining pool data across multiple accounts, with support for multiple clients, sites, and providers.

## Features

- **Multi-Account Support**: Scrape data from unlimited Antpool accounts
- **Client/Site Organization**: Organize accounts by client, site, and provider
- **Robust Error Handling**: Automatic retries, failure isolation, and detailed logging
- **Supabase Integration**: Store credentials and mining data in Supabase
- **Render.com Deployment**: Easy deployment to Render.com for scheduled execution

## Architecture

### Database Schema

The system uses Supabase for storing both credentials and mining data:

#### Account Credentials Table
```sql
CREATE TABLE IF NOT EXISTS account_credentials (
    id SERIAL PRIMARY KEY,
    account_name TEXT NOT NULL,
    access_key TEXT NOT NULL,
    user_id TEXT NOT NULL,
    coin_type TEXT NOT NULL DEFAULT 'BTC',
    client_id TEXT,                -- Client/company identifier
    site_id TEXT,                  -- Site identifier within client
    provider TEXT DEFAULT 'Antpool', -- Service provider
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    priority INTEGER NOT NULL DEFAULT 1,
    last_scraped_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### Error Handling

The multi-account scrapers include comprehensive error handling:

1. **Individual Account Isolation**: If one account fails, others continue processing
2. **Automatic Retry Logic**: Failed accounts are retried with exponential backoff
3. **Detailed Logging**: All errors are logged with timestamps and account details
4. **Failure Recovery**: If a scraper crashes mid-execution, it will resume from where it left off
5. **Network Error Handling**: Temporary network issues are handled with retries
6. **Page Load Failures**: If a website doesn't load, the script waits and retries

## Setup Instructions

### 1. Supabase Setup

1. Create the required tables in Supabase using the SQL in `supabase_create_tables.sql`
2. Add your account credentials to the `account_credentials` table

### 2. Render.com Setup

1. Connect your GitHub repository to Render.com
2. Configure environment variables for each service:
   - `SUPABASE_URL`: Your Supabase project URL
   - `SUPABASE_KEY`: Your Supabase service role key
   - `SCRIPT_NAME`: The multi-account script to run (e.g., `antpool_worker_scraper_multi.py`)

## Scripts

- `antpool_worker_scraper_multi.py`: Scrapes worker statistics for all accounts
- `antpool_dashboard_scraper_multi.py`: Scrapes dashboard metrics for all accounts
- `antpool_earnings_scraper_multi.py`: Scrapes earnings history for all accounts
- `antpool_inactive_scraper_multi.py`: Scrapes inactive worker data for all accounts

## Utilities

- `utils/supabase_utils.py`: Utilities for Supabase integration
- `utils/browser_utils.py`: Utilities for browser automation
- `utils/data_utils.py`: Utilities for data processing

## Maintenance

### Adding New Accounts

To add new accounts, simply insert them into the `account_credentials` table in Supabase:

```sql
INSERT INTO account_credentials 
(account_name, access_key, user_id, coin_type, client_id, site_id, provider, is_active, priority)
VALUES 
('AccountName', 'access_key_value', 'user_id_value', 'BTC', 'client_name', 'site_name', 'Antpool', true, 1);
```

### Prioritizing Accounts

To prioritize certain accounts, increase their `priority` value (higher numbers = higher priority).

### Disabling Accounts

To disable an account, set its `is_active` field to `false`.

## Troubleshooting

If you encounter issues:

1. Check the Render.com service logs for error messages
2. Verify that your Supabase credentials are correct
3. Ensure all required tables exist in your Supabase database
4. Check that the GitHub repository has the latest version of all scripts
