# Coalesce Catalog to Snowflake Sync

> **Disclaimer:** This is a sample project provided as-is for reference.

Extract tags from Coalesce Catalog and generate Snowflake SQL ALTER statements to apply those tags to tables and columns, with intelligent change tracking between runs.

## Key Features

- **Tag Extraction**: Reads table-level and column-level tags from the Coalesce Catalog GraphQL API
- **SQL Generation**: Generates `ALTER TABLE ... SET TAG` and `ALTER COLUMN ... SET TAG` statements
- **Change Tracking**: Compares runs to detect new, modified, and removed tags
- **Unified Changes File**: DROP statements first (cleanup), then SET statements (updates)
- **Timestamp Audit Trail**: Every SQL statement includes the tag's `updatedAt` timestamp
- **GitHub Actions**: Automated daily sync with Slack notifications
- **Zone Support**: Works with both US and EU Coalesce instances

## Quick Start

```bash
# One-command setup
make setup

# Edit .env and add your API token
# Then validate API connection:
make validate

# Run full sync (all tables):
make run
```

Or manually:

```bash
python3 -m venv .venv
.venv/bin/pip install .
cp .env.example .env
# Edit .env with your API token
.venv/bin/python3 main.py
```

## Requirements

- **Python**: 3.10+ recommended
- **Coalesce Catalog**: API token from Settings > API Tokens
- **Snowflake**: Account with permissions to create and set tags

## Configuration

Copy `.env.example` to `.env` and set your values:

```env
COALESCE_API_TOKEN=your_api_token_here
COALESCE_ZONE=US                        # US or EU
```

The API URL is derived automatically from the zone.

## Usage

All tables are processed by default. Use `--limit` or `--table-id` only for testing.

```bash
# Full sync (default — processes all tables with change tracking)
make run

# Test with 5 tables
make run-limited

# Force all tags as NEW (ignore sync history)
make run-force

# Process a specific table
make run-table TABLE_ID=your-table-id

# Validate API connection (1 table)
make validate

# Show configured env vars (masked)
make check-env

# Remove generated output files
make clean
```

Or run directly:

```bash
.venv/bin/python3 main.py                    # All tables (default)
.venv/bin/python3 main.py --limit 5          # Test with 5 tables
.venv/bin/python3 main.py --force-all        # Treat all tags as NEW
.venv/bin/python3 main.py --table-id <id>    # Specific table
```

### Command Line Arguments

| Argument | Description | Example |
| --- | --- | --- |
| *(none)* | Process all tables with change tracking | `python main.py` |
| `--limit N` | Limit tables (testing only) | `--limit 5` |
| `--table-id ID` | Process specific table | `--table-id abc123` |
| `--table-ids ID...` | Process multiple tables | `--table-ids id1 id2` |
| `--force-all` | Treat all tags as NEW | `--force-all` |
| `--output-dir` | Directory for JSON data | `--output-dir ./data` |
| `--sql-dir` | Directory for SQL files | `--sql-dir ./sql` |

## Change Tracking

Change tracking is only available on full runs (no `--limit`, `--table-id`, or `--table-ids`). This prevents misleading comparisons when processing a subset of tables.

**Full runs generate two SQL files:**

- `complete_current_state_*.sql` — All current tags (always generated)
- `changes_since_last_full_run_*.sql` — Only changes since last run (DROP first, then SET)

**Limited runs generate only:**

- `complete_current_state_*.sql`

Every SQL statement includes a timestamp comment for audit:

```sql
ALTER TABLE PROD_DB.ANALYTICS.CUSTOMERS
    SET TAG SOURCE = 'salesforce';  -- Tag applied: 2026-02-09 10:30:00
```

## Deploying to Your Org

### Step 1: Fork the repo

Fork this repo to your GitHub org. Your fork will have a `main` branch with the latest stable code. Keep `main` as the default branch — GitHub Actions scheduled triggers always run on the default branch.

> **Note:** GitHub forks of public repositories are always public. Do not commit secrets or sensitive values directly — use GitHub Secrets and Variables instead.

### Step 2: Create a `develop` branch

Create a `develop` branch from `main`. This is where you'll pull in upstream updates and make customizations before promoting to `main`.

### Step 3: Add secrets and variables

Go to **Settings > Secrets and variables > Actions** and add:

- **Secrets**: `COALESCE_API_TOKEN`, `SLACK_WEBHOOK_URL` (optional)
- **Variables**: `COALESCE_ZONE` (`US` or `EU`, default: `US`)

### Step 4: Enable the workflow

On your `develop` branch, edit `.github/workflows/sync-catalog.yml` and uncomment the triggers you want (schedule or both). See [Enabling Automated Triggers](#enabling-automated-triggers) for details.

When you merge `develop` → `main`, the workflow triggers go with it.

### Step 5: Run the workflow

Go to **Actions > Coalesce Catalog to Snowflake Sync > Run workflow** to trigger a manual run and verify everything works.

### Pulling in updates

```bash
git checkout develop
git fetch upstream
git merge upstream/main
```

Test on `develop`, then merge `develop` → `main` when ready to go live.

### Branching

- **This repo**: `main` (default) — stable releases
- **Your fork**:
  - `main` — default branch, runs scheduled and push-triggered workflows
  - `develop` — working branch for pulling upstream updates and making customizations
  - Merge `develop` → `main` to promote changes

### Enabling Automated Triggers

The workflow ships with only manual trigger (`workflow_dispatch`) enabled. To enable automated runs, edit `.github/workflows/sync-catalog.yml` and uncomment the triggers you want:

```yaml
on:
  workflow_dispatch:
  # To enable scheduled runs, uncomment the next two lines:
  # schedule:
  #   - cron: '0 2 * * *'
```

- **Schedule**: Runs daily at 2 AM UTC (adjust the cron expression as needed)

Common schedules: `'0 */6 * * *'` (every 6 hours), `'0 9 * * 1'` (weekly Monday 9 AM).

### GitHub Actions Artifacts

After each run, download from the **Actions** tab:

- **`sql-files-{run}`** — Generated SQL statements
- **`reports-{run}`** — Sync summary report
- **`catalog-data`** — Previous run data (used for change tracking)

Retention: 30 days for all artifacts.

## Slack Notifications (Optional)

1. Create a [Slack Incoming Webhook](https://api.slack.com/messaging/webhooks)
2. Add `SLACK_WEBHOOK_URL` as a **Secret** in GitHub repository settings
3. Notifications are sent automatically after each workflow run with metrics and a download link

## Tag Mapping

Tags follow a `key:value` format. The key becomes the Snowflake tag name, the value becomes the tag value:

| Catalog Tag | Snowflake SQL |
| --- | --- |
| `source: salesforce` | `SET TAG SOURCE = 'salesforce'` |
| `catalog:sensitive email` | `SET TAG SENSITIVE = 'sensitive email'` |
| `catalog:process etl` | `SET TAG PROCESS = 'process etl'` |

## Project Structure

```text
coalesce_catalog_to_snowflake_sync/
├── main.py                          # Orchestrator (entry point)
├── pyproject.toml                   # Package configuration and dependencies
├── Makefile                         # make setup, make run, make test, etc.
├── .env.example                     # Environment variable template
├── catalog_to_snowflake/            # Core package
│   ├── __init__.py                  # Package exports
│   ├── catalog_api_client.py        # GraphQL API client
│   ├── get_warehouses.py            # Fetch Snowflake warehouse IDs
│   ├── get_tables.py                # Fetch tables from catalog
│   ├── get_columns.py               # Fetch columns and tags
│   ├── generate_sql.py              # Generate ALTER statements
│   ├── compute_changes.py           # Change tracking between runs
│   └── save_outputs.py              # Save JSON, SQL, and reports
├── .github/workflows/
│   └── sync-catalog.yml             # GitHub Actions workflow
├── data/                            # JSON data (auto-created)
├── sql/                             # Generated SQL (auto-created)
├── reports/                         # Sync reports (auto-created)
└── logs/                            # Execution logs (auto-created)
```

## Workflow Steps

1. **Fetch Warehouses** — Identifies Snowflake warehouses in the catalog
2. **Fetch Tables** — Gets all tables with metadata and table-level tags
3. **Fetch Columns** — Gets columns and their tags for each table
4. **Generate SQL** — Creates ALTER statements with timestamp comments
5. **Analyze Changes** — Compares with previous run (full runs only)
6. **Save Results** — Writes SQL, JSON, and reports to output directories

## Executing in Snowflake

1. **Create tags** (if they don't exist):

   ```sql
   CREATE TAG IF NOT EXISTS SOURCE COMMENT = 'Source system tag';
   CREATE TAG IF NOT EXISTS SENSITIVITY COMMENT = 'Data sensitivity classification';
   ```

1. **Run the generated SQL** from the `sql/` directory:
   - `complete_current_state_*.sql` — Apply all current tags
   - `changes_since_last_full_run_*.sql` — Apply only changes since last run (DROP first, then SET)
