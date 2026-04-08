# Citi Bike Dashboard

A Flask web dashboard for Citi Bike station data that supports:
- Real-time station information and status (GBFS)
- Historical analytics integration via precomputed S3 outputs
- Local SQLite and cloud MySQL backends

## Overview
This project combines two workflows:
- Real-time dashboard queries for station metadata and availability
- Historical bike distribution analytics generated asynchronously (for example, via Spark jobs) and served from S3-backed outputs

## Project Structure
- app.py: Flask app entry point and routes
- fetcher.py: Scheduled data fetching and merge logic
- database.py: Database connection and persistence helpers
- models.py: Data model definitions
- config.py: Environment-driven configuration
- templates/: Dashboard HTML templates

## Setup
1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Configure environment variables.

Local mode (default):

```bash
APP_ENV=local
DB_BACKEND=sqlite
SQLITE_DB_PATH=./citibike.db
```

Cloud mode (AWS RDS MySQL):

```bash
APP_ENV=cloud
DB_BACKEND=mysql
MYSQL_DB_HOST=<your-rds-endpoint>
MYSQL_DB_PORT=3306
MYSQL_DB_NAME=<database_name>
MYSQL_DB_USER=<database_user>
MYSQL_DB_PASSWORD=<database_password>
```

Optional fetch interval override:

```bash
FETCH_INTERVAL_SECONDS=30
```

## Run
Run locally with Python:

```bash
python app.py
```

Or use helper scripts:

```bash
bash startLocal.bash
bash startCloud.bash
```

Then open http://localhost:5000.

## Create a Clean Project Zip (Exclude Runtime Files)
Use this command to archive the project while excluding runtime artifacts such as virtual environments, caches, editor settings, and Git metadata:

```bash
zip -r citi-bike-dashboard.zip . \
  -x ".venv/*" "*/.venv/*" "venv/*" "*/venv/*" \
     "__pycache__/*" "*/__pycache__/*" "*.pyc" "*.pyo" \
     ".pytest_cache/*" "*/.pytest_cache/*" ".mypy_cache/*" "*/.mypy_cache/*" \
     ".vscode/*" "*/.vscode/*" ".git/*" "*/.git/*" \
     "citi-bike-dashboard.zip"
```

## Notes
- The file citibike.db may be included in the archive if it exists. Remove it first if you want a source-only package.
- For deployment packaging, ensure cloud environment variables are set in your Elastic Beanstalk environment.
