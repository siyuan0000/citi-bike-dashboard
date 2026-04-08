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

## Spark Analysis for Ride CSV
If your CSV contains ride-level fields like Ride ID, Rideable type, Started at, Ended at, station names/IDs, coordinates, and Member/Casual type, you can generate analyzed outputs with PySpark.

Run:

```bash
python spark_analysis.py --input /path/to/rides.csv --output ./spark_output
```

Quick local test with downloaded monthly files:

```bash
bash testSparkLocal.bash
```

This script reads `202603-citibike-tripdata_*.csv` and writes analyzed outputs to `./spark_output_local`.

S3 is also supported for both input and output:

```bash
python spark_analysis.py --input s3://your-bucket/input/rides.csv --output s3://your-bucket/output/citibike-analysis
```

You can also use an S3 prefix as input (all CSV files under that prefix are loaded):

```bash
python spark_analysis.py --input s3://your-bucket/input/monthly/ --output ./spark_output
```

AWS credentials are read from your standard environment/credentials chain (for example `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, or `~/.aws/credentials`).

To make the Flask history page read Spark output directly from S3, set:

```bash
export SPARK_OUTPUT_S3_URI=s3://your-bucket/output/citibike-analysis
```

Then `/history` and `/api/history-data` will load table CSV files from that S3 prefix instead of local output folders.

Generated output folders (CSV files inside each folder):
- `spark_output/overview`: total rides, distinct ride IDs, date range
- `spark_output/duration_stats`: average/median/max ride duration
- `spark_output/rides_by_member_type`: member vs casual counts
- `spark_output/rides_by_rideable_type`: bike type distribution
- `spark_output/top_start_stations`: top 10 start stations
- `spark_output/top_end_stations`: top 10 end stations
- `spark_output/hourly_distribution`: ride counts by hour of day
- `spark_output/data_quality`: missing station/coordinate counts

The script automatically normalizes column names (for example, `Ride ID` to `ride_id`, `End Longitude` to `end_lng`) before analysis.
