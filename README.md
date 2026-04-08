# Citi Bike Dashboard

A web dashboard demonstrating real-time queries and historical data visualization using Citi Bike's General Bikeshare Feed Specification (GBFS) data. Set up to be deployed on AWS Elastic Beanstalk and integrated with Amazon S3.

## Overview
This application serves two main purposes:
- **Real-Time Data (Lab 4):** A dashboard that fetches, merges, and displays real-time static info and dynamic status properties of Citi Bike stations. Backed by RDS and deployed via AWS Elastic Beanstalk.
- **History Analytics (Lab 5):** An integration displaying historical bike distribution computed asynchronously using Spark, with pre-computed query answers stored in S3.

## Installation & Local Execution
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Configure environment (optional for local):
   ```bash
   # Local mode (default)
   APP_ENV=local
   DB_BACKEND=sqlite
   SQLITE_DB_PATH=./citibike.db

   # Cloud mode (AWS RDS Postgres)
   APP_ENV=cloud
   DB_BACKEND=postgres
   AWS_DB_HOST=<your-rds-endpoint>
   AWS_DB_PORT=5432
   AWS_DB_NAME=<database_name>
   AWS_DB_USER=<database_user>
   AWS_DB_PASSWORD=<database_password>
   AWS_DB_SSLMODE=require
   ```
3. Optional scheduler interval control:
   ```bash
   FETCH_INTERVAL_SECONDS=30
   ```
4. Run the server locally:
   ```bash
   python app.py
   ```
5. Visit `http://localhost:5000` in your web browser.
