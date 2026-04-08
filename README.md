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
2. Run the server locally:
   ```bash
   python application.py
   ```
3. Visit `http://localhost:5000` in your web browser.
