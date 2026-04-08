#!/usr/bin/env bash


export APP_ENV="cloud"
export DB_BACKEND="mysql"

export MYSQL_DB_HOST="database-project.cn3sawxyvdt4.us-east-1.rds.amazonaws.com"
export MYSQL_DB_PORT="3306"
export MYSQL_DB_NAME="comp4442_project"
export MYSQL_DB_USER="admin"
export MYSQL_DB_PASSWORD="12345678"

# export SPARK_OUTPUT_S3_URI="s3://citi-bike-dashboard/spark_output/"

# Optional: controls scheduler polling interval (seconds)
export FETCH_INTERVAL_SECONDS="30"

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
	echo "Environment variables were set only in this subshell."
	echo "Run: source ./setenv.bash"
fi

python app.py