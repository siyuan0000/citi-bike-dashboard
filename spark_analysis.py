import argparse
import os
import re
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def normalize_name(name: str) -> str:
    normalized = name.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


def rename_columns(df):
    rename_map = {}
    for old_name in df.columns:
        new_name = normalize_name(old_name)
        if new_name:
            rename_map[old_name] = new_name

    for old_name, new_name in rename_map.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df


def align_known_schema(df):
    aliases = {
        "ride_id": ["ride_id"],
        "rideable_type": ["rideable_type"],
        "started_at": ["started_at", "start_time", "start_at"],
        "ended_at": ["ended_at", "end_time", "end_at"],
        "start_station_name": ["start_station_name", "from_station_name"],
        "start_station_id": ["start_station_id", "from_station_id"],
        "end_station_name": ["end_station_name", "to_station_name"],
        "end_station_id": ["end_station_id", "to_station_id"],
        "start_lat": ["start_lat", "start_latitude"],
        "start_lng": ["start_lng", "start_longitude"],
        "end_lat": ["end_lat", "end_latitude"],
        "end_lng": ["end_lng", "end_longitude"],
        "member_casual": ["member_casual", "member_or_casual_ride"],
    }

    existing = set(df.columns)
    for target, candidates in aliases.items():
        if target in existing:
            continue
        for candidate in candidates:
            if candidate in existing and candidate != target:
                df = df.withColumnRenamed(candidate, target)
                existing.remove(candidate)
                existing.add(target)
                break
    return df


def write_small_csv(df, output_path):
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)


def is_s3_path(path: str) -> bool:
    return path.lower().startswith("s3://")


def parse_s3_uri(s3_uri: str):
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    key = parsed.path.lstrip("/")
    return parsed.netloc, key


def download_s3_input(s3_client, s3_input: str, local_root: str) -> str:
    bucket, key = parse_s3_uri(s3_input)

    # Single file path: s3://bucket/path/file.csv
    if key.lower().endswith(".csv"):
        local_file = os.path.join(local_root, Path(key).name)
        s3_client.download_file(bucket, key, local_file)
        return local_file

    # Prefix path: s3://bucket/path/to/folder
    prefix = key.rstrip("/") + "/" if key else ""
    paginator = s3_client.get_paginator("list_objects_v2")
    downloaded = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            object_key = obj["Key"]
            if object_key.endswith("/") or not object_key.lower().endswith(".csv"):
                continue

            rel = object_key[len(prefix):] if prefix and object_key.startswith(prefix) else Path(object_key).name
            local_file = os.path.join(local_root, rel)
            os.makedirs(os.path.dirname(local_file), exist_ok=True)
            s3_client.download_file(bucket, object_key, local_file)
            downloaded += 1

    if downloaded == 0:
        raise ValueError(f"No CSV files found at S3 prefix: {s3_input}")

    return local_root


def upload_directory_to_s3(s3_client, local_dir: str, s3_output: str):
    bucket, key_prefix = parse_s3_uri(s3_output)
    prefix = key_prefix.rstrip("/")

    for root, _, files in os.walk(local_dir):
        for filename in files:
            full_path = os.path.join(root, filename)
            rel_path = os.path.relpath(full_path, local_dir).replace("\\", "/")
            s3_key = f"{prefix}/{rel_path}" if prefix else rel_path
            s3_client.upload_file(full_path, bucket, s3_key)


def main():
    parser = argparse.ArgumentParser(description="Analyze Citi Bike ride CSV using PySpark")
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    parser.add_argument("--output", required=True, help="Output folder for analyzed data")
    args = parser.parse_args()

    s3_client = boto3.client("s3")
    with tempfile.TemporaryDirectory(prefix="citibike-spark-") as temp_dir:
        input_path = args.input
        output_path = args.output

        try:
            if is_s3_path(args.input):
                input_path = download_s3_input(s3_client, args.input, os.path.join(temp_dir, "input"))

            if is_s3_path(args.output):
                output_path = os.path.join(temp_dir, "output")

            os.makedirs(output_path, exist_ok=True)
        except (BotoCoreError, ClientError) as exc:
            raise RuntimeError(f"Failed while accessing S3. Check AWS credentials/permissions. Details: {exc}") from exc

        spark = (
        SparkSession.builder.appName("CitiBikeRideAnalysis")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
        )

        raw_df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
        df = rename_columns(raw_df)
        df = align_known_schema(df)

        required_columns = ["ride_id", "started_at", "ended_at"]
        missing = [c for c in required_columns if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns after normalization: {missing}")

        df = (
            df.withColumn("started_ts", F.to_timestamp("started_at"))
            .withColumn("ended_ts", F.to_timestamp("ended_at"))
            .withColumn("ride_duration_minutes", (F.unix_timestamp("ended_ts") - F.unix_timestamp("started_ts")) / 60.0)
        )

        # Keep valid rides for duration-based metrics.
        valid_duration_df = df.filter((F.col("ride_duration_minutes") >= 0) & F.col("ride_duration_minutes").isNotNull())

        overview = df.agg(
            F.count("*").alias("total_rides"),
            F.countDistinct("ride_id").alias("distinct_ride_ids"),
            F.min("started_ts").alias("first_ride_start"),
            F.max("ended_ts").alias("last_ride_end"),
        )

        duration_stats = valid_duration_df.agg(
            F.round(F.avg("ride_duration_minutes"), 2).alias("avg_duration_min"),
            F.round(F.expr("percentile_approx(ride_duration_minutes, 0.5)"), 2).alias("median_duration_min"),
            F.round(F.max("ride_duration_minutes"), 2).alias("max_duration_min"),
        )

        by_member = (
            df.groupBy("member_casual")
            .count()
            .withColumnRenamed("count", "rides")
            .orderBy(F.desc("rides"))
        ) if "member_casual" in df.columns else spark.createDataFrame([], "member_casual string, rides long")

        by_rideable_type = (
            df.groupBy("rideable_type")
            .count()
            .withColumnRenamed("count", "rides")
            .orderBy(F.desc("rides"))
        ) if "rideable_type" in df.columns else spark.createDataFrame([], "rideable_type string, rides long")

        top_start_stations = (
            df.filter(F.col("start_station_name").isNotNull() & (F.col("start_station_name") != ""))
            .groupBy("start_station_name")
            .count()
            .withColumnRenamed("count", "rides_started")
            .orderBy(F.desc("rides_started"))
            .limit(10)
        ) if "start_station_name" in df.columns else spark.createDataFrame([], "start_station_name string, rides_started long")

        top_end_stations = (
            df.filter(F.col("end_station_name").isNotNull() & (F.col("end_station_name") != ""))
            .groupBy("end_station_name")
            .count()
            .withColumnRenamed("count", "rides_ended")
            .orderBy(F.desc("rides_ended"))
            .limit(10)
        ) if "end_station_name" in df.columns else spark.createDataFrame([], "end_station_name string, rides_ended long")

        hourly_distribution = (
            df.filter(F.col("started_ts").isNotNull())
            .withColumn("hour_of_day", F.hour("started_ts"))
            .groupBy("hour_of_day")
            .count()
            .withColumnRenamed("count", "rides")
            .orderBy("hour_of_day")
        )

        null_checks = df.select(
            F.sum(F.when(F.col("start_station_name").isNull() | (F.col("start_station_name") == ""), 1).otherwise(0)).alias("missing_start_station_name") if "start_station_name" in df.columns else F.lit(None).alias("missing_start_station_name"),
            F.sum(F.when(F.col("end_station_name").isNull() | (F.col("end_station_name") == ""), 1).otherwise(0)).alias("missing_end_station_name") if "end_station_name" in df.columns else F.lit(None).alias("missing_end_station_name"),
            F.sum(F.when(F.col("start_lat").isNull() | F.col("start_lng").isNull(), 1).otherwise(0)).alias("missing_start_coords") if {"start_lat", "start_lng"}.issubset(set(df.columns)) else F.lit(None).alias("missing_start_coords"),
            F.sum(F.when(F.col("end_lat").isNull() | F.col("end_lng").isNull(), 1).otherwise(0)).alias("missing_end_coords") if {"end_lat", "end_lng"}.issubset(set(df.columns)) else F.lit(None).alias("missing_end_coords"),
        )

        write_small_csv(overview, f"{output_path}/overview")
        write_small_csv(duration_stats, f"{output_path}/duration_stats")
        write_small_csv(by_member, f"{output_path}/rides_by_member_type")
        write_small_csv(by_rideable_type, f"{output_path}/rides_by_rideable_type")
        write_small_csv(top_start_stations, f"{output_path}/top_start_stations")
        write_small_csv(top_end_stations, f"{output_path}/top_end_stations")
        write_small_csv(hourly_distribution, f"{output_path}/hourly_distribution")
        write_small_csv(null_checks, f"{output_path}/data_quality")

        spark.stop()

        if is_s3_path(args.output):
            try:
                upload_directory_to_s3(s3_client, output_path, args.output)
            except (BotoCoreError, ClientError) as exc:
                raise RuntimeError(f"Analysis completed, but upload to S3 failed: {exc}") from exc


if __name__ == "__main__":
    main()