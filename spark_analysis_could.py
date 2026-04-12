import sys
import re
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

def main():
    if len(sys.argv) != 3:
        print("Usage: spark-submit spark_analysis_emr.py <input_s3_path> <output_s3_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = (
        SparkSession.builder
        .appName("CitiBikeRideAnalysis")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    # Read directly from S3 (no local download)
    raw_df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

    df = rename_columns(raw_df)
    df = align_known_schema(df)

    required_columns = ["ride_id", "started_at", "ended_at"]
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = (
        df.withColumn("started_ts", F.to_timestamp("started_at"))
          .withColumn("ended_ts", F.to_timestamp("ended_at"))
          .withColumn("ride_duration_minutes", (F.unix_timestamp("ended_ts") - F.unix_timestamp("started_ts")) / 60.0)
    )

    valid_duration_df = df.filter((F.col("ride_duration_minutes") >= 0) & F.col("ride_duration_minutes").isNotNull())

    # All the same analysis as before
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

    by_member = df.groupBy("member_casual").count().withColumnRenamed("count", "rides").orderBy(F.desc("rides")) if "member_casual" in df.columns else spark.createDataFrame([], "member_casual string, rides long")
    by_rideable_type = df.groupBy("rideable_type").count().withColumnRenamed("count", "rides").orderBy(F.desc("rides")) if "rideable_type" in df.columns else spark.createDataFrame([], "rideable_type string, rides long")

    top_start_stations = df.filter(F.col("start_station_name").isNotNull() & (F.col("start_station_name") != "")).groupBy("start_station_name").count().withColumnRenamed("count", "rides_started").orderBy(F.desc("rides_started")).limit(10) if "start_station_name" in df.columns else spark.createDataFrame([], "start_station_name string, rides_started long")
    top_end_stations = df.filter(F.col("end_station_name").isNotNull() & (F.col("end_station_name") != "")).groupBy("end_station_name").count().withColumnRenamed("count", "rides_ended").orderBy(F.desc("rides_ended")).limit(10) if "end_station_name" in df.columns else spark.createDataFrame([], "end_station_name string, rides_ended long")

    hourly_distribution = df.filter(F.col("started_ts").isNotNull()).withColumn("hour_of_day", F.hour("started_ts")).groupBy("hour_of_day").count().withColumnRenamed("count", "rides").orderBy("hour_of_day")

    null_checks = df.select(
        F.sum(F.when(F.col("start_station_name").isNull() | (F.col("start_station_name") == ""), 1).otherwise(0)).alias("missing_start_station_name") if "start_station_name" in df.columns else F.lit(None).alias("missing_start_station_name"),
        F.sum(F.when(F.col("end_station_name").isNull() | (F.col("end_station_name") == ""), 1).otherwise(0)).alias("missing_end_station_name") if "end_station_name" in df.columns else F.lit(None).alias("missing_end_station_name"),
        F.sum(F.when(F.col("start_lat").isNull() | F.col("start_lng").isNull(), 1).otherwise(0)).alias("missing_start_coords") if {"start_lat", "start_lng"}.issubset(set(df.columns)) else F.lit(None).alias("missing_start_coords"),
        F.sum(F.when(F.col("end_lat").isNull() | F.col("end_lng").isNull(), 1).otherwise(0)).alias("missing_end_coords") if {"end_lat", "end_lng"}.issubset(set(df.columns)) else F.lit(None).alias("missing_end_coords"),
    )

    # Write directly to S3 (creates the exact folder structure you want)
    write_small_csv(overview, f"{output_path}/overview")
    write_small_csv(duration_stats, f"{output_path}/duration_stats")
    write_small_csv(by_member, f"{output_path}/rides_by_member_type")
    write_small_csv(by_rideable_type, f"{output_path}/rides_by_rideable_type")
    write_small_csv(top_start_stations, f"{output_path}/top_start_stations")
    write_small_csv(top_end_stations, f"{output_path}/top_end_stations")
    write_small_csv(hourly_distribution, f"{output_path}/hourly_distribution")
    write_small_csv(null_checks, f"{output_path}/data_quality")

    spark.stop()
    print(f"Analysis complete! Output written to: {output_path}")

if __name__ == "__main__":
    main()