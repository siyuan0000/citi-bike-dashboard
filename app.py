from flask import Flask, render_template, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import func
import fetcher
import database
import logging
import os
import glob
import csv
import io
from urllib.parse import urlparse

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from datetime import datetime
from config import settings, print_runtime_config
from models import Station, Status

app = Flask(__name__)

SPARK_OUTPUT_DIR_CANDIDATES = [
    "spark_output_local",
    "spark_output",
]

SPARK_OUTPUT_S3_URI = os.getenv("SPARK_OUTPUT_S3_URI", "").strip()

SPARK_OUTPUT_TABLES = [
    "overview",
    "duration_stats",
    "rides_by_member_type",
    "rides_by_rideable_type",
    "top_start_stations",
    "top_end_stations",
    "hourly_distribution",
    "data_quality",
]


def _workspace_path(*parts):
    return os.path.join(os.path.dirname(__file__), *parts)


def _parse_s3_uri(s3_uri):
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def _pick_spark_output_dir():
    existing = []
    for candidate in SPARK_OUTPUT_DIR_CANDIDATES:
        full_path = _workspace_path(candidate)
        if os.path.isdir(full_path):
            existing.append(full_path)

    if not existing:
        return None

    return max(existing, key=os.path.getmtime)


def _read_first_csv_rows(folder_path):
    csv_files = sorted(glob.glob(os.path.join(folder_path, "*.csv")))
    if not csv_files:
        return []

    with open(csv_files[0], "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return [dict(row) for row in reader]


def _read_first_csv_rows_from_s3(s3_client, base_s3_uri, table_name):
    bucket, prefix = _parse_s3_uri(base_s3_uri)
    table_prefix = "/".join(p for p in [prefix.rstrip("/"), table_name] if p).rstrip("/") + "/"

    paginator = s3_client.get_paginator("list_objects_v2")
    candidate_keys = []

    for page in paginator.paginate(Bucket=bucket, Prefix=table_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv"):
                candidate_keys.append(key)

    if not candidate_keys:
        return []

    candidate_keys.sort()
    response = s3_client.get_object(Bucket=bucket, Key=candidate_keys[0])
    content = response["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(content))
    return [dict(row) for row in reader]


def _load_spark_analysis_payload_from_s3(s3_uri):
    s3_client = boto3.client("s3")
    tables = {}
    for table_name in SPARK_OUTPUT_TABLES:
        tables[table_name] = _read_first_csv_rows_from_s3(s3_client, s3_uri, table_name)

    return {
        "available": True,
        "message": "Spark analysis loaded successfully from S3.",
        "output_dir": s3_uri,
        "tables": tables,
    }


def _load_spark_analysis_payload():
    if SPARK_OUTPUT_S3_URI:
        try:
            return _load_spark_analysis_payload_from_s3(SPARK_OUTPUT_S3_URI)
        except (ValueError, BotoCoreError, ClientError) as exc:
            logging.warning("Unable to load Spark output from S3 (%s): %s", SPARK_OUTPUT_S3_URI, exc)

    output_dir = _pick_spark_output_dir()
    if not output_dir:
        return {
            "available": False,
            "message": "No Spark output found. Set SPARK_OUTPUT_S3_URI or run spark_analysis.py locally first.",
            "output_dir": None,
            "tables": {},
        }

    tables = {}
    for table_name in SPARK_OUTPUT_TABLES:
        folder = os.path.join(output_dir, table_name)
        if os.path.isdir(folder):
            tables[table_name] = _read_first_csv_rows(folder)
        else:
            tables[table_name] = []

    return {
        "available": True,
        "message": "Spark analysis loaded successfully.",
        "output_dir": os.path.basename(output_dir),
        "tables": tables,
    }

print_runtime_config(mask_secrets=True)

# Initialize Local SQLite database (Table Setup)
database.init_db()

# Fetch baseline coordinates if tables are empty
fetcher.fetch_and_store_static_info()

# Start background scheduler to fetch data every 30 seconds
logging.info("Starting background scheduler...")
scheduler = BackgroundScheduler()
# Executes the job function every 30 seconds mimicking real-time monitoring
scheduler.add_job(
    func=fetcher.job_fetch_realtime_status,
    trigger="interval",
    seconds=settings.fetch_interval_seconds,
)
scheduler.start()

# Manually trigger the first fetch right now!
# This ensures that there is data in the DB immediately on startup
# preventing empty charts if a user clicks right away.
fetcher.job_fetch_realtime_status()

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/history")
def history():
    return render_template("history.html")


@app.route("/api/history-data")
def get_history_data():
    payload = _load_spark_analysis_payload()
    return jsonify(payload)

@app.route("/station/<station_id>")
def station_detail(station_id):
    """ Detail Page showing Highcharts """
    with database.get_session() as session:
        station_row = session.get(Station, station_id)
        latest_row = (
            session.query(Status)
            .filter(Status.station_id == station_id)
            .order_by(Status.id.desc())
            .first()
        )
        rows = (
            session.query(Status)
            .filter(Status.station_id == station_id)
            .order_by(Status.grab_time.asc(), Status.id.asc())
            .all()
        )

    station_name = station_row.name if station_row else station_id
    station_lat = station_row.lat if station_row else None
    station_lon = station_row.lon if station_row else None
    station_capacity = station_row.capacity if station_row else None

    initial_debug_rows = []
    for row in rows:
        initial_debug_rows.append(
            {
                "id": row.id,
                "station_id": row.station_id,
                "num_bikes_available": row.num_bikes_available,
                "num_docks_available": row.num_docks_available,
                "grab_time": row.grab_time,
                "local_time": datetime.fromtimestamp(row.grab_time).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    initial_latest_status = {
        "is_installed": bool(latest_row.is_installed) if latest_row and latest_row.is_installed is not None else None,
        "is_renting": bool(latest_row.is_renting) if latest_row and latest_row.is_renting is not None else None,
        "is_returning": bool(latest_row.is_returning) if latest_row and latest_row.is_returning is not None else None,
        "num_bikes_available": latest_row.num_bikes_available if latest_row else None,
        "num_docks_available": latest_row.num_docks_available if latest_row else None,
        "num_vehicles_disabled": latest_row.num_bikes_disabled if latest_row else None,
        "num_docks_disabled": latest_row.num_docks_disabled if latest_row else None,
        "latest_grab_time": latest_row.grab_time if latest_row else None,
    }

    return render_template(
        "station.html",
        station_id=station_id,
        station_name=station_name,
        station_lat=station_lat,
        station_lon=station_lon,
        station_capacity=station_capacity,
        initial_latest_status=initial_latest_status,
        initial_debug_rows=initial_debug_rows,
    )

@app.route("/api/stations")
def get_map_stations():
    """ 
    API endpoint returning station static info + latest station status.
    This allows map markers to show both location and real-time availability.
    """
    with database.get_session() as session:
        latest_subquery = (
            session.query(Status.station_id, func.max(Status.id).label("latest_id"))
            .group_by(Status.station_id)
            .subquery()
        )

        rows = (
            session.query(Station, Status)
            .outerjoin(latest_subquery, Station.station_id == latest_subquery.c.station_id)
            .outerjoin(Status, Status.id == latest_subquery.c.latest_id)
            .order_by(Station.name.asc())
            .all()
        )
    
    stations = []
    for station, status in rows:
        is_installed = bool(status.is_installed) if status and status.is_installed is not None else None
        is_renting = bool(status.is_renting) if status and status.is_renting is not None else None
        is_returning = bool(status.is_returning) if status and status.is_returning is not None else None

        stations.append({
            "station_id": station.station_id,
            "name": station.name,
            "lat": station.lat,
            "lon": station.lon,
            "capacity": station.capacity,
            "is_installed": is_installed,
            "is_renting": is_renting,
            "is_returning": is_returning,
            "num_bikes_available": status.num_bikes_available if status else None,
            "num_docks_available": status.num_docks_available if status else None,
            "num_vehicles_disabled": status.num_bikes_disabled if status else None,
            "num_bikes_disabled": status.num_bikes_disabled if status else None,
            "num_docks_disabled": status.num_docks_disabled if status else None,
            "latest_grab_time": status.grab_time if status else None,
            "latest_grab_time_ms": status.grab_time * 1000 if status and status.grab_time is not None else None,
        })

    return jsonify(stations)


@app.route("/api/station_latest/<station_id>")
def get_station_latest(station_id):
    """Returns static station info with the newest status row for one station."""
    with database.get_session() as session:
        station = session.get(Station, station_id)
        latest_status = (
            session.query(Status)
            .filter(Status.station_id == station_id)
            .order_by(Status.id.desc())
            .first()
        )

    if not station:
        return jsonify({"error": "Station not found"}), 404

    payload = {
        "station_id": station.station_id,
        "name": station.name,
        "lat": station.lat,
        "lon": station.lon,
        "capacity": station.capacity,
        "is_installed": bool(latest_status.is_installed) if latest_status and latest_status.is_installed is not None else None,
        "is_renting": bool(latest_status.is_renting) if latest_status and latest_status.is_renting is not None else None,
        "is_returning": bool(latest_status.is_returning) if latest_status and latest_status.is_returning is not None else None,
        "num_bikes_available": latest_status.num_bikes_available if latest_status else None,
        "num_docks_available": latest_status.num_docks_available if latest_status else None,
        "num_vehicles_disabled": latest_status.num_bikes_disabled if latest_status else None,
        "num_docks_disabled": latest_status.num_docks_disabled if latest_status else None,
        "latest_grab_time": latest_status.grab_time if latest_status else None,
        "latest_grab_time_ms": latest_status.grab_time * 1000 if latest_status and latest_status.grab_time is not None else None,
    }
    return jsonify(payload)

@app.route("/api/data/<station_id>")
def get_station_data(station_id):
    """ 
    Returns time-series data for a specific station.
    Accepts an optional URL param `last_time` to fetch only NEW records.
    """
    last_time = request.args.get('last_time', 0, type=int)
    with database.get_session() as session:
        query = (
            session.query(Status.grab_time, Status.num_bikes_available)
            .filter(Status.station_id == station_id)
            .order_by(Status.grab_time.asc())
        )
        if last_time > 0:
            query = query.filter(Status.grab_time > last_time)
        rows = query.all()
    
    # Highcharts requires timestamp in milliseconds
    data = [[row.grab_time * 1000, row.num_bikes_available] for row in rows]
    
    return jsonify(data)

@app.route("/api/data_docks/<station_id>")
def get_station_docks_data(station_id):
    """
    Returns available docks time-series for a specific station.
    Accepts an optional URL param `last_time` to fetch only NEW records.
    """
    last_time = request.args.get('last_time', 0, type=int)
    with database.get_session() as session:
        query = (
            session.query(Status.grab_time, Status.num_docks_available)
            .filter(Status.station_id == station_id)
            .order_by(Status.grab_time.asc())
        )
        if last_time > 0:
            query = query.filter(Status.grab_time > last_time)
        rows = query.all()

    data = [[row.grab_time * 1000, row.num_docks_available] for row in rows]
    return jsonify(data)

@app.route("/api/debug/<station_id>")
def get_station_debug_data(station_id):
    """Returns full raw rows for debugging front-end chart issues."""
    with database.get_session() as session:
        rows = (
            session.query(Status)
            .filter(Status.station_id == station_id)
            .order_by(Status.grab_time.asc(), Status.id.asc())
            .all()
        )

    debug_rows = []
    for row in rows:
        debug_rows.append(
            {
                "id": row.id,
                "station_id": row.station_id,
                "num_bikes_available": row.num_bikes_available,
                "num_docks_available": row.num_docks_available,
                "grab_time": row.grab_time,
                "grab_time_ms": row.grab_time * 1000,
            }
        )

    return jsonify(debug_rows)

if __name__ == "__main__":
    # Ensure background thread is killed properly on exit
    try:
        app.run(port=5000, debug=False, use_reloader=False) 
        # use_reloader=False prevents double execution of our 30 second scheduler
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()