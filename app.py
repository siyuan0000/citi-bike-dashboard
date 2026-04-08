from flask import Flask, render_template, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import func
import fetcher
import database
import logging
from datetime import datetime
from config import settings, print_runtime_config
from models import Station, Status

app = Flask(__name__)

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