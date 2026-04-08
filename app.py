from flask import Flask, render_template, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler
import fetcher
import database
import logging
from datetime import datetime

app = Flask(__name__)

# Initialize Local SQLite database (Table Setup)
database.init_db()

# Fetch baseline coordinates if tables are empty
fetcher.fetch_and_store_static_info()

# Start background scheduler to fetch data every 30 seconds
logging.info("Starting background scheduler...")
scheduler = BackgroundScheduler()
# Executes the job function every 30 seconds mimicking real-time monitoring
scheduler.add_job(func=fetcher.job_fetch_realtime_status, trigger="interval", seconds=30)
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
    conn = database.get_db_connection()
    c = conn.cursor()
    c.execute("SELECT name, lat, lon, capacity FROM Station WHERE station_id = ?", (station_id,))
    station_row = c.fetchone()
    station_name = station_row["name"] if station_row else station_id
    station_lat = station_row["lat"] if station_row else None
    station_lon = station_row["lon"] if station_row else None
    station_capacity = station_row["capacity"] if station_row else None

    c.execute(
        """
        SELECT
            is_installed,
            is_renting,
            is_returning,
            num_bikes_available,
            num_docks_available,
            num_bikes_disabled,
            num_docks_disabled,
            grab_time
        FROM Status
        WHERE station_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (station_id,),
    )
    latest_row = c.fetchone()

    c.execute(
        """
        SELECT id, station_id, num_bikes_available, num_docks_available, grab_time
        FROM Status
        WHERE station_id = ?
        ORDER BY grab_time ASC, id ASC
        """,
        (station_id,),
    )
    rows = c.fetchall()
    conn.close()

    initial_debug_rows = []
    for row in rows:
        initial_debug_rows.append(
            {
                "id": row["id"],
                "station_id": row["station_id"],
                "num_bikes_available": row["num_bikes_available"],
                "num_docks_available": row["num_docks_available"],
                "grab_time": row["grab_time"],
                "local_time": datetime.fromtimestamp(row["grab_time"]).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    initial_latest_status = {
        "is_installed": bool(latest_row["is_installed"]) if latest_row and latest_row["is_installed"] is not None else None,
        "is_renting": bool(latest_row["is_renting"]) if latest_row and latest_row["is_renting"] is not None else None,
        "is_returning": bool(latest_row["is_returning"]) if latest_row and latest_row["is_returning"] is not None else None,
        "num_bikes_available": latest_row["num_bikes_available"] if latest_row else None,
        "num_docks_available": latest_row["num_docks_available"] if latest_row else None,
        "num_vehicles_disabled": latest_row["num_bikes_disabled"] if latest_row else None,
        "num_docks_disabled": latest_row["num_docks_disabled"] if latest_row else None,
        "latest_grab_time": latest_row["grab_time"] if latest_row else None,
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
    conn = database.get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT
            s.station_id,
            s.name,
            s.lat,
            s.lon,
            s.capacity,
            st.num_bikes_available,
            st.num_docks_available,
            st.is_installed,
            st.is_renting,
            st.is_returning,
            st.num_bikes_disabled,
            st.num_docks_disabled,
            st.grab_time
        FROM Station s
        LEFT JOIN (
            SELECT
                cur.station_id,
                cur.num_bikes_available,
                cur.num_docks_available,
                cur.is_installed,
                cur.is_renting,
                cur.is_returning,
                cur.num_bikes_disabled,
                cur.num_docks_disabled,
                cur.grab_time
            FROM Status cur
            INNER JOIN (
                SELECT station_id, MAX(id) AS latest_id
                FROM Status
                GROUP BY station_id
            ) latest
                ON cur.station_id = latest.station_id
                AND cur.id = latest.latest_id
        ) st
            ON s.station_id = st.station_id
        ORDER BY s.name ASC
        """
    )
    rows = c.fetchall()
    
    stations = []
    for row in rows:
        is_installed = bool(row["is_installed"]) if row["is_installed"] is not None else None
        is_renting = bool(row["is_renting"]) if row["is_renting"] is not None else None
        is_returning = bool(row["is_returning"]) if row["is_returning"] is not None else None

        stations.append({
            "station_id": row["station_id"],
            "name": row["name"],
            "lat": row["lat"],
            "lon": row["lon"],
            "capacity": row["capacity"],
            "is_installed": is_installed,
            "is_renting": is_renting,
            "is_returning": is_returning,
            "num_bikes_available": row["num_bikes_available"],
            "num_docks_available": row["num_docks_available"],
            "num_vehicles_disabled": row["num_bikes_disabled"],
            "num_bikes_disabled": row["num_bikes_disabled"],
            "num_docks_disabled": row["num_docks_disabled"],
            "latest_grab_time": row["grab_time"],
            "latest_grab_time_ms": row["grab_time"] * 1000 if row["grab_time"] is not None else None,
        })
        
    conn.close()
    return jsonify(stations)


@app.route("/api/station_latest/<station_id>")
def get_station_latest(station_id):
    """Returns static station info with the newest status row for one station."""
    conn = database.get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT
            s.station_id,
            s.name,
            s.lat,
            s.lon,
            s.capacity,
            st.num_bikes_available,
            st.num_docks_available,
            st.is_installed,
            st.is_renting,
            st.is_returning,
            st.num_bikes_disabled,
            st.num_docks_disabled,
            st.grab_time
        FROM Station s
        LEFT JOIN (
            SELECT
                station_id,
                num_bikes_available,
                num_docks_available,
                is_installed,
                is_renting,
                is_returning,
                num_bikes_disabled,
                num_docks_disabled,
                grab_time
            FROM Status
            WHERE station_id = ?
            ORDER BY id DESC
            LIMIT 1
        ) st
            ON s.station_id = st.station_id
        WHERE s.station_id = ?
        """,
        (station_id, station_id),
    )
    row = c.fetchone()
    conn.close()

    if not row:
        return jsonify({"error": "Station not found"}), 404

    payload = {
        "station_id": row["station_id"],
        "name": row["name"],
        "lat": row["lat"],
        "lon": row["lon"],
        "capacity": row["capacity"],
        "is_installed": bool(row["is_installed"]) if row["is_installed"] is not None else None,
        "is_renting": bool(row["is_renting"]) if row["is_renting"] is not None else None,
        "is_returning": bool(row["is_returning"]) if row["is_returning"] is not None else None,
        "num_bikes_available": row["num_bikes_available"],
        "num_docks_available": row["num_docks_available"],
        "num_vehicles_disabled": row["num_bikes_disabled"],
        "num_docks_disabled": row["num_docks_disabled"],
        "latest_grab_time": row["grab_time"],
        "latest_grab_time_ms": row["grab_time"] * 1000 if row["grab_time"] is not None else None,
    }
    return jsonify(payload)

@app.route("/api/data/<station_id>")
def get_station_data(station_id):
    """ 
    Returns time-series data for a specific station.
    Accepts an optional URL param `last_time` to fetch only NEW records.
    """
    last_time = request.args.get('last_time', 0, type=int)
    conn = database.get_db_connection()
    c = conn.cursor()
    
    if last_time > 0:
         # Mimic logic seen in Lab06: fetch updates
         c.execute("SELECT grab_time, num_bikes_available FROM Status WHERE station_id = ? AND grab_time > ? ORDER BY grab_time ASC", (station_id, last_time))
    else:
         # Initial load: grab entire available history
         c.execute("SELECT grab_time, num_bikes_available FROM Status WHERE station_id = ? ORDER BY grab_time ASC", (station_id,))
         
    rows = c.fetchall()
    conn.close()
    
    # Highcharts requires timestamp in milliseconds
    data = [[row["grab_time"] * 1000, row["num_bikes_available"]] for row in rows]
    
    return jsonify(data)

@app.route("/api/data_docks/<station_id>")
def get_station_docks_data(station_id):
    """
    Returns available docks time-series for a specific station.
    Accepts an optional URL param `last_time` to fetch only NEW records.
    """
    last_time = request.args.get('last_time', 0, type=int)
    conn = database.get_db_connection()
    c = conn.cursor()

    if last_time > 0:
        c.execute(
            "SELECT grab_time, num_docks_available FROM Status WHERE station_id = ? AND grab_time > ? ORDER BY grab_time ASC",
            (station_id, last_time),
        )
    else:
        c.execute(
            "SELECT grab_time, num_docks_available FROM Status WHERE station_id = ? ORDER BY grab_time ASC",
            (station_id,),
        )

    rows = c.fetchall()
    conn.close()

    data = [[row["grab_time"] * 1000, row["num_docks_available"]] for row in rows]
    return jsonify(data)

@app.route("/api/debug/<station_id>")
def get_station_debug_data(station_id):
    """Returns full raw rows for debugging front-end chart issues."""
    conn = database.get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, station_id, num_bikes_available, num_docks_available, grab_time
        FROM Status
        WHERE station_id = ?
        ORDER BY grab_time ASC, id ASC
        """,
        (station_id,),
    )
    rows = c.fetchall()
    conn.close()

    debug_rows = []
    for row in rows:
        debug_rows.append(
            {
                "id": row["id"],
                "station_id": row["station_id"],
                "num_bikes_available": row["num_bikes_available"],
                "num_docks_available": row["num_docks_available"],
                "grab_time": row["grab_time"],
                "grab_time_ms": row["grab_time"] * 1000,
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